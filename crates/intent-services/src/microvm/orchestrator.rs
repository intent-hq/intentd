//! Per-agent VM lifecycle: boot, guest setup, provider exec, teardown
//! (monorepo#1120, EE-5).

use std::collections::BTreeMap;
use std::path::{Path, PathBuf};
use std::process::Stdio;
use std::time::{Duration, Instant};

use intent_core::{AgentId, WorkspaceId};
use tokio::io::{AsyncRead, AsyncWriteExt};
use tokio::process::{Child, Command};

use super::auth::{self, RotationWatcher};
use super::exec::{self, ExecRequest};
use super::rootfs;
use super::{MicrovmError, GUEST_INTENT_DIR, GUEST_WORKSPACE_DIR, WORKSPACE_VIRTIOFS_TAG};
use crate::sandbox_image::CachedImage;

/// Environment variable overriding the helper binary location.
pub const HELPER_EXE_ENV: &str = "INTENTD_MICROVM_HELPER";

/// Helper binary basename, resolved next to the daemon executable by default.
pub const HELPER_EXE_NAME: &str = "intentd-microvm-helper";

/// Total budget for the guest exec agent to come up after helper spawn.
const BOOT_READY_TIMEOUT: Duration = Duration::from_secs(60);
/// Poll interval for the readiness probe.
const BOOT_READY_POLL: Duration = Duration::from_millis(250);
/// Budget for each guest setup command.
const SETUP_TIMEOUT: Duration = Duration::from_secs(30);

/// Guest stderr capture file for the provider (inside the per-VM rootfs, so
/// host-visible at `<vm rootfs>/intent/acp.err`).
const GUEST_STDERR_LOG: &str = "/intent/acp.err";

/// The stdio↔TCP MCP bridge script staged into the guest (`node
/// /intent/mcp-bridge.mjs <host:port>`): the guest-side analog of
/// `intentd mcp-bridge --connect` (the macOS daemon binary cannot run in the
/// Linux guest; TSI routes the TCP connect to the host loopback listener).
/// Both sides speak line-framed JSON-RPC, so a plain byte pipe suffices.
pub const GUEST_MCP_BRIDGE_SOURCE: &str = r#"import net from 'node:net';
const addr = process.argv[2] || '';
const idx = addr.lastIndexOf(':');
if (idx <= 0) { console.error('usage: mcp-bridge.mjs <host:port>'); process.exit(2); }
const sock = net.connect({ host: addr.slice(0, idx), port: Number(addr.slice(idx + 1)) });
sock.on('error', () => process.exit(1));
sock.on('close', () => process.exit(0));
process.stdin.pipe(sock);
sock.pipe(process.stdout);
"#;

/// Guest path of the staged MCP bridge script.
pub const GUEST_MCP_BRIDGE_PATH: &str = "/intent/mcp-bridge.mjs";

/// Everything needed to boot one agent VM.
pub struct MicrovmSpawnSpec {
    /// Per-VM state directory (`<data_dir>/microvm/<agent-id>`).
    pub vm_dir: PathBuf,
    /// The resolved microVM helper binary.
    pub helper_exe: PathBuf,
    /// The verified cached image (manifest + rootfs archive).
    pub image: CachedImage,
    /// Host path of the agent's CoW workspace clone (mounted at
    /// [`GUEST_WORKSPACE_DIR`] via virtio-fs).
    pub workspace_dir: PathBuf,
    /// Host home directory for credential staging.
    pub host_home: PathBuf,
    /// `providers.claudeCodeOauthToken` when set — enables the claude-code
    /// guest onboarding file (the token itself rides the provider exec env).
    pub stage_claude_onboarding: bool,
    pub vcpus: u8,
    pub mem_mib: u32,
}

/// A booted per-agent VM: the helper child *is* the VM.
pub struct MicrovmVm {
    /// Per-VM state dir (rootfs clone, exec socket, console log).
    pub vm_dir: PathBuf,
    /// The per-VM rootfs tree (CoW clone of the extracted image).
    pub rootfs: PathBuf,
    /// Host unix socket forwarded to the guest exec agent's vsock port.
    pub exec_sock: PathBuf,
    /// The helper process; killed (whole group) to tear the VM down. Taken
    /// by the caller for handle ownership.
    pub child: Option<Child>,
    /// Host-side credential rotation watcher; dropped at teardown.
    pub rotation_watcher: Option<RotationWatcher>,
    /// Milliseconds from helper spawn to guest exec agent readiness.
    pub boot_ms: u64,
    /// Event context for the best-effort `sandbox:vm:stopped` emit on Drop.
    pub stop_event: Option<(crate::events::EventBus, WorkspaceId, AgentId)>,
}

/// Resolve the helper binary: `$INTENTD_MICROVM_HELPER` override, else
/// `intentd-microvm-helper` next to the current executable.
pub fn resolve_helper_exe() -> Result<PathBuf, MicrovmError> {
    if let Some(p) = std::env::var_os(HELPER_EXE_ENV) {
        let p = PathBuf::from(p);
        if p.is_file() {
            return Ok(p);
        }
        return Err(MicrovmError::HelperMissing(format!(
            "{HELPER_EXE_ENV}={} does not exist",
            p.display()
        )));
    }
    let sibling = std::env::current_exe()
        .ok()
        .and_then(|exe| exe.parent().map(|d| d.join(HELPER_EXE_NAME)));
    match sibling {
        Some(p) if p.is_file() => Ok(p),
        _ => Err(MicrovmError::HelperMissing(format!(
            "{HELPER_EXE_NAME} not found next to the daemon binary (set {HELPER_EXE_ENV} to override)"
        ))),
    }
}

impl MicrovmVm {
    /// Boot a VM per `spec`: materialize the per-VM rootfs (CoW clone of the
    /// extracted image tree), stage credentials + spawn material into it,
    /// spawn the helper, and wait for the guest exec agent to answer.
    pub async fn boot(spec: &MicrovmSpawnSpec) -> Result<Self, MicrovmError> {
        // Fresh per-VM state dir: any leftover from a crashed prior VM is
        // scrubbed first (also deletes previously staged credentials).
        if tokio::fs::try_exists(&spec.vm_dir).await.unwrap_or(false) {
            tokio::fs::remove_dir_all(&spec.vm_dir)
                .await
                .map_err(|e| MicrovmError::Io(format!("scrub stale vm dir: {e}")))?;
        }
        tokio::fs::create_dir_all(&spec.vm_dir)
            .await
            .map_err(|e| MicrovmError::Io(format!("create vm dir: {e}")))?;

        // 1. Per-VM rootfs: extract-once cache + CoW clone.
        let tree = rootfs::ensure_extracted_tree(&spec.image.rootfs_path).await?;
        let vm_rootfs = spec.vm_dir.join("rootfs");
        rootfs::clone_vm_rootfs(&tree, &vm_rootfs).await?;

        // 2. Stage credentials + git identity into the guest home, and the
        // spawn-material directory (/intent) into the rootfs.
        let guest_home = vm_rootfs.join("root");
        let host_home = spec.host_home.clone();
        let guest_home_clone = guest_home.clone();
        let stage_claude = spec.stage_claude_onboarding;
        let staged = tokio::task::spawn_blocking(move || {
            let staged = auth::stage_all(&host_home, &guest_home_clone)?;
            auth::stage_gitconfig(&host_home, &guest_home_clone)?;
            if stage_claude {
                // Minimal onboarding state the Claude Agent SDK checks; the
                // token itself rides the provider exec env, never disk.
                std::fs::write(
                    guest_home_clone.join(".claude.json"),
                    b"{\"hasCompletedOnboarding\":true}",
                )
                .map_err(|e| MicrovmError::AuthStage(format!("write .claude.json: {e}")))?;
            }
            Ok::<_, MicrovmError>(staged)
        })
        .await
        .map_err(|e| MicrovmError::AuthStage(format!("staging task panicked: {e}")))??;

        let intent_dir = vm_rootfs.join(GUEST_INTENT_DIR);
        tokio::fs::create_dir_all(&intent_dir)
            .await
            .map_err(|e| MicrovmError::Io(format!("create /intent dir: {e}")))?;

        // 3. Spawn the helper. The guest command is the image's init
        // entrypoint; everything else rides the exec protocol later.
        let exec_sock = spec.vm_dir.join("exec.sock");
        let console_log = spec.vm_dir.join("console.log");
        let mut cmd = Command::new(&spec.helper_exe);
        cmd.arg("--root-fs")
            .arg(&vm_rootfs)
            .arg("--vcpus")
            .arg(spec.vcpus.to_string())
            .arg("--mem-mib")
            .arg(spec.mem_mib.to_string())
            .arg("--virtiofs")
            .arg(format!(
                "{WORKSPACE_VIRTIOFS_TAG}={}",
                spec.workspace_dir.display()
            ))
            .arg("--vsock-listen")
            .arg(format!(
                "{}={}",
                spec.image.manifest.vsock_exec.port,
                exec_sock.display()
            ))
            .arg("--console-log")
            .arg(&console_log)
            .arg("--")
            .arg(&spec.image.manifest.vsock_exec.init);
        cmd.stdin(Stdio::null())
            .stdout(Stdio::null())
            .stderr(Stdio::piped())
            .kill_on_drop(true);
        // Own process group so teardown can killpg the whole VM tree (same
        // rationale as provider children, §5.6).
        #[cfg(unix)]
        cmd.process_group(0);
        let started = Instant::now();
        let mut child = cmd
            .spawn()
            .map_err(|e| MicrovmError::Boot(format!("spawn helper: {e}")))?;

        // Drain helper stderr into the tracing log (bounded lines).
        if let Some(stderr) = child.stderr.take() {
            tokio::spawn(async move {
                use tokio::io::AsyncBufReadExt;
                let mut lines = tokio::io::BufReader::new(stderr).lines();
                while let Ok(Some(line)) = lines.next_line().await {
                    tracing::debug!(target: "microvm_helper", "{line}");
                }
            });
        }

        // 4. Readiness: poll the forwarded exec socket with a trivial guest
        // command until the exec agent answers (bounded).
        let ready = wait_for_exec_agent(&exec_sock, &mut child).await;
        if let Err(e) = ready {
            let _ = child.start_kill();
            let _ = child.wait().await;
            return Err(e);
        }
        let boot_ms = started.elapsed().as_millis() as u64;

        // 5. Rotation watcher for the staged credentials.
        let rotation_watcher = match auth::watch_rotations(staged, guest_home) {
            Ok(w) => Some(w),
            Err(e) => {
                // Non-fatal: rotations just won't push until the next spawn.
                tracing::warn!(error = %e, "credential rotation watcher failed to start");
                None
            }
        };

        Ok(Self {
            vm_dir: spec.vm_dir.clone(),
            rootfs: vm_rootfs,
            exec_sock,
            child: Some(child),
            rotation_watcher,
            boot_ms,
            stop_event: None,
        })
    }

    /// Run the in-guest setup sequence: mount the workspace share and wire the
    /// gh git credential helper. Must complete before the provider launches.
    pub async fn guest_setup(&self) -> Result<(), MicrovmError> {
        // Mount the workspace virtio-fs share. Idempotent-ish: a second mount
        // attempt on a mounted target fails, but each VM runs this once.
        let script = format!(
            "mkdir -p {ws} && mount -t virtiofs {tag} {ws} && \
             (gh auth setup-git >/dev/null 2>&1 || true)",
            ws = GUEST_WORKSPACE_DIR,
            tag = WORKSPACE_VIRTIOFS_TAG,
        );
        let req = ExecRequest {
            argv: vec!["/bin/sh".into(), "-c".into(), script],
            env: BTreeMap::new(),
            cwd: "/".into(),
            merge_stderr: true,
        };
        let out = exec::run_to_completion(&self.exec_sock, &req, SETUP_TIMEOUT).await?;
        // The exec protocol carries no exit status; verify the mount landed.
        let verify = ExecRequest {
            argv: vec![
                "/bin/sh".into(),
                "-c".into(),
                format!("mountpoint -q {GUEST_WORKSPACE_DIR} && echo MOUNT_OK"),
            ],
            env: BTreeMap::new(),
            cwd: "/".into(),
            merge_stderr: true,
        };
        let verify_out = exec::run_to_completion(&self.exec_sock, &verify, SETUP_TIMEOUT).await?;
        if !String::from_utf8_lossy(&verify_out).contains("MOUNT_OK") {
            return Err(MicrovmError::GuestSetup(format!(
                "workspace virtio-fs mount failed: {}",
                String::from_utf8_lossy(&out).trim()
            )));
        }
        Ok(())
    }

    /// Stage the guest MCP bridge script into `/intent/mcp-bridge.mjs`.
    pub async fn stage_mcp_bridge(&self) -> Result<(), MicrovmError> {
        let path = self.rootfs.join(GUEST_INTENT_DIR).join("mcp-bridge.mjs");
        tokio::fs::write(&path, GUEST_MCP_BRIDGE_SOURCE)
            .await
            .map_err(|e| MicrovmError::Io(format!("stage mcp bridge: {e}")))?;
        Ok(())
    }

    /// Write `content` into the guest at `/intent/<name>`, returning the
    /// guest-absolute path.
    pub async fn stage_intent_file(
        &self,
        name: &str,
        content: &[u8],
    ) -> Result<String, MicrovmError> {
        let path = self.rootfs.join(GUEST_INTENT_DIR).join(name);
        tokio::fs::write(&path, content)
            .await
            .map_err(|e| MicrovmError::Io(format!("stage {name}: {e}")))?;
        Ok(format!("/{GUEST_INTENT_DIR}/{name}"))
    }

    /// Launch the provider in the guest, returning the raw ACP stream halves
    /// plus a host-side stderr tail. The provider command is wrapped in a
    /// shell that re-redirects stderr to [`GUEST_STDERR_LOG`] (host-visible
    /// through the rootfs) so the socket carries ONLY the ACP stdio bytes —
    /// stdin stays a pipe, never a TTY.
    pub async fn start_provider(
        &self,
        argv: &[String],
        env: &BTreeMap<String, String>,
    ) -> Result<
        (
            tokio::net::unix::OwnedWriteHalf,
            tokio::net::unix::OwnedReadHalf,
            Box<dyn AsyncRead + Unpin + Send>,
        ),
        MicrovmError,
    > {
        let quoted: Vec<String> = argv.iter().map(|a| sh_squote(a)).collect();
        let script = format!("exec {} 2>{}", quoted.join(" "), GUEST_STDERR_LOG);
        let req = ExecRequest {
            argv: vec!["/bin/sh".into(), "-c".into(), script],
            env: env.clone(),
            cwd: GUEST_WORKSPACE_DIR.into(),
            // stderr is re-redirected by the wrapper; "discard" keeps the
            // socket clean even before the shell's redirect engages.
            merge_stderr: false,
        };
        let exec = exec::start(&self.exec_sock, &req).await?;
        let (read_half, write_half) = exec.stream.into_split();
        let stderr_tail = tail_file(self.rootfs.join(GUEST_INTENT_DIR).join("acp.err"));
        Ok((write_half, read_half, stderr_tail))
    }

    /// Take the helper child out of the VM (the caller owns/tears down the
    /// process; this struct keeps owning the on-disk state + watcher).
    pub fn take_child(&mut self) -> Option<Child> {
        self.child.take()
    }
}

/// Dropping the VM scrubs its state: the rotation watcher stops (own Drop),
/// any still-owned helper child dies via `kill_on_drop`, and the per-VM
/// directory — rootfs clone with every staged credential inside it, exec
/// socket, console log — is removed on a detached thread (Drop cannot be
/// async, and the tree can be large). Every handle-teardown path drops the
/// VM after killing the helper's process group, so the scrub is universal;
/// a daemon crash instead scrubs lazily at the next boot for the same agent
/// (`MicrovmVm::boot` removes a stale vm dir first).
impl Drop for MicrovmVm {
    fn drop(&mut self) {
        let dir = self.vm_dir.clone();
        std::thread::spawn(move || {
            if let Err(e) = std::fs::remove_dir_all(&dir) {
                if e.kind() != std::io::ErrorKind::NotFound {
                    tracing::warn!(dir = %dir.display(), error = %e, "microVM scrub failed");
                }
            }
        });
        // Best-effort `sandbox:vm:stopped` — only when a runtime is live
        // (Drop may run during runtime shutdown, where publishing is moot).
        if let Some((bus, workspace_id, agent_id)) = self.stop_event.take() {
            if let Ok(handle) = tokio::runtime::Handle::try_current() {
                handle.spawn(async move {
                    let event = intent_store::NewEvent {
                        workspace_id: workspace_id.clone(),
                        timestamp: intent_core::now_iso(),
                        event_type: intent_core::events::SANDBOX_VM_STOPPED.to_string(),
                        actor: crate::system_actor(),
                        session_id: Some(agent_id.0.clone()),
                        correlation_id: None,
                        parent_event_id: None,
                        metadata: None,
                        data: serde_json::json!({
                            "workspaceId": workspace_id.as_str(),
                            "agentId": agent_id.as_str(),
                        }),
                    };
                    if let Err(e) = bus.publish(&event).await {
                        tracing::debug!(error = %e, "sandbox:vm:stopped publish failed");
                    }
                });
            }
        }
    }
}

/// Poll the exec socket with a trivial command until the guest agent answers,
/// failing fast when the helper process exits first.
async fn wait_for_exec_agent(sock: &Path, child: &mut Child) -> Result<(), MicrovmError> {
    let deadline = Instant::now() + BOOT_READY_TIMEOUT;
    let probe = ExecRequest {
        argv: vec!["/bin/true".into()],
        env: BTreeMap::new(),
        cwd: "/".into(),
        merge_stderr: false,
    };
    loop {
        if let Ok(Some(status)) = child.try_wait() {
            return Err(MicrovmError::Boot(format!(
                "microVM helper exited during boot ({status}); see console.log in the VM dir"
            )));
        }
        match exec::run_to_completion(sock, &probe, BOOT_READY_POLL * 4).await {
            Ok(_) => return Ok(()),
            Err(_) if Instant::now() < deadline => {
                tokio::time::sleep(BOOT_READY_POLL).await;
            }
            Err(e) => {
                return Err(MicrovmError::Boot(format!(
                    "guest exec agent not ready within {BOOT_READY_TIMEOUT:?}: {e}"
                )))
            }
        }
    }
}

/// Host-side tail of a guest-written file, exposed as an `AsyncRead` for the
/// ACP connection's stderr task: polls for appended bytes and feeds them into
/// a duplex pipe. Ends when the writer side is dropped (VM teardown).
fn tail_file(path: PathBuf) -> Box<dyn AsyncRead + Unpin + Send> {
    let (reader, mut writer) = tokio::io::duplex(64 * 1024);
    tokio::spawn(async move {
        let mut offset: u64 = 0;
        loop {
            tokio::time::sleep(Duration::from_millis(500)).await;
            let Ok(data) = tokio::fs::read(&path).await else {
                continue;
            };
            if (data.len() as u64) > offset {
                let chunk = &data[offset as usize..];
                if writer.write_all(chunk).await.is_err() {
                    return; // reader side dropped (connection torn down)
                }
                offset = data.len() as u64;
            }
        }
    });
    Box::new(reader)
}

/// Single-quote a string for inert interpolation into a `sh -c` script.
fn sh_squote(s: &str) -> String {
    format!("'{}'", s.replace('\'', "'\\''"))
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn sh_squote_escapes_single_quotes() {
        assert_eq!(sh_squote("plain"), "'plain'");
        assert_eq!(sh_squote("it's"), "'it'\\''s'");
    }

    #[test]
    fn provider_wrapper_script_shape() {
        let argv = ["auggie".to_string(), "--acp".to_string()];
        let quoted: Vec<String> = argv.iter().map(|a| sh_squote(a)).collect();
        let script = format!("exec {} 2>{}", quoted.join(" "), GUEST_STDERR_LOG);
        assert_eq!(script, "exec 'auggie' '--acp' 2>/intent/acp.err");
    }

    #[test]
    fn resolve_helper_env_override_missing_is_error() {
        // Guard against env pollution across parallel tests by using the
        // documented error path with an explicit bogus value.
        std::env::set_var(HELPER_EXE_ENV, "/nonexistent/helper");
        let err = resolve_helper_exe().unwrap_err();
        std::env::remove_var(HELPER_EXE_ENV);
        assert!(matches!(err, MicrovmError::HelperMissing(_)));
    }
}
