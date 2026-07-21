//! One-shot ACP model probe: spawn an adapter, `initialize` + `session/new`,
//! extract the model rows, kill the child.
//!
//! Ports the FE probe loop shared by claude-code / codex / pi / droid
//! (`*.ipc.ts` / `droid-acp-probe.ts`): models may arrive either in the
//! `session/new` result or in a `session/update`-style notification, so the
//! probe watches both. Every path is bounded by a hard overall timeout —
//! the probe never hangs and never panics.

use std::ffi::OsString;
use std::path::PathBuf;
use std::process::Stdio;
use std::time::Duration;

use intent_acp::{Connection, ConnectionHooks, JsonRpcError};
use intent_providers::enhanced_path;
use serde_json::{json, Value};
use tokio::io::AsyncRead;
use tokio::sync::mpsc;

/// Hard cap on the whole probe for resolved binaries (mirrors the FE's 15s
/// outer timeout).
const OVERALL_TIMEOUT: Duration = Duration::from_secs(15);
/// Per-request timeout for `initialize` for resolved binaries (FE: 4s).
const INITIALIZE_TIMEOUT: Duration = Duration::from_secs(4);
/// `initialize` budget for npx-run adapters: a cold `npx -y <pkg>@<version>`
/// downloads and installs the package before the adapter can answer, which
/// routinely takes tens of seconds. A pinned-version bump must not guarantee
/// a static-fallback cycle just because the cache is cold.
const NPX_INITIALIZE_TIMEOUT: Duration = Duration::from_secs(45);
/// Overall cap for npx-run adapters (cold install + handshake), kept bounded.
const NPX_OVERALL_TIMEOUT: Duration = Duration::from_secs(60);
/// Per-request timeout for `session/new` for resolved binaries (FE: 8–10s).
const SESSION_NEW_TIMEOUT: Duration = Duration::from_secs(10);
/// `session/new` budget for npx-run adapters: claude-agent-acp boots the
/// underlying CLI while creating the session, which alone takes ~10s even
/// with a warm npx cache — a flat 10s budget times out right at the wire.
const NPX_SESSION_NEW_TIMEOUT: Duration = Duration::from_secs(20);
/// Grace window to catch a late model notification after an empty
/// `session/new` result.
const NOTIFICATION_GRACE: Duration = Duration::from_secs(2);

/// How to launch the adapter for a probe.
pub(super) struct AcpProbeCommand {
    program: PathBuf,
    args: Vec<String>,
    envs: Vec<(String, OsString)>,
    /// npx-run probes get the longer cold-install timeout budget.
    via_npx: bool,
}

impl AcpProbeCommand {
    /// Run a pinned npm package via `npx -y <package>`.
    pub(super) fn npx(npx: PathBuf, package: &str) -> Self {
        Self {
            program: npx,
            args: vec!["-y".to_string(), package.to_string()],
            envs: Vec::new(),
            via_npx: true,
        }
    }

    /// Run a resolved adapter binary with the given args.
    pub(super) fn binary(bin: PathBuf, args: Vec<String>) -> Self {
        Self {
            program: bin,
            args,
            envs: Vec::new(),
            via_npx: false,
        }
    }

    /// Add an environment-variable override for the probe child.
    pub(super) fn env(mut self, key: impl Into<String>, value: impl Into<OsString>) -> Self {
        self.envs.push((key.into(), value.into()));
        self
    }

    #[cfg(test)]
    pub(super) fn env_vars(&self) -> &[(String, OsString)] {
        &self.envs
    }

    fn initialize_timeout(&self) -> Duration {
        if self.via_npx {
            NPX_INITIALIZE_TIMEOUT
        } else {
            INITIALIZE_TIMEOUT
        }
    }

    fn session_new_timeout(&self) -> Duration {
        if self.via_npx {
            NPX_SESSION_NEW_TIMEOUT
        } else {
            SESSION_NEW_TIMEOUT
        }
    }

    fn overall_timeout(&self) -> Duration {
        if self.via_npx {
            NPX_OVERALL_TIMEOUT
        } else {
            OVERALL_TIMEOUT
        }
    }
}

/// Machine-readable probe failure reasons.
#[derive(Debug)]
pub(super) enum ProbeError {
    /// The adapter process could not be spawned.
    Spawn(String),
    /// A handshake request failed at the transport level or timed out.
    Transport(String),
    /// The adapter returned a JSON-RPC error (auth detection keys off this).
    Rpc(JsonRpcError),
    /// The whole probe hit the hard overall timeout.
    Timeout,
    /// The handshake succeeded but no models were reported.
    Empty,
    /// The adapter process exited before completing the handshake (e.g. a
    /// corrupt npx cache producing an ENOENT from node); carries the exit
    /// status plus the last stderr line when available.
    Exited(String),
}

impl std::fmt::Display for ProbeError {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        match self {
            ProbeError::Spawn(e) => write!(f, "failed to spawn adapter: {e}"),
            ProbeError::Transport(e) => write!(f, "probe transport failed: {e}"),
            ProbeError::Rpc(e) => write!(f, "adapter returned an error: {e}"),
            ProbeError::Timeout => write!(f, "model probe timed out"),
            ProbeError::Empty => write!(f, "no models reported"),
            ProbeError::Exited(detail) => {
                write!(f, "adapter exited before reporting models: {detail}")
            }
        }
    }
}

/// Spawn the adapter, drive the probe handshake, and reap the child.
pub(super) async fn run_acp_probe<F>(
    cmd: AcpProbeCommand,
    extract: F,
) -> Result<Vec<Value>, ProbeError>
where
    F: Fn(&Value) -> Vec<Value>,
{
    let mut command = tokio::process::Command::new(&cmd.program);
    command
        .args(&cmd.args)
        .current_dir(std::env::temp_dir())
        .env("PATH", enhanced_path(Some(&cmd.program)))
        .stdin(Stdio::piped())
        .stdout(Stdio::piped())
        .stderr(Stdio::piped())
        .kill_on_drop(true);
    for (key, value) in &cmd.envs {
        command.env(key, value);
    }
    #[cfg(unix)]
    command.process_group(0);

    let mut child = command
        .spawn()
        .map_err(|e| ProbeError::Spawn(format!("{}: {e}", cmd.program.display())))?;
    let stdin = child
        .stdin
        .take()
        .ok_or_else(|| ProbeError::Spawn("child stdin not piped".to_string()))?;
    let stdout = child
        .stdout
        .take()
        .ok_or_else(|| ProbeError::Spawn("child stdout not piped".to_string()))?;
    let stderr = child
        .stderr
        .take()
        .map(|s| Box::new(s) as Box<dyn AsyncRead + Unpin + Send>);

    let (note_tx, note_rx) = mpsc::unbounded_channel();
    let hooks = ConnectionHooks {
        notifications: Some(note_tx),
        ..Default::default()
    };
    let conn = Connection::new(stdin, stdout, stderr, hooks);

    let result = tokio::time::timeout(
        cmd.overall_timeout(),
        drive_probe(
            &conn,
            note_rx,
            extract,
            cmd.initialize_timeout(),
            cmd.session_new_timeout(),
        ),
    )
    .await
    .unwrap_or(Err(ProbeError::Timeout));

    let result = result.map_err(|err| attribute_early_exit(err, &mut child, &conn));
    reap_child(&mut child).await;
    result
}

/// Fold an early adapter exit into the probe error: when the child already
/// died before the handshake finished (e.g. a corrupt `~/.npm/_npx` entry
/// making node fail with ENOENT), report its exit status and last stderr
/// line instead of a generic transport/timeout/empty reason. Spawn and RPC
/// errors pass through untouched (auth detection keys off `Rpc`).
fn attribute_early_exit(
    err: ProbeError,
    child: &mut tokio::process::Child,
    conn: &Connection,
) -> ProbeError {
    if matches!(err, ProbeError::Spawn(_) | ProbeError::Rpc(_)) {
        return err;
    }
    let Ok(Some(status)) = child.try_wait() else {
        return err;
    };
    let stderr = conn.recent_stderr();
    let tail = match stderr.last() {
        Some(line) => {
            let trimmed = line.trim();
            let bounded: String = trimmed
                .chars()
                .skip(trimmed.chars().count().saturating_sub(200))
                .collect();
            format!("; stderr: {bounded}")
        }
        None => String::new(),
    };
    ProbeError::Exited(format!("{status}{tail}"))
}

/// Grace window between SIGTERM and SIGKILL when reaping the probe child
/// (mirrors `host_exec::TERM_GRACE` / `mcp_servers::reap`).
const TERM_GRACE: Duration = Duration::from_millis(500);

/// Kill the probe child and reap it. Signals the whole process group (the
/// child is its own group leader via `process_group(0)`) so grandchildren
/// (e.g. `npx` → `node`) die too, following the crate's SIGTERM → grace →
/// SIGKILL pattern, then waits briefly so the child does not linger as a
/// zombie. `kill_on_drop(true)` back-stops any wait timeout.
async fn reap_child(child: &mut tokio::process::Child) {
    #[cfg(unix)]
    if let Some(pid) = child.id() {
        use nix::sys::signal::{killpg, Signal};
        use nix::unistd::Pid;
        let pgid = Pid::from_raw(pid as i32);
        let _ = killpg(pgid, Signal::SIGTERM);
        tokio::time::sleep(TERM_GRACE).await;
        if !matches!(child.try_wait(), Ok(Some(_))) {
            let _ = killpg(pgid, Signal::SIGKILL);
        }
    }
    let _ = child.kill().await;
    let _ = tokio::time::timeout(Duration::from_secs(2), child.wait()).await;
}

/// `initialize` → `session/new`, watching for model rows in either the
/// `session/new` result or a `session/update`-style notification.
async fn drive_probe<F>(
    conn: &Connection,
    mut notifications: mpsc::UnboundedReceiver<intent_acp::IncomingNotification>,
    extract: F,
    initialize_timeout: Duration,
    session_new_timeout: Duration,
) -> Result<Vec<Value>, ProbeError>
where
    F: Fn(&Value) -> Vec<Value>,
{
    let init_params = json!({
        "protocolVersion": 1,
        "clientInfo": { "name": "Intent", "version": env!("CARGO_PKG_VERSION") },
        "clientCapabilities": { "fs": { "readTextFile": false, "writeTextFile": false } },
    });
    conn.request_timeout("initialize", init_params, initialize_timeout)
        .await
        .map_err(map_acp_error)?;

    let cwd = std::env::temp_dir();
    let session_params = json!({
        "cwd": cwd.to_string_lossy(),
        "mcpServers": [],
    });

    // Race the session/new response against model notifications: some
    // adapters publish the catalog via a session update before (or instead
    // of) including it in the session/new result.
    let session_new = conn.request_timeout("session/new", session_params, session_new_timeout);
    tokio::pin!(session_new);
    let mut notifications_open = true;
    let session_result = loop {
        tokio::select! {
            resp = &mut session_new => break resp,
            note = notifications.recv(), if notifications_open => {
                match note {
                    Some(note) => {
                        if is_model_update_method(&note.method) {
                            let models = extract(&note.params);
                            if !models.is_empty() {
                                return Ok(models);
                            }
                        }
                    }
                    // Channel closed (connection dropped the sender): disable
                    // this branch so the select! cannot busy-spin and the
                    // session/new future still resolves (or times out).
                    None => notifications_open = false,
                }
            }
        }
    };

    let result = session_result.map_err(map_acp_error)?;
    let models = extract(&result);
    if !models.is_empty() {
        return Ok(models);
    }

    // Empty session/new result: give a late notification a short grace window.
    let deadline = tokio::time::Instant::now() + NOTIFICATION_GRACE;
    while let Ok(Some(note)) = tokio::time::timeout_at(deadline, notifications.recv()).await {
        if is_model_update_method(&note.method) {
            let models = extract(&note.params);
            if !models.is_empty() {
                return Ok(models);
            }
        }
    }
    Err(ProbeError::Empty)
}

/// Notification method names adapters use to publish the model catalog
/// (parity with the FE probes' accepted variants).
fn is_model_update_method(method: &str) -> bool {
    matches!(
        method,
        "sessionUpdate" | "session/update" | "session/updateModels"
    )
}

fn map_acp_error(err: intent_acp::AcpError) -> ProbeError {
    match err {
        intent_acp::AcpError::Rpc(e) => ProbeError::Rpc(e),
        other => ProbeError::Transport(other.to_string()),
    }
}
