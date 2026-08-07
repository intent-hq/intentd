//! Shared plumbing for ephemeral ACP adapter runs: how to launch the adapter,
//! the npx-aware per-stage timeout budgets, the `initialize` params, and the
//! teardown/exit-attribution helpers.
//!
//! Two callers share this module: the model probe
//! ([`crate::provider_models`], "spawn → initialize → session/new → collect
//! models → kill") and the one-shot completion runner
//! ([`crate::one_shot_acp`], which adds a `session/prompt` phase). Everything
//! here is stage-agnostic — the stage sequencing itself lives with each
//! caller.

use std::ffi::OsString;
use std::path::PathBuf;
use std::process::Stdio;
use std::time::Duration;

#[cfg(unix)]
use intent_acp::{descendant_pids, sweep_escaped_descendants};
use intent_acp::{Connection, ConnectionHooks, IncomingNotification, IncomingRequest};
use intent_providers::enhanced_path;
use serde_json::{json, Value};
use tokio::io::AsyncRead;
use tokio::sync::mpsc;

/// Hard cap on the setup phase (`initialize` + `session/new`) for resolved
/// binaries (mirrors the FE's 15s outer timeout). Deliberately smaller than
/// the sum of the per-stage budgets (4s + 10s + 2s grace), matching the FE:
/// the outer cap is the real bound and preempts slow-but-not-stuck stages.
const OVERALL_TIMEOUT: Duration = Duration::from_secs(15);
/// Per-request timeout for `initialize` for resolved binaries (FE: 4s).
const INITIALIZE_TIMEOUT: Duration = Duration::from_secs(4);
/// `initialize` budget for npx-run adapters: a cold `npx -y <pkg>@<version>`
/// downloads and installs the package before the adapter can answer, which
/// routinely takes tens of seconds. A pinned-version bump must not guarantee
/// a static-fallback cycle just because the cache is cold.
const NPX_INITIALIZE_TIMEOUT: Duration = Duration::from_secs(45);
/// Overall setup cap for npx-run adapters, kept bounded but sized to cover
/// the full per-stage sum (45s initialize + 20s session/new + 2s grace) so a
/// cold install that eats the initialize budget cannot starve `session/new`
/// of its own window. This is also the worst-case latency of a `forceRefresh`
/// `models.list` against a hung npx adapter — an accepted trade-off for
/// surviving cold installs.
const NPX_OVERALL_TIMEOUT: Duration = Duration::from_secs(70);
/// Per-request timeout for `session/new` for resolved binaries (FE: 8–10s).
const SESSION_NEW_TIMEOUT: Duration = Duration::from_secs(10);
/// `session/new` budget for npx-run adapters: claude-agent-acp boots the
/// underlying CLI while creating the session, which alone takes ~10s even
/// with a warm npx cache — a flat 10s budget times out right at the wire.
const NPX_SESSION_NEW_TIMEOUT: Duration = Duration::from_secs(20);

/// How to launch an ephemeral ACP adapter.
pub(crate) struct AcpAdapterCommand {
    program: PathBuf,
    args: Vec<String>,
    envs: Vec<(String, OsString)>,
    envs_removed: Vec<String>,
    /// Working directory for the child and the `session/new` `cwd`. `None`
    /// runs the adapter in the system temp dir (the ephemeral default).
    cwd: Option<PathBuf>,
    /// npx-run adapters get the longer cold-install timeout budget.
    via_npx: bool,
}

impl AcpAdapterCommand {
    /// Run a pinned npm package via `npx -y <package>`.
    pub(crate) fn npx(npx: PathBuf, package: &str) -> Self {
        Self {
            program: npx,
            args: vec!["-y".to_string(), package.to_string()],
            envs: Vec::new(),
            envs_removed: Vec::new(),
            cwd: None,
            via_npx: true,
        }
    }

    /// Run a resolved adapter binary with the given args.
    pub(crate) fn binary(bin: PathBuf, args: Vec<String>) -> Self {
        Self {
            program: bin,
            args,
            envs: Vec::new(),
            envs_removed: Vec::new(),
            cwd: None,
            via_npx: false,
        }
    }

    /// Append extra launch arguments after the ones already assembled.
    pub(crate) fn args(mut self, extra: impl IntoIterator<Item = String>) -> Self {
        self.args.extend(extra);
        self
    }

    /// Pin the adapter's working directory (also used as the `session/new`
    /// `cwd`). Callers that leave this unset run in the system temp dir.
    pub(crate) fn cwd(mut self, dir: PathBuf) -> Self {
        self.cwd = Some(dir);
        self
    }

    /// The effective working directory for this launch.
    pub(crate) fn working_dir(&self) -> PathBuf {
        self.cwd.clone().unwrap_or_else(std::env::temp_dir)
    }

    /// Add an environment-variable override for the adapter child.
    pub(crate) fn env(mut self, key: impl Into<String>, value: impl Into<OsString>) -> Self {
        self.envs.push((key.into(), value.into()));
        self
    }

    /// Remove an environment variable from the adapter child's inherited env.
    pub(crate) fn env_remove(mut self, key: impl Into<String>) -> Self {
        self.envs_removed.push(key.into());
        self
    }

    #[cfg(test)]
    pub(crate) fn env_vars(&self) -> &[(String, OsString)] {
        &self.envs
    }

    #[cfg(test)]
    pub(crate) fn removed_env_vars(&self) -> &[String] {
        &self.envs_removed
    }

    /// Per-request `initialize` budget for this launch.
    pub(crate) fn initialize_timeout(&self) -> Duration {
        if self.via_npx {
            NPX_INITIALIZE_TIMEOUT
        } else {
            INITIALIZE_TIMEOUT
        }
    }

    /// Per-request `session/new` budget for this launch.
    pub(crate) fn session_new_timeout(&self) -> Duration {
        if self.via_npx {
            NPX_SESSION_NEW_TIMEOUT
        } else {
            SESSION_NEW_TIMEOUT
        }
    }

    /// Cap on the whole setup phase (`initialize` + `session/new`) for this
    /// launch. The one-shot runner bounds its `session/prompt` phase
    /// separately with the caller's timeout.
    pub(crate) fn setup_timeout(&self) -> Duration {
        if self.via_npx {
            NPX_OVERALL_TIMEOUT
        } else {
            OVERALL_TIMEOUT
        }
    }
}

/// A spawned adapter: the child, its ACP connection, and the inbound
/// notification/request streams the caller drives.
pub(crate) struct SpawnedAdapter {
    /// The adapter process (reap with [`reap_child`]).
    pub(crate) child: tokio::process::Child,
    /// The JSON-RPC connection over the child's piped stdio.
    pub(crate) conn: Connection,
    /// Agent → client notifications (`session/update`, …).
    pub(crate) notifications: mpsc::UnboundedReceiver<IncomingNotification>,
    /// Agent → client requests (`session/request_permission`, `fs/*`, …).
    pub(crate) requests: mpsc::UnboundedReceiver<IncomingRequest>,
}

/// Spawn the adapter with piped stdio, its own process group, and the
/// enhanced PATH, and wire an ACP [`Connection`] around it. Returns the
/// spawn-failure detail as `Err` so callers can map it onto their own error
/// type. The child is `kill_on_drop`, so an early return still reaps it.
pub(crate) fn spawn_adapter(cmd: &AcpAdapterCommand) -> Result<SpawnedAdapter, String> {
    let mut command = tokio::process::Command::new(&cmd.program);
    command
        .args(&cmd.args)
        .current_dir(cmd.working_dir())
        .env("PATH", enhanced_path(Some(&cmd.program)))
        .stdin(Stdio::piped())
        .stdout(Stdio::piped())
        .stderr(Stdio::piped())
        .kill_on_drop(true);
    for (key, value) in &cmd.envs {
        command.env(key, value);
    }
    for key in &cmd.envs_removed {
        command.env_remove(key);
    }
    #[cfg(unix)]
    command.process_group(0);

    let mut child = command
        .spawn()
        .map_err(|e| format!("{}: {e}", cmd.program.display()))?;
    let stdin = child
        .stdin
        .take()
        .ok_or_else(|| "child stdin not piped".to_string())?;
    let stdout = child
        .stdout
        .take()
        .ok_or_else(|| "child stdout not piped".to_string())?;
    let stderr = child
        .stderr
        .take()
        .map(|s| Box::new(s) as Box<dyn AsyncRead + Unpin + Send>);

    let (note_tx, notifications) = mpsc::unbounded_channel();
    let (req_tx, requests) = mpsc::unbounded_channel();
    let hooks = ConnectionHooks {
        notifications: Some(note_tx),
        requests: Some(req_tx),
        ..Default::default()
    };
    let conn = Connection::new(stdin, stdout, stderr, hooks);
    Ok(SpawnedAdapter {
        child,
        conn,
        notifications,
        requests,
    })
}

/// The `initialize` params every ephemeral adapter run sends: no filesystem
/// capabilities, so the adapter never expects the client to serve `fs/*`.
pub(crate) fn initialize_params() -> Value {
    json!({
        "protocolVersion": 1,
        "clientInfo": { "name": "Intent", "version": env!("CARGO_PKG_VERSION") },
        "clientCapabilities": { "fs": { "readTextFile": false, "writeTextFile": false } },
    })
}

/// Bounded window to observe a crashed adapter's exit status and let the
/// stderr reader drain its final lines before attribution. A crashing child's
/// stdout close (the transport error that reports the crash) races both the
/// exit-status reap and the stderr drain, so a bare `try_wait` snapshot can
/// misattribute a genuine crash as a plain transport failure.
///
/// Latency cost: on a timeout with a hung (still-running) child,
/// `child.wait()` burns this full window before falling back to `try_wait`,
/// so a timed-out run takes ~500ms beyond its budget in production. Bounded
/// and error-path-only, so accepted.
const EXIT_OBSERVE_GRACE: Duration = Duration::from_millis(500);

/// Observe whether the adapter already exited, waiting briefly for both the
/// exit status and (on an unsuccessful exit) the child's final stderr lines
/// to land in the connection's ring buffer.
pub(crate) async fn observe_exit_status(
    child: &mut tokio::process::Child,
    conn: &Connection,
) -> Option<std::process::ExitStatus> {
    let status = match tokio::time::timeout(EXIT_OBSERVE_GRACE, child.wait()).await {
        Ok(Ok(status)) => Some(status),
        _ => child.try_wait().ok().flatten(),
    };
    if status.is_some_and(|s| !s.success()) {
        // The exited child's final stderr may still be in flight to the
        // reader task; wait briefly for the first line so the attribution
        // can carry it.
        let deadline = tokio::time::Instant::now() + EXIT_OBSERVE_GRACE;
        while conn.recent_stderr().is_empty() && tokio::time::Instant::now() < deadline {
            tokio::time::sleep(Duration::from_millis(10)).await;
        }
    }
    status
}

/// How many trailing stderr lines to include in an exit attribution. npm's
/// final line is typically just "A complete log of this run can be found
/// in: …" with the actual cause a few lines earlier, so a single line is
/// not enough.
const STDERR_TAIL_LINES: usize = 3;
/// Character bound on the joined stderr tail (kept from the end).
const STDERR_TAIL_MAX_CHARS: usize = 300;

/// The "adapter died" detail for an observed exit: `Some("<status>; stderr:
/// …")` when the child exited unsuccessfully, `None` when it is still running
/// or exited cleanly (a clean exit is a genuine empty/short result, not a
/// crash).
pub(crate) fn exited_detail(
    status: Option<std::process::ExitStatus>,
    stderr: &[String],
) -> Option<String> {
    let status = status?;
    if status.success() {
        return None;
    }
    let tail = match stderr_tail(stderr) {
        Some(t) => format!("; stderr: {t}"),
        None => String::new(),
    };
    Some(format!("{status}{tail}"))
}

/// Join the last [`STDERR_TAIL_LINES`] non-empty stderr lines, bounded to
/// [`STDERR_TAIL_MAX_CHARS`] characters kept from the end.
fn stderr_tail(stderr: &[String]) -> Option<String> {
    let non_empty: Vec<&str> = stderr
        .iter()
        .map(|l| l.trim())
        .filter(|l| !l.is_empty())
        .collect();
    let start = non_empty.len().saturating_sub(STDERR_TAIL_LINES);
    let joined = non_empty[start..].join(" | ");
    if joined.is_empty() {
        return None;
    }
    let count = joined.chars().count();
    Some(
        joined
            .chars()
            .skip(count.saturating_sub(STDERR_TAIL_MAX_CHARS))
            .collect(),
    )
}

/// Grace window between SIGTERM and SIGKILL when reaping an adapter child
/// (mirrors `host_exec::TERM_GRACE` / `mcp_servers::reap`).
const TERM_GRACE: Duration = Duration::from_millis(500);

/// Kill the adapter child and reap it. Signals the whole process group (the
/// child is its own group leader via `process_group(0)`) so grandchildren
/// (e.g. `npx` → `node`) die too, following the crate's SIGTERM → grace →
/// SIGKILL pattern, then waits briefly so the child does not linger as a
/// zombie. `kill_on_drop(true)` back-stops any wait timeout.
///
/// Group signalling alone is not enough: adapters can start MCP servers that
/// move into their OWN process groups, so descendants are snapshotted before
/// the kill and any survivors swept afterwards regardless of process group —
/// see `intent_acp::descendant_sweep` for the shared backstop and its
/// snapshot-before-kill rationale.
pub(crate) async fn reap_child(child: &mut tokio::process::Child) {
    #[cfg(unix)]
    let descendants = match child.id() {
        Some(pid) => descendant_pids(pid).await,
        None => Vec::new(),
    };
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
    #[cfg(unix)]
    sweep_escaped_descendants(&descendants).await;
}

#[cfg(all(test, unix))]
mod reap_tests {
    use super::*;

    // Table-walk unit tests for the sweep live with the shared helper in
    // `intent_acp::descendant_sweep`; this module keeps the adapter-level
    // integration regression.

    /// Regression for the live escape: an MCP-server-style grandchild that
    /// moves into its OWN process group survives `killpg` on the adapter
    /// group (observed: codex-acp's auggie ran with pgid == its own pid); the
    /// descendant sweep must still reap it. Mirrors intent-acp's
    /// `kill_reaps_grandchildren_via_process_group`, except the grandchild
    /// escapes the group via `set -m` job control (background jobs become
    /// their own group leaders).
    #[tokio::test]
    async fn reap_child_sweeps_grandchild_in_foreign_process_group() {
        use nix::unistd::{getpgid, Pid};

        let pidfile =
            std::env::temp_dir().join(format!("intent-probe-sweep-{}.pid", uuid::Uuid::new_v4()));
        let mut command = tokio::process::Command::new("bash");
        command
            .arg("-c")
            .arg(r#"set -m; sleep 300 & echo $! > "$INTENT_TEST_PIDFILE"; wait"#)
            .env("INTENT_TEST_PIDFILE", &pidfile)
            .stdin(Stdio::null())
            .stdout(Stdio::null())
            .stderr(Stdio::null())
            .kill_on_drop(true);
        command.process_group(0);
        let mut child = command.spawn().expect("spawn bash child");

        let mut grandchild_pid = None;
        for _ in 0..250 {
            if let Ok(s) = tokio::fs::read_to_string(&pidfile).await {
                if let Ok(pid) = s.trim().parse::<i32>() {
                    grandchild_pid = Some(pid);
                    break;
                }
            }
            tokio::time::sleep(Duration::from_millis(20)).await;
        }
        let grandchild_pid = grandchild_pid.expect("grandchild pid written");

        // Prove the grandchild actually escaped the adapter's process group —
        // otherwise killpg would reach it and the test would be vacuous.
        let child_pgid = getpgid(Some(Pid::from_raw(child.id().expect("child pid") as i32)))
            .expect("child pgid");
        let grandchild_pgid =
            getpgid(Some(Pid::from_raw(grandchild_pid))).expect("grandchild pgid");
        assert_ne!(
            grandchild_pgid, child_pgid,
            "grandchild must be in a foreign process group for this regression test"
        );

        // Distinct failure signal for the snapshot path: if `ps` stalls past
        // its budget on a loaded runner the snapshot comes back empty and the
        // sweep silently no-ops — fail here, not at the terminal panic below.
        let snapshot = descendant_pids(child.id().expect("child pid")).await;
        assert!(
            snapshot.contains(&grandchild_pid),
            "descendant snapshot {snapshot:?} must include grandchild {grandchild_pid} \
             (empty/partial snapshot ⇒ `ps` walk failed, not the sweep)"
        );

        reap_child(&mut child).await;
        tokio::fs::remove_file(&pidfile).await.ok();

        // `kill(pid, 0)` returns ESRCH once the pid is gone (the grandchild
        // is not our direct child, so init reaps it after the sweep's kill).
        for _ in 0..100 {
            if nix::sys::signal::kill(Pid::from_raw(grandchild_pid), None).is_err() {
                return;
            }
            tokio::time::sleep(Duration::from_millis(50)).await;
        }
        panic!("grandchild pid {grandchild_pid} still alive after reap_child sweep");
    }
}
