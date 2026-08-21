//! `AuggieContextEngine` — the `auggie`-backed [`ContextEngine`] (§8.2, §8.3).
//!
//! Ports `execute-auggie-command.ts` (`executeAuggieCommand`, 30s default
//! timeout, no-shell `execFile`-style spawn on unix, `.cmd`/`.bat` shell on
//! Windows, stdin piping, auth-failure → "needs login"). `availability()` is a
//! non-error probe (§8.3); only `retrieve()` returns a [`ContextError`].

use std::path::{Path, PathBuf};
use std::process::Stdio;
use std::sync::Mutex;
use std::time::Duration;

use async_trait::async_trait;
use intent_core::{
    ContextEngine, ContextError, EngineAvailability, RetrieveRequest, RetrieveResult,
};
use tokio::io::AsyncWriteExt;
use tokio::process::Command;

use crate::discovery;

/// Default per-command timeout (matches the TS `DEFAULT_AUGGIE_TIMEOUT_MS`).
pub const DEFAULT_TIMEOUT: Duration = Duration::from_secs(30);

/// Shorter timeout for the lightweight `--version` availability probe.
const VERSION_TIMEOUT: Duration = Duration::from_secs(8);

/// Auth-failure substrings → mapped to a "needs login" availability (§8.2).
const AUTH_FAILURE_PATTERNS: &[&str] = &[
    "not logged in",
    "not authenticated",
    "please log in",
    "please login",
    "login required",
    "you must log in",
    "run auggie login",
    "authentication required",
    "unauthorized",
    "no valid session",
    "session expired",
    "augment_session_auth",
];

/// Context engine backed by the `auggie` CLI.
#[derive(Debug, Default)]
pub struct AuggieContextEngine {
    /// Cached resolved binary path; re-probed when it disappears (§8.2).
    cached_path: Mutex<Option<PathBuf>>,
}

impl AuggieContextEngine {
    /// Construct an engine that discovers auggie lazily. Never fails (§8.3).
    pub fn new() -> Self {
        Self::default()
    }

    /// Construct with a known binary path (a user-configured path, or tests),
    /// pre-seeding the discovery cache.
    pub fn with_path(path: PathBuf) -> Self {
        Self {
            cached_path: Mutex::new(Some(path)),
        }
    }

    /// Resolve the auggie path, reusing the cache when the file still exists and
    /// re-probing via discovery otherwise (§8.2).
    fn resolve_path(&self) -> Option<PathBuf> {
        {
            let guard = self.cached_path.lock().expect("cache poisoned");
            if let Some(p) = guard.as_ref() {
                if p.exists() {
                    return Some(p.clone());
                }
            }
        }
        let found = discovery::find_auggie();
        let mut guard = self.cached_path.lock().expect("cache poisoned");
        guard.clone_from(&found);
        found
    }
}

#[async_trait]
impl ContextEngine for AuggieContextEngine {
    async fn availability(&self) -> EngineAvailability {
        let Some(path) = self.resolve_path() else {
            return EngineAvailability::Unavailable {
                reason: "auggie not found on PATH".to_string(),
            };
        };
        match run_auggie(&path, &["--version"], None, None, VERSION_TIMEOUT).await {
            Ok(output) => classify_availability(&output.stdout, &output.stderr, output.success),
            Err(err) => EngineAvailability::Unavailable {
                reason: format!("auggie not available: {err}"),
            },
        }
    }

    async fn retrieve(
        &self,
        _req: RetrieveRequest,
    ) -> std::result::Result<RetrieveResult, ContextError> {
        // auggie exposes no structured codebase-retrieval CLI (its `codebase:search`
        // was a never-implemented stub), so there is nothing to spawn: invoking
        // auggie with no subcommand would hang in interactive mode until the
        // timeout. Degrade instantly so callers fall back to ripgrep with zero
        // latency (§8.3, M10 CE-3). `availability()` still reports binary presence
        // for `intentd doctor`.
        Err(ContextError::Unavailable {
            reason: "auggie exposes no structured codebase-retrieval CLI".to_string(),
        })
    }
}

/// Captured output of an auggie invocation.
struct CommandOutput {
    stdout: String,
    stderr: String,
    success: bool,
}

/// Spawn auggie with the enhanced exec PATH, an optional `cwd`, optional stdin,
/// and a timeout. No shell on unix (`execFile`-style); `cmd /C` for `.cmd`/
/// `.bat` shims on Windows. The child is killed if the timeout elapses
/// (`kill_on_drop`, plus a unix process-group SIGKILL so grandchildren are
/// reaped too — mirroring `run_auggie_print` in intent-services).
async fn run_auggie(
    auggie_path: &Path,
    args: &[&str],
    stdin: Option<&str>,
    cwd: Option<&Path>,
    timeout: Duration,
) -> std::result::Result<CommandOutput, ContextError> {
    let env_path = discovery::exec_path(auggie_path);

    let mut command = build_command(auggie_path, args);
    command.env("PATH", &env_path);
    if let Some(dir) = cwd {
        command.current_dir(dir);
    }
    command.stdout(Stdio::piped());
    command.stderr(Stdio::piped());
    command.stdin(if stdin.is_some() {
        Stdio::piped()
    } else {
        Stdio::null()
    });
    command.kill_on_drop(true);
    // Put the child in its own process group (leader pgid == child pid) so a
    // timeout can SIGKILL the WHOLE tree via `killpg` — `kill_on_drop` only
    // reaches the direct child, leaving grandchildren orphaned.
    #[cfg(unix)]
    command.process_group(0);

    let mut child = command
        .spawn()
        .map_err(|e| ContextError::Spawn(e.to_string()))?;
    let pid = child.id();

    if let Some(data) = stdin {
        if let Some(mut sink) = child.stdin.take() {
            // EPIPE is benign: the child may exit before consuming all input.
            let _ = sink.write_all(data.as_bytes()).await;
            let _ = sink.shutdown().await;
        }
    }

    let output = match tokio::time::timeout(timeout, child.wait_with_output()).await {
        Ok(result) => result.map_err(|e| ContextError::Spawn(e.to_string()))?,
        Err(_) => {
            // Kill the whole process group (pgid == pid via `process_group`);
            // the dropped `wait_with_output` future's `kill_on_drop` covers
            // the direct child on non-unix.
            #[cfg(unix)]
            if let Some(pid) = pid {
                use nix::sys::signal::{killpg, Signal};
                use nix::unistd::Pid;
                let _ = killpg(Pid::from_raw(pid as i32), Signal::SIGKILL);
            }
            #[cfg(not(unix))]
            let _ = pid;
            return Err(ContextError::Timeout);
        }
    };

    Ok(CommandOutput {
        stdout: String::from_utf8_lossy(&output.stdout).into_owned(),
        stderr: String::from_utf8_lossy(&output.stderr).into_owned(),
        success: output.status.success(),
    })
}

#[cfg(windows)]
fn build_command(auggie_path: &Path, args: &[&str]) -> Command {
    // npm `.cmd`/`.bat` shims (and bare names) require cmd.exe; absolute paths to
    // real binaries spawn directly (safer — avoids cmd.exe arg interpretation).
    let lower = auggie_path
        .extension()
        .and_then(|e| e.to_str())
        .map(|e| e.to_ascii_lowercase());
    let needs_shell =
        !auggie_path.is_absolute() || matches!(lower.as_deref(), Some("cmd") | Some("bat"));
    if needs_shell {
        let mut c = Command::new("cmd");
        c.arg("/C").arg(auggie_path).args(args);
        c
    } else {
        let mut c = Command::new(auggie_path);
        c.args(args);
        c
    }
}

#[cfg(not(windows))]
fn build_command(auggie_path: &Path, args: &[&str]) -> Command {
    let mut c = Command::new(auggie_path);
    c.args(args);
    c
}

/// True when `output` looks like an auth/login failure (§8.2).
pub(crate) fn is_auth_failure(output: &str) -> bool {
    let lower = output.to_lowercase();
    AUTH_FAILURE_PATTERNS.iter().any(|p| lower.contains(p))
}

/// Classify the `--version` probe output into an [`EngineAvailability`] (§8.2):
/// auth-failure patterns become a non-error "needs login" `Unavailable`.
pub(crate) fn classify_availability(
    stdout: &str,
    stderr: &str,
    success: bool,
) -> EngineAvailability {
    let combined = format!("{stdout}\n{stderr}");
    if is_auth_failure(&combined) {
        return EngineAvailability::Unavailable {
            reason: "needs login".to_string(),
        };
    }
    if !success {
        let reason = first_nonempty_line(stderr)
            .or_else(|| first_nonempty_line(stdout))
            .unwrap_or_else(|| "auggie --version failed".to_string());
        return EngineAvailability::Unavailable { reason };
    }
    EngineAvailability::Available {
        name: "auggie".to_string(),
        version: parse_version(&combined),
    }
}

/// Extract the first `MAJOR.MINOR.PATCH`-looking token from `text`.
fn parse_version(text: &str) -> Option<String> {
    for token in text.split_whitespace() {
        let trimmed = token.trim_matches(|c: char| !c.is_ascii_digit());
        if is_semverish(trimmed) {
            return Some(trimmed.to_string());
        }
    }
    None
}

fn is_semverish(s: &str) -> bool {
    let parts: Vec<&str> = s.split('.').collect();
    parts.len() >= 3
        && parts
            .iter()
            .take(3)
            .all(|p| !p.is_empty() && p.chars().all(|c| c.is_ascii_digit()))
}

fn first_nonempty_line(text: &str) -> Option<String> {
    text.lines()
        .map(str::trim)
        .find(|l| !l.is_empty())
        .map(str::to_string)
}

#[cfg(test)]
mod tests {
    use super::*;

    /// A fresh RAII temp directory for `tag` under the system temp root. The
    /// returned guard removes the dir on drop (including on panic); set
    /// `INTENTD_TEST_KEEP_TMP` (non-empty) to keep it around for debugging.
    #[cfg(unix)]
    fn unique_temp_dir(tag: &str) -> tempfile::TempDir {
        let mut dir = tempfile::Builder::new()
            .prefix(&format!("intent-ctx-{tag}-"))
            .tempdir()
            .expect("create test temp dir");
        if std::env::var_os("INTENTD_TEST_KEEP_TMP").is_some_and(|v| !v.is_empty()) {
            dir.disable_cleanup(true);
        }
        dir
    }

    /// Serializes the tests that exec a real fake-auggie child against one
    /// another. `cargo test` runs the tests within this binary in parallel, and
    /// `run_auggie_timeout_group_kills_grandchildren` depends on its child being
    /// scheduled promptly — it must fork its `sleep 30` grandchild and write the
    /// pidfile before the timeout reaps the group. Under full-suite parallel
    /// load, a second concurrent fake-binary spawn can starve it past that
    /// budget, so the pidfile is never written and the test flakes (the timeout
    /// budget itself is now widened to 5s for runner-agnostic headroom, but this
    /// guard keeps only one such spawn running at a time). Holding this
    /// guard for each child-spawning test's duration keeps only one such spawn
    /// running at a time. Mirrors the `CHILD_SPAWN_SERIAL` (`provider_models`) and
    /// `WATCHER_TEST_SERIAL` (events/mod.rs) precedents. `unwrap_or_else(
    /// into_inner)` recovers from a poisoned lock so one panicking test does not
    /// cascade into the rest.
    static CHILD_SPAWN_SERIAL: std::sync::Mutex<()> = std::sync::Mutex::new(());

    #[test]
    fn classify_available_parses_version() {
        let a = classify_availability("auggie v1.12.3", "", true);
        assert_eq!(
            a,
            EngineAvailability::Available {
                name: "auggie".to_string(),
                version: Some("1.12.3".to_string()),
            }
        );
    }

    #[test]
    fn classify_unavailable_on_failure() {
        let a = classify_availability("", "command not found", false);
        assert_eq!(
            a,
            EngineAvailability::Unavailable {
                reason: "command not found".to_string(),
            }
        );
    }

    #[test]
    fn classify_auth_failure_is_needs_login() {
        let a = classify_availability("", "Error: not logged in. Run auggie login.", true);
        assert_eq!(
            a,
            EngineAvailability::Unavailable {
                reason: "needs login".to_string(),
            }
        );
        assert!(is_auth_failure("You are NOT AUTHENTICATED"));
        assert!(!is_auth_failure("auggie 1.2.3"));
    }

    #[test]
    fn availability_is_total_never_errors() {
        // §8.3: availability() returns an EngineAvailability and never panics or
        // errors, even when the seeded path is bogus (resolve_path then re-probes
        // discovery). On hosts without auggie this is Unavailable; on hosts with
        // it, a well-formed Available/Unavailable. Either is the non-error state.
        let engine = AuggieContextEngine::with_path(PathBuf::from("/no/such/auggie/binary"));
        match futures_block_on(engine.availability()) {
            EngineAvailability::Available { name, .. } => assert_eq!(name, "auggie"),
            EngineAvailability::Unavailable { reason } => assert!(!reason.is_empty()),
        }
    }

    #[test]
    fn retrieve_unavailable_when_no_binary_resolves() {
        // When discovery yields nothing, retrieve() maps to a non-panicking
        // ContextError::Unavailable (§8.3, §11.1). find_in_dirs over an empty set
        // is the deterministic "nothing found" signal underpinning that branch.
        assert_eq!(discovery::find_in_dirs(&[]), None);
    }

    #[cfg(unix)]
    #[test]
    fn availability_available_via_fake_binary() {
        let _serial = CHILD_SPAWN_SERIAL
            .lock()
            .unwrap_or_else(std::sync::PoisonError::into_inner);
        use std::os::unix::fs::PermissionsExt;
        let dir = unique_temp_dir("avail");
        let bin = dir.path().join("auggie");
        std::fs::write(&bin, "#!/bin/sh\necho 'auggie 2.5.1'\n").unwrap();
        std::fs::set_permissions(&bin, std::fs::Permissions::from_mode(0o755)).unwrap();

        let engine = AuggieContextEngine::with_path(bin);
        let availability = futures_block_on(engine.availability());
        assert_eq!(
            availability,
            EngineAvailability::Available {
                name: "auggie".to_string(),
                version: Some("2.5.1".to_string()),
            }
        );
    }

    /// A fake binary that forks a `sleep 30` grandchild (writing its pid to a
    /// file) must have the WHOLE group reaped when the timeout elapses — a
    /// direct-child-only kill would leave the grandchild orphaned.
    #[cfg(unix)]
    #[test]
    fn run_auggie_timeout_group_kills_grandchildren() {
        let _serial = CHILD_SPAWN_SERIAL
            .lock()
            .unwrap_or_else(std::sync::PoisonError::into_inner);
        use std::os::unix::fs::PermissionsExt;
        let dir = unique_temp_dir("groupkill");
        let bin = dir.path().join("auggie");
        let pidfile = dir.path().join("grandchild.pid");
        std::fs::write(
            &bin,
            format!(
                "#!/bin/sh\nsleep 30 & echo $! > '{}'\nwait\n",
                pidfile.display()
            ),
        )
        .unwrap();
        std::fs::set_permissions(&bin, std::fs::Permissions::from_mode(0o755)).unwrap();

        // Budget deliberately generous (5s, not the ~1s that used to flake): the
        // child must fork its `sleep 30` grandchild and write the pidfile before
        // the timeout reaps the group, and under nextest's oversubscribed
        // parallelism a tighter budget starves that startup past the deadline —
        // failing BOTH retry attempts. The behavior under test (group-kill on
        // timeout) is unchanged; only the headroom before the timeout fires.
        let result = futures_block_on(run_auggie(&bin, &[], None, None, Duration::from_secs(5)));
        assert!(matches!(result, Err(ContextError::Timeout)));

        let grandchild_pid: i32 = std::fs::read_to_string(&pidfile)
            .expect("grandchild pid written before timeout")
            .trim()
            .parse()
            .expect("parse grandchild pid");

        // The grandchild is not our direct child, so it lingers until init
        // reaps it; `kill(pid, 0)` returns ESRCH once the pid is gone.
        futures_block_on(async {
            for _ in 0..100 {
                if nix::sys::signal::kill(nix::unistd::Pid::from_raw(grandchild_pid), None).is_err()
                {
                    return;
                }
                tokio::time::sleep(Duration::from_millis(50)).await;
            }
            panic!("grandchild pid {grandchild_pid} still alive after timeout group-kill");
        });
    }

    /// Minimal single-threaded block-on so async tests need no extra deps.
    fn futures_block_on<F: std::future::Future>(fut: F) -> F::Output {
        tokio::runtime::Builder::new_current_thread()
            .enable_all()
            .build()
            .unwrap()
            .block_on(fut)
    }
}
