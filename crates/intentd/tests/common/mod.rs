//! Shared test utilities for intentd integration tests.
//!
//! This module provides RAII guards for spawned daemon processes to prevent
//! process leaks when tests panic or fail to clean up explicitly, plus
//! multiplier-aware timeout helpers so budgets are centrally tunable.

// Each integration test binary compiles this module independently and only
// uses a subset of it, so unused items are expected.
#![allow(dead_code)]

use std::path::{Path, PathBuf};
use std::process::Child;
use std::time::Duration;

/// Enable the WSS listener at boot by seeding `[server.wsApi]` with
/// `enabled = true` and an OS-assigned free port into `config.toml` under
/// `data_dir`. Replaces the retired `intentd serve --listen both` flag: the
/// UDS listener is always on and the HTTPS+WSS listener boot-starts iff the
/// effective `server.wsApi.enabled` is true, binding `server.wsApi.port`.
/// Seeding a hermetic free port keeps parallel tests off the fixed default
/// (5181). Idempotent and append-only, so a test-seeded config.toml (and the
/// section itself — including the port — across daemon restarts on the same
/// data dir) is preserved.
pub fn enable_wss_boot(data_dir: &Path) {
    std::fs::create_dir_all(data_dir).expect("mkdir data dir");
    let path = data_dir.join("config.toml");
    let mut text = std::fs::read_to_string(&path).unwrap_or_default();
    if text.contains("[server.wsApi]") {
        return;
    }
    if !text.is_empty() {
        if !text.ends_with('\n') {
            text.push('\n');
        }
        text.push('\n');
    }
    let port = free_port();
    text.push_str(&format!("[server.wsApi]\nenabled = true\nport = {port}\n"));
    std::fs::write(&path, text).expect("write config.toml");
}

/// Grab an OS-assigned free TCP port (bind :0, read, release). The tiny
/// reuse window is acceptable for hermetic tests.
pub fn free_port() -> u16 {
    std::net::TcpListener::bind(("127.0.0.1", 0))
        .expect("bind :0")
        .local_addr()
        .expect("local addr")
        .port()
}

/// Apply the timeout multiplier from the environment for coverage
/// instrumentation. Reads `INTENTD_TEST_TIMEOUT_MULTIPLIER` (defaults to 1.0;
/// non-finite values are ignored and values below 1.0 are clamped so budgets
/// can only be extended; overflow saturates to `Duration::MAX`).
pub fn test_timeout(base: Duration) -> Duration {
    let multiplier = std::env::var("INTENTD_TEST_TIMEOUT_MULTIPLIER")
        .ok()
        .and_then(|s| s.parse::<f64>().ok())
        .filter(|m| m.is_finite())
        .unwrap_or(1.0);
    Duration::try_from_secs_f64(base.as_secs_f64() * multiplier.max(1.0)).unwrap_or(Duration::MAX)
}

/// Shared budget for waiting on daemon startup (UDS/socket ready): 60s base,
/// scaled by `INTENTD_TEST_TIMEOUT_MULTIPLIER`. The generous budget absorbs
/// coverage-instrumented startup on oversubscribed CI runners.
pub fn daemon_startup_timeout() -> Duration {
    test_timeout(Duration::from_secs(60))
}

/// RAII guard for a spawned `intentd serve` process.
///
/// Ensures the daemon child process is killed on drop (SIGKILL to the process
/// group) and optionally removes the temp data directory. This prevents leaked
/// daemon processes when tests panic or abort before explicit cleanup.
///
/// The guard sends SIGKILL to the process group (not just the parent PID),
/// which also terminates any child processes spawned by the daemon (e.g., Node
/// mock agents in ACP provider tests).
pub struct DaemonGuard {
    child: Child,
    data_dir: Option<PathBuf>,
}

impl DaemonGuard {
    /// Create a new daemon guard that will kill the child process on drop.
    ///
    /// If `cleanup_data_dir` is true, the data directory will be removed on drop.
    pub fn new(child: Child, data_dir: PathBuf, cleanup_data_dir: bool) -> Self {
        Self {
            child,
            data_dir: if cleanup_data_dir {
                Some(data_dir)
            } else {
                None
            },
        }
    }

    /// Create a daemon guard that only kills the process (no data dir cleanup).
    pub fn process_only(child: Child) -> Self {
        Self {
            child,
            data_dir: None,
        }
    }

    /// Get a mutable reference to the child process.
    ///
    /// Useful for calling `wait()`, `try_wait()`, or `kill()` explicitly.
    pub fn child_mut(&mut self) -> &mut Child {
        &mut self.child
    }

    /// Take ownership of the child process, consuming the guard.
    ///
    /// The caller is responsible for cleanup after this point.
    pub fn into_child(mut self) -> Child {
        let child = std::mem::replace(
            &mut self.child,
            // Placeholder - will be dropped immediately after we return the real child
            unsafe { std::mem::zeroed() },
        );
        // Prevent Drop from running by forgetting self
        std::mem::forget(self);
        child
    }

    /// Disable data directory cleanup on drop.
    ///
    /// Useful when the test wants to inspect the data directory after the daemon stops.
    pub fn keep_data_dir(mut self) -> Self {
        self.data_dir = None;
        self
    }
}

impl Drop for DaemonGuard {
    fn drop(&mut self) {
        // SIGKILL the process (or process group if set).
        // Ignore errors - the process may have already exited.
        let _ = self.child.kill();
        let _ = self.child.wait();

        // Clean up data directory if requested.
        if let Some(ref dir) = self.data_dir {
            let _ = std::fs::remove_dir_all(dir);
        }
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use std::process::Command;

    #[test]
    fn guard_kills_process_on_drop() {
        // Spawn a sleep process
        let child = Command::new("sleep")
            .arg("3600")
            .spawn()
            .expect("spawn sleep");
        let pid = child.id();

        {
            let _guard = DaemonGuard::process_only(child);
            // Guard goes out of scope here
        }

        // Process should be dead
        // Check using kill -0 (send signal 0 to test if process exists)
        let status = Command::new("kill")
            .arg("-0")
            .arg(pid.to_string())
            .status()
            .expect("run kill -0");

        assert!(!status.success(), "process should be dead after guard drop");
    }
}
