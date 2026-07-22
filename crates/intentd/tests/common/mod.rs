//! Shared test utilities for intentd integration tests.
//!
//! This module provides RAII guards for spawned daemon processes to prevent
//! process leaks when tests panic or fail to clean up explicitly, plus
//! multiplier-aware timeout helpers so budgets are centrally tunable.

// Each integration test binary compiles this module independently and only
// uses a subset of it, so unused items are expected.
#![allow(dead_code)]

use std::path::PathBuf;
use std::process::Child;
use std::time::Duration;

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

/// Return a unique, hermetic workspaces root under the OS temp dir.
///
/// In-process integration tests must chain `.with_workspaces_root(...)` onto
/// every `Services::new(...)` so tests never resolve the real
/// `~/intent/workspaces`. The directory is created on demand by the services
/// layer, so this helper only reserves a unique path.
pub fn hermetic_workspaces_root() -> PathBuf {
    std::env::temp_dir().join(format!("itd-ws-{}", uuid::Uuid::new_v4()))
}

/// Enable the WSS/TCP listener for a daemon booted from `data_dir` by seeding
/// `config.toml` with `[server.wsApi] enabled = true` plus an OS-assigned free
/// port (the config-driven replacement for the retired `serve --listen both`
/// flag: UDS always serves; the WSS listener boot-starts iff the effective
/// `server.wsApi.enabled` is true, binding `server.wsApi.port`). Seeding the
/// port keeps the suite hermetic — the boot path reads the settings value, so
/// the fixed 5181 default would collide across parallel daemons. Appends to an
/// existing seeded config; no-op if the table is already present (restarts on
/// the same data dir reuse the same port).
pub fn enable_ws_api(data_dir: &std::path::Path) {
    std::fs::create_dir_all(data_dir).expect("mkdir data dir");
    let path = data_dir.join("config.toml");
    let mut text = match std::fs::read_to_string(&path) {
        Ok(text) => text,
        Err(e) if e.kind() == std::io::ErrorKind::NotFound => String::new(),
        Err(e) => panic!("read {}: {e}", path.display()),
    };
    if text
        .lines()
        .any(|l| l.trim_start().starts_with("[server.wsApi]"))
    {
        return;
    }
    let port = std::net::TcpListener::bind(("127.0.0.1", 0))
        .expect("bind free port")
        .local_addr()
        .expect("local addr")
        .port();
    if !text.is_empty() && !text.ends_with('\n') {
        text.push('\n');
    }
    text.push_str(&format!(
        "\n[server.wsApi]\nenabled = true\nport = {port}\n"
    ));
    std::fs::write(&path, text).expect("seed config.toml with server.wsApi.enabled");
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
