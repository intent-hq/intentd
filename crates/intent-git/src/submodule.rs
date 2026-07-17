//! Submodule operations.
//!
//! After a pull that updates a submodule gitlink, the submodule worktree must
//! be synced to match the new commit. This module provides the bounded shell-out
//! pattern for `git submodule update --init --recursive`, following the same
//! approach as `fetch.rs` (timeout + kill, GIT_TERMINAL_PROMPT=0, piped stderr).

use std::path::Path;
use std::process::{Command, Stdio};
use std::time::{Duration, Instant};

use intent_core::{Error, Result};

/// Wall-clock bound for `git submodule update`. Chosen below the service-layer
/// `GIT_PULL_TIMEOUT` (120s) so the submodule-update child is killed cleanly
/// by this helper before the outer timeout wrapper fires.
const SUBMODULE_UPDATE_TIMEOUT: Duration = Duration::from_secs(100);

/// Poll interval used while waiting for the submodule-update child to exit.
const SUBMODULE_UPDATE_POLL: Duration = Duration::from_millis(50);

/// Update submodules to match the current gitlinks (the post-pull step when
/// a repo has configured submodules). Shells out to `git submodule update
/// --init --recursive` with the same bounded pattern as `fetch.rs`:
/// `GIT_TERMINAL_PROMPT=0` for fail-fast, stdin null, stdout discarded, stderr
/// piped, wall-clock deadline + `Child::kill`. Errors when git is not on PATH,
/// a submodule is unreachable, or the update exceeds the timeout.
pub fn update_submodules(worktree_path: &Path) -> Result<()> {
    update_submodules_with_timeout(worktree_path, SUBMODULE_UPDATE_TIMEOUT)
}

/// Timeout-parameterised body of [`update_submodules`], factored out so tests
/// can drive the deadline-kill path without waiting 100s.
pub(crate) fn update_submodules_with_timeout(
    worktree_path: &Path,
    timeout: Duration,
) -> Result<()> {
    let mut child = Command::new("git")
        .arg("-C")
        .arg(worktree_path)
        .arg("submodule")
        .arg("update")
        .arg("--init")
        .arg("--recursive")
        .env("GIT_TERMINAL_PROMPT", "0")
        .stdin(Stdio::null())
        // Discard stdout: git-submodule progress output is only for TTY users.
        .stdout(Stdio::null())
        .stderr(Stdio::piped())
        .spawn()
        .map_err(|e| Error::Internal(format!("failed to spawn git submodule update: {e}")))?;

    let deadline = Instant::now() + timeout;
    loop {
        match child.try_wait() {
            Ok(Some(status)) => {
                if status.success() {
                    return Ok(());
                }
                let stderr = read_stderr(&mut child);
                return Err(Error::Internal(format!(
                    "git submodule update failed: {}",
                    stderr.trim()
                )));
            }
            Ok(None) => {
                if Instant::now() >= deadline {
                    let _ = child.kill();
                    let _ = child.wait();
                    return Err(Error::Internal(format!(
                        "git submodule update timed out after {}s",
                        timeout.as_secs()
                    )));
                }
                std::thread::sleep(SUBMODULE_UPDATE_POLL);
            }
            Err(e) => {
                let _ = child.kill();
                let _ = child.wait();
                return Err(Error::Internal(format!(
                    "git submodule update wait failed: {e}"
                )));
            }
        }
    }
}

/// Check whether a repository has configured submodules by testing for a
/// `.gitmodules` file in the worktree root.
pub fn has_submodules(worktree_path: &Path) -> bool {
    worktree_path.join(".gitmodules").exists()
}

fn read_stderr(child: &mut std::process::Child) -> String {
    use std::io::Read;
    let mut buf = String::new();
    if let Some(mut stderr) = child.stderr.take() {
        let _ = stderr.read_to_string(&mut buf);
    }
    buf
}

#[cfg(test)]
mod tests {
    use super::*;
    use crate::testutil::{commit_file, init_repo};

    /// Update a repo with no submodules must succeed as a no-op.
    #[test]
    fn update_submodules_with_no_submodules_is_noop() {
        let dir = init_repo("submodule-update-noop");
        commit_file(dir.path(), "a.txt", "one\n");
        assert!(!has_submodules(dir.path()));
        update_submodules(dir.path()).unwrap();
    }

    /// Repos without .gitmodules report `has_submodules() == false`.
    #[test]
    fn has_submodules_false_when_no_gitmodules() {
        let dir = init_repo("no-submodules");
        commit_file(dir.path(), "a.txt", "x\n");
        assert!(!has_submodules(dir.path()));
    }

    /// A repo with a .gitmodules file reports `has_submodules() == true`.
    #[test]
    fn has_submodules_true_when_gitmodules_exists() {
        let dir = init_repo("with-gitmodules");
        commit_file(dir.path(), ".gitmodules", "[submodule \"test\"]\n");
        assert!(has_submodules(dir.path()));
    }
}
