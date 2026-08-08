//! Submodule operations.
//!
//! After a pull that updates a submodule gitlink, the submodule worktree must
//! be synced to match the new commit. This module provides the bounded shell-out
//! pattern for `git submodule update --init --recursive`, following the same
//! approach as `fetch.rs` (timeout + kill, GIT_TERMINAL_PROMPT=0, piped stderr).

use std::path::Path;
use std::process::{Command, Stdio};
use std::time::{Duration, Instant};

use git2::{FileMode, Repository, TreeWalkMode, TreeWalkResult};
use intent_core::{Error, Result};

use crate::map_git_err;

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

/// The set of registered submodule (gitlink) paths in `repo`, worktree-relative
/// with forward-slash separators. Guards a `commit_paths_with_trailers`/`stage`
/// caller from ever flattening a submodule into the superproject (monorepo#1714):
/// a path strictly inside one of these is refused before it reaches
/// `index.add_path`, while the gitlink path itself (a pin bump) stays allowed.
///
/// Collected from three sources, unioned defensively since no single one is
/// complete on its own:
/// - the HEAD tree (filemode `160000` / [`FileMode::Commit`] entries) — covers
///   a submodule already committed even if `.gitmodules` was later deleted;
/// - the on-disk index (same filemode check) — covers a submodule staged for
///   removal or add that HEAD does not yet reflect;
/// - [`Repository::submodules`] (`.gitmodules` + config) — the backstop for a
///   submodule registered but not yet committed anywhere.
pub fn submodule_paths(repo: &Repository) -> Result<std::collections::BTreeSet<String>> {
    let mut paths = std::collections::BTreeSet::new();
    let commit_mode = i32::from(FileMode::Commit);

    if let Ok(head) = repo.head() {
        if let Some(oid) = head.target() {
            if let Ok(commit) = repo.find_commit(oid) {
                let tree = commit.tree().map_err(map_git_err)?;
                tree.walk(TreeWalkMode::PreOrder, |root, entry| {
                    if entry.filemode() == commit_mode {
                        if let Ok(name) = entry.name() {
                            paths.insert(format!("{root}{name}"));
                        }
                    }
                    TreeWalkResult::Ok
                })
                .map_err(map_git_err)?;
            }
        }
    }

    let index = repo.index().map_err(map_git_err)?;
    let commit_mode_u32 = u32::from(FileMode::Commit);
    for entry in index.iter() {
        if entry.mode == commit_mode_u32 {
            paths.insert(String::from_utf8_lossy(&entry.path).to_string());
        }
    }

    if let Ok(submodules) = repo.submodules() {
        for sm in submodules {
            paths.insert(sm.path().to_string_lossy().to_string());
        }
    }

    Ok(paths)
}

/// When `rel_path` lies strictly inside one of `submodules`, returns the
/// containing submodule path; `None` when `rel_path` names the submodule
/// itself (a pin-bump commit/stage target, always allowed) or is unrelated.
pub fn submodule_containing<'a>(
    submodules: &'a std::collections::BTreeSet<String>,
    rel_path: &str,
) -> Option<&'a str> {
    let target = Path::new(rel_path);
    for sm in submodules {
        let sm_path = Path::new(sm);
        if target == sm_path {
            continue;
        }
        if let Ok(rest) = target.strip_prefix(sm_path) {
            if !rest.as_os_str().is_empty() {
                return Some(sm.as_str());
            }
        }
    }
    None
}

/// Refuse any of `paths` that lies strictly inside a registered submodule,
/// with a message naming the offending path and its containing submodule
/// (parity with real `git add`'s "is in submodule" pathspec error). Callers
/// invoke this once, before any index mutation, so a rejected batch leaves
/// the index and HEAD untouched.
pub fn reject_submodule_internal_paths(repo: &Repository, paths: &[String]) -> Result<()> {
    let submodules = submodule_paths(repo)?;
    if submodules.is_empty() {
        return Ok(());
    }
    for raw in paths {
        if let Some(sm) = submodule_containing(&submodules, raw) {
            return Err(Error::Internal(format!(
                "fatal: Pathspec '{raw}' is in submodule '{sm}'"
            )));
        }
    }
    Ok(())
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
