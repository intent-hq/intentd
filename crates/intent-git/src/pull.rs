//! Branch pull (`git.pull`), porting the legacy `git:pullBranch` IPC handler.
//!
//! The workspace-create flow auto-pulls a behind branch before creating the
//! workspace. Semantics ported from the TS handler (`git.ipc.ts` PULL_BRANCH):
//! when `branch_name` is not the checked-out branch, a fetch of
//! `origin/<branch>` is sufficient (worktrees are created from the
//! remote-tracking ref); when it is checked out, run the equivalent of
//! `git pull --rebase origin <branch>` with the auto-stash workflow — stash a
//! dirty worktree (including untracked files), rebase onto the updated
//! remote-tracking ref, and pop the stash back, classifying a pop conflict
//! apart from an unrelated pop failure. Ordinary pull failures surface as a
//! structured `{ ok: false, error }` result, never an `Err` (only a failure to
//! open the repository does), so the FE can show its pull-conflict dialog.

use std::path::Path;

use git2::Repository;
use intent_core::{Error, GitPullResult, Result};

use crate::fetch::fetch;
use crate::rebase::{is_dirty, run_rebase};
use crate::stash::{pop_raw, push_include_untracked_raw};
use crate::{is_conflict_error, map_git_err};

const STASH_MESSAGE: &str = "Intent: auto-stash before pull";

/// TS pop-conflict message, preserved verbatim.
const POP_CONFLICT_MSG: &str = "Pull succeeded but your local changes conflict with the pulled changes. Your changes are saved in the stash. Run 'git stash pop' and resolve conflicts manually, or use 'git stash drop' to discard your local changes.";

/// Pull `branch_name` from `origin` for the repository at `repo_path`. See the
/// module docs for the fetch-only vs pull-with-rebase split and the auto-stash
/// workflow. Returns the outcome rather than an `Err` for the expected failure
/// paths (matching the TS contract).
pub fn pull_branch(repo_path: &Path, branch_name: &str) -> Result<GitPullResult> {
    let mut repo = Repository::open(repo_path).map_err(map_git_err)?;
    let current = crate::status::current_branch(&repo);

    // Branch not checked out → update the remote-tracking ref only (the TS
    // "fetch instead of pull" path used during workspace creation).
    if current != branch_name {
        return Ok(match fetch(repo_path, "origin", branch_name) {
            Ok(()) => success(),
            Err(e) => failure(error_message(e)),
        });
    }

    // `git pull --rebase origin <branch>` ≡ fetch then rebase HEAD onto the
    // updated remote-tracking ref.
    if let Err(e) = fetch(repo_path, "origin", branch_name) {
        return Ok(failure(error_message(e)));
    }

    // Auto-stash bookend: the TS handler retries the pull after stashing when
    // git rejects a dirty rebase; with libgit2 driving the rebase directly, the
    // observable equivalent is to stash a dirty worktree up front.
    let mut stash_created = false;
    if is_dirty(&repo)? {
        match push_include_untracked_raw(&mut repo, STASH_MESSAGE) {
            Ok(Some(_)) => stash_created = true,
            Ok(None) => {}
            Err(e) => {
                return Ok(failure(format!(
                    "Failed to auto-stash changes: {}",
                    e.message()
                )));
            }
        }
    }

    let upstream = format!("refs/remotes/origin/{branch_name}");
    let (rebased, rebase_error, _aborted) = run_rebase(&repo, &upstream);

    if !rebased {
        // Pull failed — restore the stash best-effort (the TS handler logs and
        // returns the original pull error either way).
        if stash_created {
            let _ = pop_raw(&mut repo);
        }
        return Ok(failure(
            rebase_error.unwrap_or_else(|| "Failed to pull branch".to_string()),
        ));
    }

    if stash_created {
        match pop_stash(&mut repo) {
            Ok(false) => {}
            // Applied with conflict markers — the stash entry is kept (git CLI
            // `stash pop` parity), so the TS message's recovery steps hold.
            Ok(true) => return Ok(failure(POP_CONFLICT_MSG.to_string())),
            Err(e) => {
                let msg = if is_conflict_error(&e) {
                    POP_CONFLICT_MSG.to_string()
                } else {
                    format!(
                        "Pull succeeded but failed to restore your local changes: {}. Your changes are saved in the stash - run 'git stash pop' to restore them.",
                        e.message()
                    )
                };
                return Ok(failure(msg));
            }
        }
    }

    Ok(success())
}

/// `git stash pop` CLI parity: apply the most recent stash, and drop it only
/// when the apply produced no conflicts (the CLI keeps the stash entry on a
/// conflicted pop; libgit2's `stash_pop` would drop it after writing conflict
/// markers). Returns whether the apply left index conflicts.
fn pop_stash(repo: &mut Repository) -> std::result::Result<bool, git2::Error> {
    repo.stash_apply(0, None)?;
    let conflicted = repo.index().map(|i| i.has_conflicts()).unwrap_or(false);
    if !conflicted {
        repo.stash_drop(0)?;
    }
    Ok(conflicted)
}

fn success() -> GitPullResult {
    GitPullResult {
        ok: true,
        error: None,
    }
}

fn failure(error: String) -> GitPullResult {
    GitPullResult {
        ok: false,
        error: Some(error),
    }
}

/// Unwrap the domain error back to its raw message for the structured `error`
/// field (avoids the `internal error:` display prefix).
fn error_message(e: Error) -> String {
    match e {
        Error::Internal(m) => m,
        other => other.to_string(),
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use crate::testutil::{
        checkout_branch, commit_file, create_branch, init_repo, write_file, TempDir,
    };
    use std::path::PathBuf;

    /// Seed a repo with one commit, push it into a fresh bare `origin`, and
    /// return `(worktree, bare_dir, branch)`.
    fn setup_with_origin(tag: &str) -> (TempDir, PathBuf, String) {
        let dir = init_repo(tag);
        commit_file(dir.path(), "base.txt", "one\n");
        let repo = Repository::open(dir.path()).unwrap();
        let branch = crate::status::current_branch(&repo);
        let bare = std::env::temp_dir().join(format!(
            "intent-git-pull-bare-{tag}-{}",
            std::time::SystemTime::now()
                .duration_since(std::time::UNIX_EPOCH)
                .unwrap()
                .as_nanos()
        ));
        Repository::init_bare(&bare).unwrap();
        repo.remote("origin", bare.to_str().unwrap()).unwrap();
        crate::push::push(dir.path(), "origin", &branch, false).unwrap();
        (dir, bare, branch)
    }

    /// Commit `rel`/`contents`, push it, then hard-reset the worktree back one
    /// commit so the local branch is behind origin. Returns the pushed tip sha.
    fn advance_origin_and_fall_behind(
        dir: &Path,
        branch: &str,
        rel: &str,
        contents: &str,
    ) -> String {
        commit_file(dir, rel, contents);
        crate::push::push(dir, "origin", branch, false).unwrap();
        let repo = Repository::open(dir).unwrap();
        let head = repo.head().unwrap().peel_to_commit().unwrap();
        let tip = head.id().to_string();
        let parent = head.parent(0).unwrap();
        repo.reset(parent.as_object(), git2::ResetType::Hard, None)
            .unwrap();
        tip
    }

    #[test]
    fn pull_fast_forwards_checked_out_branch_behind_origin() {
        let (dir, bare, branch) = setup_with_origin("pull-ff");
        let tip =
            advance_origin_and_fall_behind(dir.path(), &branch, "remote.txt", "from-remote\n");
        assert!(!dir.path().join("remote.txt").exists());

        let result = pull_branch(dir.path(), &branch).unwrap();
        assert!(result.ok, "expected pull to succeed, got {result:?}");
        assert!(result.error.is_none());
        assert!(dir.path().join("remote.txt").exists());
        let repo = Repository::open(dir.path()).unwrap();
        assert_eq!(repo.head().unwrap().target().unwrap().to_string(), tip);

        let _ = std::fs::remove_dir_all(&bare);
    }

    /// Regression for the workspace-create auto-pull: a branch already level
    /// with `origin/<branch>` (0 ahead / 0 behind, only untracked noise like
    /// `.DS_Store`) must pull as a no-op `{ ok: true }` — never a rebase
    /// failure — and the auto-stash bookends must leave no stash entry behind.
    #[test]
    fn pull_up_to_date_branch_is_noop_ok() {
        let (dir, bare, branch) = setup_with_origin("pull-up-to-date");
        write_file(dir.path(), ".DS_Store", "junk\n");
        let repo = Repository::open(dir.path()).unwrap();
        let head_before = repo.head().unwrap().target().unwrap();

        let result = pull_branch(dir.path(), &branch).unwrap();
        assert!(result.ok, "expected no-op pull to succeed, got {result:?}");
        assert!(result.error.is_none());

        let mut repo = Repository::open(dir.path()).unwrap();
        assert_eq!(repo.head().unwrap().target().unwrap(), head_before);
        assert_eq!(
            std::fs::read_to_string(dir.path().join(".DS_Store")).unwrap(),
            "junk\n"
        );
        let mut stash_count = 0;
        repo.stash_foreach(|_, _, _| {
            stash_count += 1;
            true
        })
        .unwrap();
        assert_eq!(stash_count, 0, "no stash entry may leak from a no-op pull");

        let _ = std::fs::remove_dir_all(&bare);
    }

    #[test]
    fn pull_auto_stashes_dirty_worktree_and_restores_changes() {
        let (dir, bare, branch) = setup_with_origin("pull-stash");
        advance_origin_and_fall_behind(dir.path(), &branch, "remote.txt", "from-remote\n");
        // An untracked local change must survive the pull (auto-stash + pop).
        write_file(dir.path(), "local.txt", "uncommitted\n");

        let result = pull_branch(dir.path(), &branch).unwrap();
        assert!(result.ok, "expected pull to succeed, got {result:?}");
        assert!(dir.path().join("remote.txt").exists());
        assert_eq!(
            std::fs::read_to_string(dir.path().join("local.txt")).unwrap(),
            "uncommitted\n"
        );

        let _ = std::fs::remove_dir_all(&bare);
    }

    #[test]
    fn pull_fetches_only_when_branch_not_checked_out() {
        let (dir, bare, branch) = setup_with_origin("pull-fetch-only");
        let tip =
            advance_origin_and_fall_behind(dir.path(), &branch, "remote.txt", "from-remote\n");
        create_branch(dir.path(), "other");
        checkout_branch(dir.path(), "other");
        // Drop the remote-tracking ref so the fetch-only path must recreate it.
        let repo = Repository::open(dir.path()).unwrap();
        repo.find_reference(&format!("refs/remotes/origin/{branch}"))
            .unwrap()
            .delete()
            .unwrap();

        let result = pull_branch(dir.path(), &branch).unwrap();
        assert!(
            result.ok,
            "expected fetch-only pull to succeed, got {result:?}"
        );
        // The tracking ref points at the pushed tip; the worktree is untouched.
        let tracking = repo
            .find_reference(&format!("refs/remotes/origin/{branch}"))
            .unwrap()
            .target()
            .unwrap()
            .to_string();
        assert_eq!(tracking, tip);
        assert!(!dir.path().join("remote.txt").exists());

        let _ = std::fs::remove_dir_all(&bare);
    }

    #[test]
    fn pull_without_origin_reports_structured_failure() {
        let dir = init_repo("pull-no-origin");
        commit_file(dir.path(), "a.txt", "x\n");
        let repo = Repository::open(dir.path()).unwrap();
        let branch = crate::status::current_branch(&repo);

        let result = pull_branch(dir.path(), &branch).unwrap();
        assert!(!result.ok);
        assert!(result.error.is_some());
    }

    #[test]
    fn pull_pop_conflict_reports_ts_parity_error() {
        let (dir, bare, branch) = setup_with_origin("pull-pop-conflict");
        // Origin rewrites base.txt; the local dirty edit touches the same line.
        advance_origin_and_fall_behind(dir.path(), &branch, "base.txt", "two\n");
        write_file(dir.path(), "base.txt", "local\n");

        let result = pull_branch(dir.path(), &branch).unwrap();
        assert!(!result.ok);
        assert_eq!(result.error.as_deref(), Some(POP_CONFLICT_MSG));
        // The stash entry is kept on a conflicted pop (git CLI parity), so the
        // recovery steps in the error message are actionable.
        let mut repo = Repository::open(dir.path()).unwrap();
        let mut stash_count = 0;
        repo.stash_foreach(|_, _, _| {
            stash_count += 1;
            true
        })
        .unwrap();
        assert_eq!(stash_count, 1);

        let _ = std::fs::remove_dir_all(&bare);
    }
}
