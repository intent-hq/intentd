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
use crate::submodule::{has_submodules, update_submodules};
use crate::{is_conflict_error, map_git_err};

const STASH_MESSAGE: &str = "Intent: auto-stash before pull";

/// TS pop-conflict message, preserved verbatim.
const POP_CONFLICT_MSG: &str = "Pull succeeded but your local changes conflict with the pulled changes. Your changes are saved in the stash. Run 'git stash pop' and resolve conflicts manually, or use 'git stash drop' to discard your local changes.";

/// Pull `branch_name` from `origin` for the repository at `repo_path`. See the
/// module docs for the fetch-only vs pull-with-rebase split and the auto-stash
/// workflow. `token` is an optional caller-resolved GitHub token forwarded to
/// the fetch step (see [`crate::fetch::fetch`]). Returns the outcome rather
/// than an `Err` for the expected failure paths (matching the TS contract).
pub fn pull_branch(
    repo_path: &Path,
    branch_name: &str,
    token: Option<&str>,
) -> Result<GitPullResult> {
    let mut repo = Repository::open(repo_path).map_err(map_git_err)?;
    let current = crate::status::current_branch(&repo);

    // Branch not checked out → update the remote-tracking ref only (the TS
    // "fetch instead of pull" path used during workspace creation).
    if current != branch_name {
        return Ok(match fetch(repo_path, "origin", branch_name, token) {
            Ok(()) => success(),
            Err(e) => failure(error_message(e)),
        });
    }

    // `git pull --rebase origin <branch>` ≡ fetch then rebase HEAD onto the
    // updated remote-tracking ref.
    if let Err(e) = fetch(repo_path, "origin", branch_name, token) {
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

    // Sync submodules after a successful pull when the repo has configured
    // submodules (workspace-create use case: parent repo is pulled and
    // submodules must be checked out to match the gitlinks).
    if has_submodules(repo_path) {
        if let Err(e) = update_submodules(repo_path) {
            return Ok(failure(format!(
                "Pull succeeded but failed to update submodules: {}",
                error_message(e)
            )));
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
    let conflicted = repo.index().is_ok_and(|i| i.has_conflicts());
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
        crate::push::push(dir.path(), "origin", &branch, false, None).unwrap();
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
        crate::push::push(dir, "origin", branch, false, None).unwrap();
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

        let result = pull_branch(dir.path(), &branch, None).unwrap();
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

        let result = pull_branch(dir.path(), &branch, None).unwrap();
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

        let result = pull_branch(dir.path(), &branch, None).unwrap();
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

        let result = pull_branch(dir.path(), &branch, None).unwrap();
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

        let result = pull_branch(dir.path(), &branch, None).unwrap();
        assert!(!result.ok);
        assert!(result.error.is_some());
    }

    #[test]
    fn pull_pop_conflict_reports_ts_parity_error() {
        let (dir, bare, branch) = setup_with_origin("pull-pop-conflict");
        // Origin rewrites base.txt; the local dirty edit touches the same line.
        advance_origin_and_fall_behind(dir.path(), &branch, "base.txt", "two\n");
        write_file(dir.path(), "base.txt", "local\n");

        let result = pull_branch(dir.path(), &branch, None).unwrap();
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

    /// Regression: a repo with configured submodules where origin advances with
    /// a gitlink bump must pull cleanly and sync the submodule worktree to the
    /// new gitlink commit (workspace-create use case).
    #[test]
    fn pull_with_submodule_gitlink_bump_syncs_submodule() {
        // Parent repo with a submodule configured.
        let parent_dir = init_repo("pull-parent-with-sub");
        commit_file(parent_dir.path(), "parent.txt", "parent\n");

        // Create a bare origin for the parent.
        let parent_bare = std::env::temp_dir().join(format!(
            "intent-git-pull-parent-bare-{}",
            std::time::SystemTime::now()
                .duration_since(std::time::UNIX_EPOCH)
                .unwrap()
                .as_nanos()
        ));
        Repository::init_bare(&parent_bare).unwrap();
        let parent_repo = Repository::open(parent_dir.path()).unwrap();
        let branch = crate::status::current_branch(&parent_repo);
        parent_repo
            .remote("origin", parent_bare.to_str().unwrap())
            .unwrap();

        // Create a separate submodule repo with its own bare origin.
        let sub_dir = init_repo("pull-submodule");
        commit_file(sub_dir.path(), "sub.txt", "sub-v1\n");
        let sub_bare = std::env::temp_dir().join(format!(
            "intent-git-pull-sub-bare-{}",
            std::time::SystemTime::now()
                .duration_since(std::time::UNIX_EPOCH)
                .unwrap()
                .as_nanos()
        ));
        Repository::init_bare(&sub_bare).unwrap();
        let sub_repo = Repository::open(sub_dir.path()).unwrap();
        sub_repo
            .remote("origin", sub_bare.to_str().unwrap())
            .unwrap();
        let sub_branch = crate::status::current_branch(&sub_repo);
        crate::push::push(sub_dir.path(), "origin", &sub_branch, false, None).unwrap();
        let sub_v1_sha = sub_repo.head().unwrap().target().unwrap().to_string();

        // Add the submodule to the parent repo using shell git (libgit2 submodule
        // API is complex and shell git is the production add path).
        // Allow file:// protocol for the test (git 2.38+ blocks it by default).
        let output = std::process::Command::new("git")
            .arg("-C")
            .arg(parent_dir.path())
            .arg("-c")
            .arg("protocol.file.allow=always")
            .arg("submodule")
            .arg("add")
            .arg(sub_bare.to_str().unwrap())
            .arg("mysubmodule")
            .output()
            .unwrap();
        assert!(
            output.status.success(),
            "git submodule add failed: {}",
            String::from_utf8_lossy(&output.stderr)
        );
        // The git submodule add command already staged .gitmodules and the gitlink.
        // Commit them using the standard commit helper.
        let parent_repo = Repository::open(parent_dir.path()).unwrap();
        let mut index = parent_repo.index().unwrap();
        let tree_oid = index.write_tree().unwrap();
        let tree = parent_repo.find_tree(tree_oid).unwrap();
        let sig = git2::Signature::now("Test", "test@example.com").unwrap();
        let head_commit = parent_repo.head().unwrap().peel_to_commit().unwrap();
        parent_repo
            .commit(
                Some("HEAD"),
                &sig,
                &sig,
                "Add submodule",
                &tree,
                &[&head_commit],
            )
            .unwrap();
        crate::push::push(parent_dir.path(), "origin", &branch, false, None).unwrap();

        // Origin advances: submodule adds a new commit, parent bumps the gitlink.
        commit_file(sub_dir.path(), "sub.txt", "sub-v2\n");
        crate::push::push(sub_dir.path(), "origin", &sub_branch, false, None).unwrap();

        // Update the parent's submodule to point at the new commit.
        let output = std::process::Command::new("git")
            .arg("-C")
            .arg(parent_dir.path())
            .arg("-c")
            .arg("protocol.file.allow=always")
            .arg("submodule")
            .arg("update")
            .arg("--remote")
            .arg("mysubmodule")
            .output()
            .unwrap();
        assert!(
            output.status.success(),
            "submodule update --remote failed: {}",
            String::from_utf8_lossy(&output.stderr)
        );

        // Stage the updated gitlink.
        let parent_repo_for_stage = Repository::open(parent_dir.path()).unwrap();
        let mut index = parent_repo_for_stage.index().unwrap();
        index.add_path(Path::new("mysubmodule")).unwrap();
        index.write().unwrap();

        // The submodule is now at v2 after the update --remote.

        // Commit the gitlink bump + a parent file change.
        commit_file(parent_dir.path(), "parent.txt", "parent-updated\n");
        crate::push::push(parent_dir.path(), "origin", &branch, false, None).unwrap();

        // Hard-reset the parent back one commit so it's behind origin with the
        // old gitlink.
        let parent_repo_reset = Repository::open(parent_dir.path()).unwrap();
        let head = parent_repo_reset.head().unwrap().peel_to_commit().unwrap();
        let parent_obj = head.parent(0).unwrap();
        parent_repo_reset
            .reset(parent_obj.as_object(), git2::ResetType::Hard, None)
            .unwrap();

        // Reset the submodule to match the old gitlink (v1). The hard reset above
        // updated the parent index to point at the old gitlink, so we need to sync
        // the submodule worktree to that commit.
        let sub_path = parent_dir.path().join("mysubmodule");
        std::process::Command::new("git")
            .arg("-C")
            .arg(parent_dir.path())
            .arg("-c")
            .arg("protocol.file.allow=always")
            .arg("submodule")
            .arg("update")
            .arg("mysubmodule")
            .output()
            .unwrap();

        // Confirm the submodule worktree is at v1 (the old gitlink).
        let sub_repo_local = Repository::open(&sub_path).unwrap();
        assert_eq!(
            sub_repo_local.head().unwrap().target().unwrap().to_string(),
            sub_v1_sha,
            "submodule should be at v1 before pull"
        );

        // Pull the parent branch. The gitlink bump should not be treated as "dirty".
        let result = pull_branch(parent_dir.path(), &branch, None).unwrap();
        assert!(
            result.ok,
            "pull with submodule gitlink bump must succeed, got {result:?}"
        );
        assert!(result.error.is_none());

        // The parent is now at the new commit with the bumped gitlink.
        assert_eq!(
            std::fs::read_to_string(parent_dir.path().join("parent.txt")).unwrap(),
            "parent-updated\n",
            "parent worktree must have the new commit"
        );

        // Verify the submodule worktree is synced to the gitlink in the parent.
        let parent_repo_after = Repository::open(parent_dir.path()).unwrap();
        let parent_tree = parent_repo_after.head().unwrap().peel_to_tree().unwrap();
        let submodule_entry = parent_tree.get_name("mysubmodule").unwrap();
        let gitlink_sha = submodule_entry.id().to_string();

        let sub_repo_local = Repository::open(&sub_path).unwrap();
        let actual_sub_sha = sub_repo_local.head().unwrap().target().unwrap().to_string();
        assert_eq!(
            actual_sub_sha, gitlink_sha,
            "submodule worktree must be synced to match the parent's gitlink"
        );
        assert_eq!(
            std::fs::read_to_string(sub_path.join("sub.txt")).unwrap(),
            "sub-v2\n",
            "submodule worktree must have the updated content"
        );

        // Cleanup.
        let _ = std::fs::remove_dir_all(&parent_bare);
        let _ = std::fs::remove_dir_all(&sub_bare);
    }
}
