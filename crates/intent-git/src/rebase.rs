//! Rebase with auto-stash (`accept-changes.service.ts:234 rebaseWithAutoStash`).
//!
//! Faithfully ports the TS auto-stash rebase: check for a dirty worktree, stash
//! (including untracked files) if dirty, run `git rebase <trunk>`, and on failure
//! abort the rebase and classify conflict-vs-other. Whether the rebase succeeds or
//! fails, a stash that was created is popped back; the exact TS stash-recovery
//! error strings are preserved for both the rebase-succeeded and rebase-failed
//! pop branches. libgit2 drives the rebase (the Rebase API), so no shell-out.

use std::path::Path;

use git2::{AnnotatedCommit, RebaseOptions, Repository, Signature, StatusOptions};
use intent_core::Result;

use crate::stash::{pop_raw, push_include_untracked_raw};
use crate::{is_conflict_error, map_git_err};

const STASH_MESSAGE: &str = "Intent: auto-stash before merge rebase";
const CONFLICT_MSG: &str = "Conflicts detected. Please rebase manually.";
const REBASE_FAIL_MSG: &str = "Rebase failed. Please try rebasing manually.";

/// The outcome of [`rebase_with_autostash`], mirroring the TS
/// `{ success, aborted?, error? }` shape.
#[derive(Debug, Clone, PartialEq, Eq)]
pub struct RebaseOutcome {
    pub success: bool,
    pub aborted: bool,
    pub error: Option<String>,
}

/// Rebase the current `HEAD` onto `trunk_ref` with auto-stash. Returns the outcome
/// rather than an `Err` for the expected conflict/recovery paths (matching the TS
/// contract); only a failure to open the repository surfaces as `Err`.
///
/// # Errors
///
/// Returns `Error::Internal` only when the repository cannot be opened or the dirty check/stash fails; rebase conflicts are reported in the returned outcome.
pub fn rebase_with_autostash(worktree_path: &Path, trunk_ref: &str) -> Result<RebaseOutcome> {
    let mut repo = Repository::open(worktree_path).map_err(map_git_err)?;

    // Step 1: dirty check → stash (including untracked).
    let mut stash_created = false;
    if is_dirty(&repo)? {
        match push_include_untracked_raw(&mut repo, STASH_MESSAGE) {
            Ok(Some(_)) => stash_created = true,
            Ok(None) => {}
            Err(_) => {
                return Ok(RebaseOutcome {
                    success: false,
                    aborted: false,
                    error: Some(
                        "Failed to stash uncommitted changes before rebase. Please commit or stash your changes manually and retry."
                            .to_string(),
                    ),
                });
            }
        }
    }

    // Step 2: run the rebase.
    let (success, mut error, aborted) = run_rebase(&repo, trunk_ref);

    // Step 3: pop the stash if we created one (regardless of rebase outcome).
    if stash_created {
        if let Err(e) = pop_raw(&mut repo) {
            let conflict = is_conflict_error(&e);
            if success {
                let msg = if conflict {
                    "Rebase succeeded but your local changes conflict with the rebased code. Please resolve the conflicts in your working tree and then run 'git stash drop' to clean up the stash."
                } else {
                    "Rebase succeeded but failed to restore your local changes. Your changes are saved in the stash - run 'git stash pop' to restore them."
                };
                return Ok(RebaseOutcome {
                    success: false,
                    aborted,
                    error: Some(msg.to_string()),
                });
            }
            let base = error.clone().unwrap_or_default();
            error = Some(if conflict {
                format!("{base} Your uncommitted changes were partially applied with conflicts — resolve the conflicts in your working tree and run `git stash drop` to clean up.")
            } else {
                format!("{base} Your uncommitted changes are still in the stash — run `git stash pop` to recover them.")
            });
        }
    }

    Ok(RebaseOutcome {
        success,
        aborted,
        error,
    })
}

/// `git status --porcelain` non-empty: any staged/unstaged/untracked change.
/// Shared with [`crate::pull`], whose auto-stash bookends need the same check.
/// Excludes submodules (git CLI `git stash push` parity — submodule gitlink
/// divergence is not treated as a dirty worktree).
pub(crate) fn is_dirty(repo: &Repository) -> Result<bool> {
    let mut opts = StatusOptions::new();
    opts.include_untracked(true)
        .recurse_untracked_dirs(true)
        .include_ignored(false)
        .exclude_submodules(true);
    let statuses = repo.statuses(Some(&mut opts)).map_err(map_git_err)?;
    Ok(!statuses.is_empty())
}

/// Drive the libgit2 rebase of `HEAD` onto `trunk_ref`, returning
/// `(success, error, aborted)`. On any failure the rebase is aborted (restoring
/// the pre-rebase state) and the error is classified conflict-vs-other. Also
/// backs the pull-with-rebase step of [`crate::pull`] (upstream =
/// `refs/remotes/origin/<branch>`); a behind-only branch replays zero commits
/// and fast-forwards to the upstream tip.
pub(crate) fn run_rebase(repo: &Repository, trunk_ref: &str) -> (bool, Option<String>, bool) {
    let committer = match repo.signature() {
        Ok(sig) => sig,
        Err(_) => match Signature::now("Intent", "intent@local") {
            Ok(sig) => sig,
            Err(e) => return (false, Some(e.message().to_string()), false),
        },
    };
    let upstream = match annotated(repo, trunk_ref) {
        Ok(c) => c,
        Err(e) => return (false, Some(classify(&e)), false),
    };

    let mut opts = RebaseOptions::new();
    let mut rebase = match repo.rebase(None, Some(&upstream), None, Some(&mut opts)) {
        Ok(r) => r,
        Err(e) => return (false, Some(classify(&e)), false),
    };

    loop {
        match rebase.next() {
            None => break,
            Some(Ok(_op)) => {
                let has_conflicts = repo.index().is_ok_and(|i| i.has_conflicts());
                if has_conflicts {
                    let _ = rebase.abort();
                    return (false, Some(CONFLICT_MSG.to_string()), true);
                }
                if let Err(e) = rebase.commit(None, &committer, None) {
                    let msg = classify(&e);
                    let _ = rebase.abort();
                    return (false, Some(msg), true);
                }
            }
            Some(Err(e)) => {
                let msg = classify(&e);
                let _ = rebase.abort();
                return (false, Some(msg), true);
            }
        }
    }

    match rebase.finish(Some(&committer)) {
        Ok(()) => (true, None, false),
        Err(e) => {
            let _ = rebase.abort();
            (false, Some(classify(&e)), true)
        }
    }
}

/// Resolve a refish to an [`AnnotatedCommit`] (the rebase upstream).
fn annotated<'repo>(
    repo: &'repo Repository,
    refish: &str,
) -> std::result::Result<AnnotatedCommit<'repo>, git2::Error> {
    let oid = repo.revparse_single(refish)?.peel_to_commit()?.id();
    repo.find_annotated_commit(oid)
}

/// Classify a rebase failure into the TS conflict / non-conflict message.
fn classify(e: &git2::Error) -> String {
    if is_conflict_error(e) {
        CONFLICT_MSG.to_string()
    } else {
        REBASE_FAIL_MSG.to_string()
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use crate::conflicts::current_branch;
    use crate::testutil::{checkout_branch, commit_file, create_branch, init_repo, write_file};

    #[test]
    fn rebase_clean_replays_branch_onto_trunk() {
        let dir = init_repo("rebase-clean");
        commit_file(dir.path(), "base.txt", "base\n");
        let trunk = current_branch(dir.path()).unwrap();

        create_branch(dir.path(), "feature");
        checkout_branch(dir.path(), "feature");
        commit_file(dir.path(), "feature.txt", "feature\n");

        // Trunk advances on a different file (no conflict).
        checkout_branch(dir.path(), &trunk);
        commit_file(dir.path(), "trunk.txt", "trunk\n");

        checkout_branch(dir.path(), "feature");
        let outcome = rebase_with_autostash(dir.path(), &trunk).unwrap();
        assert!(outcome.success, "expected clean rebase, got {outcome:?}");
        assert!(outcome.error.is_none());
        assert!(!outcome.aborted);

        // Feature now sits on top of the advanced trunk.
        assert!(crate::refs::is_ancestor(dir.path(), &trunk, "HEAD").unwrap());
        assert!(dir.path().join("trunk.txt").exists());
        assert!(dir.path().join("feature.txt").exists());
    }

    #[test]
    fn rebase_conflict_aborts_and_restores_stash() {
        let dir = init_repo("rebase-conflict");
        commit_file(dir.path(), "shared.txt", "base\n");
        let trunk = current_branch(dir.path()).unwrap();

        // Feature changes shared.txt one way.
        create_branch(dir.path(), "feature");
        checkout_branch(dir.path(), "feature");
        commit_file(dir.path(), "shared.txt", "feature\n");

        // Trunk changes the same line differently (rebase will conflict).
        checkout_branch(dir.path(), &trunk);
        commit_file(dir.path(), "shared.txt", "trunk\n");

        // Back on feature, leave an uncommitted (untracked) change to stash.
        checkout_branch(dir.path(), "feature");
        write_file(dir.path(), "other.txt", "uncommitted\n");

        let outcome = rebase_with_autostash(dir.path(), &trunk).unwrap();
        assert!(!outcome.success);
        assert!(outcome.aborted);
        assert_eq!(outcome.error.as_deref(), Some(CONFLICT_MSG));

        // Abort restored the pre-rebase feature commit...
        assert_eq!(
            std::fs::read_to_string(dir.path().join("shared.txt")).unwrap(),
            "feature\n"
        );
        // ...and the stash was popped back, restoring the untracked change.
        assert_eq!(
            std::fs::read_to_string(dir.path().join("other.txt")).unwrap(),
            "uncommitted\n"
        );
    }
}
