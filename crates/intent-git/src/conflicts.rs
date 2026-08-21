//! Merge-conflict detection (`git.checkMergeConflicts`).
//!
//! Ports the TS `ws.git.checkMergeConflicts` helpers: resolve the current branch,
//! determine the target branch (caller-supplied, else `main`/`master` probe like
//! `detectDefaultBranch`), and detect whether merging current into target would
//! conflict. The TS implementation shells out to `git merge-tree`; here we use
//! libgit2's in-memory tree merge (`merge_base` + `merge_trees`) which yields the
//! same has-conflicts / conflicted-files answer without touching the worktree.

use std::path::Path;

use git2::{BranchType, Commit, Repository};
use intent_core::Result;

use crate::map_git_err;

/// The merge-conflict probe result (the wire `targetBranch`/`currentBranch` are
/// added by the caller).
pub struct MergeConflicts {
    pub has_conflicts: bool,
    pub conflicted_files: Vec<String>,
    pub cannot_determine: bool,
}

/// The current branch shorthand for the repository at `repo_path` (empty on a
/// detached HEAD), mirroring `git branch --show-current`.
///
/// # Errors
///
/// Returns `Error::Internal` if the underlying libgit2 operation fails.
pub fn current_branch(repo_path: &Path) -> Result<String> {
    let repo = Repository::open(repo_path).map_err(map_git_err)?;
    Ok(crate::status::current_branch(&repo))
}

/// Probe for the local default branch, trying `main` then `master`, mirroring the
/// TS `detectDefaultBranch` (`git rev-parse --verify <branch>`). `None` when
/// neither exists locally.
///
/// # Errors
///
/// Returns `Error::Internal` if the underlying libgit2 operation fails.
pub fn detect_default_branch(repo_path: &Path) -> Result<Option<String>> {
    let repo = Repository::open(repo_path).map_err(map_git_err)?;
    for name in ["main", "master"] {
        if repo.find_branch(name, BranchType::Local).is_ok() {
            return Ok(Some(name.to_string()));
        }
    }
    Ok(None)
}

/// Detect whether merging `current_branch` into `target_branch` would conflict.
/// When the two share no merge base the result is `cannot_determine` (the TS
/// legacy fallback's only non-error producer).
///
/// # Errors
///
/// Returns `Error::Internal` if the repository cannot be opened, a branch tip cannot be resolved, or the merge analysis fails.
pub fn detect_merge_conflicts(
    repo_path: &Path,
    current_branch: &str,
    target_branch: &str,
) -> Result<MergeConflicts> {
    let repo = Repository::open(repo_path).map_err(map_git_err)?;
    let ours = resolve_commit(&repo, current_branch)?;
    let theirs = resolve_commit(&repo, target_branch)?;

    let Ok(base) = repo.merge_base(ours.id(), theirs.id()) else {
        return Ok(MergeConflicts {
            has_conflicts: false,
            conflicted_files: Vec::new(),
            cannot_determine: true,
        });
    };

    let base_tree = repo
        .find_commit(base)
        .map_err(map_git_err)?
        .tree()
        .map_err(map_git_err)?;
    let our_tree = ours.tree().map_err(map_git_err)?;
    let their_tree = theirs.tree().map_err(map_git_err)?;

    let index = repo
        .merge_trees(&base_tree, &our_tree, &their_tree, None)
        .map_err(map_git_err)?;

    let has_conflicts = index.has_conflicts();
    let mut conflicted_files = Vec::new();
    if has_conflicts {
        for conflict in index.conflicts().map_err(map_git_err)? {
            let conflict = conflict.map_err(map_git_err)?;
            let entry = conflict
                .our
                .as_ref()
                .or(conflict.their.as_ref())
                .or(conflict.ancestor.as_ref());
            if let Some(entry) = entry {
                let path = String::from_utf8_lossy(&entry.path).to_string();
                if !conflicted_files.contains(&path) {
                    conflicted_files.push(path);
                }
            }
        }
    }

    Ok(MergeConflicts {
        has_conflicts,
        conflicted_files,
        cannot_determine: false,
    })
}

/// Resolve a revspec (branch name, remote-tracking ref, or SHA) to a commit,
/// matching `git merge-tree`'s acceptance of arbitrary revs.
fn resolve_commit<'repo>(repo: &'repo Repository, rev: &str) -> Result<Commit<'repo>> {
    repo.revparse_single(rev)
        .map_err(map_git_err)?
        .peel_to_commit()
        .map_err(map_git_err)
}

#[cfg(test)]
mod tests {
    use super::*;
    use crate::testutil::{checkout_branch, commit_file, create_branch, init_repo};

    #[test]
    fn detects_default_branch_main_or_master() {
        let dir = init_repo("conflicts-default");
        // The first commit materializes the configured default branch
        // (`main`/`master`); the probe must find one of them.
        commit_file(dir.path(), "a.txt", "x\n");
        let found = detect_default_branch(dir.path()).unwrap();
        assert!(matches!(found.as_deref(), Some("main" | "master")));
    }

    #[test]
    fn no_conflicts_for_non_overlapping_changes() {
        let dir = init_repo("conflicts-clean");
        commit_file(dir.path(), "base.txt", "base\n");
        create_branch(dir.path(), "target");
        // The current branch edits a different file than target.
        commit_file(dir.path(), "current.txt", "current\n");
        let current = current_branch(dir.path()).unwrap();
        let result = detect_merge_conflicts(dir.path(), &current, "target").unwrap();
        assert!(!result.has_conflicts);
        assert!(result.conflicted_files.is_empty());
        assert!(!result.cannot_determine);
    }

    #[test]
    fn detects_conflicting_changes_to_same_file() {
        let dir = init_repo("conflicts-dirty");
        commit_file(dir.path(), "shared.txt", "base\n");
        let original = current_branch(dir.path()).unwrap();
        // Target branch changes shared.txt.
        create_branch(dir.path(), "target");
        checkout_branch(dir.path(), "target");
        commit_file(dir.path(), "shared.txt", "target change\n");
        // Back to the original branch and change the same line differently.
        checkout_branch(dir.path(), &original);
        commit_file(dir.path(), "shared.txt", "current change\n");
        let result = detect_merge_conflicts(dir.path(), &original, "target").unwrap();
        assert!(result.has_conflicts);
        assert!(result.conflicted_files.contains(&"shared.txt".to_string()));
    }
}
