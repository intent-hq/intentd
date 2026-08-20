//! Working-tree stash (`git stash push --include-untracked` / `git stash pop`).
//!
//! Ports the auto-stash bookends of the accept-changes rebase path: stash the
//! dirty worktree (including untracked files) before rebasing and pop it back
//! afterwards. `stash_push_include_untracked` returns `false` when there is
//! nothing to stash (the TS `No local changes to save` check). The `*_raw`
//! helpers expose the underlying libgit2 errors so [`crate::rebase`] can classify
//! a pop conflict apart from an unrelated pop failure.

#[cfg(test)]
use std::path::Path;

use git2::{ErrorCode, Oid, Repository, Signature, StashFlags};
#[cfg(test)]
use intent_core::Result;

#[cfg(test)]
use crate::map_git_err;

/// `git stash push --include-untracked -m <msg>`: stash the dirty worktree
/// (including untracked files). Returns `true` when a stash was created, `false`
/// when there was nothing to stash.
#[cfg(test)]
pub(crate) fn stash_push_include_untracked(worktree_path: &Path, message: &str) -> Result<bool> {
    let mut repo = Repository::open(worktree_path).map_err(map_git_err)?;
    push_include_untracked_raw(&mut repo, message)
        .map(|oid| oid.is_some())
        .map_err(map_git_err)
}

/// `git stash pop`: apply the most recent stash and drop it on success.
#[cfg(test)]
pub(crate) fn stash_pop(worktree_path: &Path) -> Result<()> {
    let mut repo = Repository::open(worktree_path).map_err(map_git_err)?;
    pop_raw(&mut repo).map_err(map_git_err)
}

/// The stasher signature, falling back to the same defaults as the rest of the
/// crate when no git identity is configured.
fn stasher(repo: &Repository) -> std::result::Result<Signature<'static>, git2::Error> {
    match repo.signature() {
        Ok(sig) => Signature::now(
            sig.name().unwrap_or("Intent"),
            sig.email().unwrap_or("intent@local"),
        ),
        Err(_) => Signature::now("Intent", "intent@local"),
    }
}

/// Stash including untracked files, returning `None` when there is nothing to
/// stash (libgit2 surfaces this as `NotFound`). Surfaces other libgit2 errors.
pub(crate) fn push_include_untracked_raw(
    repo: &mut Repository,
    message: &str,
) -> std::result::Result<Option<Oid>, git2::Error> {
    let sig = stasher(repo)?;
    match repo.stash_save2(&sig, Some(message), Some(StashFlags::INCLUDE_UNTRACKED)) {
        Ok(oid) => Ok(Some(oid)),
        Err(e) if e.code() == ErrorCode::NotFound => Ok(None),
        Err(e) => Err(e),
    }
}

/// Pop the most recent stash (index 0), preserving the raw libgit2 error so the
/// caller can distinguish a conflict from an unrelated failure.
pub(crate) fn pop_raw(repo: &mut Repository) -> std::result::Result<(), git2::Error> {
    repo.stash_pop(0, None)
}

#[cfg(test)]
mod tests {
    use super::*;
    use crate::testutil::{commit_file, init_repo, write_file};

    #[test]
    fn stash_push_returns_false_when_clean() {
        let dir = init_repo("stash-clean");
        commit_file(dir.path(), "a.txt", "one\n");
        assert!(!stash_push_include_untracked(dir.path(), "intent: test").unwrap());
    }

    #[test]
    fn stash_push_then_pop_round_trips_changes() {
        let dir = init_repo("stash-roundtrip");
        commit_file(dir.path(), "a.txt", "one\n");
        // A tracked edit plus an untracked file — both must be stashed.
        write_file(dir.path(), "a.txt", "dirty\n");
        write_file(dir.path(), "untracked.txt", "new\n");

        assert!(stash_push_include_untracked(dir.path(), "intent: test").unwrap());
        // Worktree is clean again after stashing.
        assert_eq!(
            std::fs::read_to_string(dir.path().join("a.txt")).unwrap(),
            "one\n"
        );
        assert!(!dir.path().join("untracked.txt").exists());

        stash_pop(dir.path()).unwrap();
        // Both the tracked edit and the untracked file are restored.
        assert_eq!(
            std::fs::read_to_string(dir.path().join("a.txt")).unwrap(),
            "dirty\n"
        );
        assert_eq!(
            std::fs::read_to_string(dir.path().join("untracked.txt")).unwrap(),
            "new\n"
        );
    }
}
