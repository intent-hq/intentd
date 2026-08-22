//! Branch reset (`git reset --hard` / `git reset --soft`).
//!
//! Ports the reset steps the accept-changes undo/reset-to-trunk handlers run:
//! `git reset --soft <sha>` (move HEAD, keep index + worktree) for the undo path
//! and `git reset --hard <sha>` (move HEAD, discard index + worktree changes) for
//! the reset-to-trunk path. `target` may be any revspec libgit2 resolves (a SHA,
//! branch, or remote-tracking ref).

use std::path::Path;

use git2::{Repository, ResetType};
use intent_core::Result;

use crate::map_git_err;

/// `git reset --hard <target>`: move `HEAD` to `target` and overwrite the index
/// and working tree to match it (discarding local changes).
///
/// # Errors
///
/// Returns `Error::Internal` if `target` cannot be resolved or the reset fails.
pub fn reset_hard(worktree_path: &Path, target: &str) -> Result<()> {
    reset(worktree_path, target, ResetType::Hard)
}

/// `git reset --soft <target>`: move `HEAD` to `target`, leaving the index and
/// working tree untouched (staged changes are preserved).
///
/// # Errors
///
/// Returns `Error::Internal` if `target` cannot be resolved or the reset fails.
pub fn reset_soft(worktree_path: &Path, target: &str) -> Result<()> {
    reset(worktree_path, target, ResetType::Soft)
}

fn reset(worktree_path: &Path, target: &str, kind: ResetType) -> Result<()> {
    let repo = Repository::open(worktree_path).map_err(map_git_err)?;
    let object = repo.revparse_single(target).map_err(map_git_err)?;
    repo.reset(&object, kind, None).map_err(map_git_err)?;
    Ok(())
}

#[cfg(test)]
mod tests {
    use super::*;
    use crate::testutil::{commit_file, init_repo, write_file};

    #[test]
    fn reset_hard_discards_changes_and_moves_head() {
        let dir = init_repo("reset-hard");
        commit_file(dir.path(), "a.txt", "one\n");
        let repo = Repository::open(dir.path()).unwrap();
        let first = repo.head().unwrap().target().unwrap().to_string();
        commit_file(dir.path(), "a.txt", "two\n");
        // An extra uncommitted change that hard reset must discard.
        write_file(dir.path(), "a.txt", "dirty\n");

        reset_hard(dir.path(), &first).unwrap();

        assert_eq!(repo.head().unwrap().target().unwrap().to_string(), first);
        let on_disk = std::fs::read_to_string(dir.path().join("a.txt")).unwrap();
        assert_eq!(on_disk, "one\n");
    }

    #[test]
    fn reset_soft_moves_head_but_keeps_worktree() {
        let dir = init_repo("reset-soft");
        commit_file(dir.path(), "a.txt", "one\n");
        let repo = Repository::open(dir.path()).unwrap();
        let first = repo.head().unwrap().target().unwrap().to_string();
        commit_file(dir.path(), "a.txt", "two\n");

        reset_soft(dir.path(), &first).unwrap();

        // HEAD moved back, but the worktree still has the later content.
        assert_eq!(repo.head().unwrap().target().unwrap().to_string(), first);
        let on_disk = std::fs::read_to_string(dir.path().join("a.txt")).unwrap();
        assert_eq!(on_disk, "two\n");
    }
}
