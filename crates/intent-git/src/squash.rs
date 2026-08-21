//! Squash-merge object plumbing (`git write-tree` / `commit-tree` / `update-ref`).
//!
//! Ports the local-only squash path of the accept-changes merge handler: snapshot
//! the current tree (`git write-tree`), create a single squash commit on top of the
//! merge base (`git commit-tree <tree> -p <merge-base> -m <msg>`), then advance the
//! local trunk ref to it (`git update-ref refs/heads/<trunk> <sha>`) without a
//! network push. The commit uses the repository's configured identity.

use std::path::Path;

use git2::{Oid, Repository};
use intent_core::Result;

use crate::map_git_err;

/// `git write-tree`: write the current index as a tree object and return its SHA.
///
/// # Errors
///
/// Returns `Error::Internal` if the underlying libgit2 operation fails.
pub fn write_tree(worktree_path: &Path) -> Result<String> {
    let repo = Repository::open(worktree_path).map_err(map_git_err)?;
    let mut index = repo.index().map_err(map_git_err)?;
    let oid = index.write_tree().map_err(map_git_err)?;
    Ok(oid.to_string())
}

/// `git commit-tree <tree> -p <parent_sha> -m <message>`: create a commit object
/// from the current index tree with `parent_sha` as its single parent, returning
/// the new commit SHA. Does not move any ref (see [`update_ref`]).
///
/// # Errors
///
/// Returns `Error::Internal` if `parent_sha` cannot be resolved or the commit cannot be written.
pub fn commit_tree(worktree_path: &Path, parent_sha: &str, message: &str) -> Result<String> {
    let repo = Repository::open(worktree_path).map_err(map_git_err)?;
    let tree_oid = {
        let mut index = repo.index().map_err(map_git_err)?;
        index.write_tree().map_err(map_git_err)?
    };
    let tree = repo.find_tree(tree_oid).map_err(map_git_err)?;
    let parent_oid = Oid::from_str(parent_sha).map_err(map_git_err)?;
    let parent = repo.find_commit(parent_oid).map_err(map_git_err)?;
    let sig = repo.signature().map_err(map_git_err)?;
    let oid = repo
        .commit(None, &sig, &sig, message, &tree, &[&parent])
        .map_err(map_git_err)?;
    Ok(oid.to_string())
}

/// `git update-ref <refname> <sha>`: force-point `refname` (e.g.
/// `refs/heads/<trunk>`) at `sha`. Used for the local-only trunk advance.
///
/// # Errors
///
/// Returns `Error::Internal` if `sha` is invalid or the ref cannot be updated.
pub fn update_ref(worktree_path: &Path, refname: &str, sha: &str) -> Result<()> {
    let repo = Repository::open(worktree_path).map_err(map_git_err)?;
    let oid = Oid::from_str(sha).map_err(map_git_err)?;
    repo.reference(refname, oid, true, &format!("update {refname} to {sha}"))
        .map_err(map_git_err)?;
    Ok(())
}

#[cfg(test)]
mod tests {
    use super::*;
    use crate::testutil::{commit_file, init_repo};

    #[test]
    fn squash_commit_tree_and_update_ref() {
        let dir = init_repo("squash");
        commit_file(dir.path(), "a.txt", "one\n");
        let repo = Repository::open(dir.path()).unwrap();
        let first = repo.head().unwrap().target().unwrap().to_string();
        commit_file(dir.path(), "b.txt", "two\n");
        let head_tree = repo
            .head()
            .unwrap()
            .peel_to_commit()
            .unwrap()
            .tree_id()
            .to_string();

        // write-tree snapshots the current index (== HEAD tree here).
        let tree_sha = write_tree(dir.path()).unwrap();
        assert_eq!(tree_sha, head_tree);

        // commit-tree builds a squash commit on top of the first commit.
        let squash_sha = commit_tree(dir.path(), &first, "squash merge").unwrap();
        assert_eq!(squash_sha.len(), 40);
        let squash = repo
            .find_commit(Oid::from_str(&squash_sha).unwrap())
            .unwrap();
        assert_eq!(squash.tree_id().to_string(), tree_sha);
        assert_eq!(squash.parent_count(), 1);
        assert_eq!(squash.parent_id(0).unwrap().to_string(), first);

        // update-ref advances a (trunk) ref to the squash commit.
        update_ref(dir.path(), "refs/heads/squashed", &squash_sha).unwrap();
        let advanced = repo
            .find_reference("refs/heads/squashed")
            .unwrap()
            .target()
            .unwrap()
            .to_string();
        assert_eq!(advanced, squash_sha);
    }
}
