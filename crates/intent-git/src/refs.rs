//! Revision resolution + ancestry checks.
//!
//! Ports two read helpers the accept-changes handlers lean on: `git rev-parse
//! <refish>` (resolve a refish — `HEAD`, a branch, a remote-tracking ref, a SHA —
//! to its object SHA, e.g. capturing the trunk tip before a rebase) and `git
//! merge-base --is-ancestor <ancestor> <head>` (the force-push / orphaned-commit
//! reachability check). Both are side-effect free.

use std::path::Path;

use git2::Repository;
use intent_core::Result;

use crate::map_git_err;

/// `git rev-parse <refish>`: resolve `refish` to its 40-char object SHA. Accepts
/// anything libgit2 resolves (`HEAD`, a branch name, a remote-tracking ref, a
/// partial or full SHA).
pub fn rev_parse(worktree_path: &Path, refish: &str) -> Result<String> {
    let repo = Repository::open(worktree_path).map_err(map_git_err)?;
    let object = repo.revparse_single(refish).map_err(map_git_err)?;
    Ok(object.id().to_string())
}

/// `git merge-base --is-ancestor <ancestor_ref> <head_ref>`: whether `ancestor_ref`
/// is an ancestor of `head_ref`. A commit is its own ancestor (matching git's
/// exit-status semantics).
pub fn is_ancestor(worktree_path: &Path, ancestor_ref: &str, head_ref: &str) -> Result<bool> {
    let repo = Repository::open(worktree_path).map_err(map_git_err)?;
    let ancestor = repo.revparse_single(ancestor_ref).map_err(map_git_err)?;
    let head = repo.revparse_single(head_ref).map_err(map_git_err)?;
    let ancestor_oid = ancestor.peel_to_commit().map_err(map_git_err)?.id();
    let head_oid = head.peel_to_commit().map_err(map_git_err)?.id();
    if ancestor_oid == head_oid {
        return Ok(true);
    }
    repo.graph_descendant_of(head_oid, ancestor_oid)
        .map_err(map_git_err)
}

#[cfg(test)]
mod tests {
    use super::*;
    use crate::testutil::{commit_file, init_repo};

    #[test]
    fn rev_parse_resolves_head_to_sha() {
        let dir = init_repo("refs-revparse");
        commit_file(dir.path(), "a.txt", "x\n");
        let repo = Repository::open(dir.path()).unwrap();
        let head = repo.head().unwrap().target().unwrap().to_string();
        let resolved = rev_parse(dir.path(), "HEAD").unwrap();
        assert_eq!(resolved, head);
        assert_eq!(resolved.len(), 40);
    }

    #[test]
    fn is_ancestor_true_for_earlier_commit_and_self() {
        let dir = init_repo("refs-ancestor-true");
        commit_file(dir.path(), "a.txt", "one\n");
        let repo = Repository::open(dir.path()).unwrap();
        let first = repo.head().unwrap().target().unwrap().to_string();
        commit_file(dir.path(), "b.txt", "two\n");

        assert!(is_ancestor(dir.path(), &first, "HEAD").unwrap());
        // A commit is an ancestor of itself.
        assert!(is_ancestor(dir.path(), &first, &first).unwrap());
    }

    #[test]
    fn is_ancestor_false_for_later_commit() {
        let dir = init_repo("refs-ancestor-false");
        commit_file(dir.path(), "a.txt", "one\n");
        let repo = Repository::open(dir.path()).unwrap();
        let first = repo.head().unwrap().target().unwrap().to_string();
        commit_file(dir.path(), "b.txt", "two\n");

        // HEAD (later) is not an ancestor of the first commit.
        assert!(!is_ancestor(dir.path(), "HEAD", &first).unwrap());
    }
}
