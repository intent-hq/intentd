//! Commit creation (`git.commit` / `git.agentCommit`).
//!
//! Ports `gitService.commit`: a commit is built from the current index
//! (already-staged changes) using the repository's configured identity, mirroring
//! `git commit -m <message>`. An empty commit (nothing staged) is rejected like
//! the TS "nothing to commit" path. The `agentCommit` auto-stage step computes
//! the set of changed paths via [`all_changed_paths`] (see the parity note in
//! `intent-services`: without the file-tracking attribution pipeline the set is
//! the whole worktree's changes rather than a single agent's).
//!
//! Note: libgit2 does not run git hooks, so the TS pre-commit-hook retry loop has
//! no analogue here.

use std::path::Path;

use git2::{Commit, Repository, Tree};
use intent_core::{Error, Result};

use crate::map_git_err;

/// The outcome of creating a commit: the new commit SHA and the files it changed.
#[derive(Debug, Clone)]
pub struct CommitOutcome {
    pub hash: String,
    pub files: Vec<String>,
}

/// Create a commit from the current index (already-staged changes), mirroring
/// `git commit -m <message>`. Errors when there is nothing staged to commit.
pub fn commit(worktree_path: &Path, message: &str) -> Result<CommitOutcome> {
    let repo = Repository::open(worktree_path).map_err(map_git_err)?;
    let mut index = repo.index().map_err(map_git_err)?;
    let tree_oid = index.write_tree().map_err(map_git_err)?;
    let tree = repo.find_tree(tree_oid).map_err(map_git_err)?;

    let parent = repo
        .head()
        .ok()
        .and_then(|h| h.target())
        .and_then(|oid| repo.find_commit(oid).ok());

    // Reject an empty commit, mirroring the TS "nothing to commit" failure.
    if let Some(parent) = &parent {
        if parent.tree_id() == tree_oid {
            return Err(Error::Internal(
                "nothing to commit, working tree clean".to_string(),
            ));
        }
    }

    let sig = repo.signature().map_err(map_git_err)?;
    let parents: Vec<&Commit> = parent.iter().collect();
    let oid = repo
        .commit(Some("HEAD"), &sig, &sig, message, &tree, &parents)
        .map_err(map_git_err)?;

    let files = changed_files(&repo, parent.as_ref(), &tree)?;
    Ok(CommitOutcome {
        hash: oid.to_string(),
        files,
    })
}

/// All distinct paths with worktree changes (staged, unstaged, or untracked),
/// the auto-stage set for `agentCommit` in the absence of agent attribution.
pub fn all_changed_paths(worktree_path: &Path) -> Result<Vec<String>> {
    let st = crate::status::status(worktree_path)?;
    let mut paths = Vec::new();
    for f in st.files {
        if !paths.contains(&f.path) {
            paths.push(f.path);
        }
    }
    Ok(paths)
}

/// The files changed between `parent`'s tree and `new_tree` (the committed delta),
/// mirroring `git diff-tree --no-commit-id --name-only -r <hash>`.
fn changed_files(
    repo: &Repository,
    parent: Option<&Commit>,
    new_tree: &Tree,
) -> Result<Vec<String>> {
    let parent_tree = match parent {
        Some(c) => Some(c.tree().map_err(map_git_err)?),
        None => None,
    };
    let diff = repo
        .diff_tree_to_tree(parent_tree.as_ref(), Some(new_tree), None)
        .map_err(map_git_err)?;
    let mut files = Vec::new();
    for delta in diff.deltas() {
        let path = delta.new_file().path().or_else(|| delta.old_file().path());
        if let Some(p) = path {
            let s = p.to_string_lossy().to_string();
            if !files.contains(&s) {
                files.push(s);
            }
        }
    }
    Ok(files)
}

#[cfg(test)]
mod tests {
    use super::*;
    use crate::stage::stage;
    use crate::testutil::{commit_file, init_repo, write_file};

    #[test]
    fn commits_staged_changes_and_reports_files() {
        let dir = init_repo("commit-basic");
        commit_file(dir.path(), "seed.txt", "seed\n");
        write_file(dir.path(), "a.txt", "hi\n");
        stage(dir.path(), &["a.txt".to_string()]).unwrap();
        let out = commit(dir.path(), "add a").unwrap();
        assert_eq!(out.hash.len(), 40);
        assert_eq!(out.files, vec!["a.txt".to_string()]);
    }

    #[test]
    fn empty_commit_is_rejected() {
        let dir = init_repo("commit-empty");
        commit_file(dir.path(), "seed.txt", "seed\n");
        let err = commit(dir.path(), "nothing").unwrap_err();
        assert!(format!("{err}").contains("nothing to commit"));
    }

    #[test]
    fn all_changed_paths_includes_untracked_and_modified() {
        let dir = init_repo("commit-changed");
        commit_file(dir.path(), "tracked.txt", "one\n");
        write_file(dir.path(), "tracked.txt", "two\n");
        write_file(dir.path(), "untracked.txt", "new\n");
        let mut paths = all_changed_paths(dir.path()).unwrap();
        paths.sort();
        assert_eq!(
            paths,
            vec!["tracked.txt".to_string(), "untracked.txt".to_string()]
        );
    }
}
