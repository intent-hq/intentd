//! Single-branch fetch (`git fetch <remote> <branch>`).
//!
//! Ports the `git fetch origin <trunk>` step the accept-changes merge/reset/rebase
//! handlers run before comparing against trunk. libgit2 performs the fetch with the
//! shared credential callback ([`crate::auth`]); the explicit refspec updates the
//! local remote-tracking ref `refs/remotes/<remote>/<branch>` so the downstream
//! ahead/behind + `isPushed` reads stay consistent. Local/`file://` remotes (the
//! test path) need no credentials.

use std::path::Path;

use git2::{FetchOptions, Repository};
use intent_core::{Error, Result};

use crate::auth::remote_callbacks;
use crate::map_git_err;

/// Fetch a single `branch` from `remote` (typically `origin`), updating the local
/// remote-tracking ref `refs/remotes/<remote>/<branch>`. Errors when the branch
/// name is empty or the remote is unreachable.
pub fn fetch(worktree_path: &Path, remote: &str, branch: &str) -> Result<()> {
    if branch.is_empty() {
        return Err(Error::Internal(
            "cannot fetch: empty branch name".to_string(),
        ));
    }
    let repo = Repository::open(worktree_path).map_err(map_git_err)?;
    let mut remote_handle = repo.find_remote(remote).map_err(map_git_err)?;

    let mut opts = FetchOptions::new();
    opts.remote_callbacks(remote_callbacks());

    // Explicit refspec so the remote-tracking ref is written even when the remote
    // is not configured with a default fetch refspec.
    let refspec = format!("+refs/heads/{branch}:refs/remotes/{remote}/{branch}");
    remote_handle
        .fetch(&[refspec.as_str()], Some(&mut opts), None)
        .map_err(map_git_err)?;
    Ok(())
}

#[cfg(test)]
mod tests {
    use super::*;
    use crate::testutil::{commit_file, init_repo};

    /// Fetch a branch from a local bare remote and confirm the local
    /// remote-tracking ref now points at the remote commit.
    #[test]
    fn fetch_updates_tracking_ref() {
        // Seed a source repo and push it into a bare remote to act as origin.
        let src = init_repo("fetch-src");
        commit_file(src.path(), "a.txt", "one\n");
        let src_repo = Repository::open(src.path()).unwrap();
        let branch = crate::status::current_branch(&src_repo);

        let bare_dir = std::env::temp_dir().join(format!(
            "intent-git-fetch-bare-{}",
            std::time::SystemTime::now()
                .duration_since(std::time::UNIX_EPOCH)
                .unwrap()
                .as_nanos()
        ));
        Repository::init_bare(&bare_dir).unwrap();
        src_repo
            .remote("origin", bare_dir.to_str().unwrap())
            .unwrap();
        crate::push::push(src.path(), "origin", &branch, false).unwrap();

        // A second clone-like repo points at the same bare remote and fetches.
        let consumer = init_repo("fetch-consumer");
        commit_file(consumer.path(), "seed.txt", "seed\n");
        let consumer_repo = Repository::open(consumer.path()).unwrap();
        consumer_repo
            .remote("origin", bare_dir.to_str().unwrap())
            .unwrap();

        fetch(consumer.path(), "origin", &branch).unwrap();

        let bare = Repository::open_bare(&bare_dir).unwrap();
        let remote_sha = bare
            .find_reference(&format!("refs/heads/{branch}"))
            .unwrap()
            .target()
            .unwrap()
            .to_string();
        let tracking = consumer_repo
            .find_reference(&format!("refs/remotes/origin/{branch}"))
            .unwrap()
            .target()
            .unwrap()
            .to_string();
        assert_eq!(tracking, remote_sha);

        let _ = std::fs::remove_dir_all(&bare_dir);
    }

    #[test]
    fn empty_branch_is_rejected() {
        let dir = init_repo("fetch-empty-branch");
        commit_file(dir.path(), "a.txt", "x\n");
        assert!(fetch(dir.path(), "origin", "").is_err());
    }
}
