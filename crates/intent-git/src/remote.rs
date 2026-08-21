//! Remote inspection + setup for the accept-changes status/add-remote flows.
//!
//! Ports the read pieces of the TS `fetchWorkspaceGitStatus` that talk to the
//! remote layout (origin URL, the pushed-branch ref, ahead/behind vs trunk) plus
//! the `addRemote` auto-init path (`git init` + identity + initial commit +
//! `git remote add origin`). No network access: ahead/behind and `isPushed` use
//! the local `refs/remotes/origin/*` refs (advanced by [`crate::push`]); the
//! TS keychain/fetch consent flow is deferred (see the accept-changes parity
//! notes in `intent-services`).

use std::path::Path;

use git2::{Direction, Repository, RepositoryInitOptions};
use intent_core::Result;

use crate::auth::remote_callbacks;
use crate::map_git_err;

/// Whether a remote (reachable) carries a given branch — the `git ls-remote
/// --heads <remote> <branch>` probe. Distinguishes a branch that is simply
/// absent from a remote that could not be reached (see [`ls_remote_has_branch`]).
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub enum RemoteBranch {
    /// The remote is reachable and advertises `refs/heads/<branch>`.
    Present,
    /// The remote is reachable but does not have the branch (local-only path).
    Missing,
}

/// `git ls-remote --heads <remote> <branch>`: connect to `remote` and report
/// whether it advertises `refs/heads/<branch>`. A reachable remote yields
/// [`RemoteBranch::Present`]/[`RemoteBranch::Missing`]; an unreachable remote
/// (network/auth failure) is surfaced as an `Err`, preserving the TS distinction
/// between "branch missing → local-only" and "remote unreachable → error".
/// `token` is an optional caller-resolved GitHub token used as the final
/// credential-chain step for HTTPS github.com remotes (see [`crate::auth`]).
///
/// # Errors
///
/// Returns `Error::Internal` if the remote is missing or the ls-remote fails.
pub fn ls_remote_has_branch(
    worktree_path: &Path,
    remote: &str,
    branch: &str,
    token: Option<&str>,
) -> Result<RemoteBranch> {
    let repo = Repository::open(worktree_path).map_err(map_git_err)?;
    let mut remote_handle = repo.find_remote(remote).map_err(map_git_err)?;
    let connection = remote_handle
        .connect_auth(Direction::Fetch, Some(remote_callbacks(token)), None)
        .map_err(map_git_err)?;
    let target = format!("refs/heads/{branch}");
    let present = connection
        .list()
        .map_err(map_git_err)?
        .iter()
        .any(|head| head.name() == target);
    Ok(if present {
        RemoteBranch::Present
    } else {
        RemoteBranch::Missing
    })
}

/// The `origin` remote URL, or `None` when the workspace has no `origin`.
///
/// # Errors
///
/// Returns `Error::Internal` if the underlying libgit2 operation fails.
pub fn origin_url(worktree_path: &Path) -> Result<Option<String>> {
    remote_url(worktree_path, "origin")
}

/// The URL of the named `remote`, or `None` when the repository has no such
/// remote (or the remote's URL is not valid UTF-8). Ports the FE
/// `git-tracking:get-remote-url` handler / `getRemoteUrl` helper (which shells
/// `git -C <path> config --get remote.<name>.url` and folds missing to null).
///
/// # Errors
///
/// Returns `Error::Internal` if the underlying libgit2 operation fails.
pub fn remote_url(worktree_path: &Path, remote: &str) -> Result<Option<String>> {
    let repo = Repository::open(worktree_path).map_err(map_git_err)?;
    let url = match repo.find_remote(remote) {
        Ok(handle) => handle.url().ok().map(str::to_string),
        Err(_) => None,
    };
    Ok(url)
}

/// Whether the local tracking ref `refs/remotes/<remote>/<branch>` exists — the
/// `isPushed` signal (the branch has been pushed at least once).
///
/// # Errors
///
/// Returns `Error::Internal` if the underlying libgit2 operation fails.
pub fn remote_tracking_exists(worktree_path: &Path, remote: &str, branch: &str) -> Result<bool> {
    if branch.is_empty() {
        return Ok(false);
    }
    let repo = Repository::open(worktree_path).map_err(map_git_err)?;
    let refname = format!("refs/remotes/{remote}/{branch}");
    let exists = repo.find_reference(&refname).is_ok();
    Ok(exists)
}

/// Ahead/behind commit counts of the current `HEAD` vs `base_ref` (the trunk),
/// as `(ahead_of_trunk, behind_trunk)`. Returns `(0, 0)` when either ref cannot
/// be resolved (e.g. trunk not present locally) — the TS `|| 0` fallback.
///
/// # Errors
///
/// Returns `Error::Internal` if `base_ref` or `HEAD` cannot be resolved or the ahead/behind computation fails.
pub fn ahead_behind(worktree_path: &Path, base_ref: &str) -> Result<(i64, i64)> {
    let repo = Repository::open(worktree_path).map_err(map_git_err)?;
    let Some(head_oid) = repo.head().ok().and_then(|h| h.target()) else {
        return Ok((0, 0));
    };
    let Ok(base_obj) = repo.revparse_single(base_ref) else {
        return Ok((0, 0));
    };
    match repo.graph_ahead_behind(head_oid, base_obj.id()) {
        Ok((ahead, behind)) => Ok((ahead as i64, behind as i64)),
        Err(_) => Ok((0, 0)),
    }
}

/// Retarget an existing remote at `url` (`git remote set-url` parity). Errors
/// when the repository cannot be opened or the remote does not exist.
///
/// # Errors
///
/// Returns `Error::Internal` if the underlying libgit2 operation fails.
pub fn set_remote_url(worktree_path: &Path, remote: &str, url: &str) -> Result<()> {
    let repo = Repository::open(worktree_path).map_err(map_git_err)?;
    repo.remote_set_url(remote, url).map_err(map_git_err)
}

/// Add the `origin` remote pointing at `url`, initializing a git repository at
/// `worktree_path` first when it is not already one (ports the TS `addRemote`
/// auto-init: `git init -b main`, configure identity, an initial empty commit,
/// then rename to `desired_branch`). Errors if `origin` already exists.
///
/// # Errors
///
/// Returns `Error::Internal` if the underlying libgit2 operation fails.
pub fn add_origin(worktree_path: &Path, url: &str, desired_branch: &str) -> Result<()> {
    let repo = match Repository::open(worktree_path) {
        Ok(repo) => repo,
        Err(_) => init_repo(worktree_path, desired_branch)?,
    };
    repo.remote("origin", url).map_err(map_git_err)?;
    Ok(())
}

/// Initialize a repository with an initial empty commit on `main`, renamed to
/// `desired_branch` when that differs. Identity falls back to the TS defaults
/// (`Intent <intent@local>`) when no git identity is configured.
fn init_repo(worktree_path: &Path, desired_branch: &str) -> Result<Repository> {
    let mut opts = RepositoryInitOptions::new();
    opts.initial_head("main");
    let repo = Repository::init_opts(worktree_path, &opts).map_err(map_git_err)?;

    let (name, email) = configured_identity(&repo);
    let sig = git2::Signature::now(&name, &email).map_err(map_git_err)?;
    {
        let tree_oid = {
            let mut index = repo.index().map_err(map_git_err)?;
            index.write_tree().map_err(map_git_err)?
        };
        let tree = repo.find_tree(tree_oid).map_err(map_git_err)?;
        repo.commit(Some("HEAD"), &sig, &sig, "Initial commit", &tree, &[])
            .map_err(map_git_err)?;
    }

    if desired_branch != "main" && !desired_branch.is_empty() {
        if let Ok(mut branch) = repo.find_branch("main", git2::BranchType::Local) {
            branch
                .rename(desired_branch, true)
                .map_err(map_git_err)
                .map(|_| ())?;
            repo.set_head(&format!("refs/heads/{desired_branch}"))
                .map_err(map_git_err)?;
        }
    }
    Ok(repo)
}

/// Resolve the git identity from config, falling back to the TS defaults.
fn configured_identity(repo: &Repository) -> (String, String) {
    let config = repo.config().ok();
    let get = |key: &str| {
        config
            .as_ref()
            .and_then(|c| c.get_string(key).ok())
            .map(|s| s.trim().to_string())
            .filter(|s| !s.is_empty())
    };
    (
        get("user.name").unwrap_or_else(|| "Intent".to_string()),
        get("user.email").unwrap_or_else(|| "intent@local".to_string()),
    )
}

#[cfg(test)]
mod tests {
    use super::*;
    use crate::testutil::{commit_file, init_repo as test_init_repo};

    #[test]
    fn origin_url_none_without_remote() {
        let dir = test_init_repo("remote-none");
        commit_file(dir.path(), "a.txt", "x\n");
        assert!(origin_url(dir.path()).unwrap().is_none());
    }

    #[test]
    fn remote_url_reads_named_remote_and_none_for_missing() {
        let dir = test_init_repo("remote-url-named");
        commit_file(dir.path(), "a.txt", "x\n");
        let repo = Repository::open(dir.path()).unwrap();
        repo.remote("upstream", "https://github.com/o/upstream.git")
            .unwrap();
        assert_eq!(
            remote_url(dir.path(), "upstream").unwrap().as_deref(),
            Some("https://github.com/o/upstream.git")
        );
        // Missing remote is null, not an error (FE `getRemoteUrl` parity).
        assert!(remote_url(dir.path(), "no-such-remote").unwrap().is_none());
    }

    #[test]
    fn add_origin_initializes_non_repo_and_sets_url() {
        let dir = std::env::temp_dir().join(format!(
            "intent-git-addremote-{}",
            std::time::SystemTime::now()
                .duration_since(std::time::UNIX_EPOCH)
                .unwrap()
                .as_nanos()
        ));
        std::fs::create_dir_all(&dir).unwrap();
        add_origin(&dir, "https://github.com/o/r.git", "feature").unwrap();
        assert_eq!(
            origin_url(&dir).unwrap().as_deref(),
            Some("https://github.com/o/r.git")
        );
        let repo = Repository::open(&dir).unwrap();
        assert_eq!(crate::status::current_branch(&repo), "feature");
        let _ = std::fs::remove_dir_all(&dir);
    }

    fn unique_bare(tag: &str) -> std::path::PathBuf {
        std::env::temp_dir().join(format!(
            "intent-git-{tag}-{}",
            std::time::SystemTime::now()
                .duration_since(std::time::UNIX_EPOCH)
                .unwrap()
                .as_nanos()
        ))
    }

    #[test]
    fn ls_remote_present_and_missing_against_reachable_bare() {
        let src = test_init_repo("lsremote-src");
        commit_file(src.path(), "a.txt", "one\n");
        let repo = Repository::open(src.path()).unwrap();
        let branch = crate::status::current_branch(&repo);

        let bare_dir = unique_bare("lsremote-bare");
        Repository::init_bare(&bare_dir).unwrap();
        repo.remote("origin", bare_dir.to_str().unwrap()).unwrap();
        crate::push::push(src.path(), "origin", &branch, false, None).unwrap();

        // The pushed branch is advertised; a never-pushed branch is missing.
        assert_eq!(
            ls_remote_has_branch(src.path(), "origin", &branch, None).unwrap(),
            RemoteBranch::Present
        );
        assert_eq!(
            ls_remote_has_branch(src.path(), "origin", "no-such-branch", None).unwrap(),
            RemoteBranch::Missing
        );

        let _ = std::fs::remove_dir_all(&bare_dir);
    }

    #[test]
    fn ls_remote_unreachable_is_error() {
        let src = test_init_repo("lsremote-unreachable");
        commit_file(src.path(), "a.txt", "one\n");
        let repo = Repository::open(src.path()).unwrap();
        // Point origin at a path that does not exist — connecting must fail.
        let missing = unique_bare("lsremote-missing-remote");
        repo.remote("origin", missing.to_str().unwrap()).unwrap();
        assert!(ls_remote_has_branch(src.path(), "origin", "main", None).is_err());
    }

    #[test]
    fn ahead_behind_counts_commits_past_base() {
        let dir = test_init_repo("remote-ahead");
        commit_file(dir.path(), "a.txt", "1\n");
        // Tag the base, then add two more commits.
        let repo = Repository::open(dir.path()).unwrap();
        let base = repo.head().unwrap().target().unwrap();
        repo.reference("refs/tags/base", base, true, "base")
            .unwrap();
        commit_file(dir.path(), "b.txt", "2\n");
        commit_file(dir.path(), "c.txt", "3\n");
        let (ahead, behind) = ahead_behind(dir.path(), "refs/tags/base").unwrap();
        assert_eq!((ahead, behind), (2, 0));
    }
}
