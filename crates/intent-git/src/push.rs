//! Branch push (`accept-changes.execute` push step).
//!
//! Ports the `git push origin <branch>` half of the TS accept-changes pipeline.
//! libgit2 performs the push; for local/`file://` remotes (the test path) no
//! credentials are needed. For real remotes a best-effort credential callback is
//! installed (ssh-agent → credential helper → caller-resolved GitHub token for
//! HTTPS github.com remotes); the interactive keychain consent flow the TS
//! service drives is deferred (see the accept-changes parity notes in
//! `intent-services`).
//!
//! libgit2's `push` does not update local remote-tracking refs, so after a
//! successful push the local `refs/remotes/<remote>/<branch>` is fast-forwarded
//! to the pushed commit. This keeps the ahead/behind + `isPushed` reads
//! (`status`/`history`) consistent without a follow-up fetch.

use std::path::Path;

use git2::{PushOptions, Repository};
use intent_core::{Error, Result};

use crate::auth::remote_callbacks;
use crate::map_git_err;

/// The outcome of a push: the branch and the commit SHA now on the remote.
#[derive(Debug, Clone, PartialEq, Eq)]
pub struct PushOutcome {
    pub branch: String,
    pub pushed_sha: String,
}

/// Push `branch` to `remote` (typically `origin`). When `force` is set the
/// refspec is prefixed with `+` to allow a non-fast-forward update (mirroring the
/// TS `git push --force` path used after a rebase). `token` is an optional
/// caller-resolved GitHub token used as the final credential-chain step for
/// HTTPS github.com remotes (see [`crate::auth`]). Errors when the branch has no
/// local commit or the remote rejects the push.
pub fn push(
    worktree_path: &Path,
    remote: &str,
    branch: &str,
    force: bool,
    token: Option<&str>,
) -> Result<PushOutcome> {
    if branch.is_empty() {
        return Err(Error::Internal(
            "cannot push: empty branch name".to_string(),
        ));
    }
    let repo = Repository::open(worktree_path).map_err(map_git_err)?;

    let local_ref = format!("refs/heads/{branch}");
    let pushed_sha = repo
        .find_reference(&local_ref)
        .ok()
        .and_then(|r| r.target())
        .ok_or_else(|| Error::Internal(format!("cannot push: branch {branch} has no commit")))?
        .to_string();

    let mut remote_handle = repo.find_remote(remote).map_err(map_git_err)?;

    let mut opts = PushOptions::new();
    opts.remote_callbacks(remote_callbacks(token));

    let prefix = if force { "+" } else { "" };
    let refspec = format!("{prefix}{local_ref}:{local_ref}");
    remote_handle
        .push(&[refspec.as_str()], Some(&mut opts))
        .map_err(map_git_err)?;

    // libgit2 leaves the local remote-tracking ref untouched; advance it so the
    // ahead/behind + isPushed reads see the branch as pushed.
    let tracking_ref = format!("refs/remotes/{remote}/{branch}");
    if let Ok(oid) = git2::Oid::from_str(&pushed_sha) {
        let _ = repo.reference(
            &tracking_ref,
            oid,
            true,
            &format!("update {tracking_ref} after push"),
        );
    }

    Ok(PushOutcome {
        branch: branch.to_string(),
        pushed_sha,
    })
}

/// Push an arbitrary `src` revision (a SHA, `HEAD`, or branch name) to `remote`'s
/// `refs/heads/<dst_branch>`, returning the pushed commit SHA. When `force` is set
/// a non-fast-forward update is allowed (the `+` refspec prefix). Backs the
/// accept-changes paths that push a commit other than the local branch tip: the
/// `undo-push` rewind (`<sha>:refs/heads/<branch>`) and the remote `merge` trunk
/// advance (`HEAD`/`<sha>:refs/heads/<trunk>`).
///
/// libgit2 resolves the local side of a push refspec as a reference (there is no
/// `git push <sha>:<dst>` shortcut, nor `--force-with-lease`), so `src` is first
/// resolved to its commit OID, written to a short-lived temporary ref that is
/// deleted once the push returns, and pushed with a plain force when requested.
/// `token` is an optional caller-resolved GitHub token used as the final
/// credential-chain step for HTTPS github.com remotes (see [`crate::auth`]).
pub fn push_refspec(
    worktree_path: &Path,
    remote: &str,
    src: &str,
    dst_branch: &str,
    force: bool,
    token: Option<&str>,
) -> Result<String> {
    if dst_branch.is_empty() {
        return Err(Error::Internal(
            "cannot push: empty destination branch".to_string(),
        ));
    }
    let repo = Repository::open(worktree_path).map_err(map_git_err)?;
    let oid = repo
        .revparse_single(src)
        .map_err(map_git_err)?
        .peel_to_commit()
        .map_err(map_git_err)?
        .id();

    let tmp_ref = format!(
        "refs/intent/tmp-push-{}",
        std::time::SystemTime::now()
            .duration_since(std::time::UNIX_EPOCH)
            .map_or(0, |d| d.as_nanos())
    );
    repo.reference(&tmp_ref, oid, true, "intent push-refspec temp")
        .map_err(map_git_err)?;

    let mut remote_handle = repo.find_remote(remote).map_err(map_git_err)?;
    let mut opts = PushOptions::new();
    opts.remote_callbacks(remote_callbacks(token));
    let prefix = if force { "+" } else { "" };
    let refspec = format!("{prefix}{tmp_ref}:refs/heads/{dst_branch}");
    let push_result = remote_handle.push(&[refspec.as_str()], Some(&mut opts));

    // Always remove the temporary ref, regardless of the push outcome.
    if let Ok(mut r) = repo.find_reference(&tmp_ref) {
        let _ = r.delete();
    }
    push_result.map_err(map_git_err)?;

    // Advance the local remote-tracking ref so ahead/behind + isPushed reads see
    // the new remote position without a follow-up fetch.
    let tracking_ref = format!("refs/remotes/{remote}/{dst_branch}");
    let _ = repo.reference(
        &tracking_ref,
        oid,
        true,
        &format!("update {tracking_ref} after push"),
    );

    Ok(oid.to_string())
}

#[cfg(test)]
mod tests {
    use super::*;
    use crate::testutil::{commit_file, init_repo};

    /// Push to a local bare remote and confirm the bare repo now carries the
    /// commit and the local tracking ref was advanced.
    #[test]
    fn pushes_branch_to_local_bare_remote() {
        let dir = init_repo("push-src");
        commit_file(dir.path(), "a.txt", "one\n");
        let repo = Repository::open(dir.path()).unwrap();
        let branch = crate::status::current_branch(&repo);

        let bare_dir = std::env::temp_dir().join(format!(
            "intent-git-bare-{}",
            std::time::SystemTime::now()
                .duration_since(std::time::UNIX_EPOCH)
                .unwrap()
                .as_nanos()
        ));
        Repository::init_bare(&bare_dir).unwrap();
        repo.remote("origin", bare_dir.to_str().unwrap()).unwrap();

        let out = push(dir.path(), "origin", &branch, false, None).unwrap();
        assert_eq!(out.branch, branch);
        assert_eq!(out.pushed_sha.len(), 40);

        // The bare remote now has the branch at the pushed sha.
        let bare = Repository::open_bare(&bare_dir).unwrap();
        let remote_oid = bare
            .find_reference(&format!("refs/heads/{branch}"))
            .unwrap()
            .target()
            .unwrap()
            .to_string();
        assert_eq!(remote_oid, out.pushed_sha);

        // The local tracking ref was advanced so isPushed reads see it.
        let tracking = repo
            .find_reference(&format!("refs/remotes/origin/{branch}"))
            .unwrap()
            .target()
            .unwrap()
            .to_string();
        assert_eq!(tracking, out.pushed_sha);

        let _ = std::fs::remove_dir_all(&bare_dir);
    }

    #[test]
    fn empty_branch_is_rejected() {
        let dir = init_repo("push-empty-branch");
        commit_file(dir.path(), "a.txt", "x\n");
        assert!(push(dir.path(), "origin", "", false, None).is_err());
    }

    /// Force-push an earlier commit SHA onto a branch (the `undo-push` shape):
    /// the bare remote rewinds to the older commit even though it is not the
    /// local branch tip.
    #[test]
    fn push_refspec_force_rewinds_remote_branch() {
        let dir = init_repo("push-refspec-src");
        commit_file(dir.path(), "a.txt", "one\n");
        let repo = Repository::open(dir.path()).unwrap();
        let branch = crate::status::current_branch(&repo);
        let first = repo.head().unwrap().target().unwrap().to_string();
        commit_file(dir.path(), "b.txt", "two\n");

        let bare_dir = std::env::temp_dir().join(format!(
            "intent-git-refspec-bare-{}",
            std::time::SystemTime::now()
                .duration_since(std::time::UNIX_EPOCH)
                .unwrap()
                .as_nanos()
        ));
        Repository::init_bare(&bare_dir).unwrap();
        repo.remote("origin", bare_dir.to_str().unwrap()).unwrap();
        // The remote starts at the latest commit.
        let latest = push(dir.path(), "origin", &branch, false, None)
            .unwrap()
            .pushed_sha;

        // Rewind the remote branch back to the first commit (force).
        let pushed = push_refspec(dir.path(), "origin", &first, &branch, true, None).unwrap();
        assert_eq!(pushed, first);
        assert_ne!(pushed, latest);

        let bare = Repository::open_bare(&bare_dir).unwrap();
        let remote_oid = bare
            .find_reference(&format!("refs/heads/{branch}"))
            .unwrap()
            .target()
            .unwrap()
            .to_string();
        assert_eq!(remote_oid, first);

        // The local tracking ref reflects the rewound remote position.
        let tracking = repo
            .find_reference(&format!("refs/remotes/origin/{branch}"))
            .unwrap()
            .target()
            .unwrap()
            .to_string();
        assert_eq!(tracking, first);

        let _ = std::fs::remove_dir_all(&bare_dir);
    }
}
