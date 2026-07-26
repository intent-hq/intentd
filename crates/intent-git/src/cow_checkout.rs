//! Standalone CoW checkout provisioning for `workspace.create` (§5.1).
//!
//! [`provision_cow_checkout`] is the copy-on-write counterpart of
//! [`crate::worktree::provision_worktree`]: instead of a linked worktree it
//! `cow_clone`s the whole repository directory (deps/build artifacts included),
//! then inside the clone creates + checks out the workspace branch from
//! `base_ref` and hard-resets tracked files to that base. Untracked files are
//! deliberately preserved — carrying `node_modules`/`target`-style artifacts
//! into the checkout for free is the point of CoW.

use std::path::Path;

use git2::{BranchType, Repository};
use intent_core::{Error, Result};

use crate::cow::cow_clone;
use crate::map_git_err;

/// Provision a standalone CoW checkout: clone `repo_path` to `checkout_path`
/// with copy-on-write, then in the clone create `branch` from `base_ref`
/// (resolution order matches `provision_worktree`:
/// `refs/remotes/{remote}/{base_ref}` → `refs/heads/{base_ref}` → any
/// rev-parsable spec; no `base_ref` means HEAD), check it out, and hard-reset
/// tracked files to that base while preserving untracked files. An existing
/// branch of the same name is reused rather than recreated. Returns the SHA of
/// the commit the checkout lands on. On failure after the clone, the partially
/// provisioned `checkout_path` is removed best-effort.
///
/// A `repo_path` that is itself a linked git worktree (its `.git` is a
/// gitfile) is refused with `Error::Unsupported`: the cloned gitfile would
/// still point into the ORIGINAL repository's `.git/worktrees/<name>`, so the
/// branch switch + hard reset below would rewrite the user's source checkout.
/// Callers route such repos to linked-worktree provisioning instead.
pub fn provision_cow_checkout(
    repo_path: &Path,
    checkout_path: &Path,
    branch: &str,
    base_ref: Option<&str>,
    remote: &str,
) -> Result<String> {
    if repo_path.join(".git").is_file() {
        return Err(Error::Unsupported(format!(
            "repository at {} is a linked git worktree (gitfile .git); CoW-cloning it would corrupt the source checkout",
            repo_path.display()
        )));
    }
    if let Some(parent) = checkout_path.parent() {
        std::fs::create_dir_all(parent)
            .map_err(|e| Error::Internal(format!("cannot create checkout parent dir: {e}")))?;
    }
    cow_clone(repo_path, checkout_path)?;
    strip_worktree_registrations(checkout_path)
        .and_then(|()| checkout_in_clone(checkout_path, branch, base_ref, remote))
        .inspect_err(|_| {
            let _ = std::fs::remove_dir_all(checkout_path);
        })
}

/// A cloned main repository carries the source's `.git/worktrees/<name>/`
/// registrations, which point at the ORIGINAL repo's working trees: they make
/// the clone refuse to check out branches "already checked out" in the
/// source's linked worktrees, and pruning them from the clone could touch the
/// original's trees. Remove them from the clone before branching/checkout.
/// The source repository is never modified.
fn strip_worktree_registrations(checkout_path: &Path) -> Result<()> {
    let worktrees_dir = checkout_path.join(".git").join("worktrees");
    if worktrees_dir.is_dir() {
        std::fs::remove_dir_all(&worktrees_dir).map_err(|e| {
            Error::Internal(format!(
                "cannot remove stale worktree registrations from clone: {e}"
            ))
        })?;
        tracing::debug!(
            checkout = %checkout_path.display(),
            "provision_cow_checkout: removed stale .git/worktrees registrations from clone"
        );
    }
    Ok(())
}

/// Branch + checkout + hard reset inside the freshly cloned repository.
fn checkout_in_clone(
    checkout_path: &Path,
    branch: &str,
    base_ref: Option<&str>,
    remote: &str,
) -> Result<String> {
    let repo = Repository::open(checkout_path).map_err(map_git_err)?;

    // Resolve the base commit in the clone (a full copy of the source repo's
    // refs): remote-tracking ref, then local branch, then any rev-parsable
    // spec (tag/SHA); no base_ref means HEAD.
    let base_commit = match base_ref.filter(|r| !r.is_empty()) {
        Some(r) => [
            format!("refs/remotes/{remote}/{r}"),
            format!("refs/heads/{r}"),
            r.to_string(),
        ]
        .iter()
        .find_map(|spec| repo.revparse_single(spec).ok())
        .and_then(|obj| obj.peel_to_commit().ok())
        .ok_or_else(|| Error::BaseRefUnresolvable {
            base_ref: r.to_string(),
        })?,
        None => repo
            .head()
            .and_then(|h| h.peel_to_commit())
            .map_err(map_git_err)?,
    };

    // Create the branch at the base commit, or reuse an existing branch of
    // the same name (provision_worktree parity).
    let branch_ref = match repo.find_branch(branch, BranchType::Local) {
        Ok(b) => b.into_reference(),
        Err(_) => repo
            .branch(branch, &base_commit, false)
            .map_err(map_git_err)?
            .into_reference(),
    };
    let target = branch_ref.peel_to_commit().map_err(map_git_err)?;
    let checked_out_sha = target.id().to_string();

    // Point HEAD at the branch, then hard-reset tracked files to its commit.
    // A hard reset discards tracked modifications carried over from the
    // source working tree but leaves untracked files in place.
    let refname = format!("refs/heads/{branch}");
    repo.set_head(&refname).map_err(map_git_err)?;
    repo.reset(target.as_object(), git2::ResetType::Hard, None)
        .map_err(map_git_err)?;
    Ok(checked_out_sha)
}

#[cfg(test)]
mod tests {
    use super::*;
    use crate::cow::{cow_probe, CowSupport};
    use crate::testutil::{commit_file, init_repo, write_file, TempDir};

    /// Skip guard: CoW cloning depends on the filesystem under `TMPDIR` (CI
    /// may run on non-CoW filesystems). Returns `true` when supported.
    fn cow_available(src: &std::path::Path) -> bool {
        let dst = std::env::temp_dir();
        match cow_probe(src, &dst) {
            Ok(CowSupport::Supported) => true,
            _ => {
                eprintln!("Skipping test: CoW not supported under {dst:?}");
                false
            }
        }
    }

    fn unique_checkout(tag: &str) -> std::path::PathBuf {
        let nanos = std::time::SystemTime::now()
            .duration_since(std::time::UNIX_EPOCH)
            .unwrap()
            .as_nanos();
        std::env::temp_dir().join(format!("cow-checkout-{tag}-{nanos}"))
    }

    /// Drop guard for a provisioned checkout directory.
    struct Cleanup(std::path::PathBuf);
    impl Drop for Cleanup {
        fn drop(&mut self) {
            let _ = std::fs::remove_dir_all(&self.0);
        }
    }

    fn head_sha(dir: &TempDir) -> String {
        let repo = Repository::open(dir.path()).unwrap();
        let sha = repo.head().unwrap().target().unwrap().to_string();
        sha
    }

    fn head_branch(dir: &TempDir) -> String {
        let repo = Repository::open(dir.path()).unwrap();
        let name = repo.head().unwrap().shorthand().unwrap().to_string();
        name
    }

    #[test]
    fn provisions_standalone_checkout_on_new_branch_from_base_ref() {
        let dir = init_repo("cowchk-base");
        commit_file(dir.path(), "a.txt", "x\n");
        if !cow_available(dir.path()) {
            return;
        }
        // Pin `base` at the first commit, then advance HEAD past it.
        let base_sha = head_sha(&dir);
        crate::testutil::create_branch(dir.path(), "base");
        commit_file(dir.path(), "b.txt", "y\n");

        let checkout = unique_checkout("base");
        let _cleanup = Cleanup(checkout.clone());
        let sha = provision_cow_checkout(dir.path(), &checkout, "cow-ws", Some("base"), "origin")
            .unwrap();
        assert_eq!(sha, base_sha);

        let clone = Repository::open(&checkout).unwrap();
        assert!(!clone.is_worktree(), "CoW checkout is a standalone repo");
        let head = clone.head().unwrap();
        assert_eq!(head.shorthand().expect("branch name"), "cow-ws");
        assert_eq!(head.target().unwrap().to_string(), base_sha);
        // Tracked files match the base commit, not the source HEAD.
        assert_eq!(
            std::fs::read_to_string(checkout.join("a.txt")).unwrap(),
            "x\n"
        );
        assert!(!checkout.join("b.txt").exists());
    }

    #[test]
    fn preserves_untracked_files_and_resets_dirty_tracked_files() {
        let dir = init_repo("cowchk-dirty");
        commit_file(dir.path(), "a.txt", "x\n");
        if !cow_available(dir.path()) {
            return;
        }
        let branch = head_branch(&dir);
        // Dirty the tracked file and add an untracked build artifact.
        write_file(dir.path(), "a.txt", "dirty\n");
        write_file(dir.path(), "target/build.log", "artifact\n");

        let checkout = unique_checkout("dirty");
        let _cleanup = Cleanup(checkout.clone());
        provision_cow_checkout(dir.path(), &checkout, "cow-ws", Some(&branch), "origin").unwrap();

        // Tracked file is reset to the base commit; untracked artifact survives.
        assert_eq!(
            std::fs::read_to_string(checkout.join("a.txt")).unwrap(),
            "x\n"
        );
        assert_eq!(
            std::fs::read_to_string(checkout.join("target/build.log")).unwrap(),
            "artifact\n"
        );
        // The source repo is untouched (still dirty).
        assert_eq!(
            std::fs::read_to_string(dir.path().join("a.txt")).unwrap(),
            "dirty\n"
        );
    }

    #[test]
    fn reuses_existing_branch_of_same_name() {
        let dir = init_repo("cowchk-reuse");
        commit_file(dir.path(), "a.txt", "x\n");
        if !cow_available(dir.path()) {
            return;
        }
        // `cow-ws` pinned at the first commit; HEAD advances past it.
        let pinned_sha = head_sha(&dir);
        crate::testutil::create_branch(dir.path(), "cow-ws");
        commit_file(dir.path(), "b.txt", "y\n");
        let base = head_branch(&dir);

        let checkout = unique_checkout("reuse");
        let _cleanup = Cleanup(checkout.clone());
        let sha =
            provision_cow_checkout(dir.path(), &checkout, "cow-ws", Some(&base), "origin").unwrap();
        assert_eq!(sha, pinned_sha, "existing branch is reused, not recreated");
    }

    #[test]
    fn refuses_linked_worktree_source_as_unsupported() {
        // Case A: the source repo is itself a linked git worktree (its `.git`
        // is a gitfile). CoW-cloning it would corrupt the original checkout,
        // so provisioning must refuse with Unsupported BEFORE cloning.
        let dir = init_repo("cowchk-gitfile");
        commit_file(dir.path(), "a.txt", "x\n");
        let branch = head_branch(&dir);

        // Create a linked worktree of the repo and use IT as the source.
        let wt_path = unique_checkout("gitfile-wt");
        let _wt_cleanup = Cleanup(wt_path.clone());
        crate::worktree::provision_worktree(
            dir.path(),
            "gitfile-wt",
            &wt_path,
            "wt-branch",
            Some(&branch),
            "origin",
        )
        .unwrap();
        assert!(wt_path.join(".git").is_file(), "worktree .git is a gitfile");

        let checkout = unique_checkout("gitfile-dst");
        let _cleanup = Cleanup(checkout.clone());
        let err = provision_cow_checkout(&wt_path, &checkout, "cow-ws", Some(&branch), "origin")
            .unwrap_err();
        assert!(matches!(err, Error::Unsupported(_)), "got: {err:?}");
        assert!(!checkout.exists(), "nothing is cloned for a gitfile source");
        // The source worktree is untouched.
        assert_eq!(
            std::fs::read_to_string(wt_path.join("a.txt")).unwrap(),
            "x\n"
        );
    }

    #[test]
    fn strips_stale_worktree_registrations_from_clone() {
        // Case B: the source main repo has linked worktrees; the clone
        // inherits `.git/worktrees/<name>` registrations pointing at the
        // ORIGINAL repo's trees. They must be stripped from the clone so it
        // can check out branches "already checked out" in the source's
        // worktrees — without modifying the source.
        let dir = init_repo("cowchk-strip");
        commit_file(dir.path(), "a.txt", "x\n");
        if !cow_available(dir.path()) {
            return;
        }
        let base = head_branch(&dir);

        // Register a linked worktree on branch `busy` in the source repo.
        let wt_path = unique_checkout("strip-wt");
        let _wt_cleanup = Cleanup(wt_path.clone());
        crate::worktree::provision_worktree(
            dir.path(),
            "strip-wt",
            &wt_path,
            "busy",
            Some(&base),
            "origin",
        )
        .unwrap();
        assert!(dir.path().join(".git/worktrees").is_dir());

        // Provision a CoW checkout ON the branch held by the source's linked
        // worktree; without the strip, git refuses ("already checked out").
        let checkout = unique_checkout("strip-dst");
        let _cleanup = Cleanup(checkout.clone());
        provision_cow_checkout(dir.path(), &checkout, "busy", Some(&base), "origin").unwrap();

        let clone = Repository::open(&checkout).unwrap();
        assert_eq!(clone.head().unwrap().shorthand().unwrap(), "busy");
        assert!(
            !checkout.join(".git/worktrees").exists(),
            "stale registrations are stripped from the clone"
        );
        // The source repo's registrations and worktree are untouched.
        assert!(dir.path().join(".git/worktrees").is_dir());
        assert!(wt_path.join(".git").is_file());
    }

    #[test]
    fn rejects_unresolvable_base_ref_and_cleans_up() {
        let dir = init_repo("cowchk-badref");
        commit_file(dir.path(), "a.txt", "x\n");
        if !cow_available(dir.path()) {
            return;
        }
        let checkout = unique_checkout("badref");
        let err = provision_cow_checkout(
            dir.path(),
            &checkout,
            "cow-ws",
            Some("no-such-ref"),
            "origin",
        )
        .unwrap_err();
        assert!(
            matches!(err, Error::BaseRefUnresolvable { ref base_ref } if base_ref == "no-such-ref")
        );
        assert!(!checkout.exists(), "partial checkout is removed on failure");
    }
}
