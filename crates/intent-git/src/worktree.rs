//! Worktree create + lock (§9.5; internal — Cycle C consumes it).
//!
//! [`WorktreeLocks`] ports `withGitWorktreeLock`: an async mutex per
//! caller-provided key path — `intent-services` keys it by repository dir for
//! a per-repository lock — so concurrent agents/operations on the same keyed
//! path never corrupt the index. [`create_worktree`] wraps `git worktree add`;
//! [`remove_worktree`] ports the TS `removeGitWorktree` (registration prune +
//! directory removal), split into [`detach_worktree`] /
//! [`remove_detached_worktree`] so the recursive delete can run outside locks.

use std::collections::HashMap;
use std::path::{Path, PathBuf};
use std::sync::{Arc, Mutex};

use git2::{BranchType, Repository, WorktreeAddOptions, WorktreePruneOptions};
use intent_core::{Error, Result};
use tokio::sync::Mutex as AsyncMutex;

use crate::map_git_err;

/// An async-mutex map keyed by caller-provided path (e.g. the repository
/// directory for a per-repository lock). Cheap to clone (shared inner map).
/// Each distinct key path gets its own lock, so operations under different
/// keys never contend.
#[derive(Clone, Default)]
pub struct WorktreeLocks {
    locks: Arc<Mutex<HashMap<PathBuf, Arc<AsyncMutex<()>>>>>,
}

impl WorktreeLocks {
    /// Create an empty lock registry.
    pub fn new() -> Self {
        Self::default()
    }

    /// Resolve (or create) the lock for `path`.
    fn lock_for(&self, path: &Path) -> Arc<AsyncMutex<()>> {
        let mut map = self.locks.lock().expect("worktree lock map poisoned");
        map.entry(path.to_path_buf()).or_default().clone()
    }

    /// Run `f` while holding the per-worktree lock for `path`, mirroring
    /// `withGitWorktreeLock(worktreePath, fn)`.
    pub async fn with_lock<F, Fut, T>(&self, path: &Path, f: F) -> T
    where
        F: FnOnce() -> Fut,
        Fut: std::future::Future<Output = T>,
    {
        let lock = self.lock_for(path);
        let _guard = lock.lock().await;
        f().await
    }
}

/// Create a linked worktree named `name` at `worktree_path` for the repository at
/// `repo_path` (wraps `git worktree add`). The repository must have a commit.
#[cfg(test)]
pub(crate) fn create_worktree(repo_path: &Path, name: &str, worktree_path: &Path) -> Result<()> {
    let repo = Repository::open(repo_path).map_err(map_git_err)?;
    repo.worktree(name, worktree_path, None)
        .map_err(map_git_err)?;
    Ok(())
}

/// Provision a workspace worktree: create `branch` from `base_ref` (or HEAD)
/// if it does not exist, then add a linked worktree named `name` at
/// `worktree_path` checked out on that branch. Ports the FE
/// `createGitWorktree` happy path — stale registrations are pruned first, and
/// the base resolves preferring the remote-tracking ref
/// (`refs/remotes/{remote}/{base_ref}`) over the local branch so a fresh
/// workspace starts from the latest known remote state (no network fetch).
/// Returns the SHA of the commit the worktree is checked out at.
pub fn provision_worktree(
    repo_path: &Path,
    name: &str,
    worktree_path: &Path,
    branch: &str,
    base_ref: Option<&str>,
    remote: &str,
) -> Result<String> {
    let repo = Repository::open(repo_path).map_err(map_git_err)?;

    // Best-effort prune of stale registrations (ports `git worktree prune`
    // before creation) so an orphaned entry never blocks the add below.
    if let Ok(names) = repo.worktrees() {
        for i in 0..names.len() {
            let Ok(Some(n)) = names.get(i) else { continue };
            if let Ok(wt) = repo.find_worktree(n) {
                if wt.is_prunable(None).unwrap_or(false) {
                    let _ = wt.prune(None);
                }
            }
        }
    }

    // Resolve the base commit: remote-tracking ref, then local branch, then
    // any rev-parsable spec (tag/SHA); no base_ref means HEAD.
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

    // Create the branch at the base commit, or reuse an existing branch of the
    // same name (the TS flow reuses it rather than failing).
    let branch_ref = match repo.find_branch(branch, BranchType::Local) {
        Ok(b) => b.into_reference(),
        Err(_) => repo
            .branch(branch, &base_commit, false)
            .map_err(map_git_err)?
            .into_reference(),
    };
    let checked_out_sha = branch_ref
        .peel_to_commit()
        .map_err(map_git_err)?
        .id()
        .to_string();

    if let Some(parent) = worktree_path.parent() {
        std::fs::create_dir_all(parent)
            .map_err(|e| Error::Internal(format!("cannot create worktree parent dir: {e}")))?;
    }
    let mut opts = WorktreeAddOptions::new();
    opts.reference(Some(&branch_ref));
    repo.worktree(name, worktree_path, Some(&opts))
        .map_err(|e| map_worktree_add_err(e, branch))?;
    Ok(checked_out_sha)
}

/// Map a libgit2 worktree-add error into a domain error, classifying the
/// "branch already checked out" failure as InvalidParams with an actionable
/// message (PROTOCOL §9 `-32602`) instead of a generic Internal error.
fn map_worktree_add_err(e: git2::Error, branch: &str) -> Error {
    let msg = e.message();
    // libgit2 surfaces "branch '...' is already checked out" when the branch
    // is in use by another worktree (including the main working tree).
    if msg.contains("already checked out") {
        Error::InvalidParams(format!(
            "branch '{branch}' is already checked out in another worktree; choose a different branch or remove the conflicting worktree"
        ))
    } else {
        map_git_err(e)
    }
}

/// Whether `base_ref` resolves in the repository at `repo_path`, using the
/// exact 3-spec resolution [`provision_worktree`] applies at apply time
/// (`refs/remotes/{remote}/{r}` → `refs/heads/{r}` → any rev-parsable spec),
/// so propose-time and apply-time agree on what "resolvable" means. An empty
/// `base_ref` is `Ok(true)`: [`provision_worktree`] treats it as "no baseRef"
/// and falls back to HEAD, never an unresolvable ref. Read-only probe backing
/// the best-effort base-ref validation of chief workspace-create proposals
/// (monorepo#761).
pub fn base_ref_resolves(repo_path: &Path, base_ref: &str, remote: &str) -> Result<bool> {
    let repo = Repository::open(repo_path).map_err(map_git_err)?;
    if base_ref.is_empty() {
        return Ok(true);
    }
    let r = base_ref;
    let resolves = [
        format!("refs/remotes/{remote}/{r}"),
        format!("refs/heads/{r}"),
        r.to_string(),
    ]
    .iter()
    .find_map(|spec| repo.revparse_single(spec).ok())
    .and_then(|obj| obj.peel_to_commit().ok())
    .is_some();
    Ok(resolves)
}

/// The branch checked out in the worktree at `worktree_path` (ports the
/// `git rev-parse --abbrev-ref HEAD` probe in `removeGitWorktree`). `None` on
/// a detached HEAD or when the worktree cannot be opened (the TS flow treats
/// both as "could not determine branch" and skips branch cleanup).
pub fn worktree_branch(worktree_path: &Path) -> Option<String> {
    let repo = Repository::open(worktree_path).ok()?;
    let head = repo.head().ok()?;
    if !head.is_branch() {
        return None;
    }
    head.shorthand().ok().map(str::to_string)
}

/// Remove the linked worktree at `worktree_path`, porting the TS
/// `removeGitWorktree` removal sequence. Composed from the two-phase API —
/// [`detach_worktree`] (registration prune + rename to a trash path) followed
/// by [`remove_detached_worktree`] (recursive removal) — so callers that hold
/// a per-repo lock can run the phases separately and keep the expensive
/// recursive delete outside the lock.
#[cfg(test)]
pub(crate) fn remove_worktree(repo_path: &Path, worktree_path: &Path) -> Result<()> {
    if let Some(trash) = detach_worktree(repo_path, worktree_path)? {
        remove_detached_worktree(&trash)?;
    }
    Ok(())
}

/// Phase 1 of worktree removal — cheap git-metadata work that is safe to run
/// under a per-repo lock: prune the worktree registration (metadata only —
/// libgit2's working-tree delete is deliberately not requested), rename the
/// working directory to a unique sibling trash path so the potentially
/// multi-GB recursive removal can happen later via
/// [`remove_detached_worktree`], outside the lock, then best-effort prune any
/// remaining stale registrations (including this worktree's own entry when
/// the path comparison missed — e.g. symlinked temp dirs — since its
/// directory is now gone). Returns the trash path awaiting removal, or
/// `None` when the directory was already gone or had to be removed in place
/// (rename fallback, e.g. permissions or directory busy). Idempotent: a
/// missing directory or registration is `Ok(None)`.
pub fn detach_worktree(repo_path: &Path, worktree_path: &Path) -> Result<Option<PathBuf>> {
    let repo = Repository::open(repo_path).map_err(map_git_err)?;
    if let Ok(names) = repo.worktrees() {
        for i in 0..names.len() {
            let Ok(Some(n)) = names.get(i) else { continue };
            let Ok(wt) = repo.find_worktree(n) else {
                continue;
            };
            if wt.path() != worktree_path {
                continue;
            }
            let mut opts = WorktreePruneOptions::new();
            opts.valid(true).locked(true);
            wt.prune(Some(&mut opts)).map_err(map_git_err)?;
        }
    }
    let trash = rename_worktree_to_trash(worktree_path)?;
    // Best-effort `git worktree prune` of whatever else went stale.
    if let Ok(names) = repo.worktrees() {
        for i in 0..names.len() {
            let Ok(Some(n)) = names.get(i) else { continue };
            if let Ok(wt) = repo.find_worktree(n) {
                if wt.is_prunable(None).unwrap_or(false) {
                    let _ = wt.prune(None);
                }
            }
        }
    }
    Ok(trash)
}

/// Phase 2 of worktree removal — the expensive recursive delete of a
/// directory detached by [`detach_worktree`], parallelized via
/// [`crate::fs_remove::remove_dir_all_parallel`]. Run this outside any
/// per-repo lock. Idempotent: an already-missing path is `Ok`.
pub fn remove_detached_worktree(trash_path: &Path) -> Result<()> {
    crate::fs_remove::remove_dir_all_parallel(trash_path)
        .map_err(|e| Error::Internal(format!("cannot remove detached worktree dir: {e}")))
}

/// CoW counterpart of [`detach_worktree`] for standalone checkouts
/// (`checkoutMode == "cow"`): rename the checkout directory to a unique
/// sibling trash path awaiting [`remove_detached_worktree`]. A CoW checkout
/// is a full clone with no registration in the source repository, so there
/// is nothing to prune — this is filesystem work only and never opens a
/// repository. Same semantics as the detach phase: `Ok(None)` when the
/// directory was already gone or had to be removed in place (rename
/// fallback). Idempotent.
pub fn detach_checkout_dir(checkout_path: &Path) -> Result<Option<PathBuf>> {
    rename_worktree_to_trash(checkout_path)
}

/// Rename `worktree_path` to a unique sibling trash path, returning the path
/// awaiting recursive removal. Race-tolerant: a source that vanished between
/// the probe and the rename is idempotent success (`None`), a trash-candidate
/// collision retries with a fresh nonce, and any other rename failure (e.g.
/// permissions, directory busy) falls back to an in-place recursive removal —
/// mirroring the original `fs.rm(worktreePath, { recursive, force })` — where
/// `NotFound` is also treated as already gone.
fn rename_worktree_to_trash(worktree_path: &Path) -> Result<Option<PathBuf>> {
    use std::io::ErrorKind;
    if !worktree_path.exists() {
        return Ok(None);
    }
    for _ in 0..8 {
        let candidate = detached_trash_path(worktree_path);
        match std::fs::rename(worktree_path, &candidate) {
            Ok(()) => return Ok(Some(candidate)),
            Err(e) if e.kind() == ErrorKind::NotFound => return Ok(None),
            // Trash-candidate collision (`EEXIST` / `ENOTEMPTY`; the latter has
            // no stable `ErrorKind` on our MSRV, so probe the candidate): retry
            // with a fresh nonce.
            Err(e) if e.kind() == ErrorKind::AlreadyExists || candidate.exists() => {}
            Err(e) => {
                tracing::debug!(
                    error = %e,
                    worktree = %worktree_path.display(),
                    "rename to trash path failed; falling back to in-place removal"
                );
                break;
            }
        }
    }
    match std::fs::remove_dir_all(worktree_path) {
        Ok(()) => Ok(None),
        Err(e) if e.kind() == ErrorKind::NotFound => Ok(None),
        Err(e) => Err(Error::Internal(format!("cannot remove worktree dir: {e}"))),
    }
}

/// Unique sibling trash path for a detached worktree —
/// `<wt>.deleting-<nonce>` in the same parent directory, so the rename never
/// crosses filesystems.
fn detached_trash_path(worktree_path: &Path) -> PathBuf {
    let name = worktree_path
        .file_name()
        .map(|n| n.to_string_lossy().into_owned())
        .unwrap_or_else(|| "worktree".to_string());
    let parent = worktree_path
        .parent()
        .map(Path::to_path_buf)
        .unwrap_or_default();
    let nonce = std::time::SystemTime::now()
        .duration_since(std::time::UNIX_EPOCH)
        .map(|d| d.as_nanos())
        .unwrap_or(0);
    for attempt in 0u32.. {
        let candidate = parent.join(format!("{name}.deleting-{nonce:x}-{attempt}"));
        if !candidate.exists() {
            return candidate;
        }
    }
    unreachable!("exhausted trash-path candidates")
}

/// Best-effort delete of the `index.lock` file for the worktree at
/// `worktree_path` (`gitService.removeLockFile`). Resolves the actual git dir
/// via libgit2 so linked worktrees (where `<worktree>/.git` is a pointer file)
/// are handled the same as main repositories. Returns whether a lock file was
/// removed. A missing lock file is `Ok(false)`; other filesystem errors surface
/// as [`Error::Internal`].
pub fn remove_index_lock(worktree_path: &Path) -> Result<bool> {
    let repo = Repository::open(worktree_path).map_err(map_git_err)?;
    // `Repository::path()` returns the worktree-specific git dir (e.g.
    // `<main>/.git/worktrees/<name>/`) — that's where `index.lock` lives.
    let lock = repo.path().join("index.lock");
    match std::fs::remove_file(&lock) {
        Ok(()) => Ok(true),
        Err(e) if e.kind() == std::io::ErrorKind::NotFound => Ok(false),
        Err(e) => Err(Error::Internal(format!(
            "failed to remove {}: {e}",
            lock.display()
        ))),
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use crate::testutil::{commit_file, init_repo};
    use std::sync::atomic::{AtomicUsize, Ordering};
    use std::time::Duration;

    #[test]
    fn provisions_worktree_on_new_branch_from_base_ref() {
        let dir = init_repo("wt-provision");
        commit_file(dir.path(), "a.txt", "x\n");
        let (base_branch, base_sha) = {
            let repo = Repository::open(dir.path()).unwrap();
            let head = repo.head().unwrap();
            (
                head.shorthand().expect("branch name").to_string(),
                head.target().unwrap().to_string(),
            )
        };
        let wt_path = std::env::temp_dir().join(format!("wt-prov-{}", uuid_ish()));
        let sha = provision_worktree(
            dir.path(),
            "amber-forest",
            &wt_path,
            "amber-forest",
            Some(&base_branch),
            "origin",
        )
        .unwrap();
        assert_eq!(sha, base_sha);
        let wt_repo = Repository::open(&wt_path).unwrap();
        assert!(wt_repo.is_worktree());
        assert_eq!(
            wt_repo.head().unwrap().shorthand().expect("branch name"),
            "amber-forest"
        );
        let _ = std::fs::remove_dir_all(&wt_path);
    }

    #[test]
    fn provision_rejects_unresolvable_base_ref() {
        let dir = init_repo("wt-badref");
        commit_file(dir.path(), "a.txt", "x\n");
        let wt_path = std::env::temp_dir().join(format!("wt-badref-{}", uuid_ish()));
        let err = provision_worktree(
            dir.path(),
            "bad-ref-ws",
            &wt_path,
            "bad-ref-ws",
            Some("no-such-ref"),
            "origin",
        )
        .unwrap_err();
        assert!(
            matches!(err, Error::BaseRefUnresolvable { ref base_ref } if base_ref == "no-such-ref")
        );
    }

    #[test]
    fn base_ref_resolves_uses_provision_spec_order() {
        let dir = init_repo("wt-resolves");
        commit_file(dir.path(), "a.txt", "x\n");
        let repo = Repository::open(dir.path()).unwrap();
        let head = repo.head().unwrap();
        let branch = head.shorthand().expect("branch name").to_string();
        let sha = head.target().unwrap().to_string();

        // Local branch, raw SHA, and a remote-tracking-only ref all resolve.
        assert!(base_ref_resolves(dir.path(), &branch, "origin").unwrap());
        assert!(base_ref_resolves(dir.path(), &sha, "origin").unwrap());
        repo.reference(
            "refs/remotes/origin/remote-only",
            head.target().unwrap(),
            false,
            "test",
        )
        .unwrap();
        assert!(base_ref_resolves(dir.path(), "remote-only", "origin").unwrap());

        // Missing ref → Ok(false); unopenable repo path → Err.
        assert!(!base_ref_resolves(dir.path(), "no-such-ref", "origin").unwrap());
        assert!(base_ref_resolves(Path::new("/no/such/repo"), "main", "origin").is_err());

        // Empty base_ref → Ok(true): provision_worktree treats it as "no
        // baseRef" and uses HEAD, never an unresolvable ref.
        assert!(base_ref_resolves(dir.path(), "", "origin").unwrap());
    }

    #[test]
    fn provision_rejects_branch_already_checked_out() {
        let dir = init_repo("wt-dup");
        commit_file(dir.path(), "a.txt", "x\n");
        let repo = Repository::open(dir.path()).unwrap();
        let head = repo.head().unwrap();
        let branch = head.shorthand().expect("branch name").to_string();

        // Attempt to create a worktree on the same branch that's already
        // checked out in the main working tree.
        let wt_path = std::env::temp_dir().join(format!("wt-dup-{}", uuid_ish()));
        let err = provision_worktree(
            dir.path(),
            "duplicate-ws",
            &wt_path,
            &branch,
            None,
            "origin",
        )
        .unwrap_err();

        // Expect InvalidParams (→ -32602) with "already checked out" message.
        match err {
            Error::InvalidParams(msg) => {
                assert!(
                    msg.contains("already checked out"),
                    "expected 'already checked out' in message, got: {msg}"
                );
                assert!(
                    msg.contains(&branch),
                    "expected branch name in message, got: {msg}"
                );
            }
            other => panic!("expected InvalidParams, got: {other:?}"),
        }
    }

    fn uuid_ish() -> u128 {
        std::time::SystemTime::now()
            .duration_since(std::time::UNIX_EPOCH)
            .unwrap()
            .as_nanos()
    }

    #[test]
    fn creates_a_linked_worktree() {
        let dir = init_repo("wt-create");
        commit_file(dir.path(), "a.txt", "x\n");
        let wt_path = dir.path().join("..").join(format!(
            "wt-{}",
            std::time::SystemTime::now()
                .duration_since(std::time::UNIX_EPOCH)
                .unwrap()
                .as_nanos()
        ));
        create_worktree(dir.path(), "feature-wt", &wt_path).unwrap();
        assert!(wt_path.join(".git").exists());
        let _ = std::fs::remove_dir_all(&wt_path);
    }

    #[test]
    fn remove_worktree_unregisters_and_deletes_directory() {
        let dir = init_repo("wt-remove");
        commit_file(dir.path(), "a.txt", "x\n");
        let wt_path = std::env::temp_dir().join(format!("wt-rm-{}", uuid_ish()));
        provision_worktree(
            dir.path(),
            "doomed-ws",
            &wt_path,
            "doomed-branch",
            None,
            "origin",
        )
        .unwrap();
        assert!(wt_path.exists());
        assert_eq!(
            worktree_branch(&wt_path).as_deref(),
            Some("doomed-branch"),
            "checked-out branch is readable before removal"
        );

        remove_worktree(dir.path(), &wt_path).unwrap();
        assert!(!wt_path.exists(), "worktree directory removed");
        let repo = Repository::open(dir.path()).unwrap();
        let names = repo.worktrees().unwrap();
        assert!(
            (0..names.len())
                .filter_map(|i| names.get(i).ok().flatten())
                .all(|n| n != "doomed-ws"),
            "worktree registration pruned"
        );
        // The branch itself is untouched — deletion is the caller's guarded call.
        assert!(repo.find_branch("doomed-branch", BranchType::Local).is_ok());
    }

    #[test]
    fn remove_worktree_survives_already_deleted_directory() {
        let dir = init_repo("wt-remove-gone");
        commit_file(dir.path(), "a.txt", "x\n");
        let wt_path = std::env::temp_dir().join(format!("wt-rmgone-{}", uuid_ish()));
        provision_worktree(
            dir.path(),
            "gone-ws",
            &wt_path,
            "gone-branch",
            None,
            "origin",
        )
        .unwrap();
        std::fs::remove_dir_all(&wt_path).unwrap();
        remove_worktree(dir.path(), &wt_path).unwrap();
        assert!(!wt_path.exists());
    }

    #[test]
    fn detach_worktree_defers_recursive_removal() {
        let dir = init_repo("wt-detach");
        commit_file(dir.path(), "a.txt", "x\n");
        let wt_path = std::env::temp_dir().join(format!("wt-detach-{}", uuid_ish()));
        provision_worktree(
            dir.path(),
            "detach-ws",
            &wt_path,
            "detach-branch",
            None,
            "origin",
        )
        .unwrap();
        assert!(wt_path.join("a.txt").exists());

        let trash = detach_worktree(dir.path(), &wt_path)
            .unwrap()
            .expect("directory renamed to a trash path");
        assert!(!wt_path.exists(), "original path vacated by the rename");
        assert!(
            trash.join("a.txt").exists(),
            "contents intact — no recursive removal in the detach phase"
        );
        let repo = Repository::open(dir.path()).unwrap();
        let names = repo.worktrees().unwrap();
        assert!(
            (0..names.len())
                .filter_map(|i| names.get(i).ok().flatten())
                .all(|n| n != "detach-ws"),
            "worktree registration pruned"
        );

        remove_detached_worktree(&trash).unwrap();
        assert!(!trash.exists());
    }

    #[test]
    fn detach_worktree_is_none_when_directory_already_gone() {
        let dir = init_repo("wt-detach-gone");
        commit_file(dir.path(), "a.txt", "x\n");
        let wt_path = std::env::temp_dir().join(format!("wt-detachgone-{}", uuid_ish()));
        provision_worktree(
            dir.path(),
            "detach-gone-ws",
            &wt_path,
            "detach-gone-branch",
            None,
            "origin",
        )
        .unwrap();
        std::fs::remove_dir_all(&wt_path).unwrap();
        assert!(detach_worktree(dir.path(), &wt_path).unwrap().is_none());
        let repo = Repository::open(dir.path()).unwrap();
        let names = repo.worktrees().unwrap();
        assert!(
            (0..names.len())
                .filter_map(|i| names.get(i).ok().flatten())
                .all(|n| n != "detach-gone-ws"),
            "worktree registration pruned"
        );
    }

    #[test]
    fn remove_detached_worktree_is_ok_for_missing_path() {
        remove_detached_worktree(Path::new("/nonexistent/intent-git-trash-probe")).unwrap();
    }

    // CoW-checkout detach: pure rename-to-trash with no registration prune —
    // the directory need not even be a git repository.
    #[test]
    fn detach_checkout_dir_renames_to_trash_without_touching_git() {
        let dir = std::env::temp_dir().join(format!("cow-detach-{}", uuid_ish()));
        std::fs::create_dir_all(dir.join("nested")).unwrap();
        std::fs::write(dir.join("nested").join("f.txt"), "x\n").unwrap();

        let trash = detach_checkout_dir(&dir)
            .unwrap()
            .expect("directory renamed to a trash path");
        assert!(!dir.exists(), "original path vacated by the rename");
        assert!(
            trash.join("nested").join("f.txt").exists(),
            "contents intact — no recursive removal in the detach phase"
        );
        assert!(
            trash
                .file_name()
                .unwrap()
                .to_string_lossy()
                .contains(".deleting-"),
            "trash path uses the sweep-recognized marker"
        );
        remove_detached_worktree(&trash).unwrap();
        assert!(!trash.exists());
    }

    #[test]
    fn detach_checkout_dir_is_none_when_directory_already_gone() {
        let missing = std::env::temp_dir().join(format!("cow-detach-gone-{}", uuid_ish()));
        assert!(detach_checkout_dir(&missing).unwrap().is_none());
    }

    // Regression for the delete-cleanup lock starvation: the lock-holding
    // phase (detach) leaves the heavy recursive removal for after the lock is
    // released, so a concurrent create on the same repo is never blocked by a
    // multi-GB `remove_dir_all`.
    #[tokio::test]
    async fn per_repo_lock_released_before_detached_removal() {
        let dir = init_repo("wt-lock-detach");
        commit_file(dir.path(), "a.txt", "x\n");
        let wt_path = std::env::temp_dir().join(format!("wt-lockdetach-{}", uuid_ish()));
        provision_worktree(
            dir.path(),
            "lock-detach-ws",
            &wt_path,
            "lock-detach-branch",
            None,
            "origin",
        )
        .unwrap();

        let locks = WorktreeLocks::new();
        let repo_path = dir.path().to_path_buf();
        let repo_for_task = repo_path.clone();
        let wt = wt_path.clone();
        let trash = locks
            .with_lock(&repo_path, move || async move {
                tokio::task::spawn_blocking(move || detach_worktree(&repo_for_task, &wt))
                    .await
                    .unwrap()
                    .unwrap()
            })
            .await
            .expect("trash path awaiting removal");
        // The lock is free here while the detached directory still exists —
        // the expensive removal has not run yet. Bound the acquire with a
        // timeout so a regression fails fast instead of hanging the test.
        assert!(trash.exists());
        let acquired = tokio::time::timeout(
            Duration::from_secs(5),
            locks.with_lock(&repo_path, || async { true }),
        )
        .await
        .expect("per-repo lock still held after detach phase");
        assert!(acquired);

        remove_detached_worktree(&trash).unwrap();
        assert!(!trash.exists());
    }

    #[test]
    fn worktree_branch_is_none_for_missing_path() {
        assert!(worktree_branch(Path::new("/nonexistent/wt-branch-probe")).is_none());
    }

    #[tokio::test]
    async fn lock_serializes_same_path() {
        let locks = WorktreeLocks::new();
        let path = std::env::temp_dir().join("intent-git-lock-test");
        let active = Arc::new(AtomicUsize::new(0));
        let max_seen = Arc::new(AtomicUsize::new(0));

        let mut handles = Vec::new();
        for _ in 0..8 {
            let locks = locks.clone();
            let path = path.clone();
            let active = active.clone();
            let max_seen = max_seen.clone();
            handles.push(tokio::spawn(async move {
                locks
                    .with_lock(&path, || async {
                        let now = active.fetch_add(1, Ordering::SeqCst) + 1;
                        max_seen.fetch_max(now, Ordering::SeqCst);
                        tokio::time::sleep(Duration::from_millis(5)).await;
                        active.fetch_sub(1, Ordering::SeqCst);
                    })
                    .await;
            }));
        }
        for h in handles {
            h.await.unwrap();
        }
        // The critical section was never entered concurrently.
        assert_eq!(max_seen.load(Ordering::SeqCst), 1);
    }

    #[tokio::test]
    async fn different_paths_do_not_block() {
        let locks = WorktreeLocks::new();
        let a = std::env::temp_dir().join("intent-git-lock-a");
        let b = std::env::temp_dir().join("intent-git-lock-b");
        // Holding A's lock must not prevent acquiring B's lock.
        let result = locks
            .with_lock(&a, || async { locks.with_lock(&b, || async { 42 }).await })
            .await;
        assert_eq!(result, 42);
    }

    #[test]
    fn remove_index_lock_removes_present_lock_file() {
        let dir = init_repo("lock-remove");
        commit_file(dir.path(), "a.txt", "x\n");
        let repo = Repository::open(dir.path()).unwrap();
        let lock = repo.path().join("index.lock");
        std::fs::write(&lock, b"pid").unwrap();
        assert!(lock.exists());
        assert!(remove_index_lock(dir.path()).unwrap());
        assert!(!lock.exists());
    }

    #[test]
    fn remove_index_lock_is_ok_when_missing() {
        let dir = init_repo("lock-missing");
        commit_file(dir.path(), "a.txt", "x\n");
        assert!(!remove_index_lock(dir.path()).unwrap());
    }
}
