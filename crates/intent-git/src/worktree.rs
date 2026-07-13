//! Worktree create + lock (§9.5; internal — Cycle C consumes it).
//!
//! [`WorktreeLocks`] ports `withGitWorktreeLock`: a per-worktree async mutex
//! (keyed by worktree path) so concurrent agents/operations on the same worktree
//! never corrupt the index. [`create_worktree`] wraps `git worktree add`;
//! [`remove_worktree`] wraps `git worktree remove --force` (+ the manual-rm
//! fallback and prune from the TS `removeGitWorktree`).

use std::collections::HashMap;
use std::path::{Path, PathBuf};
use std::sync::{Arc, Mutex};

use git2::{BranchType, Repository, WorktreeAddOptions, WorktreePruneOptions};
use intent_core::{Error, Result};
use tokio::sync::Mutex as AsyncMutex;

use crate::map_git_err;

/// A per-worktree async-mutex map. Cheap to clone (shared inner map). Each
/// distinct worktree path gets its own lock, so operations on different
/// worktrees never contend.
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
pub fn create_worktree(repo_path: &Path, name: &str, worktree_path: &Path) -> Result<()> {
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
        .ok_or_else(|| Error::InvalidParams(format!("cannot resolve base ref '{r}'")))?,
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
        .map_err(map_git_err)?;
    Ok(checked_out_sha)
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
/// `removeGitWorktree` removal sequence: `git worktree remove --force`
/// (libgit2 prune with the working-tree flag), then the manual
/// `fs.rm(recursive, force)` fallback when the directory survives, then a
/// best-effort prune of any remaining stale registrations.
pub fn remove_worktree(repo_path: &Path, worktree_path: &Path) -> Result<()> {
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
            opts.valid(true).locked(true).working_tree(true);
            wt.prune(Some(&mut opts)).map_err(map_git_err)?;
        }
    }
    // Manual fallback, mirroring `fs.rm(worktreePath, { recursive, force })`.
    if worktree_path.exists() {
        std::fs::remove_dir_all(worktree_path)
            .map_err(|e| Error::Internal(format!("cannot remove worktree dir: {e}")))?;
    }
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
    Ok(())
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
        assert!(matches!(err, Error::InvalidParams(_)));
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
