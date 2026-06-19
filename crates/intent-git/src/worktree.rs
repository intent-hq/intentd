//! Worktree create + lock (§9.5; internal — Cycle C consumes it).
//!
//! [`WorktreeLocks`] ports `withGitWorktreeLock`: a per-worktree async mutex
//! (keyed by worktree path) so concurrent agents/operations on the same worktree
//! never corrupt the index. [`create_worktree`] wraps `git worktree add`.

use std::collections::HashMap;
use std::path::{Path, PathBuf};
use std::sync::{Arc, Mutex};

use git2::Repository;
use intent_core::Result;
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

#[cfg(test)]
mod tests {
    use super::*;
    use crate::testutil::{commit_file, init_repo};
    use std::sync::atomic::{AtomicUsize, Ordering};
    use std::time::Duration;

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
}
