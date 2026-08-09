//! Bounded-concurrency recursive directory removal.
//!
//! [`remove_dir_all_parallel`] is the shared removal helper behind every
//! background workspace-file delete: detached-trash removal
//! ([`crate::worktree::remove_detached_worktree`]), the `workspace.delete`
//! background sweep of workspace-dir candidates, and the startup orphaned
//! trash sweep in `intent-services`. A plain `std::fs::remove_dir_all` is
//! single-threaded, so reclaiming a `node_modules`-heavy multi-GB checkout
//! takes minutes of serial unlinking; fanning the walk out across a small
//! pool of threads keeps external volumes unsaturated while cutting the
//! wall-clock time substantially.
//!
//! Pure `std` by design — callers include synchronous code and closures
//! already running on tokio's blocking pool, so the helper must not require
//! a runtime context (and must not nest `spawn_blocking`, which would tie up
//! extra blocking-pool slots for the duration of the removal).

use std::io;
use std::path::{Path, PathBuf};
use std::sync::{Condvar, Mutex};

/// Number of worker threads a single removal fans out across. Deliberately
/// small (task guidance: ~4–8): enough to overlap unlink syscalls, not
/// enough to saturate an external volume with parallel metadata writes.
const MAX_REMOVAL_THREADS: usize = 6;

/// Work-queue state shared by the removal workers: directories waiting to be
/// scanned plus the number of scans currently in flight (a scan in flight
/// may still push more directories, so `queue.is_empty()` alone is not a
/// termination condition).
struct WalkState {
    queue: Vec<PathBuf>,
    in_flight: usize,
}

/// Recursively remove `path` and everything beneath it, fanning the unlink
/// work out across up to [`MAX_REMOVAL_THREADS`] threads. A missing `path`
/// is success (`NotFound` ⇒ `Ok`), matching `remove_dir_all` call sites that
/// treat "already gone" as done.
///
/// Two phases:
/// 1. **Parallel unlink walk** (best-effort): workers pull directories from
///    a shared queue, `remove_file` their non-directory entries (symlinks
///    are unlinked, never followed — same contract as
///    `std::fs::remove_dir_all`), and push child directories back onto the
///    queue. Per-entry failures are ignored here.
/// 2. **Authoritative serial pass**: a final `std::fs::remove_dir_all(path)`
///    removes the leftover skeleton of empty directories (cheap metadata
///    ops) plus anything phase 1 could not remove, and is the sole source of
///    the returned error, so error semantics match the serial original.
pub fn remove_dir_all_parallel(path: &Path) -> io::Result<()> {
    match std::fs::symlink_metadata(path) {
        Err(e) if e.kind() == io::ErrorKind::NotFound => return Ok(()),
        Ok(meta) if meta.file_type().is_dir() => parallel_unlink_walk(path),
        // Non-directory or unreadable metadata: let the serial pass decide.
        _ => {}
    }
    match std::fs::remove_dir_all(path) {
        Ok(()) => Ok(()),
        Err(e) if e.kind() == io::ErrorKind::NotFound => Ok(()),
        Err(e) => Err(e),
    }
}

/// Phase 1 of [`remove_dir_all_parallel`]: the best-effort parallel unlink
/// walk. Directories themselves are left in place (as an empty skeleton) for
/// the authoritative serial pass to remove.
fn parallel_unlink_walk(root: &Path) {
    let state = Mutex::new(WalkState {
        queue: vec![root.to_path_buf()],
        in_flight: 0,
    });
    let idle = Condvar::new();
    std::thread::scope(|scope| {
        for _ in 0..MAX_REMOVAL_THREADS {
            scope.spawn(|| loop {
                let dir = {
                    let mut st = state.lock().expect("removal walk lock poisoned");
                    loop {
                        if let Some(dir) = st.queue.pop() {
                            st.in_flight += 1;
                            break Some(dir);
                        }
                        if st.in_flight == 0 {
                            break None;
                        }
                        st = idle.wait(st).expect("removal walk lock poisoned");
                    }
                };
                let Some(dir) = dir else { return };
                let mut children = Vec::new();
                if let Ok(entries) = std::fs::read_dir(&dir) {
                    for entry in entries.flatten() {
                        match entry.file_type() {
                            Ok(t) if t.is_dir() => children.push(entry.path()),
                            // Files and symlinks (including dir symlinks —
                            // unlink the link, never descend). Failures are
                            // re-encountered by the authoritative pass.
                            _ => {
                                let _ = std::fs::remove_file(entry.path());
                            }
                        }
                    }
                }
                let mut st = state.lock().expect("removal walk lock poisoned");
                st.in_flight -= 1;
                st.queue.append(&mut children);
                drop(st);
                idle.notify_all();
            });
        }
    });
}

#[cfg(test)]
mod tests {
    use super::remove_dir_all_parallel;
    use std::path::PathBuf;

    fn temp_dir(tag: &str) -> PathBuf {
        let dir = std::env::temp_dir().join(format!(
            "intent-git-fs-remove-{tag}-{}-{:x}",
            std::process::id(),
            std::time::SystemTime::now()
                .duration_since(std::time::UNIX_EPOCH)
                .unwrap()
                .as_nanos()
        ));
        std::fs::create_dir_all(&dir).unwrap();
        dir
    }

    #[test]
    fn removes_deep_and_wide_tree_completely() {
        let root = temp_dir("tree");
        // Wide: more first-level dirs than worker threads. Deep: nested
        // chains so workers keep feeding the queue mid-walk.
        for i in 0..16 {
            let mut dir = root.join(format!("wide-{i}"));
            for depth in 0..8 {
                dir = dir.join(format!("d{depth}"));
                std::fs::create_dir_all(&dir).unwrap();
                std::fs::write(dir.join("f.txt"), "x").unwrap();
            }
        }
        std::fs::write(root.join("top.txt"), "x").unwrap();
        std::fs::create_dir(root.join("empty")).unwrap();

        remove_dir_all_parallel(&root).unwrap();
        assert!(!root.exists(), "entire tree removed, root included");
    }

    #[test]
    fn missing_path_is_ok() {
        let missing = std::env::temp_dir().join(format!(
            "intent-git-fs-remove-missing-{:x}",
            std::time::SystemTime::now()
                .duration_since(std::time::UNIX_EPOCH)
                .unwrap()
                .as_nanos()
        ));
        remove_dir_all_parallel(&missing).unwrap();
    }

    #[cfg(unix)]
    #[test]
    fn unlinks_symlinks_without_following_them() {
        let target = temp_dir("symlink-target");
        std::fs::write(target.join("keep.txt"), "keep").unwrap();
        let root = temp_dir("symlink-root");
        std::os::unix::fs::symlink(&target, root.join("link-to-dir")).unwrap();
        std::os::unix::fs::symlink(target.join("keep.txt"), root.join("nested-file-link")).unwrap();

        remove_dir_all_parallel(&root).unwrap();
        assert!(!root.exists());
        assert!(
            target.join("keep.txt").exists(),
            "symlink target untouched — links unlinked, never followed"
        );
        std::fs::remove_dir_all(&target).unwrap();
    }

    #[test]
    fn plain_file_at_path_is_an_error() {
        let root = temp_dir("file-at-path");
        let file = root.join("not-a-dir.txt");
        std::fs::write(&file, "x").unwrap();
        remove_dir_all_parallel(&file).unwrap_err();
        assert!(file.exists(), "non-directory path is refused, not deleted");
        std::fs::remove_dir_all(&root).unwrap();
    }
}
