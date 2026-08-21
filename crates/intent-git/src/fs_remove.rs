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
use std::path::Path;
use std::sync::Mutex;

/// Number of worker threads a single removal fans out across. Deliberately
/// small (task guidance: ~4–8): enough to overlap unlink syscalls, not
/// enough to saturate an external volume with parallel metadata writes.
const MAX_REMOVAL_THREADS: usize = 6;

/// Recursively remove `path` and everything beneath it, fanning the removal
/// of first-level subdirectories out across up to [`MAX_REMOVAL_THREADS`]
/// threads. A missing `path` is success (`NotFound` ⇒ `Ok`), matching
/// `remove_dir_all` call sites that treat "already gone" as done.
///
/// Two phases:
/// 1. **Parallel subtree removal** (best-effort): first-level entries whose
///    `read_dir` file type is a directory (`d_type` — symlinks are never
///    classified as directories) are pulled from a shared queue by a bounded
///    pool of workers, each running `std::fs::remove_dir_all` on its
///    subtree. Every actual unlink therefore happens inside std's
///    TOCTOU-hardened `remove_dir_all`, which never follows symlinks — even
///    a directory swapped for a symlink after classification makes the call
///    error out rather than descend through the link. Fan-out is
///    deliberately first-level only: queueing deeper paths would compose
///    multi-component paths whose intermediate components can be retargeted
///    after classification, and plain path resolution *would* follow those.
///    Per-subtree failures are ignored here.
/// 2. **Authoritative serial pass**: a final `std::fs::remove_dir_all(path)`
///    removes the root's files, the emptied root itself, and anything phase
///    1 could not remove, and is the sole source of the returned error, so
///    error semantics match the serial original.
pub fn remove_dir_all_parallel(path: &Path) -> io::Result<()> {
    match std::fs::symlink_metadata(path) {
        Err(e) if e.kind() == io::ErrorKind::NotFound => return Ok(()),
        Ok(meta) if meta.file_type().is_dir() => remove_subtrees_parallel(path),
        // Non-directory or unreadable metadata: let the serial pass decide.
        _ => {}
    }
    match std::fs::remove_dir_all(path) {
        Ok(()) => Ok(()),
        Err(e) if e.kind() == io::ErrorKind::NotFound => Ok(()),
        Err(e) => Err(e),
    }
}

/// Phase 1 of [`remove_dir_all_parallel`]: best-effort parallel removal of
/// `root`'s first-level subdirectories. The root's non-directory entries and
/// the root itself are left for the authoritative serial pass.
fn remove_subtrees_parallel(root: &Path) {
    let mut subdirs = Vec::new();
    if let Ok(entries) = std::fs::read_dir(root) {
        for entry in entries.flatten() {
            if entry.file_type().is_ok_and(|t| t.is_dir()) {
                subdirs.push(entry.path());
            }
        }
    }
    if subdirs.len() < 2 {
        // Nothing to overlap — the serial pass handles it alone.
        return;
    }
    let workers = MAX_REMOVAL_THREADS.min(subdirs.len());
    let queue = Mutex::new(subdirs);
    std::thread::scope(|scope| {
        for _ in 0..workers {
            // Builder::spawn_scoped (unlike Scope::spawn) reports OS
            // thread-spawn failure as an Err instead of panicking; under
            // resource exhaustion we degrade gracefully — whatever workers
            // did spawn (possibly none) drain the queue, and the serial
            // pass removes the rest.
            let spawned = std::thread::Builder::new().spawn_scoped(scope, || loop {
                let Some(dir) = queue.lock().expect("removal queue lock poisoned").pop() else {
                    return;
                };
                // Failures (permissions, races) are re-encountered and
                // reported by the authoritative serial pass.
                let _ = std::fs::remove_dir_all(&dir);
            });
            if spawned.is_err() {
                break;
            }
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
