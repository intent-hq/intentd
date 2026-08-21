//! Best-effort recursive `CoW` clone walk shared by the Unix implementations.
//!
//! Clones a directory tree entry by entry: directories are recreated (with
//! permissions), symlinks recreated, regular files reflinked via the
//! platform's `clone_file`. Entries a reflink clone can never carry are
//! SKIPPED instead of failing the whole clone: sockets/FIFOs/device nodes
//! (git cannot track them), directories on a different volume than the walk
//! root (nested mounts — reflinks cannot cross volumes), and individual
//! entries whose per-entry clone fails with an Unsupported errno
//! (ENOTSUP/EOPNOTSUPP/EXDEV). Real I/O errors on regular files still fail
//! the clone — skipping is reserved for genuinely non-clonable entries,
//! never data-bearing failures. As a guard against silently producing a
//! file-less skeleton, the walk returns `Error::Unsupported` when every
//! attempted regular-file clone was skipped as unsupported (the volume pair
//! most likely cannot reflink at all).
//!
//! Subtree fast path: when the platform provides a whole-directory clone
//! primitive (`clone_dir`, e.g. recursive clonefile on macOS), each
//! directory below the walk root is first cloned in one shot; only when
//! that directory-level clone fails as Unsupported (e.g. the subtree holds
//! a live socket) does the walk recurse into it per-entry. Subtrees cloned
//! by the fast path contain no non-clonable entries — otherwise the
//! directory-level clone would have failed — so skip semantics and counts
//! are identical to the pure per-entry walk. The foreign-volume skip is
//! only checked per-entry: a subtree containing a nested mount relies on the
//! platform `clone_dir` failing with EXDEV → `Unsupported` (recursive
//! clonefile cannot cross volumes), which drops it into the per-entry walk
//! where the dev check applies.

use intent_core::{Error, Result};
use std::fs;
use std::os::unix::fs::MetadataExt;
use std::path::{Path, PathBuf};
use std::time::{Duration, Instant};

/// Platform clone primitive: reflinks one path to another, mapping
/// non-clonability errnos (ENOTSUP/EOPNOTSUPP/EXDEV) to `Error::Unsupported`.
type CloneFn = fn(&Path, &Path) -> Result<()>;

#[derive(Default)]
pub(super) struct WalkStats {
    /// Regular files successfully reflinked one by one.
    cloned: u64,
    /// Directories cloned whole via the `clone_dir` fast path.
    cloned_subtrees: u64,
    /// Sockets, FIFOs, device nodes skipped by file type.
    skipped_special: u64,
    /// Entries whose per-entry clone failed with an Unsupported errno.
    skipped_unsupported: u64,
    /// Directories on a different volume than the walk root (nested mounts).
    skipped_foreign_volume: u64,
    /// Directories skipped because they matched a caller exclusion.
    pub(super) skipped_excluded: u64,
    /// Per-subtree fast-path clone durations (root-relative path, duration),
    /// one entry per successful `clone_dir` call.
    pub(super) subtree_timings: Vec<(PathBuf, Duration)>,
}

impl WalkStats {
    fn any_skipped(&self) -> bool {
        self.skipped_special > 0 || self.skipped_unsupported > 0 || self.skipped_foreign_volume > 0
    }
}

/// Clone `src` to `dst` entry by entry with best-effort skip semantics (see
/// module docs). `clone_file` is the platform reflink primitive for a single
/// regular file; it must map non-clonability errnos to `Error::Unsupported`.
/// `clone_dir` is the optional whole-directory clone primitive used as a
/// subtree fast path (same errno mapping contract); the walk root itself is
/// never retried with it — the caller already attempted (and failed) the
/// whole-tree clone before falling back to this walk. `excludes` are
/// root-relative directory paths (pre-sanitized by the caller) whose whole
/// subtrees are skipped; a directory with an excluded descendant is never
/// fast-cloned whole, so the exclusion always applies.
pub(super) fn clone_tree(
    src: &Path,
    dst: &Path,
    clone_file: CloneFn,
    clone_dir: Option<CloneFn>,
    excludes: &[PathBuf],
) -> Result<WalkStats> {
    let stats = walk_tree(src, dst, clone_file, clone_dir, excludes)?;
    // A successful subtree fast clone proves the clone primitive works here,
    // so the file-less-skeleton guard only applies when nothing was cloned
    // by either path.
    if stats.skipped_unsupported > 0 && stats.cloned == 0 && stats.cloned_subtrees == 0 {
        return Err(Error::Unsupported(
            "CoW cloning not supported (no regular file could be reflinked)".to_string(),
        ));
    }
    if stats.any_skipped() {
        tracing::warn!(
            src = %src.display(),
            skipped_special = stats.skipped_special,
            skipped_unsupported = stats.skipped_unsupported,
            skipped_foreign_volume = stats.skipped_foreign_volume,
            cloned = stats.cloned,
            cloned_subtrees = stats.cloned_subtrees,
            "cow_clone: best-effort clone skipped non-clonable entries"
        );
    }
    Ok(stats)
}

fn walk_tree(
    src: &Path,
    dst: &Path,
    clone_file: CloneFn,
    clone_dir: Option<CloneFn>,
    excludes: &[PathBuf],
) -> Result<WalkStats> {
    let root_meta = fs::symlink_metadata(src)
        .map_err(|e| Error::Internal(format!("stat source failed: {e}")))?;
    let root_dev = root_meta.dev();
    let mut stats = WalkStats::default();
    let ctx = WalkCtx {
        root_dev,
        clone_file,
        clone_dir,
        excludes,
    };
    clone_entry(src, dst, Path::new(""), &ctx, true, &mut stats)?;
    Ok(stats)
}

/// Walk-wide invariants threaded through the recursion.
struct WalkCtx<'a> {
    root_dev: u64,
    clone_file: CloneFn,
    clone_dir: Option<CloneFn>,
    excludes: &'a [PathBuf],
}

fn clone_entry(
    src: &Path,
    dst: &Path,
    rel: &Path,
    ctx: &WalkCtx<'_>,
    is_root: bool,
    stats: &mut WalkStats,
) -> Result<()> {
    // symlink_metadata: detect symlinks without following them.
    let metadata = fs::symlink_metadata(src)
        .map_err(|e| Error::Internal(format!("stat source failed: {e}")))?;
    let file_type = metadata.file_type();

    if file_type.is_symlink() {
        let target =
            fs::read_link(src).map_err(|e| Error::Internal(format!("read symlink failed: {e}")))?;
        std::os::unix::fs::symlink(&target, dst)
            .map_err(|e| Error::Internal(format!("create symlink failed: {e}")))?;
        Ok(())
    } else if file_type.is_dir() {
        if metadata.dev() != ctx.root_dev {
            tracing::debug!(
                path = %src.display(),
                "cow_clone: skipping directory on a different volume (nested mount)"
            );
            stats.skipped_foreign_volume += 1;
            return Ok(());
        }

        if !is_root && ctx.excludes.iter().any(|e| e == rel) {
            tracing::debug!(
                path = %src.display(),
                "cow_clone: skipping excluded directory"
            );
            stats.skipped_excluded += 1;
            return Ok(());
        }

        // Subtree fast path: clone the whole directory in one shot. Skipped
        // for the walk root — the caller already attempted (and failed) the
        // whole-tree clone before falling back here — and for directories
        // with an excluded descendant, which a whole-subtree clone would
        // carry along; those recurse per-entry so the exclusion applies.
        let has_excluded_descendant = ctx.excludes.iter().any(|e| e.starts_with(rel) && e != rel);
        if !is_root && !has_excluded_descendant {
            if let Some(clone_dir_fn) = ctx.clone_dir {
                let subtree_started = Instant::now();
                match clone_dir_fn(src, dst) {
                    Ok(()) => {
                        stats.cloned_subtrees += 1;
                        stats
                            .subtree_timings
                            .push((rel.to_path_buf(), subtree_started.elapsed()));
                        return Ok(());
                    }
                    Err(Error::Unsupported(reason)) => {
                        tracing::debug!(
                            path = %src.display(),
                            %reason,
                            "cow_clone: directory-level clone unsupported; recursing per-entry"
                        );
                        // A failed whole-directory clone can leave a partial
                        // destination behind; clear it so the per-entry walk
                        // does not die on EEXIST. Match on the actual entry
                        // type: a non-directory leftover must not turn the
                        // recoverable fallback into a hard error.
                        if let Ok(dst_meta) = fs::symlink_metadata(dst) {
                            let removal = if dst_meta.file_type().is_dir() {
                                fs::remove_dir_all(dst)
                            } else {
                                fs::remove_file(dst)
                            };
                            removal.map_err(|e| {
                                Error::Internal(format!(
                                    "cannot remove partial subtree clone before per-entry retry: {e}"
                                ))
                            })?;
                        }
                    }
                    Err(e) => return Err(e),
                }
            }
        }

        fs::create_dir(dst).map_err(|e| Error::Internal(format!("create dest dir failed: {e}")))?;

        let entries =
            fs::read_dir(src).map_err(|e| Error::Internal(format!("read dir failed: {e}")))?;
        for entry in entries {
            let entry = entry.map_err(|e| Error::Internal(format!("dir entry failed: {e}")))?;
            let src_path = entry.path();
            let dst_path = dst.join(entry.file_name());
            let child_rel = rel.join(entry.file_name());
            clone_entry(&src_path, &dst_path, &child_rel, ctx, false, stats)?;
        }
        // Applied after the children are cloned: a non-writable source dir
        // (e.g. 0555) must not block creating entries inside the copy.
        fs::set_permissions(dst, metadata.permissions())
            .map_err(|e| Error::Internal(format!("set dir permissions failed: {e}")))?;
        Ok(())
    } else if file_type.is_file() {
        match (ctx.clone_file)(src, dst) {
            Ok(()) => {
                stats.cloned += 1;
                Ok(())
            }
            Err(Error::Unsupported(reason)) => {
                // warn, not debug: a skipped regular file is potentially
                // data-bearing (an untracked file is simply absent from the
                // clone), so leave an actionable per-path trace.
                tracing::warn!(
                    path = %src.display(),
                    %reason,
                    "cow_clone: skipping regular file whose per-entry clone is unsupported"
                );
                stats.skipped_unsupported += 1;
                Ok(())
            }
            Err(e) => Err(e),
        }
    } else {
        // Sockets, FIFOs, device nodes — git cannot track them and a reflink
        // clone cannot carry them; skip.
        tracing::debug!(
            path = %src.display(),
            "cow_clone: skipping special file (socket/FIFO/device)"
        );
        stats.skipped_special += 1;
        Ok(())
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use std::cell::RefCell;
    use std::os::unix::net::UnixListener;
    use std::path::PathBuf;

    thread_local! {
        /// Directories the fake `clone_dir` was asked to clone (this thread).
        static DIR_ATTEMPTS: RefCell<Vec<PathBuf>> = const { RefCell::new(Vec::new()) };
        /// Directories the fake `clone_dir` cloned successfully (this thread).
        static DIR_SUCCESSES: RefCell<Vec<PathBuf>> = const { RefCell::new(Vec::new()) };
    }

    fn reset_recording() {
        DIR_ATTEMPTS.with(|v| v.borrow_mut().clear());
        DIR_SUCCESSES.with(|v| v.borrow_mut().clear());
    }

    fn is_special(meta: &fs::Metadata) -> bool {
        let ft = meta.file_type();
        !ft.is_file() && !ft.is_dir() && !ft.is_symlink()
    }

    fn subtree_contains_special(dir: &Path) -> bool {
        for entry in fs::read_dir(dir).unwrap() {
            let path = entry.unwrap().path();
            let meta = fs::symlink_metadata(&path).unwrap();
            if is_special(&meta) {
                return true;
            }
            if meta.file_type().is_dir() && subtree_contains_special(&path) {
                return true;
            }
        }
        false
    }

    fn copy_recursive(src: &Path, dst: &Path) {
        fs::create_dir(dst).unwrap();
        for entry in fs::read_dir(src).unwrap() {
            let entry = entry.unwrap();
            let src_path = entry.path();
            let dst_path = dst.join(entry.file_name());
            let meta = fs::symlink_metadata(&src_path).unwrap();
            if meta.file_type().is_symlink() {
                let target = fs::read_link(&src_path).unwrap();
                std::os::unix::fs::symlink(target, &dst_path).unwrap();
            } else if meta.file_type().is_dir() {
                copy_recursive(&src_path, &dst_path);
            } else {
                fs::copy(&src_path, &dst_path).unwrap();
            }
        }
    }

    /// Fake per-file reflink: a plain data copy.
    fn fake_clone_file(src: &Path, dst: &Path) -> Result<()> {
        fs::copy(src, dst).map_err(|e| Error::Internal(format!("copy failed: {e}")))?;
        Ok(())
    }

    /// Per-file reflink that always fails as unsupported.
    fn unsupported_clone_file(_src: &Path, _dst: &Path) -> Result<()> {
        Err(Error::Unsupported("test: reflink unsupported".to_string()))
    }

    /// Fake whole-directory clone mimicking recursive clonefile: fails with
    /// `Unsupported` when the subtree contains a special file, otherwise
    /// copies the subtree in one shot. Records attempts and successes.
    fn fake_clone_dir(src: &Path, dst: &Path) -> Result<()> {
        DIR_ATTEMPTS.with(|v| v.borrow_mut().push(src.to_path_buf()));
        if subtree_contains_special(src) {
            return Err(Error::Unsupported(
                "test: subtree holds a special file".to_string(),
            ));
        }
        copy_recursive(src, dst);
        DIR_SUCCESSES.with(|v| v.borrow_mut().push(src.to_path_buf()));
        Ok(())
    }

    /// Whole-directory clone that leaves a partial destination behind before
    /// failing as unsupported (a failed whole-tree clone may do this).
    fn partial_then_unsupported_clone_dir(_src: &Path, dst: &Path) -> Result<()> {
        fs::create_dir(dst).unwrap();
        fs::write(dst.join("junk-partial"), b"junk").unwrap();
        Err(Error::Unsupported("test: partial then fail".to_string()))
    }

    fn test_dir(name: &str) -> PathBuf {
        let dir = std::env::temp_dir().join(format!("best_effort_{name}_{}", std::process::id()));
        let _ = fs::remove_dir_all(&dir);
        dir
    }

    /// Tree with a live socket deep in one subtree:
    ///   src/a/f1.txt, src/a/nested/f2.txt
    ///   src/b/keep.txt, src/b/sub/live.sock
    ///   src/c/f3.txt
    ///   src/top.txt
    /// The listener must stay alive while the walk runs.
    fn build_tree_with_socket(src: &Path) -> UnixListener {
        fs::create_dir_all(src.join("a/nested")).unwrap();
        fs::create_dir_all(src.join("b/sub")).unwrap();
        fs::create_dir_all(src.join("c")).unwrap();
        fs::write(src.join("a/f1.txt"), b"f1").unwrap();
        fs::write(src.join("a/nested/f2.txt"), b"f2").unwrap();
        fs::write(src.join("b/keep.txt"), b"keep").unwrap();
        fs::write(src.join("c/f3.txt"), b"f3").unwrap();
        fs::write(src.join("top.txt"), b"top").unwrap();
        let listener = UnixListener::bind(src.join("b/sub/live.sock")).unwrap();
        assert!(src.join("b/sub/live.sock").exists());
        listener
    }

    fn assert_tree_cloned_without_socket(dst: &Path) {
        assert_eq!(fs::read_to_string(dst.join("a/f1.txt")).unwrap(), "f1");
        assert_eq!(
            fs::read_to_string(dst.join("a/nested/f2.txt")).unwrap(),
            "f2"
        );
        assert_eq!(fs::read_to_string(dst.join("b/keep.txt")).unwrap(), "keep");
        assert_eq!(fs::read_to_string(dst.join("c/f3.txt")).unwrap(), "f3");
        assert_eq!(fs::read_to_string(dst.join("top.txt")).unwrap(), "top");
        assert!(dst.join("b/sub").is_dir());
        assert!(!dst.join("b/sub/live.sock").exists());
    }

    #[test]
    fn dir_fast_path_clones_sibling_subtrees_and_skips_special() {
        reset_recording();
        let base = test_dir("fast_path");
        let src = base.join("src");
        let dst = base.join("dst");
        let _listener = build_tree_with_socket(&src);

        let stats = walk_tree(&src, &dst, fake_clone_file, Some(fake_clone_dir), &[]).unwrap();

        assert_tree_cloned_without_socket(&dst);
        // a and c cloned whole via the fast path; b (and b/sub) fell back to
        // the per-entry walk, cloning top.txt and b/keep.txt individually
        // and skipping exactly the socket.
        assert_eq!(stats.cloned_subtrees, 2);
        assert_eq!(stats.cloned, 2);
        assert_eq!(stats.skipped_special, 1);
        assert_eq!(stats.skipped_unsupported, 0);
        assert_eq!(stats.skipped_foreign_volume, 0);
        assert_eq!(stats.skipped_excluded, 0);
        // One timing entry per successful fast-path subtree clone.
        assert_eq!(stats.subtree_timings.len(), 2);
        let timed: Vec<_> = stats
            .subtree_timings
            .iter()
            .map(|(p, _)| p.clone())
            .collect();
        assert!(timed.contains(&PathBuf::from("a")));
        assert!(timed.contains(&PathBuf::from("c")));

        let successes = DIR_SUCCESSES.with(|v| v.borrow().clone());
        assert!(successes.contains(&src.join("a")));
        assert!(successes.contains(&src.join("c")));
        assert_eq!(successes.len(), 2);
        let attempts = DIR_ATTEMPTS.with(|v| v.borrow().clone());
        // The walk root is never retried with the directory-level clone.
        assert!(!attempts.contains(&src));
        assert!(attempts.contains(&src.join("b")));
        assert!(attempts.contains(&src.join("b/sub")));

        let _ = fs::remove_dir_all(&base);
    }

    #[test]
    fn per_entry_walk_without_clone_dir_keeps_existing_semantics() {
        let base = test_dir("no_fast_path");
        let src = base.join("src");
        let dst = base.join("dst");
        let _listener = build_tree_with_socket(&src);

        let stats = walk_tree(&src, &dst, fake_clone_file, None, &[]).unwrap();

        assert_tree_cloned_without_socket(&dst);
        assert_eq!(stats.cloned_subtrees, 0);
        assert_eq!(stats.cloned, 5);
        assert_eq!(stats.skipped_special, 1);
        assert_eq!(stats.skipped_unsupported, 0);

        let _ = fs::remove_dir_all(&base);
    }

    #[test]
    fn partial_dir_clone_is_cleared_before_per_entry_retry() {
        let base = test_dir("partial_cleanup");
        let src = base.join("src");
        let dst = base.join("dst");
        fs::create_dir_all(src.join("a")).unwrap();
        fs::write(src.join("a/f.txt"), b"data").unwrap();

        clone_tree(
            &src,
            &dst,
            fake_clone_file,
            Some(partial_then_unsupported_clone_dir),
            &[],
        )
        .unwrap();

        assert_eq!(fs::read_to_string(dst.join("a/f.txt")).unwrap(), "data");
        assert!(!dst.join("a/junk-partial").exists());

        let _ = fs::remove_dir_all(&base);
    }

    #[test]
    fn skeleton_guard_still_fires_when_nothing_cloned() {
        let base = test_dir("guard_fires");
        let src = base.join("src");
        let dst = base.join("dst");
        fs::create_dir_all(&src).unwrap();
        fs::write(src.join("f.txt"), b"data").unwrap();

        let result = clone_tree(&src, &dst, unsupported_clone_file, None, &[]);
        assert!(matches!(result, Err(Error::Unsupported(_))));

        let _ = fs::remove_dir_all(&base);
    }

    #[test]
    fn skeleton_guard_suppressed_by_successful_subtree_clone() {
        reset_recording();
        let base = test_dir("guard_suppressed");
        let src = base.join("src");
        let dst = base.join("dst");
        fs::create_dir_all(src.join("a")).unwrap();
        fs::write(src.join("a/f.txt"), b"data").unwrap();
        fs::write(src.join("root.txt"), b"root").unwrap();

        // `a` clones whole via the fast path; the per-entry reflink of
        // root.txt fails as unsupported. The clone must still succeed —
        // the fast-path success proves the primitive works.
        clone_tree(
            &src,
            &dst,
            unsupported_clone_file,
            Some(fake_clone_dir),
            &[],
        )
        .unwrap();

        assert_eq!(fs::read_to_string(dst.join("a/f.txt")).unwrap(), "data");
        assert!(!dst.join("root.txt").exists());

        let _ = fs::remove_dir_all(&base);
    }

    /// Tree without special files for exclusion tests:
    ///   src/a/f1.txt, src/a/nested/f2.txt
    ///   src/b/keep.txt, src/b/heavy/big.bin
    ///   src/top.txt
    fn build_plain_tree(src: &Path) {
        fs::create_dir_all(src.join("a/nested")).unwrap();
        fs::create_dir_all(src.join("b/heavy")).unwrap();
        fs::write(src.join("a/f1.txt"), b"f1").unwrap();
        fs::write(src.join("a/nested/f2.txt"), b"f2").unwrap();
        fs::write(src.join("b/keep.txt"), b"keep").unwrap();
        fs::write(src.join("b/heavy/big.bin"), b"big").unwrap();
        fs::write(src.join("top.txt"), b"top").unwrap();
    }

    #[test]
    fn excluded_directory_is_skipped_and_counted() {
        reset_recording();
        let base = test_dir("excluded_dir");
        let src = base.join("src");
        let dst = base.join("dst");
        build_plain_tree(&src);

        let excludes = [PathBuf::from("b/heavy")];
        let stats =
            walk_tree(&src, &dst, fake_clone_file, Some(fake_clone_dir), &excludes).unwrap();

        // The excluded subtree is absent; everything else is present.
        assert!(!dst.join("b/heavy").exists());
        assert_eq!(fs::read_to_string(dst.join("b/keep.txt")).unwrap(), "keep");
        assert_eq!(fs::read_to_string(dst.join("a/f1.txt")).unwrap(), "f1");
        assert_eq!(
            fs::read_to_string(dst.join("a/nested/f2.txt")).unwrap(),
            "f2"
        );
        assert_eq!(fs::read_to_string(dst.join("top.txt")).unwrap(), "top");
        assert_eq!(stats.skipped_excluded, 1);

        // `b` holds an excluded descendant, so it must never be fast-cloned
        // whole (which would carry the exclusion along); `a` has none and
        // stays on the fast path.
        let attempts = DIR_ATTEMPTS.with(|v| v.borrow().clone());
        assert!(!attempts.contains(&src.join("b")));
        assert!(!attempts.contains(&src.join("b/heavy")));
        assert!(attempts.contains(&src.join("a")));

        let _ = fs::remove_dir_all(&base);
    }

    #[test]
    fn non_matching_excludes_leave_clone_untouched() {
        let base = test_dir("exclude_nomatch");
        let src = base.join("src");
        let dst = base.join("dst");
        build_plain_tree(&src);

        let excludes = [PathBuf::from("no/such/dir")];
        let stats =
            walk_tree(&src, &dst, fake_clone_file, Some(fake_clone_dir), &excludes).unwrap();

        assert_eq!(stats.skipped_excluded, 0);
        assert_eq!(fs::read_to_string(dst.join("a/f1.txt")).unwrap(), "f1");
        assert_eq!(
            fs::read_to_string(dst.join("b/heavy/big.bin")).unwrap(),
            "big"
        );
        assert_eq!(fs::read_to_string(dst.join("top.txt")).unwrap(), "top");

        let _ = fs::remove_dir_all(&base);
    }

    #[test]
    fn exclusion_only_matches_directories_not_files() {
        let base = test_dir("exclude_file");
        let src = base.join("src");
        let dst = base.join("dst");
        build_plain_tree(&src);

        // Excludes name a regular file: it is still cloned (exclusions are
        // directory prefixes only).
        let excludes = [PathBuf::from("top.txt")];
        let stats = walk_tree(&src, &dst, fake_clone_file, None, &excludes).unwrap();

        assert_eq!(stats.skipped_excluded, 0);
        assert_eq!(fs::read_to_string(dst.join("top.txt")).unwrap(), "top");

        let _ = fs::remove_dir_all(&base);
    }
}
