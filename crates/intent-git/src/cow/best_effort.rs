//! Best-effort recursive CoW clone walk shared by the Unix implementations.
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

use intent_core::{Error, Result};
use std::fs;
use std::os::unix::fs::MetadataExt;
use std::path::Path;

#[derive(Default)]
struct WalkStats {
    /// Regular files successfully reflinked.
    cloned: u64,
    /// Sockets, FIFOs, device nodes skipped by file type.
    skipped_special: u64,
    /// Entries whose per-entry clone failed with an Unsupported errno.
    skipped_unsupported: u64,
    /// Directories on a different volume than the walk root (nested mounts).
    skipped_foreign_volume: u64,
}

impl WalkStats {
    fn any_skipped(&self) -> bool {
        self.skipped_special > 0 || self.skipped_unsupported > 0 || self.skipped_foreign_volume > 0
    }
}

/// Clone `src` to `dst` entry by entry with best-effort skip semantics (see
/// module docs). `clone_file` is the platform reflink primitive for a single
/// regular file; it must map non-clonability errnos to `Error::Unsupported`.
pub(super) fn clone_tree(
    src: &Path,
    dst: &Path,
    clone_file: fn(&Path, &Path) -> Result<()>,
) -> Result<()> {
    let root_meta = fs::symlink_metadata(src)
        .map_err(|e| Error::Internal(format!("stat source failed: {e}")))?;
    let root_dev = root_meta.dev();
    let mut stats = WalkStats::default();
    clone_entry(src, dst, root_dev, clone_file, &mut stats)?;
    if stats.skipped_unsupported > 0 && stats.cloned == 0 {
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
            "cow_clone: best-effort clone skipped non-clonable entries"
        );
    }
    Ok(())
}

fn clone_entry(
    src: &Path,
    dst: &Path,
    root_dev: u64,
    clone_file: fn(&Path, &Path) -> Result<()>,
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
        if metadata.dev() != root_dev {
            tracing::debug!(
                path = %src.display(),
                "cow_clone: skipping directory on a different volume (nested mount)"
            );
            stats.skipped_foreign_volume += 1;
            return Ok(());
        }
        fs::create_dir(dst).map_err(|e| Error::Internal(format!("create dest dir failed: {e}")))?;

        let entries =
            fs::read_dir(src).map_err(|e| Error::Internal(format!("read dir failed: {e}")))?;
        for entry in entries {
            let entry = entry.map_err(|e| Error::Internal(format!("dir entry failed: {e}")))?;
            let src_path = entry.path();
            let dst_path = dst.join(entry.file_name());
            clone_entry(&src_path, &dst_path, root_dev, clone_file, stats)?;
        }
        // Applied after the children are cloned: a non-writable source dir
        // (e.g. 0555) must not block creating entries inside the copy.
        fs::set_permissions(dst, metadata.permissions())
            .map_err(|e| Error::Internal(format!("set dir permissions failed: {e}")))?;
        Ok(())
    } else if file_type.is_file() {
        match clone_file(src, dst) {
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
