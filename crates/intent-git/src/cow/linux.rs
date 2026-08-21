//! Linux `CoW` implementation using per-file FICLONE ioctl.

use intent_core::{Error, Result};
use std::fs;
use std::io;
use std::os::unix::fs::MetadataExt;
use std::os::unix::io::AsRawFd;
use std::path::Path;

use super::CowSupport;

// FICLONE ioctl number from linux/fs.h. libc::Ioctl is the target's ioctl
// request type (c_ulong on glibc, c_int on musl).
#[cfg(target_os = "linux")]
const FICLONE: libc::Ioctl = 0x4004_9409;

/// Get volume IDs (`st_dev`) for both paths as a cache key.
pub(super) fn get_volume_id_pair(src: &Path, dst: &Path) -> Option<(u64, u64)> {
    let src_meta = fs::metadata(src).ok()?;
    let dst_meta = fs::metadata(dst).ok()?;
    Some((src_meta.dev(), dst_meta.dev()))
}

pub fn probe(src_dir: &Path, dst_dir: &Path) -> Result<CowSupport> {
    // Linux has no reliable static capability flag, so go straight to live probe
    let temp_src = src_dir.join(".cow_probe_temp");
    let temp_dst = dst_dir.join(".cow_probe_temp");

    // Clean up any previous probe
    let _ = fs::remove_file(&temp_src);
    let _ = fs::remove_file(&temp_dst);

    // Create temp file
    fs::write(&temp_src, b"probe")
        .map_err(|e| Error::Internal(format!("cow probe write failed: {e}")))?;

    let result = clone_file(&temp_src, &temp_dst);

    // Cleanup
    let _ = fs::remove_file(&temp_src);
    let _ = fs::remove_file(&temp_dst);

    match result {
        Ok(()) => Ok(CowSupport::Supported),
        Err(Error::Unsupported(_)) => Ok(CowSupport::Unsupported),
        Err(e) => Err(e),
    }
}

fn clone_file(src: &Path, dst: &Path) -> Result<()> {
    let src_file =
        fs::File::open(src).map_err(|e| Error::Internal(format!("open source failed: {e}")))?;
    let dst_file =
        fs::File::create(dst).map_err(|e| Error::Internal(format!("create dest failed: {e}")))?;

    let ret = unsafe { libc::ioctl(dst_file.as_raw_fd(), FICLONE, src_file.as_raw_fd()) };

    if ret == 0 {
        Ok(())
    } else {
        let errno = io::Error::last_os_error();
        match errno.raw_os_error() {
            Some(libc::EOPNOTSUPP) | Some(libc::EXDEV) | Some(libc::EINVAL) => {
                Err(Error::Unsupported("CoW cloning not supported".to_string()))
            }
            _ => Err(Error::Internal(format!("FICLONE ioctl failed: {errno}"))),
        }
    }
}

pub fn clone(
    src: &Path,
    dst: &Path,
    excludes: &[std::path::PathBuf],
) -> Result<super::CowCloneStats> {
    // Best-effort tree walk shared with the macOS fallback: create the
    // directory structure, recreate symlinks, reflink regular files, and
    // skip genuinely non-clonable entries (sockets/FIFOs/devices, nested
    // mounts, per-entry unsupported errnos) with logging. Linux has no
    // whole-directory clone primitive (FICLONE is per-file), so there is
    // no subtree fast path here.
    super::best_effort::clone_tree(src, dst, clone_file, None, excludes)
        .map(super::CowCloneStats::from)
}
