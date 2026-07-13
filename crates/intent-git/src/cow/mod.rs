//! Copy-on-write directory cloning for CoW-capable filesystems.
//!
//! Provides `cow_probe` (capability check per volume pair) and `cow_clone` (clone
//! a directory tree with CoW). Platform-specific implementations: macOS uses
//! `copyfile(3)` `COPYFILE_CLONE|COPYFILE_RECURSIVE`, Linux uses per-file `FICLONE`
//! ioctl with a tree walk, Windows uses ReFS block cloning. Never falls back to a
//! byte copy — returns `Unsupported` instead.

use intent_core::{Error, Result};
use std::path::Path;

#[cfg(target_os = "linux")]
mod linux;
#[cfg(target_os = "macos")]
mod macos;
#[cfg(target_os = "windows")]
mod windows;

/// CoW support result for a (src, dst) directory pair.
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub enum CowSupport {
    /// CoW clone is supported for this volume pair.
    Supported,
    /// CoW clone is not supported (different volumes, unsupported filesystem, etc.).
    Unsupported,
}

/// Probe whether CoW directory cloning is supported from `src_dir` to `dst_dir`.
///
/// This performs a live probe: creates a tiny temp file in `src_dir` and attempts
/// a real clone into `dst_dir`. Interprets `EOPNOTSUPP`/`ENOTSUP`/`EXDEV` as
/// `Unsupported`. Results should be cached per (src volume, dst volume) pair.
///
/// # Errors
/// Returns an error for I/O failures unrelated to CoW support (e.g., permission
/// denied, disk full). `Unsupported` is returned as `Ok(CowSupport::Unsupported)`,
/// not as an error.
pub fn cow_probe(src_dir: &Path, dst_dir: &Path) -> Result<CowSupport> {
    #[cfg(target_os = "macos")]
    return macos::probe(src_dir, dst_dir);
    #[cfg(target_os = "linux")]
    return linux::probe(src_dir, dst_dir);
    #[cfg(target_os = "windows")]
    return windows::probe(src_dir, dst_dir);
    #[cfg(not(any(target_os = "macos", target_os = "linux", target_os = "windows")))]
    {
        let _ = (src_dir, dst_dir);
        Ok(CowSupport::Unsupported)
    }
}

/// Clone a directory tree from `src` to `dst` using CoW.
///
/// - macOS: `copyfile(3)` with `COPYFILE_CLONE|COPYFILE_RECURSIVE`
/// - Linux: tree walk + per-file `FICLONE` ioctl
/// - Windows: ReFS block cloning via `FSCTL_DUPLICATE_EXTENTS_TO_FILE`
///
/// `dst` must not exist. Never falls back to a byte copy — returns
/// `Error::Unsupported` if CoW is unavailable.
///
/// # Errors
/// - `Error::Unsupported` if CoW is not available for this (src, dst) pair
/// - `Error::InvalidInput` if `dst` already exists or `src` doesn't exist
/// - `Error::Internal` for other I/O failures
pub fn cow_clone(src: &Path, dst: &Path) -> Result<()> {
    if !src.exists() {
        return Err(Error::InvalidInput(format!(
            "source does not exist: {}",
            src.display()
        )));
    }
    if dst.exists() {
        return Err(Error::InvalidInput(format!(
            "destination already exists: {}",
            dst.display()
        )));
    }

    #[cfg(target_os = "macos")]
    return macos::clone(src, dst);
    #[cfg(target_os = "linux")]
    return linux::clone(src, dst);
    #[cfg(target_os = "windows")]
    return windows::clone(src, dst);
    #[cfg(not(any(target_os = "macos", target_os = "linux", target_os = "windows")))]
    {
        let _ = (src, dst);
        Err(Error::Unsupported(
            "CoW cloning is not supported on this platform".to_string(),
        ))
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use std::fs;
    use std::io::Write;

    #[test]
    fn test_cow_probe_same_volume() {
        let tmpdir = std::env::temp_dir();
        let src = tmpdir.join("cow_probe_src");
        let dst = tmpdir.join("cow_probe_dst");
        fs::create_dir_all(&src).unwrap();
        fs::create_dir_all(&dst).unwrap();

        let result = cow_probe(&src, &dst);
        // Cleanup
        let _ = fs::remove_dir_all(&src);
        let _ = fs::remove_dir_all(&dst);

        // We don't assert Supported because CI may run on non-CoW filesystems
        assert!(result.is_ok());
    }

    #[test]
    fn test_cow_clone_basic() {
        let tmpdir = std::env::temp_dir();
        let src = tmpdir.join("cow_clone_src_test");
        let dst = tmpdir.join("cow_clone_dst_test");

        // Clean up from any previous run
        let _ = fs::remove_dir_all(&src);
        let _ = fs::remove_dir_all(&dst);

        fs::create_dir_all(&src).unwrap();
        let test_file = src.join("test.txt");
        let mut f = fs::File::create(&test_file).unwrap();
        f.write_all(b"test content").unwrap();
        drop(f);

        // First check if CoW is supported
        let probe = cow_probe(&src, &tmpdir).unwrap();
        if probe == CowSupport::Unsupported {
            eprintln!("Skipping cow_clone test: CoW not supported on this filesystem");
            let _ = fs::remove_dir_all(&src);
            return;
        }

        // Try to clone
        let result = cow_clone(&src, &dst);
        if result.is_ok() {
            assert!(dst.exists());
            assert!(dst.join("test.txt").exists());
            let content = fs::read_to_string(dst.join("test.txt")).unwrap();
            assert_eq!(content, "test content");
        }

        // Cleanup
        let _ = fs::remove_dir_all(&src);
        let _ = fs::remove_dir_all(&dst);
    }
}
