//! Copy-on-write directory cloning for CoW-capable filesystems.
//!
//! Provides `cow_probe` (capability check per volume pair) and `cow_clone` (clone
//! a directory tree with `CoW`). Platform-specific implementations: macOS uses
//! `clonefile(2)`, Linux uses per-file `FICLONE` ioctl with a tree walk, Windows
//! uses `ReFS` block cloning. Never falls back to a byte copy — returns
//! `Unsupported` instead.

use intent_core::{Error, Result};
use std::collections::HashMap;
use std::path::{Component, Path, PathBuf};
use std::sync::{Mutex, OnceLock};
use std::time::Duration;

/// Cache of `CoW` support results keyed by (src volume ID, dst volume ID).
/// The volume ID is platform-specific: macOS uses `f_fsid`, Linux uses `st_dev`.
static PROBE_CACHE: OnceLock<Mutex<HashMap<(u64, u64), CowSupport>>> = OnceLock::new();

fn get_cache() -> &'static Mutex<HashMap<(u64, u64), CowSupport>> {
    PROBE_CACHE.get_or_init(|| Mutex::new(HashMap::new()))
}

#[cfg(unix)]
mod best_effort;
#[cfg(target_os = "linux")]
mod linux;
#[cfg(target_os = "macos")]
mod macos;
#[cfg(target_os = "windows")]
mod windows;

/// Test hook: force `cow_clone` to fail with `Error::Unsupported` when the
/// source path contains this substring. Lets tests exercise the
/// clone-fails-after-probe-passes fallback paths, which the best-effort walk
/// otherwise makes hard to trigger naturally. NOTE: this seam is compiled
/// into release binaries too (release-mode e2e runs need it); it is inert
/// unless the namespaced env var is set.
pub const TEST_COW_CLONE_UNSUPPORTED_PATH_ENV: &str = "INTENT_GIT_TEST_COW_CLONE_UNSUPPORTED_PATH";

/// `CoW` support result for a (src, dst) directory pair.
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub enum CowSupport {
    /// `CoW` clone is supported for this volume pair.
    Supported,
    /// `CoW` clone is not supported (different volumes, unsupported filesystem, etc.).
    Unsupported,
}

/// Statistics from a successful [`cow_clone_with_excludes`] call, for
/// attribution of slow clones in provisioning logs.
#[derive(Debug, Default)]
pub(crate) struct CowCloneStats {
    /// The whole tree was cloned with a single primitive call (macOS fast
    /// path); no per-entry walk ran, so the other fields are all zero/empty.
    pub whole_tree: bool,
    /// Directories skipped because they matched a caller exclusion.
    pub skipped_excluded: u64,
    /// Per-subtree fast-path clone durations (root-relative path, duration),
    /// one entry per whole-directory clone during the best-effort walk.
    pub subtree_timings: Vec<(PathBuf, Duration)>,
}

#[cfg(unix)]
impl From<best_effort::WalkStats> for CowCloneStats {
    fn from(stats: best_effort::WalkStats) -> Self {
        CowCloneStats {
            whole_tree: false,
            skipped_excluded: stats.skipped_excluded,
            subtree_timings: stats.subtree_timings,
        }
    }
}

/// Probe whether `CoW` directory cloning is supported from `src_dir` to `dst_dir`.
///
/// This performs a live probe on first call for each (src volume, dst volume) pair,
/// then caches the result. Creates a tiny temp file in `src_dir` and attempts a real
/// clone into `dst_dir`. Interprets `EOPNOTSUPP`/`ENOTSUP`/`EXDEV` as `Unsupported`.
///
/// # Errors
/// Returns an error for I/O failures unrelated to `CoW` support (e.g., permission
/// denied, disk full). `Unsupported` is returned as `Ok(CowSupport::Unsupported)`,
/// not as an error.
///
/// # Panics
///
/// Panics if the probe-cache mutex is poisoned (a prior panic while holding the lock).
pub fn cow_probe(src_dir: &Path, dst_dir: &Path) -> Result<CowSupport> {
    // Try to get volume IDs for cache lookup
    let cache_key = get_volume_pair(src_dir, dst_dir);

    // Check cache first
    if let Some((src_vol, dst_vol)) = cache_key {
        let cache = get_cache().lock().unwrap();
        if let Some(&result) = cache.get(&(src_vol, dst_vol)) {
            return Ok(result);
        }
    }

    // Run the actual probe
    #[cfg(target_os = "macos")]
    let result = macos::probe(src_dir, dst_dir)?;
    #[cfg(target_os = "linux")]
    let result = linux::probe(src_dir, dst_dir)?;
    #[cfg(target_os = "windows")]
    let result = windows::probe(src_dir, dst_dir)?;
    #[cfg(not(any(target_os = "macos", target_os = "linux", target_os = "windows")))]
    let result = {
        let _ = (src_dir, dst_dir);
        CowSupport::Unsupported
    };

    // Cache the result if we have volume IDs
    if let Some((src_vol, dst_vol)) = cache_key {
        let mut cache = get_cache().lock().unwrap();
        cache.insert((src_vol, dst_vol), result);
    }

    Ok(result)
}

/// Get volume IDs for both paths as a cache key. Returns None if volume IDs
/// cannot be determined (in which case the probe will not be cached).
#[cfg(any(target_os = "macos", target_os = "linux"))]
fn get_volume_pair(src: &Path, dst: &Path) -> Option<(u64, u64)> {
    #[cfg(target_os = "macos")]
    {
        macos::get_volume_id_pair(src, dst)
    }
    #[cfg(target_os = "linux")]
    {
        linux::get_volume_id_pair(src, dst)
    }
}

#[cfg(not(any(target_os = "macos", target_os = "linux")))]
fn get_volume_pair(_src: &Path, _dst: &Path) -> Option<(u64, u64)> {
    None
}

/// Clone a directory tree from `src` to `dst` using `CoW`.
///
/// - macOS: a single whole-tree `clonefile(2)` (clones socket/FIFO nodes on
///   APFS), falling back to a best-effort per-entry walk when the whole-tree
///   clone fails as unsupported
/// - Linux: best-effort tree walk + per-file `FICLONE` ioctl
/// - Windows: `ReFS` block cloning via `FSCTL_DUPLICATE_EXTENTS_TO_FILE`
///
/// The Unix walk skips genuinely non-clonable entries (sockets/FIFOs/device
/// nodes, nested mounts, per-entry unsupported errnos) with logging instead
/// of failing the clone; real I/O errors on regular files still fail.
///
/// `dst` must not exist. Never falls back to a byte copy — returns
/// `Error::Unsupported` if `CoW` is unavailable. On failure, a partially
/// cloned `dst` is removed best-effort (safe: `dst` did not exist on entry).
///
/// # Errors
/// - `Error::Unsupported` if `CoW` is not available for this (src, dst) pair
/// - `Error::InvalidInput` if `dst` already exists or `src` doesn't exist
/// - `Error::Internal` for other I/O failures
pub fn cow_clone(src: &Path, dst: &Path) -> Result<()> {
    cow_clone_with_excludes(src, dst, &[]).map(|_| ())
}

/// [`cow_clone`] with caller-supplied directory exclusions and clone
/// statistics. `excludes` are repo-root-relative directory paths whose whole
/// subtrees are left out of the clone; entries are sanitized first (see
/// [`sanitize_excludes`]) — `.git`, the repo root, and non-relative paths are
/// ignored with a warning. With a non-empty exclusion list the whole-tree
/// fast path is skipped (it cannot exclude anything) and the best-effort walk
/// runs directly, still fast-cloning subtrees without excluded descendants.
pub(crate) fn cow_clone_with_excludes(
    src: &Path,
    dst: &Path,
    excludes: &[String],
) -> Result<CowCloneStats> {
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
    if let Ok(needle) = std::env::var(TEST_COW_CLONE_UNSUPPORTED_PATH_ENV) {
        if !needle.is_empty() && src.to_string_lossy().contains(&needle) {
            return Err(Error::Unsupported(
                "CoW cloning not supported (test hook)".to_string(),
            ));
        }
    }
    let excludes = sanitize_excludes(excludes);

    #[cfg(target_os = "macos")]
    let result = macos::clone(src, dst, &excludes);
    #[cfg(target_os = "linux")]
    let result = linux::clone(src, dst, &excludes);
    #[cfg(target_os = "windows")]
    let result = {
        let _ = &excludes;
        windows::clone(src, dst).map(|()| CowCloneStats::default())
    };
    #[cfg(not(any(target_os = "macos", target_os = "linux", target_os = "windows")))]
    let result: Result<CowCloneStats> = {
        let _ = (src, dst, &excludes);
        Err(Error::Unsupported(
            "CoW cloning is not supported on this platform".to_string(),
        ))
    };

    if result.is_err() && dst.exists() {
        // `dst` did not exist on entry, so anything present is a partial
        // clone; remove it so fallback provisioning finds a clean path.
        let _ = std::fs::remove_dir_all(dst);
    }
    if let Ok(stats) = &result {
        if stats.skipped_excluded > 0 {
            tracing::info!(
                src = %src.display(),
                skipped_excluded = stats.skipped_excluded,
                "cow_clone: skipped excluded directories"
            );
        }
    }
    result
}

/// Sanitize caller-supplied exclusion entries into root-relative directory
/// paths. Entries that are empty, name the repo root, are absolute, contain
/// `.`/`..` components, or start with `.git` are ignored with a warning —
/// excluding `.git` or the root would corrupt the checkout rather than speed
/// it up.
fn sanitize_excludes(excludes: &[String]) -> Vec<PathBuf> {
    let mut out = Vec::new();
    for raw in excludes {
        let trimmed = raw.trim();
        let path = Path::new(trimmed);
        if trimmed.is_empty() || path.components().next().is_none() {
            tracing::warn!(entry = %raw, "cow_clone: ignoring exclusion of the repo root");
            continue;
        }
        if !path.components().all(|c| matches!(c, Component::Normal(_))) {
            tracing::warn!(
                entry = %raw,
                "cow_clone: ignoring non-relative exclusion (absolute path or ./.. components)"
            );
            continue;
        }
        if path.components().next() == Some(Component::Normal(".git".as_ref())) {
            tracing::warn!(entry = %raw, "cow_clone: ignoring exclusion of .git");
            continue;
        }
        out.push(path.to_path_buf());
    }
    out
}

#[cfg(test)]
mod tests {
    use super::*;
    use std::fs;
    use std::io::Write;

    #[test]
    fn sanitize_excludes_drops_git_root_and_non_relative_entries() {
        let raw = vec![
            ".git".to_string(),
            ".git/objects".to_string(),
            String::new(),
            "   ".to_string(),
            ".".to_string(),
            "..".to_string(),
            "../outside".to_string(),
            "/abs/path".to_string(),
            "a/../b".to_string(),
            "node_modules".to_string(),
            "packages/big/cache".to_string(),
        ];
        let sanitized = sanitize_excludes(&raw);
        assert_eq!(
            sanitized,
            vec![
                PathBuf::from("node_modules"),
                PathBuf::from("packages/big/cache")
            ]
        );
    }

    #[test]
    fn sanitize_excludes_keeps_dotfile_dirs_other_than_git() {
        let raw = vec![".cache".to_string(), ".gitignored".to_string()];
        let sanitized = sanitize_excludes(&raw);
        assert_eq!(
            sanitized,
            vec![PathBuf::from(".cache"), PathBuf::from(".gitignored")]
        );
    }

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
    fn test_cow_probe_target_dir() {
        // Test using target/test-cow-* paths to see if same-volume detection works
        let workspace_root = std::env::current_dir().unwrap();
        let src = workspace_root.join("target/test-cow-src-debug");
        let dst = workspace_root.join("target/test-cow-dst-debug");
        fs::create_dir_all(&src).unwrap();
        fs::create_dir_all(&dst).unwrap();

        let result = cow_probe(&src, &dst);
        eprintln!("cow_probe result for target dir paths: {result:?}");

        // Cleanup
        let _ = fs::remove_dir_all(&src);
        let _ = fs::remove_dir_all(&dst);

        assert!(result.is_ok());
        // On APFS macOS, target/ paths should be Supported
        #[cfg(target_os = "macos")]
        {
            if let Ok(CowSupport::Unsupported) = result {
                eprintln!("WARNING: CoW unsupported for same-volume target/ paths - this may be a probe bug!");
            }
        }
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

    #[test]
    fn test_cow_probe_caching() {
        let tmpdir = std::env::temp_dir();
        let probe_dir = tmpdir.join("cow_probe_cache_test");
        fs::create_dir_all(&probe_dir).unwrap();

        // First probe - may or may not be supported
        let first = cow_probe(&probe_dir, &probe_dir).unwrap();

        // Second probe - should return the same result instantly from cache
        let second = cow_probe(&probe_dir, &probe_dir).unwrap();

        assert_eq!(first, second);

        // Cleanup
        let _ = fs::remove_dir_all(&probe_dir);
    }

    #[cfg(unix)]
    #[test]
    fn test_cow_clone_handles_sockets_and_fifos() {
        use std::os::unix::net::UnixListener;

        let tmpdir = std::env::temp_dir();
        let src = tmpdir.join("cow_clone_special_src");
        let dst = tmpdir.join("cow_clone_special_dst");

        // Clean up from any previous run
        let _ = fs::remove_dir_all(&src);
        let _ = fs::remove_dir_all(&dst);

        fs::create_dir_all(src.join("sub")).unwrap();
        fs::write(src.join("regular.txt"), b"data").unwrap();
        fs::write(src.join("sub/nested.txt"), b"nested").unwrap();

        // A live Unix socket (e.g. a dev server's IPC socket left in the tree).
        let sock_path = src.join("sub/live.sock");
        let _listener = UnixListener::bind(&sock_path).unwrap();
        assert!(sock_path.exists());

        // A FIFO.
        let fifo_path = src.join("pipe.fifo");
        let c_path = std::ffi::CString::new(fifo_path.to_str().unwrap()).unwrap();
        assert_eq!(unsafe { libc::mkfifo(c_path.as_ptr(), 0o644) }, 0);

        // Probe between test-unique dirs (same volume as the clone) so the
        // probe's fixed temp filename cannot race parallel tests probing the
        // shared tmpdir.
        let probe_dst = tmpdir.join("cow_clone_special_probe");
        fs::create_dir_all(&probe_dst).unwrap();
        let probe = cow_probe(&src, &probe_dst).unwrap();
        let _ = fs::remove_dir_all(&probe_dst);
        if probe == CowSupport::Unsupported {
            eprintln!("Skipping special-file test: CoW not supported on this filesystem");
            let _ = fs::remove_dir_all(&src);
            return;
        }

        // The clone must succeed despite the special files. On macOS the
        // whole-tree clonefile(2) carries the socket and FIFO nodes into the
        // clone; the per-entry walk (Linux) skips them instead of failing.
        cow_clone(&src, &dst).unwrap();
        assert_eq!(fs::read_to_string(dst.join("regular.txt")).unwrap(), "data");
        assert_eq!(
            fs::read_to_string(dst.join("sub/nested.txt")).unwrap(),
            "nested"
        );
        #[cfg(target_os = "macos")]
        {
            use std::os::unix::fs::FileTypeExt;
            assert!(fs::symlink_metadata(dst.join("sub/live.sock"))
                .unwrap()
                .file_type()
                .is_socket());
            assert!(fs::symlink_metadata(dst.join("pipe.fifo"))
                .unwrap()
                .file_type()
                .is_fifo());
        }
        #[cfg(not(target_os = "macos"))]
        {
            assert!(!dst.join("sub/live.sock").exists());
            assert!(!dst.join("pipe.fifo").exists());
        }

        // Cleanup
        let _ = fs::remove_dir_all(&src);
        let _ = fs::remove_dir_all(&dst);
    }

    #[cfg(target_os = "linux")]
    #[test]
    fn test_linux_symlink_preservation() {
        use std::os::unix::fs::symlink;

        let tmpdir = std::env::temp_dir();
        let src = tmpdir.join("cow_symlink_src");
        let dst = tmpdir.join("cow_symlink_dst");

        // Clean up
        let _ = fs::remove_dir_all(&src);
        let _ = fs::remove_dir_all(&dst);

        fs::create_dir_all(&src).unwrap();

        // Create a file and a symlink to it
        let target_file = src.join("target.txt");
        fs::write(&target_file, b"symlink target").unwrap();
        let link_path = src.join("link.txt");
        symlink("target.txt", &link_path).unwrap();

        // Check if CoW is supported
        let probe = cow_probe(&src, &tmpdir).unwrap();
        if probe == CowSupport::Unsupported {
            eprintln!("Skipping Linux symlink test: CoW not supported");
            let _ = fs::remove_dir_all(&src);
            return;
        }

        // Clone
        if cow_clone(&src, &dst).is_ok() {
            // Verify the symlink was recreated
            let cloned_link = dst.join("link.txt");
            assert!(cloned_link.exists());
            assert!(fs::symlink_metadata(&cloned_link).unwrap().is_symlink());

            // Verify it points to the right target
            let link_target = fs::read_link(&cloned_link).unwrap();
            assert_eq!(link_target.to_str().unwrap(), "target.txt");
        }

        // Cleanup
        let _ = fs::remove_dir_all(&src);
        let _ = fs::remove_dir_all(&dst);
    }

    #[cfg(target_os = "linux")]
    #[test]
    fn test_linux_permission_preservation() {
        use std::os::unix::fs::PermissionsExt;

        let tmpdir = std::env::temp_dir();
        let src = tmpdir.join("cow_perms_src");
        let dst = tmpdir.join("cow_perms_dst");

        // Clean up
        let _ = fs::remove_dir_all(&src);
        let _ = fs::remove_dir_all(&dst);

        fs::create_dir_all(&src).unwrap();

        // Create a subdirectory with specific permissions
        let subdir = src.join("subdir");
        fs::create_dir(&subdir).unwrap();
        let mut perms = fs::metadata(&subdir).unwrap().permissions();
        perms.set_mode(0o750); // rwxr-x---
        fs::set_permissions(&subdir, perms).unwrap();

        // Check if CoW is supported
        let probe = cow_probe(&src, &tmpdir).unwrap();
        if probe == CowSupport::Unsupported {
            eprintln!("Skipping Linux permission test: CoW not supported");
            let _ = fs::remove_dir_all(&src);
            return;
        }

        // Clone
        if cow_clone(&src, &dst).is_ok() {
            // Verify permissions were preserved
            let cloned_subdir = dst.join("subdir");
            let cloned_perms = fs::metadata(&cloned_subdir).unwrap().permissions();
            assert_eq!(cloned_perms.mode() & 0o777, 0o750);
        }

        // Cleanup
        let _ = fs::remove_dir_all(&src);
        let _ = fs::remove_dir_all(&dst);
    }
}
