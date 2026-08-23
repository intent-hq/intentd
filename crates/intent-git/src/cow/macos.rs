//! macOS `CoW` implementation: clonefile(2) for whole-tree clones, copyfile(3)
//! with `COPYFILE_CLONE_FORCE` for single-file clones (probe path).

use intent_core::{Error, Result};
use std::ffi::CString;
use std::os::unix::ffi::OsStrExt;
use std::path::{Path, PathBuf};

use super::{CowCloneStats, CowSupport};

// copyfile(3) flags from copyfile.h. COPYFILE_CLONE_FORCE is clone-or-fail:
// unlike the best-effort COPYFILE_CLONE (1 << 24), it never falls back to a
// physical byte copy, preserving the module contract (monorepo#1124).
const COPYFILE_CLONE_FORCE: u32 = 1 << 25;

extern "C" {
    fn copyfile(
        from: *const libc::c_char,
        to: *const libc::c_char,
        state: *mut libc::c_void,
        flags: u32,
    ) -> libc::c_int;

    fn clonefile(src: *const libc::c_char, dst: *const libc::c_char, flags: u32) -> libc::c_int;

    fn statfs(path: *const libc::c_char, buf: *mut StatFs) -> libc::c_int;
}

// The `f_` prefix mirrors the C `struct statfs` field names from
// sys/mount.h verbatim; renaming would obscure the FFI correspondence.
#[allow(clippy::struct_field_names)]
#[repr(C)]
struct StatFs {
    f_bsize: u32,
    f_iosize: i32,
    f_blocks: u64,
    f_bfree: u64,
    f_bavail: u64,
    f_files: u64,
    f_ffree: u64,
    f_fsid: [i32; 2],
    f_owner: u32,
    f_type: u32,
    f_flags: u32,
    f_fssubtype: u32,
    f_fstypename: [libc::c_char; 16],
    f_mntonname: [libc::c_char; 1024],
    f_mntfromname: [libc::c_char; 1024],
    f_flags_ext: u32,
    f_reserved: [u32; 7],
}

/// Get volume IDs (`f_fsid`) for both paths as a cache key.
pub(super) fn get_volume_id_pair(src: &Path, dst: &Path) -> Option<(u64, u64)> {
    let src_id = get_volume_id(src)?;
    let dst_id = get_volume_id(dst)?;
    Some((src_id, dst_id))
}

fn get_volume_id(path: &Path) -> Option<u64> {
    let path_cstr = CString::new(path.as_os_str().as_bytes()).ok()?;
    let mut stat: StatFs = unsafe { std::mem::zeroed() };
    let ret = unsafe { statfs(path_cstr.as_ptr(), &raw mut stat) };
    if ret != 0 {
        return None;
    }
    Some(combine_fsid(stat.f_fsid))
}

/// Combine the two i32 fsid values bitwise into a u64 cache key: each half
/// is reinterpreted as its raw 32 bits so a negative component cannot
/// sign-extend across the other half of the key.
fn combine_fsid(fsid: [i32; 2]) -> u64 {
    (u64::from(fsid[0].cast_unsigned()) << 32) | u64::from(fsid[1].cast_unsigned())
}

pub fn probe(src_dir: &Path, dst_dir: &Path) -> Result<CowSupport> {
    // Live probe: create a temp file and try to clone it. (A getattrlist
    // volume-capability fast path existed but had false negatives with the
    // f_fsid comparison on APFS; see git history if it's ever revisited.)
    let temp_src = src_dir.join(".cow_probe_temp");
    let temp_dst = dst_dir.join(".cow_probe_temp");

    // Clean up any previous probe
    let _ = std::fs::remove_file(&temp_src);
    let _ = std::fs::remove_file(&temp_dst);

    // Create temp file
    std::fs::write(&temp_src, b"probe")
        .map_err(|e| Error::Internal(format!("cow probe write failed: {e}")))?;

    let result = clone_file(&temp_src, &temp_dst);

    // Cleanup
    let _ = std::fs::remove_file(&temp_src);
    let _ = std::fs::remove_file(&temp_dst);

    match result {
        Ok(()) => Ok(CowSupport::Supported),
        Err(Error::Unsupported(_)) => Ok(CowSupport::Unsupported),
        Err(e) => Err(e),
    }
}

fn clone_file(src: &Path, dst: &Path) -> Result<()> {
    let src_cstr = CString::new(src.as_os_str().as_bytes())
        .map_err(|e| Error::Internal(format!("invalid src path: {e}")))?;
    let dst_cstr = CString::new(dst.as_os_str().as_bytes())
        .map_err(|e| Error::Internal(format!("invalid dst path: {e}")))?;

    let ret = unsafe {
        copyfile(
            src_cstr.as_ptr(),
            dst_cstr.as_ptr(),
            std::ptr::null_mut(),
            COPYFILE_CLONE_FORCE,
        )
    };

    if ret == 0 {
        Ok(())
    } else {
        let errno = unsafe { *libc::__error() };
        match errno {
            libc::ENOTSUP | libc::EOPNOTSUPP | libc::EXDEV => {
                Err(Error::Unsupported("CoW cloning not supported".to_string()))
            }
            _ => Err(Error::Internal(format!("copyfile failed: errno {errno}"))),
        }
    }
}

pub fn clone(src: &Path, dst: &Path, excludes: &[PathBuf]) -> Result<CowCloneStats> {
    // A non-empty exclusion list rules out the whole-tree fast path (a single
    // recursive clonefile cannot leave anything out), so walk directly.
    if !excludes.is_empty() {
        return walk(src, dst, excludes);
    }
    match clone_tree_fast(src, dst) {
        Ok(()) => Ok(CowCloneStats {
            whole_tree: true,
            ..CowCloneStats::default()
        }),
        // clonefile(2) clones special nodes (live Unix sockets, FIFOs) on
        // APFS, so unlike the recursive copyfile(3) it replaced it does not
        // fail on socket-bearing trees. The fallback remains for the cases
        // where the whole-tree clone is still unsupported (e.g. older
        // OS/filesystem combinations); retry with the best-effort per-entry
        // walk, which skips only genuinely non-clonable entries.
        Err(Error::Unsupported(reason)) if src.is_dir() => {
            tracing::debug!(
                src = %src.display(),
                %reason,
                "cow_clone: whole-tree clonefile unsupported; retrying with best-effort per-entry clone"
            );
            if dst.exists() {
                // A failed whole-tree clone may leave a partial destination
                // tree behind; clear it before the walk. If the cleanup
                // fails the walk would die on EEXIST and obscure the real
                // failure, so surface the cleanup error directly.
                std::fs::remove_dir_all(dst).map_err(|e| {
                    Error::Internal(format!(
                        "cannot remove partial clone before best-effort retry: {e}"
                    ))
                })?;
            }
            walk(src, dst, excludes)
        }
        Err(e) => Err(e),
    }
}

/// Best-effort per-entry walk with `clone_tree_fast` as the subtree fast path:
/// each directory below the root is first cloned whole, and only subtrees
/// whose directory-level clone fails (or which hold an excluded descendant)
/// are walked per-entry.
fn walk(src: &Path, dst: &Path, excludes: &[PathBuf]) -> Result<CowCloneStats> {
    super::best_effort::clone_tree(src, dst, clone_file, Some(clone_tree_fast), excludes)
        .map(CowCloneStats::from)
}

/// Fast path: clone the whole tree with a single kernel-side clonefile(2).
/// The destination must not exist. On APFS clonefile clones special nodes
/// (live Unix sockets, FIFOs) that the recursive copyfile(3) it replaced
/// aborted on with ENOTSUP. Note that with flags 0 clonefile follows a
/// symlink root (the clone materializes the target directory), whereas the
/// recursive copyfile cloned the link itself; callers that must not follow
/// a symlinked source should canonicalize first (as `cow_checkout` does).
fn clone_tree_fast(src: &Path, dst: &Path) -> Result<()> {
    let src_cstr = CString::new(src.as_os_str().as_bytes())
        .map_err(|e| Error::Internal(format!("invalid src path: {e}")))?;
    let dst_cstr = CString::new(dst.as_os_str().as_bytes())
        .map_err(|e| Error::Internal(format!("invalid dst path: {e}")))?;

    let ret = unsafe { clonefile(src_cstr.as_ptr(), dst_cstr.as_ptr(), 0) };

    if ret == 0 {
        Ok(())
    } else {
        let errno = unsafe { *libc::__error() };
        match errno {
            libc::ENOTSUP | libc::EOPNOTSUPP | libc::EXDEV => {
                Err(Error::Unsupported("CoW cloning not supported".to_string()))
            }
            _ => Err(Error::Internal(format!("clonefile failed: errno {errno}"))),
        }
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use std::fs;
    use std::os::unix::fs::FileTypeExt;
    use std::os::unix::net::UnixListener;

    /// Regression test for the fsid cache-key composition: a negative
    /// `f_fsid` half must be zero-extended into its own 32 bits, never
    /// sign-extended across the other half. The pre-fix `as u64` cast
    /// sign-extended a negative low half across bits 32-63, flooding
    /// (and destroying) the high half's contribution — so volumes with
    /// negative `f_fsid[1]` and different `f_fsid[0]` collided.
    #[test]
    fn combine_fsid_zero_extends_negative_halves() {
        assert_eq!(combine_fsid([0, 0]), 0);
        assert_eq!(combine_fsid([1, 2]), (1u64 << 32) | 2);
        // Negative low half stays confined to bits 0-31.
        assert_eq!(combine_fsid([0, -1]), 0xFFFF_FFFF);
        assert_eq!(combine_fsid([1, -1]), (1u64 << 32) | 0xFFFF_FFFF);
        // Negative high half stays confined to bits 32-63.
        assert_eq!(combine_fsid([-1, 0]), 0xFFFF_FFFF_0000_0000);
        // The old sign-extending composition collapsed these two keys.
        assert_ne!(combine_fsid([1, -1]), combine_fsid([2, -1]));
        // Bijectivity spot-check: distinct pairs yield distinct keys.
        assert_ne!(combine_fsid([i32::MIN, 7]), combine_fsid([7, i32::MIN]));
    }

    /// Regression test for the whole-tree fast path on socket-bearing trees
    /// (intent-hq/monorepo#1125): a source tree containing a live Unix socket
    /// must clone via the single whole-tree clone — `clonefile(2)` clones
    /// socket nodes on APFS, where the previous recursive `copyfile(3)`
    /// aborted with ENOTSUP and forced the best-effort walk fallback.
    /// Gated like the other APFS-dependent cow tests: skipped when the
    /// filesystem cannot CoW-clone.
    #[test]
    fn whole_tree_fast_path_clones_tree_with_live_unix_socket() {
        let base = std::env::temp_dir().join(format!("cow_clonefile_sock_{}", std::process::id()));
        let _ = fs::remove_dir_all(&base);
        let src = base.join("src");
        let dst = base.join("dst");
        fs::create_dir_all(src.join("sub")).unwrap();
        fs::write(src.join("sub/file.txt"), b"data").unwrap();

        if !matches!(probe(&src, &base), Ok(CowSupport::Supported)) {
            eprintln!(
                "skipping whole_tree_fast_path_clones_tree_with_live_unix_socket: \
                 CoW not supported on this filesystem"
            );
            let _ = fs::remove_dir_all(&base);
            return;
        }

        let listener = UnixListener::bind(src.join("live.sock")).expect("bind unix socket");

        let stats = clone(&src, &dst, &[]).unwrap();

        assert!(
            stats.whole_tree,
            "socket-bearing tree must clone via the whole-tree fast path"
        );
        assert_eq!(
            fs::read_to_string(dst.join("sub/file.txt")).unwrap(),
            "data"
        );
        // clonefile(2) carries the socket node itself into the clone.
        assert!(fs::symlink_metadata(dst.join("live.sock"))
            .unwrap()
            .file_type()
            .is_socket());

        drop(listener);
        let _ = fs::remove_dir_all(&base);
    }

    /// Regression test for the clone-or-fail contract (intent-hq/monorepo#1124):
    /// with `COPYFILE_CLONE_FORCE` a cross-volume per-file clone must return
    /// `Unsupported` instead of silently falling back to a physical byte copy
    /// (as the best-effort `COPYFILE_CLONE` did). Needs a second writable volume
    /// to exercise the cross-volume case; skipped when none is mounted.
    #[test]
    fn clone_file_cross_volume_returns_unsupported() {
        let base = std::env::temp_dir().join(format!("cow_clone_force_{}", std::process::id()));
        let _ = fs::remove_dir_all(&base);
        fs::create_dir_all(&base).unwrap();
        let src = base.join("src.txt");
        fs::write(&src, b"data").unwrap();

        let Some(src_vol) = get_volume_id(&base) else {
            let _ = fs::remove_dir_all(&base);
            return;
        };

        let other_volume = fs::read_dir("/Volumes")
            .map(|entries| {
                entries.flatten().map(|e| e.path()).find(|p| {
                    if get_volume_id(p).is_none_or(|v| v == src_vol) {
                        return false;
                    }
                    let marker = p.join(format!(
                        ".cow_clone_force_writable_probe_{}",
                        std::process::id()
                    ));
                    if marker.exists() {
                        return false;
                    }
                    let writable = fs::write(&marker, b"w").is_ok();
                    if writable {
                        let _ = fs::remove_file(&marker);
                    }
                    writable
                })
            })
            .ok()
            .flatten();

        let Some(vol) = other_volume else {
            eprintln!(
                "skipping clone_file_cross_volume_returns_unsupported: \
                 no second writable volume mounted"
            );
            let _ = fs::remove_dir_all(&base);
            return;
        };

        let dst = vol.join(format!(".cow_clone_force_dst_{}", std::process::id()));
        let _ = fs::remove_file(&dst);
        let result = clone_file(&src, &dst);
        let dst_exists = dst.exists();
        let _ = fs::remove_file(&dst);
        let _ = fs::remove_dir_all(&base);

        match result {
            Err(Error::Unsupported(_)) => {}
            other => panic!("cross-volume clone_file must return Unsupported, got {other:?}"),
        }
        assert!(
            !dst_exists,
            "clone_file must not leave a byte-copied destination behind"
        );
    }
}
