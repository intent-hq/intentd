//! macOS CoW implementation using copyfile(3) with COPYFILE_CLONE|COPYFILE_RECURSIVE.

use intent_core::{Error, Result};
use std::ffi::CString;
use std::os::unix::ffi::OsStrExt;
use std::path::{Path, PathBuf};

use super::{CowCloneStats, CowSupport};

// copyfile(3) flags from copyfile.h
const COPYFILE_CLONE: u32 = 1 << 24;
const COPYFILE_RECURSIVE: u32 = 1 << 15;

// getattrlist volume capability constants
// TODO: Re-enable fast path once f_fsid comparison is fixed for APFS
#[allow(dead_code)]
const ATTR_VOL_INFO: u32 = 0x80000000;
#[allow(dead_code)]
const ATTR_VOL_CAPABILITIES: u32 = 1 << 17;
#[allow(dead_code)]
const VOL_CAP_INT_CLONE: u64 = 1 << 25;

extern "C" {
    fn copyfile(
        from: *const libc::c_char,
        to: *const libc::c_char,
        state: *mut libc::c_void,
        flags: u32,
    ) -> libc::c_int;

    #[allow(dead_code)]
    fn getattrlist(
        path: *const libc::c_char,
        attrlist: *const AttrList,
        attrbuf: *mut libc::c_void,
        attrBufSize: libc::size_t,
        options: libc::c_ulong,
    ) -> libc::c_int;

    fn statfs(path: *const libc::c_char, buf: *mut StatFs) -> libc::c_int;
}

#[repr(C)]
#[allow(dead_code)]
struct AttrList {
    bitmapcount: libc::c_ushort,
    reserved: libc::c_ushort,
    commonattr: u32,
    volattr: u32,
    dirattr: u32,
    fileattr: u32,
    forkattr: u32,
}

#[repr(C)]
#[allow(dead_code)]
struct VolCapabilities {
    capabilities: [u32; 4],
    valid: [u32; 4],
}

#[repr(C)]
#[allow(dead_code)]
struct AttrBuf {
    length: u32,
    caps: VolCapabilities,
}

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

/// Fast path: check volume capability flags. Returns `Some(Supported/Unsupported)`
/// if conclusive, `None` if the live probe is needed.
/// TODO: Re-enable once f_fsid comparison is fixed for APFS
#[allow(dead_code)]
fn check_volume_caps(src_dir: &Path, dst_dir: &Path) -> Option<CowSupport> {
    let src_caps = has_clone_capability(src_dir)?;
    let dst_caps = has_clone_capability(dst_dir)?;

    if !src_caps || !dst_caps {
        return Some(CowSupport::Unsupported);
    }

    // Check same volume (f_fsid must match)
    if !same_volume(src_dir, dst_dir) {
        return Some(CowSupport::Unsupported);
    }

    None // Live probe needed
}

#[allow(dead_code)]
fn has_clone_capability(path: &Path) -> Option<bool> {
    let path_cstr = CString::new(path.as_os_str().as_bytes()).ok()?;

    let attrlist = AttrList {
        bitmapcount: 5,
        reserved: 0,
        commonattr: 0,
        volattr: ATTR_VOL_INFO | ATTR_VOL_CAPABILITIES,
        dirattr: 0,
        fileattr: 0,
        forkattr: 0,
    };

    let mut attrbuf: AttrBuf = unsafe { std::mem::zeroed() };

    let ret = unsafe {
        getattrlist(
            path_cstr.as_ptr(),
            &attrlist,
            &mut attrbuf as *mut _ as *mut libc::c_void,
            std::mem::size_of::<AttrBuf>(),
            0,
        )
    };

    if ret != 0 {
        return None;
    }

    Some((attrbuf.caps.capabilities[3] & (VOL_CAP_INT_CLONE as u32)) != 0)
}

#[allow(dead_code)]
fn same_volume(src: &Path, dst: &Path) -> bool {
    let src_cstr = CString::new(src.as_os_str().as_bytes()).ok();
    let dst_cstr = CString::new(dst.as_os_str().as_bytes()).ok();

    let (Some(src_cstr), Some(dst_cstr)) = (src_cstr, dst_cstr) else {
        return false;
    };

    let mut src_stat: StatFs = unsafe { std::mem::zeroed() };
    let mut dst_stat: StatFs = unsafe { std::mem::zeroed() };

    let src_ret = unsafe { statfs(src_cstr.as_ptr(), &mut src_stat) };
    let dst_ret = unsafe { statfs(dst_cstr.as_ptr(), &mut dst_stat) };

    if src_ret != 0 || dst_ret != 0 {
        return false;
    }

    src_stat.f_fsid == dst_stat.f_fsid
}

/// Get volume IDs (f_fsid) for both paths as a cache key.
pub(super) fn get_volume_id_pair(src: &Path, dst: &Path) -> Option<(u64, u64)> {
    let src_id = get_volume_id(src)?;
    let dst_id = get_volume_id(dst)?;
    Some((src_id, dst_id))
}

fn get_volume_id(path: &Path) -> Option<u64> {
    let path_cstr = CString::new(path.as_os_str().as_bytes()).ok()?;
    let mut stat: StatFs = unsafe { std::mem::zeroed() };
    let ret = unsafe { statfs(path_cstr.as_ptr(), &mut stat) };
    if ret != 0 {
        return None;
    }
    // Combine the two i32 fsid values into a u64
    Some(((stat.f_fsid[0] as u64) << 32) | (stat.f_fsid[1] as u64))
}

pub fn probe(src_dir: &Path, dst_dir: &Path) -> Result<CowSupport> {
    // SKIP fast path for now - it has false negatives with f_fsid comparison on APFS
    // if let Some(result) = check_volume_caps(src_dir, dst_dir) {
    //     return Ok(result);
    // }

    // Live probe: create a temp file and try to clone it
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
            COPYFILE_CLONE,
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
        // The whole-tree clonefile fails with ENOTSUP when the tree contains
        // entries a reflink cannot carry (e.g. a live Unix socket or FIFO),
        // even though the volume pair supports cloning. Retry with the
        // best-effort per-entry walk, which skips only genuinely
        // non-clonable entries.
        Err(Error::Unsupported(reason)) if src.is_dir() => {
            tracing::debug!(
                src = %src.display(),
                %reason,
                "cow_clone: whole-tree clonefile unsupported; retrying with best-effort per-entry clone"
            );
            if dst.exists() {
                // A failed recursive copyfile leaves a partial destination
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

/// Best-effort per-entry walk with clone_tree_fast as the subtree fast path:
/// each directory below the root is first cloned whole, and only subtrees
/// whose directory-level clone fails (or which hold an excluded descendant)
/// are walked per-entry.
fn walk(src: &Path, dst: &Path, excludes: &[PathBuf]) -> Result<CowCloneStats> {
    super::best_effort::clone_tree(src, dst, clone_file, Some(clone_tree_fast), excludes)
        .map(CowCloneStats::from)
}

/// Fast path: clone the whole tree with a single recursive copyfile(3).
fn clone_tree_fast(src: &Path, dst: &Path) -> Result<()> {
    let src_cstr = CString::new(src.as_os_str().as_bytes())
        .map_err(|e| Error::Internal(format!("invalid src path: {e}")))?;
    let dst_cstr = CString::new(dst.as_os_str().as_bytes())
        .map_err(|e| Error::Internal(format!("invalid dst path: {e}")))?;

    let ret = unsafe {
        copyfile(
            src_cstr.as_ptr(),
            dst_cstr.as_ptr(),
            std::ptr::null_mut(),
            COPYFILE_CLONE | COPYFILE_RECURSIVE,
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
