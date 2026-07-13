//! macOS CoW implementation using copyfile(3) with COPYFILE_CLONE|COPYFILE_RECURSIVE.

use intent_core::{Error, Result};
use std::ffi::CString;
use std::os::unix::ffi::OsStrExt;
use std::path::Path;

use super::CowSupport;

// copyfile(3) flags from copyfile.h
const COPYFILE_CLONE: u32 = 1 << 24;
const COPYFILE_RECURSIVE: u32 = 1 << 15;

// getattrlist volume capability constants
const ATTR_VOL_INFO: u32 = 0x80000000;
const ATTR_VOL_CAPABILITIES: u32 = 1 << 17;
const VOL_CAP_INT_CLONE: u64 = 1 << 25;

extern "C" {
    fn copyfile(
        from: *const libc::c_char,
        to: *const libc::c_char,
        state: *mut libc::c_void,
        flags: u32,
    ) -> libc::c_int;

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
struct VolCapabilities {
    capabilities: [u32; 4],
    valid: [u32; 4],
}

#[repr(C)]
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

pub fn probe(src_dir: &Path, dst_dir: &Path) -> Result<CowSupport> {
    // Try fast path first
    if let Some(result) = check_volume_caps(src_dir, dst_dir) {
        return Ok(result);
    }

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

pub fn clone(src: &Path, dst: &Path) -> Result<()> {
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
