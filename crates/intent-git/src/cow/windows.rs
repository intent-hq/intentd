//! Windows CoW implementation using ReFS block cloning.

use intent_core::{Error, Result};
use std::path::Path;

use super::CowSupport;

pub fn probe(_src_dir: &Path, _dst_dir: &Path) -> Result<CowSupport> {
    // Windows implementation would check FILE_SUPPORTS_BLOCK_REFCOUNTING via
    // GetVolumeInformation, then do a live probe. For now, return Unsupported.
    Ok(CowSupport::Unsupported)
}

pub fn clone(_src: &Path, _dst: &Path) -> Result<()> {
    // Windows implementation would use FSCTL_DUPLICATE_EXTENTS_TO_FILE.
    // For now, return Unsupported.
    Err(Error::Unsupported(
        "CoW cloning is not yet implemented on Windows".to_string(),
    ))
}
