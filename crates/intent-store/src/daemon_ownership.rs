//! Process-scoped, nonblocking startup ownership using the repository MSRV.

use std::fs::{File, OpenOptions};
use std::path::Path;

use crate::{Error, Result};

#[cfg(unix)]
type LockedFile = nix::fcntl::Flock<File>;
#[cfg(not(unix))]
type LockedFile = File;

pub(crate) struct DaemonOwnership {
    _file: LockedFile,
}

impl DaemonOwnership {
    pub(crate) fn acquire(path: &Path) -> Result<Self> {
        let mut options = OpenOptions::new();
        options.read(true).write(true).create(true).truncate(false);
        // A non-shared Windows handle denies every competing open until the
        // last Store clone drops it. Keep the lock file: unlinking breaks identity.
        #[cfg(windows)]
        {
            use std::os::windows::fs::OpenOptionsExt;
            options.share_mode(0);
        }
        let file = options.open(path).map_err(|e| ownership_error(path, e))?;
        #[cfg(unix)]
        let file = nix::fcntl::Flock::lock(file, nix::fcntl::FlockArg::LockExclusiveNonblock)
            .map_err(|(_, e)| ownership_error(path, e))?;
        #[cfg(not(any(unix, windows)))]
        return Err(ownership_error(path, "OS locking is unsupported"));
        #[cfg(any(unix, windows))]
        Ok(Self { _file: file })
    }
}

fn ownership_error(path: &Path, error: impl std::fmt::Display) -> Error {
    Error::Internal(format!(
        "cannot acquire exclusive daemon database ownership at {}: {error}",
        path.display()
    ))
}
