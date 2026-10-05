//! Session-owned state. Dropping a persistent launch does not delete resume data;
//! ephemeral files disappear only after the final child/group guard is released.
use super::{ProfileError, ProfileResult};
use sha2::{Digest, Sha256};
use std::fmt::Write as _;
use std::io::Write;
use std::path::{Component, Path, PathBuf};
use std::sync::Arc;

pub struct ProfileIdentity<'a> {
    pub workspace: &'a str,
    pub agent: &'a str,
    pub provider: &'a str,
}

#[derive(Clone)]
pub struct ProfileDirectory(Arc<Storage>);
struct Storage {
    path: PathBuf,
    temporary: Option<tempfile::TempDir>,
}

impl ProfileDirectory {
    /// The parent must be an Intent-owned state directory. Components below it
    /// are created private and existing links/nonprivate paths are rejected.
    /// # Errors
    /// Returns a sanitized error for unsafe paths or filesystem failures.
    pub fn persistent(parent: &Path, identity: &ProfileIdentity<'_>) -> ProfileResult<Self> {
        private_dir(parent)?;
        let mut hash = Sha256::new();
        for part in [identity.provider, identity.workspace, identity.agent] {
            hash.update(part.len().to_le_bytes());
            hash.update(part.as_bytes());
        }
        let mut name = String::from("session-");
        for byte in hash.finalize() {
            let _ = write!(name, "{byte:02x}");
        }
        let path = parent.join(name);
        private_dir(&path)?;
        Ok(Self(Arc::new(Storage {
            path,
            temporary: None,
        })))
    }

    /// # Errors
    /// Returns a sanitized error if a private temporary directory cannot be made.
    pub fn ephemeral(parent: &Path) -> ProfileResult<Self> {
        private_dir(parent)?;
        let mut builder = tempfile::Builder::new();
        builder.prefix("oneshot-");
        #[cfg(unix)]
        {
            use std::os::unix::fs::PermissionsExt as _;
            builder.permissions(std::fs::Permissions::from_mode(0o700));
        }
        let temporary = builder.tempdir_in(parent).map_err(|_| ProfileError::Io)?;
        Ok(Self(Arc::new(Storage {
            path: temporary.path().into(),
            temporary: Some(temporary),
        })))
    }

    #[must_use]
    pub fn path(&self) -> &Path {
        &self.0.path
    }

    /// Create a private, single-component provider state subdirectory.
    /// # Errors
    /// Rejects traversal, symlinks, and nonprivate existing directories.
    pub fn private_subdirectory(&self, name: &str) -> ProfileResult<PathBuf> {
        check_name(name)?;
        private_dir(self.path())?;
        let path = self.path().join(name);
        private_dir(&path)?;
        Ok(path)
    }

    /// Atomic replacement avoids following a pre-existing target symlink and
    /// never truncates a live credential file before the new value is complete.
    /// # Errors
    /// Rejects paths other than a single filename and unsafe parent directories.
    pub fn write_private(&self, name: &str, bytes: &[u8]) -> ProfileResult<()> {
        check_name(name)?;
        private_dir(self.path())?;
        let mut file =
            tempfile::NamedTempFile::new_in(self.path()).map_err(|_| ProfileError::Io)?;
        file.write_all(bytes).map_err(|_| ProfileError::Io)?;
        file.as_file().sync_all().map_err(|_| ProfileError::Io)?;
        file.persist(self.path().join(name))
            .map_err(|_| ProfileError::Io)?;
        Ok(())
    }

    /// Write a private file in a single fixed provider subdirectory.
    /// # Errors
    /// Rejects nonlocal names, unsafe directories, and I/O failures.
    pub fn write_private_in(&self, directory: &str, name: &str, bytes: &[u8]) -> ProfileResult<()> {
        check_name(directory)?;
        check_name(name)?;
        private_dir(self.path())?;
        let path = self.path().join(directory);
        private_dir(&path)?;
        let mut file = tempfile::NamedTempFile::new_in(&path).map_err(|_| ProfileError::Io)?;
        file.write_all(bytes).map_err(|_| ProfileError::Io)?;
        file.as_file().sync_all().map_err(|_| ProfileError::Io)?;
        file.persist(path.join(name))
            .map_err(|_| ProfileError::Io)?;
        Ok(())
    }

    /// Explicit session deletion only, after the process group has been reaped
    /// and every other guard released. Never call from ordinary startup sweeps.
    /// # Errors
    /// Refuses deletion while any child/launch still holds a guard.
    pub fn remove_persistent(self) -> ProfileResult<()> {
        let storage = Arc::try_unwrap(self.0).map_err(|_| ProfileError::InUse)?;
        if storage.temporary.is_none() {
            private_dir(&storage.path)?;
            std::fs::remove_dir_all(&storage.path).map_err(|_| ProfileError::Io)?;
        }
        Ok(())
    }
}

fn check_name(name: &str) -> ProfileResult<()> {
    let mut components = Path::new(name).components();
    if !matches!(components.next(), Some(Component::Normal(_)))
        || components.next().is_some()
        || name.contains(['/', '\\'])
    {
        return Err(ProfileError::Io);
    }
    Ok(())
}

fn private_dir(path: &Path) -> ProfileResult<()> {
    if !path.is_absolute() {
        return Err(ProfileError::Io);
    }
    #[cfg(not(unix))]
    {
        let _ = path;
        return Err(ProfileError::UnsupportedIsolation {
            provider: "profile storage".into(),
            missing: "private profile ACL creation is not implemented on this platform",
        });
    }
    #[cfg(unix)]
    {
        use std::os::unix::fs::{DirBuilderExt, MetadataExt, PermissionsExt};
        match std::fs::DirBuilder::new().mode(0o700).create(path) {
            Ok(()) => {}
            Err(e) if e.kind() == std::io::ErrorKind::AlreadyExists => {}
            Err(_) => return Err(ProfileError::Io),
        }
        let metadata = std::fs::symlink_metadata(path).map_err(|_| ProfileError::Io)?;
        // SAFETY: geteuid has no memory-safety preconditions.
        let uid = unsafe { libc::geteuid() };
        if !metadata.is_dir()
            || metadata.file_type().is_symlink()
            || metadata.permissions().mode() & 0o077 != 0
            || metadata.uid() != uid
        {
            return Err(ProfileError::Io);
        }
        Ok(())
    }
}
