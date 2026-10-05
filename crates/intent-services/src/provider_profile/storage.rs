//! Session-owned state. Dropping a persistent launch does not delete resume data;
//! ephemeral files disappear only after the final child/group guard is released.
use super::{ProfileError, ProfileResult};
use sha2::{Digest, Sha256};
use std::collections::BTreeMap;
use std::fmt::Write as _;
use std::io::Write;
use std::path::{Component, Path, PathBuf};
use std::sync::{Arc, Mutex, OnceLock, Weak};

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
    publication: Mutex<()>,
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
        let path = std::fs::canonicalize(parent)
            .map_err(|_| ProfileError::Io)?
            .join(name);
        let mut leases = persistent_leases().lock().map_err(|_| ProfileError::Io)?;
        leases.retain(|_, lease| lease.strong_count() > 0);
        if let Some(storage) = leases.get(&path).and_then(Weak::upgrade) {
            private_dir(&path)?;
            return Ok(Self(storage));
        }
        private_dir(&path)?;
        let storage = Arc::new(Storage {
            path: path.clone(),
            temporary: None,
            publication: Mutex::new(()),
        });
        leases.insert(path, Arc::downgrade(&storage));
        Ok(Self(storage))
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
            publication: Mutex::new(()),
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
        let _publication = self.0.publication.lock().map_err(|_| ProfileError::Io)?;
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
        let _publication = self.0.publication.lock().map_err(|_| ProfileError::Io)?;
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
        // Acquisition and deletion share this lock, so reopening cannot race
        // the last-guard check. Every in-process acquisition shares one lease.
        let mut leases = persistent_leases().lock().map_err(|_| ProfileError::Io)?;
        let storage = Arc::try_unwrap(self.0).map_err(|_| ProfileError::InUse)?;
        if storage.temporary.is_none() {
            private_dir(&storage.path)?;
            std::fs::remove_dir_all(&storage.path).map_err(|_| ProfileError::Io)?;
            leases.remove(&storage.path);
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

// Process-local leases are held by the daemon's session/process-group owners.
// These do not replace lifecycle coordination with a different daemon process.
fn persistent_leases() -> &'static Mutex<BTreeMap<PathBuf, Weak<Storage>>> {
    static LEASES: OnceLock<Mutex<BTreeMap<PathBuf, Weak<Storage>>>> = OnceLock::new();
    LEASES.get_or_init(Mutex::default)
}

/// Desired generated files only. Unmentioned files (including refreshed auth and
/// native history) are retained. No mutations occur while building this plan.
#[derive(Default)]
pub(super) struct FilePlan(BTreeMap<PathBuf, Option<Vec<u8>>>);

impl FilePlan {
    // Include intentionally retained native credential refresh state, but never
    // session history. This representation remains private and is only hashed.
    pub fn effective_identity(
        &self,
        directory: &ProfileDirectory,
    ) -> ProfileResult<serde_json::Value> {
        let mut files = BTreeMap::new();
        for name in [
            "models.json",
            "settings.json",
            "mcp.json",
            "auth.json",
            ".credentials.json",
            "opencode/auth.json",
        ] {
            let relative = Path::new(name);
            let bytes = if let Some(update) = self.0.get(relative) {
                update.clone()
            } else {
                if let Some(parent) = relative
                    .parent()
                    .filter(|parent| !parent.as_os_str().is_empty())
                {
                    let parent = directory.path().join(parent);
                    match std::fs::symlink_metadata(&parent) {
                        Ok(_) => private_dir(&parent)?,
                        Err(error) if error.kind() == std::io::ErrorKind::NotFound => {}
                        Err(_) => return Err(ProfileError::Io),
                    }
                }
                let path = directory.path().join(relative);
                match std::fs::symlink_metadata(&path) {
                    Ok(meta) if meta.is_file() => {
                        Some(std::fs::read(path).map_err(|_| ProfileError::Io)?)
                    }
                    Err(error) if error.kind() == std::io::ErrorKind::NotFound => None,
                    _ => return Err(ProfileError::Io),
                }
            };
            files.insert(name, bytes);
        }
        Ok(serde_json::json!(files))
    }
    pub fn replace(&mut self, name: &str, bytes: &[u8]) -> ProfileResult<()> {
        self.insert(None, name, Some(bytes.to_vec()))
    }
    pub fn remove(&mut self, name: &str) -> ProfileResult<()> {
        self.insert(None, name, None)
    }
    pub fn insert(
        &mut self,
        directory: Option<&str>,
        name: &str,
        bytes: Option<Vec<u8>>,
    ) -> ProfileResult<()> {
        check_name(name)?;
        let path = if let Some(directory) = directory {
            check_name(directory)?;
            Path::new(directory).join(name)
        } else {
            PathBuf::from(name)
        };
        if self.0.contains_key(&path) {
            return Err(ProfileError::InvalidAuth(
                "conflicting generated file updates",
            ));
        }
        self.0.insert(path, bytes);
        Ok(())
    }
}

impl ProfileDirectory {
    /// Validate and stage every file before publication; restore preceding files
    /// if a later rename fails. The session owner must quiesce native readers
    /// during rebuild. This does not claim crash-atomic multi-file publication.
    pub(super) fn reconcile(&self, plan: FilePlan) -> ProfileResult<()> {
        let _publication = self.0.publication.lock().map_err(|_| ProfileError::Io)?;
        private_dir(self.path())?;
        let stage = Self::ephemeral(self.path())?;
        let mut prepared = Vec::new();
        for (index, (relative, bytes)) in plan.0.into_iter().enumerate() {
            if let Some(parent) = relative.parent().filter(|p| !p.as_os_str().is_empty()) {
                private_dir(&self.path().join(parent))?;
            }
            let target = self.path().join(relative);
            match std::fs::symlink_metadata(&target) {
                Ok(metadata) if metadata.is_file() || metadata.file_type().is_symlink() => {}
                Err(error) if error.kind() == std::io::ErrorKind::NotFound => {}
                Ok(_) | Err(_) => return Err(ProfileError::Io),
            }
            let replacement = if let Some(bytes) = bytes {
                let name = format!("new-{index}");
                stage.write_private(&name, &bytes)?;
                Some(stage.path().join(name))
            } else {
                None
            };
            prepared.push((
                target,
                replacement,
                stage.path().join(format!("old-{index}")),
            ));
        }
        let mut applied: Vec<(PathBuf, Option<PathBuf>)> = Vec::new();
        let result = (|| {
            for (target, replacement, backup) in prepared {
                let backup = match std::fs::rename(&target, &backup) {
                    Ok(()) => Some(backup),
                    Err(error) if error.kind() == std::io::ErrorKind::NotFound => None,
                    Err(_) => return Err(ProfileError::Io),
                };
                applied.push((target.clone(), backup));
                if let Some(replacement) = replacement {
                    std::fs::rename(replacement, target).map_err(|_| ProfileError::Io)?;
                }
            }
            Ok(())
        })();
        if result.is_err() {
            let mut restored = true;
            for (target, backup) in applied.into_iter().rev() {
                let _ = std::fs::remove_file(&target);
                if let Some(backup) = backup {
                    restored &= std::fs::rename(backup, target).is_ok();
                }
            }
            if !restored {
                // Never delete the only surviving credential backup on a
                // recovery I/O failure. Preserve its owner-only recovery dir.
                let mut storage = Arc::try_unwrap(stage.0).map_err(|_| ProfileError::Io)?;
                if let Some(temporary) = storage.temporary.take() {
                    let _ = temporary.keep();
                }
                return Err(ProfileError::Io);
            }
        }
        result
    }
}
