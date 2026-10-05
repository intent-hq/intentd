//! Private storage shared by provider profiles; persistent sessions are never swept.
use super::ProviderProfileError;
use std::fs::{self, File, OpenOptions};
use std::io::Write;
use std::path::{Path, PathBuf};

pub(super) struct ProfileStorage {
    path: PathBuf,
    // The lock is released before TempDir removes an ephemeral directory.
    _lock: File,
    _temporary: Option<tempfile::TempDir>,
}

impl ProfileStorage {
    pub(super) fn create(
        root: &Path,
        provider: &str,
        identity: Option<&str>,
        resume: bool,
    ) -> Result<Self, ProviderProfileError> {
        private_dir(root)?;
        let parent = root.join("provider-profiles-v1");
        private_dir(&parent)?;
        let (path, temporary) = if let Some(identity) = identity {
            let hash = digest_hex(format!("{provider}\0{identity}").as_bytes());
            let path = parent.join(format!("session-{hash}"));
            if resume && !path.exists() {
                return Err(ProviderProfileError::new(
                    "resume-profile-missing",
                    "The saved provider profile is missing; start a new session.",
                ));
            }
            private_dir(&path)?;
            (path, None)
        } else {
            let temp = tempfile::Builder::new()
                .prefix("ephemeral-")
                .tempdir_in(&parent)
                .map_err(io_error)?;
            (temp.path().to_owned(), Some(temp))
        };
        let lock_path = path.join(".lease");
        reject_symlink(&lock_path)?;
        let mut opts = OpenOptions::new();
        opts.read(true).write(true).create(true).truncate(false);
        #[cfg(unix)]
        {
            use std::os::unix::fs::OpenOptionsExt;
            opts.mode(0o600);
        }
        let lock = opts.open(lock_path).map_err(io_error)?;
        fs2::FileExt::try_lock_exclusive(&lock).map_err(|_| {
            ProviderProfileError::new(
                "profile-in-use",
                "This provider session is already running; stop it before resuming.",
            )
        })?;
        Ok(Self {
            path,
            _lock: lock,
            _temporary: temporary,
        })
    }

    pub(super) fn path(&self) -> &Path {
        &self.path
    }

    pub(super) fn write(&self, name: &str, bytes: &[u8]) -> Result<(), ProviderProfileError> {
        atomic_write(&self.path.join(name), bytes)
    }

    pub(super) fn seed_once(&self, source: &Path, name: &str) -> Result<(), ProviderProfileError> {
        let dest = self.path.join(name);
        reject_symlink(&dest)?;
        if dest.exists() {
            return Ok(());
        }
        if let Some(data) = read_optional(source)? {
            self.write(name, &data)?;
        }
        Ok(())
    }
}

pub(super) fn digest_hex(bytes: &[u8]) -> String {
    use sha2::{Digest, Sha256};
    use std::fmt::Write;
    Sha256::digest(bytes)
        .iter()
        .fold(String::with_capacity(64), |mut out, byte| {
            write!(out, "{byte:02x}").expect("writing to a String cannot fail");
            out
        })
}

pub(super) fn io_error(_: std::io::Error) -> ProviderProfileError {
    // Paths, executable names and credential contents never enter diagnostics.
    ProviderProfileError::new(
        "profile-io",
        "Provider configuration could not be read or stored securely; check file access.",
    )
}

pub(super) fn reject_symlink(path: &Path) -> Result<(), ProviderProfileError> {
    match fs::symlink_metadata(path) {
        Ok(meta) if meta.file_type().is_symlink() => Err(ProviderProfileError::new(
            "config-boundary-escape",
            "A provider profile path is a symbolic link; use a private daemon-owned directory.",
        )),
        Ok(_) => Ok(()),
        Err(e) if e.kind() == std::io::ErrorKind::NotFound => Ok(()),
        Err(e) => Err(io_error(e)),
    }
}

pub(super) fn private_dir(path: &Path) -> Result<(), ProviderProfileError> {
    reject_symlink(path)?;
    fs::create_dir_all(path).map_err(io_error)?;
    #[cfg(unix)]
    {
        use std::os::unix::fs::PermissionsExt;
        fs::set_permissions(path, fs::Permissions::from_mode(0o700)).map_err(io_error)?;
    }
    Ok(())
}

pub(super) fn atomic_write(path: &Path, bytes: &[u8]) -> Result<(), ProviderProfileError> {
    reject_symlink(path)?;
    let mut temp = tempfile::NamedTempFile::new_in(path.parent().expect("profile file has parent"))
        .map_err(io_error)?;
    temp.write_all(bytes).map_err(io_error)?;
    temp.persist(path).map_err(|e| io_error(e.error))?;
    Ok(())
}

pub(super) fn read_optional(path: &Path) -> Result<Option<Vec<u8>>, ProviderProfileError> {
    use std::io::Read;
    const LIMIT: u64 = 2 * 1024 * 1024;
    let not_file = || {
        ProviderProfileError::new(
            "profile-config-not-file",
            "Provider configuration must be a regular file.",
        )
    };
    // Follow source symlinks, but never block opening project-controlled pipes.
    match fs::metadata(path) {
        Ok(meta) if !meta.is_file() => return Err(not_file()),
        Ok(_) => {}
        Err(e) if e.kind() == std::io::ErrorKind::NotFound => return Ok(None),
        Err(e) => return Err(io_error(e)),
    }
    let mut options = OpenOptions::new();
    options.read(true);
    #[cfg(unix)]
    {
        use std::os::unix::fs::OpenOptionsExt;
        // Prevent a file-to-FIFO swap between metadata and open from hanging.
        options.custom_flags(libc::O_NONBLOCK | libc::O_NOCTTY);
    }
    let file = match options.open(path) {
        Ok(f) => f,
        Err(e) if e.kind() == std::io::ErrorKind::NotFound => return Ok(None),
        Err(e) => return Err(io_error(e)),
    };
    if !file.metadata().map_err(io_error)?.is_file() {
        return Err(not_file());
    }
    let mut data = Vec::new();
    file.take(LIMIT + 1)
        .read_to_end(&mut data)
        .map_err(io_error)?;
    if data.len() as u64 > LIMIT {
        return Err(ProviderProfileError::new(
            "profile-config-too-large",
            "A provider configuration exceeds the safe read limit.",
        ));
    }
    Ok(Some(data))
}

/// Remove only abandoned ephemeral profiles after daemon restart. Call before
/// accepting launches; active leases and every persistent session are retained.
///
/// # Errors
/// Returns a redacted error if a path is a symlink or cannot be inspected or
/// deleted. Never removes a persistent profile to recover from an error.
pub fn cleanup_abandoned_profiles(owned_root: &Path) -> Result<usize, ProviderProfileError> {
    let parent = owned_root.join("provider-profiles-v1");
    reject_symlink(&parent)?;
    if !parent.exists() {
        return Ok(0);
    }
    let mut removed = 0;
    for entry in fs::read_dir(parent).map_err(io_error)?.take(10_000) {
        let entry = entry.map_err(io_error)?;
        if !entry
            .file_name()
            .to_string_lossy()
            .starts_with("ephemeral-")
            || !entry.file_type().map_err(io_error)?.is_dir()
        {
            continue;
        }
        let lease = entry.path().join(".lease");
        reject_symlink(&lease)?;
        let file = match OpenOptions::new().read(true).write(true).open(&lease) {
            Ok(file) => file,
            // A crash before lease creation leaves no sensitive running child.
            Err(e) if e.kind() == std::io::ErrorKind::NotFound => {
                fs::remove_dir_all(entry.path()).map_err(io_error)?;
                removed += 1;
                continue;
            }
            Err(e) => return Err(io_error(e)),
        };
        if fs2::FileExt::try_lock_exclusive(&file).is_ok() {
            fs::remove_dir_all(entry.path()).map_err(io_error)?;
            removed += 1;
        }
    }
    Ok(removed)
}
