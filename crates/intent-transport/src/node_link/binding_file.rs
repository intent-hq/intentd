//! The node's persisted [`LeaseBinding`]: one JSON file, replaced
//! atomically and fsynced before a bind is acknowledged, so a node restart
//! can never accept an older generation again.

use std::io::Write as _;
use std::path::{Path, PathBuf};

use intent_core::node_link::LeaseBinding;
use intent_core::{Error, Result};

use super::codes;

/// Where the node keeps its current binding.
#[derive(Debug, Clone)]
pub struct BindingFile {
    path: PathBuf,
}

fn store_error(detail: &'static str) -> Error {
    Error::Internal(format!("{}: {detail}", codes::BINDING_STORE))
}

impl BindingFile {
    #[must_use]
    pub fn new(path: impl Into<PathBuf>) -> Self {
        Self { path: path.into() }
    }

    #[must_use]
    pub fn path(&self) -> &Path {
        &self.path
    }

    /// The current binding, `None` before the first bind.
    ///
    /// # Errors
    ///
    /// `node-link-binding-store` when the file exists but cannot be read or
    /// parsed. The node then refuses every bind until an operator resets it,
    /// rather than forgetting its owner.
    pub fn load(&self) -> Result<Option<LeaseBinding>> {
        match std::fs::read(&self.path) {
            Ok(bytes) => serde_json::from_slice(&bytes)
                .map(Some)
                .map_err(|_| store_error("binding file is corrupt")),
            Err(e) if e.kind() == std::io::ErrorKind::NotFound => Ok(None),
            Err(_) => Err(store_error("binding file is unreadable")),
        }
    }

    /// Durably replace the binding: write a private temporary file, fsync
    /// it, rename it over the old one and fsync the directory.
    ///
    /// # Errors
    ///
    /// `node-link-binding-store` on any IO failure.
    pub fn store(&self, binding: &LeaseBinding) -> Result<()> {
        let dir = self
            .path
            .parent()
            .filter(|p| !p.as_os_str().is_empty())
            .unwrap_or_else(|| Path::new("."));
        std::fs::create_dir_all(dir).map_err(|_| store_error("cannot create binding directory"))?;
        let bytes =
            serde_json::to_vec_pretty(binding).map_err(|_| store_error("binding encode failed"))?;
        let mut tmp = self.path.clone().into_os_string();
        tmp.push(".tmp");
        let tmp = PathBuf::from(tmp);
        let mut options = std::fs::OpenOptions::new();
        options.write(true).create(true).truncate(true);
        #[cfg(unix)]
        {
            use std::os::unix::fs::OpenOptionsExt as _;
            options.mode(0o600);
        }
        let write = || -> std::io::Result<()> {
            let mut file = options.open(&tmp)?;
            file.write_all(&bytes)?;
            file.sync_all()?;
            std::fs::rename(&tmp, &self.path)?;
            #[cfg(unix)]
            std::fs::File::open(dir)?.sync_all()?;
            Ok(())
        };
        write().map_err(|_| {
            let _ = std::fs::remove_file(&tmp);
            store_error("cannot persist binding")
        })
    }
}
