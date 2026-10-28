//! The node's private storage root: its link binding, TLS identity, journal
//! and per-agent working directories. Never the head's data directory.
//!
//! The root must be a real directory owned by the node's user and closed to
//! group and others. One `intentd node` process holds an exclusive lock on
//! it for its lifetime.

use std::fs::{File, OpenOptions};
use std::os::unix::fs::{DirBuilderExt as _, MetadataExt as _, OpenOptionsExt as _};
use std::path::{Path, PathBuf};

use anyhow::{ensure, Context as _};
use serde::{Deserialize, Serialize};

use intent_core::AgentId;
use intent_transport::node_link::BindingFile;
use intent_transport::TlsCertificate;

use super::config::valid_id;

const LOCK: &str = "node.lock";
const BINDING: &str = "binding.json";
const TLS: &str = "tls.json";
const JOURNAL: &str = "journal";
const AGENTS: &str = "agents";

/// The persisted TLS identity. The key never leaves this file.
#[derive(Serialize, Deserialize)]
#[serde(rename_all = "camelCase", deny_unknown_fields)]
struct StoredTls {
    cert: String,
    key: String,
    fingerprint256: String,
}

/// An opened, locked storage root.
#[derive(Debug)]
pub(super) struct Storage {
    root: PathBuf,
    _lock: nix::fcntl::Flock<File>,
}

impl Storage {
    /// Create (0700) or open `root` and lock it.
    pub(super) fn open(root: &Path) -> anyhow::Result<Self> {
        ensure!(root.is_absolute(), "--root must be an absolute path");
        if !root.exists() {
            std::fs::DirBuilder::new()
                .recursive(true)
                .mode(0o700)
                .create(root)
                .with_context(|| format!("create node root {}", root.display()))?;
        }
        let metadata = std::fs::symlink_metadata(root)
            .with_context(|| format!("inspect node root {}", root.display()))?;
        ensure!(
            metadata.is_dir(),
            "node root must be a directory, not a symlink or file"
        );
        ensure!(
            metadata.mode().trailing_zeros() >= 6,
            "node root must not be accessible to group or others (chmod 700)"
        );
        let lock = OpenOptions::new()
            .read(true)
            .write(true)
            .create(true)
            .truncate(false)
            .mode(0o600)
            .custom_flags(libc::O_NOFOLLOW)
            .open(root.join(LOCK))
            .context("open node root lock")?;
        // The lock file was created by (or belongs to) this process's user.
        ensure!(
            metadata.uid() == lock.metadata()?.uid(),
            "node root must be owned by the user running intentd node"
        );
        let lock = nix::fcntl::Flock::lock(lock, nix::fcntl::FlockArg::LockExclusiveNonblock)
            .map_err(|_| {
                anyhow::anyhow!("another intentd node is already using {}", root.display())
            })?;
        Ok(Self {
            root: root.to_owned(),
            _lock: lock,
        })
    }

    pub(super) fn binding_file(&self) -> BindingFile {
        BindingFile::new(self.root.join(BINDING))
    }

    pub(super) fn journal_dir(&self) -> PathBuf {
        self.root.join(JOURNAL)
    }

    /// The node's TLS identity, generated on first start and reused after,
    /// so the certificate pin head is configured with stays valid.
    pub(super) fn tls_certificate(&self) -> anyhow::Result<TlsCertificate> {
        let path = self.root.join(TLS);
        match std::fs::read(&path) {
            Ok(bytes) => {
                let stored: StoredTls = serde_json::from_slice(&bytes)
                    .context("node TLS identity is unreadable; delete tls.json to regenerate it (head must then be given the new pin)")?;
                Ok(TlsCertificate {
                    cert: stored.cert,
                    key: stored.key,
                    fingerprint256: stored.fingerprint256,
                })
            }
            Err(e) if e.kind() == std::io::ErrorKind::NotFound => {
                let generated = intent_transport::generate_certificate(vec!["intentd-node".into()])
                    .map_err(|e| anyhow::anyhow!("generate node TLS identity: {e}"))?;
                let stored = StoredTls {
                    cert: generated.cert.clone(),
                    key: generated.key.clone(),
                    fingerprint256: generated.fingerprint256.clone(),
                };
                write_private(&path, &serde_json::to_vec(&stored)?)?;
                Ok(generated)
            }
            Err(e) => Err(e).context("read node TLS identity"),
        }
    }

    /// A fresh working directory for `agent_id`, created on demand.
    pub(super) fn agent_dir(&self, agent_id: &AgentId) -> anyhow::Result<PathBuf> {
        ensure!(
            valid_id(&agent_id.0),
            "agent id is not a safe path component"
        );
        let dir = self.root.join(AGENTS).join(&agent_id.0).join("work");
        std::fs::DirBuilder::new()
            .recursive(true)
            .mode(0o700)
            .create(&dir)
            .context("create agent directory")?;
        Ok(dir)
    }
}

/// Write a 0600 file durably: temporary file, fsync, rename.
fn write_private(path: &Path, bytes: &[u8]) -> anyhow::Result<()> {
    use std::io::Write as _;
    let pending = path.with_extension("pending");
    let mut file = OpenOptions::new()
        .write(true)
        .create(true)
        .truncate(true)
        .mode(0o600)
        .open(&pending)?;
    file.write_all(bytes)?;
    file.sync_all()?;
    std::fs::rename(&pending, path)?;
    if let Some(parent) = path.parent() {
        File::open(parent)?.sync_all()?;
    }
    Ok(())
}

#[cfg(test)]
mod tests {
    use super::*;
    use std::os::unix::fs::PermissionsExt as _;

    #[test]
    fn node_storage_creates_a_private_locked_root() {
        let parent = tempfile::tempdir().unwrap();
        let root = parent.path().join("node");
        let storage = Storage::open(&root).unwrap();
        let mode = std::fs::metadata(&root).unwrap().permissions().mode();
        assert_eq!(mode & 0o077, 0, "{mode:o}");
        let error = Storage::open(&root).unwrap_err().to_string();
        assert!(error.contains("already using"), "{error}");
        drop(storage);
        Storage::open(&root).unwrap();
    }

    #[test]
    fn node_storage_refuses_shared_symlinked_or_relative_roots() {
        let parent = tempfile::tempdir().unwrap();
        let open = parent.path().join("open");
        std::fs::create_dir(&open).unwrap();
        std::fs::set_permissions(&open, std::fs::Permissions::from_mode(0o755)).unwrap();
        let error = Storage::open(&open).unwrap_err().to_string();
        assert!(error.contains("chmod 700"), "{error}");

        let real = parent.path().join("real");
        std::fs::create_dir(&real).unwrap();
        std::fs::set_permissions(&real, std::fs::Permissions::from_mode(0o700)).unwrap();
        let alias = parent.path().join("alias");
        std::os::unix::fs::symlink(&real, &alias).unwrap();
        assert!(Storage::open(&alias).is_err());

        assert!(Storage::open(Path::new("relative/root")).is_err());
    }

    #[test]
    fn node_tls_identity_is_private_and_stable_across_restarts() {
        let parent = tempfile::tempdir().unwrap();
        let root = parent.path().join("node");
        let first = Storage::open(&root).unwrap().tls_certificate().unwrap();
        let mode = std::fs::metadata(root.join(TLS))
            .unwrap()
            .permissions()
            .mode();
        assert_eq!(mode & 0o077, 0, "{mode:o}");
        let second = Storage::open(&root).unwrap().tls_certificate().unwrap();
        assert_eq!(first.fingerprint256, second.fingerprint256);
        assert_eq!(first.key, second.key);
    }

    #[test]
    fn node_agent_dirs_refuse_unsafe_ids() {
        let parent = tempfile::tempdir().unwrap();
        let storage = Storage::open(&parent.path().join("node")).unwrap();
        let dir = storage.agent_dir(&AgentId::from("agent-1")).unwrap();
        assert!(dir.is_dir());
        assert!(dir.starts_with(parent.path().join("node").join(AGENTS)));
        for bad in ["..", "a/b", ""] {
            assert!(storage.agent_dir(&AgentId::from(bad)).is_err(), "{bad}");
        }
    }
}
