//! One backend's private storage on this machine,
//! `<data_dir>/executor/<credentialId>/`: its journal, checkout caches and
//! per-agent working directories. Remote agents never get rows in the
//! daemon's store.
//!
//! The root must be a real directory owned by the daemon's user and closed
//! to group and others. The daemon holds an exclusive lock on it while it
//! serves the credential.

use std::fs::{File, OpenOptions};
use std::os::unix::fs::{DirBuilderExt as _, MetadataExt as _, OpenOptionsExt as _};
use std::path::{Path, PathBuf};

use anyhow::{ensure, Context as _};
use intent_core::AgentId;

const LOCK: &str = "executor.lock";
const JOURNAL: &str = "journal";
const AGENTS: &str = "agents";
const CACHE: &str = "cache";
const BIN: &str = "bin";
const GIT_SOCKET: &str = "git.sock";

/// Whether `id` is safe as one path component.
pub(super) fn valid_id(id: &str) -> bool {
    (1..=128).contains(&id.len())
        && id != "."
        && id != ".."
        && id
            .bytes()
            .all(|b| b.is_ascii_alphanumeric() || matches!(b, b'.' | b'_' | b'-'))
}

/// An opened, locked storage root.
#[derive(Debug)]
pub(super) struct Storage {
    root: PathBuf,
    git_socket: PathBuf,
    _lock: nix::fcntl::Flock<File>,
}

impl Storage {
    /// Create (0700) or open `root` and lock it. The helper socket lives at
    /// `git_socket` when given (a short path: Unix socket paths are limited
    /// to about 100 bytes), else in the root.
    pub(super) fn open(root: &Path, git_socket: Option<PathBuf>) -> anyhow::Result<Self> {
        ensure!(
            root.is_absolute(),
            "the storage root must be an absolute path"
        );
        if !root.exists() {
            std::fs::DirBuilder::new()
                .recursive(true)
                .mode(0o700)
                .create(root)
                .with_context(|| format!("create executor root {}", root.display()))?;
        }
        let metadata = std::fs::symlink_metadata(root)
            .with_context(|| format!("inspect executor root {}", root.display()))?;
        ensure!(
            metadata.is_dir(),
            "executor root must be a directory, not a symlink or file"
        );
        ensure!(
            metadata.mode().trailing_zeros() >= 6,
            "executor root must not be accessible to group or others (chmod 700)"
        );
        let lock = OpenOptions::new()
            .read(true)
            .write(true)
            .create(true)
            .truncate(false)
            .mode(0o600)
            .custom_flags(libc::O_NOFOLLOW)
            .open(root.join(LOCK))
            .context("open executor root lock")?;
        // The lock file was created by (or belongs to) this process's user.
        ensure!(
            metadata.uid() == lock.metadata()?.uid(),
            "executor root must be owned by the user running intentd"
        );
        let lock = nix::fcntl::Flock::lock(lock, nix::fcntl::FlockArg::LockExclusiveNonblock)
            .map_err(|_| anyhow::anyhow!("another intentd is already using {}", root.display()))?;
        Ok(Self {
            root: root.to_owned(),
            git_socket: git_socket.unwrap_or_else(|| root.join(GIT_SOCKET)),
            _lock: lock,
        })
    }

    pub(super) fn journal_dir(&self) -> PathBuf {
        self.root.join(JOURNAL)
    }

    /// Where `agent_id`'s git worktree lives. Its parent exists; the
    /// worktree itself is created by `git worktree add`.
    pub(super) fn agent_worktree(&self, agent_id: &AgentId) -> anyhow::Result<PathBuf> {
        ensure!(
            valid_id(&agent_id.0),
            "agent id is not a safe path component"
        );
        let parent = self.root.join(AGENTS).join(&agent_id.0);
        std::fs::DirBuilder::new()
            .recursive(true)
            .mode(0o700)
            .create(&parent)
            .context("create agent directory")?;
        Ok(parent.join("work"))
    }

    /// The per-repository bare caches holding agent worktrees.
    pub(super) fn cache_dir(&self) -> PathBuf {
        self.root.join(CACHE)
    }

    /// Where the `git-remote-intent` helper link is installed.
    pub(super) fn helper_dir(&self) -> PathBuf {
        self.root.join(BIN)
    }

    /// The local socket the helper reaches head's git sessions through.
    pub(super) fn git_socket(&self) -> PathBuf {
        self.git_socket.clone()
    }

    /// A fresh working directory for `agent_id`, created on demand.
    #[cfg(test)]
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

#[cfg(test)]
mod tests {
    use super::*;
    use std::os::unix::fs::PermissionsExt as _;

    #[test]
    fn executor_storage_creates_a_private_locked_root() {
        let parent = tempfile::tempdir().unwrap();
        let root = parent.path().join("node");
        let storage = Storage::open(&root, None).unwrap();
        let mode = std::fs::metadata(&root).unwrap().permissions().mode();
        assert_eq!(mode & 0o077, 0, "{mode:o}");
        let error = Storage::open(&root, None).unwrap_err().to_string();
        assert!(error.contains("already using"), "{error}");
        drop(storage);
        Storage::open(&root, None).unwrap();
    }

    #[test]
    fn executor_storage_refuses_shared_symlinked_or_relative_roots() {
        let parent = tempfile::tempdir().unwrap();
        let open = parent.path().join("open");
        std::fs::create_dir(&open).unwrap();
        std::fs::set_permissions(&open, std::fs::Permissions::from_mode(0o755)).unwrap();
        let error = Storage::open(&open, None).unwrap_err().to_string();
        assert!(error.contains("chmod 700"), "{error}");

        let real = parent.path().join("real");
        std::fs::create_dir(&real).unwrap();
        std::fs::set_permissions(&real, std::fs::Permissions::from_mode(0o700)).unwrap();
        let alias = parent.path().join("alias");
        std::os::unix::fs::symlink(&real, &alias).unwrap();
        assert!(Storage::open(&alias, None).is_err());

        assert!(Storage::open(Path::new("relative/root"), None).is_err());
    }

    #[test]
    fn executor_agent_dirs_refuse_unsafe_ids() {
        let parent = tempfile::tempdir().unwrap();
        let storage = Storage::open(&parent.path().join("node"), None).unwrap();
        let dir = storage.agent_dir(&AgentId::from("agent-1")).unwrap();
        assert!(dir.is_dir());
        assert!(dir.starts_with(parent.path().join("node").join(AGENTS)));
        for bad in ["..", "a/b", ""] {
            assert!(storage.agent_dir(&AgentId::from(bad)).is_err(), "{bad}");
        }
    }
}
