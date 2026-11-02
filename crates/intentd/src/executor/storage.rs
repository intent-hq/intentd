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

/// Longest Unix socket path in bytes, without the trailing NUL
/// (`sun_path` holds 104 bytes on macOS, 108 on Linux).
#[cfg(target_os = "macos")]
pub(super) const MAX_SOCKET_PATH: usize = 103;
#[cfg(not(target_os = "macos"))]
pub(super) const MAX_SOCKET_PATH: usize = 107;

/// Where per-user runtime files go: `$XDG_RUNTIME_DIR` when set, else
/// `/tmp`.
pub(super) fn runtime_base() -> PathBuf {
    std::env::var_os("XDG_RUNTIME_DIR")
        .map(PathBuf::from)
        .filter(|dir| dir.is_absolute())
        .unwrap_or_else(|| PathBuf::from("/tmp"))
}

/// Where `credential`'s git helper socket goes: next to its root in
/// `executor_dir`, named by a prefix of the credential id, when that path
/// fits a Unix socket address; otherwise (a deep data dir) in a private
/// per-user directory under `runtime_base`, named by a hash of the data
/// dir and the whole credential id, so its length no longer depends on the data dir. The socket
/// itself is unauthenticated, so that directory must be the user's own and
/// closed to group and others.
pub(super) fn git_socket_path(
    executor_dir: &Path,
    credential: &str,
    runtime_base: &Path,
) -> anyhow::Result<PathBuf> {
    use sha2::Digest as _;
    use std::os::unix::ffi::OsStrExt as _;

    let beside = executor_dir.join(format!("{}.sock", &credential[..credential.len().min(12)]));
    if beside.as_os_str().len() <= MAX_SOCKET_PATH {
        return Ok(beside);
    }
    let uid = nix::unistd::getuid();
    let dir = runtime_base.join(format!("intentd-{uid}"));
    match std::fs::DirBuilder::new().mode(0o700).create(&dir) {
        Ok(()) => {}
        Err(e) if e.kind() == std::io::ErrorKind::AlreadyExists => {}
        Err(e) => {
            return Err(e).with_context(|| format!("create socket directory {}", dir.display()))
        }
    }
    let metadata = std::fs::symlink_metadata(&dir)
        .with_context(|| format!("inspect socket directory {}", dir.display()))?;
    ensure!(
        metadata.is_dir() && metadata.uid() == uid.as_raw(),
        "socket directory {} must be a directory owned by the user running intentd",
        dir.display()
    );
    ensure!(
        metadata.mode().trailing_zeros() >= 6,
        "socket directory {} must not be accessible to group or others (chmod 700)",
        dir.display()
    );
    let mut hasher = sha2::Sha256::new();
    hasher.update(executor_dir.as_os_str().as_bytes());
    hasher.update([0]);
    hasher.update(credential.as_bytes());
    let digest = hasher.finalize();
    let name = digest[..8].iter().fold(String::new(), |mut name, b| {
        use std::fmt::Write as _;
        let _ = write!(name, "{b:02x}");
        name
    });
    let socket = dir.join(format!("{name}.sock"));
    ensure!(
        socket.as_os_str().len() <= MAX_SOCKET_PATH,
        "the executor's git socket path {} is longer than a Unix socket allows \
         ({MAX_SOCKET_PATH} bytes); use a shorter XDG_RUNTIME_DIR",
        socket.display()
    );
    Ok(socket)
}

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

impl Drop for Storage {
    /// The socket may sit in a shared runtime directory: remove it with
    /// its backend, while the root's lock still keeps other daemons out.
    fn drop(&mut self) {
        let _ = std::fs::remove_file(&self.git_socket);
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
    fn the_git_socket_sits_beside_the_root_when_the_path_fits() {
        let runtime = tempfile::tempdir().unwrap();
        let executor = Path::new("/data/executor");
        let socket = git_socket_path(executor, "0123456789abcdef", runtime.path()).unwrap();
        assert_eq!(socket, executor.join("0123456789ab.sock"));
        assert_eq!(
            std::fs::read_dir(runtime.path()).unwrap().count(),
            0,
            "no private directory is needed"
        );
    }

    #[test]
    fn a_long_data_dir_moves_the_git_socket_to_a_short_private_path() {
        let runtime = tempfile::tempdir().unwrap();
        let mut executor = PathBuf::from("/data");
        while executor.as_os_str().len() < 200 {
            executor.push("a-long-segment");
        }
        let socket = git_socket_path(&executor, "0123456789abcdef", runtime.path()).unwrap();
        assert!(
            socket.as_os_str().len() <= MAX_SOCKET_PATH,
            "{}",
            socket.display()
        );
        let dir = socket.parent().unwrap();
        assert!(dir.starts_with(runtime.path()), "{}", socket.display());
        let mode = std::fs::metadata(dir).unwrap().permissions().mode();
        assert_eq!(mode & 0o077, 0, "{mode:o}");
        // Stable for one data dir and credential; distinct across either.
        assert_eq!(
            git_socket_path(&executor, "0123456789abcdef", runtime.path()).unwrap(),
            socket
        );
        assert_ne!(
            git_socket_path(&executor, "0123456789abcdeX", runtime.path()).unwrap(),
            socket
        );
        assert_ne!(
            git_socket_path(&executor.join("other"), "0123456789abcdef", runtime.path()).unwrap(),
            socket
        );
    }

    #[test]
    fn a_shared_runtime_socket_dir_is_refused() {
        let runtime = tempfile::tempdir().unwrap();
        let mut executor = PathBuf::from("/data");
        while executor.as_os_str().len() < 200 {
            executor.push("a-long-segment");
        }
        let socket = git_socket_path(&executor, "0123456789abcdef", runtime.path()).unwrap();
        let dir = socket.parent().unwrap();
        std::fs::set_permissions(dir, std::fs::Permissions::from_mode(0o755)).unwrap();
        let error = git_socket_path(&executor, "0123456789abcdef", runtime.path())
            .unwrap_err()
            .to_string();
        assert!(error.contains("chmod 700"), "{error}");

        let elsewhere = tempfile::tempdir().unwrap();
        std::fs::remove_dir(dir).unwrap();
        std::os::unix::fs::symlink(elsewhere.path(), dir).unwrap();
        assert!(git_socket_path(&executor, "0123456789abcdef", runtime.path()).is_err());
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
