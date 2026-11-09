//! One backend's private storage on this machine,
//! `<data_dir>/executor/<credentialId>/`: its journal, checkout caches and
//! per-agent records. Remote agents never get rows in the daemon's store.
//!
//! An agent's home (worktree, temp folder, setup files) lives in a short
//! work dir without spaces, `~/.intentd/w/<8 hex of the credential>/<12 hex
//! of the agent>` (`INTENTD_EXECUTOR_WORK_DIR` overrides the base), because
//! a data dir such as `~/Library/Application Support/...` breaks tools that
//! split paths on spaces. The record `agents/<agentId>/home` names it. An
//! agent recorded before the work dir keeps `agents/<agentId>/` as its home.
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
const REPOS: &str = "repos";
const GIT_SOCKET: &str = "git.sock";
const PULLS: &str = "pulls";
const HOME: &str = "home";
const WORK: &str = "work";
const TMP: &str = "tmp";

/// Overrides the base of the agents' work dir (`~/.intentd/w`).
pub(super) const WORK_DIR_ENV: &str = "INTENTD_EXECUTOR_WORK_DIR";

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
    let beside = executor_dir.join(format!("{}.sock", &credential[..credential.len().min(12)]));
    if beside.as_os_str().len() <= MAX_SOCKET_PATH {
        return Ok(beside);
    }
    let dir = private_socket_dir(runtime_base)?;
    let socket = dir.join(format!("{}.sock", short_hash(executor_dir, credential)));
    ensure!(
        socket.as_os_str().len() <= MAX_SOCKET_PATH,
        "the executor's git socket path {} is longer than a Unix socket allows \
         ({MAX_SOCKET_PATH} bytes); use a shorter XDG_RUNTIME_DIR",
        socket.display()
    );
    Ok(socket)
}

/// The user's private socket directory under `runtime_base`, closed to
/// group and others.
fn private_socket_dir(runtime_base: &Path) -> anyhow::Result<PathBuf> {
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
    Ok(dir)
}

/// 16 hex digits of the SHA-256 of `path` and `id`.
fn short_hash(path: &Path, id: &str) -> String {
    use sha2::Digest as _;
    use std::os::unix::ffi::OsStrExt as _;

    let mut hasher = sha2::Sha256::new();
    hasher.update(path.as_os_str().as_bytes());
    hasher.update([0]);
    hasher.update(id.as_bytes());
    hex(&hasher.finalize()[..8])
}

/// Where a sub-agent's own forge helper socket goes (nodeProtocol 17):
/// beside the machine's `git_socket` when that path fits, else in the
/// private directory under `runtime_base`.
pub(super) fn forge_socket_path(
    git_socket: &Path,
    agent_id: &str,
    runtime_base: &Path,
) -> anyhow::Result<PathBuf> {
    let name = format!("f{}.sock", short_hash(git_socket, agent_id));
    if let Some(parent) = git_socket.parent() {
        let beside = parent.join(&name);
        if beside.as_os_str().len() <= MAX_SOCKET_PATH {
            return Ok(beside);
        }
    }
    let socket = private_socket_dir(runtime_base)?.join(name);
    ensure!(
        socket.as_os_str().len() <= MAX_SOCKET_PATH,
        "the agent's git socket path {} is longer than a Unix socket allows",
        socket.display()
    );
    Ok(socket)
}

/// The base of the agents' work dir: `override_dir`
/// (`INTENTD_EXECUTOR_WORK_DIR`) or `<home>/.intentd/w`. `None` without an
/// absolute one.
pub(super) fn work_base(
    override_dir: Option<&std::ffi::OsStr>,
    home: Option<&Path>,
) -> Option<PathBuf> {
    let base = match override_dir.filter(|dir| !dir.is_empty()) {
        Some(dir) => PathBuf::from(dir),
        None => home?.join(".intentd").join("w"),
    };
    base.is_absolute().then_some(base)
}

/// `credential`'s work dir under `base`: 8 hex digits of its id.
pub(super) fn work_dir(base: &Path, credential: &str) -> PathBuf {
    base.join(hex_digest(credential.as_bytes(), 4))
}

/// The first `bytes` bytes of the SHA-256 of `data`, in hex.
fn hex_digest(data: &[u8], bytes: usize) -> String {
    use sha2::Digest as _;
    hex(&sha2::Sha256::digest(data)[..bytes])
}

fn hex(bytes: &[u8]) -> String {
    bytes.iter().fold(String::new(), |mut name, b| {
        use std::fmt::Write as _;
        let _ = write!(name, "{b:02x}");
        name
    })
}

/// The home named by the agent record `record`, if it names a safe one.
fn read_home(record: &Path) -> Option<PathBuf> {
    use std::os::unix::ffi::OsStrExt as _;
    let body = std::fs::read(record.join(HOME)).ok()?;
    let home = PathBuf::from(std::ffi::OsStr::from_bytes(&body));
    let name = home.file_name()?.to_str()?;
    (home.is_absolute() && valid_id(name)).then_some(home)
}

/// Make a new owner-only home for `agent_id` in `work`: named by 12 hex
/// digits of its id, or by the whole id when that name is taken.
fn reserve_home(work: &Path, agent_id: &AgentId) -> anyhow::Result<PathBuf> {
    let short = work.join(hex_digest(agent_id.0.as_bytes(), 6));
    let create = |dir: &Path| std::fs::DirBuilder::new().mode(0o700).create(dir);
    match create(&short) {
        Ok(()) => return Ok(short),
        Err(e) if e.kind() == std::io::ErrorKind::AlreadyExists => {}
        Err(e) => return Err(e).with_context(|| format!("create {}", short.display())),
    }
    let full = work.join(&agent_id.0);
    // A home under the whole id that no record names is this agent's own,
    // left by a crash: it starts over.
    remove_dir(&full)?;
    create(&full).with_context(|| format!("create {}", full.display()))?;
    Ok(full)
}

/// Remove `dir`; a missing one is already removed.
fn remove_dir(dir: &Path) -> anyhow::Result<()> {
    match std::fs::remove_dir_all(dir) {
        Ok(()) => Ok(()),
        Err(e) if e.kind() == std::io::ErrorKind::NotFound => Ok(()),
        Err(e) => Err(e).with_context(|| format!("remove {}", dir.display())),
    }
}

/// Remove the homes the agent records under `root` name, before the root
/// itself goes (a revoked credential's purge). Best effort.
pub(super) fn remove_agent_homes(root: &Path) {
    let Ok(entries) = std::fs::read_dir(root.join(AGENTS)) else {
        return;
    };
    for entry in entries.filter_map(std::result::Result::ok) {
        if let Some(home) = read_home(&entry.path()) {
            if let Err(error) = remove_dir(&home) {
                tracing::warn!(%error, "executor: could not remove an agent's home");
            }
        }
    }
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
    /// Where new agents' homes go; `None` keeps them in `agents/`.
    work: Option<PathBuf>,
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
            work: None,
            _lock: lock,
        })
    }

    pub(super) fn journal_dir(&self) -> PathBuf {
        self.root.join(JOURNAL)
    }

    /// Where new agents' homes go, instead of the root's `agents/`.
    pub(super) fn with_work_dir(mut self, work: Option<PathBuf>) -> Self {
        self.work = work;
        self
    }

    fn record_dir(&self, agent_id: &AgentId) -> PathBuf {
        self.root.join(AGENTS).join(&agent_id.0)
    }

    /// `agent_id`'s home, if it has one: the directory named by its
    /// record's `home` file, else (an agent from before the work dir) the
    /// record itself.
    fn find_home(&self, agent_id: &AgentId) -> Option<PathBuf> {
        if !valid_id(&agent_id.0) {
            return None;
        }
        let record = self.record_dir(agent_id);
        read_home(&record).or_else(|| record.is_dir().then_some(record))
    }

    /// `agent_id`'s home, made on first use: owner-only, in the work dir
    /// when there is one. It holds the worktree (`work`), the temp folder
    /// and the setup files.
    fn home(&self, agent_id: &AgentId) -> anyhow::Result<PathBuf> {
        ensure!(
            valid_id(&agent_id.0),
            "agent id is not a safe path component"
        );
        if let Some(home) = self.find_home(agent_id) {
            return Ok(home);
        }
        let record = self.record_dir(agent_id);
        let private = || {
            let mut builder = std::fs::DirBuilder::new();
            builder.recursive(true).mode(0o700);
            builder
        };
        let Some(work) = &self.work else {
            private()
                .create(&record)
                .context("create agent directory")?;
            return Ok(record);
        };
        private()
            .create(work)
            .with_context(|| format!("create the work dir {}", work.display()))?;
        let metadata = std::fs::symlink_metadata(work)
            .with_context(|| format!("inspect the work dir {}", work.display()))?;
        ensure!(
            metadata.is_dir() && metadata.uid() == nix::unistd::getuid().as_raw(),
            "the work dir {} must be a directory owned by the user running intentd",
            work.display()
        );
        ensure!(
            metadata.mode().trailing_zeros() >= 6,
            "the work dir {} must not be accessible to group or others (chmod 700)",
            work.display()
        );
        let home = reserve_home(work, agent_id)?;
        private()
            .create(&record)
            .context("create agent directory")?;
        let pointer = record.join(HOME);
        let scratch = record.join("home.tmp");
        std::fs::write(&scratch, std::os::unix::ffi::OsStrExt::as_bytes(home.as_os_str()))
            .and_then(|()| std::fs::rename(&scratch, &pointer))
            .context("record the agent's home")?;
        Ok(home)
    }

    /// Where `agent_id`'s git worktree lives. Its parent exists; the
    /// worktree itself is created by `git worktree add`.
    pub(super) fn agent_worktree(&self, agent_id: &AgentId) -> anyhow::Result<PathBuf> {
        Ok(self.home(agent_id)?.join(WORK))
    }

    /// Where `agent_id`'s worktree lives, if it has a home already.
    pub(super) fn existing_worktree(&self, agent_id: &AgentId) -> Option<PathBuf> {
        Some(self.find_home(agent_id)?.join(WORK))
    }

    /// `agent_id`'s private temp folder, created owner-only on demand
    /// beside its worktree, so [`Self::remove_agent`] removes it with the
    /// checkout.
    pub(super) fn agent_tmp(&self, agent_id: &AgentId) -> anyhow::Result<PathBuf> {
        let dir = self.home(agent_id)?.join(TMP);
        std::fs::DirBuilder::new()
            .recursive(true)
            .mode(0o700)
            .create(&dir)
            .context("create the agent's temp folder")?;
        Ok(dir)
    }

    /// The agents with a directory under `agents/`; names that are not safe
    /// ids are left alone.
    pub(super) fn agent_ids(&self) -> Vec<AgentId> {
        let Ok(entries) = std::fs::read_dir(self.root.join(AGENTS)) else {
            return Vec::new();
        };
        let mut ids: Vec<AgentId> = entries
            .filter_map(std::result::Result::ok)
            .filter(|entry| entry.file_type().is_ok_and(|kind| kind.is_dir()))
            .filter_map(|entry| entry.file_name().into_string().ok())
            .filter(|name| valid_id(name))
            .map(AgentId::from)
            .collect();
        ids.sort_by(|a, b| a.0.cmp(&b.0));
        ids
    }

    /// Remove `agent_id`'s home (its worktree included), then its record;
    /// a missing one is already removed.
    pub(super) fn remove_agent(&self, agent_id: &AgentId) -> anyhow::Result<()> {
        ensure!(
            valid_id(&agent_id.0),
            "agent id is not a safe path component"
        );
        let record = self.record_dir(agent_id);
        if let Some(home) = read_home(&record) {
            remove_dir(&home)?;
        }
        remove_dir(&record)
    }

    /// The machine-local stand-in for head's main checkout of the
    /// repository `repo_key`, created empty: a setup script's
    /// `MAIN_CHECKOUT`, where the machine's owner can put untracked files.
    pub(super) fn main_checkout(&self, repo_key: &str) -> anyhow::Result<PathBuf> {
        ensure!(valid_id(repo_key), "repo key is not a safe path component");
        let dir = self.root.join(REPOS).join(repo_key).join("main");
        std::fs::DirBuilder::new()
            .recursive(true)
            .mode(0o700)
            .create(&dir)
            .context("create the main checkout directory")?;
        Ok(dir)
    }

    /// Where folders another agent pulls from this machine are packed
    /// (`ws.file.pull`, nodeProtocol 16), outside every checkout. Emptied
    /// here: a packed folder never outlives its daemon.
    pub(super) fn pull_staging_dir(&self) -> PathBuf {
        let dir = self.root.join(PULLS);
        let _ = std::fs::remove_dir_all(&dir);
        dir
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

    /// `agent_id`'s test working directory, if [`Self::agent_dir`] made it.
    #[cfg(test)]
    pub(super) fn existing_agent_dir(&self, agent_id: &AgentId) -> Option<PathBuf> {
        self.existing_worktree(agent_id).filter(|dir| dir.is_dir())
    }

    /// A fresh working directory for `agent_id`, created on demand.
    #[cfg(test)]
    pub(super) fn agent_dir(&self, agent_id: &AgentId) -> anyhow::Result<PathBuf> {
        let dir = self.agent_worktree(agent_id)?;
        std::fs::DirBuilder::new()
            .mode(0o700)
            .create(&dir)
            .or_else(|e| {
                if e.kind() == std::io::ErrorKind::AlreadyExists {
                    Ok(())
                } else {
                    Err(e)
                }
            })
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
    fn the_work_dir_is_short_and_has_no_spaces_on_macos_style_homes() {
        let credential = "0f6c1b52-9a3e-4d7a-b7f0-3c2e8d1a9b64";
        let home = Path::new("/Users/username");
        let base = work_base(None, Some(home)).unwrap();
        assert_eq!(base, Path::new("/Users/username/.intentd/w"));
        assert_eq!(
            work_base(Some("/opt/w".as_ref()), Some(home)),
            Some(PathBuf::from("/opt/w"))
        );
        assert!(work_base(Some("relative".as_ref()), None).is_none());
        assert!(work_base(None, None).is_none());
        let work = work_dir(&base, credential);
        assert_eq!(work, base.join(hex_digest(credential.as_bytes(), 4)));

        let parent = tempfile::tempdir().unwrap();
        let storage = Storage::open(&parent.path().join("Application Support/node"), None)
            .unwrap()
            .with_work_dir(Some(parent.path().join("w").join("0f6c1b52")));
        let agent = AgentId::from("agent-081a6de9-213a-4f30-9fb7-6df75e5be8e3");
        let worktree = storage.agent_worktree(&agent).unwrap();
        let name = worktree.strip_prefix(parent.path().join("w")).unwrap();
        let on_mac = Path::new("/Users/username/.intentd/w").join(name);
        assert!(on_mac.as_os_str().len() <= 60, "{}", on_mac.display());
        assert!(!worktree.to_string_lossy().contains(' '));
        assert!(worktree.parent().unwrap().is_dir());
        assert_eq!(storage.agent_worktree(&agent).unwrap(), worktree, "stable");
        let tmp = storage.agent_tmp(&agent).unwrap();
        assert_eq!(tmp.parent(), worktree.parent());
        let mode = std::fs::metadata(worktree.parent().unwrap())
            .unwrap()
            .permissions()
            .mode();
        assert_eq!(mode & 0o077, 0, "{mode:o}");
        assert_eq!(storage.agent_ids(), vec![agent]);
    }

    fn private_dirs(path: &Path) {
        use std::os::unix::fs::DirBuilderExt;
        std::fs::DirBuilder::new()
            .recursive(true)
            .mode(0o700)
            .create(path)
            .unwrap();
    }

    #[test]
    fn a_taken_short_name_falls_back_to_the_full_agent_id() {
        let parent = tempfile::tempdir().unwrap();
        let work = parent.path().join("w").join("cred");
        let storage = Storage::open(&parent.path().join("node"), None)
            .unwrap()
            .with_work_dir(Some(work.clone()));
        let agent = AgentId::from("agent-1");
        private_dirs(&work.join(hex_digest(b"agent-1", 6)));
        let worktree = storage.agent_worktree(&agent).unwrap();
        assert_eq!(worktree, work.join("agent-1").join(WORK));
    }

    #[test]
    fn a_legacy_checkout_in_the_data_dir_is_still_used() {
        let parent = tempfile::tempdir().unwrap();
        let root = parent.path().join("node");
        let legacy = root.join(AGENTS).join("a1").join(WORK);
        private_dirs(&legacy.join(".git"));
        let storage = Storage::open(&root, None)
            .unwrap()
            .with_work_dir(Some(parent.path().join("w")));
        let agent = AgentId::from("a1");
        assert_eq!(storage.existing_worktree(&agent), Some(legacy.clone()));
        assert_eq!(storage.agent_worktree(&agent).unwrap(), legacy);
        assert_eq!(
            storage.agent_tmp(&agent).unwrap(),
            root.join(AGENTS).join("a1").join(TMP)
        );
        assert!(!parent.path().join("w").exists(), "no short home is made");
        storage.remove_agent(&agent).unwrap();
        assert!(!root.join(AGENTS).join("a1").exists());
    }

    #[test]
    fn removing_an_agent_cleans_its_short_home_and_its_record() {
        let parent = tempfile::tempdir().unwrap();
        let root = parent.path().join("node");
        let storage = Storage::open(&root, None)
            .unwrap()
            .with_work_dir(Some(parent.path().join("w")));
        let agent = AgentId::from("a1");
        assert_eq!(storage.existing_worktree(&agent), None);
        let worktree = storage.agent_worktree(&agent).unwrap();
        std::fs::create_dir_all(worktree.join(".git")).unwrap();
        assert_eq!(storage.existing_worktree(&agent), Some(worktree.clone()));
        storage.remove_agent(&agent).unwrap();
        assert!(!worktree.parent().unwrap().exists());
        assert!(!root.join(AGENTS).join("a1").exists());
        assert!(storage.agent_ids().is_empty());
        storage.remove_agent(&agent).unwrap();
    }

    #[test]
    fn purging_a_root_removes_the_short_homes_it_records() {
        let parent = tempfile::tempdir().unwrap();
        let root = parent.path().join("node");
        let storage = Storage::open(&root, None)
            .unwrap()
            .with_work_dir(Some(parent.path().join("w")));
        let home = storage.agent_tmp(&AgentId::from("a1")).unwrap();
        let home = home.parent().unwrap().to_owned();
        drop(storage);
        remove_agent_homes(&root);
        assert!(!home.exists());
        assert!(root.join(AGENTS).join("a1").exists(), "the root is the caller's");
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
