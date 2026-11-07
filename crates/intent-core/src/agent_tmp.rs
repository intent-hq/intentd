//! Private temporary folders for agent processes.
//!
//! Agents on one host otherwise share the OS temp dir, so two agents that
//! write the same fixed name under it overwrite each other's work (a PR
//! description written to a shared temp file was posted to the wrong PR).
//! Every provider launch therefore gets a private [`AgentTempDir`], exported
//! to the child as `TMPDIR`, `TMP` and `TEMP` ([`AGENT_TMP_ENV_KEYS`]), so
//! shells, `mktemp`, Node's `os.tmpdir()`, Python's `tempfile` and Go tools
//! such as `gh` all resolve to a folder no other agent shares.
//!
//! An agent's folder is keyed by the agent ([`AgentTempDir::for_agent`]), not
//! by its process: the idle reaper, memory pressure, a model switch and a
//! daemon restart all relaunch the provider between turns, and the agent must
//! find the files it left in `$TMPDIR` on the previous turn. The folder
//! therefore outlives each process and is reclaimed by
//! [`sweep_agent_tmp_root`] once no launch holds it and it has sat unused for
//! [`AGENT_TMP_MAX_IDLE`].
//!
//! The folders live under a per-daemon root in the OS temp dir
//! ([`agent_tmp_root`]), not under the data dir: tools put Unix sockets in
//! `$TMPDIR` (tmux, ssh `ControlPath`, gpg-agent) and socket paths are capped
//! at 104 bytes on macOS, where the data dir sits under the long, space-bearing
//! `~/Library/Application Support`. The root's name hashes the data dir, so
//! each daemon has its own root; the data-dir lock means one live daemon owns
//! it, so the in-process [`IN_USE`] registry sees every live holder.

use std::collections::BTreeMap;
use std::path::{Path, PathBuf};
use std::sync::Mutex;
use std::time::{Duration, SystemTime};

/// Name prefix of the per-daemon root (`<prefix><16 hex>` in the OS temp dir).
pub const AGENT_TMP_ROOT_PREFIX: &str = "intentd-tmp-";

/// Name prefix of one folder under the root (`<prefix><12 hex>`).
pub const AGENT_TMP_DIR_PREFIX: &str = "a-";

/// Name prefix of a folder the sweep renamed aside before removing it.
const RECLAIM_PREFIX: &str = ".reclaim-";

/// How long an agent's folder may sit unused (no live launch, unmodified)
/// before [`sweep_agent_tmp_root`] reclaims it.
pub const AGENT_TMP_MAX_IDLE: Duration = Duration::from_secs(7 * 24 * 60 * 60);

/// The environment variables pointed at an agent's private temp folder.
/// `TMPDIR` is the Unix convention; `TMP` / `TEMP` cover Windows and the
/// tools that consult them first.
pub const AGENT_TMP_ENV_KEYS: [&str; 3] = ["TMPDIR", "TMP", "TEMP"];

/// Live holders of one agent folder.
#[derive(Debug, Default)]
struct Holders {
    /// Live [`AgentTempDir`]s (a relaunch can overlap the previous
    /// process's teardown).
    count: usize,
    /// The agent was deleted while a holder was live: remove the folder when
    /// the last one is dropped.
    remove_on_release: bool,
}

/// Folders held by a live [`AgentTempDir`]. The sweep skips them; it checks
/// and renames under this lock so a launch never reuses a folder the sweep
/// is removing.
static IN_USE: Mutex<BTreeMap<PathBuf, Holders>> = Mutex::new(BTreeMap::new());

fn in_use() -> std::sync::MutexGuard<'static, BTreeMap<PathBuf, Holders>> {
    IN_USE
        .lock()
        .unwrap_or_else(std::sync::PoisonError::into_inner)
}

/// The per-daemon agent temp root for `data_dir`, inside the OS temp dir.
#[must_use]
pub fn agent_tmp_root(data_dir: &Path) -> PathBuf {
    agent_tmp_root_in(&std::env::temp_dir(), data_dir)
}

/// [`agent_tmp_root`] under an explicit temp base.
#[must_use]
pub fn agent_tmp_root_in(temp_base: &Path, data_dir: &Path) -> PathBuf {
    temp_base.join(format!(
        "{AGENT_TMP_ROOT_PREFIX}{:016x}",
        fnv1a64(data_dir.as_os_str().as_encoded_bytes())
    ))
}

/// FNV-1a: stable across Rust releases, unlike `DefaultHasher`, so a
/// toolchain upgrade keeps the root (and its startup sweep) in place.
fn fnv1a64(bytes: &[u8]) -> u64 {
    let mut hash: u64 = 0xcbf2_9ce4_8422_2325;
    for byte in bytes {
        hash ^= u64::from(*byte);
        hash = hash.wrapping_mul(0x0000_0100_0000_01b3);
    }
    hash
}

/// Create one directory (parent must exist), owner-only (`0700`) on Unix.
fn create_private_dir(path: &Path) -> std::io::Result<()> {
    #[cfg(unix)]
    {
        use std::os::unix::fs::DirBuilderExt;
        std::fs::DirBuilder::new().mode(0o700).create(path)
    }
    #[cfg(not(unix))]
    {
        std::fs::create_dir(path)
    }
}

/// Create `root` owner-only, or confirm an existing one is a real directory
/// owned by this user and tighten it to owner-only. The root's name is
/// predictable and the OS temp dir is shared, so another local user could
/// pre-create it (or a symlink in its place); such a root is refused rather
/// than used.
///
/// # Errors
///
/// Returns the I/O error from creating or inspecting `root`, or a
/// `PermissionDenied` / `InvalidInput` error when an existing entry is a
/// symlink, not a directory, or owned by another user.
pub fn prepare_agent_tmp_root(root: &Path) -> std::io::Result<()> {
    match create_private_dir(root) {
        Ok(()) => return Ok(()),
        Err(e) if e.kind() == std::io::ErrorKind::AlreadyExists => {}
        Err(e) => return Err(e),
    }
    let meta = std::fs::symlink_metadata(root)?;
    if !meta.file_type().is_dir() {
        return Err(std::io::Error::new(
            std::io::ErrorKind::InvalidInput,
            format!("{} exists and is not a directory", root.display()),
        ));
    }
    #[cfg(unix)]
    {
        use std::os::unix::fs::{MetadataExt, PermissionsExt};
        // SAFETY: `geteuid` has no preconditions and cannot fail.
        let euid = unsafe { libc::geteuid() };
        if meta.uid() != euid {
            return Err(std::io::Error::new(
                std::io::ErrorKind::PermissionDenied,
                format!("{} is owned by another user", root.display()),
            ));
        }
        if meta.mode() & 0o077 != 0 {
            std::fs::set_permissions(root, std::fs::Permissions::from_mode(0o700))?;
        }
    }
    Ok(())
}

/// Reclaim folders under `root` that no live [`AgentTempDir`] holds and that
/// have sat unmodified for at least `max_idle` (the daemon runs it at startup
/// and periodically with [`AGENT_TMP_MAX_IDLE`]). Leftovers of an earlier
/// sweep's interrupted removal go regardless of age. A missing root is a
/// no-op; a root that fails [`prepare_agent_tmp_root`]'s ownership check is
/// left untouched. An entry that cannot be removed (say, a file an agent
/// created with `sudo`) is logged and skipped so it does not shield the
/// entries after it; the next sweep retries it.
///
/// # Errors
///
/// Returns the I/O error from validating or listing `root`.
pub fn sweep_agent_tmp_root(root: &Path, max_idle: Duration) -> std::io::Result<()> {
    if std::fs::symlink_metadata(root).is_err() {
        return Ok(());
    }
    prepare_agent_tmp_root(root)?;
    for entry in std::fs::read_dir(root)? {
        let path = match entry {
            Ok(entry) => entry.path(),
            Err(e) => {
                tracing::warn!(root = %root.display(), error = %e, "agent temp sweep: unreadable entry");
                continue;
            }
        };
        let reclaiming = path
            .file_name()
            .and_then(|name| name.to_str())
            .is_some_and(|name| name.starts_with(RECLAIM_PREFIX));
        let target = if reclaiming {
            Some(path)
        } else {
            claim_idle(root, &path, max_idle)
        };
        if let Some(target) = target {
            remove_entry(&target);
        }
    }
    Ok(())
}

/// Under the [`IN_USE`] lock, move `path` aside for removal when no launch
/// holds it and it is idle past `max_idle`; the slow removal then runs
/// outside the lock, and a launch for the same agent creates a fresh folder.
fn claim_idle(root: &Path, path: &Path, max_idle: Duration) -> Option<PathBuf> {
    let held = in_use();
    if held.contains_key(path) {
        return None;
    }
    let modified = std::fs::symlink_metadata(path).ok()?.modified().ok()?;
    let idle = SystemTime::now()
        .duration_since(modified)
        .unwrap_or(Duration::ZERO);
    if idle < max_idle {
        return None;
    }
    move_aside(root, path)
}

/// Rename `path` to a reclaim name under `root` (call with the [`IN_USE`]
/// lock held); `None` when it is gone or cannot be moved.
fn move_aside(root: &Path, path: &Path) -> Option<PathBuf> {
    let id = uuid::Uuid::new_v4().simple().to_string();
    let aside = root.join(format!("{RECLAIM_PREFIX}{}", &id[..12]));
    match std::fs::rename(path, &aside) {
        Ok(()) => Some(aside),
        Err(e) => {
            if e.kind() != std::io::ErrorKind::NotFound {
                tracing::warn!(path = %path.display(), error = %e, "agent temp sweep: entry not moved aside");
            }
            None
        }
    }
}

/// Remove a moved-aside folder (or file) and its contents. Best effort; the
/// sweep retries leftovers.
fn remove_entry(path: &Path) {
    let removed = match std::fs::symlink_metadata(path) {
        Ok(meta) if meta.file_type().is_dir() => std::fs::remove_dir_all(path),
        Ok(_) => std::fs::remove_file(path),
        Err(e) => Err(e),
    };
    if let Err(e) = removed {
        if e.kind() != std::io::ErrorKind::NotFound {
            tracing::warn!(path = %path.display(), error = %e, "agent temp folder not removed");
        }
    }
}

/// The folder [`AgentTempDir::for_agent`] uses for `agent_key` under `root`.
fn agent_dir_path(root: &Path, agent_key: &str) -> PathBuf {
    let hash = fnv1a64(agent_key.as_bytes()) & 0xffff_ffff_ffff;
    root.join(format!("{AGENT_TMP_DIR_PREFIX}{hash:012x}"))
}

/// Remove `agent_key`'s folder under `root` because the agent was deleted.
/// A folder a live launch still holds is removed when the last holder drops.
pub fn remove_agent_tmp_dir(root: &Path, agent_key: &str) {
    let path = agent_dir_path(root, agent_key);
    let aside = {
        let mut held = in_use();
        if let Some(holders) = held.get_mut(&path) {
            holders.remove_on_release = true;
            return;
        }
        move_aside(root, &path)
    };
    if let Some(aside) = aside {
        remove_entry(&aside);
    }
}

/// Set `path`'s modification time to now, marking the folder as recently
/// used for [`sweep_agent_tmp_root`]. Best effort.
fn touch(path: &Path) {
    #[cfg(unix)]
    if let Ok(dir) = std::fs::File::open(path) {
        let _ = dir.set_modified(SystemTime::now());
    }
    #[cfg(not(unix))]
    let _ = path;
}

/// An agent launch's private temp folder. A per-agent folder
/// ([`Self::for_agent`]) persists after drop for the agent's next launch; a
/// per-launch one ([`Self::create`]) is removed with its contents on drop.
/// The owner keeps it for the child's lifetime and drops it once the process
/// tree is gone.
#[derive(Debug)]
pub struct AgentTempDir {
    path: PathBuf,
    persistent: bool,
}

impl AgentTempDir {
    /// Create a fresh owner-only folder under `root`, which must already
    /// exist (see [`prepare_agent_tmp_root`]), removed on drop.
    ///
    /// # Errors
    ///
    /// Returns the I/O error from creating the folder.
    pub fn create(root: &Path) -> std::io::Result<Self> {
        let id = uuid::Uuid::new_v4().simple().to_string();
        let path = root.join(format!("{AGENT_TMP_DIR_PREFIX}{}", &id[..12]));
        create_private_dir(&path)?;
        Ok(Self {
            path,
            persistent: false,
        })
    }

    /// The folder `agent_key` uses on every launch under `root` (which must
    /// already exist, see [`prepare_agent_tmp_root`]): created owner-only on
    /// first use and reused, contents intact, after a relaunch. Held against
    /// [`sweep_agent_tmp_root`] until drop.
    ///
    /// # Errors
    ///
    /// Returns the I/O error from creating or inspecting the folder, or
    /// `InvalidInput` when the name is taken by something other than a
    /// directory.
    pub fn for_agent(root: &Path, agent_key: &str) -> std::io::Result<Self> {
        let path = agent_dir_path(root, agent_key);
        let mut held = in_use();
        match create_private_dir(&path) {
            Ok(()) => {}
            Err(e) if e.kind() == std::io::ErrorKind::AlreadyExists => {
                if !std::fs::symlink_metadata(&path)?.file_type().is_dir() {
                    return Err(std::io::Error::new(
                        std::io::ErrorKind::InvalidInput,
                        format!("{} exists and is not a directory", path.display()),
                    ));
                }
                touch(&path);
            }
            Err(e) => return Err(e),
        }
        held.entry(path.clone()).or_default().count += 1;
        Ok(Self {
            path,
            persistent: true,
        })
    }

    /// The folder's path.
    #[must_use]
    pub fn path(&self) -> &Path {
        &self.path
    }

    /// The `(variable, value)` pairs that point a child at this folder.
    pub fn env(&self) -> impl Iterator<Item = (&'static str, &Path)> + '_ {
        AGENT_TMP_ENV_KEYS
            .into_iter()
            .map(move |key| (key, self.path.as_path()))
    }
}

impl Drop for AgentTempDir {
    fn drop(&mut self) {
        if self.persistent {
            let aside = {
                let mut held = in_use();
                touch(&self.path);
                let Some(holders) = held.get_mut(&self.path) else {
                    return;
                };
                holders.count -= 1;
                if holders.count > 0 {
                    return;
                }
                let remove = holders.remove_on_release;
                held.remove(&self.path);
                let root = self.path.parent();
                match root {
                    Some(root) if remove => move_aside(root, &self.path),
                    _ => None,
                }
            };
            if let Some(aside) = aside {
                remove_entry(&aside);
            }
            return;
        }
        if let Err(e) = std::fs::remove_dir_all(&self.path) {
            if e.kind() != std::io::ErrorKind::NotFound {
                tracing::debug!(
                    path = %self.path.display(),
                    error = %e,
                    "failed to remove agent temp dir"
                );
            }
        }
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    fn base() -> tempfile::TempDir {
        tempfile::Builder::new()
            .prefix("intentd-agent-tmp-test-")
            .tempdir()
            .expect("temp base")
    }

    #[test]
    fn root_is_stable_per_data_dir_and_distinct_across_data_dirs() {
        let base = Path::new("/t");
        let a = agent_tmp_root_in(base, Path::new("/data/one"));
        assert_eq!(a, agent_tmp_root_in(base, Path::new("/data/one")));
        assert_ne!(a, agent_tmp_root_in(base, Path::new("/data/two")));
        let name = a.file_name().unwrap().to_str().unwrap();
        assert!(name.starts_with(AGENT_TMP_ROOT_PREFIX), "{name}");
        assert_eq!(name.len(), AGENT_TMP_ROOT_PREFIX.len() + 16, "{name}");
    }

    /// Pinned so an accidental hash change (which would orphan every
    /// existing root from its sweep) fails loudly.
    #[test]
    fn root_hash_is_pinned() {
        assert_eq!(fnv1a64(b""), 0xcbf2_9ce4_8422_2325);
        assert_eq!(fnv1a64(b"a"), 0xaf63_dc4c_8601_ec8c);
    }

    #[test]
    fn two_dirs_are_distinct_and_removed_on_drop() {
        let base = base();
        let root = base.path().join("root");
        prepare_agent_tmp_root(&root).unwrap();
        let a = AgentTempDir::create(&root).unwrap();
        let b = AgentTempDir::create(&root).unwrap();
        assert_ne!(a.path(), b.path());
        std::fs::write(a.path().join("pr-body.md"), "a").unwrap();
        std::fs::write(b.path().join("pr-body.md"), "b").unwrap();
        assert_eq!(
            std::fs::read_to_string(a.path().join("pr-body.md")).unwrap(),
            "a"
        );
        let (a_path, b_path) = (a.path().to_path_buf(), b.path().to_path_buf());
        drop(a);
        assert!(!a_path.exists());
        assert!(b_path.is_dir());
        drop(b);
        assert!(!b_path.exists());
        assert!(root.is_dir(), "the root outlives its folders");
    }

    #[test]
    fn env_points_every_key_at_the_folder() {
        let base = base();
        let root = base.path().join("root");
        prepare_agent_tmp_root(&root).unwrap();
        let dir = AgentTempDir::create(&root).unwrap();
        let env: Vec<_> = dir.env().collect();
        assert_eq!(
            env.iter().map(|(k, _)| *k).collect::<Vec<_>>(),
            AGENT_TMP_ENV_KEYS
        );
        assert!(env.iter().all(|(_, v)| *v == dir.path()));
    }

    #[test]
    fn sweep_removes_leftovers_and_keeps_root() {
        let base = base();
        let root = base.path().join("root");
        prepare_agent_tmp_root(&root).unwrap();
        let leaked = AgentTempDir::create(&root).unwrap();
        std::fs::write(leaked.path().join("f"), "x").unwrap();
        let leaked_path = leaked.path().to_path_buf();
        std::mem::forget(leaked);
        std::fs::write(root.join("stray"), "x").unwrap();
        sweep_agent_tmp_root(&root, Duration::ZERO).unwrap();
        assert!(!leaked_path.exists());
        assert_eq!(std::fs::read_dir(&root).unwrap().count(), 0);
    }

    /// An entry the sweep cannot remove (a read-only folder holding a file,
    /// like one an agent made with `sudo`) does not shield the entries after
    /// it.
    #[cfg(unix)]
    #[test]
    fn sweep_skips_unremovable_entries() {
        use std::os::unix::fs::PermissionsExt;
        let base = base();
        let root = base.path().join("root");
        prepare_agent_tmp_root(&root).unwrap();
        let stuck = root.join("a-stuck");
        std::fs::create_dir(&stuck).unwrap();
        std::fs::write(stuck.join("f"), "x").unwrap();
        std::fs::set_permissions(&stuck, std::fs::Permissions::from_mode(0o500)).unwrap();
        for i in 0..4 {
            let dir = root.join(format!("a-leftover{i}"));
            std::fs::create_dir(&dir).unwrap();
            std::fs::write(dir.join("f"), "x").unwrap();
        }
        sweep_agent_tmp_root(&root, Duration::ZERO).unwrap();
        let left: Vec<_> = std::fs::read_dir(&root)
            .unwrap()
            .map(|e| e.unwrap().path())
            .collect();
        assert!(
            left.len() <= 1
                && left.iter().all(|p| p
                    .file_name()
                    .and_then(|n| n.to_str())
                    .is_some_and(|n| n.starts_with(RECLAIM_PREFIX))),
            "only the unremovable entry, moved aside, may remain: {left:?}"
        );
        for path in &left {
            std::fs::set_permissions(path, std::fs::Permissions::from_mode(0o700)).unwrap();
        }
    }

    #[test]
    fn sweep_missing_root_is_noop() {
        let base = base();
        sweep_agent_tmp_root(&base.path().join("absent"), Duration::ZERO).unwrap();
    }

    #[cfg(unix)]
    #[test]
    fn prepare_creates_owner_only_and_tightens_existing() {
        use std::os::unix::fs::PermissionsExt;
        let base = base();
        let root = base.path().join("root");
        prepare_agent_tmp_root(&root).unwrap();
        let mode = |p: &Path| std::fs::metadata(p).unwrap().permissions().mode() & 0o777;
        assert_eq!(mode(&root), 0o700);
        std::fs::set_permissions(&root, std::fs::Permissions::from_mode(0o777)).unwrap();
        prepare_agent_tmp_root(&root).unwrap();
        assert_eq!(mode(&root), 0o700);
        let dir = AgentTempDir::create(&root).unwrap();
        assert_eq!(mode(dir.path()), 0o700);
    }

    #[cfg(unix)]
    #[test]
    fn prepare_refuses_symlink_and_file() {
        let base = base();
        let target = base.path().join("elsewhere");
        std::fs::create_dir(&target).unwrap();
        let link = base.path().join("link");
        std::os::unix::fs::symlink(&target, &link).unwrap();
        assert!(prepare_agent_tmp_root(&link).is_err());
        assert!(sweep_agent_tmp_root(&link, Duration::ZERO).is_err());
        let file = base.path().join("file");
        std::fs::write(&file, "x").unwrap();
        assert!(prepare_agent_tmp_root(&file).is_err());
    }
}
