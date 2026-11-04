//! Private temporary folders for agent processes.
//!
//! Agents on one host otherwise share the OS temp dir, so two agents that
//! write the same fixed name under it overwrite each other's work (a PR
//! description written to a shared temp file was posted to the wrong PR).
//! Every provider launch therefore gets its own [`AgentTempDir`], exported to
//! the child as `TMPDIR`, `TMP` and `TEMP` ([`AGENT_TMP_ENV_KEYS`]), so shells,
//! `mktemp`, Node's `os.tmpdir()`, Python's `tempfile` and Go tools such as
//! `gh` all resolve to a folder no other agent shares. The folder is removed
//! when its owner drops it, after the child's process tree is torn down.
//!
//! The folders live under a per-daemon root in the OS temp dir
//! ([`agent_tmp_root`]), not under the data dir: tools put Unix sockets in
//! `$TMPDIR` (tmux, ssh `ControlPath`, gpg-agent) and socket paths are capped
//! at 104 bytes on macOS, where the data dir sits under the long, space-bearing
//! `~/Library/Application Support`. The root's name hashes the data dir, so
//! each daemon has its own root; the data-dir lock means one live daemon owns
//! it, which makes the startup sweep ([`sweep_agent_tmp_root`]) safe — it
//! reclaims folders a killed daemon could not remove.

use std::path::{Path, PathBuf};

/// Name prefix of the per-daemon root (`<prefix><16 hex>` in the OS temp dir).
pub const AGENT_TMP_ROOT_PREFIX: &str = "intentd-tmp-";

/// Name prefix of one launch's folder under the root (`<prefix><12 hex>`).
pub const AGENT_TMP_DIR_PREFIX: &str = "a-";

/// The environment variables pointed at an agent's private temp folder.
/// `TMPDIR` is the Unix convention; `TMP` / `TEMP` cover Windows and the
/// tools that consult them first.
pub const AGENT_TMP_ENV_KEYS: [&str; 3] = ["TMPDIR", "TMP", "TEMP"];

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

/// Remove everything under `root` (crash recovery at daemon startup, before
/// any agent spawns). A missing root is a no-op; a root that fails
/// [`prepare_agent_tmp_root`]'s ownership check is left untouched. An entry
/// that cannot be removed (say, a file an agent created with `sudo`) is
/// logged and skipped so it does not shield the entries after it; the next
/// startup retries it.
///
/// # Errors
///
/// Returns the I/O error from validating or listing `root`.
pub fn sweep_agent_tmp_root(root: &Path) -> std::io::Result<()> {
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
        let removed = match std::fs::symlink_metadata(&path) {
            Ok(meta) if meta.file_type().is_dir() => std::fs::remove_dir_all(&path),
            Ok(_) => std::fs::remove_file(&path),
            Err(e) => Err(e),
        };
        if let Err(e) = removed {
            if e.kind() != std::io::ErrorKind::NotFound {
                tracing::warn!(path = %path.display(), error = %e, "agent temp sweep: entry not removed");
            }
        }
    }
    Ok(())
}

/// One agent launch's private temp folder, removed (with its contents) on
/// drop. The owner keeps it for the child's lifetime and drops it once the
/// process tree is gone.
#[derive(Debug)]
pub struct AgentTempDir {
    path: PathBuf,
}

impl AgentTempDir {
    /// Create a fresh owner-only folder under `root`, which must already
    /// exist (see [`prepare_agent_tmp_root`]).
    ///
    /// # Errors
    ///
    /// Returns the I/O error from creating the folder.
    pub fn create(root: &Path) -> std::io::Result<Self> {
        let id = uuid::Uuid::new_v4().simple().to_string();
        let path = root.join(format!("{AGENT_TMP_DIR_PREFIX}{}", &id[..12]));
        create_private_dir(&path)?;
        Ok(Self { path })
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
        sweep_agent_tmp_root(&root).unwrap();
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
        sweep_agent_tmp_root(&root).unwrap();
        let left: Vec<_> = std::fs::read_dir(&root)
            .unwrap()
            .map(|e| e.unwrap().path())
            .collect();
        assert!(
            left.iter().all(|p| *p == stuck),
            "only the unremovable entry may remain: {left:?}"
        );
        if stuck.exists() {
            std::fs::set_permissions(&stuck, std::fs::Permissions::from_mode(0o700)).unwrap();
        }
    }

    #[test]
    fn sweep_missing_root_is_noop() {
        let base = base();
        sweep_agent_tmp_root(&base.path().join("absent")).unwrap();
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
        assert!(sweep_agent_tmp_root(&link).is_err());
        let file = base.path().join("file");
        std::fs::write(&file, "x").unwrap();
        assert!(prepare_agent_tmp_root(&file).is_err());
    }
}
