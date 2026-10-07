//! Daemon-owned directory for per-agent generated config files.
//!
//! `create_agent` writes per-agent files (`intentd-mcp-*.json` `--mcp-config`,
//! `intentd-rules-*.md` `--rules`, and the pi-extension delivery pair
//! `intentd-pi-ext-*.ts` / `intentd-pi-wrapper-*.sh`). They are RAII-deleted
//! when the owning agent handle drops, but a daemon killed before drop leaks
//! them — and in the global OS temp dir nothing ever reclaims them
//! (monorepo#1302 class (a)). Placing them under `<data_dir>/agent-configs`
//! makes them daemon-owned, so a startup sweep (before any agent spawns, when
//! nothing inside is live) reclaims whatever a previous run left behind.
//!
//! The one exception is an npx launch dir ([`NPX_LAUNCH_DIR_PREFIX`]): the
//! same root hosts the neutral directory each npx provider launch starts in
//! (intent-hq/intent#5738), and a daemon whose runtime shut down before the
//! bounded process-tree kill finished retains that directory on purpose —
//! npx / Node descendants may still be running in it, and they fail on a
//! removed cwd. "Nothing inside is live" therefore does not hold for it at
//! the next start, so [`sweep_agent_configs`] leaves it alone.

use std::path::{Path, PathBuf};

/// Directory under the data dir that holds per-agent generated config files.
pub(crate) const AGENT_CONFIGS_DIR_NAME: &str = "agent-configs";

/// File-name prefix of the neutral directory an npx provider launch starts
/// in (`<prefix><uuid>`, created under the agent-configs root). Directories
/// so named are skipped by [`sweep_agent_configs`]: one may be the cwd of a
/// process tree the previous run could not finish killing, and nothing at
/// startup can tell a retained live tree from a dead one without trusting
/// reused pids. Each holds a single sentinel `package.json`, so a retained
/// orphan costs one small directory per interrupted teardown.
pub const NPX_LAUNCH_DIR_PREFIX: &str = "intentd-npx-";

/// Whether `name` is the file name of an npx launch dir a previous run may
/// have retained for a possibly live process tree.
#[must_use]
pub fn is_npx_launch_dir_name(name: &std::ffi::OsStr) -> bool {
    name.as_encoded_bytes()
        .starts_with(NPX_LAUNCH_DIR_PREFIX.as_bytes())
}

/// The agent-configs root for a data dir: `<data_dir>/agent-configs`.
#[must_use]
pub fn agent_configs_root(data_dir: &Path) -> PathBuf {
    data_dir.join(AGENT_CONFIGS_DIR_NAME)
}

/// Create the agent-configs directory (and any missing parents). On Unix
/// every directory created here gets mode `0700` at creation time (same
/// STAB-56 convention as the agent-logs layout), since the generated configs
/// may carry bridge endpoints and assembled prompts; on other platforms this
/// is a plain `create_dir_all`. Pre-existing directories are left untouched,
/// so the call is idempotent across spawns.
///
/// # Errors
///
/// Returns the underlying I/O error if creating the directory chain fails.
pub fn create_agent_configs_dir(dir: &Path) -> std::io::Result<()> {
    let mut builder = std::fs::DirBuilder::new();
    builder.recursive(true);
    #[cfg(unix)]
    {
        use std::os::unix::fs::DirBuilderExt;
        builder.mode(0o700);
    }
    builder.create(dir)
}

/// Remove any leftover contents of the agent-configs directory (crash
/// recovery): files there are per-agent and RAII-deleted on handle drop, so
/// anything present at daemon startup — before any agent spawns — was leaked
/// by a previous run that died before drop. A missing directory is a no-op.
///
/// Npx launch dirs ([`is_npx_launch_dir_name`]) are retained, not leaked:
/// the previous run kept one on purpose when its process-tree kill could not
/// finish, and its descendants may still be running in it (see the module
/// doc for the disk cost).
///
/// # Errors
///
/// Returns the first I/O error from listing or removing entries (a missing directory is a no-op success).
pub fn sweep_agent_configs(dir: &Path) -> std::io::Result<()> {
    if !dir.is_dir() {
        return Ok(());
    }
    for entry in std::fs::read_dir(dir)? {
        let entry = entry?;
        if entry.file_type()?.is_dir() {
            if is_npx_launch_dir_name(&entry.file_name()) {
                tracing::debug!(
                    path = %entry.path().display(),
                    "keeping npx launch dir retained by a previous run"
                );
                continue;
            }
            std::fs::remove_dir_all(entry.path())?;
        } else {
            std::fs::remove_file(entry.path())?;
        }
    }
    Ok(())
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn agent_configs_root_joins_dir_name() {
        let root = agent_configs_root(Path::new("/data"));
        assert_eq!(root, Path::new("/data").join(AGENT_CONFIGS_DIR_NAME));
    }

    #[test]
    fn create_agent_configs_dir_is_idempotent() {
        let base = std::env::temp_dir().join(format!("intentd-agent-cfg-{}", uuid::Uuid::new_v4()));
        let dir = agent_configs_root(&base);
        create_agent_configs_dir(&dir).unwrap();
        assert!(dir.is_dir());
        // Re-creating an existing dir is a no-op, not an error.
        create_agent_configs_dir(&dir).unwrap();
        std::fs::remove_dir_all(&base).ok();
    }

    #[cfg(unix)]
    #[test]
    fn agent_configs_dir_created_owner_only() {
        use std::os::unix::fs::PermissionsExt;
        let base = std::env::temp_dir().join(format!("intentd-agent-cfg-{}", uuid::Uuid::new_v4()));
        let dir = agent_configs_root(&base);
        create_agent_configs_dir(&dir).unwrap();
        for path in [dir.as_path(), dir.parent().unwrap()] {
            let mode = std::fs::metadata(path).unwrap().permissions().mode() & 0o777;
            assert_eq!(mode, 0o700, "{} must be owner-only", path.display());
        }
        std::fs::remove_dir_all(&base).ok();
    }

    #[test]
    fn sweep_removes_files_and_dirs() {
        let base = std::env::temp_dir().join(format!("intentd-agent-cfg-{}", uuid::Uuid::new_v4()));
        let dir = agent_configs_root(&base);
        create_agent_configs_dir(&dir).unwrap();
        std::fs::write(dir.join("intentd-mcp-stale.json"), b"{}").unwrap();
        std::fs::create_dir(dir.join("stale-dir")).unwrap();
        sweep_agent_configs(&dir).unwrap();
        assert_eq!(std::fs::read_dir(&dir).unwrap().count(), 0);
        std::fs::remove_dir_all(&base).ok();
    }

    /// A launch dir retained by a previous run (its process-tree kill never
    /// finished, so npx descendants may still run in it) survives the sweep;
    /// ordinary leftovers beside it do not.
    #[test]
    fn sweep_retains_npx_launch_dirs() {
        let base = std::env::temp_dir().join(format!("intentd-agent-cfg-{}", uuid::Uuid::new_v4()));
        let dir = agent_configs_root(&base);
        create_agent_configs_dir(&dir).unwrap();
        let retained = dir.join(format!("{NPX_LAUNCH_DIR_PREFIX}{}", uuid::Uuid::new_v4()));
        std::fs::create_dir(&retained).unwrap();
        std::fs::write(retained.join("package.json"), b"{}").unwrap();
        std::fs::write(dir.join("intentd-mcp-stale.json"), b"{}").unwrap();
        std::fs::create_dir(dir.join("stale-dir")).unwrap();
        sweep_agent_configs(&dir).unwrap();
        let left: Vec<PathBuf> = std::fs::read_dir(&dir)
            .unwrap()
            .map(|e| e.unwrap().path())
            .collect();
        assert_eq!(left, vec![retained.clone()], "only the launch dir survives");
        assert!(retained.join("package.json").is_file());
        std::fs::remove_dir_all(&base).ok();
    }

    #[test]
    fn sweep_missing_dir_is_noop() {
        let base = std::env::temp_dir().join(format!("intentd-agent-cfg-{}", uuid::Uuid::new_v4()));
        sweep_agent_configs(&agent_configs_root(&base)).unwrap();
    }
}
