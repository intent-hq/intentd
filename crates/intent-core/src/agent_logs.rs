//! Per-agent stderr log capture layout + retention sweep (STAB-53).
//!
//! ACP provider children have their stderr captured to
//! `<data_dir>/agent-logs/<agent-id>/<YYYY-MM-DD>.log` so a child that dies
//! mid-turn ("agent stdout closed") leaves a diagnosable trace. Files older
//! than [`AGENT_LOG_RETENTION_DAYS`] are pruned by [`sweep_agent_logs`],
//! driven from the daemon's existing hourly reaper cadence.

use std::path::{Path, PathBuf};
use std::time::{Duration, SystemTime};

/// Directory under the data dir that holds per-agent stderr logs.
pub(crate) const AGENT_LOGS_DIR_NAME: &str = "agent-logs";

/// Days a per-agent stderr log file is retained before the sweep deletes it.
pub const AGENT_LOG_RETENTION_DAYS: u64 = 7;

/// The agent-logs root for a data dir: `<data_dir>/agent-logs`.
pub fn agent_logs_root(data_dir: &Path) -> PathBuf {
    data_dir.join(AGENT_LOGS_DIR_NAME)
}

/// Today's (UTC) log file name for the daily-rotated per-agent stderr log,
/// e.g. `2026-07-16.log`.
pub fn current_agent_log_file_name() -> String {
    let now = time::OffsetDateTime::now_utc();
    format!(
        "{:04}-{:02}-{:02}.log",
        now.year(),
        u8::from(now.month()),
        now.day()
    )
}

/// Create the per-agent log directory (and any missing parents). On Unix
/// every directory created here gets mode `0700` at creation time (STAB-56),
/// so captured stderr — which may contain tokens, paths, or prompt fragments
/// — is never world-readable; on other platforms this is a plain
/// `create_dir_all`. Pre-existing directories are left untouched.
pub fn create_agent_log_dir(dir: &Path) -> std::io::Result<()> {
    let mut builder = std::fs::DirBuilder::new();
    builder.recursive(true);
    #[cfg(unix)]
    {
        use std::os::unix::fs::DirBuilderExt;
        builder.mode(0o700);
    }
    builder.create(dir)
}

/// Open (creating if needed) a per-agent daily log file in append mode. On
/// Unix a freshly created file gets mode `0600` at creation time (STAB-56),
/// so there is no window where it exists with umask-derived permissions;
/// pre-existing files keep their mode. On other platforms this is a plain
/// create+append open.
pub fn open_agent_log_file(path: &Path) -> std::io::Result<std::fs::File> {
    let mut options = std::fs::OpenOptions::new();
    options.create(true).append(true);
    #[cfg(unix)]
    {
        use std::os::unix::fs::OpenOptionsExt;
        options.mode(0o600);
    }
    options.open(path)
}

/// Best-effort retention sweep: delete files under `root` (the agent-logs
/// root, one subdirectory per agent) whose modification time is older than
/// `max_age`, then remove any agent directory the sweep left empty. Returns
/// the number of files deleted. A missing root is not an error (`Ok(0)`);
/// per-entry failures are skipped so one bad file never aborts the sweep.
pub fn sweep_agent_logs(root: &Path, max_age: Duration) -> std::io::Result<usize> {
    let entries = match std::fs::read_dir(root) {
        Ok(entries) => entries,
        Err(e) if e.kind() == std::io::ErrorKind::NotFound => return Ok(0),
        Err(e) => return Err(e),
    };
    let cutoff = SystemTime::now()
        .checked_sub(max_age)
        .unwrap_or(SystemTime::UNIX_EPOCH);
    let mut removed = 0;
    for agent_dir in entries.flatten() {
        let dir_path = agent_dir.path();
        let Ok(dir_ft) = agent_dir.file_type() else {
            continue;
        };
        if !dir_ft.is_dir() || dir_ft.is_symlink() {
            continue;
        }
        let Ok(files) = std::fs::read_dir(&dir_path) else {
            continue;
        };
        for file in files.flatten() {
            let path = file.path();
            let Ok(ft) = file.file_type() else { continue };
            if !ft.is_file() || ft.is_symlink() {
                continue;
            }
            let Ok(meta) = file.metadata() else { continue };
            let Ok(modified) = meta.modified() else {
                continue;
            };
            if modified < cutoff && std::fs::remove_file(&path).is_ok() {
                removed += 1;
            }
        }
        // Drop the per-agent dir when the sweep emptied it (fails harmlessly
        // when files remain).
        std::fs::remove_dir(&dir_path).ok();
    }
    Ok(removed)
}

#[cfg(test)]
mod tests {
    use super::*;

    fn temp_root() -> PathBuf {
        let root =
            std::env::temp_dir().join(format!("intentd-agent-logs-{}", uuid::Uuid::new_v4()));
        std::fs::create_dir_all(&root).unwrap();
        root
    }

    fn write_with_age(root: &Path, agent: &str, name: &str, age: Duration) -> PathBuf {
        let dir = root.join(agent);
        std::fs::create_dir_all(&dir).unwrap();
        let path = dir.join(name);
        std::fs::write(&path, "stderr line\n").unwrap();
        let mtime = SystemTime::now().checked_sub(age).unwrap();
        let file = std::fs::File::options().write(true).open(&path).unwrap();
        file.set_times(std::fs::FileTimes::new().set_modified(mtime))
            .unwrap();
        path
    }

    #[test]
    fn agent_log_sweep_prunes_files_older_than_retention() {
        let root = temp_root();
        let old = write_with_age(
            &root,
            "agent-1",
            "2026-07-01.log",
            Duration::from_secs(8 * 86400),
        );
        let fresh = write_with_age(
            &root,
            "agent-2",
            "2026-07-16.log",
            Duration::from_secs(3600),
        );

        let removed =
            sweep_agent_logs(&root, Duration::from_secs(AGENT_LOG_RETENTION_DAYS * 86400)).unwrap();

        assert_eq!(removed, 1);
        assert!(!old.exists(), "old file should be pruned");
        assert!(fresh.exists(), "fresh file must be kept");
        // The emptied agent dir is dropped; the non-empty one remains.
        assert!(!root.join("agent-1").exists());
        assert!(root.join("agent-2").exists());
        std::fs::remove_dir_all(&root).ok();
    }

    #[test]
    fn agent_log_sweep_missing_root_is_ok() {
        let missing =
            std::env::temp_dir().join(format!("intentd-agent-logs-{}", uuid::Uuid::new_v4()));
        assert_eq!(
            sweep_agent_logs(&missing, Duration::from_secs(86400)).unwrap(),
            0
        );
    }

    #[test]
    fn current_agent_log_file_name_is_daily() {
        let name = current_agent_log_file_name();
        assert_eq!(name.len(), "2026-07-16.log".len());
        assert!(name.ends_with(".log"));
    }

    #[cfg(unix)]
    #[test]
    fn agent_log_dir_created_owner_only() {
        use std::os::unix::fs::PermissionsExt;
        let base =
            std::env::temp_dir().join(format!("intentd-agent-logs-{}", uuid::Uuid::new_v4()));
        let dir = base.join("agent-logs").join("agent-1");
        create_agent_log_dir(&dir).unwrap();
        for path in [dir.as_path(), dir.parent().unwrap()] {
            let mode = std::fs::metadata(path).unwrap().permissions().mode() & 0o777;
            assert_eq!(mode, 0o700, "{} must be owner-only", path.display());
        }
        // Re-creating an existing dir is a no-op, not an error.
        create_agent_log_dir(&dir).unwrap();
        std::fs::remove_dir_all(&base).ok();
    }

    #[cfg(unix)]
    #[test]
    fn agent_log_file_created_owner_only() {
        use std::os::unix::fs::PermissionsExt;
        let dir = std::env::temp_dir().join(format!("intentd-agent-logs-{}", uuid::Uuid::new_v4()));
        create_agent_log_dir(&dir).unwrap();
        let path = dir.join("2026-07-16.log");
        drop(open_agent_log_file(&path).unwrap());
        let mode = std::fs::metadata(&path).unwrap().permissions().mode() & 0o777;
        assert_eq!(mode, 0o600, "log file must be owner-only");
        // Re-opening an existing file appends rather than failing.
        open_agent_log_file(&path).unwrap();
        // A pre-existing file keeps its mode (no retroactive chmod).
        let pre = dir.join("2026-07-15.log");
        std::fs::write(&pre, "x").unwrap();
        std::fs::set_permissions(&pre, std::fs::Permissions::from_mode(0o644)).unwrap();
        open_agent_log_file(&pre).unwrap();
        let pre_mode = std::fs::metadata(&pre).unwrap().permissions().mode() & 0o777;
        assert_eq!(pre_mode, 0o644, "pre-existing file mode is untouched");
        std::fs::remove_dir_all(&dir).ok();
    }
}
