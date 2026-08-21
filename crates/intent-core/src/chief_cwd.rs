//! Dedicated spawn cwd for Chief-of-Staff provider children (STAB-50).
//!
//! Chief has no worktree/repo on disk, so its ACP children used to spawn with
//! `cwd=/tmp` (TS agent-factory parity). Providers that index their cwd
//! (auggie with `--allow-indexing`) can then ingest an arbitrarily large
//! shared temp dir and blow past their V8 heap cap, dying mid-turn with
//! "agent stdout closed". Chief children instead get a dedicated,
//! daemon-owned, empty directory under the data dir, created on demand.

use std::path::{Path, PathBuf};

/// Directory under the data dir that chief provider children spawn in.
pub(crate) const CHIEF_CWD_DIR_NAME: &str = "chief-cwd";

/// The chief spawn-cwd dir for a data dir: `<data_dir>/chief-cwd`.
#[must_use]
pub fn chief_cwd_root(data_dir: &Path) -> PathBuf {
    data_dir.join(CHIEF_CWD_DIR_NAME)
}

/// Create the chief spawn-cwd directory (and any missing parents). On Unix
/// every directory created here gets mode `0700` at creation time (same
/// STAB-56 convention as the agent-logs layout), so a chief child's working
/// directory is never world-readable; on other platforms this is a plain
/// `create_dir_all`. Pre-existing directories are left untouched, so the
/// call is idempotent across spawns.
///
/// # Errors
///
/// Returns the underlying I/O error if creating the directory chain fails.
pub fn create_chief_cwd_dir(dir: &Path) -> std::io::Result<()> {
    let mut builder = std::fs::DirBuilder::new();
    builder.recursive(true);
    #[cfg(unix)]
    {
        use std::os::unix::fs::DirBuilderExt;
        builder.mode(0o700);
    }
    builder.create(dir)
}

/// Remove any leftover contents of the chief spawn-cwd directory so chief
/// children always start in an EMPTY directory. Providers may scribble into
/// their cwd during a run; without a sweep those files would accumulate
/// across daemon runs and be re-indexed by `--allow-indexing` providers —
/// the same pressure the shared `/tmp` caused. The composition root calls
/// this once at startup, before any chief child spawns, so nothing inside
/// is live. A missing directory is a no-op.
///
/// # Errors
///
/// Returns the first I/O error from listing or removing entries (a missing directory is a no-op success).
pub fn sweep_chief_cwd(dir: &Path) -> std::io::Result<()> {
    if !dir.is_dir() {
        return Ok(());
    }
    for entry in std::fs::read_dir(dir)? {
        let entry = entry?;
        if entry.file_type()?.is_dir() {
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
    fn chief_cwd_root_joins_dir_name() {
        let root = chief_cwd_root(Path::new("/data"));
        assert_eq!(root, Path::new("/data").join(CHIEF_CWD_DIR_NAME));
    }

    #[test]
    fn create_chief_cwd_dir_is_idempotent() {
        let base = std::env::temp_dir().join(format!("intentd-chief-cwd-{}", uuid::Uuid::new_v4()));
        let dir = chief_cwd_root(&base);
        create_chief_cwd_dir(&dir).unwrap();
        assert!(dir.is_dir());
        // Re-creating an existing dir is a no-op, not an error.
        create_chief_cwd_dir(&dir).unwrap();
        std::fs::remove_dir_all(&base).ok();
    }

    #[cfg(unix)]
    #[test]
    fn chief_cwd_dir_created_owner_only() {
        use std::os::unix::fs::PermissionsExt;
        let base = std::env::temp_dir().join(format!("intentd-chief-cwd-{}", uuid::Uuid::new_v4()));
        let dir = chief_cwd_root(&base);
        create_chief_cwd_dir(&dir).unwrap();
        for path in [dir.as_path(), dir.parent().unwrap()] {
            let mode = std::fs::metadata(path).unwrap().permissions().mode() & 0o777;
            assert_eq!(mode, 0o700, "{} must be owner-only", path.display());
        }
        std::fs::remove_dir_all(&base).ok();
    }

    #[test]
    fn sweep_chief_cwd_empties_leftovers_and_tolerates_missing_dir() {
        let base = std::env::temp_dir().join(format!("intentd-chief-cwd-{}", uuid::Uuid::new_v4()));
        let dir = chief_cwd_root(&base);
        // Missing dir is a no-op.
        sweep_chief_cwd(&dir).unwrap();
        create_chief_cwd_dir(&dir).unwrap();
        std::fs::write(dir.join("leftover.txt"), b"scribble").unwrap();
        std::fs::create_dir_all(dir.join("nested/deep")).unwrap();
        std::fs::write(dir.join("nested/deep/file"), b"x").unwrap();
        sweep_chief_cwd(&dir).unwrap();
        assert!(dir.is_dir(), "dir itself survives the sweep");
        assert_eq!(std::fs::read_dir(&dir).unwrap().count(), 0, "emptied");
        std::fs::remove_dir_all(&base).ok();
    }
}
