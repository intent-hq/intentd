//! Failure modes: an unparseable version must not update; a current agy must
//! not update; an old agy updates once, even with concurrent callers; a failed
//! update or one that leaves agy old returns an actionable error and is not
//! retried at once; a hanging update times out.

use std::os::unix::fs::PermissionsExt;
use std::path::{Path, PathBuf};
use std::time::Duration;

use super::ensure_current_with;

/// A fake `agy` whose version lives in `version` and whose `update` writes
/// `next` (when set) and exits with `exit`.
fn fake_agy(dir: &Path, version: &str, next: Option<&str>, exit: i32) -> PathBuf {
    let bin = dir.join("agy");
    std::fs::write(dir.join("version"), version).unwrap();
    let next = next.map_or(String::new(), |next| {
        format!("printf '%s' '{next}' > '{}/version'\n", dir.display())
    });
    std::fs::write(
        &bin,
        format!(
            "#!/bin/sh\ncase \"$1\" in\n--version) cat '{d}/version'; echo ;;\nupdate) echo x >> '{d}/updates'\n{next}exit {exit} ;;\nesac\n",
            d = dir.display(),
        ),
    )
    .unwrap();
    std::fs::set_permissions(&bin, std::fs::Permissions::from_mode(0o755)).unwrap();
    bin
}

fn updates(dir: &Path) -> usize {
    std::fs::read_to_string(dir.join("updates")).map_or(0, |text| text.lines().count())
}

#[tokio::test]
async fn current_or_unknown_versions_never_update() {
    for version in ["1.5.5", "agy dev build"] {
        let dir = tempfile::tempdir().unwrap();
        let bin = fake_agy(dir.path(), version, Some("9.9.9"), 0);
        ensure_current_with(&bin, Duration::from_secs(10))
            .await
            .unwrap();
        assert_eq!(updates(dir.path()), 0, "{version}");
    }
}

#[tokio::test]
async fn old_agy_updates_once_for_concurrent_callers() {
    let dir = tempfile::tempdir().unwrap();
    let bin = fake_agy(dir.path(), "1.3.4", Some("1.5.5"), 0);
    let (a, b) = tokio::join!(
        ensure_current_with(&bin, Duration::from_secs(10)),
        ensure_current_with(&bin, Duration::from_secs(10)),
    );
    a.unwrap();
    b.unwrap();
    assert_eq!(updates(dir.path()), 1);
}

#[tokio::test]
async fn failed_or_ineffective_updates_are_actionable_and_not_retried_at_once() {
    for (next, exit, detail) in [(None, 3, "exited with"), (Some("1.4.0"), 0, "still 1.4.0")] {
        let dir = tempfile::tempdir().unwrap();
        let bin = fake_agy(dir.path(), "1.3.4", next, exit);
        let error = ensure_current_with(&bin, Duration::from_secs(10))
            .await
            .unwrap_err();
        assert!(error.contains("agy 1.5.0 or newer"), "{error}");
        assert!(error.contains(detail), "{error}");
        assert!(error.contains("Run `agy update`"), "{error}");
        let again = ensure_current_with(&bin, Duration::from_secs(10))
            .await
            .unwrap_err();
        assert_eq!(again, error);
        assert_eq!(updates(dir.path()), 1);
    }
}

#[tokio::test]
async fn hanging_update_times_out() {
    let dir = tempfile::tempdir().unwrap();
    let bin = dir.path().join("agy");
    std::fs::write(
        &bin,
        "#!/bin/sh\ncase \"$1\" in\n--version) echo 1.3.4 ;;\nupdate) exec sleep 30 ;;\nesac\n",
    )
    .unwrap();
    std::fs::set_permissions(&bin, std::fs::Permissions::from_mode(0o755)).unwrap();
    let error = ensure_current_with(&bin, Duration::from_millis(300))
        .await
        .unwrap_err();
    assert!(error.contains("timed out"), "{error}");
}
