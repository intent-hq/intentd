//! Live-reload of `config.toml` (§9.8): a `notify` watch on the config file's
//! **parent directory** feeds a debounced strict re-parse through
//! [`SettingsRegistry::reload`].
//!
//! Watching the directory (not the file) survives editor rename/atomic-save
//! patterns (vim, VS Code write-then-rename): a watch attached to the file's
//! inode dies when the editor renames a temp file over it, while
//! directory-level events keep reporting the config file's name. Events are
//! filtered to the config file, coalesced within [`DEBOUNCE`], and then the
//! file is read once ([`process_config_change`], the testable core):
//!
//! - self-writes (the daemon's own `settings.update` write-back) are skipped
//!   via the registry's content-hash guard, which matches any *recent*
//!   self-write — so a stale or coalesced read observing an earlier
//!   write-back during a rapid write burst is skipped too, never adopted as
//!   an external edit,
//! - valid content is adopted through [`SettingsRegistry::reload`] (pins keep
//!   winning; registry subscribers are notified) and the `on_change` callback
//!   runs so the composition root can apply server runtime hooks and emit
//!   `settings:changed`,
//! - invalid content (parse error, unknown field, range/enum violation) keeps
//!   last-good values with a WARN naming the file and offending key,
//! - a missing file keeps last-good values with a WARN (never regenerated
//!   mid-run).
//!
//! Concurrency note: writes are last-writer-wins. If a wire `settings.update`
//! lands inside the debounce window after an external hand-edit, the
//! registry's write-back (built from its in-memory document, which predates
//! the hand-edit) overwrites the file and the follow-up watcher read matches
//! the self-write hash — the hand-edit is lost silently. Relatedly, an
//! external edit that byte-matches a recent (<10s-old) self-write — e.g. a
//! manual revert to bytes the daemon just wrote — is indistinguishable from
//! a stale read of that write and is skipped; before the guard kept a
//! history it was skipped indefinitely (the last write's hash never
//! expired), so the window strictly narrows this. Both are accepted
//! trade-offs for a human-timescale file; the next external edit wins again.

use std::future::Future;
use std::sync::Arc;
use std::time::Duration;

use intent_core::{Error, Result};
use notify::{RecommendedWatcher, RecursiveMode, Watcher};
use tokio::sync::mpsc;
use tokio::task::JoinHandle;

use crate::settings_registry::{SettingsChanged, SettingsRegistry};

/// Debounce window: the file is read once, this long after the *last* raw
/// event (editors emit create+modify+rename flurries per save).
const DEBOUNCE: Duration = Duration::from_millis(300);

/// Outcome of one debounced config-file read (see [`process_config_change`]).
#[derive(Debug)]
pub(crate) enum ReloadOutcome {
    /// New content adopted; effective values changed.
    Applied(SettingsChanged),
    /// New content adopted; no effective value changed (formatting-only edit).
    Unchanged,
    /// Content matches the registry's own last write-back; skipped.
    SelfWrite,
    /// File missing/unreadable; last-good values kept.
    Missing,
    /// Content failed the strict parse; last-good values kept.
    Invalid,
}

/// The event-handling core of the watcher, factored out of the notify loop so
/// it can be unit-tested deterministically: read the config file, suppress
/// self-writes, and strictly reload the registry. Never panics or drops
/// settings — every failure path keeps last-good values and logs a WARN.
pub(crate) fn process_config_change(registry: &SettingsRegistry) -> ReloadOutcome {
    let path = registry.config_path();
    let text = match std::fs::read_to_string(path) {
        Ok(text) => text,
        Err(e) => {
            tracing::warn!(
                file = %path.display(),
                error = %e,
                "config.toml missing or unreadable; keeping last-good settings"
            );
            return ReloadOutcome::Missing;
        }
    };
    if registry.is_self_write(&text) {
        tracing::debug!(
            file = %path.display(),
            "config.toml event matches our own write-back; skipping reload"
        );
        return ReloadOutcome::SelfWrite;
    }
    match registry.reload(&text) {
        Ok(notice) if notice.changed.is_empty() => ReloadOutcome::Unchanged,
        Ok(notice) => {
            tracing::info!(
                file = %path.display(),
                changed = ?notice.changed,
                "config.toml edited externally; changes applied"
            );
            ReloadOutcome::Applied(notice)
        }
        Err(e) => {
            tracing::warn!(
                file = %path.display(),
                error = %e,
                "invalid config.toml edit ignored; keeping last-good settings"
            );
            ReloadOutcome::Invalid
        }
    }
}

/// A live watch over `config.toml`'s parent directory. Holds the `notify`
/// watcher (the OS subscription ends when it drops) and the debounce task
/// (aborted on drop), so dropping the [`ConfigWatcher`] tears the whole
/// pipeline down — the clean-shutdown contract for `serve`.
pub struct ConfigWatcher {
    _watcher: RecommendedWatcher,
    task: JoinHandle<()>,
}

impl Drop for ConfigWatcher {
    fn drop(&mut self) {
        self.task.abort();
    }
}

impl ConfigWatcher {
    /// Start watching the parent directory of `registry.config_path()`.
    /// `on_change` runs after each debounced **valid external** edit that
    /// changed effective values (the registry has already been reloaded and
    /// its subscribers notified); the composition root uses it to apply
    /// server runtime hooks and emit `settings:changed`.
    pub fn start<F, Fut>(registry: Arc<SettingsRegistry>, on_change: F) -> Result<Self>
    where
        F: Fn(SettingsChanged) -> Fut + Send + 'static,
        Fut: Future<Output = ()> + Send + 'static,
    {
        let path = registry.config_path().to_path_buf();
        let dir = path
            .parent()
            .ok_or_else(|| {
                Error::Internal(format!("config path has no parent: {}", path.display()))
            })?
            .to_path_buf();
        let file_name = path
            .file_name()
            .ok_or_else(|| {
                Error::Internal(format!("config path has no file name: {}", path.display()))
            })?
            .to_os_string();
        // The notify callback is synchronous and runs off the tokio runtime,
        // so it forwards ticks over an unbounded channel to the debounce loop.
        let (raw_tx, raw_rx) = mpsc::unbounded_channel::<()>();
        let mut watcher = notify::recommended_watcher(move |res: notify::Result<notify::Event>| {
            let event = match res {
                Ok(event) => event,
                Err(e) => {
                    tracing::warn!(
                        error = %e,
                        "config.toml watcher callback error; settings changes may be missed"
                    );
                    return;
                }
            };
            // Access events carry no mutation; everything else (create,
            // modify, rename, remove) can affect the file's content.
            if matches!(event.kind, notify::EventKind::Access(_)) {
                return;
            }
            if event
                .paths
                .iter()
                .any(|p| p.file_name() == Some(file_name.as_os_str()))
            {
                let _ = raw_tx.send(());
            }
        })
        .map_err(|e| Error::Internal(format!("config.toml watcher: {e}")))?;
        watcher
            .watch(&dir, RecursiveMode::NonRecursive)
            .map_err(|e| Error::Internal(format!("config.toml watch on {}: {e}", dir.display())))?;
        let task = tokio::spawn(watch_loop(registry, raw_rx, on_change));
        Ok(Self {
            _watcher: watcher,
            task,
        })
    }
}

/// Coalesce raw file events within [`DEBOUNCE`], then run the reload core
/// once per burst. Returns when the watcher (and its channel sender) drops.
async fn watch_loop<F, Fut>(
    registry: Arc<SettingsRegistry>,
    mut raw_rx: mpsc::UnboundedReceiver<()>,
    on_change: F,
) where
    F: Fn(SettingsChanged) -> Fut,
    Fut: Future<Output = ()>,
{
    let mut deadline: Option<tokio::time::Instant> = None;
    loop {
        tokio::select! {
            maybe = raw_rx.recv() => match maybe {
                Some(()) => deadline = Some(tokio::time::Instant::now() + DEBOUNCE),
                None => return,
            },
            () = sleep_until(deadline), if deadline.is_some() => {
                deadline = None;
                if let ReloadOutcome::Applied(notice) = process_config_change(&registry) {
                    on_change(notice).await;
                }
            }
        }
    }
}

async fn sleep_until(deadline: Option<tokio::time::Instant>) {
    match deadline {
        Some(at) => tokio::time::sleep_until(at).await,
        None => std::future::pending().await,
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use serde_json::json;

    fn temp_registry(contents: Option<&str>) -> (tempfile::TempDir, Arc<SettingsRegistry>) {
        let dir = tempfile::tempdir().expect("tempdir");
        let path = dir.path().join("config.toml");
        if let Some(text) = contents {
            std::fs::write(&path, text).expect("seed config");
        }
        let reg = Arc::new(SettingsRegistry::load(&path).expect("load"));
        (dir, reg)
    }

    #[test]
    fn valid_external_edit_applies_and_notifies_subscribers() {
        let (_dir, reg) = temp_registry(Some("[git]\nautoCommit = true\n"));
        let rx = reg.subscribe();
        std::fs::write(reg.config_path(), "[git]\nautoCommit = false\n").expect("edit");
        match process_config_change(&reg) {
            ReloadOutcome::Applied(notice) => {
                assert!(notice.changed.contains("git.autoCommit"), "{notice:?}");
            }
            other => panic!("expected Applied, got {other:?}"),
        }
        assert_eq!(reg.get("git.autoCommit"), Some(json!(false)));
        assert!(rx.has_changed().expect("sender alive"));
    }

    #[test]
    fn invalid_external_edit_keeps_last_good() {
        let (_dir, reg) = temp_registry(Some("[git]\nautoCommit = false\n"));
        let rx = reg.subscribe();
        // type error on a known key
        std::fs::write(reg.config_path(), "[git]\nautoCommit = \"nope\"\n").expect("edit");
        assert!(matches!(
            process_config_change(&reg),
            ReloadOutcome::Invalid
        ));
        // unknown table/field
        std::fs::write(reg.config_path(), "[bogus]\nkey = 1\n").expect("edit");
        assert!(matches!(
            process_config_change(&reg),
            ReloadOutcome::Invalid
        ));
        // TOML syntax error
        std::fs::write(reg.config_path(), "[git\n").expect("edit");
        assert!(matches!(
            process_config_change(&reg),
            ReloadOutcome::Invalid
        ));
        // last-good value survives and no notification was published
        assert_eq!(reg.get("git.autoCommit"), Some(json!(false)));
        assert!(!rx.has_changed().expect("sender alive"));
    }

    #[test]
    fn self_write_event_is_suppressed() {
        let (_dir, reg) = temp_registry(None);
        reg.apply(&[("rtk.enabled".to_string(), json!(true))])
            .expect("apply");
        // subscribe after the apply so the receiver starts with no pending
        // notice; a reload would flip has_changed back on.
        let rx = reg.subscribe();
        assert!(matches!(
            process_config_change(&reg),
            ReloadOutcome::SelfWrite
        ));
        assert!(!rx.has_changed().expect("sender alive"));
        assert_eq!(reg.get("rtk.enabled"), Some(json!(true)));
    }

    #[test]
    fn stale_read_of_an_earlier_self_write_is_suppressed() {
        let (_dir, reg) = temp_registry(None);
        reg.apply(&[("rtk.enabled".to_string(), json!(false))])
            .expect("apply A");
        let write_a = std::fs::read_to_string(reg.config_path()).expect("read A");
        reg.apply(&[("rtk.enabled".to_string(), json!(true))])
            .expect("apply B");
        let rx = reg.subscribe();
        // A debounced/coalesced watcher read observing the file at write A's
        // bytes (stale read across the atomic rename) must be skipped as a
        // self-write, not adopted as an external edit.
        std::fs::write(reg.config_path(), &write_a).expect("rewrite as A");
        assert!(matches!(
            process_config_change(&reg),
            ReloadOutcome::SelfWrite
        ));
        // In-memory values stay at write B's state; no notification.
        assert_eq!(reg.get("rtk.enabled"), Some(json!(true)));
        assert!(!rx.has_changed().expect("sender alive"));
    }

    #[test]
    fn manual_revert_after_adopted_external_edit_applies() {
        let (_dir, reg) = temp_registry(None);
        reg.apply(&[("rtk.enabled".to_string(), json!(true))])
            .expect("apply");
        let self_written = std::fs::read_to_string(reg.config_path()).expect("read");
        // A genuine external edit (novel bytes) still live-reloads…
        std::fs::write(reg.config_path(), "[rtk]\nenabled = false\n").expect("edit");
        match process_config_change(&reg) {
            ReloadOutcome::Applied(notice) => {
                assert!(notice.changed.contains("rtk.enabled"), "{notice:?}");
            }
            other => panic!("expected Applied, got {other:?}"),
        }
        // …and once adopted it supersedes the self-write history: a manual
        // revert to the earlier self-written bytes is external, not skipped.
        std::fs::write(reg.config_path(), &self_written).expect("revert");
        match process_config_change(&reg) {
            ReloadOutcome::Applied(notice) => {
                assert!(notice.changed.contains("rtk.enabled"), "{notice:?}");
            }
            other => panic!("expected Applied, got {other:?}"),
        }
        assert_eq!(reg.get("rtk.enabled"), Some(json!(true)));
    }

    #[test]
    fn missing_file_is_a_warned_noop() {
        let (_dir, reg) = temp_registry(Some("[git]\nautoCommit = false\n"));
        std::fs::remove_file(reg.config_path()).expect("delete");
        assert!(matches!(
            process_config_change(&reg),
            ReloadOutcome::Missing
        ));
        assert_eq!(reg.get("git.autoCommit"), Some(json!(false)));
    }

    #[test]
    fn identical_external_rewrite_is_unchanged() {
        let (_dir, reg) = temp_registry(Some("[git]\nautoCommit = false\n"));
        // same bytes rewritten by an editor: reload succeeds, nothing changes
        std::fs::write(reg.config_path(), "[git]\nautoCommit = false\n").expect("edit");
        assert!(matches!(
            process_config_change(&reg),
            ReloadOutcome::Unchanged
        ));
    }

    #[tokio::test]
    #[allow(clippy::await_holding_lock)]
    async fn watcher_detects_rename_style_atomic_save() {
        let _serial = crate::events::WATCHER_TEST_SERIAL
            .lock()
            .unwrap_or_else(std::sync::PoisonError::into_inner);
        let (dir, reg) = temp_registry(Some("[git]\nautoCommit = true\n"));
        let (tx, mut rx) = mpsc::unbounded_channel::<SettingsChanged>();
        let _watcher = ConfigWatcher::start(reg.clone(), move |notice| {
            let tx = tx.clone();
            async move {
                let _ = tx.send(notice);
            }
        })
        .expect("start watcher");
        // Give the OS watch a moment to establish before mutating the dir.
        tokio::time::sleep(Duration::from_millis(250)).await;
        // Editor-style atomic save: write a temp file, rename over config.toml.
        let tmp = dir.path().join(".config.toml.editor-save");
        std::fs::write(&tmp, "[git]\nautoCommit = false\n").expect("write tmp");
        std::fs::rename(&tmp, reg.config_path()).expect("rename over config");
        let notice = tokio::time::timeout(crate::events::LIVENESS, rx.recv())
            .await
            .expect("watcher should observe the atomic save within the liveness bound")
            .expect("watcher task alive");
        assert!(notice.changed.contains("git.autoCommit"), "{notice:?}");
        assert_eq!(reg.get("git.autoCommit"), Some(json!(false)));
    }
}
