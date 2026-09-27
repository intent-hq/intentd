//! Live-reload of `config.toml` (§9.8): a non-recursive watch on the config
//! file's **parent directory** feeds a debounced strict re-parse through
//! [`SettingsRegistry::reload`].
//!
//! Watching the directory (not the file) survives editor rename/atomic-save
//! patterns (vim, VS Code write-then-rename): a watch attached to the file's
//! inode dies when the editor renames a temp file over it, while
//! directory-level events keep reporting the config file's name. The watch
//! rides the daemon's [`SharedWatchHub`] rather than owning a `notify`
//! watcher of its own (intent-hq/intent#4953: on Linux each watcher is one
//! inotify instance against `fs.inotify.max_user_instances`). Events are
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
use std::sync::{Arc, Mutex};
use std::time::Duration;

use intent_core::{Error, Result};
use notify::RecursiveMode;
use tokio::sync::mpsc;
use tokio::task::JoinHandle;

use crate::events::shared_watch::{
    os_watch_limits, SharedWatchHub, SubHandle, CREATE_RETRY_CAP, CREATE_RETRY_INITIAL,
};
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

/// A live watch over `config.toml`'s parent directory. Holds the hub
/// subscription (the directory is unwatched when it drops, unless another
/// subscriber still needs it) and the debounce task (aborted on drop), so
/// dropping the [`ConfigWatcher`] tears the whole pipeline down — the
/// clean-shutdown contract for `serve`. The subscription sits behind a mutex
/// because the debounce task replaces it when the watch is lost (see
/// [`watch_loop`]).
pub struct ConfigWatcher {
    sub: Arc<Mutex<Option<SubHandle>>>,
    task: JoinHandle<()>,
}

impl Drop for ConfigWatcher {
    fn drop(&mut self) {
        self.task.abort();
        *lock(&self.sub) = None;
    }
}

fn lock(sub: &Mutex<Option<SubHandle>>) -> std::sync::MutexGuard<'_, Option<SubHandle>> {
    sub.lock()
        .unwrap_or_else(std::sync::PoisonError::into_inner)
}

impl ConfigWatcher {
    /// Start watching the parent directory of `registry.config_path()` over
    /// `hub`. `on_change` runs after each debounced **valid external** edit
    /// that changed effective values (the registry has already been reloaded
    /// and its subscribers notified); the composition root uses it to apply
    /// server runtime hooks and emit `settings:changed`.
    ///
    /// The OS registration itself is deferred to the hub's registrar thread,
    /// so this returns without blocking and the watch is NOT yet live; a
    /// caller that reports readiness awaits [`Self::ready`] first. A
    /// registration failure is logged by the debounce task rather than
    /// returned.
    ///
    /// # Errors
    ///
    /// Returns `Error::Internal` if the config path has no parent directory or file name.
    pub fn start<F, Fut>(
        hub: &Arc<SharedWatchHub>,
        registry: Arc<SettingsRegistry>,
        revision_gate: Arc<tokio::sync::RwLock<()>>,
        on_change: F,
    ) -> Result<Self>
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
        let (sub, raw_rx, _) = hub.subscribe_with(&dir, RecursiveMode::NonRecursive);
        let sub = Arc::new(Mutex::new(Some(sub)));
        let task = intent_core::spawn_daemon(watch_loop(
            Arc::clone(hub),
            registry,
            revision_gate,
            dir,
            file_name,
            Arc::clone(&sub),
            raw_rx,
            on_change,
        ));
        Ok(Self { sub, task })
    }

    /// Resolve once the deferred directory registration has settled: `true`
    /// when the OS watch is live, `false` when it failed or the registrar has
    /// not answered within the hub's establish timeout. Owned, so the caller
    /// can await it while the watcher itself stays parked elsewhere.
    pub fn ready(&self) -> impl Future<Output = bool> + Send + 'static {
        let established = lock(&self.sub).as_ref().map(SubHandle::established);
        async move {
            match established {
                Some(established) => established.await,
                None => false,
            }
        }
    }

    /// Detach a probe for the hub subscription this watcher currently rides,
    /// so a test can await the directory watch going live before mutating it.
    /// `None` while the loop is between a lost watch and its re-subscription.
    #[cfg(test)]
    fn probe(&self) -> Option<crate::events::shared_watch::RegistrationProbe> {
        lock(&self.sub).as_ref().map(SubHandle::probe)
    }
}

/// Coalesce raw file events within [`DEBOUNCE`], then run the reload core
/// once per burst.
///
/// The directory watch can fail to register, or be lost later: the hub
/// re-registers the directory when a recursive co-tenant above it retires and
/// closes this loop's channel if that re-registration fails. Either way the
/// loop drops the dead subscription and re-subscribes with the hub's capped
/// backoff, as [`super::events::root_watch`] does — parking on the closed
/// channel would end config live-reload for the process lifetime after a
/// transient failure. Each live subscription starts with a catch-up read
/// (see [`debounce`]) so an edit made while unwatched is not left stale.
/// Runs until the [`ConfigWatcher`] aborts it.
#[expect(clippy::too_many_arguments)]
async fn watch_loop<F, Fut>(
    hub: Arc<SharedWatchHub>,
    registry: Arc<SettingsRegistry>,
    revision_gate: Arc<tokio::sync::RwLock<()>>,
    dir: std::path::PathBuf,
    file_name: std::ffi::OsString,
    sub: Arc<Mutex<Option<SubHandle>>>,
    mut raw_rx: mpsc::UnboundedReceiver<notify::Event>,
    mut on_change: F,
) where
    F: Fn(SettingsChanged) -> Fut,
    Fut: Future<Output = ()>,
{
    let mut backoff = CREATE_RETRY_INITIAL;
    loop {
        let established = lock(&sub).as_ref().map(SubHandle::established);
        let live = match established {
            Some(established) => established.await,
            None => false,
        };
        let reason = if live {
            backoff = CREATE_RETRY_INITIAL;
            debounce(
                &registry,
                &revision_gate,
                &file_name,
                &mut raw_rx,
                &mut on_change,
            )
            .await;
            "config directory watch lost; re-registering"
        } else {
            "config directory watch failed to register; retrying"
        };
        *lock(&sub) = None;
        tracing::warn!(
            dir = %dir.display(),
            retry_in = ?backoff,
            os_watch_limits = %os_watch_limits(),
            "{reason}"
        );
        tokio::time::sleep(backoff).await;
        backoff = (backoff * 2).min(CREATE_RETRY_CAP);
        let (fresh, rx, _) = hub.subscribe_with(&dir, RecursiveMode::NonRecursive);
        raw_rx = rx;
        *lock(&sub) = Some(fresh);
    }
}

/// The debounce loop over one live subscription; returns when its channel
/// closes.
///
/// Entry arms one catch-up read: an edit that landed while the directory was
/// unwatched (between `start` and the deferred registration going live, or
/// inside a lost watch's backoff) produced no event, so it is read here
/// through the same revision gate, strict reload, and self-write check as an
/// event-triggered read — a no-op when nothing changed.
async fn debounce<F, Fut>(
    registry: &SettingsRegistry,
    revision_gate: &tokio::sync::RwLock<()>,
    file_name: &std::ffi::OsStr,
    raw_rx: &mut mpsc::UnboundedReceiver<notify::Event>,
    on_change: &mut F,
) where
    F: Fn(SettingsChanged) -> Fut,
    Fut: Future<Output = ()>,
{
    let mut deadline: Option<tokio::time::Instant> = Some(tokio::time::Instant::now() + DEBOUNCE);
    loop {
        tokio::select! {
            maybe = raw_rx.recv() => match maybe {
                Some(event) => {
                    // Access events carry no mutation; everything else
                    // (create, modify, rename, remove) can affect the file's
                    // content.
                    if matches!(event.kind, notify::EventKind::Access(_)) {
                        continue;
                    }
                    if event
                        .paths
                        .iter()
                        .any(|p| p.file_name() == Some(file_name))
                    {
                        deadline = Some(tokio::time::Instant::now() + DEBOUNCE);
                    }
                }
                None => return,
            },
            () = sleep_until(deadline), if deadline.is_some() => {
                deadline = None;
                let _revision_guard = revision_gate.write().await;
                if let ReloadOutcome::Applied(notice) = process_config_change(registry) {
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
    #[expect(clippy::await_holding_lock)]
    async fn watcher_detects_rename_style_atomic_save() {
        let _serial = crate::events::WATCHER_TEST_SERIAL
            .lock()
            .unwrap_or_else(std::sync::PoisonError::into_inner);
        let (dir, reg) = temp_registry(Some("[git]\nautoCommit = true\n"));
        let (tx, mut rx) = mpsc::unbounded_channel::<SettingsChanged>();
        let watcher = ConfigWatcher::start(
            &SharedWatchHub::new(),
            reg.clone(),
            Arc::new(tokio::sync::RwLock::new(())),
            move |notice| {
                let tx = tx.clone();
                async move {
                    let _ = tx.send(notice);
                }
            },
        )
        .expect("start watcher");
        // Registration is deferred to the hub's registrar thread, so wait for
        // the watch to be LIVE before mutating the dir: a write that lands
        // first produces no event and the wait below hangs.
        watcher
            .probe()
            .expect("subscribed")
            .wait_live(crate::events::LIVENESS)
            .await;
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

    /// The config directory's registration is reset when a recursive
    /// co-tenant above it retires (Linux global inotify group); should that
    /// re-registration fail, the hub closes the watcher's channel. The
    /// watcher must re-subscribe and keep live-reloading rather than park on
    /// the closed channel with a dead handle.
    #[cfg(target_os = "linux")]
    #[tokio::test]
    #[expect(clippy::await_holding_lock)]
    async fn watcher_re_subscribes_after_a_failed_re_registration() {
        use crate::events::shared_watch::WatchFault;
        use crate::events::LIVENESS;

        let _serial = crate::events::WATCHER_TEST_SERIAL
            .lock()
            .unwrap_or_else(std::sync::PoisonError::into_inner);
        let base = tempfile::tempdir().expect("tempdir");
        let ancestor = base.path().join("parent");
        let cfg_dir = ancestor.join("cfg");
        std::fs::create_dir_all(&cfg_dir).expect("mk cfg");
        let path = cfg_dir.join("config.toml");
        std::fs::write(&path, "[git]\nautoCommit = true\n").expect("seed config");
        let reg = Arc::new(SettingsRegistry::load(&path).expect("load"));

        // The recursive co-tenant's pruned walk registers `cfg` first; the
        // watcher registers it on start (2nd); the third `watch()` is the
        // survivor re-registration the ancestor's retirement sends; the
        // fourth is the watcher's own re-subscription.
        let fault = WatchFault::nth("cfg", 3);
        let hub = SharedWatchHub::with_watch_fault(&fault);
        let (sub_ancestor, _rx_ancestor, _) =
            hub.subscribe_with(&ancestor, RecursiveMode::NonRecursive);
        sub_ancestor.wait_established(LIVENESS).await;
        let (sub_wide, _rx_wide, _) = hub.subscribe_with(&ancestor, RecursiveMode::Recursive);
        sub_wide.wait_established(LIVENESS).await;
        drop(sub_wide);

        let (tx, mut rx) = mpsc::unbounded_channel::<SettingsChanged>();
        let watcher = ConfigWatcher::start(
            &hub,
            reg.clone(),
            Arc::new(tokio::sync::RwLock::new(())),
            move |notice| {
                let tx = tx.clone();
                async move {
                    let _ = tx.send(notice);
                }
            },
        )
        .expect("start watcher");
        watcher
            .probe()
            .expect("subscribed")
            .wait_live(LIVENESS)
            .await;
        assert_eq!(fault.attempts(), 2);
        // Let the spawned loop observe the live registration and park in its
        // debounce before the ancestor retires, so the recovery under test is
        // the closed-channel path rather than a first poll that already finds
        // the registration reset.
        tokio::task::yield_now().await;

        drop(sub_ancestor);

        // The re-subscription lands as the fourth `watch()` on `cfg`; the
        // fresh handle is stored right after it is enqueued.
        let probe = tokio::time::timeout(LIVENESS, async {
            loop {
                if fault.attempts() >= 4 {
                    if let Some(probe) = watcher.probe() {
                        break probe;
                    }
                }
                tokio::time::sleep(Duration::from_millis(50)).await;
            }
        })
        .await
        .expect("watcher must re-subscribe after its channel is closed");
        probe.wait_live(LIVENESS).await;

        let tmp = cfg_dir.join(".config.toml.editor-save");
        std::fs::write(&tmp, "[git]\nautoCommit = false\n").expect("write tmp");
        std::fs::rename(&tmp, reg.config_path()).expect("rename over config");
        let notice = tokio::time::timeout(LIVENESS, rx.recv())
            .await
            .expect("the re-subscribed watcher should observe the atomic save")
            .expect("watcher task alive");
        assert!(notice.changed.contains("git.autoCommit"), "{notice:?}");
        assert_eq!(reg.get("git.autoCommit"), Some(json!(false)));
    }

    /// An edit made while the directory is unwatched — after the survivor
    /// re-registration failed, during the watcher's backoff — produces no
    /// event. The re-subscription's catch-up read must apply it without a
    /// second edit.
    #[cfg(target_os = "linux")]
    #[tokio::test]
    #[expect(clippy::await_holding_lock)]
    async fn watcher_catches_up_on_an_edit_made_while_unwatched() {
        use crate::events::shared_watch::WatchFault;
        use crate::events::LIVENESS;

        let _serial = crate::events::WATCHER_TEST_SERIAL
            .lock()
            .unwrap_or_else(std::sync::PoisonError::into_inner);
        let base = tempfile::tempdir().expect("tempdir");
        let ancestor = base.path().join("parent");
        let cfg_dir = ancestor.join("cfg");
        std::fs::create_dir_all(&cfg_dir).expect("mk cfg");
        let path = cfg_dir.join("config.toml");
        std::fs::write(&path, "[git]\nautoCommit = true\n").expect("seed config");
        let reg = Arc::new(SettingsRegistry::load(&path).expect("load"));

        // Same `watch()` sequence on `cfg` as the test above: pruned walk,
        // start, failing survivor re-registration, re-subscription.
        let fault = WatchFault::nth("cfg", 3);
        let hub = SharedWatchHub::with_watch_fault(&fault);
        let (sub_ancestor, _rx_ancestor, _) =
            hub.subscribe_with(&ancestor, RecursiveMode::NonRecursive);
        sub_ancestor.wait_established(LIVENESS).await;
        let (sub_wide, _rx_wide, _) = hub.subscribe_with(&ancestor, RecursiveMode::Recursive);
        sub_wide.wait_established(LIVENESS).await;
        drop(sub_wide);

        let (tx, mut rx) = mpsc::unbounded_channel::<SettingsChanged>();
        let watcher = ConfigWatcher::start(
            &hub,
            reg.clone(),
            Arc::new(tokio::sync::RwLock::new(())),
            move |notice| {
                let tx = tx.clone();
                async move {
                    let _ = tx.send(notice);
                }
            },
        )
        .expect("start watcher");
        watcher
            .probe()
            .expect("subscribed")
            .wait_live(LIVENESS)
            .await;
        assert_eq!(fault.attempts(), 2);
        tokio::task::yield_now().await;

        drop(sub_ancestor);

        // The loop has dropped its dead handle and is sleeping out the backoff
        // before the fourth `watch()`: nothing watches `cfg` right now.
        tokio::time::timeout(LIVENESS, async {
            while !(fault.attempts() == 3 && watcher.probe().is_none()) {
                tokio::time::sleep(Duration::from_millis(10)).await;
            }
        })
        .await
        .expect("watcher must enter its re-subscription backoff");
        std::fs::write(&path, "[git]\nautoCommit = false\n").expect("edit while unwatched");

        // No second edit: only the catch-up read on re-establishment can
        // surface the change.
        let notice = tokio::time::timeout(LIVENESS, rx.recv())
            .await
            .expect("the re-subscribed watcher must catch up on the unwatched edit")
            .expect("watcher task alive");
        assert!(notice.changed.contains("git.autoCommit"), "{notice:?}");
        assert_eq!(reg.get("git.autoCommit"), Some(json!(false)));
        assert!(
            fault.attempts() >= 4,
            "catch-up must ride the re-subscription"
        );
    }
}
