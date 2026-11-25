//! Change detection for a store-backed [`SettingsRegistry`]: the store
//! counterpart of the `config.toml` watcher ([`crate::ConfigWatcher`]).
//!
//! A file-backed registry learns of external edits from file events. A
//! store-backed one (an orchestrator tenant's Postgres row) has no events
//! to listen to, so [`StoreSettingsFollower`] re-reads the document's
//! revision every `interval` and, when another process wrote a newer one,
//! adopts it through [`Services::apply_stored_settings_reload`]: the same
//! gates, runtime hooks, `settings:changed` event and `watch` notification
//! as a `config.toml` edit. A write of this process's own never looks
//! newer (the registry holds the revision it wrote), so there is no
//! self-write guard to keep.
//!
//! Today one process owns a tenant, so nothing else writes its document;
//! the follower is what lets a later replica see another replica's writes.

use std::sync::Arc;
use std::time::Duration;

use intent_core::Result;
use tokio::task::JoinHandle;

use crate::config_watcher::PreparedReloadAdmission;
use crate::settings_registry::{SettingsChanged, SettingsRegistry};
use crate::Services;

/// How often the orchestrator re-reads a tenant's settings revision.
pub const DEFAULT_POLL_INTERVAL: Duration = Duration::from_secs(2);

/// What one [`poll_once`] did.
#[derive(Debug)]
pub enum FollowOutcome {
    /// The registry holds the newest revision (or keeps a file).
    Current,
    /// A newer revision was adopted.
    Adopted(SettingsChanged),
}

/// Re-read `services`' store-backed settings once and adopt a newer
/// revision. The expected snapshot is captured before the read, so a
/// write this process publishes meanwhile makes the adoption fail (and the
/// next poll retry) instead of overwriting it with older text.
///
/// # Errors
/// When the store cannot be read, or the newer document is refused (it is
/// malformed, or this registry changed meanwhile); the registry keeps its
/// last-good state.
pub async fn poll_once(
    services: &Services,
    admission: PreparedReloadAdmission,
) -> Result<FollowOutcome> {
    let Some(registry) = store_registry(services) else {
        return Ok(FollowOutcome::Current);
    };
    if registry.write_in_flight() {
        // Adopting waits for no write; a later poll sees the result.
        return Ok(FollowOutcome::Current);
    }
    let expected = registry.snapshot();
    let Some(stored) = registry.fetch_newer()? else {
        return Ok(FollowOutcome::Current);
    };
    tracing::info!(
        settings = %registry.config_path().display(),
        held = registry.store_revision(),
        revision = stored.revision,
        "adopting settings another process wrote"
    );
    services
        .apply_stored_settings_reload(stored, expected, admission)
        .await
        .map(FollowOutcome::Adopted)
}

/// After a `settings.update` / `settings.reset` lost the revision race
/// (`result` is `Error::Conflict` on a store-backed registry), adopt the
/// newer document at once, through the same path as the follower, so a
/// retry the caller makes right away writes from the winner's revision
/// instead of conflicting again until the next poll. Anything else is left
/// alone. A failed catch-up is logged; the follower retries it.
pub(crate) async fn catch_up_after_conflict<T>(services: &Services, result: &Result<T>) {
    if !matches!(result, Err(intent_core::Error::Conflict { .. })) {
        return;
    }
    if store_registry(services).is_none() {
        return;
    }
    let (_running, stopped) = tokio::sync::watch::channel(false);
    match poll_once(services, PreparedReloadAdmission::new(stopped)).await {
        Ok(FollowOutcome::Adopted(notice)) => {
            tracing::info!(changed = ?notice.changed, "adopted the settings that won the race");
        }
        Ok(FollowOutcome::Current) => {}
        Err(error) => {
            tracing::warn!(%error, "could not adopt the settings that won the race; the follower retries");
        }
    }
}

fn store_registry(services: &Services) -> Option<Arc<SettingsRegistry>> {
    services
        .settings_registry()
        .filter(|registry| registry.is_store_backed())
}

/// The running follower. [`Self::shutdown`] stops it and waits for an
/// adoption in progress; dropping it stops it without waiting.
pub struct StoreSettingsFollower {
    stop: tokio::sync::watch::Sender<bool>,
    task: Option<JoinHandle<()>>,
}

impl StoreSettingsFollower {
    /// Follow `services`' settings every `interval`. A file-backed (or
    /// missing) registry has nothing to follow: the task ends at once.
    #[must_use]
    pub fn start(services: Services, interval: Duration) -> Self {
        let (stop, stopped) = tokio::sync::watch::channel(false);
        let task = intent_core::spawn_daemon(follow(services, interval, stopped));
        Self {
            stop,
            task: Some(task),
        }
    }

    /// Whether the follower's task has ended (or was joined).
    #[must_use]
    pub fn is_finished(&self) -> bool {
        self.task.as_ref().is_none_or(JoinHandle::is_finished)
    }

    /// Stop following and wait for the task to end.
    pub async fn shutdown(&mut self) {
        let _ = self.stop.send(true);
        if let Some(task) = self.task.take() {
            let _ = task.await;
        }
    }
}

impl Drop for StoreSettingsFollower {
    fn drop(&mut self) {
        let _ = self.stop.send(true);
    }
}

async fn follow(
    services: Services,
    interval: Duration,
    mut stopped: tokio::sync::watch::Receiver<bool>,
) {
    if store_registry(&services).is_none() {
        return;
    }
    loop {
        tokio::select! {
            biased;
            _ = stopped.changed() => return,
            () = tokio::time::sleep(interval) => {}
        }
        if *stopped.borrow() {
            return;
        }
        let admission = PreparedReloadAdmission::new(stopped.clone());
        match poll_once(&services, admission).await {
            Ok(FollowOutcome::Current) => {}
            Ok(FollowOutcome::Adopted(notice)) => {
                tracing::info!(changed = ?notice.changed, "adopted newer tenant settings");
            }
            Err(error) => {
                tracing::warn!(%error, "settings follow-up failed; keeping last-good settings");
            }
        }
    }
}
