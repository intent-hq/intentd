//! A machine's watches of its home checkouts (monorepo
//! `docs/protocol/node-link.md`, "File and git events of a home",
//! nodeProtocol 19).
//!
//! A home checkout is watched as a local workspace is, under the machine's
//! hidden record of it ([`WorkspaceId::is_home_record`]): a [`FileWatcher`]
//! gives its `file:*` events, and file or `.git` changes a debounced
//! `changes:git-status`. Those events, like the ones home calls emit, never
//! reach the machine's own subscribers or store. They go to the bus's home
//! tap ([`EventBus::subscribe_home_records`]), from which the machine relays
//! them to the home's backend.
//!
//! The watches use their own [`SharedWatchHub`] and a trigger-only
//! [`GitStatusRefresher`], so they add no work to the daemon's watches of
//! its local workspaces.

use std::path::Path;
use std::sync::Arc;

use intent_core::{Event, WorkspaceApi, WorkspaceId};
use tokio::sync::broadcast;
use tokio::task::JoinHandle;

use super::bus::EventBus;
use super::git_metadata_watcher::{GitCommonDirWatches, GitMetadataWatcher};
use super::git_status_refresher::GitStatusRefresher;
use super::shared_watch::SharedWatchHub;
use super::watcher::FileWatcher;

/// The machine's watches of its home checkouts. Cloning shares them.
#[derive(Clone)]
pub struct HomeWatches {
    inner: Arc<Inner>,
}

struct Inner {
    bus: EventBus,
    hub: Arc<SharedWatchHub>,
    common: Arc<GitCommonDirWatches>,
    refresher: Arc<GitStatusRefresher>,
    /// Turns a home record's `file:*` events into git status refreshes.
    forward: JoinHandle<()>,
}

impl Drop for Inner {
    fn drop(&mut self) {
        self.forward.abort();
    }
}

/// One home checkout's watches; dropping it ends them.
pub struct HomeWatch {
    _file: FileWatcher,
    _git: Option<GitMetadataWatcher>,
}

impl HomeWatch {
    /// Wait until the file watch is live with the OS (test seam).
    #[cfg(test)]
    #[expect(clippy::used_underscore_binding)] // RAII field; read only by this test seam
    pub(super) async fn wait_established(&self, timeout: std::time::Duration) {
        self._file.wait_established(timeout).await;
    }
}

impl HomeWatches {
    /// Start the shared parts. `services` reads the hidden records when a
    /// git status is refreshed.
    #[must_use]
    pub fn start(
        bus: EventBus,
        services: Arc<dyn WorkspaceApi>,
        status_cache: Arc<crate::git_status_cache::GitStatusCache>,
        presence: crate::worktree_presence::WorktreePresence,
    ) -> Self {
        let refresher = Arc::new(GitStatusRefresher::start_triggered(
            bus.clone(),
            services,
            status_cache,
            presence,
        ));
        let mut tap = bus.subscribe_home_records();
        let trigger = Arc::clone(&refresher);
        let forward = intent_core::spawn_daemon(async move {
            loop {
                match tap.recv().await {
                    Ok(event) if event.event_type.starts_with("file:") => {
                        trigger.trigger(event.workspace_id.clone());
                    }
                    Ok(_) | Err(broadcast::error::RecvError::Lagged(_)) => {}
                    Err(broadcast::error::RecvError::Closed) => return,
                }
            }
        });
        Self {
            inner: Arc::new(Inner {
                bus,
                hub: SharedWatchHub::new(),
                common: GitCommonDirWatches::new(),
                refresher,
                forward,
            }),
        }
    }

    /// Watch the home checkout `worktree` under its hidden `record`, until
    /// the returned watch is dropped. A git status refresh follows at once.
    ///
    /// # Panics
    ///
    /// When `record` is not a hidden home record: a local workspace is
    /// watched by the daemon's watcher registry.
    #[must_use]
    pub fn watch(&self, record: &WorkspaceId, worktree: &Path) -> HomeWatch {
        assert!(record.is_home_record(), "{record} is not a home record");
        let file = FileWatcher::start(
            &self.inner.hub,
            self.inner.bus.clone(),
            record.clone(),
            worktree,
        );
        let git = GitMetadataWatcher::start(
            &self.inner.hub,
            &self.inner.common,
            Arc::clone(&self.inner.refresher),
            record.clone(),
            &worktree.to_path_buf(),
        );
        self.refresh(record);
        HomeWatch {
            _file: file,
            _git: git,
        }
    }

    /// Refresh `record`'s git status soon: a `changes:git-status` for it
    /// reaches the home tap.
    pub fn refresh(&self, record: &WorkspaceId) {
        self.inner.refresher.trigger(record.clone());
    }

    /// The events of every hidden home record ([`EventBus::subscribe_home_records`]).
    #[must_use]
    pub fn events(&self) -> broadcast::Receiver<Arc<Event>> {
        self.inner.bus.subscribe_home_records()
    }
}

impl crate::Services {
    /// The machine's home watches; `None` without an event bus.
    #[must_use]
    pub fn home_watches(&self) -> Option<HomeWatches> {
        let bus = self.event_bus.clone()?;
        Some(HomeWatches::start(
            bus,
            Arc::new(self.clone()),
            self.git_status_cache(),
            self.worktree_presence(),
        ))
    }
}

#[cfg(test)]
#[path = "home_watch_tests.rs"]
mod tests;
