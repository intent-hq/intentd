//! Bounded, non-recursive watches for canonical directories reached via links.
//! Callers supply directories from their bounded discovery walk and reconcile
//! after change callbacks. `SharedWatchHub` owns all OS registrations; this helper
//! handles registration catch-up, retry, and lifetime without walking a target.

use std::collections::{HashMap, HashSet};
use std::path::PathBuf;
use std::sync::Arc;
use std::time::Duration;

use tokio::task::JoinHandle;

use super::shared_watch::SharedWatchHub;

struct DirectoryWatch(JoinHandle<()>);

impl Drop for DirectoryWatch {
    fn drop(&mut self) {
        self.0.abort();
    }
}

pub(crate) struct LinkedWatches {
    hub: Arc<SharedWatchHub>,
    on_change: Arc<dyn Fn() + Send + Sync>,
    watches: HashMap<PathBuf, DirectoryWatch>,
}

impl LinkedWatches {
    pub(crate) fn new(
        hub: Arc<SharedWatchHub>,
        on_change: impl Fn() + Send + Sync + 'static,
    ) -> Self {
        Self {
            hub,
            on_change: Arc::new(on_change),
            watches: HashMap::new(),
        }
    }

    /// Supply canonical, existing directory paths from a bounded scan. Removed
    /// paths immediately release their subscriptions; unchanged paths stay live.
    pub(crate) fn sync(&mut self, directories: Vec<PathBuf>) {
        let directories: HashSet<_> = directories.into_iter().collect();
        self.watches
            .retain(|path, watch| directories.contains(path) && !watch.0.is_finished());
        for directory in directories {
            if self.watches.contains_key(&directory) {
                continue;
            }
            let hub = Arc::clone(&self.hub);
            let root = directory.clone();
            let on_change = Arc::clone(&self.on_change);
            let task = intent_core::spawn_daemon(async move {
                let mut backoff = Duration::from_millis(250);
                loop {
                    let (subscription, mut rx, _) =
                        hub.subscribe_with(&root, notify::RecursiveMode::NonRecursive);
                    if subscription.wait_live().await {
                        backoff = Duration::from_millis(250);
                        on_change();
                        while let Some(event) = rx.recv().await {
                            if !matches!(event.kind, notify::EventKind::Access(_)) {
                                on_change();
                            }
                        }
                    }
                    drop(subscription);
                    tokio::time::sleep(backoff).await;
                    backoff = (backoff * 2).min(Duration::from_secs(30));
                }
            });
            self.watches.insert(directory, DirectoryWatch(task));
        }
    }
}
