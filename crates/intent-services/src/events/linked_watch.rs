//! Bounded, non-recursive watches for canonical directories reached via links.
//! Callers supply directories from their bounded discovery walk and reconcile
//! after change callbacks. `SharedWatchHub` owns all OS registrations; this helper
//! handles registration catch-up, retry, and lifetime without walking a target.

use std::collections::{HashMap, HashSet};
use std::path::{Path, PathBuf};
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

/// User paths have one owner regardless of workspace count. Workspace removal
/// releases only its supplemental targets, never the shared user subscriptions.
pub(crate) struct ScopedLinkedWatches {
    hub: Arc<SharedWatchHub>,
    on_change: Arc<dyn Fn(Option<intent_core::WorkspaceId>) + Send + Sync>,
    user: LinkedWatches,
    projects: HashMap<intent_core::WorkspaceId, LinkedWatches>,
}

impl ScopedLinkedWatches {
    pub(crate) fn new(
        hub: Arc<SharedWatchHub>,
        on_change: impl Fn(Option<intent_core::WorkspaceId>) + Send + Sync + 'static,
    ) -> Self {
        let on_change: Arc<dyn Fn(Option<intent_core::WorkspaceId>) + Send + Sync> =
            Arc::new(on_change);
        let user_change = Arc::clone(&on_change);
        let user = LinkedWatches::new(Arc::clone(&hub), move || user_change(None));
        Self {
            hub,
            on_change,
            user,
            projects: HashMap::new(),
        }
    }

    pub(crate) fn sync_user(&mut self, directories: Vec<PathBuf>) {
        self.user.sync(directories);
    }

    pub(crate) fn sync_project(&mut self, id: intent_core::WorkspaceId, directories: Vec<PathBuf>) {
        if directories.is_empty() {
            self.projects.remove(&id);
            return;
        }
        self.projects
            .entry(id.clone())
            .or_insert_with(|| {
                let on_change = Arc::clone(&self.on_change);
                LinkedWatches::new(Arc::clone(&self.hub), move || on_change(Some(id.clone())))
            })
            .sync(directories);
    }

    pub(crate) fn remove(&mut self, id: &intent_core::WorkspaceId) {
        self.projects.remove(id);
    }
}

/// Filter only paths whose events the ordinary tier subscription forwards.
/// Do not canonicalize covered tier paths: a symlinked tier's external target
/// needs its own subscription, as do targets outside the tier within a workspace.
pub(crate) fn uncovered_directories(
    directories: Vec<PathBuf>,
    covered: &[PathBuf],
) -> Vec<PathBuf> {
    directories
        .into_iter()
        .filter(|directory| {
            !covered.iter().any(|root| {
                // Tier paths are workspace/<provider>/<kind>. Only ancestors inside
                // that workspace ride its stream; external parents still need a watch.
                let workspace = root.parent().and_then(Path::parent).unwrap_or(root);
                directory.starts_with(root)
                    || (root.starts_with(directory) && directory.starts_with(workspace))
            })
        })
        .collect()
}

#[cfg(test)]
mod tests {
    use super::*;
    use intent_core::WorkspaceId;

    #[tokio::test]
    async fn user_subscription_count_does_not_multiply_with_workspaces() {
        let root = crate::test_support::test_tempdir("linked-watch-fanout");
        let root = root.path().canonicalize().unwrap();
        let user: Vec<_> = (0..24).map(|n| root.join(format!("user-{n}"))).collect();
        for directory in &user {
            std::fs::create_dir_all(directory).unwrap();
        }
        let mut scopes = ScopedLinkedWatches::new(SharedWatchHub::new(), |_| {});
        scopes.sync_user(user.clone());
        for n in 0..64 {
            let id = WorkspaceId::from(format!("workspace-{n}"));
            let tier = root.join(format!("project-{n}/.claude/agents"));
            let ordinary = vec![tier.clone(), tier.join("nested")];
            scopes.sync_project(id, uncovered_directories(ordinary, &[tier]));
        }
        assert_eq!(scopes.user.watches.len(), user.len());
        assert_eq!(
            scopes
                .projects
                .values()
                .map(|watch| watch.watches.len())
                .sum::<usize>(),
            0
        );
        let id = WorkspaceId::from("workspace-0");
        let external = root.join("outside-tier");
        std::fs::create_dir_all(&external).unwrap();
        scopes.sync_project(id.clone(), vec![external]);
        assert_eq!(scopes.projects[&id].watches.len(), 1);
        scopes.remove(&id);
        assert_eq!(scopes.user.watches.len(), user.len());
        assert!(scopes
            .projects
            .values()
            .all(|watch| watch.watches.is_empty()));
        scopes.sync_user(Vec::new());
        assert!(scopes.user.watches.is_empty());
    }
}
