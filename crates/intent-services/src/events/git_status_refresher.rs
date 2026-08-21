//! `file:*` → debounced `changes:git-status` bridge (monorepo#1397).
//!
//! External (hand) edits reach the bus as `file:created`/`file:changed`/
//! `file:deleted` via [`super::watcher::FileWatcher`], but nothing converted
//! them into the `changes:git-status` refresh signal the FE Changes panel
//! subscribes to (§6.5) — that event was published only after daemon-initiated
//! git mutations. [`GitStatusRefresher`] closes the gap: it subscribes to the
//! internal bus for `file:*` events, coalesces per workspace within
//! [`DEBOUNCE`], recomputes the `WorkspaceGitStatus`
//! (`accept_changes::build_git_status_value`, on the blocking pool like
//! `accept-changes.getStatus`), and publishes the existing
//! `changes:git-status` event.
//!
//! No feedback loop: the subscription matches only the three `file:*` types,
//! so the refresher never sees its own `changes:*` output, and the status
//! recompute is a pure read (`git.status`-style reads emit no events).
//!
//! [`GitStatusRefresher::trigger`] exposes the same debounced path to
//! additional trigger sources (e.g. a `.git` metadata watch).
//!
//! The recompute also owns the read path's cache invalidation
//! (monorepo#1648): the observed change is exactly what makes the cached
//! [`crate::git_status_cache::GitStatusCache`] entry stale, so the refresh
//! discards it and repopulates it with the scan it was going to run anyway —
//! reads landing after the refresh hit a warm, current entry instead of
//! paying (or racing) a second scan.

use std::collections::HashMap;
use std::sync::Arc;
use std::time::Duration;

use intent_core::events::{FILE_CHANGED, FILE_CREATED, FILE_DELETED};
use intent_core::{WorkspaceApi, WorkspaceId};
use tokio::sync::mpsc;
use tokio::task::JoinHandle;

use super::bus::EventBus;
use super::filter::SubscriptionFilter;
use crate::git_status_cache::GitStatusCache;
use crate::{accept_changes, changes_git_status_event, git_ops};

/// Per-workspace coalescing window. Scheduled on the FIRST trigger in a burst
/// (not reset by later ones), so a sustained churn — e.g. a branch switch
/// touching many files — still refreshes within `DEBOUNCE` of its first event
/// while everything inside the window collapses into one recompute.
const DEBOUNCE: Duration = Duration::from_secs(1);

/// Bridges `file:*` events to debounced `changes:git-status` refreshes.
/// Dropping the handle tears both tasks down (clean-shutdown contract shared
/// with the watchers).
pub struct GitStatusRefresher {
    trigger_tx: mpsc::UnboundedSender<WorkspaceId>,
    forward_task: JoinHandle<()>,
    refresh_task: JoinHandle<()>,
}

impl Drop for GitStatusRefresher {
    fn drop(&mut self) {
        self.forward_task.abort();
        self.refresh_task.abort();
    }
}

impl GitStatusRefresher {
    /// Subscribe to `file:*` on `bus` and start the debounced refresh loop.
    /// `services` resolves workspace rows (worktree path, remote flag) at
    /// refresh time so the bridge always sees the current workspace state.
    /// `status_cache` is the read path's cache: each refresh invalidates and
    /// repopulates the recomputed worktree's entry (see the module note).
    pub fn start(
        bus: EventBus,
        services: Arc<dyn WorkspaceApi>,
        status_cache: Arc<GitStatusCache>,
    ) -> Self {
        let mut sub = bus.subscribe(SubscriptionFilter {
            event_types: vec![
                FILE_CREATED.to_string(),
                FILE_CHANGED.to_string(),
                FILE_DELETED.to_string(),
            ],
            ..SubscriptionFilter::default()
        });
        let (trigger_tx, trigger_rx) = mpsc::unbounded_channel::<WorkspaceId>();
        let forward_tx = trigger_tx.clone();
        let forward_task = tokio::spawn(async move {
            while let Some(batch) = sub.recv().await {
                for ev in batch {
                    let _ = forward_tx.send(ev.workspace_id.clone());
                }
            }
        });
        let refresh_task = tokio::spawn(refresh_loop(bus, services, status_cache, trigger_rx));
        Self {
            trigger_tx,
            forward_task,
            refresh_task,
        }
    }

    /// Request a debounced git-status refresh for `workspace_id` from an
    /// additional trigger source (same coalescing as the `file:*` path).
    pub fn trigger(&self, workspace_id: WorkspaceId) {
        let _ = self.trigger_tx.send(workspace_id);
    }
}

/// Coalesce triggers per workspace, then recompute + publish once per due
/// workspace. The deadline is set when a workspace first becomes pending and
/// deliberately NOT reset by further triggers (see [`DEBOUNCE`]).
async fn refresh_loop(
    bus: EventBus,
    services: Arc<dyn WorkspaceApi>,
    status_cache: Arc<GitStatusCache>,
    mut trigger_rx: mpsc::UnboundedReceiver<WorkspaceId>,
) {
    let mut pending: HashMap<WorkspaceId, tokio::time::Instant> = HashMap::new();
    loop {
        let next_deadline = pending.values().min().copied();
        tokio::select! {
            maybe = trigger_rx.recv() => match maybe {
                Some(ws_id) => {
                    pending
                        .entry(ws_id)
                        .or_insert_with(|| tokio::time::Instant::now() + DEBOUNCE);
                }
                None => return,
            },
            () = sleep_until(next_deadline), if next_deadline.is_some() => {
                let now = tokio::time::Instant::now();
                let due: Vec<WorkspaceId> = pending
                    .iter()
                    .filter(|(_, at)| **at <= now)
                    .map(|(id, _)| id.clone())
                    .collect();
                for ws_id in due {
                    pending.remove(&ws_id);
                    refresh_workspace(&bus, services.as_ref(), status_cache.as_ref(), &ws_id).await;
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

/// Recompute the `WorkspaceGitStatus` for one workspace and publish it as a
/// `changes:git-status` event. Remote workspaces and workspaces without a
/// resolvable worktree are skipped (their status cannot change via local
/// `file:*` events). Failures are logged, never fatal to the loop.
async fn refresh_workspace(
    bus: &EventBus,
    services: &dyn WorkspaceApi,
    status_cache: &GitStatusCache,
    ws_id: &WorkspaceId,
) {
    let ws = match services.get_workspace(ws_id.clone()).await {
        Ok(ws) => ws,
        Err(e) => {
            tracing::debug!(workspace = %ws_id, error = %e, "git-status refresh skipped: workspace lookup failed");
            return;
        }
    };
    if ws.is_remote {
        return;
    }
    let Some(worktree) = git_ops::worktree_path(&ws) else {
        return;
    };
    // The change that triggered this refresh is exactly what invalidates the
    // read path's cached scan (monorepo#1648): discard the stale entry and let
    // this recompute's scan repopulate it, so reads land on a warm, current
    // entry instead of paying a second walk. A non-repo worktree has no scan
    // to cache — `build_git_status_value_with` returns the minimal status
    // without touching libgit2 — so it only invalidates.
    let scanned = if worktree.join(".git").exists() {
        match status_cache.refresh(&worktree).await {
            Ok(status) => Some(status),
            Err(e) => {
                tracing::warn!(workspace = %ws_id, error = %e, "git-status refresh failed");
                return;
            }
        }
    } else {
        status_cache.invalidate(&worktree);
        None
    };
    // Bounded history walk + remote/trunk resolution (libgit2) — run on the
    // blocking pool so a slow repo cannot stall the runtime (parity with
    // `accept-changes.getStatus`). The working-tree scan itself was already
    // paid above, and is handed in so it is not repeated.
    let status = tokio::task::spawn_blocking(move || {
        accept_changes::build_git_status_value_with(&worktree, &ws, scanned)
    })
    .await;
    let status = match status {
        Ok(Ok(status)) => status,
        Ok(Err(e)) => {
            tracing::warn!(workspace = %ws_id, error = %e, "git-status refresh failed");
            return;
        }
        Err(e) => {
            tracing::warn!(workspace = %ws_id, error = %e, "git-status refresh task failed");
            return;
        }
    };
    if let Err(e) = bus.publish(&changes_git_status_event(ws_id, status)).await {
        tracing::warn!(workspace = %ws_id, error = %e, "changes:git-status publish failed");
    }
}

#[cfg(test)]
mod tests {
    use std::path::{Path, PathBuf};
    use std::sync::Mutex;

    use intent_core::events::CHANGES_GIT_STATUS;
    use intent_core::{
        chief_workspace, now_iso, ActorType, BoxFuture, Error, Event, EventActor, Result, Workspace,
    };
    use intent_store::{NewEvent, Store};
    use tokio::time::{timeout, Instant};

    use super::super::bus::Subscription;
    use super::*;

    /// Self-cleaning temp directory (workspace worktrees).
    struct TempDir {
        path: PathBuf,
    }

    impl TempDir {
        fn new(tag: &str) -> Self {
            let path =
                std::env::temp_dir().join(format!("intentd-gsr-{tag}-{}", uuid::Uuid::new_v4()));
            std::fs::create_dir_all(&path).expect("create temp dir");
            Self { path }
        }
    }

    impl Drop for TempDir {
        fn drop(&mut self) {
            let _ = std::fs::remove_dir_all(&self.path);
        }
    }

    struct TempDb {
        path: PathBuf,
    }

    impl TempDb {
        fn new() -> Self {
            let path =
                std::env::temp_dir().join(format!("intentd-gsr-{}.db", uuid::Uuid::new_v4()));
            Self { path }
        }
    }

    impl Drop for TempDb {
        fn drop(&mut self) {
            for suffix in ["", "-wal", "-shm"] {
                let _ =
                    std::fs::remove_file(PathBuf::from(format!("{}{suffix}", self.path.display())));
            }
        }
    }

    /// Minimal [`WorkspaceApi`] over a fixed workspace list: the refresher
    /// resolves workspaces via `get_workspace` at refresh time.
    struct FakeApi {
        workspaces: Mutex<Vec<Workspace>>,
    }

    impl FakeApi {
        fn new(workspaces: Vec<Workspace>) -> Self {
            Self {
                workspaces: Mutex::new(workspaces),
            }
        }
    }

    impl WorkspaceApi for FakeApi {
        fn get_workspace(&self, id: WorkspaceId) -> BoxFuture<'_, Result<Workspace>> {
            let found = self
                .workspaces
                .lock()
                .unwrap()
                .iter()
                .find(|ws| ws.id == id)
                .cloned();
            Box::pin(async move {
                found.ok_or_else(|| Error::NotFound(format!("workspace {}", id.as_str())))
            })
        }
    }

    /// Workspace whose worktree resolves to `path` (branch `feature/x`).
    fn test_workspace(id: &str, path: &Path) -> Workspace {
        let mut ws = chief_workspace();
        ws.id = WorkspaceId::from(id);
        ws.title = id.to_string();
        ws.branch = "feature/x".to_string();
        ws.worktree_path = Some(path.to_string_lossy().into_owned());
        ws
    }

    /// A `file:changed` event as the [`super::super::watcher::FileWatcher`]
    /// would publish it (system actor, TS `FileChangedEvent` data shape).
    fn file_event(ws_id: &WorkspaceId, path: &str) -> NewEvent {
        NewEvent {
            workspace_id: ws_id.clone(),
            timestamp: now_iso(),
            event_type: FILE_CHANGED.to_string(),
            actor: EventActor {
                actor_type: ActorType::System,
                id: None,
                name: None,
                email: None,
                metadata: None,
                model: None,
            },
            session_id: None,
            correlation_id: None,
            parent_event_id: None,
            metadata: None,
            data: serde_json::json!({
                "path": path,
                "relativePath": path,
                "action": "modify",
            }),
        }
    }

    async fn bus_and_status_sub() -> (TempDb, EventBus, Subscription) {
        let db = TempDb::new();
        let store = Store::open(&db.path).await.expect("open store");
        let bus = EventBus::new(store);
        let sub = bus.subscribe(SubscriptionFilter {
            event_types: vec![CHANGES_GIT_STATUS.to_string()],
            ..SubscriptionFilter::default()
        });
        (db, bus, sub)
    }

    /// Await the next `changes:git-status` event for `ws_id` within `overall`.
    async fn next_status_event(
        sub: &mut Subscription,
        ws_id: &WorkspaceId,
        overall: Duration,
    ) -> Option<Event> {
        let deadline = Instant::now() + overall;
        loop {
            let remaining = deadline.saturating_duration_since(Instant::now());
            if remaining.is_zero() {
                return None;
            }
            match timeout(remaining, sub.recv()).await {
                Ok(Some(batch)) => {
                    if let Some(ev) = batch.into_iter().find(|ev| &ev.workspace_id == ws_id) {
                        return Some(ev);
                    }
                }
                _ => return None,
            }
        }
    }

    #[tokio::test]
    async fn file_event_triggers_debounced_git_status() {
        let (_db, bus, mut sub) = bus_and_status_sub().await;
        let root = TempDir::new("single");
        let ws = test_workspace("ws-single", &root.path);
        let api: Arc<dyn WorkspaceApi> = Arc::new(FakeApi::new(vec![ws.clone()]));
        let _refresher =
            GitStatusRefresher::start(bus.clone(), api, Arc::new(GitStatusCache::new()));

        bus.publish(&file_event(&ws.id, "src/main.rs"))
            .await
            .expect("publish file event");

        let ev = next_status_event(&mut sub, &ws.id, crate::events::LIVENESS)
            .await
            .expect("file event must yield a changes:git-status event");
        assert_eq!(ev.event_type, CHANGES_GIT_STATUS);
        assert_eq!(ev.data["workspaceId"], ws.id.as_str());
        // No `.git` under the worktree → the minimal status, still carrying
        // the workspace branch (payload shape check).
        assert_eq!(ev.data["status"]["branch"], "feature/x");
        assert!(ev.data["status"].get("uncommittedCount").is_some());
    }

    #[tokio::test]
    async fn burst_of_file_events_coalesces_into_one_refresh() {
        let (_db, bus, mut sub) = bus_and_status_sub().await;
        let root = TempDir::new("burst");
        let ws = test_workspace("ws-burst", &root.path);
        let api: Arc<dyn WorkspaceApi> = Arc::new(FakeApi::new(vec![ws.clone()]));
        let _refresher =
            GitStatusRefresher::start(bus.clone(), api, Arc::new(GitStatusCache::new()));

        // A branch-switch-like burst: many file events well inside DEBOUNCE.
        for i in 0..25 {
            bus.publish(&file_event(&ws.id, &format!("src/file{i}.rs")))
                .await
                .expect("publish file event");
        }

        let first = next_status_event(&mut sub, &ws.id, crate::events::LIVENESS).await;
        assert!(first.is_some(), "burst must yield a changes:git-status");
        // The whole burst fell inside one debounce window → exactly one
        // recompute; a quiet period after the flush must stay silent.
        let second = next_status_event(&mut sub, &ws.id, DEBOUNCE + Duration::from_secs(2)).await;
        assert!(
            second.is_none(),
            "burst must coalesce into one refresh, got {second:?}"
        );
    }

    #[tokio::test]
    async fn hand_edit_in_real_repo_reports_uncommitted_change() {
        use git2::{Repository, Signature};

        let (_db, bus, mut sub) = bus_and_status_sub().await;
        let root = TempDir::new("repo");
        // Real repo with a seed commit on `main`, then a hand edit.
        let repo = Repository::init(&root.path).unwrap();
        {
            let mut cfg = repo.config().unwrap();
            cfg.set_str("user.name", "Test").unwrap();
            cfg.set_str("user.email", "test@example.com").unwrap();
        }
        repo.set_head("refs/heads/main").unwrap();
        std::fs::write(root.path.join("seed.txt"), "seed\n").unwrap();
        let mut index = repo.index().unwrap();
        index.add_path(Path::new("seed.txt")).unwrap();
        index.write().unwrap();
        let tree_oid = index.write_tree().unwrap();
        let tree = repo.find_tree(tree_oid).unwrap();
        let sig = Signature::now("Test", "test@example.com").unwrap();
        repo.commit(Some("HEAD"), &sig, &sig, "seed", &tree, &[])
            .unwrap();

        let ws = test_workspace("ws-repo", &root.path);
        let api: Arc<dyn WorkspaceApi> = Arc::new(FakeApi::new(vec![ws.clone()]));
        let cache = Arc::new(GitStatusCache::new());
        let _refresher = GitStatusRefresher::start(bus.clone(), api, Arc::clone(&cache));

        // The external hand edit + the file event the watcher would emit.
        std::fs::write(root.path.join("seed.txt"), "edited outside the app\n").unwrap();
        bus.publish(&file_event(&ws.id, "seed.txt"))
            .await
            .expect("publish file event");

        let ev = next_status_event(&mut sub, &ws.id, crate::events::LIVENESS)
            .await
            .expect("hand edit must yield a changes:git-status event");
        assert_eq!(ev.data["status"]["branch"], "main");
        assert_eq!(ev.data["status"]["uncommittedCount"], 1);
        // The refresh repopulated the read path's cache with the scan it just
        // paid for (monorepo#1648): a read landing now serves the post-edit
        // snapshot without walking the tree again.
        let cached = cache
            .get(&root.path, None)
            .await
            .expect("refresh must leave a cached status");
        assert_eq!(cached.files.len(), 1);
    }
}
