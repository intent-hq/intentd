//! Dynamic watcher registration on workspace lifecycle (#611).
//!
//! [`WatcherRegistry`] is the single registration path for all three watcher
//! families: per-workspace [`FileWatcher`]s, the [`SkillsWatcher`], and the
//! [`SpecialistsWatcher`]. At start it subscribes to the event bus first,
//! then seeds the watchers from the current workspace snapshot (boot-time
//! behavior unchanged) and follows the live set from the subscription:
//! `workspace:created`/`workspace:opened` register the
//! workspace's watch roots at runtime, `workspace:deleted`/`workspace:closed`
//! tear them down. Each watcher keeps its own debounce/fingerprint semantics;
//! the registry only routes lifecycle transitions.

use std::collections::HashMap;
use std::path::PathBuf;
use std::sync::Arc;

use intent_core::events::{
    WORKSPACE_CLOSED, WORKSPACE_CREATED, WORKSPACE_DELETED, WORKSPACE_OPENED,
};
use intent_core::{Event, WorkspaceApi, WorkspaceId};
use tokio::task::JoinHandle;

use super::bus::EventBus;
use super::filter::SubscriptionFilter;
use super::skills_watcher::SkillsWatcher;
use super::specialists_watcher::SpecialistsWatcher;
use super::watcher::FileWatcher;

/// Coordinates the three watcher families against the live workspace set.
/// Dropping the registry tears down the lifecycle task and every watcher it
/// owns (clean shutdown, matching the previous boot-time handles).
pub struct WatcherRegistry {
    task: JoinHandle<()>,
}

impl Drop for WatcherRegistry {
    fn drop(&mut self) {
        self.task.abort();
    }
}

impl WatcherRegistry {
    /// Seed watchers for every current non-archived workspace with an existing
    /// on-disk root, then follow workspace lifecycle events on `bus`.
    /// `services` resolves paths for lifecycle events whose payload does not
    /// carry the workspace row (e.g. `workspace:opened`).
    pub async fn start(bus: EventBus, services: Arc<dyn WorkspaceApi>) -> Self {
        // Subscribe BEFORE taking the workspace snapshot: subscription
        // delivery is live-only, so a lifecycle event published between the
        // snapshot and the subscribe would never be observed. A workspace
        // seen by both the snapshot and a buffered `workspace:created` is
        // fine — insert/add_workspace are idempotent replacements.
        let sub = bus.subscribe(SubscriptionFilter {
            event_types: vec![
                WORKSPACE_CREATED.to_string(),
                WORKSPACE_OPENED.to_string(),
                WORKSPACE_DELETED.to_string(),
                WORKSPACE_CLOSED.to_string(),
            ],
            ..SubscriptionFilter::default()
        });

        let initial = match services.list_workspaces(false).await {
            Ok(ws) => ws
                .into_iter()
                .filter_map(|ws| {
                    let root = ws.path.clone().or_else(|| ws.worktree_path.clone())?;
                    let path = PathBuf::from(&root);
                    path.is_dir().then_some((ws.id, path))
                })
                .collect(),
            Err(e) => {
                tracing::warn!(error = %e, "could not list workspaces for watching");
                Vec::new()
            }
        };

        let mut file_watchers: HashMap<WorkspaceId, FileWatcher> = HashMap::new();
        for (ws_id, path) in &initial {
            match FileWatcher::start(bus.clone(), ws_id.clone(), path.clone()) {
                Ok(w) => {
                    tracing::info!(workspace = %ws_id, path = %path.display(), "watching workspace files");
                    file_watchers.insert(ws_id.clone(), w);
                }
                Err(e) => {
                    tracing::warn!(workspace = %ws_id, path = %path.display(), error = %e, "file watcher start failed")
                }
            }
        }
        tracing::info!(count = file_watchers.len(), "file watchers started");

        let skills = SkillsWatcher::start(bus.clone(), initial.clone());
        tracing::info!("skills watcher started");
        let specialists = SpecialistsWatcher::start(bus.clone(), initial);
        tracing::info!("specialists watcher started");

        let task = tokio::spawn(lifecycle_loop(
            bus,
            services,
            sub,
            file_watchers,
            skills,
            specialists,
        ));
        Self { task }
    }
}

/// Follow workspace lifecycle events, registering/deregistering watch roots.
async fn lifecycle_loop(
    bus: EventBus,
    services: Arc<dyn WorkspaceApi>,
    mut sub: super::bus::Subscription,
    mut file_watchers: HashMap<WorkspaceId, FileWatcher>,
    skills: SkillsWatcher,
    specialists: SpecialistsWatcher,
) {
    while let Some(batch) = sub.recv().await {
        for ev in batch {
            let ws_id = ev.workspace_id.clone();
            match ev.event_type.as_str() {
                WORKSPACE_CREATED | WORKSPACE_OPENED => {
                    let Some(path) = resolve_path(&ev, services.as_ref()).await else {
                        tracing::debug!(workspace = %ws_id, "lifecycle event without resolvable path; not watching");
                        continue;
                    };
                    match FileWatcher::start(bus.clone(), ws_id.clone(), path.clone()) {
                        Ok(w) => {
                            tracing::info!(workspace = %ws_id, path = %path.display(), "watching workspace files (runtime registration)");
                            file_watchers.insert(ws_id.clone(), w);
                        }
                        Err(e) => {
                            tracing::warn!(workspace = %ws_id, path = %path.display(), error = %e, "file watcher start failed")
                        }
                    }
                    skills.add_workspace(ws_id.clone(), path.clone());
                    specialists.add_workspace(ws_id, path);
                }
                WORKSPACE_DELETED | WORKSPACE_CLOSED => {
                    if file_watchers.remove(&ws_id).is_some() {
                        tracing::info!(workspace = %ws_id, "workspace file watcher stopped (runtime deregistration)");
                    }
                    skills.remove_workspace(&ws_id);
                    specialists.remove_workspace(&ws_id);
                }
                _ => {}
            }
        }
    }
}

/// Resolve the on-disk root for a lifecycle event: prefer the self-sufficient
/// `data.workspace` payload (`workspace:created`, §6.7), fall back to a
/// `get_workspace` lookup (`workspace:opened` carries only the id). Returns
/// `None` when the workspace has no existing directory.
async fn resolve_path(ev: &Event, services: &dyn WorkspaceApi) -> Option<PathBuf> {
    let from_payload = ev
        .data
        .get("workspace")
        .and_then(|ws| {
            ws.get("path")
                .and_then(|v| v.as_str())
                .or_else(|| ws.get("worktreePath").and_then(|v| v.as_str()))
        })
        .map(PathBuf::from);

    let path = match from_payload {
        Some(p) => Some(p),
        None => {
            let ws = services.get_workspace(ev.workspace_id.clone()).await.ok()?;
            ws.path.or(ws.worktree_path).map(PathBuf::from)
        }
    }?;

    path.is_dir().then_some(path)
}

#[cfg(test)]
mod tests {
    use std::sync::Mutex;
    use std::time::Duration;

    use intent_core::{
        chief_workspace, now_iso, ActorType, BoxFuture, Error, EventActor, Result, Workspace,
    };
    use intent_store::{NewEvent, Store};
    use tokio::time::{timeout, Instant};

    use super::*;

    /// Self-cleaning temp directory (workspace roots).
    struct TempDir {
        path: PathBuf,
    }

    impl TempDir {
        fn new(tag: &str) -> Self {
            let path = std::env::temp_dir()
                .join(format!("intentd-registry-{tag}-{}", uuid::Uuid::new_v4()));
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
                std::env::temp_dir().join(format!("intentd-registry-{}.db", uuid::Uuid::new_v4()));
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

    /// Minimal [`WorkspaceApi`] over a fixed workspace list: `list_workspaces`
    /// seeds the registry, `get_workspace` resolves paths for `workspace:opened`.
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
        fn list_workspaces(
            &self,
            _include_archived: bool,
        ) -> BoxFuture<'_, Result<Vec<Workspace>>> {
            let ws = self.workspaces.lock().unwrap().clone();
            Box::pin(async move { Ok(ws) })
        }

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

    fn test_workspace(id: &str, path: &std::path::Path) -> Workspace {
        let mut ws = chief_workspace();
        ws.id = WorkspaceId::from(id);
        ws.title = id.to_string();
        ws.path = Some(path.to_string_lossy().into_owned());
        ws
    }

    fn lifecycle_event(event_type: &str, ws: &Workspace, with_payload: bool) -> NewEvent {
        let data = if with_payload {
            serde_json::json!({ "workspaceId": ws.id.as_str(), "workspace": ws })
        } else {
            serde_json::json!({ "workspaceId": ws.id.as_str() })
        };
        NewEvent {
            workspace_id: ws.id.clone(),
            timestamp: now_iso(),
            event_type: event_type.to_string(),
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
            data,
        }
    }

    async fn bus_and_sub() -> (TempDb, EventBus, super::super::bus::Subscription) {
        let db = TempDb::new();
        let store = Store::open(&db.path).await.expect("open store");
        let bus = EventBus::new(store);
        let sub = bus.subscribe(SubscriptionFilter {
            event_types: vec!["file:*".to_string()],
            ..SubscriptionFilter::default()
        });
        (db, bus, sub)
    }

    /// Await the next `file:*` event for `ws_id` (any path) within `overall`.
    async fn next_file_event(
        sub: &mut super::super::bus::Subscription,
        ws_id: &WorkspaceId,
        overall: Duration,
    ) -> Option<intent_core::Event> {
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
    async fn boot_time_workspace_is_watched() {
        let (_db, bus, mut sub) = bus_and_sub().await;
        let root = TempDir::new("boot");
        let ws = test_workspace("ws-boot", &root.path);
        let api: Arc<dyn WorkspaceApi> = Arc::new(FakeApi::new(vec![ws.clone()]));

        let _registry = WatcherRegistry::start(bus.clone(), api).await;
        tokio::time::sleep(Duration::from_millis(250)).await;

        std::fs::write(root.path.join("hello.txt"), "hi").expect("write file");

        let ev = next_file_event(&mut sub, &ws.id, Duration::from_secs(10)).await;
        assert!(ev.is_some(), "boot-time workspace must emit file events");
    }

    #[tokio::test]
    async fn workspace_created_after_start_gains_watching_and_deletion_stops_it() {
        let (_db, bus, mut sub) = bus_and_sub().await;
        let api: Arc<dyn WorkspaceApi> = Arc::new(FakeApi::new(Vec::new()));

        let _registry = WatcherRegistry::start(bus.clone(), api).await;
        tokio::time::sleep(Duration::from_millis(100)).await;

        // Register a workspace at runtime via `workspace:created` (payload
        // carries the workspace row per §6.7).
        let root = TempDir::new("dynamic");
        let ws = test_workspace("ws-dynamic", &root.path);
        bus.publish(&lifecycle_event(WORKSPACE_CREATED, &ws, true))
            .await
            .expect("publish created");
        tokio::time::sleep(Duration::from_millis(400)).await;

        std::fs::write(root.path.join("after-create.txt"), "hi").expect("write file");
        let ev = next_file_event(&mut sub, &ws.id, Duration::from_secs(10)).await;
        assert!(
            ev.is_some(),
            "workspace registered after start must emit file events"
        );

        // Deregister via `workspace:deleted`: watching stops.
        bus.publish(&lifecycle_event(WORKSPACE_DELETED, &ws, false))
            .await
            .expect("publish deleted");
        tokio::time::sleep(Duration::from_millis(400)).await;

        std::fs::write(root.path.join("after-delete.txt"), "hi").expect("write file");
        let ev = next_file_event(&mut sub, &ws.id, Duration::from_secs(2)).await;
        assert!(
            ev.is_none(),
            "deregistered workspace must stop emitting file events, got {ev:?}"
        );
    }

    #[tokio::test]
    async fn workspace_opened_resolves_path_via_services() {
        let (_db, bus, mut sub) = bus_and_sub().await;
        let root = TempDir::new("opened");
        let ws = test_workspace("ws-opened", &root.path);
        // Known to the service layer but NOT part of the boot seed (empty
        // list), like a workspace opened later: `workspace:opened` carries
        // only the id, so the registry must resolve the path via the api.
        let api: Arc<dyn WorkspaceApi> = Arc::new(FakeApi::new(vec![ws.clone()]));

        let _registry = WatcherRegistry::start(bus.clone(), api).await;
        tokio::time::sleep(Duration::from_millis(250)).await;

        // Simulate close → open: after close the watchers are gone, and the
        // reopen path exercises the get_workspace lookup.
        bus.publish(&lifecycle_event(WORKSPACE_CLOSED, &ws, false))
            .await
            .expect("publish closed");
        tokio::time::sleep(Duration::from_millis(400)).await;

        bus.publish(&lifecycle_event(WORKSPACE_OPENED, &ws, false))
            .await
            .expect("publish opened");
        tokio::time::sleep(Duration::from_millis(400)).await;

        std::fs::write(root.path.join("after-open.txt"), "hi").expect("write file");
        let ev = next_file_event(&mut sub, &ws.id, Duration::from_secs(10)).await;
        assert!(
            ev.is_some(),
            "reopened workspace must emit file events (path resolved via services)"
        );
    }
}
