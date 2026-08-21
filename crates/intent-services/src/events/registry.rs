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
//!
//! `workspace:created` registration is DEFERRED until the create flow's setup
//! stage finishes: the pending root is held until `workspace:setup:completed`
//! arrives for the workspace, and only then do the file watcher, git-metadata
//! watcher, and skills/specialists registrations start. No watcher exists
//! during the setup window, so setup-script churn is naturally dropped — never
//! published, never persisted (no buffering). Creates without a setup script
//! publish `completed { ranScript: false }` immediately, so their deferral is
//! just the event round-trip. A backstop starts the watchers anyway (with a
//! WARN) if no completion is observed within [`SETUP_COMPLETION_BACKSTOP`] —
//! a setup script running longer than that emits its remaining churn.
//! Boot-time seeding and `workspace:opened` start immediately as before.
//!
//! Archive/unarchive is a fifth transition. §6.5 has no `workspace:archived`,
//! so `archive_workspace`/`unarchive_workspace` publish `workspace:updated`
//! with an `archived` boolean in the delta; the registry subscribes to that
//! event and treats `archived: true` as a suspend (all watch roots torn down —
//! otherwise every archived workspace leaks its `FSEvents` streams until daemon
//! restart) and `archived: false` as a resume. Resume additionally runs a
//! catch-up so derived state changed while unwatched is not silently lost: a
//! `GitStatusRefresher::trigger` plus the skills/specialists rescan (both
//! fingerprint-checked, so an untouched tree emits nothing).

use std::collections::HashMap;
use std::path::{Path, PathBuf};
use std::sync::Arc;
use std::time::Duration;

use intent_core::events::{
    WORKSPACE_CLOSED, WORKSPACE_CREATED, WORKSPACE_DELETED, WORKSPACE_OPENED,
    WORKSPACE_SETUP_COMPLETED, WORKSPACE_UPDATED,
};
use intent_core::{Event, WorkspaceApi, WorkspaceId};
use tokio::task::JoinHandle;
use tokio::time::Instant;

use super::bus::EventBus;
use super::filter::SubscriptionFilter;
use super::git_metadata_watcher::{GitCommonDirWatches, GitMetadataWatcher};
use super::git_status_refresher::GitStatusRefresher;
use super::shared_watch::SharedWatchHub;
use super::skills_watcher::SkillsWatcher;
use super::specialists_watcher::SpecialistsWatcher;
use super::watcher::FileWatcher;

/// How long a created workspace's deferred watcher start waits for
/// `workspace:setup:completed` before starting anyway (with a WARN). Bounds
/// the deferral when the completion is never observed (missed event, daemon
/// race, hung script) so file events are never silenced indefinitely.
const SETUP_COMPLETION_BACKSTOP: Duration = Duration::from_secs(60);

/// A created workspace whose watcher start is deferred until its setup stage
/// completes: the root resolved from the `workspace:created` payload plus the
/// backstop deadline.
struct PendingSetup {
    path: PathBuf,
    deadline: Instant,
}

/// Coordinates the watcher families against the live workspace set.
/// Dropping the registry tears down the lifecycle task and every watcher it
/// owns (clean shutdown, matching the previous boot-time handles).
pub struct WatcherRegistry {
    task: JoinHandle<()>,
    /// Retained only so tests can await watch establishment; the lifecycle task
    /// owns the hub for production purposes.
    #[cfg(test)]
    hub: Arc<SharedWatchHub>,
    /// Retained only so tests can assert the shared common-dir refcount; the
    /// lifecycle task owns the registry for production purposes.
    #[cfg(test)]
    git_common: Arc<GitCommonDirWatches>,
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
    /// carry the workspace row (e.g. `workspace:opened`). `refresher` receives
    /// the `.git` metadata detections (external git operations, monorepo#1397).
    pub async fn start(
        bus: EventBus,
        services: Arc<dyn WorkspaceApi>,
        refresher: Arc<GitStatusRefresher>,
    ) -> Self {
        Self::start_with_backstop(bus, services, refresher, SETUP_COMPLETION_BACKSTOP).await
    }

    /// [`Self::start`] with an explicit setup-completion backstop, so tests
    /// can exercise the backstop without waiting out the production window.
    async fn start_with_backstop(
        bus: EventBus,
        services: Arc<dyn WorkspaceApi>,
        refresher: Arc<GitStatusRefresher>,
        setup_backstop: Duration,
    ) -> Self {
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
                WORKSPACE_UPDATED.to_string(),
                WORKSPACE_SETUP_COMPLETED.to_string(),
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

        // All four families share the hub's streams, so the steady-state
        // FSEvents stream count follows the number of distinct parent
        // directories the workspace roots live under, not the workspace count
        // times the number of watch roots each one used to register.
        let hub = SharedWatchHub::new();
        // Shared common-dir watches for linked-worktree workspaces: one watch
        // per repo, fanned out to every worktree workspace (monorepo#1663).
        let git_common = GitCommonDirWatches::new();

        let mut file_watchers: HashMap<WorkspaceId, FileWatcher> = HashMap::new();
        for (ws_id, path) in &initial {
            tracing::info!(workspace = %ws_id, path = %path.display(), "watching workspace files");
            file_watchers.insert(
                ws_id.clone(),
                FileWatcher::start(&hub, bus.clone(), ws_id.clone(), &path.clone()),
            );
        }
        tracing::info!(count = file_watchers.len(), "file watchers started");

        let mut git_watchers: HashMap<WorkspaceId, GitMetadataWatcher> = HashMap::new();
        for (ws_id, path) in &initial {
            if let Some(w) = start_git_metadata_watch(
                &hub,
                &git_common,
                &refresher,
                &ws_id.clone(),
                &path.clone(),
                "",
            ) {
                git_watchers.insert(ws_id.clone(), w);
            }
        }
        tracing::info!(count = git_watchers.len(), "git metadata watchers started");

        let skills = SkillsWatcher::start(&hub, bus.clone(), initial.clone());
        tracing::info!("skills watcher started");
        let specialists = SpecialistsWatcher::start(&hub, bus.clone(), initial);
        tracing::info!("specialists watcher started");

        let task = tokio::spawn(lifecycle_loop(
            Arc::clone(&hub),
            Arc::clone(&git_common),
            bus,
            services,
            refresher,
            sub,
            file_watchers,
            git_watchers,
            skills,
            specialists,
            setup_backstop,
        ));
        Self {
            task,
            #[cfg(test)]
            hub,
            #[cfg(test)]
            git_common,
        }
    }

    /// Await every requested shared watch actually being registered with the
    /// OS. Registration is deferred off the caller's thread (monorepo#1572), so
    /// tests must wait for it before mutating a watched tree.
    /// `expect_roots` is the number of watched roots the caller expects to
    /// exist, so a wait issued before the registry has subscribed does not
    /// return immediately against an empty hub.
    #[cfg(test)]
    async fn wait_established(&self, expect_roots: usize, timeout: std::time::Duration) {
        self.hub.wait_all_established(expect_roots, timeout).await;
    }

    /// Registration state of one specific root — see
    /// [`SharedWatchHub::root_established`].
    #[cfg(test)]
    fn root_established(&self, root: &std::path::Path) -> Option<bool> {
        self.hub.root_established(root)
    }

    /// Live shared `FSEvents` stream count — the consolidation metric.
    #[cfg(test)]
    fn stream_count(&self) -> usize {
        self.hub.stream_count()
    }

    /// Live shared common-dir watch count — the linked-worktree refcount
    /// metric.
    #[cfg(test)]
    fn common_dir_watch_count(&self) -> usize {
        self.git_common.watch_count()
    }
}

/// Start the `.git` metadata watch for one workspace, logging the outcome.
/// `None` covers the quiet non-git case.
fn start_git_metadata_watch(
    hub: &Arc<SharedWatchHub>,
    common_watches: &Arc<GitCommonDirWatches>,
    refresher: &Arc<GitStatusRefresher>,
    ws_id: &WorkspaceId,
    path: &Path,
    suffix: &str,
) -> Option<GitMetadataWatcher> {
    match GitMetadataWatcher::start(
        hub,
        common_watches,
        Arc::clone(refresher),
        ws_id.clone(),
        &path.to_path_buf(),
    ) {
        Some(w) => {
            tracing::info!(workspace = %ws_id, path = %path.display(), "watching workspace .git metadata{suffix}");
            Some(w)
        }
        None => {
            tracing::debug!(workspace = %ws_id, path = %path.display(), "no .git directory; not watching git metadata");
            None
        }
    }
}

/// Start (or replace) the file + `.git` metadata watches for one workspace.
/// `suffix` distinguishes the triggering transition in the logs.
#[allow(clippy::too_many_arguments)]
fn start_watches(
    hub: &Arc<SharedWatchHub>,
    common_watches: &Arc<GitCommonDirWatches>,
    bus: &EventBus,
    refresher: &Arc<GitStatusRefresher>,
    file_watchers: &mut HashMap<WorkspaceId, FileWatcher>,
    git_watchers: &mut HashMap<WorkspaceId, GitMetadataWatcher>,
    ws_id: &WorkspaceId,
    path: &std::path::Path,
    suffix: &str,
) {
    tracing::info!(workspace = %ws_id, path = %path.display(), "watching workspace files{suffix}");
    file_watchers.insert(
        ws_id.clone(),
        FileWatcher::start(hub, bus.clone(), ws_id.clone(), path),
    );
    if let Some(w) =
        start_git_metadata_watch(hub, common_watches, refresher, &ws_id.clone(), path, suffix)
    {
        git_watchers.insert(ws_id.clone(), w);
    }
}

/// Drop the file + `.git` metadata watches for one workspace (the OS
/// subscriptions end with the watcher handles). `reason` names the transition
/// in the logs.
fn stop_watches(
    file_watchers: &mut HashMap<WorkspaceId, FileWatcher>,
    git_watchers: &mut HashMap<WorkspaceId, GitMetadataWatcher>,
    ws_id: &WorkspaceId,
    reason: &str,
) {
    if file_watchers.remove(ws_id).is_some() {
        tracing::info!(workspace = %ws_id, "workspace file watcher stopped ({reason})");
    }
    if git_watchers.remove(ws_id).is_some() {
        tracing::info!(workspace = %ws_id, "workspace git metadata watcher stopped ({reason})");
    }
}

/// Read the `archived` boolean out of a `workspace:updated` delta
/// (`data.changes.archived`). `None` for every other update.
fn archived_delta(ev: &Event) -> Option<bool> {
    ev.data
        .get("changes")
        .and_then(|c| c.get("archived"))
        .and_then(serde_json::Value::as_bool)
}

/// Follow workspace lifecycle events, registering/deregistering watch roots.
#[allow(clippy::too_many_arguments)]
async fn lifecycle_loop(
    hub: Arc<SharedWatchHub>,
    common_watches: Arc<GitCommonDirWatches>,
    bus: EventBus,
    services: Arc<dyn WorkspaceApi>,
    refresher: Arc<GitStatusRefresher>,
    mut sub: super::bus::Subscription,
    mut file_watchers: HashMap<WorkspaceId, FileWatcher>,
    mut git_watchers: HashMap<WorkspaceId, GitMetadataWatcher>,
    skills: SkillsWatcher,
    specialists: SpecialistsWatcher,
    setup_backstop: Duration,
) {
    // Created workspaces awaiting `workspace:setup:completed` before their
    // watchers start. The loop sleeps toward the earliest deadline; a
    // deadline reached without a completion starts the watchers anyway.
    let mut pending: HashMap<WorkspaceId, PendingSetup> = HashMap::new();
    loop {
        let batch = match pending.values().map(|p| p.deadline).min() {
            None => match sub.recv().await {
                Some(batch) => batch,
                None => return,
            },
            Some(deadline) => tokio::select! {
                batch = sub.recv() => match batch {
                    Some(batch) => batch,
                    None => return,
                },
                () = tokio::time::sleep_until(deadline) => {
                    let now = Instant::now();
                    let expired: Vec<WorkspaceId> = pending
                        .iter()
                        .filter(|(_, p)| p.deadline <= now)
                        .map(|(id, _)| id.clone())
                        .collect();
                    for ws_id in expired {
                        let p = pending.remove(&ws_id).expect("expired entry present");
                        tracing::warn!(
                            workspace = %ws_id,
                            "workspace setup completion not observed within {}s; starting watchers anyway",
                            setup_backstop.as_secs(),
                        );
                        start_watches(
                            &hub,
                            &common_watches,
                            &bus,
                            &refresher,
                            &mut file_watchers,
                            &mut git_watchers,
                            &ws_id,
                            &p.path,
                            " (setup completion backstop)",
                        );
                        skills.add_workspace(ws_id.clone(), &p.path.clone());
                        specialists.add_workspace(ws_id, &p.path);
                    }
                    continue;
                }
            },
        };
        for ev in batch {
            let ws_id = ev.workspace_id.clone();
            match ev.event_type.as_str() {
                // Deferred start: hold the pending root until the create
                // flow's setup stage completes (see the module docs). No
                // watcher exists during the setup window, so setup churn is
                // naturally dropped.
                WORKSPACE_CREATED => {
                    let Some(path) = resolve_path(&ev, services.as_ref()).await else {
                        tracing::debug!(workspace = %ws_id, "lifecycle event without resolvable path; not watching");
                        continue;
                    };
                    tracing::info!(workspace = %ws_id, path = %path.display(), "deferring watcher start until workspace setup completes");
                    pending.insert(
                        ws_id,
                        PendingSetup {
                            path,
                            deadline: Instant::now() + setup_backstop,
                        },
                    );
                }
                WORKSPACE_SETUP_COMPLETED => {
                    // Only meaningful for a deferred create; a completion for
                    // an already-watched (or unknown) workspace is ignored.
                    let Some(p) = pending.remove(&ws_id) else {
                        continue;
                    };
                    start_watches(
                        &hub,
                        &common_watches,
                        &bus,
                        &refresher,
                        &mut file_watchers,
                        &mut git_watchers,
                        &ws_id,
                        &p.path,
                        " (setup completed)",
                    );
                    skills.add_workspace(ws_id.clone(), &p.path.clone());
                    specialists.add_workspace(ws_id, &p.path);
                }
                WORKSPACE_OPENED => {
                    let Some(path) = resolve_path(&ev, services.as_ref()).await else {
                        tracing::debug!(workspace = %ws_id, "lifecycle event without resolvable path; not watching");
                        continue;
                    };
                    // An open during the setup window supersedes the deferral
                    // (the user is in the workspace; watch it now) — clear
                    // the pending entry so the backstop cannot restart the
                    // watches later and reopen a brief event-loss window.
                    pending.remove(&ws_id);
                    start_watches(
                        &hub,
                        &common_watches,
                        &bus,
                        &refresher,
                        &mut file_watchers,
                        &mut git_watchers,
                        &ws_id,
                        &path,
                        " (runtime registration)",
                    );
                    skills.add_workspace(ws_id.clone(), &path.clone());
                    specialists.add_workspace(ws_id, &path);
                }
                WORKSPACE_DELETED | WORKSPACE_CLOSED => {
                    if pending.remove(&ws_id).is_some() {
                        tracing::info!(workspace = %ws_id, "discarding deferred watcher start (runtime deregistration)");
                    }
                    stop_watches(
                        &mut file_watchers,
                        &mut git_watchers,
                        &ws_id,
                        "runtime deregistration",
                    );
                    skills.remove_workspace(&ws_id);
                    specialists.remove_workspace(&ws_id);
                }
                // Archive/unarchive rides `workspace:updated` (§6.5 has no
                // `workspace:archived`). Deltas without an `archived` key —
                // the overwhelming majority — cost one JSON lookup and are
                // ignored before any path resolution.
                WORKSPACE_UPDATED => match archived_delta(&ev) {
                    Some(true) => {
                        if pending.remove(&ws_id).is_some() {
                            tracing::info!(workspace = %ws_id, "discarding deferred watcher start (workspace archived)");
                        }
                        stop_watches(
                            &mut file_watchers,
                            &mut git_watchers,
                            &ws_id,
                            "workspace archived",
                        );
                        skills.pause_workspace(&ws_id);
                        specialists.pause_workspace(&ws_id);
                    }
                    Some(false) if file_watchers.contains_key(&ws_id) => {
                        // Redundant `archived: false` (a `workspace.update`
                        // restating the flag, or a double unarchive): the
                        // watches are live, so replacing them would open a
                        // brief event-loss window while the OS streams
                        // restart, for no gain.
                        tracing::debug!(workspace = %ws_id, "already watching; ignoring redundant unarchive delta");
                    }
                    Some(false) => {
                        let Some(path) = resolve_path(&ev, services.as_ref()).await else {
                            tracing::debug!(workspace = %ws_id, "unarchived workspace without resolvable path; not watching");
                            continue;
                        };
                        start_watches(
                            &hub,
                            &common_watches,
                            &bus,
                            &refresher,
                            &mut file_watchers,
                            &mut git_watchers,
                            &ws_id,
                            &path,
                            " (workspace unarchived)",
                        );
                        // Catch-up for the unwatched window: recompute the
                        // derived state that missed `file:*`/`.git` events
                        // would have refreshed. Cost is bounded to one pass
                        // per workspace, but this is not a strict no-op for an
                        // untouched workspace. The skills and specialists
                        // flushes compare against the fingerprint each watcher
                        // retained at pause and stay silent when nothing
                        // moved; a workspace archived before daemon start has
                        // no such baseline (boot seeds only unarchived
                        // workspaces) and emits one benign extra event.
                        // `GitStatusRefresher::trigger` always republishes
                        // `changes:git-status` — it debounces and holds no
                        // baseline, exactly as it does for any single `file:*`
                        // event today.
                        refresher.trigger(ws_id.clone());
                        skills.resume_workspace(ws_id.clone(), &path.clone());
                        specialists.resume_workspace(ws_id, &path);
                    }
                    None => {}
                },
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
    use crate::events::LIVENESS;

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

    /// A `workspace:updated` event shaped exactly like the archive/unarchive
    /// emitters in `lib.rs`: id-only payload plus the applied delta, no
    /// workspace row (so the registry must resolve the path via the api).
    fn archive_event(ws: &Workspace, archived: bool) -> NewEvent {
        let mut ev = lifecycle_event(WORKSPACE_UPDATED, ws, false);
        ev.data = serde_json::json!({
            "workspaceId": ws.id.as_str(),
            "changes": { "archived": archived },
        });
        ev
    }

    /// A `workspace:setup:completed` event shaped like the create-flow
    /// publisher in `lib.rs`: id-only payload plus the `ranScript` flag (the
    /// registry keys off the workspace id alone).
    fn setup_completed_event(ws: &Workspace, ran_script: bool) -> NewEvent {
        let mut ev = lifecycle_event(WORKSPACE_SETUP_COMPLETED, ws, false);
        ev.data = serde_json::json!({
            "workspaceId": ws.id.as_str(),
            "ranScript": ran_script,
        });
        ev
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

    /// Start a registry with its own refresher (the production wiring shape).
    async fn start_registry(bus: &EventBus, api: Arc<dyn WorkspaceApi>) -> WatcherRegistry {
        start_registry_with_backstop(bus, api, SETUP_COMPLETION_BACKSTOP).await
    }

    /// [`start_registry`] with an explicit setup-completion backstop, for the
    /// tests that exercise the backstop path without the production window.
    async fn start_registry_with_backstop(
        bus: &EventBus,
        api: Arc<dyn WorkspaceApi>,
        backstop: Duration,
    ) -> WatcherRegistry {
        let refresher = Arc::new(GitStatusRefresher::start(
            bus.clone(),
            api.clone(),
            Arc::new(crate::git_status_cache::GitStatusCache::new()),
        ));
        WatcherRegistry::start_with_backstop(bus.clone(), api, refresher, backstop).await
    }

    /// Wait until `root` is watched-and-established (`want = true`) or absent
    /// from the hub entirely (`want = false`), before mutating a watched tree.
    /// Registration is deliberately off the runtime (monorepo#1572), so waiting
    /// on the hub's own establishment signal — rather than guessing with a fixed
    /// sleep — is what makes these tests deterministic. The check is per-root
    /// rather than a total count because the skills/specialists tier roots share
    /// the hub, so a count can be satisfied by the wrong root and let a test
    /// race ahead of the registration it actually cares about. The short
    /// trailing sleep is the usual FSEvents/inotify settle margin: `watch()` has
    /// returned, but the backend needs a moment before it reports changes.
    async fn wait_for_root(registry: &WatcherRegistry, root: &std::path::Path, want: bool) {
        let deadline = tokio::time::Instant::now() + LIVENESS;
        loop {
            let ready = match registry.root_established(root) {
                Some(established) => want && established,
                None => !want,
            };
            if ready || tokio::time::Instant::now() >= deadline {
                break;
            }
            tokio::time::sleep(Duration::from_millis(10)).await;
        }
        tokio::time::sleep(Duration::from_millis(300)).await;
    }

    /// Actively confirm `ws_id`'s watch is live: rewrite a throwaway probe file
    /// under `root` until an event for it comes back. Preferable to a fixed
    /// warm-up wherever a test then asserts the ABSENCE of an event, since a
    /// warm-up that lost the establishment race would make that absence
    /// vacuously true.
    async fn confirm_watch_live(
        sub: &mut super::super::bus::Subscription,
        ws_id: &WorkspaceId,
        root: &std::path::Path,
    ) {
        let probe = root.join(".watch-probe");
        // Attempt count sized so the total probe budget (attempts x 500ms)
        // reaches `LIVENESS` — a pure-liveness bound (monorepo#1630).
        let attempts = LIVENESS.as_millis() / 500;
        for attempt in 0..attempts {
            std::fs::write(&probe, format!("{attempt}")).expect("write probe");
            if next_file_event(sub, ws_id, Duration::from_millis(500))
                .await
                .is_some()
            {
                let _ = std::fs::remove_file(&probe);
                // Drain the removal's own event so it cannot be mistaken for a
                // later assertion's subject.
                while next_file_event(sub, ws_id, Duration::from_millis(500))
                    .await
                    .is_some()
                {}
                return;
            }
        }
        panic!("watch for {ws_id} never became live");
    }

    #[tokio::test]
    #[allow(clippy::await_holding_lock)]
    async fn boot_time_workspace_is_watched() {
        let _serial = crate::events::WATCHER_TEST_SERIAL
            .lock()
            .unwrap_or_else(std::sync::PoisonError::into_inner);
        let (_db, bus, mut sub) = bus_and_sub().await;
        let root = TempDir::new("boot");
        let ws = test_workspace("ws-boot", &root.path);
        let api: Arc<dyn WorkspaceApi> = Arc::new(FakeApi::new(vec![ws.clone()]));

        let _registry = start_registry(&bus, api).await;
        wait_for_root(&_registry, &root.path, true).await;

        std::fs::write(root.path.join("hello.txt"), "hi").expect("write file");

        let ev = next_file_event(&mut sub, &ws.id, LIVENESS).await;
        assert!(ev.is_some(), "boot-time workspace must emit file events");
    }

    #[tokio::test]
    #[allow(clippy::await_holding_lock)]
    async fn workspace_created_after_start_gains_watching_and_deletion_stops_it() {
        let _serial = crate::events::WATCHER_TEST_SERIAL
            .lock()
            .unwrap_or_else(std::sync::PoisonError::into_inner);
        let (_db, bus, mut sub) = bus_and_sub().await;
        let api: Arc<dyn WorkspaceApi> = Arc::new(FakeApi::new(Vec::new()));

        let _registry = start_registry(&bus, api).await;
        // Nothing is watched yet: the boot seed is empty.

        // Register a workspace at runtime via `workspace:created` (payload
        // carries the workspace row per §6.7). Watcher start is deferred
        // until the setup stage completes, so publish the completion too.
        let root = TempDir::new("dynamic");
        let ws = test_workspace("ws-dynamic", &root.path);
        bus.publish(&lifecycle_event(WORKSPACE_CREATED, &ws, true))
            .await
            .expect("publish created");
        bus.publish(&setup_completed_event(&ws, true))
            .await
            .expect("publish setup completed");
        wait_for_root(&_registry, &root.path, true).await;

        std::fs::write(root.path.join("after-create.txt"), "hi").expect("write file");
        let ev = next_file_event(&mut sub, &ws.id, LIVENESS).await;
        assert!(
            ev.is_some(),
            "workspace registered after start must emit file events"
        );

        // Deregister via `workspace:deleted`: watching stops.
        bus.publish(&lifecycle_event(WORKSPACE_DELETED, &ws, false))
            .await
            .expect("publish deleted");
        wait_for_root(&_registry, &root.path, false).await;

        std::fs::write(root.path.join("after-delete.txt"), "hi").expect("write file");
        let ev = next_file_event(&mut sub, &ws.id, Duration::from_secs(2)).await;
        assert!(
            ev.is_none(),
            "deregistered workspace must stop emitting file events, got {ev:?}"
        );
    }

    /// The deferral core (setup gating): `workspace:created` must NOT start
    /// any watcher — setup-window file churn is dropped, never published —
    /// and `workspace:setup:completed` starts the watchers, after which
    /// events flow normally.
    #[tokio::test]
    #[allow(clippy::await_holding_lock)]
    async fn created_workspace_defers_watching_until_setup_completes() {
        let _serial = crate::events::WATCHER_TEST_SERIAL
            .lock()
            .unwrap_or_else(std::sync::PoisonError::into_inner);
        let (_db, bus, mut sub) = bus_and_sub().await;
        let api: Arc<dyn WorkspaceApi> = Arc::new(FakeApi::new(Vec::new()));

        let _registry = start_registry(&bus, api).await;

        let root = TempDir::new("setup-deferred");
        let ws = test_workspace("ws-setup-deferred", &root.path);
        bus.publish(&lifecycle_event(WORKSPACE_CREATED, &ws, true))
            .await
            .expect("publish created");

        // Setup window: the root must not even be registered with the hub,
        // and churn under it must publish nothing.
        tokio::time::sleep(Duration::from_millis(300)).await;
        assert!(
            _registry.root_established(&root.path).is_none(),
            "created workspace must not be watched while setup is pending"
        );
        std::fs::write(root.path.join("during-setup.txt"), "hi").expect("write file");
        let ev = next_file_event(&mut sub, &ws.id, Duration::from_secs(2)).await;
        assert!(
            ev.is_none(),
            "setup-window file churn must be dropped, got {ev:?}"
        );

        // Completion starts the watchers; events flow from here on.
        bus.publish(&setup_completed_event(&ws, true))
            .await
            .expect("publish setup completed");
        wait_for_root(&_registry, &root.path, true).await;

        std::fs::write(root.path.join("after-setup.txt"), "hi").expect("write file");
        let ev = next_file_event(&mut sub, &ws.id, LIVENESS).await;
        assert!(
            ev.is_some(),
            "workspace must emit file events after setup completes"
        );
    }

    /// No-script creates publish `completed { ranScript: false }` right after
    /// `workspace:created`, so the deferral is just the event round-trip:
    /// watchers start promptly and events flow.
    #[tokio::test]
    #[allow(clippy::await_holding_lock)]
    async fn no_script_completion_starts_watching_promptly() {
        let _serial = crate::events::WATCHER_TEST_SERIAL
            .lock()
            .unwrap_or_else(std::sync::PoisonError::into_inner);
        let (_db, bus, mut sub) = bus_and_sub().await;
        let api: Arc<dyn WorkspaceApi> = Arc::new(FakeApi::new(Vec::new()));

        let _registry = start_registry(&bus, api).await;

        let root = TempDir::new("no-script");
        let ws = test_workspace("ws-no-script", &root.path);
        bus.publish(&lifecycle_event(WORKSPACE_CREATED, &ws, true))
            .await
            .expect("publish created");
        bus.publish(&setup_completed_event(&ws, false))
            .await
            .expect("publish setup completed");
        wait_for_root(&_registry, &root.path, true).await;

        std::fs::write(root.path.join("no-script.txt"), "hi").expect("write file");
        let ev = next_file_event(&mut sub, &ws.id, LIVENESS).await;
        assert!(
            ev.is_some(),
            "no-script create must start watching on the immediate completion"
        );
    }

    /// Backstop: when `workspace:setup:completed` is never observed (missed
    /// event, hung script), the watchers must start anyway once the backstop
    /// elapses.
    #[tokio::test]
    #[allow(clippy::await_holding_lock)]
    async fn backstop_starts_watchers_when_setup_completion_never_arrives() {
        let _serial = crate::events::WATCHER_TEST_SERIAL
            .lock()
            .unwrap_or_else(std::sync::PoisonError::into_inner);
        let (_db, bus, mut sub) = bus_and_sub().await;
        let api: Arc<dyn WorkspaceApi> = Arc::new(FakeApi::new(Vec::new()));

        let _registry = start_registry_with_backstop(&bus, api, Duration::from_millis(500)).await;

        let root = TempDir::new("backstop");
        let ws = test_workspace("ws-backstop", &root.path);
        bus.publish(&lifecycle_event(WORKSPACE_CREATED, &ws, true))
            .await
            .expect("publish created");
        // No completion published: the backstop alone must start the watch.
        wait_for_root(&_registry, &root.path, true).await;

        std::fs::write(root.path.join("after-backstop.txt"), "hi").expect("write file");
        let ev = next_file_event(&mut sub, &ws.id, LIVENESS).await;
        assert!(
            ev.is_some(),
            "backstop must start watchers when setup completion never arrives"
        );
    }

    /// A delete during the setup window discards the pending entry: neither
    /// the (late) completion nor the backstop may start watchers for it.
    #[tokio::test]
    #[allow(clippy::await_holding_lock)]
    async fn delete_while_pending_discards_the_deferred_start() {
        let _serial = crate::events::WATCHER_TEST_SERIAL
            .lock()
            .unwrap_or_else(std::sync::PoisonError::into_inner);
        let (_db, bus, mut sub) = bus_and_sub().await;
        let api: Arc<dyn WorkspaceApi> = Arc::new(FakeApi::new(Vec::new()));

        let _registry = start_registry_with_backstop(&bus, api, Duration::from_millis(500)).await;

        let root = TempDir::new("delete-pending");
        let ws = test_workspace("ws-delete-pending", &root.path);
        bus.publish(&lifecycle_event(WORKSPACE_CREATED, &ws, true))
            .await
            .expect("publish created");
        bus.publish(&lifecycle_event(WORKSPACE_DELETED, &ws, false))
            .await
            .expect("publish deleted");
        // A straggler completion after the delete must be a no-op too.
        bus.publish(&setup_completed_event(&ws, true))
            .await
            .expect("publish setup completed");

        // Ride out the backstop window: nothing may have started.
        tokio::time::sleep(Duration::from_millis(1500)).await;
        assert!(
            _registry.root_established(&root.path).is_none(),
            "deleted-while-pending workspace must never be watched"
        );
        std::fs::write(root.path.join("never.txt"), "hi").expect("write file");
        let ev = next_file_event(&mut sub, &ws.id, Duration::from_secs(2)).await;
        assert!(
            ev.is_none(),
            "deleted-while-pending workspace must not emit file events, got {ev:?}"
        );
    }

    #[tokio::test]
    #[allow(clippy::await_holding_lock)]
    async fn workspace_opened_resolves_path_via_services() {
        let _serial = crate::events::WATCHER_TEST_SERIAL
            .lock()
            .unwrap_or_else(std::sync::PoisonError::into_inner);
        let (_db, bus, mut sub) = bus_and_sub().await;
        let root = TempDir::new("opened");
        let ws = test_workspace("ws-opened", &root.path);
        // Known to the service layer but NOT part of the boot seed (empty
        // list), like a workspace opened later: `workspace:opened` carries
        // only the id, so the registry must resolve the path via the api.
        let api: Arc<dyn WorkspaceApi> = Arc::new(FakeApi::new(vec![ws.clone()]));

        let _registry = start_registry(&bus, api).await;
        wait_for_root(&_registry, &root.path, true).await;

        // Simulate close → open: after close the watchers are gone, and the
        // reopen path exercises the get_workspace lookup.
        bus.publish(&lifecycle_event(WORKSPACE_CLOSED, &ws, false))
            .await
            .expect("publish closed");
        wait_for_root(&_registry, &root.path, false).await;

        bus.publish(&lifecycle_event(WORKSPACE_OPENED, &ws, false))
            .await
            .expect("publish opened");
        wait_for_root(&_registry, &root.path, true).await;

        std::fs::write(root.path.join("after-open.txt"), "hi").expect("write file");
        let ev = next_file_event(&mut sub, &ws.id, LIVENESS).await;
        assert!(
            ev.is_some(),
            "reopened workspace must emit file events (path resolved via services)"
        );
    }

    /// Consolidation regression: two workspaces whose roots sit under the same
    /// parent share one `FSEvents` stream, so the in-process demux is the only
    /// thing keeping them apart. Each must publish `file:*` events for its own
    /// paths only — a leak here would attribute one workspace's edits to the
    /// other.
    #[tokio::test]
    #[allow(clippy::await_holding_lock)]
    async fn workspaces_sharing_a_consolidated_root_receive_only_their_own_file_events() {
        let _serial = crate::events::WATCHER_TEST_SERIAL
            .lock()
            .unwrap_or_else(std::sync::PoisonError::into_inner);
        let (_db, bus, mut sub) = bus_and_sub().await;
        // Sibling roots under one parent: the hub groups them onto a single
        // shared stream.
        let parent = TempDir::new("shared");
        let root_a = parent.path.join("ws-a");
        let root_b = parent.path.join("ws-b");
        std::fs::create_dir_all(&root_a).expect("mk ws-a");
        std::fs::create_dir_all(&root_b).expect("mk ws-b");
        let ws_a = test_workspace("ws-shared-a", &root_a);
        let ws_b = test_workspace("ws-shared-b", &root_b);
        let api: Arc<dyn WorkspaceApi> = Arc::new(FakeApi::new(vec![ws_a.clone(), ws_b.clone()]));

        let _registry = start_registry(&bus, api).await;
        // Both watches must be confirmed live before the isolation assertions:
        // the negative one below would pass vacuously against an unestablished
        // watch.
        confirm_watch_live(&mut sub, &ws_a.id, &root_a).await;
        confirm_watch_live(&mut sub, &ws_b.id, &root_b).await;

        std::fs::write(root_a.join("in-a.txt"), "hi").expect("write in a");
        let ev = next_file_event(&mut sub, &ws_a.id, LIVENESS)
            .await
            .expect("workspace a must emit for its own file");
        assert_eq!(ev.data["relativePath"], "in-a.txt");

        // Nothing for b: it shares the stream but not the path prefix.
        let leaked = next_file_event(&mut sub, &ws_b.id, Duration::from_secs(2)).await;
        assert!(
            leaked.is_none(),
            "a sibling workspace must not receive events for another's paths, got {leaked:?}"
        );

        // The reverse direction too, so the assertion is not just about
        // ordering.
        std::fs::write(root_b.join("in-b.txt"), "hi").expect("write in b");
        let ev = next_file_event(&mut sub, &ws_b.id, LIVENESS)
            .await
            .expect("workspace b must emit for its own file");
        assert_eq!(ev.data["relativePath"], "in-b.txt");
    }

    /// The consolidation metric itself: many workspaces must not scale the
    /// stream count. Before this change each workspace registered its own file
    /// watcher, `.git` watcher (two roots) and four project-tier skills /
    /// specialists watches — roughly five or six OS streams each, so eight
    /// workspaces meant ~40+. Now every root under a shared parent joins ONE
    /// stream, so the count follows the number of distinct parent directories.
    #[tokio::test]
    #[allow(clippy::await_holding_lock)]
    async fn many_workspaces_share_a_single_stream_per_parent_directory() {
        let _serial = crate::events::WATCHER_TEST_SERIAL
            .lock()
            .unwrap_or_else(std::sync::PoisonError::into_inner);
        let (_db, bus, _sub) = bus_and_sub().await;
        let parent = TempDir::new("count");
        let workspaces: Vec<_> = (0..8)
            .map(|i| {
                let root = parent.path.join(format!("ws-{i}"));
                std::fs::create_dir_all(&root).expect("mk ws");
                test_workspace(&format!("ws-count-{i}"), &root)
            })
            .collect();
        let api: Arc<dyn WorkspaceApi> = Arc::new(FakeApi::new(workspaces.clone()));

        let registry = start_registry(&bus, api).await;
        registry.wait_established(workspaces.len(), LIVENESS).await;

        assert_eq!(
            registry.stream_count(),
            1,
            "8 sibling workspaces must consolidate onto a single shared stream"
        );
    }

    /// Consolidation regression for the archive lifecycle: archiving one of two
    /// workspaces on a SHARED stream must drop only that workspace from the
    /// demux, leaving its co-tenant emitting throughout, and unarchiving must
    /// put it back. The single-workspace case is covered by
    /// `archived_workspace_stops_watching_and_unarchive_resumes_it`; what is
    /// specific here is that the stream itself stays up for the sibling, so
    /// exclusion can only come from the demux table.
    #[tokio::test]
    #[allow(clippy::await_holding_lock)]
    async fn archiving_one_workspace_leaves_its_shared_stream_co_tenant_watched() {
        let _serial = crate::events::WATCHER_TEST_SERIAL
            .lock()
            .unwrap_or_else(std::sync::PoisonError::into_inner);
        let (_db, bus, mut sub) = bus_and_sub().await;
        let parent = TempDir::new("shared-archive");
        let root_a = parent.path.join("ws-a");
        let root_b = parent.path.join("ws-b");
        std::fs::create_dir_all(&root_a).expect("mk ws-a");
        std::fs::create_dir_all(&root_b).expect("mk ws-b");
        let ws_a = test_workspace("ws-shared-arch-a", &root_a);
        let ws_b = test_workspace("ws-shared-arch-b", &root_b);
        let api: Arc<dyn WorkspaceApi> = Arc::new(FakeApi::new(vec![ws_a.clone(), ws_b.clone()]));

        let _registry = start_registry(&bus, api).await;
        confirm_watch_live(&mut sub, &ws_a.id, &root_a).await;
        confirm_watch_live(&mut sub, &ws_b.id, &root_b).await;

        // Archive a only.
        bus.publish(&archive_event(&ws_a, true))
            .await
            .expect("publish archived");
        wait_for_root(&_registry, &root_a, false).await;

        std::fs::write(root_a.join("while-archived.txt"), "hi").expect("write in a");
        let leaked = next_file_event(&mut sub, &ws_a.id, Duration::from_secs(2)).await;
        assert!(
            leaked.is_none(),
            "archived workspace must be excluded from the demux, got {leaked:?}"
        );

        // b never stopped: the shared stream is still up and still demuxing.
        // Probed rather than written once, because dropping a root rebuilds the
        // group's FSEvents stream — the co-tenant stays watched, but delivery
        // resumes a moment after `unwatch` returns.
        confirm_watch_live(&mut sub, &ws_b.id, &root_b).await;

        // Unarchive a: it rejoins the demux.
        bus.publish(&archive_event(&ws_a, false))
            .await
            .expect("publish unarchived");
        wait_for_root(&_registry, &root_a, true).await;

        std::fs::write(root_a.join("after-unarchive.txt"), "hi").expect("write in a");
        let ev = next_file_event(&mut sub, &ws_a.id, LIVENESS).await;
        assert!(
            ev.is_some(),
            "unarchived workspace must rejoin the demux and resume emitting"
        );
    }

    /// Regression: archiving a workspace must tear its watch roots down.
    /// Before the fix only `workspace:deleted`/`workspace:closed` deregistered,
    /// so every archived workspace leaked its `FSEvents` streams until restart.
    #[tokio::test]
    #[allow(clippy::await_holding_lock)]
    async fn archived_workspace_stops_watching_and_unarchive_resumes_it() {
        let _serial = crate::events::WATCHER_TEST_SERIAL
            .lock()
            .unwrap_or_else(std::sync::PoisonError::into_inner);
        let (_db, bus, mut sub) = bus_and_sub().await;
        let root = TempDir::new("archived");
        let ws = test_workspace("ws-archived", &root.path);
        let api: Arc<dyn WorkspaceApi> = Arc::new(FakeApi::new(vec![ws.clone()]));

        let _registry = start_registry(&bus, api).await;
        // Probed rather than written once: the negative assertion after the
        // archive would be vacuous if delivery had never started, and delivery
        // can lag establishment by more than a single write's budget.
        confirm_watch_live(&mut sub, &ws.id, &root.path).await;

        // Archive: `workspace:updated` with `changes.archived = true`.
        bus.publish(&archive_event(&ws, true))
            .await
            .expect("publish archived");
        wait_for_root(&_registry, &root.path, false).await;

        std::fs::write(root.path.join("while-archived.txt"), "hi").expect("write file");
        let ev = next_file_event(&mut sub, &ws.id, Duration::from_secs(2)).await;
        assert!(
            ev.is_none(),
            "archived workspace must stop emitting file events, got {ev:?}"
        );

        // Unarchive: watching resumes (path resolved via the api, like
        // `workspace:opened` — the delta carries no workspace row).
        bus.publish(&archive_event(&ws, false))
            .await
            .expect("publish unarchived");
        wait_for_root(&_registry, &root.path, true).await;

        std::fs::write(root.path.join("after-unarchive.txt"), "hi").expect("write file");
        let ev = next_file_event(&mut sub, &ws.id, LIVENESS).await;
        assert!(
            ev.is_some(),
            "unarchived workspace must resume emitting file events"
        );
    }

    /// Poll the registry's shared common-dir watch count until it reaches
    /// `want` (the guard drop runs inside the lifecycle task, so the count
    /// changes shortly after — not synchronously with — the archive event).
    async fn wait_for_common_dir_watch_count(registry: &WatcherRegistry, want: usize) {
        let deadline = tokio::time::Instant::now() + Duration::from_secs(10);
        while registry.common_dir_watch_count() != want && tokio::time::Instant::now() < deadline {
            tokio::time::sleep(Duration::from_millis(10)).await;
        }
        assert_eq!(
            registry.common_dir_watch_count(),
            want,
            "shared common-dir watch count must reach {want}"
        );
    }

    /// Deterministically settle every in-flight refresh for the workspace
    /// under test before a negative probe, by flushing the shared
    /// [`GitStatusRefresher`] pipeline with a marker workspace
    /// (monorepo#2012). A fixed quiet-gap drain is a race: a refresh from
    /// earlier churn traverses `FSEvents` delivery, the 1s debounce, and a
    /// blocking-pool recompute, and under load its `changes:git-status` can
    /// publish arbitrarily later than any fixed gap.
    ///
    /// One round = trigger the (unwatched, non-repo) marker workspace and
    /// await its status event, noting whether any non-marker event arrived
    /// before it. The refresh loop is sequential and the bus broadcasts a
    /// single publisher's events in order, so a marker event bounds every
    /// publish enqueued before its own — EXCEPT a workspace refresh due in
    /// the same debounce batch, which can be processed (and published) after
    /// the marker's (per-batch `HashMap` order), and a straggler trigger that
    /// raced in just after the marker's. Both stragglers surface during the
    /// NEXT round, so the pipeline is settled once two consecutive rounds
    /// observe nothing but their marker. Panics if settlement never happens
    /// within `LIVENESS` — a real leak, not slowness.
    async fn flush_refresher(
        refresher: &GitStatusRefresher,
        sub: &mut super::super::bus::Subscription,
        marker_id: &WorkspaceId,
    ) {
        let deadline = Instant::now() + LIVENESS;
        let mut clean_rounds = 0;
        while clean_rounds < 2 {
            refresher.trigger(marker_id.clone());
            let mut clean = true;
            'round: loop {
                let remaining = deadline.saturating_duration_since(Instant::now());
                assert!(
                    !remaining.is_zero(),
                    "refresher never settled within LIVENESS"
                );
                match timeout(remaining, sub.recv()).await {
                    Ok(Some(batch)) => {
                        for ev in batch {
                            if &ev.workspace_id == marker_id {
                                break 'round;
                            }
                            clean = false;
                        }
                    }
                    _ => panic!("marker status event never arrived"),
                }
            }
            clean_rounds = if clean { clean_rounds + 1 } else { 0 };
        }
    }

    /// Archive/unarchive lifecycle against the refcounted common-dir registry
    /// (monorepo#1663): archiving a linked-worktree workspace deregisters it
    /// from the shared common-dir watch (last one out tears the watch down)
    /// and common-dir ref changes stop triggering it; unarchiving re-registers
    /// it and triggers resume.
    #[tokio::test]
    #[allow(clippy::await_holding_lock)]
    async fn archived_worktree_workspace_releases_common_dir_watch_and_unarchive_rearms_it() {
        use git2::{Repository, Signature};
        use intent_core::events::CHANGES_GIT_STATUS;

        let _serial = crate::events::WATCHER_TEST_SERIAL
            .lock()
            .unwrap_or_else(std::sync::PoisonError::into_inner);
        let (_db, bus, _file_sub) = bus_and_sub().await;
        let mut status_sub = bus.subscribe(SubscriptionFilter {
            event_types: vec![CHANGES_GIT_STATUS.to_string()],
            ..SubscriptionFilter::default()
        });

        // Main repo with a seed commit, plus a linked worktree — the
        // workspace root is the worktree, so its `.git` is a gitdir pointer.
        let main_root = TempDir::new("wt-arch-main");
        let repo = Repository::init(&main_root.path).unwrap();
        {
            let mut cfg = repo.config().unwrap();
            cfg.set_str("user.name", "Test").unwrap();
            cfg.set_str("user.email", "test@example.com").unwrap();
        }
        repo.set_head("refs/heads/main").unwrap();
        std::fs::write(main_root.path.join("seed.txt"), "seed\n").unwrap();
        let mut index = repo.index().unwrap();
        index.add_path(std::path::Path::new("seed.txt")).unwrap();
        index.write().unwrap();
        let tree = repo.find_tree(index.write_tree().unwrap()).unwrap();
        let sig = Signature::now("Test", "test@example.com").unwrap();
        repo.commit(Some("HEAD"), &sig, &sig, "seed", &tree, &[])
            .unwrap();
        let wt_parent = TempDir::new("wt-arch-linked");
        let wt_path = wt_parent.path.join("wt");
        repo.worktree("wt", &wt_path, None).unwrap();

        let mut ws = test_workspace("ws-wt-archived", &wt_path);
        ws.worktree_path = ws.path.clone();
        let api = Arc::new(FakeApi::new(vec![ws.clone()]));

        // The refresher is built explicitly (rather than via `start_registry`)
        // so the flushes below can inject marker triggers into the same
        // debounced pipeline the common-dir watch feeds.
        let refresher = Arc::new(GitStatusRefresher::start(
            bus.clone(),
            Arc::clone(&api) as Arc<dyn WorkspaceApi>,
            Arc::new(crate::git_status_cache::GitStatusCache::new()),
        ));
        let registry = WatcherRegistry::start(
            bus.clone(),
            Arc::clone(&api) as Arc<dyn WorkspaceApi>,
            Arc::clone(&refresher),
        )
        .await;

        // Marker workspace for the refresher flushes: pushed AFTER the
        // registry snapshot so it is never watched, but resolvable via the
        // api so a marker trigger recomputes (non-repo worktree → minimal
        // status) and publishes a `changes:git-status` event for it.
        let marker_root = TempDir::new("wt-arch-flush");
        let mut marker = test_workspace("ws-wt-arch-flush", &marker_root.path);
        marker.worktree_path = marker.path.clone();
        let marker_id = marker.id.clone();
        api.workspaces.lock().unwrap().push(marker);

        wait_for_root(&registry, &wt_path, true).await;
        assert_eq!(
            registry.common_dir_watch_count(),
            1,
            "worktree workspace must register on the shared common-dir watch"
        );

        // Confirm the common-dir watch actually delivers BEFORE archiving —
        // the while-archived absence assertion below would otherwise pass
        // vacuously against a watch that never worked. Retried for the same
        // delivery-start race as the other real-watcher tests; attempt count
        // sized so the total confirmation budget (attempts x 1500ms) reaches
        // `LIVENESS` — a pure-liveness bound (monorepo#1630), where a fixed
        // 20-attempt budget gave up under full parallel test load
        // (monorepo#2012).
        let head_commit = repo.head().unwrap().peel_to_commit().unwrap();
        let attempts = LIVENESS.as_millis() / 1500;
        let mut ev = None;
        for i in 0..attempts {
            repo.branch(&format!("pre-archive-{i}"), &head_commit, true)
                .unwrap();
            ev = next_status_event(&mut status_sub, &ws.id, Duration::from_millis(1500)).await;
            if ev.is_some() {
                break;
            }
        }
        assert!(
            ev.is_some(),
            "common-dir ref change must refresh the worktree workspace before archiving"
        );

        // Archive: the watcher drop must deregister the workspace — it was
        // the only rider, so the shared watch tears down entirely.
        bus.publish(&archive_event(&ws, true))
            .await
            .expect("publish archived");
        wait_for_root(&registry, &wt_path, false).await;
        wait_for_common_dir_watch_count(&registry, 0).await;

        // A common-dir ref change while archived must not trigger anything.
        // The watch is gone (count 0), so no NEW triggers can arrive for the
        // workspace — but refreshes from the pre-archive branch churn can
        // still be in flight and publish arbitrarily late under load
        // (monorepo#2012). Settle them deterministically before asserting
        // absence; a fixed quiet-gap drain here is a race, not a guarantee.
        flush_refresher(&refresher, &mut status_sub, &marker_id).await;
        repo.branch("while-archived", &head_commit, true).unwrap();
        let ev = next_status_event(&mut status_sub, &ws.id, Duration::from_secs(2)).await;
        assert!(
            ev.is_none(),
            "archived worktree workspace must not refresh on common-dir ref changes, got {ev:?}"
        );

        // Unarchive: the watcher restart must re-register the workspace.
        bus.publish(&archive_event(&ws, false))
            .await
            .expect("publish unarchived");
        wait_for_root(&registry, &wt_path, true).await;
        wait_for_common_dir_watch_count(&registry, 1).await;
        // Settle the unarchive catch-up refresh deterministically so it
        // cannot masquerade as the watch-driven event asserted below.
        flush_refresher(&refresher, &mut status_sub, &marker_id).await;

        // Common-dir ref changes trigger again. Retried for the same
        // delivery-start race as the other real-watcher tests, with the same
        // liveness-sized attempt budget as the pre-archive confirmation — the
        // re-created shared watch's registration and delivery start both lag
        // under load, and the fixed 20-attempt budget is what flaked in
        // monorepo#2012.
        let mut ev = None;
        for i in 0..attempts {
            repo.branch(&format!("after-unarchive-{i}"), &head_commit, true)
                .unwrap();
            ev = next_status_event(&mut status_sub, &ws.id, Duration::from_millis(1500)).await;
            if ev.is_some() {
                break;
            }
        }
        assert!(
            ev.is_some(),
            "unarchived worktree workspace must resume refreshing on common-dir ref changes"
        );
    }

    /// Watcher replacement must not orphan the common-dir registration
    /// (PR #1048 review): a repeated `workspace:created` (e.g. a buffered
    /// event replaying the startup snapshot) makes `start_watches` register a
    /// NEW watcher and then drop the OLD one for the same workspace — whose
    /// guard drop must not remove the successor's registration (per-guard
    /// identity token). Ref changes must keep triggering after replacement.
    #[tokio::test]
    #[allow(clippy::await_holding_lock)]
    async fn replacing_worktree_watcher_keeps_common_dir_registration_alive() {
        use git2::{Repository, Signature};
        use intent_core::events::CHANGES_GIT_STATUS;

        let _serial = crate::events::WATCHER_TEST_SERIAL
            .lock()
            .unwrap_or_else(std::sync::PoisonError::into_inner);
        let (_db, bus, _file_sub) = bus_and_sub().await;
        let mut status_sub = bus.subscribe(SubscriptionFilter {
            event_types: vec![CHANGES_GIT_STATUS.to_string()],
            ..SubscriptionFilter::default()
        });

        // Main repo with a seed commit, plus a linked worktree.
        let main_root = TempDir::new("wt-replace-main");
        let repo = Repository::init(&main_root.path).unwrap();
        {
            let mut cfg = repo.config().unwrap();
            cfg.set_str("user.name", "Test").unwrap();
            cfg.set_str("user.email", "test@example.com").unwrap();
        }
        repo.set_head("refs/heads/main").unwrap();
        std::fs::write(main_root.path.join("seed.txt"), "seed\n").unwrap();
        let mut index = repo.index().unwrap();
        index.add_path(std::path::Path::new("seed.txt")).unwrap();
        index.write().unwrap();
        let tree = repo.find_tree(index.write_tree().unwrap()).unwrap();
        let sig = Signature::now("Test", "test@example.com").unwrap();
        repo.commit(Some("HEAD"), &sig, &sig, "seed", &tree, &[])
            .unwrap();
        let wt_parent = TempDir::new("wt-replace-linked");
        let wt_path = wt_parent.path.join("wt");
        repo.worktree("wt", &wt_path, None).unwrap();

        let mut ws = test_workspace("ws-wt-replaced", &wt_path);
        ws.worktree_path = ws.path.clone();
        let api: Arc<dyn WorkspaceApi> = Arc::new(FakeApi::new(vec![ws.clone()]));

        let registry = start_registry(&bus, api).await;
        wait_for_root(&registry, &wt_path, true).await;
        wait_for_common_dir_watch_count(&registry, 1).await;

        // Replace the watcher: a buffered `workspace:created` repeating the
        // startup snapshot (held pending until its setup completion arrives).
        // The new watcher registers on the shared entry first;
        // HashMap::insert then drops the old watcher, whose guard drop must
        // NOT deregister the fresh registration.
        bus.publish(&lifecycle_event(WORKSPACE_CREATED, &ws, true))
            .await
            .expect("publish repeated created");
        bus.publish(&setup_completed_event(&ws, true))
            .await
            .expect("publish setup completed");
        wait_for_root(&registry, &wt_path, true).await;
        wait_for_common_dir_watch_count(&registry, 1).await;
        // Drain any refresh in flight from the transition itself.
        while next_status_event(&mut status_sub, &ws.id, Duration::from_secs(2))
            .await
            .is_some()
        {}

        // Common-dir ref changes must still trigger for the workspace.
        // Retried for the same delivery-start race as the other real-watcher
        // tests; attempt count sized so the total confirmation budget
        // (attempts x 1500ms) reaches `LIVENESS` — a pure-liveness bound
        // (monorepo#1630, flaked as a fixed budget in monorepo#2012).
        let head_commit = repo.head().unwrap().peel_to_commit().unwrap();
        let attempts = LIVENESS.as_millis() / 1500;
        let mut ev = None;
        for i in 0..attempts {
            repo.branch(&format!("post-replace-{i}"), &head_commit, true)
                .unwrap();
            ev = next_status_event(&mut status_sub, &ws.id, Duration::from_millis(1500)).await;
            if ev.is_some() {
                break;
            }
        }
        assert!(
            ev.is_some(),
            "common-dir ref change must still refresh the workspace after watcher replacement"
        );
    }

    /// A `workspace:updated` with no `archived` key (title rename, status
    /// message, …) must not disturb the watch roots.
    #[tokio::test]
    #[allow(clippy::await_holding_lock)]
    async fn unrelated_workspace_update_leaves_watching_intact() {
        let _serial = crate::events::WATCHER_TEST_SERIAL
            .lock()
            .unwrap_or_else(std::sync::PoisonError::into_inner);
        let (_db, bus, mut sub) = bus_and_sub().await;
        let root = TempDir::new("updated");
        let ws = test_workspace("ws-updated", &root.path);
        let api: Arc<dyn WorkspaceApi> = Arc::new(FakeApi::new(vec![ws.clone()]));

        let _registry = start_registry(&bus, api).await;
        wait_for_root(&_registry, &root.path, true).await;

        let mut ev = lifecycle_event(WORKSPACE_UPDATED, &ws, false);
        ev.data = serde_json::json!({
            "workspaceId": ws.id.as_str(),
            "changes": { "title": "renamed" },
        });
        bus.publish(&ev).await.expect("publish updated");
        wait_for_root(&_registry, &root.path, true).await;

        std::fs::write(root.path.join("after-update.txt"), "hi").expect("write file");
        let ev = next_file_event(&mut sub, &ws.id, LIVENESS).await;
        assert!(
            ev.is_some(),
            "a non-archive workspace:updated must not stop file watching"
        );
    }

    /// Unarchive catch-up: a git workspace whose `.git` changed while archived
    /// must get a `changes:git-status` refresh even though no `.git` event was
    /// observed during the unwatched window.
    #[tokio::test]
    #[allow(clippy::await_holding_lock)]
    async fn unarchive_triggers_git_status_catch_up_refresh() {
        use git2::{Repository, Signature};
        use intent_core::events::CHANGES_GIT_STATUS;

        let _serial = crate::events::WATCHER_TEST_SERIAL
            .lock()
            .unwrap_or_else(std::sync::PoisonError::into_inner);
        let (_db, bus, _file_sub) = bus_and_sub().await;
        let mut status_sub = bus.subscribe(SubscriptionFilter {
            event_types: vec![CHANGES_GIT_STATUS.to_string()],
            ..SubscriptionFilter::default()
        });

        let root = TempDir::new("git-archived");
        let repo = Repository::init(&root.path).unwrap();
        {
            let mut cfg = repo.config().unwrap();
            cfg.set_str("user.name", "Test").unwrap();
            cfg.set_str("user.email", "test@example.com").unwrap();
        }
        repo.set_head("refs/heads/main").unwrap();
        std::fs::write(root.path.join("seed.txt"), "seed\n").unwrap();
        let mut index = repo.index().unwrap();
        index.add_path(std::path::Path::new("seed.txt")).unwrap();
        index.write().unwrap();
        let tree_oid = index.write_tree().unwrap();
        let tree = repo.find_tree(tree_oid).unwrap();
        let sig = Signature::now("Test", "test@example.com").unwrap();
        repo.commit(Some("HEAD"), &sig, &sig, "seed", &tree, &[])
            .unwrap();

        let mut ws = test_workspace("ws-git-archived", &root.path);
        ws.worktree_path = ws.path.clone();
        let api: Arc<dyn WorkspaceApi> = Arc::new(FakeApi::new(vec![ws.clone()]));

        let _registry = start_registry(&bus, api).await;
        wait_for_root(&_registry, &root.path, true).await;

        bus.publish(&archive_event(&ws, true))
            .await
            .expect("publish archived");
        wait_for_root(&_registry, &root.path, false).await;

        // Change git state while unwatched, then drain anything the archive
        // transition itself may still have had in flight.
        repo.set_head("refs/heads/other").unwrap();
        let _ = next_status_event(&mut status_sub, &ws.id, Duration::from_secs(3)).await;

        bus.publish(&archive_event(&ws, false))
            .await
            .expect("publish unarchived");

        let ev = next_status_event(&mut status_sub, &ws.id, LIVENESS).await;
        assert!(
            ev.is_some(),
            "unarchive must trigger a catch-up git-status refresh"
        );
    }

    #[tokio::test]
    #[allow(clippy::await_holding_lock)]
    async fn git_workspace_created_after_start_gains_metadata_watch_and_deletion_stops_it() {
        use git2::{Repository, Signature};
        use intent_core::events::CHANGES_GIT_STATUS;

        let _serial = crate::events::WATCHER_TEST_SERIAL
            .lock()
            .unwrap_or_else(std::sync::PoisonError::into_inner);
        let (_db, bus, _file_sub) = bus_and_sub().await;
        let mut status_sub = bus.subscribe(SubscriptionFilter {
            event_types: vec![CHANGES_GIT_STATUS.to_string()],
            ..SubscriptionFilter::default()
        });
        let fake = Arc::new(FakeApi::new(Vec::new()));
        let api: Arc<dyn WorkspaceApi> = fake.clone();

        let _registry = start_registry(&bus, api).await;
        // Nothing is watched yet: the boot seed is empty.

        // Real repo with a seed commit, registered at runtime. The refresher
        // resolves the worktree via `get_workspace`, so the api must know the
        // workspace before the lifecycle event lands.
        let root = TempDir::new("git-dynamic");
        let repo = Repository::init(&root.path).unwrap();
        {
            let mut cfg = repo.config().unwrap();
            cfg.set_str("user.name", "Test").unwrap();
            cfg.set_str("user.email", "test@example.com").unwrap();
        }
        repo.set_head("refs/heads/main").unwrap();
        std::fs::write(root.path.join("seed.txt"), "seed\n").unwrap();
        let mut index = repo.index().unwrap();
        index.add_path(std::path::Path::new("seed.txt")).unwrap();
        index.write().unwrap();
        let tree_oid = index.write_tree().unwrap();
        let tree = repo.find_tree(tree_oid).unwrap();
        let sig = Signature::now("Test", "test@example.com").unwrap();
        repo.commit(Some("HEAD"), &sig, &sig, "seed", &tree, &[])
            .unwrap();

        let mut ws = test_workspace("ws-git-dynamic", &root.path);
        ws.worktree_path = ws.path.clone();
        fake.workspaces.lock().unwrap().push(ws.clone());
        bus.publish(&lifecycle_event(WORKSPACE_CREATED, &ws, true))
            .await
            .expect("publish created");
        bus.publish(&setup_completed_event(&ws, true))
            .await
            .expect("publish setup completed");
        wait_for_root(&_registry, &root.path, true).await;

        // External `git checkout`-style HEAD rewrite → debounced refresh.
        repo.set_head("refs/heads/other").unwrap();
        let ev = next_status_event(&mut status_sub, &ws.id, LIVENESS).await;
        assert!(
            ev.is_some(),
            "git workspace registered after start must refresh git status on .git metadata changes"
        );

        // Deregister via `workspace:deleted`: the metadata watch stops.
        bus.publish(&lifecycle_event(WORKSPACE_DELETED, &ws, false))
            .await
            .expect("publish deleted");
        wait_for_root(&_registry, &root.path, false).await;

        repo.set_head("refs/heads/main").unwrap();
        let ev = next_status_event(&mut status_sub, &ws.id, Duration::from_secs(3)).await;
        assert!(
            ev.is_none(),
            "deregistered workspace must stop refreshing git status, got {ev:?}"
        );
    }

    /// Await the next `changes:git-status` event for `ws_id` within `overall`.
    async fn next_status_event(
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
}
