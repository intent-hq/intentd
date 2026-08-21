//! Specialist directory watcher → `specialists:changed` events.
//!
//! Watches the writable specialist tiers — user (`~/.intent/specialists/`) and
//! project (`<workspace>/.intent/specialists/` per workspace) — and emits
//! `specialists:changed` events when `<id>.md` files are created, modified, or
//! deleted — or when a tier directory itself appears or disappears (#612).
//! User-tier changes affect all workspaces; project-tier changes are
//! scoped to their workspace. Debounce is 500ms per workspace to coalesce rapid
//! edits, and an event is emitted only when the resolved specialist set
//! actually changed (fingerprint check, analogous to `check_skills_changed`).
//! The bundled/embedded tiers are static at runtime and are not watched.
//! Workspaces can be registered/deregistered at runtime (#611) via
//! [`SpecialistsWatcher::add_workspace`] / [`SpecialistsWatcher::remove_workspace`].

use std::collections::hash_map::DefaultHasher;
use std::collections::HashMap;
use std::hash::{Hash, Hasher};
use std::path::{Path, PathBuf};
use std::sync::{Arc, Mutex};
use std::time::Duration;

use intent_core::{events::SPECIALISTS_CHANGED, now_iso, ActorType, EventActor, WorkspaceId};
use intent_store::NewEvent;
use serde_json::json;
use tokio::sync::mpsc;
use tokio::task::JoinHandle;

use super::bus::EventBus;
use super::root_watch::{watch_root, RootWatch};
use super::shared_watch::{watch_tiers, SharedWatchHub, TierWatch};
use crate::specialists::SpecialistsService;

const DEBOUNCE: Duration = Duration::from_millis(500);

/// Holds watchers for all specialist directories (user-tier + project-tier).
/// Dropping this tears down all watchers.
///
/// The user tier keeps a [`RootWatch`] — it is shared once per daemon, so it
/// does not scale with the workspace count. The project tier per workspace no
/// longer owns a stream at all: it rides the shared workspace-root stream via
/// [`watch_tiers`].
pub(crate) struct SpecialistsWatcher {
    hub: Arc<SharedWatchHub>,
    _user_watchers: Vec<RootWatch>,
    workspace_watchers: Mutex<HashMap<WorkspaceId, TierWatch>>,
    raw_tx: mpsc::UnboundedSender<SpecialistsMsg>,
    task: JoinHandle<()>,
}

impl Drop for SpecialistsWatcher {
    fn drop(&mut self) {
        self.task.abort();
    }
}

impl SpecialistsWatcher {
    /// Start watching specialist directories for all workspaces.
    /// `workspaces` is a list of (`workspace_id`, `workspace_path`) pairs.
    pub(super) fn start(
        hub: &Arc<SharedWatchHub>,
        bus: EventBus,
        workspaces: Vec<(WorkspaceId, PathBuf)>,
    ) -> Self {
        Self::start_with_user_dir(hub, bus, workspaces, default_user_dir())
    }

    /// Like [`Self::start`] but with an explicit user-tier root (tests inject a
    /// temp dir for hermetic coverage; production passes the default).
    fn start_with_user_dir(
        hub: &Arc<SharedWatchHub>,
        bus: EventBus,
        workspaces: Vec<(WorkspaceId, PathBuf)>,
        user_dir: Option<PathBuf>,
    ) -> Self {
        let (raw_tx, raw_rx) = mpsc::unbounded_channel::<SpecialistsMsg>();

        // Start the user-tier watcher (affects all workspaces)
        let mut user_watchers = Vec::new();
        if let Some(root) = &user_dir {
            user_watchers.push(watch_directory(root.clone(), None, raw_tx.clone()));
        }

        // Start project-tier watchers (per-workspace)
        let mut workspace_watchers: HashMap<WorkspaceId, TierWatch> = HashMap::new();
        for (ws_id, ws_path) in &workspaces {
            workspace_watchers.insert(
                ws_id.clone(),
                start_project_watch(hub, ws_id, ws_path, &raw_tx),
            );
        }

        let task = tokio::spawn(debounce_loop(bus, workspaces, user_dir, raw_rx));

        Self {
            hub: Arc::clone(hub),
            _user_watchers: user_watchers,
            workspace_watchers: Mutex::new(workspace_watchers),
            raw_tx,
            task,
        }
    }

    /// Register a workspace at runtime (#611). The debounce loop learns the
    /// path first (and primes the fingerprint so pre-existing specialists do
    /// not emit a spurious event), then the project tier is watched.
    /// Re-registering replaces the watch.
    pub(crate) fn add_workspace(&self, workspace_id: WorkspaceId, workspace_path: &Path) {
        let _ = self.raw_tx.send(SpecialistsMsg::Add(
            workspace_id.clone(),
            workspace_path.to_path_buf(),
        ));
        let watch = start_project_watch(&self.hub, &workspace_id, workspace_path, &self.raw_tx);
        if let Ok(mut map) = self.workspace_watchers.lock() {
            map.insert(workspace_id, watch);
        }
    }

    /// Await the user-tier root watch actually being established. Its
    /// registration is deferred off the caller's thread (monorepo#1572), so
    /// tests must wait for it before mutating that directory. The project tier
    /// rides the shared stream and needs no separate sync point — subscribing is
    /// synchronous bookkeeping.
    #[cfg(test)]
    async fn wait_established(&self, timeout: Duration) {
        for watch in &self._user_watchers {
            watch.wait_established(timeout).await;
        }
    }

    /// Deregister a workspace at runtime (#611): tear down its project-tier
    /// watch and drop any pending flush so it stops emitting.
    pub(crate) fn remove_workspace(&self, workspace_id: &WorkspaceId) {
        if let Ok(mut map) = self.workspace_watchers.lock() {
            map.remove(workspace_id);
        }
        let _ = self
            .raw_tx
            .send(SpecialistsMsg::Remove(workspace_id.clone()));
    }

    /// Suspend a workspace (archive): tear down its project-tier watch but
    /// KEEP the fingerprint, so the [`Self::resume_workspace`] catch-up can
    /// tell whether the set changed while the workspace was unwatched.
    pub(crate) fn pause_workspace(&self, workspace_id: &WorkspaceId) {
        if let Ok(mut map) = self.workspace_watchers.lock() {
            map.remove(workspace_id);
        }
        let _ = self
            .raw_tx
            .send(SpecialistsMsg::Pause(workspace_id.clone()));
    }

    /// Resume a suspended workspace (unarchive): re-watch the project tier and
    /// schedule one catch-up flush against the retained fingerprint, so edits
    /// made while suspended emit exactly one `specialists:changed` and an
    /// untouched tree emits nothing. That silence holds only when the pause
    /// happened in this process: a workspace archived before daemon start is
    /// never seeded (boot lists unarchived workspaces only), so its resume has
    /// no baseline and emits one benign event.
    pub(crate) fn resume_workspace(&self, workspace_id: WorkspaceId, workspace_path: &Path) {
        let _ = self.raw_tx.send(SpecialistsMsg::Resume(
            workspace_id.clone(),
            workspace_path.to_path_buf(),
        ));
        let watch = start_project_watch(&self.hub, &workspace_id, workspace_path, &self.raw_tx);
        if let Ok(mut map) = self.workspace_watchers.lock() {
            map.insert(workspace_id, watch);
        }
    }
}

/// Watch the project-tier specialists root of one workspace over the shared
/// workspace-root stream — one subscription, no stream of its own (previously a
/// [`RootWatch`], its own stream even when the tier was missing).
fn start_project_watch(
    hub: &Arc<SharedWatchHub>,
    workspace_id: &WorkspaceId,
    workspace_path: &Path,
    raw_tx: &mpsc::UnboundedSender<SpecialistsMsg>,
) -> TierWatch {
    let ws_id = workspace_id.clone();
    let tx = raw_tx.clone();
    watch_tiers(hub, workspace_path, PROJECT_TIERS, is_md, move || {
        let _ = tx.send(SpecialistsMsg::Change(Some(ws_id.clone())));
    })
}

/// Message into the debounce loop: a raw filesystem change, or a runtime
/// (de)registration of a workspace (#611).
#[derive(Debug, Clone)]
enum SpecialistsMsg {
    /// Raw change from a root watch; `None` = user tier (all workspaces).
    Change(Option<WorkspaceId>),
    /// Workspace registered after start.
    Add(WorkspaceId, PathBuf),
    /// Workspace deregistered; its pending flush is dropped.
    Remove(WorkspaceId),
    /// Workspace suspended (archive): stops emitting like `Remove`, but the
    /// fingerprint is retained for the later `Resume` catch-up.
    Pause(WorkspaceId),
    /// Workspace resumed (unarchive): re-registers the path WITHOUT re-priming
    /// the fingerprint, plus a flush so changes missed while suspended emit.
    Resume(WorkspaceId, PathBuf),
}

/// Watch a single root, forwarding `*.md` and directory-level events.
/// Missing roots are handled by [`watch_root`]: a non-recursive watch on the
/// nearest existing ancestor is promoted to a recursive watch on the root
/// once it appears.
fn watch_directory(
    root: PathBuf,
    workspace_id: Option<WorkspaceId>,
    tx: mpsc::UnboundedSender<SpecialistsMsg>,
) -> RootWatch {
    watch_root(root, is_md, move || {
        let _ = tx.send(SpecialistsMsg::Change(workspace_id.clone()));
    })
}

fn is_md(path: &Path) -> bool {
    path.extension().and_then(|e| e.to_str()) == Some("md")
}

/// Default user-tier specialists directory (`~/.intent/specialists/`).
fn default_user_dir() -> Option<PathBuf> {
    home_dir().map(|h| h.join(".intent").join("specialists"))
}

/// The project-tier specialists directory, relative to the workspace root.
const PROJECT_TIERS: &[&str] = &[".intent/specialists"];

/// The project-tier specialists directory for a workspace.
#[cfg(test)]
fn project_dir(workspace_path: &Path) -> PathBuf {
    workspace_path.join(".intent").join("specialists")
}

fn home_dir() -> Option<PathBuf> {
    std::env::var_os("HOME")
        .or_else(|| std::env::var_os("USERPROFILE"))
        .map(PathBuf::from)
        .filter(|p| !p.as_os_str().is_empty())
}

/// Fingerprint the resolved specialist set for a workspace: a hash of the
/// serialized `specialist.list` view (ids + tier-resolved content), so an
/// event is emitted only when the resolved set actually changed (analogous to
/// `check_skills_changed`).
fn specialists_fingerprint(user_dir: &Option<PathBuf>, workspace_path: &Path) -> u64 {
    let svc = SpecialistsService::new(user_dir.clone(), None);
    let list = svc
        .list(Some(workspace_path))
        .unwrap_or(serde_json::Value::Null);
    let mut hasher = DefaultHasher::new();
    list.to_string().hash(&mut hasher);
    hasher.finish()
}

/// Debounce loop that coalesces rapid specialist file changes per workspace.
/// `Add`/`Remove` messages keep the workspace map and fingerprints in step
/// with runtime (de)registrations (#611).
async fn debounce_loop(
    bus: EventBus,
    workspaces: Vec<(WorkspaceId, PathBuf)>,
    user_dir: Option<PathBuf>,
    mut raw_rx: mpsc::UnboundedReceiver<SpecialistsMsg>,
) {
    let mut pending: HashMap<WorkspaceId, tokio::time::Instant> = HashMap::new();
    let mut workspace_paths: HashMap<WorkspaceId, PathBuf> = workspaces.into_iter().collect();

    // Prime the per-workspace fingerprints so the first flush compares against
    // the set as it stood at watcher start (no spurious first event).
    let mut fingerprints: HashMap<WorkspaceId, u64> = workspace_paths
        .iter()
        .map(|(ws_id, path)| (ws_id.clone(), specialists_fingerprint(&user_dir, path)))
        .collect();

    loop {
        let next_deadline = pending.values().copied().min();

        tokio::select! {
            maybe = raw_rx.recv() => match maybe {
                Some(SpecialistsMsg::Change(workspace_id)) => {
                    let deadline = tokio::time::Instant::now() + DEBOUNCE;
                    match workspace_id {
                        // User-tier change: affects all workspaces
                        None => {
                            for ws_id in workspace_paths.keys() {
                                pending.insert(ws_id.clone(), deadline);
                            }
                        }
                        // Project-tier change: affects specific workspace
                        Some(ws_id) => {
                            pending.insert(ws_id, deadline);
                        }
                    }
                }
                Some(SpecialistsMsg::Add(ws_id, path)) => {
                    // Prime the fingerprint like the start-time priming above.
                    fingerprints.insert(ws_id.clone(), specialists_fingerprint(&user_dir, &path));
                    workspace_paths.insert(ws_id, path);
                }
                Some(SpecialistsMsg::Remove(ws_id)) => {
                    workspace_paths.remove(&ws_id);
                    fingerprints.remove(&ws_id);
                    pending.remove(&ws_id);
                }
                Some(SpecialistsMsg::Pause(ws_id)) => {
                    // Path drop stops emission (user-tier fan-out and flushes
                    // both key on `workspace_paths`); the fingerprint stays.
                    workspace_paths.remove(&ws_id);
                    pending.remove(&ws_id);
                }
                Some(SpecialistsMsg::Resume(ws_id, path)) => {
                    workspace_paths.insert(ws_id.clone(), path);
                    // Catch-up: flush after the normal debounce so the
                    // re-registered watch's own events coalesce into it.
                    pending.insert(ws_id, tokio::time::Instant::now() + DEBOUNCE);
                }
                None => {
                    // All senders dropped: flush and exit
                    flush_all(&bus, &workspace_paths, &user_dir, &mut fingerprints, &mut pending).await;
                    return;
                }
            },
            () = sleep_until(next_deadline), if next_deadline.is_some() => {
                flush_due(&bus, &workspace_paths, &user_dir, &mut fingerprints, &mut pending).await;
            }
        }
    }
}

async fn flush_due(
    bus: &EventBus,
    workspace_paths: &HashMap<WorkspaceId, PathBuf>,
    user_dir: &Option<PathBuf>,
    fingerprints: &mut HashMap<WorkspaceId, u64>,
    pending: &mut HashMap<WorkspaceId, tokio::time::Instant>,
) {
    let now = tokio::time::Instant::now();
    let due: Vec<WorkspaceId> = pending
        .iter()
        .filter(|(_, &deadline)| deadline <= now)
        .map(|(ws_id, _)| ws_id.clone())
        .collect();

    for ws_id in due {
        pending.remove(&ws_id);
        if let Some(path) = workspace_paths.get(&ws_id) {
            emit_specialists_changed(bus, &ws_id, path, user_dir, fingerprints).await;
        }
    }
}

async fn flush_all(
    bus: &EventBus,
    workspace_paths: &HashMap<WorkspaceId, PathBuf>,
    user_dir: &Option<PathBuf>,
    fingerprints: &mut HashMap<WorkspaceId, u64>,
    pending: &mut HashMap<WorkspaceId, tokio::time::Instant>,
) {
    let due: Vec<WorkspaceId> = pending.drain().map(|(ws_id, _)| ws_id).collect();
    for ws_id in due {
        if let Some(path) = workspace_paths.get(&ws_id) {
            emit_specialists_changed(bus, &ws_id, path, user_dir, fingerprints).await;
        }
    }
}

async fn emit_specialists_changed(
    bus: &EventBus,
    workspace_id: &WorkspaceId,
    workspace_path: &Path,
    user_dir: &Option<PathBuf>,
    fingerprints: &mut HashMap<WorkspaceId, u64>,
) {
    // Re-resolve the set to check if it actually changed
    let fingerprint = specialists_fingerprint(user_dir, workspace_path);
    let changed = fingerprints.get(workspace_id) != Some(&fingerprint);
    fingerprints.insert(workspace_id.clone(), fingerprint);

    if changed {
        let event = NewEvent {
            workspace_id: workspace_id.clone(),
            timestamp: now_iso(),
            event_type: SPECIALISTS_CHANGED.to_string(),
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
            data: json!({ "workspaceId": workspace_id.as_str() }),
        };
        let _ = bus.publish(&event).await;
    }
}

async fn sleep_until(deadline: Option<tokio::time::Instant>) {
    if let Some(d) = deadline {
        tokio::time::sleep_until(d).await;
    } else {
        std::future::pending::<()>().await;
    }
}

#[cfg(test)]
mod tests {
    use std::time::Duration;

    use intent_core::Event;
    use intent_store::Store;
    use tokio::time::{timeout, Instant};

    use super::super::filter::SubscriptionFilter;
    use super::*;
    use crate::events::LIVENESS;

    /// Self-cleaning temp directory (workspace root / user specialists tier).
    struct TempDir {
        path: PathBuf,
    }

    impl TempDir {
        fn new(tag: &str) -> Self {
            let path = std::env::temp_dir()
                .join(format!("intentd-spec-watch-{tag}-{}", uuid::Uuid::new_v4()));
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
            let path = std::env::temp_dir()
                .join(format!("intentd-spec-watch-{}.db", uuid::Uuid::new_v4()));
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

    async fn bus_and_sub() -> (TempDb, EventBus, super::super::bus::Subscription) {
        let db = TempDb::new();
        let store = Store::open(&db.path).await.expect("open store");
        let bus = EventBus::new(store);
        let sub = bus.subscribe(SubscriptionFilter::default());
        (db, bus, sub)
    }

    /// Drain `specialists:changed` events from the subscription. Waits up to
    /// the full `deadline` for the FIRST matching event; once at least one has
    /// been collected, applies the `quiet` coalescing window (stop after
    /// `quiet` elapses with no new batch, or when `deadline` passes).
    async fn drain_specialists_events(
        sub: &mut super::super::bus::Subscription,
        quiet: Duration,
        deadline: Duration,
    ) -> Vec<Event> {
        let mut events: Vec<Event> = Vec::new();
        let end = Instant::now() + deadline;
        loop {
            let remaining = end.saturating_duration_since(Instant::now());
            if remaining.is_zero() {
                break;
            }
            let wait = if events.is_empty() {
                remaining
            } else {
                quiet.min(remaining)
            };
            match timeout(wait, sub.recv()).await {
                Ok(Some(batch)) => {
                    for ev in batch {
                        if ev.event_type == SPECIALISTS_CHANGED {
                            events.push(ev);
                        }
                    }
                }
                _ => break,
            }
        }
        events
    }

    fn specialist_md(name: &str, body: &str) -> String {
        format!("---\nname: \"{name}\"\ndescription: \"d\"\n---\n\n{body}")
    }

    #[tokio::test]
    #[allow(clippy::await_holding_lock)]
    async fn tier_directory_deletion_emits_event() {
        let _serial = crate::events::WATCHER_TEST_SERIAL
            .lock()
            .unwrap_or_else(std::sync::PoisonError::into_inner);
        let (_db, bus, mut sub) = bus_and_sub().await;
        let user = TempDir::new("rmdir-user");
        let ws = TempDir::new("rmdir-ws");
        let ws_id = WorkspaceId::from("ws-rmdir");
        let proj = project_dir(&ws.path);
        std::fs::create_dir_all(&proj).expect("mk project tier");
        // Seed BEFORE the watcher starts so the primed fingerprint includes
        // this specialist.
        std::fs::write(proj.join("doomed.md"), specialist_md("Doomed", "body"))
            .expect("seed specialist");

        let _watcher = SpecialistsWatcher::start_with_user_dir(
            &SharedWatchHub::new(),
            bus.clone(),
            vec![(ws_id.clone(), ws.path.clone())],
            Some(user.path.clone()),
        );
        _watcher.wait_established(LIVENESS).await;
        tokio::time::sleep(Duration::from_millis(250)).await;

        // `rm -rf` of the whole tier directory: possibly only directory-level
        // events surface, which the filter must still forward (#612).
        std::fs::remove_dir_all(&proj).expect("remove tier dir");

        let events = drain_specialists_events(&mut sub, Duration::from_secs(2), LIVENESS).await;
        assert_eq!(
            events.len(),
            1,
            "tier-directory deletion must emit one event, got {events:?}"
        );
        assert_eq!(events[0].workspace_id, ws_id);
    }

    #[tokio::test]
    #[allow(clippy::await_holding_lock)]
    async fn missing_root_promotes_on_creation_and_detects_changes() {
        let _serial = crate::events::WATCHER_TEST_SERIAL
            .lock()
            .unwrap_or_else(std::sync::PoisonError::into_inner);
        let (_db, bus, mut sub) = bus_and_sub().await;
        let user = TempDir::new("late-user");
        let ws = TempDir::new("late-ws");
        let ws_id = WorkspaceId::from("ws-late");
        let proj = project_dir(&ws.path);
        // The project tier does NOT exist when the watcher starts.

        let _watcher = SpecialistsWatcher::start_with_user_dir(
            &SharedWatchHub::new(),
            bus.clone(),
            vec![(ws_id.clone(), ws.path.clone())],
            Some(user.path.clone()),
        );
        // Warm-up widened 250ms -> 750ms: the missing-root promotion path must
        // establish its parent watch before the tier dir is created below, and
        // under nextest's oversubscribed parallelism a 250ms warm-up can lose
        // that race, so the creation is never observed and the drain returns
        // empty. The drains here wait up to `LIVENESS` for the first event
        // (monorepo#1630) with a generous quiet window, so only the headroom
        // before the "no event" verdict widens — behavior under test is
        // unchanged.
        _watcher.wait_established(LIVENESS).await;
        tokio::time::sleep(Duration::from_millis(750)).await;

        std::fs::create_dir_all(&proj).expect("create tier dir");
        std::fs::write(proj.join("late.md"), specialist_md("Late", "body"))
            .expect("write specialist");

        let events = drain_specialists_events(&mut sub, Duration::from_secs(8), LIVENESS).await;
        assert!(
            !events.is_empty(),
            "root created after start must emit, got {events:?}"
        );
        assert!(events.iter().all(|e| e.workspace_id == ws_id));

        // Subsequent changes under the promoted root are detected.
        std::fs::write(proj.join("late.md"), specialist_md("Late", "new body"))
            .expect("modify specialist");
        let events = drain_specialists_events(&mut sub, Duration::from_secs(6), LIVENESS).await;
        assert!(
            !events.is_empty(),
            "changes under the promoted root must emit, got {events:?}"
        );
    }

    #[tokio::test]
    #[allow(clippy::await_holding_lock)]
    async fn project_tier_burst_debounces_to_one_event() {
        let _serial = crate::events::WATCHER_TEST_SERIAL
            .lock()
            .unwrap_or_else(std::sync::PoisonError::into_inner);
        let (_db, bus, mut sub) = bus_and_sub().await;
        let user = TempDir::new("debounce-user");
        let ws = TempDir::new("debounce-ws");
        let ws_id = WorkspaceId::from("ws-debounce");
        let proj = project_dir(&ws.path);
        std::fs::create_dir_all(&proj).expect("mk project tier");

        let _watcher = SpecialistsWatcher::start_with_user_dir(
            &SharedWatchHub::new(),
            bus.clone(),
            vec![(ws_id.clone(), ws.path.clone())],
            Some(user.path.clone()),
        );
        _watcher.wait_established(LIVENESS).await;
        // Let the OS watch establish before mutating (FSEvents/inotify warm-up).
        tokio::time::sleep(Duration::from_millis(250)).await;

        // Rapid burst of specialist file changes within one debounce window.
        for i in 0..3 {
            std::fs::write(
                proj.join(format!("custom{i}.md")),
                specialist_md(&format!("Custom {i}"), "body"),
            )
            .expect("write specialist");
            tokio::time::sleep(Duration::from_millis(50)).await;
        }

        let events = drain_specialists_events(&mut sub, Duration::from_secs(2), LIVENESS).await;
        assert_eq!(
            events.len(),
            1,
            "burst must coalesce to one event, got {events:?}"
        );
        assert_eq!(events[0].workspace_id, ws_id);
        assert_eq!(events[0].data["workspaceId"], ws_id.as_str());
        assert_eq!(events[0].actor.actor_type, ActorType::System);
    }

    #[tokio::test]
    #[allow(clippy::await_holding_lock)]
    async fn user_tier_change_fans_out_to_all_workspaces() {
        let _serial = crate::events::WATCHER_TEST_SERIAL
            .lock()
            .unwrap_or_else(std::sync::PoisonError::into_inner);
        let (_db, bus, mut sub) = bus_and_sub().await;
        let user = TempDir::new("fanout-user");
        let ws1 = TempDir::new("fanout-ws1");
        let ws2 = TempDir::new("fanout-ws2");
        let ws1_id = WorkspaceId::from("ws-fanout-1");
        let ws2_id = WorkspaceId::from("ws-fanout-2");

        let _watcher = SpecialistsWatcher::start_with_user_dir(
            &SharedWatchHub::new(),
            bus.clone(),
            vec![
                (ws1_id.clone(), ws1.path.clone()),
                (ws2_id.clone(), ws2.path.clone()),
            ],
            Some(user.path.clone()),
        );
        _watcher.wait_established(LIVENESS).await;
        tokio::time::sleep(Duration::from_millis(250)).await;

        std::fs::write(
            user.path.join("shared.md"),
            specialist_md("Shared", "user-tier body"),
        )
        .expect("write user specialist");

        let events = drain_specialists_events(&mut sub, Duration::from_secs(2), LIVENESS).await;
        let mut ws_ids: Vec<&str> = events.iter().map(|e| e.workspace_id.as_str()).collect();
        ws_ids.sort_unstable();
        assert_eq!(
            ws_ids,
            vec![ws1_id.as_str(), ws2_id.as_str()],
            "user-tier change must emit exactly one event per workspace, got {events:?}"
        );
    }

    #[tokio::test]
    #[allow(clippy::await_holding_lock)]
    async fn unchanged_set_emits_nothing() {
        let _serial = crate::events::WATCHER_TEST_SERIAL
            .lock()
            .unwrap_or_else(std::sync::PoisonError::into_inner);
        let (_db, bus, mut sub) = bus_and_sub().await;
        let user = TempDir::new("noop-user");
        let ws = TempDir::new("noop-ws");
        let ws_id = WorkspaceId::from("ws-noop");
        let proj = project_dir(&ws.path);
        std::fs::create_dir_all(&proj).expect("mk project tier");
        let file = proj.join("steady.md");
        let content = specialist_md("Steady", "same body");
        // Pre-seed BEFORE the watcher starts so the primed fingerprint already
        // includes this specialist.
        std::fs::write(&file, &content).expect("seed specialist");

        let _watcher = SpecialistsWatcher::start_with_user_dir(
            &SharedWatchHub::new(),
            bus.clone(),
            vec![(ws_id.clone(), ws.path.clone())],
            Some(user.path.clone()),
        );
        _watcher.wait_established(LIVENESS).await;
        tokio::time::sleep(Duration::from_millis(250)).await;

        // Rewrite the identical bytes: the file event fires but the resolved
        // set is unchanged, so the fingerprint check must suppress the event.
        std::fs::write(&file, &content).expect("rewrite identical content");

        let events = drain_specialists_events(
            &mut sub,
            Duration::from_millis(1500),
            Duration::from_secs(3),
        )
        .await;
        assert!(
            events.is_empty(),
            "unchanged specialist set must not emit, got {events:?}"
        );

        // A real content change afterwards still emits (the watcher is live).
        std::fs::write(&file, specialist_md("Steady", "different body"))
            .expect("write changed content");
        let events = drain_specialists_events(&mut sub, Duration::from_secs(2), LIVENESS).await;
        assert_eq!(
            events.len(),
            1,
            "real change after a no-op must emit exactly one event, got {events:?}"
        );
        assert_eq!(events[0].workspace_id, ws_id);
    }

    #[tokio::test]
    #[allow(clippy::await_holding_lock)]
    async fn workspace_added_after_start_gains_watching_and_removal_stops_it() {
        let _serial = crate::events::WATCHER_TEST_SERIAL
            .lock()
            .unwrap_or_else(std::sync::PoisonError::into_inner);
        let (_db, bus, mut sub) = bus_and_sub().await;
        let user = TempDir::new("dyn-user");
        let ws = TempDir::new("dyn-ws");
        let ws_id = WorkspaceId::from("ws-dyn");
        let proj = project_dir(&ws.path);
        std::fs::create_dir_all(&proj).expect("mk project tier");
        // Seed BEFORE registration: the primed fingerprint must include this
        // specialist so registration itself does not emit.
        std::fs::write(proj.join("seed.md"), specialist_md("Seed", "body"))
            .expect("seed specialist");

        // Start with NO workspaces; register at runtime (#611).
        let watcher = SpecialistsWatcher::start_with_user_dir(
            &SharedWatchHub::new(),
            bus.clone(),
            vec![],
            Some(user.path.clone()),
        );
        watcher.wait_established(LIVENESS).await;
        tokio::time::sleep(Duration::from_millis(250)).await;

        watcher.add_workspace(ws_id.clone(), &ws.path.clone());
        watcher.wait_established(LIVENESS).await;
        tokio::time::sleep(Duration::from_millis(250)).await;

        // Registration alone must not emit (fingerprint primed at add time).
        let events = drain_specialists_events(
            &mut sub,
            Duration::from_millis(1500),
            Duration::from_secs(3),
        )
        .await;
        assert!(
            events.is_empty(),
            "runtime registration must not emit for the pre-existing set, got {events:?}"
        );

        // A project-tier change after registration emits for the new workspace.
        std::fs::write(proj.join("added.md"), specialist_md("Added", "body"))
            .expect("write specialist");
        let events = drain_specialists_events(&mut sub, Duration::from_secs(2), LIVENESS).await;
        assert_eq!(
            events.len(),
            1,
            "change after runtime registration must emit one event, got {events:?}"
        );
        assert_eq!(events[0].workspace_id, ws_id);

        // Deregister: further project-tier changes no longer emit.
        watcher.remove_workspace(&ws_id);
        tokio::time::sleep(Duration::from_millis(250)).await;

        std::fs::write(proj.join("late.md"), specialist_md("Late", "body"))
            .expect("write specialist after removal");
        let events = drain_specialists_events(
            &mut sub,
            Duration::from_millis(1500),
            Duration::from_secs(3),
        )
        .await;
        assert!(
            events.is_empty(),
            "deregistered workspace must stop emitting, got {events:?}"
        );
    }

    #[tokio::test]
    #[allow(clippy::await_holding_lock)]
    async fn pause_retains_fingerprint_so_resume_only_emits_on_real_change() {
        let _serial = crate::events::WATCHER_TEST_SERIAL
            .lock()
            .unwrap_or_else(std::sync::PoisonError::into_inner);
        let (_db, bus, mut sub) = bus_and_sub().await;
        let user = TempDir::new("pause-user");
        let ws = TempDir::new("pause-ws");
        let ws_id = WorkspaceId::from("ws-pause");
        let proj = project_dir(&ws.path);
        std::fs::create_dir_all(&proj).expect("mk project tier");
        let file = proj.join("steady.md");
        std::fs::write(&file, specialist_md("Steady", "body")).expect("seed specialist");

        let watcher = SpecialistsWatcher::start_with_user_dir(
            &SharedWatchHub::new(),
            bus.clone(),
            vec![(ws_id.clone(), ws.path.clone())],
            Some(user.path.clone()),
        );
        tokio::time::sleep(Duration::from_millis(250)).await;

        // Suspend, change nothing, resume: the retained fingerprint still
        // matches, so the catch-up flush must stay silent.
        watcher.pause_workspace(&ws_id);
        tokio::time::sleep(Duration::from_millis(250)).await;
        watcher.resume_workspace(ws_id.clone(), &ws.path.clone());

        let events = drain_specialists_events(
            &mut sub,
            Duration::from_millis(1500),
            Duration::from_secs(3),
        )
        .await;
        assert!(
            events.is_empty(),
            "resume with an unchanged set must not emit, got {events:?}"
        );

        // Suspend, edit while suspended, resume: the retained fingerprint is
        // stale, so the catch-up flush emits exactly once.
        watcher.pause_workspace(&ws_id);
        tokio::time::sleep(Duration::from_millis(250)).await;
        std::fs::write(&file, specialist_md("Steady", "edited while suspended"))
            .expect("edit while suspended");
        tokio::time::sleep(Duration::from_millis(250)).await;

        let events = drain_specialists_events(
            &mut sub,
            Duration::from_millis(1500),
            Duration::from_secs(3),
        )
        .await;
        assert!(
            events.is_empty(),
            "a suspended workspace must not emit for its own edits, got {events:?}"
        );

        watcher.resume_workspace(ws_id.clone(), &ws.path.clone());
        let events = drain_specialists_events(&mut sub, Duration::from_secs(2), LIVENESS).await;
        assert_eq!(
            events.len(),
            1,
            "resume after a suspended-window edit must emit exactly one event, got {events:?}"
        );
        assert_eq!(events[0].workspace_id, ws_id);
    }
}
