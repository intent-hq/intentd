//! Skills directory watcher → `skills:changed` events.
//!
//! Watches the 7-tier skills scan roots (4 user-tier + 3 project-tier per workspace)
//! and emits `skills:changed` events when SKILL.md files are created, modified, or
//! deleted — or when a tier directory itself appears or disappears (#612).
//! User-tier changes affect all workspaces; project-tier changes are scoped
//! to their workspace. Debounce is 500ms per workspace to coalesce rapid edits.
//! Workspaces can be registered/deregistered at runtime (#611) via
//! [`SkillsWatcher::add_workspace`] / [`SkillsWatcher::remove_workspace`].

use std::collections::hash_map::DefaultHasher;
use std::collections::HashMap;
use std::hash::{Hash, Hasher};
use std::path::{Path, PathBuf};
use std::sync::{Arc, Mutex};
use std::time::Duration;

use intent_core::{events::SKILLS_CHANGED, now_iso, ActorType, EventActor, WorkspaceId};
use intent_store::NewEvent;
use serde_json::json;
use tokio::sync::mpsc;
use tokio::task::JoinHandle;

use super::bus::EventBus;
use super::root_watch::{watch_root, RootWatch};
use super::shared_watch::{watch_tiers, SharedWatchHub, TierWatch};

const DEBOUNCE: Duration = Duration::from_millis(500);

/// Holds watchers for all skills directories (user-tier + project-tier).
/// Dropping this tears down all watchers.
///
/// The four user tiers keep a [`RootWatch`] each — they are shared once per
/// daemon, so they do not scale with the workspace count. The three project
/// tiers per workspace no longer own streams at all: they ride the shared
/// workspace-root stream via [`watch_tiers`].
pub(crate) struct SkillsWatcher {
    hub: Arc<SharedWatchHub>,
    _user_watchers: Vec<RootWatch>,
    workspace_watchers: Mutex<HashMap<WorkspaceId, TierWatch>>,
    raw_tx: mpsc::UnboundedSender<SkillsMsg>,
    task: JoinHandle<()>,
}

impl Drop for SkillsWatcher {
    fn drop(&mut self) {
        self.task.abort();
    }
}

impl SkillsWatcher {
    /// Start watching skills directories for all workspaces.
    /// `workspaces` is a list of (`workspace_id`, `workspace_path`) pairs.
    pub(super) fn start(
        hub: &Arc<SharedWatchHub>,
        bus: EventBus,
        workspaces: Vec<(WorkspaceId, PathBuf)>,
    ) -> Self {
        let (raw_tx, raw_rx) = mpsc::unbounded_channel::<SkillsMsg>();

        // Start user-tier watchers (affect all workspaces)
        let mut user_watchers = Vec::new();
        let user_roots = get_user_skill_roots();
        for root in user_roots {
            user_watchers.push(watch_directory(root, None, raw_tx.clone()));
        }

        // Start project-tier watchers (per-workspace)
        let mut workspace_watchers: HashMap<WorkspaceId, TierWatch> = HashMap::new();
        for (ws_id, ws_path) in &workspaces {
            workspace_watchers.insert(
                ws_id.clone(),
                start_project_watch(hub, ws_id, ws_path, &raw_tx),
            );
        }

        let task = tokio::spawn(debounce_loop(bus, workspaces, raw_rx));

        Self {
            hub: Arc::clone(hub),
            _user_watchers: user_watchers,
            workspace_watchers: Mutex::new(workspace_watchers),
            raw_tx,
            task,
        }
    }

    /// Register a workspace at runtime (#611). The debounce loop learns the
    /// path first so events from the new watches (including the promotion
    /// catch-up for tier roots created later) are attributable, then the
    /// project-tier roots are watched. Re-registering replaces the watches.
    pub(crate) fn add_workspace(&self, workspace_id: WorkspaceId, workspace_path: &Path) {
        let _ = self.raw_tx.send(SkillsMsg::Add(
            workspace_id.clone(),
            workspace_path.to_path_buf(),
        ));
        let watch = start_project_watch(&self.hub, &workspace_id, workspace_path, &self.raw_tx);
        if let Ok(mut map) = self.workspace_watchers.lock() {
            map.insert(workspace_id, watch);
        }
    }

    /// Await every user-tier root watch actually being established. Their
    /// registration is deferred off the caller's thread (monorepo#1572), so
    /// tests must wait for it before mutating those directories. Project tiers
    /// ride the shared stream and need no separate sync point — subscribing is
    /// synchronous bookkeeping.
    #[cfg(test)]
    async fn wait_established(&self, timeout: Duration) {
        for watch in &self._user_watchers {
            watch.wait_established(timeout).await;
        }
    }

    /// Deregister a workspace at runtime (#611): tear down its project-tier
    /// watches and drop any pending flush so it stops emitting.
    pub(crate) fn remove_workspace(&self, workspace_id: &WorkspaceId) {
        if let Ok(mut map) = self.workspace_watchers.lock() {
            map.remove(workspace_id);
        }
        let _ = self.raw_tx.send(SkillsMsg::Remove(workspace_id.clone()));
    }

    /// Suspend a workspace (archive): tear down its project-tier watches like
    /// [`Self::remove_workspace`], but first snapshot the skill set into a
    /// PRIVATE fingerprint. The shared `DISCOVERY_CACHE` cannot serve as the
    /// baseline here: every `load_skills_payload` caller refreshes it, and
    /// `unarchive_workspace` kicks agent queue drains (whose prompt build
    /// calls `format_skills_catalog_for_prompt`) before publishing the delta,
    /// so the cache can absorb an archive-window edit before the resume flush
    /// runs and silently swallow the catch-up event.
    pub(crate) fn pause_workspace(&self, workspace_id: &WorkspaceId) {
        if let Ok(mut map) = self.workspace_watchers.lock() {
            map.remove(workspace_id);
        }
        let _ = self.raw_tx.send(SkillsMsg::Pause(workspace_id.clone()));
    }

    /// Test-only barrier: resolves once the debounce loop has fully processed
    /// every message sent before this call (the raw channel is FIFO). Lets
    /// tests wait deterministically for e.g. a `Pause` fingerprint snapshot
    /// instead of sleeping, which loses under full-suite load (monorepo#1841).
    #[cfg(test)]
    async fn barrier(&self) {
        let (ack_tx, mut ack_rx) = mpsc::unbounded_channel();
        let _ = self.raw_tx.send(SkillsMsg::Barrier(ack_tx));
        tokio::time::timeout(crate::events::LIVENESS, ack_rx.recv())
            .await
            .expect("skills debounce loop did not ack barrier within LIVENESS")
            .expect("skills debounce loop dropped before acking barrier");
    }

    /// Resume a suspended workspace (unarchive): re-watch the project tier and
    /// schedule one catch-up flush compared against the fingerprint retained
    /// by [`Self::pause_workspace`], so an unchanged tree emits nothing and an
    /// edit made while suspended emits exactly once.
    pub(crate) fn resume_workspace(&self, workspace_id: WorkspaceId, workspace_path: &Path) {
        let _ = self.raw_tx.send(SkillsMsg::Resume(
            workspace_id.clone(),
            workspace_path.to_path_buf(),
        ));
        let watch = start_project_watch(&self.hub, &workspace_id, workspace_path, &self.raw_tx);
        if let Ok(mut map) = self.workspace_watchers.lock() {
            map.insert(workspace_id, watch);
        }
    }
}

/// Watch all three project-tier skill roots of one workspace over the shared
/// workspace-root stream — one subscription, no streams of its own (previously
/// three [`RootWatch`]es, each its own stream even when the tier was missing).
fn start_project_watch(
    hub: &Arc<SharedWatchHub>,
    workspace_id: &WorkspaceId,
    workspace_path: &Path,
    raw_tx: &mpsc::UnboundedSender<SkillsMsg>,
) -> TierWatch {
    let ws_id = workspace_id.clone();
    let tx = raw_tx.clone();
    watch_tiers(
        hub,
        workspace_path,
        PROJECT_SKILL_TIERS,
        is_skill_md,
        move || {
            let _ = tx.send(SkillsMsg::Change(Some(ws_id.clone())));
        },
    )
}

/// Message into the debounce loop: a raw filesystem change, or a runtime
/// (de)registration of a workspace (#611).
#[derive(Debug, Clone)]
enum SkillsMsg {
    /// Raw change from a root watch; `None` = user tier (all workspaces).
    Change(Option<WorkspaceId>),
    /// Workspace registered after start.
    Add(WorkspaceId, PathBuf),
    /// Workspace deregistered; its pending flush is dropped.
    Remove(WorkspaceId),
    /// Workspace suspended (archive): stops emitting like `Remove`, but the
    /// skill set is fingerprinted first so the later `Resume` catch-up has a
    /// baseline that the shared discovery cache cannot invalidate.
    Pause(WorkspaceId),
    /// Workspace re-registered after a suspension (unarchive): like `Add`,
    /// plus a due-now flush so changes missed while suspended are picked up.
    Resume(WorkspaceId, PathBuf),
    /// Test-only sync point: acked once all earlier messages are processed.
    #[cfg(test)]
    Barrier(mpsc::UnboundedSender<()>),
}

/// Fingerprint the resolved skill set for a workspace: a hash of the
/// serialized [`crate::skills::SkillMetadata`] list, so a body-only or
/// description-only edit during a suspension is still detected (the shared
/// `check_skills_changed` compares names and count only).
async fn skills_fingerprint(workspace_path: &Path) -> u64 {
    let skills = crate::skills::discover_skills(&workspace_path.to_string_lossy()).await;
    let rendered = serde_json::to_string(&skills).unwrap_or_default();
    let mut hasher = DefaultHasher::new();
    rendered.hash(&mut hasher);
    hasher.finish()
}

/// Watch a single root, forwarding `SKILL.md` and directory-level events.
/// Missing roots are handled by [`watch_root`]: a non-recursive watch on the
/// nearest existing ancestor is promoted to a recursive watch on the root
/// once it appears.
fn watch_directory(
    root: PathBuf,
    workspace_id: Option<WorkspaceId>,
    tx: mpsc::UnboundedSender<SkillsMsg>,
) -> RootWatch {
    watch_root(root, is_skill_md, move || {
        let _ = tx.send(SkillsMsg::Change(workspace_id.clone()));
    })
}

fn is_skill_md(path: &Path) -> bool {
    path.file_name().and_then(|n| n.to_str()) == Some("SKILL.md")
}

fn get_user_skill_roots() -> Vec<PathBuf> {
    let mut roots = Vec::new();
    if let Some(home) = home_dir() {
        roots.push(home.join(".agents").join("skills"));
        roots.push(home.join(".claude").join("skills"));
        roots.push(home.join(".intent").join("skills"));
        roots.push(home.join(".augment").join("skills"));
    }
    roots
}

/// Project-tier skill roots, relative to the workspace root.
const PROJECT_SKILL_TIERS: &[&str] = &[".agents/skills", ".intent/skills", ".augment/skills"];

fn home_dir() -> Option<PathBuf> {
    std::env::var_os("HOME")
        .or_else(|| std::env::var_os("USERPROFILE"))
        .map(PathBuf::from)
        .filter(|p| !p.as_os_str().is_empty())
}

/// Debounce loop that coalesces rapid skill file changes per workspace.
/// `Add`/`Remove` messages keep the workspace map in step with runtime
/// (de)registrations (#611).
async fn debounce_loop(
    bus: EventBus,
    workspaces: Vec<(WorkspaceId, PathBuf)>,
    mut raw_rx: mpsc::UnboundedReceiver<SkillsMsg>,
) {
    let mut pending: HashMap<WorkspaceId, tokio::time::Instant> = HashMap::new();
    let mut workspace_paths: HashMap<WorkspaceId, PathBuf> = workspaces.into_iter().collect();
    // Baselines snapshotted at `Pause`, consumed by the first flush after the
    // matching `Resume`. Only suspended workspaces have an entry — the normal
    // watch path keeps using the shared discovery cache.
    let mut suspend_baselines: HashMap<WorkspaceId, u64> = HashMap::new();

    loop {
        let next_deadline = pending.values().copied().min();

        tokio::select! {
            maybe = raw_rx.recv() => match maybe {
                Some(SkillsMsg::Change(workspace_id)) => {
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
                Some(SkillsMsg::Add(ws_id, path)) => {
                    suspend_baselines.remove(&ws_id);
                    workspace_paths.insert(ws_id, path);
                }
                Some(SkillsMsg::Remove(ws_id)) => {
                    workspace_paths.remove(&ws_id);
                    suspend_baselines.remove(&ws_id);
                    pending.remove(&ws_id);
                }
                Some(SkillsMsg::Pause(ws_id)) => {
                    if let Some(path) = workspace_paths.get(&ws_id) {
                        suspend_baselines.insert(ws_id.clone(), skills_fingerprint(path).await);
                    }
                    workspace_paths.remove(&ws_id);
                    pending.remove(&ws_id);
                }
                Some(SkillsMsg::Resume(ws_id, path)) => {
                    workspace_paths.insert(ws_id.clone(), path);
                    // Catch-up: flush after the normal debounce so the
                    // re-registered watches' own events coalesce into it.
                    pending.insert(ws_id, tokio::time::Instant::now() + DEBOUNCE);
                }
                #[cfg(test)]
                Some(SkillsMsg::Barrier(ack)) => {
                    let _ = ack.send(());
                }
                None => {
                    // All senders dropped: flush and exit
                    flush_all(&bus, &workspace_paths, &mut suspend_baselines, &mut pending).await;
                    return;
                }
            },
            () = sleep_until(next_deadline), if next_deadline.is_some() => {
                flush_due(&bus, &workspace_paths, &mut suspend_baselines, &mut pending).await;
            }
        }
    }
}

async fn flush_due(
    bus: &EventBus,
    workspace_paths: &HashMap<WorkspaceId, PathBuf>,
    suspend_baselines: &mut HashMap<WorkspaceId, u64>,
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
            let baseline = suspend_baselines.remove(&ws_id);
            emit_skills_changed(bus, &ws_id, path, baseline).await;
        }
    }
}

async fn flush_all(
    bus: &EventBus,
    workspace_paths: &HashMap<WorkspaceId, PathBuf>,
    suspend_baselines: &mut HashMap<WorkspaceId, u64>,
    pending: &mut HashMap<WorkspaceId, tokio::time::Instant>,
) {
    for (ws_id, _) in pending.drain() {
        if let Some(path) = workspace_paths.get(&ws_id) {
            let baseline = suspend_baselines.remove(&ws_id);
            emit_skills_changed(bus, &ws_id, path, baseline).await;
        }
    }
}

/// Emit `skills:changed` if the set actually changed. `suspend_baseline` is
/// `Some` only for the first flush after an unarchive: the shared discovery
/// cache is unusable as a baseline across that window (any `skills.*` reader
/// can refresh it mid-suspension), so the retained fingerprint is compared
/// instead. Without one — e.g. a workspace archived before daemon start — the
/// normal cache comparison applies and may emit one benign extra event.
async fn emit_skills_changed(
    bus: &EventBus,
    workspace_id: &WorkspaceId,
    workspace_path: &Path,
    suspend_baseline: Option<u64>,
) {
    // Re-run discovery to check if the skill set actually changed
    let (_, cache_changed) =
        crate::skills::check_skills_changed(&workspace_path.to_string_lossy()).await;
    let changed = match suspend_baseline {
        Some(baseline) => skills_fingerprint(workspace_path).await != baseline,
        None => cache_changed,
    };

    if changed {
        let event = NewEvent {
            workspace_id: workspace_id.clone(),
            timestamp: now_iso(),
            event_type: SKILLS_CHANGED.to_string(),
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

    /// Self-cleaning temp directory (workspace root).
    struct TempDir {
        path: PathBuf,
    }

    impl TempDir {
        fn new(tag: &str) -> Self {
            let path = std::env::temp_dir().join(format!(
                "intentd-skills-watch-{tag}-{}",
                uuid::Uuid::new_v4()
            ));
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
                .join(format!("intentd-skills-watch-{}.db", uuid::Uuid::new_v4()));
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

    /// Drain `skills:changed` events from the subscription. Waits up to the
    /// full `deadline` for the FIRST matching event; once at least one has
    /// been collected, applies the `quiet` coalescing window (stop after
    /// `quiet` elapses with no new batch, or when `deadline` passes).
    async fn drain_skills_events(
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
                        if ev.event_type == SKILLS_CHANGED {
                            events.push(ev);
                        }
                    }
                }
                _ => break,
            }
        }
        events
    }

    fn skill_md(name: &str) -> String {
        format!("---\nname: {name}\ndescription: d\n---\n\nbody")
    }

    #[tokio::test]
    #[allow(clippy::await_holding_lock)]
    async fn workspace_added_after_start_gains_watching_and_removal_stops_it() {
        let _serial = crate::events::WATCHER_TEST_SERIAL
            .lock()
            .unwrap_or_else(std::sync::PoisonError::into_inner);
        let (_db, bus, mut sub) = bus_and_sub().await;
        let ws = TempDir::new("dyn-ws");
        let ws_id = WorkspaceId::from("ws-skills-dyn");

        // Start with NO workspaces; register at runtime (#611). Warm-ups widened
        // 250ms -> 500ms so the runtime registration + its OS watch establish
        // before the SKILL.md is created, which under nextest's oversubscribed
        // parallelism a 250ms warm-up can lose.
        let watcher = SkillsWatcher::start(&SharedWatchHub::new(), bus.clone(), vec![]);
        watcher.wait_established(LIVENESS).await;
        tokio::time::sleep(Duration::from_millis(500)).await;

        watcher.add_workspace(ws_id.clone(), &ws.path.clone());
        watcher.wait_established(LIVENESS).await;
        tokio::time::sleep(Duration::from_millis(500)).await;

        // A project-tier SKILL.md created after registration emits for the
        // new workspace.
        let skill_dir = ws.path.join(".intent").join("skills").join("dyn-skill");
        std::fs::create_dir_all(&skill_dir).expect("mk skill dir");
        std::fs::write(skill_dir.join("SKILL.md"), skill_md("dyn-skill")).expect("write skill");

        // The drain waits up to `LIVENESS` for the first event (monorepo#1630),
        // with a generous quiet window to absorb fsevents detection +
        // forward-task latency under load. The negative-assertion drain below
        // stays tight — it asserts absence.
        let events = drain_skills_events(&mut sub, Duration::from_secs(8), LIVENESS).await;
        assert!(
            events.iter().any(|e| e.workspace_id == ws_id),
            "change after runtime registration must emit for the workspace, got {events:?}"
        );

        // Deregister: further project-tier changes no longer emit.
        watcher.remove_workspace(&ws_id);
        tokio::time::sleep(Duration::from_millis(250)).await;

        let late_dir = ws.path.join(".intent").join("skills").join("late-skill");
        std::fs::create_dir_all(&late_dir).expect("mk late skill dir");
        std::fs::write(late_dir.join("SKILL.md"), skill_md("late-skill"))
            .expect("write skill after removal");

        let events = drain_skills_events(
            &mut sub,
            Duration::from_millis(1500),
            Duration::from_secs(3),
        )
        .await;
        assert!(
            events.iter().all(|e| e.workspace_id != ws_id),
            "deregistered workspace must stop emitting, got {events:?}"
        );
    }

    #[tokio::test]
    #[allow(clippy::await_holding_lock)]
    async fn resume_catch_up_survives_a_discovery_cache_refresh_while_suspended() {
        let _serial = crate::events::WATCHER_TEST_SERIAL
            .lock()
            .unwrap_or_else(std::sync::PoisonError::into_inner);
        let (_db, bus, mut sub) = bus_and_sub().await;
        let ws = TempDir::new("pause-ws");
        let ws_id = WorkspaceId::from("ws-skills-pause");
        let skill_dir = ws.path.join(".intent").join("skills").join("seed-skill");
        std::fs::create_dir_all(&skill_dir).expect("mk skill dir");
        std::fs::write(skill_dir.join("SKILL.md"), skill_md("seed-skill")).expect("seed skill");

        let watcher = SkillsWatcher::start(
            &SharedWatchHub::new(),
            bus.clone(),
            vec![(ws_id.clone(), ws.path.clone())],
        );
        tokio::time::sleep(Duration::from_millis(500)).await;

        // Suspend, then add a skill and let an unrelated reader refresh the
        // shared DISCOVERY_CACHE — exactly what `unarchive_workspace`'s queue
        // drain does before the delta is published. The retained fingerprint,
        // not the cache, must be what the resume flush compares against.
        // The barrier (not a sleep) guarantees the pause fingerprint has been
        // snapshotted BEFORE the edit below, so the baseline cannot absorb it
        // when the debounce loop lags under full-suite load (monorepo#1841).
        watcher.pause_workspace(&ws_id);
        watcher.barrier().await;

        let added = ws.path.join(".intent").join("skills").join("added-skill");
        std::fs::create_dir_all(&added).expect("mk added skill dir");
        std::fs::write(added.join("SKILL.md"), skill_md("added-skill")).expect("write skill");
        crate::skills::discover_skills(&ws.path.to_string_lossy()).await;

        watcher.resume_workspace(ws_id.clone(), &ws.path.clone());
        let events = drain_skills_events(&mut sub, Duration::from_secs(2), LIVENESS).await;
        assert!(
            events.iter().any(|e| e.workspace_id == ws_id),
            "resume must emit for an edit made while suspended even though the cache was refreshed, got {events:?}"
        );
    }
}
