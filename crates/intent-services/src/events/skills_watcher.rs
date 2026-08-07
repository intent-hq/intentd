//! Skills directory watcher → `skills:changed` events.
//!
//! Watches the 7-tier skills scan roots (4 user-tier + 3 project-tier per workspace)
//! and emits `skills:changed` events when SKILL.md files are created, modified, or
//! deleted — or when a tier directory itself appears or disappears (#612).
//! User-tier changes affect all workspaces; project-tier changes are scoped
//! to their workspace. Debounce is 500ms per workspace to coalesce rapid edits.
//! Workspaces can be registered/deregistered at runtime (#611) via
//! [`SkillsWatcher::add_workspace`] / [`SkillsWatcher::remove_workspace`].

use std::collections::HashMap;
use std::path::{Path, PathBuf};
use std::sync::Mutex;
use std::time::Duration;

use intent_core::{events::SKILLS_CHANGED, now_iso, ActorType, EventActor, WorkspaceId};
use intent_store::NewEvent;
use serde_json::json;
use tokio::sync::mpsc;
use tokio::task::JoinHandle;

use super::bus::EventBus;
use super::root_watch::{watch_root, RootWatch};

const DEBOUNCE: Duration = Duration::from_millis(500);

/// Holds watchers for all skills directories (user-tier + project-tier).
/// Dropping this tears down all watchers.
pub struct SkillsWatcher {
    _user_watchers: Vec<RootWatch>,
    workspace_watchers: Mutex<HashMap<WorkspaceId, Vec<RootWatch>>>,
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
    /// `workspaces` is a list of (workspace_id, workspace_path) pairs.
    pub fn start(bus: EventBus, workspaces: Vec<(WorkspaceId, PathBuf)>) -> Self {
        let (raw_tx, raw_rx) = mpsc::unbounded_channel::<SkillsMsg>();

        // Start user-tier watchers (affect all workspaces)
        let mut user_watchers = Vec::new();
        let user_roots = get_user_skill_roots();
        for root in user_roots {
            user_watchers.push(watch_directory(root, None, raw_tx.clone()));
        }

        // Start project-tier watchers (per-workspace)
        let mut workspace_watchers: HashMap<WorkspaceId, Vec<RootWatch>> = HashMap::new();
        for (ws_id, ws_path) in &workspaces {
            workspace_watchers.insert(
                ws_id.clone(),
                start_project_watchers(ws_id, ws_path, &raw_tx),
            );
        }

        let task = tokio::spawn(debounce_loop(bus, workspaces, raw_rx));

        Self {
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
    pub fn add_workspace(&self, workspace_id: WorkspaceId, workspace_path: PathBuf) {
        let _ = self
            .raw_tx
            .send(SkillsMsg::Add(workspace_id.clone(), workspace_path.clone()));
        let watchers = start_project_watchers(&workspace_id, &workspace_path, &self.raw_tx);
        if let Ok(mut map) = self.workspace_watchers.lock() {
            map.insert(workspace_id, watchers);
        }
    }

    /// Await every registered root watch actually being established. Watch
    /// registration is deferred off the caller's thread (monorepo#1572), so
    /// tests must wait for it before mutating the watched directories.
    #[cfg(test)]
    async fn wait_established(&self, timeout: Duration) {
        for watch in &self._user_watchers {
            watch.wait_established(timeout).await;
        }
        let deadline = tokio::time::Instant::now() + timeout;
        loop {
            let all_up = self
                .workspace_watchers
                .lock()
                .map(|map| map.values().flatten().all(|w| w.watched().is_some()))
                .unwrap_or(true);
            if all_up || tokio::time::Instant::now() >= deadline {
                return;
            }
            tokio::time::sleep(Duration::from_millis(25)).await;
        }
    }

    /// Deregister a workspace at runtime (#611): tear down its project-tier
    /// watches and drop any pending flush so it stops emitting.
    pub fn remove_workspace(&self, workspace_id: &WorkspaceId) {
        if let Ok(mut map) = self.workspace_watchers.lock() {
            map.remove(workspace_id);
        }
        let _ = self.raw_tx.send(SkillsMsg::Remove(workspace_id.clone()));
    }
}

/// Watch the project-tier skill roots of one workspace.
fn start_project_watchers(
    workspace_id: &WorkspaceId,
    workspace_path: &Path,
    raw_tx: &mpsc::UnboundedSender<SkillsMsg>,
) -> Vec<RootWatch> {
    let mut watchers = Vec::new();
    for root in get_project_skill_roots(workspace_path) {
        watchers.push(watch_directory(
            root,
            Some(workspace_id.clone()),
            raw_tx.clone(),
        ));
    }
    watchers
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

fn get_project_skill_roots(workspace_path: &Path) -> Vec<PathBuf> {
    vec![
        workspace_path.join(".agents").join("skills"),
        workspace_path.join(".intent").join("skills"),
        workspace_path.join(".augment").join("skills"),
    ]
}

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
                    workspace_paths.insert(ws_id, path);
                }
                Some(SkillsMsg::Remove(ws_id)) => {
                    workspace_paths.remove(&ws_id);
                    pending.remove(&ws_id);
                }
                None => {
                    // All senders dropped: flush and exit
                    flush_all(&bus, &workspace_paths, &mut pending).await;
                    return;
                }
            },
            _ = sleep_until(next_deadline), if next_deadline.is_some() => {
                flush_due(&bus, &workspace_paths, &mut pending).await;
            }
        }
    }
}

async fn flush_due(
    bus: &EventBus,
    workspace_paths: &HashMap<WorkspaceId, PathBuf>,
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
            emit_skills_changed(bus, &ws_id, path).await;
        }
    }
}

async fn flush_all(
    bus: &EventBus,
    workspace_paths: &HashMap<WorkspaceId, PathBuf>,
    pending: &mut HashMap<WorkspaceId, tokio::time::Instant>,
) {
    for (ws_id, _) in pending.drain() {
        if let Some(path) = workspace_paths.get(&ws_id) {
            emit_skills_changed(bus, &ws_id, path).await;
        }
    }
}

async fn emit_skills_changed(bus: &EventBus, workspace_id: &WorkspaceId, workspace_path: &Path) {
    // Re-run discovery to check if the skill set actually changed
    let (_, changed) = crate::skills::check_skills_changed(&workspace_path.to_string_lossy()).await;

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
            .unwrap_or_else(|e| e.into_inner());
        let (_db, bus, mut sub) = bus_and_sub().await;
        let ws = TempDir::new("dyn-ws");
        let ws_id = WorkspaceId::from("ws-skills-dyn");

        // Start with NO workspaces; register at runtime (#611). Warm-ups widened
        // 250ms -> 500ms so the runtime registration + its OS watch establish
        // before the SKILL.md is created, which under nextest's oversubscribed
        // parallelism a 250ms warm-up can lose.
        let watcher = SkillsWatcher::start(bus.clone(), vec![]);
        watcher.wait_established(Duration::from_secs(10)).await;
        tokio::time::sleep(Duration::from_millis(500)).await;

        watcher.add_workspace(ws_id.clone(), ws.path.clone());
        watcher.wait_established(Duration::from_secs(10)).await;
        tokio::time::sleep(Duration::from_millis(500)).await;

        // A project-tier SKILL.md created after registration emits for the
        // new workspace.
        let skill_dir = ws.path.join(".intent").join("skills").join("dyn-skill");
        std::fs::create_dir_all(&skill_dir).expect("mk skill dir");
        std::fs::write(skill_dir.join("SKILL.md"), skill_md("dyn-skill")).expect("write skill");

        // First-event budget is `quiet` (the loop breaks after one quiet window
        // of silence), widened 2s -> 8s (with the total deadline to match) to
        // absorb fsevents detection + forward-task latency under load. The
        // negative-assertion drain below stays tight — it asserts absence.
        let events =
            drain_skills_events(&mut sub, Duration::from_secs(8), Duration::from_secs(20)).await;
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
}
