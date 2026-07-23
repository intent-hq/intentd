//! Specialist directory watcher → `specialists:changed` events.
//!
//! Watches the writable specialist tiers — user (`~/.intent/specialists/`) and
//! project (`<workspace>/.intent/specialists/` per workspace) — and emits
//! `specialists:changed` events when `<id>.md` files are created, modified, or
//! deleted. User-tier changes affect all workspaces; project-tier changes are
//! scoped to their workspace. Debounce is 500ms per workspace to coalesce rapid
//! edits, and an event is emitted only when the resolved specialist set
//! actually changed (fingerprint check, analogous to `check_skills_changed`).
//! The bundled/embedded tiers are static at runtime and are not watched.

use std::collections::hash_map::DefaultHasher;
use std::collections::HashMap;
use std::hash::{Hash, Hasher};
use std::path::{Path, PathBuf};
use std::time::Duration;

use intent_core::{events::SPECIALISTS_CHANGED, now_iso, ActorType, EventActor, WorkspaceId};
use intent_store::NewEvent;
use notify::{RecommendedWatcher, RecursiveMode, Watcher};
use serde_json::json;
use tokio::sync::mpsc;
use tokio::task::JoinHandle;

use super::bus::EventBus;
use crate::specialists::SpecialistsService;

const DEBOUNCE: Duration = Duration::from_millis(500);

/// Holds watchers for all specialist directories (user-tier + project-tier).
/// Dropping this tears down all watchers.
pub struct SpecialistsWatcher {
    _user_watchers: Vec<RecommendedWatcher>,
    _workspace_watchers: Vec<RecommendedWatcher>,
    task: JoinHandle<()>,
}

impl Drop for SpecialistsWatcher {
    fn drop(&mut self) {
        self.task.abort();
    }
}

impl SpecialistsWatcher {
    /// Start watching specialist directories for all workspaces.
    /// `workspaces` is a list of (workspace_id, workspace_path) pairs.
    pub fn start(bus: EventBus, workspaces: Vec<(WorkspaceId, PathBuf)>) -> Self {
        Self::start_with_user_dir(bus, workspaces, default_user_dir())
    }

    /// Like [`Self::start`] but with an explicit user-tier root (tests inject a
    /// temp dir for hermetic coverage; production passes the default).
    fn start_with_user_dir(
        bus: EventBus,
        workspaces: Vec<(WorkspaceId, PathBuf)>,
        user_dir: Option<PathBuf>,
    ) -> Self {
        let (raw_tx, raw_rx) = mpsc::unbounded_channel::<SpecialistsEvent>();

        // Start the user-tier watcher (affects all workspaces)
        let mut user_watchers = Vec::new();
        if let Some(root) = &user_dir {
            if let Ok(watcher) = watch_directory(root.clone(), None, raw_tx.clone()) {
                user_watchers.push(watcher);
            }
        }

        // Start project-tier watchers (per-workspace)
        let mut workspace_watchers = Vec::new();
        for (ws_id, ws_path) in &workspaces {
            let root = project_dir(ws_path);
            if let Ok(watcher) = watch_directory(root, Some(ws_id.clone()), raw_tx.clone()) {
                workspace_watchers.push(watcher);
            }
        }

        let task = tokio::spawn(debounce_loop(bus, workspaces, user_dir, raw_rx));

        Self {
            _user_watchers: user_watchers,
            _workspace_watchers: workspace_watchers,
            task,
        }
    }
}

#[derive(Debug, Clone)]
struct SpecialistsEvent {
    workspace_id: Option<WorkspaceId>, // None = affects all workspaces
}

/// Watch a single directory (or its nearest existing ancestor).
fn watch_directory(
    root: PathBuf,
    workspace_id: Option<WorkspaceId>,
    tx: mpsc::UnboundedSender<SpecialistsEvent>,
) -> notify::Result<RecommendedWatcher> {
    let watch_path = find_existing_ancestor(&root);
    // Filter against the canonical root: OS watchers (FSEvents in particular)
    // report canonicalized paths, so a symlinked root (e.g. `/var` →
    // `/private/var` on macOS) would otherwise never match.
    let root = canonical_root(&root, &watch_path);

    let mut watcher = notify::recommended_watcher(move |res: notify::Result<notify::Event>| {
        if let Ok(event) = res {
            // Only care about `<id>.md` files under the specialists root (the
            // watch may sit on an ancestor when the root does not exist yet).
            if event.paths.iter().any(|p| {
                p.starts_with(&root) && p.extension().and_then(|e| e.to_str()) == Some("md")
            }) {
                let _ = tx.send(SpecialistsEvent {
                    workspace_id: workspace_id.clone(),
                });
            }
        }
    })?;

    watcher.watch(&watch_path, RecursiveMode::Recursive)?;
    Ok(watcher)
}

/// Find the nearest existing ancestor of a path (for non-existent roots).
fn find_existing_ancestor(path: &Path) -> PathBuf {
    let mut current = path.to_path_buf();
    while !current.exists() && current.parent().is_some() {
        current = current.parent().unwrap().to_path_buf();
    }
    if current.exists() {
        current
    } else {
        path.to_path_buf()
    }
}

/// Rebase `root` onto the canonicalized form of its nearest existing
/// `ancestor`, so it can be compared against the canonical paths OS watchers
/// report.
fn canonical_root(root: &Path, ancestor: &Path) -> PathBuf {
    let canonical_ancestor = ancestor
        .canonicalize()
        .unwrap_or_else(|_| ancestor.to_path_buf());
    match root.strip_prefix(ancestor) {
        Ok(rest) => canonical_ancestor.join(rest),
        Err(_) => root.to_path_buf(),
    }
}

/// Default user-tier specialists directory (`~/.intent/specialists/`).
fn default_user_dir() -> Option<PathBuf> {
    home_dir().map(|h| h.join(".intent").join("specialists"))
}

/// The project-tier specialists directory for a workspace.
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
async fn debounce_loop(
    bus: EventBus,
    workspaces: Vec<(WorkspaceId, PathBuf)>,
    user_dir: Option<PathBuf>,
    mut raw_rx: mpsc::UnboundedReceiver<SpecialistsEvent>,
) {
    let mut pending: HashMap<WorkspaceId, tokio::time::Instant> = HashMap::new();
    let workspace_paths: HashMap<WorkspaceId, PathBuf> = workspaces.into_iter().collect();
    let all_workspace_ids: Vec<WorkspaceId> = workspace_paths.keys().cloned().collect();

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
                Some(event) => {
                    let deadline = tokio::time::Instant::now() + DEBOUNCE;
                    match event.workspace_id {
                        // User-tier change: affects all workspaces
                        None => {
                            for ws_id in &all_workspace_ids {
                                pending.insert(ws_id.clone(), deadline);
                            }
                        }
                        // Project-tier change: affects specific workspace
                        Some(ws_id) => {
                            pending.insert(ws_id, deadline);
                        }
                    }
                }
                None => {
                    // All senders dropped: flush and exit
                    flush_all(&bus, &workspace_paths, &user_dir, &mut fingerprints, &mut pending).await;
                    return;
                }
            },
            _ = sleep_until(next_deadline), if next_deadline.is_some() => {
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

    /// Drain `specialists:changed` events from the subscription until `quiet`
    /// elapses with no new batch (or `deadline` passes).
    async fn drain_specialists_events(
        sub: &mut super::super::bus::Subscription,
        quiet: Duration,
        deadline: Duration,
    ) -> Vec<Event> {
        let mut events = Vec::new();
        let end = Instant::now() + deadline;
        loop {
            let remaining = end.saturating_duration_since(Instant::now());
            if remaining.is_zero() {
                break;
            }
            match timeout(quiet.min(remaining), sub.recv()).await {
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
    async fn project_tier_burst_debounces_to_one_event() {
        let (_db, bus, mut sub) = bus_and_sub().await;
        let user = TempDir::new("debounce-user");
        let ws = TempDir::new("debounce-ws");
        let ws_id = WorkspaceId::from("ws-debounce");
        let proj = project_dir(&ws.path);
        std::fs::create_dir_all(&proj).expect("mk project tier");

        let _watcher = SpecialistsWatcher::start_with_user_dir(
            bus.clone(),
            vec![(ws_id.clone(), ws.path.clone())],
            Some(user.path.clone()),
        );
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

        let events =
            drain_specialists_events(&mut sub, Duration::from_secs(2), Duration::from_secs(10))
                .await;
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
    async fn user_tier_change_fans_out_to_all_workspaces() {
        let (_db, bus, mut sub) = bus_and_sub().await;
        let user = TempDir::new("fanout-user");
        let ws1 = TempDir::new("fanout-ws1");
        let ws2 = TempDir::new("fanout-ws2");
        let ws1_id = WorkspaceId::from("ws-fanout-1");
        let ws2_id = WorkspaceId::from("ws-fanout-2");

        let _watcher = SpecialistsWatcher::start_with_user_dir(
            bus.clone(),
            vec![
                (ws1_id.clone(), ws1.path.clone()),
                (ws2_id.clone(), ws2.path.clone()),
            ],
            Some(user.path.clone()),
        );
        tokio::time::sleep(Duration::from_millis(250)).await;

        std::fs::write(
            user.path.join("shared.md"),
            specialist_md("Shared", "user-tier body"),
        )
        .expect("write user specialist");

        let events =
            drain_specialists_events(&mut sub, Duration::from_secs(2), Duration::from_secs(10))
                .await;
        let mut ws_ids: Vec<&str> = events.iter().map(|e| e.workspace_id.as_str()).collect();
        ws_ids.sort_unstable();
        assert_eq!(
            ws_ids,
            vec![ws1_id.as_str(), ws2_id.as_str()],
            "user-tier change must emit exactly one event per workspace, got {events:?}"
        );
    }

    #[tokio::test]
    async fn unchanged_set_emits_nothing() {
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
            bus.clone(),
            vec![(ws_id.clone(), ws.path.clone())],
            Some(user.path.clone()),
        );
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
        let events =
            drain_specialists_events(&mut sub, Duration::from_secs(2), Duration::from_secs(10))
                .await;
        assert_eq!(
            events.len(),
            1,
            "real change after a no-op must emit exactly one event, got {events:?}"
        );
        assert_eq!(events[0].workspace_id, ws_id);
    }
}
