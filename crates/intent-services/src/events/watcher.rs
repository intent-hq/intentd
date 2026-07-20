//! Filesystem watcher → `file:*` events (§10).
//!
//! Ports the workspace file-watch slice from `~/src/intent/`: the `notify`
//! recursive watch + per-path debounce of
//! `workspace/main/change-detection/file-watcher.ts` (`fileWatcherDebounce`,
//! `handleFileEvent`) and the canonical event-type taxonomy of
//! `change-detection/change-processor.ts` (`getEventType`): a `create` becomes
//! `file:created`, a `delete` becomes `file:deleted`, and both `modify` and
//! `rename` stay `file:changed` (`file:renamed` is never emitted). The
//! `data.action` discriminant always carries the raw `create|modify|delete|
//! rename` verb regardless of the event type, matching the TS `FileChangedEvent`
//! wire shape. Raw FS callbacks (sync, off-runtime) feed a tokio debounce task
//! that coalesces rapid changes per path before publishing to the [`EventBus`].

use std::collections::HashMap;
use std::path::{Component, Path, PathBuf};
use std::time::Duration;

use intent_core::{now_iso, ActorType, EventActor, WorkspaceId};
use intent_store::NewEvent;
use notify::event::{EventKind, ModifyKind};
use notify::{RecommendedWatcher, RecursiveMode, Watcher};
use serde_json::json;
use tokio::sync::mpsc;
use tokio::task::JoinHandle;

use super::bus::EventBus;

/// Per-path debounce window. Matches the TS `fileWatcherDebounce` (300 ms): an
/// event is published `DEBOUNCE` after the *last* raw change for that path.
const DEBOUNCE: Duration = Duration::from_millis(300);

/// Burst threshold: when pending events exceed this count during a flush
/// window, collapse into per-directory summary events rather than emitting
/// one row per file (finding F4: prevent 31,881 INSERTs from bulk churn).
const BURST_THRESHOLD: usize = 100;

/// After a burst flush, keep collapsing due paths into directory summaries for
/// this long. Bulk churn often reaches the loop in several waves (staggered OS
/// delivery, late modify re-notifications for the same writes), and each wave
/// on its own can sit below [`BURST_THRESHOLD`]; the cooldown makes trailing
/// waves of the same churn collapse too instead of flushing per-file
/// (STAB-121). Only refreshed when the backlog itself exceeds the threshold —
/// cooldown-only collapses consume the window rather than extend it, so
/// unrelated small activity after a churn returns to per-file events within
/// one cooldown instead of staying in summary mode indefinitely.
const BURST_COOLDOWN: Duration = Duration::from_millis(1000);

/// Upper bound on raw events ingested per [`drain_ready`] call. `ingest` is
/// cheap and never awaits, but the raw channel is unbounded; the cap keeps a
/// pathological backlog from pinning the loop in a single non-yielding drain.
/// Leftovers are picked up by the next `recv`/flush iteration.
const DRAIN_MAX_PER_CALL: usize = 10_000;

/// Directory names ignored at any depth, mirroring the `IGNORE_PATTERNS` of
/// `unified-workspace-watcher.ts` plus the `.workspace-notes` additions of
/// `tracking.config.ts`. A path is dropped if any component matches.
const IGNORED_DIRS: &[&str] = &[
    ".git",
    "node_modules",
    "target",
    "dist",
    "build",
    "out",
    ".next",
    ".nuxt",
    ".cache",
    ".parcel-cache",
    ".vscode",
    ".idea",
    "coverage",
    ".nyc_output",
    ".pytest_cache",
    "__pycache__",
    "venv",
    "vendor",
    ".svn",
    ".hg",
    "CVS",
    ".sass-cache",
    "tmp",
    "temp",
    ".tmp",
    ".temp",
    ".augment",
    ".intent",
    ".workspace-notes",
    ".workspace-notes.backup",
    ".workspace",
];

/// The raw change verb carried in `data.action` of every `file:*` event
/// (`file:created`/`file:deleted`/`file:changed` alike, per the module docs).
/// Serializes to the lowercase TS values (`change.action.toLowerCase()`).
/// `pub(super)` so the flush tests can build pending maps directly.
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub(super) enum Action {
    Modify,
    Rename,
    Create,
    Delete,
}

impl Action {
    fn as_str(self) -> &'static str {
        match self {
            Action::Create => "create",
            Action::Modify => "modify",
            Action::Delete => "delete",
            Action::Rename => "rename",
        }
    }

    /// Canonical `file:*` event type for this action, mirroring the TS
    /// `change-processor.ts` `getEventType`: `create`/`delete` get their own
    /// types; `modify` and `rename` collapse onto `file:changed`.
    fn event_type(self) -> &'static str {
        use intent_core::events::{FILE_CHANGED, FILE_CREATED, FILE_DELETED};
        match self {
            Action::Create => FILE_CREATED,
            Action::Delete => FILE_DELETED,
            Action::Modify | Action::Rename => FILE_CHANGED,
        }
    }

    /// Coalescing precedence: when several raw events land on one path inside
    /// the debounce window, the highest-rank action wins (a create+modify reads
    /// as `create`; anything ending in removal reads as `delete`).
    fn rank(self) -> u8 {
        match self {
            Action::Modify => 0,
            Action::Rename => 1,
            Action::Create => 2,
            Action::Delete => 3,
        }
    }
}

/// Map a `notify` event kind to an [`Action`]; `None` for access/other kinds
/// that carry no mutation (they are dropped, matching the TS adapter which only
/// forwards add/change/unlink).
fn action_for(kind: &EventKind) -> Option<Action> {
    match kind {
        EventKind::Create(_) => Some(Action::Create),
        EventKind::Remove(_) => Some(Action::Delete),
        EventKind::Modify(ModifyKind::Name(_)) => Some(Action::Rename),
        EventKind::Modify(_) => Some(Action::Modify),
        EventKind::Any => Some(Action::Modify),
        EventKind::Access(_) | EventKind::Other => None,
    }
}

/// True when `relative` lives under an ignored directory (component match).
fn should_ignore(relative: &Path) -> bool {
    relative.components().any(|c| match c {
        Component::Normal(name) => name.to_str().is_some_and(|n| IGNORED_DIRS.contains(&n)),
        _ => false,
    })
}

/// Workspace-relative, forward-slash path for the event payload, or `None` when
/// `abs` is outside `root` (defensive; `notify` only reports under `root`).
fn relative_path(root: &Path, abs: &Path) -> Option<String> {
    let rel = abs.strip_prefix(root).ok()?;
    let joined = rel
        .components()
        .filter_map(|c| match c {
            Component::Normal(s) => s.to_str(),
            _ => None,
        })
        .collect::<Vec<_>>()
        .join("/");
    Some(joined)
}

/// Extract the parent directory of a workspace-relative path. Root files return
/// an empty string (workspace root), nested paths return the parent directory.
fn parent_dir(path: &str) -> String {
    match path.rfind('/') {
        Some(idx) => path[..idx].to_string(),
        None => String::new(),
    }
}

/// A live recursive watch over one workspace path. Holds the `notify` watcher
/// (the OS subscription ends when it drops) and the debounce task (aborted on
/// drop), so dropping the [`FileWatcher`] tears the whole pipeline down — the
/// clean-shutdown contract for `serve`.
pub struct FileWatcher {
    _watcher: RecommendedWatcher,
    task: JoinHandle<()>,
}

impl Drop for FileWatcher {
    fn drop(&mut self) {
        self.task.abort();
    }
}

impl FileWatcher {
    /// Start watching `root` recursively, publishing debounced `file:changed`
    /// events for `workspace_id` to `bus`. The `notify` callback is synchronous
    /// and runs off the tokio runtime, so it forwards raw events over an
    /// unbounded channel to the async debounce loop.
    pub fn start(bus: EventBus, workspace_id: WorkspaceId, root: PathBuf) -> notify::Result<Self> {
        // Canonicalize so the relative-path strip works against the paths the OS
        // reports (macOS FSEvents resolves `/var/...` → `/private/var/...`).
        let root = std::fs::canonicalize(&root).unwrap_or(root);
        let (raw_tx, raw_rx) = mpsc::unbounded_channel::<notify::Event>();
        let mut watcher =
            notify::recommended_watcher(move |res: notify::Result<notify::Event>| {
                if let Ok(event) = res {
                    let _ = raw_tx.send(event);
                }
            })?;
        watcher.watch(&root, RecursiveMode::Recursive)?;
        let task = tokio::spawn(debounce_loop(bus, workspace_id, root, raw_rx));
        Ok(Self {
            _watcher: watcher,
            task,
        })
    }
}

/// Coalesce raw FS events per path within [`DEBOUNCE`], then publish one
/// `file:changed` per path. A path is flushed `DEBOUNCE` after its last raw
/// event (timer reset on each new event, as in the TS `handleFileEvent`).
async fn debounce_loop(
    bus: EventBus,
    workspace_id: WorkspaceId,
    root: PathBuf,
    mut raw_rx: mpsc::UnboundedReceiver<notify::Event>,
) {
    let mut pending: HashMap<String, (Action, tokio::time::Instant)> = HashMap::new();
    let mut burst_until: Option<tokio::time::Instant> = None;
    loop {
        let next_deadline = pending.values().map(|(_, at)| *at).min();
        tokio::select! {
            maybe = raw_rx.recv() => match maybe {
                Some(event) => {
                    ingest(&root, &event, &mut pending);
                    drain_ready(&root, &mut raw_rx, &mut pending);
                }
                // Watcher dropped: flush whatever is pending, then stop.
                None => {
                    flush_all(&bus, &workspace_id, &mut pending).await;
                    return;
                }
            },
            _ = sleep_until(next_deadline), if next_deadline.is_some() => {
                // Ingest everything already delivered before deciding what is
                // due, so the burst decision sees the full backlog even when
                // publishes are slow (STAB-121).
                drain_ready(&root, &mut raw_rx, &mut pending);
                flush_due(&bus, &workspace_id, &mut pending, &mut burst_until).await;
            }
        }
    }
}

/// Ingest every raw event already sitting in the channel without awaiting.
/// `tokio::select!` only takes one branch per iteration, so slow publishes
/// would otherwise starve ingestion: each raw event would be ingested one
/// publish-latency apart, spreading per-path deadlines so far that no single
/// flush ever sees the whole churn and the burst collapse never engages
/// (STAB-121).
fn drain_ready(
    root: &Path,
    raw_rx: &mut mpsc::UnboundedReceiver<notify::Event>,
    pending: &mut HashMap<String, (Action, tokio::time::Instant)>,
) {
    for _ in 0..DRAIN_MAX_PER_CALL {
        match raw_rx.try_recv() {
            Ok(event) => ingest(root, &event, pending),
            Err(_) => break,
        }
    }
}

/// Fold one raw event into `pending`, resetting each affected path's deadline.
fn ingest(
    root: &Path,
    event: &notify::Event,
    pending: &mut HashMap<String, (Action, tokio::time::Instant)>,
) {
    let Some(action) = action_for(&event.kind) else {
        return;
    };
    let deadline = tokio::time::Instant::now() + DEBOUNCE;
    for abs in &event.paths {
        let Some(rel) = relative_path(root, abs) else {
            continue;
        };
        if rel.is_empty() || should_ignore(Path::new(&rel)) {
            continue;
        }
        let merged = match pending.get(&rel) {
            Some((existing, _)) if existing.rank() >= action.rank() => *existing,
            _ => action,
        };
        pending.insert(rel, (merged, deadline));
    }
}

async fn sleep_until(deadline: Option<tokio::time::Instant>) {
    match deadline {
        Some(at) => tokio::time::sleep_until(at).await,
        None => std::future::pending().await,
    }
}

/// Publish + remove every path whose debounce deadline has elapsed. When the
/// in-flight backlog exceeds [`BURST_THRESHOLD`], collapse into per-directory
/// summaries.
///
/// The burst decision is based on the whole `pending` map, not just the paths
/// due at this instant: staggered OS delivery spreads per-path deadlines, so
/// a single bulk churn can come due across several flushes that individually
/// sit below the threshold (STAB-121). Once a flush collapses, `burst_until`
/// keeps subsequent flushes collapsed for [`BURST_COOLDOWN`] so trailing waves
/// of the same churn (e.g. late modify re-notifications) summarize too. The
/// cooldown is only refreshed while the backlog stays above the threshold;
/// cooldown-only collapses do not extend it, so unrelated small activity
/// cannot keep the watcher in summary mode indefinitely.
///
/// `pub(super)` so tests can exercise the burst decision deterministically
/// with hand-built pending maps (no OS watcher or sleeps).
pub(super) async fn flush_due(
    bus: &EventBus,
    workspace_id: &WorkspaceId,
    pending: &mut HashMap<String, (Action, tokio::time::Instant)>,
    burst_until: &mut Option<tokio::time::Instant>,
) {
    let now = tokio::time::Instant::now();
    let due: Vec<String> = pending
        .iter()
        .filter(|(_, (_, at))| *at <= now)
        .map(|(p, _)| p.clone())
        .collect();
    if due.is_empty() {
        return;
    }

    let over_threshold = pending.len() > BURST_THRESHOLD;
    let in_cooldown = burst_until.is_some_and(|until| now < until);
    if over_threshold || in_cooldown {
        if over_threshold {
            *burst_until = Some(now + BURST_COOLDOWN);
        }
        flush_burst(bus, workspace_id, pending, &due).await;
    } else {
        for path in due {
            if let Some((action, _)) = pending.remove(&path) {
                publish(bus, workspace_id, &path, action).await;
            }
        }
    }
}

/// Handle burst scenario: collapse >BURST_THRESHOLD events into bounded
/// per-directory summary events with metadata indicating the burst.
async fn flush_burst(
    bus: &EventBus,
    workspace_id: &WorkspaceId,
    pending: &mut HashMap<String, (Action, tokio::time::Instant)>,
    due: &[String],
) {
    // Group due paths by directory.
    let mut by_dir: HashMap<String, Vec<(String, Action)>> = HashMap::new();
    for path in due {
        if let Some((action, _)) = pending.remove(path) {
            let dir = parent_dir(path);
            by_dir.entry(dir).or_default().push((path.clone(), action));
        }
    }

    // Emit one summary event per directory containing the count and actions.
    for (dir, files) in by_dir {
        publish_burst(bus, workspace_id, &dir, &files).await;
    }
}

/// Drain every pending path unconditionally (shutdown flush).
async fn flush_all(
    bus: &EventBus,
    workspace_id: &WorkspaceId,
    pending: &mut HashMap<String, (Action, tokio::time::Instant)>,
) {
    for (path, (action, _)) in std::mem::take(pending) {
        publish(bus, workspace_id, &path, action).await;
    }
}

/// Emit one `file:*` event matching the TS `FileChangedEvent` wire shape:
/// `data.{path,relativePath,action}` (both paths workspace-relative) attributed
/// to the system actor. The event type follows [`Action::event_type`]
/// (`file:created`/`file:deleted`/`file:changed`) while `data.action` always
/// carries the raw verb.
async fn publish(bus: &EventBus, workspace_id: &WorkspaceId, relative: &str, action: Action) {
    let event = NewEvent {
        workspace_id: workspace_id.clone(),
        timestamp: now_iso(),
        event_type: action.event_type().to_string(),
        actor: EventActor {
            actor_type: ActorType::System,
            id: Some("system".to_string()),
            name: Some("System".to_string()),
            ..Default::default()
        },
        session_id: None,
        correlation_id: None,
        parent_event_id: None,
        metadata: None,
        data: json!({
            "path": relative,
            "relativePath": relative,
            "action": action.as_str(),
        }),
    };
    if let Err(e) = bus.publish(&event).await {
        tracing::warn!(error = %e, path = relative, "failed to publish file:* event");
    }
}

/// Emit a burst summary event for a directory: a single `file:changed` event
/// with `data.burst = true` and `data.affectedCount` indicating the number of
/// files affected. FE consumers (event.recentFiles / directoryChanges) can
/// recognize the burst marker and query the store for recent directory activity
/// rather than expecting individual per-file rows.
async fn publish_burst(
    bus: &EventBus,
    workspace_id: &WorkspaceId,
    dir: &str,
    files: &[(String, Action)],
) {
    let count = files.len();
    let display_path = if dir.is_empty() {
        ".".to_string()
    } else {
        dir.to_string()
    };

    // Count actions to provide summary metadata.
    let mut creates = 0;
    let mut deletes = 0;
    let mut modifies = 0;
    for (_, action) in files {
        match action {
            Action::Create => creates += 1,
            Action::Delete => deletes += 1,
            Action::Modify | Action::Rename => modifies += 1,
        }
    }

    let event = NewEvent {
        workspace_id: workspace_id.clone(),
        timestamp: now_iso(),
        event_type: intent_core::events::FILE_CHANGED.to_string(),
        actor: EventActor {
            actor_type: ActorType::System,
            id: Some("system".to_string()),
            name: Some("System".to_string()),
            ..Default::default()
        },
        session_id: None,
        correlation_id: None,
        parent_event_id: None,
        metadata: None,
        data: json!({
            "path": display_path,
            "relativePath": display_path,
            "action": "modify",
            "burst": true,
            "affectedCount": count,
            "creates": creates,
            "deletes": deletes,
            "modifies": modifies,
        }),
    };
    if let Err(e) = bus.publish(&event).await {
        tracing::warn!(error = %e, dir = dir, count = count, "failed to publish burst event");
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use notify::event::{CreateKind, ModifyKind, RemoveKind, RenameMode};

    #[test]
    fn action_for_maps_notify_kinds() {
        assert_eq!(
            action_for(&EventKind::Create(CreateKind::File)),
            Some(Action::Create)
        );
        assert_eq!(
            action_for(&EventKind::Remove(RemoveKind::File)),
            Some(Action::Delete)
        );
        assert_eq!(
            action_for(&EventKind::Modify(ModifyKind::Name(RenameMode::Both))),
            Some(Action::Rename)
        );
        assert_eq!(
            action_for(&EventKind::Modify(ModifyKind::Data(
                notify::event::DataChange::Content
            ))),
            Some(Action::Modify)
        );
        assert_eq!(action_for(&EventKind::Other), None);
    }

    #[test]
    fn event_type_matches_ts_taxonomy() {
        use intent_core::events::{FILE_CHANGED, FILE_CREATED, FILE_DELETED};
        // change-processor.ts getEventType: create/delete get distinct types;
        // modify/rename collapse onto file:changed (file:renamed is never emitted).
        assert_eq!(Action::Create.event_type(), FILE_CREATED);
        assert_eq!(Action::Delete.event_type(), FILE_DELETED);
        assert_eq!(Action::Modify.event_type(), FILE_CHANGED);
        assert_eq!(Action::Rename.event_type(), FILE_CHANGED);
    }

    #[test]
    fn action_precedence_keeps_strongest() {
        // Delete > Create > Rename > Modify.
        assert!(Action::Delete.rank() > Action::Create.rank());
        assert!(Action::Create.rank() > Action::Rename.rank());
        assert!(Action::Rename.rank() > Action::Modify.rank());
    }

    #[test]
    fn should_ignore_matches_noise_dirs() {
        assert!(should_ignore(Path::new("node_modules/foo.js")));
        assert!(should_ignore(Path::new("src/.git/index")));
        assert!(should_ignore(Path::new("target/debug/x")));
        assert!(should_ignore(Path::new(".workspace-notes/n.md")));
        assert!(!should_ignore(Path::new("src/main.rs")));
        assert!(!should_ignore(Path::new("README.md")));
    }

    #[test]
    fn relative_path_strips_root_and_uses_forward_slashes() {
        let root = Path::new("/ws/root");
        assert_eq!(
            relative_path(root, Path::new("/ws/root/src/main.rs")).as_deref(),
            Some("src/main.rs")
        );
        assert_eq!(relative_path(root, Path::new("/other/x")), None);
    }

    #[test]
    fn parent_dir_extracts_directory() {
        assert_eq!(parent_dir("foo.txt"), "");
        assert_eq!(parent_dir("src/main.rs"), "src");
        assert_eq!(parent_dir("a/b/c.txt"), "a/b");
        assert_eq!(parent_dir(""), "");
    }
}
