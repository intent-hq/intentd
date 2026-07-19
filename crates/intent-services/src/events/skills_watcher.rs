//! Skills directory watcher → `skills:changed` events.
//!
//! Watches the 5-tier skills scan roots (3 user-tier + 2 project-tier per workspace)
//! and emits `skills:changed` events when SKILL.md files are created, modified, or
//! deleted. User-tier changes affect all workspaces; project-tier changes are scoped
//! to their workspace. Debounce is 500ms per workspace to coalesce rapid edits.

use std::collections::HashMap;
use std::path::{Path, PathBuf};
use std::time::Duration;

use intent_core::{events::SKILLS_CHANGED, now_iso, ActorType, EventActor, WorkspaceId};
use intent_store::NewEvent;
use notify::{RecommendedWatcher, RecursiveMode, Watcher};
use serde_json::json;
use tokio::sync::mpsc;
use tokio::task::JoinHandle;

use super::bus::EventBus;

const DEBOUNCE: Duration = Duration::from_millis(500);

/// Holds watchers for all skills directories (user-tier + project-tier).
/// Dropping this tears down all watchers.
pub struct SkillsWatcher {
    _user_watchers: Vec<RecommendedWatcher>,
    _workspace_watchers: Vec<RecommendedWatcher>,
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
        let (raw_tx, raw_rx) = mpsc::unbounded_channel::<SkillsEvent>();

        // Start user-tier watchers (affect all workspaces)
        let mut user_watchers = Vec::new();
        let user_roots = get_user_skill_roots();
        for root in user_roots {
            if let Ok(watcher) = watch_directory(root, None, raw_tx.clone()) {
                user_watchers.push(watcher);
            }
        }

        // Start project-tier watchers (per-workspace)
        let mut workspace_watchers = Vec::new();
        for (ws_id, ws_path) in &workspaces {
            let project_roots = get_project_skill_roots(ws_path);
            for root in project_roots {
                if let Ok(watcher) = watch_directory(root, Some(ws_id.clone()), raw_tx.clone()) {
                    workspace_watchers.push(watcher);
                }
            }
        }

        let task = tokio::spawn(debounce_loop(bus, workspaces, raw_rx));

        Self {
            _user_watchers: user_watchers,
            _workspace_watchers: workspace_watchers,
            task,
        }
    }
}

#[derive(Debug, Clone)]
struct SkillsEvent {
    workspace_id: Option<WorkspaceId>, // None = affects all workspaces
}

/// Watch a single directory (or its nearest existing ancestor).
fn watch_directory(
    root: PathBuf,
    workspace_id: Option<WorkspaceId>,
    tx: mpsc::UnboundedSender<SkillsEvent>,
) -> notify::Result<RecommendedWatcher> {
    let watch_path = find_existing_ancestor(&root);

    let mut watcher = notify::recommended_watcher(move |res: notify::Result<notify::Event>| {
        if let Ok(event) = res {
            // Only care about SKILL.md files
            if event.paths.iter().any(|p| {
                p.file_name()
                    .and_then(|n| n.to_str())
                    .map(|n| n == "SKILL.md")
                    .unwrap_or(false)
            }) {
                let _ = tx.send(SkillsEvent {
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

fn get_user_skill_roots() -> Vec<PathBuf> {
    let mut roots = Vec::new();
    if let Some(home) = home_dir() {
        roots.push(home.join(".agents").join("skills"));
        roots.push(home.join(".claude").join("skills"));
        roots.push(home.join(".augment").join("skills"));
    }
    roots
}

fn get_project_skill_roots(workspace_path: &Path) -> Vec<PathBuf> {
    vec![
        workspace_path.join(".agents").join("skills"),
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
async fn debounce_loop(
    bus: EventBus,
    workspaces: Vec<(WorkspaceId, PathBuf)>,
    mut raw_rx: mpsc::UnboundedReceiver<SkillsEvent>,
) {
    let mut pending: HashMap<WorkspaceId, tokio::time::Instant> = HashMap::new();
    let workspace_paths: HashMap<WorkspaceId, PathBuf> = workspaces.into_iter().collect();
    let all_workspace_ids: Vec<WorkspaceId> = workspace_paths.keys().cloned().collect();

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
