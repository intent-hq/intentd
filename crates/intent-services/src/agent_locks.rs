//! `services::agent_locks` — daemon-owned agent-lock computation (§5.19, §6.5).
//!
//! Ports the FE `agent-lock-saga` (removed in cloudlands-fe 95d908a2d): the
//! daemon computes which agents' files must not be manually staged/reverted —
//! an agent is **locked** when the workspace's effective auto-commit is
//! enabled, the agent owns at least one tracked change at the `unstaged` or
//! `staged` stage, and the agent is actively working (running a turn, or its
//! linked task note is not `complete`/`cancelled`). Locked file paths are the
//! union of the locked agents' unstaged/staged tracked-change paths.
//!
//! The snapshot is served on demand via `file-tracking.getAgentLocks`
//! (PROTOCOL §5.19) and pushed as the self-sufficient `changes:agent-locks`
//! event (§6.5) whenever a recompute — triggered by agent lifecycle, task
//! status, auto-commit policy, or tracked-change churn events — yields a
//! snapshot that differs from the last one published for that workspace.

use std::collections::{BTreeMap, BTreeSet, HashMap};
use std::time::Duration;

use intent_core::events::{
    AGENT_COMPLETED, AGENT_DELETED, AGENT_FAILED, AGENT_IDLE, AGENT_RESTORED, AGENT_RETIRED,
    AGENT_STARTED, AGENT_STATUS_CHANGED, CHANGES_AGENT_LOCKS, CHANGES_GIT_STATUS,
    CHANGES_METRICS_CHANGED, SETTINGS_CHANGED, TASK_STATUS_CHANGED, WORKSPACE_UPDATED,
};
use intent_core::{now_iso, AgentSession, AgentStatus, TaskStatus, WorkspaceId};
use intent_store::NewEvent;

use crate::events::SubscriptionFilter;
use crate::{publish_event, system_actor, Services};

/// Debounce window for the recompute subscriber: bursts of lifecycle/change
/// events within this window collapse into one recompute per workspace.
const RECOMPUTE_BATCH_WINDOW: Duration = Duration::from_millis(500);

/// One workspace's computed agent-lock state. Vectors are sorted and deduped
/// so equal states compare equal and the wire payload is deterministic.
#[derive(Debug, Clone, PartialEq, Eq)]
pub(crate) struct AgentLockSnapshot {
    pub(crate) auto_commit_enabled: bool,
    pub(crate) locked_agent_ids: Vec<String>,
    pub(crate) locked_file_paths: Vec<String>,
}

impl AgentLockSnapshot {
    /// The `file-tracking.getAgentLocks` result shape (§5.19) — also the
    /// event payload minus `workspaceId`.
    pub(crate) fn to_result_value(&self) -> serde_json::Value {
        serde_json::json!({
            "autoCommitEnabled": self.auto_commit_enabled,
            "lockedAgentIds": self.locked_agent_ids,
            "lockedFilePaths": self.locked_file_paths,
        })
    }
}

/// Whether the session is mid-turn (the same running set as the §5.5 retire
/// guard): `pending` / `active` / legacy `Processing`.
fn is_running_turn(status: AgentStatus) -> bool {
    matches!(
        status,
        AgentStatus::Pending | AgentStatus::Active | AgentStatus::Processing
    )
}

/// Whether `session` counts as actively working for lock purposes (FE
/// `isAgentActivelyWorking` parity): running a turn, or linked to a task note
/// whose status is not terminal (`complete`/`cancelled`). Retired and deleted
/// sessions never lock — they cannot run again.
async fn is_actively_working(services: &Services, session: &AgentSession) -> bool {
    if session.retired_at.is_some() || session.status == AgentStatus::Deleted {
        return false;
    }
    if is_running_turn(session.status) {
        return true;
    }
    let Some(note_id) = session.task_note_id.as_ref() else {
        return false;
    };
    let Ok(note) = services
        .store()
        .get_note(&session.workspace_id, note_id)
        .await
    else {
        return false;
    };
    match note.metadata.task.as_ref() {
        Some(task) => !matches!(task.status, TaskStatus::Complete | TaskStatus::Cancelled),
        None => false,
    }
}

/// Build the self-sufficient `changes:agent-locks` event (§6.5).
fn changes_agent_locks_event(workspace_id: &WorkspaceId, snap: &AgentLockSnapshot) -> NewEvent {
    NewEvent {
        workspace_id: workspace_id.clone(),
        timestamp: now_iso(),
        event_type: CHANGES_AGENT_LOCKS.to_string(),
        actor: system_actor(),
        session_id: None,
        correlation_id: None,
        parent_event_id: None,
        metadata: None,
        data: serde_json::json!({
            "workspaceId": workspace_id.as_str(),
            "autoCommitEnabled": snap.auto_commit_enabled,
            "lockedAgentIds": snap.locked_agent_ids,
            "lockedFilePaths": snap.locked_file_paths,
        }),
    }
}

impl Services {
    /// Compute the current agent-lock snapshot for `workspace_id`. Store
    /// failures degrade to an empty (unlocked) snapshot — locking is a UI
    /// guard, never worth failing a read over.
    pub(crate) async fn compute_agent_locks(
        &self,
        workspace_id: &WorkspaceId,
    ) -> AgentLockSnapshot {
        let auto_commit_enabled = self.effective_auto_commit(workspace_id).await;
        let empty = AgentLockSnapshot {
            auto_commit_enabled,
            locked_agent_ids: Vec::new(),
            locked_file_paths: Vec::new(),
        };
        if !auto_commit_enabled {
            return empty;
        }
        let Ok(rows) = self.store().list_tracked_changes(workspace_id).await else {
            return empty;
        };
        // Group working-stage (unstaged/staged) rows by owning agent.
        let mut paths_by_agent: BTreeMap<String, BTreeSet<String>> = BTreeMap::new();
        for row in rows {
            if row.stage != "unstaged" && row.stage != "staged" {
                continue;
            }
            let Some(agent_id) = row.agent_id else {
                continue;
            };
            paths_by_agent.entry(agent_id).or_default().insert(row.path);
        }
        if paths_by_agent.is_empty() {
            return empty;
        }
        let Ok(sessions) = self
            .store()
            .list_agent_session_summaries(workspace_id)
            .await
        else {
            return empty;
        };
        let by_id: HashMap<&str, &AgentSession> =
            sessions.iter().map(|s| (s.id.as_str(), s)).collect();
        let mut locked_agent_ids: BTreeSet<String> = BTreeSet::new();
        let mut locked_file_paths: BTreeSet<String> = BTreeSet::new();
        for (agent_id, paths) in &paths_by_agent {
            let Some(session) = by_id.get(agent_id.as_str()) else {
                continue;
            };
            if is_actively_working(self, session).await {
                locked_agent_ids.insert(agent_id.clone());
                locked_file_paths.extend(paths.iter().cloned());
            }
        }
        AgentLockSnapshot {
            auto_commit_enabled,
            locked_agent_ids: locked_agent_ids.into_iter().collect(),
            locked_file_paths: locked_file_paths.into_iter().collect(),
        }
    }

    /// Recompute `workspace_id`'s lock snapshot and publish
    /// `changes:agent-locks` iff it differs from `last` (the previously
    /// published snapshot, if any). Returns the fresh snapshot.
    pub(crate) async fn refresh_agent_locks(
        &self,
        workspace_id: &WorkspaceId,
        last: Option<&AgentLockSnapshot>,
    ) -> AgentLockSnapshot {
        let snap = self.compute_agent_locks(workspace_id).await;
        if last != Some(&snap) {
            publish_event(
                self.event_bus.as_ref(),
                changes_agent_locks_event(workspace_id, &snap),
            )
            .await;
        }
        snap
    }

    /// Spawn the agent-locks subscriber loop: watch the agent-lifecycle,
    /// task-status, auto-commit-policy, and tracked-change event families and
    /// re-publish each affected workspace's `changes:agent-locks` snapshot
    /// when it changes. No-op without an event bus.
    pub fn spawn_agent_locks_loop(&self) -> tokio::task::JoinHandle<()> {
        let Some(bus) = self.event_bus.clone() else {
            tracing::info!("agent-locks loop disabled: no event bus");
            return tokio::spawn(async {});
        };
        let services = self.clone();
        tokio::spawn(async move {
            let filter = SubscriptionFilter {
                event_types: vec![
                    AGENT_STARTED.to_string(),
                    AGENT_IDLE.to_string(),
                    AGENT_STATUS_CHANGED.to_string(),
                    AGENT_COMPLETED.to_string(),
                    AGENT_FAILED.to_string(),
                    AGENT_DELETED.to_string(),
                    // Soft retire/restore flip `retired_at` (which
                    // `is_actively_working` keys off) and emit ONLY these
                    // events — no status-changed rides along (§5.5).
                    AGENT_RETIRED.to_string(),
                    AGENT_RESTORED.to_string(),
                    TASK_STATUS_CHANGED.to_string(),
                    WORKSPACE_UPDATED.to_string(),
                    SETTINGS_CHANGED.to_string(),
                    CHANGES_GIT_STATUS.to_string(),
                    CHANGES_METRICS_CHANGED.to_string(),
                ],
                batch_window: Some(RECOMPUTE_BATCH_WINDOW),
                ..Default::default()
            };
            let mut sub = bus.subscribe(filter);
            // Last published snapshot per workspace: the diff baseline. A
            // fresh daemon publishes each workspace's first non-trivial
            // snapshot (clients hydrate via the read anyway).
            let mut last: HashMap<String, AgentLockSnapshot> = HashMap::new();
            while let Some(events) = sub.recv().await {
                let mut dirty: BTreeSet<String> = BTreeSet::new();
                let mut all_workspaces = false;
                for event in events {
                    match event.event_type.as_str() {
                        // Only the autoCommitEnabled delta can move locks;
                        // skip the high-churn lastActivity-only updates.
                        WORKSPACE_UPDATED => {
                            if event
                                .data
                                .get("changes")
                                .and_then(|c| c.get("autoCommitEnabled"))
                                .is_some()
                            {
                                dirty.insert(event.workspace_id.as_str().to_string());
                            }
                        }
                        // Global settings carry no workspace id; only the
                        // git.autoCommit path affects locks — everywhere.
                        SETTINGS_CHANGED => {
                            let touched = event
                                .data
                                .get("changes")
                                .and_then(|c| c.as_array())
                                .is_some_and(|arr| {
                                    arr.iter().any(|ch| {
                                        ch.get("path").and_then(|p| p.as_str())
                                            == Some("git.autoCommit")
                                    })
                                });
                            if touched {
                                all_workspaces = true;
                            }
                        }
                        _ => {
                            dirty.insert(event.workspace_id.as_str().to_string());
                        }
                    }
                }
                if all_workspaces {
                    if let Ok(list) = services.store().list_workspaces(false).await {
                        dirty.extend(list.into_iter().map(|w| w.id.as_str().to_string()));
                    }
                }
                for ws in dirty {
                    if ws.is_empty() {
                        continue;
                    }
                    let ws_id = WorkspaceId::from_string(ws.clone());
                    let snap = services.refresh_agent_locks(&ws_id, last.get(&ws)).await;
                    last.insert(ws, snap);
                }
            }
        })
    }
}

#[cfg(test)]
mod tests;
