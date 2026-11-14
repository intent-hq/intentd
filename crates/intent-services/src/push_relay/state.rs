//! Pure policy for the push publisher: the published workspace facts, the
//! notice decisions (the app's alert suppression rules) and the per-workspace
//! debounce.

use std::collections::HashMap;
use std::time::Duration;

use intent_core::{
    AgentId, AgentLite, Workspace, WorkspaceActivity, WorkspaceAttention, WorkspaceDisplayStatus,
    WorkspaceId,
};
use serde::Serialize;
use tokio::time::Instant;

pub(crate) const DEBOUNCE: Duration = Duration::from_secs(1);
const MAX_TITLE: usize = 300;
const MAX_TEXT: usize = 1000;

#[derive(Debug, Clone, Copy, PartialEq, Eq, Serialize)]
#[serde(rename_all = "snake_case")]
pub(crate) enum NoticeKind {
    Done,
    NeedsAttention,
    ReviewRequired,
}

#[derive(Debug, Clone, PartialEq, Eq, Serialize)]
#[serde(rename_all = "camelCase")]
pub(crate) struct Notice {
    pub id: String,
    pub kind: NoticeKind,
    pub agent_id: Option<String>,
    pub agent_name: Option<String>,
    pub at: String,
}

/// `state` in `PUT /v1/daemons/:daemonId/workspaces/:workspaceId`. Never
/// carries note or transcript content.
#[derive(Debug, Clone, PartialEq, Eq, Serialize)]
#[serde(rename_all = "camelCase")]
pub(crate) struct WorkspaceState {
    pub title: String,
    pub status_message: Option<String>,
    pub running: bool,
    pub display_status: Option<String>,
    pub attention: Option<String>,
    pub agent_id: Option<String>,
    pub agent_name: Option<String>,
    pub last_activity: String,
    pub notice: Option<Notice>,
}

/// The fields notice transitions compare between two observations.
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub(crate) struct Facts {
    pub attention: WorkspaceAttention,
    pub display_status: Option<WorkspaceDisplayStatus>,
}

impl Facts {
    pub(crate) fn of(ws: &Workspace) -> Self {
        Self {
            attention: ws.attention,
            display_status: ws.display_status,
        }
    }
}

fn truncate(text: &str, max: usize) -> String {
    text.chars().take(max).collect()
}

fn snake<T: Serialize>(value: &T) -> Option<String> {
    serde_json::to_value(value)
        .ok()
        .and_then(|v| v.as_str().map(str::to_string))
}

/// The app's `AgentSession.isActive`: a live turn or any streaming flag.
pub(crate) fn agent_is_active(agent: &AgentLite) -> bool {
    agent.is_streaming
        || agent.is_processing
        || agent.is_responding
        || agent.status.is_running_turn()
}

/// Whether a workspace is published at all; anything else publishes
/// `{state: null}`.
pub(crate) fn is_published(ws: &Workspace) -> bool {
    !ws.id.is_chief() && !ws.archived && !ws.is_retired()
}

/// The workspace's published facts. `agents` may be empty when the caller
/// skipped the read (no running agent).
pub(crate) fn build_state(ws: &Workspace, agents: &[AgentLite], now: &str) -> WorkspaceState {
    let title = ws.title.trim();
    let title = if title.is_empty() {
        ws.branch.trim()
    } else {
        title
    };
    let agent = agents
        .iter()
        .filter(|a| agent_is_active(a))
        .max_by_key(|a| {
            (
                !a.metadata.is_background,
                a.last_activity.as_deref().unwrap_or(""),
            )
        });
    let last_activity = ws
        .last_activity
        .as_deref()
        .filter(|s| !s.is_empty())
        .or(Some(ws.updated_at.as_str()).filter(|s| !s.is_empty()))
        .unwrap_or(now);
    WorkspaceState {
        title: truncate(
            if title.is_empty() { "Workspace" } else { title },
            MAX_TITLE,
        ),
        status_message: ws
            .status_message
            .as_deref()
            .map(str::trim)
            .filter(|s| !s.is_empty())
            .map(|s| truncate(s, MAX_TEXT)),
        running: ws.activity == WorkspaceActivity::AgentRunning,
        display_status: ws.display_status.as_ref().and_then(snake),
        attention: match ws.attention {
            WorkspaceAttention::None => None,
            other => snake(&other),
        },
        agent_id: agent.map(|a| a.id.0.clone()),
        agent_name: agent.map(|a| truncate(&a.name, MAX_TITLE)),
        last_activity: last_activity.to_string(),
        notice: None,
    }
}

/// A notice from an attention or display-status transition. The first
/// observation of a workspace is a baseline and never notifies.
pub(crate) fn transition_notice(prev: Option<Facts>, next: Facts) -> Option<NoticeKind> {
    let prev = prev?;
    if next.attention == WorkspaceAttention::ReviewRequired
        && prev.attention != WorkspaceAttention::ReviewRequired
    {
        return Some(NoticeKind::ReviewRequired);
    }
    let needs = |s: Option<WorkspaceDisplayStatus>| {
        matches!(
            s,
            Some(WorkspaceDisplayStatus::NeedsAttention | WorkspaceDisplayStatus::Blocked)
        )
    };
    (needs(next.display_status) && !needs(prev.display_status))
        .then_some(NoticeKind::NeedsAttention)
}

/// The app's idle-alert suppression (`shouldSuppressIdleAlert`), inverted:
/// skip background and muted agents and agents waiting on sub-agents, and
/// stay quiet while another unmuted agent in the workspace is still active.
pub(crate) fn idle_alert_allowed(idle: &AgentId, agents: &[AgentLite]) -> bool {
    if let Some(agent) = agents.iter().find(|a| &a.id == idle) {
        if agent.metadata.is_background
            || agent.notifications_muted
            || agent.is_waiting_for_other_agents
            || !agent.waiting_for_agent_ids.is_empty()
        {
            return false;
        }
    }
    !agents
        .iter()
        .any(|a| &a.id != idle && agent_is_active(a) && !a.notifications_muted)
}

/// An agent that went idle, kept until its workspace flushes.
#[derive(Debug, Clone, PartialEq, Eq)]
pub(crate) struct IdleAgent {
    pub id: AgentId,
    pub name: Option<String>,
}

/// The notice to attach: a transition wins over a completion.
pub(crate) fn choose_notice(
    transition: Option<NoticeKind>,
    idle: &[IdleAgent],
    agents: &[AgentLite],
) -> Option<(NoticeKind, Option<IdleAgent>)> {
    if let Some(kind) = transition {
        return Some((kind, None));
    }
    idle.iter()
        .rev()
        .find(|a| idle_alert_allowed(&a.id, agents))
        .map(|a| {
            let name = a.name.clone().or_else(|| {
                agents
                    .iter()
                    .find(|row| row.id == a.id)
                    .map(|row| row.name.clone())
            });
            (
                NoticeKind::Done,
                Some(IdleAgent {
                    id: a.id.clone(),
                    name,
                }),
            )
        })
}

/// Per-workspace coalescing: the first change opens a one-second window;
/// later changes in the window join it, so a busy workspace still publishes
/// once a second.
#[derive(Debug, Default)]
pub(crate) struct Pending {
    entries: HashMap<WorkspaceId, (Instant, Vec<IdleAgent>)>,
}

impl Pending {
    pub(crate) fn note(&mut self, workspace: WorkspaceId, now: Instant, idle: Option<IdleAgent>) {
        let entry = self
            .entries
            .entry(workspace)
            .or_insert_with(|| (now + DEBOUNCE, Vec::new()));
        if let Some(agent) = idle {
            entry.1.retain(|a| a.id != agent.id);
            entry.1.push(agent);
        }
    }

    pub(crate) fn next_due(&self) -> Option<Instant> {
        self.entries.values().map(|(due, _)| *due).min()
    }

    pub(crate) fn take_due(&mut self, now: Instant) -> Vec<(WorkspaceId, Vec<IdleAgent>)> {
        let due: Vec<WorkspaceId> = self
            .entries
            .iter()
            .filter(|(_, (at, _))| *at <= now)
            .map(|(id, _)| id.clone())
            .collect();
        due.into_iter()
            .filter_map(|id| self.entries.remove(&id).map(|(_, idle)| (id, idle)))
            .collect()
    }

    pub(crate) fn clear(&mut self) {
        self.entries.clear();
    }
}
