//! Daemon-owned parent→child completion-watch registry (AS-2).
//!
//! In-memory state, keyed by workspace, recording which parent agents are
//! watching which child agents for completion. A oneShot watch is registered
//! when an agent delegates with `waitMode` `immediate` (default) over the MCP
//! front door; the delivery worker that fires on child completion lands in AS-3
//! and the `after_all` delegation-group fan-in lands in AS-4.
//!
//! Mirrors the TS `subscribeCallerToAgentCompletion` / `agentSubscribe` shape
//! (oneShot, `actorIds: [child]`, AGENT completion event set
//! `['agent:idle','agent:failed','agent:deleted']`). The event-type wiring is an
//! AS-3 concern; this module only owns the registry records and helpers.

use intent_core::{now_iso, AgentId, WorkspaceId};
use uuid::Uuid;

use crate::Services;

/// One parent→child completion-watch record. A `one_shot` watch is removed once
/// the child's completion has been delivered to the parent (AS-3).
// Fields are read by the AS-3 delivery worker and by tests; AS-2 only populates
// the registry, so the lib-only build sees them as unread.
#[allow(dead_code)]
#[derive(Debug, Clone)]
pub(crate) struct CompletionWatch {
    pub id: String,
    pub parent_agent_id: AgentId,
    pub parent_agent_name: String,
    pub child_agent_id: AgentId,
    pub one_shot: bool,
    pub group_id: Option<String>,
    pub created_at: String,
}

/// Placeholder fan-in table for `waitMode: "after_all"` delegation groups.
///
// TODO(AS-4): wire the after_all delegation-group logic — track expected vs.
// completed vs. deleted child agents and fire a single grouped delivery once the
// whole group settles. AS-2 defines the record shape only; nothing populates or
// consumes this table yet.
#[allow(dead_code)]
#[derive(Debug, Clone)]
pub(crate) struct DelegationGroup {
    pub group_id: String,
    pub parent_agent_id: AgentId,
    pub await_mode: String,
    pub expected_agent_ids: Vec<AgentId>,
    pub completed_agent_ids: Vec<AgentId>,
    pub deleted_agent_ids: Vec<AgentId>,
    pub subscription_id: Option<String>,
    pub delivered: bool,
}

/// Per-workspace registry state held behind the `Services` mutex.
#[derive(Debug, Default)]
pub(crate) struct WorkspaceWatches {
    pub subscriptions: Vec<CompletionWatch>,
    // TODO(AS-4): populated/consumed by the after_all delegation-group logic.
    #[allow(dead_code)]
    pub delegation_groups: Vec<DelegationGroup>,
}

impl Services {
    /// Register a parent→child completion watch and return its subscription id.
    pub(crate) fn register_completion_watch(
        &self,
        workspace_id: &WorkspaceId,
        parent_agent_id: AgentId,
        parent_agent_name: String,
        child_agent_id: AgentId,
        one_shot: bool,
        group_id: Option<String>,
    ) -> String {
        let id = Uuid::new_v4().to_string();
        let watch = CompletionWatch {
            id: id.clone(),
            parent_agent_id,
            parent_agent_name,
            child_agent_id,
            one_shot,
            group_id,
            created_at: now_iso(),
        };
        self.agent_subscriptions
            .lock()
            .expect("agent subscription registry poisoned")
            .entry(workspace_id.clone())
            .or_default()
            .subscriptions
            .push(watch);
        id
    }

    /// All watches whose `child_agent_id` matches (the AS-3 delivery lookup).
    // TODO(AS-3): consumed by the completion-delivery worker.
    #[allow(dead_code)]
    pub(crate) fn find_watches_for_child(
        &self,
        workspace_id: &WorkspaceId,
        child_agent_id: &AgentId,
    ) -> Vec<CompletionWatch> {
        self.agent_subscriptions
            .lock()
            .expect("agent subscription registry poisoned")
            .get(workspace_id)
            .map(|w| {
                w.subscriptions
                    .iter()
                    .filter(|s| &s.child_agent_id == child_agent_id)
                    .cloned()
                    .collect()
            })
            .unwrap_or_default()
    }

    /// All watches registered by `parent_agent_id`.
    // TODO(AS-3/AS-4): consumed by `agent.getSubscriptions` + delivery/cleanup.
    #[allow(dead_code)]
    pub(crate) fn list_watches_for_parent(
        &self,
        workspace_id: &WorkspaceId,
        parent_agent_id: &AgentId,
    ) -> Vec<CompletionWatch> {
        self.agent_subscriptions
            .lock()
            .expect("agent subscription registry poisoned")
            .get(workspace_id)
            .map(|w| {
                w.subscriptions
                    .iter()
                    .filter(|s| &s.parent_agent_id == parent_agent_id)
                    .cloned()
                    .collect()
            })
            .unwrap_or_default()
    }

    /// Remove a single watch by subscription id; returns whether one was found.
    // TODO(AS-3): oneShot cleanup after a completion is delivered.
    #[allow(dead_code)]
    pub(crate) fn remove_watch(&self, workspace_id: &WorkspaceId, subscription_id: &str) -> bool {
        let mut guard = self
            .agent_subscriptions
            .lock()
            .expect("agent subscription registry poisoned");
        let Some(w) = guard.get_mut(workspace_id) else {
            return false;
        };
        let before = w.subscriptions.len();
        w.subscriptions.retain(|s| s.id != subscription_id);
        w.subscriptions.len() != before
    }

    /// Remove every watch registered by `parent_agent_id`; returns the count.
    // TODO(AS-3): `agent.cancelSubscriptions` + parent-deletion cleanup.
    #[allow(dead_code)]
    pub(crate) fn remove_all_for_parent(
        &self,
        workspace_id: &WorkspaceId,
        parent_agent_id: &AgentId,
    ) -> usize {
        let mut guard = self
            .agent_subscriptions
            .lock()
            .expect("agent subscription registry poisoned");
        let Some(w) = guard.get_mut(workspace_id) else {
            return 0;
        };
        let before = w.subscriptions.len();
        w.subscriptions
            .retain(|s| &s.parent_agent_id != parent_agent_id);
        before - w.subscriptions.len()
    }
}
