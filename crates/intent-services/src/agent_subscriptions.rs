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

/// Fan-in table for `waitMode: "after_all"` delegation groups. All children a
/// parent delegates with `after_all` share one open group; it fires a single
/// aggregated wake to the parent once it is sealed (the parent went idle, so the
/// expected set is final) and every expected child has completed or been deleted.
#[derive(Debug, Clone)]
pub(crate) struct DelegationGroup {
    pub group_id: String,
    pub parent_agent_id: AgentId,
    // Retained for parity with the TS group shape; not read by the fan-in.
    #[allow(dead_code)]
    pub await_mode: String,
    pub expected_agent_ids: Vec<AgentId>,
    pub completed_agent_ids: Vec<AgentId>,
    pub deleted_agent_ids: Vec<AgentId>,
    // Retained for parity with the TS group shape; not read by the fan-in.
    #[allow(dead_code)]
    pub subscription_id: Option<String>,
    pub sealed: bool,
    pub delivered: bool,
    pub event_summaries: Vec<String>,
}

/// Per-workspace registry state held behind the `Services` mutex.
#[derive(Debug, Default)]
pub(crate) struct WorkspaceWatches {
    pub subscriptions: Vec<CompletionWatch>,
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

    /// Return the open (unsealed && undelivered) delegation group for `parent_id`,
    /// creating a fresh one if none exists. All `after_all` children delegated by
    /// the same parent turn share this group; a sealed/delivered group is never
    /// reused, so a later turn opens a new one.
    pub(crate) fn get_or_create_delegation_group(
        &self,
        workspace_id: &WorkspaceId,
        parent_id: &AgentId,
    ) -> String {
        let mut guard = self
            .agent_subscriptions
            .lock()
            .expect("agent subscription registry poisoned");
        let entry = guard.entry(workspace_id.clone()).or_default();
        if let Some(g) = entry
            .delegation_groups
            .iter()
            .find(|g| &g.parent_agent_id == parent_id && !g.sealed && !g.delivered)
        {
            return g.group_id.clone();
        }
        let group_id = Uuid::new_v4().to_string();
        entry.delegation_groups.push(DelegationGroup {
            group_id: group_id.clone(),
            parent_agent_id: parent_id.clone(),
            await_mode: "after_all".to_string(),
            expected_agent_ids: Vec::new(),
            completed_agent_ids: Vec::new(),
            deleted_agent_ids: Vec::new(),
            subscription_id: None,
            sealed: false,
            delivered: false,
            event_summaries: Vec::new(),
        });
        group_id
    }

    /// Add `child_id` to a group's expected set (idempotent).
    pub(crate) fn enroll_child_in_group(
        &self,
        workspace_id: &WorkspaceId,
        group_id: &str,
        child_id: &AgentId,
    ) {
        let mut guard = self
            .agent_subscriptions
            .lock()
            .expect("agent subscription registry poisoned");
        let Some(w) = guard.get_mut(workspace_id) else {
            return;
        };
        if let Some(g) = w
            .delegation_groups
            .iter_mut()
            .find(|g| g.group_id == group_id)
        {
            if !g.expected_agent_ids.contains(child_id) {
                g.expected_agent_ids.push(child_id.clone());
            }
        }
    }

    /// Seal the parent's open group (its delegating turn ended, so the expected
    /// set is final); returns the sealed group id, or `None` if none was open.
    pub(crate) fn seal_group_for_parent(
        &self,
        workspace_id: &WorkspaceId,
        parent_id: &AgentId,
    ) -> Option<String> {
        let mut guard = self
            .agent_subscriptions
            .lock()
            .expect("agent subscription registry poisoned");
        let w = guard.get_mut(workspace_id)?;
        let g = w
            .delegation_groups
            .iter_mut()
            .find(|g| &g.parent_agent_id == parent_id && !g.sealed && !g.delivered)?;
        g.sealed = true;
        Some(g.group_id.clone())
    }

    /// Record one child's completion in its group (idempotent): adds it to the
    /// completed or deleted set and pushes a summary line. No-ops if the child is
    /// not expected or already recorded, or if the group no longer exists.
    pub(crate) fn record_group_child_completion(
        &self,
        workspace_id: &WorkspaceId,
        group_id: &str,
        child_id: &AgentId,
        deleted: bool,
        summary: String,
    ) {
        let mut guard = self
            .agent_subscriptions
            .lock()
            .expect("agent subscription registry poisoned");
        let Some(w) = guard.get_mut(workspace_id) else {
            return;
        };
        let Some(g) = w
            .delegation_groups
            .iter_mut()
            .find(|g| g.group_id == group_id)
        else {
            return;
        };
        if !g.expected_agent_ids.contains(child_id) {
            return;
        }
        if g.completed_agent_ids.contains(child_id) || g.deleted_agent_ids.contains(child_id) {
            return;
        }
        if deleted {
            g.deleted_agent_ids.push(child_id.clone());
        } else {
            g.completed_agent_ids.push(child_id.clone());
        }
        g.event_summaries.push(summary);
    }

    /// Atomically claim a group for delivery if it is sealed, complete, and not
    /// yet delivered: flips `delivered`, removes it from the table, and returns a
    /// clone. Returns `None` otherwise, so the aggregated wake fires exactly once.
    pub(crate) fn take_group_if_ready(
        &self,
        workspace_id: &WorkspaceId,
        group_id: &str,
    ) -> Option<DelegationGroup> {
        let mut guard = self
            .agent_subscriptions
            .lock()
            .expect("agent subscription registry poisoned");
        let w = guard.get_mut(workspace_id)?;
        let idx = w
            .delegation_groups
            .iter()
            .position(|g| g.group_id == group_id)?;
        if !(w.delegation_groups[idx].sealed
            && !w.delegation_groups[idx].delivered
            && is_group_complete(&w.delegation_groups[idx]))
        {
            return None;
        }
        let mut group = w.delegation_groups.remove(idx);
        group.delivered = true;
        Some(group)
    }

    /// Drop every completion watch carrying `group_id`; returns the count removed.
    pub(crate) fn remove_group_watches(&self, workspace_id: &WorkspaceId, group_id: &str) -> usize {
        let mut guard = self
            .agent_subscriptions
            .lock()
            .expect("agent subscription registry poisoned");
        let Some(w) = guard.get_mut(workspace_id) else {
            return 0;
        };
        let before = w.subscriptions.len();
        w.subscriptions
            .retain(|s| s.group_id.as_deref() != Some(group_id));
        before - w.subscriptions.len()
    }

    /// All delegation groups parented by `parent_id` (read snapshot for
    /// `agent.getSubscriptions`). Mirrors `delegation_group_for_parent` but
    /// returns every group rather than the first match.
    pub(crate) fn list_groups_for_parent(
        &self,
        workspace_id: &WorkspaceId,
        parent_id: &AgentId,
    ) -> Vec<DelegationGroup> {
        self.agent_subscriptions
            .lock()
            .expect("agent subscription registry poisoned")
            .get(workspace_id)
            .map(|w| {
                w.delegation_groups
                    .iter()
                    .filter(|g| &g.parent_agent_id == parent_id)
                    .cloned()
                    .collect()
            })
            .unwrap_or_default()
    }

    /// Drop every delegation group parented by `parent_id`; returns the count
    /// removed (the group side of `agent.cancelSubscriptions`).
    pub(crate) fn remove_groups_for_parent(
        &self,
        workspace_id: &WorkspaceId,
        parent_id: &AgentId,
    ) -> usize {
        let mut guard = self
            .agent_subscriptions
            .lock()
            .expect("agent subscription registry poisoned");
        let Some(w) = guard.get_mut(workspace_id) else {
            return 0;
        };
        let before = w.delegation_groups.len();
        w.delegation_groups
            .retain(|g| &g.parent_agent_id != parent_id);
        before - w.delegation_groups.len()
    }

    /// Test-only snapshot of a parent's delegation group, if one exists.
    #[cfg(test)]
    pub(crate) fn delegation_group_for_parent(
        &self,
        workspace_id: &WorkspaceId,
        parent_id: &AgentId,
    ) -> Option<DelegationGroup> {
        self.agent_subscriptions
            .lock()
            .expect("agent subscription registry poisoned")
            .get(workspace_id)
            .and_then(|w| {
                w.delegation_groups
                    .iter()
                    .find(|g| &g.parent_agent_id == parent_id)
                    .cloned()
            })
    }
}

/// A group is complete when it has at least one expected child and every
/// expected child is in the completed or deleted set.
fn is_group_complete(group: &DelegationGroup) -> bool {
    !group.expected_agent_ids.is_empty()
        && group.expected_agent_ids.iter().all(|id| {
            group.completed_agent_ids.contains(id) || group.deleted_agent_ids.contains(id)
        })
}
