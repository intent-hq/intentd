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

use std::sync::Arc;

use intent_store::PersistedDelegationGroup;

// Use `tokio::time::Instant` (not `std::time::Instant`) for the cleanup
// deadline: Tokio timers/instants follow Tokio's time source while
// `std::time::Instant` always reads real time; mixing them makes deadline
// checks incorrect in paused-time tests (see `tokio::time::pause`).
use tokio::time::Instant;

use intent_core::{now_iso, AgentId, Error, Event, Result, WorkspaceId};
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
    /// SUB-2: monotonic cleanup deadline for the leak-guard timer. Set (and
    /// bumped) by [`Services::bump_watch_cleanup_deadline`]; a spawned cleanup
    /// task only removes the watch once this instant is in the past, so an
    /// earlier-scheduled timer cannot delete a watch whose deadline was
    /// extended by a later `spawn_watch_cleanup` call. `None` means "no
    /// timed cleanup is armed" (the default for one-shot watches, which are
    /// removed on delivery instead).
    pub cleanup_deadline: Option<Instant>,
    /// Report-time wake: set to `true` when `agent.reportToParent` delivers
    /// the parent wake immediately. When `true`, `deliver_completion_to_watches`
    /// skips delivery for `agent:idle` (suppressing the duplicate wake) but
    /// still delivers for `agent:failed` / `agent:deleted` (failure after
    /// reporting is a new signal, not a duplicate).
    pub report_delivered: bool,
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
    /// Source completion events recorded per child (in the same order as
    /// `event_summaries`), retained so the aggregated wake carries the FE
    /// `event_notification` metadata (per-event `id`, `type`, `data`,
    /// `timestamp`, `actor`) alongside the human-readable summary text.
    /// Held as `Arc<Event>` so snapshot clones of `DelegationGroup` for
    /// `agent.getSubscriptions` / `agent.diagnostics` stay cheap.
    pub raw_events: Vec<Arc<Event>>,
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
            cleanup_deadline: None,
            report_delivered: false,
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

    /// SUB-2 (Copilot #104 follow-up, thread PRRT_kwDOS9Wxuc6QKPyt):
    /// atomically find a live ungrouped (immediate-mode) watch for the given
    /// caller→target pair whose `one_shot` mode matches `one_shot` and, while
    /// still holding the registry lock, refresh its stored `parent_agent_name`
    /// to `new_parent_name`. Returns the live subscription id iff a matching
    /// watch was found — so a concurrent removal by
    /// [`Services::deliver_completion_to_watches`] (oneShot cleanup) or by an
    /// expired [`Services::spawn_watch_cleanup`] task cannot land between the
    /// find and the refresh and leave `agent.wakeOrCreate` returning a "reused"
    /// subscription id that no longer exists. Callers must fall through to
    /// [`Services::register_completion_watch`] when this returns `None`.
    ///
    /// Grouped (`after_all`) watches are skipped since they are owned by the
    /// delegation-group fan-in. The `one_shot` filter ensures a queued wake
    /// (which needs a non-oneShot watch to survive the current `agent:idle`)
    /// never reuses a oneShot watch, and vice versa. Refreshing the stored
    /// name keeps `agent.getSubscriptions` / [`describe_subscription`] in sync
    /// with any rename applied via `agent.rename` / `agent.update` since the
    /// watch was registered; a no-op when the name is already current.
    ///
    /// `new_parent_name` is `None` when the caller's current display name
    /// could not be resolved (e.g. `store.get_agent_session` failed under
    /// contention, Copilot #104 thread PRRT_kwDOS9Wxuc6QKWuU): the reuse and
    /// the paired deadline bump still proceed, but the existing stored name
    /// is left intact rather than overwritten with an empty placeholder that
    /// would degrade `agent.getSubscriptions` / `describe_subscription`
    /// output.
    pub(crate) fn find_and_refresh_ungrouped_watch(
        &self,
        workspace_id: &WorkspaceId,
        parent_agent_id: &AgentId,
        child_agent_id: &AgentId,
        one_shot: bool,
        new_parent_name: Option<String>,
    ) -> Option<String> {
        let mut guard = self
            .agent_subscriptions
            .lock()
            .expect("agent subscription registry poisoned");
        let w = guard.get_mut(workspace_id)?;
        let watch = w.subscriptions.iter_mut().find(|s| {
            s.group_id.is_none()
                && s.one_shot == one_shot
                && &s.parent_agent_id == parent_agent_id
                && &s.child_agent_id == child_agent_id
        })?;
        if let Some(new_name) = new_parent_name {
            if watch.parent_agent_name != new_name {
                watch.parent_agent_name = new_name;
            }
        }
        Some(watch.id.clone())
    }

    /// SUB-2: monotonically bump a watch's cleanup deadline to at least
    /// `new_deadline` (never shortens). Returns whether the watch was found.
    /// Paired with [`Services::remove_watch_if_deadline_passed`] so that a
    /// stale cleanup task spawned by an earlier call can no-op when a later
    /// call has extended the deadline past its wake-up time.
    pub(crate) fn bump_watch_cleanup_deadline(
        &self,
        workspace_id: &WorkspaceId,
        subscription_id: &str,
        new_deadline: Instant,
    ) -> bool {
        let mut guard = self
            .agent_subscriptions
            .lock()
            .expect("agent subscription registry poisoned");
        let Some(w) = guard.get_mut(workspace_id) else {
            return false;
        };
        let Some(watch) = w.subscriptions.iter_mut().find(|s| s.id == subscription_id) else {
            return false;
        };
        watch.cleanup_deadline = Some(match watch.cleanup_deadline {
            Some(existing) => existing.max(new_deadline),
            None => new_deadline,
        });
        true
    }

    /// SUB-2: atomically remove the watch iff its `cleanup_deadline` is set
    /// and has already elapsed. Returns whether a removal happened. Called
    /// by the cleanup task spawned in [`Services::spawn_watch_cleanup`]; a
    /// task that wakes before the current deadline is a no-op and the later
    /// task (spawned for the extended deadline) performs the removal.
    pub(crate) fn remove_watch_if_deadline_passed(
        &self,
        workspace_id: &WorkspaceId,
        subscription_id: &str,
    ) -> bool {
        let now = Instant::now();
        let mut guard = self
            .agent_subscriptions
            .lock()
            .expect("agent subscription registry poisoned");
        let Some(w) = guard.get_mut(workspace_id) else {
            return false;
        };
        let Some(idx) = w.subscriptions.iter().position(|s| s.id == subscription_id) else {
            return false;
        };
        match w.subscriptions[idx].cleanup_deadline {
            Some(deadline) if deadline <= now => {
                w.subscriptions.remove(idx);
                true
            }
            _ => false,
        }
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

    /// Mark a watch as having delivered the report wake (report-time wake).
    /// When marked, `deliver_completion_to_watches` will skip delivery for
    /// `agent:idle` but still deliver for `agent:failed` / `agent:deleted`.
    pub(crate) fn mark_watch_report_delivered(
        &self,
        workspace_id: &WorkspaceId,
        subscription_id: &str,
    ) -> bool {
        let mut guard = self
            .agent_subscriptions
            .lock()
            .expect("agent subscription registry poisoned");
        let Some(w) = guard.get_mut(workspace_id) else {
            return false;
        };
        if let Some(watch) = w.subscriptions.iter_mut().find(|s| s.id == subscription_id) {
            watch.report_delivered = true;
            true
        } else {
            false
        }
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
        let group = DelegationGroup {
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
            raw_events: Vec::new(),
        };
        entry.delegation_groups.push(group.clone());
        drop(guard);
        // Write-through persist (best-effort).
        self.persist_delegation_group(workspace_id, &group);
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
        let group_clone = if let Some(g) = w
            .delegation_groups
            .iter_mut()
            .find(|g| g.group_id == group_id)
        {
            if !g.expected_agent_ids.contains(child_id) {
                g.expected_agent_ids.push(child_id.clone());
            }
            Some(g.clone())
        } else {
            None
        };
        drop(guard);
        // Write-through persist (best-effort).
        if let Some(g) = group_clone {
            self.persist_delegation_group(workspace_id, &g);
        }
    }

    /// Seal the parent's open group (its delegating turn ended, so the expected
    /// set is final); returns the sealed group id, or `None` if none was open.
    ///
    /// DURABILITY: Awaits the persist before returning so the sealed flag is durable
    /// before the caller proceeds (fixes race where daemon kill between seal and
    /// spawned persist loses the sealed state across restart).
    pub(crate) async fn seal_group_for_parent(
        &self,
        workspace_id: &WorkspaceId,
        parent_id: &AgentId,
    ) -> Option<String> {
        let (group_id, group_clone) = {
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
            let group_id = g.group_id.clone();
            let group_clone = g.clone();
            (group_id, group_clone)
        }; // guard is dropped here automatically
           // Durable write-through persist: await the write so the sealed flag is
           // persisted before the caller continues.
        let persisted = match delegation_group_to_persisted(workspace_id, &group_clone) {
            Ok(p) => p,
            Err(e) => {
                tracing::warn!("skip delegation_group persist {}: {e}", group_id);
                return Some(group_id);
            }
        };
        if let Err(e) = self.store.upsert_delegation_group(&persisted).await {
            tracing::warn!("delegation_group upsert failed {}: {e}", group_id);
        }
        Some(group_id)
    }

    /// Whether `child_id` is enrolled in an undelivered `after_all` delegation
    /// group parented by `parent_id`. Retained for tests and future callers;
    /// the immediate-mode `reportToParent` suppression has moved to SUB-2
    /// (the child's `agent:idle` drives the single wake, so grouped children
    /// no longer need the pre-persist branch this predicate used to gate).
    #[allow(dead_code)]
    pub(crate) fn child_in_undelivered_group(
        &self,
        workspace_id: &WorkspaceId,
        parent_id: &AgentId,
        child_id: &AgentId,
    ) -> bool {
        self.agent_subscriptions
            .lock()
            .expect("agent subscription registry poisoned")
            .get(workspace_id)
            .map(|w| {
                w.delegation_groups.iter().any(|g| {
                    &g.parent_agent_id == parent_id
                        && !g.delivered
                        && g.expected_agent_ids.contains(child_id)
                })
            })
            .unwrap_or(false)
    }

    /// Record one child's completion in its group (idempotent): adds it to the
    /// completed or deleted set, pushes a summary line, and retains the source
    /// event for the aggregated wake's `event_notification` metadata. No-ops if
    /// the child is not expected or already recorded, or if the group no longer
    /// exists.
    ///
    /// DURABILITY: Awaits the persist before returning so the completion is durable
    /// before the event is observable (fixes race where daemon kill between event
    /// publish and spawned persist loses the completion across restart).
    pub(crate) async fn record_group_child_completion(
        &self,
        workspace_id: &WorkspaceId,
        group_id: &str,
        child_id: &AgentId,
        deleted: bool,
        summary: String,
        event: Event,
    ) {
        let group_clone = {
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
                    return;
                }
                if g.completed_agent_ids.contains(child_id)
                    || g.deleted_agent_ids.contains(child_id)
                {
                    return;
                }
                if deleted {
                    g.deleted_agent_ids.push(child_id.clone());
                } else {
                    g.completed_agent_ids.push(child_id.clone());
                }
                g.event_summaries.push(summary);
                g.raw_events.push(Arc::new(event));
                Some(g.clone())
            } else {
                None
            }
        }; // guard is dropped here automatically
           // Durable write-through persist: await the write so the completion is
           // persisted before the caller continues / before the event is observable.
        if let Some(g) = group_clone {
            let persisted = match delegation_group_to_persisted(workspace_id, &g) {
                Ok(p) => p,
                Err(e) => {
                    tracing::warn!("skip delegation_group persist {group_id}: {e}");
                    return;
                }
            };
            if let Err(e) = self.store.upsert_delegation_group(&persisted).await {
                tracing::warn!("delegation_group upsert failed {group_id}: {e}");
            }
        }
    }

    /// Claim a group for delivery if sealed, complete, and not yet delivered.
    /// Flips `delivered` in memory, removes from in-memory table, triggers
    /// best-effort async DB delete, and returns a clone. Returns `None` otherwise.
    ///
    /// DURABLE-BEFORE-OBSERVABLE: delete the delegation-group row from the DB before
    /// returning it for wake delivery. This ensures crash-safety:
    ///
    /// - Crash BEFORE delete commits: row still present → rehydration restores the
    ///   group and re-delivers the wake (correct: wake was never observable).
    /// - Crash AFTER delete commits: row absent → rehydration skips it, no re-delivery
    ///   (correct: wake already delivered, or about to be).
    ///
    /// The synchronous delete before publish prevents double-wake.
    pub(crate) async fn take_group_if_ready(
        &self,
        workspace_id: &WorkspaceId,
        group_id: &str,
    ) -> Option<DelegationGroup> {
        // Inside the lock: check readiness and remove from in-memory map.
        let group = {
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
            w.delegation_groups.remove(idx)
        }; // Drop guard before await
           // DURABLE-BEFORE-OBSERVABLE: delete the row synchronously before returning.
           // If the delete FAILS, do NOT deliver the wake — put the group back into the
           // in-memory map (delivered=false) and return None. The next child-completion
           // or restart retry will attempt the delete again. This makes delete-commit
           // strictly precede observability in ALL paths.
        if let Err(e) = self.store.delete_delegation_group(group_id).await {
            tracing::warn!(
                "Failed to delete delegation_group row {} (workspace {}): {}. \
                 Wake NOT delivered; group restored to memory for retry.",
                group_id,
                workspace_id.0,
                e
            );
            // Restore the group to the in-memory map for retry
            let mut guard = self
                .agent_subscriptions
                .lock()
                .expect("agent subscription registry poisoned");
            if let Some(w) = guard.get_mut(workspace_id) {
                w.delegation_groups.push(group);
            }
            return None;
        }
        // Delete committed → safe to deliver the wake
        Some(group)
    }

    /// Settle a delivered group's watches in one atomic registry pass: every
    /// completion watch carrying `group_id` is dropped, EXCEPT that watches on
    /// children listed in `retain_children` are converted in place into
    /// ungrouped oneShot watches (STAB-129: failed-not-deleted members may
    /// still be working, and their eventual real settlement must keep a wake
    /// path to the parent). Conversion dedupes against a live ungrouped
    /// oneShot watch for the same parent→child pair (e.g. one created by a
    /// SUB-1 sendToTask auto-watch racing settlement) so the late settlement
    /// delivers exactly one wake. Returns the number of watches retained.
    pub(crate) fn settle_group_watches(
        &self,
        workspace_id: &WorkspaceId,
        group_id: &str,
        retain_children: &[AgentId],
    ) -> usize {
        let mut guard = self
            .agent_subscriptions
            .lock()
            .expect("agent subscription registry poisoned");
        let Some(w) = guard.get_mut(workspace_id) else {
            return 0;
        };
        let mut kept: std::collections::HashSet<(AgentId, AgentId)> = w
            .subscriptions
            .iter()
            .filter(|s| s.group_id.is_none() && s.one_shot)
            .map(|s| (s.parent_agent_id.clone(), s.child_agent_id.clone()))
            .collect();
        let mut retained = 0;
        w.subscriptions.retain_mut(|s| {
            if s.group_id.as_deref() != Some(group_id) {
                return true;
            }
            if retain_children.contains(&s.child_agent_id) {
                let pair = (s.parent_agent_id.clone(), s.child_agent_id.clone());
                if kept.insert(pair) {
                    s.group_id = None;
                    s.one_shot = true;
                    retained += 1;
                    return true;
                }
            }
            false
        });
        retained
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

    /// Every completion watch registered in the workspace (the `agent.diagnostics`
    /// workspace-wide subscription view).
    pub(crate) fn all_watches(&self, workspace_id: &WorkspaceId) -> Vec<CompletionWatch> {
        self.agent_subscriptions
            .lock()
            .expect("agent subscription registry poisoned")
            .get(workspace_id)
            .map(|w| w.subscriptions.clone())
            .unwrap_or_default()
    }

    /// Every delegation group in the workspace (the `agent.diagnostics`
    /// workspace-wide delegation-group view).
    pub(crate) fn all_groups(&self, workspace_id: &WorkspaceId) -> Vec<DelegationGroup> {
        self.agent_subscriptions
            .lock()
            .expect("agent subscription registry poisoned")
            .get(workspace_id)
            .map(|w| w.delegation_groups.clone())
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

    /// Best-effort write-through persist of a delegation group (AS-2 persistence).
    ///
    /// Spawns async persist task, **not** durable-before-observable. A crash between
    /// group creation and commit loses the persisted row, preventing restoration on
    /// the next startup. This is acceptable: the crash window is milliseconds, and
    /// the parent agent can re-delegate if needed. Consistency requirement applies
    /// to **agent completions** (must persist before `agent:idle` event), not group
    /// creation.
    fn persist_delegation_group(&self, workspace_id: &WorkspaceId, group: &DelegationGroup) {
        let store = self.store.clone();
        let workspace_id = workspace_id.clone();
        let group_id = group.group_id.clone();
        let persisted = match delegation_group_to_persisted(&workspace_id, group) {
            Ok(p) => p,
            Err(e) => {
                tracing::warn!("skip delegation_group persist {group_id}: {e}");
                return;
            }
        };
        tokio::spawn(async move {
            if let Err(e) = store.upsert_delegation_group(&persisted).await {
                tracing::warn!("delegation_group upsert failed {group_id}: {e}");
            }
        });
    }

    /// Rehydrate undelivered delegation groups on resume (AS-2 rehydration).
    /// Idempotent: skips groups already present in memory (by group_id).
    ///
    /// STAB-108 FIX: Reconciles each rehydrated group against current agent state.
    /// If an expected child is already idle/completed (or deleted/missing), records
    /// its completion using the persisted completion_report, then fires ready groups.
    pub(crate) async fn rehydrate_delegation_groups(
        &self,
        workspace_id: &WorkspaceId,
    ) -> Result<usize> {
        let persisted = self.store.list_undelivered_groups(workspace_id).await?;
        let (loaded, groups_to_reconcile) = {
            let mut guard = self
                .agent_subscriptions
                .lock()
                .expect("agent subscription registry poisoned");
            let entry = guard.entry(workspace_id.clone()).or_default();
            let mut loaded = 0;
            let mut groups_to_reconcile = Vec::new();
            for p in persisted {
                // Skip if this group is already in memory (idempotent rehydration).
                if entry
                    .delegation_groups
                    .iter()
                    .any(|g| g.group_id == p.group_id)
                {
                    continue;
                }
                // Groups are sealed on rehydration (original parent turn is gone).
                let mut group = persisted_to_delegation_group(&p)?;
                group.sealed = true;
                groups_to_reconcile.push(group.group_id.clone());
                entry.delegation_groups.push(group);
                loaded += 1;
            }
            (loaded, groups_to_reconcile)
        }; // guard dropped here

        // STAB-108 reconciliation: check each rehydrated group for already-completed children
        for group_id in groups_to_reconcile {
            self.reconcile_group_on_rehydration(workspace_id, &group_id)
                .await;
            // Fire the group if it's now ready (all children completed/deleted)
            self.try_fire_group(workspace_id, &group_id).await;
        }
        Ok(loaded)
    }

    /// STAB-108: Reconcile a delegation group against current agent state after rehydration.
    /// For each expected child not already in completed_agent_ids or deleted_agent_ids,
    /// check if the agent session is idle/completed (or deleted/missing). If so, record
    /// its completion using the persisted completion_report.
    async fn reconcile_group_on_rehydration(&self, workspace_id: &WorkspaceId, group_id: &str) {
        // Get the list of agents to check (expected but not yet recorded as complete/deleted)
        let agents_to_check = {
            let guard = self
                .agent_subscriptions
                .lock()
                .expect("agent subscription registry poisoned");
            let Some(w) = guard.get(workspace_id) else {
                return;
            };
            let Some(g) = w.delegation_groups.iter().find(|g| g.group_id == group_id) else {
                return;
            };
            g.expected_agent_ids
                .iter()
                .filter(|id| {
                    !g.completed_agent_ids.contains(id) && !g.deleted_agent_ids.contains(id)
                })
                .cloned()
                .collect::<Vec<_>>()
        };

        // For each unrecorded child, check its status and record if complete/deleted
        for child_id in agents_to_check {
            // Check agent status
            let agent_result = self.store.get_agent_session(&child_id).await;

            match agent_result {
                Ok(session) => {
                    use intent_core::AgentStatus;

                    // Conservative completion predicate (STAB-108):
                    // - If status is Completed, child is done
                    // - If status is Deleted, child is done
                    // - If status is Error, child is done (terminal failure)
                    // - If status is RuntimeIdle:
                    //   * AND completion_report is present
                    //   * AND there is NO interrupted_agent row
                    //   → then the child is genuinely complete
                    // - Otherwise, skip (child may be interrupted/healing)

                    let is_deleted = matches!(session.status, AgentStatus::Deleted);
                    let is_explicitly_completed = matches!(session.status, AgentStatus::Completed);
                    let is_failed = matches!(session.status, AgentStatus::Error);

                    let is_idle_and_genuinely_complete = if matches!(
                        session.status,
                        AgentStatus::RuntimeIdle
                    ) {
                        // Check if there's a completion report and no interrupted row
                        let has_completion_report = session.completion_report.is_some();
                        let interrupted_check = self.store.get_interrupted_agent(&child_id).await;

                        match interrupted_check {
                            Ok(opt) => has_completion_report && opt.is_none(),
                            Err(e) => {
                                // On store error, skip this child (don't mark complete)
                                tracing::warn!(
                                        "Skipping RuntimeIdle child {} due to interrupted_agent store error: {e}",
                                        child_id.0
                                    );
                                false
                            }
                        }
                    } else {
                        false
                    };

                    let should_record = is_deleted
                        || is_explicitly_completed
                        || is_failed
                        || is_idle_and_genuinely_complete;

                    if should_record {
                        // Build a synthetic agent:idle, agent:failed, or agent:deleted event
                        // Prefer the child's persisted completion_report when present
                        let report = session.completion_report;
                        let event_type = if is_deleted {
                            intent_core::events::AGENT_DELETED
                        } else if is_failed {
                            intent_core::events::AGENT_FAILED
                        } else {
                            intent_core::events::AGENT_IDLE
                        };
                        let event = Event {
                            id: uuid::Uuid::new_v4().to_string(),
                            workspace_id: workspace_id.clone(),
                            timestamp: now_iso(),
                            event_type: event_type.to_string(),
                            actor: intent_core::EventActor {
                                actor_type: intent_core::ActorType::Agent,
                                id: Some(child_id.0.clone()),
                                ..Default::default()
                            },
                            session_id: Some(child_id.0.clone()),
                            correlation_id: None,
                            parent_event_id: None,
                            metadata: None,
                            data: serde_json::json!({
                                "agentId": child_id.0,
                                "status": serde_json::to_value(session.status).unwrap_or_default(),
                            }),
                        };
                        let summary =
                            crate::format_group_child_line(&child_id, &event, report.as_deref());

                        // Record the completion
                        self.record_group_child_completion(
                            workspace_id,
                            group_id,
                            &child_id,
                            is_deleted,
                            summary,
                            event,
                        )
                        .await;
                    }
                }
                Err(e) => {
                    // Only NotFound → deleted; other errors → log and skip
                    if matches!(e, intent_store::Error::NotFound(_)) {
                        let event = Event {
                            id: uuid::Uuid::new_v4().to_string(),
                            workspace_id: workspace_id.clone(),
                            timestamp: now_iso(),
                            event_type: intent_core::events::AGENT_DELETED.to_string(),
                            actor: intent_core::EventActor {
                                actor_type: intent_core::ActorType::Agent,
                                id: Some(child_id.0.clone()),
                                ..Default::default()
                            },
                            session_id: Some(child_id.0.clone()),
                            correlation_id: None,
                            parent_event_id: None,
                            metadata: None,
                            data: serde_json::json!({
                                "agentId": child_id.0,
                                "status": "deleted",
                            }),
                        };
                        let summary = crate::format_group_child_line(&child_id, &event, None);

                        self.record_group_child_completion(
                            workspace_id,
                            group_id,
                            &child_id,
                            true, // deleted
                            summary,
                            event,
                        )
                        .await;
                    } else {
                        tracing::warn!(
                            "Skipping reconciliation for child {} due to store error: {e}",
                            child_id.0
                        );
                    }
                }
            }
        }
    }

    /// DURABLE-BEFORE-OBSERVABLE helper: if `agent_id` is in a delegation group,
    /// record its completion BEFORE the idle event is published. This ensures the
    /// persisted state is correct if the daemon is killed immediately after the
    /// event becomes observable. Called from the agent_session worker loop right
    /// before publishing `agent:idle`.
    pub(crate) async fn record_group_completion_pre_publish(
        &self,
        workspace_id: &WorkspaceId,
        agent_id: &AgentId,
        event_data: &serde_json::Value,
    ) {
        // Find which group (if any) this agent belongs to
        let group_id = {
            let guard = self
                .agent_subscriptions
                .lock()
                .expect("agent subscription registry poisoned");
            guard.get(workspace_id).and_then(|w| {
                w.delegation_groups
                    .iter()
                    .find(|g| g.expected_agent_ids.contains(agent_id))
                    .map(|g| g.group_id.clone())
            })
        };

        if let Some(group_id) = group_id {
            // Build event for group recording. Prefer the child's persisted
            // completion_report (set by agent.reportToParent) over the generic
            // summary, mirroring deliver_completion_to_watches logic.
            let report = self
                .store
                .get_agent_session(agent_id)
                .await
                .ok()
                .and_then(|s| s.completion_report);
            let event = Event {
                id: String::new(),
                workspace_id: workspace_id.clone(),
                timestamp: now_iso(),
                event_type: intent_core::events::AGENT_IDLE.to_string(),
                actor: intent_core::EventActor {
                    actor_type: intent_core::ActorType::Agent,
                    id: Some(agent_id.0.clone()),
                    ..Default::default()
                },
                session_id: Some(agent_id.0.clone()),
                correlation_id: None,
                parent_event_id: None,
                metadata: None,
                data: event_data.clone(),
            };
            let summary = crate::format_group_child_line(agent_id, &event, report.as_deref());

            self.record_group_child_completion(
                workspace_id,
                &group_id,
                agent_id,
                false, // not deleted
                summary,
                event,
            )
            .await;
        }
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

/// Convert in-memory `DelegationGroup` to persisted form.
fn delegation_group_to_persisted(
    workspace_id: &WorkspaceId,
    group: &DelegationGroup,
) -> Result<PersistedDelegationGroup> {
    let raw_events_json: Vec<String> = group
        .raw_events
        .iter()
        .map(|e| {
            serde_json::to_string(e.as_ref())
                .map_err(|err| Error::Internal(format!("serialize raw_event: {err}")))
        })
        .collect::<Result<Vec<_>>>()?;

    Ok(PersistedDelegationGroup {
        group_id: group.group_id.clone(),
        workspace_id: workspace_id.clone(),
        parent_agent_id: group.parent_agent_id.clone(),
        await_mode: group.await_mode.clone(),
        expected_agent_ids: group.expected_agent_ids.clone(),
        completed_agent_ids: group.completed_agent_ids.clone(),
        deleted_agent_ids: group.deleted_agent_ids.clone(),
        sealed: group.sealed,
        delivered: group.delivered,
        event_summaries: group.event_summaries.clone(),
        raw_events_json,
        created_at: now_iso(),
        updated_at: now_iso(),
    })
}

/// Convert persisted `PersistedDelegationGroup` back to in-memory form.
fn persisted_to_delegation_group(p: &PersistedDelegationGroup) -> Result<DelegationGroup> {
    let raw_events: Vec<Arc<Event>> = p
        .raw_events_json
        .iter()
        .map(|s| {
            serde_json::from_str::<Event>(s)
                .map(Arc::new)
                .map_err(|err| Error::Internal(format!("deserialize raw_event: {err}")))
        })
        .collect::<Result<Vec<_>>>()?;

    Ok(DelegationGroup {
        group_id: p.group_id.clone(),
        parent_agent_id: p.parent_agent_id.clone(),
        await_mode: p.await_mode.clone(),
        expected_agent_ids: p.expected_agent_ids.clone(),
        completed_agent_ids: p.completed_agent_ids.clone(),
        deleted_agent_ids: p.deleted_agent_ids.clone(),
        subscription_id: None,
        sealed: p.sealed,
        delivered: p.delivered,
        event_summaries: p.event_summaries.clone(),
        raw_events,
    })
}
