//! Daemon-owned parent→child completion-watch registry (AS-2).
//!
//! One daemon-global in-memory registry (not keyed by workspace) recording
//! which parent agents are watching which child agents for completion. Every
//! record carries the workspaces it spans: a watch knows the parent's HOME
//! workspace (where the wake is delivered) and the child's workspace (where
//! the completion event fires); a delegation group is anchored in the PARENT's
//! home workspace. For same-workspace delegation the two coincide and behavior
//! is identical to the old per-workspace map; a chief-workspace parent can
//! watch children in any workspace through the exact same code path.
//!
//! Safety gate: non-chief parents may only watch children in their own
//! workspace — enforced in [`Services::register_completion_watch`], the single
//! shared registration path, not per-caller.
//!
//! A oneShot watch is registered when an agent delegates with `waitMode`
//! `immediate` (default) over the MCP front door; the delivery worker that
//! fires on child completion lands in AS-3 and the `after_all`
//! delegation-group fan-in lands in AS-4.
//!
//! Mirrors the TS `subscribeCallerToAgentCompletion` / `agentSubscribe` shape
//! (oneShot, `actorIds: [child]`, AGENT completion event set
//! `['agent:idle','agent:failed','agent:deleted']`). The event-type wiring is an
//! AS-3 concern; this module only owns the registry records and helpers.

use std::sync::Arc;

use intent_store::{PersistedCompletionWatch, PersistedDelegationGroup};

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
    /// The parent's HOME workspace — where every wake for this watch is
    /// delivered (and where `agent:subscriptions-changed` is published). For
    /// same-workspace delegation this equals `child_workspace_id`; for a
    /// chief-workspace parent it is `__chief__`.
    pub parent_workspace_id: WorkspaceId,
    /// The child's workspace — where its completion events
    /// (`agent:idle`/`agent:failed`/`agent:deleted`) fire.
    pub child_workspace_id: WorkspaceId,
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
    /// The PARENT's home workspace — the group's anchor: where the aggregated
    /// wake is delivered, where `agent:subscriptions-changed` is published,
    /// and the `workspace_id` column the group persists/rehydrates under. For
    /// same-workspace delegation this is the delegating workspace (identical
    /// to the old per-workspace registry); for a chief parent it is
    /// `__chief__`.
    pub workspace_id: WorkspaceId,
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

/// Daemon-global registry state held behind the `Services` mutex. Watches and
/// groups from every workspace share this one table; each record carries its
/// own workspace anchors (see [`CompletionWatch`] / [`DelegationGroup`]).
#[derive(Debug, Default)]
pub(crate) struct SubscriptionRegistry {
    pub subscriptions: Vec<CompletionWatch>,
    pub delegation_groups: Vec<DelegationGroup>,
}

/// The shared registration safety gate: a non-chief parent may only watch
/// children inside its own workspace; a chief-workspace parent may watch any
/// agent. Enforced in [`Services::register_completion_watch`] (the single
/// path every registration goes through), not per-caller; also exposed to
/// callers that must validate the pair BEFORE creating side-effectful state
/// (e.g. the `after_all` delegation group in `agent_delegate_op`).
pub(crate) fn check_watch_scope(
    parent_workspace_id: &WorkspaceId,
    child_workspace_id: &WorkspaceId,
) -> Result<()> {
    if parent_workspace_id != child_workspace_id && !parent_workspace_id.is_chief() {
        return Err(Error::InvalidParams(format!(
            "cross-workspace completion watch denied: parent in workspace {} may only \
             watch agents in its own workspace (child is in workspace {}); only \
             chief-workspace parents may watch agents in any workspace",
            parent_workspace_id.0, child_workspace_id.0
        )));
    }
    Ok(())
}

impl Services {
    /// Register a parent→child completion watch and return its subscription id.
    ///
    /// `parent_workspace_id` is the parent's home workspace (where wakes are
    /// delivered); `child_workspace_id` is where the child's completion events
    /// fire. Errs when the scope gate rejects the pair (non-chief parent
    /// watching a child outside its own workspace).
    #[allow(clippy::too_many_arguments)]
    pub(crate) fn register_completion_watch(
        &self,
        parent_workspace_id: &WorkspaceId,
        child_workspace_id: &WorkspaceId,
        parent_agent_id: AgentId,
        parent_agent_name: String,
        child_agent_id: AgentId,
        one_shot: bool,
        group_id: Option<String>,
    ) -> Result<String> {
        let watch = self.insert_watch_in_memory(
            parent_workspace_id,
            child_workspace_id,
            parent_agent_id,
            parent_agent_name,
            child_agent_id,
            one_shot,
            group_id,
        )?;
        // Write-through persist (best-effort) so the watch survives a daemon
        // restart (rehydrated by `heal_completion_watches_on_startup`).
        let id = watch.id.clone();
        self.persist_completion_watch(&watch);
        Ok(id)
    }

    /// [`Services::register_completion_watch`] with an AWAITED persist:
    /// the row is committed before this returns. Required when the caller
    /// may deliver (and thus delete) the watch immediately after
    /// registration — e.g. `app.agents.waitFor`'s registration-time
    /// reconciliation of already-settled targets — where the spawned
    /// best-effort upsert could otherwise commit AFTER the fired watch's
    /// spawned delete and resurrect the row as an orphan (duplicate wake on
    /// the next restart). A failed persist only logs: the in-memory watch
    /// still fires live, matching the best-effort durability contract.
    #[allow(clippy::too_many_arguments)]
    pub(crate) async fn register_completion_watch_durable(
        &self,
        parent_workspace_id: &WorkspaceId,
        child_workspace_id: &WorkspaceId,
        parent_agent_id: AgentId,
        parent_agent_name: String,
        child_agent_id: AgentId,
        one_shot: bool,
        group_id: Option<String>,
    ) -> Result<String> {
        let watch = self.insert_watch_in_memory(
            parent_workspace_id,
            child_workspace_id,
            parent_agent_id,
            parent_agent_name,
            child_agent_id,
            one_shot,
            group_id,
        )?;
        let id = watch.id.clone();
        let persisted = completion_watch_to_persisted(&watch);
        if let Err(e) = self.store.upsert_completion_watch(&persisted).await {
            tracing::warn!("completion_watch upsert failed {id}: {e}");
        }
        Ok(id)
    }

    /// Shared body of the two registration variants: build the watch, run
    /// the scope gate, and push it into the in-memory registry.
    #[allow(clippy::too_many_arguments)]
    fn insert_watch_in_memory(
        &self,
        parent_workspace_id: &WorkspaceId,
        child_workspace_id: &WorkspaceId,
        parent_agent_id: AgentId,
        parent_agent_name: String,
        child_agent_id: AgentId,
        one_shot: bool,
        group_id: Option<String>,
    ) -> Result<CompletionWatch> {
        check_watch_scope(parent_workspace_id, child_workspace_id)?;
        let watch = CompletionWatch {
            id: Uuid::new_v4().to_string(),
            parent_workspace_id: parent_workspace_id.clone(),
            child_workspace_id: child_workspace_id.clone(),
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
            .subscriptions
            .push(watch.clone());
        // monorepo#840: a fresh watch expresses fresh interest — drop any
        // stale failure-wake dedup record for this pair so the next failure
        // (even with unchanged error text) reaches the new watcher. Dedup
        // then only suppresses replays BETWEEN registrations.
        self.clear_failure_wake_dedup_pair(&watch.parent_agent_id, &watch.child_agent_id);
        Ok(watch)
    }

    /// All watches whose `child_agent_id` matches (the AS-3 delivery lookup),
    /// regardless of workspace — the same lookup serves same-workspace and
    /// cross-workspace (chief) watches.
    pub(crate) fn find_watches_for_child(&self, child_agent_id: &AgentId) -> Vec<CompletionWatch> {
        self.agent_subscriptions
            .lock()
            .expect("agent subscription registry poisoned")
            .subscriptions
            .iter()
            .filter(|s| &s.child_agent_id == child_agent_id)
            .cloned()
            .collect()
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
    ///
    /// `resolved_parent_workspace_id` is `Some` only when the caller resolved
    /// the parent's home workspace from an actual session row (never from a
    /// call-workspace fallback): a watch originally registered with fallback
    /// anchors (a transient `get_agent_session` failure) has its
    /// `parent_workspace_id` corrected on reuse, so subsequent wakes and
    /// `agent:subscriptions-changed` land in the parent's true home
    /// workspace. Refreshing only the parent anchor cannot violate the scope
    /// gate for existing valid records — it either fixes a fallback anchor to
    /// the true home or is a no-op.
    pub(crate) fn find_and_refresh_ungrouped_watch(
        &self,
        parent_agent_id: &AgentId,
        child_agent_id: &AgentId,
        one_shot: bool,
        new_parent_name: Option<String>,
        resolved_parent_workspace_id: Option<&WorkspaceId>,
    ) -> Option<String> {
        let (id, name, home_ws, changed) = {
            let mut guard = self
                .agent_subscriptions
                .lock()
                .expect("agent subscription registry poisoned");
            let watch = guard.subscriptions.iter_mut().find(|s| {
                s.group_id.is_none()
                    && s.one_shot == one_shot
                    && &s.parent_agent_id == parent_agent_id
                    && &s.child_agent_id == child_agent_id
            })?;
            let mut changed = false;
            if let Some(new_name) = new_parent_name {
                if watch.parent_agent_name != new_name {
                    watch.parent_agent_name = new_name;
                    changed = true;
                }
            }
            if let Some(home_ws) = resolved_parent_workspace_id {
                if &watch.parent_workspace_id != home_ws
                    && check_watch_scope(home_ws, &watch.child_workspace_id).is_ok()
                {
                    watch.parent_workspace_id = home_ws.clone();
                    changed = true;
                }
            }
            (
                watch.id.clone(),
                watch.parent_agent_name.clone(),
                watch.parent_workspace_id.clone(),
                changed,
            )
        };
        // Best-effort DB sync of the refreshed name/anchor (restart
        // durability), skipped when nothing changed (the common
        // waitFor-called-twice case). This is a spawned UPDATE ... WHERE id,
        // so racing a concurrent fire/delete is benign: against a deleted
        // row it affects 0 rows (it cannot resurrect an orphan); the only
        // loss is the refreshed name/anchor not persisting — the in-memory
        // watch is already refreshed and the row is gone anyway.
        if changed {
            let store = self.store.clone();
            let watch_id = id.clone();
            tokio::spawn(async move {
                if let Err(e) = store
                    .update_completion_watch_parent(&watch_id, &name, &home_ws)
                    .await
                {
                    tracing::warn!("completion_watch parent refresh failed {watch_id}: {e}");
                }
            });
        }
        Some(id)
    }

    /// SUB-2: monotonically bump a watch's cleanup deadline to at least
    /// `new_deadline` (never shortens). Returns whether the watch was found.
    /// Paired with [`Services::remove_watch_if_deadline_passed`] so that a
    /// stale cleanup task spawned by an earlier call can no-op when a later
    /// call has extended the deadline past its wake-up time.
    pub(crate) fn bump_watch_cleanup_deadline(
        &self,
        subscription_id: &str,
        new_deadline: Instant,
    ) -> bool {
        let bumped = {
            let mut guard = self
                .agent_subscriptions
                .lock()
                .expect("agent subscription registry poisoned");
            let Some(watch) = guard
                .subscriptions
                .iter_mut()
                .find(|s| s.id == subscription_id)
            else {
                return false;
            };
            watch.cleanup_deadline = Some(match watch.cleanup_deadline {
                Some(existing) => existing.max(new_deadline),
                None => new_deadline,
            });
            watch.cleanup_deadline
        };
        // Best-effort DB sync of the leak-guard deadline as wall-clock epoch
        // ms so a rehydrated watch re-arms with the remaining time.
        if let Some(deadline) = bumped {
            let store = self.store.clone();
            let watch_id = subscription_id.to_string();
            let deadline_at_ms = instant_to_epoch_ms(deadline);
            tokio::spawn(async move {
                if let Err(e) = store
                    .set_completion_watch_deadline(&watch_id, deadline_at_ms)
                    .await
                {
                    tracing::warn!("completion_watch deadline sync failed {watch_id}: {e}");
                }
            });
        }
        true
    }

    /// SUB-2: atomically remove the watch iff its `cleanup_deadline` is set
    /// and has already elapsed. Returns whether a removal happened. Called
    /// by the cleanup task spawned in [`Services::spawn_watch_cleanup`]; a
    /// task that wakes before the current deadline is a no-op and the later
    /// task (spawned for the extended deadline) performs the removal.
    pub(crate) fn remove_watch_if_deadline_passed(&self, subscription_id: &str) -> bool {
        let now = Instant::now();
        let removed = {
            let mut guard = self
                .agent_subscriptions
                .lock()
                .expect("agent subscription registry poisoned");
            let Some(idx) = guard
                .subscriptions
                .iter()
                .position(|s| s.id == subscription_id)
            else {
                return false;
            };
            match guard.subscriptions[idx].cleanup_deadline {
                Some(deadline) if deadline <= now => {
                    guard.subscriptions.remove(idx);
                    true
                }
                _ => false,
            }
        };
        if removed {
            self.delete_persisted_watch(subscription_id);
        }
        removed
    }

    /// All watches registered by `parent_agent_id`, regardless of workspace
    /// (consumed by `agent.getSubscriptions` + delivery/cleanup).
    pub(crate) fn list_watches_for_parent(
        &self,
        parent_agent_id: &AgentId,
    ) -> Vec<CompletionWatch> {
        self.agent_subscriptions
            .lock()
            .expect("agent subscription registry poisoned")
            .subscriptions
            .iter()
            .filter(|s| &s.parent_agent_id == parent_agent_id)
            .cloned()
            .collect()
    }

    /// Remove a single watch by subscription id; returns whether one was found.
    pub(crate) fn remove_watch(&self, subscription_id: &str) -> bool {
        let removed = {
            let mut guard = self
                .agent_subscriptions
                .lock()
                .expect("agent subscription registry poisoned");
            let before = guard.subscriptions.len();
            guard.subscriptions.retain(|s| s.id != subscription_id);
            guard.subscriptions.len() != before
        };
        if removed {
            self.delete_persisted_watch(subscription_id);
        }
        removed
    }

    /// Mark a watch as having delivered the report wake (report-time wake).
    /// When marked, `deliver_completion_to_watches` will skip delivery for
    /// `agent:idle` but still deliver for `agent:failed` / `agent:deleted`.
    pub(crate) fn mark_watch_report_delivered(&self, subscription_id: &str) -> bool {
        let marked = {
            let mut guard = self
                .agent_subscriptions
                .lock()
                .expect("agent subscription registry poisoned");
            if let Some(watch) = guard
                .subscriptions
                .iter_mut()
                .find(|s| s.id == subscription_id)
            {
                watch.report_delivered = true;
                true
            } else {
                false
            }
        };
        if marked {
            // Best-effort DB sync so a rehydrated watch keeps suppressing the
            // duplicate agent:idle wake after a restart.
            let store = self.store.clone();
            let watch_id = subscription_id.to_string();
            tokio::spawn(async move {
                if let Err(e) = store
                    .mark_completion_watch_report_delivered(&watch_id)
                    .await
                {
                    tracing::warn!("completion_watch report_delivered sync failed {watch_id}: {e}");
                }
            });
        }
        marked
    }

    /// Remove every watch registered by `parent_agent_id`; returns the count
    /// (`agent.cancelSubscriptions` + parent-deletion cleanup).
    pub(crate) fn remove_all_for_parent(&self, parent_agent_id: &AgentId) -> usize {
        let removed = {
            let mut guard = self
                .agent_subscriptions
                .lock()
                .expect("agent subscription registry poisoned");
            let before = guard.subscriptions.len();
            guard
                .subscriptions
                .retain(|s| &s.parent_agent_id != parent_agent_id);
            before - guard.subscriptions.len()
        };
        if removed > 0 {
            // Best-effort DB sweep of every persisted watch for this parent.
            let store = self.store.clone();
            let parent = parent_agent_id.clone();
            tokio::spawn(async move {
                if let Err(e) = store.delete_completion_watches_for_parent(&parent).await {
                    tracing::warn!("completion_watch parent sweep failed {}: {e}", parent.0);
                }
            });
        }
        removed
    }

    /// Return the open (unsealed && undelivered) delegation group for `parent_id`,
    /// creating a fresh one if none exists. All `after_all` children delegated by
    /// the same parent turn share this group; a sealed/delivered group is never
    /// reused, so a later turn opens a new one. `parent_workspace_id` is the
    /// PARENT's home workspace — the group's anchor for wake delivery and
    /// persistence.
    pub(crate) fn get_or_create_delegation_group(
        &self,
        parent_workspace_id: &WorkspaceId,
        parent_id: &AgentId,
    ) -> String {
        let mut guard = self
            .agent_subscriptions
            .lock()
            .expect("agent subscription registry poisoned");
        if let Some(g) = guard
            .delegation_groups
            .iter()
            .find(|g| &g.parent_agent_id == parent_id && !g.sealed && !g.delivered)
        {
            return g.group_id.clone();
        }
        let group_id = Uuid::new_v4().to_string();
        let group = DelegationGroup {
            group_id: group_id.clone(),
            workspace_id: parent_workspace_id.clone(),
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
        guard.delegation_groups.push(group.clone());
        drop(guard);
        // Write-through persist (best-effort).
        self.persist_delegation_group(&group);
        group_id
    }

    /// Add `child_id` to a group's expected set (idempotent).
    pub(crate) fn enroll_child_in_group(&self, group_id: &str, child_id: &AgentId) {
        let mut guard = self
            .agent_subscriptions
            .lock()
            .expect("agent subscription registry poisoned");
        let group_clone = if let Some(g) = guard
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
            self.persist_delegation_group(&g);
        }
    }

    /// Seal the parent's open group (its delegating turn ended, so the expected
    /// set is final); returns the sealed group id, or `None` if none was open.
    ///
    /// DURABILITY: Awaits the persist before returning so the sealed flag is durable
    /// before the caller proceeds (fixes race where daemon kill between seal and
    /// spawned persist loses the sealed state across restart).
    pub(crate) async fn seal_group_for_parent(&self, parent_id: &AgentId) -> Option<String> {
        let (group_id, group_clone) = {
            let mut guard = self
                .agent_subscriptions
                .lock()
                .expect("agent subscription registry poisoned");
            let g = guard
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
        let persisted = match delegation_group_to_persisted(&group_clone) {
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
    pub(crate) fn child_in_undelivered_group(
        &self,
        parent_id: &AgentId,
        child_id: &AgentId,
    ) -> bool {
        self.agent_subscriptions
            .lock()
            .expect("agent subscription registry poisoned")
            .delegation_groups
            .iter()
            .any(|g| {
                &g.parent_agent_id == parent_id
                    && !g.delivered
                    && g.expected_agent_ids.contains(child_id)
            })
    }

    /// Record one child's completion in its group (idempotent): adds it to the
    /// completed or deleted set, pushes a summary line, and retains the source
    /// event for the aggregated wake's `event_notification` metadata. Returns
    /// `true` iff this call newly recorded the child in memory (STAB-160: the
    /// immediate failure-wake dedup guard keys off this); `false` when the
    /// child is not expected or already recorded, or the group no longer
    /// exists. The return reflects the in-memory recording only — it is still
    /// `true` when the best-effort persist below fails.
    ///
    /// DURABILITY: Awaits the persist before returning so the completion is durable
    /// before the event is observable (fixes race where daemon kill between event
    /// publish and spawned persist loses the completion across restart). The
    /// persist is best-effort: a serialization or upsert failure is logged and
    /// the in-memory recording stands (recovery across restart then relies on
    /// the STAB-108 rehydration reconciliation).
    pub(crate) async fn record_group_child_completion(
        &self,
        group_id: &str,
        child_id: &AgentId,
        deleted: bool,
        summary: String,
        event: Event,
    ) -> bool {
        let group_clone = {
            let mut guard = self
                .agent_subscriptions
                .lock()
                .expect("agent subscription registry poisoned");
            if let Some(g) = guard
                .delegation_groups
                .iter_mut()
                .find(|g| g.group_id == group_id)
            {
                if !g.expected_agent_ids.contains(child_id) {
                    return false;
                }
                if g.completed_agent_ids.contains(child_id)
                    || g.deleted_agent_ids.contains(child_id)
                {
                    return false;
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
            let persisted = match delegation_group_to_persisted(&g) {
                Ok(p) => p,
                Err(e) => {
                    tracing::warn!("skip delegation_group persist {group_id}: {e}");
                    return true;
                }
            };
            if let Err(e) = self.store.upsert_delegation_group(&persisted).await {
                tracing::warn!("delegation_group upsert failed {group_id}: {e}");
            }
            true
        } else {
            false
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
    pub(crate) async fn take_group_if_ready(&self, group_id: &str) -> Option<DelegationGroup> {
        // Inside the lock: check readiness and remove from in-memory table.
        let group = {
            let mut guard = self
                .agent_subscriptions
                .lock()
                .expect("agent subscription registry poisoned");
            let idx = guard
                .delegation_groups
                .iter()
                .position(|g| g.group_id == group_id)?;
            if !(guard.delegation_groups[idx].sealed
                && !guard.delegation_groups[idx].delivered
                && is_group_complete(&guard.delegation_groups[idx]))
            {
                return None;
            }
            guard.delegation_groups.remove(idx)
        }; // Drop guard before await
           // DURABLE-BEFORE-OBSERVABLE: delete the row synchronously before returning.
           // If the delete FAILS, do NOT deliver the wake — put the group back into the
           // in-memory table (delivered=false) and return None. The next child-completion
           // or restart retry will attempt the delete again. This makes delete-commit
           // strictly precede observability in ALL paths.
        if let Err(e) = self.store.delete_delegation_group(group_id).await {
            tracing::warn!(
                "Failed to delete delegation_group row {} (workspace {}): {}. \
                 Wake NOT delivered; group restored to memory for retry.",
                group_id,
                group.workspace_id.0,
                e
            );
            // Restore the group to the in-memory table for retry
            self.agent_subscriptions
                .lock()
                .expect("agent subscription registry poisoned")
                .delegation_groups
                .push(group);
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
    /// path to the parent). Conversion dedupes against any live ungrouped
    /// watch for the same parent→child pair — oneShot or not (e.g. a SUB-1
    /// sendToTask auto-watch or a queued non-oneShot `wakeOrCreate` watch
    /// racing settlement) — since either already gives the parent a wake path,
    /// so the late settlement delivers exactly one wake. Returns the number of
    /// watches retained.
    pub(crate) fn settle_group_watches(
        &self,
        group_id: &str,
        retain_children: &[AgentId],
    ) -> usize {
        let retain_set: std::collections::HashSet<&AgentId> = retain_children.iter().collect();
        let mut converted_ids: Vec<String> = Vec::new();
        let mut dropped_ids: Vec<String> = Vec::new();
        let retained = {
            let mut guard = self
                .agent_subscriptions
                .lock()
                .expect("agent subscription registry poisoned");
            let mut kept: std::collections::HashSet<(AgentId, AgentId)> = guard
                .subscriptions
                .iter()
                .filter(|s| s.group_id.is_none())
                .map(|s| (s.parent_agent_id.clone(), s.child_agent_id.clone()))
                .collect();
            let mut retained = 0;
            guard.subscriptions.retain_mut(|s| {
                if s.group_id.as_deref() != Some(group_id) {
                    return true;
                }
                if retain_set.contains(&s.child_agent_id) {
                    let pair = (s.parent_agent_id.clone(), s.child_agent_id.clone());
                    if kept.insert(pair) {
                        s.group_id = None;
                        s.one_shot = true;
                        converted_ids.push(s.id.clone());
                        retained += 1;
                        return true;
                    }
                }
                dropped_ids.push(s.id.clone());
                false
            });
            retained
        };
        // Best-effort DB sync: converted watches become ungrouped oneShot rows,
        // dropped watches lose their rows (restart durability).
        if !converted_ids.is_empty() || !dropped_ids.is_empty() {
            let store = self.store.clone();
            tokio::spawn(async move {
                for id in converted_ids {
                    if let Err(e) = store.ungroup_completion_watch(&id).await {
                        tracing::warn!("completion_watch ungroup failed {id}: {e}");
                    }
                }
                for id in dropped_ids {
                    if let Err(e) = store.delete_completion_watch(&id).await {
                        tracing::warn!("completion_watch delete failed {id}: {e}");
                    }
                }
            });
        }
        retained
    }

    /// All delegation groups parented by `parent_id` (read snapshot for
    /// `agent.getSubscriptions`), regardless of workspace. Mirrors
    /// `delegation_group_for_parent` but returns every group rather than the
    /// first match.
    pub(crate) fn list_groups_for_parent(&self, parent_id: &AgentId) -> Vec<DelegationGroup> {
        self.agent_subscriptions
            .lock()
            .expect("agent subscription registry poisoned")
            .delegation_groups
            .iter()
            .filter(|g| &g.parent_agent_id == parent_id)
            .cloned()
            .collect()
    }

    /// Every completion watch that touches the workspace — as the parent's
    /// home OR the child's workspace (the `agent.diagnostics` workspace-wide
    /// subscription view; for same-workspace watches this matches the old
    /// per-workspace snapshot exactly).
    pub(crate) fn all_watches(&self, workspace_id: &WorkspaceId) -> Vec<CompletionWatch> {
        self.agent_subscriptions
            .lock()
            .expect("agent subscription registry poisoned")
            .subscriptions
            .iter()
            .filter(|s| {
                &s.parent_workspace_id == workspace_id || &s.child_workspace_id == workspace_id
            })
            .cloned()
            .collect()
    }

    /// Every delegation group anchored in the workspace (the `agent.diagnostics`
    /// workspace-wide delegation-group view).
    pub(crate) fn all_groups(&self, workspace_id: &WorkspaceId) -> Vec<DelegationGroup> {
        self.agent_subscriptions
            .lock()
            .expect("agent subscription registry poisoned")
            .delegation_groups
            .iter()
            .filter(|g| &g.workspace_id == workspace_id)
            .cloned()
            .collect()
    }

    /// Drop every delegation group parented by `parent_id`; returns the count
    /// removed (the group side of `agent.cancelSubscriptions`).
    pub(crate) fn remove_groups_for_parent(&self, parent_id: &AgentId) -> usize {
        let mut guard = self
            .agent_subscriptions
            .lock()
            .expect("agent subscription registry poisoned");
        let before = guard.delegation_groups.len();
        guard
            .delegation_groups
            .retain(|g| &g.parent_agent_id != parent_id);
        before - guard.delegation_groups.len()
    }

    /// Test-only snapshot of a parent's delegation group, if one exists.
    #[cfg(test)]
    pub(crate) fn delegation_group_for_parent(
        &self,
        parent_id: &AgentId,
    ) -> Option<DelegationGroup> {
        self.agent_subscriptions
            .lock()
            .expect("agent subscription registry poisoned")
            .delegation_groups
            .iter()
            .find(|g| &g.parent_agent_id == parent_id)
            .cloned()
    }

    /// Best-effort write-through persist of a delegation group (AS-2 persistence).
    ///
    /// Spawns async persist task, **not** durable-before-observable. A crash between
    /// group creation and commit loses the persisted row, preventing restoration on
    /// the next startup. This is acceptable: the crash window is milliseconds, and
    /// the parent agent can re-delegate if needed. Consistency requirement applies
    /// to **agent completions** (must persist before `agent:idle` event), not group
    /// creation.
    fn persist_delegation_group(&self, group: &DelegationGroup) {
        let store = self.store.clone();
        let group_id = group.group_id.clone();
        let persisted = match delegation_group_to_persisted(group) {
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

    /// Best-effort write-through persist of a completion watch (restart
    /// durability). Mirrors [`Services::persist_delegation_group`]: spawns an
    /// async persist task, not durable-before-observable — the crash window
    /// between in-memory registration and commit is milliseconds and the
    /// parent can re-register.
    fn persist_completion_watch(&self, watch: &CompletionWatch) {
        let store = self.store.clone();
        let persisted = completion_watch_to_persisted(watch);
        tokio::spawn(async move {
            let id = persisted.id.clone();
            if let Err(e) = store.upsert_completion_watch(&persisted).await {
                tracing::warn!("completion_watch upsert failed {id}: {e}");
            }
        });
    }

    /// Best-effort async delete of a persisted completion-watch row (fired
    /// oneShot, cancellation, deadline expiry).
    fn delete_persisted_watch(&self, subscription_id: &str) {
        let store = self.store.clone();
        let id = subscription_id.to_string();
        tokio::spawn(async move {
            if let Err(e) = store.delete_completion_watch(&id).await {
                tracing::warn!("completion_watch delete failed {id}: {e}");
            }
        });
    }

    /// Rehydrate persisted completion watches at daemon startup: load every
    /// surviving row, prune rows whose PARENT agent is gone (deleted or
    /// missing — no wake could ever be delivered) and load the rest into the
    /// in-memory registry. A gone/deleted CHILD is NOT pruned: that is a
    /// completion signal for the parent, handled by the reconciliation pass
    /// below (synthetic `agent:deleted`). Grouped watches whose delegation
    /// group no longer exists in memory (it fired or was never rehydrated)
    /// are pruned too — group settlement owns their lifecycle — as are rows
    /// whose leak-guard deadline already elapsed. Idempotent: watches already
    /// present in memory (by id) are skipped.
    ///
    /// A rehydrated watch with a persisted leak-guard deadline re-arms its
    /// cleanup timer with the remaining wall-clock time (an already-elapsed
    /// deadline prunes the row instead). After loading, each watch's child is
    /// reconciled against current agent state: a child that completed while
    /// the daemon was down delivers its (synthetic) completion immediately,
    /// so the parent is not left waiting forever.
    pub async fn heal_completion_watches_on_startup(&self) -> Result<usize> {
        let persisted = self.store.list_completion_watches().await?;
        let mut loaded = 0usize;
        let mut to_reconcile: Vec<(AgentId, WorkspaceId)> = Vec::new();
        for p in persisted {
            // Prune when either endpoint is gone: no wake could fire (child
            // deleted watches are handled by reconciliation below instead,
            // since a deleted child IS a completion signal for the parent).
            let parent_alive = self.agent_is_live(&p.parent_agent_id).await;
            if !parent_alive {
                tracing::info!(
                    watch = %p.id,
                    parent = %p.parent_agent_id.0,
                    "pruning persisted completion watch — parent agent gone"
                );
                let _ = self.store.delete_completion_watch(&p.id).await;
                continue;
            }
            // Grouped watches belong to their delegation group's settlement;
            // if the group is gone from memory after group rehydration, the
            // group already fired (or its row was delivered) — prune.
            if let Some(gid) = &p.group_id {
                let group_live = {
                    let guard = self
                        .agent_subscriptions
                        .lock()
                        .expect("agent subscription registry poisoned");
                    guard.delegation_groups.iter().any(|g| &g.group_id == gid)
                };
                if !group_live {
                    tracing::info!(
                        watch = %p.id,
                        group = %gid,
                        "pruning persisted completion watch — delegation group gone"
                    );
                    let _ = self.store.delete_completion_watch(&p.id).await;
                    continue;
                }
            }
            // Expired leak-guard deadline: the cleanup timer would have
            // removed this watch already — prune instead of rehydrating.
            let remaining = match p.deadline_at_ms {
                Some(at_ms) => match remaining_from_epoch_ms(at_ms) {
                    Some(d) => Some(d),
                    None => {
                        tracing::info!(
                            watch = %p.id,
                            "pruning persisted completion watch — cleanup deadline elapsed"
                        );
                        let _ = self.store.delete_completion_watch(&p.id).await;
                        continue;
                    }
                },
                None => None,
            };
            let (watch_id, parent_ws, parent_agent, child_agent, child_ws) = {
                let mut guard = self
                    .agent_subscriptions
                    .lock()
                    .expect("agent subscription registry poisoned");
                if guard.subscriptions.iter().any(|s| s.id == p.id) {
                    continue;
                }
                let watch = persisted_to_completion_watch(&p);
                let ids = (
                    watch.id.clone(),
                    watch.parent_workspace_id.clone(),
                    watch.parent_agent_id.clone(),
                    watch.child_agent_id.clone(),
                    watch.child_workspace_id.clone(),
                );
                guard.subscriptions.push(watch);
                ids
            };
            loaded += 1;
            // Re-arm the leak-guard cleanup with the remaining wall-clock time.
            if let Some(after) = remaining {
                self.spawn_watch_cleanup(parent_ws, parent_agent, watch_id, after);
            }
            to_reconcile.push((child_agent, child_ws));
        }
        // Reconcile: a child that completed (or was deleted) while the daemon
        // was down must still wake its parent. Dedupe children so one synthetic
        // event covers every watch on the same child.
        to_reconcile.sort_by(|a, b| a.0 .0.cmp(&b.0 .0));
        to_reconcile.dedup_by(|a, b| a.0 == b.0);
        for (child_id, child_ws) in to_reconcile {
            self.reconcile_watch_child_on_rehydration(&child_id, &child_ws)
                .await;
        }
        Ok(loaded)
    }

    /// Whether an agent session row exists and is not `Deleted`. Store errors
    /// other than NotFound are treated as live (conservative: never prune a
    /// watch on a transient store error).
    pub(crate) async fn agent_is_live(&self, agent_id: &AgentId) -> bool {
        match self.store.get_agent_session(agent_id).await {
            Ok(session) => !matches!(session.status, intent_core::AgentStatus::Deleted),
            Err(intent_store::Error::NotFound(_)) => false,
            Err(e) => {
                tracing::warn!(
                    "completion-watch rehydration: agent liveness check failed for {}: {e}",
                    agent_id.0
                );
                true
            }
        }
    }

    /// Reconcile one watch's child against current agent state (mirrors the
    /// STAB-108 group reconciliation): if the child already completed /
    /// failed / was deleted, synthesize the matching completion event and
    /// route it through [`Services::deliver_completion_to_watches`] so the
    /// parent wakes now instead of waiting for an event that already fired.
    /// Used both at startup rehydration (child settled while the daemon was
    /// down) and at `app.agents.waitFor` registration time (target settled
    /// before — or concurrently with — the registration).
    pub(crate) async fn reconcile_watch_child_on_rehydration(
        &self,
        child_id: &AgentId,
        fallback_ws: &WorkspaceId,
    ) {
        use intent_core::AgentStatus;
        let (event_type, event_ws, status_value) =
            match self.store.get_agent_session(child_id).await {
                Ok(session) => {
                    let is_deleted = matches!(session.status, AgentStatus::Deleted);
                    let is_completed = matches!(session.status, AgentStatus::Completed);
                    let is_failed = matches!(session.status, AgentStatus::Error);
                    // RuntimeIdle: genuinely complete only with a completion
                    // report and no interrupted row (same conservative
                    // predicate as reconcile_group_on_rehydration).
                    let is_idle_complete = if matches!(session.status, AgentStatus::RuntimeIdle) {
                        let has_report = session.completion_report.is_some();
                        match self.store.get_interrupted_agent(child_id).await {
                            Ok(opt) => has_report && opt.is_none(),
                            Err(e) => {
                                tracing::warn!(
                                    "completion-watch reconciliation: interrupted_agent check \
                                     failed for {}: {e}",
                                    child_id.0
                                );
                                false
                            }
                        }
                    } else {
                        false
                    };
                    let event_type = if is_deleted {
                        intent_core::events::AGENT_DELETED
                    } else if is_failed {
                        intent_core::events::AGENT_FAILED
                    } else if is_completed || is_idle_complete {
                        intent_core::events::AGENT_IDLE
                    } else {
                        // Child still working (or interrupted/healing): the
                        // live event pipeline will deliver its completion.
                        return;
                    };
                    let status = serde_json::to_value(session.status).unwrap_or_default();
                    (event_type, session.workspace_id, status)
                }
                Err(intent_store::Error::NotFound(_)) => (
                    intent_core::events::AGENT_DELETED,
                    fallback_ws.clone(),
                    serde_json::json!("deleted"),
                ),
                Err(e) => {
                    tracing::warn!(
                        "completion-watch reconciliation: session lookup failed for {}: {e}",
                        child_id.0
                    );
                    return;
                }
            };
        let event = Event {
            id: uuid::Uuid::new_v4().to_string(),
            workspace_id: event_ws,
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
                "status": status_value,
            }),
        };
        self.deliver_completion_to_watches(child_id, &event).await;
    }

    /// Rehydrate undelivered delegation groups on resume (AS-2 rehydration).
    /// Idempotent: skips groups already present in memory (by group_id).
    /// `workspace_id` selects which persisted groups to load (the group's
    /// anchor — the parent's home workspace); the loaded groups land in the
    /// daemon-global registry.
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
            let mut loaded = 0;
            let mut groups_to_reconcile = Vec::new();
            for p in persisted {
                // Skip if this group is already in memory (idempotent rehydration).
                if guard
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
                guard.delegation_groups.push(group);
                loaded += 1;
            }
            (loaded, groups_to_reconcile)
        }; // guard dropped here

        // STAB-108 reconciliation: check each rehydrated group for already-completed children
        for group_id in groups_to_reconcile {
            self.reconcile_group_on_rehydration(&group_id).await;
            // Fire the group if it's now ready (all children completed/deleted)
            self.try_fire_group(&group_id).await;
        }
        Ok(loaded)
    }

    /// STAB-108: Reconcile a delegation group against current agent state after rehydration.
    /// For each expected child not already in completed_agent_ids or deleted_agent_ids,
    /// check if the agent session is idle/completed (or deleted/missing). If so, record
    /// its completion using the persisted completion_report.
    async fn reconcile_group_on_rehydration(&self, group_id: &str) {
        // Get the list of agents to check (expected but not yet recorded as
        // complete/deleted) plus the group's anchor workspace, used as the
        // fallback for synthetic events whose child session is gone.
        let (anchor_workspace, agents_to_check) = {
            let guard = self
                .agent_subscriptions
                .lock()
                .expect("agent subscription registry poisoned");
            let Some(g) = guard
                .delegation_groups
                .iter()
                .find(|g| g.group_id == group_id)
            else {
                return;
            };
            (
                g.workspace_id.clone(),
                g.expected_agent_ids
                    .iter()
                    .filter(|id| {
                        !g.completed_agent_ids.contains(id) && !g.deleted_agent_ids.contains(id)
                    })
                    .cloned()
                    .collect::<Vec<_>>(),
            )
        };
        let workspace_id = &anchor_workspace;

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
                        let event_type = if is_deleted {
                            intent_core::events::AGENT_DELETED
                        } else if is_failed {
                            intent_core::events::AGENT_FAILED
                        } else {
                            intent_core::events::AGENT_IDLE
                        };
                        // monorepo#1016: a synthesized agent:idle with no
                        // report and a still-incomplete assigned task gets
                        // the suspected-stall annotation (best-effort,
                        // fail-open) — mirroring the live delivery path.
                        let stall = if event_type == intent_core::events::AGENT_IDLE {
                            self.stall_suspicion_for_session(&session).await
                        } else {
                            None
                        };
                        let mut data = serde_json::json!({
                            "agentId": child_id.0,
                            "status": serde_json::to_value(session.status).unwrap_or_default(),
                        });
                        if let Some(s) = &stall {
                            s.annotate_event_data(&mut data);
                        }
                        annotate_attention_request(
                            &mut data,
                            session.attention_request_kind.as_deref(),
                            session.attention_request_reason.as_deref(),
                        );
                        let report = session.completion_report;
                        // Child completion events fire in the CHILD's own
                        // workspace (which differs from the group's anchor
                        // for chief-anchored groups).
                        let event = Event {
                            id: uuid::Uuid::new_v4().to_string(),
                            workspace_id: session.workspace_id.clone(),
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
                            data,
                        };
                        let summary = crate::format_group_child_line(
                            &child_id,
                            &event,
                            report.as_deref(),
                            stall.as_ref(),
                        );

                        // Record the completion
                        self.record_group_child_completion(
                            group_id, &child_id, is_deleted, summary, event,
                        )
                        .await;
                    }
                }
                Err(e) => {
                    // Only NotFound → deleted; other errors → log and skip.
                    // The session is gone so the child's own workspace is
                    // unknowable — fall back to the group's anchor workspace.
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
                        let summary = crate::format_group_child_line(&child_id, &event, None, None);

                        self.record_group_child_completion(
                            group_id, &child_id, true, // deleted
                            summary, event,
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
        // Find which group (if any) this agent belongs to — global lookup,
        // so a chief-anchored group finds its workspace-scoped child too.
        let group_id = {
            let guard = self
                .agent_subscriptions
                .lock()
                .expect("agent subscription registry poisoned");
            guard
                .delegation_groups
                .iter()
                .find(|g| g.expected_agent_ids.contains(agent_id))
                .map(|g| g.group_id.clone())
        };

        if let Some(group_id) = group_id {
            // Build event for group recording. Prefer the child's persisted
            // completion_report (set by agent.reportToParent) over the generic
            // summary, mirroring deliver_completion_to_watches logic.
            let session = self.store.get_agent_session(agent_id).await.ok();
            // monorepo#1016: annotate a suspected stall (idle, no report,
            // assigned task still incomplete) on the recorded line + event
            // data. Best-effort — lookup failures fail open.
            let stall = match session.as_ref() {
                Some(s) => self.stall_suspicion_for_session(s).await,
                None => None,
            };
            let attention = session.as_ref().map(|s| {
                (
                    s.attention_request_kind.clone(),
                    s.attention_request_reason.clone(),
                )
            });
            let report = session.and_then(|s| s.completion_report);
            let mut data = event_data.clone();
            if let Some(s) = &stall {
                s.annotate_event_data(&mut data);
            }
            if let Some((kind, reason)) = &attention {
                annotate_attention_request(&mut data, kind.as_deref(), reason.as_deref());
            }
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
                data,
            };
            let summary =
                crate::format_group_child_line(agent_id, &event, report.as_deref(), stall.as_ref());

            self.record_group_child_completion(
                &group_id, agent_id, false, // not deleted
                summary, event,
            )
            .await;
        }
    }
}

/// Merge a child's pending attention request (persisted session fields set by
/// `ws.agent.requestDiscussion` / `ws.agent.reportBlocker`) into a group-record
/// event's `data`, so `format_group_child_line` can fold the kind-flavored
/// attention text into the aggregated group wake (a grouped child skips its
/// immediate parent wake). No-op when no request is pending.
fn annotate_attention_request(
    data: &mut serde_json::Value,
    kind: Option<&str>,
    reason: Option<&str>,
) {
    let Some(kind) = kind.filter(|k| !k.is_empty()) else {
        return;
    };
    if let Some(obj) = data.as_object_mut() {
        obj.insert("attentionRequestKind".to_string(), serde_json::json!(kind));
        obj.insert(
            "attentionRequestReason".to_string(),
            serde_json::json!(reason.unwrap_or("")),
        );
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

/// Convert in-memory `CompletionWatch` to persisted form. The monotonic
/// `cleanup_deadline` instant is projected onto the wall clock (epoch ms) so
/// a restarted daemon — with a fresh monotonic clock — can re-arm the timer
/// with the remaining real time.
fn completion_watch_to_persisted(watch: &CompletionWatch) -> PersistedCompletionWatch {
    PersistedCompletionWatch {
        id: watch.id.clone(),
        parent_workspace_id: watch.parent_workspace_id.clone(),
        child_workspace_id: watch.child_workspace_id.clone(),
        parent_agent_id: watch.parent_agent_id.clone(),
        parent_agent_name: watch.parent_agent_name.clone(),
        child_agent_id: watch.child_agent_id.clone(),
        one_shot: watch.one_shot,
        group_id: watch.group_id.clone(),
        report_delivered: watch.report_delivered,
        deadline_at_ms: watch.cleanup_deadline.map(instant_to_epoch_ms),
        created_at: watch.created_at.clone(),
    }
}

/// Convert a persisted row back to the in-memory form. `cleanup_deadline`
/// starts `None`: rehydration re-arms it via `spawn_watch_cleanup` (which
/// bumps the deadline under the registry lock) so the sleeper task and the
/// in-memory instant stay paired.
fn persisted_to_completion_watch(p: &PersistedCompletionWatch) -> CompletionWatch {
    CompletionWatch {
        id: p.id.clone(),
        parent_workspace_id: p.parent_workspace_id.clone(),
        child_workspace_id: p.child_workspace_id.clone(),
        parent_agent_id: p.parent_agent_id.clone(),
        parent_agent_name: p.parent_agent_name.clone(),
        child_agent_id: p.child_agent_id.clone(),
        one_shot: p.one_shot,
        group_id: p.group_id.clone(),
        created_at: p.created_at.clone(),
        cleanup_deadline: None,
        report_delivered: p.report_delivered,
    }
}

/// Project a (possibly future) monotonic instant onto the wall clock as unix
/// epoch milliseconds — the persisted representation of a cleanup deadline.
fn instant_to_epoch_ms(deadline: Instant) -> i64 {
    let now = Instant::now();
    let now_ms = std::time::SystemTime::now()
        .duration_since(std::time::UNIX_EPOCH)
        .map(|d| d.as_millis() as i64)
        .unwrap_or(0);
    let delta_ms = deadline.saturating_duration_since(now).as_millis() as i64;
    now_ms + delta_ms
}

/// Remaining wall-clock time until a persisted epoch-ms deadline; `None`
/// when it already elapsed.
fn remaining_from_epoch_ms(deadline_at_ms: i64) -> Option<std::time::Duration> {
    let now_ms = std::time::SystemTime::now()
        .duration_since(std::time::UNIX_EPOCH)
        .map(|d| d.as_millis() as i64)
        .unwrap_or(0);
    let remaining = deadline_at_ms - now_ms;
    if remaining > 0 {
        Some(std::time::Duration::from_millis(remaining as u64))
    } else {
        None
    }
}

/// Convert in-memory `DelegationGroup` to persisted form. The persisted
/// `workspace_id` column carries the group's anchor (the parent's home
/// workspace).
fn delegation_group_to_persisted(group: &DelegationGroup) -> Result<PersistedDelegationGroup> {
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
        workspace_id: group.workspace_id.clone(),
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
        workspace_id: p.workspace_id.clone(),
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
