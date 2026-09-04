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
//! Pair uniqueness: a parent holds AT MOST ONE active watch per child —
//! across ungrouped and grouped watches. The shared registration path
//! enforces it by ADOPTING an existing watch for the pair instead of pushing
//! a duplicate (a grouped request converts the watch to the group watch; a
//! weaker request never degrades a stronger watch). Explicit registrations
//! (`app.agents.waitFor`) reject an already-watched target up front instead;
//! startup rehydration coalesces pre-existing duplicate persisted rows.
//!
//! An ungrouped watch is registered when an agent delegates with `waitMode`
//! `immediate` (default) over the MCP front door; the delivery worker that
//! fires on child completion lands in AS-3 and the `after_all`
//! delegation-group fan-in lands in AS-4.
//!
//! Mirrors the TS `subscribeCallerToAgentCompletion` / `agentSubscribe` shape
//! (`actorIds: [child]`, AGENT completion event set
//! `['agent:idle','agent:failed','agent:deleted']`). The event-type wiring is an
//! AS-3 concern; this module only owns the registry records and helpers.

use std::sync::Arc;

use intent_store::{PersistedCompletionWatch, PersistedDelegationGroup};

use intent_core::{now_iso, AgentId, Error, Event, Result, WorkspaceId};
use uuid::Uuid;

use crate::Services;

/// One parent→child completion-watch record. An ungrouped watch is removed
/// once the child's completion has been delivered to the parent (AS-3).
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
    pub group_id: Option<String>,
    pub created_at: String,
    /// Report-time wake: set to `true` when `agent.reportToParent` delivers
    /// the parent wake immediately. When `true`, `deliver_completion_to_watches`
    /// skips delivery for `agent:idle` (suppressing the duplicate wake) but
    /// still delivers for `agent:failed` / `agent:deleted` (failure after
    /// reporting is a new signal, not a duplicate).
    pub report_delivered: bool,
    /// Set by explicit `agent.watch` registration (monorepo#1229);
    /// auto-registered watches (delegation, SUB-1 sender watches) leave this
    /// `false`. Since monorepo#3443 the flag no longer gates the attention
    /// fan-out — `agent_request_attention_op` wakes EVERY active watch when
    /// the child raises an attention request (`agent.reportBlocker` /
    /// `agent.requestDiscussion`), excluding only the child's parent, which
    /// it already wakes directly. The field is kept as the persisted record
    /// of an explicit registration.
    pub wake_on_attention: bool,
    /// Ask-only watches wait strictly for terminal completion. Attention
    /// requests do not consume or wake this watch, and monitoring-idle
    /// advisories (child idle while only hooks / PR monitors are active) are
    /// skipped for it too — the parent hears nothing until settlement, even
    /// if the child parks on a TTL-less PR monitor (intent-hq/intent#4254
    /// design). It may coexist with an unrelated grouped watch for the same
    /// parent/child pair. The flag is set only on watches the ask path
    /// CREATES: `register_completion_watch_strict_durable` reuses an
    /// existing non-strict ungrouped watch as-is, and any later non-ask
    /// registration adopting an ask-only watch CLEARS the flag (upgrade to
    /// a full watch — the explicit watcher expects advisories; see
    /// `insert_watch_in_memory`). The skip therefore holds exactly while the
    /// ask registration is the pair's only ungrouped interest.
    pub completion_only: bool,
    /// Identity (`completion_report.timestamp`) of the child report whose
    /// report-time wake was SENT toward this parent (monorepo#4026): stamped
    /// when the wake is handed off — immediate delivery, queued, or parked
    /// as a debounce hold. The stamp alone does not prove the parent saw
    /// it: the terminal completion wake suppresses the `Report:` clause in
    /// favor of a short already-delivered reference only when this identity
    /// matches the settling report AND the #1614 retracts removed nothing
    /// (an undelivered wake is retracted there, failing open to the full
    /// report). In-memory only (not persisted): after a daemon restart the
    /// terminal wake fails open to the full report, which is benign.
    pub delivered_report_ts: Option<String>,
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
    pub await_mode: String,
    pub expected_agent_ids: Vec<AgentId>,
    pub completed_agent_ids: Vec<AgentId>,
    pub deleted_agent_ids: Vec<AgentId>,
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

/// Which call site is running
/// [`Services::reconcile_watch_child_on_rehydration`] (monorepo#2532): a NEW
/// explicit registration (`agent.watch` / `app.agents.waitFor`) asks for the
/// child's NEXT completion — a reported idle child still owning active hooks
/// or PR monitors defers instead of firing instantly with the stale report —
/// while boot REHYDRATION keeps report-equals-settlement semantics (a missed
/// wake must deliver at boot).
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub(crate) enum WatchReconcileCallSite {
    Registration,
    Rehydration,
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
    ///
    /// Pair uniqueness: when the parent already holds a watch on this child,
    /// the existing watch is ADOPTED (see [`Services::insert_watch_in_memory`])
    /// and its id is returned — no duplicate is ever pushed.
    pub(crate) fn register_completion_watch(
        &self,
        parent_workspace_id: &WorkspaceId,
        child_workspace_id: &WorkspaceId,
        parent_agent_id: AgentId,
        parent_agent_name: String,
        child_agent_id: AgentId,
        group_id: Option<String>,
    ) -> Result<String> {
        let watch = self.insert_watch_in_memory(
            parent_workspace_id,
            child_workspace_id,
            parent_agent_id,
            parent_agent_name,
            child_agent_id,
            group_id,
            false,
        )?;
        // Write-through persist (best-effort) so the watch survives a daemon
        // restart (rehydrated by `heal_completion_watches_on_startup`). On the
        // adopt path this upserts the existing row's mutable columns
        // (group_id) so the strengthened mode is restart-durable.
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
    pub(crate) async fn register_completion_watch_durable(
        &self,
        parent_workspace_id: &WorkspaceId,
        child_workspace_id: &WorkspaceId,
        parent_agent_id: AgentId,
        parent_agent_name: String,
        child_agent_id: AgentId,
        group_id: Option<String>,
    ) -> Result<String> {
        let watch = self.insert_watch_in_memory(
            parent_workspace_id,
            child_workspace_id,
            parent_agent_id,
            parent_agent_name,
            child_agent_id,
            group_id,
            false,
        )?;
        let id = watch.id.clone();
        let persisted = completion_watch_to_persisted(&watch);
        if let Err(e) = self.store.upsert_completion_watch(&persisted).await {
            tracing::warn!("completion_watch upsert failed {id}: {e}");
        }
        self.delete_watch_row_if_swept(&id).await;
        Ok(id)
    }

    /// Ask-only durable registration. A new completion-only watch is persisted
    /// before it becomes visible to completion delivery, so a concurrent
    /// delivery cannot delete it before a late upsert resurrects an orphan.
    /// An existing ungrouped watch already provides an immediate completion
    /// path and is reused without another durable write.
    pub(crate) async fn register_completion_watch_strict_durable(
        &self,
        parent_workspace_id: &WorkspaceId,
        child_workspace_id: &WorkspaceId,
        parent_agent_id: AgentId,
        parent_agent_name: String,
        child_agent_id: AgentId,
    ) -> Result<String> {
        check_watch_scope(parent_workspace_id, child_workspace_id)?;
        let _registration = self.completion_watch_registration_gate.lock().await;
        if let Some(existing) = self
            .agent_subscriptions
            .lock()
            .expect("agent subscription registry poisoned")
            .subscriptions
            .iter()
            .find(|watch| {
                watch.parent_agent_id == parent_agent_id
                    && watch.child_agent_id == child_agent_id
                    && watch.group_id.is_none()
            })
        {
            return Ok(existing.id.clone());
        }
        let watch = CompletionWatch {
            id: Uuid::new_v4().to_string(),
            parent_workspace_id: parent_workspace_id.clone(),
            child_workspace_id: child_workspace_id.clone(),
            parent_agent_id,
            parent_agent_name,
            child_agent_id,
            group_id: None,
            created_at: now_iso(),
            report_delivered: false,
            wake_on_attention: false,
            completion_only: true,
            delivered_report_ts: None,
        };
        let id = watch.id.clone();
        self.store
            .upsert_completion_watch(&completion_watch_to_persisted(&watch))
            .await?;
        self.agent_subscriptions
            .lock()
            .expect("agent subscription registry poisoned")
            .subscriptions
            .push(watch);
        Ok(id)
    }

    /// Explicit `agent.watch` registration (monorepo#1229): an ungrouped
    /// watch with an AWAITED persist (the registration is the caller's
    /// durable contract). Attention wakes reach every active watch
    /// regardless of the `wake_on_attention` flag (monorepo#3443); the flag
    /// is kept as the persisted record of an explicit registration.
    /// Adoption strengthens an existing watch for the pair
    /// (`wake_on_attention` set, `completion_only` cleared) — grouped watches
    /// keep their group but gain the flag.
    pub(crate) async fn register_agent_watch_durable(
        &self,
        parent_workspace_id: &WorkspaceId,
        child_workspace_id: &WorkspaceId,
        parent_agent_id: AgentId,
        parent_agent_name: String,
        child_agent_id: AgentId,
    ) -> Result<String> {
        let watch = self.insert_watch_in_memory(
            parent_workspace_id,
            child_workspace_id,
            parent_agent_id,
            parent_agent_name,
            child_agent_id,
            None,
            true,
        )?;
        let id = watch.id.clone();
        let persisted = completion_watch_to_persisted(&watch);
        if let Err(e) = self.store.upsert_completion_watch(&persisted).await {
            tracing::warn!("completion_watch upsert failed {id}: {e}");
        }
        self.delete_watch_row_if_swept(&id).await;
        Ok(id)
    }

    /// Close the write-through registration race (monorepo#4183): the two
    /// insert-then-upsert variants above make the watch memory-visible
    /// BEFORE the persisted row lands, so a concurrent parent sweep
    /// (retire/delete cascade) can remove the in-memory watch and its rows
    /// between the insert and the upsert — the late upsert then resurrects
    /// an orphan row that rehydrates on restart. After the upsert commits,
    /// re-check membership and best-effort delete the row when the watch is
    /// already gone. (The strict variant persists before memory-visibility
    /// and does not need this.)
    async fn delete_watch_row_if_swept(&self, watch_id: &str) {
        let still_present = self
            .agent_subscriptions
            .lock()
            .expect("agent subscription registry poisoned")
            .subscriptions
            .iter()
            .any(|s| s.id == watch_id);
        if still_present {
            return;
        }
        tracing::info!(
            watch = %watch_id,
            "watch swept during registration upsert; deleting resurrected row"
        );
        if let Err(e) = self.store.delete_completion_watch(watch_id).await {
            tracing::warn!("completion_watch post-sweep delete failed {watch_id}: {e}");
        }
    }

    /// Shared body of the two registration variants: run the scope gate,
    /// then either ADOPT the parent's existing watch on this child (pair
    /// uniqueness: at most one active watch per (parent, child)) or build a
    /// fresh watch and push it into the in-memory registry.
    ///
    /// Adoption rules (strengthen-only, never weaken):
    /// - A grouped request converts the existing watch into the group watch
    ///   (`group_id` adopted) — group settlement accounting REQUIRES the
    ///   grouped watch to exist, so the group always wins a collision (this
    ///   is the footer duplicate-wake bug: an ungrouped watch coexisting
    ///   with `after_all` membership double-woke the parent).
    /// - An ungrouped request against an existing grouped watch is a no-op
    ///   (the group already provides the wake path).
    /// - `wake_on_attention` is strengthen-only: an explicit `agent.watch`
    ///   sets it on the adopted watch; a later auto registration never
    ///   clears it.
    /// - `completion_only` is cleared: every registration through this path
    ///   is a full (non-ask) watch, so adopting an ask-only watch upgrades it
    ///   to hear monitoring-idle advisories (PR #1686 review). The ask path
    ///   (`register_completion_watch_strict_durable`) never adopts through
    ///   here, so it cannot re-narrow a full watch.
    #[allow(clippy::too_many_arguments)]
    fn insert_watch_in_memory(
        &self,
        parent_workspace_id: &WorkspaceId,
        child_workspace_id: &WorkspaceId,
        parent_agent_id: AgentId,
        parent_agent_name: String,
        child_agent_id: AgentId,
        group_id: Option<String>,
        wake_on_attention: bool,
    ) -> Result<CompletionWatch> {
        check_watch_scope(parent_workspace_id, child_workspace_id)?;
        let watch = {
            let mut guard = self
                .agent_subscriptions
                .lock()
                .expect("agent subscription registry poisoned");
            if let Some(existing) = guard.subscriptions.iter_mut().find(|s| {
                s.parent_agent_id == parent_agent_id && s.child_agent_id == child_agent_id
            }) {
                if let Some(gid) = group_id {
                    existing.group_id = Some(gid);
                }
                existing.wake_on_attention = existing.wake_on_attention || wake_on_attention;
                existing.completion_only = false;
                // monorepo#2532: adoption expresses fresh interest in the
                // child's NEXT completion — clear the report-time disarm so
                // a re-armed watch fires on the next `agent:idle` (mirrors
                // the failure-wake dedup clear below). Persisted by the
                // callers' write-through upsert of the returned watch.
                existing.report_delivered = false;
                // Refresh the parent display name and home-workspace anchor
                // from the current registration (mirroring
                // `find_and_refresh_ungrouped_watch`): a watch created with
                // fallback data must not stay stale when a later
                // registration carries the correct name/home. The anchor
                // refresh is re-gated against the EXISTING watch's child
                // workspace (which may differ from this call's).
                existing.parent_agent_name = parent_agent_name;
                if existing.parent_workspace_id != *parent_workspace_id
                    && check_watch_scope(parent_workspace_id, &existing.child_workspace_id).is_ok()
                {
                    existing.parent_workspace_id = parent_workspace_id.clone();
                }
                existing.clone()
            } else {
                let watch = CompletionWatch {
                    id: Uuid::new_v4().to_string(),
                    parent_workspace_id: parent_workspace_id.clone(),
                    child_workspace_id: child_workspace_id.clone(),
                    parent_agent_id,
                    parent_agent_name,
                    child_agent_id,
                    group_id,
                    created_at: now_iso(),
                    report_delivered: false,
                    wake_on_attention,
                    completion_only: false,
                    delivered_report_ts: None,
                };
                guard.subscriptions.push(watch.clone());
                watch
            }
        };
        // monorepo#840: a fresh watch expresses fresh interest — drop any
        // stale failure-wake dedup record for this pair so the next failure
        // (even with unchanged error text) reaches the new watcher. Dedup
        // then only suppresses replays BETWEEN registrations.
        self.clear_failure_wake_dedup_pair(&watch.parent_agent_id, &watch.child_agent_id);
        Ok(watch)
    }

    /// Whether the parent already holds ANY active watch on this child —
    /// ungrouped or grouped (the pair-uniqueness pre-check used by explicit
    /// registration paths that must reject duplicates instead of silently
    /// adopting, e.g. `app.agents.waitFor`).
    pub(crate) fn pair_watch_exists(
        &self,
        parent_agent_id: &AgentId,
        child_agent_id: &AgentId,
    ) -> bool {
        self.agent_subscriptions
            .lock()
            .expect("agent subscription registry poisoned")
            .subscriptions
            .iter()
            .any(|s| &s.parent_agent_id == parent_agent_id && &s.child_agent_id == child_agent_id)
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

    /// SUB-2 (Copilot #104 follow-up, thread `PRRT_kwDOS9Wxuc6QKPyt`):
    /// atomically find a live ungrouped (immediate-mode) watch for the given
    /// caller→target pair and, while still holding the registry lock,
    /// refresh its stored `parent_agent_name` to `new_parent_name`. Returns
    /// the live subscription id iff a matching watch was found — so a
    /// concurrent removal by [`Services::deliver_completion_to_watches`]
    /// cannot land between the find and the refresh and leave
    /// `agent.wakeOrCreate` returning a "reused" subscription id that no
    /// longer exists. Callers must fall through to
    /// [`Services::register_completion_watch`] when this returns `None`.
    ///
    /// Grouped (`after_all`) watches are skipped since they are owned by the
    /// delegation-group fan-in. Refreshing the stored name keeps
    /// `agent.getSubscriptions` / [`describe_subscription`] in sync with any
    /// rename applied via `agent.rename` / `agent.update` since the watch
    /// was registered; a no-op when the name is already current.
    ///
    /// `new_parent_name` is `None` when the caller's current display name
    /// could not be resolved (e.g. `store.get_agent_session` failed under
    /// contention, Copilot #104 thread `PRRT_kwDOS9Wxuc6QKWuU`): the reuse
    /// still proceeds, but the existing stored name is left intact rather
    /// than overwritten with an empty placeholder that would degrade
    /// `agent.getSubscriptions` / `describe_subscription` output.
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
        new_parent_name: Option<String>,
        resolved_parent_workspace_id: Option<&WorkspaceId>,
    ) -> Option<String> {
        let (id, name, home_ws, changed, rearmed) = {
            let mut guard = self
                .agent_subscriptions
                .lock()
                .expect("agent subscription registry poisoned");
            let watch = guard.subscriptions.iter_mut().find(|s| {
                s.group_id.is_none()
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
            // monorepo#2532: every reuse call site expresses fresh interest
            // in the child's NEXT completion (delegate/create/wakeOrCreate/
            // sender auto-subscribe) — clear the report-time disarm so the
            // reused watch fires on the next `agent:idle` instead of
            // silently retiring (mirrors the `insert_watch_in_memory`
            // adoption reset).
            let rearmed = std::mem::replace(&mut watch.report_delivered, false);
            (
                watch.id.clone(),
                watch.parent_agent_name.clone(),
                watch.parent_workspace_id.clone(),
                changed,
                rearmed,
            )
        };
        // Best-effort DB sync of the refreshed name/anchor/re-arm (restart
        // durability), skipped when nothing changed (the common
        // waitFor-called-twice case). These are spawned UPDATE ... WHERE id,
        // so racing a concurrent fire/delete is benign: against a deleted
        // row they affect 0 rows (they cannot resurrect an orphan); the only
        // loss is the refreshed state not persisting — the in-memory watch
        // is already refreshed and the row is gone anyway.
        if changed || rearmed {
            let store = self.store.clone();
            let watch_id = id.clone();
            tokio::spawn(async move {
                if changed {
                    if let Err(e) = store
                        .update_completion_watch_parent(&watch_id, &name, &home_ws)
                        .await
                    {
                        tracing::warn!("completion_watch parent refresh failed {watch_id}: {e}");
                    }
                }
                if rearmed {
                    if let Err(e) = store
                        .clear_completion_watch_report_delivered(&watch_id)
                        .await
                    {
                        tracing::warn!("completion_watch re-arm persist failed {watch_id}: {e}");
                    }
                }
            });
        }
        Some(id)
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

    /// The subset of [`Self::list_watches_for_parent`] that still counts as a
    /// "waiting on agents" reason: `report_delivered` watches are excluded
    /// (issues intent-hq/monorepo#1643 + #1649) — such a watch has already
    /// delivered its report-time wake and retires inline without waking the
    /// holder, so it is not something the holder is waiting FOR. Shared by the
    /// settlement predicate ([`crate::Services::agent_is_waiting_on_agents`])
    /// and the `isWaitingForOtherAgents` / `waitingForAgentIds` projections so
    /// display and settlement use one definition of "waiting on agents".
    pub(crate) fn waiting_watches_for_parent(
        &self,
        parent_agent_id: &AgentId,
    ) -> Vec<CompletionWatch> {
        self.list_watches_for_parent(parent_agent_id)
            .into_iter()
            .filter(|w| !w.report_delivered)
            .collect()
    }

    /// Whether any TOP-LEVEL agent homed in `workspace_id` holds at least
    /// one active completion watch (ungrouped or grouped) — the third
    /// `Workspace.waiting` signal alongside active hooks and active PR
    /// monitors (§5.1, via [`Services::workspace_is_waiting`]): an idle
    /// parent still waiting on delegated children reads as waiting, without
    /// promoting the `displayStatus` rollup. `report_delivered` watches are
    /// excluded, matching the agent-waiting projection and settlement
    /// predicate ([`Services::waiting_watches_for_parent`]): a watch whose
    /// report-time wake already fired no longer reads as pending work.
    /// Watches anchor in the parent's HOME
    /// workspace (`parent_workspace_id`) — where the wake will be delivered
    /// — never the child's. The top-level filter matches
    /// [`Services::workspace_attention_signals`] (no `parent_agent_id`, not
    /// background, not deleted), so watches held by child/background agents
    /// never count. The in-memory registry is consulted first, so the
    /// common no-watch case costs no store read; otherwise this is one
    /// message-free session-summaries read. Best-effort: a store read
    /// failure is logged and fails open to `false` (mirrors
    /// [`Services::workspace_has_active_hooks`]) so list/get emission is
    /// never wedged and activity is never fabricated.
    pub(crate) async fn workspace_has_waiting_agent_subscriptions(
        &self,
        workspace_id: &WorkspaceId,
    ) -> bool {
        let parents: Vec<AgentId> = {
            let guard = self
                .agent_subscriptions
                .lock()
                .expect("agent subscription registry poisoned");
            guard
                .subscriptions
                .iter()
                .filter(|s| &s.parent_workspace_id == workspace_id && !s.report_delivered)
                .map(|s| s.parent_agent_id.clone())
                .collect()
        };
        if parents.is_empty() {
            return false;
        }
        match self.store.list_agent_session_summaries(workspace_id).await {
            Ok(sessions) => sessions.iter().any(|s| {
                s.parent_agent_id.is_none()
                    && !s.is_background
                    && s.status != intent_core::AgentStatus::Deleted
                    && parents.contains(&s.id)
            }),
            Err(e) => {
                tracing::warn!(
                    workspace = %workspace_id.0,
                    error = %e,
                    "waiting-subscriptions displayStatus lookup failed; reads as none"
                );
                false
            }
        }
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

    /// Remove a fired watch from memory after its durable retirement
    /// transaction committed. Unlike [`Services::remove_watch`], this does not
    /// spawn a second best-effort store delete.
    pub(crate) fn remove_watch_after_delivery_commit(&self, subscription_id: &str) -> bool {
        let mut guard = self
            .agent_subscriptions
            .lock()
            .expect("agent subscription registry poisoned");
        let before = guard.subscriptions.len();
        guard
            .subscriptions
            .retain(|watch| watch.id != subscription_id);
        guard.subscriptions.len() != before
    }

    /// Legacy test helper for waiting-projection compatibility. Production
    /// progress delivery no longer sets this historical suppression bit.
    #[cfg(test)]
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
                // `report_delivered` is an UNGROUPED-only flag: the agent-waiting
                // classification excludes such watches (issue
                // intent-hq/monorepo#1643), so marking a grouped watch would
                // silently declassify an open `after_all` waiting edge.
                debug_assert!(
                    watch.group_id.is_none(),
                    "report_delivered must not be set on a grouped watch"
                );
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

    /// Stamp `delivered_report_ts` on every UNGROUPED watch `parent_agent_id`
    /// holds on `child_agent_id` (monorepo#4026). Called when a
    /// report-to-parent wake is sent toward the parent — immediate delivery,
    /// queued, or parked as a debounce hold. The stamp records the report
    /// identity only; actual delivery is proven at settlement by the #1614
    /// retract gate (retracts removing nothing = the wake left the queue),
    /// so a retracted held or still-queued wake never suppresses the
    /// terminal report. Grouped watches are skipped: `after_all` children
    /// defer their reports to the aggregate wake, which has no per-child
    /// duplicate to suppress.
    pub(crate) fn stamp_watch_delivered_report_ts(
        &self,
        parent_agent_id: &AgentId,
        child_agent_id: &AgentId,
        report_ts: &str,
    ) {
        let mut guard = self
            .agent_subscriptions
            .lock()
            .expect("agent subscription registry poisoned");
        for watch in guard.subscriptions.iter_mut().filter(|w| {
            w.group_id.is_none()
                && &w.parent_agent_id == parent_agent_id
                && &w.child_agent_id == child_agent_id
        }) {
            watch.delivered_report_ts = Some(report_ts.to_string());
        }
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

    /// Whether `parent_id` currently has an ENROLLMENT-OPEN (unsealed &&
    /// undelivered) delegation group — one still accepting children in the
    /// parent's current delegating turn. Used by the batch-delegate
    /// zero-started advisory (monorepo#3334) to decide whether to deliver the
    /// immediate advisory wake. Note this is deliberately NARROWER than
    /// "a settlement wake is still owed": a sealed-but-undelivered group from
    /// an earlier turn also owes a wake, but it does not suppress the
    /// advisory — that errs on the side of an extra (redundant) advisory
    /// rather than risking a permanent stall if the sealed group's delivery
    /// never fires. Fail-noisy over fail-silent.
    pub(crate) fn has_open_delegation_group(&self, parent_id: &AgentId) -> bool {
        self.agent_subscriptions
            .lock()
            .expect("agent subscription registry poisoned")
            .delegation_groups
            .iter()
            .any(|g| &g.parent_agent_id == parent_id && !g.sealed && !g.delivered)
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
    /// group parented by `parent_id`. Gates the immediate `reportToParent`
    /// wake in `agent_report_to_parent_op` (grouped children defer to the
    /// group's aggregated wake) and the SUB-1 auto-watch registration
    /// (grouped children are already covered by the grouped watch). Attention
    /// requests (`agent_request_attention_op`) intentionally bypass it — that
    /// wake is delivered immediately regardless of grouping.
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

    /// Claim a complete group for delivery without retiring its durable row.
    /// The in-memory `delivered` bit serializes live attempts; the persisted
    /// complete group remains the restart-recovery record until the aggregated
    /// parent wake is durable and final settlement commits.
    pub(crate) fn take_group_if_ready(&self, group_id: &str) -> Option<DelegationGroup> {
        let mut guard = self
            .agent_subscriptions
            .lock()
            .expect("agent subscription registry poisoned");
        let group = guard
            .delegation_groups
            .iter_mut()
            .find(|g| g.group_id == group_id)?;
        if !(group.sealed && !group.delivered && is_group_complete(group)) {
            return None;
        }
        group.delivered = true;
        Some(group.clone())
    }

    /// Release an in-memory delivery claim after delivery or durable settlement
    /// fails. The persisted row was never retired, so a later event or restart
    /// can retry the same complete group.
    pub(crate) fn release_group_delivery(&self, group_id: &str) {
        if let Some(group) = self
            .agent_subscriptions
            .lock()
            .expect("agent subscription registry poisoned")
            .delegation_groups
            .iter_mut()
            .find(|g| g.group_id == group_id)
        {
            group.delivered = false;
        }
    }

    /// Whether an in-memory delegation group still needs settlement. The
    /// durable wake retry task stops after delivery or cancellation removes it.
    pub(crate) fn has_delegation_group(&self, group_id: &str) -> bool {
        self.agent_subscriptions
            .lock()
            .expect("agent subscription registry poisoned")
            .delegation_groups
            .iter()
            .any(|group| group.group_id == group_id)
    }

    /// Finalize a group after its aggregated wake and the matching store
    /// settlement are durable. Every completion watch carrying `group_id` is
    /// dropped, EXCEPT that watches on children listed in `retain_children`
    /// are converted in place into ungrouped watches. The group is removed in
    /// the same registry lock, so live readers never observe half-settlement.
    /// children listed in `retain_children` are converted in place into
    /// ungrouped watches (STAB-129: failed-not-deleted members may still be
    /// working, and their eventual real settlement must keep a wake path to
    /// the parent). Conversion dedupes against any live ungrouped watch for
    /// the same parent→child pair (e.g. a SUB-1 sendToTask auto-watch or a
    /// `wakeOrCreate` watch racing settlement) — since either already gives
    /// the parent a wake path, so the late settlement delivers exactly one
    /// wake. Returns the number of watches retained.
    pub(crate) fn finalize_group_delivery(
        &self,
        group_id: &str,
        retain_children: &[AgentId],
    ) -> usize {
        let retain_set: std::collections::HashSet<&AgentId> = retain_children.iter().collect();
        let mut guard = self
            .agent_subscriptions
            .lock()
            .expect("agent subscription registry poisoned");
        guard
            .delegation_groups
            .retain(|group| group.group_id != group_id);
        let mut kept: std::collections::HashSet<(AgentId, AgentId)> = guard
            .subscriptions
            .iter()
            .filter(|s| s.group_id.is_none())
            .map(|s| (s.parent_agent_id.clone(), s.child_agent_id.clone()))
            .collect();
        let mut retained = 0;
        guard.subscriptions.retain_mut(|watch| {
            if watch.group_id.as_deref() != Some(group_id) {
                return true;
            }
            if retain_set.contains(&watch.child_agent_id) {
                let pair = (watch.parent_agent_id.clone(), watch.child_agent_id.clone());
                if kept.insert(pair) {
                    watch.group_id = None;
                    retained += 1;
                    return true;
                }
            }
            false
        });
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
        let removed_ids: Vec<String> = {
            let mut guard = self
                .agent_subscriptions
                .lock()
                .expect("agent subscription registry poisoned");
            let mut ids = Vec::new();
            guard.delegation_groups.retain(|g| {
                if &g.parent_agent_id == parent_id {
                    ids.push(g.group_id.clone());
                    false
                } else {
                    true
                }
            });
            ids
        };
        let removed = removed_ids.len();
        if removed > 0 {
            // Best-effort DB sweep (mirrors `remove_all_for_parent`): drop the
            // persisted rows so cancelled groups don't rehydrate on restart. A
            // failed delete self-heals — cancel is idempotent, so a repeat
            // cancel (or group delivery) clears any resurrected group.
            let store = self.store.clone();
            tokio::spawn(async move {
                for gid in removed_ids {
                    if let Err(e) = store.delete_delegation_group(&gid).await {
                        tracing::warn!("delegation_group parent sweep failed {gid}: {e}");
                    }
                }
            });
        }
        removed
    }

    /// Remove ONE delegation group by id together with EVERY watch carrying
    /// its `group_id`, in a single registry critical section — closing the
    /// window in which a concurrently registered grouped watch could survive
    /// as an orphan pointing at a deleted group. Only removes a group
    /// parented by `parent_id`; returns the removed snapshot or `None` when
    /// no such group exists. The persisted watch rows are swept best-effort
    /// (spawned, like every other watch-delete path); the caller owns the
    /// persisted GROUP row delete (durable-before-observable in the scoped
    /// `agent.cancelSubscriptions` path).
    pub(crate) fn remove_group_with_watches(
        &self,
        parent_id: &AgentId,
        group_id: &str,
    ) -> Option<DelegationGroup> {
        let (group, watch_ids) = {
            let mut guard = self
                .agent_subscriptions
                .lock()
                .expect("agent subscription registry poisoned");
            let idx = guard
                .delegation_groups
                .iter()
                .position(|g| g.group_id == group_id && &g.parent_agent_id == parent_id)?;
            let group = guard.delegation_groups.remove(idx);
            let mut watch_ids = Vec::new();
            guard.subscriptions.retain(|s| {
                if s.group_id.as_deref() == Some(group_id) {
                    watch_ids.push(s.id.clone());
                    false
                } else {
                    true
                }
            });
            (group, watch_ids)
        };
        if !watch_ids.is_empty() {
            let store = self.store.clone();
            tokio::spawn(async move {
                for id in watch_ids {
                    if let Err(e) = store.delete_completion_watch(&id).await {
                        tracing::warn!("completion_watch delete failed {id}: {e}");
                    }
                }
            });
        }
        Some(group)
    }

    /// Drop `child_id` from a group's expected/completed/deleted sets (the
    /// scoped `agent.cancelSubscriptions` of a grouped watch). At steady
    /// state group settlement is driven exclusively by the grouped watch, so
    /// a cancelled child must also stop gating `is_group_complete` or the
    /// group would stall forever — the surviving siblings' aggregated wake
    /// included. The shrunk group is persisted write-through (best-effort,
    /// like `enroll_child_in_group`); when the expected set becomes empty the
    /// group can never fire, so it is removed outright and its persisted row
    /// swept (best-effort). Returns `true` when the group still exists
    /// afterwards — the caller should `try_fire_group` it, since the shrunk
    /// group may now be sealed AND complete.
    pub(crate) fn remove_child_from_group(&self, group_id: &str, child_id: &AgentId) -> bool {
        let (shrunk, emptied) = {
            let mut guard = self
                .agent_subscriptions
                .lock()
                .expect("agent subscription registry poisoned");
            let Some(idx) = guard
                .delegation_groups
                .iter()
                .position(|g| g.group_id == group_id)
            else {
                return false;
            };
            let g = &mut guard.delegation_groups[idx];
            g.expected_agent_ids.retain(|id| id != child_id);
            g.completed_agent_ids.retain(|id| id != child_id);
            g.deleted_agent_ids.retain(|id| id != child_id);
            if g.expected_agent_ids.is_empty() {
                guard.delegation_groups.remove(idx);
                (None, true)
            } else {
                (Some(g.clone()), false)
            }
        };
        if let Some(g) = shrunk {
            self.persist_delegation_group(&g);
            return true;
        }
        if emptied {
            let store = self.store.clone();
            let gid = group_id.to_string();
            tokio::spawn(async move {
                if let Err(e) = store.delete_delegation_group(&gid).await {
                    tracing::warn!("delegation_group delete failed {gid}: {e}");
                }
            });
        }
        false
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
    /// watch, cancellation).
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
    /// are pruned too — group settlement owns their lifecycle. Idempotent:
    /// watches already present in memory (by id) are skipped.
    ///
    /// After loading, each watch's child is reconciled against current agent
    /// state: a child that completed while the daemon was down delivers its
    /// (synthetic) completion immediately, so the parent is not left waiting
    /// forever.
    ///
    /// Pair uniqueness: rows are processed grouped-first (then ungrouped,
    /// older first within a rank), and a row whose (parent, child) pair is
    /// already watched in memory is pruned — so duplicate rows persisted by
    /// a pre-invariant daemon are coalesced onto the single strongest watch
    /// on upgrade.
    ///
    /// # Errors
    ///
    /// Returns `Error::Internal` if listing the persisted completion watches fails.
    ///
    /// # Panics
    ///
    /// Panics if the internal mutex is poisoned (a prior panic while holding the lock).
    pub async fn heal_completion_watches_on_startup(&self) -> Result<usize> {
        let mut persisted = self.store.list_completion_watches().await?;
        // Strongest-first: grouped (0) < ungrouped (1), with the store's
        // created_at ASC ordering as the tiebreaker (stable sort).
        let rank =
            |p: &intent_store::PersistedCompletionWatch| -> u8 { u8::from(p.group_id.is_none()) };
        persisted.sort_by_key(rank);
        let mut loaded = 0usize;
        let mut to_reconcile: Vec<(AgentId, WorkspaceId)> = Vec::new();
        for p in persisted {
            enum LoadOutcome {
                Loaded(AgentId, WorkspaceId),
                AlreadyInMemory,
                DuplicatePair,
            }
            // Prune when either endpoint is gone: no wake could fire (child
            // deleted watches are handled by reconciliation below instead,
            // since a deleted child IS a completion signal for the parent).
            // A RETIRED parent is equally gone (monorepo#4183): the
            // soft-retire inertness gate rejects every wake, so rehydrating
            // its watch only feeds the delivery-retry loop; `agent.restore`
            // does not resurrect watches, matching the retire sweep.
            let parent_alive = self.agent_is_live(&p.parent_agent_id).await
                && !matches!(
                    self.store
                        .get_agent_session_retired_at(&p.parent_agent_id)
                        .await,
                    Ok(Some(_))
                );
            if !parent_alive {
                tracing::info!(
                    watch = %p.id,
                    parent = %p.parent_agent_id.0,
                    "pruning persisted completion watch — parent agent gone or retired"
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
            let outcome = {
                let mut guard = self
                    .agent_subscriptions
                    .lock()
                    .expect("agent subscription registry poisoned");
                if guard.subscriptions.iter().any(|s| s.id == p.id) {
                    LoadOutcome::AlreadyInMemory
                } else if guard.subscriptions.iter().any(|s| {
                    s.parent_agent_id == p.parent_agent_id && s.child_agent_id == p.child_agent_id
                }) && !p.completion_only
                {
                    // Pair uniqueness: a stronger (grouped) watch for the
                    // same (parent, child) already loaded — this row is a
                    // pre-invariant duplicate.
                    LoadOutcome::DuplicatePair
                } else {
                    let watch = persisted_to_completion_watch(&p);
                    let ids = LoadOutcome::Loaded(
                        watch.child_agent_id.clone(),
                        watch.child_workspace_id.clone(),
                    );
                    guard.subscriptions.push(watch);
                    ids
                }
            };
            let (child_agent, child_ws) = match outcome {
                LoadOutcome::Loaded(ca, cws) => (ca, cws),
                LoadOutcome::AlreadyInMemory => continue,
                LoadOutcome::DuplicatePair => {
                    tracing::info!(
                        watch = %p.id,
                        parent = %p.parent_agent_id.0,
                        child = %p.child_agent_id.0,
                        "pruning persisted completion watch — duplicate (parent, child) pair"
                    );
                    let _ = self.store.delete_completion_watch(&p.id).await;
                    continue;
                }
            };
            loaded += 1;
            to_reconcile.push((child_agent, child_ws));
        }
        // Reconcile: a child that completed (or was deleted) while the daemon
        // was down must still wake its parent. Dedupe children so one synthetic
        // event covers every watch on the same child.
        to_reconcile.sort_by(|a, b| a.0 .0.cmp(&b.0 .0));
        to_reconcile.dedup_by(|a, b| a.0 == b.0);
        for (child_id, child_ws) in to_reconcile {
            self.reconcile_watch_child_on_rehydration(
                &child_id,
                &child_ws,
                WatchReconcileCallSite::Rehydration,
            )
            .await;
        }
        Ok(loaded)
    }

    /// Whether an agent session row exists and is not `Deleted`. Store errors
    /// other than `NotFound` are treated as live (conservative: never prune a
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
    /// failed / was deleted / retired, synthesize the matching completion
    /// event and route it through
    /// [`Services::deliver_completion_to_watches`] so the
    /// parent wakes now instead of waiting for an event that already fired.
    /// Used both at startup rehydration (child settled while the daemon was
    /// down) and at `agent.watch` / `app.agents.waitFor` registration time
    /// (target settled before — or concurrently with — the registration);
    /// `call_site` distinguishes the two (monorepo#2532, see
    /// [`WatchReconcileCallSite`]).
    pub(crate) async fn reconcile_watch_child_on_rehydration(
        &self,
        child_id: &AgentId,
        fallback_ws: &WorkspaceId,
        call_site: WatchReconcileCallSite,
    ) {
        use intent_core::AgentStatus;
        let (event_type, event_ws, status_value, completion_report, stop_reason, agent_name) =
            match self.store.get_agent_session(child_id).await {
                Ok(session) => {
                    let is_deleted = matches!(session.status, AgentStatus::Deleted);
                    // A retired session (`retired_at` set) is inert — no
                    // future completion can fire, so the watch resolves NOW
                    // with a synthetic `agent:retired` (a child that retired
                    // while the daemon was down, or a watch racing a
                    // concurrent retire). Deletion still wins: a deleted row
                    // stays deleted regardless of the retire mark.
                    let is_retired = !is_deleted && session.retired_at.is_some();
                    let is_completed = matches!(session.status, AgentStatus::Completed);
                    let is_failed = matches!(session.status, AgentStatus::Error);
                    // RuntimeIdle: genuinely complete only with a completion
                    // report and no interrupted row (same conservative
                    // predicate as reconcile_group_on_rehydration). A retired
                    // child skips this block entirely — its idle-deferral
                    // early-returns (agent-waiting / hooks / monitors) must
                    // not leave a watch armed on an inert session.
                    let is_idle_complete = if !is_retired
                        && matches!(session.status, AgentStatus::RuntimeIdle)
                    {
                        let has_report = session.completion_report.is_some();
                        let settled = match self.store.get_interrupted_agent(child_id).await {
                            Ok(opt) => has_report && opt.is_none(),
                            Err(e) => {
                                tracing::warn!(
                                    "completion-watch reconciliation: interrupted_agent check \
                                     failed for {}: {e}",
                                    child_id.0
                                );
                                false
                            }
                        };
                        // Agent-waiting deferral (issue intent-hq/monorepo#1468):
                        // an idle child that itself holds live outgoing
                        // completion watches on other agents has not settled —
                        // it will run again when a watched target completes.
                        // Record the interim-skip marker (like the live
                        // delivery path) so the backstops (`agent.unwatch`,
                        // `agent.cancelSubscriptions`) can synthesize the real
                        // completion if the last outgoing watch disappears
                        // without a wake, then leave the watch armed.
                        if settled && self.agent_is_waiting_on_agents(child_id) {
                            self.mark_interim_skipped_idle(child_id);
                            return;
                        }
                        // Registration-time hook/PR-monitor/event-subscription
                        // deferral (monorepo#2532 Gap B; subscriptions added
                        // with monorepo#2972): a NEW explicit watch on a
                        // reported idle child that still owns active hooks,
                        // PR monitors, or live event subscriptions asks for
                        // the child's NEXT completion —
                        // the caller already has the report, so an instant
                        // synthetic idle (whose #1945 report bypass would
                        // skip the live path's deferrals) fires the fresh
                        // watch with the STALE report. Defer like the
                        // agent-waiting branch above: record the interim-skip
                        // marker WITH stale-report provenance — so the
                        // redelivery backstop's own #1945 bypass also keeps
                        // deferring while hooks/monitors remain — and leave
                        // the watch armed; the LAST hook/monitor
                        // terminal-transition backstop
                        // (`redeliver_completion_after_queue_mutation`)
                        // synthesizes the real completion. Boot REHYDRATION
                        // keeps current semantics (report = settlement; a
                        // missed wake must deliver at boot — e.g. SUB-1
                        // sender-watches that get no report-time wake).
                        if settled
                            && matches!(call_site, WatchReconcileCallSite::Registration)
                            && (!self.active_hooks_for_agent(child_id).await.is_empty()
                                || !self.active_pr_monitors_for_agent(child_id).await.is_empty()
                                || !self.list_event_subscriptions_for_agent(child_id).is_empty())
                        {
                            self.mark_interim_skipped_idle_stale_report(child_id);
                            return;
                        }
                        settled
                    } else {
                        false
                    };
                    let event_type = if is_deleted {
                        intent_core::events::AGENT_DELETED
                    } else if is_retired {
                        // Retired outranks failed/completed: whatever the
                        // status was when the mark landed, the session is
                        // inert now and "retired" is the accurate settlement.
                        intent_core::events::AGENT_RETIRED
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
                    (
                        event_type,
                        session.workspace_id,
                        status,
                        session.completion_report,
                        session.stop_reason,
                        Some(session.name),
                    )
                }
                Err(intent_store::Error::NotFound(_)) => (
                    intent_core::events::AGENT_DELETED,
                    fallback_ws.clone(),
                    serde_json::json!("deleted"),
                    None,
                    None,
                    None,
                ),
                Err(e) => {
                    tracing::warn!(
                        "completion-watch reconciliation: session lookup failed for {}: {e}",
                        child_id.0
                    );
                    return;
                }
            };
        let mut data = serde_json::json!({
            "agentId": child_id.0,
            "status": status_value,
        });
        // A synthesized `agent:failed` carries the persisted `stop_reason` as
        // `data.error` — like a live terminal-failure emit — so the wake text
        // names the failure and, critically, the delivery pass can derive the
        // failure's persistent dedup identity (monorepo#2862: the identity
        // guard requires the event-carried error to match the session's
        // persisted stop_reason). Without it a boot/registration replay of a
        // historical failure would fail open and re-deliver.
        if event_type == intent_core::events::AGENT_FAILED {
            if let Some(reason) = &stop_reason {
                data["error"] = serde_json::Value::String(reason.clone());
            }
        }
        // `agentName` enrichment (intent-hq/monorepo#2869): a synthesized
        // completion carries the same name stamp as the live emits, so wake
        // subscribers never render the raw agent id. Omitted (never null)
        // when the session row is gone (NotFound fallback).
        if let Some(name) = agent_name {
            data["agentName"] = serde_json::Value::String(name);
        }
        // Idle-visibility: a synthesized idle carries the same
        // `waitingOnHooks` / `waitingOnPrMonitors` stamps as a live
        // `agent:idle` emit (each omitted when the child owns none), the
        // same emit-time `isWaitingForOtherAgents` flag (pending-watch
        // derivation with the shared `report_delivered` filter, no 2-cycle
        // guard — matching the live emit sites), and the persisted
        // completionReport under the same dual keys (canonical + legacy) —
        // which is also what lets the delivery pass apply the monorepo#1945
        // report bypass to a boot-time synthesized idle.
        if event_type == intent_core::events::AGENT_IDLE {
            if let Some(report) = &completion_report {
                data["completionReport"] = serde_json::Value::String(report.clone());
                data["report"] = serde_json::Value::String(report.clone());
            }
            self.annotate_waiting_on_hooks(child_id, &mut data).await;
            self.annotate_waiting_on_pr_monitors(child_id, &mut data)
                .await;
            data["isWaitingForOtherAgents"] =
                serde_json::json!(!self.waiting_watches_for_parent(child_id).is_empty());
        }
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
            data,
        };
        // No-advisory variant: registration-time / boot reconciliation must
        // never fire the monitoring-idle advisory — a deferred idle here
        // leaves the watch armed, exactly as before the advisory existed.
        self.deliver_completion_to_watches_no_advisory(child_id, &event)
            .await;
    }

    /// Rehydrate undelivered delegation groups on resume (AS-2 rehydration).
    /// Idempotent: skips groups already present in memory (by `group_id`).
    /// `workspace_id` selects which persisted groups to load (the group's
    /// anchor — the parent's home workspace); the loaded groups land in the
    /// daemon-global registry.
    ///
    /// STAB-108 FIX: Reconciles each rehydrated group against current agent state.
    /// If an expected child is already idle/completed (or deleted/missing), records
    /// its completion using the persisted `completion_report`, then fires ready groups.
    pub(crate) async fn rehydrate_delegation_groups(
        &self,
        workspace_id: &WorkspaceId,
    ) -> Result<usize> {
        let persisted = self.store.list_undelivered_groups(workspace_id).await?;
        // Prune groups whose parent is permanently gone (deleted/missing or
        // retired) BEFORE loading them (monorepo#4183): an aggregated wake
        // toward such a parent can never be delivered, so rehydrating the
        // group only feeds the delivery-retry loop from persisted state
        // after every restart. Delete the row so it stays pruned. The
        // liveness probe fails open — a transient store error keeps the
        // group (the delivery-path backstop catches it later).
        let mut survivors = Vec::with_capacity(persisted.len());
        for p in persisted {
            let parent_gone = !self.agent_is_live(&p.parent_agent_id).await
                || matches!(
                    self.store
                        .get_agent_session_retired_at(&p.parent_agent_id)
                        .await,
                    Ok(Some(_)) | Err(intent_store::Error::NotFound(_))
                );
            if parent_gone {
                tracing::info!(
                    group = %p.group_id,
                    parent = %p.parent_agent_id.0,
                    "pruning persisted delegation group — parent agent gone or retired"
                );
                if let Err(e) = self.store.delete_delegation_group(&p.group_id).await {
                    tracing::warn!("delegation_group delete failed {}: {e}", p.group_id);
                }
                continue;
            }
            survivors.push(p);
        }
        let persisted = survivors;
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
    /// For each expected child not already in `completed_agent_ids` or `deleted_agent_ids`,
    /// check if the agent session is idle/completed (or deleted/missing). If so, record
    /// its completion using the persisted `completion_report`.
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
                    // A retired child (`retired_at` set) settles like a
                    // deleted one: the session is inert, no future completion
                    // can fire, so the group must not hang on it. Deletion
                    // still wins for the event kind.
                    let is_retired = !is_deleted && session.retired_at.is_some();
                    let is_explicitly_completed = matches!(session.status, AgentStatus::Completed);
                    let is_failed = matches!(session.status, AgentStatus::Error);

                    let is_idle_and_genuinely_complete = if !is_retired
                        && matches!(session.status, AgentStatus::RuntimeIdle)
                    {
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
                        || is_retired
                        || is_explicitly_completed
                        || is_failed
                        || is_idle_and_genuinely_complete;

                    if should_record {
                        // Build a synthetic agent:idle, agent:failed,
                        // agent:deleted, or agent:retired event.
                        // Prefer the child's persisted completion_report when present
                        let event_type = if is_deleted {
                            intent_core::events::AGENT_DELETED
                        } else if is_retired {
                            intent_core::events::AGENT_RETIRED
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
                            "agentName": session.name,
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
                        // monorepo#1945: a non-empty persisted
                        // completion_report (set by `agent.reportToParent`)
                        // is the child's explicit completion signal — it
                        // records at rehydration regardless of active hooks
                        // or PR monitors (which stay armed; resumed monitors
                        // keep polling and can still wake the settled child
                        // later). Mirrors the live-path bypass in
                        // `deliver_completion_to_watches` /
                        // `record_group_completion_pre_publish`.
                        let completion_reported = session
                            .completion_report
                            .as_deref()
                            .is_some_and(|r| !r.is_empty());
                        // Idle-visibility deferral: an idle child that still
                        // owns active background hooks has not settled — do
                        // NOT record it at rehydration; resumed hooks keep
                        // their original TTL (§5.40), so the child's genuine
                        // completion (post-hook idle / failure / deletion)
                        // records through the live delivery path later.
                        // Failed/deleted children record regardless.
                        if event_type == intent_core::events::AGENT_IDLE
                            && !self
                                .annotate_waiting_on_hooks(&child_id, &mut data)
                                .await
                                .is_empty()
                            && !completion_reported
                        {
                            continue;
                        }
                        // Idle-visibility deferral (unified external-wait,
                        // mirrors the hook check above): an idle child that
                        // still owns active PR monitors has not settled — do
                        // NOT record it at rehydration; the monitor's own
                        // centralized poll loop resumes independently and
                        // wakes the child on its next change/completion, so
                        // the child's genuine completion (post-monitor idle /
                        // failure / deletion) records through the live
                        // delivery path later.
                        if event_type == intent_core::events::AGENT_IDLE
                            && !self
                                .annotate_waiting_on_pr_monitors(&child_id, &mut data)
                                .await
                                .is_empty()
                            && !completion_reported
                        {
                            continue;
                        }
                        // Agent-waiting deferral (issue intent-hq/monorepo#1468):
                        // an idle child that itself holds live outgoing
                        // completion watches on other agents has not settled
                        // either — skip the record (marker recorded, like the
                        // live grouped branch) so the group waits for the
                        // child's genuine completion; the watch-removal
                        // backstops synthesize it if the last outgoing watch
                        // disappears without a wake. The durable variant is
                        // required here: groups rehydrate BEFORE the
                        // completion-watch registry loads at startup, so the
                        // child's outgoing watches may only exist as
                        // persisted rows at this point.
                        if event_type == intent_core::events::AGENT_IDLE
                            && self.agent_is_waiting_on_agents_durable(&child_id).await
                        {
                            self.mark_interim_skipped_idle(&child_id);
                            continue;
                        }
                        // Same emit-time `isWaitingForOtherAgents` stamp as
                        // the live idle emits. Usually false here (a waiting
                        // child was skipped above), but not always: the skip
                        // uses the durable classification WITH the 2-cycle
                        // guard, while this stamp is the in-memory watch
                        // derivation (report_delivered-filtered, no 2-cycle
                        // guard) — a child whose only outgoing watch was
                        // declassified as a mutual-idle 2-cycle reaches this
                        // line and stamps `true`.
                        if event_type == intent_core::events::AGENT_IDLE {
                            data["isWaitingForOtherAgents"] = serde_json::json!(!self
                                .waiting_watches_for_parent(&child_id)
                                .is_empty());
                        }
                        // Record-time trigger capture (intent-hq/monorepo#2044):
                        // same stamp as the live record paths — the child's
                        // linked task plus its recorded flipped completions
                        // (consumed here) — so the aggregated wake keeps the
                        // triggers even if this reconciled child is deleted
                        // before the group fires. Placed AFTER the deferral
                        // checks above so a deferred (not-recorded) idle never
                        // consumes the flip set.
                        if event_type == intent_core::events::AGENT_IDLE {
                            let mut triggers: Vec<(String, String)> = Vec::new();
                            if let Some(n) = &session.task_note_id {
                                triggers.push((session.workspace_id.0.clone(), n.0.clone()));
                            }
                            for pair in self.take_flipped_completion_triggers(&child_id).await {
                                if !triggers.contains(&pair) {
                                    triggers.push(pair);
                                }
                            }
                            crate::agent_ops::ready_delta::stamp_event_trigger_tasks(
                                &mut data, &triggers,
                            );
                        }
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

                        // Record the completion. Retired records in the
                        // deleted bucket (terminal, non-completing — same as
                        // the live delivery path).
                        self.record_group_child_completion(
                            group_id,
                            &child_id,
                            is_deleted || is_retired,
                            summary,
                            event,
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
    /// event becomes observable. Called from the `agent_session` worker loop right
    /// before publishing `agent:idle`.
    pub(crate) async fn record_group_completion_pre_publish(
        &self,
        workspace_id: &WorkspaceId,
        agent_id: &AgentId,
        event_data: &serde_json::Value,
    ) {
        // monorepo#1945: an idle whose event data carries a non-empty
        // completionReport (stamped by the emit sites from the session's
        // persisted report, set exclusively by `agent.reportToParent`) is
        // the child's EXPLICIT completion signal — it records regardless of
        // active hooks or PR monitors, which stay armed (a later fire still
        // wakes the settled child normally). Without this bypass a child
        // that reported and idled while holding a TTL-less PR monitor would
        // starve its group forever (the monitor only terminates on the
        // merge decision the withheld aggregated wake was meant to inform).
        // The agent-waiting deferral below is NOT bypassed (monorepo#1468).
        let completion_reported = crate::event_completion_report(event_data).is_some();
        // Idle-visibility deferral: a hook-waiting idle (the emit sites stamp
        // `waitingOnHooks` onto the data before calling here) is NOT the
        // child's settlement — it will run again when a hook dispatches,
        // fails, or expires (TTL-bounded, §5.40) — so its group completion
        // must not be recorded yet. The genuine completion (post-hook idle,
        // failure, deletion, or the external-cancel redelivery) records
        // through this path or the watch-delivery grouped branch later.
        if !completion_reported
            && event_data
                .get("waitingOnHooks")
                .and_then(serde_json::Value::as_array)
                .is_some_and(|hooks| !hooks.is_empty())
        {
            return;
        }
        // Idle-visibility deferral (unified external-wait, mirrors the hook
        // check above): an agent that owns active PR monitors has not
        // settled — its group completion must not be recorded yet. Not
        // stamped onto `event_data` (internal classification only), so
        // probed live here, matching the agent-waiting check below.
        if !completion_reported && !self.active_pr_monitors_for_agent(agent_id).await.is_empty() {
            return;
        }
        // Agent-waiting deferral (issue intent-hq/monorepo#1468): an idle
        // agent that itself holds live outgoing completion watches on other
        // agents has not settled — it will run again when a watched target
        // completes, so its group completion must not be recorded yet.
        // Classified live (with the 2-cycle deadlock guard) rather than from
        // the emit-time `isWaitingForOtherAgents` stamp, matching the grouped
        // branch of `deliver_completion_to_watches`; the genuine completion
        // (post-wait idle, failure, deletion, or the watch-removal backstop
        // redelivery) records through this path or that branch later.
        if self.agent_is_waiting_on_agents(agent_id) {
            return;
        }
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
            // monorepo#1898: an emit-time payload carrying the report means
            // the child DID report — suppress the suspicion even if the
            // re-read session's persisted report was cleared since, so the
            // recorded line/flag can never contradict its `Report:` clause.
            let stall = if crate::event_carries_report(event_data) {
                None
            } else {
                match session.as_ref() {
                    Some(s) => self.stall_suspicion_for_session(s).await,
                    None => None,
                }
            };
            let attention = session.as_ref().map(|s| {
                (
                    s.attention_request_kind.clone(),
                    s.attention_request_reason.clone(),
                )
            });
            // Record-time trigger capture (intent-hq/monorepo#2044): stamp
            // the settled child's linked task plus its recorded flipped
            // completions (consumed here) onto the recorded event so the
            // aggregated wake keeps the triggers even if the child session
            // is deleted before the group settles.
            let mut triggers: Vec<(String, String)> = Vec::new();
            if let Some(s) = session.as_ref() {
                if let Some(n) = &s.task_note_id {
                    triggers.push((s.workspace_id.0.clone(), n.0.clone()));
                }
            }
            for pair in self.take_flipped_completion_triggers(agent_id).await {
                if !triggers.contains(&pair) {
                    triggers.push(pair);
                }
            }
            let report = session.and_then(|s| s.completion_report);
            let mut data = event_data.clone();
            if let Some(s) = &stall {
                s.annotate_event_data(&mut data);
            }
            if let Some((kind, reason)) = &attention {
                annotate_attention_request(&mut data, kind.as_deref(), reason.as_deref());
            }
            crate::agent_ops::ready_delta::stamp_event_trigger_tasks(&mut data, &triggers);
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
/// attention text into the aggregated group wake as the record (the immediate
/// parent wake already fired at raise time — the alert). No-op when no request
/// is pending — including when a parent reply before settlement already
/// cleared the child's session fields. Shared by all group-record annotation
/// sites (including the
/// completion-watch path in `lib.rs`) so the payload shape and empty-kind
/// guard cannot drift.
pub(crate) fn annotate_attention_request(
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

/// Convert in-memory `CompletionWatch` to persisted form.
fn completion_watch_to_persisted(watch: &CompletionWatch) -> PersistedCompletionWatch {
    PersistedCompletionWatch {
        id: watch.id.clone(),
        parent_workspace_id: watch.parent_workspace_id.clone(),
        child_workspace_id: watch.child_workspace_id.clone(),
        parent_agent_id: watch.parent_agent_id.clone(),
        parent_agent_name: watch.parent_agent_name.clone(),
        child_agent_id: watch.child_agent_id.clone(),
        group_id: watch.group_id.clone(),
        report_delivered: watch.report_delivered,
        wake_on_attention: watch.wake_on_attention,
        completion_only: watch.completion_only,
        created_at: watch.created_at.clone(),
    }
}

/// Convert a persisted row back to the in-memory form.
fn persisted_to_completion_watch(p: &PersistedCompletionWatch) -> CompletionWatch {
    CompletionWatch {
        id: p.id.clone(),
        parent_workspace_id: p.parent_workspace_id.clone(),
        child_workspace_id: p.child_workspace_id.clone(),
        parent_agent_id: p.parent_agent_id.clone(),
        parent_agent_name: p.parent_agent_name.clone(),
        child_agent_id: p.child_agent_id.clone(),
        group_id: p.group_id.clone(),
        created_at: p.created_at.clone(),
        // Compatibility: older daemons persisted this bit after a progress
        // report and then suppressed the terminal idle wake. Progress no longer
        // consumes completion interest, so rehydration deliberately re-arms
        // those legacy rows.
        report_delivered: false,
        wake_on_attention: p.wake_on_attention,
        completion_only: p.completion_only,
        // In-memory only: after a restart the terminal wake falls back to
        // rendering the full report (fail-open, monorepo#4026).
        delivered_report_ts: None,
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
        sealed: p.sealed,
        delivered: p.delivered,
        event_summaries: p.event_summaries.clone(),
        raw_events,
    })
}

#[cfg(test)]
mod tests {
    use std::path::PathBuf;

    use intent_core::{
        AgentSession, AgentStatus, Workspace, WorkspaceActivity, WorkspaceAttention,
        WorkspaceDisplayStatus, WorkspaceStatus,
    };
    use intent_store::Store;

    use super::*;

    struct TempDb {
        path: PathBuf,
    }

    impl TempDb {
        fn new() -> Self {
            let path =
                std::env::temp_dir().join(format!("intentd-subs-{}.db", uuid::Uuid::new_v4()));
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

    fn workspace(id: &WorkspaceId) -> Workspace {
        let ts = now_iso();
        Workspace {
            id: id.clone(),
            title: "WS".to_string(),
            branch: "main".to_string(),
            base_ref: None,
            base_commit_sha: None,
            status: WorkspaceStatus::Active,
            status_message: None,
            status_image_asset_id: None,
            activity: WorkspaceActivity::Idle,
            attention: WorkspaceAttention::None,
            created_at: ts.clone(),
            updated_at: ts,
            last_activity: None,
            tags: vec![],
            path: None,
            repository_path: None,
            repository_owner: None,
            repository_name: None,
            worktree_path: None,
            scope: None,
            skip_worktree: false,
            setup_script: None,
            setup_result: None,
            is_remote: false,
            default_model: None,
            pr_number: None,
            pr_url: None,
            pr_status: None,
            active_pull_request: None,
            pull_requests: None,
            context_links: None,
            archived: false,
            archived_at: None,
            task_stats: None,
            agent_summary: None,
            diff_summary: None,
            token_usage: None,
            cow_supported: None,
            display_status: None,
            waiting: false,
            checkout_mode: None,
            disk_usage: None,
            pending_delete_at: None,
        }
    }

    fn agent(ws: &WorkspaceId, id: &str, parent: Option<&str>, background: bool) -> AgentSession {
        AgentSession {
            harness_version: intent_core::CURRENT_HARNESS_VERSION.to_string(),
            harness_features: None,
            id: AgentId::from(id),
            workspace_id: ws.clone(),
            parent_agent_id: parent.map(AgentId::from),
            backend_session_id: None,
            acp_session_id: None,
            name: id.to_string(),
            name_explicitly_set: true,
            model: None,
            reasoning_effort: None,
            effort_levels: None,
            provider: None,
            system_prompt: None,
            specialist: None,
            status: AgentStatus::Active,
            is_active: false,
            messages: vec![],
            stats: None,
            task_note_id: None,
            skip_auto_commit: false,
            completion_report: None,
            completion_report_timestamp: None,
            attention_request_kind: None,
            attention_request_reason: None,
            attention_request_timestamp: None,
            delegation_depth: None,
            initial_message: None,
            context_references: None,
            image_blocks: None,
            file_blocks: None,
            is_background: background,
            metadata: None,
            created_at: now_iso(),
            updated_at: now_iso(),
            sandbox_id: None,
            sandbox_path: None,
            sandbox_branch: None,
            stop_reason: None,
            stop_reason_timestamp: None,
            session_corrupted: false,
            pending_delete_at: None,
            retired_at: None,
        }
    }

    /// Store + Services + workspace with a top-level parent (`agent-parent`)
    /// and its delegated child (`agent-child`). The temp workspaces root
    /// keeps the cowSupported probe hermetic (mirrors the `hook_manager` test
    /// setup).
    async fn setup() -> (TempDb, tempfile::TempDir, Services, WorkspaceId) {
        let tmp = TempDb::new();
        let store = Store::open(&tmp.path).await.expect("open store");
        let ws = WorkspaceId::new();
        store.insert_workspace(&workspace(&ws)).await.expect("ws");
        store
            .insert_agent_session(&agent(&ws, "agent-parent", None, false))
            .await
            .expect("parent");
        store
            .insert_agent_session(&agent(&ws, "agent-child", Some("agent-parent"), false))
            .await
            .expect("child");
        let root = tempfile::tempdir().expect("temp workspaces root");
        let services = Services::new(store).with_workspaces_root(root.path().to_path_buf());
        (tmp, root, services, ws)
    }

    async fn enriched(svc: &Services, ws: &WorkspaceId) -> (WorkspaceDisplayStatus, bool) {
        let mut row = svc.store().get_workspace(ws).await.expect("get ws");
        svc.enrich_workspace_aggregates(&mut row).await;
        (
            row.display_status.expect("display_status computed"),
            row.waiting,
        )
    }

    /// Refresh the last-observed `waiting` baseline after a direct registry
    /// mutation. Production watch mutations always route through
    /// [`Services::publish_subscriptions_changed`], which runs this recompute;
    /// tests that poke the registry directly must mirror it because the
    /// list/get enrichment serves `waiting` from the last-observed cache
    /// (rung 1 of the derived-field ladder), probing the store only on a
    /// cache miss.
    async fn recompute_waiting(svc: &Services, ws: &WorkspaceId) {
        svc.maybe_emit_waiting_changed(ws).await;
    }

    /// An armed completion watch held by an idle top-level parent sets the
    /// orthogonal `waiting` flag on the list/get enrichment path without
    /// promoting the derived `displayStatus` — for ungrouped and grouped
    /// (`after_all`) watches alike.
    #[tokio::test]
    async fn armed_watch_sets_waiting_without_promoting_display_status() {
        let (_tmp, _root, svc, ws) = setup().await;
        assert!(!svc.workspace_has_waiting_agent_subscriptions(&ws).await);
        assert_eq!(
            enriched(&svc, &ws).await,
            (WorkspaceDisplayStatus::Idle, false)
        );
        let ungrouped = svc
            .register_completion_watch(
                &ws,
                &ws,
                AgentId::from("agent-parent"),
                "agent-parent".to_string(),
                AgentId::from("agent-child"),
                None,
            )
            .expect("register");
        recompute_waiting(&svc, &ws).await;
        assert!(svc.workspace_has_waiting_agent_subscriptions(&ws).await);
        assert_eq!(
            enriched(&svc, &ws).await,
            (WorkspaceDisplayStatus::Idle, true),
            "idle parent with an armed watch reads waiting on the base rollup"
        );
        // A grouped (`after_all`) watch counts through the same registry.
        svc.register_completion_watch(
            &ws,
            &ws,
            AgentId::from("agent-parent"),
            "agent-parent".to_string(),
            AgentId::from("agent-child-2"),
            Some("group-1".to_string()),
        )
        .expect("register grouped");
        assert!(svc.remove_watch(&ungrouped));
        recompute_waiting(&svc, &ws).await;
        assert!(svc.workspace_has_waiting_agent_subscriptions(&ws).await);
        assert_eq!(
            enriched(&svc, &ws).await,
            (WorkspaceDisplayStatus::Idle, true),
            "a grouped watch reads waiting too"
        );
    }

    /// Retiring the last watch drops the `waiting` flag; the base rollup is
    /// unchanged throughout.
    #[tokio::test]
    async fn retired_watch_drops_waiting() {
        let (_tmp, _root, svc, ws) = setup().await;
        let id = svc
            .register_completion_watch(
                &ws,
                &ws,
                AgentId::from("agent-parent"),
                "agent-parent".to_string(),
                AgentId::from("agent-child"),
                None,
            )
            .expect("register");
        assert_eq!(
            enriched(&svc, &ws).await,
            (WorkspaceDisplayStatus::Idle, true)
        );
        assert!(svc.remove_watch(&id));
        recompute_waiting(&svc, &ws).await;
        assert!(!svc.workspace_has_waiting_agent_subscriptions(&ws).await);
        assert_eq!(
            enriched(&svc, &ws).await,
            (WorkspaceDisplayStatus::Idle, false),
            "retired watches never read waiting"
        );
    }

    /// A `report_delivered` watch stops reading as waiting: once the
    /// report-time wake fired, the parent no longer reads as pending work —
    /// matching the agent-waiting projection and settlement predicate
    /// (monorepo#1649).
    #[tokio::test]
    async fn report_delivered_watch_does_not_read_waiting() {
        let (_tmp, _root, svc, ws) = setup().await;
        let id = svc
            .register_completion_watch(
                &ws,
                &ws,
                AgentId::from("agent-parent"),
                "agent-parent".to_string(),
                AgentId::from("agent-child"),
                None,
            )
            .expect("register");
        assert!(svc.workspace_has_waiting_agent_subscriptions(&ws).await);
        assert!(svc.mark_watch_report_delivered(&id));
        recompute_waiting(&svc, &ws).await;
        assert!(!svc.workspace_has_waiting_agent_subscriptions(&ws).await);
        assert_eq!(
            enriched(&svc, &ws).await,
            (WorkspaceDisplayStatus::Idle, false),
            "report_delivered watches must not read waiting"
        );
    }

    /// Watches held by child or background agents never count — only
    /// top-level sessions do (same filter as `workspace_attention_signals`).
    #[tokio::test]
    async fn child_or_background_held_watch_does_not_read_waiting() {
        let (_tmp, _root, svc, ws) = setup().await;
        svc.store()
            .insert_agent_session(&agent(&ws, "agent-bg", None, true))
            .await
            .expect("background agent");
        // The mid-level child delegates its own grandchild…
        svc.register_completion_watch(
            &ws,
            &ws,
            AgentId::from("agent-child"),
            "agent-child".to_string(),
            AgentId::from("agent-grandchild"),
            None,
        )
        .expect("register child-held");
        // …and a background session watches one too.
        svc.register_completion_watch(
            &ws,
            &ws,
            AgentId::from("agent-bg"),
            "agent-bg".to_string(),
            AgentId::from("agent-bg-child"),
            None,
        )
        .expect("register background-held");
        assert!(!svc.workspace_has_waiting_agent_subscriptions(&ws).await);
        assert_eq!(
            enriched(&svc, &ws).await,
            (WorkspaceDisplayStatus::Idle, false),
            "child/background-held watches must not read waiting"
        );
    }

    /// A watch anchors in the parent's HOME workspace: a chief-workspace
    /// parent watching a child in `ws` reads waiting on chief, never `ws`.
    #[tokio::test]
    async fn watch_anchors_in_parents_home_workspace() {
        let (_tmp, _root, svc, ws) = setup().await;
        svc.register_completion_watch(
            &WorkspaceId::chief(),
            &ws,
            AgentId::from("agent-chief"),
            "agent-chief".to_string(),
            AgentId::from("agent-child"),
            None,
        )
        .expect("register cross-workspace");
        assert!(
            !svc.workspace_has_waiting_agent_subscriptions(&ws).await,
            "the child's workspace must not read the chief-anchored watch"
        );
        assert_eq!(
            enriched(&svc, &ws).await,
            (WorkspaceDisplayStatus::Idle, false)
        );
    }
}
