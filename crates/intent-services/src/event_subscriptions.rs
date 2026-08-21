//! Event-subscription registry + delivery (monorepo#937).
//!
//! Backs the deprecated-alias `event.subscribe` / `event.unsubscribe`
//! service surface (and the `agent.subscribe` / `agent.unsubscribe`
//! aliases): a daemon-global in-memory registry of live subscriptions, each
//! with its own bus delivery task that matches published workspace events
//! (category wildcards + exact types via [`crate::events::SubscriptionFilter`]),
//! coalesces them over the subscription's `batchWindow` (default 500ms), and
//! delivers one wake message per batch to the subscriber agent.
//!
//! Persistence mirrors the completion-watch pattern
//! (`agent_subscriptions.rs`): agent-owned subscriptions are written through
//! to the `event_subscription` table (best-effort async — NOT
//! durable-before-observable) and rehydrated by
//! [`Services::heal_event_subscriptions_on_startup`]; rows whose subscriber
//! agent is gone are pruned. Subscriptions with no subscriber agent (front
//! door callers) are in-memory only — there is no wake target to survive a
//! restart for.

use std::time::Duration;

use intent_core::{now_iso, AgentId, Error, Event, Result, WorkspaceId};
use intent_store::PersistedEventSubscription;
use uuid::Uuid;

use crate::events::{self, SubscriptionFilter};
use crate::Services;

/// Subscribe-time guard for AGENT callers (monorepo#1229): every explicit
/// `agent:`-prefixed entry (exact types, the `agent:*` wildcard, and the
/// observability events) and `chat:stream:delta` is rejected with
/// `InvalidParams` — agents watch other agents via `ws.agent.watch`, not the
/// event bus. A bare `*` passes: it is silently narrowed to the non-agent
/// categories at resolution time ([`events::resolve_event_types_for_agent`]).
pub(crate) fn validate_agent_event_types(event_types: &[String]) -> Result<()> {
    for t in event_types {
        if events::is_agent_restricted_event_type(t) {
            return Err(Error::InvalidParams(format!(
                "\"{t}\" is not subscribable by agents: agent events are internal. Use \
                 ws.agent.watch(agentId) to be woken when another agent completes, fails, \
                 or raises a blocker/discussion; non-agent categories (\"file:*\", \
                 \"task:*\", \"git:*\", \"note:*\", ...) remain available."
            )));
        }
    }
    Ok(())
}

/// One live `event.subscribe` registration.
#[derive(Debug, Clone)]
pub(crate) struct EventSubscriptionRecord {
    pub id: String,
    /// Workspace whose events this subscription matches — also where the
    /// wake is delivered.
    pub workspace_id: WorkspaceId,
    /// Wake target. `None` (front-door caller): the subscription is
    /// registered for id-tracking parity but delivers nowhere.
    pub subscriber_agent_id: Option<AgentId>,
    /// Resolved event-type patterns (bare `*` already expanded).
    pub event_types: Vec<String>,
    pub exclude_self: bool,
    pub batch_window_ms: i64,
    pub created_at: String,
}

/// Registry slot: the record plus the running bus delivery task (absent when
/// no event bus is wired or the subscription has no wake target).
#[derive(Debug)]
pub(crate) struct EventSubscriptionEntry {
    pub record: EventSubscriptionRecord,
    task: Option<tokio::task::JoinHandle<()>>,
}

impl Services {
    /// Validate a caller-supplied subscriber identity before registering
    /// (fail closed, mirroring the monorepo#568 `agent.watchCompletion`
    /// precedent): the agent must exist and not be deleted, and a subscriber
    /// outside `workspace_id` is only allowed from the chief workspace (the
    /// same scope rule as `check_watch_scope`). Transient store errors are
    /// treated as valid — never reject a live subscribe on a flaky read.
    pub(crate) async fn validate_event_subscriber(
        &self,
        workspace_id: &WorkspaceId,
        subscriber: &AgentId,
    ) -> Result<()> {
        match self.store.get_agent_session(subscriber).await {
            Ok(session) => {
                if matches!(session.status, intent_core::AgentStatus::Deleted) {
                    return Err(Error::InvalidParams(format!(
                        "subscriber agent {} is deleted",
                        subscriber.0
                    )));
                }
                if &session.workspace_id != workspace_id && !session.workspace_id.is_chief() {
                    return Err(Error::InvalidParams(format!(
                        "subscriber agent {} belongs to workspace {}, not {}; only \
                         chief-workspace agents may subscribe to another workspace's events",
                        subscriber.0, session.workspace_id.0, workspace_id.0
                    )));
                }
                Ok(())
            }
            Err(intent_store::Error::NotFound(_)) => Err(Error::InvalidParams(format!(
                "subscriber agent {} not found",
                subscriber.0
            ))),
            Err(e) => {
                tracing::warn!(
                    "event.subscribe: subscriber liveness check failed for {}: {e}",
                    subscriber.0
                );
                Ok(())
            }
        }
    }

    /// Register a subscription: resolve wildcards, insert into the registry,
    /// spawn its delivery task, and (for agent-owned subscriptions) persist
    /// the row with an AWAITED upsert — the row is committed before this
    /// returns, so an immediately following `event.unsubscribe` (whose
    /// delete is also awaited) can never lose the INSERT/DELETE race and
    /// resurrect the subscription on the next restart. A failed persist only
    /// logs: the in-memory subscription still delivers live. Returns
    /// `(subscription_id, resolved_types)`.
    pub(crate) async fn register_event_subscription(
        &self,
        workspace_id: &WorkspaceId,
        subscriber_agent_id: Option<AgentId>,
        event_types: &[String],
        exclude_self: Option<bool>,
        batch_window: Option<i64>,
    ) -> (String, Vec<String>) {
        // Agent subscribers get the narrowed bare-`*` expansion (no `agent:*`,
        // monorepo#1229); front-door subscriptions keep the full category list.
        let resolved_types = if subscriber_agent_id.is_some() {
            events::resolve_event_types_for_agent(event_types)
        } else {
            events::resolve_event_types(event_types)
        };
        let record = EventSubscriptionRecord {
            id: Uuid::new_v4().to_string(),
            workspace_id: workspace_id.clone(),
            subscriber_agent_id,
            event_types: resolved_types,
            // TS default: excludeSelf true (subscribers skip their own events).
            exclude_self: exclude_self.unwrap_or(true),
            batch_window_ms: normalize_batch_window_ms(batch_window),
            created_at: now_iso(),
        };
        self.insert_event_subscription(record.clone());
        if let Some(persisted) = record_to_persisted(&record) {
            if let Err(e) = self.store.upsert_event_subscription(&persisted).await {
                tracing::warn!("event_subscription upsert failed {}: {e}", record.id);
            }
        }
        (record.id, record.event_types)
    }

    /// Insert one record into the registry, spawning its delivery task.
    /// Shared by live registration and startup rehydration.
    fn insert_event_subscription(&self, record: EventSubscriptionRecord) {
        let task = self.spawn_event_subscription_delivery(&record);
        self.event_subscriptions
            .lock()
            .expect("event subscription registry poisoned")
            .insert(record.id.clone(), EventSubscriptionEntry { record, task });
    }

    /// Remove a subscription: abort its delivery task and delete the
    /// persisted row (AWAITED, so the registration-time awaited upsert and
    /// this delete are strictly ordered — no resurrection on restart).
    /// Returns `false` when the id is unknown.
    pub(crate) async fn remove_event_subscription(&self, subscription_id: &str) -> bool {
        let entry = self
            .event_subscriptions
            .lock()
            .expect("event subscription registry poisoned")
            .remove(subscription_id);
        let Some(entry) = entry else {
            return false;
        };
        if let Some(task) = entry.task {
            task.abort();
        }
        if entry.record.subscriber_agent_id.is_some() {
            if let Err(e) = self.store.delete_event_subscription(&entry.record.id).await {
                tracing::warn!("event_subscription delete failed {}: {e}", entry.record.id);
            }
        }
        true
    }

    /// Remove every subscription owned by `agent_id` (subscriber deleted /
    /// `agent.cancelSubscriptions`) — aborts delivery tasks and clears the
    /// persisted rows (AWAITED, same INSERT/DELETE ordering guarantee as
    /// [`Services::remove_event_subscription`]).
    pub(crate) async fn remove_event_subscriptions_for_agent(&self, agent_id: &AgentId) -> usize {
        let removed: Vec<EventSubscriptionEntry> = {
            let mut guard = self
                .event_subscriptions
                .lock()
                .expect("event subscription registry poisoned");
            let ids: Vec<String> = guard
                .iter()
                .filter(|(_, e)| e.record.subscriber_agent_id.as_ref() == Some(agent_id))
                .map(|(id, _)| id.clone())
                .collect();
            ids.iter().filter_map(|id| guard.remove(id)).collect()
        };
        for entry in &removed {
            if let Some(task) = &entry.task {
                task.abort();
            }
        }
        if !removed.is_empty() {
            if let Err(e) = self
                .store
                .delete_event_subscriptions_for_agent(agent_id)
                .await
            {
                tracing::warn!(
                    "event_subscription delete for agent {} failed: {e}",
                    agent_id.0
                );
            }
        }
        removed.len()
    }

    /// Remove every subscription scoped to `workspace_id` (workspace deleted
    /// — the workspace-scoped filter can never match again, monorepo#947).
    /// Aborts delivery tasks and clears the persisted rows (AWAITED, same
    /// INSERT/DELETE ordering guarantee as
    /// [`Services::remove_event_subscription`]).
    pub(crate) async fn remove_event_subscriptions_for_workspace(
        &self,
        workspace_id: &WorkspaceId,
    ) -> usize {
        let removed: Vec<EventSubscriptionEntry> = {
            // Recover through a poisoned lock (`into_inner`), unlike the
            // panicking `.expect()` used elsewhere in this file: this runs on
            // the workspace-delete path, the last chance to unlink this
            // state, so best-effort teardown outweighs propagating a
            // mutex-poison panic (mirrors the `agent_subscriptions` sweep in
            // `delete_workspace`).
            let mut guard = self
                .event_subscriptions
                .lock()
                .unwrap_or_else(std::sync::PoisonError::into_inner);
            let ids: Vec<String> = guard
                .iter()
                .filter(|(_, e)| &e.record.workspace_id == workspace_id)
                .map(|(id, _)| id.clone())
                .collect();
            ids.iter().filter_map(|id| guard.remove(id)).collect()
        };
        for entry in &removed {
            if let Some(task) = &entry.task {
                task.abort();
            }
        }
        // Unconditional row delete: a persisted row may exist without a live
        // registry entry (rehydration raced or was skipped), and the delete
        // is idempotent.
        if let Err(e) = self
            .store
            .delete_event_subscriptions_for_workspace(workspace_id)
            .await
        {
            tracing::warn!(
                "event_subscription delete for workspace {} failed: {e}",
                workspace_id.0
            );
        }
        removed.len()
    }

    /// Snapshot the live subscriptions owned by `agent_id` (introspection —
    /// `agent.getSubscriptions`, monorepo#947), oldest first.
    pub(crate) fn list_event_subscriptions_for_agent(
        &self,
        agent_id: &AgentId,
    ) -> Vec<EventSubscriptionRecord> {
        let mut records: Vec<EventSubscriptionRecord> = self
            .event_subscriptions
            .lock()
            .expect("event subscription registry poisoned")
            .values()
            .filter(|e| e.record.subscriber_agent_id.as_ref() == Some(agent_id))
            .map(|e| e.record.clone())
            .collect();
        records.sort_by(|a, b| a.created_at.cmp(&b.created_at));
        records
    }

    /// Snapshot the live subscriptions scoped to `workspace_id`
    /// (introspection — `agent.diagnostics`, monorepo#947), oldest first.
    pub(crate) fn list_event_subscriptions_for_workspace(
        &self,
        workspace_id: &WorkspaceId,
    ) -> Vec<EventSubscriptionRecord> {
        let mut records: Vec<EventSubscriptionRecord> = self
            .event_subscriptions
            .lock()
            .expect("event subscription registry poisoned")
            .values()
            .filter(|e| &e.record.workspace_id == workspace_id)
            .map(|e| e.record.clone())
            .collect();
        records.sort_by(|a, b| a.created_at.cmp(&b.created_at));
        records
    }

    /// Spawn the per-subscription bus delivery task: match events against the
    /// subscription's filter, coalesce over its batch window, and wake the
    /// subscriber once per batch. Returns `None` when no event bus is wired
    /// or the subscription has no wake target (nothing to deliver to).
    fn spawn_event_subscription_delivery(
        &self,
        record: &EventSubscriptionRecord,
    ) -> Option<tokio::task::JoinHandle<()>> {
        let bus = self.event_bus.clone()?;
        let subscriber = record.subscriber_agent_id.clone()?;
        let filter = SubscriptionFilter {
            event_types: record.event_types.clone(),
            exclude_actor_ids: if record.exclude_self {
                vec![subscriber.0.clone()]
            } else {
                Vec::new()
            },
            workspace_id: Some(record.workspace_id.0.clone()),
            batch_window: Some(Duration::from_millis(record.batch_window_ms as u64)),
            // Match-time guard (monorepo#1229): agent-owned subscriptions —
            // including rehydrated rows persisted before the subscribe-time
            // guard existed — never match agent events or `chat:stream:delta`.
            exclude_agent_events: true,
            ..Default::default()
        };
        let services = self.clone();
        let workspace_id = record.workspace_id.clone();
        let subscription_id = record.id.clone();
        Some(tokio::spawn(async move {
            let mut sub = bus.subscribe(filter);
            while let Some(batch) = sub.recv().await {
                if batch.is_empty() {
                    continue;
                }
                let refs: Vec<&Event> = batch.iter().collect();
                let wake = format_event_subscription_wake(&refs);
                let metadata = crate::build_event_notification_metadata(&refs);
                if let Err(e) = services
                    .deliver_parent_wake(&workspace_id, subscriber.clone(), wake, Some(metadata))
                    .await
                {
                    tracing::warn!(
                        error = %e,
                        subscriber = %subscriber.0,
                        subscription = %subscription_id,
                        "failed to deliver event-subscription wake"
                    );
                }
            }
        }))
    }

    /// Rehydrate persisted event subscriptions at daemon startup: load every
    /// surviving row, prune rows whose subscriber agent is gone (deleted or
    /// missing — no wake could ever be delivered) or whose workspace no
    /// longer exists (the workspace-scoped filter can never match again,
    /// monorepo#947; the `__chief__` anchor is exempt — it has no workspace
    /// row by design), and load the rest into the registry, spawning each
    /// one's delivery task. Idempotent: subscriptions already present in
    /// memory (by id) are skipped.
    ///
    /// # Errors
    ///
    /// Returns `Error::Internal` if listing the persisted event subscriptions fails.
    ///
    /// # Panics
    ///
    /// Panics if the internal mutex is poisoned (a prior panic while holding the lock).
    pub async fn heal_event_subscriptions_on_startup(&self) -> intent_core::Result<usize> {
        let persisted = self.store.list_event_subscriptions().await?;
        let mut loaded = 0usize;
        for p in persisted {
            if !self.agent_is_live(&p.subscriber_agent_id).await {
                tracing::info!(
                    subscription = %p.id,
                    subscriber = %p.subscriber_agent_id.0,
                    "pruning persisted event subscription — subscriber agent gone"
                );
                let _ = self.store.delete_event_subscription(&p.id).await;
                continue;
            }
            if !self
                .workspace_exists_for_subscription(&p.workspace_id)
                .await
            {
                tracing::info!(
                    subscription = %p.id,
                    workspace = %p.workspace_id.0,
                    "pruning persisted event subscription — workspace gone"
                );
                let _ = self.store.delete_event_subscription(&p.id).await;
                continue;
            }
            let already_live = self
                .event_subscriptions
                .lock()
                .expect("event subscription registry poisoned")
                .contains_key(&p.id);
            if already_live {
                continue;
            }
            self.insert_event_subscription(persisted_to_record(&p));
            loaded += 1;
        }
        Ok(loaded)
    }

    /// Whether a persisted subscription's workspace still exists. The
    /// `__chief__` anchor has no workspace row and is always kept; transient
    /// store errors are treated as existing — never prune a row on a flaky
    /// read (mirrors [`Services::agent_is_live`]).
    async fn workspace_exists_for_subscription(&self, workspace_id: &WorkspaceId) -> bool {
        if workspace_id.is_chief() {
            return true;
        }
        match self.store.get_workspace(workspace_id).await {
            Ok(_) => true,
            Err(intent_store::Error::NotFound(_)) => false,
            Err(e) => {
                tracing::warn!(
                    "event-subscription rehydration: workspace check failed for {}: {e}",
                    workspace_id.0
                );
                true
            }
        }
    }
}

/// Normalize the caller-supplied `batchWindow` (ms): non-positive / absent
/// values fall back to the default (TS `batchWindow || 500`).
fn normalize_batch_window_ms(batch_window: Option<i64>) -> i64 {
    match batch_window {
        Some(ms) if ms > 0 => ms,
        _ => events::DEFAULT_BATCH_WINDOW.as_millis() as i64,
    }
}

/// Human-readable wake text summarizing one delivered batch (the FE-visible
/// `event_notification` metadata carries the structured per-event payload).
/// Wording owned by the harness (H6); this resolves the deduplicated
/// first-seen type list.
pub(crate) fn format_event_subscription_wake(events: &[&Event]) -> String {
    let mut seen: std::collections::HashSet<&str> = std::collections::HashSet::new();
    let mut types: Vec<&str> = Vec::new();
    for e in events {
        if seen.insert(e.event_type.as_str()) {
            types.push(e.event_type.as_str());
        }
    }
    crate::harness::latest().event_subscription_wake(events.len(), &types)
}

/// In-memory → persisted row. `None` for front-door subscriptions (no
/// subscriber agent): they are never persisted.
fn record_to_persisted(record: &EventSubscriptionRecord) -> Option<PersistedEventSubscription> {
    Some(PersistedEventSubscription {
        id: record.id.clone(),
        workspace_id: record.workspace_id.clone(),
        subscriber_agent_id: record.subscriber_agent_id.clone()?,
        event_types: record.event_types.clone(),
        exclude_self: record.exclude_self,
        batch_window_ms: record.batch_window_ms,
        created_at: record.created_at.clone(),
    })
}

fn persisted_to_record(p: &PersistedEventSubscription) -> EventSubscriptionRecord {
    EventSubscriptionRecord {
        id: p.id.clone(),
        workspace_id: p.workspace_id.clone(),
        subscriber_agent_id: Some(p.subscriber_agent_id.clone()),
        event_types: p.event_types.clone(),
        exclude_self: p.exclude_self,
        // Re-run the registration-time guard: a non-positive stored window
        // (hand-edited DB, migration bug) would otherwise wrap through the
        // `as u64` cast into an astronomically large batch window.
        batch_window_ms: normalize_batch_window_ms(Some(p.batch_window_ms)),
        created_at: p.created_at.clone(),
    }
}
