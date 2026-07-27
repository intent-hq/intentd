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

use intent_core::{now_iso, AgentId, Event, WorkspaceId};
use intent_store::PersistedEventSubscription;
use uuid::Uuid;

use crate::events::{self, SubscriptionFilter};
use crate::Services;

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
    /// Register a subscription: resolve wildcards, insert into the registry,
    /// spawn its delivery task, and (for agent-owned subscriptions)
    /// write-through persist. Returns `(subscription_id, resolved_types)`.
    pub(crate) fn register_event_subscription(
        &self,
        workspace_id: &WorkspaceId,
        subscriber_agent_id: Option<AgentId>,
        event_types: &[String],
        exclude_self: Option<bool>,
        batch_window: Option<i64>,
    ) -> (String, Vec<String>) {
        let record = EventSubscriptionRecord {
            id: Uuid::new_v4().to_string(),
            workspace_id: workspace_id.clone(),
            subscriber_agent_id,
            event_types: events::resolve_event_types(event_types),
            // TS default: excludeSelf true (subscribers skip their own events).
            exclude_self: exclude_self.unwrap_or(true),
            batch_window_ms: normalize_batch_window_ms(batch_window),
            created_at: now_iso(),
        };
        self.insert_event_subscription(record.clone());
        self.persist_event_subscription(&record);
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
    /// persisted row. Returns `false` when the id is unknown.
    pub(crate) fn remove_event_subscription(&self, subscription_id: &str) -> bool {
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
            self.delete_persisted_event_subscription(&entry.record.id);
        }
        true
    }

    /// Remove every subscription owned by `agent_id` (subscriber deleted) —
    /// aborts delivery tasks and clears the persisted rows.
    pub(crate) fn remove_event_subscriptions_for_agent(&self, agent_id: &AgentId) -> usize {
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
            let store = self.store.clone();
            let agent = agent_id.clone();
            tokio::spawn(async move {
                if let Err(e) = store.delete_event_subscriptions_for_agent(&agent).await {
                    tracing::warn!(
                        "event_subscription delete for agent {} failed: {e}",
                        agent.0
                    );
                }
            });
        }
        removed.len()
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

    /// Best-effort write-through persist of an agent-owned subscription
    /// (restart durability). Mirrors `Services::persist_completion_watch`:
    /// spawns an async persist task, not durable-before-observable — the
    /// crash window between in-memory registration and commit is
    /// milliseconds and the subscriber can re-subscribe. Front-door
    /// subscriptions (no subscriber agent) are in-memory only.
    fn persist_event_subscription(&self, record: &EventSubscriptionRecord) {
        let Some(persisted) = record_to_persisted(record) else {
            return;
        };
        let store = self.store.clone();
        tokio::spawn(async move {
            let id = persisted.id.clone();
            if let Err(e) = store.upsert_event_subscription(&persisted).await {
                tracing::warn!("event_subscription upsert failed {id}: {e}");
            }
        });
    }

    /// Best-effort async delete of a persisted subscription row
    /// (`event.unsubscribe`).
    fn delete_persisted_event_subscription(&self, subscription_id: &str) {
        let store = self.store.clone();
        let id = subscription_id.to_string();
        tokio::spawn(async move {
            if let Err(e) = store.delete_event_subscription(&id).await {
                tracing::warn!("event_subscription delete failed {id}: {e}");
            }
        });
    }

    /// Rehydrate persisted event subscriptions at daemon startup: load every
    /// surviving row, prune rows whose subscriber agent is gone (deleted or
    /// missing — no wake could ever be delivered), and load the rest into
    /// the registry, spawning each one's delivery task. Idempotent:
    /// subscriptions already present in memory (by id) are skipped.
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
fn format_event_subscription_wake(events: &[&Event]) -> String {
    let mut seen: std::collections::HashSet<&str> = std::collections::HashSet::new();
    let mut types: Vec<&str> = Vec::new();
    for e in events {
        if seen.insert(e.event_type.as_str()) {
            types.push(e.event_type.as_str());
        }
    }
    format!(
        "[WORKSPACE EVENTS] {} event(s) matched your subscription: {}.",
        events.len(),
        types.join(", ")
    )
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
        batch_window_ms: p.batch_window_ms,
        created_at: p.created_at.clone(),
    }
}
