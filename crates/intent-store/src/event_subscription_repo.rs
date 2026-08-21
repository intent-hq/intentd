//! Event-subscription repository: CRUD for persisted `event.subscribe` /
//! `agent.subscribe` (deprecated alias) service subscriptions (monorepo#937).
//!
//! Registration persists the row via a best-effort async write-through (NOT
//! durable-before-observable — see `Services::persist_event_subscription`);
//! `event.unsubscribe` and subscriber-agent deletion delete it. On startup
//! the daemon rehydrates surviving rows into the in-memory registry
//! (`event_subscriptions.rs`) so a subscription registered before a restart
//! still wakes the subscriber on matching events after the restart.

use intent_core::{AgentId, Error, Result, WorkspaceId};
use sqlx::Row;

use crate::Store;

/// Persisted event-subscription row (mirrors the in-memory
/// `EventSubscription`; `event_types` is stored as a JSON string array).
/// Only agent-owned subscriptions are persisted — a row always carries a
/// subscriber, so rehydration always has a wake target.
#[derive(Debug, Clone)]
pub struct PersistedEventSubscription {
    pub id: String,
    pub workspace_id: WorkspaceId,
    pub subscriber_agent_id: AgentId,
    pub event_types: Vec<String>,
    pub exclude_self: bool,
    pub batch_window_ms: i64,
    pub created_at: String,
}

impl Store {
    /// Insert an `event_subscription` row, or update its mutable columns on id
    /// conflict. The identity columns — workspace, subscriber, `created_at` —
    /// are fixed at registration and intentionally not overwritten.
    ///
    /// # Errors
    ///
    /// Returns `Error::Internal` if the database operation fails.
    pub async fn upsert_event_subscription(&self, s: &PersistedEventSubscription) -> Result<()> {
        let event_types = serde_json::to_string(&s.event_types)
            .map_err(|e| Error::Internal(format!("encode event_types: {e}")))?;
        sqlx::query(
            "INSERT INTO event_subscription (
                id, workspace_id, subscriber_agent_id, event_types,
                exclude_self, batch_window_ms, created_at
            ) VALUES (?,?,?,?,?,?,?)
            ON CONFLICT(id) DO UPDATE SET
                event_types = excluded.event_types,
                exclude_self = excluded.exclude_self,
                batch_window_ms = excluded.batch_window_ms",
        )
        .bind(&s.id)
        .bind(&s.workspace_id.0)
        .bind(&s.subscriber_agent_id.0)
        .bind(event_types)
        .bind(i64::from(s.exclude_self))
        .bind(s.batch_window_ms)
        .bind(&s.created_at)
        .execute(self.write_pool())
        .await
        .map_err(|e| Error::Internal(format!("upsert event_subscription failed: {e}")))?;
        Ok(())
    }

    /// Load every persisted `event_subscription` row (the registry is
    /// daemon-global, so startup rehydration loads all rows in one pass).
    ///
    /// # Errors
    ///
    /// Returns `Error::Internal` if the database operation fails.
    pub async fn list_event_subscriptions(&self) -> Result<Vec<PersistedEventSubscription>> {
        let rows = sqlx::query(
            "SELECT id, workspace_id, subscriber_agent_id, event_types,
                    exclude_self, batch_window_ms, created_at
             FROM event_subscription
             ORDER BY created_at ASC",
        )
        .fetch_all(self.read_pool())
        .await
        .map_err(|e| Error::Internal(format!("list event subscriptions: {e}")))?;

        rows.iter().map(decode_subscription_row).collect()
    }

    /// Delete an `event_subscription` row (`event.unsubscribe`, startup prune).
    ///
    /// # Errors
    ///
    /// Returns `Error::Internal` if the database operation fails.
    pub async fn delete_event_subscription(&self, id: &str) -> Result<()> {
        sqlx::query("DELETE FROM event_subscription WHERE id = ?")
            .bind(id)
            .execute(self.write_pool())
            .await
            .map_err(|e| Error::Internal(format!("delete event_subscription failed: {e}")))?;
        Ok(())
    }

    /// Delete every `event_subscription` row registered by `subscriber_agent_id`
    /// (subscriber agent deleted).
    ///
    /// # Errors
    ///
    /// Returns `Error::Internal` if the database operation fails.
    pub async fn delete_event_subscriptions_for_agent(
        &self,
        subscriber_agent_id: &AgentId,
    ) -> Result<()> {
        sqlx::query("DELETE FROM event_subscription WHERE subscriber_agent_id = ?")
            .bind(&subscriber_agent_id.0)
            .execute(self.write_pool())
            .await
            .map_err(|e| {
                Error::Internal(format!("delete event_subscriptions for agent failed: {e}"))
            })?;
        Ok(())
    }

    /// Delete every `event_subscription` row scoped to `workspace_id`
    /// (workspace deleted — the subscriptions can never match again, see
    /// monorepo#947).
    ///
    /// # Errors
    ///
    /// Returns `Error::Internal` if the database operation fails.
    pub async fn delete_event_subscriptions_for_workspace(
        &self,
        workspace_id: &WorkspaceId,
    ) -> Result<()> {
        sqlx::query("DELETE FROM event_subscription WHERE workspace_id = ?")
            .bind(&workspace_id.0)
            .execute(self.write_pool())
            .await
            .map_err(|e| {
                Error::Internal(format!(
                    "delete event_subscriptions for workspace failed: {e}"
                ))
            })?;
        Ok(())
    }
}

fn decode_subscription_row(row: &sqlx::sqlite::SqliteRow) -> Result<PersistedEventSubscription> {
    let event_types_raw: String = row
        .try_get("event_types")
        .map_err(|e| Error::Internal(format!("decode event_types: {e}")))?;
    let event_types: Vec<String> = serde_json::from_str(&event_types_raw)
        .map_err(|e| Error::Internal(format!("parse event_types: {e}")))?;
    Ok(PersistedEventSubscription {
        id: row
            .try_get("id")
            .map_err(|e| Error::Internal(format!("decode id: {e}")))?,
        workspace_id: WorkspaceId::from(
            row.try_get::<String, _>("workspace_id")
                .map_err(|e| Error::Internal(format!("decode workspace_id: {e}")))?
                .as_str(),
        ),
        subscriber_agent_id: AgentId::from(
            row.try_get::<String, _>("subscriber_agent_id")
                .map_err(|e| Error::Internal(format!("decode subscriber_agent_id: {e}")))?
                .as_str(),
        ),
        event_types,
        exclude_self: row
            .try_get::<i64, _>("exclude_self")
            .map_err(|e| Error::Internal(format!("decode exclude_self: {e}")))?
            != 0,
        batch_window_ms: row
            .try_get("batch_window_ms")
            .map_err(|e| Error::Internal(format!("decode batch_window_ms: {e}")))?,
        created_at: row
            .try_get("created_at")
            .map_err(|e| Error::Internal(format!("decode created_at: {e}")))?,
    })
}
