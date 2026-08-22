//! Stop-redelivery repository: durable write-through mirror of the in-memory
//! per-agent zero-output stop-redelivery payload (migration 0090,
//! intent-hq/monorepo#1899). At most one payload per agent (the newest arm
//! wins), so each mutation is a single upsert/delete on the write pool. Rows
//! cascade with their `agent_session` row (workspace/agent delete needs no
//! explicit cleanup).

use intent_core::{AgentId, Error, Result};
use sqlx::Row;

use crate::Store;

/// One persisted stop-redelivery payload. `payload` is the internal
/// `QueuedPrepend` JSON (owned by `intent-services`); the store treats it as
/// opaque.
#[derive(Debug, Clone)]
pub struct StopRedeliveryRow {
    pub agent_id: AgentId,
    pub payload: serde_json::Value,
    pub created_at: String,
}

impl Store {
    /// Upsert the persisted stop-redelivery payload for one agent (the newest
    /// arm replaces any prior payload).
    ///
    /// # Errors
    ///
    /// Returns `Error::Internal` if the database operation fails.
    pub async fn set_stop_redelivery(
        &self,
        agent_id: &AgentId,
        payload: &serde_json::Value,
        created_at: &str,
    ) -> Result<()> {
        let payload = serde_json::to_string(payload)
            .map_err(|e| Error::Internal(format!("encode stop redelivery payload failed: {e}")))?;
        sqlx::query(
            "INSERT INTO agent_stop_redelivery (agent_id, payload, created_at) VALUES (?,?,?) \
             ON CONFLICT(agent_id) DO UPDATE SET \
               payload = excluded.payload, created_at = excluded.created_at",
        )
        .bind(&agent_id.0)
        .bind(payload)
        .bind(created_at)
        .execute(self.write_pool())
        .await
        .map_err(|e| Error::Internal(format!("set stop redelivery failed: {e}")))?;
        Ok(())
    }

    /// Delete the persisted stop-redelivery payload for one agent (no-op when
    /// none is armed).
    ///
    /// # Errors
    ///
    /// Returns `Error::Internal` if the database operation fails.
    pub async fn clear_stop_redelivery(&self, agent_id: &AgentId) -> Result<()> {
        sqlx::query("DELETE FROM agent_stop_redelivery WHERE agent_id = ?")
            .bind(&agent_id.0)
            .execute(self.write_pool())
            .await
            .map_err(|e| Error::Internal(format!("clear stop redelivery failed: {e}")))?;
        Ok(())
    }

    /// Load every persisted stop-redelivery payload for startup rehydration.
    /// Joined against `agent_session` so rows whose session no longer exists
    /// are skipped (defensive; the FK cascade should already have removed
    /// them). A row whose stored payload is not valid JSON comes back as
    /// `Value::Null` rather than failing the whole load — rehydration is
    /// best-effort and the caller skips entries it cannot decode.
    ///
    /// # Errors
    ///
    /// Returns `Error::Internal` if the database operation fails.
    pub async fn load_all_stop_redeliveries(&self) -> Result<Vec<StopRedeliveryRow>> {
        let rows = sqlx::query(
            "SELECT r.agent_id, r.payload, r.created_at \
             FROM agent_stop_redelivery r JOIN agent_session s ON s.id = r.agent_id \
             ORDER BY r.agent_id",
        )
        .fetch_all(self.read_pool())
        .await
        .map_err(|e| Error::Internal(format!("load stop redeliveries failed: {e}")))?;
        Ok(rows
            .iter()
            .map(|row| {
                let payload_raw: String = row.get("payload");
                let payload = serde_json::from_str(&payload_raw).unwrap_or(serde_json::Value::Null);
                StopRedeliveryRow {
                    agent_id: AgentId(row.get("agent_id")),
                    payload,
                    created_at: row.get("created_at"),
                }
            })
            .collect())
    }
}
