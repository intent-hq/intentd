//! Agent queue repository: durable write-through snapshots of the per-agent
//! in-memory send queue (migration 0046). Queues are small, so each mutation
//! replaces the agent's whole persisted queue in one transaction — simple and
//! race-safe under the single-connection write pool. Rows cascade with their
//! `agent_session` row (workspace/agent delete needs no explicit cleanup).

use intent_core::{AgentId, Error, Result};
use sqlx::Row;

use crate::Store;

/// One persisted queue entry. `payload` is the full internal `QueuedMessage`
/// JSON (owned by `intent-services`); the store treats it as opaque.
#[derive(Debug, Clone)]
pub struct AgentQueueRow {
    pub id: String,
    pub agent_id: AgentId,
    /// 0-based index in the agent's queue at snapshot time (0 = next to send).
    pub position: i64,
    pub payload: serde_json::Value,
    pub created_at: String,
}

impl Store {
    /// Replace the persisted queue for one agent with the given snapshot
    /// (delete-then-insert in a single transaction). An empty `rows` slice
    /// clears the agent's persisted queue.
    pub async fn replace_agent_queue(
        &self,
        agent_id: &AgentId,
        rows: &[AgentQueueRow],
    ) -> Result<()> {
        let pool = self.write_pool();
        let agent_id = agent_id.clone();
        let owned: Vec<(String, i64, String, String)> = rows
            .iter()
            .map(|r| {
                serde_json::to_string(&r.payload)
                    .map(|payload| (r.id.clone(), r.position, payload, r.created_at.clone()))
                    .map_err(|e| Error::Internal(format!("encode agent queue payload failed: {e}")))
            })
            .collect::<Result<_>>()?;

        crate::with_write_txn_retry(|| async {
            let mut tx = pool
                .begin()
                .await
                .map_err(|e| Error::Internal(format!("replace agent queue begin failed: {e}")))?;
            sqlx::query("DELETE FROM agent_queue WHERE agent_id = ?")
                .bind(&agent_id.0)
                .execute(&mut *tx)
                .await
                .map_err(|e| Error::Internal(format!("replace agent queue clear failed: {e}")))?;
            for (id, position, payload, created_at) in &owned {
                sqlx::query(
                    "INSERT INTO agent_queue (id, agent_id, position, payload, created_at) \
                     VALUES (?,?,?,?,?)",
                )
                .bind(id)
                .bind(&agent_id.0)
                .bind(position)
                .bind(payload)
                .bind(created_at)
                .execute(&mut *tx)
                .await
                .map_err(|e| Error::Internal(format!("replace agent queue insert failed: {e}")))?;
            }
            tx.commit()
                .await
                .map_err(|e| Error::Internal(format!("replace agent queue commit failed: {e}")))?;
            Ok(())
        })
        .await
    }

    /// Load every persisted queue entry, ordered by agent then queue position,
    /// for startup rehydration. Joined against `agent_session` so entries
    /// whose session row no longer exists are skipped (defensive; the FK
    /// cascade should already have removed them).
    pub async fn load_all_agent_queues(&self) -> Result<Vec<AgentQueueRow>> {
        let rows = sqlx::query(
            "SELECT q.id, q.agent_id, q.position, q.payload, q.created_at \
             FROM agent_queue q JOIN agent_session s ON s.id = q.agent_id \
             ORDER BY q.agent_id, q.position",
        )
        .fetch_all(self.read_pool())
        .await
        .map_err(|e| Error::Internal(format!("load agent queues failed: {e}")))?;
        rows.iter()
            .map(|row| {
                let payload_raw: String = row.get("payload");
                let payload = serde_json::from_str(&payload_raw).map_err(|e| {
                    Error::Internal(format!("decode agent queue payload failed: {e}"))
                })?;
                Ok(AgentQueueRow {
                    id: row.get("id"),
                    agent_id: AgentId(row.get("agent_id")),
                    position: row.get("position"),
                    payload,
                    created_at: row.get("created_at"),
                })
            })
            .collect()
    }

    /// Delete every persisted queue entry for one agent.
    pub async fn delete_agent_queue(&self, agent_id: &AgentId) -> Result<()> {
        sqlx::query("DELETE FROM agent_queue WHERE agent_id = ?")
            .bind(&agent_id.0)
            .execute(self.write_pool())
            .await
            .map_err(|e| Error::Internal(format!("delete agent queue failed: {e}")))?;
        Ok(())
    }
}
