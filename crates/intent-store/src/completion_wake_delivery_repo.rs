//! Delivered-completion dedup markers (intent-hq/monorepo#2842): one row per
//! (parent, child) pair recording the identity — the child's
//! `completion_report_timestamp` — of the most recent terminal completion
//! wake delivered to that parent. Consulted by completion-watch delivery so a
//! restart-recovery replay of the child's historical completion (or a watch
//! re-armed on an already-completed child) is delivered at most once; a
//! future completion carries a new identity and delivers normally.

use intent_core::{AgentId, Error, Result};
use sqlx::Row;

use crate::Store;

impl Store {
    /// Record (or overwrite) the delivered-completion identity for the pair.
    /// Identities are monotonic per pair (a new report gets a new timestamp),
    /// so a plain upsert is sufficient.
    pub async fn record_completion_wake_delivery(
        &self,
        parent_agent_id: &AgentId,
        child_agent_id: &AgentId,
        completion_identity: &str,
        delivered_at: &str,
    ) -> Result<()> {
        sqlx::query(
            "INSERT INTO completion_wake_delivery (
                parent_agent_id, child_agent_id, completion_identity, delivered_at
            ) VALUES (?,?,?,?)
            ON CONFLICT(parent_agent_id, child_agent_id) DO UPDATE SET
                completion_identity = excluded.completion_identity,
                delivered_at = excluded.delivered_at",
        )
        .bind(&parent_agent_id.0)
        .bind(&child_agent_id.0)
        .bind(completion_identity)
        .bind(delivered_at)
        .execute(self.write_pool())
        .await
        .map_err(|e| Error::Internal(format!("record completion_wake_delivery failed: {e}")))?;
        Ok(())
    }

    /// The delivered-completion identity recorded for the pair, if any.
    pub async fn get_completion_wake_delivery(
        &self,
        parent_agent_id: &AgentId,
        child_agent_id: &AgentId,
    ) -> Result<Option<String>> {
        let row = sqlx::query(
            "SELECT completion_identity FROM completion_wake_delivery \
             WHERE parent_agent_id = ? AND child_agent_id = ?",
        )
        .bind(&parent_agent_id.0)
        .bind(&child_agent_id.0)
        .fetch_optional(self.read_pool())
        .await
        .map_err(|e| Error::Internal(format!("get completion_wake_delivery failed: {e}")))?;
        row.map(|r| {
            r.try_get("completion_identity")
                .map_err(|e| Error::Internal(format!("decode completion_identity: {e}")))
        })
        .transpose()
    }
}
