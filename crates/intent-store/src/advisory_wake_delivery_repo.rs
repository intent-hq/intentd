//! Once-per-waiting-period advisory-wake markers: one row per (parent, child)
//! pair recording that the parent already received the advisory wake for the
//! child's current hook-/PR-monitor-waiting period. Consulted by
//! completion-watch delivery so a monitoring idle fires at most one advisory
//! per continuous waiting period; cleared when the period ends — at the
//! child's genuine settlement (completion/failure/deletion) or when the child
//! starts a real turn (the turn-start clear in `AgentManager`) — so the
//! child's NEXT monitoring-idle period may advise re-armed watchers again.

use intent_core::{AgentId, Error, Result};

use crate::Store;

impl Store {
    /// Record (or refresh) the advisory-delivered marker for the pair.
    ///
    /// # Errors
    ///
    /// Returns `Error::Internal` if the database operation fails.
    pub async fn record_advisory_wake_delivery(
        &self,
        parent_agent_id: &AgentId,
        child_agent_id: &AgentId,
        delivered_at: &str,
    ) -> Result<()> {
        sqlx::query(
            "INSERT INTO advisory_wake_delivery (
                parent_agent_id, child_agent_id, delivered_at
            ) VALUES (?,?,?)
            ON CONFLICT(parent_agent_id, child_agent_id) DO UPDATE SET
                delivered_at = excluded.delivered_at",
        )
        .bind(&parent_agent_id.0)
        .bind(&child_agent_id.0)
        .bind(delivered_at)
        .execute(self.write_pool())
        .await
        .map_err(|e| Error::Internal(format!("record advisory_wake_delivery failed: {e}")))?;
        Ok(())
    }

    /// Whether an advisory-delivered marker stands for the pair.
    ///
    /// # Errors
    ///
    /// Returns `Error::Internal` if the database operation fails.
    pub async fn has_advisory_wake_delivery(
        &self,
        parent_agent_id: &AgentId,
        child_agent_id: &AgentId,
    ) -> Result<bool> {
        let row = sqlx::query(
            "SELECT 1 FROM advisory_wake_delivery \
             WHERE parent_agent_id = ? AND child_agent_id = ?",
        )
        .bind(&parent_agent_id.0)
        .bind(&child_agent_id.0)
        .fetch_optional(self.read_pool())
        .await
        .map_err(|e| Error::Internal(format!("get advisory_wake_delivery failed: {e}")))?;
        Ok(row.is_some())
    }

    /// Clear the advisory-delivered marker for the pair (the child's genuine
    /// completion/failure/deletion wake delivered — the waiting period is
    /// over).
    ///
    /// # Errors
    ///
    /// Returns `Error::Internal` if the database operation fails.
    pub async fn clear_advisory_wake_delivery(
        &self,
        parent_agent_id: &AgentId,
        child_agent_id: &AgentId,
    ) -> Result<()> {
        sqlx::query(
            "DELETE FROM advisory_wake_delivery \
             WHERE parent_agent_id = ? AND child_agent_id = ?",
        )
        .bind(&parent_agent_id.0)
        .bind(&child_agent_id.0)
        .execute(self.write_pool())
        .await
        .map_err(|e| Error::Internal(format!("clear advisory_wake_delivery failed: {e}")))?;
        Ok(())
    }

    /// Clear every advisory-delivered marker naming `child_agent_id` as the
    /// child: its genuine settlement — or a real turn start (the child left
    /// monitoring-idle, so the current waiting period is over) — ends the
    /// waiting period for EVERY advised parent, whether or not any still
    /// holds an armed watch (a parent that never re-armed after the advisory
    /// has no watch to clear through).
    ///
    /// # Errors
    ///
    /// Returns `Error::Internal` if the database operation fails.
    pub async fn clear_advisory_wake_deliveries_for_child(
        &self,
        child_agent_id: &AgentId,
    ) -> Result<()> {
        sqlx::query("DELETE FROM advisory_wake_delivery WHERE child_agent_id = ?")
            .bind(&child_agent_id.0)
            .execute(self.write_pool())
            .await
            .map_err(|e| {
                Error::Internal(format!("clear advisory_wake_delivery by child failed: {e}"))
            })?;
        Ok(())
    }

    /// Batched [`Store::clear_advisory_wake_deliveries_for_child`]: clear the
    /// markers naming ANY of `child_agent_ids` as the child in ONE `IN`-list
    /// statement per 32 000 ids (the workspace-delete sweep —
    /// intent-hq/monorepo#4130 — settles every session at once, so the
    /// per-child clear would otherwise cost one statement per agent). No-op
    /// on an empty list.
    ///
    /// # Errors
    ///
    /// Returns `Error::Internal` if the database operation fails.
    pub async fn clear_advisory_wake_deliveries_for_children(
        &self,
        child_agent_ids: &[AgentId],
    ) -> Result<()> {
        const IDS_PER_STATEMENT: usize = 32_000;
        for chunk in child_agent_ids.chunks(IDS_PER_STATEMENT) {
            let placeholders = vec!["?"; chunk.len()].join(",");
            let sql = format!(
                "DELETE FROM advisory_wake_delivery WHERE child_agent_id IN ({placeholders})"
            );
            let mut query = sqlx::query(&sql);
            for id in chunk {
                query = query.bind(&id.0);
            }
            query.execute(self.write_pool()).await.map_err(|e| {
                Error::Internal(format!(
                    "clear advisory_wake_delivery by children failed: {e}"
                ))
            })?;
        }
        Ok(())
    }
}
