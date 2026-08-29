//! Once-per-episode advisory-wake markers: one row per (parent, child) pair
//! recording that the parent already received the advisory wake for the
//! child's current hook-/PR-monitor-waiting episode. Consulted by
//! completion-watch delivery so a monitoring idle fires at most one advisory
//! per episode; cleared at the child's genuine settlement
//! (completion/failure/deletion), which opens the next episode.

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

    /// Atomically record the advisory-delivered marker AND retire the
    /// ungrouped watch the advisory consumed — one transaction, so a crash
    /// can never leave the watch retired without its episode marker (the
    /// re-armed watch would carry a NEW id, so neither the old stable
    /// message id nor a marker would cover the next monitoring idle and a
    /// second advisory would fire in the same episode — PR #1578 review).
    /// The caller invokes this only after the advisory wake is durable: a
    /// failed transaction leaves the watch as the retry/restart-recovery
    /// record, and the stable message id keeps the replayed send idempotent.
    ///
    /// # Errors
    ///
    /// Returns `Error::Internal` if the transaction cannot begin, write the
    /// marker, retire the watch, or commit.
    pub async fn retire_advisory_watch_after_delivery(
        &self,
        watch_id: &str,
        parent_agent_id: &AgentId,
        child_agent_id: &AgentId,
        delivered_at: &str,
    ) -> Result<()> {
        let pool = self.write_pool().clone();
        crate::with_write_txn_retry(|| async {
            let mut tx = pool
                .begin()
                .await
                .map_err(|e| Error::Internal(format!("retire advisory watch begin failed: {e}")))?;
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
            .execute(&mut *tx)
            .await
            .map_err(|e| {
                Error::Internal(format!("retire advisory watch marker write failed: {e}"))
            })?;
            sqlx::query("DELETE FROM completion_watch WHERE id = ?")
                .bind(watch_id)
                .execute(&mut *tx)
                .await
                .map_err(|e| {
                    Error::Internal(format!("retire advisory watch delete failed: {e}"))
                })?;
            tx.commit().await.map_err(|e| {
                Error::Internal(format!("retire advisory watch commit failed: {e}"))
            })?;
            Ok(())
        })
        .await
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
    /// completion/failure/deletion wake delivered — the episode is over).
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
    /// child: its genuine settlement ends the waiting episode for EVERY
    /// advised parent, whether or not any still holds an armed watch (a
    /// parent that never re-armed after the advisory has no watch to clear
    /// through).
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
}
