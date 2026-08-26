//! Completion-watch repository: CRUD for persisted parent→child completion
//! watches (ungrouped and grouped).
//!
//! Registration persists the row via a best-effort async write-through (NOT
//! durable-before-observable — see `Services::persist_completion_watch`);
//! firing a watch and cancellation delete it. On startup the daemon
//! rehydrates surviving rows into the in-memory registry
//! (`agent_subscriptions.rs`) so a watch registered before a restart still
//! wakes the parent when the child completes after the restart.

use intent_core::{AgentId, Error, Result, WorkspaceId};
use sqlx::Row;

use crate::Store;

/// Persisted completion-watch row (mirrors the in-memory `CompletionWatch`).
#[derive(Debug, Clone)]
pub struct PersistedCompletionWatch {
    pub id: String,
    pub parent_workspace_id: WorkspaceId,
    pub child_workspace_id: WorkspaceId,
    pub parent_agent_id: AgentId,
    pub parent_agent_name: String,
    pub child_agent_id: AgentId,
    pub group_id: Option<String>,
    pub report_delivered: bool,
    /// Explicit `agent.watch` watches also wake on the child's attention
    /// requests (blocker/discussion); auto-registered watches default false.
    pub wake_on_attention: bool,
    pub created_at: String,
}

impl Store {
    /// Atomically retire one fired completion watch and record the delivered
    /// completion identity when one exists. The caller invokes this only after
    /// the parent wake is durable, so a failed transaction leaves the watch as
    /// the restart-recovery record and a successful transaction cannot leave a
    /// stale watch behind its dedup marker.
    ///
    /// # Errors
    ///
    /// Returns [`Error::Internal`] if the transaction cannot begin, update the
    /// delivery marker, retire the watch, or commit.
    pub async fn retire_completion_watch_after_delivery(
        &self,
        watch_id: &str,
        parent_agent_id: &AgentId,
        child_agent_id: &AgentId,
        completion_identity: Option<&str>,
        delivered_at: &str,
    ) -> Result<()> {
        let pool = self.write_pool().clone();
        crate::with_write_txn_retry(|| async {
            let mut tx = pool.begin().await.map_err(|e| {
                Error::Internal(format!("retire completion_watch begin failed: {e}"))
            })?;
            if let Some(identity) = completion_identity {
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
                .bind(identity)
                .bind(delivered_at)
                .execute(&mut *tx)
                .await
                .map_err(|e| {
                    Error::Internal(format!(
                        "retire completion_watch delivery marker failed: {e}"
                    ))
                })?;
            }
            sqlx::query("DELETE FROM completion_watch WHERE id = ?")
                .bind(watch_id)
                .execute(&mut *tx)
                .await
                .map_err(|e| {
                    Error::Internal(format!("retire completion_watch delete failed: {e}"))
                })?;
            tx.commit().await.map_err(|e| {
                Error::Internal(format!("retire completion_watch commit failed: {e}"))
            })?;
            Ok(())
        })
        .await
    }

    /// Atomically settle a delivered delegation group: failed children that
    /// can still complete become ungrouped watches, all other group watches are
    /// retired, and the group row is deleted. A failure leaves the complete
    /// group and all watches restart-recoverable.
    ///
    /// # Errors
    ///
    /// Returns [`Error::Internal`] if any settlement statement or the final
    /// transaction commit fails.
    pub async fn settle_delegation_group_after_delivery(
        &self,
        group_id: &str,
        retain_children: &[AgentId],
    ) -> Result<()> {
        let pool = self.write_pool().clone();
        let retained: Vec<String> = retain_children.iter().map(|id| id.0.clone()).collect();
        crate::with_write_txn_retry(|| async {
            let mut tx = pool.begin().await.map_err(|e| {
                Error::Internal(format!("settle delegation_group begin failed: {e}"))
            })?;
            for child_id in &retained {
                sqlx::query(
                    "UPDATE completion_watch SET group_id = NULL \
                     WHERE group_id = ? AND child_agent_id = ?",
                )
                .bind(group_id)
                .bind(child_id)
                .execute(&mut *tx)
                .await
                .map_err(|e| {
                    Error::Internal(format!("settle delegation_group retain watch failed: {e}"))
                })?;
            }
            if retained.is_empty() {
                sqlx::query("DELETE FROM completion_watch WHERE group_id = ?")
                    .bind(group_id)
                    .execute(&mut *tx)
                    .await
                    .map_err(|e| {
                        Error::Internal(format!(
                            "settle delegation_group delete watches failed: {e}"
                        ))
                    })?;
            } else {
                let placeholders = std::iter::repeat_n("?", retained.len())
                    .collect::<Vec<_>>()
                    .join(",");
                let sql = format!(
                    "DELETE FROM completion_watch WHERE group_id = ? \
                     AND child_agent_id NOT IN ({placeholders})"
                );
                let mut query = sqlx::query(&sql).bind(group_id);
                for child_id in &retained {
                    query = query.bind(child_id);
                }
                query.execute(&mut *tx).await.map_err(|e| {
                    Error::Internal(format!(
                        "settle delegation_group delete watches failed: {e}"
                    ))
                })?;
            }
            sqlx::query("DELETE FROM delegation_group WHERE group_id = ?")
                .bind(group_id)
                .execute(&mut *tx)
                .await
                .map_err(|e| {
                    Error::Internal(format!("settle delegation_group delete failed: {e}"))
                })?;
            tx.commit().await.map_err(|e| {
                Error::Internal(format!("settle delegation_group commit failed: {e}"))
            })?;
            Ok(())
        })
        .await
    }

    /// Insert a `completion_watch` row, or update its mutable columns on id
    /// conflict (parent anchor/name, `group_id`, `report_delivered`). The
    /// identity columns — child ids/workspace and `created_at` — are fixed at
    /// registration and intentionally not overwritten.
    ///
    /// # Errors
    ///
    /// Returns `Error::Internal` if the database operation fails.
    pub async fn upsert_completion_watch(&self, w: &PersistedCompletionWatch) -> Result<()> {
        sqlx::query(
            "INSERT INTO completion_watch (
                id, parent_workspace_id, child_workspace_id, parent_agent_id,
                parent_agent_name, child_agent_id, group_id,
                report_delivered, wake_on_attention, created_at
            ) VALUES (?,?,?,?,?,?,?,?,?,?)
            ON CONFLICT(id) DO UPDATE SET
                parent_workspace_id = excluded.parent_workspace_id,
                parent_agent_name = excluded.parent_agent_name,
                group_id = excluded.group_id,
                report_delivered = excluded.report_delivered,
                wake_on_attention = excluded.wake_on_attention",
        )
        .bind(&w.id)
        .bind(&w.parent_workspace_id.0)
        .bind(&w.child_workspace_id.0)
        .bind(&w.parent_agent_id.0)
        .bind(&w.parent_agent_name)
        .bind(&w.child_agent_id.0)
        .bind(&w.group_id)
        .bind(i64::from(w.report_delivered))
        .bind(i64::from(w.wake_on_attention))
        .bind(&w.created_at)
        .execute(self.write_pool())
        .await
        .map_err(|e| Error::Internal(format!("upsert completion_watch failed: {e}")))?;
        Ok(())
    }

    /// Load every persisted `completion_watch` row (the registry is
    /// daemon-global, so startup rehydration loads all rows in one pass).
    ///
    /// # Errors
    ///
    /// Returns `Error::Internal` if the database operation fails.
    pub async fn list_completion_watches(&self) -> Result<Vec<PersistedCompletionWatch>> {
        let rows = sqlx::query(
            "SELECT id, parent_workspace_id, child_workspace_id, parent_agent_id,
                    parent_agent_name, child_agent_id, group_id,
                    report_delivered, wake_on_attention, created_at
             FROM completion_watch
             ORDER BY created_at ASC",
        )
        .fetch_all(self.read_pool())
        .await
        .map_err(|e| Error::Internal(format!("list completion watches: {e}")))?;

        rows.iter().map(decode_watch_row).collect()
    }

    /// Delete a `completion_watch` row (fired watch, cancellation).
    ///
    /// # Errors
    ///
    /// Returns `Error::Internal` if the database operation fails.
    pub async fn delete_completion_watch(&self, id: &str) -> Result<()> {
        sqlx::query("DELETE FROM completion_watch WHERE id = ?")
            .bind(id)
            .execute(self.write_pool())
            .await
            .map_err(|e| Error::Internal(format!("delete completion_watch failed: {e}")))?;
        Ok(())
    }

    /// Delete every `completion_watch` row registered by `parent_agent_id`
    /// (`agent.cancelSubscriptions`).
    ///
    /// # Errors
    ///
    /// Returns `Error::Internal` if the database operation fails.
    pub async fn delete_completion_watches_for_parent(
        &self,
        parent_agent_id: &AgentId,
    ) -> Result<()> {
        sqlx::query("DELETE FROM completion_watch WHERE parent_agent_id = ?")
            .bind(&parent_agent_id.0)
            .execute(self.write_pool())
            .await
            .map_err(|e| {
                Error::Internal(format!("delete completion_watches for parent failed: {e}"))
            })?;
        Ok(())
    }

    /// Set `report_delivered = 1` (report-time wake already delivered).
    ///
    /// # Errors
    ///
    /// Returns `Error::Internal` if the database operation fails.
    pub async fn mark_completion_watch_report_delivered(&self, id: &str) -> Result<()> {
        sqlx::query("UPDATE completion_watch SET report_delivered = 1 WHERE id = ?")
            .bind(id)
            .execute(self.write_pool())
            .await
            .map_err(|e| {
                Error::Internal(format!(
                    "mark completion_watch report_delivered failed: {e}"
                ))
            })?;
        Ok(())
    }

    /// Reset `report_delivered = 0` (fresh-interest re-arm on watch reuse,
    /// monorepo#2532).
    ///
    /// # Errors
    ///
    /// Returns `Error::Internal` if the database operation fails.
    pub async fn clear_completion_watch_report_delivered(&self, id: &str) -> Result<()> {
        sqlx::query("UPDATE completion_watch SET report_delivered = 0 WHERE id = ?")
            .bind(id)
            .execute(self.write_pool())
            .await
            .map_err(|e| {
                Error::Internal(format!(
                    "clear completion_watch report_delivered failed: {e}"
                ))
            })?;
        Ok(())
    }

    /// Convert a grouped watch into an ungrouped watch (group settlement
    /// retaining a failed-not-deleted member, STAB-129).
    ///
    /// # Errors
    ///
    /// Returns `Error::Internal` if the database operation fails.
    pub async fn ungroup_completion_watch(&self, id: &str) -> Result<()> {
        sqlx::query("UPDATE completion_watch SET group_id = NULL WHERE id = ?")
            .bind(id)
            .execute(self.write_pool())
            .await
            .map_err(|e| Error::Internal(format!("ungroup completion_watch failed: {e}")))?;
        Ok(())
    }

    /// Refresh a watch's stored parent display name and home-workspace anchor
    /// (the `find_and_refresh_ungrouped_watch` reuse path).
    ///
    /// # Errors
    ///
    /// Returns `Error::Internal` if the database operation fails.
    pub async fn update_completion_watch_parent(
        &self,
        id: &str,
        parent_agent_name: &str,
        parent_workspace_id: &WorkspaceId,
    ) -> Result<()> {
        sqlx::query(
            "UPDATE completion_watch SET parent_agent_name = ?, parent_workspace_id = ? \
             WHERE id = ?",
        )
        .bind(parent_agent_name)
        .bind(&parent_workspace_id.0)
        .bind(id)
        .execute(self.write_pool())
        .await
        .map_err(|e| Error::Internal(format!("update completion_watch parent failed: {e}")))?;
        Ok(())
    }
}

fn decode_watch_row(row: &sqlx::sqlite::SqliteRow) -> Result<PersistedCompletionWatch> {
    Ok(PersistedCompletionWatch {
        id: row
            .try_get("id")
            .map_err(|e| Error::Internal(format!("decode id: {e}")))?,
        parent_workspace_id: WorkspaceId::from(
            row.try_get::<String, _>("parent_workspace_id")
                .map_err(|e| Error::Internal(format!("decode parent_workspace_id: {e}")))?
                .as_str(),
        ),
        child_workspace_id: WorkspaceId::from(
            row.try_get::<String, _>("child_workspace_id")
                .map_err(|e| Error::Internal(format!("decode child_workspace_id: {e}")))?
                .as_str(),
        ),
        parent_agent_id: AgentId::from(
            row.try_get::<String, _>("parent_agent_id")
                .map_err(|e| Error::Internal(format!("decode parent_agent_id: {e}")))?
                .as_str(),
        ),
        parent_agent_name: row
            .try_get("parent_agent_name")
            .map_err(|e| Error::Internal(format!("decode parent_agent_name: {e}")))?,
        child_agent_id: AgentId::from(
            row.try_get::<String, _>("child_agent_id")
                .map_err(|e| Error::Internal(format!("decode child_agent_id: {e}")))?
                .as_str(),
        ),
        group_id: row
            .try_get("group_id")
            .map_err(|e| Error::Internal(format!("decode group_id: {e}")))?,
        report_delivered: row
            .try_get::<i64, _>("report_delivered")
            .map_err(|e| Error::Internal(format!("decode report_delivered: {e}")))?
            != 0,
        wake_on_attention: row
            .try_get::<i64, _>("wake_on_attention")
            .map_err(|e| Error::Internal(format!("decode wake_on_attention: {e}")))?
            != 0,
        created_at: row
            .try_get("created_at")
            .map_err(|e| Error::Internal(format!("decode created_at: {e}")))?,
    })
}
