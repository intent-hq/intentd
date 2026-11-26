//! Completion-watch repository: CRUD for persisted parent→child completion
//! watches (ungrouped and grouped).
//!
//! Registration persists the row via a best-effort async write-through (NOT
//! durable-before-observable — see `Services::persist_completion_watch`);
//! firing a watch and cancellation delete it. On startup the daemon
//! rehydrates surviving rows into the in-memory registry
//! (`agent_subscriptions.rs`) so a watch registered before a restart still
//! wakes the parent when the child completes after the restart.

use std::time::Duration;

use intent_core::{AgentId, Error, Result, WorkspaceId};
use sqlx::Row;

use crate::{Lease, LeaseKeeper, Store};

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
    /// Ask-only watches wait for terminal completion and ignore attention.
    pub completion_only: bool,
    pub created_at: String,
}

/// The lease scope of a completion watch's delivery claim (key: the
/// watch id).
pub const COMPLETION_WATCH_LEASE_SCOPE: &str = "completion-watch";

/// The lease scope of a delegation group's delivery claim (key: the group
/// id).
pub const DELEGATION_GROUP_LEASE_SCOPE: &str = "delegation-group";

/// Fails unless `claim` is the delivery claim of (`scope`, `key`), and
/// returns its lease; `Error::LeaseLost` once the claim was released.
fn claimed<'a>(
    claim: &'a tokio::sync::MutexGuard<'_, Option<Lease>>,
    scope: &str,
    key: &str,
) -> Result<&'a Lease> {
    let Some(lease) = claim.as_ref() else {
        return Err(Error::LeaseLost(format!(
            "delivery claim {scope}/{key} was released"
        )));
    };
    if lease.scope() != scope || lease.key() != key {
        return Err(Error::InvalidParams(format!(
            "the delivery claim {}/{} does not cover {scope}/{key}",
            lease.scope(),
            lease.key()
        )));
    }
    Ok(lease)
}

/// The lease of the delivery claim `claim` for a write that guards it;
/// `Error::LeaseLost` once the claim was released.
pub(crate) fn held_claim<'a>(
    held: &'a tokio::sync::MutexGuard<'_, Option<Lease>>,
    claim: &LeaseKeeper,
) -> Result<&'a Lease> {
    held.as_ref().ok_or_else(|| {
        Error::LeaseLost(format!(
            "delivery claim {}/{} was released",
            claim.scope(),
            claim.key()
        ))
    })
}

impl Store {
    /// Claims the watch `watch_id` for this process's delivery of its
    /// terminal wake: the lease (`completion-watch`, `watch_id`) for `ttl`,
    /// kept by the returned [`LeaseKeeper`] until delivery ends. `None`
    /// while another holder has it (another process, or another pass):
    /// retry later, the holder delivers or releases, and a crashed holder's
    /// claim lapses after its TTL by the database clock. Neither the tenant
    /// epoch nor the row matter: a tenant takeover leaves a live claim
    /// alone, and a watch whose row has not landed yet is claimed all the
    /// same.
    ///
    /// Settle with [`Store::retire_completion_watch_after_delivery`], which
    /// fences on the claim; otherwise [`LeaseKeeper::release`] it so a retry
    /// can claim it at once.
    ///
    /// # Errors
    ///
    /// As [`Store::acquire_lease`].
    pub async fn claim_completion_watch(
        &self,
        watch_id: &str,
        ttl: Duration,
    ) -> Result<Option<LeaseKeeper>> {
        // Boxed: delivery passes nest deep, and the acquire is large.
        Box::pin(LeaseKeeper::acquire(
            self,
            COMPLETION_WATCH_LEASE_SCOPE,
            watch_id,
            ttl,
        ))
        .await
    }

    /// [`Store::claim_completion_watch`] for a delegation group's aggregate
    /// wake (`delegation-group`, `group_id`); settle with
    /// [`Store::settle_delegation_group_after_delivery`].
    ///
    /// # Errors
    ///
    /// As [`Store::acquire_lease`].
    pub async fn claim_delegation_group(
        &self,
        group_id: &str,
        ttl: Duration,
    ) -> Result<Option<LeaseKeeper>> {
        // Boxed, as for a watch.
        Box::pin(LeaseKeeper::acquire(
            self,
            DELEGATION_GROUP_LEASE_SCOPE,
            group_id,
            ttl,
        ))
        .await
    }

    /// Atomically retire one fired completion watch and record the delivered
    /// completion identity when one exists. The caller invokes this only after
    /// the parent wake is durable, so a failed transaction leaves the watch as
    /// the restart-recovery record and a successful transaction cannot leave a
    /// stale watch behind its dedup marker.
    ///
    /// # Errors
    ///
    /// The transaction guards `claim` (this watch's delivery claim, from
    /// [`Store::claim_completion_watch`]) and, when it commits, retires the
    /// claim's lease row too: a holder whose claim lapsed or was taken over
    /// writes nothing.
    ///
    /// Returns [`Error::LeaseLost`] when `claim` is no longer held,
    /// [`Error::InvalidParams`] when it is not this watch's claim, and
    /// [`Error::Internal`] if the transaction cannot begin, update the
    /// delivery marker, retire the watch, or commit.
    pub async fn retire_completion_watch_after_delivery(
        &self,
        watch_id: &str,
        parent_agent_id: &AgentId,
        child_agent_id: &AgentId,
        completion_identity: Option<&str>,
        delivered_at: &str,
        claim: &LeaseKeeper,
    ) -> Result<()> {
        let held = claim.lease().await;
        let lease = claimed(&held, COMPLETION_WATCH_LEASE_SCOPE, watch_id)?;
        dispatch!(self, backend => Box::pin(backend.retire_completion_watch_after_delivery(watch_id, parent_agent_id, child_agent_id, completion_identity, delivered_at, lease)).await)
    }

    /// Atomically settle a delivered delegation group: failed children that
    /// can still complete become ungrouped watches, all other group watches are
    /// retired, and the group row is deleted. A failure leaves the complete
    /// group and all watches restart-recoverable.
    ///
    /// # Errors
    ///
    /// The transaction guards `claim` (the group's delivery claim, from
    /// [`Store::claim_delegation_group`]) and retires its lease row, as
    /// [`Store::retire_completion_watch_after_delivery`].
    ///
    /// Returns [`Error::LeaseLost`] when `claim` is no longer held,
    /// [`Error::InvalidParams`] when it is not this group's claim, and
    /// [`Error::Internal`] if any settlement statement or the final
    /// transaction commit fails.
    pub async fn settle_delegation_group_after_delivery(
        &self,
        group_id: &str,
        retain_children: &[AgentId],
        claim: &LeaseKeeper,
    ) -> Result<()> {
        let held = claim.lease().await;
        let lease = claimed(&held, DELEGATION_GROUP_LEASE_SCOPE, group_id)?;
        dispatch!(self, backend => backend.settle_delegation_group_after_delivery(group_id, retain_children, lease).await)
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
        dispatch!(self, backend => backend.upsert_completion_watch(w).await)
    }

    /// [`Store::upsert_completion_watch`] for the holder of `lease`: the
    /// write guards it in its own transaction, so once the lease is lost
    /// (lapsed, taken over) it fails with `Error::LeaseLost` and writes
    /// nothing. Strict ask registration inserts a pair's watch under the
    /// pair's registration lease this way.
    ///
    /// # Errors
    ///
    /// `Error::LeaseLost` when `lease` is no longer held; otherwise as
    /// [`Store::upsert_completion_watch`].
    pub async fn upsert_completion_watch_guarded(
        &self,
        w: &PersistedCompletionWatch,
        lease: &Lease,
    ) -> Result<()> {
        match &self.backend {
            crate::Backend::Sqlite(backend) => {
                backend.guard_lease(lease)?;
                backend.upsert_completion_watch(w).await
            }
            #[cfg(feature = "postgres")]
            crate::Backend::Postgres(backend) => {
                backend
                    .upsert_completion_watch_guarded(w, Some(lease))
                    .await
            }
        }
    }

    /// A stored ungrouped watch of `parent_agent_id` on `child_agent_id`
    /// (the oldest; registration keeps one per pair), as any process
    /// sharing the store registered it.
    ///
    /// # Errors
    ///
    /// Returns `Error::Internal` if the database operation fails.
    pub async fn find_ungrouped_completion_watch(
        &self,
        parent_agent_id: &AgentId,
        child_agent_id: &AgentId,
    ) -> Result<Option<PersistedCompletionWatch>> {
        dispatch!(self, backend => backend.find_ungrouped_completion_watch(parent_agent_id, child_agent_id).await)
    }

    /// Load every persisted `completion_watch` row (the registry is
    /// daemon-global, so startup rehydration loads all rows in one pass).
    ///
    /// # Errors
    ///
    /// Returns `Error::Internal` if the database operation fails.
    pub async fn list_completion_watches(&self) -> Result<Vec<PersistedCompletionWatch>> {
        dispatch!(self, backend => backend.list_completion_watches().await)
    }

    /// Delete a `completion_watch` row (fired watch, cancellation).
    ///
    /// # Errors
    ///
    /// Returns `Error::Internal` if the database operation fails.
    pub async fn delete_completion_watch(&self, id: &str) -> Result<()> {
        dispatch!(self, backend => backend.delete_completion_watch(id).await)
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
        dispatch!(self, backend => backend.delete_completion_watches_for_parent(parent_agent_id).await)
    }

    /// Set `report_delivered = 1` (report-time wake already delivered).
    ///
    /// # Errors
    ///
    /// Returns `Error::Internal` if the database operation fails.
    pub async fn mark_completion_watch_report_delivered(&self, id: &str) -> Result<()> {
        dispatch!(self, backend => backend.mark_completion_watch_report_delivered(id).await)
    }

    /// Reset `report_delivered = 0` (fresh-interest re-arm on watch reuse,
    /// monorepo#2532).
    ///
    /// # Errors
    ///
    /// Returns `Error::Internal` if the database operation fails.
    pub async fn clear_completion_watch_report_delivered(&self, id: &str) -> Result<()> {
        dispatch!(self, backend => backend.clear_completion_watch_report_delivered(id).await)
    }

    /// Convert a grouped watch into an ungrouped watch (group settlement
    /// retaining a failed-not-deleted member, STAB-129).
    ///
    /// # Errors
    ///
    /// Returns `Error::Internal` if the database operation fails.
    pub async fn ungroup_completion_watch(&self, id: &str) -> Result<()> {
        dispatch!(self, backend => backend.ungroup_completion_watch(id).await)
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
        dispatch!(self, backend => backend.update_completion_watch_parent(id, parent_agent_name, parent_workspace_id).await)
    }
}

impl crate::SqliteStore {
    pub async fn retire_completion_watch_after_delivery(
        &self,
        watch_id: &str,
        parent_agent_id: &AgentId,
        child_agent_id: &AgentId,
        completion_identity: Option<&str>,
        delivered_at: &str,
        claim: &Lease,
    ) -> Result<()> {
        let pool = self.write_pool().clone();
        crate::with_write_txn_retry(|| async {
            let mut tx = pool.begin().await.map_err(|e| {
                Error::Internal(format!("retire completion_watch begin failed: {e}"))
            })?;
            self.guard_lease(claim)?;
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
        .await?;
        self.retire_lease(claim);
        Ok(())
    }

    pub async fn settle_delegation_group_after_delivery(
        &self,
        group_id: &str,
        retain_children: &[AgentId],
        claim: &Lease,
    ) -> Result<()> {
        let pool = self.write_pool().clone();
        let retained: Vec<String> = retain_children.iter().map(|id| id.0.clone()).collect();
        crate::with_write_txn_retry(|| async {
            let mut tx = pool.begin().await.map_err(|e| {
                Error::Internal(format!("settle delegation_group begin failed: {e}"))
            })?;
            self.guard_lease(claim)?;
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
                let mut query = sqlx::query(sqlx::AssertSqlSafe(sql.as_str())).bind(group_id);
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
        .await?;
        self.retire_lease(claim);
        Ok(())
    }

    pub async fn upsert_completion_watch(&self, w: &PersistedCompletionWatch) -> Result<()> {
        sqlx::query(
            "INSERT INTO completion_watch (
                id, parent_workspace_id, child_workspace_id, parent_agent_id,
                parent_agent_name, child_agent_id, group_id,
                report_delivered, wake_on_attention, completion_only, created_at
            ) VALUES (?,?,?,?,?,?,?,?,?,?,?)
            ON CONFLICT(id) DO UPDATE SET
                parent_workspace_id = excluded.parent_workspace_id,
                parent_agent_name = excluded.parent_agent_name,
                group_id = excluded.group_id,
                report_delivered = excluded.report_delivered,
                wake_on_attention = excluded.wake_on_attention,
                completion_only = excluded.completion_only",
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
        .bind(i64::from(w.completion_only))
        .bind(&w.created_at)
        .execute(self.write_pool())
        .await
        .map_err(|e| Error::Internal(format!("upsert completion_watch failed: {e}")))?;
        Ok(())
    }

    pub async fn list_completion_watches(&self) -> Result<Vec<PersistedCompletionWatch>> {
        let rows = sqlx::query(
            "SELECT id, parent_workspace_id, child_workspace_id, parent_agent_id,
                    parent_agent_name, child_agent_id, group_id,
                    report_delivered, wake_on_attention, completion_only, created_at
             FROM completion_watch
             ORDER BY created_at ASC",
        )
        .fetch_all(self.read_pool())
        .await
        .map_err(|e| Error::Internal(format!("list completion watches: {e}")))?;

        rows.iter().map(decode_watch_row).collect()
    }

    pub async fn find_ungrouped_completion_watch(
        &self,
        parent_agent_id: &AgentId,
        child_agent_id: &AgentId,
    ) -> Result<Option<PersistedCompletionWatch>> {
        let row = sqlx::query(
            "SELECT id, parent_workspace_id, child_workspace_id, parent_agent_id,
                    parent_agent_name, child_agent_id, group_id,
                    report_delivered, wake_on_attention, completion_only, created_at
             FROM completion_watch
             WHERE parent_agent_id = ? AND child_agent_id = ? AND group_id IS NULL
             ORDER BY created_at ASC LIMIT 1",
        )
        .bind(&parent_agent_id.0)
        .bind(&child_agent_id.0)
        .fetch_optional(self.read_pool())
        .await
        .map_err(|e| Error::Internal(format!("find completion watch: {e}")))?;
        row.as_ref().map(decode_watch_row).transpose()
    }

    pub async fn delete_completion_watch(&self, id: &str) -> Result<()> {
        sqlx::query("DELETE FROM completion_watch WHERE id = ?")
            .bind(id)
            .execute(self.write_pool())
            .await
            .map_err(|e| Error::Internal(format!("delete completion_watch failed: {e}")))?;
        Ok(())
    }

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

    pub async fn ungroup_completion_watch(&self, id: &str) -> Result<()> {
        sqlx::query("UPDATE completion_watch SET group_id = NULL WHERE id = ?")
            .bind(id)
            .execute(self.write_pool())
            .await
            .map_err(|e| Error::Internal(format!("ungroup completion_watch failed: {e}")))?;
        Ok(())
    }

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
        completion_only: row
            .try_get::<i64, _>("completion_only")
            .map_err(|e| Error::Internal(format!("decode completion_only: {e}")))?
            != 0,
        created_at: row
            .try_get("created_at")
            .map_err(|e| Error::Internal(format!("decode created_at: {e}")))?,
    })
}
