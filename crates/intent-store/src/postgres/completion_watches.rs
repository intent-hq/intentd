//! Completion watches, delegation groups and stop redeliveries (Postgres).
//! Mirrors `completion_watch_repo.rs`, `delegation_group_repo.rs` and
//! `stop_redelivery_repo.rs`.
//!
//! `SQLite` orders by `created_at` alone; ties here fall back to insertion
//! order (`row_seq`). Text ordering uses `COLLATE "C"` to match `SQLite`'s
//! byte order.

use intent_core::{AgentId, Error, Result, WorkspaceId};
use sqlx::postgres::PgRow;
use sqlx::Row;

use super::{db, PgStore, WriteTx};
use crate::completion_watch_repo::PersistedCompletionWatch;
use crate::delegation_group_repo::PersistedDelegationGroup;
use crate::lease_repo::Lease;
use crate::stop_redelivery_repo::StopRedeliveryRow;

const WATCH_COLUMNS: &str = "id, parent_workspace_id, child_workspace_id, parent_agent_id, \
    parent_agent_name, child_agent_id, group_id, report_delivered, wake_on_attention, \
    completion_only, created_at";

const GROUP_COLUMNS: &str = "group_id, workspace_id, parent_agent_id, await_mode, \
    expected_agent_ids, completed_agent_ids, deleted_agent_ids, sealed, delivered, \
    event_summaries, raw_events, created_at, updated_at";

fn col<'r, T>(row: &'r PgRow, name: &str) -> Result<T>
where
    T: sqlx::Decode<'r, sqlx::Postgres> + sqlx::Type<sqlx::Postgres>,
{
    row.try_get::<T, _>(name)
        .map_err(|e| Error::Internal(format!("decode {name}: {e}")))
}

fn decode_watch_row(row: &PgRow) -> Result<PersistedCompletionWatch> {
    Ok(PersistedCompletionWatch {
        id: col(row, "id")?,
        parent_workspace_id: WorkspaceId(col(row, "parent_workspace_id")?),
        child_workspace_id: WorkspaceId(col(row, "child_workspace_id")?),
        parent_agent_id: AgentId(col(row, "parent_agent_id")?),
        parent_agent_name: col(row, "parent_agent_name")?,
        child_agent_id: AgentId(col(row, "child_agent_id")?),
        group_id: col(row, "group_id")?,
        report_delivered: col::<i64>(row, "report_delivered")? != 0,
        wake_on_attention: col::<i64>(row, "wake_on_attention")? != 0,
        completion_only: col::<i64>(row, "completion_only")? != 0,
        created_at: col(row, "created_at")?,
    })
}

fn json_list(row: &PgRow, name: &str) -> Result<Vec<String>> {
    let raw: String = col(row, name)?;
    serde_json::from_str(&raw).map_err(|e| Error::Internal(format!("parse {name}: {e}")))
}

fn agent_list(row: &PgRow, name: &str) -> Result<Vec<AgentId>> {
    Ok(json_list(row, name)?.into_iter().map(AgentId).collect())
}

fn decode_group_row(row: &PgRow) -> Result<PersistedDelegationGroup> {
    let expected_agent_ids = agent_list(row, "expected_agent_ids")?;
    let completed_agent_ids = agent_list(row, "completed_agent_ids")?;
    let deleted_agent_ids = agent_list(row, "deleted_agent_ids")?;
    let event_summaries = json_list(row, "event_summaries")?;
    let raw_events_json = json_list(row, "raw_events")?;
    Ok(PersistedDelegationGroup {
        group_id: col(row, "group_id")?,
        workspace_id: WorkspaceId(col(row, "workspace_id")?),
        parent_agent_id: AgentId(col(row, "parent_agent_id")?),
        await_mode: col(row, "await_mode")?,
        expected_agent_ids,
        completed_agent_ids,
        deleted_agent_ids,
        sealed: col::<i64>(row, "sealed")? != 0,
        delivered: col::<i64>(row, "delivered")? != 0,
        event_summaries,
        raw_events_json,
        created_at: col(row, "created_at")?,
        updated_at: col(row, "updated_at")?,
    })
}

fn encode<T: serde::Serialize>(value: &T, name: &str) -> Result<String> {
    serde_json::to_string(value).map_err(|e| Error::Internal(format!("encode {name}: {e}")))
}

impl PgStore {
    // ---- completion watches ----------------------------------------------------------

    pub async fn retire_completion_watch_after_delivery(
        &self,
        watch_id: &str,
        parent_agent_id: &AgentId,
        child_agent_id: &AgentId,
        completion_identity: Option<&str>,
        delivered_at: &str,
        claim: &Lease,
    ) -> Result<()> {
        // A process that lost the tenant gets `NotOwner` as it is.
        let mut tx = self.begin_write().await?;
        self.guard_lease(&mut tx, claim).await?;
        if let Some(identity) = completion_identity {
            sqlx::query(
                "INSERT INTO completion_wake_delivery \
                 (tenant_id, parent_agent_id, child_agent_id, completion_identity, delivered_at) \
                 VALUES ($1, $2, $3, $4, $5) \
                 ON CONFLICT (tenant_id, parent_agent_id, child_agent_id) DO UPDATE SET \
                 completion_identity = excluded.completion_identity, \
                 delivered_at = excluded.delivered_at",
            )
            .bind(self.tenant())
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
        sqlx::query("DELETE FROM completion_watch WHERE tenant_id = $1 AND id = $2")
            .bind(self.tenant())
            .bind(watch_id)
            .execute(&mut *tx)
            .await
            .map_err(|e| Error::Internal(format!("retire completion_watch delete failed: {e}")))?;
        self.retire_lease(&mut tx, claim).await?;
        tx.commit()
            .await
            .map_err(|e| Error::Internal(format!("retire completion_watch commit failed: {e}")))
    }

    pub async fn settle_delegation_group_after_delivery(
        &self,
        group_id: &str,
        retain_children: &[AgentId],
        claim: &Lease,
    ) -> Result<()> {
        let retained: Vec<&str> = retain_children.iter().map(|id| id.0.as_str()).collect();
        // A process that lost the tenant gets `NotOwner` as it is.
        let mut tx = self.begin_write().await?;
        self.guard_lease(&mut tx, claim).await?;
        sqlx::query(
            "UPDATE completion_watch SET group_id = NULL \
             WHERE tenant_id = $1 AND group_id = $2 AND child_agent_id = ANY($3)",
        )
        .bind(self.tenant())
        .bind(group_id)
        .bind(&retained)
        .execute(&mut *tx)
        .await
        .map_err(|e| {
            Error::Internal(format!("settle delegation_group retain watch failed: {e}"))
        })?;
        sqlx::query(
            "DELETE FROM completion_watch \
             WHERE tenant_id = $1 AND group_id = $2 AND NOT (child_agent_id = ANY($3))",
        )
        .bind(self.tenant())
        .bind(group_id)
        .bind(&retained)
        .execute(&mut *tx)
        .await
        .map_err(|e| {
            Error::Internal(format!(
                "settle delegation_group delete watches failed: {e}"
            ))
        })?;
        sqlx::query("DELETE FROM delegation_group WHERE tenant_id = $1 AND group_id = $2")
            .bind(self.tenant())
            .bind(group_id)
            .execute(&mut *tx)
            .await
            .map_err(|e| Error::Internal(format!("settle delegation_group delete failed: {e}")))?;
        self.retire_lease(&mut tx, claim).await?;
        tx.commit()
            .await
            .map_err(|e| Error::Internal(format!("settle delegation_group commit failed: {e}")))
    }

    pub async fn upsert_completion_watch(&self, w: &PersistedCompletionWatch) -> Result<()> {
        self.upsert_completion_watch_guarded(w, None).await
    }

    /// [`Self::upsert_completion_watch`], guarding `guard` (when given) in
    /// the write transaction first: a lost lease writes nothing.
    pub async fn upsert_completion_watch_guarded(
        &self,
        w: &PersistedCompletionWatch,
        guard: Option<&Lease>,
    ) -> Result<()> {
        let mut tx = self.begin_write().await?;
        if let Some(lease) = guard {
            self.guard_lease(&mut tx, lease).await?;
        }
        sqlx::query(
            "INSERT INTO completion_watch (tenant_id, id, parent_workspace_id, \
             child_workspace_id, parent_agent_id, parent_agent_name, child_agent_id, group_id, \
             report_delivered, wake_on_attention, completion_only, created_at) \
             VALUES ($1, $2, $3, $4, $5, $6, $7, $8, $9, $10, $11, $12) \
             ON CONFLICT (tenant_id, id) DO UPDATE SET \
             parent_workspace_id = excluded.parent_workspace_id, \
             parent_agent_name = excluded.parent_agent_name, \
             group_id = excluded.group_id, \
             report_delivered = excluded.report_delivered, \
             wake_on_attention = excluded.wake_on_attention, \
             completion_only = excluded.completion_only",
        )
        .bind(self.tenant())
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
        .execute(&mut *tx)
        .await
        .map_err(|e| Error::Internal(format!("upsert completion_watch failed: {e}")))?;
        tx.commit().await.map_err(db)
    }

    pub async fn find_ungrouped_completion_watch(
        &self,
        parent_agent_id: &AgentId,
        child_agent_id: &AgentId,
    ) -> Result<Option<PersistedCompletionWatch>> {
        let sql = format!(
            "SELECT {WATCH_COLUMNS} FROM completion_watch WHERE tenant_id = $1 \
             AND parent_agent_id = $2 AND child_agent_id = $3 AND group_id IS NULL \
             ORDER BY created_at COLLATE \"C\", row_seq LIMIT 1"
        );
        let mut tx = self.begin().await?;
        let row = sqlx::query(sqlx::AssertSqlSafe(sql.as_str()))
            .bind(self.tenant())
            .bind(&parent_agent_id.0)
            .bind(&child_agent_id.0)
            .fetch_optional(&mut *tx)
            .await
            .map_err(|e| Error::Internal(format!("find completion watch: {e}")))?;
        tx.commit().await.map_err(db)?;
        row.as_ref().map(decode_watch_row).transpose()
    }

    pub async fn list_completion_watches(&self) -> Result<Vec<PersistedCompletionWatch>> {
        let sql = format!(
            "SELECT {WATCH_COLUMNS} FROM completion_watch WHERE tenant_id = $1 \
             ORDER BY created_at COLLATE \"C\", row_seq"
        );
        let mut tx = self.begin().await?;
        let rows = sqlx::query(sqlx::AssertSqlSafe(sql.as_str()))
            .bind(self.tenant())
            .fetch_all(&mut *tx)
            .await
            .map_err(|e| Error::Internal(format!("list completion watches: {e}")))?;
        tx.commit().await.map_err(db)?;
        rows.iter().map(decode_watch_row).collect()
    }

    /// Run one write in `tx` (from the caller's `begin_write()`) and commit;
    /// the tenant binds as `$1`.
    async fn write_one(
        &self,
        mut tx: WriteTx,
        sql: &'static str,
        binds: &[&str],
        what: &str,
    ) -> Result<()> {
        let mut query = sqlx::query(sql).bind(self.tenant());
        for value in binds {
            query = query.bind(*value);
        }
        query
            .execute(&mut *tx)
            .await
            .map_err(|e| Error::Internal(format!("{what} failed: {e}")))?;
        tx.commit().await.map_err(db)
    }

    pub async fn delete_completion_watch(&self, id: &str) -> Result<()> {
        let tx = self.begin_write().await?;
        self.write_one(
            tx,
            "DELETE FROM completion_watch WHERE tenant_id = $1 AND id = $2",
            &[id],
            "delete completion_watch",
        )
        .await
    }

    pub async fn delete_completion_watches_for_parent(
        &self,
        parent_agent_id: &AgentId,
    ) -> Result<()> {
        let tx = self.begin_write().await?;
        self.write_one(
            tx,
            "DELETE FROM completion_watch WHERE tenant_id = $1 AND parent_agent_id = $2",
            &[&parent_agent_id.0],
            "delete completion_watches for parent",
        )
        .await
    }

    pub async fn mark_completion_watch_report_delivered(&self, id: &str) -> Result<()> {
        let tx = self.begin_write().await?;
        self.write_one(
            tx,
            "UPDATE completion_watch SET report_delivered = 1 WHERE tenant_id = $1 AND id = $2",
            &[id],
            "mark completion_watch report_delivered",
        )
        .await
    }

    pub async fn clear_completion_watch_report_delivered(&self, id: &str) -> Result<()> {
        let tx = self.begin_write().await?;
        self.write_one(
            tx,
            "UPDATE completion_watch SET report_delivered = 0 WHERE tenant_id = $1 AND id = $2",
            &[id],
            "clear completion_watch report_delivered",
        )
        .await
    }

    pub async fn ungroup_completion_watch(&self, id: &str) -> Result<()> {
        let tx = self.begin_write().await?;
        self.write_one(
            tx,
            "UPDATE completion_watch SET group_id = NULL WHERE tenant_id = $1 AND id = $2",
            &[id],
            "ungroup completion_watch",
        )
        .await
    }

    pub async fn update_completion_watch_parent(
        &self,
        id: &str,
        parent_agent_name: &str,
        parent_workspace_id: &WorkspaceId,
    ) -> Result<()> {
        let tx = self.begin_write().await?;
        self.write_one(
            tx,
            "UPDATE completion_watch SET parent_agent_name = $3, parent_workspace_id = $4 \
             WHERE tenant_id = $1 AND id = $2",
            &[id, parent_agent_name, &parent_workspace_id.0],
            "update completion_watch parent",
        )
        .await
    }

    // ---- delegation groups -----------------------------------------------------------

    pub async fn upsert_delegation_group(&self, g: &PersistedDelegationGroup) -> Result<()> {
        let expected_json = encode(&g.expected_agent_ids, "expected_agent_ids")?;
        let completed_json = encode(&g.completed_agent_ids, "completed_agent_ids")?;
        let deleted_json = encode(&g.deleted_agent_ids, "deleted_agent_ids")?;
        let summaries_json = encode(&g.event_summaries, "event_summaries")?;
        let raw_events_json = encode(&g.raw_events_json, "raw_events")?;
        let mut tx = self.begin_write().await?;
        sqlx::query(
            "INSERT INTO delegation_group (tenant_id, group_id, workspace_id, parent_agent_id, \
             await_mode, expected_agent_ids, completed_agent_ids, deleted_agent_ids, sealed, \
             delivered, event_summaries, raw_events, created_at, updated_at) \
             VALUES ($1, $2, $3, $4, $5, $6, $7, $8, $9, $10, $11, $12, $13, $14) \
             ON CONFLICT (tenant_id, group_id) DO UPDATE SET \
             expected_agent_ids = excluded.expected_agent_ids, \
             completed_agent_ids = excluded.completed_agent_ids, \
             deleted_agent_ids = excluded.deleted_agent_ids, \
             sealed = excluded.sealed, \
             delivered = excluded.delivered, \
             event_summaries = excluded.event_summaries, \
             raw_events = excluded.raw_events, \
             updated_at = excluded.updated_at",
        )
        .bind(self.tenant())
        .bind(&g.group_id)
        .bind(&g.workspace_id.0)
        .bind(&g.parent_agent_id.0)
        .bind(&g.await_mode)
        .bind(&expected_json)
        .bind(&completed_json)
        .bind(&deleted_json)
        .bind(i64::from(g.sealed))
        .bind(i64::from(g.delivered))
        .bind(&summaries_json)
        .bind(&raw_events_json)
        .bind(&g.created_at)
        .bind(&g.updated_at)
        .execute(&mut *tx)
        .await
        .map_err(|e| Error::Internal(format!("upsert delegation_group failed: {e}")))?;
        tx.commit().await.map_err(db)
    }

    pub async fn list_undelivered_groups(
        &self,
        workspace_id: &WorkspaceId,
    ) -> Result<Vec<PersistedDelegationGroup>> {
        let sql = format!(
            "SELECT {GROUP_COLUMNS} FROM delegation_group \
             WHERE tenant_id = $1 AND workspace_id = $2 AND delivered = 0 \
             ORDER BY created_at COLLATE \"C\", row_seq"
        );
        let mut tx = self.begin().await?;
        let rows = sqlx::query(sqlx::AssertSqlSafe(sql.as_str()))
            .bind(self.tenant())
            .bind(&workspace_id.0)
            .fetch_all(&mut *tx)
            .await
            .map_err(|e| Error::Internal(format!("list undelivered delegation groups: {e}")))?;
        tx.commit().await.map_err(db)?;
        rows.iter().map(decode_group_row).collect()
    }

    pub async fn delete_delegation_group(&self, group_id: &str) -> Result<()> {
        let tx = self.begin_write().await?;
        self.write_one(
            tx,
            "DELETE FROM delegation_group WHERE tenant_id = $1 AND group_id = $2",
            &[group_id],
            "delete delegation_group",
        )
        .await
    }

    pub async fn list_workspaces_with_undelivered_groups(&self) -> Result<Vec<WorkspaceId>> {
        let mut tx = self.begin().await?;
        let ids: Vec<String> = sqlx::query_scalar(
            "SELECT DISTINCT workspace_id COLLATE \"C\" AS workspace_id FROM delegation_group \
             WHERE tenant_id = $1 AND delivered = 0 ORDER BY 1",
        )
        .bind(self.tenant())
        .fetch_all(&mut *tx)
        .await
        .map_err(|e| Error::Internal(format!("list workspaces with undelivered groups: {e}")))?;
        tx.commit().await.map_err(db)?;
        Ok(ids.into_iter().map(WorkspaceId).collect())
    }

    // ---- stop redeliveries -----------------------------------------------------------

    pub async fn set_stop_redelivery(
        &self,
        agent_id: &AgentId,
        payload: &serde_json::Value,
        created_at: &str,
    ) -> Result<()> {
        let payload = serde_json::to_string(payload)
            .map_err(|e| Error::Internal(format!("encode stop redelivery payload failed: {e}")))?;
        let mut tx = self.begin_write().await?;
        sqlx::query(
            "INSERT INTO agent_stop_redelivery (tenant_id, agent_id, payload, created_at) \
             VALUES ($1, $2, $3, $4) \
             ON CONFLICT (tenant_id, agent_id) DO UPDATE SET \
             payload = excluded.payload, created_at = excluded.created_at",
        )
        .bind(self.tenant())
        .bind(&agent_id.0)
        .bind(payload)
        .bind(created_at)
        .execute(&mut *tx)
        .await
        .map_err(|e| Error::Internal(format!("set stop redelivery failed: {e}")))?;
        tx.commit().await.map_err(db)
    }

    pub async fn clear_stop_redelivery(&self, agent_id: &AgentId) -> Result<()> {
        let tx = self.begin_write().await?;
        self.write_one(
            tx,
            "DELETE FROM agent_stop_redelivery WHERE tenant_id = $1 AND agent_id = $2",
            &[&agent_id.0],
            "clear stop redelivery",
        )
        .await
    }

    pub async fn clear_stop_redeliveries(&self, agent_ids: &[AgentId]) -> Result<()> {
        if agent_ids.is_empty() {
            return Ok(());
        }
        let ids: Vec<&str> = agent_ids.iter().map(|id| id.0.as_str()).collect();
        let mut tx = self.begin_write().await?;
        sqlx::query(
            "DELETE FROM agent_stop_redelivery WHERE tenant_id = $1 AND agent_id = ANY($2)",
        )
        .bind(self.tenant())
        .bind(&ids)
        .execute(&mut *tx)
        .await
        .map_err(|e| Error::Internal(format!("clear stop redeliveries failed: {e}")))?;
        tx.commit().await.map_err(db)
    }

    pub async fn load_all_stop_redeliveries(&self) -> Result<Vec<StopRedeliveryRow>> {
        let mut tx = self.begin().await?;
        let rows: Vec<(String, String, String)> = sqlx::query_as(
            "SELECT r.agent_id, r.payload, r.created_at \
             FROM agent_stop_redelivery r \
             JOIN agent_session s ON s.tenant_id = $1 AND s.id = r.agent_id \
             WHERE r.tenant_id = $1 \
             ORDER BY r.agent_id COLLATE \"C\"",
        )
        .bind(self.tenant())
        .fetch_all(&mut *tx)
        .await
        .map_err(|e| Error::Internal(format!("load stop redeliveries failed: {e}")))?;
        tx.commit().await.map_err(db)?;
        Ok(rows
            .into_iter()
            .map(|(agent_id, payload, created_at)| StopRedeliveryRow {
                agent_id: AgentId(agent_id),
                payload: serde_json::from_str(&payload).unwrap_or(serde_json::Value::Null),
                created_at,
            })
            .collect())
    }
}

/// Drops the idempotency reservations of every other process ($1 tenant,
/// $2 process token; a reservation's token is `<process token>/<id>`), so a
/// retry runs the operation again (batch G 2b).
const CLEAR_FOREIGN_RESERVATIONS_SQL: &str = "DELETE FROM idempotency_key \
     WHERE tenant_id = $1 AND result_json IS NULL \
     AND claim_token NOT LIKE ($2 || '/%')";
/// Drops the delete grace claims of every other process ($1 tenant, $2
/// process token): their commits never finished, so the rows re-arm here.
const CLEAR_FOREIGN_GRACE_CLAIMS_SQL: &str = "UPDATE delete_grace \
     SET claim_epoch = NULL, claim_token = NULL \
     WHERE tenant_id = $1 AND claim_token IS NOT NULL AND claim_token <> $2";

impl PgStore {
    /// On this process's first open of the tenant, drops the idempotency
    /// reservations and delete grace claims other processes left (a crashed
    /// or stopped daemon): a process that opens the tenant anew takes over
    /// their keyed operations and deletes, as a takeover's newer epoch does.
    /// Without it a claim left at an unchanged epoch (a tenant never leased)
    /// would block its row for good. Later opens in the same process share
    /// its token and clear nothing. Delivery claims are leases and lapse by
    /// their TTL instead (`completion_watch_repo.rs`).
    ///
    /// Assumption: only the unleased open (`open_postgres`) runs this, and
    /// an unleased tenant is served by one process by design, so every
    /// other token's claim belongs to a process that is gone. A leased open
    /// (`open_postgres_owned`) never clears: there a live holder keeps its
    /// claim until a takeover's newer epoch makes it stale.
    pub(super) async fn clear_foreign_claims_once(&self) -> Result<()> {
        if self
            .tenant_state
            .foreign_claims_cleared
            .swap(true, std::sync::atomic::Ordering::SeqCst)
        {
            return Ok(());
        }
        let cleared = async {
            let mut tx = self.begin_write().await?;
            for sql in [
                CLEAR_FOREIGN_RESERVATIONS_SQL,
                CLEAR_FOREIGN_GRACE_CLAIMS_SQL,
            ] {
                sqlx::query(sql)
                    .bind(self.tenant())
                    .bind(self.process_token())
                    .execute(&mut *tx)
                    .await
                    .map_err(|e| Error::Internal(format!("clear foreign claims failed: {e}")))?;
            }
            tx.commit().await.map_err(db)
        }
        .await;
        if cleared.is_err() {
            // Let the next open try again.
            self.tenant_state
                .foreign_claims_cleared
                .store(false, std::sync::atomic::Ordering::SeqCst);
        }
        cleared
    }

    /// This process's token for the rows it claims; see [`ProcessToken`].
    pub(super) fn process_token(&self) -> &str {
        self.process_token.as_str()
    }
}

/// Names one process in the rows it claims (idempotency reservations,
/// delete grace claims, and the `holder` of its leases): a random id made
/// when the process first opens the tenant, shared by every store of the
/// tenant in the process (`tenant_registry`). With a claim's epoch it tells
/// this process's claim (re-entrant, releasable) from another's.
#[derive(Clone)]
pub(crate) struct ProcessToken(std::sync::Arc<str>);

impl Default for ProcessToken {
    fn default() -> Self {
        Self(uuid::Uuid::new_v4().to_string().into())
    }
}

impl ProcessToken {
    fn as_str(&self) -> &str {
        &self.0
    }

    #[cfg(feature = "test-support")]
    pub(crate) fn same_as(&self, other: &Self) -> bool {
        std::sync::Arc::ptr_eq(&self.0, &other.0)
    }
}
