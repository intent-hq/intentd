//! Background hooks (Postgres). Mirrors `hook_repo.rs`.
//!
//! `SQLite` orders by `created_at` alone; ties here fall back to insertion
//! order (`row_seq`). Text ordering uses `COLLATE "C"` to match `SQLite`'s
//! byte order.

use std::collections::HashSet;

use intent_core::{
    AgentId, Error, Hook, HookId, HookListRow, HookState, HookSummary, Result, WorkspaceId,
};
use sqlx::postgres::PgRow;
use sqlx::Row;

use super::{db, PgStore};
use crate::hook_repo::{state_from_db, state_to_db, ActiveHookMetadata};

const COLUMNS: &str = "hook_id, workspace_id, agent_id, name, code, delay_ms, state, \
    created_at, last_run_at, next_run_at, run_count, last_error, last_logs, last_state, \
    expires_at, perpetual, dispatch_count, cron, run_at";

/// `hook.list` projection: the heavy columns are read only for active rows.
const LIST_COLUMNS: &str = "hook_id, workspace_id, agent_id, name, delay_ms, state, \
    created_at, last_run_at, next_run_at, run_count, last_error, expires_at, perpetual, \
    dispatch_count, cron, run_at, \
    CASE WHEN state IN ('scheduled', 'running') THEN code END AS code, \
    CASE WHEN state IN ('scheduled', 'running') THEN last_logs END AS last_logs, \
    CASE WHEN state IN ('scheduled', 'running') THEN last_state END AS last_state";

fn col<'r, T>(row: &'r PgRow, name: &str) -> Result<T>
where
    T: sqlx::Decode<'r, sqlx::Postgres> + sqlx::Type<sqlx::Postgres>,
{
    row.try_get::<T, _>(name)
        .map_err(|e| Error::Internal(format!("read hook row failed: {e}")))
}

fn hook_from_row(r: &PgRow) -> Result<Hook> {
    Ok(Hook {
        hook_id: HookId(col(r, "hook_id")?),
        workspace_id: WorkspaceId(col(r, "workspace_id")?),
        agent_id: AgentId(col(r, "agent_id")?),
        name: col(r, "name")?,
        code: col(r, "code")?,
        delay_ms: col(r, "delay_ms")?,
        cron: col(r, "cron")?,
        run_at: col(r, "run_at")?,
        state: state_from_db(&col::<String>(r, "state")?)?,
        created_at: col(r, "created_at")?,
        last_run_at: col(r, "last_run_at")?,
        next_run_at: col(r, "next_run_at")?,
        run_count: col(r, "run_count")?,
        last_error: col(r, "last_error")?,
        last_logs: col(r, "last_logs")?,
        last_state: col(r, "last_state")?,
        expires_at: col(r, "expires_at")?,
        perpetual: col::<i64>(r, "perpetual")? != 0,
        dispatch_count: col(r, "dispatch_count")?,
    })
}

fn hook_list_row_from_row(r: &PgRow) -> Result<HookListRow> {
    let state = state_from_db(&col::<String>(r, "state")?)?;
    if matches!(state, HookState::Scheduled | HookState::Running) {
        return hook_from_row(r).map(HookListRow::Active);
    }
    Ok(HookListRow::Retired(HookSummary {
        hook_id: HookId(col(r, "hook_id")?),
        workspace_id: WorkspaceId(col(r, "workspace_id")?),
        agent_id: AgentId(col(r, "agent_id")?),
        name: col(r, "name")?,
        delay_ms: col(r, "delay_ms")?,
        cron: col(r, "cron")?,
        run_at: col(r, "run_at")?,
        state,
        created_at: col(r, "created_at")?,
        last_run_at: col(r, "last_run_at")?,
        next_run_at: col(r, "next_run_at")?,
        run_count: col(r, "run_count")?,
        last_error: col(r, "last_error")?,
        expires_at: col(r, "expires_at")?,
        perpetual: col::<i64>(r, "perpetual")? != 0,
        dispatch_count: col(r, "dispatch_count")?,
    }))
}

fn not_found(hook_id: &HookId) -> Error {
    Error::NotFound(format!("hook {} not found", hook_id.0))
}

/// `NotFound` when an UPDATE by hook id matched no row.
fn updated(res: &sqlx::postgres::PgQueryResult, hook_id: &HookId) -> Result<()> {
    if res.rows_affected() == 0 {
        return Err(not_found(hook_id));
    }
    Ok(())
}

impl PgStore {
    async fn hook_rows(&self, sql: &str, binds: &[&str], what: &str) -> Result<Vec<PgRow>> {
        let mut tx = self.begin().await?;
        let mut query = sqlx::query(sqlx::AssertSqlSafe(sql)).bind(self.tenant());
        for value in binds {
            query = query.bind(*value);
        }
        let rows = query
            .fetch_all(&mut *tx)
            .await
            .map_err(|e| Error::Internal(format!("{what} failed: {e}")))?;
        tx.commit().await.map_err(db)?;
        Ok(rows)
    }

    pub async fn insert_hook(&self, h: &Hook) -> Result<()> {
        let sql = format!(
            "INSERT INTO hook (tenant_id, {COLUMNS}) VALUES ($1, $2, $3, $4, $5, $6, $7, $8, \
             $9, $10, $11, $12, $13, $14, $15, $16, $17, $18, $19, $20)"
        );
        let mut tx = self.begin_write().await?;
        sqlx::query(sqlx::AssertSqlSafe(sql.as_str()))
            .bind(self.tenant())
            .bind(&h.hook_id.0)
            .bind(&h.workspace_id.0)
            .bind(&h.agent_id.0)
            .bind(&h.name)
            .bind(&h.code)
            .bind(h.delay_ms)
            .bind(state_to_db(h.state))
            .bind(&h.created_at)
            .bind(&h.last_run_at)
            .bind(&h.next_run_at)
            .bind(h.run_count)
            .bind(&h.last_error)
            .bind(&h.last_logs)
            .bind(&h.last_state)
            .bind(&h.expires_at)
            .bind(i64::from(h.perpetual))
            .bind(h.dispatch_count)
            .bind(&h.cron)
            .bind(&h.run_at)
            .execute(&mut *tx)
            .await
            .map_err(|e| Error::Internal(format!("insert hook failed: {e}")))?;
        tx.commit().await.map_err(db)
    }

    pub async fn get_hook(&self, hook_id: &HookId) -> Result<Hook> {
        let sql = format!("SELECT {COLUMNS} FROM hook WHERE tenant_id = $1 AND hook_id = $2");
        let rows = self.hook_rows(&sql, &[&hook_id.0], "get hook").await?;
        rows.first()
            .map_or_else(|| Err(not_found(hook_id)), hook_from_row)
    }

    pub async fn list_hooks_by_workspace(&self, workspace_id: &WorkspaceId) -> Result<Vec<Hook>> {
        let sql = format!(
            "SELECT {COLUMNS} FROM hook WHERE tenant_id = $1 AND workspace_id = $2 \
             ORDER BY created_at COLLATE \"C\", row_seq"
        );
        let rows = self
            .hook_rows(&sql, &[&workspace_id.0], "list hooks by workspace")
            .await?;
        rows.iter().map(hook_from_row).collect()
    }

    pub async fn active_hook_metadata_by_agent(
        &self,
        agent_id: &AgentId,
    ) -> Result<Vec<ActiveHookMetadata>> {
        self.active_hook_metadata(
            "SELECT agent_id, hook_id, name, next_run_at, expires_at FROM hook \
             WHERE tenant_id = $1 AND agent_id = $2 AND state IN ('scheduled', 'running') \
             ORDER BY created_at COLLATE \"C\", row_seq",
            &agent_id.0,
        )
        .await
    }

    pub async fn active_hook_metadata_by_workspace(
        &self,
        workspace_id: &WorkspaceId,
    ) -> Result<Vec<ActiveHookMetadata>> {
        self.active_hook_metadata(
            "SELECT agent_id, hook_id, name, next_run_at, expires_at FROM hook \
             WHERE tenant_id = $1 AND workspace_id = $2 AND state IN ('scheduled', 'running') \
             ORDER BY created_at COLLATE \"C\", row_seq",
            &workspace_id.0,
        )
        .await
    }

    async fn active_hook_metadata(&self, sql: &str, id: &str) -> Result<Vec<ActiveHookMetadata>> {
        let rows = self
            .hook_rows(sql, &[id], "read active hook metadata")
            .await?;
        rows.iter()
            .map(|row| {
                Ok(ActiveHookMetadata {
                    agent_id: AgentId(col(row, "agent_id")?),
                    hook_id: HookId(col(row, "hook_id")?),
                    name: col(row, "name")?,
                    next_run_at: col(row, "next_run_at")?,
                    expires_at: col(row, "expires_at")?,
                })
            })
            .collect()
    }

    pub async fn list_hook_rows(
        &self,
        workspace_id: &WorkspaceId,
        agent_id: Option<&AgentId>,
        include_retired: bool,
    ) -> Result<Vec<HookListRow>> {
        let state_clause = if include_retired {
            ""
        } else {
            " AND state IN ('scheduled', 'running')"
        };
        let sql = format!(
            "SELECT {LIST_COLUMNS} FROM hook WHERE tenant_id = $1 AND workspace_id = $2 \
             AND ($3::text IS NULL OR agent_id = $3){state_clause} \
             ORDER BY created_at COLLATE \"C\", row_seq"
        );
        let mut tx = self.begin().await?;
        let rows = sqlx::query(sqlx::AssertSqlSafe(sql.as_str()))
            .bind(self.tenant())
            .bind(&workspace_id.0)
            .bind(agent_id.map(|a| a.0.as_str()))
            .fetch_all(&mut *tx)
            .await
            .map_err(|e| Error::Internal(format!("list hook rows failed: {e}")))?;
        tx.commit().await.map_err(db)?;
        rows.iter().map(hook_list_row_from_row).collect()
    }

    pub async fn list_hook_rows_page(
        &self,
        workspace_id: &WorkspaceId,
        include_retired: bool,
        after: Option<(&str, &str)>,
        limit: usize,
    ) -> Result<Vec<(String, HookListRow)>> {
        let state_clause = if include_retired {
            ""
        } else {
            " AND state IN ('scheduled', 'running')"
        };
        let sql = format!(
            "SELECT {LIST_COLUMNS} FROM hook WHERE tenant_id = $1 AND workspace_id = $2\
             {state_clause} AND ($3::text IS NULL \
             OR (created_at COLLATE \"C\", hook_id COLLATE \"C\") > ($3, $4)) \
             ORDER BY created_at COLLATE \"C\", hook_id COLLATE \"C\" LIMIT $5"
        );
        let (created_at, hook_id) = after.unzip();
        let mut tx = self.begin().await?;
        let rows = sqlx::query(sqlx::AssertSqlSafe(sql.as_str()))
            .bind(self.tenant())
            .bind(&workspace_id.0)
            .bind(created_at)
            .bind(hook_id)
            .bind(i64::try_from(limit).unwrap_or(i64::MAX))
            .fetch_all(&mut *tx)
            .await
            .map_err(|e| Error::Internal(format!("list hook page failed: {e}")))?;
        tx.commit().await.map_err(db)?;
        rows.iter()
            .map(|row| {
                Ok((
                    col::<String>(row, "created_at")?,
                    hook_list_row_from_row(row)?,
                ))
            })
            .collect()
    }

    async fn count_active_hooks(&self, sql: &str, id: &str, what: &str) -> Result<u64> {
        let mut tx = self.begin().await?;
        let n: i64 = sqlx::query_scalar(sqlx::AssertSqlSafe(sql))
            .bind(self.tenant())
            .bind(id)
            .fetch_one(&mut *tx)
            .await
            .map_err(|e| Error::Internal(format!("{what} failed: {e}")))?;
        tx.commit().await.map_err(db)?;
        Ok(n.cast_unsigned())
    }

    pub async fn count_active_hooks_by_agent(&self, agent_id: &AgentId) -> Result<u64> {
        self.count_active_hooks(
            "SELECT COUNT(*) FROM hook \
             WHERE tenant_id = $1 AND agent_id = $2 AND state IN ('scheduled', 'running')",
            &agent_id.0,
            "count active hooks",
        )
        .await
    }

    pub async fn count_active_hooks_by_workspace(&self, workspace_id: &WorkspaceId) -> Result<u64> {
        self.count_active_hooks(
            "SELECT COUNT(*) FROM hook \
             WHERE tenant_id = $1 AND workspace_id = $2 AND state IN ('scheduled', 'running')",
            &workspace_id.0,
            "count workspace active hooks",
        )
        .await
    }

    pub async fn workspaces_with_active_hooks(
        &self,
        workspace_ids: &[WorkspaceId],
    ) -> Result<HashSet<WorkspaceId>> {
        if workspace_ids.is_empty() {
            return Ok(HashSet::new());
        }
        let ids: Vec<&str> = workspace_ids.iter().map(|id| id.0.as_str()).collect();
        let mut tx = self.begin().await?;
        let rows: Vec<String> = sqlx::query_scalar(
            "SELECT DISTINCT workspace_id FROM hook WHERE tenant_id = $1 \
             AND workspace_id = ANY($2) AND state IN ('scheduled', 'running')",
        )
        .bind(self.tenant())
        .bind(&ids)
        .fetch_all(&mut *tx)
        .await
        .map_err(|e| Error::Internal(format!("batch active workspace hooks failed: {e}")))?;
        tx.commit().await.map_err(db)?;
        Ok(rows.into_iter().map(WorkspaceId).collect())
    }

    pub async fn list_hooks_by_agent(&self, agent_id: &AgentId) -> Result<Vec<Hook>> {
        let sql = format!(
            "SELECT {COLUMNS} FROM hook WHERE tenant_id = $1 AND agent_id = $2 \
             ORDER BY created_at COLLATE \"C\", row_seq"
        );
        let rows = self
            .hook_rows(&sql, &[&agent_id.0], "list hooks by agent")
            .await?;
        rows.iter().map(hook_from_row).collect()
    }

    pub async fn update_hook_state(&self, hook_id: &HookId, state: HookState) -> Result<()> {
        let mut tx = self.begin_write().await?;
        let res = sqlx::query("UPDATE hook SET state = $3 WHERE tenant_id = $1 AND hook_id = $2")
            .bind(self.tenant())
            .bind(&hook_id.0)
            .bind(state_to_db(state))
            .execute(&mut *tx)
            .await
            .map_err(|e| Error::Internal(format!("hook state update failed: {e}")))?;
        updated(&res, hook_id)?;
        tx.commit().await.map_err(db)
    }

    pub async fn expire_hook(&self, hook_id: &HookId) -> Result<()> {
        let mut tx = self.begin_write().await?;
        let res = sqlx::query(
            "UPDATE hook SET state = $3, next_run_at = NULL WHERE tenant_id = $1 AND hook_id = $2",
        )
        .bind(self.tenant())
        .bind(&hook_id.0)
        .bind(state_to_db(HookState::Expired))
        .execute(&mut *tx)
        .await
        .map_err(|e| Error::Internal(format!("expire hook failed: {e}")))?;
        updated(&res, hook_id)?;
        tx.commit().await.map_err(db)
    }

    pub async fn update_hook_run(
        &self,
        hook_id: &HookId,
        last_run_at: &str,
        next_run_at: Option<&str>,
    ) -> Result<()> {
        let mut tx = self.begin_write().await?;
        let res = sqlx::query(
            "UPDATE hook SET \
             run_count = CASE WHEN last_run_at IS NOT DISTINCT FROM $3 THEN run_count ELSE run_count + 1 END, \
             last_run_at = $3, next_run_at = $4 \
             WHERE tenant_id = $1 AND hook_id = $2",
        )
        .bind(self.tenant())
        .bind(&hook_id.0)
        .bind(last_run_at)
        .bind(next_run_at)
        .execute(&mut *tx)
        .await
        .map_err(|e| Error::Internal(format!("hook run update failed: {e}")))?;
        updated(&res, hook_id)?;
        tx.commit().await.map_err(db)
    }

    pub async fn record_hook_dispatch(&self, hook_id: &HookId, dispatch_count: i64) -> Result<()> {
        let mut tx = self.begin_write().await?;
        let res = sqlx::query(
            "UPDATE hook SET dispatch_count = GREATEST(dispatch_count, $3) \
             WHERE tenant_id = $1 AND hook_id = $2",
        )
        .bind(self.tenant())
        .bind(&hook_id.0)
        .bind(dispatch_count)
        .execute(&mut *tx)
        .await
        .map_err(|e| Error::Internal(format!("hook dispatch count update failed: {e}")))?;
        updated(&res, hook_id)?;
        tx.commit().await.map_err(db)
    }

    pub async fn update_hook_next_run(
        &self,
        hook_id: &HookId,
        next_run_at: Option<&str>,
    ) -> Result<()> {
        let mut tx = self.begin_write().await?;
        let res =
            sqlx::query("UPDATE hook SET next_run_at = $3 WHERE tenant_id = $1 AND hook_id = $2")
                .bind(self.tenant())
                .bind(&hook_id.0)
                .bind(next_run_at)
                .execute(&mut *tx)
                .await
                .map_err(|e| Error::Internal(format!("hook next run update failed: {e}")))?;
        updated(&res, hook_id)?;
        tx.commit().await.map_err(db)
    }

    pub async fn update_hook_last_error(
        &self,
        hook_id: &HookId,
        last_error: Option<&str>,
    ) -> Result<()> {
        let mut tx = self.begin_write().await?;
        let res =
            sqlx::query("UPDATE hook SET last_error = $3 WHERE tenant_id = $1 AND hook_id = $2")
                .bind(self.tenant())
                .bind(&hook_id.0)
                .bind(last_error)
                .execute(&mut *tx)
                .await
                .map_err(|e| Error::Internal(format!("hook last error update failed: {e}")))?;
        updated(&res, hook_id)?;
        tx.commit().await.map_err(db)
    }

    pub async fn update_hook_last_logs(
        &self,
        hook_id: &HookId,
        last_logs: Option<&str>,
    ) -> Result<()> {
        let mut tx = self.begin_write().await?;
        let res =
            sqlx::query("UPDATE hook SET last_logs = $3 WHERE tenant_id = $1 AND hook_id = $2")
                .bind(self.tenant())
                .bind(&hook_id.0)
                .bind(last_logs)
                .execute(&mut *tx)
                .await
                .map_err(|e| Error::Internal(format!("hook last logs update failed: {e}")))?;
        updated(&res, hook_id)?;
        tx.commit().await.map_err(db)
    }

    pub async fn update_hook_last_state(
        &self,
        hook_id: &HookId,
        last_state: Option<&str>,
    ) -> Result<()> {
        let mut tx = self.begin_write().await?;
        let res =
            sqlx::query("UPDATE hook SET last_state = $3 WHERE tenant_id = $1 AND hook_id = $2")
                .bind(self.tenant())
                .bind(&hook_id.0)
                .bind(last_state)
                .execute(&mut *tx)
                .await
                .map_err(|e| Error::Internal(format!("hook last state update failed: {e}")))?;
        updated(&res, hook_id)?;
        tx.commit().await.map_err(db)
    }

    pub async fn load_active_hooks(&self) -> Result<Vec<Hook>> {
        let sql = format!(
            "SELECT {COLUMNS} FROM hook WHERE tenant_id = $1 AND state IN ('scheduled', 'running') \
             ORDER BY created_at COLLATE \"C\", row_seq"
        );
        let rows = self.hook_rows(&sql, &[], "load active hooks").await?;
        rows.iter().map(hook_from_row).collect()
    }
}
