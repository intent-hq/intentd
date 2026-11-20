//! Script definitions, run admission and the script list page (Postgres).
//! Mirrors `script_repo.rs`, `script_list_repo.rs` and the script-table read
//! in `script_monitor_repo.rs` (`latest_script_run`).
//!
//! `SQLite`'s `INSERT OR REPLACE` deletes the old row and inserts a new one,
//! so unlisted columns (run admission, `was_running`) return to their
//! defaults and the insert trigger runs. The upserts here do the same with a
//! DELETE and an INSERT in one write transaction.
//!
//! Text ordering uses `COLLATE "C"` to match `SQLite`'s byte order.

use std::collections::HashSet;

use intent_core::script_lists::{self, ScriptListOptions};
use intent_core::{AgentId, Error, Result, Script, ScriptLastRun, WorkspaceId};
use serde_json::{json, Map, Value};
use sqlx::postgres::PgRow;
use sqlx::Row;

use super::{db, PgStore, WriteTx};
use crate::enum_to_db;
use crate::script_repo::{env_from_db, env_to_db, remote_author_to_db};

const SCRIPT_COLUMNS: &str = "id, workspace_id, name, command, cwd, env, mode, category, \
    source, auto_start, created_at, updated_at, purpose, archived_at, last_run, remote_author";

fn col<'r, T>(row: &'r PgRow, name: &str) -> Result<T>
where
    T: sqlx::Decode<'r, sqlx::Postgres> + sqlx::Type<sqlx::Postgres>,
{
    row.try_get::<T, _>(name)
        .map_err(|e| Error::Internal(format!("column {name}: {e}")))
}

fn map_script_row(r: &PgRow) -> Result<Script> {
    Ok(Script {
        purpose: crate::enum_from_db(&col::<String>(r, "purpose")?)?,
        archived_at: col(r, "archived_at")?,
        last_run: col::<Option<String>>(r, "last_run")?
            .map(|value| serde_json::from_str(&value))
            .transpose()
            .map_err(|e| Error::Internal(format!("decode script result failed: {e}")))?,
        id: col(r, "id")?,
        workspace_id: col(r, "workspace_id")?,
        name: col(r, "name")?,
        command: col(r, "command")?,
        cwd: col(r, "cwd")?,
        env: env_from_db(col(r, "env")?)?,
        mode: crate::enum_from_db(&col::<String>(r, "mode")?)?,
        category: col(r, "category")?,
        source: col(r, "source")?,
        auto_start: col::<Option<i64>>(r, "auto_start")?.map(|v| v != 0),
        created_at: col(r, "created_at")?,
        updated_at: col(r, "updated_at")?,
        remote_author: col::<Option<String>>(r, "remote_author")?
            .map(|value| serde_json::from_str(&value))
            .transpose()
            .map_err(|e| Error::Internal(format!("decode script remoteAuthor failed: {e}")))?,
    })
}

/// The bound values of one script row, in `SCRIPT_COLUMNS` order.
struct ScriptValues<'a> {
    s: &'a Script,
    env: Option<String>,
    mode: String,
    purpose: String,
    last_run: Option<String>,
    remote_author: Option<String>,
}

impl<'a> ScriptValues<'a> {
    fn new(s: &'a Script) -> Result<Self> {
        Ok(Self {
            s,
            env: env_to_db(s)?,
            mode: enum_to_db(&s.mode)?,
            purpose: enum_to_db(&s.purpose)?,
            last_run: s
                .last_run
                .as_ref()
                .map(serde_json::to_string)
                .transpose()
                .map_err(|e| Error::Internal(format!("encode script result failed: {e}")))?,
            remote_author: remote_author_to_db(s)?,
        })
    }

    fn bind<'q>(
        &'q self,
        query: sqlx::query::Query<'q, sqlx::Postgres, sqlx::postgres::PgArguments>,
    ) -> sqlx::query::Query<'q, sqlx::Postgres, sqlx::postgres::PgArguments> {
        let s = self.s;
        query
            .bind(&s.id)
            .bind(&s.workspace_id)
            .bind(&s.name)
            .bind(&s.command)
            .bind(&s.cwd)
            .bind(&self.env)
            .bind(&self.mode)
            .bind(&s.category)
            .bind(&s.source)
            .bind(s.auto_start.map(i64::from))
            .bind(&s.created_at)
            .bind(&s.updated_at)
            .bind(&self.purpose)
            .bind(&s.archived_at)
            .bind(&self.last_run)
            .bind(&self.remote_author)
    }
}

impl PgStore {
    async fn script_rows(&self, sql: &str, binds: &[&str]) -> Result<Vec<Script>> {
        let mut tx = self.begin().await?;
        let mut query = sqlx::query(sqlx::AssertSqlSafe(sql)).bind(self.tenant());
        for value in binds {
            query = query.bind(*value);
        }
        let rows = query.fetch_all(&mut *tx).await.map_err(db)?;
        tx.commit().await.map_err(db)?;
        rows.iter().map(map_script_row).collect()
    }

    /// Replace the rows of `scripts` (by id) inside the caller's transaction.
    /// A repeated id keeps its last definition, as consecutive replaces do.
    /// `what` names the operation in errors, as the `SQLite` messages do.
    async fn replace_scripts(
        &self,
        tx: &mut WriteTx,
        scripts: &[Script],
        what: &str,
    ) -> Result<()> {
        const MAX_BINDS_PER_STATEMENT: usize = 32_000;
        let mut seen = HashSet::new();
        let mut last: Vec<&Script> = scripts
            .iter()
            .rev()
            .filter(|s| seen.insert(s.id.as_str()))
            .collect();
        last.reverse();
        let binds_per_row = SCRIPT_COLUMNS.split(',').count();
        let rows_per_statement = MAX_BINDS_PER_STATEMENT / binds_per_row;
        for chunk in last.chunks(rows_per_statement) {
            let ids: Vec<&str> = chunk.iter().map(|s| s.id.as_str()).collect();
            sqlx::query("DELETE FROM script WHERE tenant_id = $1 AND id = ANY($2)")
                .bind(self.tenant())
                .bind(&ids)
                .execute(&mut **tx)
                .await
                .map_err(|e| Error::Internal(format!("{what} failed: {e}")))?;
            let mut placeholders = Vec::with_capacity(chunk.len());
            for row in 0..chunk.len() {
                let first = 2 + row * binds_per_row;
                let params: Vec<String> = (first..first + binds_per_row)
                    .map(|n| format!("${n}"))
                    .collect();
                placeholders.push(format!("($1, {})", params.join(", ")));
            }
            let sql = format!(
                "INSERT INTO script (tenant_id, {SCRIPT_COLUMNS}) VALUES {}",
                placeholders.join(", ")
            );
            let values = chunk
                .iter()
                .map(|s| ScriptValues::new(s))
                .collect::<Result<Vec<_>>>()?;
            let mut query = sqlx::query(sqlx::AssertSqlSafe(sql.as_str())).bind(self.tenant());
            for v in &values {
                query = v.bind(query);
            }
            query
                .execute(&mut **tx)
                .await
                .map_err(|e| Error::Internal(format!("{what} failed: {e}")))?;
        }
        Ok(())
    }

    // ---- script_repo ----------------------------------------------------

    pub async fn workspace_scripts_initialized(&self, workspace_id: &WorkspaceId) -> Result<bool> {
        let mut tx = self.begin().await?;
        let value: Option<i64> = sqlx::query_scalar(
            "SELECT scripts_initialized FROM workspace WHERE tenant_id = $1 AND id = $2",
        )
        .bind(self.tenant())
        .bind(workspace_id.as_str())
        .fetch_optional(&mut *tx)
        .await
        .map_err(|e| Error::Internal(format!("get scripts initialization failed: {e}")))?;
        tx.commit().await.map_err(db)?;
        Ok(value.is_some_and(|v| v != 0))
    }

    pub async fn script_workspace(&self, id: &str) -> Result<Option<WorkspaceId>> {
        let mut tx = self.begin().await?;
        let workspace: Option<String> =
            sqlx::query_scalar("SELECT workspace_id FROM script WHERE tenant_id = $1 AND id = $2")
                .bind(self.tenant())
                .bind(id)
                .fetch_optional(&mut *tx)
                .await
                .map_err(|e| Error::Internal(format!("get script workspace failed: {e}")))?;
        tx.commit().await.map_err(db)?;
        Ok(workspace.map(WorkspaceId::from))
    }

    pub async fn upsert_script(&self, s: &Script) -> Result<()> {
        self.upsert_script_with_scope(s, false).await
    }

    pub async fn upsert_script_in_workspace(&self, s: &Script) -> Result<()> {
        self.upsert_script_with_scope(s, true).await
    }

    async fn upsert_script_with_scope(&self, s: &Script, scoped: bool) -> Result<()> {
        let mut tx = self.begin_write().await?;
        if scoped {
            let foreign: bool = sqlx::query_scalar(
                "SELECT EXISTS(SELECT 1 FROM script \
                 WHERE tenant_id = $1 AND id = $2 AND workspace_id <> $3)",
            )
            .bind(self.tenant())
            .bind(&s.id)
            .bind(&s.workspace_id)
            .fetch_one(&mut *tx)
            .await
            .map_err(|e| Error::Internal(format!("upsert script failed: {e}")))?;
            if foreign {
                return Err(Error::NotFound(format!("script {}", s.id)));
            }
        }
        self.replace_scripts(&mut tx, std::slice::from_ref(s), "upsert script")
            .await?;
        tx.commit()
            .await
            .map_err(|e| Error::Internal(format!("upsert script failed: {e}")))
    }

    pub async fn upsert_scripts(&self, scripts: &[Script]) -> Result<()> {
        self.upsert_scripts_inner(scripts, None).await.map(|_| ())
    }

    pub async fn bootstrap_scripts(
        &self,
        workspace_id: &WorkspaceId,
        scripts: &[Script],
    ) -> Result<bool> {
        if scripts.is_empty() {
            return Ok(false);
        }
        if scripts
            .iter()
            .any(|s| s.workspace_id != workspace_id.as_str())
        {
            return Err(Error::InvalidParams(
                "bootstrap scripts workspace mismatch".into(),
            ));
        }
        self.upsert_scripts_inner(scripts, Some(workspace_id)).await
    }

    async fn upsert_scripts_inner(
        &self,
        scripts: &[Script],
        initialize_workspace: Option<&WorkspaceId>,
    ) -> Result<bool> {
        let mut tx = self.begin_write().await?;
        if let Some(workspace_id) = initialize_workspace {
            let claimed = sqlx::query(
                "UPDATE workspace SET scripts_initialized = 1 \
                 WHERE tenant_id = $1 AND id = $2 AND scripts_initialized = 0",
            )
            .bind(self.tenant())
            .bind(workspace_id.as_str())
            .execute(&mut *tx)
            .await
            .map_err(|e| Error::Internal(format!("claim script initialization failed: {e}")))?;
            if claimed.rows_affected() == 0 {
                return Ok(false);
            }
        }
        self.replace_scripts(&mut tx, scripts, "bulk upsert scripts")
            .await?;
        tx.commit()
            .await
            .map_err(|e| Error::Internal(format!("bulk upsert scripts commit failed: {e}")))?;
        Ok(true)
    }

    pub async fn remove_script(&self, id: &str) -> Result<bool> {
        let mut tx = self.begin_write().await?;
        let res = sqlx::query("DELETE FROM script WHERE tenant_id = $1 AND id = $2")
            .bind(self.tenant())
            .bind(id)
            .execute(&mut *tx)
            .await
            .map_err(|e| Error::Internal(format!("remove script failed: {e}")))?;
        tx.commit().await.map_err(db)?;
        Ok(res.rows_affected() > 0)
    }

    pub async fn remove_script_in_workspace(
        &self,
        workspace_id: &WorkspaceId,
        id: &str,
    ) -> Result<()> {
        let mut tx = self.begin_write().await?;
        let res = sqlx::query(
            "DELETE FROM script WHERE tenant_id = $1 AND id = $2 AND workspace_id = $3",
        )
        .bind(self.tenant())
        .bind(id)
        .bind(workspace_id.as_str())
        .execute(&mut *tx)
        .await
        .map_err(|e| Error::Internal(format!("remove script failed: {e}")))?;
        if res.rows_affected() == 0 {
            return Err(Error::NotFound(format!("script {id}")));
        }
        tx.commit().await.map_err(db)
    }

    pub async fn list_all_scripts(&self) -> Result<Vec<Script>> {
        let sql = format!(
            "SELECT {SCRIPT_COLUMNS} FROM script WHERE tenant_id = $1 \
             ORDER BY created_at COLLATE \"C\", id COLLATE \"C\""
        );
        self.script_rows(&sql, &[]).await
    }

    pub async fn list_unarchived_scripts(&self) -> Result<Vec<Script>> {
        let sql = format!(
            "SELECT {SCRIPT_COLUMNS} FROM script WHERE tenant_id = $1 AND archived_at IS NULL \
             ORDER BY created_at COLLATE \"C\", id COLLATE \"C\""
        );
        self.script_rows(&sql, &[]).await
    }

    pub async fn get_active_script(&self, ws: &WorkspaceId, id: &str) -> Result<Option<Script>> {
        let sql = format!(
            "SELECT {SCRIPT_COLUMNS} FROM script \
             WHERE tenant_id = $1 AND workspace_id = $2 AND id = $3 AND archived_at IS NULL"
        );
        Ok(self
            .script_rows(&sql, &[ws.as_str(), id])
            .await?
            .into_iter()
            .next())
    }

    pub async fn get_script_in_workspace(
        &self,
        workspace_id: &WorkspaceId,
        id: &str,
    ) -> Result<Option<Script>> {
        let sql = format!(
            "SELECT {SCRIPT_COLUMNS} FROM script WHERE tenant_id = $1 AND id = $2 AND workspace_id = $3"
        );
        Ok(self
            .script_rows(&sql, &[id, workspace_id.as_str()])
            .await?
            .into_iter()
            .next())
    }

    pub async fn set_script_archived_at(
        &self,
        workspace_id: &WorkspaceId,
        id: &str,
        archived_at: Option<&str>,
    ) -> Result<()> {
        let mut tx = self.begin_write().await?;
        let result = sqlx::query(
            "UPDATE script SET archived_at = $4 WHERE tenant_id = $1 AND workspace_id = $2 AND id = $3",
        )
        .bind(self.tenant())
        .bind(workspace_id.as_str())
        .bind(id)
        .bind(archived_at)
        .execute(&mut *tx)
        .await
        .map_err(|e| Error::Internal(format!("archive script failed: {e}")))?;
        if result.rows_affected() == 0 {
            return Err(Error::NotFound(format!("script {id}")));
        }
        tx.commit().await.map_err(db)
    }

    pub async fn admit_script_run(&self, ws: &WorkspaceId, id: &str, token: &str) -> Result<()> {
        let mut tx = self.begin_write().await?;
        let result = sqlx::query(
            "UPDATE script SET pending_run_id = $4, latest_run_id = $4, latest_run_result = NULL, \
             pending_started_at = NULL, \
             was_running = CASE WHEN mode = 'command' THEN 1 ELSE was_running END \
             WHERE tenant_id = $1 AND workspace_id = $2 AND id = $3",
        )
        .bind(self.tenant())
        .bind(ws.as_str())
        .bind(id)
        .bind(token)
        .execute(&mut *tx)
        .await
        .map_err(|e| Error::Internal(format!("admit script run failed: {e}")))?;
        if result.rows_affected() == 0 {
            return Err(Error::NotFound(format!("script {id}")));
        }
        tx.commit().await.map_err(db)
    }

    pub async fn start_script_run(
        &self,
        ws: &WorkspaceId,
        id: &str,
        token: &str,
        started: &str,
    ) -> Result<()> {
        let mut tx = self.begin_write().await?;
        sqlx::query(
            "UPDATE script SET pending_started_at = $5, was_running = 1 \
             WHERE tenant_id = $1 AND workspace_id = $2 AND id = $3 AND pending_run_id = $4",
        )
        .bind(self.tenant())
        .bind(ws.as_str())
        .bind(id)
        .bind(token)
        .bind(started)
        .execute(&mut *tx)
        .await
        .map_err(|e| Error::Internal(format!("start script run failed: {e}")))?;
        tx.commit().await.map_err(db)
    }

    pub async fn abandon_script_run(
        &self,
        ws: &WorkspaceId,
        id: &str,
        token: &str,
        preserve_marker: bool,
    ) -> Result<()> {
        let mut tx = self.begin_write().await?;
        sqlx::query(
            "UPDATE script SET pending_run_id = NULL, pending_started_at = NULL, was_running = $5 \
             WHERE tenant_id = $1 AND workspace_id = $2 AND id = $3 AND pending_run_id = $4",
        )
        .bind(self.tenant())
        .bind(ws.as_str())
        .bind(id)
        .bind(token)
        .bind(i64::from(preserve_marker))
        .execute(&mut *tx)
        .await
        .map_err(|e| Error::Internal(format!("abandon script run failed: {e}")))?;
        tx.commit().await.map_err(db)
    }

    pub async fn settle_script_run(
        &self,
        ws: &WorkspaceId,
        id: &str,
        token: &str,
        result: &ScriptLastRun,
        recovery: bool,
    ) -> Result<bool> {
        let encoded = serde_json::to_string(result).map_err(|e| Error::Internal(e.to_string()))?;
        let mut tx = self.begin_write().await?;
        let settled = sqlx::query(
            "UPDATE script SET last_run = CASE WHEN mode = 'command' THEN $5 ELSE last_run END, \
             latest_run_result = $5, \
             archived_at = CASE WHEN purpose = 'oneOff' THEN coalesce(archived_at, $6) ELSE archived_at END, \
             pending_run_id = NULL, pending_started_at = NULL, \
             was_running = CASE WHEN $7 THEN CASE WHEN mode = 'command' THEN 1 ELSE was_running END ELSE 0 END \
             WHERE tenant_id = $1 AND workspace_id = $2 AND id = $3 AND pending_run_id = $4",
        )
        .bind(self.tenant())
        .bind(ws.as_str())
        .bind(id)
        .bind(token)
        .bind(&encoded)
        .bind(&result.stopped_at)
        .bind(recovery)
        .execute(&mut *tx)
        .await
        .map_err(|e| Error::Internal(format!("settle script run failed: {e}")))?;
        tx.commit().await.map_err(db)?;
        Ok(settled.rows_affected() != 0)
    }

    pub async fn pending_script_runs(
        &self,
    ) -> Result<Vec<(WorkspaceId, String, String, Option<String>)>> {
        let mut tx = self.begin().await?;
        let rows = sqlx::query(
            "SELECT workspace_id, id, pending_run_id, pending_started_at FROM script \
             WHERE tenant_id = $1 AND pending_run_id IS NOT NULL ORDER BY row_seq",
        )
        .bind(self.tenant())
        .fetch_all(&mut *tx)
        .await
        .map_err(|e| Error::Internal(format!("read pending script runs failed: {e}")))?;
        tx.commit().await.map_err(db)?;
        rows.iter()
            .map(|r| {
                Ok((
                    WorkspaceId::from(col::<String>(r, "workspace_id")?),
                    col(r, "id")?,
                    col(r, "pending_run_id")?,
                    col(r, "pending_started_at")?,
                ))
            })
            .collect()
    }

    pub async fn set_script_was_running(
        &self,
        workspace_id: &str,
        id: &str,
        was_running: bool,
    ) -> Result<()> {
        let mut tx = self.begin_write().await?;
        sqlx::query(
            "UPDATE script SET was_running = $4 WHERE tenant_id = $1 AND id = $2 AND workspace_id = $3",
        )
        .bind(self.tenant())
        .bind(id)
        .bind(workspace_id)
        .bind(i64::from(was_running))
        .execute(&mut *tx)
        .await
        .map_err(|e| Error::Internal(format!("set script was_running failed: {e}")))?;
        tx.commit().await.map_err(db)
    }

    pub async fn set_script_auto_start(
        &self,
        workspace_id: &str,
        id: &str,
        auto_start: Option<bool>,
        updated_at: &str,
    ) -> Result<bool> {
        let mut tx = self.begin_write().await?;
        let changed = sqlx::query(
            "UPDATE script SET auto_start = $4, updated_at = $5 \
             WHERE tenant_id = $1 AND id = $2 AND workspace_id = $3",
        )
        .bind(self.tenant())
        .bind(id)
        .bind(workspace_id)
        .bind(auto_start.map(i64::from))
        .bind(updated_at)
        .execute(&mut *tx)
        .await
        .map_err(|e| Error::Internal(format!("set script auto_start failed: {e}")))?
        .rows_affected()
            > 0;
        tx.commit().await.map_err(db)?;
        Ok(changed)
    }

    pub async fn list_was_running_script_ids(&self) -> Result<Vec<(String, String)>> {
        let mut tx = self.begin().await?;
        let rows: Vec<(String, String)> = sqlx::query_as(
            "SELECT workspace_id, id FROM script WHERE tenant_id = $1 AND was_running = 1 \
             ORDER BY row_seq",
        )
        .bind(self.tenant())
        .fetch_all(&mut *tx)
        .await
        .map_err(|e| Error::Internal(format!("list was-running scripts failed: {e}")))?;
        tx.commit().await.map_err(db)?;
        Ok(rows)
    }

    // ---- script_monitor_repo (script table) -------------------------------

    pub async fn latest_script_run(
        &self,
        ws: &WorkspaceId,
        id: &str,
    ) -> Result<Option<(String, Option<ScriptLastRun>)>> {
        let mut tx = self.begin().await?;
        let row: Option<(String, Option<String>, Option<String>)> = sqlx::query_as(
            "SELECT latest_run_id, pending_run_id, latest_run_result FROM script \
             WHERE tenant_id = $1 AND workspace_id = $2 AND id = $3 AND latest_run_id IS NOT NULL",
        )
        .bind(self.tenant())
        .bind(ws.as_str())
        .bind(id)
        .fetch_optional(&mut *tx)
        .await
        .map_err(list_db)?;
        tx.commit().await.map_err(db)?;
        row.map(|(token, pending, result)| {
            let last = if pending.is_none() {
                result
                    .map(|s| serde_json::from_str::<ScriptLastRun>(&s).map_err(list_db))
                    .transpose()?
                    .filter(|r| r.run_id.as_deref() == Some(token.as_str()))
            } else {
                None
            };
            Ok((token, last))
        })
        .transpose()
    }

    // ---- script_list_repo -----------------------------------------------

    pub async fn script_summary_page(
        &self,
        ws: &WorkspaceId,
        owner: Option<&AgentId>,
        monitor: bool,
        options: &ScriptListOptions,
    ) -> Result<Value> {
        self.script_summary_page_inner(ws, owner, monitor, options, false)
            .await
    }

    pub async fn saved_script_summary_page(
        &self,
        ws: &WorkspaceId,
        options: &ScriptListOptions,
    ) -> Result<Value> {
        self.script_summary_page_inner(ws, None, false, options, true)
            .await
    }

    async fn script_summary_page_inner(
        &self,
        ws: &WorkspaceId,
        owner: Option<&AgentId>,
        monitor: bool,
        options: &ScriptListOptions,
        saved_only: bool,
    ) -> Result<Value> {
        let method = if monitor {
            "scriptMonitor.list"
        } else {
            "script.list"
        };
        let scope = script_lists::scope(method, ws.as_str(), owner.map(AgentId::as_str));
        let boundary = options.boundary(&scope)?;
        let (time, id) = boundary.as_ref().map_or((None, None), |(time, id)| {
            (Some(time.as_str()), Some(id.as_str()))
        });
        let owner = owner.map(AgentId::as_str);
        let saved = if saved_only {
            " AND purpose = 'saved'"
        } else {
            ""
        };
        let keys_sql = if monitor {
            "SELECT created_at, id FROM script_monitor \
             WHERE tenant_id = $1 AND workspace_id = $2 AND state = 'active' \
             AND ($3::text IS NULL OR agent_id = $3) \
             AND ($4::text IS NULL OR (created_at COLLATE \"C\", id COLLATE \"C\") < ($4, $5)) \
             ORDER BY created_at COLLATE \"C\" DESC, id COLLATE \"C\" DESC LIMIT $6"
                .to_owned()
        } else {
            format!(
                "SELECT created_at, id FROM script \
                 WHERE tenant_id = $1 AND workspace_id = $2 AND archived_at IS NULL{saved} \
                 AND $3::text IS NULL \
                 AND ($4::text IS NULL OR (created_at COLLATE \"C\", id COLLATE \"C\") < ($4, $5)) \
                 ORDER BY created_at COLLATE \"C\" DESC, id COLLATE \"C\" DESC LIMIT $6"
            )
        };
        // The page's keys and rows come from one snapshot, so a row archived
        // in between cannot shorten the page.
        let mut tx = self.begin_read_snapshot().await?;
        let mut keys: Vec<(String, String)> = sqlx::query_as(sqlx::AssertSqlSafe(keys_sql))
            .bind(self.tenant())
            .bind(ws.as_str())
            .bind(owner)
            .bind(time)
            .bind(id)
            .bind(i64::try_from(options.limit + 1).map_err(list_db)?)
            .fetch_all(&mut *tx)
            .await
            .map_err(list_db)?;
        super::read_seam::probe::between(self.tenant(), "script_summary_page").await;
        let more = keys.len() > options.limit;
        keys.truncate(options.limit);
        let ids: Vec<&str> = keys.iter().map(|(_, id)| id.as_str()).collect();
        let rows = if monitor {
            sqlx::query(
                "SELECT id, workspace_id, agent_id, script_id, run_id, created_at, \
                 jsonb_build_object('scriptName', row_json::jsonb -> 'scriptName', \
                   'mode', row_json::jsonb -> 'mode', 'expiresAt', row_json::jsonb -> 'expiresAt', \
                   'lineCount', row_json::jsonb -> 'lineCount')::text AS extra, \
                 left(row_json::jsonb ->> 'outputPattern', 160) AS preview, \
                 octet_length(row_json::jsonb ->> 'outputPattern') AS preview_bytes \
                 FROM script_monitor WHERE tenant_id = $1 AND workspace_id = $2 \
                 AND state = 'active' AND ($3::text IS NULL OR agent_id = $3) AND id = ANY($4) \
                 ORDER BY created_at COLLATE \"C\" DESC, id COLLATE \"C\" DESC",
            )
            .bind(self.tenant())
            .bind(ws.as_str())
            .bind(owner)
            .bind(&ids)
            .fetch_all(&mut *tx)
            .await
            .map_err(list_db)?
        } else {
            sqlx::query(sqlx::AssertSqlSafe(format!(
                "SELECT id, workspace_id, name, mode, source, created_at, updated_at, category, \
                 auto_start, purpose, remote_author, \
                 left(command, 160) AS preview, octet_length(command) AS preview_bytes, \
                 CASE WHEN jsonb_typeof(last_run::jsonb) = 'object' \
                   THEN (last_run::jsonb - 'error')::text ELSE last_run END AS last_run, \
                 left(last_run::jsonb ->> 'error', 256) AS last_error, \
                 octet_length(last_run::jsonb ->> 'error') AS last_error_bytes \
                 FROM script WHERE tenant_id = $1 AND workspace_id = $2 \
                 AND archived_at IS NULL{saved} AND id = ANY($3) \
                 ORDER BY created_at COLLATE \"C\" DESC, id COLLATE \"C\" DESC"
            )))
            .bind(self.tenant())
            .bind(ws.as_str())
            .bind(&ids)
            .fetch_all(&mut *tx)
            .await
            .map_err(list_db)?
        };
        tx.commit().await.map_err(db)?;
        let rows: Vec<Value> = rows
            .iter()
            .map(|r| {
                if monitor {
                    monitor_summary(r)
                } else {
                    script_summary(r)
                }
            })
            .collect::<Result<_>>()?;
        let mut result = json!({"view":"summary"});
        result[if monitor { "monitors" } else { "scripts" }] = json!(rows);
        if more {
            if let Some((time, id)) = keys.last() {
                result["nextCursor"] = json!(script_lists::cursor(&scope, time, id));
            }
        }
        Ok(result)
    }
}

fn list_db(e: impl std::fmt::Display) -> Error {
    Error::Internal(format!("script list storage: {e}"))
}

fn put(map: &mut Map<String, Value>, key: &str, value: Value) {
    if !value.is_null() {
        map.insert(key.to_owned(), value);
    }
}

/// The bounded preview of `preview` and whether `bytes` exceeds it.
fn bounded(r: &PgRow, limit: usize) -> Result<Option<(String, bool)>> {
    let text: Option<String> = col(r, "preview")?;
    let bytes: Option<i32> = col(r, "preview_bytes")?;
    Ok(text.map(|text| {
        let (prefix, _) = script_lists::prefix(&text, limit);
        let truncated = exceeds(bytes, prefix.len());
        (prefix.to_owned(), truncated)
    }))
}

/// Whether a stored `octet_length` is longer than a `len`-byte prefix.
fn exceeds(bytes: Option<i32>, len: usize) -> bool {
    usize::try_from(bytes.unwrap_or_default()).unwrap_or_default() > len
}

fn parse_json(raw: Option<String>) -> Result<Value> {
    raw.map(|s| serde_json::from_str(&s).map_err(list_db))
        .transpose()
        .map(Option::unwrap_or_default)
}

/// One `script.list` summary row, shaped like the `SQLite` projection.
fn script_summary(r: &PgRow) -> Result<Value> {
    let mut row = Map::new();
    put(&mut row, "id", json!(col::<String>(r, "id")?));
    put(
        &mut row,
        "workspaceId",
        json!(col::<String>(r, "workspace_id")?),
    );
    put(&mut row, "name", json!(col::<String>(r, "name")?));
    put(&mut row, "mode", json!(col::<String>(r, "mode")?));
    put(&mut row, "source", json!(col::<String>(r, "source")?));
    put(
        &mut row,
        "createdAt",
        json!(col::<String>(r, "created_at")?),
    );
    put(
        &mut row,
        "updatedAt",
        json!(col::<Option<String>>(r, "updated_at")?),
    );
    put(
        &mut row,
        "category",
        json!(col::<Option<String>>(r, "category")?),
    );
    put(
        &mut row,
        "autoStart",
        json!(col::<Option<i64>>(r, "auto_start")?.map(|n| n != 0)),
    );
    put(&mut row, "purpose", json!(col::<String>(r, "purpose")?));
    let preview = bounded(r, 160)?;
    if let Some((text, _)) = &preview {
        put(&mut row, "commandPreview", json!(text));
    }
    // jsonb sorts `lastRun`'s keys (SQLite keeps the stored order); clients
    // read it as JSON, so only the raw text differs.
    let mut last_run = parse_json(col(r, "last_run")?)?;
    if let Some(error) = col::<Option<String>>(r, "last_error")? {
        let (prefix, _) = script_lists::prefix(&error, 256);
        let bytes: Option<i32> = col(r, "last_error_bytes")?;
        if exceeds(bytes, prefix.len()) {
            last_run["errorTruncated"] = json!(true);
        }
        last_run["error"] = json!(prefix);
    }
    put(&mut row, "lastRun", last_run);
    put(
        &mut row,
        "remoteAuthor",
        parse_json(col(r, "remote_author")?)?,
    );
    if let Some((_, truncated)) = preview {
        row.insert("commandTruncated".into(), json!(truncated));
    }
    Ok(Value::Object(row))
}

/// One `scriptMonitor.list` summary row, shaped like the `SQLite` projection.
fn monitor_summary(r: &PgRow) -> Result<Value> {
    let extra = parse_json(col(r, "extra")?)?;
    let mut row = Map::new();
    put(&mut row, "monitorId", json!(col::<String>(r, "id")?));
    put(
        &mut row,
        "workspaceId",
        json!(col::<String>(r, "workspace_id")?),
    );
    put(&mut row, "agentId", json!(col::<String>(r, "agent_id")?));
    put(&mut row, "scriptId", json!(col::<String>(r, "script_id")?));
    put(&mut row, "runId", json!(col::<String>(r, "run_id")?));
    put(&mut row, "scriptName", extra["scriptName"].clone());
    put(&mut row, "mode", extra["mode"].clone());
    put(&mut row, "state", json!("active"));
    put(
        &mut row,
        "createdAt",
        json!(col::<String>(r, "created_at")?),
    );
    put(&mut row, "expiresAt", extra["expiresAt"].clone());
    put(&mut row, "lineCount", extra["lineCount"].clone());
    let preview = bounded(r, 160)?;
    if let Some((text, truncated)) = preview {
        row.insert("outputPatternPreview".into(), json!(text));
        row.insert("outputPatternTruncated".into(), json!(truncated));
    }
    Ok(Value::Object(row))
}
