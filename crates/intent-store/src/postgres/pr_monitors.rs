//! PR monitors (Postgres). Mirrors `pr_monitor_repo.rs`, its prepared writes
//! and the legacy registration re-arm.
//!
//! - Text ordering uses `COLLATE "C"` to match `SQLite`'s byte order; `row_seq`
//!   stands in for the rowid where `SQLite` leaves ties in scan order.
//! - `SQLite` compares `repo_owner` / `repo_name` under `COLLATE NOCASE`; here
//!   the comparison is `lower()`, the expression the identity indexes use.
//!   `lower()` may also fold non-ASCII letters, which NOCASE does not; forge
//!   slugs are ASCII, so results differ only for slugs that name no real
//!   repository. An ASCII-only fold here would disagree with those indexes.
//! - The rate-limit pause fragments are the `SQLite` ones with `strpos` for
//!   `instr`, `left` for `substr(x, 1, n)` (Postgres rejects a negative
//!   length) and `sqlite_julianday` for `strftime('%s', …)`.

use std::collections::HashMap;

use intent_core::{
    AgentId, Error, PrMonitor, PrMonitorId, PrMonitorState, Result, ReviewTarget, WorkspaceId,
};
use serde_json::json;
use sqlx::postgres::{PgArguments, PgRow, Postgres};
use sqlx::Row;

use super::{PgStore, WriteTx};
use crate::agent_repo::SESSION_SUMMARY_COLUMNS;
use crate::pr_monitor_repo::{
    check_rearm_compatible, check_replacement_identity, kind_word, list_entry_from_row,
    monitor_from_row, pending_to_db, provider_word, qualified_from_row, state_to_db, target_number,
    validate_monitor_target, workspace_reads_from_rows, MonitorQualificationOutcome,
    MonitorTargetProvenance, PersistedMonitorTarget, PrMonitorListEntry, PrMonitorPollUpdate,
    PreparedMonitorWrite, QualifiedPrMonitor, TargetColumns, WorkspacePrMonitorReads, COLUMNS,
    LEGACY_LOOKUP, PAUSE_UNTIL_SEPARATOR, PR_MONITOR_PAUSE_MARKER, TARGET_COLUMNS,
};
use crate::subscription_agent_repo::{SubscriptionAgentProjection, IDS_PER_STATEMENT};

type PgQuery = sqlx::query::Query<'static, Postgres, PgArguments>;

fn internal(context: &str, e: impl std::fmt::Display) -> Error {
    Error::Internal(format!("{context}: {e}"))
}

fn unique_violation(e: &sqlx::Error) -> bool {
    e.as_database_error()
        .is_some_and(sqlx::error::DatabaseError::is_unique_violation)
}

fn not_found(id: &PrMonitorId) -> Error {
    Error::NotFound(format!("pr monitor {} not found", id.0))
}

/// `instant` (an RFC 3339 UTC expression, any fraction) as whole Unix
/// seconds, `NULL` when it does not parse; the fraction is cut textually.
fn instant_secs(instant: &str) -> String {
    format!(
        "CAST(round((sqlite_julianday(substr({instant}, 1, 19)) - 2440587.5) * 86400) AS bigint)"
    )
}

fn pause_deadline_secs(annotation: &str, marker: &str) -> String {
    let skip = PAUSE_UNTIL_SEPARATOR.len() + 1;
    instant_secs(&format!("substr({annotation}, length({marker}) + {skip})"))
}

fn row_pause(marker: &str) -> String {
    format!(
        "CASE WHEN strpos(COALESCE(last_error, ''), {marker}) > 0 \
             THEN substr(last_error, strpos(last_error, {marker})) ELSE '' END"
    )
}

/// `pause_preserving_last_error_sql` in `pr_monitor_repo.rs`.
fn pause_preserving_last_error(genuine: &str, marker: &str, floor: &str) -> String {
    let row_pause = row_pause(marker);
    let row_deadline = pause_deadline_secs(&row_pause, marker);
    let floor_secs = instant_secs(floor);
    let pause = format!("CASE WHEN {row_deadline} >= {floor_secs} THEN {row_pause} ELSE NULL END");
    let caller_genuine = format!(
        "NULLIF(CASE WHEN strpos(COALESCE({genuine}, ''), {marker}) > 0 \
                    THEN rtrim(left({genuine}, strpos({genuine}, {marker}) - 1), '; ') \
                    ELSE {genuine} END, '')"
    );
    format!(
        "CASE WHEN {pause} IS NULL THEN {caller_genuine} \
              WHEN {caller_genuine} IS NULL THEN {pause} \
              ELSE {caller_genuine} || '; ' || {pause} END"
    )
}

/// A JSON boolean (or number) at `path` of `doc` as a SQL boolean, the way
/// `SQLite`'s `json_extract` result decodes as `bool`.
fn json_bool(doc: &str, path: &str) -> String {
    format!(
        "CASE WHEN sqlite_json_type({doc}, '{path}') IN ('true', 'false', 'integer', 'real') \
         THEN sqlite_json_extract({doc}, '{path}')::numeric <> 0 END"
    )
}

/// The [`PrMonitorListEntry`] snapshot scalars of the valid-JSON `doc`.
fn snapshot_scalars(doc: &str) -> String {
    format!(
        "sqlite_json_extract({doc}, '$.url') AS snapshot_url, \
         sqlite_json_extract({doc}, '$.title') AS snapshot_title, \
         sqlite_json_extract({doc}, '$.headSha') AS snapshot_head_sha, \
         sqlite_json_extract({doc}, '$.requirements.state') AS snapshot_state, \
         {} AS snapshot_is_draft, {} AS snapshot_mergeable, {} AS snapshot_is_in_merge_queue, \
         sqlite_json_extract({doc}, '$.prUpdatedAt') AS snapshot_pr_updated_at",
        json_bool(doc, "$.requirements.isDraft"),
        json_bool(doc, "$.requirements.mergeable"),
        json_bool(doc, "$.requirements.isInMergeQueue"),
    )
}

/// `col IS NOT DISTINCT FROM $n` for every monitor and target column, from
/// `$first`; bind them with [`bind_captured`].
fn captured_columns(first: usize) -> String {
    COLUMNS
        .split(',')
        .chain(TARGET_COLUMNS.split(','))
        .map(str::trim)
        .enumerate()
        .map(|(i, column)| format!("{column} IS NOT DISTINCT FROM ${}", first + i))
        .collect::<Vec<_>>()
        .join(" AND ")
}

fn bind_captured(mut query: PgQuery, expected: &QualifiedPrMonitor) -> PgQuery {
    for (i, column) in COLUMNS.split(',').map(str::trim).enumerate() {
        query = if column == "pr_number" {
            query.bind(expected.monitor().pr_number)
        } else {
            query.bind(expected.original_text[i].clone())
        };
    }
    for value in expected.columns.values() {
        query = query.bind(value.map(str::to_owned));
    }
    query
}

/// `pr_monitor_repo::captured_monitor_exists`: the captured row, still active
/// and unchanged.
pub(crate) fn captured_monitor_exists(tenant: &str, expected: &QualifiedPrMonitor) -> PgQuery {
    let sql = format!(
        "SELECT monitor_id FROM pr_monitor WHERE tenant_id = $1 AND state = 'active' AND {}",
        captured_columns(2)
    );
    bind_captured(
        sqlx::query(sqlx::AssertSqlSafe(sql)).bind(tenant.to_owned()),
        expected,
    )
}

/// A prepared monitor write holding the tenant's writer lock until it
/// finishes, like `SQLite`'s reserved `BEGIN IMMEDIATE` writer.
pub(crate) struct PreparedPgWrite {
    tx: WriteTx,
    query: PgQuery,
    insert: bool,
}

impl PreparedPgWrite {
    pub(crate) async fn finish(self) -> Result<bool> {
        let Self {
            mut tx,
            query,
            insert,
        } = self;
        let changed = match query.execute(&mut *tx).await {
            Ok(result) => result.rows_affected() == 1,
            // The failed statement aborted the transaction; dropping it rolls back.
            Err(error) if insert && unique_violation(&error) => return Ok(false),
            Err(error) => return Err(internal("qualified monitor write", error)),
        };
        tx.commit()
            .await
            .map_err(|error| internal("qualified monitor commit", error))?;
        Ok(changed)
    }
}

impl PgStore {
    async fn fetch_monitors(&self, query: PgQuery, context: &str) -> Result<Vec<PgRow>> {
        let mut tx = self.begin().await?;
        let rows = query
            .fetch_all(&mut *tx)
            .await
            .map_err(|e| internal(context, e))?;
        tx.commit().await.map_err(|e| internal(context, e))?;
        Ok(rows)
    }

    async fn fetch_monitor(&self, query: PgQuery, context: &str) -> Result<Option<PgRow>> {
        let mut tx = self.begin().await?;
        let row = query
            .fetch_optional(&mut *tx)
            .await
            .map_err(|e| internal(context, e))?;
        tx.commit().await.map_err(|e| internal(context, e))?;
        Ok(row)
    }

    pub async fn cancel_qualified_pr_monitor(
        &self,
        expected: &QualifiedPrMonitor,
        now: &str,
    ) -> Result<bool> {
        let sql = format!(
            "UPDATE pr_monitor SET state = 'cancelled', updated_at = $2 \
             WHERE tenant_id = $1 AND state = 'active' AND {}",
            captured_columns(3)
        );
        let mut tx = self.begin_write().await?;
        let query = sqlx::query(sqlx::AssertSqlSafe(sql))
            .bind(self.tenant().to_owned())
            .bind(now.to_owned());
        let changed = bind_captured(query, expected)
            .execute(&mut *tx)
            .await
            .map_err(|e| internal("cancel captured monitor", e))?
            .rows_affected()
            == 1;
        tx.commit()
            .await
            .map_err(|e| internal("cancel captured monitor", e))?;
        Ok(changed)
    }

    pub async fn get_qualified_pr_monitor(&self, id: &PrMonitorId) -> Result<QualifiedPrMonitor> {
        let sql = format!(
            "SELECT {COLUMNS}, {TARGET_COLUMNS} FROM pr_monitor \
             WHERE tenant_id = $1 AND monitor_id = $2"
        );
        let query = sqlx::query(sqlx::AssertSqlSafe(sql))
            .bind(self.tenant().to_owned())
            .bind(id.0.clone());
        let row = self
            .fetch_monitor(query, "get qualified monitor")
            .await?
            .ok_or_else(|| not_found(id))?;
        qualified_from_row(&row)
    }

    pub async fn load_active_qualified_pr_monitors(&self) -> Result<Vec<QualifiedPrMonitor>> {
        let sql = format!(
            "SELECT {COLUMNS}, {TARGET_COLUMNS} FROM pr_monitor \
             WHERE tenant_id = $1 AND state = 'active' \
             ORDER BY created_at COLLATE \"C\", monitor_id COLLATE \"C\""
        );
        let query = sqlx::query(sqlx::AssertSqlSafe(sql)).bind(self.tenant().to_owned());
        self.fetch_monitors(query, "load qualified monitors")
            .await?
            .iter()
            .map(qualified_from_row)
            .collect()
    }

    pub async fn list_qualified_pr_monitors_by_agent(
        &self,
        agent: &AgentId,
    ) -> Result<Vec<QualifiedPrMonitor>> {
        self.list_qualified_monitors("agent_id", &agent.0).await
    }

    pub async fn list_qualified_pr_monitors_by_workspace(
        &self,
        workspace: &WorkspaceId,
    ) -> Result<Vec<QualifiedPrMonitor>> {
        self.list_qualified_monitors("workspace_id", &workspace.0)
            .await
    }

    async fn list_qualified_monitors(
        &self,
        scope: &str,
        id: &str,
    ) -> Result<Vec<QualifiedPrMonitor>> {
        let sql = format!(
            "SELECT {COLUMNS}, {TARGET_COLUMNS} FROM pr_monitor \
             WHERE tenant_id = $1 AND {scope} = $2 \
             ORDER BY created_at COLLATE \"C\", monitor_id COLLATE \"C\""
        );
        let query = sqlx::query(sqlx::AssertSqlSafe(sql))
            .bind(self.tenant().to_owned())
            .bind(id.to_owned());
        self.fetch_monitors(query, "list qualified monitors")
            .await?
            .iter()
            .map(qualified_from_row)
            .collect()
    }

    pub async fn find_active_qualified_pr_monitor(
        &self,
        agent: &AgentId,
        target: &ReviewTarget,
    ) -> Result<Option<QualifiedPrMonitor>> {
        self.find_qualified_monitor("agent_id", &agent.0, target)
            .await
    }

    pub async fn find_active_qualified_pr_monitor_in_workspace(
        &self,
        workspace: &WorkspaceId,
        target: &ReviewTarget,
    ) -> Result<Option<QualifiedPrMonitor>> {
        self.find_qualified_monitor("workspace_id", &workspace.0, target)
            .await
    }

    async fn find_qualified_monitor(
        &self,
        scope: &str,
        id: &str,
        target: &ReviewTarget,
    ) -> Result<Option<QualifiedPrMonitor>> {
        let number = target_number(target)?;
        let sql = format!(
            "SELECT {COLUMNS}, {TARGET_COLUMNS} FROM pr_monitor \
             WHERE tenant_id = $1 AND {scope} = $2 \
             AND state = 'active' AND target_provenance != 'unresolved' \
             AND target_provider = $3 AND target_instance_base_url = $4 AND target_kind = $5 \
             AND pr_number = $6 \
             AND CASE WHEN target_provider = 'github' THEN lower(target_project_path) \
                 ELSE target_project_path END \
                 = CASE WHEN $3 = 'github' THEN lower($7) ELSE $7 END"
        );
        let query = sqlx::query(sqlx::AssertSqlSafe(sql))
            .bind(self.tenant().to_owned())
            .bind(id.to_owned())
            .bind(provider_word(target.repository.provider))
            .bind(target.repository.instance_base_url.clone())
            .bind(kind_word(target.kind))
            .bind(number)
            .bind(target.repository.project_path.clone());
        let row = self.fetch_monitor(query, "find qualified monitor").await?;
        let found = row.as_ref().map(qualified_from_row).transpose()?;
        Ok(found.filter(|m| matches!(m.target(), PersistedMonitorTarget::Resolved { .. })))
    }

    pub async fn insert_qualified_pr_monitor(
        &self,
        m: &PrMonitor,
        target: &ReviewTarget,
        provenance: MonitorTargetProvenance,
    ) -> Result<bool> {
        validate_monitor_target(m, target, provenance)?;
        let sql = format!(
            "INSERT INTO pr_monitor (tenant_id, {COLUMNS}, {TARGET_COLUMNS}) \
             VALUES ($1, $2, $3, $4, $5, $6, $7, $8, $9, $10, $11, $12, $13, $14, $15, $16, $17, \
             $18, $19, $20, $21, $22, NULL)"
        );
        let mut tx = self.begin_write().await?;
        let query = bind_monitor(
            sqlx::query(sqlx::AssertSqlSafe(sql)).bind(self.tenant().to_owned()),
            m,
        )
        .bind(provider_word(target.repository.provider))
        .bind(target.repository.instance_base_url.clone())
        .bind(target.repository.project_path.clone())
        .bind(kind_word(target.kind))
        .bind(provenance.as_str());
        match query.execute(&mut *tx).await {
            Ok(_) => {}
            Err(e) if unique_violation(&e) => return Ok(false),
            Err(e) => return Err(internal("insert qualified monitor", e)),
        }
        tx.commit()
            .await
            .map_err(|e| internal("insert qualified monitor", e))?;
        Ok(true)
    }

    pub async fn qualify_legacy_pr_monitor_target(
        &self,
        expected: &QualifiedPrMonitor,
        target: &ReviewTarget,
        provenance: MonitorTargetProvenance,
    ) -> Result<MonitorQualificationOutcome> {
        validate_monitor_target(expected.monitor(), target, provenance)?;
        let desired = TargetColumns::resolved(target, provenance);
        let sql = format!(
            "UPDATE pr_monitor SET target_provider = $2, target_instance_base_url = $3, \
             target_project_path = $4, target_kind = $5, target_provenance = $6, \
             target_unresolved_reason = $7 \
             WHERE tenant_id = $1 AND target_provenance = 'unresolved' AND {}",
            captured_columns(8)
        );
        let mut tx = self.begin_write().await?;
        let mut query = sqlx::query(sqlx::AssertSqlSafe(sql)).bind(self.tenant().to_owned());
        for value in desired.values() {
            query = query.bind(value.map(str::to_owned));
        }
        match bind_captured(query, expected).execute(&mut *tx).await {
            Ok(r) if r.rows_affected() == 1 => {
                tx.commit()
                    .await
                    .map_err(|e| internal("qualify monitor target", e))?;
                return Ok(MonitorQualificationOutcome::Applied);
            }
            Ok(_) => {}
            Err(e) if unique_violation(&e) => return Ok(MonitorQualificationOutcome::Conflict),
            Err(e) => return Err(internal("qualify monitor target", e)),
        }
        let current = qualified_in(&mut tx, self.tenant(), &expected.monitor().monitor_id).await;
        tx.commit()
            .await
            .map_err(|e| internal("qualify monitor target", e))?;
        match current {
            Ok(current)
                if current.original_text == expected.original_text
                    && current.monitor().pr_number == expected.monitor().pr_number
                    && current.columns == desired =>
            {
                Ok(MonitorQualificationOutcome::AlreadyQualified)
            }
            Ok(_) | Err(Error::NotFound(_)) => Ok(MonitorQualificationOutcome::Conflict),
            Err(e) => Err(e),
        }
    }

    pub async fn insert_pr_monitor(&self, m: &PrMonitor) -> Result<bool> {
        let sql = format!(
            "INSERT INTO pr_monitor (tenant_id, {COLUMNS}) \
             VALUES ($1, $2, $3, $4, $5, $6, $7, $8, $9, $10, $11, $12, $13, $14, $15, $16, $17)"
        );
        let mut tx = self.begin_write().await?;
        let query = bind_monitor(
            sqlx::query(sqlx::AssertSqlSafe(sql)).bind(self.tenant().to_owned()),
            m,
        );
        match query.execute(&mut *tx).await {
            Ok(_) => {}
            Err(e) if unique_violation(&e) => return Ok(false),
            Err(e) => return Err(internal("insert pr monitor failed", e)),
        }
        tx.commit()
            .await
            .map_err(|e| internal("insert pr monitor failed", e))?;
        Ok(true)
    }

    pub async fn get_pr_monitor(&self, monitor_id: &PrMonitorId) -> Result<PrMonitor> {
        let sql =
            format!("SELECT {COLUMNS} FROM pr_monitor WHERE tenant_id = $1 AND monitor_id = $2");
        let query = sqlx::query(sqlx::AssertSqlSafe(sql))
            .bind(self.tenant().to_owned())
            .bind(monitor_id.0.clone());
        match self.fetch_monitor(query, "get pr monitor failed").await? {
            Some(row) => monitor_from_row(&row),
            None => Err(not_found(monitor_id)),
        }
    }

    pub async fn find_active_pr_monitor(
        &self,
        agent_id: &AgentId,
        repo_owner: &str,
        repo_name: &str,
        pr_number: i64,
    ) -> Result<Option<PrMonitor>> {
        self.find_legacy_monitor(
            "agent_id",
            &agent_id.0,
            repo_owner,
            repo_name,
            pr_number,
            "find pr monitor failed",
        )
        .await
    }

    pub async fn find_active_pr_monitor_in_workspace(
        &self,
        workspace_id: &WorkspaceId,
        repo_owner: &str,
        repo_name: &str,
        pr_number: i64,
    ) -> Result<Option<PrMonitor>> {
        self.find_legacy_monitor(
            "workspace_id",
            &workspace_id.0,
            repo_owner,
            repo_name,
            pr_number,
            "find pr monitor in workspace failed",
        )
        .await
    }

    async fn find_legacy_monitor(
        &self,
        scope: &str,
        id: &str,
        repo_owner: &str,
        repo_name: &str,
        pr_number: i64,
        context: &str,
    ) -> Result<Option<PrMonitor>> {
        let sql = format!(
            "SELECT {COLUMNS} FROM pr_monitor WHERE tenant_id = $1 AND {scope} = $2 \
             AND lower(repo_owner) = lower($3) AND lower(repo_name) = lower($4) \
             AND pr_number = $5 AND state = 'active' AND {LEGACY_LOOKUP}"
        );
        let query = sqlx::query(sqlx::AssertSqlSafe(sql))
            .bind(self.tenant().to_owned())
            .bind(id.to_owned())
            .bind(repo_owner.to_owned())
            .bind(repo_name.to_owned())
            .bind(pr_number);
        let row = self.fetch_monitor(query, context).await?;
        row.as_ref().map(monitor_from_row).transpose()
    }

    pub async fn list_pr_monitors_by_agent(&self, agent_id: &AgentId) -> Result<Vec<PrMonitor>> {
        self.list_monitors(
            "agent_id",
            &agent_id.0,
            "",
            "list pr monitors by agent failed",
        )
        .await
    }

    pub async fn list_pr_monitors_by_workspace(
        &self,
        workspace_id: &WorkspaceId,
    ) -> Result<Vec<PrMonitor>> {
        self.list_monitors(
            "workspace_id",
            &workspace_id.0,
            "",
            "list pr monitors by workspace failed",
        )
        .await
    }

    pub async fn list_active_pr_monitors_by_agent(
        &self,
        agent_id: &AgentId,
    ) -> Result<Vec<PrMonitor>> {
        self.list_monitors(
            "agent_id",
            &agent_id.0,
            " AND state = 'active'",
            "list active pr monitors by agent failed",
        )
        .await
    }

    pub async fn list_active_pr_monitors_by_workspace(
        &self,
        workspace_id: &WorkspaceId,
    ) -> Result<Vec<PrMonitor>> {
        self.list_monitors(
            "workspace_id",
            &workspace_id.0,
            " AND state = 'active'",
            "list active pr monitors by workspace failed",
        )
        .await
    }

    async fn list_monitors(
        &self,
        scope: &str,
        id: &str,
        filter: &str,
        context: &str,
    ) -> Result<Vec<PrMonitor>> {
        let sql = format!(
            "SELECT {COLUMNS} FROM pr_monitor WHERE tenant_id = $1 AND {scope} = $2{filter} \
             ORDER BY created_at COLLATE \"C\", row_seq"
        );
        let query = sqlx::query(sqlx::AssertSqlSafe(sql))
            .bind(self.tenant().to_owned())
            .bind(id.to_owned());
        self.fetch_monitors(query, context)
            .await?
            .iter()
            .map(monitor_from_row)
            .collect()
    }

    pub async fn list_display_status_pr_monitors_by_workspace(
        &self,
        workspace_id: &WorkspaceId,
    ) -> Result<Vec<PrMonitor>> {
        let sql = format!(
            "SELECT {COLUMNS} FROM ( \
               SELECT {COLUMNS}, row_seq, 0 AS part FROM pr_monitor \
               WHERE tenant_id = $1 AND workspace_id = $2 AND state = 'active' \
               UNION ALL \
               (SELECT {COLUMNS}, row_seq, 1 AS part FROM pr_monitor \
               WHERE tenant_id = $1 AND workspace_id = $2 AND state = 'completed' \
               ORDER BY updated_at COLLATE \"C\" DESC, row_seq LIMIT 1)) d \
             ORDER BY created_at COLLATE \"C\", part, row_seq"
        );
        let query = sqlx::query(sqlx::AssertSqlSafe(sql))
            .bind(self.tenant().to_owned())
            .bind(workspace_id.0.clone());
        self.fetch_monitors(query, "list display-status pr monitors by workspace failed")
            .await?
            .iter()
            .map(monitor_from_row)
            .collect()
    }

    pub async fn list_display_status_pr_monitors_by_workspaces(
        &self,
        workspace_ids: &[WorkspaceId],
    ) -> Result<Vec<PrMonitor>> {
        if workspace_ids.is_empty() {
            return Ok(Vec::new());
        }
        let ids: Vec<String> = workspace_ids.iter().map(|id| id.0.clone()).collect();
        let sql = format!(
            "SELECT {COLUMNS} FROM ( \
               SELECT {COLUMNS}, row_seq, ROW_NUMBER() OVER ( \
                 PARTITION BY workspace_id, state ORDER BY updated_at COLLATE \"C\" DESC, row_seq \
               ) AS state_rank FROM pr_monitor \
               WHERE tenant_id = $1 AND workspace_id = ANY($2) \
               AND state IN ('active', 'completed')) d \
             WHERE state = 'active' OR state_rank = 1 \
             ORDER BY workspace_id COLLATE \"C\", created_at COLLATE \"C\", row_seq"
        );
        let query = sqlx::query(sqlx::AssertSqlSafe(sql))
            .bind(self.tenant().to_owned())
            .bind(ids);
        self.fetch_monitors(query, "batch list display-status pr monitors failed")
            .await?
            .iter()
            .map(monitor_from_row)
            .collect()
    }

    pub async fn load_active_pr_monitors(&self) -> Result<Vec<PrMonitor>> {
        let sql = format!(
            "SELECT {COLUMNS} FROM pr_monitor WHERE tenant_id = $1 AND state = 'active' \
             ORDER BY created_at COLLATE \"C\", row_seq"
        );
        let query = sqlx::query(sqlx::AssertSqlSafe(sql)).bind(self.tenant().to_owned());
        self.fetch_monitors(query, "load active pr monitors failed")
            .await?
            .iter()
            .map(monitor_from_row)
            .collect()
    }

    pub async fn load_non_cancelled_pr_monitor_list_entries(
        &self,
        include_archived: bool,
    ) -> Result<Vec<PrMonitorListEntry>> {
        let sql = format!(
            "SELECT workspace_id, repo_owner, repo_name, pr_number, state, created_at, \
             updated_at, {} \
             FROM (SELECT workspace_id, repo_owner, repo_name, pr_number, state, created_at, \
             updated_at, row_seq, \
             CASE WHEN sqlite_json_valid(last_snapshot) THEN last_snapshot END AS snapshot \
             FROM pr_monitor WHERE tenant_id = $1 AND state != 'cancelled' \
             AND ($2 OR workspace_id IN ( \
               SELECT id FROM workspace WHERE tenant_id = $1 AND archived = 0))) e \
             ORDER BY created_at COLLATE \"C\", row_seq",
            snapshot_scalars("snapshot")
        );
        let query = sqlx::query(sqlx::AssertSqlSafe(sql))
            .bind(self.tenant().to_owned())
            .bind(include_archived);
        self.fetch_monitors(query, "load pr monitor list entries failed")
            .await?
            .iter()
            .map(list_entry_from_row)
            .collect()
    }

    pub async fn load_workspace_pr_monitor_reads(
        &self,
        workspace_id: &WorkspaceId,
    ) -> Result<WorkspacePrMonitorReads> {
        let sql = format!(
            "SELECT monitor_id, agent_id, workspace_id, repo_owner, repo_name, pr_number, \
             state, created_at, updated_at, {}, display_row, \
             CASE WHEN display_row THEN last_snapshot END AS display_snapshot \
             FROM (SELECT m.monitor_id, m.agent_id, m.workspace_id, m.repo_owner, m.repo_name, \
             m.pr_number, m.state, m.created_at, m.updated_at, m.last_snapshot, m.row_seq, \
             CASE WHEN sqlite_json_valid(m.last_snapshot) THEN m.last_snapshot END AS snapshot, \
             (m.state = 'active' OR r.state_rank = 1) AS display_row \
             FROM (SELECT monitor_id, ROW_NUMBER() OVER ( \
               PARTITION BY state ORDER BY updated_at COLLATE \"C\" DESC, row_seq) AS state_rank \
             FROM pr_monitor WHERE tenant_id = $1 AND workspace_id = $2 \
             AND state != 'cancelled') AS r \
             JOIN pr_monitor AS m ON m.tenant_id = $1 AND m.monitor_id = r.monitor_id) s \
             ORDER BY created_at COLLATE \"C\", row_seq",
            snapshot_scalars("snapshot")
        );
        let query = sqlx::query(sqlx::AssertSqlSafe(sql))
            .bind(self.tenant().to_owned())
            .bind(workspace_id.0.clone());
        let rows = self
            .fetch_monitors(query, "load pr monitor reads for workspace failed")
            .await?;
        workspace_reads_from_rows(&rows)
    }

    pub async fn update_pr_monitor_state(
        &self,
        monitor_id: &PrMonitorId,
        state: PrMonitorState,
        updated_at: &str,
    ) -> Result<bool> {
        let context = "update pr monitor state failed";
        let mut tx = self.begin_write().await?;
        let changed = sqlx::query(
            "UPDATE pr_monitor SET state = $2, updated_at = $3 \
             WHERE tenant_id = $1 AND monitor_id = $4 AND state = 'active'",
        )
        .bind(self.tenant())
        .bind(state_to_db(state))
        .bind(updated_at)
        .bind(&monitor_id.0)
        .execute(&mut *tx)
        .await
        .map_err(|e| internal(context, e))?
        .rows_affected();
        tx.commit().await.map_err(|e| internal(context, e))?;
        Ok(changed > 0)
    }

    pub async fn complete_pr_monitor(
        &self,
        monitor_id: &PrMonitorId,
        updated_at: &str,
        expected_updated_at: &str,
    ) -> Result<bool> {
        let context = "complete pr monitor failed";
        let mut tx = self.begin_write().await?;
        let changed = sqlx::query(
            "UPDATE pr_monitor SET state = 'completed', pending_changes = NULL, \
             pending_since = NULL, last_change_at = NULL, updated_at = $2 \
             WHERE tenant_id = $1 AND monitor_id = $3 AND state = 'active' AND updated_at = $4",
        )
        .bind(self.tenant())
        .bind(updated_at)
        .bind(&monitor_id.0)
        .bind(expected_updated_at)
        .execute(&mut *tx)
        .await
        .map_err(|e| internal(context, e))?
        .rows_affected();
        tx.commit().await.map_err(|e| internal(context, e))?;
        Ok(changed > 0)
    }

    pub async fn update_pr_monitor_poll(
        &self,
        monitor_id: &PrMonitorId,
        update: PrMonitorPollUpdate<'_>,
    ) -> Result<bool> {
        let context = "update pr monitor poll failed";
        let sql = format!(
            "UPDATE pr_monitor SET last_snapshot = $2, baseline_snapshot = $3, \
             pending_changes = $4, pending_since = $5, last_change_at = $6, last_polled_at = $7, \
             last_error = {}, updated_at = $9 \
             WHERE tenant_id = $1 AND monitor_id = $10 AND state = 'active' AND updated_at = $11",
            pause_preserving_last_error("$8", "$12", "$9")
        );
        let mut tx = self.begin_write().await?;
        let changed = sqlx::query(sqlx::AssertSqlSafe(sql))
            .bind(self.tenant())
            .bind(update.last_snapshot)
            .bind(update.baseline_snapshot)
            .bind(pending_to_db(update.pending_changes))
            .bind(update.pending_since)
            .bind(update.last_change_at)
            .bind(update.last_polled_at)
            .bind(update.last_error)
            .bind(update.updated_at)
            .bind(&monitor_id.0)
            .bind(update.expected_updated_at)
            .bind(PR_MONITOR_PAUSE_MARKER)
            .execute(&mut *tx)
            .await
            .map_err(|e| internal(context, e))?
            .rows_affected();
        tx.commit().await.map_err(|e| internal(context, e))?;
        Ok(changed > 0)
    }

    pub async fn annotate_active_pr_monitors_pause(&self, pause: &str) -> Result<u64> {
        let context = "annotate active pr monitors pause failed";
        let row_deadline = pause_deadline_secs(&row_pause("$3"), "$3");
        let new_deadline = pause_deadline_secs("$2", "$3");
        let sql = format!(
            "UPDATE pr_monitor SET last_error = CASE \
                 WHEN last_error IS NULL OR last_error = '' THEN $2 \
                 WHEN strpos(last_error, $3) = 1 THEN $2 \
                 WHEN strpos(last_error, $3) > 1 \
                     THEN left(last_error, strpos(last_error, $3) - 1) || $2 \
                 ELSE last_error || '; ' || $2 \
             END \
             WHERE tenant_id = $1 AND state = 'active' \
               AND target_provenance = 'legacy-github-writer' AND target_provider = 'github' \
               AND NOT COALESCE({row_deadline} > {new_deadline}, false)"
        );
        let mut tx = self.begin_write().await?;
        let changed = sqlx::query(sqlx::AssertSqlSafe(sql))
            .bind(self.tenant())
            .bind(pause)
            .bind(PR_MONITOR_PAUSE_MARKER)
            .execute(&mut *tx)
            .await
            .map_err(|e| internal(context, e))?
            .rows_affected();
        tx.commit().await.map_err(|e| internal(context, e))?;
        Ok(changed)
    }

    pub async fn clear_active_pr_monitors_pause(&self, lifted: Option<&str>) -> Result<u64> {
        let context = "clear active pr monitors pause failed";
        let row_deadline = pause_deadline_secs(&row_pause("$2"), "$2");
        let lifted_deadline = pause_deadline_secs("$3", "$2");
        let sql = format!(
            "UPDATE pr_monitor SET last_error = \
               NULLIF(rtrim(left(last_error, strpos(last_error, $2) - 1), '; '), '') \
             WHERE tenant_id = $1 AND state = 'active' \
               AND target_provenance = 'legacy-github-writer' AND target_provider = 'github' \
               AND strpos(COALESCE(last_error, ''), $2) > 0 \
               AND NOT COALESCE({row_deadline} > {lifted_deadline}, false)"
        );
        let mut tx = self.begin_write().await?;
        let changed = sqlx::query(sqlx::AssertSqlSafe(sql))
            .bind(self.tenant())
            .bind(PR_MONITOR_PAUSE_MARKER)
            .bind(lifted)
            .execute(&mut *tx)
            .await
            .map_err(|e| internal(context, e))?
            .rows_affected();
        tx.commit().await.map_err(|e| internal(context, e))?;
        Ok(changed)
    }

    pub async fn adopt_pr_monitor(
        &self,
        monitor_id: &PrMonitorId,
        from_agent_id: &AgentId,
        to_agent_id: &AgentId,
        update: PrMonitorPollUpdate<'_>,
    ) -> Result<bool> {
        let context = "adopt pr monitor failed";
        let sql = format!(
            "UPDATE pr_monitor SET agent_id = $2, last_snapshot = $3, baseline_snapshot = $4, \
             pending_changes = $5, pending_since = $6, last_change_at = $7, last_polled_at = $8, \
             last_error = {}, updated_at = $10 \
             WHERE tenant_id = $1 AND monitor_id = $11 AND agent_id = $12 AND state = 'active' \
             AND updated_at = $13",
            pause_preserving_last_error("$9", "$14", "$10")
        );
        let mut tx = self.begin_write().await?;
        let changed = sqlx::query(sqlx::AssertSqlSafe(sql))
            .bind(self.tenant())
            .bind(&to_agent_id.0)
            .bind(update.last_snapshot)
            .bind(update.baseline_snapshot)
            .bind(pending_to_db(update.pending_changes))
            .bind(update.pending_since)
            .bind(update.last_change_at)
            .bind(update.last_polled_at)
            .bind(update.last_error)
            .bind(update.updated_at)
            .bind(&monitor_id.0)
            .bind(&from_agent_id.0)
            .bind(update.expected_updated_at)
            .bind(PR_MONITOR_PAUSE_MARKER)
            .execute(&mut *tx)
            .await
            .map_err(|e| internal(context, e))?
            .rows_affected();
        tx.commit().await.map_err(|e| internal(context, e))?;
        Ok(changed > 0)
    }

    pub async fn prepare_monitor_insert(
        &self,
        m: &PrMonitor,
        target: &ReviewTarget,
    ) -> Result<PreparedMonitorWrite> {
        validate_monitor_target(m, target, MonitorTargetProvenance::CapturedRequest)?;
        let sql = format!(
            "INSERT INTO pr_monitor (tenant_id, {COLUMNS}, {TARGET_COLUMNS}) \
             VALUES ($1, $2, $3, $4, $5, $6, $7, $8, $9, $10, $11, $12, $13, $14, $15, $16, $17, \
             $18, $19, $20, $21, $22, NULL)"
        );
        let query = bind_monitor(
            sqlx::query(sqlx::AssertSqlSafe(sql)).bind(self.tenant().to_owned()),
            m,
        )
        .bind(provider_word(target.repository.provider))
        .bind(target.repository.instance_base_url.clone())
        .bind(target.repository.project_path.clone())
        .bind(kind_word(target.kind))
        .bind(MonitorTargetProvenance::CapturedRequest.as_str());
        let tx = self.begin_write().await?;
        Ok(PreparedMonitorWrite::postgres(PreparedPgWrite {
            tx,
            query,
            insert: true,
        }))
    }

    pub async fn prepare_monitor_replace(
        &self,
        expected: &QualifiedPrMonitor,
        replacement: &PrMonitor,
    ) -> Result<PreparedMonitorWrite> {
        check_replacement_identity(expected, replacement)?;
        let sql = format!(
            "UPDATE pr_monitor SET agent_id = $2, state = $3, last_snapshot = $4, \
             baseline_snapshot = $5, pending_changes = $6, pending_since = $7, \
             last_change_at = $8, last_polled_at = $9, last_error = $10, updated_at = $11 \
             WHERE tenant_id = $1 AND state = 'active' AND {}",
            captured_columns(12)
        );
        let query = sqlx::query(sqlx::AssertSqlSafe(sql))
            .bind(self.tenant().to_owned())
            .bind(replacement.agent_id.0.clone())
            .bind(state_to_db(replacement.state))
            .bind(replacement.last_snapshot.clone())
            .bind(replacement.baseline_snapshot.clone())
            .bind(pending_to_db(&replacement.pending_changes))
            .bind(replacement.pending_since.clone())
            .bind(replacement.last_change_at.clone())
            .bind(replacement.last_polled_at.clone())
            .bind(replacement.last_error.clone())
            .bind(replacement.updated_at.clone());
        let query = bind_captured(query, expected);
        let tx = self.begin_write().await?;
        Ok(PreparedMonitorWrite::postgres(PreparedPgWrite {
            tx,
            query,
            insert: false,
        }))
    }

    pub async fn prepare_monitor_cancel(
        &self,
        expected: &QualifiedPrMonitor,
        now: &str,
    ) -> Result<PreparedMonitorWrite> {
        let sql = format!(
            "UPDATE pr_monitor SET state = 'cancelled', updated_at = $2 \
             WHERE tenant_id = $1 AND state = 'active' AND {}",
            captured_columns(3)
        );
        let query = sqlx::query(sqlx::AssertSqlSafe(sql))
            .bind(self.tenant().to_owned())
            .bind(now.to_owned());
        let query = bind_captured(query, expected);
        let tx = self.begin_write().await?;
        Ok(PreparedMonitorWrite::postgres(PreparedPgWrite {
            tx,
            query,
            insert: false,
        }))
    }

    pub async fn rearm_legacy_pr_monitor(
        &self,
        expected: &PrMonitor,
        owner: &AgentId,
        target: &ReviewTarget,
        baseline: Option<&str>,
        now: &str,
        pause: Option<&str>,
    ) -> Result<bool> {
        validate_monitor_target(
            expected,
            target,
            MonitorTargetProvenance::LegacyGithubWriter,
        )?;
        let context = "rearm legacy pr monitor";
        let mut tx = self.begin_write().await?;
        let captured = qualified_in(&mut tx, self.tenant(), &expected.monitor_id).await?;
        check_rearm_compatible(&captured)?;
        let sql = format!(
            "UPDATE pr_monitor SET agent_id = $2, last_snapshot = $3, baseline_snapshot = $3, \
             pending_changes = '[]', pending_since = NULL, last_change_at = NULL, \
             last_polled_at = $4, \
             last_error = CASE WHEN target_provenance = 'unresolved' THEN $21 ELSE {} END, \
             updated_at = $5, target_provider = 'github', \
             target_instance_base_url = 'https://github.com', target_project_path = $7, \
             target_kind = 'pull-request', target_provenance = 'legacy-github-writer', \
             target_unresolved_reason = NULL \
             WHERE tenant_id = $1 AND monitor_id = $8 AND agent_id = $9 AND workspace_id = $10 \
             AND repo_owner = $11 AND repo_name = $12 AND pr_number = $13 \
             AND state = 'active' AND updated_at = $14 \
             AND target_provider IS NOT DISTINCT FROM $15 \
             AND target_instance_base_url IS NOT DISTINCT FROM $16 \
             AND target_project_path IS NOT DISTINCT FROM $17 \
             AND target_kind IS NOT DISTINCT FROM $18 \
             AND target_provenance IS NOT DISTINCT FROM $19 \
             AND target_unresolved_reason IS NOT DISTINCT FROM $20",
            pause_preserving_last_error("NULL::text", "$6", "$5"),
        );
        let mut query = sqlx::query(sqlx::AssertSqlSafe(sql))
            .bind(self.tenant().to_owned())
            .bind(owner.0.clone())
            .bind(baseline.map(str::to_owned))
            .bind(baseline.map(|_| now.to_owned()))
            .bind(now.to_owned())
            .bind(PR_MONITOR_PAUSE_MARKER)
            .bind(target.repository.project_path.clone())
            .bind(expected.monitor_id.0.clone())
            .bind(expected.agent_id.0.clone())
            .bind(expected.workspace_id.0.clone())
            .bind(expected.repo_owner.clone())
            .bind(expected.repo_name.clone())
            .bind(expected.pr_number)
            .bind(expected.updated_at.clone());
        for value in captured.columns.values() {
            query = query.bind(value.map(str::to_owned));
        }
        let changed = query
            .bind(pause.map(str::to_owned))
            .execute(&mut *tx)
            .await
            .map_err(|e| internal(context, e))?
            .rows_affected()
            == 1;
        tx.commit().await.map_err(|e| internal(context, e))?;
        Ok(changed)
    }
}

impl PgStore {
    /// `subscription_agent_repo.rs`: one batch of session, hook and PR monitor
    /// reads per chunk, in one transaction. Ties keep insertion order.
    pub async fn get_subscription_agent_projections(
        &self,
        ids: &[AgentId],
    ) -> Result<Vec<SubscriptionAgentProjection>> {
        let mut out = Vec::with_capacity(ids.len());
        let session_sql = format!(
            "SELECT {SESSION_SUMMARY_COLUMNS}, message_count, last_assistant_preview, \
             last_user_preview, last_message_role, last_message_id, last_tool_use_preview \
             FROM agent_session WHERE tenant_id = $1 AND id = ANY($2)"
        );
        let monitors_sql = format!(
            "SELECT agent_id, monitor_id, repo_owner, repo_name, pr_number, {TARGET_COLUMNS}, \
             CASE WHEN sqlite_json_valid(last_snapshot) THEN \
             CASE WHEN sqlite_json_type(last_snapshot, '$.title') = 'text' \
             THEN sqlite_json_extract(last_snapshot, '$.title') END END AS title \
             FROM pr_monitor WHERE tenant_id = $1 AND agent_id = ANY($2) AND state = 'active' \
             ORDER BY created_at COLLATE \"C\", row_seq"
        );
        // Sessions, hooks and monitors of every chunk come from one snapshot.
        let mut tx = self.begin_read_snapshot().await?;
        for chunk in ids.chunks(IDS_PER_STATEMENT) {
            let chunk: Vec<&str> = chunk.iter().map(|id| id.0.as_str()).collect();
            let rows = sqlx::query(sqlx::AssertSqlSafe(session_sql.as_str()))
                .bind(self.tenant())
                .bind(&chunk)
                .fetch_all(&mut *tx)
                .await
                .map_err(|e| internal("subscription agent projection failed", e))?;
            super::read_seam::probe::between(self.tenant(), "get_subscription_agent_projections")
                .await;
            let mut batch = HashMap::with_capacity(rows.len());
            for row in rows {
                let session = match super::agents::map_summary(&row) {
                    Ok(session) => session,
                    Err(e) => {
                        tracing::warn!(agent = %row.get::<String, _>("id"), error = %e,
                            "decode subscription agent failed; omitting row");
                        continue;
                    }
                };
                let messages = super::agent_messages::projection_from_row(&row)?;
                batch.insert(
                    session.id.0.clone(),
                    SubscriptionAgentProjection {
                        session,
                        messages,
                        waiting_on_hooks: Vec::new(),
                        waiting_on_pr_monitors: Vec::new(),
                    },
                );
            }
            for row in sqlx::query(
                "SELECT agent_id, hook_id, name, next_run_at, expires_at FROM hook \
                 WHERE tenant_id = $1 AND agent_id = ANY($2) AND state IN ('scheduled', 'running') \
                 ORDER BY created_at COLLATE \"C\", row_seq",
            )
            .bind(self.tenant())
            .bind(&chunk)
            .fetch_all(&mut *tx)
            .await
            .map_err(|e| internal("subscription hook projection failed", e))?
            {
                if let Some(agent) = batch.get_mut(&row.get::<String, _>("agent_id")) {
                    let mut value = json!({
                        "hookId": row.get::<String, _>("hook_id"),
                        "name": row.get::<String, _>("name"),
                    });
                    for (column, field) in
                        [("next_run_at", "nextRunAt"), ("expires_at", "expiresAt")]
                    {
                        if let Some(text) = row.get::<Option<String>, _>(column) {
                            value[field] = json!(text);
                        }
                    }
                    agent.waiting_on_hooks.push(value);
                }
            }
            for row in sqlx::query(sqlx::AssertSqlSafe(monitors_sql.as_str()))
                .bind(self.tenant())
                .bind(&chunk)
                .fetch_all(&mut *tx)
                .await
                .map_err(|e| internal("subscription PR monitor projection failed", e))?
            {
                if !crate::pr_monitor_repo::legacy_monitor_summary(&row)? {
                    continue;
                }
                if let Some(agent) = batch.get_mut(&row.get::<String, _>("agent_id")) {
                    let mut value = json!({
                        "monitorId": row.get::<String, _>("monitor_id"),
                        "repo": format!(
                            "{}/{}",
                            row.get::<String, _>("repo_owner"),
                            row.get::<String, _>("repo_name")
                        ),
                        "prNumber": row.get::<i64, _>("pr_number"),
                    });
                    if let Some(title) = row.get::<Option<String>, _>("title") {
                        value["title"] = json!(title);
                    }
                    agent.waiting_on_pr_monitors.push(value);
                }
            }
            out.extend(batch.into_values());
        }
        tx.commit()
            .await
            .map_err(|e| internal("subscription agent projection failed", e))?;
        Ok(out)
    }
}

/// Bind the 16 monitor columns in `COLUMNS` order.
fn bind_monitor(query: PgQuery, m: &PrMonitor) -> PgQuery {
    query
        .bind(m.monitor_id.0.clone())
        .bind(m.workspace_id.0.clone())
        .bind(m.agent_id.0.clone())
        .bind(m.repo_owner.clone())
        .bind(m.repo_name.clone())
        .bind(m.pr_number)
        .bind(state_to_db(m.state))
        .bind(m.last_snapshot.clone())
        .bind(m.baseline_snapshot.clone())
        .bind(pending_to_db(&m.pending_changes))
        .bind(m.pending_since.clone())
        .bind(m.last_change_at.clone())
        .bind(m.last_polled_at.clone())
        .bind(m.last_error.clone())
        .bind(m.created_at.clone())
        .bind(m.updated_at.clone())
}

/// [`PgStore::get_qualified_pr_monitor`] inside a writer's transaction.
async fn qualified_in(
    tx: &mut WriteTx,
    tenant: &str,
    id: &PrMonitorId,
) -> Result<QualifiedPrMonitor> {
    let sql = format!(
        "SELECT {COLUMNS}, {TARGET_COLUMNS} FROM pr_monitor \
         WHERE tenant_id = $1 AND monitor_id = $2"
    );
    let row = sqlx::query(sqlx::AssertSqlSafe(sql))
        .bind(tenant)
        .bind(&id.0)
        .fetch_optional(&mut **tx)
        .await
        .map_err(|e| internal("get qualified monitor", e))?
        .ok_or_else(|| not_found(id))?;
    qualified_from_row(&row)
}
