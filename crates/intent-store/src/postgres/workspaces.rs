//! Workspaces, their git roots, homes, context items, UI context and MCP
//! disables (Postgres). Mirrors `workspace_repo.rs`, `workspace_git_root_repo.rs`,
//! `workspace_home_repo.rs`, `workspace_context_repo.rs`,
//! `workspace_ui_context_repo.rs` and `workspace_mcp_repo.rs`.
//!
//! Workspace inserts and deletes take the repository lifecycle bracket as on
//! `SQLite`; the other writers here predate the Postgres lifecycle domain and
//! leave it out.
//!
//! Text ordering uses `COLLATE "C"` to match `SQLite`'s byte order.

use std::collections::{HashMap, HashSet};
use std::sync::atomic::{AtomicBool, Ordering};
use std::sync::Arc;

use intent_core::{
    now_iso, AgentId, CheckoutMode, ClientId, ContextItem, Error, HomeId, HomeState,
    PullRequestInfo, PullRequestStatus, PullRequestWorkspace, Result, TokenUsage, Workspace,
    WorkspaceAttention, WorkspaceGitRoot, WorkspaceGitRootId, WorkspaceGitRootSource,
    WorkspaceHome, WorkspaceId, WorkspaceListFilter, WorkspaceStatus, CHIEF_WORKSPACE_ID,
};
use sqlx::postgres::{PgConnection, PgRow};
use sqlx::Row;

use super::{db, PgStore};
use crate::workspace_git_root_repo as roots;
use crate::workspace_repo::{
    self as ws_repo, PullRequestLink, RepositoryWorkspaceDeleteDisposition,
    RepositoryWorkspaceDeleteOutcome, WorkspaceContentClock,
};
use crate::{enum_from_db, enum_to_db, tags_from_db, tags_to_db, AgentUsageRow};

/// The `workspace_id` children `SQLite` deletes explicitly after notes'
/// comments, in its order; the others cascade from the workspace row.
const WORKSPACE_CHILD_TABLES: [&str; 14] = [
    "note_version",
    "note_line_attribution",
    "note",
    "tracked_changes",
    "diffs",
    "delegation_group",
    "task_agent_link",
    "agent_metrics",
    "workspace_context_item",
    "workspace_git_root",
    "pull_request_workspace",
    "workspace_invite",
    "workspace_mcp_disabled_server",
    "draft",
];

const HOME_COLUMNS: &str = "workspace_id, home_id, node_id, cloud_machine_id, name, state, \
    since, last_backup_at, backup_failing_since";

fn col<'r, T>(row: &'r PgRow, name: &str) -> Result<T>
where
    T: sqlx::Decode<'r, sqlx::Postgres> + sqlx::Type<sqlx::Postgres>,
{
    row.try_get::<T, _>(name)
        .map_err(|e| Error::Internal(format!("column {name}: {e}")))
}

/// Settle the brackets of sessions a workspace delete removed that were
/// created after it began.
async fn settle_all(
    tenant: &str,
    brackets: Vec<crate::repository_lifecycle::LifecycleWrite>,
) -> Result<()> {
    for bracket in brackets {
        super::purge::probe::late_settlement(tenant)?;
        bracket.settle_after_retirement().await?;
    }
    Ok(())
}

/// A workspace's agent ids, in creation order.
async fn workspace_agents(
    tx: &mut super::WriteTx,
    tenant: &str,
    id: &WorkspaceId,
) -> Result<Vec<AgentId>> {
    let agents: Vec<String> = sqlx::query_scalar(
        "SELECT id FROM agent_session WHERE tenant_id = $1 AND workspace_id = $2 \
         ORDER BY row_seq",
    )
    .bind(tenant)
    .bind(&id.0)
    .fetch_all(&mut **tx)
    .await
    .map_err(db)?;
    Ok(agents.into_iter().map(AgentId).collect())
}

fn not_found(id: &WorkspaceId) -> Error {
    Error::NotFound(format!("workspace {id}"))
}

fn root_not_found(id: &WorkspaceGitRootId) -> Error {
    Error::NotFound(format!("workspace git root {id}"))
}

fn unique_ids(ids: &[WorkspaceId]) -> Vec<String> {
    ids.iter()
        .map(|id| id.0.as_str())
        .collect::<HashSet<_>>()
        .into_iter()
        .map(str::to_owned)
        .collect()
}

fn map_workspace_row(row: &PgRow) -> Result<Workspace> {
    let pr_number: Option<i64> = col(row, "pr_number")?;
    let pr_status = col::<Option<String>>(row, "pr_status")?
        .map(|s| enum_from_db::<PullRequestStatus>(&s))
        .transpose()?;
    let checkout_mode = col::<Option<String>>(row, "checkout_mode")?
        .map(|s| enum_from_db::<CheckoutMode>(&s))
        .transpose()?;
    Ok(Workspace {
        id: WorkspaceId(col(row, "id")?),
        title: col(row, "title")?,
        branch: col(row, "branch")?,
        base_ref: col(row, "base_ref")?,
        base_commit_sha: col(row, "base_commit_sha")?,
        status: enum_from_db::<WorkspaceStatus>(&col::<String>(row, "status")?)?,
        status_message: col(row, "status_message")?,
        status_image_asset_id: col(row, "status_image_asset_id")?,
        attention: enum_from_db::<WorkspaceAttention>(&col::<String>(row, "attention")?)?,
        created_at: col(row, "created_at")?,
        updated_at: col(row, "updated_at")?,
        last_activity: col(row, "last_activity")?,
        last_content_activity: col(row, "last_content_activity")?,
        tags: tags_from_db(&col::<String>(row, "tags")?)?,
        path: col(row, "path")?,
        repository_path: col(row, "repository_path")?,
        repository_owner: col(row, "repository_owner")?,
        repository_name: col(row, "repository_name")?,
        worktree_path: col(row, "worktree_path")?,
        scope: col(row, "scope")?,
        skip_worktree: col::<i64>(row, "skip_worktree")? != 0,
        setup_script: ws_repo::setup_script_from_db(col(row, "setup_script")?)?,
        is_remote: col::<i64>(row, "is_remote")? != 0,
        default_model: col(row, "default_model")?,
        pr_number: pr_number.map(i64::cast_unsigned),
        pr_url: col(row, "pr_url")?,
        pr_status,
        active_pull_request: ws_repo::active_pr_from_db(col(row, "active_pull_request")?)?,
        pull_requests: ws_repo::pull_requests_from_db(col(row, "pull_requests")?)?,
        context_links: ws_repo::context_links_from_db(col(row, "context_links")?)?,
        archived: col::<i64>(row, "archived")? != 0,
        archived_at: col(row, "archived_at")?,
        retired_at: col(row, "retired_at")?,
        token_usage: ws_repo::token_usage_from_db(col(row, "token_usage")?)?,
        checkout_mode,
        browser_client_id: col::<Option<String>>(row, "browser_client_id")?.map(ClientId),
        label: col(row, "label")?,
        auto_archive_disabled: col::<i64>(row, "auto_archive_disabled")? != 0,
        plain_folder: col::<i64>(row, "plain_folder")? != 0,
        ..Default::default()
    })
}

fn map_root_row(row: &PgRow) -> Result<WorkspaceGitRoot> {
    let pr_status = col::<Option<String>>(row, "pr_status")?
        .map(|s| enum_from_db::<PullRequestStatus>(&s))
        .transpose()?;
    Ok(WorkspaceGitRoot {
        id: WorkspaceGitRootId(col(row, "id")?),
        workspace_id: WorkspaceId(col(row, "workspace_id")?),
        path: col(row, "path")?,
        source: enum_from_db::<WorkspaceGitRootSource>(&col::<String>(row, "source")?)?,
        repo_owner: col(row, "repo_owner")?,
        repo_name: col(row, "repo_name")?,
        registered_by_agent_ids: roots::agent_ids_from_db(&col::<String>(
            row,
            "registered_by_agent_ids",
        )?)?,
        registered_commit_sha: col(row, "registered_commit_sha")?,
        pr_number: col::<Option<i64>>(row, "pr_number")?.map(i64::cast_unsigned),
        pr_url: col(row, "pr_url")?,
        pr_status,
        pull_requests: roots::pull_requests_from_db(col(row, "pull_requests")?)?,
        created_at: col(row, "created_at")?,
        updated_at: col(row, "updated_at")?,
    })
}

fn map_home_row(row: &PgRow, now: time::OffsetDateTime) -> Result<(WorkspaceId, WorkspaceHome)> {
    let failing: Option<String> = col(row, "backup_failing_since")?;
    Ok((
        WorkspaceId(col(row, "workspace_id")?),
        WorkspaceHome {
            home_id: HomeId(col(row, "home_id")?),
            node_id: col(row, "node_id")?,
            cloud_machine_id: col(row, "cloud_machine_id")?,
            name: col(row, "name")?,
            state: crate::workspace_home_repo::state_from_db(&col::<String>(row, "state")?)?,
            since: col(row, "since")?,
            last_backup_at: col(row, "last_backup_at")?,
            backup_warning: crate::workspace_home_repo::warning(failing.as_deref(), now),
            setup: None,
        },
    ))
}

impl PgStore {
    async fn workspace_exists(&self, conn: &mut PgConnection, id: &WorkspaceId) -> Result<bool> {
        sqlx::query_scalar(
            "SELECT EXISTS(SELECT 1 FROM workspace WHERE tenant_id = $1 AND id = $2)",
        )
        .bind(self.tenant())
        .bind(&id.0)
        .fetch_one(conn)
        .await
        .map_err(db)
    }

    async fn workspace_rows(&self, sql: &str, binds: &[&str]) -> Result<Vec<Workspace>> {
        let mut tx = self.begin().await?;
        let mut query = sqlx::query(sqlx::AssertSqlSafe(sql)).bind(self.tenant());
        for value in binds {
            query = query.bind(*value);
        }
        let rows = query.fetch_all(&mut *tx).await.map_err(db)?;
        tx.commit().await.map_err(db)?;
        rows.iter().map(map_workspace_row).collect()
    }

    // ---- workspace_repo -------------------------------------------------

    pub async fn workspace_content_clocks(
        &self,
        workspace_ids: &[WorkspaceId],
    ) -> Result<HashMap<WorkspaceId, WorkspaceContentClock>> {
        let mut tx = self.begin().await?;
        let rows = sqlx::query(
            "SELECT id, created_at, last_content_activity FROM workspace \
             WHERE tenant_id = $1 AND id = ANY($2)",
        )
        .bind(self.tenant())
        .bind(unique_ids(workspace_ids))
        .fetch_all(&mut *tx)
        .await
        .map_err(db)?;
        tx.commit().await.map_err(db)?;
        rows.iter()
            .map(|row| {
                Ok((
                    WorkspaceId(col(row, "id")?),
                    WorkspaceContentClock {
                        created_at: col(row, "created_at")?,
                        last_content_activity: col(row, "last_content_activity")?,
                    },
                ))
            })
            .collect()
    }

    pub async fn workspaces_with_settled_pull_requests(
        &self,
        workspace_ids: &[WorkspaceId],
    ) -> Result<HashSet<WorkspaceId>> {
        let mut tx = self.begin().await?;
        let rows: Vec<String> = sqlx::query_scalar(
            "SELECT s.workspace_id FROM ( \
               SELECT w.id AS workspace_id, w.pr_status AS status FROM workspace w \
                 WHERE w.tenant_id = $1 AND w.id = ANY($2) \
                 AND (w.pr_status IS NOT NULL OR w.pr_url IS NOT NULL) \
               UNION ALL \
               SELECT r.workspace_id, r.pr_status FROM workspace_git_root r \
                 WHERE r.tenant_id = $1 AND r.workspace_id = ANY($2) \
                 AND (r.pr_status IS NOT NULL OR r.pr_url IS NOT NULL) \
               UNION ALL \
               SELECT m.workspace_id, NULL FROM pr_monitor m \
                 WHERE m.tenant_id = $1 AND m.workspace_id = ANY($2) AND m.state = 'active' \
               UNION ALL \
               SELECT w.id, CASE WHEN sqlite_json_valid(w.active_pull_request) \
                 THEN sqlite_json_extract(w.active_pull_request, '$.status') END \
                 FROM workspace w WHERE w.tenant_id = $1 AND w.id = ANY($2) \
                 AND w.active_pull_request IS NOT NULL \
               UNION ALL \
               SELECT p.workspace_id, CASE WHEN j.type = 'object' \
                 THEN sqlite_json_extract(j.value, '$.status') END \
                 FROM (SELECT w.id AS workspace_id, w.pull_requests AS pool FROM workspace w \
                         WHERE w.tenant_id = $1 AND w.id = ANY($2) AND w.pull_requests IS NOT NULL \
                       UNION ALL \
                       SELECT r.workspace_id, r.pull_requests FROM workspace_git_root r \
                         WHERE r.tenant_id = $1 AND r.workspace_id = ANY($2) \
                         AND r.pull_requests IS NOT NULL) p, \
                   sqlite_json_each(CASE WHEN sqlite_json_valid(p.pool) \
                     AND sqlite_json_type(p.pool) = 'array' THEN p.pool ELSE '[null]' END) j \
             ) s GROUP BY s.workspace_id \
             HAVING bool_and(COALESCE(s.status IN ('Merged', 'Closed'), false))",
        )
        .bind(self.tenant())
        .bind(unique_ids(workspace_ids))
        .fetch_all(&mut *tx)
        .await
        .map_err(db)?;
        tx.commit().await.map_err(db)?;
        Ok(rows.into_iter().map(WorkspaceId).collect())
    }

    pub async fn insert_workspace(&self, ws: &Workspace) -> Result<()> {
        self.insert_workspace_with_auto_commit(ws, None).await
    }

    pub async fn insert_workspace_with_auto_commit(
        &self,
        ws: &Workspace,
        auto_commit: Option<bool>,
    ) -> Result<()> {
        type NoAdmission<'a> = (
            &'a crate::RepositoryHostAuthoritySnapshot,
            Option<&'a str>,
            fn() -> Result<()>,
        );
        self.insert_workspace_inner(ws, auto_commit, None::<NoAdmission<'_>>)
            .await
    }

    pub async fn insert_workspace_with_host_admission(
        &self,
        ws: &Workspace,
        auto_commit: Option<bool>,
        expected: &crate::RepositoryHostAuthoritySnapshot,
        token_hash: Option<&str>,
        admit: impl FnOnce() -> Result<()> + Send,
    ) -> Result<()> {
        self.insert_workspace_inner(ws, auto_commit, Some((expected, token_hash, admit)))
            .await
    }

    /// As on `SQLite`: the original host authority is compared and `admit`
    /// runs once the tenant writer lock is held, just before the insert, so
    /// a credential revocation or host-member removal (which take that
    /// lock) cannot commit in between.
    async fn insert_workspace_inner(
        &self,
        ws: &Workspace,
        auto_commit: Option<bool>,
        admission: Option<(
            &crate::RepositoryHostAuthoritySnapshot,
            Option<&str>,
            impl FnOnce() -> Result<()> + Send,
        )>,
    ) -> Result<()> {
        let mut lifecycle = self.repository_lifecycle_write().await?;
        let columns = ws_repo::WORKSPACE_COLUMNS;
        let sql = format!(
            "INSERT INTO workspace (tenant_id, {columns}, auto_commit_enabled) VALUES \
             ($1, $2, $3, $4, $5, $6, $7, $8, $9, $10, $11, $12, $13, $14, $15, $16, $17, \
             $18, $19, $20, $21, $22, $23, $24, $25, $26, $27, $28, $29, $30, $31, $32, $33, \
             $34, $35, $36, $37, $38, $39, $40, $41)"
        );
        lifecycle.begin(&[crate::RepositoryLifecycleKey::Workspace(ws.id.clone())])?;
        let mut tx = self.begin_write().await?;
        if let Some((expected, token_hash, admit)) = admission {
            let actual = self
                .read_host_authority_snapshot(&mut tx, &expected.principal_id, token_hash)
                .await?;
            if actual != *expected {
                return Err(Error::Forbidden("Repository checkout unavailable".into()));
            }
            admit()?;
        }
        sqlx::query(sqlx::AssertSqlSafe(sql.as_str()))
            .bind(self.tenant())
            .bind(&ws.id.0)
            .bind(&ws.title)
            .bind(&ws.branch)
            .bind(&ws.base_ref)
            .bind(&ws.base_commit_sha)
            .bind(enum_to_db(&ws.status)?)
            .bind(&ws.status_message)
            .bind(&ws.status_image_asset_id)
            .bind(enum_to_db(&ws.attention)?)
            .bind(&ws.path)
            .bind(&ws.repository_path)
            .bind(&ws.repository_owner)
            .bind(&ws.repository_name)
            .bind(&ws.worktree_path)
            .bind(&ws.scope)
            .bind(i64::from(ws.skip_worktree))
            .bind(i64::from(ws.is_remote))
            .bind(&ws.default_model)
            .bind(ws.pr_number.map(u64::cast_signed))
            .bind(&ws.pr_url)
            .bind(ws_repo::pr_status_to_db(ws)?)
            .bind(ws_repo::active_pr_to_db(ws)?)
            .bind(ws_repo::pull_requests_to_db(ws)?)
            .bind(ws_repo::context_links_to_db(ws)?)
            .bind(i64::from(ws.archived))
            .bind(&ws.archived_at)
            .bind(tags_to_db(&ws.tags)?)
            .bind(&ws.created_at)
            .bind(&ws.updated_at)
            .bind(&ws.last_activity)
            .bind(ws_repo::token_usage_to_db(ws)?)
            .bind(ws_repo::setup_script_to_db(ws)?)
            .bind(ws_repo::checkout_mode_to_db(ws)?)
            .bind(ws.browser_client_id.as_ref().map(|c| c.0.clone()))
            .bind(&ws.last_content_activity)
            .bind(&ws.retired_at)
            .bind(&ws.label)
            .bind(i64::from(ws.auto_archive_disabled))
            .bind(i64::from(ws.plain_folder))
            .bind(auto_commit.map(i64::from))
            .execute(&mut *tx)
            .await
            .map_err(|e| Error::Internal(format!("workspace insert failed: {e}")))?;
        tx.commit().await.map_err(db)?;
        lifecycle.settle_after_retirement().await
    }

    pub async fn get_workspace(&self, id: &WorkspaceId) -> Result<Workspace> {
        let columns = ws_repo::WORKSPACE_COLUMNS;
        let sql = format!("SELECT {columns} FROM workspace WHERE tenant_id = $1 AND id = $2");
        self.workspace_rows(&sql, &[id.0.as_str()])
            .await?
            .pop()
            .ok_or_else(|| not_found(id))
    }

    pub async fn update_workspace(&self, ws: &Workspace) -> Result<String> {
        self.update_workspace_with_branch(ws, None).await
    }

    pub async fn update_workspace_with_branch(
        &self,
        ws: &Workspace,
        branch: Option<&str>,
    ) -> Result<String> {
        let mut tx = self.begin_write().await?;
        let stored: Option<String> = sqlx::query_scalar(
            "UPDATE workspace SET title = $3, branch = COALESCE($4, branch), base_ref = $5, \
             base_commit_sha = $6, \
             status = CASE WHEN archived = 1 OR $7 = $8 THEN status ELSE $7 END, \
             status_message = $9, status_image_asset_id = $10, attention = $11, path = $12, \
             repository_path = $13, repository_owner = $14, repository_name = $15, \
             worktree_path = $16, scope = $17, skip_worktree = $18, is_remote = $19, \
             default_model = $20, pr_number = $21, pr_url = $22, pr_status = $23, \
             active_pull_request = $24, pull_requests = $25, context_links = $26, tags = $27, \
             created_at = $28, updated_at = $29, \
             last_activity = CASE WHEN sqlite_julianday($30) IS NOT NULL \
               AND (last_activity IS NULL OR sqlite_julianday(last_activity) IS NULL \
               OR sqlite_julianday(last_activity) < sqlite_julianday($30)) \
               THEN $30 ELSE last_activity END, \
             token_usage = $31, setup_script = $32, checkout_mode = $33 \
             WHERE tenant_id = $1 AND id = $2 RETURNING branch",
        )
        .bind(self.tenant())
        .bind(&ws.id.0)
        .bind(&ws.title)
        .bind(branch)
        .bind(&ws.base_ref)
        .bind(&ws.base_commit_sha)
        .bind(enum_to_db(&ws.status)?)
        .bind(enum_to_db(&WorkspaceStatus::Archived)?)
        .bind(&ws.status_message)
        .bind(&ws.status_image_asset_id)
        .bind(enum_to_db(&ws.attention)?)
        .bind(&ws.path)
        .bind(&ws.repository_path)
        .bind(&ws.repository_owner)
        .bind(&ws.repository_name)
        .bind(&ws.worktree_path)
        .bind(&ws.scope)
        .bind(i64::from(ws.skip_worktree))
        .bind(i64::from(ws.is_remote))
        .bind(&ws.default_model)
        .bind(ws.pr_number.map(u64::cast_signed))
        .bind(&ws.pr_url)
        .bind(ws_repo::pr_status_to_db(ws)?)
        .bind(ws_repo::active_pr_to_db(ws)?)
        .bind(ws_repo::pull_requests_to_db(ws)?)
        .bind(ws_repo::context_links_to_db(ws)?)
        .bind(tags_to_db(&ws.tags)?)
        .bind(&ws.created_at)
        .bind(&ws.updated_at)
        .bind(&ws.last_activity)
        .bind(ws_repo::token_usage_to_db(ws)?)
        .bind(ws_repo::setup_script_to_db(ws)?)
        .bind(ws_repo::checkout_mode_to_db(ws)?)
        .fetch_optional(&mut *tx)
        .await
        .map_err(|e| Error::Internal(format!("workspace update failed: {e}")))?;
        tx.commit().await.map_err(db)?;
        stored.ok_or_else(|| not_found(&ws.id))
    }

    pub async fn reconcile_workspace_branch(
        &self,
        expected: &Workspace,
        branch: &str,
    ) -> Result<bool> {
        let mut tx = self.begin_write().await?;
        let changed = sqlx::query(
            "UPDATE workspace SET branch = $3, branch_auto_generated = 0 \
             WHERE tenant_id = $1 AND id = $2 AND branch = $4 AND branch <> $3 \
             AND worktree_path IS NOT DISTINCT FROM $5 \
             AND repository_path IS NOT DISTINCT FROM $6 AND is_remote = 0",
        )
        .bind(self.tenant())
        .bind(&expected.id.0)
        .bind(branch)
        .bind(&expected.branch)
        .bind(&expected.worktree_path)
        .bind(&expected.repository_path)
        .execute(&mut *tx)
        .await
        .map_err(db)?
        .rows_affected();
        tx.commit().await.map_err(db)?;
        Ok(changed != 0)
    }

    pub async fn update_workspace_pr_linkage(&self, ws: &Workspace) -> Result<()> {
        let mut ws = ws.clone();
        self.update_workspace_pr_linkage_rebased(&mut ws, |_, _| {})
            .await
    }

    pub async fn update_workspace_pr_linkage_rebased<F>(
        &self,
        ws: &mut Workspace,
        rebase: F,
    ) -> Result<()>
    where
        F: FnOnce(&mut Workspace, Option<Vec<PullRequestInfo>>),
    {
        let mut tx = self.begin_write().await?;
        let stored: Option<Option<String>> = sqlx::query_scalar(
            "SELECT pull_requests FROM workspace WHERE tenant_id = $1 AND id = $2",
        )
        .bind(self.tenant())
        .bind(&ws.id.0)
        .fetch_optional(&mut *tx)
        .await
        .map_err(db)?;
        let Some(stored) = stored else {
            return Err(not_found(&ws.id));
        };
        let persisted = stored.and_then(|s| serde_json::from_str::<Vec<PullRequestInfo>>(&s).ok());
        rebase(ws, persisted);
        let updated = sqlx::query(
            "UPDATE workspace SET pr_number = $3, pr_url = $4, pr_status = $5, \
             active_pull_request = $6, pull_requests = $7, updated_at = $8 \
             WHERE tenant_id = $1 AND id = $2",
        )
        .bind(self.tenant())
        .bind(&ws.id.0)
        .bind(ws.pr_number.map(u64::cast_signed))
        .bind(&ws.pr_url)
        .bind(ws_repo::pr_status_to_db(ws)?)
        .bind(ws_repo::active_pr_to_db(ws)?)
        .bind(ws_repo::pull_requests_to_db(ws)?)
        .bind(&ws.updated_at)
        .execute(&mut *tx)
        .await
        .map_err(|e| Error::Internal(format!("workspace pr linkage write failed: {e}")))?
        .rows_affected();
        if updated == 0 {
            return Err(not_found(&ws.id));
        }
        tx.commit().await.map_err(db)
    }

    pub async fn project_workspace_pr_snapshots<F>(
        &self,
        id: &WorkspaceId,
        updated_at: &str,
        project: F,
    ) -> Result<Option<(Option<Vec<PullRequestInfo>>, Option<PullRequestInfo>)>>
    where
        F: FnOnce(&mut Option<Vec<PullRequestInfo>>, &mut Option<PullRequestInfo>) -> bool,
    {
        let mut tx = self.begin_write().await?;
        let row = sqlx::query(
            "SELECT pull_requests, active_pull_request FROM workspace \
             WHERE tenant_id = $1 AND id = $2",
        )
        .bind(self.tenant())
        .bind(&id.0)
        .fetch_optional(&mut *tx)
        .await
        .map_err(db)?;
        let Some(row) = row else {
            return Err(not_found(id));
        };
        let mut pool = ws_repo::pull_requests_from_db(col(&row, "pull_requests")?)?;
        let mut active = ws_repo::active_pr_from_db(col(&row, "active_pull_request")?)?;
        if !project(&mut pool, &mut active) {
            return Ok(None);
        }
        let encode = |e: serde_json::Error| Error::Internal(format!("encode pr snapshot: {e}"));
        let pool_json = pool
            .as_ref()
            .map(serde_json::to_string)
            .transpose()
            .map_err(encode)?;
        let active_json = active
            .as_ref()
            .map(serde_json::to_string)
            .transpose()
            .map_err(encode)?;
        let updated = sqlx::query(
            "UPDATE workspace SET pull_requests = $3, active_pull_request = $4, updated_at = $5 \
             WHERE tenant_id = $1 AND id = $2",
        )
        .bind(self.tenant())
        .bind(&id.0)
        .bind(pool_json)
        .bind(active_json)
        .bind(updated_at)
        .execute(&mut *tx)
        .await
        .map_err(db)?
        .rows_affected();
        if updated == 0 {
            return Err(not_found(id));
        }
        tx.commit().await.map_err(db)?;
        Ok(Some((pool, active)))
    }

    pub async fn set_workspace_attention(
        &self,
        id: &WorkspaceId,
        attention: WorkspaceAttention,
        updated_at: Option<&str>,
        expected: Option<WorkspaceAttention>,
    ) -> Result<bool> {
        debug_assert!(
            expected.as_ref() != Some(&attention),
            "expected == attention degenerates to a same-value rewrite"
        );
        let target = enum_to_db(&attention)?;
        let guard = match &expected {
            Some(from) => enum_to_db(from)?,
            None => target.clone(),
        };
        let sql = match (updated_at.is_some(), expected.is_some()) {
            (true, true) => {
                "UPDATE workspace SET attention = $3, updated_at = $5 \
                 WHERE tenant_id = $1 AND id = $2 AND attention = $4"
            }
            (true, false) => {
                "UPDATE workspace SET attention = $3, updated_at = $5 \
                 WHERE tenant_id = $1 AND id = $2 AND attention <> $4"
            }
            (false, true) => {
                "UPDATE workspace SET attention = $3 \
                 WHERE tenant_id = $1 AND id = $2 AND attention = $4"
            }
            (false, false) => {
                "UPDATE workspace SET attention = $3 \
                 WHERE tenant_id = $1 AND id = $2 AND attention <> $4"
            }
        };
        let mut tx = self.begin_write().await?;
        let mut query = sqlx::query(sql)
            .bind(self.tenant())
            .bind(&id.0)
            .bind(&target)
            .bind(&guard);
        if let Some(ts) = updated_at {
            query = query.bind(ts);
        }
        let changed = query.execute(&mut *tx).await.map_err(db)?.rows_affected() > 0;
        let present = changed || self.workspace_exists(&mut tx, id).await?;
        tx.commit().await.map_err(db)?;
        if !present {
            return Err(not_found(id));
        }
        Ok(changed)
    }

    pub async fn clear_workspace_unread_if_all_seen(&self, id: &WorkspaceId) -> Result<bool> {
        let mut tx = self.begin_write().await?;
        let cleared = sqlx::query(
            "UPDATE workspace w SET attention = 'none' \
             WHERE w.tenant_id = $1 AND w.id = $2 AND w.attention = 'unread' \
             AND NOT EXISTS(SELECT 1 FROM agent_session a \
               WHERE a.tenant_id = $1 AND a.workspace_id = w.id \
               AND a.parent_agent_id IS NULL AND a.is_background = 0 \
               AND a.status <> 'deleted' AND a.last_message_id IS NOT NULL \
               AND a.last_message_role = 'assistant' AND a.retired_at IS NULL \
               AND a.notifications_muted = 0 \
               AND (sqlite_json_extract(a.metadata, '$.lastSeenMessageId') IS NULL \
                    OR sqlite_json_extract(a.metadata, '$.lastSeenMessageId') \
                       <> a.last_message_id))",
        )
        .bind(self.tenant())
        .bind(&id.0)
        .execute(&mut *tx)
        .await
        .map_err(db)?
        .rows_affected();
        tx.commit().await.map_err(db)?;
        Ok(cleared > 0)
    }

    pub async fn unarchive_workspace_if_archived(
        &self,
        id: &WorkspaceId,
        updated_at: &str,
    ) -> Result<bool> {
        let mut tx = self.begin_write().await?;
        let changed = sqlx::query(
            "UPDATE workspace SET status = $3, archived = 0, archived_at = NULL, updated_at = $4 \
             WHERE tenant_id = $1 AND id = $2 AND archived = 1 AND status <> $5",
        )
        .bind(self.tenant())
        .bind(&id.0)
        .bind(enum_to_db(&WorkspaceStatus::Active)?)
        .bind(updated_at)
        .bind(enum_to_db(&WorkspaceStatus::Retired)?)
        .execute(&mut *tx)
        .await
        .map_err(db)?
        .rows_affected()
            > 0;
        let present = changed || self.workspace_exists(&mut tx, id).await?;
        tx.commit().await.map_err(db)?;
        if !present {
            return Err(not_found(id));
        }
        Ok(changed)
    }

    pub async fn bump_workspace_last_activity(
        &self,
        id: &WorkspaceId,
        last_activity: &str,
    ) -> Result<bool> {
        let mut tx = self.begin_write().await?;
        let changed = sqlx::query(
            "UPDATE workspace SET last_activity = $3 \
             WHERE tenant_id = $1 AND id = $2 AND sqlite_julianday($3) IS NOT NULL \
             AND (last_activity IS NULL OR sqlite_julianday(last_activity) IS NULL \
             OR sqlite_julianday(last_activity) < sqlite_julianday($3))",
        )
        .bind(self.tenant())
        .bind(&id.0)
        .bind(last_activity)
        .execute(&mut *tx)
        .await
        .map_err(db)?
        .rows_affected()
            > 0;
        let present = changed || self.workspace_exists(&mut tx, id).await?;
        tx.commit().await.map_err(db)?;
        if !present {
            return Err(not_found(id));
        }
        Ok(changed)
    }

    pub async fn update_workspace_token_usage<F>(
        &self,
        workspace_id: &WorkspaceId,
        compute: F,
    ) -> Result<Option<TokenUsage>>
    where
        F: FnOnce(&[AgentUsageRow], Option<&TokenUsage>) -> Option<TokenUsage>,
    {
        let mut tx = self.begin_write().await?;
        let stored: Option<Option<String>> = sqlx::query_scalar(
            "SELECT token_usage FROM workspace WHERE tenant_id = $1 AND id = $2",
        )
        .bind(self.tenant())
        .bind(&workspace_id.0)
        .fetch_optional(&mut *tx)
        .await
        .map_err(db)?;
        let Some(stored) = stored else {
            return Err(not_found(workspace_id));
        };
        // As on SQLite, a malformed stored snapshot decodes to None.
        let current: Option<TokenUsage> = stored.and_then(|s| serde_json::from_str(&s).ok());
        let usage_rows = self.agent_usage_rows(&mut tx, workspace_id, None).await?;
        let Some(new_usage) = compute(&usage_rows, current.as_ref()) else {
            tx.commit().await.map_err(db)?;
            return Ok(None);
        };
        let json = serde_json::to_string(&new_usage)
            .map_err(|e| Error::Internal(format!("encode token_usage failed: {e}")))?;
        let updated =
            sqlx::query("UPDATE workspace SET token_usage = $3 WHERE tenant_id = $1 AND id = $2")
                .bind(self.tenant())
                .bind(&workspace_id.0)
                .bind(json)
                .execute(&mut *tx)
                .await
                .map_err(db)?
                .rows_affected();
        if updated == 0 {
            return Err(not_found(workspace_id));
        }
        tx.commit().await.map_err(db)?;
        Ok(Some(new_usage))
    }

    pub async fn delete_workspace(&self, id: &WorkspaceId) -> Result<()> {
        self.delete_workspace_with_outcome(id).await.result
    }

    pub async fn delete_workspace_with_outcome(
        &self,
        id: &WorkspaceId,
    ) -> RepositoryWorkspaceDeleteOutcome {
        let mut disposition = RepositoryWorkspaceDeleteDisposition::Unknown;
        let committed = Arc::new(AtomicBool::new(false));
        let result = self
            .delete_workspace_observed(id, &mut disposition, committed.clone())
            .await;
        if committed.load(Ordering::Acquire) {
            disposition = RepositoryWorkspaceDeleteDisposition::Committed;
        }
        RepositoryWorkspaceDeleteOutcome {
            result,
            disposition,
        }
    }

    /// The `SQLite` deletion as a bounded, resumable purge (see `purge`): a
    /// short transaction records it, bounded batches drain the heavy child
    /// tables, and one final transaction deletes the rest and the record.
    async fn delete_workspace_observed(
        &self,
        id: &WorkspaceId,
        disposition: &mut RepositoryWorkspaceDeleteDisposition,
        committed: Arc<AtomicBool>,
    ) -> Result<()> {
        // This workspace's desktop gate, held through the final commit as on
        // SQLite: none of its private desktop writes can start meanwhile,
        // while other workspaces' proceed. The maintenance gate is taken only
        // around the final transaction (see `DesktopWrites::delete_scope`).
        // Both move into the purge task, so a cancelled caller does not
        // release them early.
        let desktop_scope = self.desktop_writes.delete_scope(id).await;
        let mut lifecycle = self.repository_lifecycle_write().await?;
        let mut tx = self.begin_write().await?;
        if !self.workspace_exists(&mut tx, id).await? {
            // A record a stopped delete left has nothing more to do.
            super::purge::clear_workspace_purges(&mut tx, self.tenant(), id).await?;
            tx.commit().await.map_err(db)?;
            *disposition = RepositoryWorkspaceDeleteDisposition::NoEffect;
            return Err(not_found(id));
        }
        // Another workspace's retry key naming one of this workspace's
        // attachments blocks the final delete (the key's foreign key); fail
        // now, before any batch, so the delete stays all-or-nothing.
        let foreign_key: bool = sqlx::query_scalar(
            "SELECT EXISTS(SELECT 1 FROM attachment_idempotency_keys k \
             JOIN attachments a ON a.tenant_id = $1 AND a.id = k.attachment_id \
             WHERE k.tenant_id = $1 AND a.workspace_id = $2 AND k.workspace_id <> $2)",
        )
        .bind(self.tenant())
        .bind(&id.0)
        .fetch_one(&mut *tx)
        .await
        .map_err(db)?;
        if foreign_key {
            return Err(Error::Internal(format!(
                "delete workspace {id}: another workspace's attachment retry key \
                 names one of its attachments"
            )));
        }
        let agents = workspace_agents(&mut tx, self.tenant(), id).await?;
        // SQLite begins the workspace here and each agent in its own nested
        // deletion; this one bracket owns all of them (sessions created
        // during the purge get their own, below).
        let keys: Vec<crate::RepositoryLifecycleKey> =
            std::iter::once(crate::RepositoryLifecycleKey::Workspace(id.clone()))
                .chain(
                    agents
                        .iter()
                        .cloned()
                        .map(crate::RepositoryLifecycleKey::Agent),
                )
                .collect();
        lifecycle.begin(&keys)?;
        super::purge::record_purge(
            &mut tx,
            self.tenant(),
            super::purge::PurgeKind::Workspace,
            id,
            None,
        )
        .await?;
        tx.commit().await.map_err(db)?;
        // As on SQLite, the bounded sweep does not hold lifecycle
        // serialization; the bracket stays owned through completion.
        lifecycle.release_serialization();
        // A recorded purge finishes its batches, final commit and overlay
        // eviction even if the caller is cancelled.
        let store = self.clone();
        let id = id.clone();
        intent_core::caller::spawn_with_current_caller(async move {
            let _desktop_scope = desktop_scope;
            let mut late = Vec::new();
            let result = store.finish_workspace_delete(&id, agents, &mut late).await;
            // The final transaction committed: record it before settling the
            // late sessions' brackets, whose failure is still returned
            // (`Committed` with an error, like a failed main settlement).
            if result.is_ok() {
                committed.store(true, Ordering::Release);
            }
            let result = match result {
                Ok(()) => settle_all(store.tenant(), late).await,
                Err(e) => Err(e),
            };
            lifecycle.finish(result).await
        })
        .await
        .map_err(|e| Error::Internal(format!("delete workspace task failed: {e}")))?
    }

    /// Drain, then run the final transaction. A session created since the
    /// purge began gets its own lifecycle bracket (pushed to `late`, as
    /// `SQLite`'s nested agent deletion would begin one) and its drain before
    /// the final transaction deletes it.
    async fn finish_workspace_delete(
        &self,
        id: &WorkspaceId,
        mut known: Vec<AgentId>,
        late: &mut Vec<crate::repository_lifecycle::LifecycleWrite>,
    ) -> Result<()> {
        self.purge_workspace_children(id, &known).await?;
        loop {
            let maintenance = self.desktop_writes.maintenance_read().await;
            let mut tx = self.begin_write().await?;
            let agents = workspace_agents(&mut tx, self.tenant(), id).await?;
            let new: Vec<AgentId> = agents
                .iter()
                .filter(|a| !known.contains(a))
                .cloned()
                .collect();
            if new.is_empty() {
                let result = self.delete_workspace_rows(tx, id, &agents).await;
                drop(maintenance);
                return result;
            }
            tx.rollback().await.map_err(db)?;
            drop(maintenance);
            for agent in new {
                let mut bracket = self.repository_lifecycle_write().await?;
                bracket.begin(&[crate::RepositoryLifecycleKey::Agent(agent.clone())])?;
                bracket.release_serialization();
                late.push(bracket);
                self.purge_agent_children(&agent).await?;
                known.push(agent);
            }
        }
    }

    async fn delete_workspace_rows(
        &self,
        mut tx: super::WriteTx,
        id: &WorkspaceId,
        agents: &[AgentId],
    ) -> Result<()> {
        let tenant = self.tenant();
        let exec = |sql: &'static str| sqlx::query(sql).bind(tenant.to_owned()).bind(id.0.clone());
        // Recovery history of this workspace, except a row naming a live
        // session of another workspace.
        exec(
            "DELETE FROM interrupted_agent i WHERE i.tenant_id = $1 AND i.workspace_id = $2 \
             AND NOT EXISTS (SELECT 1 FROM agent_session s WHERE s.tenant_id = $1 \
               AND s.id = i.agent_id AND s.workspace_id <> $2)",
        )
        .execute(&mut *tx)
        .await
        .map_err(db)?;
        super::desktop::delete_desktop_scope(&mut tx, tenant, id, None).await?;
        for agent in agents {
            super::agents::delete_workspace_session(&mut tx, tenant, id, agent).await?;
        }
        // No workspace foreign key. Retry keys go before attachments (their
        // key is NO ACTION), and a foreign workspace's key blocks deletion.
        for sql in [
            "DELETE FROM script WHERE tenant_id = $1 AND workspace_id = $2",
            "DELETE FROM attachment_idempotency_keys k WHERE k.tenant_id = $1 \
             AND k.workspace_id = $2 AND EXISTS (SELECT 1 FROM attachments a \
               WHERE a.tenant_id = $1 AND a.id = k.attachment_id AND a.workspace_id = $2)",
            "DELETE FROM attachments WHERE tenant_id = $1 AND workspace_id = $2",
            "UPDATE note SET parent_id = NULL \
             WHERE tenant_id = $1 AND workspace_id = $2 AND parent_id IS NOT NULL",
            "DELETE FROM note_page_entry WHERE tenant_id = $1 AND workspace_id = $2",
            "DELETE FROM note_page_head WHERE tenant_id = $1 AND workspace_id = $2",
            // Comments with no note stay, as on SQLite.
            "DELETE FROM comment WHERE tenant_id = $1 AND workspace_id = $2 \
             AND note_id IS NOT NULL",
        ] {
            exec(sql).execute(&mut *tx).await.map_err(db)?;
        }
        for table in WORKSPACE_CHILD_TABLES {
            let sql = format!("DELETE FROM {table} WHERE tenant_id = $1 AND workspace_id = $2");
            sqlx::query(sqlx::AssertSqlSafe(sql))
                .bind(tenant)
                .bind(&id.0)
                .execute(&mut *tx)
                .await
                .map_err(db)?;
        }
        let tab_ids: Vec<String> = sqlx::query_scalar(
            "DELETE FROM browser_tab WHERE tenant_id = $1 AND workspace_id = $2 RETURNING tab_id",
        )
        .bind(tenant)
        .bind(&id.0)
        .fetch_all(&mut *tx)
        .await
        .map_err(db)?;
        sqlx::query("DELETE FROM settings WHERE tenant_id = $1 AND key = $2")
            .bind(tenant)
            .bind(crate::settings_repo::agent_creation_preferences_key(id))
            .execute(&mut *tx)
            .await
            .map_err(db)?;
        // A plain-folder row's folder outlives the row on the tombstone.
        let plain_folder_path: Option<String> = sqlx::query_scalar(
            "SELECT repository_path FROM workspace WHERE tenant_id = $1 AND id = $2 \
             AND plain_folder = 1 AND repository_path <> ''",
        )
        .bind(tenant)
        .bind(&id.0)
        .fetch_optional(&mut *tx)
        .await
        .map_err(db)?
        .flatten();
        let now = now_iso();
        super::homes::retire_workspace_home(&mut tx, tenant, id, &now, true).await?;
        let deleted = exec("DELETE FROM workspace WHERE tenant_id = $1 AND id = $2")
            .execute(&mut *tx)
            .await
            .map_err(db)?
            .rows_affected();
        if deleted == 0 {
            return Err(not_found(id));
        }
        sqlx::query(
            "INSERT INTO deleted_workspace_id (tenant_id, id, deleted_at, plain_folder_path) \
             VALUES ($1, $2, $3, $4) ON CONFLICT (tenant_id, id) DO UPDATE SET \
             deleted_at = excluded.deleted_at, plain_folder_path = excluded.plain_folder_path",
        )
        .bind(tenant)
        .bind(&id.0)
        .bind(&now)
        .bind(plain_folder_path)
        .execute(&mut *tx)
        .await
        .map_err(db)?;
        super::purge::clear_workspace_purges(&mut tx, tenant, id).await?;
        tx.commit().await.map_err(db)?;
        self.browser_tab_displayed
            .forget_all(tab_ids.iter().map(String::as_str));
        Ok(())
    }

    pub async fn plain_folder_paths(&self) -> Result<Vec<String>> {
        let mut tx = self.begin().await?;
        let paths = sqlx::query_scalar(
            "SELECT repository_path FROM workspace WHERE tenant_id = $1 AND plain_folder = 1 \
             AND repository_path IS NOT NULL AND repository_path != '' \
             UNION SELECT plain_folder_path FROM deleted_workspace_id WHERE tenant_id = $1 \
             AND plain_folder_path IS NOT NULL AND plain_folder_path != ''",
        )
        .bind(self.tenant())
        .fetch_all(&mut *tx)
        .await
        .map_err(db)?;
        tx.commit().await.map_err(db)?;
        Ok(paths)
    }

    pub async fn workspace_id_ever_used(&self, id: &WorkspaceId) -> Result<bool> {
        let mut tx = self.begin().await?;
        let used = sqlx::query_scalar(
            "SELECT EXISTS(SELECT 1 FROM workspace WHERE tenant_id = $1 AND id = $2) \
             OR EXISTS(SELECT 1 FROM deleted_workspace_id WHERE tenant_id = $1 AND id = $2)",
        )
        .bind(self.tenant())
        .bind(&id.0)
        .fetch_one(&mut *tx)
        .await
        .map_err(db)?;
        tx.commit().await.map_err(db)?;
        Ok(used)
    }

    /// One scoped column write by id; `NotFound` when no row matched.
    async fn set_workspace_column(
        &self,
        column: &'static str,
        id: &WorkspaceId,
        value: Option<String>,
        int: Option<i64>,
    ) -> Result<()> {
        let sql = format!("UPDATE workspace SET {column} = $3 WHERE tenant_id = $1 AND id = $2");
        let mut tx = self.begin_write().await?;
        let query = sqlx::query(sqlx::AssertSqlSafe(sql.as_str()))
            .bind(self.tenant())
            .bind(&id.0);
        let query = match int {
            Some(v) => query.bind(v),
            None => query.bind(value),
        };
        let updated = query.execute(&mut *tx).await.map_err(db)?.rows_affected();
        tx.commit().await.map_err(db)?;
        if updated == 0 {
            return Err(not_found(id));
        }
        Ok(())
    }

    /// One column of one workspace row; `NotFound` when the row is absent.
    async fn workspace_column<T>(&self, sql: &'static str, id: &WorkspaceId) -> Result<T>
    where
        T: for<'r> sqlx::Decode<'r, sqlx::Postgres> + sqlx::Type<sqlx::Postgres> + Send + Unpin,
    {
        let mut tx = self.begin().await?;
        let value: Option<T> = sqlx::query_scalar(sql)
            .bind(self.tenant())
            .bind(&id.0)
            .fetch_optional(&mut *tx)
            .await
            .map_err(db)?;
        tx.commit().await.map_err(db)?;
        value.ok_or_else(|| not_found(id))
    }

    pub async fn set_workspace_branch_auto_generated(
        &self,
        id: &WorkspaceId,
        auto_generated: bool,
    ) -> Result<()> {
        self.set_workspace_column(
            "branch_auto_generated",
            id,
            None,
            Some(i64::from(auto_generated)),
        )
        .await
    }

    pub async fn workspace_branch_auto_generated(&self, id: &WorkspaceId) -> Result<bool> {
        let value: i64 = self
            .workspace_column(
                "SELECT branch_auto_generated FROM workspace WHERE tenant_id = $1 AND id = $2",
                id,
            )
            .await?;
        Ok(value != 0)
    }

    pub async fn set_workspace_auto_commit(&self, id: &WorkspaceId, enabled: bool) -> Result<()> {
        self.set_workspace_column("auto_commit_enabled", id, None, Some(i64::from(enabled)))
            .await
    }

    pub async fn set_workspace_label(&self, id: &WorkspaceId, label: Option<&str>) -> Result<()> {
        self.set_workspace_column("label", id, label.map(str::to_owned), None)
            .await
    }

    pub async fn set_workspace_auto_archive_disabled(
        &self,
        id: &WorkspaceId,
        disabled: bool,
    ) -> Result<()> {
        self.set_workspace_column("auto_archive_disabled", id, None, Some(i64::from(disabled)))
            .await
    }

    pub async fn workspace_auto_commit(&self, id: &WorkspaceId) -> Result<Option<bool>> {
        let value: Option<i64> = self
            .workspace_column(
                "SELECT auto_commit_enabled FROM workspace WHERE tenant_id = $1 AND id = $2",
                id,
            )
            .await?;
        Ok(value.map(|v| v != 0))
    }

    pub async fn set_workspace_browser_client(
        &self,
        id: &WorkspaceId,
        client_id: Option<&ClientId>,
    ) -> Result<()> {
        self.set_workspace_column(
            "browser_client_id",
            id,
            client_id.map(|c| c.0.clone()),
            None,
        )
        .await
    }

    pub async fn workspace_browser_client(&self, id: &WorkspaceId) -> Result<Option<ClientId>> {
        let value: Option<String> = self
            .workspace_column(
                "SELECT browser_client_id FROM workspace WHERE tenant_id = $1 AND id = $2",
                id,
            )
            .await?;
        Ok(value.map(ClientId))
    }

    fn list_predicate(filter: WorkspaceListFilter) -> &'static str {
        // A retired row is also archived: retired rows pass only on
        // `include_retired`, other archived rows only on `include_archived`.
        match (filter.include_archived, filter.include_retired) {
            (true, true) => "1 = 1 OR status = $3",
            (true, false) => "status <> $3",
            (false, true) => "archived = 0 OR status = $3",
            (false, false) => "archived = 0 AND status <> $3",
        }
    }

    pub async fn list_workspaces(&self, filter: WorkspaceListFilter) -> Result<Vec<Workspace>> {
        let retired = enum_to_db(&WorkspaceStatus::Retired)?;
        let columns = ws_repo::WORKSPACE_COLUMNS;
        let predicate = Self::list_predicate(filter);
        let sql = format!(
            "SELECT {columns} FROM workspace WHERE tenant_id = $1 AND id <> $2 \
             AND substr(id, 1, 9) <> '__home__:' AND ({predicate}) \
             ORDER BY created_at COLLATE \"C\", row_seq"
        );
        self.workspace_rows(&sql, &[CHIEF_WORKSPACE_ID, retired.as_str()])
            .await
    }

    pub async fn list_workspaces_page(
        &self,
        filter: WorkspaceListFilter,
        after: Option<(&str, &str)>,
        limit: usize,
    ) -> Result<Vec<Workspace>> {
        let retired = enum_to_db(&WorkspaceStatus::Retired)?;
        let columns = ws_repo::WORKSPACE_COLUMNS;
        let predicate = Self::list_predicate(filter);
        let (keyset, limit_param) = if after.is_some() {
            (
                " AND (created_at COLLATE \"C\" > $4 \
                 OR (created_at = $4 AND id COLLATE \"C\" > $5))",
                "$6",
            )
        } else {
            ("", "$4")
        };
        let sql = format!(
            "SELECT {columns} FROM workspace WHERE tenant_id = $1 AND id <> $2 \
             AND substr(id, 1, 9) <> '__home__:' AND ({predicate}){keyset} \
             ORDER BY created_at COLLATE \"C\", id COLLATE \"C\" LIMIT {limit_param}"
        );
        let mut tx = self.begin().await?;
        let mut query = sqlx::query(sqlx::AssertSqlSafe(sql.as_str()))
            .bind(self.tenant())
            .bind(CHIEF_WORKSPACE_ID)
            .bind(&retired);
        if let Some((created_at, id)) = after {
            query = query.bind(created_at).bind(id);
        }
        let rows = query
            .bind(i64::try_from(limit).unwrap_or(i64::MAX))
            .fetch_all(&mut *tx)
            .await
            .map_err(db)?;
        tx.commit().await.map_err(db)?;
        rows.iter().map(map_workspace_row).collect()
    }

    pub async fn retired_workspace_for(
        &self,
        workspace_id: Option<&WorkspaceId>,
        agent_id: Option<&AgentId>,
    ) -> Result<Option<WorkspaceId>> {
        let mut tx = self.begin().await?;
        let id: Option<String> = sqlx::query_scalar(
            "SELECT w.id FROM workspace AS w WHERE w.tenant_id = $1 AND w.status = $2 \
             AND (w.id = $3 OR w.id = (SELECT a.workspace_id FROM agent_session AS a \
             WHERE a.tenant_id = $1 AND a.id = $4)) LIMIT 1",
        )
        .bind(self.tenant())
        .bind(enum_to_db(&WorkspaceStatus::Retired)?)
        .bind(workspace_id.map(|w| w.0.as_str()))
        .bind(agent_id.map(|a| a.0.as_str()))
        .fetch_optional(&mut *tx)
        .await
        .map_err(db)?;
        tx.commit().await.map_err(db)?;
        Ok(id.map(WorkspaceId))
    }

    pub async fn retire_workspace_if_archived(
        &self,
        id: &WorkspaceId,
        retired_at: &str,
    ) -> Result<bool> {
        let mut tx = self.begin_write().await?;
        let changed = sqlx::query(
            "UPDATE workspace SET status = $3, retired_at = $4, updated_at = $4, \
             status_message = $5 \
             WHERE tenant_id = $1 AND id = $2 AND archived = 1 AND status <> $3",
        )
        .bind(self.tenant())
        .bind(&id.0)
        .bind(enum_to_db(&WorkspaceStatus::Retired)?)
        .bind(retired_at)
        .bind(intent_core::WORKSPACE_PURGED_STATUS_MESSAGE)
        .execute(&mut *tx)
        .await
        .map_err(db)?
        .rows_affected()
            > 0;
        let present = changed || self.workspace_exists(&mut tx, id).await?;
        tx.commit().await.map_err(db)?;
        if !present {
            return Err(not_found(id));
        }
        Ok(changed)
    }

    pub async fn revive_workspace_if_retired(
        &self,
        id: &WorkspaceId,
        branch: &str,
        base_commit_sha: Option<&str>,
        updated_at: &str,
    ) -> Result<bool> {
        self.revive_workspace_if_retired_with_notice(id, branch, base_commit_sha, updated_at, None)
            .await
    }

    pub async fn revive_workspace_if_retired_with_notice(
        &self,
        id: &WorkspaceId,
        branch: &str,
        base_commit_sha: Option<&str>,
        updated_at: &str,
        notice: Option<(&str, &str, Option<&AgentId>)>,
    ) -> Result<bool> {
        let retired = enum_to_db(&WorkspaceStatus::Retired)?;
        let mut tx = self.begin_write().await?;
        let changed = sqlx::query(
            "UPDATE workspace SET status = $3, archived = 0, archived_at = NULL, \
             retired_at = NULL, branch = $4, base_commit_sha = $5, updated_at = $6, \
             status_message = CASE WHEN status_message = $7 THEN NULL ELSE status_message END \
             WHERE tenant_id = $1 AND id = $2 AND status = $8",
        )
        .bind(self.tenant())
        .bind(&id.0)
        .bind(enum_to_db(&WorkspaceStatus::Active)?)
        .bind(branch)
        .bind(base_commit_sha)
        .bind(updated_at)
        .bind(intent_core::WORKSPACE_PURGED_STATUS_MESSAGE)
        .bind(&retired)
        .execute(&mut *tx)
        .await
        .map_err(db)?
        .rows_affected()
            > 0;
        if !changed {
            let present = self.workspace_exists(&mut tx, id).await?;
            tx.commit().await.map_err(db)?;
            return if present {
                Ok(false)
            } else {
                Err(not_found(id))
            };
        }
        if let Some((key, text, excluded)) = notice {
            sqlx::query(
                "UPDATE agent_session SET metadata = sqlite_json_set( \
                   CASE WHEN metadata IS NULL THEN '{}' \
                     WHEN sqlite_json_type(metadata) = 'object' THEN metadata \
                     ELSE jsonb_build_object('priorNonObjectMetadata', sqlite_json(metadata))::text \
                   END, '$.' || $3, $4) \
                 WHERE tenant_id = $1 AND workspace_id = $2 AND ($5 IS NULL OR id <> $5)",
            )
            .bind(self.tenant())
            .bind(&id.0)
            .bind(key)
            .bind(text)
            .bind(excluded.map(|a| a.0.as_str()))
            .execute(&mut *tx)
            .await
            .map_err(|e| Error::Internal(format!("revive pending notice write failed: {e}")))?;
        }
        tx.commit().await.map_err(db)?;
        Ok(true)
    }

    pub async fn home_record_ids(&self) -> Result<Vec<WorkspaceId>> {
        let mut tx = self.begin().await?;
        let ids: Vec<String> = sqlx::query_scalar(
            "SELECT id FROM workspace WHERE tenant_id = $1 AND substr(id, 1, 9) = '__home__:' \
             ORDER BY row_seq",
        )
        .bind(self.tenant())
        .fetch_all(&mut *tx)
        .await
        .map_err(db)?;
        tx.commit().await.map_err(db)?;
        Ok(ids.into_iter().map(WorkspaceId).collect())
    }

    pub async fn delete_home_record(&self, id: &WorkspaceId) -> Result<()> {
        if !id.is_home_record() {
            return Err(Error::InvalidParams(format!("{id} is not a home record")));
        }
        let mut tx = self.begin_write().await?;
        sqlx::query("DELETE FROM workspace WHERE tenant_id = $1 AND id = $2")
            .bind(self.tenant())
            .bind(&id.0)
            .execute(&mut *tx)
            .await
            .map_err(db)?;
        tx.commit().await.map_err(db)
    }

    pub async fn ordinary_workspace_ids(&self) -> Result<Vec<WorkspaceId>> {
        let mut tx = self.begin().await?;
        let ids: Vec<String> = sqlx::query_scalar(
            "SELECT id FROM workspace WHERE tenant_id = $1 AND id <> $2 \
             AND substr(id, 1, 9) <> '__home__:' ORDER BY row_seq",
        )
        .bind(self.tenant())
        .bind(CHIEF_WORKSPACE_ID)
        .fetch_all(&mut *tx)
        .await
        .map_err(db)?;
        tx.commit().await.map_err(db)?;
        Ok(ids.into_iter().map(WorkspaceId).collect())
    }

    pub async fn list_workspaces_referencing_pr_url(&self, url: &str) -> Result<Vec<Workspace>> {
        let columns = ws_repo::WORKSPACE_COLUMNS;
        let sql = format!(
            "SELECT {columns} FROM workspace w WHERE w.tenant_id = $1 AND w.id <> $2 \
             AND substr(w.id, 1, 9) <> '__home__:' AND w.archived = 0 AND w.is_remote = 0 \
             AND (lower(w.pr_url) = lower($3) OR (w.pull_requests IS NOT NULL \
             AND sqlite_json_valid(w.pull_requests) AND EXISTS ( \
             SELECT 1 FROM sqlite_json_each(w.pull_requests) AS je \
             WHERE lower(sqlite_json_extract(je.value, '$.url')) = lower($3)))) \
             ORDER BY w.created_at COLLATE \"C\", w.row_seq"
        );
        self.workspace_rows(&sql, &[CHIEF_WORKSPACE_ID, url]).await
    }

    pub async fn list_pull_request_workspaces(
        &self,
        pr_key: &str,
        include_retired: bool,
    ) -> Result<Vec<PullRequestWorkspace>> {
        let mut tx = self.begin().await?;
        let rows = sqlx::query(
            "SELECT w.id, w.title, w.archived, p.linked_at FROM pull_request_workspace AS p \
             JOIN workspace AS w ON w.tenant_id = p.tenant_id AND w.id = p.workspace_id \
             WHERE p.tenant_id = $1 AND w.tenant_id = $1 AND p.pr_key = $2 AND w.id <> $3 \
             AND substr(w.id, 1, 9) <> '__home__:' AND w.is_remote = 0 \
             AND ($4 OR w.status <> $5) \
             ORDER BY p.linked_at COLLATE \"C\" DESC, w.id COLLATE \"C\" ASC",
        )
        .bind(self.tenant())
        .bind(pr_key)
        .bind(CHIEF_WORKSPACE_ID)
        .bind(include_retired)
        .bind(enum_to_db(&WorkspaceStatus::Retired)?)
        .fetch_all(&mut *tx)
        .await
        .map_err(db)?;
        tx.commit().await.map_err(db)?;
        rows.iter()
            .map(|row| {
                Ok(PullRequestWorkspace {
                    id: WorkspaceId(col(row, "id")?),
                    title: col(row, "title")?,
                    archived: col::<i64>(row, "archived")? != 0,
                    linked_at: col(row, "linked_at")?,
                })
            })
            .collect()
    }

    pub async fn list_pull_request_links_for_workspaces(
        &self,
        workspace_ids: &[WorkspaceId],
    ) -> Result<Vec<PullRequestLink>> {
        if workspace_ids.is_empty() {
            return Ok(Vec::new());
        }
        let mut tx = self.begin().await?;
        let rows = sqlx::query(
            "SELECT pr_key, workspace_id, linked_at FROM pull_request_workspace \
             WHERE tenant_id = $1 AND workspace_id = ANY($2) \
             ORDER BY linked_at COLLATE \"C\" DESC, pr_key COLLATE \"C\" ASC, \
             workspace_id COLLATE \"C\" ASC",
        )
        .bind(self.tenant())
        .bind(unique_ids(workspace_ids))
        .fetch_all(&mut *tx)
        .await
        .map_err(db)?;
        tx.commit().await.map_err(db)?;
        rows.iter()
            .map(|row| {
                Ok(PullRequestLink {
                    pr_key: col(row, "pr_key")?,
                    workspace_id: WorkspaceId(col(row, "workspace_id")?),
                    linked_at: col(row, "linked_at")?,
                })
            })
            .collect()
    }

    // ---- workspace_context_repo / workspace_ui_context_repo ---------------

    pub async fn list_workspace_context_items(
        &self,
        workspace_id: &WorkspaceId,
    ) -> Result<Vec<ContextItem>> {
        let mut tx = self.begin().await?;
        let rows = sqlx::query(
            "SELECT id, payload FROM workspace_context_item \
             WHERE tenant_id = $1 AND workspace_id = $2 ORDER BY ordinal",
        )
        .bind(self.tenant())
        .bind(&workspace_id.0)
        .fetch_all(&mut *tx)
        .await
        .map_err(db)?;
        tx.commit().await.map_err(db)?;
        rows.iter()
            .map(|row| {
                let mut item: ContextItem =
                    serde_json::from_str(&col::<String>(row, "payload")?)
                        .map_err(|e| Error::Internal(format!("decode context item failed: {e}")))?;
                item.id = col(row, "id")?;
                Ok(item)
            })
            .collect()
    }

    pub async fn replace_workspace_context_items(
        &self,
        workspace_id: &WorkspaceId,
        items: &[ContextItem],
    ) -> Result<Vec<ContextItem>> {
        let mut tx = self.begin_write().await?;
        sqlx::query(
            "DELETE FROM workspace_context_item WHERE tenant_id = $1 AND workspace_id = $2",
        )
        .bind(self.tenant())
        .bind(&workspace_id.0)
        .execute(&mut *tx)
        .await
        .map_err(db)?;
        for (idx, item) in items.iter().enumerate() {
            let payload = serde_json::to_string(item)
                .map_err(|e| Error::Internal(format!("encode context item failed: {e}")))?;
            sqlx::query(
                "INSERT INTO workspace_context_item (tenant_id, workspace_id, id, ordinal, payload) \
                 VALUES ($1, $2, $3, $4, $5)",
            )
            .bind(self.tenant())
            .bind(&workspace_id.0)
            .bind(&item.id)
            .bind(i64::try_from(idx).unwrap_or(i64::MAX))
            .bind(payload)
            .execute(&mut *tx)
            .await
            .map_err(|e| Error::Internal(format!("context item insert failed: {e}")))?;
        }
        tx.commit().await.map_err(db)?;
        self.list_workspace_context_items(workspace_id).await
    }

    pub async fn get_workspace_ui_context(
        &self,
        workspace_id: &WorkspaceId,
    ) -> Result<Option<serde_json::Value>> {
        let mut tx = self.begin().await?;
        let payload: Option<String> = sqlx::query_scalar(
            "SELECT payload FROM workspace_ui_context WHERE tenant_id = $1 AND workspace_id = $2",
        )
        .bind(self.tenant())
        .bind(&workspace_id.0)
        .fetch_optional(&mut *tx)
        .await
        .map_err(db)?;
        tx.commit().await.map_err(db)?;
        payload
            .map(|p| {
                serde_json::from_str(&p)
                    .map_err(|e| Error::Internal(format!("decode ui context failed: {e}")))
            })
            .transpose()
    }

    pub async fn update_workspace_ui_context(
        &self,
        workspace_id: &WorkspaceId,
        ui_context: &serde_json::Value,
    ) -> Result<serde_json::Value> {
        let payload = serde_json::to_string(ui_context)
            .map_err(|e| Error::Internal(format!("encode ui context failed: {e}")))?;
        let mut tx = self.begin_write().await?;
        sqlx::query(
            "INSERT INTO workspace_ui_context (tenant_id, workspace_id, payload) \
             VALUES ($1, $2, $3) \
             ON CONFLICT (tenant_id, workspace_id) DO UPDATE SET payload = excluded.payload",
        )
        .bind(self.tenant())
        .bind(&workspace_id.0)
        .bind(&payload)
        .execute(&mut *tx)
        .await
        .map_err(|e| Error::Internal(format!("workspace ui context write failed: {e}")))?;
        tx.commit().await.map_err(db)?;
        self.get_workspace_ui_context(workspace_id)
            .await?
            .ok_or_else(|| Error::Internal("ui context disappeared after insert".into()))
    }

    // ---- workspace_mcp_repo -----------------------------------------------

    pub async fn set_workspace_mcp_server_disabled(
        &self,
        id: &WorkspaceId,
        server_id: &str,
        disabled: bool,
    ) -> Result<()> {
        let mut tx = self.begin_write().await?;
        if !self.workspace_exists(&mut tx, id).await? {
            return Err(not_found(id));
        }
        if disabled {
            sqlx::query(
                "INSERT INTO workspace_mcp_disabled_server \
                 (tenant_id, workspace_id, server_id, created_at) VALUES ($1, $2, $3, $4) \
                 ON CONFLICT (tenant_id, workspace_id, server_id) DO NOTHING",
            )
            .bind(self.tenant())
            .bind(&id.0)
            .bind(server_id)
            .bind(now_iso())
            .execute(&mut *tx)
            .await
            .map_err(db)?;
        } else {
            sqlx::query(
                "DELETE FROM workspace_mcp_disabled_server \
                 WHERE tenant_id = $1 AND workspace_id = $2 AND server_id = $3",
            )
            .bind(self.tenant())
            .bind(&id.0)
            .bind(server_id)
            .execute(&mut *tx)
            .await
            .map_err(db)?;
        }
        tx.commit().await.map_err(db)
    }

    pub async fn workspace_mcp_disabled_servers(&self, id: &WorkspaceId) -> Result<Vec<String>> {
        let mut tx = self.begin().await?;
        let servers = sqlx::query_scalar(
            "SELECT server_id FROM workspace_mcp_disabled_server \
             WHERE tenant_id = $1 AND workspace_id = $2 ORDER BY server_id COLLATE \"C\"",
        )
        .bind(self.tenant())
        .bind(&id.0)
        .fetch_all(&mut *tx)
        .await
        .map_err(db)?;
        tx.commit().await.map_err(db)?;
        Ok(servers)
    }

    pub async fn workspace_mcp_server_disabled(
        &self,
        id: &WorkspaceId,
        server_id: &str,
    ) -> Result<bool> {
        let mut tx = self.begin().await?;
        let disabled = sqlx::query_scalar(
            "SELECT EXISTS(SELECT 1 FROM workspace_mcp_disabled_server \
             WHERE tenant_id = $1 AND workspace_id = $2 AND server_id = $3)",
        )
        .bind(self.tenant())
        .bind(&id.0)
        .bind(server_id)
        .fetch_one(&mut *tx)
        .await
        .map_err(db)?;
        tx.commit().await.map_err(db)?;
        Ok(disabled)
    }

    // ---- workspace_git_root_repo -------------------------------------------

    pub async fn upsert_workspace_git_root(
        &self,
        root: &WorkspaceGitRoot,
    ) -> Result<(WorkspaceGitRoot, bool)> {
        let columns = roots::COLUMNS;
        let mut tx = self.begin_write().await?;
        let sql = format!(
            "SELECT {columns} FROM workspace_git_root \
             WHERE tenant_id = $1 AND workspace_id = $2 AND path = $3"
        );
        let existing = sqlx::query(sqlx::AssertSqlSafe(sql.as_str()))
            .bind(self.tenant())
            .bind(&root.workspace_id.0)
            .bind(&root.path)
            .fetch_optional(&mut *tx)
            .await
            .map_err(db)?;
        let result = if let Some(row) = existing {
            let mut current = map_root_row(&row)?;
            for id in &root.registered_by_agent_ids {
                if !current.registered_by_agent_ids.contains(id) {
                    current.registered_by_agent_ids.push(id.clone());
                }
            }
            if root.repo_owner.is_some() {
                current.repo_owner.clone_from(&root.repo_owner);
            }
            if root.repo_name.is_some() {
                current.repo_name.clone_from(&root.repo_name);
            }
            if current.source == WorkspaceGitRootSource::Auto
                && root.source == WorkspaceGitRootSource::Agent
            {
                current.source = WorkspaceGitRootSource::Agent;
            }
            current.updated_at.clone_from(&root.updated_at);
            sqlx::query(
                "UPDATE workspace_git_root SET registered_by_agent_ids = $3, repo_owner = $4, \
                 repo_name = $5, source = $6, updated_at = $7 WHERE tenant_id = $1 AND id = $2",
            )
            .bind(self.tenant())
            .bind(&current.id.0)
            .bind(roots::agent_ids_to_db(&current.registered_by_agent_ids)?)
            .bind(&current.repo_owner)
            .bind(&current.repo_name)
            .bind(enum_to_db(&current.source)?)
            .bind(&current.updated_at)
            .execute(&mut *tx)
            .await
            .map_err(|e| Error::Internal(format!("merge workspace git root failed: {e}")))?;
            (current, false)
        } else {
            let mut fresh = root.clone();
            let mut seen: Vec<AgentId> = Vec::new();
            fresh.registered_by_agent_ids.retain(|id| {
                if seen.contains(id) {
                    false
                } else {
                    seen.push(id.clone());
                    true
                }
            });
            let sql = format!(
                "INSERT INTO workspace_git_root (tenant_id, {columns}) \
                 VALUES ($1, $2, $3, $4, $5, $6, $7, $8, $9, $10, $11, $12, $13, $14, $15)"
            );
            sqlx::query(sqlx::AssertSqlSafe(sql.as_str()))
                .bind(self.tenant())
                .bind(&fresh.id.0)
                .bind(&fresh.workspace_id.0)
                .bind(&fresh.path)
                .bind(enum_to_db(&fresh.source)?)
                .bind(&fresh.repo_owner)
                .bind(&fresh.repo_name)
                .bind(roots::agent_ids_to_db(&fresh.registered_by_agent_ids)?)
                .bind(&fresh.registered_commit_sha)
                .bind(fresh.pr_number.map(u64::cast_signed))
                .bind(&fresh.pr_url)
                .bind(fresh.pr_status.map(|s| enum_to_db(&s)).transpose()?)
                .bind(roots::pull_requests_to_db(fresh.pull_requests.as_ref())?)
                .bind(&fresh.created_at)
                .bind(&fresh.updated_at)
                .execute(&mut *tx)
                .await
                .map_err(|e| Error::Internal(format!("workspace git root insert failed: {e}")))?;
            (fresh, true)
        };
        tx.commit().await.map_err(db)?;
        Ok(result)
    }

    async fn root_rows(&self, sql: &str, binds: &[&str]) -> Result<Vec<WorkspaceGitRoot>> {
        let mut tx = self.begin().await?;
        let mut query = sqlx::query(sqlx::AssertSqlSafe(sql)).bind(self.tenant());
        for value in binds {
            query = query.bind(*value);
        }
        let rows = query.fetch_all(&mut *tx).await.map_err(db)?;
        tx.commit().await.map_err(db)?;
        rows.iter().map(map_root_row).collect()
    }

    pub async fn get_workspace_git_root(
        &self,
        id: &WorkspaceGitRootId,
    ) -> Result<WorkspaceGitRoot> {
        let columns = roots::COLUMNS;
        let sql =
            format!("SELECT {columns} FROM workspace_git_root WHERE tenant_id = $1 AND id = $2");
        self.root_rows(&sql, &[id.0.as_str()])
            .await?
            .pop()
            .ok_or_else(|| root_not_found(id))
    }

    pub async fn find_workspace_git_root_by_path(
        &self,
        workspace_id: &WorkspaceId,
        path: &str,
    ) -> Result<Option<WorkspaceGitRoot>> {
        let columns = roots::COLUMNS;
        let sql = format!(
            "SELECT {columns} FROM workspace_git_root \
             WHERE tenant_id = $1 AND workspace_id = $2 AND path = $3"
        );
        Ok(self
            .root_rows(&sql, &[workspace_id.0.as_str(), path])
            .await?
            .pop())
    }

    pub async fn list_workspace_git_roots(
        &self,
        workspace_id: &WorkspaceId,
    ) -> Result<Vec<WorkspaceGitRoot>> {
        let columns = roots::COLUMNS;
        let sql = format!(
            "SELECT {columns} FROM workspace_git_root WHERE tenant_id = $1 AND workspace_id = $2 \
             ORDER BY created_at COLLATE \"C\", row_seq"
        );
        self.root_rows(&sql, &[workspace_id.0.as_str()]).await
    }

    pub async fn list_workspace_git_roots_with_prs(
        &self,
        include_archived: bool,
    ) -> Result<Vec<WorkspaceGitRoot>> {
        let columns = roots::COLUMNS;
        let sql = if include_archived {
            format!(
                "SELECT {columns} FROM workspace_git_root \
                 WHERE tenant_id = $1 AND pull_requests IS NOT NULL \
                 ORDER BY created_at COLLATE \"C\", row_seq"
            )
        } else {
            format!(
                "SELECT {columns} FROM workspace_git_root \
                 WHERE tenant_id = $1 AND pull_requests IS NOT NULL AND workspace_id IN \
                 (SELECT id FROM workspace WHERE tenant_id = $1 AND archived = 0) \
                 ORDER BY created_at COLLATE \"C\", row_seq"
            )
        };
        self.root_rows(&sql, &[]).await
    }

    pub async fn list_workspace_git_roots_referencing_pr_url(
        &self,
        url: &str,
    ) -> Result<Vec<WorkspaceGitRoot>> {
        let columns = roots::COLUMNS;
        let sql = format!(
            "SELECT {columns} FROM workspace_git_root r WHERE r.tenant_id = $1 \
             AND r.workspace_id IN (SELECT w.id FROM workspace w WHERE w.tenant_id = $1 \
             AND w.archived = 0 AND w.is_remote = 0) \
             AND (lower(r.pr_url) = lower($2) OR (r.pull_requests IS NOT NULL \
             AND sqlite_json_valid(r.pull_requests) AND EXISTS ( \
             SELECT 1 FROM sqlite_json_each(r.pull_requests) AS je \
             WHERE lower(sqlite_json_extract(je.value, '$.url')) = lower($2)))) \
             ORDER BY r.created_at COLLATE \"C\", r.row_seq"
        );
        self.root_rows(&sql, &[url]).await
    }

    pub async fn delete_workspace_git_root(&self, id: &WorkspaceGitRootId) -> Result<()> {
        let mut tx = self.begin_write().await?;
        let deleted =
            sqlx::query("DELETE FROM workspace_git_root WHERE tenant_id = $1 AND id = $2")
                .bind(self.tenant())
                .bind(&id.0)
                .execute(&mut *tx)
                .await
                .map_err(db)?
                .rows_affected();
        tx.commit().await.map_err(db)?;
        if deleted == 0 {
            return Err(root_not_found(id));
        }
        Ok(())
    }

    pub async fn update_workspace_git_root_pr(&self, root: &WorkspaceGitRoot) -> Result<()> {
        let mut root = root.clone();
        self.update_workspace_git_root_pr_rebased(&mut root, |_, _| {})
            .await
    }

    pub async fn update_workspace_git_root_pr_rebased<F>(
        &self,
        root: &mut WorkspaceGitRoot,
        rebase: F,
    ) -> Result<()>
    where
        F: FnOnce(&mut WorkspaceGitRoot, Option<Vec<PullRequestInfo>>),
    {
        let mut tx = self.begin_write().await?;
        let stored: Option<Option<String>> = sqlx::query_scalar(
            "SELECT pull_requests FROM workspace_git_root WHERE tenant_id = $1 AND id = $2",
        )
        .bind(self.tenant())
        .bind(&root.id.0)
        .fetch_optional(&mut *tx)
        .await
        .map_err(db)?;
        let Some(stored) = stored else {
            return Err(root_not_found(&root.id));
        };
        let persisted = stored.and_then(|s| serde_json::from_str::<Vec<PullRequestInfo>>(&s).ok());
        rebase(root, persisted);
        let updated = sqlx::query(
            "UPDATE workspace_git_root SET pr_number = $3, pr_url = $4, pr_status = $5, \
             pull_requests = $6, updated_at = $7 WHERE tenant_id = $1 AND id = $2",
        )
        .bind(self.tenant())
        .bind(&root.id.0)
        .bind(root.pr_number.map(u64::cast_signed))
        .bind(&root.pr_url)
        .bind(root.pr_status.map(|s| enum_to_db(&s)).transpose()?)
        .bind(roots::pull_requests_to_db(root.pull_requests.as_ref())?)
        .bind(&root.updated_at)
        .execute(&mut *tx)
        .await
        .map_err(|e| Error::Internal(format!("workspace git root pr write failed: {e}")))?
        .rows_affected();
        if updated == 0 {
            return Err(root_not_found(&root.id));
        }
        tx.commit().await.map_err(db)
    }

    pub async fn project_workspace_git_root_pull_requests<F>(
        &self,
        id: &WorkspaceGitRootId,
        updated_at: &str,
        project: F,
    ) -> Result<Option<Option<Vec<PullRequestInfo>>>>
    where
        F: FnOnce(&mut Option<Vec<PullRequestInfo>>) -> bool,
    {
        let mut tx = self.begin_write().await?;
        let stored: Option<Option<String>> = sqlx::query_scalar(
            "SELECT pull_requests FROM workspace_git_root WHERE tenant_id = $1 AND id = $2",
        )
        .bind(self.tenant())
        .bind(&id.0)
        .fetch_optional(&mut *tx)
        .await
        .map_err(db)?;
        let Some(stored) = stored else {
            return Err(root_not_found(id));
        };
        let mut pool = roots::pull_requests_from_db(stored)?;
        if !project(&mut pool) {
            return Ok(None);
        }
        let updated = sqlx::query(
            "UPDATE workspace_git_root SET pull_requests = $3, updated_at = $4 \
             WHERE tenant_id = $1 AND id = $2",
        )
        .bind(self.tenant())
        .bind(&id.0)
        .bind(roots::pull_requests_to_db(pool.as_ref())?)
        .bind(updated_at)
        .execute(&mut *tx)
        .await
        .map_err(db)?
        .rows_affected();
        if updated == 0 {
            return Err(root_not_found(id));
        }
        tx.commit().await.map_err(db)?;
        Ok(Some(pool))
    }

    pub async fn backfill_workspace_git_root_commit_sha(
        &self,
        id: &WorkspaceGitRootId,
        sha: &str,
        updated_at: &str,
    ) -> Result<bool> {
        let mut tx = self.begin_write().await?;
        let stamped = sqlx::query(
            "UPDATE workspace_git_root SET registered_commit_sha = $3, updated_at = $4 \
             WHERE tenant_id = $1 AND id = $2 AND registered_commit_sha IS NULL",
        )
        .bind(self.tenant())
        .bind(&id.0)
        .bind(sha)
        .bind(updated_at)
        .execute(&mut *tx)
        .await
        .map_err(db)?
        .rows_affected()
            > 0;
        let present = stamped
            || sqlx::query_scalar::<_, bool>(
                "SELECT EXISTS(SELECT 1 FROM workspace_git_root WHERE tenant_id = $1 AND id = $2)",
            )
            .bind(self.tenant())
            .bind(&id.0)
            .fetch_one(&mut *tx)
            .await
            .map_err(db)?;
        tx.commit().await.map_err(db)?;
        if !present {
            return Err(root_not_found(id));
        }
        Ok(stamped)
    }

    // ---- workspace_home_repo ---------------------------------------------------

    pub async fn set_workspace_home(
        &self,
        workspace_id: &WorkspaceId,
        home: &WorkspaceHome,
    ) -> Result<()> {
        let mut tx = self.begin_write().await?;
        sqlx::query(
            "INSERT INTO workspace_home (tenant_id, workspace_id, home_id, node_id, \
             cloud_machine_id, name, state, since, last_backup_at) \
             VALUES ($1, $2, $3, $4, $5, $6, $7, $8, $9) \
             ON CONFLICT (tenant_id, workspace_id) DO UPDATE SET home_id = excluded.home_id, \
             node_id = excluded.node_id, cloud_machine_id = excluded.cloud_machine_id, \
             name = excluded.name, state = excluded.state, since = excluded.since, \
             last_backup_at = excluded.last_backup_at, \
             backup_failing_since = CASE WHEN workspace_home.home_id = excluded.home_id \
             THEN workspace_home.backup_failing_since END, \
             auto_start_lease_id = CASE WHEN workspace_home.home_id = excluded.home_id \
             THEN workspace_home.auto_start_lease_id END, \
             auto_start_seq = CASE WHEN workspace_home.home_id = excluded.home_id \
             THEN workspace_home.auto_start_seq END",
        )
        .bind(self.tenant())
        .bind(&workspace_id.0)
        .bind(&home.home_id.0)
        .bind(&home.node_id)
        .bind(&home.cloud_machine_id)
        .bind(&home.name)
        .bind(home.state.as_str())
        .bind(&home.since)
        .bind(&home.last_backup_at)
        .execute(&mut *tx)
        .await
        .map_err(db)?;
        tx.commit().await.map_err(db)
    }

    async fn home_rows(
        &self,
        sql: &str,
        bind: Option<&str>,
    ) -> Result<Vec<(WorkspaceId, WorkspaceHome)>> {
        let mut tx = self.begin().await?;
        let mut query = sqlx::query(sqlx::AssertSqlSafe(sql)).bind(self.tenant());
        if let Some(value) = bind {
            query = query.bind(value);
        }
        let rows = query.fetch_all(&mut *tx).await.map_err(db)?;
        tx.commit().await.map_err(db)?;
        let now = time::OffsetDateTime::now_utc();
        rows.iter().map(|row| map_home_row(row, now)).collect()
    }

    pub async fn get_workspace_home(
        &self,
        workspace_id: &WorkspaceId,
    ) -> Result<Option<WorkspaceHome>> {
        let sql = format!(
            "SELECT {HOME_COLUMNS} FROM workspace_home WHERE tenant_id = $1 AND workspace_id = $2"
        );
        Ok(self
            .home_rows(&sql, Some(&workspace_id.0))
            .await?
            .pop()
            .map(|(_, home)| home))
    }

    pub async fn list_workspace_homes(&self) -> Result<HashMap<WorkspaceId, WorkspaceHome>> {
        let sql = format!("SELECT {HOME_COLUMNS} FROM workspace_home WHERE tenant_id = $1");
        Ok(self.home_rows(&sql, None).await?.into_iter().collect())
    }

    pub async fn homes_on_machine(
        &self,
        node_id: &str,
    ) -> Result<Vec<(WorkspaceId, WorkspaceHome)>> {
        let sql = format!(
            "SELECT {HOME_COLUMNS} FROM workspace_home WHERE tenant_id = $1 AND node_id = $2 \
             ORDER BY workspace_id COLLATE \"C\""
        );
        self.home_rows(&sql, Some(node_id)).await
    }

    pub async fn set_workspace_home_state(
        &self,
        workspace_id: &WorkspaceId,
        home_id: &HomeId,
        state: HomeState,
        since: &str,
    ) -> Result<bool> {
        rows_changed(
            self.begin_write().await?,
            sqlx::query(
                "UPDATE workspace_home SET state = $4, since = $5 \
                 WHERE tenant_id = $1 AND workspace_id = $2 AND home_id = $3 AND state != $4",
            )
            .bind(self.tenant())
            .bind(&workspace_id.0)
            .bind(&home_id.0)
            .bind(state.as_str())
            .bind(since),
        )
        .await
    }

    pub async fn record_workspace_home_backup(
        &self,
        workspace_id: &WorkspaceId,
        home_id: &HomeId,
        node_id: &str,
        at: &str,
    ) -> Result<bool> {
        rows_changed(
            self.begin_write().await?,
            sqlx::query(
                "UPDATE workspace_home SET last_backup_at = $5, backup_failing_since = NULL \
                 WHERE tenant_id = $1 AND workspace_id = $2 AND home_id = $3 AND node_id = $4",
            )
            .bind(self.tenant())
            .bind(&workspace_id.0)
            .bind(&home_id.0)
            .bind(node_id)
            .bind(at),
        )
        .await
    }

    pub async fn claim_workspace_home_auto_start(
        &self,
        workspace_id: &WorkspaceId,
        home_id: &HomeId,
        node_id: &str,
        lease_id: &str,
        seq: u64,
    ) -> Result<bool> {
        let seq = i64::try_from(seq).map_err(db)?;
        rows_changed(
            self.begin_write().await?,
            sqlx::query(
                "UPDATE workspace_home SET auto_start_lease_id = $5, auto_start_seq = $6 \
                 WHERE tenant_id = $1 AND workspace_id = $2 AND home_id = $3 AND node_id = $4 \
                 AND (auto_start_lease_id IS NULL OR auto_start_lease_id != $5 \
                 OR auto_start_seq IS NULL OR auto_start_seq < $6)",
            )
            .bind(self.tenant())
            .bind(&workspace_id.0)
            .bind(&home_id.0)
            .bind(node_id)
            .bind(lease_id)
            .bind(seq),
        )
        .await
    }

    pub async fn record_workspace_home_backup_failure(
        &self,
        workspace_id: &WorkspaceId,
        home_id: &HomeId,
        at: &str,
    ) -> Result<Option<String>> {
        let mut tx = self.begin_write().await?;
        let since: Option<Option<String>> = sqlx::query_scalar(
            "UPDATE workspace_home SET backup_failing_since = COALESCE(backup_failing_since, $4) \
             WHERE tenant_id = $1 AND workspace_id = $2 AND home_id = $3 \
             RETURNING backup_failing_since",
        )
        .bind(self.tenant())
        .bind(&workspace_id.0)
        .bind(&home_id.0)
        .bind(at)
        .fetch_optional(&mut *tx)
        .await
        .map_err(db)?;
        tx.commit().await.map_err(db)?;
        Ok(since.flatten())
    }

    pub async fn workspace_home_backup_failing_since(
        &self,
        workspace_id: &WorkspaceId,
        home_id: &HomeId,
    ) -> Result<Option<String>> {
        let mut tx = self.begin().await?;
        let since: Option<Option<String>> = sqlx::query_scalar(
            "SELECT backup_failing_since FROM workspace_home \
             WHERE tenant_id = $1 AND workspace_id = $2 AND home_id = $3",
        )
        .bind(self.tenant())
        .bind(&workspace_id.0)
        .bind(&home_id.0)
        .fetch_optional(&mut *tx)
        .await
        .map_err(db)?;
        tx.commit().await.map_err(db)?;
        Ok(since.flatten())
    }

    pub async fn move_workspace_home_state(
        &self,
        workspace_id: &WorkspaceId,
        home_id: &HomeId,
        from: HomeState,
        to: HomeState,
        since: &str,
    ) -> Result<bool> {
        rows_changed(
            self.begin_write().await?,
            sqlx::query(
                "UPDATE workspace_home SET state = $5, since = $6 \
                 WHERE tenant_id = $1 AND workspace_id = $2 AND home_id = $3 AND state = $4",
            )
            .bind(self.tenant())
            .bind(&workspace_id.0)
            .bind(&home_id.0)
            .bind(from.as_str())
            .bind(to.as_str())
            .bind(since),
        )
        .await
    }

    pub async fn claim_cloud_home(
        &self,
        workspace_id: &WorkspaceId,
        home_id: &HomeId,
        cloud_machine_id: &str,
        node_id: &str,
    ) -> Result<bool> {
        rows_changed(
            self.begin_write().await?,
            sqlx::query(
                "UPDATE workspace_home SET node_id = $5 \
                 WHERE tenant_id = $1 AND workspace_id = $2 AND home_id = $3 \
                 AND cloud_machine_id = $4 AND node_id IS NULL AND state != 'moving'",
            )
            .bind(self.tenant())
            .bind(&workspace_id.0)
            .bind(&home_id.0)
            .bind(cloud_machine_id)
            .bind(node_id),
        )
        .await
    }

    pub async fn mark_cloud_home_starting(
        &self,
        workspace_id: &WorkspaceId,
        home_id: &HomeId,
        since: &str,
    ) -> Result<bool> {
        rows_changed(
            self.begin_write().await?,
            sqlx::query(
                "UPDATE workspace_home SET state = 'starting', since = $4 \
                 WHERE tenant_id = $1 AND workspace_id = $2 AND home_id = $3 \
                 AND cloud_machine_id IS NOT NULL \
                 AND (state = 'detached' OR (state = 'attached' AND node_id IS NULL))",
            )
            .bind(self.tenant())
            .bind(&workspace_id.0)
            .bind(&home_id.0)
            .bind(since),
        )
        .await
    }

    pub async fn reset_unstarted_cloud_homes(&self, since: &str) -> Result<Vec<WorkspaceId>> {
        let mut tx = self.begin_write().await?;
        let ids: Vec<String> = sqlx::query_scalar(
            "UPDATE workspace_home SET state = 'attached', since = $2 \
             WHERE tenant_id = $1 AND state = 'starting' AND node_id IS NULL \
             AND cloud_machine_id IS NOT NULL RETURNING workspace_id",
        )
        .bind(self.tenant())
        .bind(since)
        .fetch_all(&mut *tx)
        .await
        .map_err(db)?;
        tx.commit().await.map_err(db)?;
        Ok(ids.into_iter().map(WorkspaceId).collect())
    }

    pub async fn detach_cloud_home(
        &self,
        workspace_id: &WorkspaceId,
        home_id: &HomeId,
        node_id: Option<&str>,
        since: &str,
    ) -> Result<bool> {
        rows_changed(
            self.begin_write().await?,
            sqlx::query(
                "UPDATE workspace_home SET node_id = NULL, state = 'detached', since = $5 \
                 WHERE tenant_id = $1 AND workspace_id = $2 AND home_id = $3 \
                 AND cloud_machine_id IS NOT NULL AND state IN ('attached', 'starting') \
                 AND (node_id IS NULL OR node_id = $4)",
            )
            .bind(self.tenant())
            .bind(&workspace_id.0)
            .bind(&home_id.0)
            .bind(node_id)
            .bind(since),
        )
        .await
    }
}

/// Run one conditional write in `tx` and commit; whether a row changed.
async fn rows_changed(
    mut tx: super::WriteTx,
    query: sqlx::query::Query<'_, sqlx::Postgres, sqlx::postgres::PgArguments>,
) -> Result<bool> {
    let changed = query.execute(&mut *tx).await.map_err(db)?.rows_affected() > 0;
    tx.commit().await.map_err(db)?;
    Ok(changed)
}
