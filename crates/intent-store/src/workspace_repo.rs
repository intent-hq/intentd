//! Workspace repository: insert + list, mapping rows ↔ [`Workspace`] (§9.2).

use intent_core::{
    now_iso, Error, PullRequestInfo, Result, SetupScript, TokenUsage, Workspace, WorkspaceActivity,
    WorkspaceAttention, WorkspaceId, WorkspaceStatus, CHIEF_WORKSPACE_ID,
};
use sqlx::sqlite::SqliteRow;
use sqlx::Row;

use crate::{enum_from_db, enum_to_db, tags_from_db, tags_to_db, Store};

const WORKSPACE_COLUMNS: &str = "id, title, branch, base_ref, base_commit_sha, status, \
    status_message, attention, path, repository_path, repository_owner, repository_name, \
    worktree_path, scope, skip_worktree, is_remote, default_model, pr_number, pr_url, pr_status, \
    active_pull_request, pull_requests, archived, archived_at, tags, created_at, updated_at, \
    last_activity, token_usage, setup_script";

impl Store {
    /// Insert a workspace row. `activity` is derived and never persisted (§9.9).
    pub async fn insert_workspace(&self, ws: &Workspace) -> Result<()> {
        let sql = format!(
            "INSERT INTO workspace ({WORKSPACE_COLUMNS}) VALUES \
             (?,?,?,?,?,?,?,?,?,?,?,?,?,?,?,?,?,?,?,?,?,?,?,?,?,?,?,?,?,?)"
        );
        sqlx::query(&sql)
            .bind(&ws.id.0)
            .bind(&ws.title)
            .bind(&ws.branch)
            .bind(&ws.base_ref)
            .bind(&ws.base_commit_sha)
            .bind(enum_to_db(&ws.status)?)
            .bind(&ws.status_message)
            .bind(enum_to_db(&ws.attention)?)
            .bind(&ws.path)
            .bind(&ws.repository_path)
            .bind(&ws.repository_owner)
            .bind(&ws.repository_name)
            .bind(&ws.worktree_path)
            .bind(&ws.scope)
            .bind(ws.skip_worktree as i64)
            .bind(ws.is_remote as i64)
            .bind(&ws.default_model)
            .bind(ws.pr_number.map(|n| n as i64))
            .bind(&ws.pr_url)
            .bind(pr_status_to_db(ws)?)
            .bind(active_pr_to_db(ws)?)
            .bind(pull_requests_to_db(ws)?)
            .bind(ws.archived as i64)
            .bind(&ws.archived_at)
            .bind(tags_to_db(&ws.tags)?)
            .bind(&ws.created_at)
            .bind(&ws.updated_at)
            .bind(&ws.last_activity)
            .bind(token_usage_to_db(ws)?)
            .bind(setup_script_to_db(ws)?)
            .execute(self.pool())
            .await
            .map_err(|e| Error::Internal(format!("insert workspace failed: {e}")))?;
        Ok(())
    }

    /// Fetch a single workspace by id, or `NotFound`.
    pub async fn get_workspace(&self, id: &WorkspaceId) -> Result<Workspace> {
        let sql = format!("SELECT {WORKSPACE_COLUMNS} FROM workspace WHERE id = ?");
        let row = sqlx::query(&sql)
            .bind(&id.0)
            .fetch_optional(self.pool())
            .await
            .map_err(|e| Error::Internal(format!("get workspace failed: {e}")))?;
        match row {
            Some(r) => map_workspace_row(&r),
            None => Err(Error::NotFound(format!("workspace {id}"))),
        }
    }

    /// Update an existing workspace (full row replace, except `id`), or
    /// `NotFound`. `activity` is derived and never persisted (§9.9).
    pub async fn update_workspace(&self, ws: &Workspace) -> Result<()> {
        let res = sqlx::query(
            "UPDATE workspace SET title=?, branch=?, base_ref=?, base_commit_sha=?, status=?, \
             status_message=?, attention=?, path=?, repository_path=?, repository_owner=?, \
             repository_name=?, worktree_path=?, scope=?, skip_worktree=?, is_remote=?, \
             default_model=?, pr_number=?, pr_url=?, pr_status=?, active_pull_request=?, \
             pull_requests=?, archived=?, archived_at=?, tags=?, created_at=?, updated_at=?, \
             last_activity=?, token_usage=?, setup_script=? \
             WHERE id=?",
        )
        .bind(&ws.title)
        .bind(&ws.branch)
        .bind(&ws.base_ref)
        .bind(&ws.base_commit_sha)
        .bind(enum_to_db(&ws.status)?)
        .bind(&ws.status_message)
        .bind(enum_to_db(&ws.attention)?)
        .bind(&ws.path)
        .bind(&ws.repository_path)
        .bind(&ws.repository_owner)
        .bind(&ws.repository_name)
        .bind(&ws.worktree_path)
        .bind(&ws.scope)
        .bind(ws.skip_worktree as i64)
        .bind(ws.is_remote as i64)
        .bind(&ws.default_model)
        .bind(ws.pr_number.map(|n| n as i64))
        .bind(&ws.pr_url)
        .bind(pr_status_to_db(ws)?)
        .bind(active_pr_to_db(ws)?)
        .bind(pull_requests_to_db(ws)?)
        .bind(ws.archived as i64)
        .bind(&ws.archived_at)
        .bind(tags_to_db(&ws.tags)?)
        .bind(&ws.created_at)
        .bind(&ws.updated_at)
        .bind(&ws.last_activity)
        .bind(token_usage_to_db(ws)?)
        .bind(setup_script_to_db(ws)?)
        .bind(&ws.id.0)
        .execute(self.pool())
        .await
        .map_err(|e| Error::Internal(format!("update workspace failed: {e}")))?;
        if res.rows_affected() == 0 {
            return Err(Error::NotFound(format!("workspace {}", ws.id)));
        }
        Ok(())
    }

    /// Delete a workspace by id, or `NotFound`. Records a tombstone in
    /// `deleted_workspace_id` (same transaction as the row delete) so
    /// `workspace.create` never recycles the id for a later workspace (FE
    /// `recentlyDeletedWorkspaces` parity, persisted across restarts).
    pub async fn delete_workspace(&self, id: &WorkspaceId) -> Result<()> {
        let mut tx = self
            .pool()
            .begin()
            .await
            .map_err(|e| Error::Internal(format!("delete workspace tx failed: {e}")))?;
        let res = sqlx::query("DELETE FROM workspace WHERE id = ?")
            .bind(&id.0)
            .execute(&mut *tx)
            .await
            .map_err(|e| Error::Internal(format!("delete workspace failed: {e}")))?;
        if res.rows_affected() == 0 {
            return Err(Error::NotFound(format!("workspace {id}")));
        }
        sqlx::query("INSERT OR REPLACE INTO deleted_workspace_id (id, deleted_at) VALUES (?, ?)")
            .bind(&id.0)
            .bind(now_iso())
            .execute(&mut *tx)
            .await
            .map_err(|e| Error::Internal(format!("record deleted workspace id failed: {e}")))?;
        tx.commit()
            .await
            .map_err(|e| Error::Internal(format!("delete workspace commit failed: {e}")))?;
        Ok(())
    }

    /// Whether a workspace id was ever used — a live row exists **or** a
    /// delete tombstone is recorded. `workspace.create` uses this to uniquify
    /// derived slug ids so a deleted workspace's id is never recycled.
    pub async fn workspace_id_ever_used(&self, id: &WorkspaceId) -> Result<bool> {
        let row = sqlx::query(
            "SELECT EXISTS(SELECT 1 FROM workspace WHERE id = ?) \
             OR EXISTS(SELECT 1 FROM deleted_workspace_id WHERE id = ?) AS used",
        )
        .bind(&id.0)
        .bind(&id.0)
        .fetch_one(self.pool())
        .await
        .map_err(|e| Error::Internal(format!("workspace id lookup failed: {e}")))?;
        Ok(col::<i64>(&row, "used")? != 0)
    }

    /// Record whether the workspace's branch was auto-generated by the daemon
    /// at create time (vs supplied by the caller). Read back by the
    /// `workspace.delete` cleanup guard: only an auto-generated branch is ever
    /// deleted with the worktree (TS `removeGitWorktree` parity). Store-only —
    /// the flag never appears on the wire, so it lives outside [`Workspace`].
    pub async fn set_workspace_branch_auto_generated(
        &self,
        id: &WorkspaceId,
        auto_generated: bool,
    ) -> Result<()> {
        let res = sqlx::query("UPDATE workspace SET branch_auto_generated = ? WHERE id = ?")
            .bind(auto_generated as i64)
            .bind(&id.0)
            .execute(self.pool())
            .await
            .map_err(|e| Error::Internal(format!("set branch_auto_generated failed: {e}")))?;
        if res.rows_affected() == 0 {
            return Err(Error::NotFound(format!("workspace {id}")));
        }
        Ok(())
    }

    /// Whether the workspace's branch was auto-generated at create time.
    /// `NotFound` when the workspace does not exist.
    pub async fn workspace_branch_auto_generated(&self, id: &WorkspaceId) -> Result<bool> {
        let row = sqlx::query("SELECT branch_auto_generated FROM workspace WHERE id = ?")
            .bind(&id.0)
            .fetch_optional(self.pool())
            .await
            .map_err(|e| Error::Internal(format!("get branch_auto_generated failed: {e}")))?;
        match row {
            Some(r) => Ok(col::<i64>(&r, "branch_auto_generated")? != 0),
            None => Err(Error::NotFound(format!("workspace {id}"))),
        }
    }

    /// List workspaces, filtering archived rows unless `include_archived`.
    /// The seeded virtual [`CHIEF_WORKSPACE_ID`] row is always excluded — Chief
    /// is synthesized on read by the service layer and never surfaces via
    /// `workspace.list` (TS `findAll` parity, `workspace.repository.ts`).
    pub async fn list_workspaces(&self, include_archived: bool) -> Result<Vec<Workspace>> {
        let sql = if include_archived {
            format!("SELECT {WORKSPACE_COLUMNS} FROM workspace WHERE id <> ? ORDER BY created_at")
        } else {
            format!(
                "SELECT {WORKSPACE_COLUMNS} FROM workspace WHERE id <> ? AND archived = 0 ORDER BY created_at"
            )
        };
        let rows = sqlx::query(&sql)
            .bind(CHIEF_WORKSPACE_ID)
            .fetch_all(self.pool())
            .await
            .map_err(|e| Error::Internal(format!("list workspaces failed: {e}")))?;
        rows.iter().map(map_workspace_row).collect()
    }
}

fn col<'r, T>(row: &'r SqliteRow, name: &str) -> Result<T>
where
    T: sqlx::Decode<'r, sqlx::Sqlite> + sqlx::Type<sqlx::Sqlite>,
{
    row.try_get::<T, _>(name)
        .map_err(|e| Error::Internal(format!("column {name}: {e}")))
}

/// Encode the optional `pr_status` enum to its PascalCase DB word, or `None`.
fn pr_status_to_db(ws: &Workspace) -> Result<Option<String>> {
    ws.pr_status.map(|s| enum_to_db(&s)).transpose()
}

/// Encode the optional `active_pull_request` snapshot to a JSON TEXT column.
fn active_pr_to_db(ws: &Workspace) -> Result<Option<String>> {
    ws.active_pull_request
        .as_ref()
        .map(|pr| {
            serde_json::to_string(pr)
                .map_err(|e| Error::Internal(format!("encode active_pull_request failed: {e}")))
        })
        .transpose()
}

/// Decode the optional `active_pull_request` JSON TEXT column.
fn active_pr_from_db(s: Option<String>) -> Result<Option<PullRequestInfo>> {
    s.map(|json| {
        serde_json::from_str::<PullRequestInfo>(&json)
            .map_err(|e| Error::Internal(format!("decode active_pull_request failed: {e}")))
    })
    .transpose()
}

/// Encode the optional `pull_requests` snapshot list to a JSON TEXT column.
fn pull_requests_to_db(ws: &Workspace) -> Result<Option<String>> {
    ws.pull_requests
        .as_ref()
        .map(|prs| {
            serde_json::to_string(prs)
                .map_err(|e| Error::Internal(format!("encode pull_requests failed: {e}")))
        })
        .transpose()
}

/// Decode the optional `pull_requests` JSON TEXT column.
fn pull_requests_from_db(s: Option<String>) -> Result<Option<Vec<PullRequestInfo>>> {
    s.map(|json| {
        serde_json::from_str::<Vec<PullRequestInfo>>(&json)
            .map_err(|e| Error::Internal(format!("decode pull_requests failed: {e}")))
    })
    .transpose()
}

/// Encode the optional `token_usage` snapshot to a JSON TEXT column (§5.23).
fn token_usage_to_db(ws: &Workspace) -> Result<Option<String>> {
    ws.token_usage
        .as_ref()
        .map(|tu| {
            serde_json::to_string(tu)
                .map_err(|e| Error::Internal(format!("encode token_usage failed: {e}")))
        })
        .transpose()
}

/// Decode the optional `token_usage` JSON TEXT column (§5.23).
fn token_usage_from_db(s: Option<String>) -> Result<Option<TokenUsage>> {
    s.map(|json| {
        serde_json::from_str::<TokenUsage>(&json)
            .map_err(|e| Error::Internal(format!("decode token_usage failed: {e}")))
    })
    .transpose()
}

/// Encode the optional `setup_script` record to a JSON TEXT column (§5.25).
fn setup_script_to_db(ws: &Workspace) -> Result<Option<String>> {
    ws.setup_script
        .as_ref()
        .map(|s| {
            serde_json::to_string(s)
                .map_err(|e| Error::Internal(format!("encode setup_script failed: {e}")))
        })
        .transpose()
}

/// Decode the optional `setup_script` JSON TEXT column (§5.25).
fn setup_script_from_db(s: Option<String>) -> Result<Option<SetupScript>> {
    s.map(|json| {
        serde_json::from_str::<SetupScript>(&json)
            .map_err(|e| Error::Internal(format!("decode setup_script failed: {e}")))
    })
    .transpose()
}

fn map_workspace_row(row: &SqliteRow) -> Result<Workspace> {
    let pr_number: Option<i64> = col(row, "pr_number")?;
    let pr_status = col::<Option<String>>(row, "pr_status")?
        .map(|s| enum_from_db::<intent_core::PullRequestStatus>(&s))
        .transpose()?;
    let active_pull_request =
        active_pr_from_db(col::<Option<String>>(row, "active_pull_request")?)?;
    let pull_requests = pull_requests_from_db(col::<Option<String>>(row, "pull_requests")?)?;
    let token_usage = token_usage_from_db(col::<Option<String>>(row, "token_usage")?)?;
    let setup_script = setup_script_from_db(col::<Option<String>>(row, "setup_script")?)?;
    Ok(Workspace {
        id: WorkspaceId(col(row, "id")?),
        title: col(row, "title")?,
        branch: col(row, "branch")?,
        base_ref: col(row, "base_ref")?,
        base_commit_sha: col(row, "base_commit_sha")?,
        status: enum_from_db::<WorkspaceStatus>(&col::<String>(row, "status")?)?,
        status_message: col(row, "status_message")?,
        // Derived, read-only; never persisted (§9.9).
        activity: WorkspaceActivity::Idle,
        attention: enum_from_db::<WorkspaceAttention>(&col::<String>(row, "attention")?)?,
        created_at: col(row, "created_at")?,
        updated_at: col(row, "updated_at")?,
        last_activity: col(row, "last_activity")?,
        tags: tags_from_db(&col::<String>(row, "tags")?)?,
        path: col(row, "path")?,
        repository_path: col(row, "repository_path")?,
        repository_owner: col(row, "repository_owner")?,
        repository_name: col(row, "repository_name")?,
        worktree_path: col(row, "worktree_path")?,
        scope: col(row, "scope")?,
        skip_worktree: col::<i64>(row, "skip_worktree")? != 0,
        setup_script,
        is_remote: col::<i64>(row, "is_remote")? != 0,
        default_model: col(row, "default_model")?,
        pr_number: pr_number.map(|n| n as u64),
        pr_url: col(row, "pr_url")?,
        pr_status,
        active_pull_request,
        pull_requests,
        archived: col::<i64>(row, "archived")? != 0,
        archived_at: col(row, "archived_at")?,
        // Card aggregates are computed on the workspace.list/get emit path
        // (intent-services), never persisted.
        task_stats: None,
        agent_summary: None,
        diff_summary: None,
        token_usage,
    })
}
