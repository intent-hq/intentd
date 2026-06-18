//! Workspace repository: insert + list, mapping rows ↔ [`Workspace`] (§9.2).

use intent_core::{
    Error, Result, Workspace, WorkspaceActivity, WorkspaceAttention, WorkspaceId, WorkspaceStatus,
};
use sqlx::sqlite::SqliteRow;
use sqlx::Row;

use crate::{enum_from_db, enum_to_db, tags_from_db, tags_to_db, Store};

const WORKSPACE_COLUMNS: &str = "id, title, branch, base_ref, base_commit_sha, status, \
    status_message, attention, repository_owner, repository_name, worktree_path, scope, \
    skip_worktree, is_remote, default_model, pr_number, pr_url, archived, archived_at, tags, \
    created_at, updated_at, last_activity";

impl Store {
    /// Insert a workspace row. `activity` is derived and never persisted (§9.9).
    pub async fn insert_workspace(&self, ws: &Workspace) -> Result<()> {
        let sql = format!(
            "INSERT INTO workspace ({WORKSPACE_COLUMNS}) VALUES \
             (?,?,?,?,?,?,?,?,?,?,?,?,?,?,?,?,?,?,?,?,?,?,?)"
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
            .bind(&ws.repository_owner)
            .bind(&ws.repository_name)
            .bind(&ws.worktree_path)
            .bind(&ws.scope)
            .bind(ws.skip_worktree as i64)
            .bind(ws.is_remote as i64)
            .bind(&ws.default_model)
            .bind(ws.pr_number.map(|n| n as i64))
            .bind(&ws.pr_url)
            .bind(ws.archived as i64)
            .bind(&ws.archived_at)
            .bind(tags_to_db(&ws.tags)?)
            .bind(&ws.created_at)
            .bind(&ws.updated_at)
            .bind(&ws.last_activity)
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
             status_message=?, attention=?, repository_owner=?, repository_name=?, \
             worktree_path=?, scope=?, skip_worktree=?, is_remote=?, default_model=?, \
             pr_number=?, pr_url=?, archived=?, archived_at=?, tags=?, created_at=?, \
             updated_at=?, last_activity=? WHERE id=?",
        )
        .bind(&ws.title)
        .bind(&ws.branch)
        .bind(&ws.base_ref)
        .bind(&ws.base_commit_sha)
        .bind(enum_to_db(&ws.status)?)
        .bind(&ws.status_message)
        .bind(enum_to_db(&ws.attention)?)
        .bind(&ws.repository_owner)
        .bind(&ws.repository_name)
        .bind(&ws.worktree_path)
        .bind(&ws.scope)
        .bind(ws.skip_worktree as i64)
        .bind(ws.is_remote as i64)
        .bind(&ws.default_model)
        .bind(ws.pr_number.map(|n| n as i64))
        .bind(&ws.pr_url)
        .bind(ws.archived as i64)
        .bind(&ws.archived_at)
        .bind(tags_to_db(&ws.tags)?)
        .bind(&ws.created_at)
        .bind(&ws.updated_at)
        .bind(&ws.last_activity)
        .bind(&ws.id.0)
        .execute(self.pool())
        .await
        .map_err(|e| Error::Internal(format!("update workspace failed: {e}")))?;
        if res.rows_affected() == 0 {
            return Err(Error::NotFound(format!("workspace {}", ws.id)));
        }
        Ok(())
    }

    /// Delete a workspace by id, or `NotFound`.
    pub async fn delete_workspace(&self, id: &WorkspaceId) -> Result<()> {
        let res = sqlx::query("DELETE FROM workspace WHERE id = ?")
            .bind(&id.0)
            .execute(self.pool())
            .await
            .map_err(|e| Error::Internal(format!("delete workspace failed: {e}")))?;
        if res.rows_affected() == 0 {
            return Err(Error::NotFound(format!("workspace {id}")));
        }
        Ok(())
    }

    /// List workspaces, filtering archived rows unless `include_archived`.
    pub async fn list_workspaces(&self, include_archived: bool) -> Result<Vec<Workspace>> {
        let sql = if include_archived {
            format!("SELECT {WORKSPACE_COLUMNS} FROM workspace ORDER BY created_at")
        } else {
            format!(
                "SELECT {WORKSPACE_COLUMNS} FROM workspace WHERE archived = 0 ORDER BY created_at"
            )
        };
        let rows = sqlx::query(&sql)
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

fn map_workspace_row(row: &SqliteRow) -> Result<Workspace> {
    let pr_number: Option<i64> = col(row, "pr_number")?;
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
        // Not persisted in this slice.
        path: None,
        repository_owner: col(row, "repository_owner")?,
        repository_name: col(row, "repository_name")?,
        worktree_path: col(row, "worktree_path")?,
        scope: col(row, "scope")?,
        skip_worktree: col::<i64>(row, "skip_worktree")? != 0,
        // Not persisted in this slice.
        setup_script: None,
        is_remote: col::<i64>(row, "is_remote")? != 0,
        default_model: col(row, "default_model")?,
        pr_number: pr_number.map(|n| n as u64),
        pr_url: col(row, "pr_url")?,
        archived: col::<i64>(row, "archived")? != 0,
        archived_at: col(row, "archived_at")?,
    })
}
