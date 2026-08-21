//! Diffs repository: persistent diff storage (§9.11, §17.3), independent of raw
//! git so a change's before/after + extracted hunks survive staging/commit
//! churn. INTERNAL storage only — there are no `diffs.*` wire methods. One row
//! per `(workspace_id, file_path, staged)`; full content is lazy via blob SHAs
//! on `tracked_changes`, so `old_content`/`new_content` stay NULL here.

use intent_core::{now_iso, Error, Result, WorkspaceId};
use sqlx::sqlite::SqliteRow;
use sqlx::Row;
use uuid::Uuid;

use crate::Store;

const DIFF_COLUMNS: &str = "id, workspace_id, file_path, staged, old_content, new_content, \
    hunks_json, created_at, updated_at";

/// Input to [`Store::upsert_diff`]: a diff without its id / timestamps.
#[derive(Debug, Clone)]
pub struct NewDiff {
    pub workspace_id: WorkspaceId,
    pub file_path: String,
    pub staged: bool,
    pub old_content: Option<String>,
    pub new_content: Option<String>,
    pub hunks_json: String,
}

/// A persisted `diffs` row.
#[derive(Debug, Clone, PartialEq, Eq)]
pub struct DiffRow {
    pub id: String,
    pub workspace_id: WorkspaceId,
    pub file_path: String,
    pub staged: bool,
    pub old_content: Option<String>,
    pub new_content: Option<String>,
    pub hunks_json: String,
    pub created_at: String,
    pub updated_at: String,
}

impl Store {
    /// Upsert a diff keyed by the `UNIQUE(workspace_id, file_path, staged)` index:
    /// insert with a minted `UUIDv7` id, or refresh the content/hunks of the
    /// existing row in place (its id + `created_at` are preserved).
    ///
    /// # Errors
    ///
    /// Returns `Error::Internal` if the database operation fails.
    pub async fn upsert_diff(&self, d: &NewDiff) -> Result<()> {
        let id = Uuid::now_v7().to_string();
        let now = now_iso();
        let sql = format!(
            "INSERT INTO diffs ({DIFF_COLUMNS}) VALUES (?,?,?,?,?,?,?,?,?) \
             ON CONFLICT(workspace_id, file_path, staged) DO UPDATE SET \
             old_content = excluded.old_content, new_content = excluded.new_content, \
             hunks_json = excluded.hunks_json, updated_at = excluded.updated_at"
        );
        sqlx::query(&sql)
            .bind(&id)
            .bind(&d.workspace_id.0)
            .bind(&d.file_path)
            .bind(i64::from(d.staged))
            .bind(&d.old_content)
            .bind(&d.new_content)
            .bind(&d.hunks_json)
            .bind(&now)
            .bind(&now)
            .execute(self.write_pool())
            .await
            .map_err(|e| Error::Internal(format!("upsert diff failed: {e}")))?;
        Ok(())
    }

    /// List a workspace's stored diffs, oldest first. Internal read used by the
    /// pipeline + tests (UI-facing diffs surface via file-tracking reads, M4.8).
    ///
    /// # Errors
    ///
    /// Returns `Error::Internal` if the database operation fails.
    pub async fn list_diffs(&self, workspace_id: &WorkspaceId) -> Result<Vec<DiffRow>> {
        let sql =
            format!("SELECT {DIFF_COLUMNS} FROM diffs WHERE workspace_id = ? ORDER BY created_at");
        let rows = sqlx::query(&sql)
            .bind(&workspace_id.0)
            .fetch_all(self.read_pool())
            .await
            .map_err(|e| Error::Internal(format!("list diffs failed: {e}")))?;
        rows.iter().map(map_diff_row).collect()
    }
}

#[allow(clippy::unnecessary_wraps)] // row mapper; call sites collect::<Result<_>> uniformly
fn map_diff_row(r: &SqliteRow) -> Result<DiffRow> {
    Ok(DiffRow {
        id: r.get("id"),
        workspace_id: WorkspaceId(r.get("workspace_id")),
        file_path: r.get("file_path"),
        staged: r.get::<i64, _>("staged") != 0,
        old_content: r.get("old_content"),
        new_content: r.get("new_content"),
        hunks_json: r.get("hunks_json"),
        created_at: r.get("created_at"),
        updated_at: r.get("updated_at"),
    })
}
