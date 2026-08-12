//! Attachment registry repository (PROTOCOL §5.9): UUID-keyed rows for files
//! placed by `file.placeAttachment` into `.intent/attachments/`. Rows are
//! insert-only — the file on disk may be deleted out-of-band, in which case
//! the row survives and readers report `exists: false`.

use intent_core::{Error, Result, WorkspaceId};
use serde::{Deserialize, Serialize};
use sqlx::Row;

use crate::Store;

/// One attachment-registry row. `stored_path` is workspace-relative (under
/// `.intent/attachments/`); `file_name` is the collision-safe placed name the
/// stored path ends with.
#[derive(Debug, Clone, PartialEq, Serialize, Deserialize)]
#[serde(rename_all = "camelCase")]
pub struct AttachmentRecord {
    pub id: String,
    pub workspace_id: WorkspaceId,
    pub file_name: String,
    pub mime_type: Option<String>,
    pub size: i64,
    pub uploaded_at: String,
    pub stored_path: String,
}

const COLUMNS: &str = "id, workspace_id, file_name, mime_type, size, uploaded_at, stored_path";

impl Store {
    /// Insert an attachment-registry row.
    pub async fn insert_attachment(&self, a: &AttachmentRecord) -> Result<()> {
        let sql = format!("INSERT INTO attachments ({COLUMNS}) VALUES (?,?,?,?,?,?,?)");
        sqlx::query(&sql)
            .bind(&a.id)
            .bind(&a.workspace_id.0)
            .bind(&a.file_name)
            .bind(&a.mime_type)
            .bind(a.size)
            .bind(&a.uploaded_at)
            .bind(&a.stored_path)
            .execute(self.write_pool())
            .await
            .map_err(|e| Error::Internal(format!("insert attachment failed: {e}")))?;
        Ok(())
    }

    /// Load one attachment by id. `Error::NotFound` when no row exists.
    pub async fn get_attachment(&self, id: &str) -> Result<AttachmentRecord> {
        let sql = format!("SELECT {COLUMNS} FROM attachments WHERE id = ?");
        let row = sqlx::query(&sql)
            .bind(id)
            .fetch_optional(self.read_pool())
            .await
            .map_err(|e| Error::Internal(format!("get attachment failed: {e}")))?
            .ok_or_else(|| Error::NotFound(format!("attachment {id}")))?;
        Ok(row_to_record(&row))
    }

    /// All attachment rows for one workspace, ordered by id (stable manifest
    /// ordering for the transfer pipeline).
    pub async fn list_attachments(
        &self,
        workspace_id: &WorkspaceId,
    ) -> Result<Vec<AttachmentRecord>> {
        let sql = format!("SELECT {COLUMNS} FROM attachments WHERE workspace_id = ? ORDER BY id");
        let rows = sqlx::query(&sql)
            .bind(&workspace_id.0)
            .fetch_all(self.read_pool())
            .await
            .map_err(|e| Error::Internal(format!("list attachments failed: {e}")))?;
        Ok(rows.iter().map(row_to_record).collect())
    }
}

fn row_to_record(row: &sqlx::sqlite::SqliteRow) -> AttachmentRecord {
    AttachmentRecord {
        id: row.get("id"),
        workspace_id: WorkspaceId(row.get("workspace_id")),
        file_name: row.get("file_name"),
        mime_type: row.get("mime_type"),
        size: row.get("size"),
        uploaded_at: row.get("uploaded_at"),
        stored_path: row.get("stored_path"),
    }
}
