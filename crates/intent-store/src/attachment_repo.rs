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

/// One `attachment_idempotency_keys` row (PROTOCOL §5.9 "Idempotent
/// placement"; intent-hq/intent#4691): the client-minted `key`, scoped to
/// `workspace_id`, bound to the attachment it placed. `fingerprint` is the
/// payload identity the key was bound to; `created_at` starts the 7-day
/// retention clock.
#[derive(Debug, Clone, PartialEq, Serialize, Deserialize)]
#[serde(rename_all = "camelCase")]
pub struct AttachmentIdempotencyBinding {
    pub workspace_id: WorkspaceId,
    pub key: String,
    pub attachment_id: String,
    pub fingerprint: String,
    pub created_at: String,
}

const COLUMNS: &str = "id, workspace_id, file_name, mime_type, size, uploaded_at, stored_path";

impl Store {
    /// Insert an attachment-registry row AND its idempotency-key binding in
    /// ONE write transaction, so the binding is exactly as durable as the
    /// placement. The binding's `created_at` is the record's `uploaded_at`.
    /// An EXPIRED binding of the same `(workspace_id, key)` — created
    /// at/before `expired_before`, the same cutoff the caller's lookup used —
    /// is replaced inside the transaction, so a binding that crosses the
    /// retention boundary between the sweep and the lookup (or survives a
    /// failed sweep) still yields the documented fresh placement rather
    /// than a spurious "already bound"; the previous attachment row is left
    /// untouched. A LIVE `(workspace_id, key)` binding is
    /// `Error::InvalidParams` — the caller is expected to look the key up
    /// first; the primary key is the last line of defence against a double
    /// insert.
    ///
    /// # Errors
    ///
    /// Returns `Error::InvalidParams` when the key is already bound (live)
    /// in the workspace; `Error::Internal` if the database operation fails.
    pub async fn insert_attachment_with_idempotency_key(
        &self,
        a: &AttachmentRecord,
        key: &str,
        fingerprint: &str,
        expired_before: &str,
    ) -> Result<()> {
        let pool = self.write_pool();
        let sql = format!("INSERT INTO attachments ({COLUMNS}) VALUES (?,?,?,?,?,?,?)");
        crate::with_write_txn_retry(|| async {
            let mut tx = pool.begin().await.map_err(|e| {
                Error::Internal(format!("insert attachment (keyed) begin failed: {e}"))
            })?;
            sqlx::query(
                "DELETE FROM attachment_idempotency_keys \
                 WHERE workspace_id = ? AND key = ? AND created_at <= ?",
            )
            .bind(&a.workspace_id.0)
            .bind(key)
            .bind(expired_before)
            .execute(&mut *tx)
            .await
            .map_err(|e| {
                Error::Internal(format!(
                    "replace expired attachment idempotency key failed: {e}"
                ))
            })?;
            sqlx::query(&sql)
                .bind(&a.id)
                .bind(&a.workspace_id.0)
                .bind(&a.file_name)
                .bind(&a.mime_type)
                .bind(a.size)
                .bind(&a.uploaded_at)
                .bind(&a.stored_path)
                .execute(&mut *tx)
                .await
                .map_err(|e| Error::Internal(format!("insert attachment failed: {e}")))?;
            // The binding's FK needs the row above; a duplicate key rolls
            // the whole transaction back, so the row never lands alone.
            sqlx::query(
                "INSERT INTO attachment_idempotency_keys \
                 (workspace_id, key, attachment_id, fingerprint, created_at) \
                 VALUES (?,?,?,?,?)",
            )
            .bind(&a.workspace_id.0)
            .bind(key)
            .bind(&a.id)
            .bind(fingerprint)
            .bind(&a.uploaded_at)
            .execute(&mut *tx)
            .await
            .map_err(|e| {
                if e.as_database_error()
                    .is_some_and(sqlx::error::DatabaseError::is_unique_violation)
                {
                    Error::InvalidParams(format!(
                        "idempotencyKey {key:?} is already bound in this workspace"
                    ))
                } else {
                    Error::Internal(format!("insert attachment idempotency key failed: {e}"))
                }
            })?;
            tx.commit().await.map_err(|e| {
                Error::Internal(format!("insert attachment (keyed) commit failed: {e}"))
            })?;
            Ok(())
        })
        .await
    }

    /// Resolve a live idempotency-key binding to its attachment row. `None`
    /// when the key is unbound in the workspace OR its binding was created
    /// at/before `expired_before` (an ISO-8601 UTC cutoff — the retention
    /// boundary; expired rows read as unknown even before the sweep removes
    /// them).
    ///
    /// # Errors
    ///
    /// Returns `Error::Internal` if the database operation fails.
    pub async fn get_attachment_by_idempotency_key(
        &self,
        workspace_id: &WorkspaceId,
        key: &str,
        expired_before: &str,
    ) -> Result<Option<(AttachmentIdempotencyBinding, AttachmentRecord)>> {
        let sql = format!(
            "SELECT k.workspace_id AS k_workspace_id, k.key AS k_key, \
             k.attachment_id AS k_attachment_id, k.fingerprint AS k_fingerprint, \
             k.created_at AS k_created_at, {} \
             FROM attachment_idempotency_keys k \
             JOIN attachments a ON a.id = k.attachment_id \
             WHERE k.workspace_id = ? AND k.key = ? AND k.created_at > ?",
            COLUMNS
                .split(", ")
                .map(|c| format!("a.{c}"))
                .collect::<Vec<_>>()
                .join(", ")
        );
        let row = sqlx::query(&sql)
            .bind(&workspace_id.0)
            .bind(key)
            .bind(expired_before)
            .fetch_optional(self.read_pool())
            .await
            .map_err(|e| {
                Error::Internal(format!("get attachment by idempotency key failed: {e}"))
            })?;
        Ok(row.map(|row| {
            (
                AttachmentIdempotencyBinding {
                    workspace_id: WorkspaceId(row.get("k_workspace_id")),
                    key: row.get("k_key"),
                    attachment_id: row.get("k_attachment_id"),
                    fingerprint: row.get("k_fingerprint"),
                    created_at: row.get("k_created_at"),
                },
                row_to_record(&row),
            )
        }))
    }

    /// Retention sweep: delete every idempotency-key binding created
    /// at/before `expired_before`. The referenced `attachments` rows are
    /// untouched. Returns the number of bindings removed.
    ///
    /// # Errors
    ///
    /// Returns `Error::Internal` if the database operation fails.
    pub async fn sweep_expired_attachment_idempotency_keys(
        &self,
        expired_before: &str,
    ) -> Result<u64> {
        let done = sqlx::query("DELETE FROM attachment_idempotency_keys WHERE created_at <= ?")
            .bind(expired_before)
            .execute(self.write_pool())
            .await
            .map_err(|e| {
                Error::Internal(format!("sweep attachment idempotency keys failed: {e}"))
            })?;
        Ok(done.rows_affected())
    }

    /// Insert an attachment-registry row.
    ///
    /// # Errors
    ///
    /// Returns `Error::Internal` if the database operation fails.
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
    ///
    /// # Errors
    ///
    /// Returns `Error::NotFound` if the attachment does not exist; `Error::Internal` if the database operation fails.
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
    ///
    /// # Errors
    ///
    /// Returns `Error::Internal` if the database operation fails.
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
