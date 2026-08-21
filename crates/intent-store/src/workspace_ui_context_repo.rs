//! Per-workspace UI context (§5.1 `workspace.getUiContext` /
//! `updateUiContext`). The store treats the payload as an opaque JSON blob
//! authored by the FE (`WorkspaceUIContext` in
//! `packages/cloudlands-fe/src/features/workspace/types.ts`); no
//! interpretation, no shape coercion — the daemon must round-trip the blob
//! verbatim to avoid the data-loss class of bug that killed the first
//! adoption attempt.

use intent_core::{Error, Result, WorkspaceId};
use serde_json::Value;
use sqlx::Row;

use crate::Store;

impl Store {
    /// Get the UI context blob for a workspace. Returns `None` when nothing
    /// has been stored yet (pre-first-save default).
    ///
    /// # Errors
    ///
    /// Returns `Error::Internal` if the database operation fails.
    pub async fn get_workspace_ui_context(
        &self,
        workspace_id: &WorkspaceId,
    ) -> Result<Option<Value>> {
        let row = sqlx::query("SELECT payload FROM workspace_ui_context WHERE workspace_id = ?")
            .bind(&workspace_id.0)
            .fetch_optional(self.read_pool())
            .await
            .map_err(|e| Error::Internal(format!("get workspace ui context failed: {e}")))?;

        match row {
            None => Ok(None),
            Some(r) => {
                let payload: String = r.get("payload");
                let value = serde_json::from_str(&payload)
                    .map_err(|e| Error::Internal(format!("decode ui context failed: {e}")))?;
                Ok(Some(value))
            }
        }
    }

    /// Update the UI context blob for a workspace. Upserts the row (replaces
    /// if it exists, inserts otherwise). Returns the persisted blob read back
    /// from the store so callers can verify round-trip fidelity.
    ///
    /// The payload is stored verbatim as JSON text. No shape validation, no
    /// coercion (e.g., no `null` → `[]`, no missing-field defaults). The FE
    /// owns the schema; the daemon is a dumb pipe.
    ///
    /// # Errors
    ///
    /// Returns `Error::Internal` if the database operation fails.
    pub async fn update_workspace_ui_context(
        &self,
        workspace_id: &WorkspaceId,
        ui_context: &Value,
    ) -> Result<Value> {
        let payload = serde_json::to_string(ui_context)
            .map_err(|e| Error::Internal(format!("encode ui context failed: {e}")))?;

        sqlx::query(
            "INSERT INTO workspace_ui_context (workspace_id, payload) \
             VALUES (?, ?) \
             ON CONFLICT(workspace_id) DO UPDATE SET payload = excluded.payload",
        )
        .bind(&workspace_id.0)
        .bind(&payload)
        .execute(self.write_pool())
        .await
        .map_err(|e| Error::Internal(format!("update workspace ui context failed: {e}")))?;

        // Read back to verify round-trip.
        self.get_workspace_ui_context(workspace_id)
            .await?
            .ok_or_else(|| {
                Error::Internal(
                    "ui context disappeared after insert (should never happen)".to_string(),
                )
            })
    }
}
