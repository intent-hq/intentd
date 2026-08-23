//! Line-attribution repository (PROTOCOL §5.2.1). Persists the most recently
//! computed `attributeLines` output for a note so `note.lineAttribution.load`
//! is O(1) across restarts. `attributions_json` stores the FE-parity map
//! `{ <lineNumber>: LineAttributionInfo }` verbatim.

use intent_core::{Error, LineAttributionData, NoteId, Result, WorkspaceId};
use sqlx::Row;

use crate::Store;

impl Store {
    /// Upsert the latest attribution snapshot for `note_id`. Replaces any
    /// prior row (attribution is a full snapshot, not a delta).
    ///
    /// # Errors
    ///
    /// Returns `Error::Internal` if the database operation fails.
    pub async fn upsert_note_line_attribution(&self, data: &LineAttributionData) -> Result<()> {
        let attributions_json = serde_json::to_string(&data.attributions).map_err(|e| {
            Error::Internal(format!(
                "encode note_line_attribution attributions failed: {e}"
            ))
        })?;
        sqlx::query(
            "INSERT INTO note_line_attribution (note_id, workspace_id, computed_at, attributions_json) \
             VALUES (?, ?, ?, ?) \
             ON CONFLICT(workspace_id, note_id) DO UPDATE SET \
               computed_at = excluded.computed_at, \
               attributions_json = excluded.attributions_json",
        )
        .bind(data.note_id.as_str())
        .bind(data.workspace_id.as_str())
        .bind(&data.computed_at)
        .bind(&attributions_json)
        .execute(self.write_pool())
        .await
        .map_err(|e| Error::Internal(format!("upsert note_line_attribution failed: {e}")))?;
        Ok(())
    }

    /// Load the persisted attribution snapshot for a note, or `None` if never
    /// computed. `workspace_id` scopes the lookup so a cross-workspace note-id
    /// collision surfaces as a miss rather than a cross-workspace leak.
    ///
    /// # Errors
    ///
    /// Returns `Error::Internal` if the database operation fails.
    pub async fn get_note_line_attribution(
        &self,
        workspace_id: &WorkspaceId,
        note_id: &NoteId,
    ) -> Result<Option<LineAttributionData>> {
        let row = sqlx::query(
            "SELECT workspace_id, computed_at, attributions_json FROM note_line_attribution \
             WHERE note_id = ? AND workspace_id = ?",
        )
        .bind(note_id.as_str())
        .bind(workspace_id.as_str())
        .fetch_optional(self.read_pool())
        .await
        .map_err(|e| Error::Internal(format!("get note_line_attribution failed: {e}")))?;
        let Some(row) = row else { return Ok(None) };
        let workspace_id: String = row
            .try_get("workspace_id")
            .map_err(|e| Error::Internal(format!("column workspace_id: {e}")))?;
        let computed_at: String = row
            .try_get("computed_at")
            .map_err(|e| Error::Internal(format!("column computed_at: {e}")))?;
        let attributions_json: String = row
            .try_get("attributions_json")
            .map_err(|e| Error::Internal(format!("column attributions_json: {e}")))?;
        let attributions = serde_json::from_str(&attributions_json).map_err(|e| {
            Error::Internal(format!(
                "decode note_line_attribution attributions failed: {e}"
            ))
        })?;
        Ok(Some(LineAttributionData {
            note_id: note_id.clone(),
            workspace_id: WorkspaceId::from(workspace_id.as_str()),
            computed_at,
            attributions,
        }))
    }
}
