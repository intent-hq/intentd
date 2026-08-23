//! Agent-flipped completion records: one row per (agent, workspace, task
//! note) recording that the agent transitioned that OTHER task note into
//! `complete` (`task.updateNoteStatus` / `task.markAsTask` with a
//! `caller_agent_id`). Wake composition later attributes these as
//! unblocked-hint triggers when the agent settles. Deduped by primary key,
//! capped per agent (oldest evicted on insert), removed when the task
//! transitions back out of `complete`, and cascaded with the recording
//! agent's session.

use intent_core::{AgentId, Error, NoteId, Result, WorkspaceId};
use sqlx::Row;

use crate::Store;

/// Per-agent cap on recorded flipped completions. Bounds the trigger set a
/// wake can carry; the oldest rows are evicted on insert.
pub const AGENT_FLIPPED_COMPLETIONS_CAP: i64 = 50;

impl Store {
    /// Record that `agent_id` flipped `task_note_id` into `complete`.
    /// Re-recording the same pair refreshes `recorded_at` (dedup by primary
    /// key); rows beyond [`AGENT_FLIPPED_COMPLETIONS_CAP`] are evicted
    /// oldest-first.
    ///
    /// # Errors
    ///
    /// Returns `Error::Internal` if the database operation fails.
    pub async fn record_agent_flipped_completion(
        &self,
        agent_id: &AgentId,
        workspace_id: &WorkspaceId,
        task_note_id: &NoteId,
        recorded_at: &str,
    ) -> Result<()> {
        sqlx::query(
            "INSERT INTO agent_flipped_completion (
                agent_id, workspace_id, task_note_id, recorded_at
            ) VALUES (?,?,?,?)
            ON CONFLICT(agent_id, workspace_id, task_note_id) DO UPDATE SET
                recorded_at = excluded.recorded_at",
        )
        .bind(&agent_id.0)
        .bind(&workspace_id.0)
        .bind(&task_note_id.0)
        .bind(recorded_at)
        .execute(self.write_pool())
        .await
        .map_err(|e| Error::Internal(format!("record agent_flipped_completion failed: {e}")))?;
        sqlx::query(
            "DELETE FROM agent_flipped_completion \
             WHERE agent_id = ? AND rowid NOT IN (\
                SELECT rowid FROM agent_flipped_completion \
                 WHERE agent_id = ? \
                 ORDER BY recorded_at DESC, rowid DESC \
                 LIMIT ?)",
        )
        .bind(&agent_id.0)
        .bind(&agent_id.0)
        .bind(AGENT_FLIPPED_COMPLETIONS_CAP)
        .execute(self.write_pool())
        .await
        .map_err(|e| Error::Internal(format!("cap agent_flipped_completion failed: {e}")))?;
        Ok(())
    }

    /// Remove every agent's recorded flip of `task_note_id` — called when the
    /// task transitions back out of `complete`, which stales the flip
    /// regardless of who reverted it.
    ///
    /// # Errors
    ///
    /// Returns `Error::Internal` if the database operation fails.
    pub async fn remove_agent_flipped_completions_for_task(
        &self,
        workspace_id: &WorkspaceId,
        task_note_id: &NoteId,
    ) -> Result<()> {
        sqlx::query(
            "DELETE FROM agent_flipped_completion \
             WHERE workspace_id = ? AND task_note_id = ?",
        )
        .bind(&workspace_id.0)
        .bind(&task_note_id.0)
        .execute(self.write_pool())
        .await
        .map_err(|e| Error::Internal(format!("remove agent_flipped_completion failed: {e}")))?;
        Ok(())
    }

    /// Take (list and clear) the agent's recorded flipped completions,
    /// oldest-first — the consume-on-stamp read used at wake composition,
    /// so a flip is attributed as a trigger at most once and can never be
    /// re-attributed by a later completion cycle. A single atomic
    /// `DELETE ... RETURNING`, so a flip recorded concurrently with the take
    /// is either returned by it or left for the next one — never silently
    /// consumed without attribution. `RETURNING` row order is unspecified,
    /// so the oldest-first ordering is applied after the fetch.
    ///
    /// # Errors
    ///
    /// Returns `Error::Internal` if the database operation fails.
    pub async fn take_agent_flipped_completions(
        &self,
        agent_id: &AgentId,
    ) -> Result<Vec<(WorkspaceId, NoteId)>> {
        let rows = sqlx::query(
            "DELETE FROM agent_flipped_completion WHERE agent_id = ? \
             RETURNING workspace_id, task_note_id, recorded_at, rowid",
        )
        .bind(&agent_id.0)
        .fetch_all(self.write_pool())
        .await
        .map_err(|e| Error::Internal(format!("take agent_flipped_completion failed: {e}")))?;
        let mut decoded = rows
            .into_iter()
            .map(|r| {
                let ws: String = r
                    .try_get("workspace_id")
                    .map_err(|e| Error::Internal(format!("decode workspace_id: {e}")))?;
                let note: String = r
                    .try_get("task_note_id")
                    .map_err(|e| Error::Internal(format!("decode task_note_id: {e}")))?;
                let recorded_at: String = r
                    .try_get("recorded_at")
                    .map_err(|e| Error::Internal(format!("decode recorded_at: {e}")))?;
                let rowid: i64 = r
                    .try_get("rowid")
                    .map_err(|e| Error::Internal(format!("decode rowid: {e}")))?;
                Ok((
                    recorded_at,
                    rowid,
                    WorkspaceId::from(ws),
                    NoteId::from(note),
                ))
            })
            .collect::<Result<Vec<_>>>()?;
        decoded.sort_by(|a, b| (&a.0, a.1).cmp(&(&b.0, b.1)));
        Ok(decoded
            .into_iter()
            .map(|(_, _, ws, note)| (ws, note))
            .collect())
    }

    /// The agent's recorded flipped completions, oldest-first.
    ///
    /// # Errors
    ///
    /// Returns `Error::Internal` if the database operation fails.
    pub async fn list_agent_flipped_completions(
        &self,
        agent_id: &AgentId,
    ) -> Result<Vec<(WorkspaceId, NoteId)>> {
        let rows = sqlx::query(
            "SELECT workspace_id, task_note_id FROM agent_flipped_completion \
             WHERE agent_id = ? ORDER BY recorded_at ASC, rowid ASC",
        )
        .bind(&agent_id.0)
        .fetch_all(self.read_pool())
        .await
        .map_err(|e| Error::Internal(format!("list agent_flipped_completion failed: {e}")))?;
        rows.into_iter()
            .map(|r| {
                let ws: String = r
                    .try_get("workspace_id")
                    .map_err(|e| Error::Internal(format!("decode workspace_id: {e}")))?;
                let note: String = r
                    .try_get("task_note_id")
                    .map_err(|e| Error::Internal(format!("decode task_note_id: {e}")))?;
                Ok((WorkspaceId::from(ws), NoteId::from(note)))
            })
            .collect()
    }
}
