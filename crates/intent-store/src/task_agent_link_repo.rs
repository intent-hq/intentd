//! Task↔agent linkage rows (§5.4 `task.linkAgent` / `unlinkAgent` /
//! `listAgentLinks`). Migrates the renderer-only
//! `localStorage["task-agent-associations:{workspaceId}"]` map into
//! daemon-owned rows keyed by `(workspace_id, note_id, task_key)`.

use intent_core::{Error, NoteId, Result, TaskAgentLink, WorkspaceId};
use sqlx::sqlite::SqliteRow;
use sqlx::Row;

use crate::Store;

const TASK_AGENT_LINK_COLUMNS: &str =
    "workspace_id, note_id, task_key, task_text, agent_id, created_at";

impl Store {
    /// Insert or replace a link (upsert on `(workspace_id, note_id,
    /// task_key)`). FE parity: `addTaskAgentAssociation` overwrites any
    /// existing entry at the same key. Returns the persisted row.
    ///
    /// # Errors
    ///
    /// Returns `Error::Internal` if the database operation fails.
    pub async fn upsert_task_agent_link(&self, link: &TaskAgentLink) -> Result<TaskAgentLink> {
        sqlx::query(&format!(
            "INSERT OR REPLACE INTO task_agent_link ({TASK_AGENT_LINK_COLUMNS}) \
             VALUES (?, ?, ?, ?, ?, ?)"
        ))
        .bind(&link.workspace_id.0)
        .bind(link.note_id.as_str())
        .bind(&link.task_key)
        .bind(&link.task_text)
        .bind(&link.agent_id)
        .bind(link.created_at)
        .execute(self.write_pool())
        .await
        .map_err(|e| Error::Internal(format!("upsert task agent link failed: {e}")))?;
        Ok(link.clone())
    }

    /// Delete a single link by its full key. Returns whether a row was
    /// actually removed; deleting an unknown key is not an error.
    ///
    /// # Errors
    ///
    /// Returns `Error::Internal` if the database operation fails.
    pub async fn delete_task_agent_link(
        &self,
        workspace_id: &WorkspaceId,
        note_id: &NoteId,
        task_key: &str,
    ) -> Result<bool> {
        let res = sqlx::query(
            "DELETE FROM task_agent_link \
             WHERE workspace_id = ? AND note_id = ? AND task_key = ?",
        )
        .bind(&workspace_id.0)
        .bind(note_id.as_str())
        .bind(task_key)
        .execute(self.write_pool())
        .await
        .map_err(|e| Error::Internal(format!("delete task agent link failed: {e}")))?;
        Ok(res.rows_affected() > 0)
    }

    /// List every link for a workspace, oldest first — hydration read for
    /// `task.listAgentLinks`.
    ///
    /// # Errors
    ///
    /// Returns `Error::Internal` if the database operation fails.
    pub async fn list_task_agent_links(
        &self,
        workspace_id: &WorkspaceId,
    ) -> Result<Vec<TaskAgentLink>> {
        let rows = sqlx::query(&format!(
            "SELECT {TASK_AGENT_LINK_COLUMNS} FROM task_agent_link \
             WHERE workspace_id = ? ORDER BY created_at, note_id, task_key"
        ))
        .bind(&workspace_id.0)
        .fetch_all(self.read_pool())
        .await
        .map_err(|e| Error::Internal(format!("list task agent links failed: {e}")))?;
        rows.iter().map(map_link_row).collect()
    }
}

fn map_link_row(r: &SqliteRow) -> Result<TaskAgentLink> {
    Ok(TaskAgentLink {
        workspace_id: WorkspaceId::from(r.get::<String, _>("workspace_id")),
        note_id: NoteId::from(r.get::<String, _>("note_id")),
        task_key: r.get("task_key"),
        task_text: r.get("task_text"),
        agent_id: r.get("agent_id"),
        created_at: r.get("created_at"),
    })
}
