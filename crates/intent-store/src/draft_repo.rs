//! Per-client chat draft repository (§9.2, §9.10, §15). Drafts are keyed by the
//! `(workspace_id, agent_id, client_id)` triple so concurrent clients never
//! clobber one another; an empty draft is represented by the row's absence (an
//! empty `drafts.set` clears it).

use intent_core::{now_iso, AgentId, ClientId, Draft, Error, Result, WorkspaceId};
use sqlx::sqlite::SqliteRow;
use sqlx::Row;

use crate::Store;

impl Store {
    /// Fetch the draft for one `(workspace, agent, client)` triple, or `None`.
    pub async fn get_draft(
        &self,
        workspace_id: &WorkspaceId,
        agent_id: &AgentId,
        client_id: &ClientId,
    ) -> Result<Option<Draft>> {
        let row = sqlx::query(
            "SELECT workspace_id, agent_id, client_id, text, updated_at FROM draft \
             WHERE workspace_id = ? AND agent_id = ? AND client_id = ?",
        )
        .bind(&workspace_id.0)
        .bind(&agent_id.0)
        .bind(&client_id.0)
        .fetch_optional(self.pool())
        .await
        .map_err(|e| Error::Internal(format!("get draft failed: {e}")))?;
        row.as_ref().map(map_draft_row).transpose()
    }

    /// Upsert the draft for one triple, refreshing `text`/`updated_at` in place
    /// on conflict. Returns the `updated_at` timestamp written.
    pub async fn upsert_draft(
        &self,
        workspace_id: &WorkspaceId,
        agent_id: &AgentId,
        client_id: &ClientId,
        text: &str,
    ) -> Result<String> {
        let now = now_iso();
        sqlx::query(
            "INSERT INTO draft (workspace_id, agent_id, client_id, text, updated_at) \
             VALUES (?,?,?,?,?) \
             ON CONFLICT(workspace_id, agent_id, client_id) DO UPDATE SET \
             text = excluded.text, updated_at = excluded.updated_at",
        )
        .bind(&workspace_id.0)
        .bind(&agent_id.0)
        .bind(&client_id.0)
        .bind(text)
        .bind(&now)
        .execute(self.pool())
        .await
        .map_err(|e| Error::Internal(format!("upsert draft failed: {e}")))?;
        Ok(now)
    }

    /// Delete the draft for one triple. Returns `true` when a row was removed
    /// (an idempotent no-op success otherwise).
    pub async fn delete_draft(
        &self,
        workspace_id: &WorkspaceId,
        agent_id: &AgentId,
        client_id: &ClientId,
    ) -> Result<bool> {
        let res = sqlx::query(
            "DELETE FROM draft WHERE workspace_id = ? AND agent_id = ? AND client_id = ?",
        )
        .bind(&workspace_id.0)
        .bind(&agent_id.0)
        .bind(&client_id.0)
        .execute(self.pool())
        .await
        .map_err(|e| Error::Internal(format!("delete draft failed: {e}")))?;
        Ok(res.rows_affected() > 0)
    }
}

fn map_draft_row(r: &SqliteRow) -> Result<Draft> {
    Ok(Draft {
        workspace_id: WorkspaceId(r.get("workspace_id")),
        agent_id: AgentId(r.get("agent_id")),
        client_id: ClientId(r.get("client_id")),
        text: r.get("text"),
        updated_at: r.get("updated_at"),
    })
}
