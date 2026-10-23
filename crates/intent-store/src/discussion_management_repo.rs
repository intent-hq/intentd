//! Workspace-wide discussion metadata, with permissions checked inside the write transaction.
use crate::Store;
use intent_core::{discussion::DiscussionConversation, Error, PrincipalId, Result, WorkspaceId};
use sqlx::Row;

impl Store {
    /// Rename or delete a workspace-wide discussion. Private history is never widened.
    /// # Errors
    /// Returns permission, validation, missing discussion, or database errors.
    pub async fn manage_discussion(
        &self,
        ws: &WorkspaceId,
        principal: &PrincipalId,
        id: &str,
        name: Option<&str>,
    ) -> Result<DiscussionConversation> {
        let mut tx = self.write_pool().begin().await.map_err(|e| db(&e))?;
        let sql = format!("SELECT p.is_primary, (SELECT role FROM workspace_member WHERE workspace_id=?1 AND principal_id=p.id) AS role FROM principal p JOIN workspace w ON w.id=?1 WHERE p.id=?2 AND {}", super::discussion_conversation_repo::ELIGIBLE);
        let row = sqlx::query(&sql)
            .bind(ws.as_str())
            .bind(principal.as_str())
            .bind(ws.as_str())
            .bind(ws.as_str())
            .fetch_optional(&mut *tx)
            .await
            .map_err(|e| db(&e))?
            .ok_or_else(|| Error::NotFound("Workspace not found".into()))?;
        let role: Option<String> = row.get("role");
        let owner = row.get::<bool, _>("is_primary") || role.as_deref() == Some("owner");
        if !(owner || name.is_some() && role.as_deref() == Some("manager")) {
            return Err(Error::Forbidden(
                "discussion management requires the workspace owner, or a manager for rename"
                    .into(),
            ));
        }
        let name = name.map(str::trim);
        if name.is_some_and(|n| n.is_empty() || n.len() > 128) {
            return Err(Error::InvalidParams(
                "name must be nonblank and at most 128 UTF-8 bytes".into(),
            ));
        }
        let mut conversation =
            super::discussion_conversation_repo::read_conversation(&mut tx, ws, id).await?;
        if conversation.kind != "workspace" {
            return Err(Error::Forbidden(
                "legacy private discussions cannot be managed".into(),
            ));
        }
        conversation.name = name.map(str::to_owned);
        sqlx::query("INSERT INTO workspace_discussion_conversation(workspace_id,id,name,created_at,deleted_at) VALUES(?,?,?,?,?) ON CONFLICT(workspace_id,id) DO UPDATE SET name=excluded.name,deleted_at=excluded.deleted_at")
            .bind(ws.as_str()).bind(&conversation.id).bind(&conversation.name).bind(&conversation.created_at).bind(if name.is_none() {Some(intent_core::now_iso())} else {None}).execute(&mut *tx).await.map_err(|e| db(&e))?;
        if name.is_none() {
            sqlx::query("DELETE FROM workspace_discussion_message WHERE workspace_id=? AND conversation_id=?").bind(ws.as_str()).bind(&conversation.id).execute(&mut *tx).await.map_err(|e| db(&e))?;
            sqlx::query(
                "DELETE FROM discussion_agent_grant WHERE workspace_id=? AND conversation_id=?",
            )
            .bind(ws.as_str())
            .bind(&conversation.id)
            .execute(&mut *tx)
            .await
            .map_err(|e| db(&e))?;
        }
        tx.commit().await.map_err(|e| db(&e))?;
        Ok(conversation)
    }
}
fn db(e: &sqlx::Error) -> Error {
    Error::Internal(format!("discussion management: {e}"))
}
