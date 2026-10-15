//! Bounded sequence paging and atomic per-human retry deduplication.
use intent_core::discussion::{
    DiscussionAuthor, DiscussionMessage, DiscussionPage, DISCUSSION_PAGE_MAX,
};
use intent_core::{now_iso, Error, Result, WorkspaceId};
use sqlx::{sqlite::SqliteRow, Row};

use crate::Store;

fn store_error(error: &sqlx::Error) -> Error {
    Error::Internal(format!("workspace discussion: {error}"))
}

fn message(row: &SqliteRow) -> Result<DiscussionMessage> {
    Ok(DiscussionMessage {
        id: row.get("id"),
        workspace_id: WorkspaceId(row.get("workspace_id")),
        sequence: row.get("sequence"),
        body: row.get("body"),
        author: serde_json::from_str(row.get("author_json"))
            .map_err(|e| Error::Internal(format!("discussion author: {e}")))?,
        created_at: row.get("created_at"),
    })
}

impl Store {
    /// Insert once, returning the immutable original and whether it was inserted.
    /// Sequence allocation and retry lookup share the serialized write transaction.
    ///
    /// # Errors
    /// Returns a storage error if the workspace does not exist or `SQLite` fails.
    pub async fn insert_discussion_message(
        &self,
        workspace_id: &WorkspaceId,
        client_message_id: &str,
        body: &str,
        author: &DiscussionAuthor,
    ) -> Result<(DiscussionMessage, bool)> {
        let mut tx = self
            .write_pool()
            .begin()
            .await
            .map_err(|e| store_error(&e))?;
        let inserted = sqlx::query(
            "INSERT INTO workspace_discussion_message \
             (id,workspace_id,sequence,principal_id,client_message_id,body,author_json,created_at) \
             VALUES (?,?,(SELECT COALESCE(MAX(sequence),0)+1 FROM workspace_discussion_message \
             WHERE workspace_id=?),?,?,?,?,?) \
             ON CONFLICT(workspace_id,principal_id,client_message_id) DO NOTHING",
        )
        .bind(uuid::Uuid::now_v7().to_string())
        .bind(workspace_id.as_str())
        .bind(workspace_id.as_str())
        .bind(author.principal_id.as_str())
        .bind(client_message_id)
        .bind(body)
        .bind(serde_json::to_string(author).map_err(|e| Error::Internal(e.to_string()))?)
        .bind(now_iso())
        .execute(&mut *tx)
        .await
        .map_err(|e| store_error(&e))?
        .rows_affected()
            > 0;
        let row = sqlx::query(
            "SELECT id,workspace_id,sequence,body,author_json,created_at \
             FROM workspace_discussion_message \
             WHERE workspace_id=? AND principal_id=? AND client_message_id=?",
        )
        .bind(workspace_id.as_str())
        .bind(author.principal_id.as_str())
        .bind(client_message_id)
        .fetch_one(&mut *tx)
        .await
        .map_err(|e| store_error(&e))?;
        let message = message(&row)?;
        tx.commit().await.map_err(|e| store_error(&e))?;
        Ok((message, inserted))
    }

    /// Read the newest bounded page before an exclusive sequence, oldest first.
    ///
    /// # Errors
    /// Returns `InvalidParams` for invalid bounds or a storage error on failure.
    pub async fn list_discussion_messages(
        &self,
        workspace_id: &WorkspaceId,
        before_sequence: Option<i64>,
        limit: u32,
    ) -> Result<DiscussionPage> {
        if limit == 0 || limit > DISCUSSION_PAGE_MAX || before_sequence.is_some_and(|n| n <= 0) {
            return Err(Error::InvalidParams(
                "invalid discussion page bounds".into(),
            ));
        }
        let rows = sqlx::query(
            "SELECT id,workspace_id,sequence,body,author_json,created_at \
             FROM workspace_discussion_message WHERE workspace_id=? AND sequence < ? \
             ORDER BY sequence DESC LIMIT ?",
        )
        .bind(workspace_id.as_str())
        .bind(before_sequence.unwrap_or(i64::MAX))
        .bind(i64::from(limit) + 1)
        .fetch_all(self.read_pool())
        .await
        .map_err(|e| store_error(&e))?;
        let has_more = rows.len() > limit as usize;
        let mut messages = rows
            .iter()
            .take(limit as usize)
            .map(message)
            .collect::<Result<Vec<_>>>()?;
        messages.reverse();
        Ok(DiscussionPage { messages, has_more })
    }
}
