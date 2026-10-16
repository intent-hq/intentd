//! Bounded sequence paging and atomic per-human retry deduplication.
use intent_core::discussion::{
    DiscussionAuthor, DiscussionMessage, DiscussionPage, DISCUSSION_PAGE_MAX,
};
use intent_core::{Error, Result, WorkspaceId};
use sqlx::{sqlite::SqliteRow, Row};

use crate::Store;

fn store_error(error: &sqlx::Error) -> Error {
    Error::Internal(format!("workspace discussion: {error}"))
}

pub(super) fn message(row: &SqliteRow) -> Result<DiscussionMessage> {
    Ok(DiscussionMessage {
        id: row.get("id"),
        workspace_id: WorkspaceId(row.get("workspace_id")),
        conversation_id: row.get("conversation_id"),
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
    /// Returns `InvalidParams` for a retry with a different body, or a storage
    /// error if the workspace does not exist or `SQLite` fails.
    pub async fn insert_discussion_message(
        &self,
        workspace_id: &WorkspaceId,
        client_message_id: &str,
        body: &str,
        author: &DiscussionAuthor,
    ) -> Result<(DiscussionMessage, bool)> {
        let (result, inserted) = self
            .send_discussion_message(
                workspace_id,
                client_message_id,
                body,
                author,
                &intent_core::discussion::DiscussionTarget::default(),
            )
            .await?;
        Ok((result.message, inserted))
    }

    /// Read a bounded page, oldest first. After a cursor, take the first newer
    /// rows; otherwise take the newest rows before the optional cursor.
    ///
    /// # Errors
    /// Returns `InvalidParams` for invalid bounds or a storage error on failure.
    pub async fn list_discussion_messages(
        &self,
        workspace_id: &WorkspaceId,
        before_sequence: Option<i64>,
        after_sequence: Option<i64>,
        limit: u32,
    ) -> Result<DiscussionPage> {
        self.list_discussion_messages_in(
            workspace_id,
            "default",
            before_sequence,
            after_sequence,
            limit,
        )
        .await
    }

    /// Read one conversation's bounded history.
    /// # Errors
    /// Returns invalid bounds or database errors.
    pub async fn list_discussion_messages_in(
        &self,
        workspace_id: &WorkspaceId,
        conversation_id: &str,
        before_sequence: Option<i64>,
        after_sequence: Option<i64>,
        limit: u32,
    ) -> Result<DiscussionPage> {
        if limit == 0
            || limit > DISCUSSION_PAGE_MAX
            || before_sequence.is_some_and(|n| n <= 0)
            || after_sequence.is_some_and(|n| n < 0)
            || (before_sequence.is_some() && after_sequence.is_some())
        {
            return Err(Error::InvalidParams(
                "invalid discussion page bounds".into(),
            ));
        }
        let (comparison, order, cursor) = match after_sequence {
            Some(after) => (">", "ASC", after),
            None => ("<", "DESC", before_sequence.unwrap_or(i64::MAX)),
        };
        let sql = format!(
            "SELECT id,workspace_id,conversation_id,sequence,body,author_json,created_at \
             FROM workspace_discussion_message WHERE workspace_id=? AND conversation_id=? AND sequence {comparison} ? \
             ORDER BY sequence {order} LIMIT ?"
        );
        let rows = sqlx::query(&sql)
            .bind(workspace_id.as_str())
            .bind(conversation_id)
            .bind(cursor)
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
        if after_sequence.is_none() {
            messages.reverse();
        }
        Ok(DiscussionPage { messages, has_more })
    }
}
