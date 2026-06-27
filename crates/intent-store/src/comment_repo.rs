//! Comment repository: insert/get/update/delete/list + thread assembly (§9.2).
//!
//! The `comment` table stores the [`CommentAnchor`] as `anchor_json` and the
//! suggestion/session-specific fields (`anchorBefore`/`anchorAfter`,
//! `suggestionOriginal`/`suggestionProposed`, `agentId`) as a compact
//! `extra_json` blob, keeping the row narrow while round-tripping the full
//! wire-facing [`Comment`].

use intent_core::{
    AgentId, Comment, CommentAnchor, CommentStatus, CommentThread, CommentType, Error, NoteId,
    Result,
};
use serde::{Deserialize, Serialize};
use sqlx::sqlite::SqliteRow;
use sqlx::Row;

use crate::{enum_from_db, enum_to_db, Store};

const COMMENT_COLUMNS: &str = "id, thread_id, note_id, kind, content, author, author_type, \
    status, parent_id, anchor_json, anchor_text, extra_json, created_at, updated_at";

/// The non-columnar comment fields, persisted together as `extra_json`.
#[derive(Default, Serialize, Deserialize)]
#[serde(rename_all = "camelCase")]
struct ExtraFields {
    #[serde(default, skip_serializing_if = "Option::is_none")]
    anchor_before: Option<String>,
    #[serde(default, skip_serializing_if = "Option::is_none")]
    anchor_after: Option<String>,
    #[serde(default, skip_serializing_if = "Option::is_none")]
    suggestion_original: Option<String>,
    #[serde(default, skip_serializing_if = "Option::is_none")]
    suggestion_proposed: Option<String>,
    #[serde(default, skip_serializing_if = "Option::is_none")]
    agent_id: Option<AgentId>,
}

impl ExtraFields {
    fn is_empty(&self) -> bool {
        self.anchor_before.is_none()
            && self.anchor_after.is_none()
            && self.suggestion_original.is_none()
            && self.suggestion_proposed.is_none()
            && self.agent_id.is_none()
    }
}

impl Store {
    /// Insert a comment row.
    pub async fn insert_comment(&self, c: &Comment) -> Result<()> {
        let anchor_json = serde_json::to_string(&c.anchor)
            .map_err(|e| Error::Internal(format!("encode anchor failed: {e}")))?;
        let extra = ExtraFields {
            anchor_before: c.anchor_before.clone(),
            anchor_after: c.anchor_after.clone(),
            suggestion_original: c.suggestion_original.clone(),
            suggestion_proposed: c.suggestion_proposed.clone(),
            agent_id: c.agent_id.clone(),
        };
        let extra_json = if extra.is_empty() {
            None
        } else {
            Some(
                serde_json::to_string(&extra)
                    .map_err(|e| Error::Internal(format!("encode extra failed: {e}")))?,
            )
        };
        let sql =
            format!("INSERT INTO comment ({COMMENT_COLUMNS}) VALUES (?,?,?,?,?,?,?,?,?,?,?,?,?,?)");
        sqlx::query(&sql)
            .bind(&c.id)
            .bind(&c.thread_id)
            .bind(c.note_id.as_ref().map(|n| n.0.clone()))
            .bind(enum_to_db(&c.kind)?)
            .bind(&c.content)
            .bind(&c.author)
            .bind(enum_to_db(&c.author_type)?)
            .bind(enum_to_db(&c.status)?)
            .bind(&c.parent_id)
            .bind(anchor_json)
            .bind(&c.anchor_text)
            .bind(extra_json)
            .bind(&c.created_at)
            .bind(&c.updated_at)
            .execute(self.pool())
            .await
            .map_err(|e| Error::Internal(format!("insert comment failed: {e}")))?;
        Ok(())
    }

    /// Fetch a single comment by id, or `NotFound`.
    pub async fn get_comment(&self, id: &str) -> Result<Comment> {
        let sql = format!("SELECT {COMMENT_COLUMNS} FROM comment WHERE id = ?");
        let row = sqlx::query(&sql)
            .bind(id)
            .fetch_optional(self.pool())
            .await
            .map_err(|e| Error::Internal(format!("get comment failed: {e}")))?;
        match row {
            Some(r) => map_comment_row(&r),
            None => Err(Error::NotFound(format!("comment {id}"))),
        }
    }

    /// Update an existing comment (full row replace), or `NotFound`.
    pub async fn update_comment(&self, c: &Comment) -> Result<()> {
        let anchor_json = serde_json::to_string(&c.anchor)
            .map_err(|e| Error::Internal(format!("encode anchor failed: {e}")))?;
        let extra = ExtraFields {
            anchor_before: c.anchor_before.clone(),
            anchor_after: c.anchor_after.clone(),
            suggestion_original: c.suggestion_original.clone(),
            suggestion_proposed: c.suggestion_proposed.clone(),
            agent_id: c.agent_id.clone(),
        };
        let extra_json = if extra.is_empty() {
            None
        } else {
            Some(
                serde_json::to_string(&extra)
                    .map_err(|e| Error::Internal(format!("encode extra failed: {e}")))?,
            )
        };
        let res = sqlx::query(
            "UPDATE comment SET thread_id=?, note_id=?, kind=?, content=?, author=?, \
             author_type=?, status=?, parent_id=?, anchor_json=?, anchor_text=?, extra_json=?, \
             updated_at=? WHERE id=?",
        )
        .bind(&c.thread_id)
        .bind(c.note_id.as_ref().map(|n| n.0.clone()))
        .bind(enum_to_db(&c.kind)?)
        .bind(&c.content)
        .bind(&c.author)
        .bind(enum_to_db(&c.author_type)?)
        .bind(enum_to_db(&c.status)?)
        .bind(&c.parent_id)
        .bind(anchor_json)
        .bind(&c.anchor_text)
        .bind(extra_json)
        .bind(&c.updated_at)
        .bind(&c.id)
        .execute(self.pool())
        .await
        .map_err(|e| Error::Internal(format!("update comment failed: {e}")))?;
        if res.rows_affected() == 0 {
            return Err(Error::NotFound(format!("comment {}", c.id)));
        }
        Ok(())
    }

    /// Delete a comment by id, or `NotFound`.
    pub async fn delete_comment(&self, id: &str) -> Result<()> {
        let res = sqlx::query("DELETE FROM comment WHERE id = ?")
            .bind(id)
            .execute(self.pool())
            .await
            .map_err(|e| Error::Internal(format!("delete comment failed: {e}")))?;
        if res.rows_affected() == 0 {
            return Err(Error::NotFound(format!("comment {id}")));
        }
        Ok(())
    }

    /// List a note's comments, ordered by creation time.
    pub async fn list_comments(&self, note_id: &NoteId) -> Result<Vec<Comment>> {
        let sql = format!(
            "SELECT {COMMENT_COLUMNS} FROM comment WHERE note_id = ? ORDER BY created_at, id"
        );
        let rows = sqlx::query(&sql)
            .bind(&note_id.0)
            .fetch_all(self.pool())
            .await
            .map_err(|e| Error::Internal(format!("list comments failed: {e}")))?;
        rows.iter().map(map_comment_row).collect()
    }

    /// List the comments in one thread, ordered by creation time.
    pub async fn list_thread_comments(&self, thread_id: &str) -> Result<Vec<Comment>> {
        let sql = format!(
            "SELECT {COMMENT_COLUMNS} FROM comment WHERE thread_id = ? ORDER BY created_at, id"
        );
        let rows = sqlx::query(&sql)
            .bind(thread_id)
            .fetch_all(self.pool())
            .await
            .map_err(|e| Error::Internal(format!("list thread comments failed: {e}")))?;
        rows.iter().map(map_comment_row).collect()
    }

    /// Set the `status` of every comment in a thread, refreshing `updated_at`.
    /// Returns the number of rows updated (0 when the thread does not exist).
    pub async fn set_thread_status(
        &self,
        thread_id: &str,
        status: CommentStatus,
        updated_at: &str,
    ) -> Result<u64> {
        let res = sqlx::query("UPDATE comment SET status=?, updated_at=? WHERE thread_id=?")
            .bind(enum_to_db(&status)?)
            .bind(updated_at)
            .bind(thread_id)
            .execute(self.pool())
            .await
            .map_err(|e| Error::Internal(format!("set thread status failed: {e}")))?;
        Ok(res.rows_affected())
    }

    /// Assemble a [`CommentThread`] (the comments sharing `thread_id`).
    pub async fn get_thread(&self, thread_id: &str) -> Result<CommentThread> {
        let comments = self.list_thread_comments(thread_id).await?;
        if comments.is_empty() {
            return Err(Error::NotFound(format!("thread {thread_id}")));
        }
        Ok(CommentThread {
            thread_id: thread_id.to_string(),
            comments,
        })
    }
}

fn col<'r, T>(row: &'r SqliteRow, name: &str) -> Result<T>
where
    T: sqlx::Decode<'r, sqlx::Sqlite> + sqlx::Type<sqlx::Sqlite>,
{
    row.try_get::<T, _>(name)
        .map_err(|e| Error::Internal(format!("column {name}: {e}")))
}

fn map_comment_row(row: &SqliteRow) -> Result<Comment> {
    let note_id: Option<String> = col(row, "note_id")?;
    let anchor: CommentAnchor = serde_json::from_str(&col::<String>(row, "anchor_json")?)
        .map_err(|e| Error::Internal(format!("decode anchor failed: {e}")))?;
    let extra: ExtraFields = match col::<Option<String>>(row, "extra_json")? {
        Some(s) => serde_json::from_str(&s)
            .map_err(|e| Error::Internal(format!("decode extra failed: {e}")))?,
        None => ExtraFields::default(),
    };
    Ok(Comment {
        id: col(row, "id")?,
        thread_id: col(row, "thread_id")?,
        note_id: note_id.map(NoteId),
        kind: enum_from_db::<CommentType>(&col::<String>(row, "kind")?)?,
        content: col(row, "content")?,
        author: col(row, "author")?,
        author_type: enum_from_db(&col::<String>(row, "author_type")?)?,
        status: enum_from_db::<CommentStatus>(&col::<String>(row, "status")?)?,
        parent_id: col(row, "parent_id")?,
        anchor,
        anchor_text: col(row, "anchor_text")?,
        anchor_before: extra.anchor_before,
        anchor_after: extra.anchor_after,
        suggestion_original: extra.suggestion_original,
        suggestion_proposed: extra.suggestion_proposed,
        agent_id: extra.agent_id,
        created_at: col(row, "created_at")?,
        updated_at: col(row, "updated_at")?,
    })
}
