//! Comments and per-client chat drafts (Postgres). Mirrors `comment_repo.rs`
//! and `draft_repo.rs`.

use intent_core::{
    now_iso, AgentId, ClientId, Comment, CommentAnchor, CommentStatus, CommentThread, CommentType,
    Draft, Error, Note, NoteId, NoteVersionAuthor, Result, WorkspaceId,
};
use serde_json::{Map, Value};
use sqlx::postgres::PgRow;

use super::notes::{cas_rev, col, exec_update_note, insert_note_version};
use super::{db, PgStore, WriteTx};
use crate::comment_repo::{
    encode_comment_json, encode_comment_update, ExtraFields, COMMENT_COLUMNS,
};
use crate::note_repo::NoteUpdateScope;
use crate::{enum_from_db, enum_to_db, NoteWriteLock};

fn internal(context: &str) -> impl Fn(sqlx::Error) -> Error + '_ {
    move |e| Error::Internal(format!("{context}: {e}"))
}

impl PgStore {
    pub async fn insert_comment(&self, workspace_id: &WorkspaceId, c: &Comment) -> Result<()> {
        self.insert_comment_with_extras(workspace_id, c, &Map::new())
            .await
    }

    pub async fn insert_comment_with_extras(
        &self,
        workspace_id: &WorkspaceId,
        c: &Comment,
        legacy_extra: &Map<String, Value>,
    ) -> Result<()> {
        let (anchor_json, extra_json) = encode_comment_json(c, legacy_extra)?;
        let mut tx = self.begin_write().await?;
        exec_insert_comment(
            self,
            &mut tx,
            workspace_id,
            c,
            &anchor_json,
            extra_json.as_deref(),
        )
        .await
        .map_err(|e| Error::Internal(format!("insert comment failed: {e}")))?;
        tx.commit().await.map_err(db)
    }

    pub async fn update_note_with_comment(
        &self,
        lock: &NoteWriteLock,
        note: &Note,
        expected_version: Option<i64>,
        c: &Comment,
        author: &NoteVersionAuthor,
    ) -> Result<i64> {
        lock.ensure_covers(&self.note_write_lanes, note)?;
        let (anchor_json, extra_json) = encode_comment_json(c, &Map::new())?;
        self.prepare_task_events(&note.workspace_id).await;
        let mut tx = self.begin_write().await?;
        let Some(new_rev) = exec_update_note(
            self,
            &mut tx,
            note,
            cas_rev(note, expected_version),
            NoteUpdateScope::FullRow,
            None,
        )
        .await?
        else {
            drop(tx);
            return Err(self.note_update_miss(note).await);
        };
        insert_note_version(self, &mut tx, note, author, &note.updated_at, new_rev).await?;
        exec_insert_comment(
            self,
            &mut tx,
            &note.workspace_id,
            c,
            &anchor_json,
            extra_json.as_deref(),
        )
        .await
        .map_err(|e| {
            // A duplicate `comment.id` is caller input (see the SQLite side).
            if e.as_database_error()
                .is_some_and(sqlx::error::DatabaseError::is_unique_violation)
            {
                Error::InvalidInput(format!("comment {} already exists", c.id))
            } else {
                Error::Internal(format!("insert comment failed: {e}"))
            }
        })?;
        tx.commit().await.map_err(db)?;
        Ok(new_rev)
    }

    pub async fn get_comment(&self, id: &str) -> Result<Comment> {
        let mut tx = self.begin().await?;
        let row = sqlx::query(sqlx::AssertSqlSafe(format!(
            "SELECT {COMMENT_COLUMNS} FROM comment WHERE tenant_id = $1 AND id = $2"
        )))
        .bind(self.tenant())
        .bind(id)
        .fetch_optional(&mut *tx)
        .await
        .map_err(internal("get comment failed"))?;
        tx.commit().await.map_err(db)?;
        match row {
            Some(r) => map_comment_row(&r),
            None => Err(Error::NotFound(format!("comment {id}"))),
        }
    }

    pub async fn update_comment(&self, workspace_id: &WorkspaceId, c: &Comment) -> Result<()> {
        let mut tx = self.begin_write().await?;
        let existing: Option<Option<String>> = sqlx::query_scalar(
            "SELECT extra_json FROM comment WHERE tenant_id = $1 AND id = $2 AND workspace_id = $3",
        )
        .bind(self.tenant())
        .bind(&c.id)
        .bind(workspace_id.as_str())
        .fetch_optional(&mut *tx)
        .await
        .map_err(internal("read comment extras failed"))?;
        let (anchor_json, extra_json) = encode_comment_update(c, existing.flatten().as_deref())?;
        let updated = sqlx::query(
            "UPDATE comment SET thread_id = $4, note_id = $5, kind = $6, content = $7, \
             author = $8, author_type = $9, status = $10, parent_id = $11, anchor_json = $12, \
             anchor_text = $13, extra_json = $14, updated_at = $15 \
             WHERE tenant_id = $1 AND id = $2 AND workspace_id = $3",
        )
        .bind(self.tenant())
        .bind(&c.id)
        .bind(workspace_id.as_str())
        .bind(&c.thread_id)
        .bind(c.note_id.as_ref().map(NoteId::as_str))
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
        .execute(&mut *tx)
        .await
        .map_err(internal("comment update failed"))?
        .rows_affected();
        tx.commit().await.map_err(db)?;
        if updated == 0 {
            return Err(Error::NotFound(format!("comment {}", c.id)));
        }
        Ok(())
    }

    pub async fn mark_comment_orphaned(
        &self,
        workspace_id: &WorkspaceId,
        note_id: &NoteId,
        comment_id: &str,
        updated_at: &str,
    ) -> Result<bool> {
        let mut tx = self.begin_write().await?;
        let updated = sqlx::query(
            "UPDATE comment SET extra_json = \
                 sqlite_json_set(COALESCE(extra_json, '{}'), '$.isOrphaned', sqlite_json('true')), \
             updated_at = $5 \
             WHERE tenant_id = $1 AND id = $2 AND workspace_id = $3 AND note_id = $4 \
             AND parent_id IS NULL",
        )
        .bind(self.tenant())
        .bind(comment_id)
        .bind(workspace_id.as_str())
        .bind(note_id.as_str())
        .bind(updated_at)
        .execute(&mut *tx)
        .await
        .map_err(internal("mark comment orphaned failed"))?
        .rows_affected();
        tx.commit().await.map_err(db)?;
        Ok(updated > 0)
    }

    pub async fn delete_comment(&self, workspace_id: &WorkspaceId, id: &str) -> Result<()> {
        let mut tx = self.begin_write().await?;
        let deleted = sqlx::query(
            "DELETE FROM comment WHERE tenant_id = $1 AND id = $2 AND workspace_id = $3",
        )
        .bind(self.tenant())
        .bind(id)
        .bind(workspace_id.as_str())
        .execute(&mut *tx)
        .await
        .map_err(internal("delete comment failed"))?
        .rows_affected();
        tx.commit().await.map_err(db)?;
        if deleted == 0 {
            return Err(Error::NotFound(format!("comment {id}")));
        }
        Ok(())
    }

    pub async fn delete_comment_in_note(
        &self,
        workspace_id: &WorkspaceId,
        note_id: &NoteId,
        id: &str,
    ) -> Result<String> {
        let mut tx = self.begin_write().await?;
        let thread: Option<String> = sqlx::query_scalar(
            "DELETE FROM comment WHERE tenant_id = $1 AND id = $2 AND workspace_id = $3 \
             AND note_id = $4 RETURNING thread_id",
        )
        .bind(self.tenant())
        .bind(id)
        .bind(workspace_id.as_str())
        .bind(note_id.as_str())
        .fetch_optional(&mut *tx)
        .await
        .map_err(internal("delete comment failed"))?;
        tx.commit().await.map_err(db)?;
        thread.ok_or_else(|| Error::NotFound(format!("comment {id}")))
    }

    pub async fn list_comments(&self, note_id: &NoteId) -> Result<Vec<Comment>> {
        let mut tx = self.begin().await?;
        let rows = sqlx::query(sqlx::AssertSqlSafe(format!(
            "SELECT {COMMENT_COLUMNS} FROM comment WHERE tenant_id = $1 AND note_id = $2 \
             ORDER BY created_at COLLATE \"C\", id COLLATE \"C\""
        )))
        .bind(self.tenant())
        .bind(note_id.as_str())
        .fetch_all(&mut *tx)
        .await
        .map_err(internal("list comments failed"))?;
        tx.commit().await.map_err(db)?;
        rows.iter().map(map_comment_row).collect()
    }

    pub async fn list_comments_in_workspace(
        &self,
        workspace_id: &WorkspaceId,
        note_id: &NoteId,
    ) -> Result<Vec<Comment>> {
        let mut tx = self.begin().await?;
        let rows = sqlx::query(sqlx::AssertSqlSafe(format!(
            "SELECT {COMMENT_COLUMNS} FROM comment \
             WHERE tenant_id = $1 AND note_id = $2 AND workspace_id = $3 \
             ORDER BY created_at COLLATE \"C\", id COLLATE \"C\""
        )))
        .bind(self.tenant())
        .bind(note_id.as_str())
        .bind(workspace_id.as_str())
        .fetch_all(&mut *tx)
        .await
        .map_err(internal("list comments failed"))?;
        tx.commit().await.map_err(db)?;
        rows.iter().map(map_comment_row).collect()
    }

    pub async fn set_thread_status(
        &self,
        workspace_id: &WorkspaceId,
        thread_id: &str,
        status: CommentStatus,
        updated_at: &str,
    ) -> Result<u64> {
        let mut tx = self.begin_write().await?;
        let updated = sqlx::query(
            "UPDATE comment SET status = $4, updated_at = $5 \
             WHERE tenant_id = $1 AND thread_id = $2 AND workspace_id = $3",
        )
        .bind(self.tenant())
        .bind(thread_id)
        .bind(workspace_id.as_str())
        .bind(enum_to_db(&status)?)
        .bind(updated_at)
        .execute(&mut *tx)
        .await
        .map_err(internal("set thread status failed"))?
        .rows_affected();
        tx.commit().await.map_err(db)?;
        Ok(updated)
    }

    pub async fn get_thread(&self, thread_id: &str) -> Result<CommentThread> {
        let mut tx = self.begin().await?;
        let rows = sqlx::query(sqlx::AssertSqlSafe(format!(
            "SELECT {COMMENT_COLUMNS} FROM comment WHERE tenant_id = $1 AND thread_id = $2 \
             ORDER BY created_at COLLATE \"C\", id COLLATE \"C\""
        )))
        .bind(self.tenant())
        .bind(thread_id)
        .fetch_all(&mut *tx)
        .await
        .map_err(internal("list thread comments failed"))?;
        tx.commit().await.map_err(db)?;
        let comments = rows
            .iter()
            .map(map_comment_row)
            .collect::<Result<Vec<_>>>()?;
        if comments.is_empty() {
            return Err(Error::NotFound(format!("thread {thread_id}")));
        }
        Ok(CommentThread {
            thread_id: thread_id.to_string(),
            comments,
        })
    }

    // Drafts.

    pub async fn get_draft(
        &self,
        workspace_id: &WorkspaceId,
        agent_id: &AgentId,
        client_id: &ClientId,
    ) -> Result<Option<Draft>> {
        let mut tx = self.begin().await?;
        let row = sqlx::query(
            "SELECT workspace_id, agent_id, client_id, text, attachments, updated_at FROM draft \
             WHERE tenant_id = $1 AND workspace_id = $2 AND agent_id = $3 AND client_id = $4",
        )
        .bind(self.tenant())
        .bind(workspace_id.as_str())
        .bind(agent_id.as_str())
        .bind(client_id.0.as_str())
        .fetch_optional(&mut *tx)
        .await
        .map_err(internal("get draft failed"))?;
        tx.commit().await.map_err(db)?;
        row.as_ref().map(map_draft_row).transpose()
    }

    pub async fn upsert_draft(
        &self,
        workspace_id: &WorkspaceId,
        agent_id: &AgentId,
        client_id: &ClientId,
        text: &str,
        attachments: Option<&Value>,
    ) -> Result<String> {
        let now = now_iso();
        let mut tx = self.begin_write().await?;
        sqlx::query(
            "INSERT INTO draft (tenant_id, workspace_id, agent_id, client_id, text, attachments, \
                 updated_at) \
             VALUES ($1, $2, $3, $4, $5, $6, $7) \
             ON CONFLICT (tenant_id, workspace_id, agent_id, client_id) DO UPDATE SET \
             text = excluded.text, attachments = excluded.attachments, \
             updated_at = excluded.updated_at",
        )
        .bind(self.tenant())
        .bind(workspace_id.as_str())
        .bind(agent_id.as_str())
        .bind(client_id.0.as_str())
        .bind(text)
        .bind(attachments.map(ToString::to_string))
        .bind(&now)
        .execute(&mut *tx)
        .await
        .map_err(internal("upsert draft failed"))?;
        tx.commit().await.map_err(db)?;
        Ok(now)
    }

    pub async fn delete_draft(
        &self,
        workspace_id: &WorkspaceId,
        agent_id: &AgentId,
        client_id: &ClientId,
    ) -> Result<bool> {
        let mut tx = self.begin_write().await?;
        let deleted = sqlx::query(
            "DELETE FROM draft \
             WHERE tenant_id = $1 AND workspace_id = $2 AND agent_id = $3 AND client_id = $4",
        )
        .bind(self.tenant())
        .bind(workspace_id.as_str())
        .bind(agent_id.as_str())
        .bind(client_id.0.as_str())
        .execute(&mut *tx)
        .await
        .map_err(internal("delete draft failed"))?
        .rows_affected();
        tx.commit().await.map_err(db)?;
        Ok(deleted > 0)
    }
}

async fn exec_insert_comment(
    store: &PgStore,
    conn: &mut WriteTx,
    workspace_id: &WorkspaceId,
    c: &Comment,
    anchor_json: &str,
    extra_json: Option<&str>,
) -> std::result::Result<(), sqlx::Error> {
    let kind = enum_to_db(&c.kind).map_err(|e| sqlx::Error::Encode(e.to_string().into()))?;
    let author_type =
        enum_to_db(&c.author_type).map_err(|e| sqlx::Error::Encode(e.to_string().into()))?;
    let status = enum_to_db(&c.status).map_err(|e| sqlx::Error::Encode(e.to_string().into()))?;
    sqlx::query(sqlx::AssertSqlSafe(format!(
        "INSERT INTO comment (tenant_id, {COMMENT_COLUMNS}, workspace_id) \
         VALUES ($1,$2,$3,$4,$5,$6,$7,$8,$9,$10,$11,$12,$13,$14,$15,$16)"
    )))
    .bind(store.tenant())
    .bind(&c.id)
    .bind(&c.thread_id)
    .bind(c.note_id.as_ref().map(NoteId::as_str))
    .bind(kind)
    .bind(&c.content)
    .bind(&c.author)
    .bind(author_type)
    .bind(status)
    .bind(&c.parent_id)
    .bind(anchor_json)
    .bind(&c.anchor_text)
    .bind(extra_json)
    .bind(&c.created_at)
    .bind(&c.updated_at)
    .bind(workspace_id.as_str())
    .execute(&mut **conn)
    .await?;
    Ok(())
}

fn map_comment_row(row: &PgRow) -> Result<Comment> {
    let anchor: Option<CommentAnchor> =
        serde_json::from_str(&col::<String>(row, "anchor_json")?)
            .map_err(|e| Error::Internal(format!("decode anchor failed: {e}")))?;
    let extra: ExtraFields = match col::<Option<String>>(row, "extra_json")? {
        Some(s) => serde_json::from_str(&s)
            .map_err(|e| Error::Internal(format!("decode extra failed: {e}")))?,
        None => ExtraFields::default(),
    };
    Ok(Comment {
        id: col(row, "id")?,
        thread_id: col(row, "thread_id")?,
        note_id: col::<Option<String>>(row, "note_id")?.map(NoteId),
        kind: enum_from_db::<CommentType>(&col::<String>(row, "kind")?)?,
        content: col(row, "content")?,
        author: col(row, "author")?,
        author_type: enum_from_db(&col::<String>(row, "author_type")?)?,
        author_principal_id: extra.author_principal_id,
        author_identity: extra.author_identity,
        status: enum_from_db::<CommentStatus>(&col::<String>(row, "status")?)?,
        parent_id: col(row, "parent_id")?,
        anchor,
        anchor_text: col(row, "anchor_text")?,
        anchor_before: extra.anchor_before,
        anchor_after: extra.anchor_after,
        suggestion_original: extra.suggestion_original,
        suggestion_proposed: extra.suggestion_proposed,
        agent_id: extra.agent_id,
        is_orphaned: extra.is_orphaned,
        created_at: col(row, "created_at")?,
        updated_at: col(row, "updated_at")?,
    })
}

fn map_draft_row(row: &PgRow) -> Result<Draft> {
    Ok(Draft {
        workspace_id: WorkspaceId(col(row, "workspace_id")?),
        agent_id: AgentId(col(row, "agent_id")?),
        client_id: ClientId(col(row, "client_id")?),
        text: col(row, "text")?,
        attachments: col::<Option<String>>(row, "attachments")?
            .map(|s| {
                serde_json::from_str(&s)
                    .map_err(|e| Error::Internal(format!("parse draft attachments failed: {e}")))
            })
            .transpose()?,
        updated_at: col(row, "updated_at")?,
    })
}
