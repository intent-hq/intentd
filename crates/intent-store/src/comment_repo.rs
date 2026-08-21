//! Comment repository: insert/get/update/delete/list + thread assembly (§9.2).
//!
//! The `comment` table stores the [`CommentAnchor`] as `anchor_json` and the
//! suggestion/session-specific fields (`anchorBefore`/`anchorAfter`,
//! `suggestionOriginal`/`suggestionProposed`, `agentId`) as a compact
//! `extra_json` blob, keeping the row narrow while round-tripping the full
//! wire-facing [`Comment`].

use intent_core::{
    AgentId, Comment, CommentAnchor, CommentStatus, CommentThread, CommentType, Error, Note,
    NoteId, Result, WorkspaceId,
};
use serde::{Deserialize, Serialize};
use serde_json::{Map, Value};
use sqlx::sqlite::SqliteRow;
use sqlx::Row;

use crate::{enum_from_db, enum_to_db, tags_to_db, Store};

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
    #[serde(
        default,
        skip_serializing_if = "Option::is_none",
        deserialize_with = "lenient_bool"
    )]
    is_orphaned: Option<bool>,
}

/// Tolerate wrong-typed legacy `isOrphaned` values preserved verbatim in
/// `extra_json` by the legacy importer: anything but a JSON boolean decodes
/// as `None` instead of failing the whole row.
fn lenient_bool<'de, D>(deserializer: D) -> std::result::Result<Option<bool>, D::Error>
where
    D: serde::Deserializer<'de>,
{
    Ok(match Option::<Value>::deserialize(deserializer) {
        Ok(Some(Value::Bool(b))) => Some(b),
        _ => None,
    })
}

impl ExtraFields {
    /// The camelCase keys this struct owns inside `extra_json`. Anything else
    /// in the blob is a preserved legacy/unknown key (see
    /// [`Store::insert_comment_with_extras`]) that updates must not drop.
    const KNOWN_KEYS: [&'static str; 6] = [
        "anchorBefore",
        "anchorAfter",
        "suggestionOriginal",
        "suggestionProposed",
        "agentId",
        "isOrphaned",
    ];

    /// Serialize into the `extra_json` object map (`None` fields omitted).
    fn to_map(&self) -> Result<Map<String, Value>> {
        match serde_json::to_value(self) {
            Ok(Value::Object(m)) => Ok(m),
            Ok(_) => Err(Error::Internal("encode extra failed: not an object".into())),
            Err(e) => Err(Error::Internal(format!("encode extra failed: {e}"))),
        }
    }
}

/// Encode a merged `extra_json` map for persistence: empty → SQL `NULL`.
fn extra_map_to_json(merged: Map<String, Value>) -> Result<Option<String>> {
    if merged.is_empty() {
        Ok(None)
    } else {
        Ok(Some(
            serde_json::to_string(&Value::Object(merged))
                .map_err(|e| Error::Internal(format!("encode extra failed: {e}")))?,
        ))
    }
}

/// Encode a comment's `anchor_json` + `extra_json` column values, merging
/// `legacy_extra` (comment's own fields win on key collision).
fn encode_comment_json(
    c: &Comment,
    legacy_extra: &Map<String, Value>,
) -> Result<(String, Option<String>)> {
    let anchor_json = serde_json::to_string(&c.anchor)
        .map_err(|e| Error::Internal(format!("encode anchor failed: {e}")))?;
    let extra = ExtraFields {
        anchor_before: c.anchor_before.clone(),
        anchor_after: c.anchor_after.clone(),
        suggestion_original: c.suggestion_original.clone(),
        suggestion_proposed: c.suggestion_proposed.clone(),
        agent_id: c.agent_id.clone(),
        is_orphaned: c.is_orphaned,
    };
    let mut merged = extra.to_map()?;
    for (k, v) in legacy_extra {
        merged.entry(k.clone()).or_insert_with(|| v.clone());
    }
    let extra_json = extra_map_to_json(merged)?;
    Ok((anchor_json, extra_json))
}

impl Store {
    /// Insert a comment row, scoping it to `workspace_id` (0022 added the
    /// per-workspace column plus the composite FK to `note(id, workspace_id)`).
    /// The wire-facing [`Comment`] itself carries no `workspace_id`, so the
    /// caller supplies it explicitly.
    ///
    /// # Errors
    ///
    /// Returns `Error::Internal` if encoding the comment JSON or the insert fails.
    pub async fn insert_comment(&self, workspace_id: &WorkspaceId, c: &Comment) -> Result<()> {
        self.insert_comment_with_extras(workspace_id, c, &Map::new())
            .await
    }

    /// [`Store::insert_comment`], additionally merging `legacy_extra` — legacy
    /// fields intentd does not model (used by the legacy workspace importer so
    /// unknown source fields are preserved instead of dropped) — into the
    /// `extra_json` blob. On key collision the comment's own fields win.
    /// Unknown keys in `extra_json` are ignored when the row is read back.
    ///
    /// # Errors
    ///
    /// Returns `Error::Internal` if the database operation fails.
    pub async fn insert_comment_with_extras(
        &self,
        workspace_id: &WorkspaceId,
        c: &Comment,
        legacy_extra: &Map<String, Value>,
    ) -> Result<()> {
        let (anchor_json, extra_json) = encode_comment_json(c, legacy_extra)?;
        let sql = format!(
            "INSERT INTO comment ({COMMENT_COLUMNS}, workspace_id) \
             VALUES (?,?,?,?,?,?,?,?,?,?,?,?,?,?,?)"
        );
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
            .bind(&workspace_id.0)
            .execute(self.write_pool())
            .await
            .map_err(|e| Error::Internal(format!("insert comment failed: {e}")))?;
        Ok(())
    }

    /// Atomically persist a `comment.add`: the anchor-marker note rewrite
    /// (the same full-row UPDATE + unconditional `rev` bump as
    /// [`Store::update_note`]) and the comment INSERT run in ONE transaction,
    /// so a failure between the two can never leave anchor markers embedded
    /// in the note with no comment row (monorepo#638). Returns the
    /// post-rewrite note `rev` so the caller can echo the authoritative
    /// value. `NotFound` if the note row is absent; nothing persists on any
    /// error.
    ///
    /// # Errors
    ///
    /// Returns `Error::NotFound` if the note row is absent; `Error::InvalidInput` on a duplicate comment id; `Error::Internal` if encoding fields or the transaction fails.
    pub async fn update_note_with_comment(&self, note: &Note, c: &Comment) -> Result<i64> {
        let parent_id = note.parent_id.as_ref().map(|n| n.0.clone());
        let task_json = note
            .metadata
            .task
            .as_ref()
            .map(crate::note_repo::encode_task_json)
            .transpose()?;
        let (anchor_json, extra_json) = encode_comment_json(c, &Map::new())?;

        // IMMEDIATE mode: acquires the write lock upfront, avoiding the
        // DEFERRED-mode transaction-upgrade race that surfaces SQLITE_BUSY
        // when concurrent connections hold read locks (STAB-1).
        let mut conn = self
            .write_pool()
            .acquire()
            .await
            .map_err(|e| Error::Internal(format!("acquire connection failed: {e}")))?;
        sqlx::query("BEGIN IMMEDIATE")
            .execute(&mut *conn)
            .await
            .map_err(|e| Error::Internal(format!("begin IMMEDIATE failed: {e}")))?;

        // Execute the transaction body; rollback explicitly on error.
        let result = async {
            // Same statement as `update_note_versioned` (no expected_version
            // gate): full-row replace scoped by (id, workspace_id) with the
            // store-owned `rev = rev + 1` bump.
            let res = sqlx::query(
                "UPDATE note SET title=?, content=?, content_type=?, tags=?, \
                 is_pinned=?, is_archived=?, is_default=?, parent_id=?, visibility=?, task_json=?, \
                 created_at=?, updated_at=?, rev = rev + 1 WHERE id=? AND workspace_id=?",
            )
            .bind(&note.title)
            .bind(&note.content)
            .bind(enum_to_db(&note.content_type)?)
            .bind(tags_to_db(&note.tags)?)
            .bind(note.is_pinned as i64)
            .bind(note.is_archived as i64)
            .bind(note.is_default as i64)
            .bind(parent_id)
            .bind(enum_to_db(&note.visibility)?)
            .bind(task_json)
            .bind(&note.created_at)
            .bind(&note.updated_at)
            .bind(&note.id.0)
            .bind(&note.workspace_id.0)
            .execute(&mut *conn)
            .await
            .map_err(|e| Error::Internal(format!("update note failed: {e}")))?;
            if res.rows_affected() == 0 {
                return Err(Error::NotFound(format!("note {}", note.id)));
            }

            let new_rev: i64 = sqlx::query("SELECT rev FROM note WHERE id=? AND workspace_id=?")
                .bind(&note.id.0)
                .bind(&note.workspace_id.0)
                .fetch_one(&mut *conn)
                .await
                .map_err(|e| Error::Internal(format!("read back note rev failed: {e}")))?
                .try_get("rev")
                .map_err(|e| Error::Internal(format!("column rev: {e}")))?;

            let sql = format!(
                "INSERT INTO comment ({COMMENT_COLUMNS}, workspace_id) \
                 VALUES (?,?,?,?,?,?,?,?,?,?,?,?,?,?,?)"
            );
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
                .bind(&anchor_json)
                .bind(&c.anchor_text)
                .bind(&extra_json)
                .bind(&c.created_at)
                .bind(&c.updated_at)
                .bind(&note.workspace_id.0)
                .execute(&mut *conn)
                .await
                .map_err(|e| {
                    // A duplicate `comment.id` is caller input, not an internal
                    // failure: surface it distinguishably so the service layer
                    // can reject a client-supplied `commentId` collision with
                    // InvalidParams even when the race beats a pre-check.
                    if e.as_database_error()
                        .is_some_and(sqlx::error::DatabaseError::is_unique_violation)
                    {
                        Error::InvalidInput(format!("comment {} already exists", c.id))
                    } else {
                        Error::Internal(format!("insert comment failed: {e}"))
                    }
                })?;

            Ok(new_rev)
        }
        .await;

        // COMMIT on success (with rollback, and detach+close on double
        // failure, if the COMMIT itself fails — monorepo#638) or roll back
        // the failed body (monorepo#680), so the sole write-pool connection
        // is never returned holding an open transaction.
        crate::commit_with_rollback_guard(conn, result, "commit note+comment tx failed").await
    }

    /// Fetch a single comment by id, or `NotFound`.
    ///
    /// # Errors
    ///
    /// Returns `Error::NotFound` if the comment does not exist; `Error::Internal` if the database operation fails.
    pub async fn get_comment(&self, id: &str) -> Result<Comment> {
        let sql = format!("SELECT {COMMENT_COLUMNS} FROM comment WHERE id = ?");
        let row = sqlx::query(&sql)
            .bind(id)
            .fetch_optional(self.read_pool())
            .await
            .map_err(|e| Error::Internal(format!("get comment failed: {e}")))?;
        match row {
            Some(r) => map_comment_row(&r),
            None => Err(Error::NotFound(format!("comment {id}"))),
        }
    }

    /// Update an existing comment (full row replace). Scoped to `workspace_id`
    /// (defense-in-depth) so a caller bound to workspace B cannot mutate a
    /// comment row that belongs to workspace A. `NotFound` if the row is absent
    /// or the workspace does not match.
    ///
    /// `extra_json` is rebuilt from the comment's own fields but any
    /// unknown/legacy keys already present on the row (preserved by
    /// [`Store::insert_comment_with_extras`]) are carried over, so updates
    /// never silently drop imported legacy data.
    ///
    /// # Errors
    ///
    /// Returns `Error::NotFound` if the comment does not exist in the workspace; `Error::Internal` if the database operation fails.
    pub async fn update_comment(&self, workspace_id: &WorkspaceId, c: &Comment) -> Result<()> {
        let anchor_json = serde_json::to_string(&c.anchor)
            .map_err(|e| Error::Internal(format!("encode anchor failed: {e}")))?;
        let extra = ExtraFields {
            anchor_before: c.anchor_before.clone(),
            anchor_after: c.anchor_after.clone(),
            suggestion_original: c.suggestion_original.clone(),
            suggestion_proposed: c.suggestion_proposed.clone(),
            agent_id: c.agent_id.clone(),
            is_orphaned: c.is_orphaned,
        };
        let mut merged = extra.to_map()?;
        // Carry over preserved legacy/unknown keys from the existing row.
        let existing: Option<String> =
            sqlx::query("SELECT extra_json FROM comment WHERE id = ? AND workspace_id = ?")
                .bind(&c.id)
                .bind(&workspace_id.0)
                .fetch_optional(self.read_pool())
                .await
                .map_err(|e| Error::Internal(format!("read comment extras failed: {e}")))?
                .and_then(|r| r.get::<Option<String>, _>("extra_json"));
        if let Some(raw) = existing {
            if let Ok(Value::Object(old)) = serde_json::from_str::<Value>(&raw) {
                for (k, v) in old {
                    // A non-bool `isOrphaned` can only be a legacy value the
                    // importer preserved verbatim (the store itself only ever
                    // encodes booleans here) — carry it over too.
                    let legacy_orphaned = k == "isOrphaned" && !matches!(v, Value::Bool(_));
                    if !ExtraFields::KNOWN_KEYS.contains(&k.as_str()) || legacy_orphaned {
                        merged.entry(k).or_insert(v);
                    }
                }
            }
        }
        let extra_json = extra_map_to_json(merged)?;
        let res = sqlx::query(
            "UPDATE comment SET thread_id=?, note_id=?, kind=?, content=?, author=?, \
             author_type=?, status=?, parent_id=?, anchor_json=?, anchor_text=?, extra_json=?, \
             updated_at=? WHERE id=? AND workspace_id=?",
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
        .bind(&workspace_id.0)
        .execute(self.write_pool())
        .await
        .map_err(|e| Error::Internal(format!("update comment failed: {e}")))?;
        if res.rows_affected() == 0 {
            return Err(Error::NotFound(format!("comment {}", c.id)));
        }
        Ok(())
    }

    /// Delete a comment by id, scoped to `workspace_id` (defense-in-depth).
    /// `NotFound` if the row is absent or the workspace does not match.
    ///
    /// # Errors
    ///
    /// Returns `Error::NotFound` if the comment does not exist in the workspace; `Error::Internal` if the database operation fails.
    pub async fn delete_comment(&self, workspace_id: &WorkspaceId, id: &str) -> Result<()> {
        let res = sqlx::query("DELETE FROM comment WHERE id = ? AND workspace_id = ?")
            .bind(id)
            .bind(&workspace_id.0)
            .execute(self.write_pool())
            .await
            .map_err(|e| Error::Internal(format!("delete comment failed: {e}")))?;
        if res.rows_affected() == 0 {
            return Err(Error::NotFound(format!("comment {id}")));
        }
        Ok(())
    }

    /// List a note's comments, ordered by creation time.
    ///
    /// # Errors
    ///
    /// Returns `Error::Internal` if the database operation fails.
    pub async fn list_comments(&self, note_id: &NoteId) -> Result<Vec<Comment>> {
        let sql = format!(
            "SELECT {COMMENT_COLUMNS} FROM comment WHERE note_id = ? ORDER BY created_at, id"
        );
        let rows = sqlx::query(&sql)
            .bind(&note_id.0)
            .fetch_all(self.read_pool())
            .await
            .map_err(|e| Error::Internal(format!("list comments failed: {e}")))?;
        rows.iter().map(map_comment_row).collect()
    }

    /// List a note's comments scoped to `workspace_id`, ordered by creation
    /// time. Callers that resolve a caller-supplied `comment_id` from the
    /// result set must use this variant so a cross-workspace bare-id probe
    /// cannot match a comment belonging to a different workspace's note that
    /// happens to share the same `note_id` (e.g. the well-known `spec` id).
    ///
    /// # Errors
    ///
    /// Returns `Error::Internal` if the database operation fails.
    pub async fn list_comments_in_workspace(
        &self,
        workspace_id: &WorkspaceId,
        note_id: &NoteId,
    ) -> Result<Vec<Comment>> {
        let sql = format!(
            "SELECT {COMMENT_COLUMNS} FROM comment \
             WHERE note_id = ? AND workspace_id = ? ORDER BY created_at, id"
        );
        let rows = sqlx::query(&sql)
            .bind(&note_id.0)
            .bind(&workspace_id.0)
            .fetch_all(self.read_pool())
            .await
            .map_err(|e| Error::Internal(format!("list comments failed: {e}")))?;
        rows.iter().map(map_comment_row).collect()
    }

    /// List the comments in one thread, ordered by creation time.
    pub(crate) async fn list_thread_comments(&self, thread_id: &str) -> Result<Vec<Comment>> {
        let sql = format!(
            "SELECT {COMMENT_COLUMNS} FROM comment WHERE thread_id = ? ORDER BY created_at, id"
        );
        let rows = sqlx::query(&sql)
            .bind(thread_id)
            .fetch_all(self.read_pool())
            .await
            .map_err(|e| Error::Internal(format!("list thread comments failed: {e}")))?;
        rows.iter().map(map_comment_row).collect()
    }

    /// Set the `status` of every comment in a thread, refreshing `updated_at`.
    /// Scoped to `workspace_id` (defense-in-depth) so a caller bound to
    /// workspace B cannot resolve a thread owned by workspace A. Returns the
    /// number of rows updated (0 when the thread does not exist in that
    /// workspace).
    ///
    /// # Errors
    ///
    /// Returns `Error::Internal` if the database operation fails.
    pub async fn set_thread_status(
        &self,
        workspace_id: &WorkspaceId,
        thread_id: &str,
        status: CommentStatus,
        updated_at: &str,
    ) -> Result<u64> {
        let res = sqlx::query(
            "UPDATE comment SET status=?, updated_at=? WHERE thread_id=? AND workspace_id=?",
        )
        .bind(enum_to_db(&status)?)
        .bind(updated_at)
        .bind(thread_id)
        .bind(&workspace_id.0)
        .execute(self.write_pool())
        .await
        .map_err(|e| Error::Internal(format!("set thread status failed: {e}")))?;
        Ok(res.rows_affected())
    }

    /// Assemble a [`CommentThread`] (the comments sharing `thread_id`).
    ///
    /// # Errors
    ///
    /// Returns `Error::NotFound` if no comments share `thread_id`; `Error::Internal` if the database query fails.
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
    // Replies store `null` (they anchor via their thread/parent, monorepo#729);
    // legacy reply rows and all roots store the anchor object.
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
        is_orphaned: extra.is_orphaned,
        created_at: col(row, "created_at")?,
        updated_at: col(row, "updated_at")?,
    })
}
