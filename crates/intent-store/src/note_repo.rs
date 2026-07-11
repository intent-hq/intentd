//! Note repository: insert + list, mapping rows ↔ [`Note`] (§9.2).

use intent_core::{
    ContentType, Error, Note, NoteId, NoteMetadata, NoteVisibility, Result, TaskMetadata,
    WorkspaceId,
};
use sqlx::sqlite::SqliteRow;
use sqlx::Row;

use crate::{enum_from_db, enum_to_db, tags_from_db, tags_to_db, Store};

const NOTE_COLUMNS: &str = "id, workspace_id, title, content, content_type, tags, is_pinned, \
    is_archived, is_default, parent_id, visibility, task_json, created_at, rev, updated_at";

impl Store {
    /// Insert a note row. `metadata.task` is stored opaquely as `task_json` TEXT.
    pub async fn insert_note(&self, note: &Note) -> Result<()> {
        let parent_id = note.parent_id.as_ref().map(|n| n.0.clone());
        let task_json = match &note.metadata.task {
            Some(v) => Some(
                serde_json::to_string(v)
                    .map_err(|e| Error::Internal(format!("encode task_json failed: {e}")))?,
            ),
            None => None,
        };
        let sql =
            format!("INSERT INTO note ({NOTE_COLUMNS}) VALUES (?,?,?,?,?,?,?,?,?,?,?,?,?,?,?)");
        sqlx::query(&sql)
            .bind(&note.id.0)
            .bind(&note.workspace_id.0)
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
            .bind(note.rev)
            .bind(&note.updated_at)
            .execute(self.pool())
            .await
            .map_err(|e| Error::Internal(format!("insert note failed: {e}")))?;
        Ok(())
    }

    /// List notes in a workspace, ordered by creation time.
    pub async fn list_notes(&self, workspace_id: &WorkspaceId) -> Result<Vec<Note>> {
        let sql =
            format!("SELECT {NOTE_COLUMNS} FROM note WHERE workspace_id = ? ORDER BY created_at");
        let rows = sqlx::query(&sql)
            .bind(&workspace_id.0)
            .fetch_all(self.pool())
            .await
            .map_err(|e| Error::Internal(format!("list notes failed: {e}")))?;
        rows.iter().map(map_note_row).collect()
    }

    /// List every note across all workspaces, oldest first. Backs the global
    /// `search.notes` adapter (PROTOCOL §5.15), which has no `workspaceId`.
    pub async fn list_all_notes(&self) -> Result<Vec<Note>> {
        let sql = format!("SELECT {NOTE_COLUMNS} FROM note ORDER BY created_at");
        let rows = sqlx::query(&sql)
            .fetch_all(self.pool())
            .await
            .map_err(|e| Error::Internal(format!("list all notes failed: {e}")))?;
        rows.iter().map(map_note_row).collect()
    }

    /// Fetch a single note by id, or `NotFound`.
    pub async fn get_note(&self, id: &NoteId) -> Result<Note> {
        let sql = format!("SELECT {NOTE_COLUMNS} FROM note WHERE id = ?");
        let row = sqlx::query(&sql)
            .bind(&id.0)
            .fetch_optional(self.pool())
            .await
            .map_err(|e| Error::Internal(format!("get note failed: {e}")))?;
        match row {
            Some(r) => map_note_row(&r),
            None => Err(Error::NotFound(format!("note {id}"))),
        }
    }

    /// Update an existing note (full-row replace, except `id`), or `NotFound`.
    /// Unconditional last-writer-wins bump of `rev`; `metadata.task` is stored
    /// opaquely as `task_json` TEXT.
    pub async fn update_note(&self, note: &Note) -> Result<()> {
        self.update_note_versioned(note, None).await
    }

    /// Update an existing note, optionally gating on `expected_version`
    /// (optimistic concurrency, PROTOCOL §5.6). When `expected_version` is
    /// `Some(rev)`, the write is a conditional `... WHERE id = ? AND rev = ?`
    /// that only succeeds if the stored `rev` matches; on a 0-row result the row
    /// is re-read to distinguish a [`Error::Conflict`] (row present, carrying the
    /// current entity) from a [`Error::NotFound`] (row absent). When `None`, this
    /// is the unconditional last-writer-wins bump. In all cases `rev` increments.
    pub async fn update_note_versioned(
        &self,
        note: &Note,
        expected_version: Option<i64>,
    ) -> Result<()> {
        let parent_id = note.parent_id.as_ref().map(|n| n.0.clone());
        let task_json = match &note.metadata.task {
            Some(v) => Some(
                serde_json::to_string(v)
                    .map_err(|e| Error::Internal(format!("encode task_json failed: {e}")))?,
            ),
            None => None,
        };
        let mut sql = String::from(
            "UPDATE note SET workspace_id=?, title=?, content=?, content_type=?, tags=?, \
             is_pinned=?, is_archived=?, is_default=?, parent_id=?, visibility=?, task_json=?, \
             created_at=?, updated_at=?, rev = rev + 1 WHERE id=?",
        );
        if expected_version.is_some() {
            sql.push_str(" AND rev=?");
        }
        let mut query = sqlx::query(&sql)
            .bind(&note.workspace_id.0)
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
            .bind(&note.id.0);
        if let Some(rev) = expected_version {
            query = query.bind(rev);
        }
        let res = query
            .execute(self.pool())
            .await
            .map_err(|e| Error::Internal(format!("update note failed: {e}")))?;
        if res.rows_affected() == 0 {
            // Re-read by id: a present row means the `expected_version` gate
            // failed (conflict); an absent row is a genuine not-found.
            return match self.get_note(&note.id).await {
                Ok(current) => {
                    let current = serde_json::to_value(&current)
                        .map_err(|e| Error::Internal(format!("encode current note failed: {e}")))?;
                    Err(Error::Conflict { current })
                }
                Err(Error::NotFound(_)) => Err(Error::NotFound(format!("note {}", note.id))),
                Err(e) => Err(e),
            };
        }
        Ok(())
    }

    /// Delete a note by id (unconditional), or `NotFound`.
    pub async fn delete_note(&self, id: &NoteId) -> Result<()> {
        self.delete_note_versioned(id, None).await
    }

    /// Delete a note, optionally gating on `expected_version` (optimistic
    /// concurrency, PROTOCOL §5.6). When `expected_version` is `Some(rev)`, the
    /// delete is conditional (`... WHERE id = ? AND rev = ?`); on a 0-row result
    /// the row is re-read to distinguish a [`Error::Conflict`] (row present,
    /// carrying the current entity snapshot prior to deletion) from a
    /// [`Error::NotFound`] (row absent). When `None`, this is the unconditional
    /// delete.
    pub async fn delete_note_versioned(
        &self,
        id: &NoteId,
        expected_version: Option<i64>,
    ) -> Result<()> {
        let res = match expected_version {
            Some(rev) => sqlx::query("DELETE FROM note WHERE id = ? AND rev = ?")
                .bind(&id.0)
                .bind(rev)
                .execute(self.pool())
                .await
                .map_err(|e| Error::Internal(format!("delete note failed: {e}")))?,
            None => sqlx::query("DELETE FROM note WHERE id = ?")
                .bind(&id.0)
                .execute(self.pool())
                .await
                .map_err(|e| Error::Internal(format!("delete note failed: {e}")))?,
        };
        if res.rows_affected() == 0 {
            // Re-read by id: a present row means the `expected_version` gate
            // failed (conflict, carrying the current snapshot); an absent row is
            // a genuine not-found.
            return match self.get_note(id).await {
                Ok(current) => {
                    let current = serde_json::to_value(&current)
                        .map_err(|e| Error::Internal(format!("encode current note failed: {e}")))?;
                    Err(Error::Conflict { current })
                }
                Err(Error::NotFound(_)) => Err(Error::NotFound(format!("note {id}"))),
                Err(e) => Err(e),
            };
        }
        Ok(())
    }

    /// List a workspace's task notes (those carrying `task_json`), using the
    /// `idx_note_task` partial index. Ordered by creation time.
    pub async fn list_tasks(&self, workspace_id: &WorkspaceId) -> Result<Vec<Note>> {
        let sql = format!(
            "SELECT {NOTE_COLUMNS} FROM note WHERE workspace_id = ? AND task_json IS NOT NULL \
             ORDER BY created_at"
        );
        let rows = sqlx::query(&sql)
            .bind(&workspace_id.0)
            .fetch_all(self.pool())
            .await
            .map_err(|e| Error::Internal(format!("list tasks failed: {e}")))?;
        rows.iter().map(map_note_row).collect()
    }
}

fn col<'r, T>(row: &'r SqliteRow, name: &str) -> Result<T>
where
    T: sqlx::Decode<'r, sqlx::Sqlite> + sqlx::Type<sqlx::Sqlite>,
{
    row.try_get::<T, _>(name)
        .map_err(|e| Error::Internal(format!("column {name}: {e}")))
}

fn map_note_row(row: &SqliteRow) -> Result<Note> {
    let parent_id: Option<String> = col(row, "parent_id")?;
    let task_json: Option<String> = col(row, "task_json")?;
    let task: Option<TaskMetadata> = match task_json {
        Some(s) => Some(
            serde_json::from_str(&s)
                .map_err(|e| Error::Internal(format!("decode task_json failed: {e}")))?,
        ),
        None => None,
    };
    Ok(Note {
        id: NoteId(col(row, "id")?),
        workspace_id: WorkspaceId(col(row, "workspace_id")?),
        title: col(row, "title")?,
        content: col(row, "content")?,
        content_type: enum_from_db::<ContentType>(&col::<String>(row, "content_type")?)?,
        tags: tags_from_db(&col::<String>(row, "tags")?)?,
        is_pinned: col::<i64>(row, "is_pinned")? != 0,
        is_archived: col::<i64>(row, "is_archived")? != 0,
        is_default: col::<i64>(row, "is_default")? != 0,
        parent_id: parent_id.map(NoteId),
        visibility: enum_from_db::<NoteVisibility>(&col::<String>(row, "visibility")?)?,
        metadata: NoteMetadata { task },
        created_at: col(row, "created_at")?,
        rev: col::<i64>(row, "rev")?,
        updated_at: col(row, "updated_at")?,
    })
}
