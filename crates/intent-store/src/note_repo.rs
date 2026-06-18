//! Note repository: insert + list, mapping rows ↔ [`Note`] (§9.2).

use intent_core::{ContentType, Error, Note, NoteId, NoteVisibility, Result, WorkspaceId};
use sqlx::sqlite::SqliteRow;
use sqlx::Row;

use crate::{enum_from_db, enum_to_db, tags_from_db, tags_to_db, Store};

const NOTE_COLUMNS: &str = "id, workspace_id, title, content, content_type, tags, is_pinned, \
    is_archived, is_default, parent_id, visibility, task_json, created_at, updated_at";

impl Store {
    /// Insert a note row. `task` is stored opaquely as `task_json` TEXT.
    pub async fn insert_note(&self, note: &Note) -> Result<()> {
        let parent_id = note.parent_id.as_ref().map(|n| n.0.clone());
        let task_json = match &note.task {
            Some(v) => Some(
                serde_json::to_string(v)
                    .map_err(|e| Error::Internal(format!("encode task_json failed: {e}")))?,
            ),
            None => None,
        };
        let sql = format!("INSERT INTO note ({NOTE_COLUMNS}) VALUES (?,?,?,?,?,?,?,?,?,?,?,?,?,?)");
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
    let task = match task_json {
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
        task,
        created_at: col(row, "created_at")?,
        updated_at: col(row, "updated_at")?,
    })
}
