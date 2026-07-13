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

    /// Fetch a single note by (workspace, id), or `NotFound`. Note identity is
    /// composite (`(id, workspace_id)`, migration 0030) so callers must supply
    /// both halves; bare-id lookups would silently match a same-id note owned
    /// by another workspace.
    pub async fn get_note(&self, workspace_id: &WorkspaceId, id: &NoteId) -> Result<Note> {
        let sql = format!("SELECT {NOTE_COLUMNS} FROM note WHERE id = ? AND workspace_id = ?");
        let row = sqlx::query(&sql)
            .bind(&id.0)
            .bind(&workspace_id.0)
            .fetch_optional(self.pool())
            .await
            .map_err(|e| Error::Internal(format!("get note failed: {e}")))?;
        match row {
            Some(r) => map_note_row(&r),
            None => Err(Error::NotFound(format!("note {id}"))),
        }
    }

    /// Update an existing note (full-row replace, except `id` and
    /// `workspace_id`), or `NotFound`. Unconditional last-writer-wins bump of
    /// `rev`; `metadata.task` is stored opaquely as `task_json` TEXT. The write
    /// is scoped by the note's own `workspace_id` so same-id notes across
    /// workspaces never collide.
    pub async fn update_note(&self, note: &Note) -> Result<()> {
        self.update_note_versioned(note, None).await
    }

    /// Update an existing note, optionally gating on `expected_version`
    /// (optimistic concurrency, PROTOCOL §5.6). Scoped by
    /// `(id, workspace_id)` (migration 0030 composite PK). When
    /// `expected_version` is `Some(rev)`, the write is a conditional
    /// `... WHERE id = ? AND workspace_id = ? AND rev = ?` that only succeeds
    /// if the stored `rev` matches; on a 0-row result the row is re-read to
    /// distinguish a [`Error::Conflict`] (row present, carrying the current
    /// entity) from a [`Error::NotFound`] (row absent). When `None`, this is
    /// the unconditional last-writer-wins bump. In all cases `rev` increments.
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
            "UPDATE note SET title=?, content=?, content_type=?, tags=?, \
             is_pinned=?, is_archived=?, is_default=?, parent_id=?, visibility=?, task_json=?, \
             created_at=?, updated_at=?, rev = rev + 1 WHERE id=? AND workspace_id=?",
        );
        if expected_version.is_some() {
            sql.push_str(" AND rev=?");
        }
        let mut query = sqlx::query(&sql)
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
            .bind(&note.workspace_id.0);
        if let Some(rev) = expected_version {
            query = query.bind(rev);
        }
        let res = query
            .execute(self.pool())
            .await
            .map_err(|e| Error::Internal(format!("update note failed: {e}")))?;
        if res.rows_affected() == 0 {
            // Re-read by composite key: a present row means the
            // `expected_version` gate failed (conflict); an absent row is a
            // genuine not-found.
            return match self.get_note(&note.workspace_id, &note.id).await {
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

    /// Delete a note by (workspace, id), unconditional. `NotFound` if absent.
    pub async fn delete_note(&self, workspace_id: &WorkspaceId, id: &NoteId) -> Result<()> {
        self.delete_note_versioned(workspace_id, id, None).await
    }

    /// Delete a note, optionally gating on `expected_version` (optimistic
    /// concurrency, PROTOCOL §5.6). Scoped by `(id, workspace_id)` (migration
    /// 0030 composite PK). When `expected_version` is `Some(rev)`, the delete
    /// is conditional (`... WHERE id = ? AND workspace_id = ? AND rev = ?`);
    /// on a 0-row result the row is re-read to distinguish a
    /// [`Error::Conflict`] (row present, carrying the current entity snapshot
    /// prior to deletion) from a [`Error::NotFound`] (row absent). When
    /// `None`, this is the unconditional delete.
    pub async fn delete_note_versioned(
        &self,
        workspace_id: &WorkspaceId,
        id: &NoteId,
        expected_version: Option<i64>,
    ) -> Result<()> {
        let res = match expected_version {
            Some(rev) => {
                sqlx::query("DELETE FROM note WHERE id = ? AND workspace_id = ? AND rev = ?")
                    .bind(&id.0)
                    .bind(&workspace_id.0)
                    .bind(rev)
                    .execute(self.pool())
                    .await
                    .map_err(|e| Error::Internal(format!("delete note failed: {e}")))?
            }
            None => sqlx::query("DELETE FROM note WHERE id = ? AND workspace_id = ?")
                .bind(&id.0)
                .bind(&workspace_id.0)
                .execute(self.pool())
                .await
                .map_err(|e| Error::Internal(format!("delete note failed: {e}")))?,
        };
        if res.rows_affected() == 0 {
            // Re-read by composite key: a present row means the
            // `expected_version` gate failed (conflict, carrying the current
            // snapshot); an absent row is a genuine not-found.
            return match self.get_note(workspace_id, id).await {
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

    /// Self-heal for workspaces damaged by the pre-#110 global-note-identity
    /// bug: if the workspace has no `id='spec'` note but *exactly one*
    /// top-level (`parent_id IS NULL`), non-task (`task_json IS NULL`),
    /// non-archived note titled "Spec" (trim + case-insensitive), adopt it as
    /// the reserved spec in a single transaction — rewrite `note.id` to
    /// `'spec'`, re-parent children, move `note_version`,
    /// `note_line_attribution`, and `comment` rows to the new id, set
    /// `is_pinned=1, is_default=1`, and ensure the `spec` tag is present.
    /// Foreign keys are deferred to commit so the referenced key can be
    /// rewritten alongside its dependents (§9.4 keeps `foreign_keys = ON`
    /// outside this window). Returns `Some((old_id, title))` on adoption, or
    /// `None` when zero or ≥2 candidates match — both fall through to the
    /// caller's empty-seed path. The caller (`ensure_spec_note`) has already
    /// confirmed no `id='spec'` note exists; a concurrent adoption race is
    /// resolved by the composite PK on `(id, workspace_id)`.
    pub async fn adopt_stray_spec_note(
        &self,
        workspace_id: &WorkspaceId,
    ) -> Result<Option<(NoteId, String)>> {
        let mut tx = self
            .pool()
            .begin()
            .await
            .map_err(|e| Error::Internal(format!("begin adopt_spec tx failed: {e}")))?;
        // Scan for candidates inside the tx so the caller's "no `id='spec'`"
        // precondition still holds at commit. `LIMIT 2` is enough to
        // distinguish "exactly one" from "≥2".
        let rows = sqlx::query(
            "SELECT id, title, tags FROM note \
             WHERE workspace_id = ? \
               AND id != 'spec' \
               AND parent_id IS NULL \
               AND task_json IS NULL \
               AND is_archived = 0 \
               AND LOWER(TRIM(title)) = 'spec' \
             LIMIT 2",
        )
        .bind(&workspace_id.0)
        .fetch_all(&mut *tx)
        .await
        .map_err(|e| Error::Internal(format!("scan stray spec candidates failed: {e}")))?;
        if rows.len() != 1 {
            tx.rollback()
                .await
                .map_err(|e| Error::Internal(format!("rollback adopt_spec tx failed: {e}")))?;
            return Ok(None);
        }
        let row = &rows[0];
        let old_id: String = col(row, "id")?;
        let title: String = col(row, "title")?;
        let existing_tags: Vec<String> = tags_from_db(&col::<String>(row, "tags")?)?;
        let mut new_tags = existing_tags;
        if !new_tags.iter().any(|t| t == "spec") {
            new_tags.push("spec".to_string());
        }
        let tags_json = tags_to_db(&new_tags)?;
        // Defer FK enforcement to commit so the composite `(note_id,
        // workspace_id)` FKs on note_version / note_line_attribution /
        // comment stay consistent while we rewrite the referenced key.
        sqlx::query("PRAGMA defer_foreign_keys = ON")
            .execute(&mut *tx)
            .await
            .map_err(|e| Error::Internal(format!("defer FKs failed: {e}")))?;
        // Belt-and-braces alongside `id != 'spec'` in the SELECT: if another
        // transaction adopted the same stray between our SELECT and this
        // UPDATE, the row would already be gone and `rows_affected` would be
        // 0 — bail out rather than emit spurious delete/create events for a
        // no-op rewrite.
        let rewrite = sqlx::query(
            "UPDATE note SET id = 'spec', is_pinned = 1, is_default = 1, tags = ? \
             WHERE id = ? AND workspace_id = ? AND id != 'spec'",
        )
        .bind(&tags_json)
        .bind(&old_id)
        .bind(&workspace_id.0)
        .execute(&mut *tx)
        .await
        .map_err(|e| Error::Internal(format!("rewrite spec note id failed: {e}")))?;
        if rewrite.rows_affected() != 1 {
            tx.rollback()
                .await
                .map_err(|e| Error::Internal(format!("rollback adopt_spec tx failed: {e}")))?;
            return Ok(None);
        }
        sqlx::query("UPDATE note SET parent_id = 'spec' WHERE parent_id = ? AND workspace_id = ?")
            .bind(&old_id)
            .bind(&workspace_id.0)
            .execute(&mut *tx)
            .await
            .map_err(|e| Error::Internal(format!("reparent spec children failed: {e}")))?;
        sqlx::query(
            "UPDATE note_version SET note_id = 'spec' WHERE note_id = ? AND workspace_id = ?",
        )
        .bind(&old_id)
        .bind(&workspace_id.0)
        .execute(&mut *tx)
        .await
        .map_err(|e| Error::Internal(format!("move spec versions failed: {e}")))?;
        sqlx::query(
            "UPDATE note_line_attribution SET note_id = 'spec' \
             WHERE note_id = ? AND workspace_id = ?",
        )
        .bind(&old_id)
        .bind(&workspace_id.0)
        .execute(&mut *tx)
        .await
        .map_err(|e| Error::Internal(format!("move spec line attribution failed: {e}")))?;
        sqlx::query("UPDATE comment SET note_id = 'spec' WHERE note_id = ? AND workspace_id = ?")
            .bind(&old_id)
            .bind(&workspace_id.0)
            .execute(&mut *tx)
            .await
            .map_err(|e| Error::Internal(format!("move spec comments failed: {e}")))?;
        tx.commit()
            .await
            .map_err(|e| Error::Internal(format!("commit adopt_spec tx failed: {e}")))?;
        Ok(Some((NoteId(old_id), title)))
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
