//! Note version repository: full-snapshot version history backing the
//! `note.listVersions` / `note.getVersion` / `note.restoreVersion` methods
//! (PROTOCOL §5.2 version-history extensions). Every captured version stores
//! the complete note content; append prunes to the newest
//! [`MAX_NOTE_VERSIONS`].

use intent_core::{
    Error, Note, NoteId, NoteVersion, NoteVersionAuthor, NoteVersionSummary, Result, WorkspaceId,
};
use sqlx::sqlite::SqliteRow;
use sqlx::Row;

use crate::Store;

/// Prune-on-append cap, mirroring the FE `VERSION_CONFIG.MAX_VERSIONS`.
pub(crate) const MAX_NOTE_VERSIONS: i64 = 50;

impl Store {
    /// Append a full-snapshot version of `note` (its *current* content) and
    /// prune to the newest [`MAX_NOTE_VERSIONS`]. Returns the new version
    /// number (1-based, strictly increasing per note).
    pub async fn append_note_version(
        &self,
        note: &Note,
        author: &NoteVersionAuthor,
        date: &str,
    ) -> Result<i64> {
        // IMMEDIATE mode: acquires write lock upfront, avoiding the
        // DEFERRED-mode transaction-upgrade race that surfaces SQLITE_BUSY
        // when concurrent connections hold read locks (STAB-1). The upgrade
        // path is outside `busy_timeout`'s retry scope; IMMEDIATE acquisition
        // is retried by the handler.
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
            let next_v: i64 = sqlx::query(
                "SELECT COALESCE(MAX(v), 0) + 1 AS v FROM note_version \
                 WHERE note_id = ? AND workspace_id = ?",
            )
            .bind(&note.id.0)
            .bind(&note.workspace_id.0)
            .fetch_one(&mut *conn)
            .await
            .map_err(|e| Error::Internal(format!("next note_version failed: {e}")))?
            .try_get("v")
            .map_err(|e| Error::Internal(format!("column v: {e}")))?;

            sqlx::query(
                "INSERT INTO note_version (note_id, workspace_id, v, date, author_id, author_name, \
                 author_type, title, content) VALUES (?,?,?,?,?,?,?,?,?)",
            )
            .bind(&note.id.0)
            .bind(&note.workspace_id.0)
            .bind(next_v)
            .bind(date)
            .bind(&author.id)
            .bind(&author.name)
            .bind(&author.author_type)
            .bind(&note.title)
            .bind(&note.content)
            .execute(&mut *conn)
            .await
            .map_err(|e| Error::Internal(format!("insert note_version failed: {e}")))?;

            sqlx::query(
                "DELETE FROM note_version WHERE note_id = ? AND workspace_id = ? AND v <= ?",
            )
            .bind(&note.id.0)
            .bind(&note.workspace_id.0)
            .bind(next_v - MAX_NOTE_VERSIONS)
            .execute(&mut *conn)
            .await
            .map_err(|e| Error::Internal(format!("prune note_version failed: {e}")))?;

            Ok(next_v)
        }
        .await;

        // COMMIT on success (with rollback, and detach+close on double
        // failure, if the COMMIT itself fails — monorepo#657) or roll back
        // the failed body (monorepo#680), so the sole write-pool connection
        // is never returned holding an open transaction.
        crate::commit_with_rollback_guard(conn, result, "commit note_version tx failed").await
    }

    /// List a note's stored versions ascending by `v`, without content blobs
    /// (`content_length` is computed in SQL). Scoped by
    /// `(workspace_id, note_id)` (migration 0030 composite FK) so a same-id
    /// note in another workspace cannot leak its version history.
    pub async fn list_note_versions(
        &self,
        workspace_id: &WorkspaceId,
        note_id: &NoteId,
    ) -> Result<Vec<NoteVersionSummary>> {
        let rows = sqlx::query(
            "SELECT v, date, author_id, author_name, author_type, title, \
             LENGTH(content) AS content_length FROM note_version \
             WHERE note_id = ? AND workspace_id = ? ORDER BY v",
        )
        .bind(&note_id.0)
        .bind(&workspace_id.0)
        .fetch_all(self.read_pool())
        .await
        .map_err(|e| Error::Internal(format!("list note_versions failed: {e}")))?;
        rows.iter().map(map_summary_row).collect()
    }

    /// Fetch one stored version (with content), or `NotFound`. Scoped by
    /// `(workspace_id, note_id)`.
    pub async fn get_note_version(
        &self,
        workspace_id: &WorkspaceId,
        note_id: &NoteId,
        v: i64,
    ) -> Result<NoteVersion> {
        let row = sqlx::query(
            "SELECT v, date, author_id, author_name, author_type, title, content \
             FROM note_version WHERE note_id = ? AND workspace_id = ? AND v = ?",
        )
        .bind(&note_id.0)
        .bind(&workspace_id.0)
        .bind(v)
        .fetch_optional(self.read_pool())
        .await
        .map_err(|e| Error::Internal(format!("get note_version failed: {e}")))?;
        match row {
            Some(r) => map_version_row(&r),
            None => Err(Error::NotFound(format!("note version {note_id}@{v}"))),
        }
    }
}

fn col<'r, T>(row: &'r SqliteRow, name: &str) -> Result<T>
where
    T: sqlx::Decode<'r, sqlx::Sqlite> + sqlx::Type<sqlx::Sqlite>,
{
    row.try_get::<T, _>(name)
        .map_err(|e| Error::Internal(format!("column {name}: {e}")))
}

fn map_author(row: &SqliteRow) -> Result<NoteVersionAuthor> {
    Ok(NoteVersionAuthor {
        id: col(row, "author_id")?,
        name: col(row, "author_name")?,
        author_type: col(row, "author_type")?,
    })
}

fn map_summary_row(row: &SqliteRow) -> Result<NoteVersionSummary> {
    Ok(NoteVersionSummary {
        entry_type: "snapshot".to_string(),
        v: col(row, "v")?,
        date: col(row, "date")?,
        author: map_author(row)?,
        title: col(row, "title")?,
        content_length: col(row, "content_length")?,
    })
}

fn map_version_row(row: &SqliteRow) -> Result<NoteVersion> {
    Ok(NoteVersion {
        entry_type: "snapshot".to_string(),
        v: col(row, "v")?,
        date: col(row, "date")?,
        author: map_author(row)?,
        title: col(row, "title")?,
        content: col(row, "content")?,
    })
}
