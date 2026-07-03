//! Note version repository: full-snapshot version history backing the
//! `note.listVersions` / `note.getVersion` / `note.restoreVersion` methods
//! (PROTOCOL §5.2 version-history extensions). Every captured version stores
//! the complete note content; append prunes to the newest
//! [`MAX_NOTE_VERSIONS`].

use intent_core::{
    Error, Note, NoteId, NoteVersion, NoteVersionAuthor, NoteVersionSummary, Result,
};
use sqlx::sqlite::SqliteRow;
use sqlx::Row;

use crate::Store;

/// Prune-on-append cap, mirroring the FE `VERSION_CONFIG.MAX_VERSIONS`.
pub const MAX_NOTE_VERSIONS: i64 = 50;

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
        let mut tx = self
            .pool()
            .begin()
            .await
            .map_err(|e| Error::Internal(format!("begin note_version tx failed: {e}")))?;
        let next_v: i64 =
            sqlx::query("SELECT COALESCE(MAX(v), 0) + 1 AS v FROM note_version WHERE note_id = ?")
                .bind(&note.id.0)
                .fetch_one(&mut *tx)
                .await
                .map_err(|e| Error::Internal(format!("next note_version failed: {e}")))?
                .try_get("v")
                .map_err(|e| Error::Internal(format!("column v: {e}")))?;
        sqlx::query(
            "INSERT INTO note_version (note_id, v, date, author_id, author_name, author_type, \
             title, content) VALUES (?,?,?,?,?,?,?,?)",
        )
        .bind(&note.id.0)
        .bind(next_v)
        .bind(date)
        .bind(&author.id)
        .bind(&author.name)
        .bind(&author.author_type)
        .bind(&note.title)
        .bind(&note.content)
        .execute(&mut *tx)
        .await
        .map_err(|e| Error::Internal(format!("insert note_version failed: {e}")))?;
        sqlx::query("DELETE FROM note_version WHERE note_id = ? AND v <= ?")
            .bind(&note.id.0)
            .bind(next_v - MAX_NOTE_VERSIONS)
            .execute(&mut *tx)
            .await
            .map_err(|e| Error::Internal(format!("prune note_version failed: {e}")))?;
        tx.commit()
            .await
            .map_err(|e| Error::Internal(format!("commit note_version tx failed: {e}")))?;
        Ok(next_v)
    }

    /// List a note's stored versions ascending by `v`, without content blobs
    /// (`content_length` is computed in SQL).
    pub async fn list_note_versions(&self, note_id: &NoteId) -> Result<Vec<NoteVersionSummary>> {
        let rows = sqlx::query(
            "SELECT v, date, author_id, author_name, author_type, title, \
             LENGTH(content) AS content_length FROM note_version WHERE note_id = ? ORDER BY v",
        )
        .bind(&note_id.0)
        .fetch_all(self.pool())
        .await
        .map_err(|e| Error::Internal(format!("list note_versions failed: {e}")))?;
        rows.iter().map(map_summary_row).collect()
    }

    /// Fetch one stored version (with content), or `NotFound`.
    pub async fn get_note_version(&self, note_id: &NoteId, v: i64) -> Result<NoteVersion> {
        let row = sqlx::query(
            "SELECT v, date, author_id, author_name, author_type, title, content \
             FROM note_version WHERE note_id = ? AND v = ?",
        )
        .bind(&note_id.0)
        .bind(v)
        .fetch_optional(self.pool())
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
