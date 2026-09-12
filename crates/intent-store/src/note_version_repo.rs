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
    /// prune to the newest [`MAX_NOTE_VERSIONS`]. `rev` is the note's
    /// post-write `rev` (the value the persisted row now carries), recorded
    /// on the snapshot so [`Store::get_note_version_content_by_rev`] can
    /// recover a writer's base. Returns the new version number (1-based,
    /// strictly increasing per note).
    ///
    /// # Errors
    ///
    /// Returns `Error::Internal` if the database operation fails.
    pub async fn append_note_version(
        &self,
        note: &Note,
        author: &NoteVersionAuthor,
        date: &str,
        rev: i64,
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
        let result = insert_note_version(&mut conn, note, author, date, rev).await;

        // COMMIT on success (with rollback, and detach+close on double
        // failure, if the COMMIT itself fails — monorepo#657) or roll back
        // the failed body (monorepo#680), so the sole write-pool connection
        // is never returned holding an open transaction.
        crate::commit_with_rollback_guard(conn, result, "commit note_version tx failed").await
    }

    /// Persist a note content write and its version snapshot in ONE
    /// transaction: the same conditional UPDATE as
    /// [`Store::update_note_versioned`] (gated on `expected_version` when
    /// `Some`) followed by the snapshot [`Store::append_note_version`] would
    /// record for the bumped rev. Committing them together means no reader can
    /// observe the new `rev` on the note row while
    /// [`Store::get_note_version_content_by_rev`] still resolves that rev to
    /// the previous content, and a snapshot can never land after a later
    /// write's, out of rev order. Returns `(rev, v)`: the post-write rev and
    /// the new version number.
    ///
    /// # Errors
    ///
    /// Returns `Error::Conflict` (carrying the current entity) when `expected_version` is supplied and does not match the stored `rev`; `Error::NotFound` if the note does not exist in the workspace; `Error::Internal` if encoding fields or a statement fails.
    pub async fn update_note_with_version(
        &self,
        note: &Note,
        expected_version: Option<i64>,
        author: &NoteVersionAuthor,
        date: &str,
    ) -> Result<(i64, i64)> {
        let mut conn = self
            .write_pool()
            .acquire()
            .await
            .map_err(|e| Error::Internal(format!("acquire connection failed: {e}")))?;
        sqlx::query("BEGIN IMMEDIATE")
            .execute(&mut *conn)
            .await
            .map_err(|e| Error::Internal(format!("begin IMMEDIATE failed: {e}")))?;

        let result = async {
            let Some(rev) = crate::note_repo::exec_update_note(
                &mut *conn,
                note,
                expected_version,
                crate::note_repo::NoteUpdateScope::FullRow,
            )
            .await?
            else {
                return Ok(None);
            };
            let v = insert_note_version(&mut conn, note, author, date, rev).await?;
            Ok(Some((rev, v)))
        }
        .await;

        match crate::commit_with_rollback_guard(conn, result, "commit note write tx failed").await?
        {
            Some(written) => Ok(written),
            None => Err(self.note_update_miss(note).await),
        }
    }

    /// Persist a parent-note content write together with the child-note
    /// inserts the new content refers to, in ONE transaction: the gated
    /// UPDATE + snapshot of [`Store::update_note_with_version`] for `note`
    /// (gated on `expected_version` when `Some`), then for each of `children`
    /// the row insert + initial snapshot of [`Store::insert_note_with_version`]
    /// (at each child's `rev`, stamped with its `updated_at`). Children exist
    /// only if the parent write lands: a CAS miss commits nothing, so a
    /// caller that re-derives its work from the fresh parent never leaves
    /// orphaned or duplicate children behind (`task.convertBlocks` racing a
    /// save that already converted the same block). Returns `(rev, v)` for
    /// the parent.
    ///
    /// # Errors
    ///
    /// Returns `Error::Conflict` (carrying the current entity) when `expected_version` is supplied and does not match the stored `rev`; `Error::NotFound` if the parent does not exist in the workspace; `Error::Internal` if encoding fields or a statement fails (including a duplicate child `(id, workspace_id)`).
    pub async fn update_note_with_version_and_children(
        &self,
        note: &Note,
        expected_version: Option<i64>,
        children: &[Note],
        author: &NoteVersionAuthor,
        date: &str,
    ) -> Result<(i64, i64)> {
        let mut conn = self
            .write_pool()
            .acquire()
            .await
            .map_err(|e| Error::Internal(format!("acquire connection failed: {e}")))?;
        sqlx::query("BEGIN IMMEDIATE")
            .execute(&mut *conn)
            .await
            .map_err(|e| Error::Internal(format!("begin IMMEDIATE failed: {e}")))?;

        // The gated parent UPDATE runs first so a miss leaves the body with
        // nothing written before the (no-op) commit.
        let result = async {
            let Some(rev) = crate::note_repo::exec_update_note(
                &mut *conn,
                note,
                expected_version,
                crate::note_repo::NoteUpdateScope::FullRow,
            )
            .await?
            else {
                return Ok(None);
            };
            let v = insert_note_version(&mut conn, note, author, date, rev).await?;
            for child in children {
                crate::note_repo::exec_insert_note(&mut *conn, child).await?;
                insert_note_version(&mut conn, child, author, &child.updated_at, child.rev).await?;
            }
            Ok(Some((rev, v)))
        }
        .await;

        match crate::commit_with_rollback_guard(
            conn,
            result,
            "commit note write + children tx failed",
        )
        .await?
        {
            Some(written) => Ok(written),
            None => Err(self.note_update_miss(note).await),
        }
    }

    /// Insert a note row and its initial version snapshot (at `note.rev`) in
    /// ONE transaction, the insert-side counterpart of
    /// [`Store::update_note_with_version`]: the row is never visible while
    /// [`Store::get_note_version_content_by_rev`] cannot yet resolve its rev,
    /// so a writer that reads the fresh note and later sends that rev as a
    /// stale base always merges instead of degrading to last-writer-wins, and
    /// the initial snapshot can never land after a later write's. Returns the
    /// new version number.
    ///
    /// # Errors
    ///
    /// Returns `Error::Internal` if encoding fields or a statement fails (including a duplicate `(id, workspace_id)`).
    pub async fn insert_note_with_version(
        &self,
        note: &Note,
        author: &NoteVersionAuthor,
        date: &str,
    ) -> Result<i64> {
        let mut conn = self
            .write_pool()
            .acquire()
            .await
            .map_err(|e| Error::Internal(format!("acquire connection failed: {e}")))?;
        sqlx::query("BEGIN IMMEDIATE")
            .execute(&mut *conn)
            .await
            .map_err(|e| Error::Internal(format!("begin IMMEDIATE failed: {e}")))?;

        let result = async {
            crate::note_repo::exec_insert_note(&mut *conn, note).await?;
            insert_note_version(&mut conn, note, author, date, note.rev).await
        }
        .await;

        crate::commit_with_rollback_guard(conn, result, "commit note insert tx failed").await
    }

    /// List a note's stored versions ascending by `v`, without content blobs
    /// (`content_length` is computed in SQL). Scoped by
    /// `(workspace_id, note_id)` (migration 0030 composite FK) so a same-id
    /// note in another workspace cannot leak its version history.
    ///
    /// # Errors
    ///
    /// Returns `Error::Internal` if the database operation fails.
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
    ///
    /// # Errors
    ///
    /// Returns `Error::NotFound` if the note version does not exist in the workspace; `Error::Internal` if the database operation fails.
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

    /// The note's content *as of* `rev`: the content of the newest snapshot
    /// whose recorded `rev` is `<= rev` (one range scan on
    /// `idx_note_version_rev`). Every content write snapshots at its
    /// post-write rev, while metadata-only writes bump `rev` without a
    /// snapshot, so the content at rev N is the last content-write snapshot
    /// at or below N — an exact hit for a content rev, the preceding content
    /// for a metadata-only rev. `None` when no such snapshot exists: a rev
    /// older than the oldest retained snapshot (pruned past
    /// [`MAX_NOTE_VERSIONS`] or predating the note) — pre-migration rows
    /// (`rev IS NULL`) never match. This is the writer's base for a
    /// three-way merge.
    ///
    /// # Errors
    ///
    /// Returns `Error::Internal` if the database operation fails.
    pub async fn get_note_version_content_by_rev(
        &self,
        workspace_id: &WorkspaceId,
        note_id: &NoteId,
        rev: i64,
    ) -> Result<Option<String>> {
        sqlx::query_scalar(
            "SELECT content FROM note_version \
             WHERE workspace_id = ? AND note_id = ? AND rev IS NOT NULL AND rev <= ? \
             ORDER BY rev DESC, v DESC LIMIT 1",
        )
        .bind(&workspace_id.0)
        .bind(&note_id.0)
        .bind(rev)
        .fetch_optional(self.read_pool())
        .await
        .map_err(|e| Error::Internal(format!("get note_version by rev failed: {e}")))
    }
}

/// Snapshot body shared by [`Store::append_note_version`] and
/// [`Store::update_note_with_version`]: inside the caller's open transaction,
/// allocate the next `v`, insert the full-content row stamped with `rev`, and
/// prune to the newest [`MAX_NOTE_VERSIONS`]. Returns the new `v`.
pub(crate) async fn insert_note_version(
    conn: &mut sqlx::SqliteConnection,
    note: &Note,
    author: &NoteVersionAuthor,
    date: &str,
    rev: i64,
) -> Result<i64> {
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
         author_type, title, content, rev) VALUES (?,?,?,?,?,?,?,?,?,?)",
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
    .bind(rev)
    .execute(&mut *conn)
    .await
    .map_err(|e| Error::Internal(format!("insert note_version failed: {e}")))?;

    sqlx::query("DELETE FROM note_version WHERE note_id = ? AND workspace_id = ? AND v <= ?")
        .bind(&note.id.0)
        .bind(&note.workspace_id.0)
        .bind(next_v - MAX_NOTE_VERSIONS)
        .execute(&mut *conn)
        .await
        .map_err(|e| Error::Internal(format!("prune note_version failed: {e}")))?;

    Ok(next_v)
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
