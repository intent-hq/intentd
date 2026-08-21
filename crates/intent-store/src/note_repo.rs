//! Note repository: insert + list, mapping rows ↔ [`Note`] (§9.2).

use std::collections::HashSet;

use intent_core::{
    ContentType, Error, Note, NoteId, NoteMetadata, NoteVisibility, Result, TaskMetadata,
    WorkspaceId, WorkspaceTaskStats,
};
use sqlx::sqlite::SqliteRow;
use sqlx::Row;

use crate::{enum_from_db, enum_to_db, tags_from_db, tags_to_db, Store};

const NOTE_COLUMNS: &str = "id, workspace_id, title, content, content_type, tags, is_pinned, \
    is_archived, is_default, parent_id, visibility, task_json, created_at, rev, updated_at";

/// Serialize `metadata.task` for the `task_json` column, stripping the
/// computed `unmet_depends_on` projection so it is never persisted
/// (monorepo#1979) — readers recompute it from `depends_on` + task statuses.
pub(crate) fn encode_task_json(task: &intent_core::TaskMetadata) -> Result<String> {
    let mut stored = task.clone();
    stored.unmet_depends_on = Vec::new();
    serde_json::to_string(&stored)
        .map_err(|e| Error::Internal(format!("encode task_json failed: {e}")))
}

impl Store {
    /// Insert a note row. `metadata.task` is stored opaquely as `task_json` TEXT.
    ///
    /// # Errors
    ///
    /// Returns `Error::Internal` if the database operation fails.
    pub async fn insert_note(&self, note: &Note) -> Result<()> {
        let parent_id = note.parent_id.as_ref().map(|n| n.0.clone());
        let task_json = note
            .metadata
            .task
            .as_ref()
            .map(encode_task_json)
            .transpose()?;
        let sql =
            format!("INSERT INTO note ({NOTE_COLUMNS}) VALUES (?,?,?,?,?,?,?,?,?,?,?,?,?,?,?)");
        sqlx::query(&sql)
            .bind(&note.id.0)
            .bind(&note.workspace_id.0)
            .bind(&note.title)
            .bind(&note.content)
            .bind(enum_to_db(&note.content_type)?)
            .bind(tags_to_db(&note.tags)?)
            .bind(i64::from(note.is_pinned))
            .bind(i64::from(note.is_archived))
            .bind(i64::from(note.is_default))
            .bind(parent_id)
            .bind(enum_to_db(&note.visibility)?)
            .bind(task_json)
            .bind(&note.created_at)
            .bind(note.rev)
            .bind(&note.updated_at)
            .execute(self.write_pool())
            .await
            .map_err(|e| Error::Internal(format!("insert note failed: {e}")))?;
        Ok(())
    }

    /// List notes in a workspace, ordered by creation time.
    ///
    /// # Errors
    ///
    /// Returns `Error::Internal` if the database operation fails.
    pub async fn list_notes(&self, workspace_id: &WorkspaceId) -> Result<Vec<Note>> {
        let sql =
            format!("SELECT {NOTE_COLUMNS} FROM note WHERE workspace_id = ? ORDER BY created_at");
        let rows = sqlx::query(&sql)
            .bind(&workspace_id.0)
            .fetch_all(self.read_pool())
            .await
            .map_err(|e| Error::Internal(format!("list notes failed: {e}")))?;
        rows.iter().map(map_note_row).collect()
    }

    /// Newest `updated_at` across a workspace's notes, or `None` when the
    /// workspace has none — the note half of the `lastActivity` derivation
    /// (`enrich_workspace_aggregates` / `derive_last_activity`) as a single
    /// aggregate answered from the covering
    /// `idx_note_workspace_updated_at(workspace_id, updated_at)` index, so
    /// the hot list/get emit paths never hydrate note bodies just to fold
    /// timestamps (monorepo#3058).
    ///
    /// `MAX` here is a lexicographic TEXT max: it assumes uniform
    /// daemon-written RFC3339 UTC strings (`now_iso()`), where lexicographic
    /// order ≈ chronological order. Sub-second skew is possible when
    /// fractional-second precision varies within the same second (a bare
    /// `..:00Z` sorts above `..:00.5Z`) — acceptable for `lastActivity`,
    /// which is a sidebar sort/label.
    ///
    /// # Errors
    ///
    /// Returns [`Error::Internal`] when the underlying query fails.
    pub async fn max_note_updated_at(&self, workspace_id: &WorkspaceId) -> Result<Option<String>> {
        sqlx::query_scalar("SELECT MAX(updated_at) FROM note WHERE workspace_id = ?")
            .bind(&workspace_id.0)
            .fetch_one(self.read_pool())
            .await
            .map_err(|e| Error::Internal(format!("max note updated_at failed: {e}")))
    }

    /// List every note across all workspaces, oldest first. Backs the global
    /// `search.notes` adapter (PROTOCOL §5.15), which has no `workspaceId`.
    ///
    /// # Errors
    ///
    /// Returns `Error::Internal` if the database operation fails.
    pub async fn list_all_notes(&self) -> Result<Vec<Note>> {
        let sql = format!("SELECT {NOTE_COLUMNS} FROM note ORDER BY created_at");
        let rows = sqlx::query(&sql)
            .fetch_all(self.read_pool())
            .await
            .map_err(|e| Error::Internal(format!("list all notes failed: {e}")))?;
        rows.iter().map(map_note_row).collect()
    }

    /// Fetch a single note by (workspace, id), or `NotFound`. Note identity is
    /// composite (`(id, workspace_id)`, migration 0030) so callers must supply
    /// both halves; bare-id lookups would silently match a same-id note owned
    /// by another workspace.
    ///
    /// The query runs under [`crate::with_read_retry`]: this is the exact read
    /// that surfaced a transient "get note failed: ... (code: 5) database is
    /// locked" to a production client under heavy write load (monorepo#1139).
    ///
    /// # Errors
    ///
    /// Returns `Error::NotFound` if the note does not exist in the workspace; `Error::Internal` if the database operation fails.
    pub async fn get_note(&self, workspace_id: &WorkspaceId, id: &NoteId) -> Result<Note> {
        let sql = format!("SELECT {NOTE_COLUMNS} FROM note WHERE id = ? AND workspace_id = ?");
        let row = crate::with_read_retry(|| async {
            sqlx::query(&sql)
                .bind(&id.0)
                .bind(&workspace_id.0)
                .fetch_optional(self.read_pool())
                .await
                .map_err(|e| Error::Internal(format!("get note failed: {e}")))
        })
        .await?;
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
    ///
    /// # Errors
    ///
    /// Returns `Error::NotFound` if the note does not exist in the workspace; `Error::Internal` if encoding fields or the update fails.
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
    ///
    /// # Errors
    ///
    /// Returns `Error::Conflict` (carrying the current entity) when `expected_version` is supplied and does not match the stored `rev`; `Error::NotFound` if the note does not exist in the workspace; `Error::Internal` if encoding fields or the update fails.
    pub async fn update_note_versioned(
        &self,
        note: &Note,
        expected_version: Option<i64>,
    ) -> Result<()> {
        let parent_id = note.parent_id.as_ref().map(|n| n.0.clone());
        let task_json = note
            .metadata
            .task
            .as_ref()
            .map(encode_task_json)
            .transpose()?;
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
            .bind(i64::from(note.is_pinned))
            .bind(i64::from(note.is_archived))
            .bind(i64::from(note.is_default))
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
            .execute(self.write_pool())
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
    ///
    /// # Errors
    ///
    /// Returns `Error::NotFound` if the note does not exist in the workspace; `Error::Internal` if the delete fails.
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
    ///
    /// # Errors
    ///
    /// Returns `Error::Conflict` (carrying the current entity snapshot prior to deletion) when `expected_version` is supplied and does not match the stored `rev`; `Error::NotFound` if the note does not exist in the workspace; `Error::Internal` if the delete fails.
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
                    .execute(self.write_pool())
                    .await
                    .map_err(|e| Error::Internal(format!("delete note failed: {e}")))?
            }
            None => sqlx::query("DELETE FROM note WHERE id = ? AND workspace_id = ?")
                .bind(&id.0)
                .bind(&workspace_id.0)
                .execute(self.write_pool())
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
    ///
    /// # Errors
    ///
    /// Returns `Error::Internal` if the database operation fails.
    pub async fn list_tasks(&self, workspace_id: &WorkspaceId) -> Result<Vec<Note>> {
        let sql = format!(
            "SELECT {NOTE_COLUMNS} FROM note WHERE workspace_id = ? AND task_json IS NOT NULL \
             ORDER BY created_at"
        );
        let rows = sqlx::query(&sql)
            .bind(&workspace_id.0)
            .fetch_all(self.read_pool())
            .await
            .map_err(|e| Error::Internal(format!("list tasks failed: {e}")))?;
        rows.iter().map(map_note_row).collect()
    }

    /// Cheap per-workspace `taskStats` counting query (no note-body
    /// hydration). Semantics mirror the enriched `compute_task_stats` in
    /// intent-services (the canonical TS `computeTaskStats` port): count the
    /// spec's direct child task notes, restricted to the spec-linked ids when
    /// the spec body carries `intent://local/task/{id}` links (TS
    /// backward-compat fallback: no links → all direct children with task
    /// metadata count). `cancelled` is excluded from `total`, `complete`
    /// counts as `completed`, and `in_progress`/`review_required` count as
    /// `in_progress`.
    ///
    /// Reads only the spec note's `content` (needed for the linked-id filter)
    /// plus `id` + `json_extract(task_json, '$.status')` for the spec's child
    /// task rows — never the task notes' content bodies — so it stays cheap
    /// on workspaces with large notes.
    ///
    /// # Errors
    ///
    /// Returns `Error::Internal` if the database operation fails.
    pub async fn count_task_stats(&self, workspace_id: &WorkspaceId) -> Result<WorkspaceTaskStats> {
        let spec_content: Option<String> =
            sqlx::query_scalar("SELECT content FROM note WHERE workspace_id = ? AND id = 'spec'")
                .bind(&workspace_id.0)
                .fetch_optional(self.read_pool())
                .await
                .map_err(|e| Error::Internal(format!("task stats spec read failed: {e}")))?;
        let linked: HashSet<String> = spec_content
            .as_deref()
            .map(intent_core::extract_spec_task_ids)
            .unwrap_or_default();
        let has_links = !linked.is_empty();

        let rows = sqlx::query(
            "SELECT id, json_extract(task_json, '$.status') AS status FROM note \
             WHERE workspace_id = ? AND task_json IS NOT NULL \
               AND id != 'spec' AND parent_id = 'spec'",
        )
        .bind(&workspace_id.0)
        .fetch_all(self.read_pool())
        .await
        .map_err(|e| Error::Internal(format!("task stats count failed: {e}")))?;

        let mut stats = WorkspaceTaskStats::default();
        for row in &rows {
            let id: String = col(row, "id")?;
            if has_links && !linked.contains(&id) {
                continue;
            }
            let status: Option<String> = col(row, "status")?;
            match status.as_deref() {
                Some("cancelled") => {}
                Some("complete") => {
                    stats.total += 1;
                    stats.completed += 1;
                }
                Some("in_progress") | Some("review_required") => {
                    stats.total += 1;
                    stats.in_progress += 1;
                }
                _ => stats.total += 1,
            }
        }
        Ok(stats)
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
    ///
    /// Uses the house raw `BEGIN IMMEDIATE` + [`crate::commit_with_rollback_guard`]
    /// write-transaction pattern (monorepo#783/#796, same shape as
    /// `Store::write_acp_session_id` / `Store::update_workspace_token_usage`):
    /// IMMEDIATE mode acquires the RESERVED (write) lock upfront, avoiding the
    /// DEFERRED-mode lock-upgrade race (read → write inside one transaction)
    /// that intermittently fails with `SQLITE_BUSY` (code 5). With
    /// `max_connections=1` on the write pool, in-process writers serialize at
    /// `pool.acquire()`. The `with_write_txn_retry` wrapper (STAB-7) is kept as
    /// belt-and-braces: `BEGIN IMMEDIATE` can still return BUSY when a
    /// connection outside the write pool (e.g. an external process on the same
    /// DB) holds the write lock past `busy_timeout`.
    ///
    /// # Errors
    ///
    /// Returns `Error::Internal` if the database operation fails.
    pub async fn adopt_stray_spec_note(
        &self,
        workspace_id: &WorkspaceId,
    ) -> Result<Option<(NoteId, String)>> {
        let workspace_id = workspace_id.clone();

        crate::with_write_txn_retry(|| async {
            let mut conn = self
                .write_pool()
                .acquire()
                .await
                .map_err(|e| Error::Internal(format!("adopt_spec acquire failed: {e}")))?;
            sqlx::query("BEGIN IMMEDIATE")
                .execute(&mut *conn)
                .await
                .map_err(|e| Error::Internal(format!("begin adopt_spec tx failed: {e}")))?;

            let body_result = async {
                // Re-check the "no `id='spec'`" precondition *inside* the tx: the
                // caller's `fetch_note(spec)` runs on a fresh connection before we
                // begin, so a racing `workspace.create` or a sibling `note.list`
                // may have committed a real spec in the gap. Bailing here lets that
                // caller list the freshly-created spec on its next round-trip
                // rather than surfacing a UNIQUE PK conflict from the UPDATE.
                let existing_spec: Option<String> = sqlx::query_scalar(
                    "SELECT id FROM note WHERE workspace_id = ? AND id = 'spec'",
                )
                .bind(&workspace_id.0)
                .fetch_optional(&mut *conn)
                .await
                .map_err(|e| Error::Internal(format!("recheck spec precondition failed: {e}")))?;
                if existing_spec.is_some() {
                    // Early return before any write statement: the guard's COMMIT
                    // closes a read-only transaction — there is no partial write
                    // to undo and the connection never returns to the pool with a
                    // transaction open.
                    return Ok(None);
                }
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
                .fetch_all(&mut *conn)
                .await
                .map_err(|e| Error::Internal(format!("scan stray spec candidates failed: {e}")))?;
                if rows.len() != 1 {
                    // Still read-only at this point — guard-COMMIT closes the
                    // transaction with nothing written (see above).
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
                // `defer_foreign_keys` is scoped to the open transaction and
                // auto-resets at COMMIT or ROLLBACK regardless of how the
                // transaction was started (DEFERRED vs IMMEDIATE); deferred FK
                // violations still fail the COMMIT.
                sqlx::query("PRAGMA defer_foreign_keys = ON")
                    .execute(&mut *conn)
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
                .execute(&mut *conn)
                .await
                .map_err(|e| Error::Internal(format!("rewrite spec note id failed: {e}")))?;
                if rewrite.rows_affected() != 1 {
                    // Early return after the defer-FK pragma, but with zero rows
                    // written (the only write statement executed matched nothing),
                    // so guard-COMMIT is still the right close: it commits an
                    // empty transaction, and `defer_foreign_keys` resets at that
                    // COMMIT exactly as it would at a ROLLBACK.
                    return Ok(None);
                }
                sqlx::query(
                    "UPDATE note SET parent_id = 'spec' WHERE parent_id = ? AND workspace_id = ?",
                )
                .bind(&old_id)
                .bind(&workspace_id.0)
                .execute(&mut *conn)
                .await
                .map_err(|e| Error::Internal(format!("reparent spec children failed: {e}")))?;
                sqlx::query(
                    "UPDATE note_version SET note_id = 'spec' \
                     WHERE note_id = ? AND workspace_id = ?",
                )
                .bind(&old_id)
                .bind(&workspace_id.0)
                .execute(&mut *conn)
                .await
                .map_err(|e| Error::Internal(format!("move spec versions failed: {e}")))?;
                sqlx::query(
                    "UPDATE note_line_attribution SET note_id = 'spec' \
                     WHERE note_id = ? AND workspace_id = ?",
                )
                .bind(&old_id)
                .bind(&workspace_id.0)
                .execute(&mut *conn)
                .await
                .map_err(|e| Error::Internal(format!("move spec line attribution failed: {e}")))?;
                sqlx::query(
                    "UPDATE comment SET note_id = 'spec' WHERE note_id = ? AND workspace_id = ?",
                )
                .bind(&old_id)
                .bind(&workspace_id.0)
                .execute(&mut *conn)
                .await
                .map_err(|e| Error::Internal(format!("move spec comments failed: {e}")))?;
                Ok(Some((NoteId(old_id), title)))
            }
            .await;

            crate::commit_with_rollback_guard(conn, body_result, "commit adopt_spec tx failed")
                .await
        })
        .await
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
