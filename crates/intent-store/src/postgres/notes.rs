//! Notes, their version history, content-write locks, task projections,
//! line attribution and the retired paging tables (Postgres). Mirrors
//! `note_repo.rs`, `note_version_repo.rs`, `note_write_lock.rs`,
//! `task_event_repo.rs`, `note_line_attribution_repo.rs` and
//! `note_paging_cleanup.rs`.

use std::collections::{BTreeMap, HashMap, HashSet};
use std::sync::atomic::Ordering;
use std::sync::Arc;

use intent_core::{
    AgentId, ContentType, Error, LineAttributionData, Note, NoteId, NoteMetadata, NoteVersion,
    NoteVersionAuthor, NoteVersionSummary, NoteVisibility, Result, TaskListResult, TaskMetadata,
    TaskStatus, WorkspaceId, WorkspaceTask, WorkspaceTaskStats,
};
use sqlx::postgres::PgRow;
use sqlx::Row;

use super::{db, PgStore, WriteTx};
use crate::note_paging_cleanup::{
    NotePagingCleanupOutcome, NotePagingCleanupPolicy, RetiredNotePagingStep,
    RETIRED_NOTE_PAGING_TABLES,
};
use crate::note_repo::{encode_task_json, NoteUpdateScope, NOTE_COLUMNS};
use crate::note_version_repo::MAX_NOTE_VERSIONS;
use crate::{
    enum_from_db, enum_to_db, tags_from_db, tags_to_db, NoteWriteLock, TaskEventBaselines,
};

pub(super) fn col<'r, T>(row: &'r PgRow, name: &str) -> Result<T>
where
    T: sqlx::Decode<'r, sqlx::Postgres> + sqlx::Type<sqlx::Postgres>,
{
    row.try_get::<T, _>(name)
        .map_err(|e| Error::Internal(format!("column {name}: {e}")))
}

fn internal(context: &str) -> impl Fn(sqlx::Error) -> Error + '_ {
    move |e| Error::Internal(format!("{context}: {e}"))
}

fn ids(workspace_ids: &[WorkspaceId]) -> Vec<String> {
    workspace_ids.iter().map(|id| id.0.clone()).collect()
}

/// One task's status contribution to [`WorkspaceTaskStats`].
fn count_status(totals: &mut WorkspaceTaskStats, status: Option<&str>) {
    match status {
        Some("cancelled") => {}
        Some("complete") => {
            totals.total += 1;
            totals.completed += 1;
        }
        Some("in_progress" | "review_required") => {
            totals.total += 1;
            totals.in_progress += 1;
        }
        _ => totals.total += 1,
    }
}

impl PgStore {
    pub async fn insert_note(&self, note: &Note) -> Result<()> {
        self.prepare_task_events(&note.workspace_id).await;
        let mut tx = self.begin_write().await?;
        exec_insert_note(self, &mut tx, note).await?;
        tx.commit().await.map_err(db)
    }

    pub async fn list_notes(&self, workspace_id: &WorkspaceId) -> Result<Vec<Note>> {
        let mut tx = self.begin().await?;
        let rows = sqlx::query(sqlx::AssertSqlSafe(format!(
            "SELECT {NOTE_COLUMNS} FROM note WHERE tenant_id = $1 AND workspace_id = $2 \
             ORDER BY created_at COLLATE \"C\", row_seq"
        )))
        .bind(self.tenant())
        .bind(workspace_id.as_str())
        .fetch_all(&mut *tx)
        .await
        .map_err(internal("list notes failed"))?;
        tx.commit().await.map_err(db)?;
        rows.iter().map(map_note_row).collect()
    }

    pub async fn list_workspace_tasks(
        &self,
        workspace_id: &WorkspaceId,
        filter: Option<TaskStatus>,
    ) -> Result<TaskListResult> {
        Ok(self
            .list_workspace_tasks_keyed(workspace_id, filter)
            .await?
            .0)
    }

    pub async fn list_workspace_tasks_keyed(
        &self,
        workspace_id: &WorkspaceId,
        filter: Option<TaskStatus>,
    ) -> Result<(TaskListResult, Vec<String>)> {
        let filter = filter.map(|s| enum_to_db(&s)).transpose()?;
        let mut tx = self.begin().await?;
        let rows = sqlx::query(
            "SELECT id, parent_id, created_at, title, updated_at, task_json, \
             sqlite_json_extract(task_json, '$.status') AS status, \
             CASE WHEN id = 'spec' THEN content END AS spec_content \
             FROM note WHERE tenant_id = $1 AND workspace_id = $2 \
             AND (id = 'spec' OR task_json IS NOT NULL) \
             ORDER BY created_at COLLATE \"C\", row_seq",
        )
        .bind(self.tenant())
        .bind(workspace_id.as_str())
        .fetch_all(&mut *tx)
        .await
        .map_err(internal("list workspace tasks failed"))?;
        tx.commit().await.map_err(db)?;

        let mut linked = HashSet::new();
        let mut status_by_id = HashMap::new();
        let mut children = Vec::new();
        let mut tasks = Vec::new();
        let mut keys = Vec::new();
        for row in &rows {
            let id: String = col(row, "id")?;
            let raw_status: Option<String> = col(row, "status")?;
            let status = raw_status
                .as_deref()
                .map(enum_from_db::<TaskStatus>)
                .transpose()?;
            if let Some(status) = status {
                status_by_id.insert(id.clone(), status);
            }
            if id == "spec" {
                let content: String = col(row, "spec_content")?;
                linked = intent_core::extract_spec_task_ids(&content);
                continue;
            }
            let status =
                status.ok_or_else(|| Error::Internal(format!("missing task status: {id}")))?;
            let parent_id: Option<String> = col(row, "parent_id")?;
            if parent_id.as_deref() == Some("spec") {
                children.push((id.clone(), status));
            }
            if filter.is_some() && raw_status != filter {
                continue;
            }
            let task_json: String = col(row, "task_json")?;
            let task: serde_json::Value = serde_json::from_str(&task_json)
                .map_err(|e| Error::Internal(format!("decode task summary failed: {e}")))?;
            let id_list = |key: &str| -> Result<Vec<NoteId>> {
                match task.get(key) {
                    None | Some(serde_json::Value::Null) => Ok(Vec::new()),
                    Some(value) => serde_json::from_value(value.clone())
                        .map_err(|e| Error::Internal(format!("decode task summary failed: {e}"))),
                }
            };
            let title: String = col(row, "title")?;
            keys.push(col::<String>(row, "created_at")?);
            tasks.push(WorkspaceTask {
                id: NoteId::from(id),
                title: if title.is_empty() {
                    "Untitled task".into()
                } else {
                    title
                },
                status,
                updated_at: col(row, "updated_at")?,
                parent_id: parent_id.map(NoteId::from),
                spec_linked: false,
                depends_on: id_list("dependsOn")?,
                conflicts_with: id_list("conflictsWith")?,
                unmet_depends_on: Vec::new(),
            });
        }
        for task in &mut tasks {
            task.spec_linked = linked.contains(task.id.as_str());
            task.unmet_depends_on = task
                .depends_on
                .iter()
                .filter(|id| status_by_id.get(id.as_str()) != Some(&TaskStatus::Complete))
                .cloned()
                .collect();
        }
        let mut stats = WorkspaceTaskStats::default();
        for (id, status) in children {
            if !linked.is_empty() && !linked.contains(&id) {
                continue;
            }
            count_status(&mut stats, Some(enum_to_db(&status)?.as_str()));
        }
        Ok((TaskListResult { tasks, stats }, keys))
    }

    pub async fn note_exists(&self, workspace_id: &WorkspaceId, id: &NoteId) -> Result<bool> {
        let mut tx = self.begin().await?;
        let present: bool = sqlx::query_scalar(
            "SELECT EXISTS (SELECT 1 FROM note WHERE tenant_id = $1 AND id = $2 AND workspace_id = $3)",
        )
        .bind(self.tenant())
        .bind(id.as_str())
        .bind(workspace_id.as_str())
        .fetch_one(&mut *tx)
        .await
        .map_err(internal("note existence check failed"))?;
        tx.commit().await.map_err(db)?;
        Ok(present)
    }

    pub async fn max_note_updated_at(&self, workspace_id: &WorkspaceId) -> Result<Option<String>> {
        let mut tx = self.begin().await?;
        let max = sqlx::query_scalar(
            "SELECT MAX(updated_at COLLATE \"C\") FROM note WHERE tenant_id = $1 AND workspace_id = $2",
        )
        .bind(self.tenant())
        .bind(workspace_id.as_str())
        .fetch_one(&mut *tx)
        .await
        .map_err(internal("max note updated_at failed"))?;
        tx.commit().await.map_err(db)?;
        Ok(max)
    }

    pub async fn max_note_updated_at_by_workspace(
        &self,
        workspace_ids: &[WorkspaceId],
    ) -> Result<HashMap<WorkspaceId, String>> {
        if workspace_ids.is_empty() {
            return Ok(HashMap::new());
        }
        let mut tx = self.begin().await?;
        let rows = sqlx::query(
            "SELECT workspace_id, MAX(updated_at COLLATE \"C\") AS updated_at FROM note \
             WHERE tenant_id = $1 AND workspace_id = ANY($2) GROUP BY workspace_id",
        )
        .bind(self.tenant())
        .bind(ids(workspace_ids))
        .fetch_all(&mut *tx)
        .await
        .map_err(internal("batch max note updated_at failed"))?;
        tx.commit().await.map_err(db)?;
        rows.iter()
            .map(|row| {
                Ok((
                    WorkspaceId(col(row, "workspace_id")?),
                    col(row, "updated_at")?,
                ))
            })
            .collect()
    }

    pub async fn list_all_notes(&self) -> Result<Vec<Note>> {
        let mut tx = self.begin().await?;
        let rows = sqlx::query(sqlx::AssertSqlSafe(format!(
            "SELECT {NOTE_COLUMNS} FROM note WHERE tenant_id = $1 \
             ORDER BY created_at COLLATE \"C\", row_seq"
        )))
        .bind(self.tenant())
        .fetch_all(&mut *tx)
        .await
        .map_err(internal("list all notes failed"))?;
        tx.commit().await.map_err(db)?;
        rows.iter().map(map_note_row).collect()
    }

    pub async fn get_note(&self, workspace_id: &WorkspaceId, id: &NoteId) -> Result<Note> {
        let mut tx = self.begin().await?;
        let row = sqlx::query(sqlx::AssertSqlSafe(format!(
            "SELECT {NOTE_COLUMNS} FROM note WHERE tenant_id = $1 AND id = $2 AND workspace_id = $3"
        )))
        .bind(self.tenant())
        .bind(id.as_str())
        .bind(workspace_id.as_str())
        .fetch_optional(&mut *tx)
        .await
        .map_err(internal("get note failed"))?;
        tx.commit().await.map_err(db)?;
        match row {
            Some(r) => map_note_row(&r),
            None => Err(Error::NotFound(format!("note {id}"))),
        }
    }

    pub async fn get_note_metadata(&self, workspace_id: &WorkspaceId, id: &NoteId) -> Result<Note> {
        let columns = NOTE_COLUMNS.replace(" content,", " '' AS content,");
        let mut tx = self.begin().await?;
        let row = sqlx::query(sqlx::AssertSqlSafe(format!(
            "SELECT {columns} FROM note WHERE tenant_id = $1 AND id = $2 AND workspace_id = $3"
        )))
        .bind(self.tenant())
        .bind(id.as_str())
        .bind(workspace_id.as_str())
        .fetch_optional(&mut *tx)
        .await
        .map_err(internal("get note metadata failed"))?;
        tx.commit().await.map_err(db)?;
        row.as_ref()
            .map(map_note_row)
            .transpose()?
            .ok_or_else(|| Error::NotFound(format!("note {id}")))
    }

    pub async fn update_note(&self, lock: &NoteWriteLock, note: &Note) -> Result<i64> {
        self.update_note_versioned(lock, note, None).await
    }

    pub async fn update_note_versioned(
        &self,
        lock: &NoteWriteLock,
        note: &Note,
        expected_version: Option<i64>,
    ) -> Result<i64> {
        lock.ensure_covers(&self.note_write_lanes, note)?;
        self.note_update(note, expected_version, NoteUpdateScope::FullRow)
            .await
    }

    pub async fn update_note_metadata(&self, lock: &NoteWriteLock, note: &Note) -> Result<i64> {
        self.update_note_metadata_versioned(lock, note, None).await
    }

    pub async fn update_note_metadata_versioned(
        &self,
        lock: &NoteWriteLock,
        note: &Note,
        expected_version: Option<i64>,
    ) -> Result<i64> {
        lock.ensure_covers(&self.note_write_lanes, note)?;
        self.note_update(note, expected_version, NoteUpdateScope::Metadata)
            .await
    }

    pub async fn update_note_metadata_if_agent_linked(
        &self,
        lock: &NoteWriteLock,
        note: &Note,
        expected_version: i64,
        agent_id: &AgentId,
    ) -> Result<Option<i64>> {
        lock.ensure_covers(&self.note_write_lanes, note)?;
        self.prepare_task_events(&note.workspace_id).await;
        let mut tx = self.begin_write().await?;
        let rev = exec_update_note(
            self,
            &mut tx,
            note,
            expected_version,
            NoteUpdateScope::Metadata,
            Some(agent_id),
        )
        .await?;
        tx.commit().await.map_err(db)?;
        Ok(rev)
    }

    async fn note_update(
        &self,
        note: &Note,
        expected_version: Option<i64>,
        scope: NoteUpdateScope,
    ) -> Result<i64> {
        self.prepare_task_events(&note.workspace_id).await;
        let mut tx = self.begin_write().await?;
        let expected_rev = cas_rev(note, expected_version);
        let rev = exec_update_note(self, &mut tx, note, expected_rev, scope, None).await?;
        tx.commit().await.map_err(db)?;
        match rev {
            Some(rev) => Ok(rev),
            None => Err(self.note_update_miss(note).await),
        }
    }

    /// A 0-row note write: a present row means the `expected_version` gate
    /// failed ([`Error::Conflict`] with the current note), else not found.
    pub(super) async fn note_update_miss(&self, note: &Note) -> Error {
        self.note_miss(&note.workspace_id, &note.id).await
    }

    async fn note_miss(&self, workspace_id: &WorkspaceId, id: &NoteId) -> Error {
        match self.get_note(workspace_id, id).await {
            Ok(current) => match serde_json::to_value(&current) {
                Ok(current) => Error::Conflict { current },
                Err(e) => Error::Internal(format!("encode current note failed: {e}")),
            },
            Err(Error::NotFound(_)) => Error::NotFound(format!("note {id}")),
            Err(e) => e,
        }
    }

    pub async fn delete_note(&self, workspace_id: &WorkspaceId, id: &NoteId) -> Result<()> {
        self.delete_note_versioned(workspace_id, id, None).await
    }

    pub async fn delete_note_versioned(
        &self,
        workspace_id: &WorkspaceId,
        id: &NoteId,
        expected_version: Option<i64>,
    ) -> Result<()> {
        self.prepare_task_events(workspace_id).await;
        let mut tx = self.begin_write().await?;
        let deleted = sqlx::query(
            "DELETE FROM note WHERE tenant_id = $1 AND id = $2 AND workspace_id = $3 \
             AND ($4::bigint IS NULL OR rev = $4)",
        )
        .bind(self.tenant())
        .bind(id.as_str())
        .bind(workspace_id.as_str())
        .bind(expected_version)
        .execute(&mut *tx)
        .await
        .map_err(internal("delete note failed"))?
        .rows_affected();
        tx.commit().await.map_err(db)?;
        if deleted == 0 {
            return Err(self.note_miss(workspace_id, id).await);
        }
        Ok(())
    }

    pub async fn list_tasks(&self, workspace_id: &WorkspaceId) -> Result<Vec<Note>> {
        let mut tx = self.begin().await?;
        let rows = sqlx::query(sqlx::AssertSqlSafe(format!(
            "SELECT {NOTE_COLUMNS} FROM note WHERE tenant_id = $1 AND workspace_id = $2 \
             AND task_json IS NOT NULL ORDER BY created_at COLLATE \"C\", row_seq"
        )))
        .bind(self.tenant())
        .bind(workspace_id.as_str())
        .fetch_all(&mut *tx)
        .await
        .map_err(internal("list tasks failed"))?;
        tx.commit().await.map_err(db)?;
        rows.iter().map(map_note_row).collect()
    }

    pub async fn count_task_stats(&self, workspace_id: &WorkspaceId) -> Result<WorkspaceTaskStats> {
        Ok(self
            .count_task_stats_by_workspace(std::slice::from_ref(workspace_id))
            .await?
            .remove(workspace_id)
            .unwrap_or_default())
    }

    pub async fn count_tasks_by_status(
        &self,
        workspace_id: &WorkspaceId,
    ) -> Result<BTreeMap<String, u64>> {
        let mut tx = self.begin().await?;
        let rows = sqlx::query(
            "SELECT sqlite_json_extract(task_json, '$.status') AS status, COUNT(*) AS n FROM note \
             WHERE tenant_id = $1 AND workspace_id = $2 AND task_json IS NOT NULL AND id != 'spec' \
             GROUP BY 1",
        )
        .bind(self.tenant())
        .bind(workspace_id.as_str())
        .fetch_all(&mut *tx)
        .await
        .map_err(internal("task status counts failed"))?;
        tx.commit().await.map_err(db)?;
        let mut counts: BTreeMap<String, u64> = BTreeMap::new();
        for row in &rows {
            let status: Option<String> = col(row, "status")?;
            let n: i64 = col(row, "n")?;
            let status = status
                .and_then(|s| {
                    serde_json::from_value::<TaskStatus>(serde_json::Value::String(s)).ok()
                })
                .unwrap_or_default();
            *counts.entry(enum_to_db(&status)?).or_insert(0) += u64::try_from(n).unwrap_or(0);
        }
        Ok(counts)
    }

    pub async fn count_task_stats_by_workspace(
        &self,
        workspace_ids: &[WorkspaceId],
    ) -> Result<HashMap<WorkspaceId, WorkspaceTaskStats>> {
        let mut out: HashMap<WorkspaceId, WorkspaceTaskStats> = workspace_ids
            .iter()
            .cloned()
            .map(|id| (id, WorkspaceTaskStats::default()))
            .collect();
        if workspace_ids.is_empty() {
            return Ok(out);
        }
        let mut tx = self.begin().await?;
        let rows = sqlx::query(
            "SELECT workspace_id, id, \
                CASE WHEN id = 'spec' THEN content END AS spec_content, \
                CASE WHEN task_json IS NOT NULL \
                    THEN sqlite_json_extract(task_json, '$.status') END AS status \
             FROM note WHERE tenant_id = $1 AND workspace_id = ANY($2) AND \
                (id = 'spec' OR (task_json IS NOT NULL AND id != 'spec' AND parent_id = 'spec'))",
        )
        .bind(self.tenant())
        .bind(ids(workspace_ids))
        .fetch_all(&mut *tx)
        .await
        .map_err(internal("batch task stats read failed"))?;
        tx.commit().await.map_err(db)?;
        let mut linked_by_workspace: HashMap<WorkspaceId, HashSet<String>> = HashMap::new();
        for row in &rows {
            if let Some(content) = col::<Option<String>>(row, "spec_content")? {
                linked_by_workspace.insert(
                    WorkspaceId(col(row, "workspace_id")?),
                    intent_core::extract_spec_task_ids(&content),
                );
            }
        }
        for row in &rows {
            let workspace_id = WorkspaceId(col(row, "workspace_id")?);
            let id: String = col(row, "id")?;
            if id == "spec" {
                continue;
            }
            let linked = linked_by_workspace.get(&workspace_id);
            if linked.is_some_and(|ids| !ids.is_empty() && !ids.contains(&id)) {
                continue;
            }
            let status = col::<Option<String>>(row, "status")?;
            count_status(out.entry(workspace_id).or_default(), status.as_deref());
        }
        Ok(out)
    }

    /// Postgres foreign keys are not deferrable here, so instead of
    /// rewriting the key in place (`SQLite` defers its FKs) this copies the
    /// stray row to `id = 'spec'`, moves its dependents, then deletes it.
    pub async fn adopt_stray_spec_note(
        &self,
        workspace_id: &WorkspaceId,
    ) -> Result<Option<(NoteId, String)>> {
        self.prepare_task_events(workspace_id).await;
        let mut tx = self.begin_write().await?;
        let existing: bool = sqlx::query_scalar(
            "SELECT EXISTS (SELECT 1 FROM note WHERE tenant_id = $1 AND workspace_id = $2 AND id = 'spec')",
        )
        .bind(self.tenant())
        .bind(workspace_id.as_str())
        .fetch_one(&mut *tx)
        .await
        .map_err(internal("recheck spec precondition failed"))?;
        if existing {
            tx.commit().await.map_err(db)?;
            return Ok(None);
        }
        let rows = sqlx::query(
            "SELECT id, title, tags FROM note \
             WHERE tenant_id = $1 AND workspace_id = $2 AND id != 'spec' AND parent_id IS NULL \
               AND task_json IS NULL AND is_archived = 0 AND lower(btrim(title)) = 'spec' \
             LIMIT 2",
        )
        .bind(self.tenant())
        .bind(workspace_id.as_str())
        .fetch_all(&mut *tx)
        .await
        .map_err(internal("scan stray spec candidates failed"))?;
        if rows.len() != 1 {
            tx.commit().await.map_err(db)?;
            return Ok(None);
        }
        let row = &rows[0];
        let old_id: String = col(row, "id")?;
        let title: String = col(row, "title")?;
        let mut tags = tags_from_db(&col::<String>(row, "tags")?)?;
        if !tags.iter().any(|t| t == "spec") {
            tags.push("spec".to_string());
        }
        let copied = sqlx::query(
            "INSERT INTO note (tenant_id, id, workspace_id, title, content, content_type, tags, \
                 is_pinned, is_archived, is_default, parent_id, visibility, task_json, \
                 created_at, updated_at, rev, row_seq) \
             SELECT tenant_id, 'spec', workspace_id, title, content, content_type, $4, \
                 1, is_archived, 1, parent_id, visibility, task_json, \
                 created_at, updated_at, rev, row_seq \
             FROM note WHERE tenant_id = $1 AND workspace_id = $2 AND id = $3 \
             ON CONFLICT (tenant_id, id, workspace_id) DO NOTHING",
        )
        .bind(self.tenant())
        .bind(workspace_id.as_str())
        .bind(&old_id)
        .bind(tags_to_db(&tags)?)
        .execute(&mut *tx)
        .await
        .map_err(internal("rewrite spec note id failed"))?;
        if copied.rows_affected() != 1 {
            tx.commit().await.map_err(db)?;
            return Ok(None);
        }
        for (sql, context) in [
            (
                "UPDATE note SET parent_id = 'spec' \
                 WHERE tenant_id = $1 AND workspace_id = $2 AND parent_id = $3",
                "reparent spec children failed",
            ),
            (
                "UPDATE note_version SET note_id = 'spec' \
                 WHERE tenant_id = $1 AND workspace_id = $2 AND note_id = $3",
                "move spec versions failed",
            ),
            (
                "UPDATE note_line_attribution SET note_id = 'spec' \
                 WHERE tenant_id = $1 AND workspace_id = $2 AND note_id = $3",
                "move spec line attribution failed",
            ),
            (
                "UPDATE comment SET note_id = 'spec' \
                 WHERE tenant_id = $1 AND workspace_id = $2 AND note_id = $3",
                "move spec comments failed",
            ),
            (
                "DELETE FROM note WHERE tenant_id = $1 AND workspace_id = $2 AND id = $3",
                "remove stray spec row failed",
            ),
        ] {
            sqlx::query(sql)
                .bind(self.tenant())
                .bind(workspace_id.as_str())
                .bind(&old_id)
                .execute(&mut *tx)
                .await
                .map_err(internal(context))?;
        }
        tx.commit().await.map_err(db)?;
        Ok(Some((NoteId(old_id), title)))
    }

    // Version history.

    pub async fn append_note_version(
        &self,
        note: &Note,
        author: &NoteVersionAuthor,
        date: &str,
        rev: i64,
    ) -> Result<i64> {
        let mut tx = self.begin_write().await?;
        let v = insert_note_version(self, &mut tx, note, author, date, rev).await?;
        tx.commit().await.map_err(db)?;
        Ok(v)
    }

    pub async fn update_note_with_version(
        &self,
        lock: &NoteWriteLock,
        note: &Note,
        expected_version: Option<i64>,
        author: &NoteVersionAuthor,
        date: &str,
    ) -> Result<(i64, i64)> {
        self.update_note_with_version_and_children(lock, note, expected_version, &[], author, date)
            .await
    }

    pub async fn update_note_with_version_and_children(
        &self,
        lock: &NoteWriteLock,
        note: &Note,
        expected_version: Option<i64>,
        children: &[Note],
        author: &NoteVersionAuthor,
        date: &str,
    ) -> Result<(i64, i64)> {
        lock.ensure_covers(&self.note_write_lanes, note)?;
        self.prepare_task_events(&note.workspace_id).await;
        let mut tx = self.begin_write().await?;
        let Some(rev) = exec_update_note(
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
        let v = insert_note_version(self, &mut tx, note, author, date, rev).await?;
        for child in children {
            exec_insert_note(self, &mut tx, child).await?;
            insert_note_version(self, &mut tx, child, author, &child.updated_at, child.rev).await?;
        }
        tx.commit().await.map_err(db)?;
        Ok((rev, v))
    }

    pub async fn insert_note_with_version(
        &self,
        note: &Note,
        author: &NoteVersionAuthor,
        date: &str,
    ) -> Result<i64> {
        self.prepare_task_events(&note.workspace_id).await;
        let mut tx = self.begin_write().await?;
        exec_insert_note(self, &mut tx, note).await?;
        let v = insert_note_version(self, &mut tx, note, author, date, note.rev).await?;
        tx.commit().await.map_err(db)?;
        Ok(v)
    }

    pub async fn list_note_versions(
        &self,
        workspace_id: &WorkspaceId,
        note_id: &NoteId,
    ) -> Result<Vec<NoteVersionSummary>> {
        let mut tx = self.begin().await?;
        let rows = sqlx::query(
            "SELECT v, date, author_id, author_name, author_type, title, \
             length(content)::bigint AS content_length FROM note_version \
             WHERE tenant_id = $1 AND note_id = $2 AND workspace_id = $3 ORDER BY v",
        )
        .bind(self.tenant())
        .bind(note_id.as_str())
        .bind(workspace_id.as_str())
        .fetch_all(&mut *tx)
        .await
        .map_err(internal("list note_versions failed"))?;
        tx.commit().await.map_err(db)?;
        rows.iter()
            .map(|row| {
                Ok(NoteVersionSummary {
                    entry_type: "snapshot".to_string(),
                    v: col(row, "v")?,
                    date: col(row, "date")?,
                    author: map_author(row)?,
                    title: col(row, "title")?,
                    content_length: col(row, "content_length")?,
                })
            })
            .collect()
    }

    pub async fn get_note_version(
        &self,
        workspace_id: &WorkspaceId,
        note_id: &NoteId,
        v: i64,
    ) -> Result<NoteVersion> {
        let mut tx = self.begin().await?;
        let row = sqlx::query(
            "SELECT v, date, author_id, author_name, author_type, title, content \
             FROM note_version WHERE tenant_id = $1 AND note_id = $2 AND workspace_id = $3 AND v = $4",
        )
        .bind(self.tenant())
        .bind(note_id.as_str())
        .bind(workspace_id.as_str())
        .bind(v)
        .fetch_optional(&mut *tx)
        .await
        .map_err(internal("get note_version failed"))?;
        tx.commit().await.map_err(db)?;
        let Some(row) = row else {
            return Err(Error::NotFound(format!("note version {note_id}@{v}")));
        };
        Ok(NoteVersion {
            entry_type: "snapshot".to_string(),
            v: col(&row, "v")?,
            date: col(&row, "date")?,
            author: map_author(&row)?,
            title: col(&row, "title")?,
            content: col(&row, "content")?,
        })
    }

    pub async fn get_note_version_content_by_rev(
        &self,
        workspace_id: &WorkspaceId,
        note_id: &NoteId,
        rev: i64,
    ) -> Result<Option<String>> {
        let mut tx = self.begin().await?;
        let content = sqlx::query_scalar(
            "SELECT content FROM note_version \
             WHERE tenant_id = $1 AND workspace_id = $2 AND note_id = $3 \
             AND rev IS NOT NULL AND rev <= $4 ORDER BY rev DESC, v DESC LIMIT 1",
        )
        .bind(self.tenant())
        .bind(workspace_id.as_str())
        .bind(note_id.as_str())
        .bind(rev)
        .fetch_optional(&mut *tx)
        .await
        .map_err(internal("get note_version by rev failed"))?;
        tx.commit().await.map_err(db)?;
        Ok(content)
    }

    // Content-write locks and task projection baselines.

    pub async fn lock_note_content(
        &self,
        workspace_id: &WorkspaceId,
        note_id: &NoteId,
    ) -> NoteWriteLock {
        self.note_write_lanes
            .lock((workspace_id.clone(), note_id.clone()))
            .await
    }

    #[cfg(any(test, feature = "test-support"))]
    pub fn note_content_lock_users(&self, workspace_id: &WorkspaceId, note_id: &NoteId) -> usize {
        self.note_write_lanes
            .users(&(workspace_id.clone(), note_id.clone()))
    }

    #[cfg(any(test, feature = "test-support"))]
    pub fn note_content_locks_idle(&self) -> bool {
        self.note_write_lanes.is_empty()
    }

    pub fn task_event_baselines(&self) -> Arc<TaskEventBaselines> {
        self.task_events_enabled.store(true, Ordering::Release);
        self.task_event_baselines.clone()
    }

    /// Same contract as `SqliteStore::prepare_task_events`: capture the
    /// workspace's first task baseline before a mutation.
    pub(super) async fn prepare_task_events(&self, workspace_id: &WorkspaceId) {
        if !self.task_events_enabled.load(Ordering::Acquire) {
            return;
        }
        let mut baseline = self.task_event_baselines.lock(workspace_id).await;
        if baseline.is_some() {
            return;
        }
        match Box::pin(self.list_workspace_tasks(workspace_id, None)).await {
            Ok(snapshot) => *baseline = Some(snapshot),
            Err(error) => tracing::warn!(%workspace_id, %error, "task event baseline unavailable"),
        }
    }

    // Line attribution.

    pub async fn upsert_note_line_attribution(&self, data: &LineAttributionData) -> Result<()> {
        let attributions_json = serde_json::to_string(&data.attributions).map_err(|e| {
            Error::Internal(format!(
                "encode note_line_attribution attributions failed: {e}"
            ))
        })?;
        let mut tx = self.begin_write().await?;
        sqlx::query(
            "INSERT INTO note_line_attribution \
                 (tenant_id, note_id, workspace_id, computed_at, attributions_json) \
             VALUES ($1, $2, $3, $4, $5) \
             ON CONFLICT (tenant_id, workspace_id, note_id) DO UPDATE SET \
               computed_at = excluded.computed_at, \
               attributions_json = excluded.attributions_json",
        )
        .bind(self.tenant())
        .bind(data.note_id.as_str())
        .bind(data.workspace_id.as_str())
        .bind(&data.computed_at)
        .bind(&attributions_json)
        .execute(&mut *tx)
        .await
        .map_err(internal("upsert note_line_attribution failed"))?;
        tx.commit().await.map_err(db)
    }

    pub async fn get_note_line_attribution(
        &self,
        workspace_id: &WorkspaceId,
        note_id: &NoteId,
    ) -> Result<Option<LineAttributionData>> {
        let mut tx = self.begin().await?;
        let row = sqlx::query(
            "SELECT workspace_id, computed_at, attributions_json FROM note_line_attribution \
             WHERE tenant_id = $1 AND note_id = $2 AND workspace_id = $3",
        )
        .bind(self.tenant())
        .bind(note_id.as_str())
        .bind(workspace_id.as_str())
        .fetch_optional(&mut *tx)
        .await
        .map_err(internal("get note_line_attribution failed"))?;
        tx.commit().await.map_err(db)?;
        let Some(row) = row else { return Ok(None) };
        let attributions = serde_json::from_str(&col::<String>(&row, "attributions_json")?)
            .map_err(|e| {
                Error::Internal(format!(
                    "decode note_line_attribution attributions failed: {e}"
                ))
            })?;
        Ok(Some(LineAttributionData {
            note_id: note_id.clone(),
            workspace_id: WorkspaceId(col(&row, "workspace_id")?),
            computed_at: col(&row, "computed_at")?,
            attributions,
        }))
    }

    // Retired note paging tables. They are shared by every tenant, so a
    // tenant store only deletes its own rows and never drops a table; the
    // cleanup is done for this tenant once both hold none of its rows.

    pub async fn retired_note_paging_step(&self, batch_rows: u32) -> Result<RetiredNotePagingStep> {
        let batch = i64::from(batch_rows.max(1));
        for (table, sql) in [
            (
                RETIRED_NOTE_PAGING_TABLES[0],
                "DELETE FROM note_page_entry WHERE tenant_id = $1 AND row_seq IN \
                 (SELECT row_seq FROM note_page_entry WHERE tenant_id = $1 LIMIT $2)",
            ),
            (
                RETIRED_NOTE_PAGING_TABLES[1],
                "DELETE FROM note_page_head WHERE tenant_id = $1 AND row_seq IN \
                 (SELECT row_seq FROM note_page_head WHERE tenant_id = $1 LIMIT $2)",
            ),
        ] {
            let mut tx = self.begin_write().await?;
            let rows = sqlx::query(sql)
                .bind(self.tenant())
                .bind(batch)
                .execute(&mut *tx)
                .await
                .map_err(|e| Error::Internal(format!("delete retired {table} rows: {e}")))?
                .rows_affected();
            tx.commit().await.map_err(db)?;
            if rows > 0 {
                return Ok(RetiredNotePagingStep::Deleted { table, rows });
            }
        }
        Ok(RetiredNotePagingStep::Done)
    }

    pub async fn run_retired_note_paging_cleanup(
        &self,
        policy: NotePagingCleanupPolicy,
    ) -> NotePagingCleanupOutcome {
        crate::note_paging_cleanup::run_paced_cleanup(policy, |batch| {
            self.retired_note_paging_step(batch)
        })
        .await
    }
}

/// The note INSERT, inside the caller's transaction.
pub(super) async fn exec_insert_note(
    store: &PgStore,
    conn: &mut WriteTx,
    note: &Note,
) -> Result<()> {
    let task_json = note
        .metadata
        .task
        .as_ref()
        .map(encode_task_json)
        .transpose()?;
    sqlx::query(sqlx::AssertSqlSafe(format!(
        "INSERT INTO note (tenant_id, {NOTE_COLUMNS}) \
         VALUES ($1,$2,$3,$4,$5,$6,$7,$8,$9,$10,$11,$12,$13,$14,$15,$16)"
    )))
    .bind(store.tenant())
    .bind(note.id.as_str())
    .bind(note.workspace_id.as_str())
    .bind(&note.title)
    .bind(&note.content)
    .bind(enum_to_db(&note.content_type)?)
    .bind(tags_to_db(&note.tags)?)
    .bind(i64::from(note.is_pinned))
    .bind(i64::from(note.is_archived))
    .bind(i64::from(note.is_default))
    .bind(note.parent_id.as_ref().map(NoteId::as_str))
    .bind(enum_to_db(&note.visibility)?)
    .bind(task_json)
    .bind(&note.created_at)
    .bind(note.rev)
    .bind(&note.updated_at)
    .execute(&mut **conn)
    .await
    .map_err(internal("insert note failed"))?;
    Ok(())
}

/// The `rev` a note write compares against: the caller's `expectedVersion`
/// when given, else the `rev` of the note the caller read (batch G 2a).
/// Every Postgres note write is a compare-and-swap, so a stale process (a
/// second handle, or a daemon that lost the tenant) cannot silently overwrite
/// a newer edit: it gets `Conflict` and must re-read. One process sees no
/// change, since its note lane orders the read before the write.
pub(super) fn cas_rev(note: &Note, expected_version: Option<i64>) -> i64 {
    expected_version.unwrap_or(note.rev)
}

/// The note UPDATE every content/metadata write runs, inside the caller's
/// transaction: `Some(rev)` after the write, `None` when no row matched
/// (gone, or its `rev` is no longer `expected_rev`; see [`cas_rev`]).
/// Every parameter is always bound so the numbering stays fixed.
pub(super) async fn exec_update_note(
    store: &PgStore,
    conn: &mut WriteTx,
    note: &Note,
    expected_rev: i64,
    scope: NoteUpdateScope,
    required_link: Option<&AgentId>,
) -> Result<Option<i64>> {
    let task_json = note
        .metadata
        .task
        .as_ref()
        .map(encode_task_json)
        .transpose()?;
    let content = if scope == NoteUpdateScope::FullRow {
        "content = $5, content_type = $6, "
    } else {
        ""
    };
    let link = if required_link.is_some() {
        " AND EXISTS (SELECT 1 FROM agent_session a WHERE a.tenant_id = $1 AND a.id = $17 \
         AND a.workspace_id = note.workspace_id AND a.task_note_id = note.id)"
    } else {
        ""
    };
    let rev = sqlx::query_scalar(sqlx::AssertSqlSafe(format!(
        "UPDATE note SET title = $4, {content}tags = $7, is_pinned = $8, is_archived = $9, \
         is_default = $10, parent_id = $11, visibility = $12, task_json = $13, \
         created_at = $14, updated_at = $15, rev = rev + 1 \
         WHERE tenant_id = $1 AND id = $2 AND workspace_id = $3 AND rev = $16{link} RETURNING rev"
    )))
    .bind(store.tenant())
    .bind(note.id.as_str())
    .bind(note.workspace_id.as_str())
    .bind(&note.title)
    .bind(&note.content)
    .bind(enum_to_db(&note.content_type)?)
    .bind(tags_to_db(&note.tags)?)
    .bind(i64::from(note.is_pinned))
    .bind(i64::from(note.is_archived))
    .bind(i64::from(note.is_default))
    .bind(note.parent_id.as_ref().map(NoteId::as_str))
    .bind(enum_to_db(&note.visibility)?)
    .bind(task_json)
    .bind(&note.created_at)
    .bind(&note.updated_at)
    .bind(expected_rev)
    .bind(required_link.map(|a| a.0.as_str()))
    .fetch_optional(&mut **conn)
    .await
    .map_err(internal("note update failed"))?;
    Ok(rev)
}

/// Allocate the next version number, insert the snapshot stamped with
/// `rev`, and prune to the newest [`MAX_NOTE_VERSIONS`]. Returns the new `v`.
pub(super) async fn insert_note_version(
    store: &PgStore,
    conn: &mut WriteTx,
    note: &Note,
    author: &NoteVersionAuthor,
    date: &str,
    rev: i64,
) -> Result<i64> {
    let next_v: i64 = sqlx::query_scalar(
        "SELECT COALESCE(MAX(v), 0) + 1 FROM note_version \
         WHERE tenant_id = $1 AND note_id = $2 AND workspace_id = $3",
    )
    .bind(store.tenant())
    .bind(note.id.as_str())
    .bind(note.workspace_id.as_str())
    .fetch_one(&mut **conn)
    .await
    .map_err(internal("next note_version failed"))?;
    sqlx::query(
        "INSERT INTO note_version (tenant_id, note_id, workspace_id, v, date, author_id, \
         author_name, author_type, title, content, rev) \
         VALUES ($1,$2,$3,$4,$5,$6,$7,$8,$9,$10,$11)",
    )
    .bind(store.tenant())
    .bind(note.id.as_str())
    .bind(note.workspace_id.as_str())
    .bind(next_v)
    .bind(date)
    .bind(&author.id)
    .bind(&author.name)
    .bind(&author.author_type)
    .bind(&note.title)
    .bind(&note.content)
    .bind(rev)
    .execute(&mut **conn)
    .await
    .map_err(internal("insert note_version failed"))?;
    sqlx::query(
        "DELETE FROM note_version WHERE tenant_id = $1 AND note_id = $2 AND workspace_id = $3 \
         AND v <= $4",
    )
    .bind(store.tenant())
    .bind(note.id.as_str())
    .bind(note.workspace_id.as_str())
    .bind(next_v - MAX_NOTE_VERSIONS)
    .execute(&mut **conn)
    .await
    .map_err(internal("prune note_version failed"))?;
    Ok(next_v)
}

fn map_author(row: &PgRow) -> Result<NoteVersionAuthor> {
    Ok(NoteVersionAuthor {
        id: col(row, "author_id")?,
        name: col(row, "author_name")?,
        author_type: col(row, "author_type")?,
    })
}

fn map_note_row(row: &PgRow) -> Result<Note> {
    let task: Option<TaskMetadata> = col::<Option<String>>(row, "task_json")?
        .map(|s| {
            serde_json::from_str(&s)
                .map_err(|e| Error::Internal(format!("decode task_json failed: {e}")))
        })
        .transpose()?;
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
        parent_id: col::<Option<String>>(row, "parent_id")?.map(NoteId),
        visibility: enum_from_db::<NoteVisibility>(&col::<String>(row, "visibility")?)?,
        metadata: NoteMetadata { task },
        created_at: col(row, "created_at")?,
        rev: col(row, "rev")?,
        updated_at: col(row, "updated_at")?,
    })
}
