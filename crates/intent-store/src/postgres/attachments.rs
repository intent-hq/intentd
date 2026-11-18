//! Attachments, stored diffs and tracked changes (Postgres). Mirrors
//! `attachment_repo.rs`, `diffs_repo.rs` and `tracked_changes_repo.rs`.
//! Timestamps and ids are text compared bytewise in `SQLite`, so their
//! comparisons and ordering use `COLLATE "C"`; `row_seq` breaks ties in
//! insertion order.

use intent_core::{now_iso, Error, Result, WorkspaceId};
use sqlx::postgres::PgRow;
use sqlx::Row;
use uuid::Uuid;

use super::{db, PgStore, WriteTx};
use crate::attachment_repo::{AttachmentIdempotencyBinding, AttachmentRecord};
use crate::diffs_repo::{DiffRow, NewDiff};
use crate::tracked_changes_repo::{NewTrackedChange, TrackedChangeRow};

fn row_to_attachment(row: &PgRow) -> AttachmentRecord {
    AttachmentRecord {
        id: row.get("id"),
        workspace_id: WorkspaceId(row.get("workspace_id")),
        file_name: row.get("file_name"),
        mime_type: row.get("mime_type"),
        size: row.get("size"),
        uploaded_at: row.get("uploaded_at"),
        stored_path: row.get("stored_path"),
    }
}

fn row_to_diff(r: &PgRow) -> DiffRow {
    DiffRow {
        id: r.get("id"),
        workspace_id: WorkspaceId(r.get("workspace_id")),
        file_path: r.get("file_path"),
        staged: r.get::<i64, _>("staged") != 0,
        old_content: r.get("old_content"),
        new_content: r.get("new_content"),
        hunks_json: r.get("hunks_json"),
        created_at: r.get("created_at"),
        updated_at: r.get("updated_at"),
    }
}

fn row_to_tracked_change(r: &PgRow) -> TrackedChangeRow {
    TrackedChangeRow {
        id: r.get("id"),
        workspace_id: WorkspaceId(r.get("workspace_id")),
        path: r.get("path"),
        stage: r.get("stage"),
        status: r.get("status"),
        agent_id: r.get("agent_id"),
        session_id: r.get("session_id"),
        turn: r.get("turn"),
        commit_hash: r.get("commit_hash"),
        old_blob_sha: r.get("old_blob_sha"),
        new_blob_sha: r.get("new_blob_sha"),
        additions: r.get("additions"),
        deletions: r.get("deletions"),
        created_at: r.get("created_at"),
        updated_at: r.get("updated_at"),
    }
}

async fn insert_attachment_row(tx: &mut WriteTx, tenant: &str, a: &AttachmentRecord) -> Result<()> {
    sqlx::query(
        "INSERT INTO attachments \
         (tenant_id, id, workspace_id, file_name, mime_type, size, uploaded_at, stored_path) \
         VALUES ($1, $2, $3, $4, $5, $6, $7, $8)",
    )
    .bind(tenant)
    .bind(&a.id)
    .bind(&a.workspace_id.0)
    .bind(&a.file_name)
    .bind(&a.mime_type)
    .bind(a.size)
    .bind(&a.uploaded_at)
    .bind(&a.stored_path)
    .execute(&mut **tx)
    .await
    .map_err(|e| Error::Internal(format!("insert attachment failed: {e}")))?;
    Ok(())
}

impl PgStore {
    pub async fn insert_attachment_with_idempotency_key(
        &self,
        a: &AttachmentRecord,
        key: &str,
        fingerprint: &str,
        expired_before: &str,
    ) -> Result<()> {
        let mut tx = self.begin_write().await?;
        sqlx::query(
            "DELETE FROM attachment_idempotency_keys \
             WHERE tenant_id = $1 AND workspace_id = $2 AND \"key\" = $3 \
             AND created_at <= $4 COLLATE \"C\"",
        )
        .bind(self.tenant())
        .bind(&a.workspace_id.0)
        .bind(key)
        .bind(expired_before)
        .execute(&mut *tx)
        .await
        .map_err(db)?;
        insert_attachment_row(&mut tx, self.tenant(), a).await?;
        sqlx::query(
            "INSERT INTO attachment_idempotency_keys \
             (tenant_id, workspace_id, \"key\", attachment_id, fingerprint, created_at) \
             VALUES ($1, $2, $3, $4, $5, $6)",
        )
        .bind(self.tenant())
        .bind(&a.workspace_id.0)
        .bind(key)
        .bind(&a.id)
        .bind(fingerprint)
        .bind(&a.uploaded_at)
        .execute(&mut *tx)
        .await
        .map_err(|e| {
            if e.as_database_error()
                .is_some_and(sqlx::error::DatabaseError::is_unique_violation)
            {
                Error::InvalidParams(format!(
                    "idempotencyKey {key:?} is already bound in this workspace"
                ))
            } else {
                Error::Internal(format!("insert attachment idempotency key failed: {e}"))
            }
        })?;
        tx.commit().await.map_err(db)
    }

    pub async fn get_attachment_by_idempotency_key(
        &self,
        workspace_id: &WorkspaceId,
        key: &str,
        expired_before: &str,
    ) -> Result<Option<(AttachmentIdempotencyBinding, AttachmentRecord)>> {
        let mut tx = self.begin().await?;
        let row = sqlx::query(
            "SELECT k.workspace_id AS k_workspace_id, k.\"key\" AS k_key, \
             k.attachment_id AS k_attachment_id, k.fingerprint AS k_fingerprint, \
             k.created_at AS k_created_at, a.id, a.workspace_id, a.file_name, a.mime_type, \
             a.size, a.uploaded_at, a.stored_path \
             FROM attachment_idempotency_keys k \
             JOIN attachments a ON a.tenant_id = $1 AND a.id = k.attachment_id \
             WHERE k.tenant_id = $1 AND k.workspace_id = $2 AND k.\"key\" = $3 \
             AND k.created_at > $4 COLLATE \"C\"",
        )
        .bind(self.tenant())
        .bind(&workspace_id.0)
        .bind(key)
        .bind(expired_before)
        .fetch_optional(&mut *tx)
        .await
        .map_err(db)?;
        tx.commit().await.map_err(db)?;
        Ok(row.map(|row| {
            (
                AttachmentIdempotencyBinding {
                    workspace_id: WorkspaceId(row.get("k_workspace_id")),
                    key: row.get("k_key"),
                    attachment_id: row.get("k_attachment_id"),
                    fingerprint: row.get("k_fingerprint"),
                    created_at: row.get("k_created_at"),
                },
                row_to_attachment(&row),
            )
        }))
    }

    pub async fn sweep_expired_attachment_idempotency_keys(
        &self,
        expired_before: &str,
    ) -> Result<u64> {
        let mut tx = self.begin_write().await?;
        let removed = sqlx::query(
            "DELETE FROM attachment_idempotency_keys \
             WHERE tenant_id = $1 AND created_at <= $2 COLLATE \"C\"",
        )
        .bind(self.tenant())
        .bind(expired_before)
        .execute(&mut *tx)
        .await
        .map_err(db)?
        .rows_affected();
        tx.commit().await.map_err(db)?;
        Ok(removed)
    }

    pub async fn insert_attachment(&self, a: &AttachmentRecord) -> Result<()> {
        let mut tx = self.begin_write().await?;
        insert_attachment_row(&mut tx, self.tenant(), a).await?;
        tx.commit().await.map_err(db)
    }

    pub async fn get_attachment(&self, id: &str) -> Result<AttachmentRecord> {
        let mut tx = self.begin().await?;
        let row = sqlx::query(
            "SELECT id, workspace_id, file_name, mime_type, size, uploaded_at, stored_path \
             FROM attachments WHERE tenant_id = $1 AND id = $2",
        )
        .bind(self.tenant())
        .bind(id)
        .fetch_optional(&mut *tx)
        .await
        .map_err(db)?;
        tx.commit().await.map_err(db)?;
        row.map(|r| row_to_attachment(&r))
            .ok_or_else(|| Error::NotFound(format!("attachment {id}")))
    }

    pub async fn list_attachments(
        &self,
        workspace_id: &WorkspaceId,
    ) -> Result<Vec<AttachmentRecord>> {
        let mut tx = self.begin().await?;
        let rows = sqlx::query(
            "SELECT id, workspace_id, file_name, mime_type, size, uploaded_at, stored_path \
             FROM attachments WHERE tenant_id = $1 AND workspace_id = $2 ORDER BY id COLLATE \"C\"",
        )
        .bind(self.tenant())
        .bind(&workspace_id.0)
        .fetch_all(&mut *tx)
        .await
        .map_err(db)?;
        tx.commit().await.map_err(db)?;
        Ok(rows.iter().map(row_to_attachment).collect())
    }

    pub async fn upsert_diff(&self, d: &NewDiff) -> Result<()> {
        let now = now_iso();
        let mut tx = self.begin_write().await?;
        sqlx::query(
            "INSERT INTO diffs (tenant_id, id, workspace_id, file_path, staged, old_content, \
             new_content, hunks_json, created_at, updated_at) \
             VALUES ($1, $2, $3, $4, $5, $6, $7, $8, $9, $9) \
             ON CONFLICT (tenant_id, workspace_id, file_path, staged) DO UPDATE SET \
             old_content = excluded.old_content, new_content = excluded.new_content, \
             hunks_json = excluded.hunks_json, updated_at = excluded.updated_at",
        )
        .bind(self.tenant())
        .bind(Uuid::now_v7().to_string())
        .bind(&d.workspace_id.0)
        .bind(&d.file_path)
        .bind(i64::from(d.staged))
        .bind(&d.old_content)
        .bind(&d.new_content)
        .bind(&d.hunks_json)
        .bind(&now)
        .execute(&mut *tx)
        .await
        .map_err(|e| Error::Internal(format!("upsert diff failed: {e}")))?;
        tx.commit().await.map_err(db)
    }

    pub async fn list_diffs(&self, workspace_id: &WorkspaceId) -> Result<Vec<DiffRow>> {
        let mut tx = self.begin().await?;
        let rows = sqlx::query(
            "SELECT id, workspace_id, file_path, staged, old_content, new_content, hunks_json, \
             created_at, updated_at FROM diffs WHERE tenant_id = $1 AND workspace_id = $2 \
             ORDER BY created_at COLLATE \"C\", row_seq",
        )
        .bind(self.tenant())
        .bind(&workspace_id.0)
        .fetch_all(&mut *tx)
        .await
        .map_err(db)?;
        tx.commit().await.map_err(db)?;
        Ok(rows.iter().map(row_to_diff).collect())
    }

    pub async fn upsert_tracked_change(&self, c: &NewTrackedChange) -> Result<Option<(i64, i64)>> {
        let now = now_iso();
        let mut tx = self.begin_write().await?;
        let existing: Option<(String, i64, i64)> = sqlx::query_as(
            "SELECT id, additions, deletions FROM tracked_changes \
             WHERE tenant_id = $1 AND workspace_id = $2 AND path = $3 AND stage = $4 \
             AND agent_id IS NOT DISTINCT FROM $5 ORDER BY row_seq LIMIT 1",
        )
        .bind(self.tenant())
        .bind(&c.workspace_id.0)
        .bind(&c.path)
        .bind(&c.stage)
        .bind(&c.agent_id)
        .fetch_optional(&mut *tx)
        .await
        .map_err(db)?;

        let previous = if let Some((id, prev_additions, prev_deletions)) = existing {
            sqlx::query(
                "UPDATE tracked_changes SET status = $3, agent_id = $4, session_id = $5, \
                 turn = $6, commit_hash = $7, old_blob_sha = $8, new_blob_sha = $9, \
                 additions = $10, deletions = $11, updated_at = $12 \
                 WHERE tenant_id = $1 AND id = $2",
            )
            .bind(self.tenant())
            .bind(&id)
            .bind(&c.status)
            .bind(&c.agent_id)
            .bind(&c.session_id)
            .bind(c.turn)
            .bind(&c.commit_hash)
            .bind(&c.old_blob_sha)
            .bind(&c.new_blob_sha)
            .bind(c.additions)
            .bind(c.deletions)
            .bind(&now)
            .execute(&mut *tx)
            .await
            .map_err(|e| Error::Internal(format!("update tracked change failed: {e}")))?;
            Some((prev_additions, prev_deletions))
        } else {
            sqlx::query(
                "INSERT INTO tracked_changes (tenant_id, id, workspace_id, path, stage, status, \
                 agent_id, session_id, turn, commit_hash, old_blob_sha, new_blob_sha, additions, \
                 deletions, created_at, updated_at) \
                 VALUES ($1, $2, $3, $4, $5, $6, $7, $8, $9, $10, $11, $12, $13, $14, $15, $15)",
            )
            .bind(self.tenant())
            .bind(Uuid::now_v7().to_string())
            .bind(&c.workspace_id.0)
            .bind(&c.path)
            .bind(&c.stage)
            .bind(&c.status)
            .bind(&c.agent_id)
            .bind(&c.session_id)
            .bind(c.turn)
            .bind(&c.commit_hash)
            .bind(&c.old_blob_sha)
            .bind(&c.new_blob_sha)
            .bind(c.additions)
            .bind(c.deletions)
            .bind(&now)
            .execute(&mut *tx)
            .await
            .map_err(|e| Error::Internal(format!("insert tracked change failed: {e}")))?;
            None
        };
        tx.commit().await.map_err(db)?;
        Ok(previous)
    }

    pub async fn max_sibling_tracked_change_counters(
        &self,
        workspace_id: &WorkspaceId,
        path: &str,
        stage: &str,
        agent_id: Option<&str>,
    ) -> Result<Option<(i64, i64)>> {
        let mut tx = self.begin().await?;
        let (additions, deletions): (Option<i64>, Option<i64>) = sqlx::query_as(
            "SELECT MAX(additions), MAX(deletions) FROM tracked_changes \
             WHERE tenant_id = $1 AND workspace_id = $2 AND path = $3 AND stage = $4 \
             AND agent_id IS DISTINCT FROM $5",
        )
        .bind(self.tenant())
        .bind(&workspace_id.0)
        .bind(path)
        .bind(stage)
        .bind(agent_id)
        .fetch_one(&mut *tx)
        .await
        .map_err(db)?;
        tx.commit().await.map_err(db)?;
        Ok(additions.zip(deletions))
    }

    pub async fn set_tracked_change_stage(
        &self,
        workspace_id: &WorkspaceId,
        path: &str,
        from_stage: &str,
        to_stage: &str,
    ) -> Result<u64> {
        let mut tx = self.begin_write().await?;
        let moved = sqlx::query(
            "UPDATE tracked_changes SET stage = $2, updated_at = $3 \
             WHERE tenant_id = $1 AND workspace_id = $4 AND path = $5 AND stage = $6",
        )
        .bind(self.tenant())
        .bind(to_stage)
        .bind(now_iso())
        .bind(&workspace_id.0)
        .bind(path)
        .bind(from_stage)
        .execute(&mut *tx)
        .await
        .map_err(db)?
        .rows_affected();
        tx.commit().await.map_err(db)?;
        Ok(moved)
    }

    pub async fn list_workspaces_with_working_tracked_changes(&self) -> Result<Vec<WorkspaceId>> {
        let mut tx = self.begin().await?;
        let ids: Vec<String> = sqlx::query_scalar(
            "SELECT workspace_id FROM tracked_changes \
             WHERE tenant_id = $1 AND stage IN ('unstaged', 'staged') \
             GROUP BY workspace_id ORDER BY workspace_id COLLATE \"C\"",
        )
        .bind(self.tenant())
        .fetch_all(&mut *tx)
        .await
        .map_err(db)?;
        tx.commit().await.map_err(db)?;
        Ok(ids.into_iter().map(WorkspaceId).collect())
    }

    pub async fn delete_unchanged_working_tracked_changes(
        &self,
        workspace_id: &WorkspaceId,
        rows: &[(String, String)],
    ) -> Result<u64> {
        let (ids, stamps): (Vec<&str>, Vec<&str>) = rows
            .iter()
            .map(|(id, updated_at)| (id.as_str(), updated_at.as_str()))
            .unzip();
        let mut tx = self.begin_write().await?;
        let deleted = sqlx::query(
            "DELETE FROM tracked_changes WHERE tenant_id = $1 AND workspace_id = $2 \
             AND stage IN ('unstaged', 'staged') \
             AND (id, updated_at) IN (SELECT * FROM unnest($3::text[], $4::text[]))",
        )
        .bind(self.tenant())
        .bind(&workspace_id.0)
        .bind(&ids)
        .bind(&stamps)
        .execute(&mut *tx)
        .await
        .map_err(db)?
        .rows_affected();
        tx.commit().await.map_err(db)?;
        Ok(deleted)
    }

    pub async fn list_tracked_changes(
        &self,
        workspace_id: &WorkspaceId,
    ) -> Result<Vec<TrackedChangeRow>> {
        let mut tx = self.begin().await?;
        let rows = sqlx::query(
            "SELECT id, workspace_id, path, stage, status, agent_id, session_id, turn, \
             commit_hash, old_blob_sha, new_blob_sha, additions, deletions, created_at, \
             updated_at FROM tracked_changes WHERE tenant_id = $1 AND workspace_id = $2 \
             ORDER BY created_at COLLATE \"C\", row_seq",
        )
        .bind(self.tenant())
        .bind(&workspace_id.0)
        .fetch_all(&mut *tx)
        .await
        .map_err(db)?;
        tx.commit().await.map_err(db)?;
        Ok(rows.iter().map(row_to_tracked_change).collect())
    }
}
