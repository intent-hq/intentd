//! Persisted delete grace deadlines (Postgres). Mirrors `delete_grace_repo.rs`.

use intent_core::{now_iso, Result};

use super::{db, PgStore};
use crate::DeleteGrace;

impl PgStore {
    pub async fn record_delete_grace(&self, grace: &DeleteGrace) -> Result<String> {
        let mut tx = self.begin_write().await?;
        sqlx::query(
            "INSERT INTO delete_grace \
             (tenant_id, subject_kind, subject_id, workspace_id, delete_at, created_at) \
             VALUES ($1, $2, $3, $4, $5, $6) \
             ON CONFLICT (tenant_id, subject_kind, subject_id) DO NOTHING",
        )
        .bind(self.tenant())
        .bind(&grace.subject_kind)
        .bind(&grace.subject_id)
        .bind(&grace.workspace_id)
        .bind(&grace.delete_at)
        .bind(now_iso())
        .execute(&mut *tx)
        .await
        .map_err(db)?;
        let stored: String = sqlx::query_scalar(
            "SELECT delete_at FROM delete_grace \
             WHERE tenant_id = $1 AND subject_kind = $2 AND subject_id = $3",
        )
        .bind(self.tenant())
        .bind(&grace.subject_kind)
        .bind(&grace.subject_id)
        .fetch_one(&mut *tx)
        .await
        .map_err(db)?;
        tx.commit().await.map_err(db)?;
        Ok(stored)
    }

    pub async fn take_delete_grace(&self, subject_kind: &str, subject_id: &str) -> Result<bool> {
        let mut tx = self.begin_write().await?;
        let taken = sqlx::query(
            "DELETE FROM delete_grace \
             WHERE tenant_id = $1 AND subject_kind = $2 AND subject_id = $3",
        )
        .bind(self.tenant())
        .bind(subject_kind)
        .bind(subject_id)
        .execute(&mut *tx)
        .await
        .map_err(db)?
        .rows_affected();
        tx.commit().await.map_err(db)?;
        Ok(taken > 0)
    }

    pub async fn list_delete_graces(&self) -> Result<Vec<DeleteGrace>> {
        let mut tx = self.begin().await?;
        let rows: Vec<(String, String, String, String)> = sqlx::query_as(
            "SELECT subject_kind, subject_id, workspace_id, delete_at FROM delete_grace \
             WHERE tenant_id = $1 \
             ORDER BY delete_at COLLATE \"C\", subject_kind COLLATE \"C\", subject_id COLLATE \"C\"",
        )
        .bind(self.tenant())
        .fetch_all(&mut *tx)
        .await
        .map_err(db)?;
        tx.commit().await.map_err(db)?;
        Ok(rows
            .into_iter()
            .map(
                |(subject_kind, subject_id, workspace_id, delete_at)| DeleteGrace {
                    subject_kind,
                    subject_id,
                    workspace_id,
                    delete_at,
                },
            )
            .collect())
    }
}
