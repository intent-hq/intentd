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

    pub async fn claim_delete_grace(&self, subject_kind: &str, subject_id: &str) -> Result<bool> {
        let mut tx = self.begin_write().await?;
        let epoch = tx.writer().epoch;
        let claimed = sqlx::query(
            "UPDATE delete_grace SET claim_epoch = $4, claim_token = $5 \
             WHERE tenant_id = $1 AND subject_kind = $2 AND subject_id = $3 \
             AND (claim_token IS NULL OR claim_token = $5 OR claim_epoch < $4)",
        )
        .bind(self.tenant())
        .bind(subject_kind)
        .bind(subject_id)
        .bind(epoch)
        .bind(self.process_token())
        .execute(&mut *tx)
        .await
        .map_err(db)?
        .rows_affected();
        tx.commit().await.map_err(db)?;
        Ok(claimed > 0)
    }

    pub async fn finish_delete_grace(&self, subject_kind: &str, subject_id: &str) -> Result<()> {
        let mut tx = self.begin_write().await?;
        sqlx::query(
            "DELETE FROM delete_grace \
             WHERE tenant_id = $1 AND subject_kind = $2 AND subject_id = $3 AND claim_token = $4",
        )
        .bind(self.tenant())
        .bind(subject_kind)
        .bind(subject_id)
        .bind(self.process_token())
        .execute(&mut *tx)
        .await
        .map_err(db)?;
        tx.commit().await.map_err(db)
    }

    pub async fn release_delete_grace(&self, subject_kind: &str, subject_id: &str) -> Result<()> {
        let mut tx = self.begin_write().await?;
        sqlx::query(
            "UPDATE delete_grace SET claim_epoch = NULL, claim_token = NULL \
             WHERE tenant_id = $1 AND subject_kind = $2 AND subject_id = $3 AND claim_token = $4",
        )
        .bind(self.tenant())
        .bind(subject_kind)
        .bind(subject_id)
        .bind(self.process_token())
        .execute(&mut *tx)
        .await
        .map_err(db)?;
        tx.commit().await.map_err(db)
    }

    pub async fn cancel_delete_grace(&self, subject_kind: &str, subject_id: &str) -> Result<bool> {
        let mut tx = self.begin_write().await?;
        let cancelled = sqlx::query(
            "DELETE FROM delete_grace \
             WHERE tenant_id = $1 AND subject_kind = $2 AND subject_id = $3 \
             AND claim_token IS NULL",
        )
        .bind(self.tenant())
        .bind(subject_kind)
        .bind(subject_id)
        .execute(&mut *tx)
        .await
        .map_err(db)?
        .rows_affected();
        tx.commit().await.map_err(db)?;
        Ok(cancelled > 0)
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
