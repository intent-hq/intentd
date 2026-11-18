//! Idempotency-key dedupe (Postgres). Mirrors `idempotency_repo.rs`.

use intent_core::{now_iso, Result};

use super::{db, PgStore};

impl PgStore {
    pub async fn get_idempotent(&self, workspace_id: &str, key: &str) -> Result<Option<String>> {
        let mut tx = self.begin().await?;
        let value = sqlx::query_scalar(
            "SELECT result_json FROM idempotency_key \
             WHERE tenant_id = $1 AND workspace_id = $2 AND idempotency_key = $3",
        )
        .bind(self.tenant())
        .bind(workspace_id)
        .bind(key)
        .fetch_optional(&mut *tx)
        .await
        .map_err(db)?;
        tx.commit().await.map_err(db)?;
        Ok(value)
    }

    pub async fn put_idempotent(
        &self,
        workspace_id: &str,
        key: &str,
        method: &str,
        result_json: &str,
    ) -> Result<bool> {
        let mut tx = self.begin_write().await?;
        let inserted = sqlx::query(
            "INSERT INTO idempotency_key \
             (tenant_id, workspace_id, idempotency_key, method, result_json, created_at) \
             VALUES ($1, $2, $3, $4, $5, $6) ON CONFLICT DO NOTHING",
        )
        .bind(self.tenant())
        .bind(workspace_id)
        .bind(key)
        .bind(method)
        .bind(result_json)
        .bind(now_iso())
        .execute(&mut *tx)
        .await
        .map_err(db)?
        .rows_affected();
        tx.commit().await.map_err(db)?;
        Ok(inserted > 0)
    }

    pub async fn reap_idempotent(&self, cutoff: &str) -> Result<u64> {
        let mut tx = self.begin_write().await?;
        let removed = sqlx::query(
            "DELETE FROM idempotency_key WHERE tenant_id = $1 AND created_at < $2 COLLATE \"C\"",
        )
        .bind(self.tenant())
        .bind(cutoff)
        .execute(&mut *tx)
        .await
        .map_err(db)?
        .rows_affected();
        tx.commit().await.map_err(db)?;
        Ok(removed)
    }
}
