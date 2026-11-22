//! Idempotency-key dedupe (Postgres). Mirrors `idempotency_repo.rs`.

use intent_core::{now_iso, Result};

use crate::idempotency_repo::IdempotencyReservation;

use super::{db, PgStore};

impl PgStore {
    /// Inserts a `pending` row for the key, or takes over one that is this
    /// process's or from an older epoch, under the writer lock; else reports
    /// the row that is there.
    pub async fn reserve_idempotent(
        &self,
        workspace_id: &str,
        key: &str,
        method: &str,
    ) -> Result<IdempotencyReservation> {
        let mut tx = self.begin_write().await?;
        let epoch = tx.writer().epoch;
        let reserved = sqlx::query(
            "INSERT INTO idempotency_key (tenant_id, workspace_id, idempotency_key, method, \
             result_json, claim_epoch, claim_token, created_at) \
             VALUES ($1, $2, $3, $4, NULL, $5, $6, $7) \
             ON CONFLICT (tenant_id, workspace_id, idempotency_key) DO UPDATE SET \
             method = excluded.method, claim_epoch = excluded.claim_epoch, \
             claim_token = excluded.claim_token \
             WHERE idempotency_key.result_json IS NULL \
             AND (idempotency_key.claim_token = excluded.claim_token \
               OR idempotency_key.claim_epoch < excluded.claim_epoch)",
        )
        .bind(self.tenant())
        .bind(workspace_id)
        .bind(key)
        .bind(method)
        .bind(epoch)
        .bind(self.process_token())
        .bind(now_iso())
        .execute(&mut *tx)
        .await
        .map_err(db)?
        .rows_affected()
            > 0;
        let outcome = if reserved {
            IdempotencyReservation::Reserved
        } else {
            let row: Option<Option<String>> = sqlx::query_scalar(
                "SELECT result_json FROM idempotency_key \
                 WHERE tenant_id = $1 AND workspace_id = $2 AND idempotency_key = $3",
            )
            .bind(self.tenant())
            .bind(workspace_id)
            .bind(key)
            .fetch_optional(&mut *tx)
            .await
            .map_err(db)?;
            match row {
                Some(Some(result)) => IdempotencyReservation::Done(result),
                _ => IdempotencyReservation::InProgress,
            }
        };
        tx.commit().await.map_err(db)?;
        Ok(outcome)
    }

    /// Marks this process's reservation `done` with the result. When the
    /// reservation is gone (reaped) the result is inserted as before; when
    /// another process took it over, that process's outcome stands.
    pub async fn complete_idempotent(
        &self,
        workspace_id: &str,
        key: &str,
        method: &str,
        result_json: &str,
    ) -> Result<()> {
        let mut tx = self.begin_write().await?;
        let done = sqlx::query(
            "UPDATE idempotency_key SET result_json = $4, \
             claim_epoch = NULL, claim_token = NULL \
             WHERE tenant_id = $1 AND workspace_id = $2 AND idempotency_key = $3 \
             AND result_json IS NULL AND claim_token = $5",
        )
        .bind(self.tenant())
        .bind(workspace_id)
        .bind(key)
        .bind(result_json)
        .bind(self.process_token())
        .execute(&mut *tx)
        .await
        .map_err(db)?
        .rows_affected();
        if done == 0 {
            sqlx::query(
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
            .map_err(db)?;
        }
        tx.commit().await.map_err(db)
    }

    /// Deletes this process's `pending` reservation; another's stays.
    pub async fn release_idempotent(&self, workspace_id: &str, key: &str) -> Result<()> {
        let mut tx = self.begin_write().await?;
        sqlx::query(
            "DELETE FROM idempotency_key \
             WHERE tenant_id = $1 AND workspace_id = $2 AND idempotency_key = $3 \
             AND result_json IS NULL AND claim_token = $4",
        )
        .bind(self.tenant())
        .bind(workspace_id)
        .bind(key)
        .bind(self.process_token())
        .execute(&mut *tx)
        .await
        .map_err(db)?;
        tx.commit().await.map_err(db)
    }

    pub async fn get_idempotent(&self, workspace_id: &str, key: &str) -> Result<Option<String>> {
        let mut tx = self.begin().await?;
        let value = sqlx::query_scalar(
            "SELECT result_json FROM idempotency_key \
             WHERE tenant_id = $1 AND workspace_id = $2 AND idempotency_key = $3 \
             AND result_json IS NOT NULL",
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
