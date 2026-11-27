//! Idempotency-key dedupe (Postgres). Mirrors `idempotency_repo.rs`.

use intent_core::{now_iso, Result};

use crate::idempotency_repo::{IdempotencyClaim, IdempotencyReservation, IDEMPOTENCY_CLAIM_TTL};

use super::{db, PgStore};

/// One reaper batch: `$1` tenant, `$2` cutoff, `$3` batch size.
const REAP_IDEMPOTENT_SQL: &str = "DELETE FROM idempotency_key WHERE tenant_id = $1 \
     AND (workspace_id, idempotency_key) IN (SELECT k.workspace_id, k.idempotency_key \
       FROM idempotency_key k WHERE k.tenant_id = $1 AND k.created_at < $2 COLLATE \"C\" \
       LIMIT $3)";

/// [`IDEMPOTENCY_CLAIM_TTL`] in milliseconds, for the expiry arithmetic.
fn claim_ttl_millis() -> Result<i64> {
    crate::lease_repo::ttl_millis(IDEMPOTENCY_CLAIM_TTL)
}

impl PgStore {
    /// Inserts a `pending` row for the key, or takes over one whose expiry
    /// passed by the database clock, under the writer lock; else reports the
    /// row that is there. The new reservation's token is this process's
    /// token plus a random id. The tenant epoch plays no part: a takeover
    /// leaves live reservations alone.
    pub async fn reserve_idempotent(
        &self,
        workspace_id: &str,
        key: &str,
        method: &str,
    ) -> Result<IdempotencyReservation> {
        let ttl = claim_ttl_millis()?;
        let claim =
            IdempotencyClaim::new(format!("{}/{}", self.process_token(), uuid::Uuid::new_v4()));
        let mut tx = self.begin_write().await?;
        let reserved = sqlx::query(
            "INSERT INTO idempotency_key (tenant_id, workspace_id, idempotency_key, method, \
             result_json, claim_token, claim_expires_at, created_at) \
             VALUES ($1, $2, $3, $4, NULL, $5, \
             clock_timestamp() + $6::bigint * interval '1 millisecond', $7) \
             ON CONFLICT (tenant_id, workspace_id, idempotency_key) DO UPDATE SET \
             method = excluded.method, claim_token = excluded.claim_token, \
             claim_expires_at = excluded.claim_expires_at \
             WHERE idempotency_key.result_json IS NULL \
             AND idempotency_key.claim_expires_at <= clock_timestamp()",
        )
        .bind(self.tenant())
        .bind(workspace_id)
        .bind(key)
        .bind(method)
        .bind(claim.token())
        .bind(ttl)
        .bind(now_iso())
        .execute(&mut *tx)
        .await
        .map_err(db)?
        .rows_affected()
            > 0;
        let outcome = if reserved {
            IdempotencyReservation::Reserved(claim)
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

    /// Marks `claim`'s reservation `done` with the result. When the
    /// reservation is gone (reaped) the result is inserted as before; when
    /// another process took it over, that process's outcome stands and this
    /// returns `false`.
    pub async fn complete_idempotent(
        &self,
        workspace_id: &str,
        key: &str,
        method: &str,
        claim: &IdempotencyClaim,
        result_json: &str,
    ) -> Result<bool> {
        let mut tx = self.begin_write().await?;
        let mut stored = sqlx::query(
            "UPDATE idempotency_key SET result_json = $4, \
             claim_token = NULL, claim_expires_at = NULL \
             WHERE tenant_id = $1 AND workspace_id = $2 AND idempotency_key = $3 \
             AND result_json IS NULL AND claim_token = $5",
        )
        .bind(self.tenant())
        .bind(workspace_id)
        .bind(key)
        .bind(result_json)
        .bind(claim.token())
        .execute(&mut *tx)
        .await
        .map_err(db)?
        .rows_affected();
        if stored == 0 {
            stored = sqlx::query(
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
        }
        tx.commit().await.map_err(db)?;
        Ok(stored > 0)
    }

    /// Restarts the expiry of `claim`'s `pending` reservation, also after it
    /// passed while nobody took the key over.
    pub async fn renew_idempotent(
        &self,
        workspace_id: &str,
        key: &str,
        claim: &IdempotencyClaim,
    ) -> Result<bool> {
        let ttl = claim_ttl_millis()?;
        let mut tx = self.begin_write().await?;
        let renewed = sqlx::query(
            "UPDATE idempotency_key \
             SET claim_expires_at = clock_timestamp() + $5::bigint * interval '1 millisecond' \
             WHERE tenant_id = $1 AND workspace_id = $2 AND idempotency_key = $3 \
             AND result_json IS NULL AND claim_token = $4",
        )
        .bind(self.tenant())
        .bind(workspace_id)
        .bind(key)
        .bind(claim.token())
        .bind(ttl)
        .execute(&mut *tx)
        .await
        .map_err(db)?
        .rows_affected()
            > 0;
        tx.commit().await.map_err(db)?;
        Ok(renewed)
    }

    /// Deletes `claim`'s `pending` reservation; another's stays.
    pub async fn release_idempotent(
        &self,
        workspace_id: &str,
        key: &str,
        claim: &IdempotencyClaim,
    ) -> Result<()> {
        let mut tx = self.begin_write().await?;
        sqlx::query(
            "DELETE FROM idempotency_key \
             WHERE tenant_id = $1 AND workspace_id = $2 AND idempotency_key = $3 \
             AND result_json IS NULL AND claim_token = $4",
        )
        .bind(self.tenant())
        .bind(workspace_id)
        .bind(key)
        .bind(claim.token())
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

    /// Deletes the tenant's keys created before `cutoff`, pending or not,
    /// in bounded batches (each its own writer transaction, see
    /// `delete_in_batches`). Each batch's `LIMIT` subquery can use
    /// `idx_idempotency_created` for the range only where the column's
    /// collation is `"C"` (the comparison is `COLLATE "C"`); otherwise it
    /// scans the tenant's keys.
    pub async fn reap_idempotent(&self, cutoff: &str) -> Result<u64> {
        self.delete_in_batches(REAP_IDEMPOTENT_SQL, cutoff, super::purge::REAP_BATCH_ROWS)
            .await
    }
}
