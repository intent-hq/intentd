//! Work claims (Postgres). Mirrors `work_claim_repo.rs`.
//!
//! The `work_claim` row names the claim (`claim_id`), the claiming process
//! (`claim_token`, its [`ProcessToken`](super::completion_watches)) and the
//! tenant epoch it was taken at, as delivery claims do, plus an expiry set
//! and compared by the database clock (`clock_timestamp()`) alone, so the
//! processes' clocks never meet. Every statement runs under the tenant
//! writer lock (`begin_write`), which also fences a process that lost the
//! tenant.
//!
//! Unlike delivery claims, the unleased first open
//! (`clear_foreign_claims_once`) leaves work claims alone: they lapse by
//! their TTL, and clearing them would let two unleased processes both run
//! the work.

use intent_core::{Error, Result};

use super::{db, PgStore};
use crate::work_claim_repo::{ttl_millis, WorkClaim};

/// Takes the work ($1 tenant, $2 kind, $3 key) for claim $4 of process $5
/// at epoch $6 for $7 ms: when no row exists, or the row's claim expired by
/// the database clock or was taken at an older epoch. Returns a row only
/// when it was taken.
const TAKE_SQL: &str = "INSERT INTO work_claim \
     (tenant_id, kind, work_key, claim_id, claim_token, claim_epoch, expires_at, claimed_at) \
     VALUES ($1, $2, $3, $4, $5, $6, \
     clock_timestamp() + $7::bigint * interval '1 millisecond', clock_timestamp()) \
     ON CONFLICT (tenant_id, kind, work_key) DO UPDATE SET \
     claim_id = excluded.claim_id, claim_token = excluded.claim_token, \
     claim_epoch = excluded.claim_epoch, expires_at = excluded.expires_at, \
     claimed_at = excluded.claimed_at \
     WHERE work_claim.expires_at <= clock_timestamp() \
     OR work_claim.claim_epoch < excluded.claim_epoch \
     RETURNING claim_id";

/// Restarts the TTL ($5 ms) of claim $4 on ($1 tenant, $2 kind, $3 key)
/// while the row still names it at epoch $6.
const RENEW_SQL: &str = "UPDATE work_claim \
     SET expires_at = clock_timestamp() + $5::bigint * interval '1 millisecond' \
     WHERE tenant_id = $1 AND kind = $2 AND work_key = $3 AND claim_id = $4 \
     AND claim_epoch = $6";

/// Drops the row of ($1 tenant, $2 kind, $3 key) while it names claim $4.
const RELEASE_SQL: &str = "DELETE FROM work_claim \
     WHERE tenant_id = $1 AND kind = $2 AND work_key = $3 AND claim_id = $4";

impl PgStore {
    pub async fn take_work_claim(&self, claim: &WorkClaim) -> Result<bool> {
        let ttl = ttl_millis(claim.ttl())?;
        let mut tx = self.begin_write().await?;
        let epoch = tx.writer().epoch;
        let taken = sqlx::query_scalar::<_, String>(TAKE_SQL)
            .bind(self.tenant())
            .bind(claim.kind())
            .bind(claim.key())
            .bind(claim.claim_id())
            .bind(self.process_token())
            .bind(epoch)
            .bind(ttl)
            .fetch_optional(&mut *tx)
            .await
            .map_err(|e| Error::Internal(format!("claim work failed: {e}")))?
            .is_some();
        tx.commit().await.map_err(db)?;
        Ok(taken)
    }

    pub async fn renew_work_claim(&self, claim: &WorkClaim) -> Result<bool> {
        let ttl = ttl_millis(claim.ttl())?;
        let mut tx = self.begin_write().await?;
        let epoch = tx.writer().epoch;
        let kept = sqlx::query(RENEW_SQL)
            .bind(self.tenant())
            .bind(claim.kind())
            .bind(claim.key())
            .bind(claim.claim_id())
            .bind(ttl)
            .bind(epoch)
            .execute(&mut *tx)
            .await
            .map_err(|e| Error::Internal(format!("renew work claim failed: {e}")))?
            .rows_affected()
            > 0;
        tx.commit().await.map_err(db)?;
        Ok(kept)
    }

    pub async fn release_work_claim(&self, claim: &WorkClaim) -> Result<()> {
        let mut tx = self.begin_write().await?;
        sqlx::query(RELEASE_SQL)
            .bind(self.tenant())
            .bind(claim.kind())
            .bind(claim.key())
            .bind(claim.claim_id())
            .execute(&mut *tx)
            .await
            .map_err(|e| Error::Internal(format!("release work claim failed: {e}")))?;
        tx.commit().await.map_err(db)
    }
}
