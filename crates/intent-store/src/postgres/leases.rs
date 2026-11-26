//! Leases (Postgres). Mirrors `lease_repo.rs`, which states the rules.
//!
//! The `lease` row of (tenant, scope, key) names the holding process
//! (`holder`, its [`ProcessToken`](super::completion_watches)), the holding
//! lease (`lease_id`) and the key's fencing `token`, with an expiry set and
//! compared by the database clock (`clock_timestamp()`) alone, so the
//! processes' clocks never meet. Release clears the holder and keeps the
//! row, so the token only grows.
//!
//! No statement reads the tenant epoch: a tenant lease takeover leaves every
//! lease as it was. While the tenant lease exists, acquire, renew and
//! release take the tenant writer lock without its owner check
//! (`Fence::LeaseOnly`), so they order with the tenant's other writes and pass
//! the writer-lock lint; a process that lost the tenant keeps its leases.
//!
//! The unleased first open (`clear_foreign_claims_once`) leaves leases
//! alone: they lapse by their TTL, and clearing them would let two unleased
//! processes both run the work.

use std::time::Duration;

use intent_core::{Error, Result};

use super::{db, Fence, PgStore, WriteTx};
use crate::lease_repo::{ttl_millis, Lease, TENANT_SCOPE};

/// Acquires ($1 tenant, $2 scope, $3 key) for lease $4 of process $5 for
/// $6 ms when no row exists or the row is released or expired by the
/// database clock; the new holder's token is one higher than the row's.
/// Returns the token only when acquired.
const ACQUIRE_SQL: &str = "INSERT INTO lease \
     (tenant_id, scope, lease_key, holder, lease_id, token, expires_at, acquired_at) \
     VALUES ($1, $2, $3, $5, $4, 1, \
     clock_timestamp() + $6::bigint * interval '1 millisecond', clock_timestamp()) \
     ON CONFLICT (tenant_id, scope, lease_key) DO UPDATE SET \
     holder = excluded.holder, lease_id = excluded.lease_id, \
     token = lease.token + 1, expires_at = excluded.expires_at, \
     acquired_at = excluded.acquired_at \
     WHERE lease.lease_id IS NULL OR lease.expires_at <= clock_timestamp() \
     RETURNING token";

/// Restarts the TTL ($6 ms) of lease $4 with token $5 on ($1 tenant,
/// $2 scope, $3 key) while the row still names it.
const RENEW_SQL: &str = "UPDATE lease \
     SET expires_at = clock_timestamp() + $6::bigint * interval '1 millisecond' \
     WHERE tenant_id = $1 AND scope = $2 AND lease_key = $3 \
     AND lease_id = $4 AND token = $5";

/// Frees ($1 tenant, $2 scope, $3 key) while it names lease $4 with token
/// $5, keeping the row and its token.
const RELEASE_SQL: &str = "UPDATE lease \
     SET holder = NULL, lease_id = NULL, expires_at = clock_timestamp() \
     WHERE tenant_id = $1 AND scope = $2 AND lease_key = $3 \
     AND lease_id = $4 AND token = $5";

/// Whether ($1 tenant, $2 scope, $3 key) names lease $4 with token $5 and
/// has not expired by the database clock.
const HELD_SQL: &str = "SELECT EXISTS (SELECT 1 FROM lease \
     WHERE tenant_id = $1 AND scope = $2 AND lease_key = $3 \
     AND lease_id = $4 AND token = $5 AND expires_at > clock_timestamp())";

/// Deletes ($1 tenant, $2 scope, $3 key) while it names lease $4 with
/// token $5: the work under the key is done for good.
const RETIRE_SQL: &str = "DELETE FROM lease \
     WHERE tenant_id = $1 AND scope = $2 AND lease_key = $3 \
     AND lease_id = $4 AND token = $5";

/// [`HELD_SQL`] as a row lock: `FOR SHARE` makes an acquire, renewal or
/// release of the key wait until the guarding transaction ends.
const GUARD_SQL: &str = "SELECT 1 FROM lease \
     WHERE tenant_id = $1 AND scope = $2 AND lease_key = $3 \
     AND lease_id = $4 AND token = $5 AND expires_at > clock_timestamp() \
     FOR SHARE";

impl PgStore {
    pub async fn lease_acquire(&self, lease: &Lease) -> Result<Option<u64>> {
        let ttl = ttl_millis(lease.ttl())?;
        let mut tx: WriteTx = self.begin_write_as(Fence::LeaseOnly).await?;
        let token = sqlx::query_scalar::<_, i64>(ACQUIRE_SQL)
            .bind(self.tenant())
            .bind(lease.scope())
            .bind(lease.key())
            .bind(lease.lease_id())
            .bind(self.process_token())
            .bind(ttl)
            .fetch_optional(&mut *tx)
            .await
            .map_err(|e| Error::Internal(format!("acquire lease failed: {e}")))?;
        tx.commit().await.map_err(db)?;
        Ok(token.map(|t| u64::try_from(t).unwrap_or_default()))
    }

    pub async fn lease_renew(&self, lease: &Lease, ttl: Duration) -> Result<bool> {
        let ttl = ttl_millis(ttl)?;
        let mut tx: WriteTx = self.begin_write_as(Fence::LeaseOnly).await?;
        let kept = sqlx::query(RENEW_SQL)
            .bind(self.tenant())
            .bind(lease.scope())
            .bind(lease.key())
            .bind(lease.lease_id())
            .bind(lease.db_token())
            .bind(ttl)
            .execute(&mut *tx)
            .await
            .map_err(|e| Error::Internal(format!("renew lease failed: {e}")))?
            .rows_affected()
            > 0;
        tx.commit().await.map_err(db)?;
        Ok(kept)
    }

    pub async fn lease_release(&self, lease: &Lease) -> Result<()> {
        let mut tx: WriteTx = self.begin_write_as(Fence::LeaseOnly).await?;
        sqlx::query(RELEASE_SQL)
            .bind(self.tenant())
            .bind(lease.scope())
            .bind(lease.key())
            .bind(lease.lease_id())
            .bind(lease.db_token())
            .execute(&mut *tx)
            .await
            .map_err(|e| Error::Internal(format!("release lease failed: {e}")))?;
        tx.commit().await.map_err(db)
    }

    pub async fn lease_held(&self, lease: &Lease) -> Result<bool> {
        let mut tx = self.begin().await?;
        let held = sqlx::query_scalar::<_, bool>(HELD_SQL)
            .bind(self.tenant())
            .bind(lease.scope())
            .bind(lease.key())
            .bind(lease.lease_id())
            .bind(lease.db_token())
            .fetch_one(&mut *tx)
            .await
            .map_err(|e| Error::Internal(format!("read lease failed: {e}")))?;
        tx.commit().await.map_err(db)?;
        Ok(held)
    }

    /// Inside a write transaction: fails with `Error::LeaseLost` unless
    /// `lease` is still held (its row names it and has not expired by the
    /// database clock), and locks the row so nobody acquires the key before
    /// `tx` ends. The caller then rolls `tx` back by dropping it.
    pub(crate) async fn guard_lease(&self, tx: &mut WriteTx, lease: &Lease) -> Result<()> {
        let held = sqlx::query_scalar::<_, i32>(GUARD_SQL)
            .bind(self.tenant())
            .bind(lease.scope())
            .bind(lease.key())
            .bind(lease.lease_id())
            .bind(lease.db_token())
            .fetch_optional(&mut **tx)
            .await
            .map_err(|e| Error::Internal(format!("guard lease failed: {e}")))?;
        if held.is_some() {
            Ok(())
        } else {
            Err(lease.lost())
        }
    }

    /// Inside a write transaction: guards every lease of this tenant that
    /// the running work guards (`LeaseKeeper::guard_writes`), as
    /// [`Self::guard_lease`]. Called by `begin_write` under the writer lock.
    pub(crate) async fn guard_scoped_leases(&self, tx: &mut WriteTx) -> Result<()> {
        let guards = crate::LeaseWriteGuards::current();
        for lease in guards.for_tenant(self.tenant()) {
            self.guard_lease(tx, lease).await?;
        }
        Ok(())
    }

    /// Inside a write transaction that guarded `lease`: deletes its row,
    /// for work whose key is never used again (a delivered completion watch
    /// or delegation group, keyed by its UUID). The key's token then starts
    /// over, which no holder can tell: every holder is matched on its own
    /// lease id too. Commits with `tx`. Never the tenant lease: only the
    /// tenant claim and its release write that row (`lease.rs`).
    pub(crate) async fn retire_lease(&self, tx: &mut WriteTx, lease: &Lease) -> Result<()> {
        if lease.scope() == TENANT_SCOPE {
            return Err(Error::InvalidParams(
                "the tenant lease is never retired".to_owned(),
            ));
        }
        sqlx::query(RETIRE_SQL)
            .bind(self.tenant())
            .bind(lease.scope())
            .bind(lease.key())
            .bind(lease.lease_id())
            .bind(lease.db_token())
            .execute(&mut **tx)
            .await
            .map_err(|e| Error::Internal(format!("retire lease failed: {e}")))?;
        Ok(())
    }

    #[cfg(any(test, feature = "test-support"))]
    pub async fn write_guarded_by_lease_for_tests(&self, lease: &Lease) -> Result<()> {
        let mut tx = self.begin_write().await?;
        if let Err(e) = self.guard_lease(&mut tx, lease).await {
            let _ = tx.rollback().await;
            return Err(e);
        }
        tx.commit().await.map_err(db)
    }
}
