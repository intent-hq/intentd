//! The fenced tenant lease (orchestrator mode): one process owns a tenant
//! at a time.
//!
//! - **Claim** ([`Store::claim_tenant`](crate::Store::claim_tenant)): under
//!   the tenant's writer lock, then its `tenant_lease` row lock, a process
//!   takes a tenant that has no lease or whose lease expired by the database
//!   clock. It sets itself as owner and bumps the tenant's epoch on both
//!   `tenant_lease` and `tenant_writer`, in one single-shard transaction on
//!   the tenant's rows (no advisory locks: Neki limits them). A live lease
//!   is [`TenantClaim::AlreadyOwned`] and changes no row.
//! - **Fence**: every write ([`PgStore::begin_write`]) compares the locked
//!   `tenant_writer` row with this process's [`ClaimSlot`]; a mismatch is
//!   `Error::NotOwner` and the transaction rolls back.
//! - **Heartbeat** ([`Store::renew_tenant_lease`](crate::Store::renew_tenant_lease),
//!   [`LeaseHeartbeat`]): a plain `UPDATE` of the lease row, outside the
//!   writer lock so a busy tenant cannot starve it, matched on owner and
//!   epoch. A renewal after a takeover matches no row: `NotOwner`.
//!
//! A renewal is accepted while the owner and epoch still match, even past
//! `expires_at`: until another process claims, no one else has written, so
//! the fence still holds. A claim locks the lease row, so a renewal racing a
//! takeover either commits first (the claim then sees a live lease) or
//! waits and then matches no row.
//!
//! Self-fencing (stopping the tenant's work when renewals fail) is the
//! caller's: the heartbeat only reports the loss.
//!
//! `SQLite` has no lease: one daemon owns a database file (`intentd.lock`
//! and the database flock), so the methods refuse there.

use std::sync::{Arc, PoisonError, RwLock};
use std::time::{Duration, Instant};

use intent_core::{Error, Result};
use time::OffsetDateTime;
use tokio::sync::watch;

use super::{db, Fence, PgPool, PgStore, PostgresOptions, TenantId, WriteTx, WriterRow};

/// How long a claim or renewal holds the tenant. A takeover waits this long
/// after the owner's last renewal. Far below Neki's router idle limits
/// (idle in transaction 5 min, idle session 30 min): renewals are single
/// statements.
pub const LEASE_TTL: Duration = Duration::from_secs(30);

/// How often [`LeaseHeartbeat`] renews: a third of [`LEASE_TTL`], so two
/// renewals can fail before the lease lapses.
pub const LEASE_RENEW_INTERVAL: Duration = Duration::from_secs(10);

/// Longest accepted instance id, in bytes.
const MAX_INSTANCE_LEN: usize = 256;

/// The tenant's lease, as this process holds it after a claim.
#[derive(Debug, Clone, PartialEq, Eq)]
pub struct TenantLease {
    /// The instance id the process claimed as.
    pub instance: String,
    /// The tenant's epoch under this claim (1 for the first claim ever).
    pub epoch: i64,
    /// When the lease lapses unless renewed, by the database clock.
    pub expires_at: OffsetDateTime,
}

/// What [`Store::claim_tenant`](crate::Store::claim_tenant) found.
#[derive(Debug, Clone, PartialEq, Eq)]
pub enum TenantClaim {
    /// This process owns the tenant now, under a new epoch.
    TenantOwned(TenantLease),
    /// Another claim holds a live lease; nothing changed.
    AlreadyOwned {
        owner: String,
        expires_at: OffsetDateTime,
    },
}

/// This process's claim on one tenant, shared by every store of the tenant
/// in the process (`tenant_registry`). `None` until a claim succeeds.
#[derive(Clone, Default)]
pub(crate) struct ClaimSlot(Arc<RwLock<Option<Claim>>>);

#[derive(Debug, Clone, PartialEq, Eq)]
struct Claim {
    instance: Arc<str>,
    epoch: i64,
}

impl ClaimSlot {
    fn get(&self) -> Option<Claim> {
        self.0
            .read()
            .unwrap_or_else(PoisonError::into_inner)
            .clone()
    }

    fn set(&self, claim: Claim) {
        *self.0.write().unwrap_or_else(PoisonError::into_inner) = Some(claim);
    }

    /// Whether both handles are the same slot.
    #[cfg(feature = "test-support")]
    pub(crate) fn same_as(&self, other: &Self) -> bool {
        Arc::ptr_eq(&self.0, &other.0)
    }

    /// The fence: may a store of this process write, given the tenant's
    /// locked `tenant_writer` row?
    pub(crate) fn check(&self, tenant: &str, row: &WriterRow) -> Result<()> {
        fence(tenant, self.get().as_ref(), row)
    }
}

fn fence(tenant: &str, claim: Option<&Claim>, row: &WriterRow) -> Result<()> {
    match (claim, row.owner_instance.as_deref()) {
        (None, None) => Ok(()),
        (Some(c), Some(owner)) if *c.instance == *owner && c.epoch == row.epoch => Ok(()),
        (None, Some(owner)) => Err(Error::NotOwner(format!(
            "tenant {tenant} is owned by instance {owner} (epoch {}), and this process never \
             claimed it; nothing was written",
            row.epoch
        ))),
        (Some(c), owner) => Err(Error::NotOwner(format!(
            "tenant {tenant} was claimed by this process as instance {} at epoch {}, but \
             instance {} owns it now (epoch {}); nothing was written",
            c.instance,
            c.epoch,
            owner.unwrap_or("<none>"),
            row.epoch
        ))),
    }
}

/// Locks the tenant's lease row and reads it: owner, epoch, expiry (epoch
/// milliseconds) and whether it has expired by the database clock.
const LEASE_LOCK_SQL: &str = "SELECT owner_instance, epoch, \
     (extract(epoch FROM expires_at) * 1000)::bigint, expires_at <= clock_timestamp() \
     FROM tenant_lease WHERE tenant_id = $1 FOR UPDATE";

/// Takes the lease ($1 tenant, $2 instance, $3 epoch, $4 TTL in ms).
const LEASE_TAKE_SQL: &str = "INSERT INTO tenant_lease \
     (tenant_id, owner_instance, epoch, expires_at, renewed_at) \
     VALUES ($1, $2, $3, clock_timestamp() + $4::bigint * interval '1 millisecond', \
     clock_timestamp()) \
     ON CONFLICT (tenant_id) DO UPDATE SET owner_instance = EXCLUDED.owner_instance, \
     epoch = EXCLUDED.epoch, expires_at = EXCLUDED.expires_at, renewed_at = EXCLUDED.renewed_at \
     RETURNING (extract(epoch FROM expires_at) * 1000)::bigint";

/// Moves the fence to the new owner ($2) and epoch ($3).
const FENCE_MOVE_SQL: &str =
    "UPDATE tenant_writer SET owner_instance = $2, epoch = $3 WHERE tenant_id = $1";

/// Renews the lease ($1 tenant) while it is still ($2 instance, $3 epoch)'s,
/// for $4 ms; returns the new expiry, or no row once the lease moved on.
const LEASE_RENEW_SQL: &str = "UPDATE tenant_lease \
     SET expires_at = clock_timestamp() + $4::bigint * interval '1 millisecond', \
     renewed_at = clock_timestamp() \
     WHERE tenant_id = $1 AND owner_instance = $2 AND epoch = $3 \
     RETURNING (extract(epoch FROM expires_at) * 1000)::bigint";

fn ttl_millis(ttl: Duration) -> Result<i64> {
    let ms = i64::try_from(ttl.as_millis()).unwrap_or(i64::MAX);
    if ms < 1 {
        return Err(Error::InvalidParams(
            "a tenant lease TTL must be at least 1 ms".to_owned(),
        ));
    }
    Ok(ms)
}

fn timestamp(ms: i64) -> Result<OffsetDateTime> {
    OffsetDateTime::from_unix_timestamp_nanos(i128::from(ms) * 1_000_000)
        .map_err(|e| Error::Internal(format!("postgres: lease expiry out of range: {e}")))
}

fn check_instance(instance: &str) -> Result<()> {
    if instance.is_empty() || instance.len() > MAX_INSTANCE_LEN {
        return Err(Error::InvalidParams(format!(
            "a tenant lease instance id needs 1-{MAX_INSTANCE_LEN} bytes, got {}",
            instance.len()
        )));
    }
    Ok(())
}

fn no_lease_on_sqlite() -> Error {
    Error::InvalidParams(
        "tenant leases exist only on the Postgres store; a SQLite database has one daemon"
            .to_owned(),
    )
}

impl PgStore {
    /// See [`Store::claim_tenant`](crate::Store::claim_tenant).
    pub(crate) async fn claim_lease(&self, instance: &str, ttl: Duration) -> Result<TenantClaim> {
        check_instance(instance)?;
        let ttl_ms = ttl_millis(ttl)?;
        // The writer lock without the fence check: a claim is how the fence
        // moves. It orders this claim against every writer and claimant.
        let mut tx: WriteTx = self.begin_write_as(Fence::Skip).await?;
        let current: Option<(String, i64, i64, bool)> = sqlx::query_as(LEASE_LOCK_SQL)
            .bind(self.tenant())
            .fetch_optional(&mut *tx)
            .await
            .map_err(db)?;
        let mut last_epoch = tx.writer().epoch;
        if let Some((owner, epoch, expires_ms, expired)) = current {
            if !expired {
                tx.rollback().await.map_err(db)?;
                return Ok(TenantClaim::AlreadyOwned {
                    owner,
                    expires_at: timestamp(expires_ms)?,
                });
            }
            last_epoch = last_epoch.max(epoch);
        }
        let epoch = last_epoch + 1;
        let expires_ms: i64 = sqlx::query_scalar(LEASE_TAKE_SQL)
            .bind(self.tenant())
            .bind(instance)
            .bind(epoch)
            .bind(ttl_ms)
            .fetch_one(&mut *tx)
            .await
            .map_err(db)?;
        sqlx::query(FENCE_MOVE_SQL)
            .bind(self.tenant())
            .bind(instance)
            .bind(epoch)
            .execute(&mut *tx)
            .await
            .map_err(db)?;
        tx.commit().await.map_err(db)?;
        self.claim.set(Claim {
            instance: instance.into(),
            epoch,
        });
        Ok(TenantClaim::TenantOwned(TenantLease {
            instance: instance.to_owned(),
            epoch,
            expires_at: timestamp(expires_ms)?,
        }))
    }

    /// See [`Store::renew_tenant_lease`](crate::Store::renew_tenant_lease).
    /// One statement on the lease row, outside the writer lock.
    pub(crate) async fn renew_lease(&self, ttl: Duration) -> Result<OffsetDateTime> {
        let ttl_ms = ttl_millis(ttl)?;
        let Some(claim) = self.claim.get() else {
            return Err(Error::NotOwner(format!(
                "tenant {}: this process never claimed it, so it has no lease to renew",
                self.tenant()
            )));
        };
        let mut tx = self.begin().await?;
        let renewed: Option<i64> = sqlx::query_scalar(LEASE_RENEW_SQL)
            .bind(self.tenant())
            .bind(&*claim.instance)
            .bind(claim.epoch)
            .bind(ttl_ms)
            .fetch_optional(&mut *tx)
            .await
            .map_err(db)?;
        tx.commit().await.map_err(db)?;
        match renewed {
            Some(ms) => timestamp(ms),
            None => Err(Error::NotOwner(format!(
                "tenant {}: the lease of instance {} at epoch {} was taken over; \
                 nothing was renewed",
                self.tenant(),
                claim.instance,
                claim.epoch
            ))),
        }
    }
}

impl crate::Store {
    /// Claim this store's Postgres tenant for this process as `instance`,
    /// for `ttl`. It succeeds when the tenant has no lease or its lease has
    /// expired by the database clock, and bumps the tenant's epoch; every
    /// store of the tenant in this process then writes under the new claim,
    /// and any other process's writes fail `Error::NotOwner`. A live lease
    /// (also this process's own) is [`TenantClaim::AlreadyOwned`] and
    /// changes nothing. See the `lease` module docs.
    ///
    /// # Errors
    /// `Error::InvalidParams` on `SQLite` (no lease there), for an empty or
    /// too long `instance` or a zero `ttl`; `Error::StoreBusy` when the
    /// writer lock stays held past the wait budget; `Error::Internal` when a
    /// statement fails.
    pub async fn claim_tenant(&self, instance: &str, ttl: Duration) -> Result<TenantClaim> {
        match &self.backend {
            crate::Backend::Postgres(store) => store.claim_lease(instance, ttl).await,
            crate::Backend::Sqlite(_) => Err(no_lease_on_sqlite()),
        }
    }

    /// Renew this process's lease on the tenant for `ttl` from now (database
    /// clock); returns the new expiry.
    ///
    /// # Errors
    /// `Error::NotOwner` when this process never claimed the tenant or
    /// another process took it over; `Error::InvalidParams` on `SQLite` or
    /// for a zero `ttl`; `Error::Internal` when the statement fails (the
    /// lease may still be held: retry before it expires).
    pub async fn renew_tenant_lease(&self, ttl: Duration) -> Result<OffsetDateTime> {
        match &self.backend {
            crate::Backend::Postgres(store) => store.renew_lease(ttl).await,
            crate::Backend::Sqlite(_) => Err(no_lease_on_sqlite()),
        }
    }

    /// The orchestrator's startup open: [`Self::open_postgres`] that claims
    /// the tenant for this process as `instance` (for `ttl`) after the
    /// schema gate and before anything else writes (provisioning, then every
    /// startup sweep the caller runs on the returned store).
    ///
    /// # Errors
    /// `Error::NotOwner` naming the owner and its lease expiry when another
    /// claim holds a live lease (nothing was written); otherwise as
    /// [`Self::open_postgres`] and [`Self::claim_tenant`].
    pub async fn open_postgres_owned(
        pool: PgPool,
        tenant: TenantId,
        options: PostgresOptions,
        instance: &str,
        ttl: Duration,
        _orchestrator: crate::OrchestratorOnly,
    ) -> Result<(Self, TenantLease)> {
        let key = super::tenant_registry::tenant_key(&pool, &tenant);
        Self::open_owned_keyed(pool, tenant, options, key, instance, ttl).await
    }

    /// Test-only: [`Self::open_postgres_owned`] as if from another process
    /// named `process` (see [`Self::open_postgres_as_process`]).
    ///
    /// # Errors
    /// As [`Self::open_postgres_owned`].
    #[cfg(feature = "test-support")]
    #[doc(hidden)]
    pub async fn open_postgres_owned_as_process(
        pool: PgPool,
        tenant: TenantId,
        options: PostgresOptions,
        process: &str,
        instance: &str,
        ttl: Duration,
        _orchestrator: crate::OrchestratorOnly,
    ) -> Result<(Self, TenantLease)> {
        let key = format!(
            "{}@process:{process}",
            super::tenant_registry::tenant_key(&pool, &tenant)
        );
        Self::open_owned_keyed(pool, tenant, options, key, instance, ttl).await
    }

    async fn open_owned_keyed(
        pool: PgPool,
        tenant: TenantId,
        options: PostgresOptions,
        key: String,
        instance: &str,
        ttl: Duration,
    ) -> Result<(Self, TenantLease)> {
        let store = PgStore::for_key(pool, tenant, options, key);
        super::check_schema(&store.pool).await?;
        let lease = match store.claim_lease(instance, ttl).await? {
            TenantClaim::TenantOwned(lease) => lease,
            TenantClaim::AlreadyOwned { owner, expires_at } => {
                return Err(Error::NotOwner(format!(
                    "tenant {} is owned by instance {owner} until {expires_at} (database \
                     clock); this process can claim it only after that lease expires",
                    store.tenant()
                )));
            }
        };
        store.provision().await?;
        Ok((
            Self {
                backend: crate::Backend::Postgres(store),
            },
            lease,
        ))
    }
}

/// The lease as [`LeaseHeartbeat`] last saw it.
#[derive(Debug, Clone, PartialEq, Eq)]
pub enum LeaseState {
    /// Renewed (or claimed) until `expires_at`, by the database clock.
    Held { expires_at: OffsetDateTime },
    /// The lease is gone; the heartbeat stopped. The text says why.
    Lost(String),
}

/// Renews a claimed tenant's lease every interval until it is lost.
///
/// A renewal that fails `NotOwner` (taken over) ends it at once. Other
/// failures (connection, timeout) are retried at the next tick until the
/// lease's local deadline passes: the start of the last renewal that
/// succeeded plus the TTL, which is never later than the database's expiry.
/// Either way the state turns [`LeaseState::Lost`] and [`Self::lost`]
/// returns the error. Stopping the tenant's work then is the caller's
/// (self-fence). Dropping the heartbeat stops it.
pub struct LeaseHeartbeat {
    state: watch::Receiver<LeaseState>,
    task: Option<tokio::task::JoinHandle<Error>>,
}

impl LeaseHeartbeat {
    /// Start renewing `store`'s lease, claimed as `lease`, for `ttl` every
    /// `interval` (see [`LEASE_TTL`], [`LEASE_RENEW_INTERVAL`]).
    #[must_use]
    pub fn spawn(
        store: crate::Store,
        lease: &TenantLease,
        ttl: Duration,
        interval: Duration,
    ) -> Self {
        let left = lease.expires_at - OffsetDateTime::now_utc();
        let left = Duration::try_from(left).unwrap_or(Duration::ZERO).min(ttl);
        let deadline = Instant::now() + left;
        let (tx, rx) = watch::channel(LeaseState::Held {
            expires_at: lease.expires_at,
        });
        let task = tokio::spawn(heartbeat(store, ttl, interval, deadline, tx));
        Self {
            state: rx,
            task: Some(task),
        }
    }

    /// The lease as last seen; changes on every renewal and on loss.
    #[must_use]
    pub fn state(&self) -> watch::Receiver<LeaseState> {
        self.state.clone()
    }

    /// Wait until the lease is lost; the error says why (`NotOwner`). Later
    /// calls return an `Internal` error.
    pub async fn lost(&mut self) -> Error {
        match self.task.take() {
            Some(task) => task
                .await
                .unwrap_or_else(|e| Error::Internal(format!("lease heartbeat ended: {e}"))),
            None => Error::Internal("lease heartbeat already reported its loss".to_owned()),
        }
    }
}

impl Drop for LeaseHeartbeat {
    fn drop(&mut self) {
        if let Some(task) = &self.task {
            task.abort();
        }
    }
}

async fn heartbeat(
    store: crate::Store,
    ttl: Duration,
    interval: Duration,
    mut deadline: Instant,
    state: watch::Sender<LeaseState>,
) -> Error {
    let lose = |e: Error| {
        state.send_replace(LeaseState::Lost(e.to_string()));
        e
    };
    loop {
        tokio::time::sleep(interval).await;
        let started = Instant::now();
        let left = deadline.saturating_duration_since(started);
        let renewed = match tokio::time::timeout(left, store.renew_tenant_lease(ttl)).await {
            Ok(renewed) => renewed,
            Err(_) => Err(Error::Internal(format!(
                "lease renewal took longer than the {} ms the lease had left",
                left.as_millis()
            ))),
        };
        match renewed {
            Ok(expires_at) => {
                deadline = started + ttl;
                state.send_replace(LeaseState::Held { expires_at });
            }
            Err(e @ Error::NotOwner(_)) => return lose(e),
            Err(e) if lapsed(deadline, Instant::now()) => {
                return lose(Error::NotOwner(format!(
                    "the tenant lease lapsed: no renewal succeeded before it expired; last \
                     error: {e}"
                )));
            }
            Err(e) => tracing::warn!(error = %e, "tenant lease renewal failed; retrying"),
        }
    }
}

/// Whether the lease's local deadline has passed.
fn lapsed(deadline: Instant, now: Instant) -> bool {
    now >= deadline
}

#[cfg(test)]
mod tests {
    use super::{fence, lapsed, ttl_millis, Claim, WriterRow};
    use intent_core::Error;
    use std::time::{Duration, Instant};

    fn row(owner: Option<&str>, epoch: i64) -> WriterRow {
        WriterRow {
            owner_instance: owner.map(str::to_owned),
            epoch,
        }
    }

    fn claim(instance: &str, epoch: i64) -> Claim {
        Claim {
            instance: instance.into(),
            epoch,
        }
    }

    #[test]
    fn the_fence_admits_only_the_current_claim_or_an_unclaimed_tenant() {
        assert!(fence("t", None, &row(None, 0)).is_ok());
        assert!(fence("t", Some(&claim("a", 3)), &row(Some("a"), 3)).is_ok());
        for (c, r) in [
            (None, row(Some("a"), 1)),
            (Some(claim("a", 1)), row(Some("a"), 2)),
            (Some(claim("a", 2)), row(Some("b"), 2)),
            (Some(claim("a", 1)), row(None, 0)),
        ] {
            let e = fence("t", c.as_ref(), &r).expect_err("fenced");
            assert!(matches!(e, Error::NotOwner(_)), "{e:?}");
            assert!(e.to_string().contains("nothing was written"), "{e}");
        }
    }

    #[test]
    fn a_ttl_must_be_a_whole_millisecond() {
        assert!(ttl_millis(Duration::ZERO).is_err());
        assert!(ttl_millis(Duration::from_micros(999)).is_err());
        assert_eq!(ttl_millis(Duration::from_secs(30)).unwrap(), 30_000);
    }

    #[test]
    fn the_lease_lapses_at_its_local_deadline() {
        let now = Instant::now();
        assert!(!lapsed(now + Duration::from_millis(1), now));
        assert!(lapsed(now, now));
    }
}
