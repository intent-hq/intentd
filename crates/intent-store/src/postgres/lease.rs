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
//! - **Release** ([`Store::release_tenant_lease`](crate::Store::release_tenant_lease),
//!   [`LeaseHeartbeat::release`]): on a clean shutdown, once the claim's
//!   fence fired and its writes settled, one `UPDATE` of the lease row
//!   matched on owner and epoch sets `expires_at` to now, so the next claim
//!   (another process, or this one restarted) succeeds at once with the next
//!   epoch instead of waiting out the TTL. A release by a process that no
//!   longer owns the lease matches no row and changes nothing.
//!
//! A renewal is accepted while the owner and epoch still match, even past
//! `expires_at`: until another process claims, no one else has written, so
//! the fence still holds. A claim locks the lease row, so a renewal racing a
//! takeover either commits first (the claim then sees a live lease) or
//! waits and then matches no row.
//!
//! A store that never claimed writes only while no process ever claimed the
//! tenant. So once an orchestrator has claimed a tenant, an unclaimed tool
//! (admin, backfill, repair) cannot write to it again, even after the lease
//! expires: it must claim the tenant itself (after expiry) to write.
//!
//! **Self-fence** ([`TenantFence`]): each claim gets a fence that fires
//! when the heartbeat cannot renew by the lease's expiry minus a margin
//! ([`LEASE_FENCE_MARGIN`]), when the heartbeat is dropped, when any write
//! or renewal of the tenant in this process meets `NotOwner`, or when a new
//! claim replaces it. A fired fence refuses every later write and renewal
//! of the claim in this process (`NotOwner`) before it touches the
//! database, though the database may still name this process; the
//! process's consumers stop serving the tenant on it.
//!
//! `SQLite` has no lease: one daemon owns a database file (`intentd.lock`
//! and the database flock), so the methods refuse there.

use std::sync::{Arc, PoisonError, RwLock};
use std::time::{Duration, Instant};

use intent_core::{Error, Result};
use time::OffsetDateTime;
use tokio::sync::watch;

use super::{db, Fence, PgPool, PgStore, PostgresOptions, TenantId, WriteTx, WriterRow};
use crate::TenantFence;

/// How long a claim or renewal holds the tenant. A takeover waits this long
/// after the owner's last renewal. Far below Neki's router idle limits
/// (idle in transaction 5 min, idle session 30 min): renewals are single
/// statements.
pub const LEASE_TTL: Duration = Duration::from_secs(30);

/// How often [`LeaseHeartbeat`] renews: a third of [`LEASE_TTL`], so two
/// renewals can fail before the lease lapses.
pub const LEASE_RENEW_INTERVAL: Duration = Duration::from_secs(10);

/// How long before the lease's expiry [`LeaseHeartbeat`] fires the fence
/// when no renewal succeeded: a sixth of [`LEASE_TTL`]. The fence fires
/// while the lease is still live, so the process has stopped serving the
/// tenant before another process can take it over. [`LeaseHeartbeat::spawn`]
/// uses a sixth of its TTL.
pub const LEASE_FENCE_MARGIN: Duration = Duration::from_secs(5);

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
    /// When the lease lapses at the latest, on this host's monotonic clock:
    /// the start of the claim plus its TTL. The database set `expires_at`
    /// after that start, so this is never later, and it needs no agreement
    /// between the host's and the database's wall clocks.
    pub renew_by: Instant,
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
    /// Fires when this process must stop serving the tenant under this
    /// claim; see [`TenantFence`].
    fence: TenantFence,
}

impl ClaimSlot {
    fn get(&self) -> Option<Claim> {
        self.0
            .read()
            .unwrap_or_else(PoisonError::into_inner)
            .clone()
    }

    /// Record a new claim. The claim it replaces (if any) is fenced: its
    /// holders must not keep serving under the old epoch.
    fn set(&self, claim: Claim) {
        let epoch = claim.epoch;
        let old = self
            .0
            .write()
            .unwrap_or_else(PoisonError::into_inner)
            .replace(claim);
        if let Some(old) = old {
            old.fence.fire(format!(
                "this process claimed the tenant again at epoch {epoch}, replacing its claim \
                 at epoch {}",
                old.epoch
            ));
        }
    }

    /// The current claim's fence.
    pub(crate) fn fence(&self) -> Option<TenantFence> {
        self.get().map(|c| c.fence)
    }

    /// Refuse a write before it starts once the claim's fence fired.
    pub(crate) fn refuse_if_fenced(&self, tenant: &str) -> Result<()> {
        match self.get() {
            Some(claim) => refuse_fenced(tenant, &claim),
            None => Ok(()),
        }
    }

    /// Whether both handles are the same slot.
    #[cfg(feature = "test-support")]
    pub(crate) fn same_as(&self, other: &Self) -> bool {
        Arc::ptr_eq(&self.0, &other.0)
    }

    /// The fence: may a store of this process write, given the tenant's
    /// locked `tenant_writer` row?
    ///
    /// A fired fence refuses. A mismatch fires the claim's fence: another
    /// process owns the tenant now.
    pub(crate) fn check(&self, tenant: &str, row: &WriterRow) -> Result<()> {
        let claim = self.get();
        if let Some(claim) = &claim {
            refuse_fenced(tenant, claim)?;
        }
        let checked = fence(tenant, claim.as_ref(), row);
        if let (Err(e), Some(claim)) = (&checked, &claim) {
            claim.fence.fire(e.to_string());
        }
        checked
    }
}

fn refuse_fenced(tenant: &str, claim: &Claim) -> Result<()> {
    match claim.fence.reason() {
        None => Ok(()),
        Some(reason) => Err(Error::NotOwner(format!(
            "tenant {tenant}: this process fenced itself (instance {} at epoch {}) and no \
             longer serves it: {reason}; nothing was written",
            claim.instance, claim.epoch
        ))),
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

/// Ends the lease ($1 tenant) now while it is still ($2 instance, $3
/// epoch)'s; never extends it. No row once the lease moved on.
const LEASE_RELEASE_SQL: &str = "UPDATE tenant_lease \
     SET expires_at = LEAST(expires_at, clock_timestamp()) \
     WHERE tenant_id = $1 AND owner_instance = $2 AND epoch = $3";

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
        let started = Instant::now();
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
        // Record the claim before the writer slot is released: a writer of
        // this process queued behind the claim must see it, not compare the
        // new row with the old claim and fail `NotOwner`.
        let slot = tx.commit_holding_slot().await.map_err(db)?;
        self.claim.set(Claim {
            instance: instance.into(),
            epoch,
            fence: TenantFence::guarding_writes(self.writer_slot.permit.clone()),
        });
        drop(slot);
        Ok(TenantClaim::TenantOwned(TenantLease {
            instance: instance.to_owned(),
            epoch,
            expires_at: timestamp(expires_ms)?,
            renew_by: started + ttl,
        }))
    }

    /// See [`Store::renew_tenant_lease`](crate::Store::renew_tenant_lease).
    /// One statement on the lease row, outside the writer lock.
    /// With `epoch`, it renews only while the current claim is that epoch's
    /// (a heartbeat must not renew a newer claim).
    pub(crate) async fn renew_lease(
        &self,
        ttl: Duration,
        epoch: Option<i64>,
    ) -> Result<OffsetDateTime> {
        let ttl_ms = ttl_millis(ttl)?;
        let Some(claim) = self.claim.get() else {
            return Err(Error::NotOwner(format!(
                "tenant {}: this process never claimed it, so it has no lease to renew",
                self.tenant()
            )));
        };
        if let Some(epoch) = epoch.filter(|e| *e != claim.epoch) {
            return Err(stale_claim(self.tenant(), epoch, claim.epoch));
        }
        // A fenced claim stays fenced: its consumers are stopping.
        refuse_fenced(self.tenant(), &claim)?;
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
        let Some(ms) = renewed else {
            let e = Error::NotOwner(format!(
                "tenant {}: the lease of instance {} at epoch {} was taken over; \
                 nothing was renewed",
                self.tenant(),
                claim.instance,
                claim.epoch
            ));
            claim.fence.fire(e.to_string());
            return Err(e);
        };
        timestamp(ms)
    }

    /// See [`Store::release_tenant_lease`](crate::Store::release_tenant_lease).
    /// One statement on the lease row, outside the writer lock.
    pub(crate) async fn release_lease(&self, epoch: i64) -> Result<bool> {
        let Some(claim) = self.claim.get() else {
            return Err(Error::NotOwner(format!(
                "tenant {}: this process never claimed it, so it has no lease to release",
                self.tenant()
            )));
        };
        if claim.epoch != epoch {
            return Err(Error::NotOwner(format!(
                "tenant {}: the lease at epoch {epoch} was replaced by this process's claim at \
                 epoch {}; nothing was released",
                self.tenant(),
                claim.epoch
            )));
        }
        if !claim.fence.is_fenced() {
            return Err(Error::InvalidParams(format!(
                "tenant {}: a lease is released only after its fence fired and the tenant's \
                 work stopped; nothing was released",
                self.tenant()
            )));
        }
        // No write of this claim is still open, so none can land after the
        // next owner claims.
        claim.fence.settled().await;
        let mut tx = self.begin().await?;
        let released = sqlx::query(LEASE_RELEASE_SQL)
            .bind(self.tenant())
            .bind(&*claim.instance)
            .bind(claim.epoch)
            .execute(&mut *tx)
            .await
            .map_err(db)?
            .rows_affected();
        tx.commit().await.map_err(db)?;
        Ok(released > 0)
    }

    /// See [`Store::tenant_fence`](crate::Store::tenant_fence).
    pub(crate) fn tenant_fence(&self) -> Option<TenantFence> {
        self.claim.fence()
    }

    /// The fence of the current claim if it is `epoch`'s.
    fn fence_for_epoch(&self, epoch: i64) -> Option<TenantFence> {
        self.claim
            .get()
            .filter(|c| c.epoch == epoch)
            .map(|c| c.fence)
    }
}

fn stale_claim(tenant: &str, epoch: i64, current: i64) -> Error {
    Error::NotOwner(format!(
        "tenant {tenant}: the lease at epoch {epoch} was replaced by this process's claim at \
         epoch {current}; nothing was renewed"
    ))
}

impl crate::Store {
    /// Renew the claim at `epoch` only (see [`Self::renew_tenant_lease`]).
    async fn renew_tenant_lease_at(&self, ttl: Duration, epoch: i64) -> Result<OffsetDateTime> {
        match &self.backend {
            crate::Backend::Postgres(store) => store.renew_lease(ttl, Some(epoch)).await,
            crate::Backend::Sqlite(_) => Err(no_lease_on_sqlite()),
        }
    }

    /// The fence of this process's claim at `epoch`, if that is the current
    /// claim.
    fn tenant_fence_at(&self, epoch: i64) -> Option<TenantFence> {
        match &self.backend {
            crate::Backend::Postgres(store) => store.fence_for_epoch(epoch),
            crate::Backend::Sqlite(_) => None,
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
    /// The process's claim is recorded only once the commit is acknowledged.
    /// If the commit lands but its reply is lost, the database names this
    /// instance while the process holds no claim: its writes fail
    /// `NotOwner` (safe), and claiming again sees its own live lease
    /// (`AlreadyOwned`) until the TTL passes, so the tenant is unserved for
    /// up to one TTL.
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
    /// `Error::NotOwner` when this process never claimed the tenant, its
    /// claim's fence fired, or another process took it over (which fires
    /// the fence); `Error::InvalidParams` on `SQLite` or
    /// for a zero `ttl`; `Error::Internal` when the statement fails (the
    /// lease may still be held: retry before it expires).
    pub async fn renew_tenant_lease(&self, ttl: Duration) -> Result<OffsetDateTime> {
        match &self.backend {
            crate::Backend::Postgres(store) => store.renew_lease(ttl, None).await,
            crate::Backend::Sqlite(_) => Err(no_lease_on_sqlite()),
        }
    }

    /// Release this process's lease on the tenant at `epoch` now, for a
    /// clean shutdown: the next claim (another process, or this one after a
    /// restart) succeeds at once, with the next epoch, instead of waiting
    /// out the TTL. One statement on the lease row, matched on this claim's
    /// owner and epoch; it never extends the lease. Returns whether it
    /// released: `false` when the lease is no longer this claim's (taken
    /// over), which changes nothing.
    ///
    /// Only after the claim's fence fired, and once every write that passed
    /// the fence has ended ([`TenantFence::settled`]); the caller must also
    /// have stopped the tenant's other work. Never release a lease this
    /// process lost (its fence fired for another reason): its work may not
    /// have stopped in time.
    ///
    /// # Errors
    /// `Error::NotOwner` when this process never claimed the tenant or a
    /// newer claim replaced the one at `epoch`; `Error::InvalidParams` on
    /// `SQLite` or while the claim's fence has not fired; `Error::Internal`
    /// when the statement fails (the lease then lapses after its TTL).
    pub async fn release_tenant_lease(&self, epoch: i64) -> Result<bool> {
        match &self.backend {
            crate::Backend::Postgres(store) => store.release_lease(epoch).await,
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

/// Renews a claimed tenant's lease every interval until it is lost, and
/// fires the claim's [`TenantFence`] when it is.
///
/// The renew deadline is the lease's local deadline minus a margin: the
/// start of the last renewal that succeeded (or of the claim) plus the TTL,
/// which is never later than the database's expiry, minus the margin. A
/// renewal that fails `NotOwner` (taken over) loses the lease at once.
/// Other failures (connection, timeout, a renewal blocked on a lock) are
/// retried every interval until the renew deadline; no renewal may run past
/// it. At the deadline the lease is lost, while it is still live in the
/// database. Either way the fence fires, the state turns
/// [`LeaseState::Lost`] and [`Self::lost`] returns the error. A fence fired
/// elsewhere (a write met `NotOwner`, the caller fired it) stops the
/// heartbeat too. Dropping the heartbeat stops renewals and fires the fence:
/// nothing keeps the lease past its TTL any more. [`Self::stop`] stops
/// renewals without firing it, for a shutdown that stops writing on its own.
/// [`Self::release`] ends the lease at once after a clean stop.
///
/// The heartbeat belongs to its lease's epoch: it renews only while the
/// process's current claim is that epoch's, fires only that claim's fence,
/// and stops (`NotOwner`) once a newer claim replaces it.
pub struct LeaseHeartbeat {
    state: watch::Receiver<LeaseState>,
    fence: TenantFence,
    /// The renewed store and the lease's epoch, for [`Self::release`].
    store: crate::Store,
    epoch: i64,
    /// Fire the fence on drop (everything but [`Self::stop`]).
    fire_on_drop: bool,
    /// Kept for the heartbeat's whole life (never moved out), so dropping
    /// the heartbeat always aborts it, even after a cancelled `lost()`.
    task: tokio::task::JoinHandle<Error>,
    /// `lost()` already returned the task's result.
    reported: bool,
}

impl LeaseHeartbeat {
    /// Start renewing `store`'s lease, claimed as `lease`, for `ttl` every
    /// `interval` (see [`LEASE_TTL`], [`LEASE_RENEW_INTERVAL`]), with a
    /// fence margin of a sixth of `ttl` ([`LEASE_FENCE_MARGIN`] for
    /// [`LEASE_TTL`]). The first local deadline is the claim's
    /// [`TenantLease::renew_by`].
    #[must_use]
    pub fn spawn(
        store: crate::Store,
        lease: &TenantLease,
        ttl: Duration,
        interval: Duration,
    ) -> Self {
        Self::spawn_with_margin(store, lease, ttl, interval, ttl / 6)
    }

    /// [`Self::spawn`] with an explicit fence `margin`: the fence fires once
    /// no renewal succeeded by the lease's local deadline minus `margin`.
    /// The margin is capped at half the TTL.
    ///
    /// It fires the fence of `store`'s claim at `lease.epoch`
    /// ([`Store::tenant_fence`](crate::Store::tenant_fence)). When that is
    /// not the current claim (never claimed, or since replaced), the
    /// heartbeat gets a fence of its own and stops at once with `NotOwner`,
    /// leaving any newer claim alone.
    #[must_use]
    pub fn spawn_with_margin(
        store: crate::Store,
        lease: &TenantLease,
        ttl: Duration,
        interval: Duration,
        margin: Duration,
    ) -> Self {
        let (tx, rx) = watch::channel(LeaseState::Held {
            expires_at: lease.expires_at,
        });
        let fence = store.tenant_fence_at(lease.epoch).unwrap_or_default();
        let timing = Timing {
            ttl,
            interval,
            margin: margin.min(ttl / 2),
        };
        let task = tokio::spawn(heartbeat(
            store.clone(),
            timing,
            lease.epoch,
            lease.renew_by,
            fence.clone(),
            tx,
        ));
        Self {
            state: rx,
            fence,
            store,
            epoch: lease.epoch,
            fire_on_drop: true,
            task,
            reported: false,
        }
    }

    /// The lease as last seen; changes on every renewal and on loss.
    #[must_use]
    pub fn state(&self) -> watch::Receiver<LeaseState> {
        self.state.clone()
    }

    /// The fence this heartbeat fires when the lease is lost.
    #[must_use]
    pub fn fence(&self) -> TenantFence {
        self.fence.clone()
    }

    /// Wait until the lease is lost; the error says why (`NotOwner`). Later
    /// calls return an `Internal` error. Cancel-safe: dropping a pending
    /// call leaves the heartbeat running, and a later call waits again.
    pub async fn lost(&mut self) -> Error {
        if self.reported {
            return Error::Internal("lease heartbeat already reported its loss".to_owned());
        }
        let ended = (&mut self.task).await;
        self.reported = true;
        ended.unwrap_or_else(|e| Error::Internal(format!("lease heartbeat ended: {e}")))
    }
}

impl LeaseHeartbeat {
    /// Release the lease now, after a clean stop: see
    /// [`Store::release_tenant_lease`](crate::Store::release_tenant_lease).
    /// The fence must have fired (which also stops renewals) and the
    /// tenant's work stopped; call it only when this process fired the
    /// fence itself, never after the lease was lost.
    ///
    /// It first waits until the heartbeat stopped ([`LeaseState::Lost`],
    /// set only once its renewal in flight, if any, was dropped), so no
    /// renewal of this heartbeat starts after the release.
    ///
    /// # Errors
    /// As [`Store::release_tenant_lease`](crate::Store::release_tenant_lease).
    pub async fn release(&self) -> Result<bool> {
        if self.fence.is_fenced() {
            // A closed channel means the task already ended.
            let mut state = self.state.clone();
            let _ = state.wait_for(|s| matches!(s, LeaseState::Lost(_))).await;
        }
        self.store.release_tenant_lease(self.epoch).await
    }

    /// Stop renewing without firing the fence: for a shutdown that stops
    /// the tenant's work and writes on its own. The lease then lapses after
    /// its TTL ([`Self::release`] ends it at once, but only after the fence
    /// fired).
    pub fn stop(mut self) {
        self.fire_on_drop = false;
    }
}

impl Drop for LeaseHeartbeat {
    fn drop(&mut self) {
        self.task.abort();
        if self.fire_on_drop {
            self.fence
                .fire("the lease heartbeat stopped, so nothing renews the tenant lease");
        }
    }
}

#[derive(Debug, Clone, Copy)]
struct Timing {
    ttl: Duration,
    interval: Duration,
    margin: Duration,
}

impl Timing {
    /// The renew deadline for a lease whose local deadline is `deadline`.
    fn renew_deadline(self, deadline: Instant) -> Instant {
        deadline.checked_sub(self.margin).unwrap_or(deadline)
    }
}

async fn heartbeat(
    store: crate::Store,
    timing: Timing,
    epoch: i64,
    renew_by: Instant,
    fence: TenantFence,
    state: watch::Sender<LeaseState>,
) -> Error {
    let lose = |e: Error| {
        fence.fire(e.to_string());
        state.send_replace(LeaseState::Lost(e.to_string()));
        e
    };
    let fenced_elsewhere = |fence: &TenantFence| {
        Error::NotOwner(format!(
            "the tenant fence fired: {}",
            fence.reason().unwrap_or_default()
        ))
    };
    let mut deadline = timing.renew_deadline(renew_by);
    loop {
        // Renew every interval, but never sleep through the deadline: with
        // less than an interval left, try at half the time left, so a
        // renewal is always attempted (and retried) before the fence fires.
        let now = Instant::now();
        let wake = now
            + timing
                .interval
                .min(deadline.saturating_duration_since(now) / 2);
        tokio::select! {
            () = tokio::time::sleep_until(wake.into()) => {}
            () = fence.fenced() => return lose(fenced_elsewhere(&fence)),
        }
        let started = Instant::now();
        if lapsed(deadline, started) {
            return lose(lapse(timing, None));
        }
        let left = deadline.saturating_duration_since(started);
        let renewed = tokio::select! {
            renewed = tokio::time::timeout(left, store.renew_tenant_lease_at(timing.ttl, epoch)) => {
                renewed.unwrap_or_else(|_| Err(Error::Internal(format!(
                    "lease renewal took longer than the {} ms left before the renew deadline",
                    left.as_millis()
                ))))
            }
            () = fence.fenced() => return lose(fenced_elsewhere(&fence)),
        };
        match renewed {
            Ok(expires_at) => {
                deadline = timing.renew_deadline(started + timing.ttl);
                state.send_replace(LeaseState::Held { expires_at });
            }
            Err(e @ Error::NotOwner(_)) => return lose(e),
            Err(e) if lapsed(deadline, Instant::now()) => return lose(lapse(timing, Some(&e))),
            Err(e) => tracing::warn!(error = %e, "tenant lease renewal failed; retrying"),
        }
    }
}

fn lapse(timing: Timing, last: Option<&Error>) -> Error {
    let last = last.map_or_else(String::new, |e| format!("; last error: {e}"));
    Error::NotOwner(format!(
        "the tenant lease lapsed: no renewal succeeded by the renew deadline ({} ms before \
         the lease expires){last}",
        timing.margin.as_millis()
    ))
}

/// Whether the renew deadline has passed.
fn lapsed(deadline: Instant, now: Instant) -> bool {
    now >= deadline
}

#[cfg(test)]
mod tests {
    use super::{fence, lapsed, ttl_millis, Claim, ClaimSlot, Timing, WriterRow};
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
            fence: crate::TenantFence::new(),
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
    fn a_mismatch_fires_the_claims_fence_and_a_fired_fence_refuses() {
        let slot = ClaimSlot::default();
        slot.set(claim("a", 3));
        let fence = slot.fence().expect("claimed");
        assert!(slot.check("t", &row(Some("a"), 3)).is_ok());
        assert!(!fence.is_fenced());
        let e = slot.check("t", &row(Some("b"), 4)).expect_err("taken over");
        assert!(matches!(e, Error::NotOwner(_)), "{e:?}");
        assert!(fence.is_fenced(), "a NotOwner write fires the fence");
        // The database row may still name this claim: the fence refuses.
        let e = slot.check("t", &row(Some("a"), 3)).expect_err("fenced");
        assert!(e.to_string().contains("fenced itself"), "{e}");
        assert!(slot.refuse_if_fenced("t").is_err());
    }

    #[test]
    fn a_new_claim_fences_the_one_it_replaces() {
        let slot = ClaimSlot::default();
        assert!(slot.fence().is_none());
        assert!(
            slot.refuse_if_fenced("t").is_ok(),
            "an unclaimed store is not fenced"
        );
        slot.set(claim("a", 1));
        let first = slot.fence().unwrap();
        slot.set(claim("a", 2));
        assert!(first.is_fenced());
        assert!(!slot.fence().unwrap().is_fenced());
        assert!(slot.check("t", &row(Some("a"), 2)).is_ok());
    }

    #[test]
    fn the_renew_deadline_is_the_local_deadline_minus_the_margin() {
        let timing = Timing {
            ttl: Duration::from_secs(30),
            interval: Duration::from_secs(10),
            margin: Duration::from_secs(5),
        };
        let now = Instant::now();
        let deadline = now + Duration::from_secs(30);
        assert_eq!(
            timing.renew_deadline(deadline),
            now + Duration::from_secs(25)
        );
    }

    #[test]
    fn the_lease_lapses_at_its_local_deadline() {
        let now = Instant::now();
        assert!(!lapsed(now + Duration::from_millis(1), now));
        assert!(lapsed(now, now));
    }
}
