//! The tenant lease (orchestrator mode): one process owns a tenant at a
//! time. It is the store lease of scope `tenant` (`lease_repo.rs`), key
//! `""`: its token is the tenant's epoch and its holder the claiming
//! instance. This module is the tenant's adapter over that lease:
//!
//! - **Claim** ([`Store::claim_tenant`](crate::Store::claim_tenant)): under
//!   the tenant writer lock without the fence check (`Fence::Skip`: a claim
//!   is how the fence moves), acquire the tenant lease by the lease rules
//!   (free, released, or expired by the database clock; token + 1), then
//!   record the claim in this process's [`ClaimSlot`] before the writer slot
//!   is released. A live lease is [`TenantClaim::AlreadyOwned`] and changes
//!   no row. The generic API refuses the scope, so only the claim takes it.
//! - **Fence**: every write ([`PgStore::begin_write`]) reads the tenant
//!   lease right after it took the writer lock and compares its lease id and
//!   token with the [`ClaimSlot`]; a mismatch is `Error::NotOwner` and the
//!   transaction rolls back.
//! - **Renewal** ([`Store::renew_tenant_lease`](crate::Store::renew_tenant_lease),
//!   [`TenantLeaseKeeper`]) and **release**
//!   ([`Store::release_tenant_lease`](crate::Store::release_tenant_lease)):
//!   one statement on the lease row, matched on the claim's lease id and
//!   token, outside the writer lock so a busy tenant cannot starve them. A
//!   renewal is accepted while the row names the claim, even past its
//!   expiry (until another process claims, no one else has written). A
//!   release, only after the claim's fence fired and its writes settled,
//!   frees the row (keeping its token) without extending it, so the next
//!   claim succeeds at once.
//!
//! Leases of other scopes never read the tenant lease, so a tenant takeover
//! neither voids nor fences them.
//!
//! A store that never claimed writes only while no process ever claimed the
//! tenant (no tenant lease row). So once an orchestrator has claimed a
//! tenant, an unclaimed tool (admin, backfill, repair) cannot write to it
//! again, even after the lease expires: it must claim the tenant itself.
//!
//! **Self-fence** ([`TenantFence`]): each claim gets a fence that fires
//! when its [`TenantLeaseKeeper`] cannot renew by the lease's expiry minus a
//! margin ([`LEASE_FENCE_MARGIN`]), when the keeper is dropped, when any
//! write or renewal of the tenant in this process meets `NotOwner`, or when
//! a new claim replaces it. A fired fence refuses every later write and
//! renewal of the claim in this process (`NotOwner`) before it touches the
//! database; the process's consumers stop serving the tenant on it.
//!
//! **State** ([`Store::move_tenant_state`](crate::Store::move_tenant_state),
//! `tenant_state`): the `tenant_writer` row also says whether the tenant
//! takes writes at all (`open`, `closing`, `closed`). Every write reads it
//! with the lease, under the writer lock; a move takes the same lock without
//! the fence or state check (`Fence::Skip`) and, from a process that
//! claimed the tenant, only while the lease still names that claim. A claim
//! by the startup open moves the tenant back to `open`.
//!
//! `SQLite` has no tenant lease: one daemon owns a database file
//! (`intentd.lock` and the database flock), so the methods refuse there.

use std::sync::{Arc, PoisonError, RwLock};
use std::time::Duration;

use intent_core::{Error, Result};
use time::OffsetDateTime;
use tokio::time::Instant;

use super::{db, Fence, PgPool, PgStore, PostgresOptions, TenantId, WriteTx, WriterRow};
use crate::lease_keeper::{LeaseFence, LeaseKeeper, Renew, Renewal};
use crate::lease_repo::{ttl_millis, TENANT_SCOPE};
use crate::{Lease, TenantFence, TenantState, TenantStateMove};

/// How long a claim or renewal holds the tenant. A takeover waits this long
/// after the owner's last renewal. Far below Neki's router idle limits
/// (idle in transaction 5 min, idle session 30 min): renewals are single
/// statements. The keeper renews at half of it.
pub const LEASE_TTL: Duration = Duration::from_secs(30);

/// How long before the lease's expiry [`TenantLeaseKeeper`] fires the fence
/// when no renewal succeeded: a sixth of [`LEASE_TTL`]. The fence fires
/// while the lease is still live, so the process has stopped serving the
/// tenant before another process can take it over.
/// [`TenantLeaseKeeper::spawn`] uses a sixth of the claim's TTL.
pub const LEASE_FENCE_MARGIN: Duration = Duration::from_secs(5);

/// Longest accepted instance id, in bytes.
const MAX_INSTANCE_LEN: usize = 256;

/// The tenant's lease, as this process holds it after a claim.
#[derive(Debug)]
pub struct TenantLease {
    /// The instance id the process claimed as.
    pub instance: String,
    /// The tenant's epoch under this claim: the lease token (1 for the
    /// first claim ever).
    pub epoch: i64,
    /// When the lease lapses unless renewed, by the database clock.
    pub expires_at: OffsetDateTime,
    /// The held lease; its local deadline starts before the claim, so it is
    /// never later than `expires_at`.
    lease: Lease,
}

impl TenantLease {
    /// Time left before the lease may lapse, by this process's clock.
    #[must_use]
    pub fn remaining(&self) -> Duration {
        self.lease.remaining()
    }
}

/// What [`Store::claim_tenant`](crate::Store::claim_tenant) found.
#[derive(Debug)]
pub enum TenantClaim {
    /// This process owns the tenant now, under a new epoch.
    TenantOwned(TenantLease),
    /// Another claim holds a live lease; nothing changed.
    AlreadyOwned {
        owner: String,
        expires_at: OffsetDateTime,
    },
}

/// Another instance holds a live lease on a tenant, so a startup open
/// ([`Store::try_open_postgres_owned`](crate::Store::try_open_postgres_owned))
/// claimed and wrote nothing.
#[derive(Debug, Clone, PartialEq, Eq)]
pub struct LeaseHeld {
    /// The tenant id.
    pub tenant: String,
    /// The instance that holds the lease.
    pub owner: String,
    /// When the lease lapses unless renewed, by the database clock.
    pub expires_at: OffsetDateTime,
}

impl LeaseHeld {
    /// The `Error::NotOwner` [`Store::open_postgres_owned`](crate::Store::open_postgres_owned)
    /// answers for it, naming the owner and the expiry.
    #[must_use]
    pub fn into_error(self) -> Error {
        Error::NotOwner(format!(
            "tenant {} is owned by instance {} until {} (database clock); this process can \
             claim it only after that lease expires",
            self.tenant, self.owner, self.expires_at
        ))
    }
}

/// What a startup open found: the tenant opened and claimed, or
/// [`LeaseHeld`] (nothing was claimed or written).
pub type OwnedOpen = std::result::Result<(crate::Store, TenantLease), LeaseHeld>;

/// This process's claim on one tenant, shared by every store of the tenant
/// in the process (`tenant_registry`). `None` until a claim succeeds.
#[derive(Clone, Default)]
pub(crate) struct ClaimSlot(Arc<RwLock<Option<Claim>>>);

#[derive(Debug, Clone, PartialEq, Eq)]
struct Claim {
    instance: Arc<str>,
    /// The tenant lease this claim acquired.
    lease_id: Arc<str>,
    /// Its token: the tenant's epoch.
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

    /// For a tenant state move: no claim passes (a tool that never claimed
    /// moves the state whoever owns the tenant); a claim passes while the
    /// tenant lease, read under the writer lock, still names it, even after
    /// its fence fired for a clean stop. A mismatch fires the claim's fence.
    pub(crate) fn check_current(&self, tenant: &str, row: &WriterRow) -> Result<()> {
        let Some(claim) = self.get() else {
            return Ok(());
        };
        let checked = fence(tenant, Some(&claim), row);
        if let Err(e) = &checked {
            claim.fence.fire_lost(e.to_string());
        }
        checked
    }

    /// The fence: may a store of this process write, given the tenant lease
    /// it read under the writer lock?
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
            claim.fence.fire_lost(e.to_string());
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
    let owner = row.owner_instance.as_deref().unwrap_or("<none>");
    match claim {
        None if row.epoch == 0 => Ok(()),
        Some(c) if row.lease_id.as_deref() == Some(&*c.lease_id) && c.epoch == row.epoch => Ok(()),
        None => Err(Error::NotOwner(format!(
            "tenant {tenant} is owned by instance {owner} (epoch {}), and this process never \
             claimed it; nothing was written",
            row.epoch
        ))),
        Some(c) => Err(Error::NotOwner(format!(
            "tenant {tenant} was claimed by this process as instance {} at epoch {}, but \
             instance {owner} owns it now (epoch {}); nothing was written",
            c.instance, c.epoch, row.epoch
        ))),
    }
}

/// Acquires the tenant lease ($1 tenant) for instance $2 as lease $3 for
/// $4 ms by the lease rules (`leases.rs` `ACQUIRE_SQL`): when no row exists
/// or the row is released or expired by the database clock, with a token one
/// higher. Returns the token and the expiry (epoch ms) only when acquired.
const TENANT_ACQUIRE_SQL: &str = "INSERT INTO lease \
     (tenant_id, scope, lease_key, holder, lease_id, token, expires_at, acquired_at) \
     VALUES ($1, 'tenant', '', $2, $3, 1, \
     clock_timestamp() + $4::bigint * interval '1 millisecond', clock_timestamp()) \
     ON CONFLICT (tenant_id, scope, lease_key) DO UPDATE SET \
     holder = excluded.holder, lease_id = excluded.lease_id, \
     token = lease.token + 1, expires_at = excluded.expires_at, \
     acquired_at = excluded.acquired_at \
     WHERE lease.lease_id IS NULL OR lease.expires_at <= clock_timestamp() \
     RETURNING token, (extract(epoch FROM expires_at) * 1000)::bigint";

/// The tenant lease's ($1) holder and expiry (epoch ms).
const TENANT_HOLDER_SQL: &str = "SELECT holder, (extract(epoch FROM expires_at) * 1000)::bigint \
     FROM lease WHERE tenant_id = $1 AND scope = 'tenant' AND lease_key = ''";

/// How long the tenant's ($1) lease has left by the database clock, in ms;
/// 0 once it expired or was released.
const TENANT_EXPIRES_IN_SQL: &str = "SELECT GREATEST(0, \
     (extract(epoch FROM expires_at - clock_timestamp()) * 1000)::bigint) \
     FROM lease WHERE tenant_id = $1 AND scope = 'tenant' AND lease_key = ''";

/// Renews the tenant lease ($1 tenant) while it is still lease $2 with
/// token $3, for $4 ms; returns the new expiry, or no row once it moved on.
const TENANT_RENEW_SQL: &str = "UPDATE lease \
     SET expires_at = clock_timestamp() + $4::bigint * interval '1 millisecond' \
     WHERE tenant_id = $1 AND scope = 'tenant' AND lease_key = '' \
     AND lease_id = $2 AND token = $3 \
     RETURNING (extract(epoch FROM expires_at) * 1000)::bigint";

/// Frees the tenant lease ($1 tenant) now while it is still lease $2 with
/// token $3, keeping its token; never extends it. No row once it moved on.
const TENANT_RELEASE_SQL: &str = "UPDATE lease \
     SET holder = NULL, lease_id = NULL, expires_at = LEAST(expires_at, clock_timestamp()) \
     WHERE tenant_id = $1 AND scope = 'tenant' AND lease_key = '' \
     AND lease_id = $2 AND token = $3";

/// Sets the tenant ($1) state to $2 on its `tenant_writer` row, which the
/// mover holds locked; returns the tenant's row counter. The mover decided
/// from the state it read under the lock whether the move is allowed ($2 is
/// that state again when it is not).
const TENANT_STATE_SQL: &str = "UPDATE tenant_writer SET state = $2 WHERE tenant_id = $1 \
     RETURNING last_row_seq";

/// The tenant's ($1) state, without the writer lock.
const TENANT_STATE_READ_SQL: &str = "SELECT state FROM tenant_writer WHERE tenant_id = $1";

/// The refusal of a write to a tenant that is not open.
pub(crate) fn closed_error(tenant: &str, state: TenantState) -> Error {
    Error::TenantClosed(format!(
        "tenant {tenant} is {state}, so it takes no writes; nothing was written"
    ))
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
        let lease = Lease::new(TENANT_SCOPE, "", ttl, Instant::now());
        // The writer lock without the fence check: a claim is how the fence
        // moves. It orders this claim against every writer and claimant.
        let mut tx: WriteTx = self.begin_write_as(Fence::Skip).await?;
        let taken: Option<(i64, i64)> = sqlx::query_as(TENANT_ACQUIRE_SQL)
            .bind(self.tenant())
            .bind(instance)
            .bind(lease.lease_id())
            .bind(ttl_ms)
            .fetch_optional(&mut *tx)
            .await
            .map_err(db)?;
        let Some((epoch, expires_ms)) = taken else {
            let (owner, expires_ms): (Option<String>, i64) = sqlx::query_as(TENANT_HOLDER_SQL)
                .bind(self.tenant())
                .fetch_one(&mut *tx)
                .await
                .map_err(db)?;
            tx.rollback().await.map_err(db)?;
            return Ok(TenantClaim::AlreadyOwned {
                owner: owner.unwrap_or_default(),
                expires_at: timestamp(expires_ms)?,
            });
        };
        // Record the claim before the writer slot is released: a writer of
        // this process queued behind the claim must see it, not compare the
        // new lease with the old claim and fail `NotOwner`.
        let slot = tx.commit_holding_slot().await.map_err(db)?;
        self.claim.set(Claim {
            instance: instance.into(),
            lease_id: lease.lease_id().into(),
            epoch,
            fence: TenantFence::guarding_writes(self.writer_slot.permit.clone()),
        });
        drop(slot);
        Ok(TenantClaim::TenantOwned(TenantLease {
            instance: instance.to_owned(),
            epoch,
            expires_at: timestamp(expires_ms)?,
            lease: lease.acquired(u64::try_from(epoch).unwrap_or_default()),
        }))
    }

    /// See [`Store::move_tenant_state`](crate::Store::move_tenant_state).
    /// Under the writer lock without the fence or state check
    /// (`Fence::Skip`: the move is how the state changes), so it waits for
    /// the write in flight, and the next writer reads the new state. A
    /// reopen also provisions the tenant's seed rows, in the same
    /// transaction, for a tenant closed before it ever wrote.
    pub(crate) async fn move_state(&self, to: TenantState) -> Result<TenantStateMove> {
        let mut tx: WriteTx = self.begin_write_as(Fence::Skip).await?;
        if let Err(e) = self.claim.check_current(self.tenant(), tx.writer()) {
            let _ = tx.rollback().await;
            return Err(e);
        }
        let from = tx.writer().state;
        let state = if to.moves_from().contains(&from) {
            to
        } else {
            from
        };
        let last_row_seq: i64 = sqlx::query_scalar(TENANT_STATE_SQL)
            .bind(self.tenant())
            .bind(state.as_str())
            .fetch_one(&mut *tx)
            .await
            .map_err(db)?;
        if state == TenantState::Open && from != TenantState::Open {
            sqlx::query("SELECT intent_provision_tenant($1)")
                .bind(self.tenant())
                .execute(&mut *tx)
                .await
                .map_err(db)?;
        }
        tx.commit().await.map_err(db)?;
        if state != from {
            tracing::info!(tenant = self.tenant(), %from, %state, last_row_seq, "tenant state moved");
        }
        Ok(TenantStateMove {
            from,
            state,
            last_row_seq,
        })
    }

    /// See [`Store::tenant_state`](crate::Store::tenant_state). No row: the
    /// tenant never wrote, so it is open.
    pub(crate) async fn read_state(&self) -> Result<TenantState> {
        let mut tx = self.begin().await?;
        let state: Option<String> = sqlx::query_scalar(TENANT_STATE_READ_SQL)
            .bind(self.tenant())
            .fetch_optional(&mut *tx)
            .await
            .map_err(db)?;
        tx.commit().await.map_err(db)?;
        state.map_or(Ok(TenantState::Open), TenantState::try_from)
    }

    /// See [`Store::postgres_lease_expires_in`](crate::Store::postgres_lease_expires_in).
    /// One read of the lease row, outside the writer lock.
    pub(crate) async fn lease_expires_in(&self) -> Result<Option<Duration>> {
        let mut tx = self.begin().await?;
        let ms: Option<i64> = sqlx::query_scalar(TENANT_EXPIRES_IN_SQL)
            .bind(self.tenant())
            .fetch_optional(&mut *tx)
            .await
            .map_err(db)?;
        tx.commit().await.map_err(db)?;
        Ok(ms.map(|ms| Duration::from_millis(u64::try_from(ms).unwrap_or(0))))
    }

    /// See [`Store::renew_tenant_lease`](crate::Store::renew_tenant_lease).
    /// One statement on the lease row, outside the writer lock.
    /// With `epoch`, it renews only while the current claim is that epoch's
    /// (a keeper must not renew a newer claim).
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
        let renewed: Option<i64> = sqlx::query_scalar(TENANT_RENEW_SQL)
            .bind(self.tenant())
            .bind(&*claim.lease_id)
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
            claim.fence.fire_lost(e.to_string());
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
        let released = sqlx::query(TENANT_RELEASE_SQL)
            .bind(self.tenant())
            .bind(&*claim.lease_id)
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

    /// Claim this store's Postgres tenant for this process as `instance`,
    /// for `ttl`. It succeeds when the tenant has no lease or its lease has
    /// expired by the database clock (or was released), and bumps the
    /// tenant's epoch (the lease token); every store of the tenant in this
    /// process then writes under the new claim, and any other process's
    /// writes fail `Error::NotOwner`. A live lease (also this process's own)
    /// is [`TenantClaim::AlreadyOwned`] and changes nothing. Leases of other
    /// scopes are left as they are. See the `lease` module docs.
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
    /// lease id and token; it never extends the lease. Returns whether it
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

    /// How long the current lease on Postgres tenant `tenant` has left, by
    /// the database clock: zero once it expired or was released (a claim may
    /// then succeed), `None` when the tenant was never claimed. It reads the
    /// lease row only, claiming and writing nothing, so a process refused
    /// with `Error::NotOwner` by [`Self::open_postgres_owned`] can wait that
    /// long before it claims again.
    ///
    /// # Errors
    /// `Error::StoreBusy` when no pool connection comes free;
    /// `Error::Internal` when the statement fails.
    pub async fn postgres_lease_expires_in(
        pool: PgPool,
        tenant: TenantId,
    ) -> Result<Option<Duration>> {
        let key = super::tenant_registry::tenant_key(&pool, &tenant);
        PgStore::for_key(pool, tenant, PostgresOptions::default(), key)
            .lease_expires_in()
            .await
    }

    /// The orchestrator's startup open: [`Self::open_postgres`] that claims
    /// the tenant for this process as `instance` (for `ttl`) after the
    /// schema gate and before anything else writes (provisioning, then every
    /// startup sweep the caller runs on the returned store). Once claimed it
    /// opens the tenant (`TenantState::Open`): starting a tenant undoes the
    /// close of the stop before it, finished or not.
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
        Self::open_owned_keyed(pool, tenant, options, key, instance, ttl)
            .await?
            .map_err(LeaseHeld::into_error)
    }

    /// [`Self::open_postgres_owned`], with another instance's live lease as
    /// an inner `Err(LeaseHeld)` instead of an error, so a caller can tell that
    /// refusal (nothing claimed or written) from any later failure.
    ///
    /// # Errors
    /// As [`Self::open_postgres_owned`], except a live lease.
    pub async fn try_open_postgres_owned(
        pool: PgPool,
        tenant: TenantId,
        options: PostgresOptions,
        instance: &str,
        ttl: Duration,
        _orchestrator: crate::OrchestratorOnly,
    ) -> Result<OwnedOpen> {
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
        Self::open_owned_keyed(pool, tenant, options, key, instance, ttl)
            .await?
            .map_err(LeaseHeld::into_error)
    }

    async fn open_owned_keyed(
        pool: PgPool,
        tenant: TenantId,
        options: PostgresOptions,
        key: String,
        instance: &str,
        ttl: Duration,
    ) -> Result<OwnedOpen> {
        let store = PgStore::for_key(pool, tenant, options, key);
        super::check_schema(&store.pool).await?;
        let lease = match store.claim_lease(instance, ttl).await? {
            TenantClaim::TenantOwned(lease) => lease,
            TenantClaim::AlreadyOwned { owner, expires_at } => {
                return Ok(Err(LeaseHeld {
                    tenant: store.tenant().to_owned(),
                    owner,
                    expires_at,
                }));
            }
        };
        // Starting the tenant opens it: a stop that closed it (or crashed
        // while closing it) left it refusing every write.
        store.move_state(TenantState::Open).await?;
        store.provision().await?;
        Ok(Ok((
            Self {
                backend: crate::Backend::Postgres(Box::new(store)),
            },
            lease,
        )))
    }
}

/// Renews the tenant lease for a [`LeaseKeeper`]: through the claim
/// ([`Store::renew_tenant_lease`](crate::Store::renew_tenant_lease) at its
/// epoch), and never once the claim's fence fired, which reads as lost.
struct TenantRenew {
    store: crate::Store,
    epoch: i64,
    fence: TenantFence,
}

impl Renew for TenantRenew {
    fn renew<'a>(&'a self, lease: &'a mut Lease, ttl: Duration) -> Renewal<'a> {
        Box::pin(async move {
            if self.fence.is_fenced() {
                return Ok(false);
            }
            let started = Instant::now();
            tokio::select! {
                renewed = self.store.renew_tenant_lease_at(ttl, self.epoch) => match renewed {
                    Ok(_) => {
                        lease.renewed(started, ttl);
                        Ok(true)
                    }
                    Err(Error::NotOwner(_)) => Ok(false),
                    Err(e) => Err(e),
                },
                () = self.fence.fenced() => Ok(false),
            }
        })
    }
}

/// Keeps a claimed tenant's lease: a [`LeaseKeeper`] that renews it at half
/// its TTL, whose fence fires the claim's [`TenantFence`].
///
/// The keeper's fence fires once a renewal finds the lease taken over, or
/// no renewal succeeded by the lease's local deadline (the start of the
/// last renewal that succeeded, or of the claim, plus the TTL: never later
/// than the database's expiry) minus a margin, while the lease is still
/// live in the database. A fence fired elsewhere (a write met `NotOwner`,
/// the caller fired it) stops the renewals too. Dropping the keeper stops
/// renewals and fires the fence: nothing keeps the lease past its TTL any
/// more. [`Self::release`] ends the lease at once after a clean stop.
///
/// The keeper belongs to its lease's epoch: it renews only while the
/// process's current claim is that epoch's, fires only that claim's fence,
/// and stops once a newer claim replaces it.
pub struct TenantLeaseKeeper {
    keeper: tokio::sync::Mutex<Option<LeaseKeeper>>,
    lost: LeaseFence,
    fence: TenantFence,
    /// The renewed store and the lease's epoch, for [`Self::release`].
    store: crate::Store,
    epoch: i64,
    /// Fires the tenant fence from the keeper's; kept for the keeper's whole
    /// life, so dropping the keeper always aborts it.
    bridge: tokio::task::JoinHandle<Error>,
    /// `lost()` already returned the bridge's result.
    reported: bool,
}

impl TenantLeaseKeeper {
    /// Keep `store`'s tenant lease, claimed as `lease`, with a fence margin
    /// of a sixth of its TTL ([`LEASE_FENCE_MARGIN`] for [`LEASE_TTL`]).
    #[must_use]
    pub fn spawn(store: crate::Store, lease: TenantLease) -> Self {
        let margin = lease.lease.ttl() / 6;
        Self::spawn_with_margin(store, lease, margin)
    }

    /// [`Self::spawn`] with an explicit fence `margin`: the fence fires once
    /// no renewal succeeded by the lease's local deadline minus `margin`
    /// (capped at half the TTL).
    ///
    /// It fires the fence of `store`'s claim at `lease.epoch`
    /// ([`Store::tenant_fence`](crate::Store::tenant_fence)). When that is
    /// not the current claim (never claimed, or since replaced), the keeper
    /// gets a fence of its own, fired at once, leaving any newer claim alone.
    #[must_use]
    pub fn spawn_with_margin(store: crate::Store, lease: TenantLease, margin: Duration) -> Self {
        let epoch = lease.epoch;
        let fence = store.tenant_fence_at(epoch).unwrap_or_else(|| {
            let own = TenantFence::new();
            own.fire_lost(format!(
                "the tenant lease at epoch {epoch} is not this process's current claim"
            ));
            own
        });
        let renew = TenantRenew {
            store: store.clone(),
            epoch,
            fence: fence.clone(),
        };
        let keeper = LeaseKeeper::keep_with_margin(renew, lease.lease, margin);
        let lost = keeper.fence();
        let bridge = tokio::spawn(fire_on_loss(lost.clone(), fence.clone()));
        Self {
            keeper: tokio::sync::Mutex::new(Some(keeper)),
            lost,
            fence,
            store,
            epoch,
            bridge,
            reported: false,
        }
    }

    /// The fence this keeper fires when the lease is lost.
    #[must_use]
    pub fn fence(&self) -> TenantFence {
        self.fence.clone()
    }

    /// The epoch (lease token) of the kept claim.
    #[must_use]
    pub fn epoch(&self) -> i64 {
        self.epoch
    }

    /// Why the keeper lost the lease (renewal), once it did; `None` while
    /// it renews. A fence fired elsewhere shows here at the next renewal.
    #[must_use]
    pub fn lost_reason(&self) -> Option<String> {
        self.lost.reason()
    }

    /// Wait until the claim's fence fired; the error says why (`NotOwner`).
    /// Later calls return an `Internal` error. Cancel-safe: dropping a
    /// pending call leaves the keeper running, and a later call waits again.
    pub async fn lost(&mut self) -> Error {
        if self.reported {
            return Error::Internal("tenant lease keeper already reported its loss".to_owned());
        }
        let ended = (&mut self.bridge).await;
        self.reported = true;
        ended.unwrap_or_else(|e| Error::Internal(format!("tenant lease keeper ended: {e}")))
    }

    /// Release the lease now, after a clean stop: see
    /// [`Store::release_tenant_lease`](crate::Store::release_tenant_lease).
    /// The fence must have fired and the tenant's work stopped; call it only
    /// when this process fired the fence itself, never after the lease was
    /// lost. It first stops the renewals, so none starts after the release.
    ///
    /// # Errors
    /// As [`Store::release_tenant_lease`](crate::Store::release_tenant_lease).
    pub async fn release(&self) -> Result<bool> {
        if self.fence.is_fenced() {
            if let Some(keeper) = self.keeper.lock().await.take() {
                drop(keeper.stop().await);
            }
        }
        self.store.release_tenant_lease(self.epoch).await
    }
}

impl Drop for TenantLeaseKeeper {
    fn drop(&mut self) {
        self.bridge.abort();
        self.fence
            .fire("the tenant lease keeper stopped, so nothing renews the tenant lease");
    }
}

/// Fires `fence` once the keeper's fence (`lost`) fires; ends with why the
/// tenant fence fired (its first reason).
async fn fire_on_loss(lost: LeaseFence, fence: TenantFence) -> Error {
    tokio::select! {
        () = lost.fired() => {
            fence.fire_lost(format!(
                "the tenant lease is lost: {}",
                lost.reason().unwrap_or_default()
            ));
        }
        () = fence.fenced() => {}
    }
    // The first reason the fence fired with: a renewal that met a takeover
    // fires it (naming the takeover) before the keeper reports the loss.
    Error::NotOwner(fence.reason().unwrap_or_default())
}

#[cfg(test)]
mod tests {
    use super::{fence, Claim, ClaimSlot, WriterRow};
    use intent_core::Error;

    fn row(lease: Option<&str>, epoch: i64) -> WriterRow {
        WriterRow {
            lease_id: lease.map(str::to_owned),
            owner_instance: lease.map(|_| "a".to_owned()),
            epoch,
            isolation: "read committed".to_owned(),
            state: crate::TenantState::Open,
        }
    }

    fn claim(lease: &str, epoch: i64) -> Claim {
        Claim {
            instance: "a".into(),
            lease_id: lease.into(),
            epoch,
            fence: crate::TenantFence::new(),
        }
    }

    #[test]
    fn the_fence_admits_only_the_current_claim_or_an_unclaimed_tenant() {
        assert!(fence("t", None, &row(None, 0)).is_ok());
        assert!(fence("t", Some(&claim("l1", 3)), &row(Some("l1"), 3)).is_ok());
        for (c, r) in [
            (None, row(Some("l1"), 1)),
            (None, row(None, 1)),
            (Some(claim("l1", 1)), row(Some("l2"), 2)),
            (Some(claim("l1", 2)), row(Some("l2"), 2)),
            (Some(claim("l1", 2)), row(None, 2)),
            (Some(claim("l1", 1)), row(None, 0)),
        ] {
            let e = fence("t", c.as_ref(), &r).expect_err("fenced");
            assert!(matches!(e, Error::NotOwner(_)), "{e:?}");
            assert!(e.to_string().contains("nothing was written"), "{e}");
        }
    }

    #[test]
    fn a_mismatch_fires_the_claims_fence_and_a_fired_fence_refuses() {
        let slot = ClaimSlot::default();
        slot.set(claim("l3", 3));
        let fence = slot.fence().expect("claimed");
        assert!(slot.check("t", &row(Some("l3"), 3)).is_ok());
        assert!(!fence.is_fenced());
        let e = slot
            .check("t", &row(Some("l4"), 4))
            .expect_err("taken over");
        assert!(matches!(e, Error::NotOwner(_)), "{e:?}");
        assert!(fence.is_fenced(), "a NotOwner write fires the fence");
        // The database row may still name this claim: the fence refuses.
        let e = slot.check("t", &row(Some("l3"), 3)).expect_err("fenced");
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
        slot.set(claim("l1", 1));
        let first = slot.fence().unwrap();
        slot.set(claim("l2", 2));
        assert!(first.is_fenced());
        assert!(!slot.fence().unwrap().is_fenced());
        assert!(slot.check("t", &row(Some("l2"), 2)).is_ok());
    }
}
