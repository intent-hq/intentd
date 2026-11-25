//! Leases: one owner-agnostic way for a process to hold a named unit of
//! work (`scope`, `key`) alone, for a bounded time.
//!
//! Any process of a tenant may acquire any lease; nothing ties a lease to
//! the tenant owner or the tenant epoch, so a tenant lease takeover neither
//! voids nor fences one. The work fences its own writes instead: a write
//! made for the lease holder guards the lease in the same transaction
//! (`guard_lease`), which fails unless the lease is still held.
//!
//! Postgres keeps leases in the `lease` table and decides expiry by the
//! database clock alone (`postgres/leases.rs`). `SQLite` has one process per
//! database file, so its leases live in this process (`LocalLeases`) under
//! the same rules, timed by the monotonic tokio clock.
//!
//! Rules, on both backends:
//! - a lease is acquired when nobody holds the key: no holder yet, the
//!   holder released it, or its TTL passed;
//! - every acquire makes a new lease id and a **fencing token** one higher
//!   than the key's last token, also across release and reacquire, so a
//!   later holder always has a higher token than every earlier one;
//! - leases are not re-entrant: a second acquire of a held key fails even in
//!   the holding process;
//! - renewal succeeds while the key still names this lease (also after its
//!   TTL passed, as long as nobody acquired it), and restarts the TTL;
//! - release frees the key at once when it still names this lease (keeping
//!   its token) and is a no-op otherwise;
//! - a lease is held (`lease_is_held`, `guard_lease`) while the key names it
//!   and its TTL has not passed: a lapsed lease guards nothing until it is
//!   renewed.
//!
//! While the tenant lease still exists, Postgres lease writes take the
//! tenant writer lock (`begin_write_as(Fence::LeaseOnly)`) for the writer-lock
//! lint and its ordering, without its owner check.

use std::collections::HashMap;
use std::sync::{Arc, Mutex};
use std::time::Duration;

use intent_core::{Error, Result};
use tokio::time::Instant;

use crate::Store;

/// A held lease on (`scope`, `key`); see the module docs.
///
/// Its local deadline starts before the store call that acquired or
/// renewed it, so it never runs past the deadline the store recorded.
#[derive(Debug)]
pub struct Lease {
    scope: String,
    key: String,
    id: String,
    token: u64,
    ttl: Duration,
    deadline: Instant,
}

impl Lease {
    fn new(scope: &str, key: &str, ttl: Duration, started: Instant) -> Self {
        Self {
            scope: scope.to_owned(),
            key: key.to_owned(),
            id: uuid::Uuid::new_v4().to_string(),
            token: 0,
            ttl,
            deadline: started + ttl,
        }
    }

    /// What kind of work is leased (for example `token-usage-scan`).
    #[must_use]
    pub fn scope(&self) -> &str {
        &self.scope
    }

    /// Which unit of `scope` is leased; `""` for work with one unit.
    #[must_use]
    pub fn key(&self) -> &str {
        &self.key
    }

    /// This lease's own id (a fresh one per acquire).
    #[must_use]
    pub fn lease_id(&self) -> &str {
        &self.id
    }

    /// The fencing token: strictly increasing per (tenant, `scope`, `key`)
    /// over every holder; at least 1.
    #[must_use]
    pub fn token(&self) -> u64 {
        self.token
    }

    /// The TTL the last acquire or renewal granted.
    #[must_use]
    pub fn ttl(&self) -> Duration {
        self.ttl
    }

    /// Time left before the lease may lapse, by this process's clock (a
    /// lower bound of the store's).
    #[must_use]
    pub fn remaining(&self) -> Duration {
        self.deadline.saturating_duration_since(Instant::now())
    }

    /// Whether half the TTL has passed since the last acquire or renewal: a
    /// holder renews then, well before another process can acquire it.
    #[must_use]
    pub fn needs_renewal(&self) -> bool {
        self.remaining() < self.ttl / 2
    }

    /// The token as Postgres stores it.
    #[cfg(feature = "postgres")]
    pub(crate) fn db_token(&self) -> i64 {
        i64::try_from(self.token).unwrap_or(i64::MAX)
    }

    pub(crate) fn lost(&self) -> Error {
        Error::LeaseLost(format!(
            "lease {}/{} (token {}) is no longer held",
            self.scope, self.key, self.token
        ))
    }
}

/// A lease TTL in whole milliseconds; at least 1 ms.
pub(crate) fn ttl_millis(ttl: Duration) -> Result<i64> {
    let ms = i64::try_from(ttl.as_millis()).unwrap_or(i64::MAX);
    if ms < 1 {
        return Err(Error::InvalidParams(
            "a lease TTL must be at least 1 ms".to_owned(),
        ));
    }
    Ok(ms)
}

fn validate(scope: &str, ttl: Duration) -> Result<()> {
    if scope.is_empty() {
        return Err(Error::InvalidParams(
            "a lease needs a non-empty scope".to_owned(),
        ));
    }
    ttl_millis(ttl).map(|_| ())
}

impl Store {
    /// Acquires the lease (`scope`, `key`) for `ttl`; `None` while another
    /// lease holds the key. See the module docs for the rules.
    ///
    /// # Errors
    ///
    /// `Error::InvalidParams` for an empty `scope` or a TTL under 1 ms;
    /// `Error::StoreBusy` or `Error::Internal` if the database operation
    /// fails (nothing was acquired).
    pub async fn acquire_lease(
        &self,
        scope: &str,
        key: &str,
        ttl: Duration,
    ) -> Result<Option<Lease>> {
        validate(scope, ttl)?;
        let mut lease = Lease::new(scope, key, ttl, Instant::now());
        let token = dispatch!(self, backend => backend.lease_acquire(&lease).await)?;
        Ok(token.map(|token| {
            lease.token = token;
            lease
        }))
    }

    /// Renews `lease` for `ttl` from now. `false` means it was lost
    /// (another lease holds the key, or it was released): stop the work and
    /// drop the lease.
    ///
    /// # Errors
    ///
    /// `Error::InvalidParams` for a TTL under 1 ms; `Error::StoreBusy` or
    /// `Error::Internal` if the database operation fails. The lease may
    /// still be held then; renewing again tells.
    pub async fn renew_lease(&self, lease: &mut Lease, ttl: Duration) -> Result<bool> {
        ttl_millis(ttl)?;
        let started = Instant::now();
        let kept = dispatch!(self, backend => backend.lease_renew(lease, ttl).await)?;
        if kept {
            lease.ttl = ttl;
            lease.deadline = started + ttl;
        }
        Ok(kept)
    }

    /// Frees the key of `lease` at once, so another process can acquire it
    /// without waiting for the TTL. A no-op when the lease was lost.
    ///
    /// # Errors
    ///
    /// `Error::StoreBusy` or `Error::Internal` if the database operation
    /// fails (the lease then lapses after its TTL).
    pub async fn release_lease(&self, lease: Lease) -> Result<()> {
        dispatch!(self, backend => backend.lease_release(&lease).await)
    }

    /// Whether `lease` is still held: its key names it and its TTL has not
    /// passed by the store's clock. A read; it changes nothing.
    ///
    /// # Errors
    ///
    /// `Error::StoreBusy` or `Error::Internal` if the database read fails.
    pub async fn lease_is_held(&self, lease: &Lease) -> Result<bool> {
        dispatch!(self, backend => backend.lease_held(lease).await)
    }

    /// Test seam: an empty write transaction guarded by `lease`
    /// (`guard_lease`), committed only when the guard passes.
    ///
    /// # Errors
    ///
    /// `Error::LeaseLost` when the lease is no longer held.
    #[cfg(any(test, feature = "test-support"))]
    #[doc(hidden)]
    pub async fn write_guarded_by_lease_for_tests(&self, lease: &Lease) -> Result<()> {
        dispatch!(self, backend => backend.write_guarded_by_lease_for_tests(lease).await)
    }
}

/// One key's state in `LocalLeases`: the holding lease (`None` once
/// released), the key's last token, and the holder's deadline.
struct LocalSlot {
    lease_id: Option<String>,
    token: u64,
    deadline: Instant,
}

/// `SQLite`'s leases: in this process, keyed by (`scope`, `key`). Shared by
/// every clone of the store. A released key keeps its slot, so its token
/// keeps increasing.
#[derive(Clone, Default)]
pub(crate) struct LocalLeases(Arc<Mutex<HashMap<(String, String), LocalSlot>>>);

impl LocalLeases {
    fn lock(&self) -> std::sync::MutexGuard<'_, HashMap<(String, String), LocalSlot>> {
        self.0
            .lock()
            .unwrap_or_else(std::sync::PoisonError::into_inner)
    }

    fn slot(lease: &Lease) -> (String, String) {
        (lease.scope.clone(), lease.key.clone())
    }

    fn names(slot: &LocalSlot, lease: &Lease) -> bool {
        slot.lease_id.as_deref() == Some(lease.id.as_str()) && slot.token == lease.token
    }

    fn acquire(&self, lease: &Lease) -> Option<u64> {
        let now = Instant::now();
        let mut leases = self.lock();
        let slot = leases.entry(Self::slot(lease)).or_insert(LocalSlot {
            lease_id: None,
            token: 0,
            deadline: now,
        });
        if slot.lease_id.is_some() && slot.deadline > now {
            return None;
        }
        slot.lease_id = Some(lease.id.clone());
        slot.token += 1;
        slot.deadline = now + lease.ttl;
        Some(slot.token)
    }

    fn renew(&self, lease: &Lease, ttl: Duration) -> bool {
        let now = Instant::now();
        let mut leases = self.lock();
        match leases.get_mut(&Self::slot(lease)) {
            Some(slot) if Self::names(slot, lease) => {
                slot.deadline = now + ttl;
                true
            }
            _ => false,
        }
    }

    fn release(&self, lease: &Lease) {
        let now = Instant::now();
        let mut leases = self.lock();
        if let Some(slot) = leases.get_mut(&Self::slot(lease)) {
            if Self::names(slot, lease) {
                slot.lease_id = None;
                slot.deadline = now;
            }
        }
    }

    fn is_held(&self, lease: &Lease) -> bool {
        let now = Instant::now();
        self.lock()
            .get(&Self::slot(lease))
            .is_some_and(|slot| Self::names(slot, lease) && slot.deadline > now)
    }
}

impl crate::SqliteStore {
    // One process per database file: the leases live in this process.
    #[expect(clippy::unused_async, reason = "mirrors the Postgres lease")]
    pub async fn lease_acquire(&self, lease: &Lease) -> Result<Option<u64>> {
        Ok(self.leases.acquire(lease))
    }

    #[expect(clippy::unused_async, reason = "mirrors the Postgres lease")]
    pub async fn lease_renew(&self, lease: &Lease, ttl: Duration) -> Result<bool> {
        Ok(self.leases.renew(lease, ttl))
    }

    #[expect(clippy::unused_async, reason = "mirrors the Postgres lease")]
    pub async fn lease_release(&self, lease: &Lease) -> Result<()> {
        self.leases.release(lease);
        Ok(())
    }

    #[expect(clippy::unused_async, reason = "mirrors the Postgres lease")]
    pub async fn lease_held(&self, lease: &Lease) -> Result<bool> {
        Ok(self.leases.is_held(lease))
    }

    /// Inside a write transaction: fails with `Error::LeaseLost` unless
    /// `lease` is still held. `SQLite` runs one writer at a time in one
    /// process, so the check holds for the transaction unless this process
    /// itself acquires the key after the lease lapsed, which a holder that
    /// renews at half TTL never lets happen.
    pub(crate) fn guard_lease(&self, lease: &Lease) -> Result<()> {
        if self.leases.is_held(lease) {
            Ok(())
        } else {
            Err(lease.lost())
        }
    }

    // A lost lease fails before the transaction opens, so the paused-clock
    // tests never wait on a pool timer.
    #[cfg(any(test, feature = "test-support"))]
    pub async fn write_guarded_by_lease_for_tests(&self, lease: &Lease) -> Result<()> {
        self.guard_lease(lease)?;
        let tx = self
            .write_pool()
            .begin_with("BEGIN IMMEDIATE")
            .await
            .map_err(|e| Error::Internal(e.to_string()))?;
        self.guard_lease(lease)?;
        tx.commit()
            .await
            .map_err(|e| Error::Internal(e.to_string()))
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    async fn store() -> (Store, tempfile::TempDir) {
        let dir = tempfile::tempdir().unwrap();
        let store = Store::open(&dir.path().join("store.db")).await.unwrap();
        (store, dir)
    }

    /// A store, then paused time: `SQLite` leases never reach the database,
    /// so no pool timer runs while the clock is moved by hand.
    async fn paused_store() -> (Store, tempfile::TempDir) {
        let opened = store().await;
        tokio::time::pause();
        opened
    }

    const TTL: Duration = Duration::from_secs(60);

    #[tokio::test]
    async fn one_holder_at_a_time_until_release_and_tokens_keep_rising() {
        let (store, _dir) = paused_store().await;
        let held = store.acquire_lease("scan", "", TTL).await.unwrap().unwrap();
        assert_eq!((held.scope(), held.key(), held.token()), ("scan", "", 1));
        assert!(store.lease_is_held(&held).await.unwrap());
        assert!(
            store
                .acquire_lease("scan", "", TTL)
                .await
                .unwrap()
                .is_none(),
            "not re-entrant, even in the holding process"
        );
        let other = store.acquire_lease("scan", "other", TTL).await.unwrap();
        assert_eq!(
            other.map(|l| l.token()),
            Some(1),
            "another key is other work with its own tokens"
        );
        assert!(store
            .clone()
            .acquire_lease("scan", "", TTL)
            .await
            .unwrap()
            .is_none());
        let first_id = held.lease_id().to_owned();
        store.release_lease(held).await.unwrap();
        let again = store
            .acquire_lease("scan", "", TTL)
            .await
            .unwrap()
            .expect("release frees the key at once");
        assert_eq!(again.token(), 2, "the token survives release");
        assert_ne!(again.lease_id(), first_id);
    }

    #[tokio::test]
    async fn expiry_hands_over_with_a_higher_token_and_fences_the_old_holder() {
        let (store, _dir) = paused_store().await;
        let mut old = store.acquire_lease("scan", "", TTL).await.unwrap().unwrap();
        tokio::time::advance(TTL / 2).await;
        assert!(store
            .acquire_lease("scan", "", TTL)
            .await
            .unwrap()
            .is_none());
        tokio::time::advance(TTL).await;
        assert_eq!(old.remaining(), Duration::ZERO);
        assert!(
            !store.lease_is_held(&old).await.unwrap(),
            "a lapsed lease is not held"
        );
        assert!(
            matches!(
                store.write_guarded_by_lease_for_tests(&old).await,
                Err(Error::LeaseLost(_))
            ),
            "a stale holder's guarded write fails even before a takeover"
        );
        let mut new = store.acquire_lease("scan", "", TTL).await.unwrap().unwrap();
        assert!(new.token() > old.token());
        assert!(!store.renew_lease(&mut old, TTL).await.unwrap(), "lost");
        assert!(matches!(
            store.write_guarded_by_lease_for_tests(&old).await,
            Err(Error::LeaseLost(_))
        ));
        store.release_lease(old).await.unwrap();
        assert!(
            store.renew_lease(&mut new, TTL).await.unwrap(),
            "the old holder's release leaves the new lease"
        );
    }

    #[tokio::test]
    async fn renewal_extends_the_lease() {
        let (store, _dir) = paused_store().await;
        let mut held = store.acquire_lease("scan", "", TTL).await.unwrap().unwrap();
        tokio::time::advance(TTL * 3 / 4).await;
        assert!(held.needs_renewal());
        assert!(store.renew_lease(&mut held, TTL * 2).await.unwrap());
        assert_eq!(held.ttl(), TTL * 2);
        assert!(!held.needs_renewal());
        tokio::time::advance(TTL * 3 / 2).await;
        assert!(
            store
                .acquire_lease("scan", "", TTL)
                .await
                .unwrap()
                .is_none(),
            "renewed past the first TTL"
        );
        // Lapsed but untaken: still this lease, so it renews (same token).
        tokio::time::advance(TTL * 2).await;
        assert!(store.renew_lease(&mut held, TTL).await.unwrap());
        assert_eq!(held.token(), 1);
        assert!(store.lease_is_held(&held).await.unwrap());
        assert!(store
            .acquire_lease("scan", "", TTL)
            .await
            .unwrap()
            .is_none());
    }

    /// Every acquirer is spawned before any is awaited and all wait at one
    /// barrier, on several worker threads, so they contend for the lease
    /// table's lock together.
    #[tokio::test(flavor = "multi_thread", worker_threads = 4)]
    async fn concurrent_acquirers_get_one_lease() {
        let (store, _dir) = store().await;
        let start = std::sync::Arc::new(tokio::sync::Barrier::new(16));
        let tries: Vec<_> = (0..16)
            .map(|_| {
                let s = store.clone();
                let start = std::sync::Arc::clone(&start);
                tokio::spawn(async move {
                    start.wait().await;
                    s.acquire_lease("scan", "", TTL).await.unwrap()
                })
            })
            .collect();
        let mut won = Vec::new();
        for t in tries {
            won.extend(t.await.unwrap());
        }
        assert_eq!(won.len(), 1);
        store
            .write_guarded_by_lease_for_tests(&won[0])
            .await
            .unwrap();
    }

    #[tokio::test]
    async fn rejects_an_empty_scope_or_zero_ttl() {
        let (store, _dir) = store().await;
        assert!(matches!(
            store.acquire_lease("", "", TTL).await,
            Err(Error::InvalidParams(_))
        ));
        assert!(matches!(
            store.acquire_lease("scan", "", Duration::ZERO).await,
            Err(Error::InvalidParams(_))
        ));
        let mut held = store.acquire_lease("scan", "", TTL).await.unwrap().unwrap();
        assert!(matches!(
            store.renew_lease(&mut held, Duration::ZERO).await,
            Err(Error::InvalidParams(_))
        ));
    }
}
