//! Keeping a store lease (`Store::acquire_lease`) while work runs under it.
//!
//! A [`LeaseKeeper`] owns one held [`Lease`] and renews it in the
//! background once half its TTL has passed. Its fence fires when the lease
//! is lost (renewal finds another holder, or the lease was released) or
//! when renewals keep failing until the lease's TTL ran out by this
//! process's clock (a lower bound of the store's). Work under the lease
//! stops once the fence fired; writes it makes guard the lease in their
//! transaction (`guard_lease`), so a stale holder's writes fail even before
//! it notices.
//!
//! A clean stop calls [`LeaseKeeper::release`], so another process can
//! acquire the key at once; a dropped keeper stops renewing and the lease
//! lapses after its TTL.
//!
//! It lives next to the lease API so every crate above the store can use
//! it (intent-services re-exports it). The first user is the token-usage
//! scan (`intent-services`).

use std::sync::Arc;
use std::time::Duration;

use tokio::sync::{watch, Mutex, MutexGuard};

use crate::{Lease, Store};

/// The shortest pause between failed renewals.
const RETRY_MIN: Duration = Duration::from_millis(50);
/// The longest pause between failed renewals.
const RETRY_MAX: Duration = Duration::from_secs(2);

/// Holds a lease and renews it at half TTL; see the module docs.
pub struct LeaseKeeper {
    scope: String,
    key: String,
    token: u64,
    lease: Arc<Mutex<Option<Lease>>>,
    fence: LeaseFence,
    renewer: tokio::task::JoinHandle<()>,
}

/// Fires once the kept lease is lost; clones share one fence.
#[derive(Clone)]
pub struct LeaseFence(watch::Receiver<Option<String>>);

impl LeaseFence {
    /// Whether the fence fired: stop the work that held the lease.
    #[must_use]
    pub fn is_fired(&self) -> bool {
        self.0.borrow().is_some()
    }

    /// Why the fence fired, once it did.
    #[must_use]
    pub fn reason(&self) -> Option<String> {
        self.0.borrow().clone()
    }

    /// Waits until the fence fires. Cancel-safe.
    pub async fn fired(&self) {
        let mut rx = self.0.clone();
        // The keeper's renewer holds the sender; once it ended without
        // firing (the keeper was released or dropped), wait forever.
        if rx.wait_for(Option::is_some).await.is_err() {
            std::future::pending::<()>().await;
        }
    }
}

impl LeaseKeeper {
    /// Acquires (`scope`, `key`) for `ttl` and keeps it; `None` while
    /// another holder has it.
    ///
    /// # Errors
    ///
    /// As [`Store::acquire_lease`].
    pub async fn acquire(
        store: &Store,
        scope: &str,
        key: &str,
        ttl: Duration,
    ) -> intent_core::Result<Option<Self>> {
        Ok(store
            .acquire_lease(scope, key, ttl)
            .await?
            .map(|lease| Self::keep(store.clone(), lease)))
    }

    /// Keeps `lease`, renewing it for its TTL at each half TTL.
    #[must_use]
    pub fn keep(store: Store, lease: Lease) -> Self {
        let (tx, rx) = watch::channel(None);
        let scope = lease.scope().to_owned();
        let key = lease.key().to_owned();
        let token = lease.token();
        let lease = Arc::new(Mutex::new(Some(lease)));
        let renewer = intent_core::spawn_daemon(renew_until_lost(store, Arc::clone(&lease), tx));
        Self {
            scope,
            key,
            token,
            lease,
            fence: LeaseFence(rx),
            renewer,
        }
    }

    /// The kept lease's scope.
    #[must_use]
    pub fn scope(&self) -> &str {
        &self.scope
    }

    /// The kept lease's key.
    #[must_use]
    pub fn key(&self) -> &str {
        &self.key
    }

    /// The kept lease's fencing token.
    #[must_use]
    pub fn token(&self) -> u64 {
        self.token
    }

    /// The fence: fires when the lease is lost.
    #[must_use]
    pub fn fence(&self) -> LeaseFence {
        self.fence.clone()
    }

    /// Whether the fence fired.
    #[must_use]
    pub fn is_fired(&self) -> bool {
        self.fence.is_fired()
    }

    /// Waits until the fence fires. Cancel-safe.
    pub async fn fired(&self) {
        self.fence.fired().await;
    }

    /// The kept lease, for a write that guards it (`guard_lease`); `None`
    /// once released. Renewal waits while the guard is held, so hold it
    /// only for the write.
    pub async fn lease(&self) -> MutexGuard<'_, Option<Lease>> {
        self.lease.lock().await
    }

    /// Stops renewing and frees the lease at once (a clean stop). Best
    /// effort: a failed release lapses after the TTL.
    pub async fn release(self, store: &Store) {
        self.renewer.abort();
        let Some(lease) = self.lease.lock().await.take() else {
            return;
        };
        if let Err(e) = store.release_lease(lease).await {
            tracing::debug!(
                scope = self.scope,
                error = %e,
                "lease release failed; it lapses by TTL"
            );
        }
    }
}

impl Drop for LeaseKeeper {
    fn drop(&mut self) {
        self.renewer.abort();
    }
}

/// The renewer: at each half TTL renews the lease for its TTL. A renewal
/// that reports the lease lost fires the fence; one that fails is retried
/// (backing off) until it succeeds or the lease ran out, which fires it.
async fn renew_until_lost(
    store: Store,
    lease: Arc<Mutex<Option<Lease>>>,
    fence: watch::Sender<Option<String>>,
) {
    let mut retry = RETRY_MIN;
    loop {
        let wait = {
            let held = lease.lock().await;
            let Some(held) = held.as_ref() else { return };
            held.remaining().saturating_sub(held.ttl() / 2)
        };
        tokio::time::sleep(wait).await;
        let mut held = lease.lock().await;
        let Some(current) = held.as_mut() else { return };
        let ttl = current.ttl();
        match store.renew_lease(current, ttl).await {
            Ok(true) => retry = RETRY_MIN,
            Ok(false) => {
                tracing::info!(scope = current.scope(), "lease lost");
                fence.send_replace(Some("lease lost to another holder".to_owned()));
                return;
            }
            Err(e) => {
                let left = current.remaining();
                if left.is_zero() {
                    tracing::warn!(
                        scope = current.scope(),
                        error = %e,
                        "lease renewal failed past expiry; fencing"
                    );
                    fence.send_replace(Some(format!("lease renewal failed past expiry: {e}")));
                    return;
                }
                tracing::warn!(scope = current.scope(), error = %e, "lease renewal failed; retrying");
                drop(held);
                tokio::time::sleep(retry.min(left)).await;
                retry = (retry * 2).min(RETRY_MAX);
            }
        }
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    const TTL: Duration = Duration::from_secs(600);

    async fn sqlite() -> (Store, tempfile::TempDir) {
        let dir = tempfile::tempdir().unwrap();
        let store = Store::open(&dir.path().join("store.db")).await.unwrap();
        (store, dir)
    }

    fn timeout() -> Duration {
        intentd_test_support::test_timeout(Duration::from_secs(10))
    }

    #[tokio::test]
    async fn the_keeper_renews_at_half_ttl_and_holds_the_key() {
        let (store, _dir) = sqlite().await;
        let ttl = Duration::from_millis(200);
        let keeper = LeaseKeeper::acquire(&store, "scan", "", ttl)
            .await
            .unwrap()
            .unwrap();
        assert_eq!(
            (keeper.scope(), keeper.key(), keeper.token()),
            ("scan", "", 1)
        );
        // timing-guard: several TTLs, so only renewals keep the lease
        tokio::time::sleep(ttl * 4).await;
        assert!(!keeper.is_fired());
        assert!(store
            .lease_is_held(keeper.lease().await.as_ref().unwrap())
            .await
            .unwrap());
        assert!(store
            .acquire_lease("scan", "", ttl)
            .await
            .unwrap()
            .is_none());
        keeper.release(&store).await;
        let next = store
            .acquire_lease("scan", "", ttl)
            .await
            .unwrap()
            .expect("release frees the key at once");
        assert_eq!(next.token(), 2);
    }

    #[tokio::test]
    async fn the_fence_fires_when_the_lease_is_lost() {
        let (store, _dir) = sqlite().await;
        let ttl = Duration::from_millis(40);
        let keeper = LeaseKeeper::acquire(&store, "scan", "", ttl)
            .await
            .unwrap()
            .unwrap();
        // Take the lease from under the keeper: stop its renewals by holding
        // its lease lock, let the lease lapse, and acquire it elsewhere.
        let other = {
            let _held = keeper.lease().await;
            tokio::time::timeout(timeout(), async {
                loop {
                    if let Some(l) = store.acquire_lease("scan", "", TTL).await.unwrap() {
                        break l;
                    }
                    tokio::task::yield_now().await;
                }
            })
            .await
            .expect("the lease lapses")
        };
        tokio::time::timeout(timeout(), keeper.fired())
            .await
            .expect("the fence fires on loss");
        assert!(keeper.is_fired());
        assert!(keeper.fence().reason().unwrap().contains("lost"));
        keeper.release(&store).await;
        assert!(
            store.lease_is_held(&other).await.unwrap(),
            "a lost keeper's release leaves the new holder"
        );
    }

    #[tokio::test]
    async fn the_fence_waits_forever_once_released_without_loss() {
        let (store, _dir) = sqlite().await;
        let keeper = LeaseKeeper::acquire(&store, "scan", "", TTL)
            .await
            .unwrap()
            .unwrap();
        let fence = keeper.fence();
        keeper.release(&store).await;
        assert!(!fence.is_fired());
        // fixed-deadline: allow — an observation window: asserts the fence stays unfired
        assert!(
            tokio::time::timeout(Duration::from_millis(50), fence.fired())
                .await
                .is_err(),
            "a clean release does not fire the fence"
        );
    }
}
