//! Keeping a store lease (`Store::acquire_lease`) while work runs under it.
//!
//! A [`LeaseKeeper`] owns one held [`Lease`] and renews it in the
//! background once half its TTL has passed. Its fence fires when the lease
//! is lost (renewal finds another holder, or the lease was released) or
//! when renewals keep failing until the lease's TTL ran out by this
//! process's clock (a lower bound of the store's). Each renewal is bounded
//! by the lease's remaining time, so a store call that never returns (a
//! half-open connection) still fires the fence once the lease can no
//! longer be proven held, and frees the lease lock for guarded writes. Work under the lease
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

use std::future::Future;
use std::pin::Pin;
use std::sync::Arc;
use std::time::Duration;

use tokio::sync::{watch, Mutex, MutexGuard};

use crate::{Lease, Store};

/// The shortest pause between failed renewals.
const RETRY_MIN: Duration = Duration::from_millis(50);
/// The longest pause between failed renewals.
const RETRY_MAX: Duration = Duration::from_secs(2);

/// A renewal's result: as [`Store::renew_lease`].
pub(crate) type Renewal<'a> = Pin<Box<dyn Future<Output = intent_core::Result<bool>> + Send + 'a>>;

/// What the renewer renews through: the store, the tenant lease
/// (`postgres/lease.rs`), or a test double. A renewer other than the store
/// records a kept renewal on the lease itself (`Lease::renewed`).
pub(crate) trait Renew: Send + Sync + 'static {
    fn renew<'a>(&'a self, lease: &'a mut Lease, ttl: Duration) -> Renewal<'a>;
}

impl Renew for Store {
    fn renew<'a>(&'a self, lease: &'a mut Lease, ttl: Duration) -> Renewal<'a> {
        Box::pin(self.renew_lease(lease, ttl))
    }
}

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
        Self::keep_with(store, lease)
    }

    /// [`Self::keep`], renewing through `renewer`.
    fn keep_with(renewer: impl Renew, lease: Lease) -> Self {
        Self::keep_with_margin(renewer, lease, Duration::ZERO)
    }

    /// [`Self::keep_with`], with the fence firing once no renewal succeeded
    /// by the lease's local deadline minus `margin` (capped at half the
    /// TTL), while the lease is still held: work under it stops before
    /// another process can acquire it. Each renewal is bounded by that
    /// deadline too.
    pub(crate) fn keep_with_margin(renewer: impl Renew, lease: Lease, margin: Duration) -> Self {
        let margin = margin.min(lease.ttl() / 2);
        let (tx, rx) = watch::channel(None);
        let scope = lease.scope().to_owned();
        let key = lease.key().to_owned();
        let token = lease.token();
        let lease = Arc::new(Mutex::new(Some(lease)));
        let renewer =
            intent_core::spawn_daemon(renew_until_lost(renewer, Arc::clone(&lease), margin, tx));
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

    /// Stops renewing and hands the lease back without releasing it, for a
    /// holder that releases it its own way (the tenant lease). `None` when
    /// it was already taken.
    #[cfg(feature = "postgres")]
    pub(crate) async fn stop(self) -> Option<Lease> {
        self.renewer.abort();
        self.lease.lock().await.take()
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

/// The renewer: halfway to each fence deadline (at each half TTL with no
/// margin) renews the lease for its TTL. A renewal
/// that reports the lease lost fires the fence; one that fails is retried
/// (backing off) until it succeeds or the lease ran out (its remaining time
/// reached `margin`), which fires it. A renewal that does not answer within
/// that time fires it too: the lease can no longer be proven held.
async fn renew_until_lost(
    store: impl Renew,
    lease: Arc<Mutex<Option<Lease>>>,
    margin: Duration,
    fence: watch::Sender<Option<String>>,
) {
    let mut retry = RETRY_MIN;
    loop {
        let wait = {
            let held = lease.lock().await;
            let Some(held) = held.as_ref() else { return };
            // Halfway to the fence deadline (the TTL less the margin), so
            // half that time is left to retry: half the TTL with no margin.
            let due_left = margin + held.ttl().saturating_sub(margin) / 2;
            held.remaining().saturating_sub(due_left)
        };
        tokio::time::sleep(wait).await;
        let mut held = lease.lock().await;
        let Some(current) = held.as_mut() else { return };
        let ttl = current.ttl();
        let left = current.remaining().saturating_sub(margin);
        // Bounded by the lease's own remaining time: past it the lease is unproven.
        let Ok(renewed) = tokio::time::timeout(left, store.renew(current, ttl)).await else {
            tracing::warn!(
                scope = current.scope(),
                "lease renewal did not answer before expiry; fencing"
            );
            fence.send_replace(Some(
                "lease renewal did not answer before the lease expired".to_owned(),
            ));
            return;
        };
        match renewed {
            Ok(true) => retry = RETRY_MIN,
            Ok(false) => {
                tracing::info!(scope = current.scope(), "lease lost");
                fence.send_replace(Some("lease lost to another holder".to_owned()));
                return;
            }
            Err(e) => {
                let left = current.remaining().saturating_sub(margin);
                if left.is_zero() {
                    tracing::warn!(
                        scope = current.scope(),
                        error = %e,
                        "lease renewal failed past expiry; fencing"
                    );
                    fence.send_replace(Some(format!(
                        "lease renewal kept failing until the lease's deadline: {e}"
                    )));
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

    /// A store whose renewal never answers, like a half-open connection.
    struct HungRenewal;

    impl Renew for HungRenewal {
        fn renew<'a>(&'a self, _: &'a mut Lease, _: Duration) -> Renewal<'a> {
            Box::pin(std::future::pending())
        }
    }

    #[tokio::test]
    async fn a_renewal_that_never_answers_fires_the_fence_by_expiry() {
        let (store, _dir) = sqlite().await;
        let ttl = Duration::from_millis(100);
        let lease = store.acquire_lease("scan", "", ttl).await.unwrap().unwrap();
        let keeper = LeaseKeeper::keep_with(HungRenewal, lease);
        tokio::time::timeout(timeout(), keeper.fired())
            .await
            .expect("the fence fires once the lease's remaining time ran out");
        assert!(keeper.fence().reason().unwrap().contains("did not answer"));
        // The hung renewal no longer holds the lease lock.
        let guard = tokio::time::timeout(timeout(), keeper.lease())
            .await
            .expect("guarded writes are not blocked by the hung renewal");
        assert!(
            guard.is_some(),
            "the fence fired but the lease is still kept"
        );
    }

    /// With a fence margin the fence fires while the lease is still held
    /// (paused clock: the hung renewal's bound decides alone).
    #[tokio::test]
    async fn a_fence_margin_fires_while_the_lease_is_still_held() {
        let (store, _dir) = sqlite().await;
        tokio::time::pause();
        let ttl = Duration::from_secs(60);
        let margin = Duration::from_secs(10);
        let lease = store.acquire_lease("scan", "", ttl).await.unwrap().unwrap();
        let keeper = LeaseKeeper::keep_with_margin(HungRenewal, lease, margin);
        keeper.fired().await;
        let guard = keeper.lease().await;
        let lease = guard.as_ref().expect("still kept");
        // The timer rounds to whole milliseconds.
        assert!(
            lease.remaining() + Duration::from_millis(1) >= margin,
            "fired with {:?} left, under the {margin:?} margin",
            lease.remaining()
        );
        assert!(store.lease_is_held(lease).await.unwrap());
    }

    /// A margin of half the TTL still leaves time to renew: the renewal is
    /// due halfway to the fence deadline, not at half the TTL.
    #[tokio::test]
    async fn a_keeper_with_a_margin_renews_before_its_deadline() {
        let (store, _dir) = sqlite().await;
        tokio::time::pause();
        let ttl = Duration::from_secs(60);
        let lease = store.acquire_lease("scan", "", ttl).await.unwrap().unwrap();
        let keeper = LeaseKeeper::keep_with_margin(store.clone(), lease, ttl / 2);
        // timing-guard: paused clock; ten TTLs pass only through renewals
        tokio::time::sleep(ttl * 10).await;
        assert!(!keeper.is_fired(), "{:?}", keeper.fence().reason());
        assert!(store
            .lease_is_held(keeper.lease().await.as_ref().unwrap())
            .await
            .unwrap());
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
