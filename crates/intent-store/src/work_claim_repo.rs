//! Work claims: a named unit of periodic work (`kind`, `key`) that at most
//! one holder runs at a time, for a bounded time.
//!
//! A periodic loop that several processes of one tenant run (orchestrator
//! replicas) claims its work before each run and skips the run when another
//! holder has it. A claim expires after its TTL unless renewed, so a holder
//! that crashed without releasing frees it on its own.
//!
//! Postgres keeps the claim in `work_claim` and decides expiry by the
//! database clock alone (`postgres/work_claims.rs`). `SQLite` has one process
//! per database file, so its claims live in this process (`LocalWorkClaims`)
//! under the same rules, timed by the monotonic tokio clock.
//!
//! Rules, on both backends:
//! - a claim is taken when no holder has it, its holder's TTL has passed, or
//!   (Postgres) its holder claimed at an older tenant epoch (a takeover);
//! - every claim has its own id, so claims are not re-entrant: a second
//!   claim of held work fails even in the holding process;
//! - renewal succeeds while the work still names this claim (also after its
//!   TTL passed, as long as nobody took it), and restarts the TTL;
//! - release frees the work at once when it still names this claim, and is
//!   a no-op otherwise.

use std::collections::HashMap;
use std::sync::{Arc, Mutex};
use std::time::Duration;

use intent_core::{Error, Result};
use tokio::time::Instant;

use crate::Store;

/// A held claim on one unit of work; see the module docs.
///
/// Its local deadline starts before the store call that took or renewed
/// it, so it never runs past the deadline the store recorded.
#[derive(Debug)]
pub struct WorkClaim {
    kind: String,
    key: String,
    claim_id: String,
    ttl: Duration,
    deadline: Instant,
}

impl WorkClaim {
    fn new(kind: &str, key: &str, ttl: Duration, started: Instant) -> Self {
        Self {
            kind: kind.to_owned(),
            key: key.to_owned(),
            claim_id: uuid::Uuid::new_v4().to_string(),
            ttl,
            deadline: started + ttl,
        }
    }

    /// The kind of work claimed (for example `token-usage-scan`).
    #[must_use]
    pub fn kind(&self) -> &str {
        &self.kind
    }

    /// Which unit of `kind` is claimed; `""` for work with one unit.
    #[must_use]
    pub fn key(&self) -> &str {
        &self.key
    }

    /// The TTL each take or renewal grants.
    #[must_use]
    pub fn ttl(&self) -> Duration {
        self.ttl
    }

    /// Time left before the claim may lapse, by this process's clock (a
    /// lower bound of the store's).
    #[must_use]
    pub fn remaining(&self) -> Duration {
        self.deadline.saturating_duration_since(Instant::now())
    }

    /// Whether half the TTL has passed since the last take or renewal: a
    /// long run renews then, well before another holder can take over.
    #[must_use]
    pub fn needs_renewal(&self) -> bool {
        self.remaining() < self.ttl / 2
    }

    pub(crate) fn claim_id(&self) -> &str {
        &self.claim_id
    }
}

/// A work TTL in whole milliseconds; at least 1 ms.
pub(crate) fn ttl_millis(ttl: Duration) -> Result<i64> {
    let ms = i64::try_from(ttl.as_millis()).unwrap_or(i64::MAX);
    if ms < 1 {
        return Err(Error::InvalidParams(
            "a work claim TTL must be at least 1 ms".to_owned(),
        ));
    }
    Ok(ms)
}

fn validate(kind: &str, ttl: Duration) -> Result<()> {
    if kind.is_empty() {
        return Err(Error::InvalidParams(
            "a work claim needs a non-empty kind".to_owned(),
        ));
    }
    ttl_millis(ttl).map(|_| ())
}

impl Store {
    /// Claims the work (`kind`, `key`) for `ttl`; `None` when another holder
    /// has it. See the module docs for the rules.
    ///
    /// # Errors
    ///
    /// `Error::InvalidParams` for an empty `kind` or a TTL under 1 ms;
    /// `Error::NotOwner` when this process lost the tenant (Postgres);
    /// `Error::Internal` if the database operation fails.
    pub async fn claim_work(
        &self,
        kind: &str,
        key: &str,
        ttl: Duration,
    ) -> Result<Option<WorkClaim>> {
        validate(kind, ttl)?;
        let claim = WorkClaim::new(kind, key, ttl, Instant::now());
        let won = dispatch!(self, backend => backend.take_work_claim(&claim).await)?;
        Ok(won.then_some(claim))
    }

    /// Renews `claim` for another TTL. `false` means it was lost (another
    /// holder took the work, or the tenant moved to a newer epoch): stop
    /// the work and drop the claim.
    ///
    /// # Errors
    ///
    /// `Error::NotOwner` when this process lost the tenant (Postgres);
    /// `Error::Internal` if the database operation fails. The claim may
    /// still be held then; renewing again tells.
    pub async fn renew_work_claim(&self, claim: &mut WorkClaim) -> Result<bool> {
        let started = Instant::now();
        let kept = dispatch!(self, backend => backend.renew_work_claim(claim).await)?;
        if kept {
            claim.deadline = started + claim.ttl;
        }
        Ok(kept)
    }

    /// Frees the work of `claim` at once, so another holder can take it
    /// without waiting for the TTL. A no-op when the claim was lost.
    ///
    /// # Errors
    ///
    /// `Error::NotOwner` when this process lost the tenant (Postgres);
    /// `Error::Internal` if the database operation fails (the claim then
    /// lapses after its TTL).
    pub async fn release_work_claim(&self, claim: WorkClaim) -> Result<()> {
        dispatch!(self, backend => backend.release_work_claim(&claim).await)
    }
}

/// `SQLite`'s work claims: in this process, keyed by (`kind`, `key`), each
/// naming its claim id and deadline. Shared by every clone of the store.
#[derive(Clone, Default)]
pub(crate) struct LocalWorkClaims(Arc<Mutex<HashMap<(String, String), (String, Instant)>>>);

impl LocalWorkClaims {
    fn lock(&self) -> std::sync::MutexGuard<'_, HashMap<(String, String), (String, Instant)>> {
        self.0
            .lock()
            .unwrap_or_else(std::sync::PoisonError::into_inner)
    }

    fn take(&self, claim: &WorkClaim) -> bool {
        let now = Instant::now();
        let mut claims = self.lock();
        let slot = (claim.kind.clone(), claim.key.clone());
        if claims
            .get(&slot)
            .is_some_and(|(_, deadline)| *deadline > now)
        {
            return false;
        }
        claims.insert(slot, (claim.claim_id.clone(), now + claim.ttl));
        true
    }

    fn renew(&self, claim: &WorkClaim) -> bool {
        let now = Instant::now();
        let mut claims = self.lock();
        match claims.get_mut(&(claim.kind.clone(), claim.key.clone())) {
            Some((id, deadline)) if *id == claim.claim_id => {
                *deadline = now + claim.ttl;
                true
            }
            _ => false,
        }
    }

    fn release(&self, claim: &WorkClaim) {
        let mut claims = self.lock();
        let slot = (claim.kind.clone(), claim.key.clone());
        if claims
            .get(&slot)
            .is_some_and(|(id, _)| *id == claim.claim_id)
        {
            claims.remove(&slot);
        }
    }
}

impl crate::SqliteStore {
    // One process per database file: the claims live in this process.
    #[expect(clippy::unused_async, reason = "mirrors the Postgres claim")]
    pub async fn take_work_claim(&self, claim: &WorkClaim) -> Result<bool> {
        Ok(self.work_claims.take(claim))
    }

    #[expect(clippy::unused_async, reason = "mirrors the Postgres claim")]
    pub async fn renew_work_claim(&self, claim: &WorkClaim) -> Result<bool> {
        Ok(self.work_claims.renew(claim))
    }

    #[expect(clippy::unused_async, reason = "mirrors the Postgres claim")]
    pub async fn release_work_claim(&self, claim: &WorkClaim) -> Result<()> {
        self.work_claims.release(claim);
        Ok(())
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

    /// A store, then paused time: `SQLite` claims never reach the database,
    /// so no pool timer runs while the clock is moved by hand.
    async fn paused_store() -> (Store, tempfile::TempDir) {
        let opened = store().await;
        tokio::time::pause();
        opened
    }

    const TTL: Duration = Duration::from_secs(60);

    #[tokio::test]
    async fn one_holder_at_a_time_until_release() {
        let (store, _dir) = paused_store().await;
        let held = store.claim_work("scan", "", TTL).await.unwrap().unwrap();
        assert_eq!((held.kind(), held.key()), ("scan", ""));
        assert!(
            store.claim_work("scan", "", TTL).await.unwrap().is_none(),
            "not re-entrant, even in the holding process"
        );
        assert!(
            store
                .claim_work("scan", "other", TTL)
                .await
                .unwrap()
                .is_some(),
            "another key is other work"
        );
        assert!(store
            .clone()
            .claim_work("scan", "", TTL)
            .await
            .unwrap()
            .is_none());
        store.release_work_claim(held).await.unwrap();
        assert!(
            store.claim_work("scan", "", TTL).await.unwrap().is_some(),
            "release frees the work at once"
        );
    }

    #[tokio::test]
    async fn expiry_hands_over_and_the_old_holder_cannot_renew() {
        let (store, _dir) = paused_store().await;
        let mut old = store.claim_work("scan", "", TTL).await.unwrap().unwrap();
        tokio::time::advance(TTL / 2).await;
        assert!(store.claim_work("scan", "", TTL).await.unwrap().is_none());
        tokio::time::advance(TTL).await;
        assert_eq!(old.remaining(), Duration::ZERO);
        let _new = store.claim_work("scan", "", TTL).await.unwrap().unwrap();
        assert!(!store.renew_work_claim(&mut old).await.unwrap(), "lost");
        store.release_work_claim(old).await.unwrap();
        assert!(
            store.claim_work("scan", "", TTL).await.unwrap().is_none(),
            "the old holder's release leaves the new claim"
        );
    }

    #[tokio::test]
    async fn renewal_extends_the_claim() {
        let (store, _dir) = paused_store().await;
        let mut held = store.claim_work("scan", "", TTL).await.unwrap().unwrap();
        tokio::time::advance(TTL * 3 / 4).await;
        assert!(held.needs_renewal());
        assert!(store.renew_work_claim(&mut held).await.unwrap());
        assert!(!held.needs_renewal());
        tokio::time::advance(TTL * 3 / 4).await;
        assert!(
            store.claim_work("scan", "", TTL).await.unwrap().is_none(),
            "renewed past the first TTL"
        );
        // Lapsed but untaken: still this claim, so it renews.
        tokio::time::advance(TTL * 2).await;
        assert!(store.renew_work_claim(&mut held).await.unwrap());
        assert!(store.claim_work("scan", "", TTL).await.unwrap().is_none());
    }

    #[tokio::test]
    async fn concurrent_claimers_get_one_claim() {
        let (store, _dir) = store().await;
        let tries = (0..16).map(|_| {
            let s = store.clone();
            tokio::spawn(async move { s.claim_work("scan", "", TTL).await.unwrap() })
        });
        let mut won = 0;
        for t in tries {
            won += usize::from(t.await.unwrap().is_some());
        }
        assert_eq!(won, 1);
    }

    #[tokio::test]
    async fn rejects_an_empty_kind_or_zero_ttl() {
        let (store, _dir) = store().await;
        assert!(matches!(
            store.claim_work("", "", TTL).await,
            Err(Error::InvalidParams(_))
        ));
        assert!(matches!(
            store.claim_work("scan", "", Duration::ZERO).await,
            Err(Error::InvalidParams(_))
        ));
    }
}
