//! [`LeaseLinkOwnership`]: link ownership any process can hold, one store
//! lease per machine link (`scope` [`LINK_LEASE_SCOPE`], key the machine
//! id).
//!
//! - A claim acquires the machine's lease (or reuses the one this backend
//!   holds) and advances the link generation in one write transaction
//!   guarded by the lease, so the generation only moves for the holder and
//!   is shared by every holder: a later holder always dials a newer one.
//! - The claim's epoch is the lease's fencing token.
//! - The welcome and journal-ack writes are guarded by the lease in their
//!   own transaction: a holder that lost the lease writes nothing, even if
//!   it passed every earlier check and then stalled past a takeover.
//! - A [`LeaseKeeper`] per held lease renews it at half its TTL. When
//!   renewal finds the lease taken, or keeps failing until the lease's
//!   expiry, its fence fires and the claim is lost: [`LinkOwnership::lost`]
//!   resolves and the link closes.
//! - A connector that stops, goes idle or stops wanting the link releases
//!   the lease, so another process can claim the link at once. Only the
//!   latest claim of a lease releases it: a connector that replaced another
//!   for the same machine in this process keeps the lease the old one
//!   claimed before it.

use std::collections::HashMap;
use std::sync::atomic::{AtomicU64, Ordering};
use std::sync::{Arc, Mutex};
use std::time::Duration;

use intent_core::node_link::LinkWelcome;
use intent_core::nodes::{LeaseOwner, NodeCounter};
use intent_core::{BoxFuture, Error, Result};
use intent_store::{LeaseKeeper, Store};
use tokio::sync::watch;

use super::{Fenced, LinkClaim, LinkOwnership};
use crate::lease::LinkWrite;
use crate::lock;

/// The lease scope of a machine link; the key is the machine id.
pub const LINK_LEASE_SCOPE: &str = "machine-link";

/// How long a link lease lasts without renewal: a crashed holder's links
/// can be taken over after this long.
pub const LINK_LEASE_TTL: Duration = Duration::from_secs(30);

/// Link ownership through store leases; see the module docs. One per
/// backend, shared by all its connectors.
pub struct LeaseLinkOwnership {
    store: Store,
    ttl: Duration,
    head_session: Arc<str>,
    /// The link leases this backend holds, by machine id.
    links: Mutex<HashMap<String, Arc<HeldLink>>>,
    /// Claims and releases of one machine's link take turns, so two in
    /// this process never race each other for its lease; other machines'
    /// do not wait on them.
    turns: Mutex<HashMap<String, Arc<tokio::sync::Mutex<()>>>>,
    /// Test seam: releases each stalled lease's renewals once dropped.
    #[cfg(any(test, feature = "test-support"))]
    stalls: Mutex<Vec<tokio::sync::oneshot::Sender<()>>>,
}

/// One held link lease, kept (renewed) by its [`LeaseKeeper`].
struct HeldLink {
    keeper: LeaseKeeper,
    /// The link generation of the latest claim under this lease: only that
    /// claim releases it.
    claimed: AtomicU64,
    /// Set once this backend gave the lease up (released it, or found it
    /// lost in a guarded write): the keeper's fence does not fire then.
    given_up: watch::Sender<bool>,
}

impl HeldLink {
    fn is_lost(&self) -> bool {
        self.keeper.is_fired() || *self.given_up.borrow()
    }

    fn give_up(&self) {
        self.given_up.send_replace(true);
    }

    /// Resolves once the lease is lost or given up.
    async fn lost(&self) {
        let mut given_up = self.given_up.subscribe();
        tokio::select! {
            () = self.keeper.fired() => {}
            _ = given_up.wait_for(|given_up| *given_up) => {}
        }
    }
}

fn fenced(claim: &LinkClaim) -> Fenced {
    Fenced {
        machine_id: claim.owner.node_id.clone(),
        epoch: claim.epoch,
    }
}

fn guarded<T>(claim: &LinkClaim, result: Result<T>) -> std::result::Result<T, LinkWrite> {
    match result {
        Ok(value) => Ok(value),
        Err(Error::LeaseLost(_)) => Err(LinkWrite::Fenced(fenced(claim))),
        Err(error) => Err(LinkWrite::Failed(error)),
    }
}

impl LeaseLinkOwnership {
    /// Ownership of `store`'s machine links through leases of `ttl`,
    /// offering `head_session` on every link.
    #[must_use]
    pub fn new(store: Store, head_session: Arc<str>, ttl: Duration) -> Self {
        Self {
            store,
            ttl,
            head_session,
            links: Mutex::default(),
            turns: Mutex::default(),
            #[cfg(any(test, feature = "test-support"))]
            stalls: Mutex::default(),
        }
    }

    /// Test seam: stop renewing every link lease held now, as a holder cut
    /// off from the database would, so they lapse after their TTL. Renewal
    /// (and every write guarded by a stalled lease) waits until
    /// [`Self::resume_renewals`].
    #[cfg(any(test, feature = "test-support"))]
    pub async fn stall_renewals(&self) {
        let held: Vec<_> = lock(&self.links).values().cloned().collect();
        for link in held {
            let (resume, resumed) = tokio::sync::oneshot::channel::<()>();
            let (stalled, is_stalled) = tokio::sync::oneshot::channel::<()>();
            tokio::spawn(async move {
                let _renewal_waits = link.keeper.lease().await;
                let _ = stalled.send(());
                let _ = resumed.await;
            });
            let _ = is_stalled.await;
            lock(&self.stalls).push(resume);
        }
    }

    /// Test seam: let the leases [`Self::stall_renewals`] stalled renew
    /// again; a lease another process took meanwhile is then found lost.
    #[cfg(any(test, feature = "test-support"))]
    pub fn resume_renewals(&self) {
        lock(&self.stalls).clear();
    }

    /// The held lease of `claim`'s link, unless it was lost or replaced.
    fn held(&self, claim: &LinkClaim) -> Option<Arc<HeldLink>> {
        lock(&self.links)
            .get(&claim.owner.node_id)
            .filter(|held| held.keeper.token() == claim.epoch && !held.is_lost())
            .cloned()
    }

    /// Give `held` up and forget it as `machine_id`'s lease, if it still is.
    fn forget(&self, machine_id: &str, held: &Arc<HeldLink>) {
        held.give_up();
        let mut links = lock(&self.links);
        if links
            .get(machine_id)
            .is_some_and(|current| Arc::ptr_eq(current, held))
        {
            links.remove(machine_id);
        }
    }

    /// The lock `machine_id`'s claims and releases take turns on.
    fn turn(&self, machine_id: &str) -> Arc<tokio::sync::Mutex<()>> {
        lock(&self.turns)
            .entry(machine_id.to_owned())
            .or_default()
            .clone()
    }

    /// A guarded write under `held` found the lease lost: forget it, so the
    /// next claim acquires one again rather than reusing it.
    fn on_write<T>(
        &self,
        claim: &LinkClaim,
        held: &Arc<HeldLink>,
        result: Result<T>,
    ) -> std::result::Result<T, LinkWrite> {
        if matches!(result, Err(Error::LeaseLost(_))) {
            self.forget(&claim.owner.node_id, held);
        }
        guarded(claim, result)
    }

    /// `machine_id`'s held lease, acquired if this backend holds none.
    async fn lease_for(&self, machine_id: &str) -> Result<Arc<HeldLink>> {
        let held = lock(&self.links)
            .get(machine_id)
            .filter(|held| !held.is_lost())
            .cloned();
        if let Some(held) = held {
            return Ok(held);
        }
        let Some(keeper) =
            LeaseKeeper::acquire(&self.store, LINK_LEASE_SCOPE, machine_id, self.ttl).await?
        else {
            return Err(Error::LeaseLost(format!(
                "remote-link-owned-elsewhere: another process owns the link to machine {machine_id}"
            )));
        };
        let held = Arc::new(HeldLink {
            keeper,
            claimed: AtomicU64::new(0),
            given_up: watch::channel(false).0,
        });
        lock(&self.links).insert(machine_id.to_owned(), held.clone());
        Ok(held)
    }
}

impl LinkOwnership for LeaseLinkOwnership {
    fn claim<'a>(&'a self, owner: &'a LeaseOwner) -> BoxFuture<'a, Result<LinkClaim>> {
        Box::pin(async move {
            let machine_id = owner.node_id.as_str();
            let turn = self.turn(machine_id);
            let _turn = turn.lock().await;
            let held = self.lease_for(machine_id).await?;
            let generation = {
                let guard = held.keeper.lease().await;
                match guard.as_ref() {
                    Some(lease) => {
                        self.store
                            .next_node_link_generation_under_lease(owner, lease)
                            .await
                    }
                    None => Err(Error::LeaseLost(format!(
                        "the link lease of machine {machine_id} was given up"
                    ))),
                }
            };
            match generation {
                Ok(generation) => {
                    held.claimed.store(generation.0, Ordering::SeqCst);
                    Ok(LinkClaim {
                        owner: owner.clone(),
                        epoch: held.keeper.token(),
                        generation,
                    })
                }
                Err(error) => {
                    // A lost lease: forget it, so the next claim acquires
                    // one again.
                    if matches!(error, Error::LeaseLost(_)) {
                        self.forget(machine_id, &held);
                    }
                    Err(error)
                }
            }
        })
    }

    fn fence<'a>(&'a self, claim: &'a LinkClaim) -> BoxFuture<'a, std::result::Result<(), Fenced>> {
        Box::pin(async move {
            let held = self.held(claim).ok_or_else(|| fenced(claim))?;
            let guard = held.keeper.lease().await;
            let lease = guard.as_ref().ok_or_else(|| fenced(claim))?;
            match self.store.lease_is_held(lease).await {
                Ok(true) => Ok(()),
                Ok(false) => Err(fenced(claim)),
                Err(error) => {
                    tracing::warn!(%error, machine = %claim.owner.node_id, "remote machine: could not check the link lease; treating the link as fenced");
                    Err(fenced(claim))
                }
            }
        })
    }

    fn persist_ack<'a>(
        &'a self,
        _store: &'a Store,
        claim: &'a LinkClaim,
        seq: NodeCounter,
    ) -> BoxFuture<'a, std::result::Result<NodeCounter, LinkWrite>> {
        Box::pin(async move {
            let held = self
                .held(claim)
                .ok_or_else(|| LinkWrite::Fenced(fenced(claim)))?;
            let guard = held.keeper.lease().await;
            let lease = guard
                .as_ref()
                .ok_or_else(|| LinkWrite::Fenced(fenced(claim)))?;
            let result = self
                .store
                .advance_node_lease_ack_under_lease(&claim.owner, seq, lease)
                .await;
            drop(guard);
            self.on_write(claim, &held, result)
        })
    }

    fn record_welcome<'a>(
        &'a self,
        _store: &'a Store,
        claim: &'a LinkClaim,
        welcome: &'a LinkWelcome,
    ) -> BoxFuture<'a, std::result::Result<(), LinkWrite>> {
        Box::pin(async move {
            let held = self
                .held(claim)
                .ok_or_else(|| LinkWrite::Fenced(fenced(claim)))?;
            let guard = held.keeper.lease().await;
            let lease = guard
                .as_ref()
                .ok_or_else(|| LinkWrite::Fenced(fenced(claim)))?;
            let capabilities = &welcome.capabilities;
            let result = self
                .store
                .record_node_welcome_under_lease(
                    &claim.owner,
                    capabilities.platform.os,
                    capabilities.platform.arch,
                    capabilities.capacity.agent_cap,
                    &capabilities.intentd_version,
                    lease,
                )
                .await
                .map(drop);
            drop(guard);
            self.on_write(claim, &held, result)
        })
    }

    fn lost<'a>(&'a self, claim: &'a LinkClaim) -> BoxFuture<'a, ()> {
        Box::pin(async move {
            if let Some(held) = self.held(claim) {
                held.lost().await;
            }
        })
    }

    fn release<'a>(&'a self, claim: &'a LinkClaim) -> BoxFuture<'a, ()> {
        Box::pin(async move {
            let owner = &claim.owner;
            let turn = self.turn(&owner.node_id);
            let _turn = turn.lock().await;
            let held = {
                let mut links = lock(&self.links);
                // Only the latest claim of the held lease releases it: an
                // older connector of this machine (replaced, or superseded
                // by a newer dial) leaves it to the one that claimed since.
                let latest = links.get(&owner.node_id).is_some_and(|held| {
                    held.keeper.token() == claim.epoch
                        && held.claimed.load(Ordering::SeqCst) == claim.generation.0
                });
                if !latest {
                    return;
                }
                links.remove(&owner.node_id)
            };
            let Some(held) = held else { return };
            held.give_up();
            // Taking the lease ends its renewals; a write still in flight
            // under it finishes first.
            let lease = held.keeper.lease().await.take();
            if let Some(lease) = lease {
                if let Err(error) = self.store.release_lease(lease).await {
                    tracing::warn!(%error, machine = %owner.node_id, "remote machine: could not release the link lease; it lapses after its TTL");
                }
            }
        })
    }

    fn head_session(&self) -> &str {
        &self.head_session
    }
}

#[cfg(test)]
mod tests {
    use super::super::tests::store_with_lease;
    use super::*;

    const TTL: Duration = Duration::from_millis(400);

    #[tokio::test]
    async fn a_guarded_write_that_finds_the_lease_lost_forgets_it() {
        let (_dir, store, owner) = store_with_lease().await;
        let a = LeaseLinkOwnership::new(store.clone(), "a".into(), TTL);
        let b = LeaseLinkOwnership::new(store.clone(), "b".into(), TTL);
        let first = a.claim(&owner).await.unwrap();
        let held = lock(&a.links).get("machine-1").cloned().unwrap();
        // Hold the lease out of the keeper (its renewals end) until it
        // lapses and `b` takes the link, then hand it back: `a` now holds a
        // stale lease its keeper never found lost.
        let lease = held.keeper.lease().await.take().unwrap();
        let deadline = tokio::time::Instant::now() + intentd_test_support::test_timeout(TTL * 10);
        let taken = loop {
            match b.claim(&owner).await {
                Ok(claim) => break claim,
                Err(_) if tokio::time::Instant::now() < deadline => {
                    tokio::time::sleep(TTL / 8).await;
                }
                Err(error) => panic!("the lapsed link was never free: {error}"),
            }
        };
        *held.keeper.lease().await = Some(lease);
        assert!(!held.is_lost());

        assert!(matches!(
            a.persist_ack(&store, &first, NodeCounter(3)).await,
            Err(LinkWrite::Fenced(_))
        ));
        assert!(
            lock(&a.links).get("machine-1").is_none(),
            "the lost lease is forgotten"
        );
        // So the next claim tries a fresh acquire instead of reusing it.
        let error = a.claim(&owner).await.unwrap_err();
        assert!(
            error.to_string().contains("remote-link-owned-elsewhere"),
            "{error}"
        );
        assert_eq!(b.fence(&taken).await, Ok(()));
    }
}
