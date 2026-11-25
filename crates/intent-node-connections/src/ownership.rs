//! Link ownership: which process may drive a machine's link, and the fence
//! every durable link write passes first.
//!
//! A dial claims the link ([`LinkOwnership::claim`]): the claim carries the
//! lease's new link generation, which the machine enforces (it accepts only
//! the newest one). Before each durable write for that link (the journal
//! ack, the welcome) the holder checks its claim ([`LinkOwnership::fence`]).
//! A fenced holder writes nothing and closes the link.
//!
//! [`LocalOwnership`] (the desktop) owns every link of its process.
//! [`LeaseLinkOwnership`] (the orchestrator) holds one store lease per
//! machine link, so any process may own any link and a takeover fences the
//! previous holder's writes in the database.

use intent_core::node_link::LinkWelcome;
use intent_core::nodes::{LeaseOwner, NodeCounter};
use intent_core::{BoxFuture, Result};
use intent_store::Store;

use crate::lease::LinkWrite;

mod leased;
pub use leased::{LeaseLinkOwnership, LINK_LEASE_SCOPE, LINK_LEASE_TTL};

/// The epoch of every [`LocalOwnership`] claim: one process owns every
/// link, so no claim is ever compared against another's epoch.
const LOCAL_EPOCH: u64 = 1;

/// A held claim on one machine link.
#[derive(Debug, Clone, PartialEq, Eq)]
pub struct LinkClaim {
    /// The lease the link binds.
    pub owner: LeaseOwner,
    /// Bumped each time another process takes the link over.
    pub epoch: u64,
    /// The link generation this claim dials with.
    pub generation: NodeCounter,
}

/// The claim no longer holds: another process owns the link now.
#[derive(Debug, Clone, PartialEq, Eq)]
pub struct Fenced {
    /// The machine whose link this process no longer owns.
    pub machine_id: String,
    /// The epoch of the claim that was fenced.
    pub epoch: u64,
}

impl std::fmt::Display for Fenced {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        write!(
            f,
            "this process no longer owns the link to machine {} (epoch {})",
            self.machine_id, self.epoch
        )
    }
}

impl std::error::Error for Fenced {}

impl From<Fenced> for intent_core::Error {
    fn from(fenced: Fenced) -> Self {
        Self::Internal(fenced.to_string())
    }
}

/// Who owns each machine link.
pub trait LinkOwnership: Send + Sync + 'static {
    /// Claim the link of `owner`'s lease before a dial, advancing its link
    /// generation durably.
    ///
    /// # Errors
    ///
    /// When the lease has ended or cannot be written.
    fn claim<'a>(&'a self, owner: &'a LeaseOwner) -> BoxFuture<'a, Result<LinkClaim>>;

    /// Check that `claim` still holds, right before a durable write for its
    /// link.
    ///
    /// # Errors
    ///
    /// [`Fenced`] when another process owns the link now.
    fn fence<'a>(&'a self, claim: &'a LinkClaim) -> BoxFuture<'a, std::result::Result<(), Fenced>>;

    /// Persist the journal ack `seq` for `claim`'s link: only while the claim
    /// holds, checked in the write itself where the owner can.
    ///
    /// # Errors
    ///
    /// [`LinkWrite::Fenced`] when the claim no longer holds; otherwise when
    /// the store write fails.
    fn persist_ack<'a>(
        &'a self,
        store: &'a Store,
        claim: &'a LinkClaim,
        seq: NodeCounter,
    ) -> BoxFuture<'a, std::result::Result<NodeCounter, LinkWrite>> {
        Box::pin(async move {
            self.fence(claim).await.map_err(LinkWrite::Fenced)?;
            Ok(store.advance_node_lease_ack(&claim.owner, seq).await?)
        })
    }

    /// Record `welcome` for `claim`'s link: only while the claim holds,
    /// checked in the write itself where the owner can.
    ///
    /// # Errors
    ///
    /// [`LinkWrite::Fenced`] when the claim no longer holds; otherwise when
    /// the store write fails.
    fn record_welcome<'a>(
        &'a self,
        store: &'a Store,
        claim: &'a LinkClaim,
        welcome: &'a LinkWelcome,
    ) -> BoxFuture<'a, std::result::Result<(), LinkWrite>> {
        Box::pin(async move {
            self.fence(claim).await.map_err(LinkWrite::Fenced)?;
            let platform = &welcome.capabilities.platform;
            store
                .record_node_welcome(
                    &claim.owner,
                    platform.os,
                    platform.arch,
                    welcome.capabilities.capacity.agent_cap,
                    &welcome.capabilities.intentd_version,
                )
                .await
                .map(drop)
                .map_err(LinkWrite::Failed)
        })
    }

    /// Resolves once `claim` is lost (taken over, or its renewal failed past
    /// its expiry): the link bound under it must close. Never resolves for
    /// an owner whose claims cannot be lost.
    fn lost<'a>(&'a self, claim: &'a LinkClaim) -> BoxFuture<'a, ()> {
        let _ = claim;
        Box::pin(std::future::pending())
    }

    /// Give up the link `claim` claimed, so another process can claim it at
    /// once: its connector stopped, went idle or no longer wants the link.
    /// A no-op for an owner that holds nothing, and for a claim that is no
    /// longer the latest of its machine in this process (a replacement
    /// connector claimed since).
    fn release<'a>(&'a self, claim: &'a LinkClaim) -> BoxFuture<'a, ()> {
        let _ = claim;
        Box::pin(async {})
    }

    /// The `hello.headSession` every link of this owner offers.
    fn head_session(&self) -> &str;
}

/// This process's `hello.headSession` (nodeProtocol 14): picked once per
/// start, so a machine drops the agent secrets it held for the previous
/// process.
#[must_use]
pub fn process_head_session() -> &'static str {
    static SESSION: std::sync::OnceLock<String> = std::sync::OnceLock::new();
    SESSION.get_or_init(|| uuid::Uuid::new_v4().to_string())
}

/// The desktop's ownership: this process owns every link it dials, so a
/// claim is the generation advance alone and a claim is never fenced.
pub struct LocalOwnership {
    store: Store,
    head_session: Option<std::sync::Arc<str>>,
}

impl LocalOwnership {
    /// Ownership offering [`process_head_session`].
    #[must_use]
    pub fn new(store: Store) -> Self {
        Self {
            store,
            head_session: None,
        }
    }

    /// Ownership offering `head_session`: one backend's own session, so two
    /// tenants of one process never share one.
    #[must_use]
    pub fn with_head_session(store: Store, head_session: std::sync::Arc<str>) -> Self {
        Self {
            store,
            head_session: Some(head_session),
        }
    }
}

impl LinkOwnership for LocalOwnership {
    fn claim<'a>(&'a self, owner: &'a LeaseOwner) -> BoxFuture<'a, Result<LinkClaim>> {
        Box::pin(async move {
            let generation = self.store.next_node_link_generation(owner).await?;
            Ok(LinkClaim {
                owner: owner.clone(),
                epoch: LOCAL_EPOCH,
                generation,
            })
        })
    }

    fn fence<'a>(
        &'a self,
        _claim: &'a LinkClaim,
    ) -> BoxFuture<'a, std::result::Result<(), Fenced>> {
        Box::pin(async { Ok(()) })
    }

    fn head_session(&self) -> &str {
        self.head_session
            .as_deref()
            .unwrap_or_else(|| process_head_session())
    }
}

#[cfg(test)]
mod tests {
    use intent_core::nodes::{
        LeaseState, NodeArch, NodeCapacity, NodeKind, NodeLease, NodeOs, NodeRecord, NodeState,
    };

    use super::*;

    pub(super) async fn store_with_lease() -> (tempfile::TempDir, Store, LeaseOwner) {
        let dir = tempfile::tempdir().unwrap();
        let store = Store::open(&dir.path().join("intent.db")).await.unwrap();
        let owner = LeaseOwner {
            head_id: "head-1".into(),
            node_id: "machine-1".into(),
            node_identity: "machine-1".into(),
            lease_id: "lease-1".into(),
            incarnation: uuid::Uuid::new_v4().to_string(),
        };
        let record = NodeRecord {
            id: owner.node_id.clone(),
            head_id: owner.head_id.clone(),
            node_identity: owner.node_identity.clone(),
            name: "Machine".into(),
            kind: NodeKind::Static,
            os: NodeOs::Linux,
            arch: NodeArch::X86_64,
            state: NodeState::Offline,
            draining: false,
            capacity: NodeCapacity {
                max_agents: 0,
                reserved_agents: 0,
                memory_budget_bytes: 0,
                used_memory_bytes: 0,
                exclusive_reserved: false,
            },
            version: String::new(),
            endpoint: Some("wss://localhost:7443/node".into()),
            certificate_sha256: Some("a".repeat(64)),
            last_seen_at: None,
        };
        let lease = NodeLease {
            owner: owner.clone(),
            state: LeaseState::Acquiring,
            created_at: intent_core::now_iso(),
            last_seen_at: None,
            ack_seq: NodeCounter(0),
            link_generation: NodeCounter(0),
            release_requested: false,
            fence: None,
        };
        store
            .renew_machine_lease(&record, &lease, NodeCounter(1))
            .await
            .unwrap();
        (dir, store, owner)
    }

    #[tokio::test]
    async fn each_claim_durably_advances_the_link_generation() {
        let (_dir, store, owner) = store_with_lease().await;
        let ownership = LocalOwnership::new(store.clone());
        let first = ownership.claim(&owner).await.unwrap();
        let second = ownership.claim(&owner).await.unwrap();
        assert!(second.generation > first.generation);
        assert_eq!(
            store.get_node_lease(&owner).await.unwrap().link_generation,
            second.generation
        );
        assert_eq!(first.owner, owner);
        assert_eq!(first.epoch, second.epoch);
    }

    #[tokio::test]
    async fn an_unknown_lease_cannot_be_claimed() {
        let (_dir, store, owner) = store_with_lease().await;
        let ownership = LocalOwnership::new(store);
        let other = LeaseOwner {
            lease_id: "lease-unknown".into(),
            ..owner
        };
        assert!(ownership.claim(&other).await.is_err());
    }

    #[tokio::test]
    async fn a_local_claim_is_never_fenced() {
        let (_dir, store, owner) = store_with_lease().await;
        let ownership = LocalOwnership::new(store);
        let stale = ownership.claim(&owner).await.unwrap();
        ownership.claim(&owner).await.unwrap();
        assert_eq!(ownership.fence(&stale).await, Ok(()));
    }

    #[tokio::test]
    async fn every_local_owner_offers_the_process_head_session() {
        let (_dir, store, _owner) = store_with_lease().await;
        let a = LocalOwnership::new(store.clone());
        let b = LocalOwnership::new(store);
        assert_eq!(a.head_session(), b.head_session());
        assert_eq!(a.head_session(), process_head_session());
        assert!(uuid::Uuid::parse_str(a.head_session()).is_ok());
    }

    #[tokio::test]
    async fn an_owner_given_a_session_offers_that_session() {
        let (_dir, store, _owner) = store_with_lease().await;
        let session: std::sync::Arc<str> = uuid::Uuid::new_v4().to_string().into();
        let owned = LocalOwnership::with_head_session(store, session.clone());
        assert_eq!(owned.head_session(), &*session);
        assert_ne!(owned.head_session(), process_head_session());
    }

    fn leased(store: &Store, ttl: std::time::Duration) -> LeaseLinkOwnership {
        LeaseLinkOwnership::new(store.clone(), uuid::Uuid::new_v4().to_string().into(), ttl)
    }

    const TTL: std::time::Duration = std::time::Duration::from_millis(400);

    #[tokio::test]
    async fn one_leased_owner_holds_a_link_until_it_releases_it() {
        let (_dir, store, owner) = store_with_lease().await;
        let (a, b) = (leased(&store, TTL), leased(&store, TTL));
        let first = a.claim(&owner).await.unwrap();
        let redial = a.claim(&owner).await.unwrap();
        assert_eq!(redial.epoch, first.epoch, "a redial keeps the lease");
        let error = b.claim(&owner).await.unwrap_err();
        assert!(
            error.to_string().contains("remote-link-owned-elsewhere"),
            "{error}"
        );
        assert_eq!(a.fence(&first).await, Ok(()));
        // Renewed past its TTL.
        tokio::time::sleep(TTL * 2).await;
        assert!(b.claim(&owner).await.is_err());
        assert_eq!(a.fence(&first).await, Ok(()));

        // The latest claim (the redial) releases it.
        a.release(&redial).await;
        let taken = b
            .claim(&owner)
            .await
            .expect("a release frees the link at once");
        assert!(taken.epoch > first.epoch);
        assert!(taken.generation > first.generation);
        assert!(a.fence(&first).await.is_err());
        a.lost(&first).await;
        assert!(matches!(
            a.persist_ack(&store, &first, NodeCounter(3)).await,
            Err(LinkWrite::Fenced(_))
        ));
        assert_eq!(
            store.get_node_lease(&owner).await.unwrap().ack_seq,
            NodeCounter(0)
        );
        assert_eq!(
            b.persist_ack(&store, &taken, NodeCounter(3)).await.unwrap(),
            NodeCounter(3)
        );
    }

    #[tokio::test]
    async fn a_stalled_leased_owner_loses_the_link_after_its_ttl() {
        let (_dir, store, owner) = store_with_lease().await;
        let (a, b) = (leased(&store, TTL), leased(&store, TTL));
        let first = a.claim(&owner).await.unwrap();
        a.stall_renewals().await;
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
        assert!(taken.epoch > first.epoch);
        assert!(taken.generation > first.generation);
        // Back in touch: the next renewal finds the lease taken.
        a.resume_renewals();
        tokio::time::timeout(intentd_test_support::test_timeout(TTL * 5), a.lost(&first))
            .await
            .expect("a taken lease loses the claim");
        assert!(a.fence(&first).await.is_err());
        let error = a.claim(&owner).await.unwrap_err();
        assert!(
            error.to_string().contains("remote-link-owned-elsewhere"),
            "{error}"
        );
    }

    #[tokio::test]
    async fn only_the_latest_claim_of_a_held_lease_releases_it() {
        // Two connectors of one machine in this process (one replacing the
        // other) share the lease; the older one stopping keeps it.
        let (_dir, store, owner) = store_with_lease().await;
        let (a, b) = (leased(&store, TTL), leased(&store, TTL));
        let old = a.claim(&owner).await.unwrap();
        let new = a.claim(&owner).await.unwrap();
        assert_eq!(new.epoch, old.epoch);
        assert!(new.generation > old.generation);

        a.release(&old).await;
        assert_eq!(a.fence(&new).await, Ok(()), "the newer claim still holds");
        assert!(
            b.claim(&owner).await.is_err(),
            "the older claim's release left the lease held"
        );

        a.release(&new).await;
        let taken = b.claim(&owner).await.expect("the latest claim released it");
        assert!(taken.epoch > new.epoch);
        // A second release of an already released claim does nothing.
        a.release(&new).await;
        assert_eq!(b.fence(&taken).await, Ok(()));
    }

    #[tokio::test]
    async fn a_claim_of_a_superseded_generation_resolves_lost_at_once() {
        let (_dir, store, owner) = store_with_lease().await;
        let a = leased(&store, TTL);
        let first = a.claim(&owner).await.unwrap();
        let other = LinkClaim {
            epoch: first.epoch + 1,
            ..first.clone()
        };
        tokio::time::timeout(intentd_test_support::test_timeout(TTL), a.lost(&other))
            .await
            .expect("a claim this owner does not hold is lost");
        assert!(a.fence(&other).await.is_err());
        assert_eq!(a.fence(&first).await, Ok(()));
    }
}
