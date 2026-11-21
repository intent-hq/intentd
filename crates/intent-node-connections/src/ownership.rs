//! Link ownership: which process may drive a machine's link, and the fence
//! every durable link write passes first.
//!
//! A dial claims the link ([`LinkOwnership::claim`]): the claim carries the
//! lease's new link generation, which the machine enforces (it accepts only
//! the newest one). Before each durable write for that link (the journal
//! ack, the welcome) the holder checks its claim ([`LinkOwnership::fence`]).
//! A fenced holder writes nothing and closes the link.

use intent_core::nodes::{LeaseOwner, NodeCounter};
use intent_core::{BoxFuture, Result};
use intent_store::Store;

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
}

impl LocalOwnership {
    #[must_use]
    pub fn new(store: Store) -> Self {
        Self { store }
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
        process_head_session()
    }
}

#[cfg(test)]
mod tests {
    use intent_core::nodes::{
        LeaseState, NodeArch, NodeCapacity, NodeKind, NodeLease, NodeOs, NodeRecord, NodeState,
    };

    use super::*;

    async fn store_with_lease() -> (tempfile::TempDir, Store, LeaseOwner) {
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
}
