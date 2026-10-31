//! Head's durable side of a machine binding: the exposed machine's
//! registration, the live lease of its current executor credential, the
//! link generation (advanced before every dial) and the journal ack
//! (persisted before it is sent).
//!
//! The head id is this daemon's id ([`Store::daemon_id`]), the backend id
//! the machine minted the credential for. Each credential gets its own
//! lease, so a re-added machine starts a fresh journal.

use intent_core::node_link::{LeaseBinding, LinkHello, LinkWelcome, NODE_LINK_PROTOCOLS};
use intent_core::nodes::{
    LeaseOwner, LeaseState, NodeArch, NodeCapacity, NodeCounter, NodeKind, NodeLease, NodeOs,
    NodeRecord, NodeState,
};
use intent_core::{now_iso, Error, Result};
use intent_store::Store;

use crate::machine::MachineTarget;

/// The hello of a probe: binds `owner`'s binding at `generation` with no
/// journal acked yet (a new credential's journal is empty).
pub(crate) fn probe_hello(owner: &LeaseOwner, generation: NodeCounter) -> LinkHello {
    LinkHello {
        protocol: NODE_LINK_PROTOCOLS,
        intentd_version: env!("CARGO_PKG_VERSION").to_owned(),
        binding: LeaseBinding {
            head_id: owner.head_id.clone(),
            node_id: owner.node_id.clone(),
            lease_id: owner.lease_id.clone(),
            generation,
        },
        acked_seq: NodeCounter(0),
    }
}

/// A new lease owner on machine `machine_id` for this backend.
pub(crate) async fn new_owner(store: &Store, machine_id: &str) -> Result<LeaseOwner> {
    Ok(LeaseOwner {
        head_id: store.daemon_id().await?,
        node_id: machine_id.to_owned(),
        node_identity: machine_id.to_owned(),
        lease_id: uuid::Uuid::new_v4().to_string(),
        incarnation: uuid::Uuid::new_v4().to_string(),
    })
}

/// Durably start `owner`'s lease on `target` after a probe bound it at
/// `generation`, ending any earlier lease on the machine.
pub(crate) async fn record_new_lease(
    store: &Store,
    target: &MachineTarget,
    owner: &LeaseOwner,
    generation: NodeCounter,
) -> Result<()> {
    // The platform, capacity and version are filled in by each welcome.
    let record = NodeRecord {
        id: target.id.clone(),
        head_id: owner.head_id.clone(),
        node_identity: owner.node_identity.clone(),
        name: target.name.clone(),
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
        endpoint: target
            .direct_routes()
            .first()
            .map(|route| format!("{}/node", route.endpoint.url)),
        certificate_sha256: Some(target.certificate_sha256.clone()),
        last_seen_at: None,
    };
    let lease = NodeLease {
        owner: owner.clone(),
        state: LeaseState::Acquiring,
        created_at: now_iso(),
        last_seen_at: None,
        ack_seq: NodeCounter(0),
        link_generation: NodeCounter(0),
        release_requested: false,
        fence: None,
    };
    store.renew_machine_lease(&record, &lease, generation).await
}

/// An exposed machine's durable lease, as head sees it.
#[derive(Clone)]
pub(crate) struct HeadLease {
    store: Store,
    owner: LeaseOwner,
    /// Test seam: fail every ack write while set.
    #[cfg(test)]
    fail_ack_writes: std::sync::Arc<std::sync::atomic::AtomicBool>,
}

impl HeadLease {
    pub(crate) fn store(&self) -> &Store {
        &self.store
    }

    /// This backend's live lease on machine `machine_id`.
    ///
    /// # Errors
    ///
    /// `remote-node-lease-ended` when the machine has no live lease (it was
    /// removed, or its credential replaced).
    pub(crate) async fn open(store: Store, machine_id: &str) -> Result<Self> {
        let head_id = store.daemon_id().await?;
        let lease = store
            .list_owned_node_leases(&head_id)
            .await?
            .into_iter()
            .find(|lease| lease.owner.node_id == machine_id && !lease.state.is_terminal())
            .ok_or_else(|| {
                Error::InvalidParams(format!(
                    "remote-node-lease-ended: this backend holds no live lease on machine {machine_id}; add it again"
                ))
            })?;
        Ok(Self {
            store,
            owner: lease.owner,
            #[cfg(test)]
            fail_ack_writes: std::sync::Arc::default(),
        })
    }

    /// Test seam: fail ack writes while `flag` is set.
    #[cfg(test)]
    pub(crate) fn failing_ack_writes_when(
        mut self,
        flag: std::sync::Arc<std::sync::atomic::AtomicBool>,
    ) -> Self {
        self.fail_ack_writes = flag;
        self
    }

    pub(crate) fn owner(&self) -> &LeaseOwner {
        &self.owner
    }

    /// The persisted journal ack.
    pub(crate) async fn acked(&self) -> Result<NodeCounter> {
        Ok(self.store.get_node_lease(&self.owner).await?.ack_seq)
    }

    /// Durably advance the generation, then build the hello offering it.
    /// The hello acks only what is persisted: the node deletes the records
    /// it acks, so offering `ingested` before it is saved could lose records
    /// a restarted head still needs. Head first tries to persist `ingested`;
    /// if that fails it offers the older durable ack and its in-memory
    /// sequence skips the replayed duplicates.
    pub(crate) async fn next_hello(&self, ingested: NodeCounter) -> Result<LinkHello> {
        let acked = match self.persist_ack(ingested).await {
            Ok(acked) => acked,
            Err(error) => {
                tracing::warn!(%error, seq = ingested.0, "remote node: could not persist the journal ack; offering the saved one");
                self.acked().await?
            }
        };
        let generation = self.store.next_node_link_generation(&self.owner).await?;
        Ok(LinkHello {
            protocol: NODE_LINK_PROTOCOLS,
            intentd_version: env!("CARGO_PKG_VERSION").to_owned(),
            binding: LeaseBinding {
                head_id: self.owner.head_id.clone(),
                node_id: self.owner.node_id.clone(),
                lease_id: self.owner.lease_id.clone(),
                generation,
            },
            acked_seq: acked,
        })
    }

    /// Record what the node reported for a new link.
    pub(crate) async fn record_welcome(&self, welcome: &LinkWelcome) -> Result<()> {
        self.store
            .record_node_welcome(
                &self.owner,
                welcome.capabilities.platform.os,
                welcome.capabilities.platform.arch,
                welcome.capabilities.capacity.agent_cap,
                &welcome.capabilities.intentd_version,
            )
            .await
            .map(drop)
    }

    /// Persist that every record up to `seq` is ingested.
    pub(crate) async fn persist_ack(&self, seq: NodeCounter) -> Result<NodeCounter> {
        #[cfg(test)]
        if self
            .fail_ack_writes
            .load(std::sync::atomic::Ordering::SeqCst)
        {
            return Err(Error::Internal("injected ack write failure".into()));
        }
        self.store.advance_node_lease_ack(&self.owner, seq).await
    }
}
