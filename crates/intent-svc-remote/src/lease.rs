//! Head's durable side of a machine binding: the exposed machine's
//! registration, the live lease of its current executor credential, the
//! link generation (advanced before every dial) and the journal ack
//! (persisted before it is sent).
//!
//! The head id is this daemon's id ([`Store::daemon_id`]), the backend id
//! the machine minted the credential for. Each credential gets its own
//! lease, so a re-added machine starts a fresh journal.

use intent_core::node_link::{HomeRef, LeaseBinding, LinkHello, LinkWelcome, NODE_LINK_PROTOCOLS};
use intent_core::nodes::{
    LeaseOwner, LeaseState, NodeArch, NodeCapacity, NodeCounter, NodeKind, NodeLease, NodeOs,
    NodeRecord, NodeState,
};
use std::sync::{Arc, Mutex};

use intent_core::{now_iso, AgentId, Error, Result};
use intent_node_connections::{Fenced, LinkClaim, LinkOwnership, LocalOwnership};
use intent_store::Store;

use crate::machine::MachineTarget;

/// This backend's ownership of its machine links: the desktop's, where this
/// process owns every link, unless a test replaced it.
pub(crate) fn ownership(
    store: &Store,
    tuning: &crate::connector::Tuning,
) -> Arc<dyn LinkOwnership> {
    #[cfg(test)]
    if let Some(ownership) = &tuning.ownership.0 {
        return ownership.clone();
    }
    #[cfg(not(test))]
    let _ = tuning;
    Arc::new(LocalOwnership::new(store.clone()))
}

/// Why a durable link write did not happen.
#[derive(Debug)]
pub(crate) enum LinkWrite {
    /// Another process owns the link, or a newer dial of this one
    /// superseded it: the link must close.
    Fenced(Fenced),
    Failed(Error),
}

impl std::fmt::Display for LinkWrite {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        match self {
            Self::Fenced(fenced) => fenced.fmt(f),
            Self::Failed(error) => error.fmt(f),
        }
    }
}

impl From<Error> for LinkWrite {
    fn from(error: Error) -> Self {
        Self::Failed(error)
    }
}

/// The hello of a probe: binds `owner`'s binding at `generation` with no
/// journal acked yet (a new credential's journal is empty).
pub(crate) fn probe_hello(
    owner: &LeaseOwner,
    generation: NodeCounter,
    head_session: &str,
) -> LinkHello {
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
        lease_agents: Vec::new(),
        head_session: Some(head_session.to_owned()),
        lease_homes: None,
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
        // Informational (the connector dials from the target). A machine
        // with no direct host is reached over tailcat, whose pipe ends at
        // the machine's own listener: record that route's endpoint.
        endpoint: Some(target.direct_routes().first().map_or_else(
            || format!("wss://localhost:{}/node", target.port),
            |route| format!("{}/node", route.endpoint.url),
        )),
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
    ownership: Arc<dyn LinkOwnership>,
    /// The claim of the latest dial, shared by every clone; `None` before
    /// the first. A link's writes pass only while its generation is this
    /// claim's: a superseded link writes nothing.
    claim: Arc<Mutex<Option<LinkClaim>>>,
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
    pub(crate) async fn open(
        store: Store,
        machine_id: &str,
        ownership: Arc<dyn LinkOwnership>,
    ) -> Result<Self> {
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
            ownership,
            claim: Arc::default(),
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

    /// Check the claim before a durable write for the link bound at
    /// `generation` (`None`: a write between links). Before the first claim
    /// there is nothing to check.
    async fn fence(&self, generation: Option<NodeCounter>) -> std::result::Result<(), Fenced> {
        let claim = crate::lock(&self.claim).clone();
        let Some(claim) = claim else {
            return Ok(());
        };
        if generation.is_some_and(|generation| generation != claim.generation) {
            return Err(Fenced {
                machine_id: claim.owner.node_id,
                epoch: claim.epoch,
            });
        }
        self.ownership.fence(&claim).await
    }

    /// The persisted journal ack.
    pub(crate) async fn acked(&self) -> Result<NodeCounter> {
        Ok(self.store.get_node_lease(&self.owner).await?.ack_seq)
    }

    /// The agents whose checkouts the machine keeps (nodeProtocol 9): those
    /// with a pending final stop, and those recorded here whose session has
    /// not ended. The machine sweeps every other checkout of this backend's
    /// agents that is not running, so an ended agent's checkout left behind
    /// (an ack below protocol 9, a final stop past its bound, or one from
    /// before final stops) goes at the next bind. A session that cannot be
    /// read for another reason than its absence is kept.
    pub(crate) async fn lease_agents(&self) -> Result<Vec<AgentId>> {
        let mut lease_agents: Vec<AgentId> = self
            .store
            .list_final_stops(&self.owner.lease_id)
            .await?
            .into_iter()
            .map(|stop| stop.agent_id)
            .collect();
        for record in self.store.list_remote_agents(&self.owner.lease_id).await? {
            if lease_agents.contains(&record.agent_id) {
                continue;
            }
            let ended = match self
                .store
                .get_agent_session_with_transcript(&record.agent_id)
                .await
            {
                Ok(session) => crate::final_stop::session_ended(&session),
                Err(Error::NotFound(_)) => true,
                Err(_) => false,
            };
            if !ended {
                lease_agents.push(record.agent_id);
            }
        }
        Ok(lease_agents)
    }

    /// Claim the link (durably advancing the generation), then build the
    /// hello offering it.
    /// The hello acks only what is persisted: the node deletes the records
    /// it acks, so offering `ingested` before it is saved could lose records
    /// a restarted head still needs. Head first tries to persist `ingested`;
    /// if that fails it offers the older durable ack and its in-memory
    /// sequence skips the replayed duplicates.
    pub(crate) async fn next_hello(&self, ingested: NodeCounter) -> Result<LinkHello> {
        let acked = match self.persist_ack(ingested, None).await {
            Ok(acked) => acked,
            Err(error) => {
                tracing::warn!(%error, seq = ingested.0, "remote node: could not persist the journal ack; offering the saved one");
                self.acked().await?
            }
        };
        let claim = self.ownership.claim(&self.owner).await?;
        let generation = claim.generation;
        *crate::lock(&self.claim) = Some(claim);
        let lease_agents = self.lease_agents().await?;
        let lease_homes = self.lease_homes().await?;
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
            lease_agents,
            head_session: Some(self.ownership.head_session().to_owned()),
            lease_homes: Some(lease_homes),
        })
    }

    /// The homes the machine keeps (nodeProtocol 19): every workspace's
    /// current home on it, and every home with a pending stop (its stop
    /// removes it, after its last push). The machine sweeps every other
    /// home checkout of this backend.
    pub(crate) async fn lease_homes(&self) -> Result<Vec<HomeRef>> {
        let mut homes: Vec<HomeRef> = self
            .store
            .homes_on_machine(&self.owner.node_id)
            .await?
            .into_iter()
            .map(|(workspace_id, home)| HomeRef {
                workspace_id,
                home_id: home.home_id,
            })
            .collect();
        for home_move in self
            .store
            .home_moves_to_machine(&self.owner.node_id)
            .await?
        {
            if !homes
                .iter()
                .any(|home| home.home_id == home_move.target.home_id)
            {
                homes.push(HomeRef {
                    workspace_id: home_move.workspace_id,
                    home_id: home_move.target.home_id,
                });
            }
        }
        for stop in self.store.list_home_stops(&self.owner.node_id).await? {
            if !homes.iter().any(|home| home.home_id == stop.home_id) {
                homes.push(stop.frame().home());
            }
        }
        Ok(homes)
    }

    /// Record what the node reported for the link bound at `generation`.
    pub(crate) async fn record_welcome(
        &self,
        welcome: &LinkWelcome,
        generation: NodeCounter,
    ) -> std::result::Result<(), LinkWrite> {
        self.fence(Some(generation))
            .await
            .map_err(LinkWrite::Fenced)?;
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
            .map_err(LinkWrite::Failed)
    }

    /// Persist that every record up to `seq` is ingested, for the link bound
    /// at `generation` (`None`: between links).
    pub(crate) async fn persist_ack(
        &self,
        seq: NodeCounter,
        generation: Option<NodeCounter>,
    ) -> std::result::Result<NodeCounter, LinkWrite> {
        #[cfg(test)]
        if self
            .fail_ack_writes
            .load(std::sync::atomic::Ordering::SeqCst)
        {
            return Err(Error::Internal("injected ack write failure".into()).into());
        }
        self.fence(generation).await.map_err(LinkWrite::Fenced)?;
        Ok(self.store.advance_node_lease_ack(&self.owner, seq).await?)
    }
}
