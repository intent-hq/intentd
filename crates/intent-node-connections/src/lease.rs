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

use crate::lock;
use crate::machine::MachineTarget;
use crate::{Fenced, LinkClaim, LinkOwnership, LocalOwnership};
use intent_core::{now_iso, AgentId, AgentSession, AgentStatus, Error, Result};
use intent_store::Store;

/// Why a durable link write did not happen.
#[derive(Debug)]
pub enum LinkWrite {
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
#[must_use]
pub fn probe_hello(owner: &LeaseOwner, generation: NodeCounter, head_session: &str) -> LinkHello {
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
///
/// # Errors
///
/// When the daemon id cannot be read.
pub async fn new_owner(store: &Store, machine_id: &str) -> Result<LeaseOwner> {
    Ok(LeaseOwner {
        head_id: store.daemon_id().await?,
        node_id: machine_id.to_owned(),
        node_identity: machine_id.to_owned(),
        lease_id: uuid::Uuid::new_v4().to_string(),
        incarnation: uuid::Uuid::new_v4().to_string(),
    })
}

/// Whether `session` has ended for good or for now (deleted, completed or
/// retired): its agent must not run on a machine.
#[must_use]
pub fn session_ended(session: &AgentSession) -> bool {
    session.retired_at.is_some()
        || matches!(
            session.status,
            AgentStatus::Deleted | AgentStatus::Completed
        )
}

/// An exposed machine's durable lease, as head sees it.
#[derive(Clone)]
pub struct HeadLease {
    store: Store,
    owner: LeaseOwner,
    ownership: Arc<dyn LinkOwnership>,
    /// The claim of the latest dial, shared by every clone; `None` before
    /// the first. A link's writes pass only while its generation is this
    /// claim's: a superseded link writes nothing.
    claim: Arc<Mutex<Option<LinkClaim>>>,
    /// Test seam: fail every ack write while set.
    #[cfg(any(test, feature = "test-support"))]
    fail_ack_writes: std::sync::Arc<std::sync::atomic::AtomicBool>,
}

impl HeadLease {
    #[must_use]
    pub fn store(&self) -> &Store {
        &self.store
    }

    /// This backend's live lease on machine `machine_id`.
    ///
    /// # Errors
    ///
    /// `remote-node-lease-ended` when the machine has no live lease (it was
    /// removed, or its credential replaced).
    pub async fn open(
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
            #[cfg(any(test, feature = "test-support"))]
            fail_ack_writes: std::sync::Arc::default(),
        })
    }

    /// Test seam: fail ack writes while `flag` is set.
    #[cfg(any(test, feature = "test-support"))]
    #[must_use]
    pub fn failing_ack_writes_when(
        mut self,
        flag: std::sync::Arc<std::sync::atomic::AtomicBool>,
    ) -> Self {
        self.fail_ack_writes = flag;
        self
    }

    #[must_use]
    pub fn owner(&self) -> &LeaseOwner {
        &self.owner
    }

    /// The latest dial's claim, for a durable write for the link bound at
    /// `generation` (`None`: a write between links); `None` before the
    /// first claim, when there is nothing to check.
    ///
    /// # Errors
    ///
    /// [`Fenced`] when a newer dial superseded that link.
    fn current_claim(
        &self,
        generation: Option<NodeCounter>,
    ) -> std::result::Result<Option<LinkClaim>, Fenced> {
        let claim = lock(&self.claim).clone();
        let Some(claim) = claim else {
            return Ok(None);
        };
        if generation.is_some_and(|generation| generation != claim.generation) {
            return Err(Fenced {
                machine_id: claim.owner.node_id,
                epoch: claim.epoch,
            });
        }
        Ok(Some(claim))
    }

    /// Resolves once the claim the link bound at `generation` dialed with
    /// is lost; at once when a newer dial superseded it.
    pub fn lost(
        &self,
        generation: NodeCounter,
    ) -> impl std::future::Future<Output = ()> + Send + 'static {
        let claim = lock(&self.claim).clone();
        let ownership = self.ownership.clone();
        async move {
            match claim {
                Some(claim) if claim.generation == generation => ownership.lost(&claim).await,
                Some(_) => {}
                None => std::future::pending().await,
            }
        }
    }

    /// Give the link up (the connector stopped, went idle or no longer
    /// wants it): the next dial claims it again. A no-op before the first
    /// claim, and once a newer claim of the machine replaced this one.
    pub async fn release(&self) {
        let claim = lock(&self.claim).clone();
        if let Some(claim) = claim {
            self.ownership.release(&claim).await;
        }
    }

    /// The persisted journal ack.
    ///
    /// # Errors
    ///
    /// When the lease cannot be read.
    pub async fn acked(&self) -> Result<NodeCounter> {
        Ok(self.store.get_node_lease(&self.owner).await?.ack_seq)
    }

    /// The agents whose checkouts the machine keeps (nodeProtocol 9): those
    /// with a pending final stop, and those recorded here whose session has
    /// not ended. The machine sweeps every other checkout of this backend's
    /// agents that is not running, so an ended agent's checkout left behind
    /// (an ack below protocol 9, a final stop past its bound, or one from
    /// before final stops) goes at the next bind. A session that cannot be
    /// read for another reason than its absence is kept.
    ///
    /// # Errors
    ///
    /// When the lease's agent records or final stops cannot be read.
    pub async fn lease_agents(&self) -> Result<Vec<AgentId>> {
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
                Ok(session) => session_ended(&session),
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
    /// hello offering it. The claim comes first so the ack below is written
    /// under it: an ack written before it would be fenced by the previous
    /// claim (released when the link went idle) or, before the first claim,
    /// written with no ownership check.
    /// The hello acks only what is persisted: the node deletes the records
    /// it acks, so offering `ingested` before it is saved could lose records
    /// a restarted head still needs. Head first tries to persist `ingested`;
    /// if that fails it offers the older durable ack and its in-memory
    /// sequence skips the replayed duplicates.
    ///
    /// # Errors
    ///
    /// When the claim or the lease's agents and homes cannot be read.
    pub async fn next_hello(&self, ingested: NodeCounter) -> Result<LinkHello> {
        let claim = self.ownership.claim(&self.owner).await?;
        let generation = claim.generation;
        *lock(&self.claim) = Some(claim);
        let acked = match self.persist_ack(ingested, None).await {
            Ok(acked) => acked,
            Err(error) => {
                tracing::warn!(%error, seq = ingested.0, "remote node: could not persist the journal ack; offering the saved one");
                self.acked().await?
            }
        };
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
    ///
    /// # Errors
    ///
    /// When the machine's homes, moves or stops cannot be read.
    pub async fn lease_homes(&self) -> Result<Vec<HomeRef>> {
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
    ///
    /// # Errors
    ///
    /// [`LinkWrite::Fenced`] when the link is fenced; otherwise when the store
    /// write fails.
    pub async fn record_welcome(
        &self,
        welcome: &LinkWelcome,
        generation: NodeCounter,
    ) -> std::result::Result<(), LinkWrite> {
        let claim = self
            .current_claim(Some(generation))
            .map_err(LinkWrite::Fenced)?;
        match claim {
            Some(claim) => {
                self.ownership
                    .record_welcome(&self.store, &claim, welcome)
                    .await
            }
            None => self
                .store
                .record_node_welcome(
                    &self.owner,
                    welcome.capabilities.platform.os,
                    welcome.capabilities.platform.arch,
                    welcome.capabilities.capacity.agent_cap,
                    &welcome.capabilities.intentd_version,
                )
                .await
                .map(drop)
                .map_err(LinkWrite::Failed),
        }
    }

    /// Persist that every record up to `seq` is ingested, for the link bound
    /// at `generation` (`None`: between links).
    ///
    /// # Errors
    ///
    /// [`LinkWrite::Fenced`] when the link is fenced; otherwise when the store
    /// write fails.
    pub async fn persist_ack(
        &self,
        seq: NodeCounter,
        generation: Option<NodeCounter>,
    ) -> std::result::Result<NodeCounter, LinkWrite> {
        #[cfg(any(test, feature = "test-support"))]
        if self
            .fail_ack_writes
            .load(std::sync::atomic::Ordering::SeqCst)
        {
            return Err(Error::Internal("injected ack write failure".into()).into());
        }
        match self.current_claim(generation).map_err(LinkWrite::Fenced)? {
            Some(claim) => self.ownership.persist_ack(&self.store, &claim, seq).await,
            None => Ok(self.store.advance_node_lease_ack(&self.owner, seq).await?),
        }
    }
}

/// This backend's ownership of its machine links: [`Tuning::ownership`]
/// when set (the orchestrator's leases, or a test's), else the desktop's,
/// where this process owns every link.
///
/// [`Tuning::ownership`]: crate::connector::Tuning::ownership
#[must_use]
pub fn ownership(store: &Store, tuning: &crate::connector::Tuning) -> Arc<dyn LinkOwnership> {
    if let Some(ownership) = &tuning.ownership.0 {
        return ownership.clone();
    }
    match &tuning.head_session {
        Some(session) => Arc::new(LocalOwnership::with_head_session(
            store.clone(),
            session.clone(),
        )),
        None => Arc::new(LocalOwnership::new(store.clone())),
    }
}

/// Durably start `owner`'s lease on `target` after a probe bound it at
/// `generation`, ending any earlier lease on the machine.
///
/// # Errors
///
/// When the store refuses the record or the lease.
pub async fn record_new_lease(
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
