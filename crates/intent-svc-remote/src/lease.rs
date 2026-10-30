//! Head's durable side of the node binding: the node's registration, its
//! live lease, the link generation (advanced before every dial) and the
//! journal ack (persisted before it is sent).

use intent_core::node_link::{LeaseBinding, LinkHello, LinkWelcome, NODE_LINK_PROTOCOLS};
use intent_core::nodes::{
    LeaseOwner, LeaseState, NodeArch, NodeCapacity, NodeCounter, NodeKind, NodeLease, NodeOs,
    NodeRecord, NodeState,
};
use intent_core::{now_iso, Error, Result};
use intent_store::Store;

use crate::config::HeadNodeConfig;

/// The configured node's durable lease, as head sees it.
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

    /// Find this head's live lease on the configured node, registering the
    /// node and a new lease on first use. The head id is this installation's
    /// existing one, or a new random id when it has registered no node yet.
    pub(crate) async fn open(store: Store, config: &HeadNodeConfig) -> Result<Self> {
        if let Some(node) = store.find_execution_node(&config.node_id).await? {
            let lease = store
                .list_owned_node_leases(&node.head_id)
                .await?
                .into_iter()
                .find(|lease| lease.owner.node_id == node.id && !lease.state.is_terminal())
                .ok_or_else(|| {
                    Error::InvalidParams(format!(
                        "remote-node-lease-ended: the lease on node {} has ended; give the node a new nodeId to bind it again",
                        node.id
                    ))
                })?;
            return Ok(Self {
                store,
                owner: lease.owner,
                #[cfg(test)]
                fail_ack_writes: std::sync::Arc::default(),
            });
        }
        let head_id = match store.node_head_id().await? {
            Some(head_id) => head_id,
            None => uuid::Uuid::new_v4().to_string(),
        };
        // The platform, capacity and version are unknown until the node's
        // first welcome; the node stays offline until then.
        let record = NodeRecord {
            id: config.node_id.clone(),
            head_id: head_id.clone(),
            node_identity: config.node_id.clone(),
            name: config.name.clone(),
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
            endpoint: Some(format!("{}/node", config.endpoint.url)),
            certificate_sha256: Some(config.certificate_sha256.clone()),
            last_seen_at: None,
        };
        let owner = LeaseOwner {
            head_id,
            node_id: config.node_id.clone(),
            node_identity: config.node_id.clone(),
            lease_id: uuid::Uuid::new_v4().to_string(),
            incarnation: uuid::Uuid::new_v4().to_string(),
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
        store.enroll_node(&record, &lease).await?;
        Ok(Self {
            store,
            owner,
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
