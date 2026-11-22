//! Head's durable side of a machine binding that stays with the domain: the
//! lease a newly added credential starts, and the link ownership this
//! backend dials with. The lease, its journal ack and link generation live
//! in `intent-node-connections`.

use std::sync::Arc;

use intent_core::nodes::{
    LeaseOwner, LeaseState, NodeArch, NodeCapacity, NodeCounter, NodeKind, NodeLease, NodeOs,
    NodeRecord, NodeState,
};
use intent_core::{now_iso, Result};
use intent_node_connections::{LinkOwnership, LocalOwnership};
use intent_store::Store;

pub(crate) use intent_node_connections::lease::{new_owner, probe_hello, HeadLease, LinkWrite};

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
