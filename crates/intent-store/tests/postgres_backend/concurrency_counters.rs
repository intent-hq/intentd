//! Counters under concurrent increments (spec §6.5.3).
//!
//! Invariant: a node lease's link generation never repeats. N concurrent
//! `next_node_link_generation` calls, from several handles, return N
//! distinct values, and the stored generation ends at the start value plus
//! N (no lost update). The method reads the lease record, increments it in
//! Rust and writes it back, with no row lock and no compare-and-swap, so
//! only the tenant writer lock keeps two increments apart.

use intent_core::nodes::{
    LeaseOwner, LeaseState, NodeArch, NodeCapacity, NodeCounter, NodeKind, NodeLease, NodeOs,
    NodeRecord, NodeState,
};

use crate::concurrency_support::{handles, race, WRITERS};
use crate::harness::parity_test;

const T0: &str = "2026-01-01T00:00:00.000Z";

fn node(tenant: &str) -> NodeRecord {
    NodeRecord {
        id: "node-race".into(),
        head_id: "head-race".into(),
        node_identity: "identity-race".into(),
        name: format!("{tenant} race"),
        kind: NodeKind::Static,
        os: NodeOs::Linux,
        arch: NodeArch::X86_64,
        state: NodeState::Offline,
        draining: false,
        capacity: NodeCapacity {
            max_agents: 4,
            reserved_agents: 0,
            memory_budget_bytes: 1 << 30,
            used_memory_bytes: 0,
            exclusive_reserved: false,
        },
        version: "1.0.0".into(),
        endpoint: Some("wss://node.example:443/node".into()),
        certificate_sha256: Some("a".repeat(64)),
        last_seen_at: None,
    }
}

fn lease(record: &NodeRecord) -> NodeLease {
    NodeLease {
        owner: LeaseOwner {
            head_id: record.head_id.clone(),
            node_id: record.id.clone(),
            node_identity: record.node_identity.clone(),
            lease_id: "lease-race".into(),
            incarnation: "00000000-0000-4000-8000-000000000001".into(),
        },
        state: LeaseState::Acquiring,
        created_at: T0.into(),
        last_seen_at: None,
        ack_seq: NodeCounter(0),
        link_generation: NodeCounter(0),
        release_requested: false,
        fence: None,
    }
}

parity_test!(
    concurrent_increments_never_repeat_a_link_generation,
    |store, tenant, db| {
        let record = node(tenant);
        let initial = lease(&record);
        store.enroll_node(&record, &initial).await.unwrap();
        let all = handles(&store, db, tenant).await;
        let owner = initial.owner.clone();
        let mut issued = race(&all, WRITERS, |_, store| {
            let owner = owner.clone();
            async move { store.next_node_link_generation(&owner).await.unwrap().0 }
        })
        .await;
        issued.sort_unstable();
        let writers = u64::try_from(WRITERS).unwrap();
        assert_eq!(
            issued,
            (1..=writers).collect::<Vec<_>>(),
            "every increment returns its own generation"
        );
        assert_eq!(
            store
                .get_node_lease(&initial.owner)
                .await
                .unwrap()
                .link_generation,
            NodeCounter(writers),
            "no increment is lost"
        );
    }
);
