//! The tenant writer lock under writers that only the database can order:
//! [`PROCESSES`] stores for one tenant, each opened as its own process (its
//! own writer slot and other process-local gates), each running
//! [`INCREMENTS`] read-then-write increments at once. Shared by the Postgres
//! backend suite and the Neki suite.
//!
//! The increment is `next_node_link_generation`: it reads the lease, adds
//! one in Rust and writes it back, with no row lock and no compare-and-swap.
//! Only the tenant writer lock keeps two increments apart, so if it does not
//! exclude, two writers read the same generation and one update is lost.

use std::sync::Arc;

use intent_core::nodes::{
    LeaseOwner, LeaseState, NodeArch, NodeCapacity, NodeCounter, NodeKind, NodeLease, NodeOs,
    NodeRecord, NodeState,
};
use intent_store::postgres::{PgPool, PostgresOptions, TenantId};
use intent_store::Store;
use tokio::sync::Barrier;

/// Simulated daemons, each with its own process-local gates.
pub const PROCESSES: usize = 8;
/// Increments each of them runs, one after another.
pub const INCREMENTS: u64 = 5;

fn node() -> NodeRecord {
    NodeRecord {
        id: "node-writer-race".into(),
        head_id: "head-writer-race".into(),
        node_identity: "identity-writer-race".into(),
        name: "writer race".into(),
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
            lease_id: "lease-writer-race".into(),
            incarnation: "00000000-0000-4000-8000-000000000001".into(),
        },
        state: LeaseState::Acquiring,
        created_at: "2026-01-01T00:00:00.000Z".into(),
        last_seen_at: None,
        ack_seq: NodeCounter(0),
        link_generation: NodeCounter(0),
        release_requested: false,
        fence: None,
    }
}

/// What a race left: every generation the increments returned, sorted, and
/// the generation stored at the end.
pub struct RaceOutcome {
    pub issued: Vec<u64>,
    pub stored: u64,
}

impl RaceOutcome {
    /// `Err` with what went wrong unless every increment got its own
    /// generation and none was lost.
    pub fn check(&self) -> Result<(), String> {
        let total = u64::try_from(PROCESSES).expect("small") * INCREMENTS;
        let expected: Vec<u64> = (1..=total).collect();
        if self.issued != expected {
            let mut repeated = self.issued.clone();
            repeated.dedup();
            return Err(format!(
                "increments repeated a generation: {} issued, {} distinct, \
                 expected 1..={total}: {:?}",
                self.issued.len(),
                repeated.len(),
                self.issued
            ));
        }
        if self.stored != total {
            return Err(format!(
                "lost update: stored generation {} after {total} increments",
                self.stored
            ));
        }
        Ok(())
    }
}

/// Run the race for `tenant` (a tenant no other test uses) on `pool`, which
/// should allow [`PROCESSES`] connections at once.
///
/// # Errors
/// A store error, as text.
pub async fn race_increments(
    pool: &PgPool,
    tenant: &str,
    options: PostgresOptions,
) -> Result<RaceOutcome, String> {
    let tenant_id = TenantId::new(tenant).map_err(|e| e.to_string())?;
    let mut stores = Vec::with_capacity(PROCESSES);
    for i in 0..PROCESSES {
        let store = Store::open_postgres_as_process(
            pool.clone(),
            tenant_id.clone(),
            options,
            &format!("writer-race-{i}"),
        )
        .await
        .map_err(|e| format!("open process {i}: {e}"))?;
        stores.push(store);
    }
    let record = node();
    let initial = lease(&record);
    stores[0]
        .enroll_node(&record, &initial)
        .await
        .map_err(|e| format!("enroll: {e}"))?;

    let start = Arc::new(Barrier::new(PROCESSES));
    let mut set = tokio::task::JoinSet::new();
    for store in &stores {
        let store = store.clone();
        let owner = initial.owner.clone();
        let start = start.clone();
        set.spawn(async move {
            start.wait().await;
            let mut issued = Vec::new();
            for _ in 0..INCREMENTS {
                let generation = store
                    .next_node_link_generation(&owner)
                    .await
                    .map_err(|e| format!("increment: {e:?}"))?;
                issued.push(generation.0);
            }
            Ok::<_, String>(issued)
        });
    }
    let mut issued = Vec::new();
    while let Some(joined) = set.join_next().await {
        issued.extend(joined.map_err(|e| format!("writer task: {e}"))??);
    }
    issued.sort_unstable();
    let final_generation = stores[0]
        .get_node_lease(&initial.owner)
        .await
        .map_err(|e| format!("read the lease: {e}"))?
        .link_generation
        .0;
    for store in stores {
        store.close().await;
    }
    Ok(RaceOutcome {
        issued,
        stored: final_generation,
    })
}
