//! One machine link claimed by two processes of a Postgres tenant
//! (stateless replicas groundwork): each holds its own unclaimed store of the
//! tenant and its own `LeaseLinkOwnership`, as two orchestrators would. One
//! wins the link's lease; the other's claim fails until the winner stops
//! renewing and the lease lapses, then it takes over with a higher epoch and
//! a newer link generation, and the old holder's link writes are fenced.
//!
//! Needs `--features postgres` and `INTENT_STORE_TEST_PG_URL` (the
//! `store-postgres` CI job sets both); skipped without the URL unless
//! `INTENT_STORE_REQUIRE_PG=1`.
#![cfg(feature = "postgres")]

use std::sync::Arc;
use std::time::Duration;

use intent_core::nodes::{LeaseOwner, NodeCounter};
use intent_node_connections::lease::{new_owner, record_new_lease, HeadLease, LinkWrite};
use intent_node_connections::machine::MachineTarget;
use intent_node_connections::{Fenced, LeaseLinkOwnership, LinkOwnership, LINK_LEASE_SCOPE};
use intent_store::postgres::{self, PgPool, PostgresOptions, TenantId};
use intent_store::{OrchestratorOnly, Store};

const TTL: Duration = Duration::from_secs(2);

async fn pool() -> Option<PgPool> {
    let Ok(url) = std::env::var("INTENT_STORE_TEST_PG_URL") else {
        assert!(
            std::env::var("INTENT_STORE_REQUIRE_PG").as_deref() != Ok("1"),
            "INTENT_STORE_REQUIRE_PG=1 but INTENT_STORE_TEST_PG_URL is unset"
        );
        eprintln!("skipping: INTENT_STORE_TEST_PG_URL is unset");
        return None;
    };
    let pool = postgres::connect(&url, 8).await.expect("connect");
    postgres::migrate(&pool).await.expect("migrate");
    Some(pool)
}

/// An unclaimed store of `tenant` with its own process-local state, as if
/// opened by the daemon `process`.
async fn open(pool: &PgPool, tenant: &str, process: &str) -> Store {
    Store::open_postgres_as_process(
        pool.clone(),
        TenantId::new(tenant).unwrap(),
        PostgresOptions::default(),
        process,
        OrchestratorOnly::for_tests(),
    )
    .await
    .expect("open the tenant store")
}

/// A live node lease on machine `m1`, recorded through `store`.
async fn leased_machine(store: &Store) -> LeaseOwner {
    let target = MachineTarget::from_parts(
        "m1",
        "Machine",
        vec!["127.0.0.1".into()],
        7443,
        None,
        &"a".repeat(64),
        &"t".repeat(32),
        None,
    )
    .unwrap();
    let owner = new_owner(store, "m1").await.unwrap();
    record_new_lease(store, &target, &owner, NodeCounter(1))
        .await
        .unwrap();
    owner
}

fn ownership(store: &Store) -> Arc<LeaseLinkOwnership> {
    Arc::new(LeaseLinkOwnership::new(
        store.clone(),
        uuid::Uuid::new_v4().to_string().into(),
        TTL,
    ))
}

#[tokio::test(flavor = "multi_thread", worker_threads = 4)]
async fn two_processes_claim_one_link_and_the_survivor_takes_it_over_after_expiry() {
    let Some(pool) = pool().await else { return };
    let tenant = format!(
        "link-owner-{}",
        &uuid::Uuid::new_v4().simple().to_string()[..12]
    );
    let (store_a, store_b) = (
        open(&pool, &tenant, "process-a").await,
        open(&pool, &tenant, "process-b").await,
    );
    let owner = leased_machine(&store_a).await;
    let (a, b) = (ownership(&store_a), ownership(&store_b));

    // Both dial at once: exactly one wins the link.
    let (claimed_a, claimed_b) = tokio::join!(a.claim(&owner), b.claim(&owner));
    let (winner, loser, won, lost_claim) = match (claimed_a, claimed_b) {
        (Ok(won), Err(error)) => (
            (a.clone(), store_a.clone()),
            (b.clone(), store_b.clone()),
            won,
            error,
        ),
        (Err(error), Ok(won)) => (
            (b.clone(), store_b.clone()),
            (a.clone(), store_a.clone()),
            won,
            error,
        ),
        other => panic!("exactly one process must win the link: {other:?}"),
    };
    assert!(
        lost_claim
            .to_string()
            .contains("remote-link-owned-elsewhere"),
        "{lost_claim}"
    );
    let (winner, winner_store) = winner;
    let (loser, loser_store) = loser;
    assert_eq!(winner.fence(&won).await, Ok(()));
    assert_eq!(
        loser.fence(&won).await,
        Err(Fenced {
            machine_id: "m1".into(),
            epoch: won.epoch
        }),
        "the loser holds no lease, so the winner's claim does not hold for it"
    );
    assert_eq!(
        winner_store
            .get_node_lease(&owner)
            .await
            .unwrap()
            .link_generation,
        won.generation,
        "the claim advanced the shared generation durably"
    );
    winner
        .persist_ack(&winner_store, &won, NodeCounter(4))
        .await
        .expect("the holder persists its ack");

    // A while past its TTL the winner still holds: its keeper renews.
    // timing-guard: negative path: time must pass beyond the TTL to prove renewal keeps the lease
    tokio::time::sleep(TTL * 2).await;
    assert!(loser.claim(&owner).await.is_err(), "renewed: still owned");
    assert_eq!(winner.fence(&won).await, Ok(()));

    // The winner stalls (cut off from the database): once its lease lapses
    // the loser takes the link over.
    winner.stall_renewals().await;
    let lost = winner.lost(&won);
    let deadline = tokio::time::Instant::now() + intentd_test_support::test_timeout(TTL * 10);
    let taken = loop {
        match loser.claim(&owner).await {
            Ok(claim) => break claim,
            Err(_) if tokio::time::Instant::now() < deadline => {
                // timing-guard: poll interval while waiting for the lapsed lease to be taken over
                tokio::time::sleep(TTL / 4).await;
            }
            Err(error) => panic!("the lapsed link was never taken over: {error}"),
        }
    };
    assert!(taken.epoch > won.epoch, "a new holder has a higher token");
    assert!(
        taken.generation > won.generation,
        "a new holder dials a newer link generation"
    );
    // Back in touch: the stalled holder's next renewal finds the lease
    // taken.
    winner.resume_renewals();
    tokio::time::timeout(intentd_test_support::test_timeout(TTL * 5), lost)
        .await
        .expect("the stalled holder's claim is lost, so its link closes");

    // The stale holder's link writes are fenced, and write nothing.
    assert!(winner.fence(&won).await.is_err());
    assert!(matches!(
        winner
            .persist_ack(&winner_store, &won, NodeCounter(9))
            .await,
        Err(LinkWrite::Fenced(_))
    ));
    assert_eq!(
        loser_store.get_node_lease(&owner).await.unwrap().ack_seq,
        NodeCounter(4)
    );
    loser
        .persist_ack(&loser_store, &taken, NodeCounter(6))
        .await
        .expect("the new holder persists its ack");
    assert_eq!(
        loser_store
            .get_node_lease(&owner)
            .await
            .unwrap()
            .link_generation,
        taken.generation
    );

    // A clean stop frees the link at once.
    loser.release(&owner).await;
    assert!(loser_store
        .acquire_lease(LINK_LEASE_SCOPE, "m1", TTL)
        .await
        .unwrap()
        .is_some());
}

/// Through the connector's `HeadLease`: a stalled holder's ack after a
/// takeover is fenced and leaves the ack where it was.
#[tokio::test(flavor = "multi_thread", worker_threads = 4)]
async fn a_head_lease_of_a_stale_holder_writes_no_ack_after_a_takeover() {
    let Some(pool) = pool().await else { return };
    let tenant = format!(
        "link-ack-{}",
        &uuid::Uuid::new_v4().simple().to_string()[..12]
    );
    let (store_a, store_b) = (
        open(&pool, &tenant, "process-a").await,
        open(&pool, &tenant, "process-b").await,
    );
    let owner = leased_machine(&store_a).await;
    let (a, b) = (ownership(&store_a), ownership(&store_b));
    let lease_a = HeadLease::open(store_a.clone(), "m1", a.clone())
        .await
        .unwrap();
    let hello = lease_a.next_hello(NodeCounter(0)).await.unwrap();
    lease_a
        .persist_ack(NodeCounter(2), Some(hello.binding.generation))
        .await
        .unwrap();

    a.stall_renewals().await;
    let lease_b = HeadLease::open(store_b.clone(), "m1", b.clone())
        .await
        .unwrap();
    let deadline = tokio::time::Instant::now() + intentd_test_support::test_timeout(TTL * 10);
    let hello_b = loop {
        match lease_b.next_hello(NodeCounter(2)).await {
            Ok(hello) => break hello,
            Err(_) if tokio::time::Instant::now() < deadline => {
                // timing-guard: poll interval while waiting for the lapsed lease to be taken over
                tokio::time::sleep(TTL / 4).await;
            }
            Err(error) => panic!("never taken over: {error}"),
        }
    };
    assert!(hello_b.binding.generation > hello.binding.generation);
    a.resume_renewals();
    assert!(matches!(
        lease_a
            .persist_ack(NodeCounter(50), Some(hello.binding.generation))
            .await,
        Err(LinkWrite::Fenced(_))
    ));
    assert_eq!(lease_b.acked().await.unwrap(), NodeCounter(2));
    assert_eq!(owner.node_id, "m1");
}

/// The node-link writes guard the lease in their own transaction: a holder
/// that kept its `Lease` past a takeover (it skipped every earlier check)
/// still advances nothing.
#[tokio::test(flavor = "multi_thread", worker_threads = 4)]
async fn node_link_writes_under_a_lost_lease_write_nothing() {
    let Some(pool) = pool().await else { return };
    let tenant = format!(
        "link-guard-{}",
        &uuid::Uuid::new_v4().simple().to_string()[..12]
    );
    let (store_a, store_b) = (
        open(&pool, &tenant, "process-a").await,
        open(&pool, &tenant, "process-b").await,
    );
    let owner = leased_machine(&store_a).await;
    let short = Duration::from_millis(300);
    let stale = store_a
        .acquire_lease(LINK_LEASE_SCOPE, "m1", short)
        .await
        .unwrap()
        .unwrap();
    let generation = store_a
        .next_node_link_generation_under_lease(&owner, &stale)
        .await
        .unwrap();
    // timing-guard: the lease must expire by the database clock before a fresh acquire can win
    tokio::time::sleep(short * 2).await;
    let fresh = store_b
        .acquire_lease(LINK_LEASE_SCOPE, "m1", TTL)
        .await
        .unwrap()
        .expect("the lapsed lease is free");
    let lost =
        |result: intent_core::Result<_>| matches!(result, Err(intent_core::Error::LeaseLost(_)));
    assert!(lost(
        store_a
            .next_node_link_generation_under_lease(&owner, &stale)
            .await
            .map(drop)
    ));
    assert!(lost(
        store_a
            .advance_node_lease_ack_under_lease(&owner, NodeCounter(7), &stale)
            .await
            .map(drop)
    ));
    assert!(lost(
        store_a
            .record_node_welcome_under_lease(
                &owner,
                intent_core::nodes::NodeOs::Linux,
                intent_core::nodes::NodeArch::X86_64,
                2,
                "9.9.9",
                &stale,
            )
            .await
            .map(drop)
    ));
    let lease = store_b.get_node_lease(&owner).await.unwrap();
    assert_eq!(lease.link_generation, generation, "no generation advanced");
    assert_eq!(lease.ack_seq, NodeCounter(0), "no ack persisted");
    let next = store_b
        .next_node_link_generation_under_lease(&owner, &fresh)
        .await
        .unwrap();
    assert!(next > generation);
}
