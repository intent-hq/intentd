//! One agent's send queue on a Postgres tenant shared by two processes
//! (stateless replicas groundwork): each `Services` runs on its own unclaimed
//! store of the tenant, as two daemons would. Entries enqueued on either are
//! seen by both in order, and draining from both delivers each entry once.
//!
//! Needs `--features postgres` and `INTENT_STORE_TEST_PG_URL` (the
//! `store-postgres` CI job sets both); skipped without the URL unless
//! `INTENT_STORE_REQUIRE_PG=1`.
#![cfg(feature = "postgres")]

use std::collections::HashSet;

use intent_core::{AgentId, AgentSession, AgentStatus, MessageOrigin, WorkspaceId};
use intent_services::Services;
use intent_store::postgres::{self, PgPool, PostgresOptions, TenantId};
use intent_store::{OrchestratorOnly, Store};

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

async fn queue_contents(services: &Services, agent: &AgentId) -> Vec<String> {
    let queue = services
        .agent_get_queue_op_for_test(agent)
        .await
        .expect("agent.getQueue");
    queue["queue"]
        .as_array()
        .unwrap_or_else(|| panic!("no queue in {queue}"))
        .iter()
        .map(|entry| entry["content"].as_str().unwrap().to_owned())
        .collect()
}

#[tokio::test(flavor = "multi_thread", worker_threads = 4)]
async fn two_processes_share_one_agent_queue_and_drain_each_entry_once() {
    let Some(pool) = pool().await else { return };
    let tenant = format!(
        "shared-queue-{}",
        &uuid::Uuid::new_v4().simple().to_string()[..12]
    );
    let (store_a, store_b) = (
        open(&pool, &tenant, "process-a").await,
        open(&pool, &tenant, "process-b").await,
    );
    let agent = AgentId("agent-shared".into());
    let now = intent_core::now_iso();
    store_a
        .insert_agent_session(&AgentSession {
            id: agent.clone(),
            workspace_id: WorkspaceId("__chief__".into()),
            name: "shared".into(),
            status: AgentStatus::Idle,
            created_at: now.clone(),
            updated_at: now,
            ..AgentSession::default()
        })
        .await
        .expect("seed the agent");
    let a = Services::new_isolated(store_a);
    let b = Services::new_isolated(store_b);

    // Enqueue alternately on both, each persisted by its write-through.
    let mut ids = Vec::new();
    for (services, text) in [(&a, "a1"), (&b, "b1"), (&a, "a2"), (&b, "b2")] {
        let (entry, _) = services.enqueue_message_for_test(
            &agent,
            text.into(),
            None,
            None,
            None,
            false,
            MessageOrigin::User,
        );
        services.persist_queue_snapshot_for_test(&agent).await;
        ids.push(entry.id);
    }
    for services in [&a, &b] {
        assert_eq!(
            queue_contents(services, &agent).await,
            ["a1", "b1", "a2", "b2"],
            "each process sees the whole queue in enqueue order"
        );
    }

    // Drain from both at once until both queues are empty: every entry is
    // delivered exactly once, whichever process pops it.
    let mut delivered = Vec::new();
    let mut lost = 0;
    loop {
        let (from_a, from_b) = tokio::join!(
            a.claim_next_queued_message_for_test(&agent),
            b.claim_next_queued_message_for_test(&agent),
        );
        if from_a.is_none() && from_b.is_none() {
            break;
        }
        for (id, claims_lost) in [from_a, from_b].into_iter().flatten() {
            if claims_lost.contains(&id) {
                lost += 1;
            } else {
                delivered.push(id);
            }
        }
    }
    let unique: HashSet<&String> = delivered.iter().collect();
    assert_eq!(
        unique.len(),
        delivered.len(),
        "an entry was delivered twice: {delivered:?}"
    );
    assert_eq!(
        unique,
        ids.iter().collect::<HashSet<_>>(),
        "every entry is delivered"
    );
    eprintln!("delivered {delivered:?}; {lost} pops lost to the other process");
    for services in [&a, &b] {
        assert!(queue_contents(services, &agent).await.is_empty());
    }
}
