//! Multi-step lifecycle gates on a Postgres tenant shared by two processes:
//! each `Services` runs on its own store of the tenant, as two daemons
//! would. An archive fence or a retirement gate held in one blocks the same
//! one in the other until it is released; other targets stay free.
//!
//! Needs `--features postgres` and `INTENT_STORE_TEST_PG_URL` (the
//! `store-postgres` CI job sets both); skipped without the URL unless
//! `INTENT_STORE_REQUIRE_PG=1`.
#![cfg(feature = "postgres")]

use std::time::Duration;

use intent_core::{AgentId, WorkspaceId};
use intent_services::Services;
use intent_store::postgres::{self, PgPool, PostgresOptions, TenantId};
use intent_store::{OrchestratorOnly, Store};
use intentd_test_support::test_timeout;

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

/// Waits until waiters in this process found (`scope`, `key`) held at
/// least `n` times: the waiter is really waiting on the other process.
async fn wait_contended(scope: &str, key: &str, n: u64) {
    tokio::time::timeout(test_timeout(Duration::from_secs(30)), async {
        while intent_store::contended_lease_attempts_for_tests(scope, key) < n {
            tokio::task::yield_now().await;
        }
    })
    .await
    .expect("the waiter tries the held lease");
}

#[tokio::test(flavor = "multi_thread", worker_threads = 4)]
async fn an_archive_fence_in_one_process_blocks_the_same_fence_in_another() {
    let Some(pool) = pool().await else { return };
    let tenant = format!(
        "lifecycle-archive-{}",
        &uuid::Uuid::new_v4().simple().to_string()[..12]
    );
    let a = Services::new_isolated(open(&pool, &tenant, "process-a").await);
    let b = Services::new_isolated(open(&pool, &tenant, "process-b").await);
    let ws = WorkspaceId("ws-archive".into());

    let held = a
        .feature_archive_fence()
        .acquire(&ws)
        .await
        .expect("A takes the fence");
    let waiter = {
        let (b, ws) = (b.clone(), ws.clone());
        tokio::spawn(async move {
            let guard = b.feature_archive_fence().acquire(&ws).await;
            guard.map(drop)
        })
    };
    // Another workspace's fence is other work.
    let other = b
        .feature_archive_fence()
        .acquire(&WorkspaceId("ws-other".into()))
        .await
        .expect("another workspace is free");
    wait_contended(intent_store::ARCHIVE_FENCE_SCOPE, "ws-archive", 2).await;
    assert!(
        !waiter.is_finished(),
        "B's archive waits while A holds the fence"
    );
    drop(other);
    drop(held);
    tokio::time::timeout(test_timeout(Duration::from_secs(30)), waiter)
        .await
        .expect("B takes the fence once A released it")
        .unwrap()
        .expect("B's fence");
}

#[tokio::test(flavor = "multi_thread", worker_threads = 4)]
async fn a_retirement_in_one_process_blocks_the_same_retirement_in_another() {
    let Some(pool) = pool().await else { return };
    let tenant = format!(
        "lifecycle-retire-{}",
        &uuid::Uuid::new_v4().simple().to_string()[..12]
    );
    let a = Services::new_isolated(open(&pool, &tenant, "process-a").await);
    let b = Services::new_isolated(open(&pool, &tenant, "process-b").await);
    let agent = AgentId("agent-retiring".into());

    let gate = a.feature_agent_wake().retirement_gate(&a, &agent);
    let held = gate.lock().await.expect("A takes the retirement gate");
    let waiter = {
        let (b, agent) = (b.clone(), agent.clone());
        tokio::spawn(async move {
            let gate = b.feature_agent_wake().retirement_gate(&b, &agent);
            gate.lock().await.map(drop)
        })
    };
    let other = b
        .feature_agent_wake()
        .retirement_gate(&b, &AgentId("agent-other".into()));
    let other = other.lock().await.expect("another agent is free");
    wait_contended(intent_store::AGENT_RETIREMENT_SCOPE, "agent-retiring", 2).await;
    assert!(
        !waiter.is_finished(),
        "B's retirement waits while A holds the gate"
    );
    drop(other);
    drop(held);
    tokio::time::timeout(test_timeout(Duration::from_secs(30)), waiter)
        .await
        .expect("B takes the gate once A released it")
        .unwrap()
        .expect("B's gate");
}
