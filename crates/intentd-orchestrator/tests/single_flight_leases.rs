//! Single-flight work across the processes of one Postgres tenant (stateless
//! replicas groundwork): two `Services`, each on its own unclaimed store of
//! the tenant as two daemons would run, never refresh one workspace's PR or
//! poll one PR monitor at once, and a process whose refresh lease was taken
//! over writes nothing late.
//!
//! Needs `--features postgres` and `INTENT_STORE_TEST_PG_URL` (the
//! `store-postgres` CI job sets both); skipped without the URL unless
//! `INTENT_STORE_REQUIRE_PG=1`.
#![cfg(feature = "postgres")]

use std::sync::Arc;

use intent_core::{AgentId, AgentSession, AgentStatus, Workspace, WorkspaceId};
use intent_services::test_support::stub_forge::{GetPrPark, StubForge};
use intent_services::Services;
use intent_store::postgres::{self, PgPool, PostgresOptions, TenantId};
use intent_store::{OrchestratorOnly, Store};
use tokio::sync::Barrier;

async fn pool() -> Option<PgPool> {
    let Ok(url) = std::env::var("INTENT_STORE_TEST_PG_URL") else {
        assert!(
            std::env::var("INTENT_STORE_REQUIRE_PG").as_deref() != Ok("1"),
            "INTENT_STORE_REQUIRE_PG=1 but INTENT_STORE_TEST_PG_URL is unset"
        );
        eprintln!("skipping: INTENT_STORE_TEST_PG_URL is unset");
        return None;
    };
    let pool = postgres::connect(&url, 16).await.expect("connect");
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

fn tenant(prefix: &str) -> String {
    format!(
        "{prefix}-{}",
        &uuid::Uuid::new_v4().simple().to_string()[..12]
    )
}

/// A workspace of repository `o/r` linked to PR 42 (the stub forge's PR).
async fn seed_workspace(store: &Store) -> WorkspaceId {
    let id = WorkspaceId::new();
    let now = intent_core::now_iso();
    store
        .insert_workspace(&Workspace {
            id: id.clone(),
            title: "single flight".into(),
            branch: "main".into(),
            created_at: now.clone(),
            updated_at: now,
            repository_owner: Some("o".into()),
            repository_name: Some("r".into()),
            pr_number: Some(42),
            ..Workspace::default()
        })
        .await
        .expect("seed the workspace");
    id
}

/// A stub forge whose first `get_pr` parks until released.
fn parking_forge() -> (Arc<StubForge>, Arc<GetPrPark>) {
    let park = Arc::new(GetPrPark::default());
    let forge = Arc::new(StubForge {
        get_pr_park: Some(Arc::clone(&park)),
        ..StubForge::default()
    });
    (forge, park)
}

fn get_pr_calls(forge: &StubForge) -> usize {
    forge.seen_get_pr.lock().unwrap().len()
}

/// Both processes start one workspace's automatic PR refresh together:
/// one takes the refresh lease and calls the forge; the other skips with
/// no forge call while it runs.
#[tokio::test(flavor = "multi_thread", worker_threads = 4)]
async fn two_processes_never_refresh_one_workspace_at_once() {
    let Some(pool) = pool().await else { return };
    let tenant = tenant("single-flight-refresh");
    let store_a = open(&pool, &tenant, "process-a").await;
    let store_b = open(&pool, &tenant, "process-b").await;
    let ws = seed_workspace(&store_a).await;
    let (forge_a, park_a) = parking_forge();
    let (forge_b, park_b) = parking_forge();
    let a = Services::new_isolated(store_a).with_source_control(forge_a.clone());
    let b = Services::new_isolated(store_b).with_source_control(forge_b.clone());

    let start = Arc::new(Barrier::new(2));
    let refresh = |svc: Services| {
        let ws = ws.clone();
        let start = Arc::clone(&start);
        tokio::spawn(async move {
            start.wait().await;
            svc.refresh_workspace_pr_automatically_for_test(&ws).await
        })
    };
    let (run_a, run_b) = (refresh(a), refresh(b));
    // Whoever took the lease is parked in its forge read.
    let a_won = tokio::select! {
        () = park_a.entered.notified() => true,
        () = park_b.entered.notified() => false,
    };
    let (winner, loser, winner_park, loser_forge) = if a_won {
        (run_a, run_b, park_a, &forge_b)
    } else {
        (run_b, run_a, park_b, &forge_a)
    };
    assert_eq!(
        loser.await.unwrap().unwrap(),
        "skipped",
        "the other process skips while the refresh runs"
    );
    assert_eq!(get_pr_calls(loser_forge), 0, "and never calls the forge");
    winner_park.release.notify_one();
    assert_ne!(winner.await.unwrap().unwrap(), "skipped");
    assert_eq!(get_pr_calls(&forge_a) + get_pr_calls(&forge_b), 1);
}

/// A refresh whose lease lapsed and was taken by another process writes
/// nothing when its forge read returns: its write guards the lease, fails
/// with `LeaseLost`, and the workspace keeps what the new holder wrote.
#[tokio::test(flavor = "multi_thread", worker_threads = 4)]
async fn a_refresh_that_lost_its_lease_writes_nothing_late() {
    let Some(pool) = pool().await else { return };
    let tenant = tenant("single-flight-late");
    let store_a = open(&pool, &tenant, "process-a").await;
    let store_b = open(&pool, &tenant, "process-b").await;
    let ws = seed_workspace(&store_a).await;
    // Process A would record the PR as merged; process B sees it open.
    let park = Arc::new(GetPrPark::default());
    let forge_a = Arc::new(StubForge {
        get_pr_park: Some(Arc::clone(&park)),
        merged_linked: true,
        ..StubForge::default()
    });
    let a = Services::new_isolated(store_a).with_source_control(forge_a);
    let b =
        Services::new_isolated(store_b.clone()).with_source_control(Arc::new(StubForge::default()));

    let stale = tokio::spawn({
        let ws = ws.clone();
        async move { a.refresh_workspace_pr_automatically_for_test(&ws).await }
    });
    park.entered.notified().await;
    // A's holder stops renewing long enough for the lease to lapse.
    sqlx::query(
        "UPDATE lease SET expires_at = clock_timestamp() \
         WHERE tenant_id = $1 AND scope = 'pr-refresh' AND lease_key = $2",
    )
    .bind(&tenant)
    .bind(ws.as_str())
    .execute(&pool)
    .await
    .unwrap();
    let fresh = b.refresh_workspace_pr_automatically_for_test(&ws).await;
    assert_ne!(fresh.unwrap(), "skipped", "B takes the lapsed lease");
    let after_b = store_b.get_workspace(&ws).await.unwrap();

    park.release.notify_one();
    let late = stale.await.unwrap();
    assert!(
        matches!(late, Err(intent_core::Error::LeaseLost(_))),
        "the stale holder's write fails: {late:?}"
    );
    let now = store_b.get_workspace(&ws).await.unwrap();
    assert_eq!(now.pr_status, after_b.pr_status, "nothing landed late");
    assert_eq!(now.updated_at, after_b.updated_at);
    assert_ne!(now.pr_status, Some(intent_core::PullRequestStatus::Merged));
}

/// Both processes sweep the tenant's PR monitors together: the monitor is
/// polled by one of them only; the other leaves it while the poll runs.
#[tokio::test(flavor = "multi_thread", worker_threads = 4)]
async fn two_processes_never_poll_one_monitor_at_once() {
    let Some(pool) = pool().await else { return };
    let tenant = tenant("single-flight-monitor");
    let store_a = open(&pool, &tenant, "process-a").await;
    let store_b = open(&pool, &tenant, "process-b").await;
    let ws = seed_workspace(&store_a).await;
    let owner = AgentId::from("agent-single-flight");
    let now = intent_core::now_iso();
    store_a
        .insert_agent_session(&AgentSession {
            id: owner.clone(),
            workspace_id: ws.clone(),
            name: "Owner".into(),
            name_explicitly_set: true,
            status: AgentStatus::Active,
            created_at: now.clone(),
            updated_at: now,
            ..AgentSession::default()
        })
        .await
        .expect("seed the owner");
    Services::new_isolated(store_a.clone())
        .with_source_control(Arc::new(StubForge::default()))
        .pr_monitor_register(&ws, &owner, "o", "r", 42)
        .await
        .expect("register the monitor");

    let (forge_a, park_a) = parking_forge();
    let (forge_b, park_b) = parking_forge();
    let a = Services::new_isolated(store_a).with_source_control(forge_a.clone());
    let b = Services::new_isolated(store_b).with_source_control(forge_b.clone());
    let start = Arc::new(Barrier::new(2));
    let sweep = |svc: Services| {
        let start = Arc::clone(&start);
        tokio::spawn(async move {
            start.wait().await;
            svc.poll_pr_monitors().await;
        })
    };
    let (run_a, run_b) = (sweep(a), sweep(b));
    let a_won = tokio::select! {
        () = park_a.entered.notified() => true,
        () = park_b.entered.notified() => false,
    };
    let (winner, loser, winner_park, loser_forge) = if a_won {
        (run_a, run_b, park_a, &forge_b)
    } else {
        (run_b, run_a, park_b, &forge_a)
    };
    loser.await.unwrap();
    assert_eq!(
        get_pr_calls(loser_forge),
        0,
        "the other process's sweep leaves the monitor being polled"
    );
    winner_park.release.notify_one();
    winner.await.unwrap();
    assert_eq!(get_pr_calls(&forge_a) + get_pr_calls(&forge_b), 1);
}
