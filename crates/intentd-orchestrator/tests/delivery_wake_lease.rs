//! A completion wake is written under its delivery claim (a lease): two
//! processes share one Postgres tenant, as two daemons would, and both hold
//! the same completion watch. A holder whose claim lapsed between its
//! duplicate probe and the wake write writes nothing (here: queues nothing
//! for a busy parent); the process that took the claim over wakes the
//! parent exactly once.
//!
//! Needs `--features postgres` and `INTENT_STORE_TEST_PG_URL` (the
//! `store-postgres` CI job sets both); skipped without the URL unless
//! `INTENT_STORE_REQUIRE_PG=1`.
#![cfg(feature = "postgres")]

use std::sync::Arc;
use std::time::Duration;

use intent_core::{ActorType, AgentId, AgentSession, AgentStatus, Event, EventActor, WorkspaceId};
use intent_services::events::EventBus;
use intent_services::{AgentManager, BusEventSink, CompletionClassifyPark, Services};
use intent_store::postgres::{self, PgPool, PostgresOptions, TenantId};
use intent_store::{OrchestratorOnly, Store, COMPLETION_WATCH_LEASE_SCOPE};

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

fn session(id: &AgentId, name: &str, status: AgentStatus) -> AgentSession {
    let now = intent_core::now_iso();
    AgentSession {
        id: id.clone(),
        workspace_id: WorkspaceId::chief(),
        name: name.into(),
        status,
        created_at: now.clone(),
        updated_at: now,
        ..AgentSession::default()
    }
}

fn idle_event(child: &AgentId) -> Event {
    Event {
        id: uuid::Uuid::new_v4().to_string(),
        workspace_id: WorkspaceId::chief(),
        timestamp: intent_core::now_iso(),
        event_type: "agent:idle".into(),
        actor: EventActor {
            actor_type: ActorType::Agent,
            id: Some(child.0.clone()),
            ..Default::default()
        },
        session_id: Some(child.0.clone()),
        correlation_id: None,
        parent_event_id: None,
        metadata: None,
        data: serde_json::json!({ "agentId": child.0 }),
    }
}

async fn wakes(store: &Store, parent: &AgentId) -> usize {
    store
        .get_agent_session_with_transcript(parent)
        .await
        .expect("parent transcript")
        .messages
        .len()
}

fn wait() -> Duration {
    intentd_test_support::test_timeout(Duration::from_secs(30))
}

#[tokio::test(flavor = "multi_thread", worker_threads = 4)]
async fn a_holder_whose_claim_lapsed_before_the_send_writes_no_wake() {
    let Some(pool) = pool().await else { return };
    let tenant = format!(
        "wake-lease-{}",
        &uuid::Uuid::new_v4().simple().to_string()[..12]
    );
    let (store_a, store_b) = (
        open(&pool, &tenant, "process-a").await,
        open(&pool, &tenant, "process-b").await,
    );
    let (parent, child) = (AgentId("parent".into()), AgentId("child".into()));
    store_a
        .insert_agent_session(&session(&parent, "parent", AgentStatus::RuntimeIdle))
        .await
        .expect("seed the parent");
    // Busy while process B loads the watch, so B's startup reconcile finds
    // nothing to deliver; it completes below.
    store_a
        .insert_agent_session(&session(&child, "child", AgentStatus::Active))
        .await
        .expect("seed the child");

    let park = Arc::new(CompletionClassifyPark::default());
    let a =
        Services::new_isolated(store_a.clone()).with_completion_send_park_for_test(park.clone());
    let b = Services::new_isolated(store_b.clone());
    // A delivers through an agent manager whose parent is busy (its turn
    // slot held), so A's wake goes to the parent's queue, where the stable
    // id of B's transcript row cannot stop it.
    let manager = Arc::new(AgentManager::new(
        a.clone(),
        Arc::new(BusEventSink::new(EventBus::new(store_a.clone()))),
        4,
    ));
    a.attach_agent_manager(&manager);
    let _busy = manager
        .try_begin_turn_for_test(&parent, &WorkspaceId::chief())
        .await
        .expect("the parent's turn slot");
    let watch = a
        .register_completion_watch_durable_for_test(
            &WorkspaceId::chief(),
            parent.clone(),
            child.clone(),
            None,
        )
        .await
        .expect("A registers the watch");
    b.heal_completion_watches_on_startup()
        .await
        .expect("B loads the stored watch");
    assert_eq!(b.watch_ids_for_child_for_test(&child), [watch.clone()]);
    store_a
        .set_agent_session_status(
            &WorkspaceId::chief(),
            &child,
            AgentStatus::RuntimeIdle,
            false,
            &intent_core::now_iso(),
            None,
        )
        .await
        .expect("the child completes");

    // A claims the watch and parks after its held check, before the send.
    let event = idle_event(&child);
    let delivering = tokio::spawn({
        let a = a.clone();
        let event = event.clone();
        async move { a.handle_completion_event_for_test(&event).await }
    });
    tokio::time::timeout(wait(), park.entered.notified())
        .await
        .expect("A reaches the send");

    // A's claim lapses (its renewals stalled), and B takes it over.
    let lapsed = sqlx::query(
        "UPDATE lease SET expires_at = clock_timestamp() - interval '1 second' \
         WHERE tenant_id = $1 AND scope = $2 AND lease_key = $3 AND lease_id IS NOT NULL",
    )
    .bind(&tenant)
    .bind(COMPLETION_WATCH_LEASE_SCOPE)
    .bind(&watch)
    .execute(&pool)
    .await
    .expect("expire A's claim")
    .rows_affected();
    assert_eq!(lapsed, 1, "A holds the claim while parked");
    tokio::time::timeout(wait(), b.handle_completion_event_for_test(&event))
        .await
        .expect("B delivers");
    assert_eq!(wakes(&store_b, &parent).await, 1, "B wakes the parent");
    assert!(
        b.watch_ids_for_child_for_test(&child).is_empty(),
        "B retired the watch"
    );

    // A resumes with a lapsed claim: its guarded write fails and writes
    // nothing.
    park.release.notify_one();
    tokio::time::timeout(wait(), delivering)
        .await
        .expect("A's pass ends")
        .expect("A's pass");
    assert_eq!(
        wakes(&store_a, &parent).await,
        1,
        "the stale holder wrote no second wake"
    );
    assert!(
        a.queue_snapshot_for_test(&parent).is_empty(),
        "the stale holder queued no second wake"
    );
    // A schedules a retry under a new claim; whenever it runs, its probe
    // finds B's wake (no pass can add one: that is what is asserted above).
}
