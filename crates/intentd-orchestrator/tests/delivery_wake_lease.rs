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

async fn stored_queue_len(store: &Store, agent: &AgentId) -> usize {
    store
        .load_all_agent_queues()
        .await
        .expect("stored queues")
        .iter()
        .filter(|row| row.agent_id == *agent)
        .count()
}

/// Busy parent: the stale holder's wake would go to the parent's queue,
/// which B's transcript row cannot stop by its id.
#[tokio::test(flavor = "multi_thread", worker_threads = 4)]
async fn a_holder_whose_claim_lapsed_before_the_send_queues_no_wake() {
    race(true).await;
}

/// Idle parent: the stale holder's direct transcript write fails with
/// `LeaseLost` and does not fall back to the queue (before, the duplicate id
/// failed the write and the wake was queued instead).
#[tokio::test(flavor = "multi_thread", worker_threads = 4)]
async fn a_holder_whose_claim_lapsed_before_the_send_writes_no_wake() {
    race(false).await;
}

async fn race(busy: bool) {
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
    // A delivers through an agent manager, as a daemon does; for a busy
    // parent its turn slot is held, so A's wake goes to the parent's queue.
    let manager = Arc::new(AgentManager::new(
        a.clone(),
        Arc::new(BusEventSink::new(EventBus::new(store_a.clone()))),
        4,
    ));
    a.attach_agent_manager(&manager);
    let _busy = if busy {
        Some(
            manager
                .try_begin_turn_for_test(&parent, &WorkspaceId::chief())
                .await
                .expect("the parent's turn slot"),
        )
    } else {
        None
    };
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
    assert_eq!(
        b.watch_ids_for_child_for_test(&child),
        std::slice::from_ref(&watch)
    );
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
    assert_eq!(
        stored_queue_len(&store_b, &parent).await,
        0,
        "nothing durable landed in the parent's queue either"
    );
    // A schedules a retry under a new claim; whenever it runs, its probe
    // finds B's wake (no pass can add one: that is what is asserted above).
    // That retry parks on the send seam for good (the release permit is
    // spent), which ends with the test.
}

/// A wake queued under a delivery claim stays undrainable until its guarded
/// queue write decides: paused between the in-memory insert and that write,
/// the entry is listed but no drain takes it (the worker's ready drain skips
/// it, "send now" refuses it). The claim then lapses and B delivers, so the
/// guarded write fails and the entry leaves memory: nothing was delivered or
/// stored from it.
#[tokio::test(flavor = "multi_thread", worker_threads = 4)]
async fn a_wake_queued_under_a_lost_claim_is_never_drained() {
    let Some(pool) = pool().await else { return };
    let tenant = format!(
        "wake-pending-{}",
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
    store_a
        .insert_agent_session(&session(&child, "child", AgentStatus::Active))
        .await
        .expect("seed the child");
    let park = Arc::new(CompletionClassifyPark::default());
    let a =
        Services::new_isolated(store_a.clone()).with_guarded_enqueue_park_for_test(park.clone());
    let b = Services::new_isolated(store_b.clone());
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

    let event = idle_event(&child);
    let delivering = tokio::spawn({
        let a = a.clone();
        let event = event.clone();
        async move { a.handle_completion_event_for_test(&event).await }
    });
    tokio::time::timeout(wait(), park.entered.notified())
        .await
        .expect("A queued the wake and waits on its guarded write");
    let wake_id = format!("completion-wake:{watch}");
    assert_eq!(
        a.queue_snapshot_for_test(&parent).len(),
        1,
        "the pending wake is listed"
    );
    assert!(
        a.dequeue_message_draining_for_test(&parent).is_none(),
        "the ready drain skips a wake whose write is pending"
    );
    assert!(
        matches!(
            a.agent_send_queued_message_now_op_for_test(parent.clone(), wake_id.clone())
                .await,
            Err(intent_core::Error::StoreBusy(_))
        ),
        "send now refuses a wake whose write is pending"
    );

    // A's claim lapses and B delivers; A's guarded write then fails.
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
    park.release.notify_one();
    tokio::time::timeout(wait(), delivering)
        .await
        .expect("A's pass ends")
        .expect("A's pass");
    assert_eq!(wakes(&store_a, &parent).await, 1, "only B's wake");
    assert!(
        a.queue_snapshot_for_test(&parent).is_empty(),
        "A's pending wake left memory"
    );
    assert_eq!(
        stored_queue_len(&store_b, &parent).await,
        0,
        "and was never stored"
    );
    // A's retry under a new claim parks on the spent seam; it ends with the
    // test.
}

/// Strict ask registration inserts the pair's watch under the pair's
/// registration lease: a registration paused past that lease while another
/// process took the pair over and inserted its watch writes nothing, then
/// registers again and reuses the winner's watch. One watch for the pair.
#[tokio::test(flavor = "multi_thread", worker_threads = 4)]
async fn a_registration_that_lost_its_pair_lease_reuses_the_winners_watch() {
    let Some(pool) = pool().await else { return };
    let tenant = format!(
        "watch-reg-{}",
        &uuid::Uuid::new_v4().simple().to_string()[..12]
    );
    let (store_a, store_b) = (
        open(&pool, &tenant, "process-a").await,
        open(&pool, &tenant, "process-b").await,
    );
    let (parent, child) = (AgentId("parent".into()), AgentId("child".into()));
    for (id, name) in [(&parent, "parent"), (&child, "child")] {
        store_a
            .insert_agent_session(&session(id, name, AgentStatus::Active))
            .await
            .expect("seed an agent");
    }
    let park = Arc::new(CompletionClassifyPark::default());
    let a =
        Services::new_isolated(store_a.clone()).with_watch_registration_park_for_test(park.clone());
    let b = Services::new_isolated(store_b.clone());

    let registering = tokio::spawn({
        let (a, parent, child) = (a.clone(), parent.clone(), child.clone());
        async move {
            a.register_completion_watch_strict_for_test(&WorkspaceId::chief(), parent, child)
                .await
        }
    });
    tokio::time::timeout(wait(), park.entered.notified())
        .await
        .expect("A found no watch and is about to insert one");

    let lapsed = sqlx::query(
        "UPDATE lease SET expires_at = clock_timestamp() - interval '1 second' \
         WHERE tenant_id = $1 AND scope = 'completion-watch-registration' \
         AND lease_id IS NOT NULL",
    )
    .bind(&tenant)
    .execute(&pool)
    .await
    .expect("expire A's registration lease")
    .rows_affected();
    assert_eq!(lapsed, 1, "A holds the pair's registration lease");
    let winner = tokio::time::timeout(
        wait(),
        b.register_completion_watch_strict_for_test(
            &WorkspaceId::chief(),
            parent.clone(),
            child.clone(),
        ),
    )
    .await
    .expect("B registers")
    .expect("B's registration");

    park.release.notify_one();
    let reused = tokio::time::timeout(wait(), registering)
        .await
        .expect("A's registration ends")
        .expect("A's registration task")
        .expect("A's registration");
    assert_eq!(reused, winner, "A reuses B's watch");
    let stored: Vec<_> = store_a
        .list_completion_watches()
        .await
        .expect("stored watches")
        .into_iter()
        .filter(|w| w.parent_agent_id == parent && w.child_agent_id == child)
        .map(|w| w.id)
        .collect();
    assert_eq!(stored, [winner], "one watch for the pair");
}
