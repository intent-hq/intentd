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

use intent_core::{
    ActorType, AgentId, AgentSession, AgentStatus, Event, EventActor, Workspace, WorkspaceId,
};
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

/// A guarded enqueue cancelled while its write is pending leaves nothing
/// behind: a parent in an archived workspace gets its wake parked in the
/// queue before `send_message`'s detached boundary, so shutdown cancelling
/// the delivery task drops the enqueue there. The pending entry must leave
/// memory with it, or the shutdown queue save (an unguarded write) would
/// store a wake that never passed its delivery lease.
#[tokio::test(flavor = "multi_thread", worker_threads = 4)]
async fn a_cancelled_guarded_enqueue_leaves_nothing_to_persist() {
    let Some(pool) = pool().await else { return };
    let tenant = format!(
        "wake-cancel-{}",
        &uuid::Uuid::new_v4().simple().to_string()[..12]
    );
    let store = open(&pool, &tenant, "process-a").await;
    let ws = WorkspaceId("archived-ws".into());
    let now = intent_core::now_iso();
    store
        .insert_workspace(&Workspace {
            id: ws.clone(),
            title: "archived".into(),
            branch: "main".into(),
            created_at: now.clone(),
            updated_at: now.clone(),
            ..Default::default()
        })
        .await
        .expect("seed the workspace");
    let (parent, child) = (AgentId("parent".into()), AgentId("child".into()));
    for (id, name, status) in [
        (&parent, "parent", AgentStatus::RuntimeIdle),
        (&child, "child", AgentStatus::Active),
    ] {
        store
            .insert_agent_session(&AgentSession {
                workspace_id: ws.clone(),
                ..session(id, name, status)
            })
            .await
            .expect("seed an agent");
    }
    let park = Arc::new(CompletionClassifyPark::default());
    let a = Services::new_isolated(store.clone()).with_guarded_enqueue_park_for_test(park.clone());
    let manager = Arc::new(AgentManager::new(
        a.clone(),
        Arc::new(BusEventSink::new(EventBus::new(store.clone()))),
        4,
    ));
    a.attach_agent_manager(&manager);
    a.register_completion_watch_durable_for_test(&ws, parent.clone(), child.clone(), None)
        .await
        .expect("register the watch");
    store
        .archive_workspace_detaching_guests(&ws, &now)
        .await
        .expect("archive the workspace");
    store
        .set_agent_session_status(&ws, &child, AgentStatus::RuntimeIdle, false, &now, None)
        .await
        .expect("the child completes");

    let mut event = idle_event(&child);
    event.workspace_id = ws.clone();
    let delivering = tokio::spawn({
        let a = a.clone();
        async move { a.handle_completion_event_for_test(&event).await }
    });
    tokio::time::timeout(wait(), park.entered.notified())
        .await
        .expect("the wake is parked in the queue, its guarded write pending");
    assert_eq!(a.queue_snapshot_for_test(&parent).len(), 1);

    // Shutdown cancels the delivery task with the write still pending.
    delivering.abort();
    assert!(delivering
        .await
        .expect_err("the delivery was cancelled")
        .is_cancelled());
    assert!(
        a.queue_snapshot_for_test(&parent).is_empty(),
        "the cancelled enqueue took its entry out of memory"
    );
    a.persist_shutdown_queues_for_test().await;
    assert_eq!(
        stored_queue_len(&store, &parent).await,
        0,
        "the shutdown queue save stored no unguarded wake"
    );
}

/// A quarantined (poisoned) parent whose completion wake is being queued
/// under its delivery claim, while `agent.wakeOrCreate` migrates the
/// parent's queue to a replacement.
struct PoisonedMigration {
    a: Services,
    store: Store,
    parent: AgentId,
    replacement: AgentId,
    enqueue: Arc<CompletionClassifyPark>,
    migration: Arc<CompletionClassifyPark>,
    delivering: tokio::task::JoinHandle<()>,
    migrating: tokio::task::JoinHandle<intent_core::Result<usize>>,
}

/// Parks the wake's guarded enqueue (holding the persist gate, write
/// pending), then starts the migration and lets it run up to the gate.
async fn poisoned_migration(pool: &PgPool, label: &str) -> PoisonedMigration {
    let tenant = format!(
        "{label}-{}",
        &uuid::Uuid::new_v4().simple().to_string()[..12]
    );
    let store = open(pool, &tenant, "process-a").await;
    let (parent, child, replacement) = (
        AgentId("parent".into()),
        AgentId("child".into()),
        AgentId("replacement".into()),
    );
    for (id, name, status) in [
        (&parent, "parent", AgentStatus::RuntimeIdle),
        (&child, "child", AgentStatus::Active),
        (&replacement, "replacement", AgentStatus::RuntimeIdle),
    ] {
        store
            .insert_agent_session(&session(id, name, status))
            .await
            .expect("seed an agent");
    }
    let (enqueue, migration) = (
        Arc::new(CompletionClassifyPark::default()),
        Arc::new(CompletionClassifyPark::default()),
    );
    let a = Services::new_isolated(store.clone())
        .with_guarded_enqueue_park_for_test(enqueue.clone())
        .with_migration_park_for_test(migration.clone());
    let manager = Arc::new(AgentManager::new(
        a.clone(),
        Arc::new(BusEventSink::new(EventBus::new(store.clone()))),
        4,
    ));
    a.attach_agent_manager(&manager);
    a.register_completion_watch_durable_for_test(
        &WorkspaceId::chief(),
        parent.clone(),
        child.clone(),
        None,
    )
    .await
    .expect("register the watch");
    let now = intent_core::now_iso();
    // A session-fatal stop reason quarantines the parent: its wake is
    // parked in the queue before send_message's detached boundary.
    store
        .set_agent_session_status(
            &WorkspaceId::chief(),
            &parent,
            AgentStatus::Error,
            false,
            &now,
            Some(Some("Please start a new session to continue.".into())),
        )
        .await
        .expect("quarantine the parent");
    store
        .set_agent_session_status(
            &WorkspaceId::chief(),
            &child,
            AgentStatus::RuntimeIdle,
            false,
            &now,
            None,
        )
        .await
        .expect("the child completes");

    let event = idle_event(&child);
    let delivering = tokio::spawn({
        let a = a.clone();
        async move { a.handle_completion_event_for_test(&event).await }
    });
    tokio::time::timeout(wait(), enqueue.entered.notified())
        .await
        .expect("the wake is queued, its guarded write pending");
    assert_eq!(a.queue_snapshot_for_test(&parent).len(), 1);
    let migrating = tokio::spawn({
        let (a, parent, replacement) = (a.clone(), parent.clone(), replacement.clone());
        async move {
            a.migrate_queue_and_gc_poisoned_session_for_test(
                &parent,
                &replacement,
                &WorkspaceId::chief(),
            )
            .await
        }
    });
    tokio::time::timeout(wait(), migration.entered.notified())
        .await
        .expect("the migration starts");
    migration.release.notify_one();
    PoisonedMigration {
        a,
        store,
        parent,
        replacement,
        enqueue,
        migration,
        delivering,
        migrating,
    }
}

/// The guarded enqueue is cancelled while the migration waits for the
/// persist gate: the migration then finds nothing to move, and neither the
/// poisoned parent's queue nor the replacement's stores the rejected wake.
#[tokio::test(flavor = "multi_thread", worker_threads = 4)]
async fn a_migration_never_moves_a_pending_guarded_wake() {
    let Some(pool) = pool().await else { return };
    let m = poisoned_migration(&pool, "migrate-cancel").await;
    m.delivering.abort();
    assert!(m.delivering.await.expect_err("cancelled").is_cancelled());
    let moved = tokio::time::timeout(wait(), m.migrating)
        .await
        .expect("the migration ends")
        .expect("the migration task")
        .expect("the migration");
    assert_eq!(moved, 0, "nothing settled was left to move");
    for agent in [&m.parent, &m.replacement] {
        assert!(
            m.a.queue_snapshot_for_test(agent).is_empty(),
            "no wake in memory for {}",
            agent.0
        );
        assert_eq!(
            stored_queue_len(&m.store, agent).await,
            0,
            "no wake stored for {}",
            agent.0
        );
    }
    drop(m.enqueue);
    drop(m.migration);
}

/// The guarded write commits while the migration waits for the persist
/// gate: the migration then moves the committed wake, which is stored on
/// the replacement and drainable there (not left pending).
#[tokio::test(flavor = "multi_thread", worker_threads = 4)]
async fn a_migration_moves_a_committed_guarded_wake_drainable() {
    let Some(pool) = pool().await else { return };
    let m = poisoned_migration(&pool, "migrate-commit").await;
    m.enqueue.release.notify_one();
    tokio::time::timeout(wait(), m.delivering)
        .await
        .expect("the delivery ends")
        .expect("the delivery task");
    let moved = tokio::time::timeout(wait(), m.migrating)
        .await
        .expect("the migration ends")
        .expect("the migration task")
        .expect("the migration");
    assert_eq!(moved, 1, "the committed wake moves");
    assert_eq!(stored_queue_len(&m.store, &m.parent).await, 0);
    assert_eq!(
        stored_queue_len(&m.store, &m.replacement).await,
        1,
        "stored on the replacement"
    );
    assert!(
        m.a.dequeue_message_draining_for_test(&m.replacement)
            .is_some(),
        "and drainable there"
    );
    drop(m.migration);
}
