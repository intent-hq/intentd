//! The tenant state (`tenant_writer.state`): a tenant that is closing or
//! closed refuses every write with `Error::TenantClosed`, whoever holds its
//! lease. Each writer reads the state under the writer lock it already
//! takes, and a state move takes the same row lock, so a write in flight
//! commits before the move and none commits after it.
//!
//! Each "process" here is a store opened through the `*_as_process` test
//! seams: its own process-local gates and claim, as between two daemons.

use std::sync::Arc;
use std::time::Duration;

use intent_core::{ActorType, EventActor, WorkspaceId};
use intent_store::postgres::{
    self, PgPool, PostgresOptions, TenantId, TenantLease, WriterLimits, LEASE_TTL,
};
use intent_store::{Error, NewEvent, Store, TenantState, TenantStateMove};
use intentd_test_support::test_timeout;
use tokio::sync::Barrier;

use super::harness::{hold_writer_lock, test_db};

/// Patient writers: one lock attempt outlasts every wait in these tests.
fn options() -> PostgresOptions {
    let long = test_timeout(Duration::from_secs(60));
    PostgresOptions {
        writer: WriterLimits {
            wait_budget: long,
            lock_attempt: long,
            ..WriterLimits::default()
        },
        ..PostgresOptions::default()
    }
}

fn tid(tenant: &str) -> TenantId {
    TenantId::new(tenant).unwrap()
}

async fn open_owned(pool: &PgPool, tenant: &str, process: &str) -> (Store, TenantLease) {
    Store::open_postgres_owned_as_process(
        pool.clone(),
        tid(tenant),
        options(),
        process,
        process,
        LEASE_TTL,
        intent_store::OrchestratorOnly::for_tests(),
    )
    .await
    .expect("claim the tenant")
}

async fn open_unclaimed(pool: &PgPool, tenant: &str, process: &str) -> Store {
    Store::open_postgres_as_process(
        pool.clone(),
        tid(tenant),
        options(),
        process,
        intent_store::OrchestratorOnly::for_tests(),
    )
    .await
    .expect("open the tenant")
}

/// Move `tenant` as a tool that never claimed it.
async fn admin_move(pool: &PgPool, tenant: &str, to: TenantState) -> TenantStateMove {
    Store::postgres_move_tenant_state(pool.clone(), tid(tenant), to)
        .await
        .expect("move the tenant state")
}

async fn state_row(admin: &PgPool, tenant: &str) -> String {
    sqlx::query_scalar("SELECT state FROM tenant_writer WHERE tenant_id = $1")
        .bind(tenant)
        .fetch_one(admin)
        .await
        .expect("read the tenant state")
}

fn event(n: usize) -> NewEvent {
    NewEvent {
        workspace_id: WorkspaceId("ws-state".into()),
        timestamp: "2026-11-27T00:00:00Z".into(),
        event_type: "test:state".into(),
        actor: EventActor {
            actor_type: ActorType::System,
            ..Default::default()
        },
        session_id: None,
        correlation_id: None,
        parent_event_id: None,
        metadata: None,
        data: serde_json::json!({ "n": n }),
    }
}

/// The `row_seq` of every event row of `tenant`, ascending.
async fn event_seqs(admin: &PgPool, tenant: &str) -> Vec<i64> {
    sqlx::query_scalar("SELECT row_seq FROM event WHERE tenant_id = $1 ORDER BY row_seq")
        .bind(tenant)
        .fetch_all(admin)
        .await
        .expect("read the event rows")
}

#[track_caller]
fn assert_closed<T: std::fmt::Debug>(result: intent_store::Result<T>, what: &str) {
    match result {
        Err(Error::TenantClosed(message)) => {
            assert!(message.contains("nothing was written"), "{what}: {message}");
        }
        other => panic!("{what}: expected TenantClosed, got {other:?}"),
    }
}

#[tokio::test]
async fn a_closing_or_closed_tenant_refuses_owner_and_lease_writes_and_a_reopen_restores_them() {
    let Some(db) = test_db(false).await else {
        return;
    };
    let tenant = "state-owner";
    let (store, _lease) = open_owned(&db.app, tenant, "a").await;
    store.set_setting("userRules", "\"open\"").await.unwrap();
    let mut held = store
        .acquire_lease("state-test", "held", LEASE_TTL)
        .await
        .unwrap()
        .expect("a free key");
    assert_eq!(store.tenant_state().await.unwrap(), TenantState::Open);

    for to in [TenantState::Closing, TenantState::Closed] {
        let moved = store.move_tenant_state(to).await.unwrap();
        assert_eq!(moved.state, to, "the owner moves its tenant: {moved:?}");
        assert_eq!(state_row(&db.admin, tenant).await, to.as_str());
        assert_eq!(store.tenant_state().await.unwrap(), to);
        assert_closed(store.set_setting("userRules", "\"x\"").await, "owner write");
        assert_closed(store.insert_event_seq(&event(0)).await, "event insert");
        assert_closed(
            store.acquire_lease("state-test", "new", LEASE_TTL).await,
            "lease acquire",
        );
        assert_closed(
            store.renew_lease(&mut held, LEASE_TTL).await,
            "lease renewal",
        );
        // Reads still work.
        assert_eq!(
            store.get_setting("userRules").await.unwrap().as_deref(),
            Some("\"open\"")
        );
    }
    // A release only gives the key up: it runs while closed.
    store.release_lease(held).await.unwrap();

    let reopened = store.move_tenant_state(TenantState::Open).await.unwrap();
    assert_eq!(
        (reopened.from, reopened.state),
        (TenantState::Closed, TenantState::Open)
    );
    store.set_setting("userRules", "\"again\"").await.unwrap();
    let again = store
        .acquire_lease("state-test", "held", LEASE_TTL)
        .await
        .unwrap();
    assert!(again.is_some(), "the released key is free after the reopen");
}

#[tokio::test]
async fn a_tool_closes_an_unclaimed_tenant_and_an_unclaimed_open_reads_it() {
    let Some(db) = test_db(false).await else {
        return;
    };
    let tenant = "state-unclaimed";
    // Never written: the move seeds the writer row.
    let closing = admin_move(&db.app, tenant, TenantState::Closing).await;
    assert_eq!(
        (closing.from, closing.state),
        (TenantState::Open, TenantState::Closing)
    );
    // An unclaimed open of a closed tenant opens it for reads; its writes
    // (provisioning included) are refused.
    let store = open_unclaimed(&db.app, tenant, "tool").await;
    assert_eq!(store.tenant_state().await.unwrap(), TenantState::Closing);
    assert_closed(
        store.set_setting("userRules", "\"x\"").await,
        "unclaimed write",
    );
    assert_eq!(store.get_setting("userRules").await.unwrap(), None);
    // The reopen provisions the tenant it never provisioned.
    let open = store.move_tenant_state(TenantState::Open).await.unwrap();
    assert_eq!(open.state, TenantState::Open);
    store.set_setting("userRules", "\"open\"").await.unwrap();
}

/// A write that holds the writer lock when the tenant starts closing
/// commits; the move waits for it, and the next write is refused.
#[tokio::test(flavor = "multi_thread", worker_threads = 4)]
async fn closing_lets_the_write_in_flight_commit_and_refuses_the_next() {
    let Some(db) = test_db(false).await else {
        return;
    };
    let tenant = "state-in-flight";
    let store = open_unclaimed(&db.app, tenant, "writer").await;
    store.set_setting("userRules", "\"seed\"").await.unwrap();

    // The write in flight: the store's own lock function, then a numbered
    // row, not yet committed.
    let mut in_flight = hold_writer_lock(&db.admin, tenant).await;
    let seq: i64 = sqlx::query_scalar(
        "INSERT INTO event (tenant_id, id, workspace_id, timestamp, event_type, actor) \
         VALUES ($1, 'in-flight', 'ws', 't', 'test:state', 'system') RETURNING row_seq",
    )
    .bind(tenant)
    .fetch_one(&mut *in_flight)
    .await
    .expect("the write in flight numbers its row");

    let pool = db.app.clone();
    let close = tokio::spawn(async move {
        Store::postgres_move_tenant_state(pool, tid(tenant), TenantState::Closing).await
    });
    // The move needs the row lock the write in flight holds.
    tokio::task::yield_now().await;
    assert!(
        !close.is_finished(),
        "the move waits for the write in flight"
    );
    in_flight
        .commit()
        .await
        .expect("the write in flight commits");
    let closed = tokio::time::timeout(test_timeout(Duration::from_secs(30)), close)
        .await
        .expect("the move ends once the write committed")
        .unwrap()
        .expect("the move succeeds");
    assert_eq!(closed.state, TenantState::Closing);
    assert!(
        closed.last_row_seq >= seq,
        "the move ran after the write in flight: {closed:?}, row {seq}"
    );
    assert_eq!(event_seqs(&db.admin, tenant).await, vec![seq]);
    assert_closed(store.insert_event_seq(&event(1)).await, "the next write");
}

/// Writers of two processes race a close, all started together: every
/// write that returned Ok committed before the close, and none after it.
#[tokio::test(flavor = "multi_thread", worker_threads = 4)]
async fn a_close_racing_writers_never_leaves_a_write_committed_after_it() {
    const WRITERS: usize = 16;
    let Some(db) = test_db(false).await else {
        return;
    };
    let tenant = "state-race";
    let stores = [
        open_unclaimed(&db.app, tenant, "p1").await,
        open_unclaimed(&db.app, tenant, "p2").await,
    ];
    let start = Arc::new(Barrier::new(WRITERS + 1));
    let mut writers = Vec::new();
    for n in 0..WRITERS {
        let store = stores[n % 2].clone();
        let start = start.clone();
        writers.push(tokio::spawn(async move {
            start.wait().await;
            // Keep writing until the close refuses one.
            let mut written = Vec::new();
            for i in 0.. {
                match store.insert_event_seq(&event(n * 1000 + i)).await {
                    Ok((_, seq)) => written.push(i64::try_from(seq).unwrap()),
                    Err(Error::TenantClosed(_)) => return written,
                    Err(other) => panic!("writer {n}: {other:?}"),
                }
            }
            unreachable!()
        }));
    }
    let pool = db.app.clone();
    let close = {
        let start = start.clone();
        tokio::spawn(async move {
            start.wait().await;
            Store::postgres_move_tenant_state(pool, tid(tenant), TenantState::Closing).await
        })
    };
    let joined = tokio::time::timeout(test_timeout(Duration::from_secs(60)), async {
        let moved = close.await.unwrap().expect("the close succeeds");
        let mut written = Vec::new();
        for writer in writers {
            written.extend(writer.await.unwrap());
        }
        (moved, written)
    })
    .await
    .expect("the writers stop once the tenant is closing");
    let (closed, mut written) = joined;
    assert_eq!(closed.state, TenantState::Closing);
    written.sort_unstable();
    assert!(
        written.iter().all(|seq| *seq <= closed.last_row_seq),
        "a write committed after the close (at {}): {written:?}",
        closed.last_row_seq
    );
    assert_eq!(
        event_seqs(&db.admin, tenant).await,
        written,
        "the rows are exactly the writes that returned Ok"
    );
}

/// A reopen during a close wins: closing → closed moves only from
/// `closing`, so a stop that finishes after the reopen leaves it open.
#[tokio::test(flavor = "multi_thread", worker_threads = 4)]
async fn a_reopen_during_a_close_leaves_the_tenant_open() {
    let Some(db) = test_db(false).await else {
        return;
    };
    let tenant = "state-reopen";
    let store = open_unclaimed(&db.app, tenant, "p").await;
    admin_move(&db.app, tenant, TenantState::Closing).await;
    admin_move(&db.app, tenant, TenantState::Open).await;
    let finish = admin_move(&db.app, tenant, TenantState::Closed).await;
    assert_eq!(
        (finish.from, finish.state),
        (TenantState::Open, TenantState::Open),
        "closed only from closing"
    );
    store.set_setting("userRules", "\"open\"").await.unwrap();

    // Both at once, in either order, end open.
    admin_move(&db.app, tenant, TenantState::Closing).await;
    let start = Arc::new(Barrier::new(2));
    let moves = [TenantState::Closed, TenantState::Open].map(|to| {
        let (pool, start) = (db.app.clone(), start.clone());
        tokio::spawn(async move {
            start.wait().await;
            Store::postgres_move_tenant_state(pool, tid(tenant), to).await
        })
    });
    for mv in moves {
        tokio::time::timeout(test_timeout(Duration::from_secs(30)), mv)
            .await
            .expect("the move ends")
            .unwrap()
            .expect("the move succeeds");
    }
    assert_eq!(store.tenant_state().await.unwrap(), TenantState::Open);
    store
        .set_setting("userRules", "\"still open\"")
        .await
        .unwrap();
}

/// A stop that crashed between closing and closed leaves the tenant
/// closing; the next start opens it.
#[tokio::test]
async fn a_start_after_a_stop_that_crashed_while_closing_opens_the_tenant() {
    let Some(db) = test_db(false).await else {
        return;
    };
    let tenant = "state-crash";
    admin_move(&db.app, tenant, TenantState::Closing).await;
    let (store, _lease) = open_owned(&db.app, tenant, "next").await;
    assert_eq!(store.tenant_state().await.unwrap(), TenantState::Open);
    store.set_setting("userRules", "\"serving\"").await.unwrap();
}

/// A process that lost the tenant cannot close it under its new owner,
/// even with its own fence fired for a clean stop.
#[tokio::test]
async fn a_process_that_lost_the_tenant_cannot_move_its_state() {
    let Some(db) = test_db(false).await else {
        return;
    };
    let tenant = "state-lost";
    let (old, lease) = open_owned(&db.app, tenant, "old").await;
    old.tenant_fence().expect("claimed").fire("stopping");
    assert!(old.release_tenant_lease(lease.epoch).await.unwrap());
    let (new, _lease) = open_owned(&db.app, tenant, "new").await;
    match old.move_tenant_state(TenantState::Closing).await {
        Err(Error::NotOwner(_)) => {}
        other => panic!("expected NotOwner, got {other:?}"),
    }
    assert_eq!(state_row(&db.admin, tenant).await, "open");
    new.set_setting("userRules", "\"new owner\"").await.unwrap();
    // The new owner, with its fence fired for a stop, still moves it.
    new.tenant_fence().expect("claimed").fire("stopping");
    let moved = new.move_tenant_state(TenantState::Closing).await.unwrap();
    assert_eq!(moved.state, TenantState::Closing);
}

/// A writer that takes the writer lock but skips the state check cannot
/// number a row: the schema's `row_seq` counter refuses it.
#[tokio::test]
async fn a_writer_that_skips_the_state_check_cannot_insert_a_row() {
    let Some(db) = test_db(false).await else {
        return;
    };
    let tenant = "state-skip";
    let store = open_unclaimed(&db.app, tenant, "p").await;
    store.set_setting("userRules", "\"seed\"").await.unwrap();
    admin_move(&db.app, tenant, TenantState::Closing).await;
    let mut tx = db.admin.begin().await.unwrap();
    let long = test_timeout(Duration::from_secs(30));
    let limits = WriterLimits {
        wait_budget: long,
        lock_attempt: long,
        statement_timeout: long,
        idle_in_transaction_timeout: long,
    };
    let row = postgres::lock_tenant_writer(&mut tx, tenant, long, &limits)
        .await
        .expect("the lock itself is granted");
    assert_eq!(row.state, TenantState::Closing, "the lock reads the state");
    let inserted = sqlx::query(
        "INSERT INTO event (tenant_id, id, workspace_id, timestamp, event_type, actor) \
         VALUES ($1, 'skipped', 'ws', 't', 'test:state', 'system')",
    )
    .bind(tenant)
    .execute(&mut *tx)
    .await;
    let message = inserted.expect_err("the row is refused").to_string();
    assert!(message.contains("takes no writes"), "{message}");
    assert!(event_seqs(&db.admin, tenant).await.is_empty());
}
