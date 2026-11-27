//! The write role of a store handle (`WriteRole`): owner work vs request
//! writes in orchestrator mode.
//!
//! - An **owner** handle (`open_postgres_owned`) writes only while the
//!   tenant lease names its process's claim.
//! - An **unclaimed** handle (`open_postgres`) keeps today's rule: it writes
//!   only while no process ever claimed the tenant.
//! - A **request** handle (`open_postgres_request`, `Store::request_writer`)
//!   still takes the tenant writer lock and reads the tenant state, but
//!   skips the owner comparison, so an owner takeover does not refuse it. In
//!   a process whose claim fenced itself it refuses, because that process's
//!   in-memory gates are not converted yet.
//! - Owner-only writes (lease rows, tenant claims and state moves, and
//!   `begin_owner_write`) refuse a request handle with `Error::NotOwner`.
//!
//! Each "process" here is a store opened through the `*_as_process` test
//! seams: its own process-local gates and claim, as between two daemons.

use std::time::Duration;

use intent_core::{ActorType, EventActor, WorkspaceId};
use intent_store::postgres::{
    PgPool, PostgresOptions, TenantId, TenantLease, WriteRole, WriterLimits,
};
use intent_store::{Error, LeaseKeeper, NewEvent, Store, TenantState};
use intentd_test_support::test_timeout;

use super::harness::{hold_writer_lock, sqlite_store, test_db, writer_lock_waiters, TestDb};

const LONG_TTL: Duration = Duration::from_secs(60);
const SHORT_TTL: Duration = Duration::from_millis(500);
const SCOPE: &str = "write-role-test";

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

async fn open_owned(
    pool: &PgPool,
    tenant: &str,
    process: &str,
    ttl: Duration,
) -> (Store, TenantLease) {
    Store::open_postgres_owned_as_process(
        pool.clone(),
        tid(tenant),
        options(),
        process,
        process,
        ttl,
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

async fn open_request(pool: &PgPool, tenant: &str, process: &str) -> Store {
    Store::open_postgres_request_as_process(
        pool.clone(),
        tid(tenant),
        options(),
        process,
        intent_store::OrchestratorOnly::for_tests(),
    )
    .await
    .expect("open the tenant for requests")
}

fn event(n: usize) -> NewEvent {
    NewEvent {
        workspace_id: WorkspaceId("ws-write-role".into()),
        timestamp: "2026-11-27T00:00:00Z".into(),
        event_type: "test:write-role".into(),
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

/// The `n` of every event row of `tenant`, in `row_seq` order.
async fn event_ns(admin: &PgPool, tenant: &str) -> Vec<i64> {
    sqlx::query_scalar(
        "SELECT (data_json::jsonb ->> 'n')::bigint FROM event WHERE tenant_id = $1 ORDER BY row_seq",
    )
    .bind(tenant)
    .fetch_all(admin)
    .await
    .expect("read the event rows")
}

/// The tenant lease as `(holder, token)`.
async fn tenant_lease(admin: &PgPool, tenant: &str) -> Option<(Option<String>, i64)> {
    sqlx::query_as(
        "SELECT holder, token FROM lease \
         WHERE tenant_id = $1 AND scope = 'tenant' AND lease_key = ''",
    )
    .bind(tenant)
    .fetch_optional(admin)
    .await
    .expect("read the tenant lease")
}

/// The rows of `tenant`'s store leases of [`SCOPE`].
async fn scoped_leases(admin: &PgPool, tenant: &str) -> i64 {
    sqlx::query_scalar("SELECT count(*) FROM lease WHERE tenant_id = $1 AND scope = $2")
        .bind(tenant)
        .bind(SCOPE)
        .fetch_one(admin)
        .await
        .expect("count the scoped leases")
}

async fn state_row(admin: &PgPool, tenant: &str) -> String {
    sqlx::query_scalar("SELECT state FROM tenant_writer WHERE tenant_id = $1")
        .bind(tenant)
        .fetch_one(admin)
        .await
        .expect("read the tenant state")
}

/// Wait until the tenant's lease has expired by the database clock.
async fn wait_for_expiry(admin: &PgPool, tenant: &str) {
    tokio::time::timeout(test_timeout(Duration::from_secs(30)), async {
        loop {
            let expired: bool = sqlx::query_scalar(
                "SELECT expires_at <= clock_timestamp() FROM lease \
                 WHERE tenant_id = $1 AND scope = 'tenant' AND lease_key = ''",
            )
            .bind(tenant)
            .fetch_one(admin)
            .await
            .expect("read lease expiry");
            if expired {
                return;
            }
            // timing-guard: poll interval while waiting for the lease to expire by the database clock
            tokio::time::sleep(Duration::from_millis(50)).await;
        }
    })
    .await
    .expect("the lease expires");
}

/// Wait until `n` sessions wait for a `tenant_writer` row lock.
async fn wait_for_lock_waiters(db: &TestDb, n: usize) {
    tokio::time::timeout(test_timeout(Duration::from_secs(30)), async {
        while writer_lock_waiters(&db.admin).await.len() < n {
            // timing-guard: poll interval while waiting for the lock waiters to appear in pg_stat_activity
            tokio::time::sleep(Duration::from_millis(20)).await;
        }
    })
    .await
    .unwrap_or_else(|_| panic!("{n} writers wait for the writer lock"));
}

#[track_caller]
fn assert_not_owner<T: std::fmt::Debug>(result: intent_store::Result<T>, what: &str) -> String {
    match result {
        Err(Error::NotOwner(message)) => message,
        other => panic!("{what}: expected NotOwner, got {other:?}"),
    }
}

/// A request handle of a process that never claimed the tenant writes
/// before any claim, under the first owner, and across a takeover, while
/// the old owner's writes are refused.
#[tokio::test]
async fn a_request_handle_writes_across_an_owner_takeover() {
    let Some(db) = test_db(false).await else {
        return;
    };
    let tenant = "write-role-takeover";
    let request = open_request(&db.app, tenant, "replica").await;
    assert_eq!(request.postgres_write_role(), Some(WriteRole::Request));
    request
        .insert_event_seq(&event(1))
        .await
        .expect("before any claim");

    let (a, _lease_a) = open_owned(&db.app, tenant, "a", SHORT_TTL).await;
    a.insert_event_seq(&event(2))
        .await
        .expect("the owner writes");
    request
        .insert_event_seq(&event(3))
        .await
        .expect("a request write under the first owner");

    wait_for_expiry(&db.admin, tenant).await;
    let (b, _lease_b) = open_owned(&db.app, tenant, "b", LONG_TTL).await;
    assert_eq!(
        tenant_lease(&db.admin, tenant).await,
        Some((Some("b".into()), 2))
    );

    assert_not_owner(a.insert_event_seq(&event(99)).await, "the old owner");
    request
        .insert_event_seq(&event(4))
        .await
        .expect("a request write across the takeover");
    b.insert_event_seq(&event(5))
        .await
        .expect("the new owner writes");

    assert_eq!(event_ns(&db.admin, tenant).await, vec![1, 2, 3, 4, 5]);
}

/// `Store::request_writer` of an owner store writes across a takeover
/// until its process's claim fences itself; then it refuses, because that
/// process's in-memory gates are not converted yet. The owner store it came
/// from keeps its owner role.
#[tokio::test]
async fn a_request_writer_of_an_owner_store_writes_until_its_process_fences() {
    let Some(db) = test_db(false).await else {
        return;
    };
    let tenant = "write-role-request-writer";
    let (a, _lease_a) = open_owned(&db.app, tenant, "a", SHORT_TTL).await;
    let requests = a.request_writer();
    assert_eq!(a.postgres_write_role(), Some(WriteRole::Owner));
    assert_eq!(requests.postgres_write_role(), Some(WriteRole::Request));
    let gates = requests.postgres_shared_gates(&a).expect("postgres stores");
    assert!(gates.iter().all(|(_, shared)| *shared), "{gates:?}");
    requests
        .insert_event_seq(&event(1))
        .await
        .expect("request write");

    wait_for_expiry(&db.admin, tenant).await;
    let (_b, _lease_b) = open_owned(&db.app, tenant, "b", LONG_TTL).await;
    let fence = a.tenant_fence().expect("a's claim");
    assert!(!fence.is_fenced(), "nothing in a has seen the takeover yet");
    requests
        .insert_event_seq(&event(2))
        .await
        .expect("no owner comparison: the takeover does not refuse it");
    assert!(
        !fence.is_fenced(),
        "a request write does not fire the fence"
    );

    // The owner store still compares the owner, and its refusal fires a's
    // fence.
    assert_not_owner(a.insert_event_seq(&event(98)).await, "the owner store");
    assert!(fence.is_fenced());
    let message = assert_not_owner(
        requests.insert_event_seq(&event(99)).await,
        "a request write in a fenced process",
    );
    assert!(message.contains("fenced itself"), "{message}");

    assert_eq!(event_ns(&db.admin, tenant).await, vec![1, 2]);
}

/// An unclaimed handle keeps today's rule: it writes while no process ever
/// claimed the tenant, and never again once one did, even after that lease
/// expired. Store lease writes are not owner-fenced for it.
#[tokio::test]
async fn an_unclaimed_handle_keeps_todays_rule() {
    let Some(db) = test_db(false).await else {
        return;
    };
    let tenant = "write-role-unclaimed";
    let unclaimed = open_unclaimed(&db.app, tenant, "tool").await;
    assert_eq!(unclaimed.postgres_write_role(), Some(WriteRole::Unclaimed));
    unclaimed
        .insert_event_seq(&event(1))
        .await
        .expect("never claimed");
    unclaimed
        .acquire_lease(SCOPE, "before", LONG_TTL)
        .await
        .expect("acquire")
        .expect("free key");

    let (_a, _lease_a) = open_owned(&db.app, tenant, "a", SHORT_TTL).await;
    assert_not_owner(unclaimed.insert_event_seq(&event(98)).await, "claimed");
    wait_for_expiry(&db.admin, tenant).await;
    let message = assert_not_owner(
        unclaimed.insert_event_seq(&event(99)).await,
        "claimed once, lease expired",
    );
    assert!(message.contains("never claimed"), "{message}");
    unclaimed
        .acquire_lease(SCOPE, "after", LONG_TTL)
        .await
        .expect("a store lease is fenced by its own token, not the owner")
        .expect("free key");

    assert_eq!(event_ns(&db.admin, tenant).await, vec![1]);
    assert_eq!(scoped_leases(&db.admin, tenant).await, 2);
}

/// Owner-only writes refuse a request handle, from another process or from
/// the owner's own process, and write nothing: store lease rows, the tenant
/// lease (claim, renew, release) and tenant state moves. The owner handle
/// still makes them.
#[tokio::test]
async fn owner_only_writes_refuse_a_request_handle() {
    let Some(db) = test_db(false).await else {
        return;
    };
    let tenant = "write-role-owner-only";
    let (a, lease_a) = open_owned(&db.app, tenant, "a", LONG_TTL).await;
    let replica = open_request(&db.app, tenant, "replica").await;
    let mut held = a
        .acquire_lease(SCOPE, "held", LONG_TTL)
        .await
        .unwrap()
        .expect("the owner acquires");

    for (who, store) in [
        ("replica", &replica),
        ("owner's request writer", &a.request_writer()),
    ] {
        assert_not_owner(store.acquire_lease(SCOPE, who, LONG_TTL).await, who);
        assert_not_owner(store.renew_lease(&mut held, LONG_TTL).await, who);
        let other = a
            .acquire_lease(SCOPE, &format!("release-{who}"), LONG_TTL)
            .await
            .unwrap()
            .expect("the owner acquires");
        assert_not_owner(store.release_lease(other).await, who);
        assert_not_owner(store.claim_tenant(who, LONG_TTL).await, who);
        assert_not_owner(store.renew_tenant_lease(LONG_TTL).await, who);
        assert_not_owner(store.release_tenant_lease(lease_a.epoch).await, who);
        assert_not_owner(store.move_tenant_state(TenantState::Closing).await, who);
        assert_not_owner(store.probe_owner_write().await, who);
        // Request writes still pass.
        store
            .insert_event_seq(&event(1))
            .await
            .expect("request write");
    }

    assert_eq!(
        scoped_leases(&db.admin, tenant).await,
        3,
        "only the owner's, none released"
    );
    assert!(
        a.lease_is_held(&held).await.unwrap(),
        "the owner's lease is untouched"
    );
    assert_eq!(
        tenant_lease(&db.admin, tenant).await,
        Some((Some("a".into()), 1))
    );
    assert_eq!(state_row(&db.admin, tenant).await, "open");

    a.probe_owner_write().await.expect("the owner passes");
    a.move_tenant_state(TenantState::Closing)
        .await
        .expect("the owner moves the state");
}

/// A request write still takes the tenant writer lock (so commit order
/// stays `row_seq` order), still reads the tenant state, and still guards
/// the leases of the work it runs under.
#[tokio::test(flavor = "multi_thread", worker_threads = 4)]
async fn a_request_write_keeps_the_writer_lock_the_tenant_state_and_lease_guards() {
    let Some(db) = test_db(false).await else {
        return;
    };
    let tenant = "write-role-lock";
    let (a, _lease_a) = open_owned(&db.app, tenant, "a", LONG_TTL).await;
    let replica = open_request(&db.app, tenant, "replica").await;

    // The writer lock.
    let held = hold_writer_lock(&db.admin, tenant).await;
    let write = tokio::spawn({
        let replica = replica.clone();
        async move { replica.insert_event_seq(&event(1)).await }
    });
    wait_for_lock_waiters(&db, 1).await;
    assert!(!write.is_finished(), "the request write waits for the lock");
    held.rollback().await.unwrap();
    write
        .await
        .unwrap()
        .expect("it commits once the lock is free");

    // The lease guard of the work it runs under.
    let keeper = LeaseKeeper::acquire(&a, SCOPE, "work", LONG_TTL)
        .await
        .unwrap()
        .expect("free key");
    keeper
        .guard_writes(replica.insert_event_seq(&event(2)))
        .await
        .expect("guarded while held");
    sqlx::query(
        "UPDATE lease SET expires_at = clock_timestamp() \
         WHERE tenant_id = $1 AND scope = $2 AND lease_key = 'work'",
    )
    .bind(tenant)
    .bind(SCOPE)
    .execute(&db.admin)
    .await
    .unwrap();
    a.acquire_lease(SCOPE, "work", LONG_TTL)
        .await
        .unwrap()
        .expect("the lapsed lease is taken again");
    let lost = keeper
        .guard_writes(replica.insert_event_seq(&event(98)))
        .await;
    assert!(matches!(lost, Err(Error::LeaseLost(_))), "{lost:?}");

    // The tenant state.
    a.move_tenant_state(TenantState::Closing).await.unwrap();
    match replica.insert_event_seq(&event(99)).await {
        Err(Error::TenantClosed(message)) => {
            assert!(message.contains("nothing was written"), "{message}");
        }
        other => panic!("expected TenantClosed, got {other:?}"),
    }

    assert_eq!(event_ns(&db.admin, tenant).await, vec![1, 2]);
}

/// `SQLite` has one process per database: a request writer is the same
/// store and writes as it does.
#[tokio::test]
async fn a_sqlite_request_writer_writes_as_the_store() {
    let (store, _dir) = sqlite_store().await;
    store
        .request_writer()
        .insert_event_seq(&event(1))
        .await
        .expect("request write on SQLite");
    assert_eq!(store.request_writer().postgres_write_role(), None);
    store
        .probe_owner_write()
        .await
        .expect("SQLite owns its database");
}
