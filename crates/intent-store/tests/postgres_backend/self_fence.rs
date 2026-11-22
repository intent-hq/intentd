//! Self-fencing on tenant lease loss: a process that cannot renew its lease
//! by `expires_at - margin`, or meets `NotOwner` on any write or renewal,
//! fires its tenant fence at once. From then on every write of that tenant
//! in the process fails `Error::NotOwner` before it starts, and
//! [`TenantFence::settled`] returns once no write that passed the fence is
//! still open, so no row lands after it: the event bus writer goes through
//! the same `begin_write`.
//!
//! Each "process" is a store opened through the `*_as_process` test seams.

use std::time::{Duration, Instant};

use intent_core::tenant_fence::{FenceStep, SelfFence};
use intent_core::{ActorType, EventActor, WorkspaceId};
use intent_store::postgres::{
    LeaseHeartbeat, PgPool, PostgresOptions, TenantId, TenantLease, WriterLimits,
};
use intent_store::{Error, NewEvent, Store, TenantFence};
use intentd_test_support::test_timeout;
use serde_json::json;

use super::harness::{sqlite_store, test_db};

const LONG_TTL: Duration = Duration::from_secs(60);

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

async fn open_owned(
    pool: &PgPool,
    tenant: &str,
    process: &str,
    ttl: Duration,
) -> (Store, TenantLease) {
    Store::open_postgres_owned_as_process(
        pool.clone(),
        TenantId::new(tenant).unwrap(),
        options(),
        process,
        process,
        ttl,
        intent_store::OrchestratorOnly::for_tests(),
    )
    .await
    .expect("claim the tenant")
}

fn event(n: usize) -> NewEvent {
    NewEvent {
        workspace_id: WorkspaceId("ws-fence".into()),
        timestamp: "2026-11-22T00:00:00Z".into(),
        event_type: "test:fence".into(),
        actor: EventActor {
            actor_type: ActorType::System,
            ..Default::default()
        },
        session_id: None,
        correlation_id: None,
        parent_event_id: None,
        metadata: None,
        data: json!({ "n": n }),
    }
}

async fn event_rows(admin: &PgPool, tenant: &str) -> i64 {
    sqlx::query_scalar("SELECT count(*) FROM event WHERE tenant_id = $1")
        .bind(tenant)
        .fetch_one(admin)
        .await
        .expect("count event rows")
}

#[track_caller]
fn assert_not_owner<T: std::fmt::Debug>(result: intent_store::Result<T>, what: &str) -> String {
    match result {
        Err(Error::NotOwner(message)) => message,
        other => panic!("{what}: expected NotOwner, got {other:?}"),
    }
}

async fn wait_fenced(fence: &TenantFence) {
    tokio::time::timeout(test_timeout(Duration::from_secs(30)), fence.fenced())
        .await
        .expect("the fence fires");
}

/// Whether the tenant's lease is still live by the database clock.
async fn lease_live(admin: &PgPool, tenant: &str) -> bool {
    sqlx::query_scalar(
        "SELECT expires_at > clock_timestamp() FROM tenant_lease WHERE tenant_id = $1",
    )
    .bind(tenant)
    .fetch_one(admin)
    .await
    .expect("read the lease expiry")
}

/// Renewals block (another session holds the lease row lock), so none
/// succeeds: the fence fires by the renew deadline, and the self-fence
/// closes the tenant's client connections and machine links and cancels its
/// tasks while the lease is still live by the database clock. No event row
/// lands after the fence, also from a writer that keeps writing throughout.
#[tokio::test]
async fn blocked_renewals_fence_the_tenant_before_its_lease_expires() {
    let Some(db) = test_db(false).await else {
        return;
    };
    let tenant = "fence-blocked";
    let ttl = Duration::from_secs(4);
    let margin = Duration::from_secs(2);
    let (store, lease) = open_owned(&db.app, tenant, "proc-a", ttl).await;
    let heartbeat = LeaseHeartbeat::spawn_with_margin(
        store.clone(),
        &lease,
        ttl,
        Duration::from_millis(100),
        margin,
    );
    let fence = heartbeat.fence();
    assert!(!fence.is_fenced());
    assert!(
        store
            .tenant_fence()
            .expect("a claimed store has a fence")
            .same_as(&fence),
        "the heartbeat fires the store's own fence"
    );

    // The tenant's serving side: each close records whether the lease was
    // still live when it ran.
    let mut runner = SelfFence::new(fence.clone());
    let (closed_tx, mut closed_rx) = tokio::sync::mpsc::unbounded_channel();
    for what in ["client connections", "machine links"] {
        let admin = db.admin.clone();
        let closed = closed_tx.clone();
        let close = move || async move {
            let live = lease_live(&admin, tenant).await;
            closed.send((what, live)).unwrap();
        };
        if what == "client connections" {
            runner.on_client_connections(close);
        } else {
            runner.on_machine_links(close);
        }
    }
    drop(closed_tx);
    let orchestration = tokio::spawn(std::future::pending::<()>());
    let cancelled = orchestration.abort_handle();
    runner.adopt_task(orchestration);
    runner.own(store.clone());
    let self_fence = runner.spawn();

    // A writer that writes until the fence stops it (as the bus writer).
    let writer = {
        let store = store.clone();
        tokio::spawn(async move {
            let mut n = 0;
            loop {
                match store.insert_event(&event(n)).await {
                    Ok(_) => n += 1,
                    Err(e) => return (n, e),
                }
                // timing-guard: pace the background writer so it does not monopolise the writer lock
                tokio::time::sleep(Duration::from_millis(5)).await;
            }
        })
    };

    // Block every renewal: hold the lease row lock in an open transaction.
    let mut blocker = db.admin.begin().await.unwrap();
    sqlx::query("SELECT 1 FROM tenant_lease WHERE tenant_id = $1 FOR UPDATE")
        .bind(tenant)
        .execute(&mut *blocker)
        .await
        .unwrap();
    let blocked_at = Instant::now();

    wait_fenced(&fence).await;
    let fenced_after = blocked_at.elapsed();
    let report = tokio::time::timeout(test_timeout(Duration::from_secs(30)), self_fence)
        .await
        .expect("the self-fence finishes")
        .unwrap();
    // Closed before the lease expired, by the database clock (renewals are
    // blocked, so its expiry no longer moves).
    let mut closed = Vec::new();
    while let Some(c) = closed_rx.recv().await {
        closed.push(c);
    }
    assert_eq!(
        closed,
        [("client connections", true), ("machine links", true)],
        "fenced after {fenced_after:?}; each close must run while the lease is live"
    );
    assert!(
        lease_live(&db.admin, tenant).await,
        "the lease is still live once stopped"
    );
    assert!(cancelled.is_finished(), "the tenant's tasks are cancelled");
    assert!(report.overran.is_empty(), "{report:?}");
    assert!(report.finished(FenceStep::WritesSettled).is_some());
    assert!(
        fence
            .reason()
            .expect("a fired fence says why")
            .contains("renew"),
        "{:?}",
        fence.reason()
    );

    tokio::time::timeout(test_timeout(Duration::from_secs(30)), fence.settled())
        .await
        .expect("open writes settle");
    let at_fence = event_rows(&db.admin, tenant).await;

    let (written, stopped) = tokio::time::timeout(test_timeout(Duration::from_secs(30)), writer)
        .await
        .expect("the writer stops")
        .unwrap();
    assert!(written > 0, "the writer wrote before the fence");
    assert!(matches!(stopped, Error::NotOwner(_)), "{stopped:?}");
    assert_not_owner(
        store.insert_event(&event(usize::MAX)).await,
        "a write after the fence",
    );
    assert_not_owner(
        store.insert_events(&[event(1), event(2)]).await,
        "a batch write after the fence",
    );

    // Renewals unblock: the fence stays fired, and nothing lands.
    blocker.rollback().await.unwrap();
    assert_not_owner(
        store.renew_tenant_lease(ttl).await,
        "a renewal after the fence",
    );
    // timing-guard: give any write that slipped past the fence time to commit
    tokio::time::sleep(Duration::from_millis(300)).await;
    assert_eq!(event_rows(&db.admin, tenant).await, at_fence);
    assert!(fence.is_fenced());
}

/// A write that meets `NotOwner` (another process took the tenant over)
/// fires the fence at once, with no heartbeat running.
#[tokio::test]
async fn a_not_owner_write_fires_the_fence() {
    let Some(db) = test_db(false).await else {
        return;
    };
    let tenant = "fence-not-owner";
    let (store, _lease) = open_owned(&db.app, tenant, "proc-a", LONG_TTL).await;
    let fence = store.tenant_fence().expect("claimed");
    store.insert_event(&event(0)).await.unwrap();
    sqlx::raw_sql(sqlx::AssertSqlSafe(format!(
        "UPDATE tenant_writer SET owner_instance = 'proc-b', epoch = epoch + 1 \
         WHERE tenant_id = '{tenant}'"
    )))
    .execute(&db.admin)
    .await
    .unwrap();
    assert!(!fence.is_fenced());
    assert_not_owner(
        store.insert_event(&event(1)).await,
        "a write after the takeover",
    );
    assert!(fence.is_fenced(), "NotOwner on a write fires the fence");
    assert!(
        fence.reason().unwrap().contains("proc-b"),
        "{:?}",
        fence.reason()
    );
    assert_eq!(event_rows(&db.admin, tenant).await, 1);
}

/// A renewal that meets `NotOwner` fires the fence too.
#[tokio::test]
async fn a_not_owner_renewal_fires_the_fence() {
    let Some(db) = test_db(false).await else {
        return;
    };
    let tenant = "fence-renewal";
    let (store, _lease) = open_owned(&db.app, tenant, "proc-a", LONG_TTL).await;
    let fence = store.tenant_fence().expect("claimed");
    sqlx::raw_sql(sqlx::AssertSqlSafe(format!(
        "UPDATE tenant_lease SET owner_instance = 'proc-b', epoch = epoch + 1 \
         WHERE tenant_id = '{tenant}'"
    )))
    .execute(&db.admin)
    .await
    .unwrap();
    assert_not_owner(
        store.renew_tenant_lease(LONG_TTL).await,
        "a renewal after takeover",
    );
    assert!(fence.is_fenced());
}

/// A fence fired by the caller (or the heartbeat) refuses every later write
/// and renewal locally, though the database still names this process; a
/// new claim (after the lease expires) gets a new, unfired fence.
#[tokio::test]
async fn a_fired_fence_holds_until_a_new_claim() {
    let Some(db) = test_db(false).await else {
        return;
    };
    let tenant = "fence-latched";
    let ttl = Duration::from_millis(500);
    let (store, _lease) = open_owned(&db.app, tenant, "proc-a", ttl).await;
    let fence = store.tenant_fence().expect("claimed");
    let other = store.clone();
    assert!(fence.fire("test: stop serving"));
    assert!(!fence.fire("again"), "only the first fire counts");
    assert_eq!(fence.reason().as_deref(), Some("test: stop serving"));
    let message = assert_not_owner(other.insert_event(&event(0)).await, "a write after fire");
    assert!(message.contains("fenced"), "{message}");
    assert_not_owner(other.renew_tenant_lease(ttl).await, "a renewal after fire");
    assert_eq!(event_rows(&db.admin, tenant).await, 0);

    // timing-guard: let the short lease lapse by the database clock
    tokio::time::sleep(ttl * 2).await;
    let claim = store.claim_tenant("proc-a", LONG_TTL).await.unwrap();
    assert!(matches!(
        claim,
        intent_store::postgres::TenantClaim::TenantOwned(_)
    ));
    let renewed = store.tenant_fence().expect("claimed again");
    assert!(!renewed.same_as(&fence));
    assert!(!renewed.is_fenced());
    store.insert_event(&event(1)).await.unwrap();
    assert_eq!(event_rows(&db.admin, tenant).await, 1);
    assert!(fence.is_fenced(), "the old claim's fence stays fired");
}

/// Dropping the heartbeat stops renewals, so the tenant is fenced: the
/// process can no longer promise to renew before the lease expires.
#[tokio::test]
async fn dropping_the_heartbeat_fires_the_fence() {
    let Some(db) = test_db(false).await else {
        return;
    };
    let (store, lease) = open_owned(&db.app, "fence-drop", "proc-a", LONG_TTL).await;
    let heartbeat = LeaseHeartbeat::spawn(store.clone(), &lease, LONG_TTL, Duration::from_secs(10));
    let fence = heartbeat.fence();
    assert!(!fence.is_fenced());
    drop(heartbeat);
    assert!(fence.is_fenced());
    assert_not_owner(
        store.insert_event(&event(0)).await,
        "a write after the drop",
    );
}

/// An unclaimed Postgres store and a `SQLite` store have no fence.
#[tokio::test]
async fn only_a_claimed_store_has_a_fence() {
    let (sqlite, _dir) = sqlite_store().await;
    assert!(sqlite.tenant_fence().is_none());
    let Some(db) = test_db(false).await else {
        return;
    };
    let unclaimed = Store::open_postgres_as_process(
        db.app.clone(),
        TenantId::new("fence-unclaimed").unwrap(),
        options(),
        "proc-a",
        intent_store::OrchestratorOnly::for_tests(),
    )
    .await
    .unwrap();
    assert!(unclaimed.tenant_fence().is_none());
}
