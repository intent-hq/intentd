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
    PgPool, PostgresOptions, TenantId, TenantLease, TenantLeaseKeeper, WriterLimits,
};
use intent_store::{Error, NewEvent, Store, TenantFence};
use intentd_test_support::test_timeout;
use serde_json::json;

use super::harness::{hold_writer_lock, sqlite_store, test_db, writer_lock_waiters};

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

/// Another process takes the tenant lease, as a claim would.
async fn take_over(admin: &PgPool, tenant: &str) {
    sqlx::query(sqlx::AssertSqlSafe(format!(
        "UPDATE {} SET holder = 'proc-b', lease_id = 'taken', token = token + 1 WHERE {}",
        "lease",
        TENANT_LEASE.trim_start_matches("lease WHERE ")
    )))
    .bind(tenant)
    .execute(admin)
    .await
    .expect("take the tenant lease over");
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

/// The tenant lease row's filter (`$1` the tenant).
const TENANT_LEASE: &str = "lease WHERE tenant_id = $1 AND scope = 'tenant' AND lease_key = ''";

/// How long the tenant's lease has left by the database clock, in ms
/// (negative once expired).
async fn lease_left_ms(admin: &PgPool, tenant: &str) -> i64 {
    sqlx::query_scalar(
        "SELECT (extract(epoch FROM expires_at - clock_timestamp()) * 1000)::bigint \
         FROM lease WHERE tenant_id = $1 AND scope = 'tenant' AND lease_key = ''",
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
    let keeper = TenantLeaseKeeper::spawn_with_margin(store.clone(), lease, margin);
    let fence = keeper.fence();
    assert!(!fence.is_fenced());
    assert!(
        store
            .tenant_fence()
            .expect("a claimed store has a fence")
            .same_as(&fence),
        "the keeper fires the store's own fence"
    );

    // The tenant's serving side: each close records whether the lease was
    // still live when it ran.
    let mut runner = SelfFence::new(fence.clone());
    let (closed_tx, mut closed_rx) = tokio::sync::mpsc::unbounded_channel();
    for what in ["client connections", "machine links"] {
        let admin = db.admin.clone();
        let closed = closed_tx.clone();
        let close = move || async move {
            let left = lease_left_ms(&admin, tenant).await;
            closed.send((what, left)).unwrap();
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
    sqlx::query(sqlx::AssertSqlSafe(format!(
        "SELECT 1 FROM {TENANT_LEASE} FOR UPDATE"
    )))
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
    // The fence fires `margin` before the local deadline, so each close runs
    // with well over half the margin left on the lease; a fence that fired
    // only at the deadline (no margin) would leave next to nothing.
    let names: Vec<&str> = closed.iter().map(|(what, _)| *what).collect();
    assert_eq!(names, ["client connections", "machine links"]);
    let half_margin = i64::try_from(margin.as_millis() / 2).unwrap();
    for (what, left) in &closed {
        assert!(
            *left > half_margin,
            "fenced after {fenced_after:?}; {what} closed with only {left} ms left on the lease"
        );
    }
    assert!(
        lease_left_ms(&db.admin, tenant).await > 0,
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
/// fires the fence at once, with no keeper running.
#[tokio::test]
async fn a_not_owner_write_fires_the_fence() {
    let Some(db) = test_db(false).await else {
        return;
    };
    let tenant = "fence-not-owner";
    let (store, _lease) = open_owned(&db.app, tenant, "proc-a", LONG_TTL).await;
    let fence = store.tenant_fence().expect("claimed");
    store.insert_event(&event(0)).await.unwrap();
    take_over(&db.admin, tenant).await;
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
    take_over(&db.admin, tenant).await;
    assert_not_owner(
        store.renew_tenant_lease(LONG_TTL).await,
        "a renewal after takeover",
    );
    assert!(fence.is_fenced());
}

/// A fence fired by the caller (or the keeper) refuses every later write
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

/// Dropping the keeper stops renewals, so the tenant is fenced: the
/// process can no longer promise to renew before the lease expires.
#[tokio::test]
async fn dropping_the_keeper_fires_the_fence() {
    let Some(db) = test_db(false).await else {
        return;
    };
    let (store, lease) = open_owned(&db.app, "fence-drop", "proc-a", LONG_TTL).await;
    let keeper = TenantLeaseKeeper::spawn(store.clone(), lease);
    let fence = keeper.fence();
    assert!(!fence.is_fenced());
    drop(keeper);
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

/// Wait until `n` sessions wait for a `tenant_writer` row lock.
async fn wait_for_lock_waiters(admin: &PgPool, n: usize) {
    tokio::time::timeout(test_timeout(Duration::from_secs(30)), async {
        while writer_lock_waiters(admin).await.len() < n {
            // timing-guard: poll interval while waiting for the writer to queue on the row lock
            tokio::time::sleep(Duration::from_millis(20)).await;
        }
    })
    .await
    .unwrap_or_else(|_| panic!("{n} writers wait for the writer lock"));
}

/// A write that passed the entry check and queued for the row lock before
/// the fence fired is refused under the lock, and `settled()` waits for it
/// to end: it holds the writer slot.
#[tokio::test]
async fn a_write_queued_before_the_fire_is_refused_and_settled_waits_for_it() {
    let Some(db) = test_db(false).await else {
        return;
    };
    let tenant = "fence-queued";
    let (store, _lease) = open_owned(&db.app, tenant, "proc-a", LONG_TTL).await;
    let fence = store.tenant_fence().expect("claimed");
    let holder = hold_writer_lock(&db.admin, tenant).await;
    let write = tokio::spawn({
        let store = store.clone();
        async move { store.insert_event(&event(0)).await }
    });
    wait_for_lock_waiters(&db.admin, 1).await;
    assert!(fence.fire("test: lost"));
    let settled = tokio::spawn({
        let fence = fence.clone();
        async move { fence.settled().await }
    });
    // timing-guard: give settled() a chance to (wrongly) return while the queued write holds the slot
    tokio::time::sleep(Duration::from_millis(200)).await;
    assert!(
        !settled.is_finished(),
        "the queued write holds the writer slot"
    );
    holder.rollback().await.unwrap();
    let message = assert_not_owner(
        tokio::time::timeout(test_timeout(Duration::from_secs(30)), write)
            .await
            .expect("the write ends")
            .unwrap(),
        "a write queued before the fire",
    );
    assert!(message.contains("fenced itself"), "{message}");
    tokio::time::timeout(test_timeout(Duration::from_secs(30)), settled)
        .await
        .expect("settles once the write ends")
        .unwrap();
    assert_eq!(event_rows(&db.admin, tenant).await, 0);
}

/// A healthy lease with a wide margin is renewed before its fence
/// deadline: the keeper never fences a healthy lease without renewing it.
#[tokio::test]
async fn the_keeper_renews_before_its_fence_deadline() {
    let Some(db) = test_db(false).await else {
        return;
    };
    let tenant = "fence-short-deadline";
    // Renewal due after 1.5 s, with 1.5 s to retry before the fence.
    let ttl = Duration::from_secs(4);
    let (store, lease) = open_owned(&db.app, tenant, "proc-a", ttl).await;
    let claimed = lease.expires_at;
    let keeper = TenantLeaseKeeper::spawn_with_margin(store, lease, ttl / 4);
    let fence = keeper.fence();
    // timing-guard: outlive two TTLs; with no renewal the fence would fire within one
    tokio::time::sleep(ttl * 2).await;
    assert!(!fence.is_fenced(), "{:?}", fence.reason());
    assert!(keeper.lost_reason().is_none());
    let expires_ms: i64 = sqlx::query_scalar(sqlx::AssertSqlSafe(format!(
        "SELECT (extract(epoch FROM expires_at) * 1000)::bigint FROM {TENANT_LEASE}"
    )))
    .bind(tenant)
    .fetch_one(&db.admin)
    .await
    .unwrap();
    assert!(
        i128::from(expires_ms) > claimed.unix_timestamp_nanos() / 1_000_000,
        "it renewed"
    );
}

/// A keeper started for a lease the process has since replaced with a
/// newer claim stops at once and never fences the newer claim.
#[tokio::test]
async fn a_keeper_for_a_stale_lease_leaves_the_new_claim_alone() {
    let Some(db) = test_db(false).await else {
        return;
    };
    let ttl = Duration::from_millis(500);
    let (store, stale) = open_owned(&db.app, "fence-stale", "proc-a", ttl).await;
    // timing-guard: let the short lease lapse by the database clock
    tokio::time::sleep(ttl * 2).await;
    let claim = store.claim_tenant("proc-a", LONG_TTL).await.unwrap();
    assert!(matches!(
        claim,
        intent_store::postgres::TenantClaim::TenantOwned(_)
    ));
    let current = store.tenant_fence().expect("claimed again");
    let mut keeper = TenantLeaseKeeper::spawn(store.clone(), stale);
    assert!(!keeper.fence().same_as(&current));
    let lost = tokio::time::timeout(test_timeout(Duration::from_secs(30)), keeper.lost())
        .await
        .expect("the stale keeper stops");
    assert!(matches!(lost, Error::NotOwner(_)), "{lost:?}");
    drop(keeper);
    assert!(!current.is_fenced(), "{:?}", current.reason());
    store.insert_event(&event(0)).await.unwrap();
}

/// The self-fence's budgeted steps fit inside the lease's fence margin, so
/// a stop that keeps to its budget ends before the lease can expire.
#[test]
fn the_self_fence_budget_fits_inside_the_fence_margin() {
    use intent_core::tenant_fence::{SELF_FENCE_BUDGETED_STEPS, SELF_FENCE_STEP_BUDGET};
    use intent_store::postgres::{LEASE_FENCE_MARGIN, LEASE_TTL};
    assert!(SELF_FENCE_STEP_BUDGET * SELF_FENCE_BUDGETED_STEPS < LEASE_FENCE_MARGIN);
    assert_eq!(LEASE_FENCE_MARGIN, LEASE_TTL / 6, "spawn's default margin");
}
