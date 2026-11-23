//! The fenced tenant lease: one orchestrator process owns a tenant at a
//! time. A claim succeeds when the tenant has no lease or its lease expired,
//! and bumps the tenant's epoch; every write then compares the locked
//! `tenant_writer` row's owner and epoch with its store's claim, so a write
//! by a process that lost the tenant fails `Error::NotOwner` and writes
//! nothing, also when it waited behind the takeover for the writer lock.
//!
//! Each "process" here is a store opened through the `*_as_process` test
//! seams: its own process-local gates and claim, as between two daemons.

use std::time::Duration;

use intent_store::postgres::{
    self, LeaseHeartbeat, LeaseState, PgPool, PostgresOptions, TenantClaim, TenantId, TenantLease,
    WriterLimits,
};
use intent_store::{Error, Store};
use intentd_test_support::test_timeout;

use super::harness::{
    hold_writer_lock, sqlite_store, tenant_rows, test_db, writer_lock_waiters, TestDb,
};

const LONG_TTL: Duration = Duration::from_secs(60);
const SHORT_TTL: Duration = Duration::from_millis(500);

/// Patient writers: one lock attempt outlasts every wait in these tests, so
/// a queued writer keeps its place in the row lock's queue.
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

/// Open `tenant` as process `process` (instance id = process name) and claim
/// it for `ttl`.
async fn open_owned(
    pool: &PgPool,
    tenant: &str,
    process: &str,
    ttl: Duration,
) -> intent_store::Result<(Store, TenantLease)> {
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
}

/// An unclaimed store of `tenant` in process `process`.
async fn open_unclaimed(pool: &PgPool, tenant: &str, process: &str) -> intent_store::Result<Store> {
    Store::open_postgres_as_process(
        pool.clone(),
        TenantId::new(tenant).unwrap(),
        options(),
        process,
        intent_store::OrchestratorOnly::for_tests(),
    )
    .await
}

/// The tenant's lease and writer rows as `(owner, epoch)` pairs, read on
/// the admin pool.
async fn owner_rows(
    admin: &PgPool,
    tenant: &str,
) -> (Option<(String, i64)>, (Option<String>, i64)) {
    let lease =
        sqlx::query_as("SELECT owner_instance, epoch FROM tenant_lease WHERE tenant_id = $1")
            .bind(tenant)
            .fetch_optional(admin)
            .await
            .expect("read tenant_lease");
    let writer =
        sqlx::query_as("SELECT owner_instance, epoch FROM tenant_writer WHERE tenant_id = $1")
            .bind(tenant)
            .fetch_one(admin)
            .await
            .expect("read tenant_writer");
    (lease, writer)
}

/// Wait until the tenant's lease has expired by the database clock.
async fn wait_for_expiry(admin: &PgPool, tenant: &str) {
    tokio::time::timeout(test_timeout(Duration::from_secs(30)), async {
        loop {
            let expired: bool = sqlx::query_scalar(
                "SELECT expires_at <= clock_timestamp() FROM tenant_lease WHERE tenant_id = $1",
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
fn assert_not_owner<T>(result: intent_store::Result<T>, what: &str) -> String {
    match result {
        Err(Error::NotOwner(message)) => message,
        Err(other) => panic!("{what}: expected NotOwner, got {other:?}"),
        Ok(_) => panic!("{what}: expected NotOwner, but it succeeded"),
    }
}

#[tokio::test]
async fn the_first_claim_owns_the_tenant_at_epoch_one() {
    let Some(db) = test_db(false).await else {
        return;
    };
    let tenant = "lease-first";
    let (store, lease) = open_owned(&db.app, tenant, "proc-a", LONG_TTL)
        .await
        .unwrap();
    assert_eq!(lease.instance, "proc-a");
    assert_eq!(lease.epoch, 1);
    assert!(lease.expires_at > time::OffsetDateTime::now_utc());
    assert_eq!(
        owner_rows(&db.admin, tenant).await,
        (
            Some(("proc-a".to_owned(), 1)),
            (Some("proc-a".to_owned()), 1)
        )
    );
    // The owner writes, and the open provisioned the tenant under its claim.
    store.set_setting("userRules", "\"a\"").await.unwrap();
    assert!(store.daemon_id().await.is_ok());
}

#[tokio::test]
async fn a_second_claim_fails_names_the_owner_and_changes_no_row() {
    let Some(db) = test_db(false).await else {
        return;
    };
    let tenant = "lease-second";
    let (owner, lease) = open_owned(&db.app, tenant, "proc-a", LONG_TTL)
        .await
        .unwrap();
    owner.set_setting("userRules", "\"a\"").await.unwrap();
    let before = tenant_rows(&db.admin, tenant).await;

    // A second process's startup open refuses, naming owner and expiry.
    let message = assert_not_owner(
        open_owned(&db.app, tenant, "proc-b", LONG_TTL).await,
        "a second owned open",
    );
    assert!(message.contains("proc-a"), "{message}");
    assert!(message.contains("until"), "{message}");
    assert_eq!(tenant_rows(&db.admin, tenant).await, before);

    // A claim through a store the owner's own process holds is refused too:
    // the lease has not expired.
    let again = owner.claim_tenant("proc-b", LONG_TTL).await.unwrap();
    match again {
        TenantClaim::AlreadyOwned {
            owner: by,
            expires_at,
        } => {
            assert_eq!(by, "proc-a");
            assert_eq!(expires_at, lease.expires_at);
        }
        TenantClaim::TenantOwned(lease) => panic!("claimed an owned tenant: {lease:?}"),
    }
    assert_eq!(tenant_rows(&db.admin, tenant).await, before);
    // The owner still writes.
    owner.set_setting("userRules", "\"a2\"").await.unwrap();
}

#[tokio::test]
async fn an_unclaimed_store_cannot_write_to_an_owned_tenant() {
    let Some(db) = test_db(false).await else {
        return;
    };
    let tenant = "lease-unclaimed";
    // Unclaimed stores write freely while no process owns the tenant.
    let unclaimed = open_unclaimed(&db.app, tenant, "proc-x").await.unwrap();
    unclaimed.set_setting("userRules", "\"x\"").await.unwrap();

    let (_owner, _) = open_owned(&db.app, tenant, "proc-a", LONG_TTL)
        .await
        .unwrap();
    let before = tenant_rows(&db.admin, tenant).await;
    assert_not_owner(
        unclaimed.set_setting("userRules", "\"y\"").await,
        "an unclaimed write",
    );
    // A fresh unclaimed open provisions under the writer lock: refused too.
    assert_not_owner(
        open_unclaimed(&db.app, tenant, "proc-y").await,
        "an unclaimed open",
    );
    assert_eq!(tenant_rows(&db.admin, tenant).await, before);
}

#[tokio::test]
async fn after_the_owner_stops_renewing_another_takes_over_and_the_old_owner_is_fenced() {
    let Some(db) = test_db(false).await else {
        return;
    };
    let tenant = "lease-takeover";
    let (old, first) = open_owned(&db.app, tenant, "proc-a", SHORT_TTL)
        .await
        .unwrap();
    old.set_setting("userRules", "\"a\"").await.unwrap();
    wait_for_expiry(&db.admin, tenant).await;

    let (new, second) = open_owned(&db.app, tenant, "proc-b", LONG_TTL)
        .await
        .unwrap();
    assert_eq!(second.epoch, first.epoch + 1);
    assert_eq!(second.instance, "proc-b");
    assert_eq!(
        owner_rows(&db.admin, tenant).await,
        (
            Some(("proc-b".to_owned(), 2)),
            (Some("proc-b".to_owned()), 2)
        )
    );
    let before = tenant_rows(&db.admin, tenant).await;

    // The old owner's write fails and writes nothing.
    let message = assert_not_owner(
        old.set_setting("userRules", "\"stale\"").await,
        "the old owner's write",
    );
    assert!(message.contains("proc-b"), "{message}");
    // So does its renewal: the lease is someone else's now.
    assert_not_owner(
        old.renew_tenant_lease(LONG_TTL).await,
        "the old owner's renewal",
    );
    assert_eq!(tenant_rows(&db.admin, tenant).await, before);

    // The new owner writes and reads its tenant as the old one left it.
    assert_eq!(
        new.get_setting("userRules").await.unwrap().as_deref(),
        Some("\"a\"")
    );
    new.set_setting("userRules", "\"b\"").await.unwrap();
}

/// The fence holds under READ COMMITTED's re-read: a write that queued for
/// the writer lock before a takeover, and gets it after the takeover
/// commits, sees the new owner and fails.
#[tokio::test]
async fn a_write_queued_behind_the_takeover_fails_not_owner() {
    let Some(db) = test_db(false).await else {
        return;
    };
    let tenant = "lease-queued";
    let (old, _) = open_owned(&db.app, tenant, "proc-a", SHORT_TTL)
        .await
        .unwrap();
    old.set_setting("userRules", "\"a\"").await.unwrap();
    wait_for_expiry(&db.admin, tenant).await;

    // Hold the writer lock so both queue: the takeover first, then the old
    // owner's write.
    let holder = hold_writer_lock(&db.admin, tenant).await;
    let pool = db.app.clone();
    let takeover = tokio::spawn(async move { open_owned(&pool, tenant, "proc-b", LONG_TTL).await });
    wait_for_lock_waiters(&db, 1).await;
    let writer = old.clone();
    let stale = tokio::spawn(async move { writer.set_setting("userRules", "\"stale\"").await });
    wait_for_lock_waiters(&db, 2).await;
    holder.rollback().await.expect("release the writer lock");

    let (new, lease) = takeover.await.unwrap().expect("the takeover claims");
    assert_eq!(lease.epoch, 2);
    assert_not_owner(stale.await.unwrap(), "the queued stale write");
    assert_eq!(
        new.get_setting("userRules").await.unwrap().as_deref(),
        Some("\"a\"")
    );
}

#[tokio::test]
async fn renew_extends_the_expiry_and_keeps_the_epoch() {
    let Some(db) = test_db(false).await else {
        return;
    };
    let tenant = "lease-renew";
    let (store, lease) = open_owned(&db.app, tenant, "proc-a", SHORT_TTL)
        .await
        .unwrap();
    let renewed = store.renew_tenant_lease(LONG_TTL).await.unwrap();
    assert!(
        renewed > lease.expires_at + Duration::from_secs(30),
        "{renewed} vs {}",
        lease.expires_at
    );
    let (row_epoch, row_expiry_ms): (i64, i64) = sqlx::query_as(
        "SELECT epoch, (extract(epoch FROM expires_at) * 1000)::bigint FROM tenant_lease \
         WHERE tenant_id = $1",
    )
    .bind(tenant)
    .fetch_one(&db.admin)
    .await
    .unwrap();
    assert_eq!(row_epoch, 1);
    assert_eq!(
        i128::from(row_expiry_ms),
        renewed.unix_timestamp_nanos() / 1_000_000
    );
    // Renewed past the short TTL, nobody can take over.
    // timing-guard: outlive the short TTL the claim started with, to prove the renewal (not the TTL) holds the lease
    tokio::time::sleep(SHORT_TTL * 2).await;
    assert_not_owner(
        open_owned(&db.app, tenant, "proc-b", LONG_TTL).await,
        "a claim of a renewed lease",
    );
}

#[tokio::test]
async fn an_unclaimed_store_has_no_lease_to_renew() {
    let Some(db) = test_db(false).await else {
        return;
    };
    let store = open_unclaimed(&db.app, "lease-none", "proc-a")
        .await
        .unwrap();
    assert_not_owner(
        store.renew_tenant_lease(LONG_TTL).await,
        "renewing without a claim",
    );
}

#[tokio::test]
async fn the_heartbeat_renews_and_reports_a_lost_lease() {
    let Some(db) = test_db(false).await else {
        return;
    };
    let tenant = "lease-heartbeat";
    let (store, lease) = open_owned(&db.app, tenant, "proc-a", LONG_TTL)
        .await
        .unwrap();
    let mut heartbeat = LeaseHeartbeat::spawn(store, &lease, LONG_TTL, Duration::from_millis(50));
    let mut state = heartbeat.state();
    // It renews: the expiry moves forward.
    tokio::time::timeout(test_timeout(Duration::from_secs(30)), async {
        loop {
            state.changed().await.expect("heartbeat runs");
            if let LeaseState::Held { expires_at } = *state.borrow() {
                if expires_at > lease.expires_at {
                    return;
                }
            }
        }
    })
    .await
    .expect("the heartbeat renews");

    // Another process takes the tenant (as a takeover would): the next
    // renewal fails and the heartbeat surfaces it and stops.
    sqlx::raw_sql(sqlx::AssertSqlSafe(format!(
        "BEGIN; \
         UPDATE tenant_writer SET owner_instance = 'proc-b', epoch = epoch + 1 \
         WHERE tenant_id = '{tenant}'; \
         UPDATE tenant_lease SET owner_instance = 'proc-b', epoch = epoch + 1 \
         WHERE tenant_id = '{tenant}'; \
         COMMIT;"
    )))
    .execute(&db.admin)
    .await
    .unwrap();
    let lost = tokio::time::timeout(test_timeout(Duration::from_secs(30)), heartbeat.lost())
        .await
        .expect("the heartbeat reports the loss");
    assert!(matches!(lost, Error::NotOwner(_)), "{lost:?}");
    assert!(matches!(*heartbeat.state().borrow(), LeaseState::Lost(_)));
}

/// The lease row's expiry has passed by the database clock.
async fn lease_expired(admin: &PgPool, tenant: &str) -> bool {
    sqlx::query_scalar(
        "SELECT expires_at <= clock_timestamp() FROM tenant_lease WHERE tenant_id = $1",
    )
    .bind(tenant)
    .fetch_one(admin)
    .await
    .expect("read lease expiry")
}

/// The lease row's expiry, in epoch milliseconds.
async fn lease_expiry_ms(admin: &PgPool, tenant: &str) -> i64 {
    sqlx::query_scalar(
        "SELECT (extract(epoch FROM expires_at) * 1000)::bigint FROM tenant_lease \
         WHERE tenant_id = $1",
    )
    .bind(tenant)
    .fetch_one(admin)
    .await
    .expect("read lease expiry")
}

#[tokio::test]
async fn a_released_lease_is_claimed_at_once_with_the_next_epoch() {
    let Some(db) = test_db(false).await else {
        return;
    };
    let tenant = "lease-release";
    let (old, first) = open_owned(&db.app, tenant, "proc-a", LONG_TTL)
        .await
        .unwrap();
    old.set_setting("userRules", "\"a\"").await.unwrap();

    // Not while the claim still serves the tenant: its fence has not fired.
    assert!(matches!(
        old.release_tenant_lease(first.epoch).await,
        Err(Error::InvalidParams(_))
    ));
    // Not a claim this process does not hold.
    assert_not_owner(
        old.release_tenant_lease(first.epoch + 1).await,
        "a release at another epoch",
    );
    assert!(!lease_expired(&db.admin, tenant).await);

    let fence = old.tenant_fence().expect("claimed");
    fence.fire("stopping");
    assert!(old.release_tenant_lease(first.epoch).await.unwrap());
    assert!(lease_expired(&db.admin, tenant).await);
    // The row still names the old claim until someone claims.
    assert_eq!(
        owner_rows(&db.admin, tenant).await,
        (
            Some(("proc-a".to_owned(), 1)),
            (Some("proc-a".to_owned()), 1)
        )
    );

    // Long before the 60 s TTL: another process claims at once, epoch + 1.
    let (new, second) = open_owned(&db.app, tenant, "proc-b", LONG_TTL)
        .await
        .expect("a released lease is free at once");
    assert_eq!(second.epoch, first.epoch + 1);
    assert_eq!(
        new.get_setting("userRules").await.unwrap().as_deref(),
        Some("\"a\"")
    );
    new.set_setting("userRules", "\"b\"").await.unwrap();
    assert_not_owner(
        old.set_setting("userRules", "\"stale\"").await,
        "the released owner's write",
    );
}

#[tokio::test]
async fn a_release_by_a_process_that_lost_the_lease_changes_nothing() {
    let Some(db) = test_db(false).await else {
        return;
    };
    let tenant = "lease-release-lost";
    let (old, first) = open_owned(&db.app, tenant, "proc-a", SHORT_TTL)
        .await
        .unwrap();
    wait_for_expiry(&db.admin, tenant).await;
    let (_new, second) = open_owned(&db.app, tenant, "proc-b", LONG_TTL)
        .await
        .unwrap();
    let rows = owner_rows(&db.admin, tenant).await;
    let expiry = lease_expiry_ms(&db.admin, tenant).await;
    let before = tenant_rows(&db.admin, tenant).await;

    old.tenant_fence().expect("claimed").fire("stopping");
    assert!(
        !old.release_tenant_lease(first.epoch).await.unwrap(),
        "the lease is proc-b's now"
    );
    assert_eq!(owner_rows(&db.admin, tenant).await, rows);
    assert_eq!(lease_expiry_ms(&db.admin, tenant).await, expiry);
    assert_eq!(tenant_rows(&db.admin, tenant).await, before);
    assert!(!lease_expired(&db.admin, tenant).await);
    // So proc-b's lease still holds against a third claimant.
    assert_not_owner(
        open_owned(&db.app, tenant, "proc-c", LONG_TTL).await,
        "a claim after a stale release",
    );
    assert_eq!(second.epoch, first.epoch + 1);
}

#[tokio::test]
async fn a_release_after_expiry_keeps_the_earlier_expiry() {
    let Some(db) = test_db(false).await else {
        return;
    };
    let tenant = "lease-release-expired";
    let (old, first) = open_owned(&db.app, tenant, "proc-a", SHORT_TTL)
        .await
        .unwrap();
    wait_for_expiry(&db.admin, tenant).await;
    let expiry = lease_expiry_ms(&db.admin, tenant).await;
    old.tenant_fence().expect("claimed").fire("stopping");
    // Still this claim's lease (no one claimed): released, but never moved
    // later than it already lapsed.
    assert!(old.release_tenant_lease(first.epoch).await.unwrap());
    assert_eq!(lease_expiry_ms(&db.admin, tenant).await, expiry);
    let (_new, second) = open_owned(&db.app, tenant, "proc-b", LONG_TTL)
        .await
        .unwrap();
    assert_eq!(second.epoch, first.epoch + 1);
}

/// A write that passed the fence before it fired is still open: the
/// release waits for it to end, so it cannot land under the next owner.
#[tokio::test]
async fn a_release_waits_for_a_write_that_passed_the_fence() {
    let Some(db) = test_db(false).await else {
        return;
    };
    let tenant = "lease-release-settle";
    let (old, first) = open_owned(&db.app, tenant, "proc-a", LONG_TTL)
        .await
        .unwrap();
    // The write takes the writer slot, then queues on the held row lock.
    let holder = hold_writer_lock(&db.admin, tenant).await;
    let writer = old.clone();
    let write = tokio::spawn(async move { writer.set_setting("userRules", "\"late\"").await });
    wait_for_lock_waiters(&db, 1).await;

    old.tenant_fence().expect("claimed").fire("stopping");
    let store = old.clone();
    let mut pending = tokio::spawn(async move { store.release_tenant_lease(first.epoch).await });
    // timing-guard: bounded window in which the release must stay blocked behind the open write
    let early = tokio::time::timeout(Duration::from_millis(300), &mut pending).await;
    assert!(early.is_err(), "released while a write was still open");
    assert!(!lease_expired(&db.admin, tenant).await);

    holder.rollback().await.expect("release the writer lock");
    // The queued write sees the fired fence once it holds the lock.
    assert_not_owner(write.await.unwrap(), "the write behind the fence");
    let released = tokio::time::timeout(test_timeout(Duration::from_secs(30)), pending)
        .await
        .expect("the release finishes once the write ended")
        .unwrap();
    assert!(released.unwrap());
    assert!(lease_expired(&db.admin, tenant).await);
}

#[tokio::test]
async fn the_heartbeat_releases_its_lease_after_the_fence_fired() {
    let Some(db) = test_db(false).await else {
        return;
    };
    let tenant = "lease-release-heartbeat";
    let (store, lease) = open_owned(&db.app, tenant, "proc-a", LONG_TTL)
        .await
        .unwrap();
    let heartbeat = LeaseHeartbeat::spawn(
        store.clone(),
        &lease,
        LONG_TTL,
        postgres::LEASE_RENEW_INTERVAL,
    );
    assert!(matches!(
        heartbeat.release().await,
        Err(Error::InvalidParams(_))
    ));
    heartbeat.fence().fire("stopping");
    assert!(heartbeat.release().await.unwrap());
    assert!(lease_expired(&db.admin, tenant).await);
    let (_next, next) = open_owned(&db.app, tenant, "proc-b", LONG_TTL)
        .await
        .expect("claimed at once");
    assert_eq!(next.epoch, lease.epoch + 1);
}

#[tokio::test]
async fn sqlite_stores_have_no_tenant_lease() {
    let (store, _dir) = sqlite_store().await;
    assert!(matches!(
        store.claim_tenant("proc-a", LONG_TTL).await,
        Err(Error::InvalidParams(_))
    ));
    assert!(matches!(
        store.renew_tenant_lease(LONG_TTL).await,
        Err(Error::InvalidParams(_))
    ));
    assert!(matches!(
        store.release_tenant_lease(1).await,
        Err(Error::InvalidParams(_))
    ));
    // Writes need no claim on SQLite.
    store.set_setting("userRules", "\"s\"").await.unwrap();
    assert_eq!(
        store.get_setting("userRules").await.unwrap().as_deref(),
        Some("\"s\"")
    );
}

#[test]
fn the_lease_constants_suit_the_neki_router_timeouts() {
    // Neki's router ends a transaction idle for 5 min and a session idle for
    // 30 min; renewals are single statements well inside both, and a
    // renewal is due several times per TTL.
    assert!(postgres::LEASE_TTL <= Duration::from_secs(60));
    assert!(postgres::LEASE_RENEW_INTERVAL * 3 <= postgres::LEASE_TTL);
    assert!(postgres::LEASE_RENEW_INTERVAL < Duration::from_secs(5 * 60));
}

/// Sessions of `admin`'s database waiting for a `tenant_lease` row lock.
async fn lease_lock_waiters(admin: &PgPool) -> usize {
    let n: i64 = sqlx::query_scalar(
        "SELECT count(*) FROM pg_stat_activity WHERE wait_event_type = 'Lock' \
         AND datname = current_database() AND query LIKE '%tenant_lease%'",
    )
    .fetch_one(admin)
    .await
    .expect("count lease lock waiters");
    usize::try_from(n).expect("a count")
}

async fn wait_for_lease_waiters(admin: &PgPool, n: usize) {
    tokio::time::timeout(test_timeout(Duration::from_secs(30)), async {
        while lease_lock_waiters(admin).await < n {
            // timing-guard: poll interval while waiting for lease lock waiters in pg_stat_activity
            tokio::time::sleep(Duration::from_millis(20)).await;
        }
    })
    .await
    .unwrap_or_else(|_| panic!("{n} sessions wait for the lease row lock"));
}

/// When the lease was last renewed, by the database clock (epoch ms).
async fn renewed_at(admin: &PgPool, tenant: &str) -> i64 {
    sqlx::query_scalar(
        "SELECT (extract(epoch FROM renewed_at) * 1000)::bigint FROM tenant_lease \
         WHERE tenant_id = $1",
    )
    .bind(tenant)
    .fetch_one(admin)
    .await
    .expect("read renewed_at")
}

/// Until another process claims, an expired lease is still this process's:
/// nobody else has written, so renewing it is safe and keeps the tenant.
#[tokio::test]
async fn a_renewal_after_expiry_but_before_a_takeover_keeps_the_tenant() {
    let Some(db) = test_db(false).await else {
        return;
    };
    let tenant = "lease-late-renew";
    let (store, _) = open_owned(&db.app, tenant, "proc-a", SHORT_TTL)
        .await
        .unwrap();
    wait_for_expiry(&db.admin, tenant).await;
    store.renew_tenant_lease(LONG_TTL).await.unwrap();
    assert_not_owner(
        open_owned(&db.app, tenant, "proc-b", LONG_TTL).await,
        "a claim of a renewed lease",
    );
    store.set_setting("userRules", "\"a\"").await.unwrap();
}

/// A renewal racing a real takeover: the claim holds the lease row lock, the
/// renewal waits for it, then matches no row (the epoch moved) and fails.
#[tokio::test]
async fn a_renewal_queued_behind_a_takeover_fails_not_owner() {
    let Some(db) = test_db(false).await else {
        return;
    };
    let tenant = "lease-renew-race";
    let (old, _) = open_owned(&db.app, tenant, "proc-a", SHORT_TTL)
        .await
        .unwrap();
    wait_for_expiry(&db.admin, tenant).await;

    // Hold the lease row so the takeover queues on it first, then the renewal.
    let mut holder = db.admin.begin().await.unwrap();
    sqlx::query("SELECT 1 FROM tenant_lease WHERE tenant_id = $1 FOR UPDATE")
        .bind(tenant)
        .fetch_one(&mut *holder)
        .await
        .unwrap();
    let pool = db.app.clone();
    let takeover = tokio::spawn(async move { open_owned(&pool, tenant, "proc-b", LONG_TTL).await });
    wait_for_lease_waiters(&db.admin, 1).await;
    let renewer = old.clone();
    let renewal = tokio::spawn(async move { renewer.renew_tenant_lease(LONG_TTL).await });
    wait_for_lease_waiters(&db.admin, 2).await;
    holder.rollback().await.unwrap();

    let (_new, lease) = takeover.await.unwrap().expect("the takeover claims");
    assert_eq!(lease.epoch, 2);
    assert_not_owner(renewal.await.unwrap(), "the queued renewal");
    assert_eq!(
        owner_rows(&db.admin, tenant).await,
        (
            Some(("proc-b".to_owned(), 2)),
            (Some("proc-b".to_owned()), 2)
        )
    );
}

/// Renewals that keep failing for another reason (here, a closed pool) are
/// retried until the lease's local deadline, then reported as a lapse.
#[tokio::test]
async fn the_heartbeat_reports_a_lapse_when_renewals_keep_failing() {
    let Some(db) = test_db(false).await else {
        return;
    };
    let tenant = "lease-lapse";
    let pool = postgres::connect(&db.url, 2).await.unwrap();
    let (store, lease) = open_owned(&pool, tenant, "proc-a", SHORT_TTL)
        .await
        .unwrap();
    let mut heartbeat = LeaseHeartbeat::spawn(store, &lease, SHORT_TTL, Duration::from_millis(50));
    pool.close().await;
    let lost = tokio::time::timeout(test_timeout(Duration::from_secs(30)), heartbeat.lost())
        .await
        .expect("the heartbeat reports the lapse");
    match lost {
        Error::NotOwner(message) => assert!(message.contains("lapsed"), "{message}"),
        other => panic!("expected a NotOwner lapse, got {other:?}"),
    }
    assert!(matches!(*heartbeat.state().borrow(), LeaseState::Lost(_)));
}

/// A `lost()` wait that is cancelled (as in a `select!` on shutdown) leaves
/// the heartbeat in place, and dropping the heartbeat stops renewals, so
/// the lease can lapse and another process take over.
#[tokio::test]
async fn dropping_the_heartbeat_after_a_cancelled_wait_stops_renewals() {
    let Some(db) = test_db(false).await else {
        return;
    };
    let tenant = "lease-drop";
    let (store, lease) = open_owned(&db.app, tenant, "proc-a", LONG_TTL)
        .await
        .unwrap();
    let interval = Duration::from_millis(50);
    let mut heartbeat = LeaseHeartbeat::spawn(store, &lease, LONG_TTL, interval);
    let mut state = heartbeat.state();
    tokio::time::timeout(test_timeout(Duration::from_secs(30)), state.changed())
        .await
        .expect("a renewal")
        .expect("heartbeat runs");
    // A cancelled wait: the heartbeat keeps running and can still report.
    assert!(
        tokio::time::timeout(test_timeout(interval * 2), heartbeat.lost())
            .await
            .is_err(),
        "the lease is held, so lost() is still pending"
    );
    tokio::time::timeout(test_timeout(Duration::from_secs(30)), state.changed())
        .await
        .expect("renewals go on after the cancelled wait")
        .expect("heartbeat runs");

    drop(heartbeat);
    // timing-guard: let a renewal already on the wire when the task was aborted settle
    tokio::time::sleep(interval * 2).await;
    let before = renewed_at(&db.admin, tenant).await;
    // timing-guard: several renew intervals with no heartbeat, to see that none renews
    tokio::time::sleep(interval * 10).await;
    assert_eq!(renewed_at(&db.admin, tenant).await, before);
}
