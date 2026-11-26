//! The fenced tenant lease: one orchestrator process owns a tenant at a
//! time. It is the store lease of scope `tenant`. A claim succeeds when the
//! tenant has no lease or its lease expired or was released, and bumps the
//! lease token (the tenant's epoch); every write then compares the tenant
//! lease, read once it holds the writer lock, with its store's claim, so a
//! write by a process that lost the tenant fails `Error::NotOwner` and
//! writes nothing, also when it waited behind the takeover for the writer
//! lock. Leases of other scopes do not move on a takeover.
//!
//! Each "process" here is a store opened through the `*_as_process` test
//! seams: its own process-local gates and claim, as between two daemons.

use std::time::Duration;

use intent_store::postgres::{
    self, PgPool, PostgresOptions, TenantClaim, TenantId, TenantLease, TenantLeaseKeeper,
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

/// The tenant lease row, after `SELECT <columns>`.
const TENANT_LEASE_ROW: &str =
    " FROM lease WHERE tenant_id = $1 AND scope = 'tenant' AND lease_key = ''";

fn tenant_lease_query(columns: &str) -> sqlx::AssertSqlSafe<String> {
    sqlx::AssertSqlSafe(format!("SELECT {columns}{TENANT_LEASE_ROW}"))
}

/// The tenant lease row as `(holder, token)`, read on the admin pool.
async fn owner_rows(admin: &PgPool, tenant: &str) -> Option<(Option<String>, i64)> {
    sqlx::query_as(tenant_lease_query("holder, token"))
        .bind(tenant)
        .fetch_optional(admin)
        .await
        .expect("read the tenant lease")
}

#[expect(clippy::unnecessary_wraps, reason = "compared with owner_rows")]
fn held_by(instance: &str, epoch: i64) -> Option<(Option<String>, i64)> {
    Some((Some(instance.to_owned()), epoch))
}

/// Wait until the tenant's lease has expired by the database clock.
async fn wait_for_expiry(admin: &PgPool, tenant: &str) {
    tokio::time::timeout(test_timeout(Duration::from_secs(30)), async {
        loop {
            let expired: bool =
                sqlx::query_scalar(tenant_lease_query("expires_at <= clock_timestamp()"))
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
    assert_eq!(owner_rows(&db.admin, tenant).await, held_by("proc-a", 1));
    // The owner writes, and the open provisioned the tenant under its claim.
    store.set_setting("userRules", "\"a\"").await.unwrap();
    assert!(store.daemon_id().await.is_ok());
}

/// A refused process reads how long the owner's lease has left (database
/// clock), without claiming or writing: `None` before any claim, at most
/// the TTL while it is live, zero once it lapsed.
#[tokio::test]
async fn a_refused_process_reads_how_long_the_lease_has_left() {
    let Some(db) = test_db(false).await else {
        return;
    };
    let tenant = "lease-expires-in";
    let expires_in =
        || Store::postgres_lease_expires_in(db.app.clone(), TenantId::new(tenant).unwrap());
    assert_eq!(expires_in().await.unwrap(), None);

    let (_owner, _) = open_owned(&db.app, tenant, "proc-a", LONG_TTL)
        .await
        .unwrap();
    let before = tenant_rows(&db.admin, tenant).await;
    // The typed startup open reports the live lease, claiming nothing.
    match Store::try_open_postgres_owned(
        db.app.clone(),
        TenantId::new(tenant).unwrap(),
        options(),
        "proc-b",
        LONG_TTL,
        intent_store::OrchestratorOnly::for_tests(),
    )
    .await
    .unwrap()
    {
        Err(held) => {
            assert_eq!(
                (held.tenant.as_str(), held.owner.as_str()),
                (tenant, "proc-a")
            );
            let message = held.into_error().to_string();
            assert!(message.contains("until"), "{message}");
        }
        Ok(_) => panic!("claimed an owned tenant"),
    }
    assert_eq!(tenant_rows(&db.admin, tenant).await, before);
    let left = expires_in().await.unwrap().expect("a claimed tenant");
    assert!(
        left > Duration::ZERO && left <= LONG_TTL,
        "{left:?} left of a {LONG_TTL:?} lease"
    );
    assert_eq!(tenant_rows(&db.admin, tenant).await, before);

    let short = "lease-expires-in-short";
    let (_lapsed, _) = open_owned(&db.app, short, "proc-a", SHORT_TTL)
        .await
        .unwrap();
    // timing-guard: outlive the short lease
    tokio::time::sleep(SHORT_TTL * 2).await;
    assert_eq!(
        Store::postgres_lease_expires_in(db.app.clone(), TenantId::new(short).unwrap())
            .await
            .unwrap(),
        Some(Duration::ZERO)
    );
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
    assert_eq!(owner_rows(&db.admin, tenant).await, held_by("proc-b", 2));
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
    let (row_epoch, row_expiry_ms): (i64, i64) = sqlx::query_as(tenant_lease_query(
        "token, (extract(epoch FROM expires_at) * 1000)::bigint",
    ))
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

/// A keeper's claim TTL: renewals come about every 1.25 s (halfway to the
/// fence deadline, a sixth of the TTL before expiry), with as long again to
/// retry before the fence fires.
const KEEPER_TTL: Duration = Duration::from_secs(3);

/// Wait until the tenant lease's expiry moves past `after` (epoch ms): a
/// renewal landed.
async fn wait_for_renewal(admin: &PgPool, tenant: &str, after: i64) -> i64 {
    tokio::time::timeout(test_timeout(Duration::from_secs(30)), async {
        loop {
            let now = lease_expiry_ms(admin, tenant).await;
            if now > after {
                return now;
            }
            // timing-guard: poll interval while waiting for the keeper's next renewal
            tokio::time::sleep(Duration::from_millis(20)).await;
        }
    })
    .await
    .expect("the keeper renews")
}

/// Another process takes the tenant lease, as a claim would.
async fn take_over(admin: &PgPool, tenant: &str) {
    sqlx::query(sqlx::AssertSqlSafe(format!(
        "UPDATE lease SET holder = 'proc-b', lease_id = 'taken', token = token + 1{}",
        TENANT_LEASE_ROW.replacen(" FROM lease", "", 1)
    )))
    .bind(tenant)
    .execute(admin)
    .await
    .expect("take the tenant lease over");
}

#[tokio::test]
async fn the_keeper_renews_and_reports_a_lost_lease() {
    let Some(db) = test_db(false).await else {
        return;
    };
    let tenant = "lease-keeper";
    let (store, lease) = open_owned(&db.app, tenant, "proc-a", KEEPER_TTL)
        .await
        .unwrap();
    let claimed = lease_expiry_ms(&db.admin, tenant).await;
    let mut keeper = TenantLeaseKeeper::spawn(store, lease);
    // It renews: the expiry moves forward.
    wait_for_renewal(&db.admin, tenant, claimed).await;
    assert!(keeper.lost_reason().is_none());

    // Another process takes the tenant: the next renewal fails and the
    // keeper surfaces it and stops.
    take_over(&db.admin, tenant).await;
    let lost = tokio::time::timeout(test_timeout(Duration::from_secs(30)), keeper.lost())
        .await
        .expect("the keeper reports the loss");
    match lost {
        Error::NotOwner(message) => assert!(
            message.contains("taken over"),
            "lost to the takeover, not a lapse: {message}"
        ),
        other => panic!("expected NotOwner, got {other:?}"),
    }
    assert!(keeper.fence().is_fenced());
}

/// The lease row's expiry has passed by the database clock.
async fn lease_expired(admin: &PgPool, tenant: &str) -> bool {
    sqlx::query_scalar(tenant_lease_query("expires_at <= clock_timestamp()"))
        .bind(tenant)
        .fetch_one(admin)
        .await
        .expect("read lease expiry")
}

/// The lease row's expiry, in epoch milliseconds.
async fn lease_expiry_ms(admin: &PgPool, tenant: &str) -> i64 {
    sqlx::query_scalar(tenant_lease_query(
        "(extract(epoch FROM expires_at) * 1000)::bigint",
    ))
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
    // The row is free and keeps its token, so the next claim's is higher.
    assert_eq!(owner_rows(&db.admin, tenant).await, Some((None, 1)));

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
    // fixed-deadline: allow — stays under the queued write's 60 s lock_attempt (options()), which is what keeps the release blocked
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
async fn the_keeper_releases_its_lease_after_the_fence_fired() {
    let Some(db) = test_db(false).await else {
        return;
    };
    let tenant = "lease-release-keeper";
    let (store, lease) = open_owned(&db.app, tenant, "proc-a", LONG_TTL)
        .await
        .unwrap();
    let epoch = lease.epoch;
    let keeper = TenantLeaseKeeper::spawn(store.clone(), lease);
    assert_eq!(keeper.epoch(), epoch);
    assert!(matches!(
        keeper.release().await,
        Err(Error::InvalidParams(_))
    ));
    keeper.fence().fire("stopping");
    assert!(keeper.release().await.unwrap());
    assert!(lease_expired(&db.admin, tenant).await);
    let (_next, next) = open_owned(&db.app, tenant, "proc-b", LONG_TTL)
        .await
        .expect("claimed at once");
    assert_eq!(next.epoch, epoch + 1);
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
    // 30 min; renewals are single statements well inside both, due before
    // half the TTL, and the fence margin is a sixth of it.
    assert!(postgres::LEASE_TTL <= Duration::from_secs(60));
    assert!(postgres::LEASE_TTL / 2 < Duration::from_secs(5 * 60));
    assert_eq!(postgres::LEASE_FENCE_MARGIN * 6, postgres::LEASE_TTL);
}

/// Sessions of `admin`'s database waiting for a `lease` row lock (a claim's
/// acquire or a renewal).
async fn lease_lock_waiters(admin: &PgPool) -> usize {
    let n: i64 = sqlx::query_scalar(
        "SELECT count(*) FROM pg_stat_activity WHERE wait_event_type = 'Lock' \
         AND datname = current_database() \
         AND (query LIKE '%INTO lease %' OR query LIKE '%UPDATE lease %')",
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
    sqlx::query(sqlx::AssertSqlSafe(format!(
        "SELECT 1{TENANT_LEASE_ROW} FOR UPDATE"
    )))
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
    assert_eq!(owner_rows(&db.admin, tenant).await, held_by("proc-b", 2));
}

/// Renewals that keep failing for another reason (here, a closed pool) are
/// retried until the lease's local deadline, then reported as a lapse.
#[tokio::test]
async fn the_keeper_reports_a_lapse_when_renewals_keep_failing() {
    let Some(db) = test_db(false).await else {
        return;
    };
    let tenant = "lease-lapse";
    let pool = postgres::connect(&db.url, 2).await.unwrap();
    let (store, lease) = open_owned(&pool, tenant, "proc-a", SHORT_TTL)
        .await
        .unwrap();
    let mut keeper = TenantLeaseKeeper::spawn(store, lease);
    pool.close().await;
    let lost = tokio::time::timeout(test_timeout(Duration::from_secs(30)), keeper.lost())
        .await
        .expect("the keeper reports the lapse");
    match lost {
        Error::NotOwner(message) => assert!(message.contains("renewal"), "{message}"),
        other => panic!("expected a NotOwner lapse, got {other:?}"),
    }
    assert!(keeper.lost_reason().is_some());
}

/// A `lost()` wait that is cancelled (as in a `select!` on shutdown) leaves
/// the keeper in place, and dropping the keeper stops renewals, so the
/// lease can lapse and another process take over.
#[tokio::test]
async fn dropping_the_keeper_after_a_cancelled_wait_stops_renewals() {
    let Some(db) = test_db(false).await else {
        return;
    };
    let tenant = "lease-drop";
    let (store, lease) = open_owned(&db.app, tenant, "proc-a", KEEPER_TTL)
        .await
        .unwrap();
    let claimed = lease_expiry_ms(&db.admin, tenant).await;
    let mut keeper = TenantLeaseKeeper::spawn(store, lease);
    let renewed = wait_for_renewal(&db.admin, tenant, claimed).await;
    // A cancelled wait: the keeper keeps running and can still report.
    assert!(
        tokio::time::timeout(test_timeout(Duration::from_millis(100)), keeper.lost())
            .await
            .is_err(),
        "the lease is held, so lost() is still pending"
    );
    wait_for_renewal(&db.admin, tenant, renewed).await;

    drop(keeper);
    // With no keeper the lease lapses (a renewal already on the wire when
    // the task was aborted can extend it once at most), and another
    // process takes the tenant over.
    wait_for_expiry(&db.admin, tenant).await;
    let (_next, next) = open_owned(&db.app, tenant, "proc-b", LONG_TTL)
        .await
        .expect("the lapsed lease is claimed");
    assert_eq!(next.epoch, 2);
}

/// The fence holds only at READ COMMITTED: under REPEATABLE READ a writer's
/// snapshot predates its lock wait and would show the old tenant lease, so
/// a writer at any other isolation level is refused before it writes.
#[tokio::test]
async fn a_writer_not_at_read_committed_is_refused() {
    let Some(db) = test_db(false).await else {
        return;
    };
    let tenant = "lease-isolation";
    let (owner, _) = open_owned(&db.app, tenant, "proc-a", LONG_TTL)
        .await
        .unwrap();
    owner.set_setting("userRules", "\"a\"").await.unwrap();
    let before = tenant_rows(&db.admin, tenant).await;
    // A database whose sessions default to REPEATABLE READ (test database
    // only): new connections inherit it.
    let name: String = sqlx::query_scalar("SELECT current_database()")
        .fetch_one(&db.admin)
        .await
        .unwrap();
    sqlx::query(sqlx::AssertSqlSafe(format!(
        "ALTER DATABASE \"{name}\" SET default_transaction_isolation = 'repeatable read'"
    )))
    .execute(&db.admin)
    .await
    .expect("set the database default");
    let repeatable = postgres::connect(&db.url, 2).await.unwrap();
    match open_owned(&repeatable, tenant, "proc-b", LONG_TTL).await {
        Err(Error::Internal(message)) => {
            assert!(message.contains("READ COMMITTED"), "{message}");
        }
        Err(other) => panic!("expected the isolation refusal, got {other:?}"),
        Ok(_) => panic!("a REPEATABLE READ writer claimed the tenant"),
    }
    assert_eq!(tenant_rows(&db.admin, tenant).await, before);
}

/// How many processes race for one tenant.
const CONTENDERS: usize = 8;

/// Processes that claim the tenant at once: exactly one owns it, at the
/// first epoch; every other sees `AlreadyOwned` naming the winner.
#[tokio::test(flavor = "multi_thread", worker_threads = 4)]
async fn concurrent_claims_have_one_winner() {
    let Some(db) = test_db(false).await else {
        return;
    };
    let tenant = "lease-race";
    let mut stores = Vec::new();
    for n in 0..CONTENDERS {
        stores.push(
            open_unclaimed(&db.app, tenant, &format!("proc-{n}"))
                .await
                .unwrap(),
        );
    }
    let start = std::sync::Arc::new(tokio::sync::Barrier::new(CONTENDERS));
    let claims: Vec<_> = stores
        .into_iter()
        .enumerate()
        .map(|(n, store)| {
            let start = std::sync::Arc::clone(&start);
            tokio::spawn(async move {
                start.wait().await;
                let claim = store.claim_tenant(&format!("proc-{n}"), LONG_TTL).await;
                (n, store, claim.expect("a claim answers"))
            })
        })
        .collect();
    let mut winners = Vec::new();
    let mut refused = Vec::new();
    for claim in claims {
        match claim.await.unwrap() {
            (n, store, TenantClaim::TenantOwned(lease)) => winners.push((n, store, lease)),
            (_, _, TenantClaim::AlreadyOwned { owner, .. }) => refused.push(owner),
        }
    }
    assert_eq!(winners.len(), 1, "one claim wins");
    let (n, store, lease) = winners.pop().unwrap();
    let winner = format!("proc-{n}");
    assert_eq!(lease.epoch, 1);
    assert!(refused.iter().all(|owner| *owner == winner), "{refused:?}");
    assert_eq!(owner_rows(&db.admin, tenant).await, held_by(&winner, 1));
    store.set_setting("userRules", "\"won\"").await.unwrap();
}

/// A tenant takeover moves only the tenant lease: a lease of another scope
/// the old owner holds keeps its holder, token and expiry, stays held and
/// renewable, and the new owner cannot acquire it.
#[tokio::test]
async fn a_takeover_moves_only_the_tenant_lease() {
    let Some(db) = test_db(false).await else {
        return;
    };
    let tenant = "lease-takeover-scopes";
    let (old, _) = open_owned(&db.app, tenant, "proc-a", SHORT_TTL)
        .await
        .unwrap();
    let mut link = old
        .acquire_lease("machine-link", "m1", LONG_TTL)
        .await
        .unwrap()
        .expect("a free key");
    let others = || async {
        sqlx::query_as::<_, (String, String, Option<String>, Option<String>, i64, i64)>(
            "SELECT scope, lease_key, holder, lease_id, token, \
             (extract(epoch FROM expires_at) * 1000)::bigint \
             FROM lease WHERE tenant_id = $1 AND scope <> 'tenant' ORDER BY scope, lease_key",
        )
        .bind(tenant)
        .fetch_all(&db.admin)
        .await
        .expect("read the other leases")
    };
    let before = others().await;
    assert_eq!(before.len(), 1);
    wait_for_expiry(&db.admin, tenant).await;

    let (new, second) = open_owned(&db.app, tenant, "proc-b", LONG_TTL)
        .await
        .unwrap();
    assert_eq!(second.epoch, 2);
    assert_eq!(others().await, before, "the takeover left the other leases");
    assert!(new.lease_is_held(&link).await.unwrap());
    assert!(new
        .acquire_lease("machine-link", "m1", LONG_TTL)
        .await
        .unwrap()
        .is_none());
    assert!(
        old.renew_lease(&mut link, LONG_TTL).await.unwrap(),
        "the old owner keeps its link lease"
    );
    assert_eq!(link.token(), 1);
    // The tenant fence still refuses the old owner's tenant writes.
    assert_not_owner(
        old.set_setting("userRules", "\"stale\"").await,
        "the old owner's write",
    );
}
