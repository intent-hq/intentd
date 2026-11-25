//! Leases on Postgres: processes that share one tenant hold a leased unit
//! of work one at a time. See `lease_repo.rs` for the rules.

use std::sync::Arc;
use std::time::Duration;

use intent_store::{Error, Store};
use tokio::sync::Barrier;

use crate::harness::TestDb;

const TTL: Duration = Duration::from_secs(60);
const SHORT_TTL: Duration = Duration::from_millis(400);
const SCOPE: &str = "token-usage-scan";

/// A store of `tenant` opened as if by another process named `process`: its
/// own process token and gates, so only the database orders it.
async fn open_as_process(db: &TestDb, tenant: &str, process: &str) -> Store {
    Store::open_postgres_as_process(
        db.app.clone(),
        intent_store::postgres::TenantId::new(tenant).unwrap(),
        intent_store::postgres::PostgresOptions::default(),
        process,
        intent_store::OrchestratorOnly::for_tests(),
    )
    .await
    .expect("open tenant store as another process")
}

/// Two processes race one key: exactly one wins; the holder renews and the
/// other keeps losing; release frees the key at once with a higher token
/// for the next holder; a lease is not re-entrant; other keys and other
/// tenants are other work; a non-holder's release is a no-op.
#[tokio::test(flavor = "multi_thread", worker_threads = 4)]
async fn two_processes_race_one_key_and_release_frees_it() {
    let Some(db) = crate::harness::test_db(false).await else {
        return;
    };
    let tenant = "leases";
    let a = open_as_process(&db, tenant, "process-a").await;
    let b = open_as_process(&db, tenant, "process-b").await;

    // Every racer is spawned before any is awaited and all wait at one
    // barrier, so the acquisitions reach the database together.
    let start = Arc::new(Barrier::new(8));
    let racers: Vec<_> = (0..8)
        .map(|i| {
            let s = if i % 2 == 0 { a.clone() } else { b.clone() };
            let start = Arc::clone(&start);
            tokio::spawn(async move {
                start.wait().await;
                (i % 2, s.acquire_lease(SCOPE, "", TTL).await.unwrap())
            })
        })
        .collect();
    let mut winners = Vec::new();
    for racer in racers {
        let (side, lease) = racer.await.unwrap();
        if let Some(lease) = lease {
            winners.push((side, lease));
        }
    }
    assert_eq!(winners.len(), 1, "exactly one acquirer wins");
    let (side, mut held) = winners.pop().unwrap();
    assert_eq!(held.token(), 1);
    let (holder, other) = if side == 0 { (&a, &b) } else { (&b, &a) };

    assert!(
        holder
            .acquire_lease(SCOPE, "", TTL)
            .await
            .unwrap()
            .is_none(),
        "not re-entrant"
    );
    assert!(other.acquire_lease(SCOPE, "", TTL).await.unwrap().is_none());
    assert!(holder.renew_lease(&mut held, TTL).await.unwrap());
    assert!(holder.lease_is_held(&held).await.unwrap());
    assert!(
        other.lease_is_held(&held).await.unwrap(),
        "any process can read"
    );
    holder
        .write_guarded_by_lease_for_tests(&held)
        .await
        .unwrap();

    let elsewhere = other.acquire_lease(SCOPE, "other-key", TTL).await.unwrap();
    assert!(elsewhere.is_some(), "another key is other work");
    let other_tenant = open_as_process(&db, "leases-other", "process-b").await;
    assert!(
        other_tenant
            .acquire_lease(SCOPE, "", TTL)
            .await
            .unwrap()
            .is_some(),
        "another tenant is other work"
    );

    holder.release_lease(held).await.unwrap();
    let mut taken = other
        .acquire_lease(SCOPE, "", TTL)
        .await
        .expect("acquire")
        .expect("release frees the key at once");
    assert_eq!(
        taken.token(),
        2,
        "release keeps the token, reacquire bumps it"
    );
    assert!(other.renew_lease(&mut taken, TTL).await.unwrap());
    assert!(holder
        .acquire_lease(SCOPE, "", TTL)
        .await
        .unwrap()
        .is_none());
}

/// A holder that stops renewing (crashed, or work that outlived its TTL)
/// loses the lease by the database clock: a lapsed lease guards nothing,
/// another process acquires it with a higher token, the old holder's
/// renewal reports the loss, its guarded write fails, and its late release
/// leaves the new lease alone. A lapsed lease nobody took still renews.
#[tokio::test]
async fn expiry_hands_the_lease_over_with_a_higher_token() {
    let Some(db) = crate::harness::test_db(false).await else {
        return;
    };
    let tenant = "lease-expiry";
    let a = open_as_process(&db, tenant, "process-a").await;
    let b = open_as_process(&db, tenant, "process-b").await;

    let mut lapsed = a
        .acquire_lease(SCOPE, "", SHORT_TTL)
        .await
        .unwrap()
        .unwrap();
    assert!(b.acquire_lease(SCOPE, "", TTL).await.unwrap().is_none());
    // timing-guard: outlive the short lease's TTL
    tokio::time::sleep(SHORT_TTL * 2).await;
    assert_eq!(lapsed.remaining(), Duration::ZERO);
    assert!(!a.lease_is_held(&lapsed).await.unwrap());
    assert!(
        matches!(
            a.write_guarded_by_lease_for_tests(&lapsed).await,
            Err(Error::LeaseLost(_))
        ),
        "a lapsed lease guards nothing"
    );
    assert!(
        a.renew_lease(&mut lapsed, SHORT_TTL).await.unwrap(),
        "lapsed but untaken: still this lease"
    );
    a.write_guarded_by_lease_for_tests(&lapsed).await.unwrap();
    assert!(b.acquire_lease(SCOPE, "", TTL).await.unwrap().is_none());

    // timing-guard: outlive the renewed short TTL
    tokio::time::sleep(SHORT_TTL * 2).await;
    let mut taken = b
        .acquire_lease(SCOPE, "", TTL)
        .await
        .unwrap()
        .expect("expiry hands the lease over");
    assert!(
        taken.token() > lapsed.token(),
        "the new holder's token is higher"
    );
    assert!(
        !a.renew_lease(&mut lapsed, TTL).await.unwrap(),
        "the old holder lost it"
    );
    assert!(matches!(
        a.write_guarded_by_lease_for_tests(&lapsed).await,
        Err(Error::LeaseLost(_))
    ));
    a.release_lease(lapsed).await.unwrap();
    assert!(
        b.renew_lease(&mut taken, TTL).await.unwrap(),
        "a late release by the old holder is a no-op"
    );
    assert!(a.acquire_lease(SCOPE, "", TTL).await.unwrap().is_none());
}

/// An acquire waits on a locked lease row, which is what `guard_lease`'s
/// `FOR SHARE` relies on: a takeover of a lapsed key cannot start until the
/// transaction holding the row ends. (Today a guarded write also holds the
/// tenant writer lock, which orders acquires behind it on its own; the row
/// lock keeps the rule once leases leave that lock.)
#[tokio::test]
async fn a_guard_holds_off_a_takeover_until_its_transaction_ends() {
    let Some(db) = crate::harness::test_db(false).await else {
        return;
    };
    let tenant = "lease-guard-lock";
    let a = open_as_process(&db, tenant, "process-a").await;
    let held = a.acquire_lease(SCOPE, "", TTL).await.unwrap().unwrap();
    // Lock the row as a guard does, from an admin transaction, and expire
    // the lease under it by the database clock.
    let mut lock = db.admin.begin().await.unwrap();
    sqlx::query(
        "UPDATE lease SET expires_at = clock_timestamp() \
         WHERE tenant_id = $1 AND scope = $2 AND lease_key = ''",
    )
    .bind(tenant)
    .bind(SCOPE)
    .execute(&mut *lock)
    .await
    .unwrap();
    let b = open_as_process(&db, tenant, "process-b").await;
    let acquire = tokio::spawn(async move { b.acquire_lease(SCOPE, "", TTL).await });
    // timing-guard: give the acquire time to block on the row lock
    tokio::time::sleep(Duration::from_millis(300)).await;
    assert!(!acquire.is_finished(), "the acquire waits for the row lock");
    lock.commit().await.unwrap();
    let taken = acquire.await.unwrap().unwrap().expect("then it takes over");
    assert!(taken.token() > held.token());
}

/// A tenant lease takeover (a newer tenant epoch) leaves every lease as it
/// was: the new tenant owner cannot acquire a key the old process holds,
/// and the old process still renews, reads and releases its lease.
#[tokio::test]
async fn a_tenant_takeover_does_not_void_a_lease() {
    let Some(db) = crate::harness::test_db(false).await else {
        return;
    };
    let tenant = "lease-takeover";
    let old = open_as_process(&db, tenant, "process-old").await;
    let mut kept = old.acquire_lease(SCOPE, "", TTL).await.unwrap().unwrap();

    let (owner, _tenant_lease) = Store::open_postgres_owned_as_process(
        db.app.clone(),
        intent_store::postgres::TenantId::new(tenant).unwrap(),
        intent_store::postgres::PostgresOptions::default(),
        "process-new",
        "process-new",
        Duration::from_secs(60),
        intent_store::OrchestratorOnly::for_tests(),
    )
    .await
    .expect("claim the tenant");
    assert!(
        owner.acquire_lease(SCOPE, "", TTL).await.unwrap().is_none(),
        "the takeover did not free the lease"
    );
    assert!(owner.lease_is_held(&kept).await.unwrap());
    assert!(
        old.renew_lease(&mut kept, TTL).await.unwrap(),
        "the old process keeps its lease"
    );
    assert!(old.lease_is_held(&kept).await.unwrap());
    let token = kept.token();
    old.release_lease(kept).await.unwrap();
    let mut next = owner
        .acquire_lease(SCOPE, "", TTL)
        .await
        .unwrap()
        .expect("its release frees the key");
    assert!(next.token() > token);
    assert!(owner.renew_lease(&mut next, TTL).await.unwrap());
    owner.write_guarded_by_lease_for_tests(&next).await.unwrap();
}

/// The lease row's expiry comes from the database clock, not the caller's:
/// it lies within the TTL of the database's own `clock_timestamp()`; the row
/// names the holding process and keeps its token once released.
#[tokio::test]
async fn expiry_is_set_by_the_database_clock() {
    let Some(db) = crate::harness::test_db(false).await else {
        return;
    };
    let tenant = "lease-clock";
    let a = open_as_process(&db, tenant, "process-a").await;
    let held = a.acquire_lease(SCOPE, "", TTL).await.unwrap().unwrap();
    let (left_ms, token, holder_set): (i64, i64, bool) = sqlx::query_as(
        "SELECT (extract(epoch FROM expires_at - clock_timestamp()) * 1000)::bigint, \
         token, holder IS NOT NULL AND holder <> '' \
         FROM lease WHERE tenant_id = $1 AND scope = $2 AND lease_key = ''",
    )
    .bind(tenant)
    .bind(SCOPE)
    .fetch_one(&db.admin)
    .await
    .unwrap();
    let ttl_ms = i64::try_from(TTL.as_millis()).unwrap();
    assert!(
        left_ms > 0 && left_ms <= ttl_ms,
        "{left_ms} ms left of {ttl_ms}"
    );
    assert_eq!(token, 1);
    assert!(holder_set);

    a.release_lease(held).await.unwrap();
    let (token, freed): (i64, bool) = sqlx::query_as(
        "SELECT token, holder IS NULL AND lease_id IS NULL \
         FROM lease WHERE tenant_id = $1 AND scope = $2 AND lease_key = ''",
    )
    .bind(tenant)
    .bind(SCOPE)
    .fetch_one(&db.admin)
    .await
    .unwrap();
    assert_eq!((token, freed), (1, true), "release keeps the row and token");
}
