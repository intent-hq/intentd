//! A tenant's settings document in Postgres: it follows the tenant to the
//! process that takes it over; each write is a compare-and-set on its
//! revision, so of two writers from one revision one wins and the other
//! sees a conflict; a process that lost the tenant, or whose self-fence
//! fired, cannot write.
//!
//! Each "process" is a store opened through the `*_as_process` test seams.
//! The tests run on the multi-thread runtime, which the store requires.

use std::time::Duration;

use intent_store::postgres::{
    ConfigWrite, PgPool, PostgresOptions, StoredConfig, TenantConfigStore, TenantId, WriterLimits,
};
use intent_store::{Error, Store};
use intentd_test_support::test_timeout;

use super::harness::{open_trapped, sqlite_store, test_db, trapped_db};

const LONG_TTL: Duration = Duration::from_secs(60);
const SHORT_TTL: Duration = Duration::from_millis(500);

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

async fn open_owned(pool: &PgPool, tenant: &str, process: &str, ttl: Duration) -> Store {
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
    .map(|(store, _lease)| store)
    .expect("open and claim the tenant")
}

async fn open_unclaimed(pool: &PgPool, tenant: &str, process: &str) -> Store {
    Store::open_postgres_as_process(
        pool.clone(),
        TenantId::new(tenant).unwrap(),
        options(),
        process,
        intent_store::OrchestratorOnly::for_tests(),
    )
    .await
    .expect("open tenant store")
}

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

fn stored(body: &str, revision: u64) -> Option<StoredConfig> {
    Some(StoredConfig {
        body: body.to_owned(),
        revision,
    })
}

#[track_caller]
fn assert_not_owner<T: std::fmt::Debug>(result: intent_store::Result<T>, what: &str) {
    match result {
        Err(Error::NotOwner(_)) => {}
        other => panic!("{what}: expected NotOwner, got {other:?}"),
    }
}

#[tokio::test(flavor = "multi_thread", worker_threads = 2)]
async fn each_write_raises_the_revision_and_a_stale_one_conflicts() {
    let Some(db) = test_db(false).await else {
        return;
    };
    let store = open_unclaimed(&db.app, "config-revisions", "proc-a").await;
    let config = TenantConfigStore::new(&store).unwrap();
    assert_eq!(config.load().await.unwrap(), None);
    assert_eq!(
        config.compare_and_set(0, "a = 1\n").await.unwrap(),
        ConfigWrite::Written(1)
    );
    assert_eq!(
        config.compare_and_set(1, "a = 2\n").await.unwrap(),
        ConfigWrite::Written(2)
    );
    assert_eq!(config.load().await.unwrap(), stored("a = 2\n", 2));
    // A writer still at an older revision (or at none) writes nothing.
    assert_eq!(
        config.compare_and_set(1, "stale\n").await.unwrap(),
        ConfigWrite::Conflict(2)
    );
    assert_eq!(
        config.compare_and_set(0, "stale\n").await.unwrap(),
        ConfigWrite::Conflict(2)
    );
    assert_eq!(config.load().await.unwrap(), stored("a = 2\n", 2));
}

/// Two processes read the same revision and both write: one wins, the
/// other gets the winner's revision back and wrote nothing.
#[tokio::test(flavor = "multi_thread", worker_threads = 4)]
async fn of_two_concurrent_writers_from_one_revision_one_wins() {
    let Some(db) = test_db(false).await else {
        return;
    };
    let tenant = "config-race";
    let a = TenantConfigStore::new(&open_unclaimed(&db.app, tenant, "proc-a").await).unwrap();
    let b = TenantConfigStore::new(&open_unclaimed(&db.app, tenant, "proc-b").await).unwrap();
    assert_eq!(
        a.compare_and_set(0, "v = 0\n").await.unwrap(),
        ConfigWrite::Written(1)
    );
    let held = b.load().await.unwrap().unwrap().revision;
    assert_eq!(held, 1);
    let (from_a, from_b) = tokio::join!(
        a.compare_and_set(held, "v = 'a'\n"),
        b.compare_and_set(held, "v = 'b'\n"),
    );
    let outcomes = [from_a.unwrap(), from_b.unwrap()];
    let won = outcomes
        .iter()
        .filter(|o| **o == ConfigWrite::Written(2))
        .count();
    let lost = outcomes
        .iter()
        .filter(|o| **o == ConfigWrite::Conflict(2))
        .count();
    assert_eq!((won, lost), (1, 1), "{outcomes:?}");
    let body = a.load().await.unwrap().unwrap();
    let winner = if outcomes[0] == ConfigWrite::Written(2) {
        "v = 'a'\n"
    } else {
        "v = 'b'\n"
    };
    assert_eq!(
        body,
        StoredConfig {
            body: winner.into(),
            revision: 2
        }
    );
}

#[tokio::test(flavor = "multi_thread", worker_threads = 2)]
async fn the_document_follows_the_tenant_and_the_old_owner_cannot_write() {
    let Some(db) = test_db(false).await else {
        return;
    };
    let tenant = "config-takeover";
    let old =
        TenantConfigStore::new(&open_owned(&db.app, tenant, "proc-a", SHORT_TTL).await).unwrap();
    assert_eq!(
        old.compare_and_set(0, "x = 1\n").await.unwrap(),
        ConfigWrite::Written(1)
    );
    wait_for_expiry(&db.admin, tenant).await;
    let new =
        TenantConfigStore::new(&open_owned(&db.app, tenant, "proc-b", LONG_TTL).await).unwrap();
    assert_eq!(new.load().await.unwrap(), stored("x = 1\n", 1));
    assert_not_owner(
        old.compare_and_set(1, "x = 2\n").await,
        "the old owner's write",
    );
    assert_eq!(new.load().await.unwrap(), stored("x = 1\n", 1));
    assert_eq!(
        new.compare_and_set(1, "x = 3\n").await.unwrap(),
        ConfigWrite::Written(2)
    );
}

#[tokio::test(flavor = "multi_thread", worker_threads = 2)]
async fn a_fired_self_fence_refuses_config_writes() {
    let Some(db) = test_db(false).await else {
        return;
    };
    let store = open_owned(&db.app, "config-fenced", "proc-a", LONG_TTL).await;
    let config = TenantConfigStore::new(&store).unwrap();
    config.compare_and_set(0, "x = 1\n").await.unwrap();
    assert!(store
        .tenant_fence()
        .expect("claimed")
        .fire("test: stop serving"));
    assert_not_owner(config.compare_and_set(1, "x = 2\n").await, "a fenced write");
    assert_eq!(config.load().await.unwrap(), stored("x = 1\n", 1));
}

#[tokio::test(flavor = "multi_thread", worker_threads = 2)]
async fn the_blocking_methods_work_on_and_off_the_runtime() {
    let Some(db) = test_db(false).await else {
        return;
    };
    let store = open_unclaimed(&db.app, "config-blocking", "proc-a").await;
    let config = TenantConfigStore::new(&store).unwrap();
    assert_eq!(
        config.compare_and_set_blocking(0, "a = 1\n").unwrap(),
        ConfigWrite::Written(1)
    );
    let outside = config.clone();
    let loaded = std::thread::spawn(move || outside.load_blocking().unwrap())
        .join()
        .unwrap();
    assert_eq!(loaded, stored("a = 1\n", 1));
}

/// Every statement is scoped to its tenant: on the leak-trap database, a
/// read or write that reaches another tenant's row fails.
#[tokio::test(flavor = "multi_thread", worker_threads = 2)]
async fn config_statements_never_touch_another_tenants_row() {
    let Some(db) = trapped_db().await else {
        return;
    };
    let a = TenantConfigStore::new(&open_trapped(&db, "trap-a").await).unwrap();
    let b = TenantConfigStore::new(&open_trapped(&db, "trap-b").await).unwrap();
    assert_eq!(
        a.compare_and_set(0, "a\n").await.unwrap(),
        ConfigWrite::Written(1)
    );
    assert_eq!(
        b.compare_and_set(0, "b\n").await.unwrap(),
        ConfigWrite::Written(1)
    );
    assert_eq!(
        a.compare_and_set(1, "a2\n").await.unwrap(),
        ConfigWrite::Written(2)
    );
    assert_eq!(
        b.compare_and_set(2, "nope\n").await.unwrap(),
        ConfigWrite::Conflict(1)
    );
    assert_eq!(a.load().await.unwrap(), stored("a2\n", 2));
    assert_eq!(b.load().await.unwrap(), stored("b\n", 1));
}

#[tokio::test]
async fn a_sqlite_store_has_no_tenant_config() {
    let (store, _dir) = sqlite_store().await;
    let err = TenantConfigStore::new(&store).unwrap_err().to_string();
    assert!(err.contains("need a Postgres store"), "{err}");
}
