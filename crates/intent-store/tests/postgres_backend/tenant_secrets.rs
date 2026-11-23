//! Tenant secrets, encrypted in Postgres: a secret written by one process
//! is read by the process that takes the tenant over; the database holds
//! ciphertext only; a wrong or missing key fails closed; a process that lost
//! the tenant, or whose self-fence fired, cannot write.
//!
//! Each "process" is a store opened through the `*_as_process` test seams.
//! The tests run on the multi-thread runtime, which the store requires.

use std::sync::Arc;
use std::time::Duration;

use intent_store::postgres::{
    PgPool, PostgresOptions, SecretKeyring, TenantId, TenantSecretStore, WriterLimits,
};
use intent_store::{Error, Store};
use intentd_test_support::test_timeout;

use super::harness::{open_trapped, sqlite_store, test_db, trapped_db};

const LONG_TTL: Duration = Duration::from_secs(60);
const SHORT_TTL: Duration = Duration::from_millis(500);
const KEY_1: [u8; 32] = [0x11; 32];
const KEY_2: [u8; 32] = [0x22; 32];

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

fn ring(id: &str, key: &[u8; 32]) -> Arc<SecretKeyring> {
    Arc::new(SecretKeyring::new(id, key).unwrap())
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

/// The tenant's rows as the database holds them: `(account, key_id, nonce, ciphertext)`.
async fn raw_rows(admin: &PgPool, tenant: &str) -> Vec<(String, String, Vec<u8>, Vec<u8>)> {
    sqlx::query_as(
        "SELECT account, key_id, nonce, ciphertext FROM tenant_secrets \
         WHERE tenant_id = $1 ORDER BY account",
    )
    .bind(tenant)
    .fetch_all(admin)
    .await
    .expect("read tenant_secrets")
}

#[track_caller]
fn assert_not_owner<T: std::fmt::Debug>(result: intent_store::Result<T>, what: &str) {
    match result {
        Err(Error::NotOwner(_)) => {}
        other => panic!("{what}: expected NotOwner, got {other:?}"),
    }
}

#[tokio::test(flavor = "multi_thread", worker_threads = 2)]
async fn a_secret_follows_the_tenant_to_the_process_that_takes_it_over() {
    let Some(db) = test_db(false).await else {
        return;
    };
    let tenant = "secrets-takeover";
    let keys = ring("k1", &KEY_1);
    let old = open_owned(&db.app, tenant, "proc-a", SHORT_TTL).await;
    let old_secrets = TenantSecretStore::new(&old, keys.clone()).unwrap();
    old_secrets
        .store("github.token", "gho_first")
        .await
        .unwrap();
    old_secrets.store("gitlab.token", "glpat_x").await.unwrap();
    old_secrets
        .store("github.token", "gho_second")
        .await
        .unwrap();
    old_secrets.delete("gitlab.token").await.unwrap();
    old_secrets.delete("never.set").await.unwrap();
    wait_for_expiry(&db.admin, tenant).await;

    let new = open_owned(&db.app, tenant, "proc-b", LONG_TTL).await;
    let new_secrets = TenantSecretStore::new(&new, keys).unwrap();
    assert_eq!(
        new_secrets.load("github.token").await.unwrap().as_deref(),
        Some("gho_second")
    );
    assert_eq!(new_secrets.load("gitlab.token").await.unwrap(), None);

    // The old owner can no longer store or delete, and wrote nothing.
    let before = raw_rows(&db.admin, tenant).await;
    assert_not_owner(
        old_secrets.store("github.token", "gho_stale").await,
        "the old owner's store",
    );
    assert_not_owner(
        old_secrets.delete("github.token").await,
        "the old owner's delete",
    );
    assert_eq!(raw_rows(&db.admin, tenant).await, before);

    // The new owner writes as usual.
    new_secrets
        .store("github.token", "gho_third")
        .await
        .unwrap();
    assert_eq!(
        new_secrets.load("github.token").await.unwrap().as_deref(),
        Some("gho_third")
    );
}

#[tokio::test(flavor = "multi_thread", worker_threads = 2)]
async fn a_fired_self_fence_refuses_secret_writes() {
    let Some(db) = test_db(false).await else {
        return;
    };
    let tenant = "secrets-fenced";
    let store = open_owned(&db.app, tenant, "proc-a", LONG_TTL).await;
    let secrets = TenantSecretStore::new(&store, ring("k1", &KEY_1)).unwrap();
    secrets.store("acct", "v1").await.unwrap();
    assert!(store
        .tenant_fence()
        .expect("claimed")
        .fire("test: stop serving"));
    assert_not_owner(secrets.store("acct", "v2").await, "a fenced store");
    assert_not_owner(secrets.delete("acct").await, "a fenced delete");
    assert_eq!(secrets.load("acct").await.unwrap().as_deref(), Some("v1"));
}

#[tokio::test(flavor = "multi_thread", worker_threads = 2)]
async fn the_database_holds_ciphertext_only() {
    let Some(db) = test_db(false).await else {
        return;
    };
    let tenant = "secrets-at-rest";
    let store = open_unclaimed(&db.app, tenant, "proc-a").await;
    let secrets = TenantSecretStore::new(&store, ring("k1", &KEY_1)).unwrap();
    let value = "plaintext-marker-6c1f";
    secrets.store("acct", value).await.unwrap();

    let rows = raw_rows(&db.admin, tenant).await;
    assert_eq!(rows.len(), 1);
    let (account, key_id, nonce, ciphertext) = &rows[0];
    assert_eq!((account.as_str(), key_id.as_str()), ("acct", "k1"));
    assert_eq!(nonce.len(), 24);
    assert_eq!(ciphertext.len(), value.len() + 16, "ciphertext plus tag");
    // No column of any row, as text, contains the value.
    let leaked: i64 = sqlx::query_scalar(
        "SELECT count(*) FROM tenant_secrets t WHERE t.tenant_id = $1 \
         AND position($2 in t::text) > 0",
    )
    .bind(tenant)
    .bind(value)
    .fetch_one(&db.admin)
    .await
    .unwrap();
    assert_eq!(leaked, 0);
    assert!(!ciphertext
        .windows(value.len())
        .any(|w| w == value.as_bytes()));
}

#[tokio::test(flavor = "multi_thread", worker_threads = 2)]
async fn a_wrong_or_missing_key_fails_closed() {
    let Some(db) = test_db(false).await else {
        return;
    };
    let tenant = "secrets-wrong-key";
    let store = open_unclaimed(&db.app, tenant, "proc-a").await;
    TenantSecretStore::new(&store, ring("k1", &KEY_1))
        .unwrap()
        .store("acct", "v")
        .await
        .unwrap();

    let wrong = TenantSecretStore::new(&store, ring("k1", &KEY_2)).unwrap();
    let err = wrong.load("acct").await.unwrap_err().to_string();
    assert!(
        err.contains("does not decrypt") && err.contains("\"acct\"") && err.contains("\"k1\""),
        "{err}"
    );
    let missing = TenantSecretStore::new(&store, ring("k2", &KEY_1)).unwrap();
    let err = missing.load("acct").await.unwrap_err().to_string();
    assert!(err.contains("no key with this id"), "{err}");
    // An absent account is still plainly absent.
    assert_eq!(wrong.load("other").await.unwrap(), None);
}

#[tokio::test(flavor = "multi_thread", worker_threads = 2)]
async fn a_row_copied_to_another_tenant_or_account_does_not_open() {
    let Some(db) = test_db(false).await else {
        return;
    };
    let keys = ring("k1", &KEY_1);
    let a = open_unclaimed(&db.app, "secrets-a", "proc-a").await;
    let b = open_unclaimed(&db.app, "secrets-b", "proc-b").await;
    let a_secrets = TenantSecretStore::new(&a, keys.clone()).unwrap();
    let b_secrets = TenantSecretStore::new(&b, keys).unwrap();
    a_secrets.store("acct", "a-value").await.unwrap();
    b_secrets.store("acct", "b-value").await.unwrap();
    assert_eq!(
        a_secrets.load("acct").await.unwrap().as_deref(),
        Some("a-value")
    );
    assert_eq!(
        b_secrets.load("acct").await.unwrap().as_deref(),
        Some("b-value")
    );

    // Someone with database access copies a's row over b's, and onto
    // another account of a's.
    sqlx::query(
        "UPDATE tenant_secrets AS b SET key_id = a.key_id, nonce = a.nonce, \
         ciphertext = a.ciphertext FROM tenant_secrets AS a \
         WHERE a.tenant_id = 'secrets-a' AND a.account = 'acct' \
         AND b.tenant_id = 'secrets-b' AND b.account = 'acct'",
    )
    .execute(&db.admin)
    .await
    .unwrap();
    sqlx::query(
        "INSERT INTO tenant_secrets (tenant_id, account, key_id, nonce, ciphertext, updated_at) \
         SELECT tenant_id, 'other', key_id, nonce, ciphertext, updated_at \
         FROM tenant_secrets WHERE tenant_id = 'secrets-a' AND account = 'acct'",
    )
    .execute(&db.admin)
    .await
    .unwrap();
    assert!(b_secrets.load("acct").await.is_err());
    assert!(a_secrets.load("other").await.is_err());
}

#[tokio::test(flavor = "multi_thread", worker_threads = 2)]
async fn rotation_reads_old_rows_and_reseals_them_on_write() {
    let Some(db) = test_db(false).await else {
        return;
    };
    let tenant = "secrets-rotation";
    let store = open_unclaimed(&db.app, tenant, "proc-a").await;
    let before = TenantSecretStore::new(&store, ring("k1", &KEY_1)).unwrap();
    before.store("one", "v1").await.unwrap();
    before.store("two", "v2").await.unwrap();

    let rotated = Arc::new(
        SecretKeyring::new("k2", &KEY_2)
            .unwrap()
            .with_previous_key("k1", &KEY_1)
            .unwrap(),
    );
    let after = TenantSecretStore::new(&store, rotated).unwrap();
    assert_eq!(after.load("one").await.unwrap().as_deref(), Some("v1"));
    after.store("one", "v1b").await.unwrap();
    let ids: Vec<(String, String)> = raw_rows(&db.admin, tenant)
        .await
        .into_iter()
        .map(|(account, key_id, ..)| (account, key_id))
        .collect();
    assert_eq!(
        ids,
        [("one".into(), "k2".into()), ("two".into(), "k1".into())]
    );
    assert_eq!(after.load("two").await.unwrap().as_deref(), Some("v2"));
    // The old keyring cannot read the resealed row.
    assert!(before.load("one").await.is_err());
}

#[tokio::test(flavor = "multi_thread", worker_threads = 2)]
async fn an_empty_value_loads_as_unset() {
    let Some(db) = test_db(false).await else {
        return;
    };
    let store = open_unclaimed(&db.app, "secrets-empty", "proc-a").await;
    let secrets = TenantSecretStore::new(&store, ring("k1", &KEY_1)).unwrap();
    secrets.store("acct", "").await.unwrap();
    assert_eq!(secrets.load("acct").await.unwrap(), None);
}

#[tokio::test(flavor = "multi_thread", worker_threads = 2)]
async fn the_blocking_methods_work_on_a_multi_thread_runtime() {
    let Some(db) = test_db(false).await else {
        return;
    };
    let store = open_unclaimed(&db.app, "secrets-blocking", "proc-a").await;
    let secrets = TenantSecretStore::new(&store, ring("k1", &KEY_1)).unwrap();
    secrets.store_blocking("acct", "v").unwrap();
    assert_eq!(secrets.load_blocking("acct").unwrap().as_deref(), Some("v"));
    secrets.delete_blocking("acct").unwrap();
    assert_eq!(secrets.load_blocking("acct").unwrap(), None);

    // Outside the runtime (a plain thread), on the store's runtime.
    let outside = secrets.clone();
    std::thread::spawn(move || {
        outside.store_blocking("acct", "w").unwrap();
        outside.load_blocking("acct").unwrap()
    })
    .join()
    .map(|v| assert_eq!(v.as_deref(), Some("w")))
    .unwrap();
}

/// A current-thread runtime cannot drive the database from a blocked
/// thread: the store refuses one when built, and a blocking call made from
/// inside one fails instead of hanging.
#[tokio::test]
async fn the_store_refuses_a_current_thread_runtime() {
    let Some(db) = test_db(false).await else {
        return;
    };
    let store = open_unclaimed(&db.app, "secrets-current-thread", "proc-a").await;
    let err = TenantSecretStore::new(&store, ring("k1", &KEY_1))
        .unwrap_err()
        .to_string();
    assert!(err.contains("multi-thread"), "{err}");
}

#[tokio::test(flavor = "multi_thread", worker_threads = 2)]
async fn a_blocking_call_inside_a_current_thread_runtime_fails() {
    let Some(db) = test_db(false).await else {
        return;
    };
    let store = open_unclaimed(&db.app, "secrets-nested-runtime", "proc-a").await;
    let secrets = TenantSecretStore::new(&store, ring("k1", &KEY_1)).unwrap();
    let err = std::thread::spawn(move || {
        tokio::runtime::Builder::new_current_thread()
            .build()
            .unwrap()
            .block_on(async { secrets.load_blocking("acct") })
    })
    .join()
    .unwrap()
    .unwrap_err()
    .to_string();
    assert!(err.contains("current-thread"), "{err}");
}

/// Every statement is scoped to its tenant: on the leak-trap database, any
/// read, overwrite or delete that reaches another tenant's row fails, and
/// two tenants with the same account keep their own values.
#[tokio::test(flavor = "multi_thread", worker_threads = 2)]
async fn secret_statements_never_touch_another_tenants_rows() {
    let Some(db) = trapped_db().await else {
        return;
    };
    let keys = ring("k1", &KEY_1);
    let a = open_trapped(&db, "trap-a").await;
    let b = open_trapped(&db, "trap-b").await;
    let a_secrets = TenantSecretStore::new(&a, keys.clone()).unwrap();
    let b_secrets = TenantSecretStore::new(&b, keys).unwrap();
    a_secrets.store("acct", "a1").await.unwrap();
    b_secrets.store("acct", "b1").await.unwrap();
    a_secrets.store("acct", "a2").await.unwrap();
    assert_eq!(a_secrets.load("acct").await.unwrap().as_deref(), Some("a2"));
    assert_eq!(b_secrets.load("acct").await.unwrap().as_deref(), Some("b1"));
    a_secrets.delete("acct").await.unwrap();
    assert_eq!(a_secrets.load("acct").await.unwrap(), None);
    assert_eq!(b_secrets.load("acct").await.unwrap().as_deref(), Some("b1"));
    b_secrets.store_blocking("acct", "b2").unwrap();
    assert_eq!(
        b_secrets.load_blocking("acct").unwrap().as_deref(),
        Some("b2")
    );
    assert_eq!(raw_rows(&db.admin, "trap-a").await, []);
}

/// The `SQLite` daemon keeps its file store: a `SQLite` store has no tenant
/// secrets.
#[tokio::test]
async fn a_sqlite_store_has_no_tenant_secrets() {
    let (store, _dir) = sqlite_store().await;
    let err = TenantSecretStore::new(&store, ring("k1", &KEY_1))
        .unwrap_err()
        .to_string();
    assert!(err.contains("need a Postgres store"), "{err}");
}
