//! The tenant writer lock is a row lock on the tenant's `tenant_writer` row
//! (audit finding 1): a statement keyed by `tenant_id`, so a database
//! sharded by tenant routes it to the tenant's shard. `postgres_neki` runs
//! the same race on a sharded Neki branch.

use std::time::Duration;

use intent_store::postgres::{self, PoolSettings, PostgresOptions, TenantId, WriterLimits};
use intent_store::Store;
use intentd_test_support::test_timeout;

use crate::harness::{hold_writer_lock, test_db, writer_lock_is_free};
use crate::writer_race::race_increments;

fn options() -> PostgresOptions {
    PostgresOptions {
        writer: WriterLimits {
            wait_budget: test_timeout(Duration::from_secs(60)),
            ..WriterLimits::default()
        },
        ..PostgresOptions::default()
    }
}

#[tokio::test]
async fn writers_of_one_tenant_in_separate_processes_never_lose_an_increment() {
    let Some(db) = test_db(false).await else {
        return;
    };
    let outcome = tokio::time::timeout(
        test_timeout(Duration::from_secs(120)),
        race_increments(&db.app, "writer-lock-race", options()),
    )
    .await
    .expect("the race finishes")
    .expect("the race runs");
    outcome.check().expect("the writer lock excludes");
}

#[tokio::test]
async fn the_first_write_seeds_the_tenant_lock_row_and_a_missing_row_is_reseeded() {
    let Some(db) = test_db(false).await else {
        return;
    };
    let tenant = "writer-lock-seed";
    let rows = |admin: sqlx::PgPool| async move {
        sqlx::query_scalar::<_, i64>("SELECT count(*) FROM tenant_writer WHERE tenant_id = $1")
            .bind(tenant)
            .fetch_one(&admin)
            .await
            .unwrap()
    };
    assert_eq!(rows(db.admin.clone()).await, 0);
    // Opening provisions the tenant under the writer lock, which seeds it.
    let store = Store::open_postgres(db.app.clone(), TenantId::new(tenant).unwrap(), options())
        .await
        .unwrap();
    assert_eq!(rows(db.admin.clone()).await, 1);
    // A tenant from before the table has no row: its next write adds it.
    sqlx::query("DELETE FROM tenant_writer WHERE tenant_id = $1")
        .bind(tenant)
        .execute(&db.admin)
        .await
        .unwrap();
    store.set_setting("userRules", "\"x\"").await.unwrap();
    assert_eq!(rows(db.admin.clone()).await, 1);
    assert_eq!(
        store.get_setting("userRules").await.unwrap().as_deref(),
        Some("\"x\"")
    );
}

/// The lock row is the tenant's, not shared: holding one tenant's leaves
/// another's free, and a held one blocks the tenant's writes until it ends.
#[tokio::test]
async fn the_writer_lock_row_is_per_tenant() {
    let Some(db) = test_db(false).await else {
        return;
    };
    let quick = PostgresOptions {
        writer: WriterLimits {
            wait_budget: Duration::from_millis(300),
            lock_attempt: Duration::from_millis(100),
            ..WriterLimits::default()
        },
        ..options()
    };
    let a = Store::open_postgres(db.app.clone(), TenantId::new("lock-row-a").unwrap(), quick)
        .await
        .unwrap();
    let b = Store::open_postgres(db.app.clone(), TenantId::new("lock-row-b").unwrap(), quick)
        .await
        .unwrap();
    let held = hold_writer_lock(&db.admin, "lock-row-a").await;
    assert!(!writer_lock_is_free(&db.admin, "lock-row-a").await);
    assert!(writer_lock_is_free(&db.admin, "lock-row-b").await);
    b.set_setting("userRules", "\"b\"").await.unwrap();
    let busy = a.set_setting("userRules", "\"a\"").await;
    assert!(
        matches!(busy, Err(intent_core::Error::StoreBusy(_))),
        "{busy:?}"
    );
    held.rollback().await.unwrap();
    a.set_setting("userRules", "\"a\"").await.unwrap();
}

/// A pool with `single_shard` opens every connection in Neki's single-shard
/// mode (a startup option, so no transaction can leave it); `migrate` still
/// runs on such a pool, switching its own connection back to `multi`.
#[tokio::test]
async fn single_shard_pools_open_connections_in_single_shard_mode() {
    let Some(db) = test_db(false).await else {
        return;
    };
    for (single, tenant) in [(true, "mode-single"), (false, "mode-multi")] {
        let pool = postgres::connect_with(
            &db.url,
            PoolSettings {
                single_shard: single,
                ..PoolSettings::new(1)
            },
        )
        .await
        .unwrap();
        postgres::migrate(&pool).await.expect("migrate on the pool");
        let store = Store::open_postgres(pool.clone(), TenantId::new(tenant).unwrap(), options())
            .await
            .unwrap();
        store.set_setting("userRules", "\"x\"").await.unwrap();
        let mode: Option<String> =
            sqlx::query_scalar("SELECT current_setting('__neki.tx_mode', true)")
                .fetch_one(&pool)
                .await
                .unwrap();
        if single {
            assert_eq!(mode.as_deref(), Some("single"));
        } else {
            assert_ne!(mode.as_deref(), Some("single"));
        }
        pool.close().await;
    }
}
