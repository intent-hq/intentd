//! The schema gate: serving daemons never migrate. `Store::open_postgres`
//! compares the schema's migration ledger with this build's migrations and
//! refuses (`Error::SchemaMismatch`) before it writes anything; only
//! `postgres::migrate`, run by a separate job, changes the schema.

use intent_store::postgres::{self, PgPool, PostgresOptions, TenantId, MIGRATOR};
use intent_store::{Error, Store};

use super::harness::{empty_db, exec, sqlite_store, tenant_rows, test_db};

fn expected_versions() -> Vec<i64> {
    MIGRATOR.iter().map(|m| m.version).collect()
}

async fn open_on(pool: &PgPool, tenant: &str) -> intent_store::Result<Store> {
    Store::open_postgres(
        pool.clone(),
        TenantId::new(tenant).unwrap(),
        PostgresOptions::default(),
        intent_store::OrchestratorOnly::for_tests(),
    )
    .await
}

/// The mismatch an open must fail with; panics on success or another error.
async fn refused(pool: &PgPool, tenant: &str) -> intent_core::error::SchemaMismatch {
    match open_on(pool, tenant).await {
        Err(Error::SchemaMismatch(mismatch)) => *mismatch,
        Err(other) => panic!("expected SchemaMismatch, got {other:?}"),
        Ok(_) => panic!("open must refuse a schema that does not match this build"),
    }
}

/// Every relation and routine in the database's `public` schema.
async fn public_objects(pool: &PgPool) -> Vec<String> {
    sqlx::query_scalar(
        "SELECT relname::text FROM pg_class c JOIN pg_namespace n ON n.oid = c.relnamespace \
         WHERE n.nspname = 'public' \
         UNION ALL \
         SELECT proname::text FROM pg_proc p JOIN pg_namespace n ON n.oid = p.pronamespace \
         WHERE n.nspname = 'public' ORDER BY 1",
    )
    .fetch_all(pool)
    .await
    .expect("list public objects")
}

#[tokio::test]
async fn open_on_an_unmigrated_schema_fails_and_writes_nothing() {
    let Some((_, admin)) = empty_db().await else {
        return;
    };
    let mismatch = refused(&admin, "unmigrated").await;
    assert_eq!(mismatch.applied, Vec::<i64>::new());
    assert_eq!(mismatch.expected, expected_versions());
    // Not even the ledger table: the gate only reads.
    assert_eq!(public_objects(&admin).await, Vec::<String>::new());
    let message = Error::SchemaMismatch(Box::new(mismatch)).to_string();
    assert!(message.contains("migrate"), "{message}");
}

#[tokio::test]
async fn open_on_a_schema_with_an_unknown_migration_fails_and_writes_nothing() {
    let Some(db) = test_db(false).await else {
        return;
    };
    exec(
        &db.admin,
        "INSERT INTO intent_schema_migrations \
         (version, description, checksum, applied_statements, applied_prefix, complete) \
         VALUES (9999, 'future', '\\x00', 0, '\\x00', true)"
            .to_owned(),
    )
    .await;
    let mismatch = refused(&db.app, "older-binary").await;
    let mut newer = expected_versions();
    newer.push(9999);
    assert_eq!(mismatch.applied, newer);
    assert_eq!(mismatch.expected, expected_versions());
    assert_eq!(tenant_rows(&db.admin, "older-binary").await, Vec::new());
}

#[tokio::test]
async fn open_on_a_changed_or_part_way_migration_fails() {
    let Some(db) = test_db(false).await else {
        return;
    };
    let first = expected_versions()[0];
    let last = *expected_versions().last().unwrap();
    exec(
        &db.admin,
        format!("UPDATE intent_schema_migrations SET checksum = '\\x00' WHERE version = {first}"),
    )
    .await;
    let changed = refused(&db.app, "changed").await;
    assert_eq!(changed.changed, vec![first]);
    assert_eq!(changed.applied, expected_versions());

    // Restore the first checksum, then leave the last file part-way.
    sqlx::query("UPDATE intent_schema_migrations SET checksum = $1 WHERE version = $2")
        .bind(MIGRATOR.iter().next().unwrap().checksum.as_ref())
        .bind(first)
        .execute(&db.admin)
        .await
        .unwrap();
    exec(
        &db.admin,
        format!("UPDATE intent_schema_migrations SET complete = false WHERE version = {last}"),
    )
    .await;
    let part_way = refused(&db.app, "part-way").await;
    assert_eq!(part_way.changed, Vec::<i64>::new());
    assert_eq!(part_way.part_way, vec![last]);
    assert_eq!(
        part_way.applied,
        expected_versions()[..expected_versions().len() - 1]
    );
    for tenant in ["changed", "part-way"] {
        assert_eq!(tenant_rows(&db.admin, tenant).await, Vec::new());
    }
}

#[tokio::test]
async fn open_after_migrate_succeeds_and_migrate_is_idempotent() {
    let Some((_, admin)) = empty_db().await else {
        return;
    };
    let first = postgres::migrate(&admin).await.expect("first migrate");
    assert_eq!(first.applied, expected_versions());
    // Vanilla Postgres has no Neki routers to wait for.
    assert_eq!(first.neki_ddl, None);
    let ledger = || async {
        sqlx::query_as::<_, (i64, Vec<u8>, bool, String)>(
            "SELECT version, checksum, complete, applied_at::text \
             FROM intent_schema_migrations ORDER BY version",
        )
        .fetch_all(&admin)
        .await
        .unwrap()
    };
    let before = ledger().await;
    let objects = public_objects(&admin).await;
    let again = postgres::migrate(&admin).await.expect("second migrate");
    assert_eq!(again.applied, Vec::<i64>::new());
    assert_eq!(ledger().await, before);
    assert_eq!(public_objects(&admin).await, objects);

    postgres::check_schema(&admin)
        .await
        .expect("the gate passes on a migrated schema");
    let store = open_on(&admin, "migrated")
        .await
        .expect("open after migrate");
    store.set_setting("k", "1").await.unwrap();
    assert_eq!(store.get_setting("k").await.unwrap().as_deref(), Some("1"));
}

/// Only a missing `__neki` schema means "not Neki". A `__neki` schema
/// without `ddl_versions()` (renamed or changed on a Neki upgrade) must fail
/// the migrate job, not skip the wait for the routers.
#[tokio::test]
async fn migrate_fails_when_neki_ddl_versions_is_missing() {
    let Some((_, admin)) = empty_db().await else {
        return;
    };
    exec(&admin, "CREATE SCHEMA __neki".to_owned()).await;
    let err = postgres::migrate(&admin)
        .await
        .expect_err("a __neki schema without ddl_versions() must not pass as vanilla")
        .to_string();
    assert!(err.contains("ddl_versions"), "{err}");
    // The migrations themselves applied: a re-run after the fix only waits.
    postgres::check_schema(&admin)
        .await
        .expect("the schema is migrated");
}

#[tokio::test]
async fn sqlite_open_still_migrates_itself() {
    let (store, _dir) = sqlite_store().await;
    let status = store.migration_status().await.unwrap();
    assert!(status.is_current(), "{status:?}");
    assert!(!status.applied.is_empty());
}
