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

/// A schema the earlier sqlx migrator built has no ledger; `migrate`
/// refuses it, so the gate must name the real repair, not "run migrate".
#[tokio::test]
async fn open_on_a_legacy_sqlx_schema_says_to_recreate_it() {
    let Some((_, admin)) = empty_db().await else {
        return;
    };
    exec(
        &admin,
        "CREATE TABLE _sqlx_migrations (version bigint PRIMARY KEY)".to_owned(),
    )
    .await;
    let mismatch = refused(&admin, "legacy").await;
    assert!(mismatch.legacy_sqlx, "{mismatch:?}");
    let message = Error::SchemaMismatch(Box::new(mismatch)).to_string();
    assert!(message.contains("recreate the schema"), "{message}");
    let migrate = postgres::migrate(&admin).await.unwrap_err().to_string();
    assert!(migrate.contains("recreate the schema"), "{migrate}");
}

/// The numbered Postgres migrations (versions 1 to 15) are one schema file
/// now. A schema left at that history must not be reused or "upgraded": the
/// gate and the migrate job both say to recreate it, and migrate changes
/// nothing.
#[tokio::test]
async fn a_schema_left_at_the_numbered_migrations_says_to_recreate_it() {
    let Some((_, admin)) = empty_db().await else {
        return;
    };
    // What a run of the numbered migrations left: `0001_compat.sql`'s
    // checksum (SHA-384) on version 1, then versions 2 to 15.
    let compat_checksum = "d7858ba9e8b41588417655e346f4c2565b812c941edf2368114bb67655ef700f\
                           b85f8a20b801421a578de19c6996ba1f";
    let rows: Vec<String> = (1..=15)
        .map(|v| {
            let (description, checksum) = if v == 1 {
                ("compat", compat_checksum)
            } else {
                ("numbered", "00")
            };
            format!("({v}, '{description}', '\\x{checksum}', 1, '\\x00', true)")
        })
        .collect();
    exec(
        &admin,
        format!(
            "CREATE TABLE intent_schema_migrations \
             (version bigint PRIMARY KEY, description text NOT NULL, checksum bytea NOT NULL, \
              applied_statements integer NOT NULL, applied_prefix bytea NOT NULL, \
              complete boolean NOT NULL, applied_at timestamptz NOT NULL DEFAULT now()); \
             INSERT INTO intent_schema_migrations \
             (version, description, checksum, applied_statements, applied_prefix, complete) \
             VALUES {}",
            rows.join(", ")
        ),
    )
    .await;
    let objects = public_objects(&admin).await;

    let mismatch = refused(&admin, "numbered").await;
    assert!(mismatch.folded_history, "{mismatch:?}");
    assert_eq!(mismatch.applied, (1..=15).collect::<Vec<i64>>());
    let message = Error::SchemaMismatch(Box::new(mismatch)).to_string();
    assert!(message.contains("recreate the schema"), "{message}");

    let migrate = postgres::migrate(&admin).await.unwrap_err().to_string();
    assert!(
        migrate.contains("numbered Postgres migrations") && migrate.contains("recreate the schema"),
        "{migrate}"
    );
    assert_eq!(
        public_objects(&admin).await,
        objects,
        "migrate changed nothing"
    );

    // Recreated, the schema migrates and opens.
    exec(&admin, "DROP TABLE intent_schema_migrations".to_owned()).await;
    postgres::migrate(&admin)
        .await
        .expect("migrate the recreated schema");
    postgres::check_schema(&admin)
        .await
        .expect("the gate passes on the recreated schema");
}

/// Only a missing `__neki` schema means "not Neki". A `__neki` schema
/// without `ddl_versions()` (renamed or changed on a Neki upgrade) must fail
/// the migrate job, not skip the wait for the routers.
#[tokio::test]
async fn migrate_fails_when_neki_ddl_versions_is_missing() {
    let Some((_, admin)) = empty_db().await else {
        return;
    };
    // An empty data topology, so only ddl_versions() is missing.
    exec(
        &admin,
        "CREATE SCHEMA __neki; \
         CREATE FUNCTION __neki.get_data_topology() RETURNS text \
         LANGUAGE sql AS $$ SELECT '{}'::text $$"
            .to_owned(),
    )
    .await;
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

/// A fake `__neki` schema on vanilla Postgres: a data topology that binds
/// every table with a `tenant_id` column of the current schema to shard
/// group `tenant_data`, except those named in `__neki.unbound`, with `sh1` as
/// the default group; `ddl_versions()` and a `wait_for_ddl` that returns at
/// once.
const FAKE_NEKI: &str = "CREATE SCHEMA __neki; \
    CREATE TABLE __neki.unbound (name text PRIMARY KEY); \
    CREATE FUNCTION __neki.get_data_topology() RETURNS text LANGUAGE sql AS $$ \
      SELECT json_build_object( \
        'default_shard_group', 'sh1', 'authoritative_shard_group', 'sh1', \
        'databases', json_build_object(current_database(), json_build_object('schemas', \
          json_build_object(current_schema(), json_build_object('tables', \
            COALESCE((SELECT json_object_agg(c.relname, \
                        json_build_object('shard_group', 'tenant_data')) \
                      FROM pg_class c \
                      WHERE c.relnamespace = current_schema()::regnamespace \
                        AND c.relkind = 'r' \
                        AND EXISTS (SELECT 1 FROM pg_attribute a \
                                    WHERE a.attrelid = c.oid AND a.attname = 'tenant_id') \
                        AND c.relname NOT IN (SELECT name FROM __neki.unbound)), \
                     '{}'::json))))))::text $$; \
    CREATE FUNCTION __neki.ddl_versions(OUT schema_version bigint, OUT cluster_version bigint) \
      LANGUAGE sql AS $$ SELECT 1::bigint, 1::bigint $$; \
    CREATE FUNCTION __neki.wait_for_ddl(bigint, bigint, interval) RETURNS void \
      LANGUAGE plpgsql AS $$ BEGIN END $$;";

/// The migration that adds `tenant_lease`, the table the sharded run found
/// unbound: the one schema file.
const TENANT_LEASE_MIGRATION: i64 = 1;

/// The sharded Neki run on #3642: the topology bound every tenant table but
/// `tenant_lease`, so claims (which lock `tenant_writer` and `tenant_lease`
/// in one single-shard transaction) would fail for every tenant off the
/// default shard. On Neki, migrate must stop before it marks the migration
/// that adds such a table complete, so the gate keeps daemons out until the
/// table is bound, and finish it on the re-run once it is.
#[tokio::test]
async fn migrate_stops_while_the_neki_topology_leaves_a_tenant_table_unbound() {
    let Some((_, admin)) = empty_db().await else {
        return;
    };
    exec(&admin, FAKE_NEKI.to_owned()).await;
    exec(
        &admin,
        "INSERT INTO __neki.unbound VALUES ('tenant_lease')".to_owned(),
    )
    .await;

    let err = postgres::migrate(&admin)
        .await
        .expect_err("an unbound tenant table must stop migrate on Neki")
        .to_string();
    assert!(
        err.contains(&format!("migration {TENANT_LEASE_MIGRATION} "))
            && err.contains("not bound: tenant_lease")
            && err.contains("tenant_data")
            && err.contains("set_data_topology"),
        "{err}"
    );
    // The table exists (empty), but the migration is part-way: the gate
    // refuses every open, so nothing writes it before it is bound.
    let mismatch = refused(&admin, "unbound-lease").await;
    assert_eq!(mismatch.part_way, vec![TENANT_LEASE_MIGRATION]);
    let gap = postgres::neki_topology_gap(&admin)
        .await
        .expect("read the topology")
        .expect("tenant_lease is unbound");
    assert_eq!(gap.shard_group, "tenant_data");
    assert_eq!(gap.unbound, vec!["tenant_lease".to_owned()]);
    assert_eq!(gap.schema, "public");

    // The operator binds it; the re-run runs no statement again and
    // completes the migration.
    exec(&admin, "DELETE FROM __neki.unbound".to_owned()).await;
    assert_eq!(postgres::neki_topology_gap(&admin).await.unwrap(), None);
    let resumed = postgres::migrate(&admin).await.expect("migrate once bound");
    let rest: Vec<i64> = expected_versions()
        .into_iter()
        .filter(|v| *v >= TENANT_LEASE_MIGRATION)
        .collect();
    assert_eq!(resumed.applied, rest);
    assert!(resumed.neki_ddl.is_some());
    postgres::check_schema(&admin)
        .await
        .expect("the gate passes once the table is bound");

    // A topology that loses a binding later still fails the migrate job,
    // though the ledger is complete.
    exec(
        &admin,
        "INSERT INTO __neki.unbound VALUES ('tenant_lease')".to_owned(),
    )
    .await;
    let drift = postgres::migrate(&admin)
        .await
        .expect_err("a later gap fails the job")
        .to_string();
    assert!(
        drift.contains("migrations are applied, but") && drift.contains("tenant_lease"),
        "{drift}"
    );
}

/// A schema the Neki topology does not shard (no bound tenant table, as a
/// fresh test schema or a one-shard branch) migrates as before.
#[tokio::test]
async fn migrate_passes_on_neki_when_the_schema_is_not_sharded() {
    let Some((_, admin)) = empty_db().await else {
        return;
    };
    exec(&admin, FAKE_NEKI.to_owned()).await;
    // Every tenant table unbound: all of them on the default group.
    exec(
        &admin,
        "CREATE OR REPLACE FUNCTION __neki.get_data_topology() RETURNS text \
         LANGUAGE sql AS $$ SELECT '{\"default_shard_group\": \"sh1\"}'::text $$"
            .to_owned(),
    )
    .await;
    let report = postgres::migrate(&admin).await.expect("migrate");
    assert_eq!(report.applied, expected_versions());
    assert_eq!(postgres::neki_topology_gap(&admin).await.unwrap(), None);
}

/// Like `ddl_versions()`: a `__neki` schema without `get_data_topology()`
/// is a changed Neki, not vanilla Postgres, so migrate fails rather than
/// skip the check; no migration is marked complete.
#[tokio::test]
async fn migrate_fails_when_neki_get_data_topology_is_missing() {
    let Some((_, admin)) = empty_db().await else {
        return;
    };
    exec(&admin, "CREATE SCHEMA __neki".to_owned()).await;
    let err = postgres::migrate(&admin)
        .await
        .expect_err("a __neki schema without get_data_topology() must not pass")
        .to_string();
    assert!(err.contains("get_data_topology"), "{err}");
    assert_eq!(
        refused(&admin, "no-topology").await.applied,
        Vec::<i64>::new()
    );
}

/// The sharded Neki runs lost migrate's connection part-way ("write: broken
/// pipe" from the router, in the tables and triggers). Migrate resumes on a
/// fresh connection from the ledger and finishes, and reports the retry.
#[tokio::test]
async fn migrate_resumes_on_a_fresh_connection_after_losing_its_own() {
    let Some((_, admin)) = empty_db().await else {
        return;
    };
    // Statement 5 of the triggers, past the compatibility functions (38
    // statements) and the tables, indexes and foreign keys (271): where the
    // sharded run lost its connection in the numbered `0003_triggers.sql`.
    let (version, statement) = (expected_versions()[0], 314);
    let report = postgres::migrate_losing_the_connection_at(&admin, version, statement)
        .await
        .expect("migrate retries a lost connection");
    assert_eq!(report.applied, expected_versions());
    assert_eq!(report.transient_errors.len(), 1, "{report:?}");
    assert!(
        report.transient_errors[0].contains(&format!("migration {version} "))
            && report.transient_errors[0].contains(&format!("statement {statement} ")),
        "{report:?}"
    );
    postgres::check_schema(&admin)
        .await
        .expect("the gate passes after the resumed run");
    let again = postgres::migrate(&admin).await.expect("second migrate");
    assert_eq!(again.applied, Vec::<i64>::new());
    assert!(again.transient_errors.is_empty());
}
