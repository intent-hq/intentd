//! The multi-tenant Postgres backend against a real database.
//!
//! Needs `--features postgres` and `INTENT_STORE_TEST_PG_URL` (a role that
//! may create databases and roles). Without the URL the tests skip, unless
//! `INTENT_STORE_REQUIRE_PG=1` (set in CI) turns the skip into a failure.
//!
//! Each test creates a fresh database and applies the migrations. Isolation
//! tests open two tenants that use identical ids, so a statement missing its
//! tenant filter returns or changes the other tenant's rows and fails. One
//! test installs forced row-level security policies on every tenant table
//! and connects as a non-superuser (superusers bypass them) to prove the
//! store's `rls_guard` mode works with them, for a later production opt-in.
#![cfg(feature = "postgres")]

#[path = "postgres_backend/agent_messages.rs"]
mod agent_messages;
#[path = "postgres_backend/agents.rs"]
mod agents;
#[path = "postgres_backend/apps.rs"]
mod apps;
#[path = "postgres_backend/attachments.rs"]
mod attachments;
#[path = "postgres_backend/bounded_deletes.rs"]
mod bounded_deletes;
#[path = "postgres_backend/browser_sender.rs"]
mod browser_sender;
#[path = "postgres_backend/clients.rs"]
mod clients;
#[path = "postgres_backend/completion_watches.rs"]
mod completion_watches;
#[path = "postgres_backend/concurrency_claims.rs"]
mod concurrency_claims;
#[path = "postgres_backend/concurrency_counters.rs"]
mod concurrency_counters;
#[path = "postgres_backend/concurrency_delete_desktop.rs"]
mod concurrency_delete_desktop;
#[path = "postgres_backend/concurrency_handles.rs"]
mod concurrency_handles;
#[path = "postgres_backend/concurrency_note_lanes.rs"]
mod concurrency_note_lanes;
#[path = "postgres_backend/concurrency_pool.rs"]
mod concurrency_pool;
#[path = "postgres_backend/concurrency_queue_order.rs"]
mod concurrency_queue_order;
#[path = "postgres_backend/concurrency_support.rs"]
mod concurrency_support;
#[path = "postgres_backend/delete_grace.rs"]
mod delete_grace;
#[path = "postgres_backend/desktop.rs"]
mod desktop;
#[path = "postgres_backend/discussions.rs"]
mod discussions;
#[path = "postgres_backend/dismissal_notices.rs"]
mod dismissal_notices;
#[path = "postgres_backend/events.rs"]
mod events;
#[path = "postgres_backend/harness.rs"]
mod harness;
#[path = "postgres_backend/homes.rs"]
mod homes;
#[path = "postgres_backend/hooks.rs"]
mod hooks;
#[path = "postgres_backend/idempotency.rs"]
mod idempotency;
#[path = "postgres_backend/leases.rs"]
mod leases;
#[path = "postgres_backend/lifecycle_leases.rs"]
mod lifecycle_leases;
#[path = "postgres_backend/mcp_oauth.rs"]
mod mcp_oauth;
#[path = "postgres_backend/nodes.rs"]
mod nodes;
#[path = "postgres_backend/notes.rs"]
mod notes;
#[path = "postgres_backend/pr_monitors.rs"]
mod pr_monitors;
#[path = "postgres_backend/presence_focus.rs"]
mod presence_focus;
#[path = "postgres_backend/principals.rs"]
mod principals;
#[path = "postgres_backend/read_snapshots.rs"]
mod read_snapshots;
#[path = "postgres_backend/repositories.rs"]
mod repositories;
#[path = "postgres_backend/retention.rs"]
mod retention;
#[path = "postgres_backend/row_seq.rs"]
mod row_seq;
#[path = "postgres_backend/schema_gate.rs"]
mod schema_gate;
#[path = "postgres_backend/script_monitors.rs"]
mod script_monitors;
#[path = "postgres_backend/scripts.rs"]
mod scripts;
#[path = "postgres_backend/self_fence.rs"]
mod self_fence;
#[path = "postgres_backend/settings.rs"]
mod settings;
#[path = "postgres_backend/tenant_config.rs"]
mod tenant_config;
#[path = "postgres_backend/tenant_lease.rs"]
mod tenant_lease;
#[path = "postgres_backend/tenant_secrets.rs"]
mod tenant_secrets;
#[path = "postgres_backend/tenant_state.rs"]
mod tenant_state;
#[path = "postgres_backend/usage_metrics.rs"]
mod usage_metrics;
#[path = "postgres_backend/workspaces.rs"]
mod workspaces;
#[path = "postgres_backend/write_role.rs"]
mod write_role;
#[path = "postgres_backend/writer_lock.rs"]
mod writer_lock;
#[path = "support/writer_race.rs"]
mod writer_race;

use harness::{empty_db, exec, open, open_trapped, open_with, test_db, trapped_db};
use intent_store::postgres::{self, TenantId};

#[tokio::test]
async fn settings_are_isolated_between_tenants_with_identical_keys() {
    let Some(db) = test_db(false).await else {
        return;
    };
    let a = open(&db, "tenant-a").await;
    let b = open(&db, "tenant-b").await;

    a.set_setting("userRules", "\"a\"").await.unwrap();
    b.set_setting("userRules", "\"b\"").await.unwrap();
    assert_eq!(
        a.get_setting("userRules").await.unwrap().as_deref(),
        Some("\"a\"")
    );
    assert_eq!(
        b.get_setting("userRules").await.unwrap().as_deref(),
        Some("\"b\"")
    );

    a.set_setting("userRules", "\"a2\"").await.unwrap();
    assert_eq!(
        b.get_setting("userRules").await.unwrap().as_deref(),
        Some("\"b\"")
    );

    assert!(a.delete_setting("userRules").await.unwrap());
    assert_eq!(a.get_setting("userRules").await.unwrap(), None);
    assert_eq!(
        b.get_setting("userRules").await.unwrap().as_deref(),
        Some("\"b\"")
    );
    assert!(!a.delete_setting("userRules").await.unwrap());
}

#[tokio::test]
async fn each_tenant_has_its_own_stable_daemon_id() {
    let Some(db) = test_db(false).await else {
        return;
    };
    let a = open(&db, "tenant-a").await;
    let b = open(&db, "tenant-b").await;
    let a_id = a.daemon_id().await.unwrap();
    assert_eq!(a.daemon_id().await.unwrap(), a_id);
    assert_ne!(b.daemon_id().await.unwrap(), a_id);
    let reopened = open(&db, "tenant-a").await;
    assert_eq!(reopened.daemon_id().await.unwrap(), a_id);
}

#[tokio::test]
async fn opening_a_tenant_provisions_its_seed_rows_once() {
    let Some(db) = test_db(false).await else {
        return;
    };
    for _ in 0..2 {
        open(&db, "tenant-a").await;
        open(&db, "tenant-b").await;
    }
    let primaries: Vec<(String, i64)> = sqlx::query_as(
        "SELECT tenant_id, count(*) FROM principal WHERE is_primary = 1 GROUP BY 1 ORDER BY 1",
    )
    .fetch_all(&db.admin)
    .await
    .unwrap();
    assert_eq!(primaries, [("tenant-a".into(), 1), ("tenant-b".into(), 1)]);
    let chiefs: i64 = sqlx::query_scalar(
        "SELECT count(*) FROM workspace WHERE id = '__chief__' AND owner_principal_id IS NOT NULL",
    )
    .fetch_one(&db.admin)
    .await
    .unwrap();
    assert_eq!(chiefs, 2);
}

/// A table-less advisory lock carries no shard key: on Neki sharded by
/// `tenant_id` the router runs it on another shard than the tenant's, and a
/// single-shard pool fails with NK313 (0005's `intent_provision_tenant` lock
/// made every tenant open fail on a non-authoritative shard). No function
/// the migrations leave behind may take one; the session lock `migrate`
/// takes on its own cross-shard connection is not in a function.
#[tokio::test]
async fn no_migrated_function_takes_an_advisory_lock() {
    let Some(db) = test_db(false).await else {
        return;
    };
    let offenders: Vec<String> = sqlx::query_scalar(
        "SELECT p.proname::text FROM pg_proc p \
         WHERE p.pronamespace = current_schema()::regnamespace \
           AND p.prosrc ILIKE '%pg\\_advisory%' ORDER BY 1",
    )
    .fetch_all(&db.admin)
    .await
    .unwrap();
    assert!(
        offenders.is_empty(),
        "functions taking an advisory lock: {offenders:?}"
    );
}

#[tokio::test]
async fn row_level_security_policies_work_with_the_guard_mode() {
    let Some(db) = test_db(true).await else {
        return;
    };
    let a = open_with(&db, "tenant-a", true).await;
    let b = open_with(&db, "tenant-b", true).await;
    a.set_setting("k", "1").await.unwrap();
    b.set_setting("k", "2").await.unwrap();

    let mut tx = db.app.begin().await.unwrap();
    sqlx::query("SELECT set_config('intent.tenant_id', 'tenant-a', true)")
        .execute(&mut *tx)
        .await
        .unwrap();
    let seen: Vec<String> = sqlx::query_scalar("SELECT value FROM settings WHERE key = 'k'")
        .fetch_all(&mut *tx)
        .await
        .unwrap();
    assert_eq!(seen, ["1"]);
    // Claim tenant B's writer lock marker directly (the guard hides B's lock
    // row) and give an explicit row_seq, so the row_seq trigger neither
    // refuses the insert nor reads tenant_writer: only the guard can reject.
    sqlx::query("SELECT set_config('intent.writer_tenant', 'tenant-b', true)")
        .execute(&mut *tx)
        .await
        .unwrap();
    let foreign = sqlx::query(
        "INSERT INTO settings (tenant_id, key, value, row_seq) VALUES ('tenant-b', 'x', '1', 1)",
    )
    .execute(&mut *tx)
    .await;
    let code = match &foreign {
        Err(sqlx::Error::Database(e)) => e.code().map(std::borrow::Cow::into_owned),
        other => panic!("the guard must reject a write for another tenant: {other:?}"),
    };
    // 42501: new row violates row-level security policy.
    assert_eq!(code.as_deref(), Some("42501"), "{foreign:?}");
    drop(tx);

    let unset: i64 = sqlx::query_scalar("SELECT count(*) FROM settings")
        .fetch_one(&db.app)
        .await
        .unwrap();
    assert_eq!(unset, 0, "without a tenant the guard shows no rows");
}

#[tokio::test]
#[should_panic(expected = "write_pool is SQLite-only and has no Postgres equivalent")]
async fn a_sqlite_only_method_panics_instead_of_touching_sqlite() {
    let Some(db) = test_db(false).await else {
        panic!("write_pool is SQLite-only and has no Postgres equivalent (no test database)");
    };
    let a = open(&db, "tenant-a").await;
    let _ = a.write_pool();
}

#[tokio::test]
async fn closing_one_tenant_store_leaves_the_shared_pool_open() {
    let Some(db) = test_db(false).await else {
        return;
    };
    let a = open(&db, "tenant-a").await;
    let b = open(&db, "tenant-b").await;
    a.close().await;
    b.set_setting("k", "1")
        .await
        .expect("tenant B still works after A closes");
    assert_eq!(b.get_setting("k").await.unwrap().as_deref(), Some("1"));
}

#[tokio::test]
async fn migrate_resumes_a_part_way_file_and_refuses_unknown_history() {
    let Some(db) = test_db(false).await else {
        return;
    };
    let ledger = |sql: &str| exec(&db.admin, sql.to_owned());
    // Every statement of the schema file committed, but the run stopped before it
    // was marked complete: a re-run must not repeat its CREATE TABLEs.
    ledger("UPDATE intent_schema_migrations SET complete = false WHERE version = 1").await;
    postgres::migrate(&db.admin)
        .await
        .expect("resume after the last applied statement");
    let (complete,): (bool,) =
        sqlx::query_as("SELECT complete FROM intent_schema_migrations WHERE version = 1")
            .fetch_one(&db.admin)
            .await
            .unwrap();
    assert!(complete);

    ledger(
        "UPDATE intent_schema_migrations SET complete = false, applied_prefix = '\\x00' \
         WHERE version = 1",
    )
    .await;
    let changed = postgres::migrate(&db.admin).await.unwrap_err().to_string();
    assert!(changed.contains("changed since"), "{changed}");
    ledger("UPDATE intent_schema_migrations SET complete = true WHERE version = 1").await;

    ledger(
        "INSERT INTO intent_schema_migrations \
         (version, description, checksum, applied_statements, applied_prefix, complete) \
         VALUES (9999, 'future', '\\x00', 0, '\\x00', true)",
    )
    .await;
    let unknown = postgres::migrate(&db.admin).await.unwrap_err().to_string();
    assert!(unknown.contains("unknown to this build"), "{unknown}");
}

/// A crash between a statement and its progress record replays that
/// statement: running each one twice must still build a working schema.
#[tokio::test]
async fn every_migration_statement_survives_a_replay() {
    let Some((_, admin)) = empty_db().await else {
        return;
    };
    postgres::migrate_replaying_each_statement(&admin)
        .await
        .expect("each statement runs twice");
    let store = intent_store::Store::open_postgres(
        admin,
        TenantId::new("replayed").unwrap(),
        intent_store::postgres::PostgresOptions::default(),
        intent_store::OrchestratorOnly::for_tests(),
    )
    .await
    .expect("open a tenant on the replayed schema");
    store.set_setting("k", "1").await.unwrap();
    assert_eq!(store.get_setting("k").await.unwrap().as_deref(), Some("1"));
}

/// Runs one statement as the app role with `tenant` set, index scans off so
/// every row is visited, and rolls back.
async fn run_as(db: &harness::TestDb, tenant: &str, sql: &str) -> Result<(), String> {
    run_as_writer_of(db, tenant, tenant, sql).await
}

/// [`run_as`], holding `writer`'s writer lock marker instead of `tenant`'s
/// (set directly: `tenant` cannot see `writer`'s lock row under the trap).
async fn run_as_writer_of(
    db: &harness::TestDb,
    tenant: &str,
    writer: &str,
    sql: &str,
) -> Result<(), String> {
    let mut tx = db.app.begin().await.unwrap();
    for setup in [
        "SET LOCAL enable_indexscan = off",
        "SET LOCAL enable_indexonlyscan = off",
        "SET LOCAL enable_bitmapscan = off",
    ] {
        sqlx::raw_sql(setup).execute(&mut *tx).await.unwrap();
    }
    sqlx::query("SELECT set_config('intent.tenant_id', $1, true)")
        .bind(tenant)
        .execute(&mut *tx)
        .await
        .unwrap();
    // Writes need the tenant's writer lock (the schema's row_seq
    // trigger); the lock statement is itself tenant-scoped.
    if writer == tenant {
        sqlx::query(postgres::WRITER_LOCK_SQL)
            .bind(tenant)
            .fetch_one(&mut *tx)
            .await
            .unwrap();
    } else {
        sqlx::query("SELECT set_config('intent.writer_tenant', $1, true)")
            .bind(writer)
            .execute(&mut *tx)
            .await
            .unwrap();
    }
    let outcome = sqlx::query(sqlx::AssertSqlSafe(sql.to_owned()))
        .bind(tenant)
        .execute(&mut *tx)
        .await
        .map(|_| ())
        .map_err(|e| e.to_string());
    tx.rollback().await.unwrap();
    outcome
}

/// The leak trap behind `parity_test!`: it must fail every statement that
/// reaches another tenant's row (even a write that changes nothing) and must
/// never fire on a correctly filtered statement, even on a full table scan.
#[tokio::test]
async fn the_leak_trap_fails_unscoped_statements_and_only_those() {
    let Some(db) = trapped_db().await else {
        return;
    };
    for tenant in ["decoy", "subject"] {
        open_trapped(&db, tenant)
            .await
            .set_setting("k", "\"same\"")
            .await
            .unwrap();
    }
    let leak = |r: Result<(), String>, sql: &str| {
        let e = r.expect_err(sql);
        assert!(e.contains("tenant leak"), "{sql}: {e}");
    };
    for scoped in [
        "SELECT value FROM settings WHERE tenant_id = $1 AND key = 'k'",
        "SELECT count(*) FROM settings WHERE tenant_id = $1",
        "UPDATE settings SET value = value WHERE tenant_id = $1 AND key = 'k'",
        "DELETE FROM settings WHERE tenant_id = $1 AND key = 'k'",
        "SELECT s.key FROM settings s JOIN settings t ON t.tenant_id = $1 AND t.key = s.key \
         WHERE s.tenant_id = $1",
        "SELECT s.key FROM settings s JOIN settings t ON t.tenant_id = s.tenant_id \
         AND t.key = s.key WHERE s.tenant_id = $1",
        "SELECT key FROM settings s WHERE tenant_id = $1 AND EXISTS \
         (SELECT 1 FROM settings t WHERE t.tenant_id = s.tenant_id AND t.key = s.key)",
        "INSERT INTO settings (tenant_id, key, value) VALUES ($1, 'k', '1') \
         ON CONFLICT (tenant_id, key) DO UPDATE SET value = excluded.value",
    ] {
        run_as(&db, "subject", scoped).await.expect(scoped);
    }
    for unscoped in [
        "SELECT value FROM settings WHERE key = 'k' AND $1 IS NOT NULL",
        "SELECT count(*) FROM settings WHERE $1 IS NOT NULL",
        // A write whose result would be identical still reaches the row.
        "UPDATE settings SET value = value WHERE key = 'k' AND $1 IS NOT NULL",
        "DELETE FROM settings WHERE key = 'k' AND $1 IS NOT NULL",
        // A join that scopes only one side.
        "SELECT s.key FROM settings s JOIN settings t ON t.key = s.key WHERE s.tenant_id = $1",
    ] {
        leak(run_as(&db, "subject", unscoped).await, unscoped);
    }
    // With the decoy's lock marker and an explicit row_seq, so the row_seq
    // trigger (which refuses another tenant's insert on its own) neither
    // refuses it nor reads tenant_writer: the trap sees the inserted row.
    leak(
        run_as_writer_of(
            &db,
            "subject",
            "decoy",
            "INSERT INTO settings (tenant_id, key, value, row_seq) VALUES ('decoy', 'x', $1, 1)",
        )
        .await,
        "insert for another tenant",
    );
    // The known limit the coverage check exists for: an unscoped statement
    // reaches only rows matching its other filters, so a key the decoy lacks
    // is not trapped.
    open_trapped(&db, "subject")
        .await
        .set_setting("subject-only", "1")
        .await
        .unwrap();
    run_as(
        &db,
        "subject",
        "SELECT value FROM settings WHERE key = 'subject-only' AND $1 IS NOT NULL",
    )
    .await
    .expect("an unscoped read of a key only the subject has reaches no decoy row");
    // A store that does not name its tenant cannot read anything on a
    // trapped database, so no code path can skip the tenant context.
    let unnamed = intent_store::Store::open_postgres(
        db.app.clone(),
        TenantId::new("subject").unwrap(),
        intent_store::postgres::PostgresOptions::default(),
        intent_store::OrchestratorOnly::for_tests(),
    )
    .await;
    assert!(
        unnamed.is_err_and(|e| e.to_string().contains("while running as <no tenant>")),
        "a store without tenant context reached a trapped table"
    );
}

#[test]
fn keys_without_decoy_rows_lists_uncovered_keys() {
    let row = |t: &str, k: &str| (t.to_owned(), k.to_owned());
    let decoy = [row("note", "(n1,)"), row("agent_message", "*")];
    let subject = [
        row("note", "(n1,)"),
        row("note", "(subject-only,)"),
        row("note", "(1b4e28ba-2fa1-11d2-883f-0016d3cca427,)"),
        row("agent_message", "*"),
        row("comment", "*"),
    ];
    assert_eq!(
        harness::keys_without_decoy_rows(&decoy, &subject),
        vec!["comment *".to_owned(), "note (subject-only,)".to_owned()]
    );
}

// An early `return` in a body must end only that run: the subject run (the
// one that catches leaks) still happens.
harness::parity_test!(
    #[should_panic(expected = "the subject run happened")]
    an_early_return_does_not_skip_the_subject_run,
    |_store, tenant| {
        let postgres = std::env::var("INTENT_STORE_TEST_PG_URL").is_ok();
        assert!(tenant != "subject" && postgres, "the subject run happened");
        return;
    }
);

#[test]
fn tenant_ids_are_validated() {
    assert!(TenantId::new("org_1:prod-eu.a").is_ok());
    for bad in ["", "has space", "semi;colon", "quote'", &"x".repeat(129)] {
        assert!(TenantId::new(bad).is_err(), "{bad:?} must be rejected");
    }
}
