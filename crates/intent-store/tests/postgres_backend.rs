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

use intent_store::postgres::{self, PgPool, PostgresOptions, TenantId};
use intent_store::Store;

const APP_ROLE: &str = "intent_store_test_app";

async fn exec(pool: &PgPool, sql: String) {
    sqlx::raw_sql(sqlx::AssertSqlSafe(sql))
        .execute(pool)
        .await
        .expect("setup statement");
}

struct TestDb {
    admin: PgPool,
    app: PgPool,
}

async fn test_db(rls_guard: bool) -> Option<TestDb> {
    let Ok(base) = std::env::var("INTENT_STORE_TEST_PG_URL") else {
        assert!(
            std::env::var("INTENT_STORE_REQUIRE_PG").as_deref() != Ok("1"),
            "INTENT_STORE_REQUIRE_PG=1 but INTENT_STORE_TEST_PG_URL is unset"
        );
        eprintln!("skipping: INTENT_STORE_TEST_PG_URL is unset");
        return None;
    };
    let name = format!("intent_store_test_{}", uuid::Uuid::new_v4().simple());
    let server = postgres::connect(&base, 2)
        .await
        .expect("connect to server");
    exec(&server, format!("CREATE DATABASE {name}")).await;
    // Roles are cluster-wide; tests may run in parallel.
    exec(
        &server,
        format!(
            "DO $$ BEGIN CREATE ROLE {APP_ROLE} NOSUPERUSER NOLOGIN; \
             EXCEPTION WHEN duplicate_object OR unique_violation THEN NULL; END $$"
        ),
    )
    .await;
    server.close().await;

    let url = with_database(&base, &name);
    let admin = postgres::connect(&url, 2).await.expect("connect admin");
    postgres::migrate(&admin).await.expect("migrate");
    if rls_guard {
        exec(&admin, rls_guard_sql(&admin).await).await;
    }
    exec(
        &admin,
        format!(
            "GRANT USAGE ON SCHEMA public TO {APP_ROLE}; \
             GRANT SELECT, INSERT, UPDATE, DELETE ON ALL TABLES IN SCHEMA public TO {APP_ROLE}; \
             GRANT USAGE, SELECT ON ALL SEQUENCES IN SCHEMA public TO {APP_ROLE}; \
             GRANT EXECUTE ON ALL FUNCTIONS IN SCHEMA public TO {APP_ROLE};"
        ),
    )
    .await;
    let app = sqlx::postgres::PgPoolOptions::new()
        .max_connections(4)
        .after_connect(|conn, _| {
            Box::pin(async move {
                sqlx::raw_sql(sqlx::AssertSqlSafe(format!("SET ROLE {APP_ROLE}")))
                    .execute(&mut *conn)
                    .await?;
                Ok(())
            })
        })
        .connect(&url)
        .await
        .expect("connect app pool");
    Some(TestDb { admin, app })
}

fn with_database(url: &str, name: &str) -> String {
    let (head, query) = url
        .split_once('?')
        .map_or((url, None), |(h, q)| (h, Some(q)));
    let base = head.rsplit_once('/').map_or(head, |(b, _)| b);
    match query {
        Some(q) => format!("{base}/{name}?{q}"),
        None => format!("{base}/{name}"),
    }
}

/// FORCE row-level security on every table with a `tenant_id` column.
async fn rls_guard_sql(admin: &PgPool) -> String {
    let tables: Vec<String> = sqlx::query_scalar(
        "SELECT table_name::text FROM information_schema.columns \
         WHERE table_schema = 'public' AND column_name = 'tenant_id' ORDER BY 1",
    )
    .fetch_all(admin)
    .await
    .expect("list tenant tables");
    assert!(
        tables.len() > 50,
        "found only {} tenant tables",
        tables.len()
    );
    tables
        .iter()
        .map(|t| {
            format!(
                "ALTER TABLE {t} ENABLE ROW LEVEL SECURITY; ALTER TABLE {t} FORCE ROW LEVEL SECURITY; \
                 CREATE POLICY tenant_guard ON {t} \
                 USING (tenant_id = current_setting('intent.tenant_id', true)) \
                 WITH CHECK (tenant_id = current_setting('intent.tenant_id', true));"
            )
        })
        .collect()
}

async fn open(db: &TestDb, tenant: &str) -> Store {
    open_with(db, tenant, false).await
}

async fn open_with(db: &TestDb, tenant: &str, rls_guard: bool) -> Store {
    Store::open_postgres(
        db.app.clone(),
        TenantId::new(tenant).unwrap(),
        PostgresOptions { rls_guard },
    )
    .await
    .expect("open tenant store")
}

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
    let foreign =
        sqlx::query("INSERT INTO settings (tenant_id, key, value) VALUES ('tenant-b', 'x', '1')")
            .execute(&mut *tx)
            .await;
    assert!(
        foreign.is_err(),
        "the guard must reject a write for another tenant"
    );
    drop(tx);

    let unset: i64 = sqlx::query_scalar("SELECT count(*) FROM settings")
        .fetch_one(&db.app)
        .await
        .unwrap();
    assert_eq!(unset, 0, "without a tenant the guard shows no rows");
}

#[tokio::test]
async fn an_unported_method_reports_an_error_instead_of_touching_sqlite() {
    let Some(db) = test_db(false).await else {
        return;
    };
    let a = open(&db, "tenant-a").await;
    let err = a
        .list_workspaces(intent_core::WorkspaceListFilter::default())
        .await
        .unwrap_err()
        .to_string();
    assert!(
        err.contains("list_workspaces is not yet supported on the Postgres store"),
        "{err}"
    );
}

#[test]
fn tenant_ids_are_validated() {
    assert!(TenantId::new("org_1:prod-eu.a").is_ok());
    for bad in ["", "has space", "semi;colon", "quote'", &"x".repeat(129)] {
        assert!(TenantId::new(bad).is_err(), "{bad:?} must be rejected");
    }
}
