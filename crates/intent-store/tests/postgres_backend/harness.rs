//! Shared setup for the Postgres backend tests: a fresh migrated database per
//! test, tenant stores, and [`parity_test!`] for running one test body on
//! both backends.

use intent_store::postgres::{self, PgPool, PostgresOptions, TenantId};
use intent_store::Store;
use std::fmt::Write as _;

const APP_ROLE: &str = "intent_store_test_app";
const DB_PREFIX: &str = "intent_store_test_";
/// Test databases older than this are left over from earlier runs.
const STALE_AFTER_SECS: u64 = 3600;

/// Drop databases earlier runs left behind (each test makes its own and
/// cannot drop it from a synchronous `Drop`). Their names carry the creation
/// time; one still in use fails to drop and is skipped.
async fn drop_stale_databases(server: &PgPool, now: u64) {
    let names: Vec<String> =
        sqlx::query_scalar("SELECT datname::text FROM pg_database WHERE starts_with(datname, $1)")
            .bind(DB_PREFIX)
            .fetch_all(server)
            .await
            .expect("list test databases");
    for name in names {
        let created = name
            .strip_prefix(DB_PREFIX)
            .and_then(|rest| rest.split('_').next())
            .and_then(|secs| secs.parse::<u64>().ok());
        if created.is_some_and(|t| now.saturating_sub(t) > STALE_AFTER_SECS) {
            let _ = sqlx::raw_sql(sqlx::AssertSqlSafe(format!(
                "DROP DATABASE IF EXISTS {name}"
            )))
            .execute(server)
            .await;
        }
    }
}

async fn exec(pool: &PgPool, sql: String) {
    sqlx::raw_sql(sqlx::AssertSqlSafe(sql))
        .execute(pool)
        .await
        .expect("setup statement");
}

pub struct TestDb {
    pub admin: PgPool,
    pub app: PgPool,
}

pub async fn test_db(rls_guard: bool) -> Option<TestDb> {
    let Ok(base) = std::env::var("INTENT_STORE_TEST_PG_URL") else {
        assert!(
            std::env::var("INTENT_STORE_REQUIRE_PG").as_deref() != Ok("1"),
            "INTENT_STORE_REQUIRE_PG=1 but INTENT_STORE_TEST_PG_URL is unset"
        );
        eprintln!("skipping: INTENT_STORE_TEST_PG_URL is unset");
        return None;
    };
    let now = std::time::SystemTime::now()
        .duration_since(std::time::UNIX_EPOCH)
        .expect("clock")
        .as_secs();
    let name = format!("{DB_PREFIX}{now}_{}", uuid::Uuid::new_v4().simple());
    let server = postgres::connect(&base, 2)
        .await
        .expect("connect to server");
    drop_stale_databases(&server, now).await;
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
    let mut sql = String::new();
    for t in &tables {
        write!(
            sql,
            "ALTER TABLE {t} ENABLE ROW LEVEL SECURITY; ALTER TABLE {t} FORCE ROW LEVEL SECURITY; \
             CREATE POLICY tenant_guard ON {t} \
             USING (tenant_id = current_setting('intent.tenant_id', true)) \
             WITH CHECK (tenant_id = current_setting('intent.tenant_id', true));"
        )
        .expect("write to String");
    }
    sql
}

pub async fn open(db: &TestDb, tenant: &str) -> Store {
    open_with(db, tenant, false).await
}

pub async fn open_with(db: &TestDb, tenant: &str, rls_guard: bool) -> Store {
    Store::open_postgres(
        db.app.clone(),
        TenantId::new(tenant).unwrap(),
        PostgresOptions { rls_guard },
    )
    .await
    .expect("open tenant store")
}

/// A fresh `SQLite` store in a temporary directory (kept alive by the guard).
pub async fn sqlite_store() -> (Store, tempfile::TempDir) {
    let dir = tempfile::tempdir().expect("temp dir");
    let store = Store::open(&dir.path().join("intent.db"))
        .await
        .expect("open sqlite store");
    (store, dir)
}

/// One test body, run on every backend: `SQLite`, then Postgres twice in one
/// database — first for a decoy tenant, then for the tenant under test. The
/// body uses fixed ids, so a Postgres statement missing its tenant filter
/// sees or changes the decoy's rows on the second run and the test fails.
/// Without `INTENT_STORE_TEST_PG_URL` only the `SQLite` run happens (CI
/// requires the URL). Each run prints its backend, so a failure names it.
///
/// ```ignore
/// parity_test!(name, |store| { store.set_setting("k", "v").await.unwrap(); });
/// ```
macro_rules! parity_test {
    ($name:ident, |$store:ident| $body:block) => {
        #[tokio::test]
        async fn $name() {
            {
                eprintln!("[{}] backend: sqlite", stringify!($name));
                let (store, _dir) = $crate::harness::sqlite_store().await;
                let $store = store;
                $body
            }
            if let Some(db) = $crate::harness::test_db(false).await {
                for tenant in ["decoy", "subject"] {
                    eprintln!("[{}] backend: postgres, tenant {tenant}", stringify!($name));
                    let $store = $crate::harness::open(&db, tenant).await;
                    $body
                }
            }
        }
    };
}
pub(crate) use parity_test;
