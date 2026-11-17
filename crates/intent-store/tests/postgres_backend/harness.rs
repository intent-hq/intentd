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

pub async fn exec(pool: &PgPool, sql: String) {
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

/// Every row the tenant owns, as `(table, row text)`, in a stable order.
pub async fn tenant_rows(admin: &PgPool, tenant: &str) -> Vec<(String, String)> {
    let tables: Vec<String> = sqlx::query_scalar(
        "SELECT table_name::text FROM information_schema.columns \
         WHERE table_schema = 'public' AND column_name = 'tenant_id' ORDER BY 1",
    )
    .fetch_all(admin)
    .await
    .expect("list tenant tables");
    let mut rows = Vec::new();
    for table in tables {
        let texts: Vec<String> = sqlx::query_scalar(sqlx::AssertSqlSafe(format!(
            "SELECT t::text FROM {table} t WHERE tenant_id = $1 ORDER BY 1"
        )))
        .bind(tenant)
        .fetch_all(admin)
        .await
        .expect("read tenant rows");
        rows.extend(texts.into_iter().map(|t| (table.clone(), t)));
    }
    rows
}

/// One test body, run on every backend: `SQLite`, then Postgres twice in one
/// database — first for a decoy tenant, then for the tenant under test.
/// - Reads: the body uses fixed ids and leaves rows behind, so a statement
///   missing its tenant filter sees the decoy's rows on the subject run.
/// - Writes: every decoy row is snapshotted before the subject run and
///   compared after it, so a write that reaches another tenant fails.
///
/// The body may also bind the tenant name (`"sqlite"`, `"decoy"`,
/// `"subject"`) to write tenant-specific values or decoy-only rows. Without
/// `INTENT_STORE_TEST_PG_URL` only the `SQLite` run happens (CI requires the
/// URL). Each run prints its backend, so a failure names it.
///
/// ```ignore
/// parity_test!(name, |store, tenant| { store.set_setting("k", tenant).await.unwrap(); });
/// ```
macro_rules! parity_test {
    ($name:ident, |$store:ident| $body:block) => {
        $crate::harness::parity_test!($name, |$store, _tenant| $body);
    };
    ($name:ident, |$store:ident, $tenant:ident| $body:block) => {
        #[tokio::test]
        async fn $name() {
            {
                eprintln!("[{}] backend: sqlite", stringify!($name));
                let (store, _dir) = $crate::harness::sqlite_store().await;
                let $store = store;
                let $tenant: &str = "sqlite";
                $body
            }
            if let Some(db) = $crate::harness::test_db(false).await {
                eprintln!("[{}] backend: postgres, tenant decoy", stringify!($name));
                {
                    let $store = $crate::harness::open(&db, "decoy").await;
                    let $tenant: &str = "decoy";
                    $body
                }
                let before = $crate::harness::tenant_rows(&db.admin, "decoy").await;
                eprintln!("[{}] backend: postgres, tenant subject", stringify!($name));
                {
                    let $store = $crate::harness::open(&db, "subject").await;
                    let $tenant: &str = "subject";
                    $body
                }
                let after = $crate::harness::tenant_rows(&db.admin, "decoy").await;
                assert!(
                    before == after,
                    "the subject tenant's run changed the decoy tenant's rows"
                );
            }
        }
    };
}
pub(crate) use parity_test;
