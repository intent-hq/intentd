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
    let (url, admin) = empty_db().await?;
    postgres::migrate(&admin).await.expect("migrate");
    finish_test_db(url, admin, rls_guard).await
}

/// A migrated database with the leak trap on every tenant table: any
/// statement that reads, updates, deletes or inserts a row of a tenant other
/// than the store's own fails with "tenant leak". Open stores on it with
/// [`open_trapped`] so each transaction names its tenant.
pub async fn trapped_db() -> Option<TestDb> {
    let (url, admin) = empty_db().await?;
    postgres::migrate(&admin).await.expect("migrate");
    exec(&admin, trap_sql(&admin).await).await;
    finish_test_db(url, admin, false).await
}

/// A fresh, unmigrated database: its URL and an admin pool.
pub async fn empty_db() -> Option<(String, PgPool)> {
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
    Some((url, admin))
}

async fn finish_test_db(url: String, admin: PgPool, rls_guard: bool) -> Option<TestDb> {
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
    let tables = tenant_tables(admin).await;
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

/// Policies that raise on any row of another tenant. A tenant filter
/// (`tenant_id = $n`, a leakproof comparison) runs before the policy, so
/// correctly scoped statements never reach foreign rows; a statement that
/// misses its filter reaches the decoy's rows and fails, whatever its result
/// would have been.
async fn trap_sql(admin: &PgPool) -> String {
    let mut sql = String::from(
        "CREATE FUNCTION intent_test_tenant_trap(row_tenant text) RETURNS boolean \
         LANGUAGE plpgsql STABLE AS $trap$ \
         BEGIN \
           IF row_tenant IS DISTINCT FROM current_setting('intent.tenant_id', true) THEN \
             RAISE EXCEPTION 'tenant leak: a statement touched a row of tenant % while running as %', \
               row_tenant, coalesce(nullif(current_setting('intent.tenant_id', true), ''), '<no tenant>'); \
           END IF; \
           RETURN true; \
         END $trap$;",
    );
    for t in tenant_tables(admin).await {
        write!(
            sql,
            "ALTER TABLE {t} ENABLE ROW LEVEL SECURITY; \
             CREATE POLICY tenant_trap ON {t} \
             USING (intent_test_tenant_trap(tenant_id)) \
             WITH CHECK (intent_test_tenant_trap(tenant_id));"
        )
        .expect("write to String");
    }
    sql
}

async fn tenant_tables(admin: &PgPool) -> Vec<String> {
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
}

/// A store whose every transaction names its tenant, for [`trapped_db`].
pub async fn open_trapped(db: &TestDb, tenant: &str) -> Store {
    open_with(db, tenant, true).await
}

/// The tables in which the subject left rows but the decoy had none when the
/// subject started: there the trap had nothing to catch.
pub fn tables_without_decoy_rows(
    decoy_before: &[(String, String)],
    subject_after: &[(String, String)],
) -> Vec<String> {
    let decoy: std::collections::BTreeSet<&str> =
        decoy_before.iter().map(|(t, _)| t.as_str()).collect();
    let mut missing: Vec<String> = subject_after
        .iter()
        .map(|(t, _)| t.as_str())
        .filter(|t| !decoy.contains(t))
        .map(str::to_owned)
        .collect();
    missing.dedup();
    missing
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
    let tables = tenant_tables(admin).await;
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
/// - Leak trap: the Postgres database fails any statement that touches a row
///   of another tenant ([`trapped_db`]), so a statement missing its tenant
///   filter fails on the subject run as soon as it reaches a decoy row, even
///   when its result or write would not have differed.
/// - Coverage: every table the subject leaves rows in must hold decoy rows
///   when the subject starts, or the trap had nothing to catch there; the
///   body leaves rows behind (fixed ids) and adds decoy-only rows as needed.
/// - Writes: every decoy row is snapshotted before the subject run and
///   compared after it.
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
            if let Some(db) = $crate::harness::trapped_db().await {
                eprintln!("[{}] backend: postgres, tenant decoy", stringify!($name));
                {
                    let $store = $crate::harness::open_trapped(&db, "decoy").await;
                    let $tenant: &str = "decoy";
                    $body
                }
                let before = $crate::harness::tenant_rows(&db.admin, "decoy").await;
                eprintln!("[{}] backend: postgres, tenant subject", stringify!($name));
                {
                    let $store = $crate::harness::open_trapped(&db, "subject").await;
                    let $tenant: &str = "subject";
                    $body
                }
                let after = $crate::harness::tenant_rows(&db.admin, "decoy").await;
                assert!(
                    before == after,
                    "the subject tenant's run changed the decoy tenant's rows"
                );
                let subject = $crate::harness::tenant_rows(&db.admin, "subject").await;
                let uncovered = $crate::harness::tables_without_decoy_rows(&before, &subject);
                assert!(
                    uncovered.is_empty(),
                    "the decoy had no rows in {uncovered:?} when the subject ran, so a missing \
                     tenant filter there would go unnoticed: leave decoy rows in those tables"
                );
            }
        }
    };
}
pub(crate) use parity_test;
