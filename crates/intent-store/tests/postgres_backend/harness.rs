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

/// Each tenant row's primary key without `tenant_id`, as `(table, key)`.
/// Keys the store generates itself (identity columns, UUIDs) can never match
/// between tenants and are reported as `(table, "*")`: there only the table is
/// compared.
pub async fn tenant_keys(admin: &PgPool, tenant: &str) -> Vec<(String, String)> {
    let mut keys = Vec::new();
    for table in tenant_tables(admin).await {
        let cols: Vec<(String, bool)> = sqlx::query_as(
            "SELECT a.attname::text, (a.attidentity <> '' OR coalesce(pg_get_expr(d.adbin, d.adrelid), '') \
                 LIKE 'nextval(%') \
             FROM pg_index i \
             JOIN pg_attribute a ON a.attrelid = i.indrelid AND a.attnum = ANY(i.indkey) \
             LEFT JOIN pg_attrdef d ON d.adrelid = a.attrelid AND d.adnum = a.attnum \
             WHERE i.indrelid = $1::regclass AND i.indisprimary AND a.attname <> 'tenant_id' \
             ORDER BY array_position(i.indkey::int2[], a.attnum)",
        )
        .bind(&table)
        .fetch_all(admin)
        .await
        .expect("read primary key");
        let generated = cols.is_empty() || cols.iter().any(|(_, g)| *g);
        let expr = if generated {
            "'*'".to_owned()
        } else {
            let list: Vec<&str> = cols.iter().map(|(c, _)| c.as_str()).collect();
            format!("ROW({}, NULL)::text", list.join(", "))
        };
        let rows: Vec<String> = sqlx::query_scalar(sqlx::AssertSqlSafe(format!(
            "SELECT DISTINCT {expr} FROM {table} WHERE tenant_id = $1"
        )))
        .bind(tenant)
        .fetch_all(admin)
        .await
        .expect("read tenant keys");
        keys.extend(rows.into_iter().map(|k| (table.clone(), k)));
    }
    keys
}

/// Subject rows the trap could not have guarded: their key (or, for
/// generated keys, their table) had no decoy row when the subject started.
/// Keys holding a UUID are skipped: the store generated them, so no tenant
/// can share them.
pub fn keys_without_decoy_rows(
    decoy_before: &[(String, String)],
    subject_after: &[(String, String)],
) -> Vec<String> {
    let decoy: std::collections::BTreeSet<&(String, String)> = decoy_before.iter().collect();
    let decoy_tables: std::collections::BTreeSet<&str> =
        decoy_before.iter().map(|(t, _)| t.as_str()).collect();
    let missing: std::collections::BTreeSet<String> = subject_after
        .iter()
        .filter(|row| {
            let (table, key) = row;
            if key == "*" {
                !decoy_tables.contains(table.as_str())
            } else {
                !decoy.contains(row) && !holds_uuid(key)
            }
        })
        .map(|(t, k)| format!("{t} {k}"))
        .collect();
    missing.into_iter().collect()
}

fn holds_uuid(key: &str) -> bool {
    key.as_bytes().windows(36).any(|w| {
        w.iter().enumerate().all(|(i, b)| match i {
            8 | 13 | 18 | 23 => *b == b'-',
            _ => b.is_ascii_hexdigit(),
        })
    })
}

pub async fn open(db: &TestDb, tenant: &str) -> Store {
    open_with(db, tenant, false).await
}

pub async fn open_with(db: &TestDb, tenant: &str, rls_guard: bool) -> Store {
    Store::open_postgres(
        db.app.clone(),
        TenantId::new(tenant).unwrap(),
        PostgresOptions {
            rls_guard,
            ..PostgresOptions::default()
        },
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

/// `stmt` on this run's rows. `{t}` becomes `tenant_id, ` (column lists) and
/// `{v}` the tenant value on Postgres, `{w}` the tenant filter; Postgres runs
/// it through the admin pool, which the leak trap does not apply to.
pub async fn seed_sql(store: &Store, db: Option<&TestDb>, tenant: &str, stmt: &str) {
    let (column, value, filter) = if db.is_some() {
        (
            "tenant_id, ".to_owned(),
            format!("'{tenant}', "),
            format!("tenant_id = '{tenant}'"),
        )
    } else {
        (String::new(), String::new(), "1 = 1".to_owned())
    };
    let stmt = stmt
        .replace("{t}", &column)
        .replace("{v}", &value)
        .replace("{w}", &filter);
    let query = sqlx::raw_sql(sqlx::AssertSqlSafe(stmt));
    if let Some(db) = db {
        query.execute(&db.admin).await.expect("setup statement");
    } else {
        query
            .execute(store.write_pool())
            .await
            .expect("setup statement");
    }
}

/// Rows of `table` matching `filter`, among this run's rows.
pub async fn count_rows(
    store: &Store,
    db: Option<&TestDb>,
    tenant: &str,
    table: &str,
    filter: &str,
) -> i64 {
    match db {
        Some(db) => sqlx::query_scalar(sqlx::AssertSqlSafe(format!(
            "SELECT count(*) FROM {table} WHERE tenant_id = '{tenant}' AND ({filter})"
        )))
        .fetch_one(&db.admin)
        .await
        .expect("count rows"),
        None => sqlx::query_scalar(sqlx::AssertSqlSafe(format!(
            "SELECT count(*) FROM {table} WHERE {filter}"
        )))
        .fetch_one(store.read_pool())
        .await
        .expect("count rows"),
    }
}

/// One test body, run on every backend: `SQLite`, then Postgres twice in one
/// database — first for a decoy tenant, then for the tenant under test.
/// - Leak trap: the Postgres database fails any statement that touches a row
///   of another tenant ([`trapped_db`]), so a statement missing its tenant
///   filter fails on the subject run as soon as it reaches a decoy row, even
///   when its result or write would not have differed.
/// - Coverage: the trap fires only on rows a statement reaches through its
///   other filters (an unscoped `WHERE id = $2` reaches only rows with that
///   id), so every row the subject leaves must have a decoy row with the same
///   primary key when the subject starts (generated keys: the same table).
///   The body uses fixed ids, leaves rows behind and adds decoy-only rows as
///   needed. Not covered: tables the body empties by its end, and ids the
///   subject only probes as absent; give the decoy those rows.
/// - Writes: every decoy row is snapshotted before the subject run and
///   compared after it.
///
/// The body may also bind the tenant name (`"sqlite"`, `"decoy"`,
/// `"subject"`) to write tenant-specific values or decoy-only rows. Without
/// `INTENT_STORE_TEST_PG_URL` only the `SQLite` run happens (CI requires the
/// URL). Each run prints its backend, so a failure names it.
///
/// Each run is its own `async` block, so an early `return` ends only that
/// run. A third closure argument binds the test database (`None` on
/// `SQLite`) for bodies that seed rows with SQL. Attributes such as `#[should_panic]` may precede the name.
///
/// ```ignore
/// parity_test!(name, |store, tenant| { store.set_setting("k", tenant).await.unwrap(); });
/// ```
macro_rules! parity_test {
    ($(#[$meta:meta])* $name:ident, |$store:ident| $body:block) => {
        $crate::harness::parity_test!($(#[$meta])* $name, |$store, _tenant| $body);
    };
    ($(#[$meta:meta])* $name:ident, |$store:ident, $tenant:ident| $body:block) => {
        $crate::harness::parity_test!($(#[$meta])* $name, |$store, $tenant, _db| $body);
    };
    ($(#[$meta:meta])* $name:ident, |$store:ident, $tenant:ident, $db:ident| $body:block) => {
        #[tokio::test]
        $(#[$meta])*
        async fn $name() {
            {
                eprintln!("[{}] backend: sqlite", stringify!($name));
                let (store, _dir) = $crate::harness::sqlite_store().await;
                let $store = store;
                let $tenant: &str = "sqlite";
                let $db: Option<&$crate::harness::TestDb> = None;
                async { $body }.await;
            }
            if let Some(db) = $crate::harness::trapped_db().await {
                eprintln!("[{}] backend: postgres, tenant decoy", stringify!($name));
                {
                    let $store = $crate::harness::open_trapped(&db, "decoy").await;
                    let $tenant: &str = "decoy";
                    let $db: Option<&$crate::harness::TestDb> = Some(&db);
                    async { $body }.await;
                }
                let before = $crate::harness::tenant_rows(&db.admin, "decoy").await;
                let decoy_keys = $crate::harness::tenant_keys(&db.admin, "decoy").await;
                eprintln!("[{}] backend: postgres, tenant subject", stringify!($name));
                {
                    let $store = $crate::harness::open_trapped(&db, "subject").await;
                    let $tenant: &str = "subject";
                    let $db: Option<&$crate::harness::TestDb> = Some(&db);
                    async { $body }.await;
                }
                let after = $crate::harness::tenant_rows(&db.admin, "decoy").await;
                assert!(
                    before == after,
                    "the subject tenant's run changed the decoy tenant's rows"
                );
                let subject = $crate::harness::tenant_keys(&db.admin, "subject").await;
                let uncovered = $crate::harness::keys_without_decoy_rows(&decoy_keys, &subject);
                assert!(
                    uncovered.is_empty(),
                    "the decoy had no row with these keys when the subject ran, so a statement \
                     missing its tenant filter there would go unnoticed (leave decoy rows with \
                     the same keys): {uncovered:?}"
                );
            }
        }
    };
}
pub(crate) use parity_test;
