//! Multi-tenant Postgres backend (cargo feature `postgres`).
//!
//! A [`Store`](crate::Store) opened with [`Store::open_postgres`](crate::Store::open_postgres)
//! serves exactly one tenant: every statement binds that tenant's id, so
//! callers never pass one. Tenants share one database (PlanetScale Neki in
//! production, sharded by `tenant_id`); the schema lives in
//! `postgres/migrations/` and `postgres_tenant_schema_lint` keeps every table,
//! key and trigger tenant-scoped.
//!
//! Release builds never enable this feature. Methods not ported yet go
//! through `pending_postgres!` and return [`unported`] errors.

use std::sync::Arc;

use intent_core::{Error, Result};
use sqlx::postgres::{PgConnectOptions, PgPoolOptions, Postgres};
use sqlx::Transaction;

pub use sqlx::postgres::PgPool;

mod settings;
mod workspaces;

/// Postgres schema migrations, separate from the `SQLite` ones.
pub static MIGRATOR: sqlx::migrate::Migrator = sqlx::migrate!("./postgres/migrations");

/// The tenant a Postgres store serves: 1-128 characters of ASCII letters,
/// digits, `-`, `_`, `.` and `:`.
#[derive(Debug, Clone, PartialEq, Eq, Hash)]
pub struct TenantId(Arc<str>);

impl TenantId {
    /// # Errors
    /// `Error::InvalidParams` when `id` is empty, longer than 128 characters,
    /// or uses another character.
    pub fn new(id: &str) -> Result<Self> {
        let valid = (1..=128).contains(&id.len())
            && id
                .bytes()
                .all(|b| b.is_ascii_alphanumeric() || matches!(b, b'-' | b'_' | b'.' | b':'));
        if !valid {
            return Err(Error::InvalidParams(format!("invalid tenant id: {id:?}")));
        }
        Ok(Self(id.into()))
    }

    #[must_use]
    pub fn as_str(&self) -> &str {
        &self.0
    }
}

/// How a Postgres store runs its transactions.
#[derive(Debug, Clone, Copy, Default)]
pub struct PostgresOptions {
    /// Set `intent.tenant_id` in every transaction, for the row-level
    /// security guard the store tests install. Production stores leave it
    /// off: their isolation is the tenant filter in every statement.
    pub rls_guard: bool,
}

/// Connect options for a `postgres://` URL. libpq's `sslrootcert=system`
/// ("verify against the system trust store", which `PlanetScale` URLs carry)
/// is dropped: sqlx would read it as a file path. The bundled webpki roots
/// then verify the server, and `sslmode` (for example `verify-full`) stays.
///
/// # Errors
/// `Error::InvalidParams` when the URL does not parse.
pub fn connect_options(url: &str) -> Result<PgConnectOptions> {
    without_system_root_cert(url)
        .parse::<PgConnectOptions>()
        .map_err(|e| Error::InvalidParams(format!("invalid postgres url: {e}")))
}

fn without_system_root_cert(url: &str) -> String {
    let Some((base, query)) = url.split_once('?') else {
        return url.to_owned();
    };
    let kept: Vec<&str> = query
        .split('&')
        .filter(|kv| *kv != "sslrootcert=system")
        .collect();
    if kept.is_empty() {
        base.to_owned()
    } else {
        format!("{base}?{}", kept.join("&"))
    }
}

/// Connect a pool. The caller runs [`migrate`] once per database.
///
/// # Errors
/// `Error::Internal` when the connection fails.
pub async fn connect(url: &str, max_connections: u32) -> Result<PgPool> {
    PgPoolOptions::new()
        .max_connections(max_connections)
        .connect_with(connect_options(url)?)
        .await
        .map_err(|e| Error::Internal(format!("postgres connect failed: {e}")))
}

/// Apply the Postgres schema migrations that are not applied yet.
///
/// Statement by statement, each in its own transaction: `PlanetScale` Neki
/// cannot plan a statement against a relation created earlier in the same
/// transaction (Neki error 817), and both sqlx's migrator and a
/// multi-statement simple query wrap a whole file in one. A session advisory
/// lock keeps concurrent callers from migrating at once. A file that fails
/// part-way is not recorded and its earlier statements stay applied; fix it
/// forward.
///
/// # Errors
/// `Error::Internal` when a statement fails or an applied migration's
/// checksum changed.
pub async fn migrate(pool: &PgPool) -> Result<()> {
    let mut conn = pool.acquire().await.map_err(db)?;
    sqlx::query("SELECT pg_advisory_lock(hashtextextended('intent-schema-migrations', 0))")
        .execute(&mut *conn)
        .await
        .map_err(db)?;
    let outcome = migrate_locked(&mut conn).await;
    let unlocked =
        sqlx::query("SELECT pg_advisory_unlock(hashtextextended('intent-schema-migrations', 0))")
            .execute(&mut *conn)
            .await
            .map_err(db);
    outcome.and(unlocked.map(|_| ()))
}

async fn migrate_locked(conn: &mut sqlx::PgConnection) -> Result<()> {
    // The applied-migration ledger belongs to the schema, not to a tenant.
    sqlx::raw_sql(
        "CREATE TABLE IF NOT EXISTS intent_schema_migrations \
         (version bigint PRIMARY KEY, description text NOT NULL, checksum bytea NOT NULL, \
          applied_at timestamptz NOT NULL DEFAULT now())",
    )
    .execute(&mut *conn)
    .await
    .map_err(db)?;
    let applied: Vec<(i64, Vec<u8>)> =
        sqlx::query_as("SELECT version, checksum FROM intent_schema_migrations ORDER BY version")
            .fetch_all(&mut *conn)
            .await
            .map_err(db)?;
    for migration in MIGRATOR.iter() {
        if let Some((_, checksum)) = applied.iter().find(|(v, _)| *v == migration.version) {
            if checksum.as_slice() != migration.checksum.as_ref() {
                return Err(Error::Internal(format!(
                    "postgres migration {} changed after it was applied",
                    migration.version
                )));
            }
            continue;
        }
        for statement in statements(migration.sql.as_str()) {
            sqlx::raw_sql(sqlx::AssertSqlSafe(statement.to_owned()))
                .execute(&mut *conn)
                .await
                .map_err(|e| {
                    Error::Internal(format!(
                        "postgres migration {} ({}) failed: {e}",
                        migration.version, migration.description
                    ))
                })?;
        }
        sqlx::query(
            "INSERT INTO intent_schema_migrations (version, description, checksum) \
             VALUES ($1, $2, $3)",
        )
        .bind(migration.version)
        .bind(migration.description.as_ref())
        .bind(migration.checksum.as_ref())
        .execute(&mut *conn)
        .await
        .map_err(db)?;
    }
    Ok(())
}

/// The statements of a migration file: split at a `;` that ends a line,
/// outside `$$` function bodies; comment-only chunks dropped.
fn statements(sql: &str) -> Vec<&str> {
    let mut out = Vec::new();
    let mut start = 0;
    let mut in_body = false;
    let mut offset = 0;
    for line in sql.split_inclusive('\n') {
        if line.matches("$$").count() % 2 == 1 {
            in_body = !in_body;
        }
        offset += line.len();
        if !in_body && line.trim_end().ends_with(';') {
            out.push(&sql[start..offset]);
            start = offset;
        }
    }
    out.push(&sql[start..]);
    out.into_iter()
        .map(str::trim)
        .filter(|s| {
            s.lines()
                .any(|l| !l.trim().is_empty() && !l.trim_start().starts_with("--"))
        })
        .collect()
}

#[derive(Clone)]
pub(crate) struct PgStore {
    pool: PgPool,
    tenant: TenantId,
    options: PostgresOptions,
}

impl PgStore {
    pub(crate) fn tenant(&self) -> &str {
        self.tenant.as_str()
    }

    /// Every operation runs in one transaction bound to this store's tenant.
    pub(crate) async fn begin(&self) -> Result<Transaction<'static, Postgres>> {
        let mut tx = self.pool.begin().await.map_err(db)?;
        if self.options.rls_guard {
            sqlx::query("SELECT set_config('intent.tenant_id', $1, true)")
                .bind(self.tenant())
                .execute(&mut *tx)
                .await
                .map_err(db)?;
        }
        Ok(tx)
    }

    /// A write transaction that holds this tenant's writer lock until it
    /// ends. `SQLite` runs one writer at a time (`BEGIN IMMEDIATE` on a
    /// single-connection pool) and store code relies on that for its
    /// read-then-write sequences; this keeps that guarantee per tenant while
    /// other tenants write in parallel. Use it for any method that reads and
    /// then writes, or writes more than one statement.
    pub(crate) async fn begin_write(&self) -> Result<Transaction<'static, Postgres>> {
        let mut tx = self.begin().await?;
        sqlx::query("SELECT pg_advisory_xact_lock(hashtextextended('intent-writer:' || $1, 0))")
            .bind(self.tenant())
            .execute(&mut *tx)
            .await
            .map_err(db)?;
        Ok(tx)
    }

    /// Nothing to release: tenants share the pool, and closing it would end
    /// every other tenant's store. The pool's owner closes it at shutdown.
    #[expect(clippy::unused_async, reason = "same signature as Store::close")]
    pub async fn close(&self) {}
}

impl crate::Store {
    /// Open the store for one tenant on a migrated database, creating the
    /// tenant's seed rows on first use (`intent_provision_tenant`).
    ///
    /// # Errors
    /// `Error::Internal` when provisioning fails.
    pub async fn open_postgres(
        pool: PgPool,
        tenant: TenantId,
        options: PostgresOptions,
    ) -> Result<Self> {
        let store = PgStore {
            pool,
            tenant,
            options,
        };
        let mut tx = store.begin().await?;
        sqlx::query("SELECT intent_provision_tenant($1)")
            .bind(store.tenant())
            .execute(&mut *tx)
            .await
            .map_err(db)?;
        tx.commit().await.map_err(db)?;
        Ok(Self {
            backend: crate::Backend::Postgres(store),
        })
    }
}

pub(crate) fn db(e: impl std::fmt::Display) -> Error {
    Error::Internal(format!("postgres: {e}"))
}

/// The error a not-yet-ported `Store` method returns on Postgres.
pub(crate) fn unported(method: &'static str) -> Error {
    Error::Internal(format!(
        "{method} is not yet supported on the Postgres store"
    ))
}

/// For the few not-yet-ported methods that do not return `Result`.
#[track_caller]
pub(crate) fn unported_panic(method: &'static str) -> ! {
    panic!("{method} is not yet supported on the Postgres store")
}

#[cfg(test)]
mod tests {
    use super::{statements, without_system_root_cert};

    #[test]
    fn migration_files_split_into_statements_outside_function_bodies() {
        let sql = "-- header\nCREATE TABLE a (x int);\n\nCREATE FUNCTION f() RETURNS int\nLANGUAGE plpgsql AS $$\nBEGIN\n    RETURN 1;\nEND\n$$;\n-- trailing comment\n";
        let parts = statements(sql);
        assert_eq!(parts.len(), 2, "{parts:?}");
        assert!(parts[0].ends_with("CREATE TABLE a (x int);"));
        assert!(parts[1].starts_with("CREATE FUNCTION f()") && parts[1].contains("RETURN 1;"));
    }

    #[test]
    fn system_root_cert_is_dropped_and_other_options_kept() {
        assert_eq!(
            without_system_root_cert(
                "postgresql://u:p@h:5432/db?sslmode=verify-full&sslrootcert=system"
            ),
            "postgresql://u:p@h:5432/db?sslmode=verify-full"
        );
        assert_eq!(
            without_system_root_cert("postgresql://h/db?sslrootcert=system"),
            "postgresql://h/db"
        );
        assert_eq!(
            without_system_root_cert("postgresql://h/db?sslrootcert=/etc/ca.pem&sslmode=require"),
            "postgresql://h/db?sslrootcert=/etc/ca.pem&sslmode=require"
        );
        assert_eq!(
            without_system_root_cert("postgresql://h/db"),
            "postgresql://h/db"
        );
    }
}
