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
use sqlx::postgres::{PgPoolOptions, Postgres};
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

/// Connect a pool. The caller runs [`migrate`] once per database.
///
/// # Errors
/// `Error::Internal` when the connection fails.
pub async fn connect(url: &str, max_connections: u32) -> Result<PgPool> {
    PgPoolOptions::new()
        .max_connections(max_connections)
        .connect(url)
        .await
        .map_err(|e| Error::Internal(format!("postgres connect failed: {e}")))
}

/// Apply the Postgres schema migrations.
///
/// # Errors
/// `Error::Internal` when a migration fails.
pub async fn migrate(pool: &PgPool) -> Result<()> {
    MIGRATOR
        .run(pool)
        .await
        .map_err(|e| Error::Internal(format!("postgres migration failed: {e}")))
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
