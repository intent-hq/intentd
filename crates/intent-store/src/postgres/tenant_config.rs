//! A tenant's settings document in Postgres (`tenant_config`).
//!
//! The desktop daemon keeps its non-secret settings in `config.toml`. An
//! orchestrator serves many tenants and hands a tenant to another process
//! on takeover, so a tenant's settings cannot live in one process's file:
//! [`TenantConfigStore`] keeps the document in the tenant's row instead.
//! Only the orchestrator's Postgres composition builds it.
//!
//! **Revisions.** The row carries a revision that goes up by one on every
//! write. [`TenantConfigStore::compare_and_set`] writes only when the row
//! is still at the revision the caller read (0: no row yet), so of two
//! processes that read the same revision and write, one wins and the other
//! gets [`ConfigWrite::Conflict`] with the revision that beat it. A reader
//! that polls [`TenantConfigStore::load`] and sees a higher revision than it
//! holds knows another process wrote.
//!
//! **Writes** take the tenant's fenced writer path
//! ([`PgStore::begin_write`]): a process that lost the tenant, or whose
//! self-fence fired, cannot write (`Error::NotOwner`).

use std::fmt;

use intent_core::{now_iso, Error, Result};

use super::{db, PgStore};

/// The stored document and its revision (1 or more).
#[derive(Debug, Clone, PartialEq, Eq)]
pub struct StoredConfig {
    pub body: String,
    pub revision: u64,
}

/// What [`TenantConfigStore::compare_and_set`] did.
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub enum ConfigWrite {
    /// The document was written at this new revision.
    Written(u64),
    /// Another write came first: nothing was written, and the row is at
    /// this revision (0: there is still no row).
    Conflict(u64),
}

fn to_db(revision: u64) -> Result<i64> {
    i64::try_from(revision)
        .map_err(|_| Error::Internal(format!("tenant config revision {revision} is out of range")))
}

fn from_db(revision: i64) -> Result<u64> {
    u64::try_from(revision)
        .map_err(|_| Error::Internal(format!("tenant config revision {revision} is negative")))
}

impl PgStore {
    async fn get_tenant_config(&self) -> Result<Option<StoredConfig>> {
        let mut tx = self.begin().await?;
        let row: Option<(String, i64)> =
            sqlx::query_as("SELECT body, revision FROM tenant_config WHERE tenant_id = $1")
                .bind(self.tenant())
                .fetch_optional(&mut *tx)
                .await
                .map_err(db)?;
        tx.commit().await.map_err(db)?;
        row.map(|(body, revision)| {
            Ok(StoredConfig {
                body,
                revision: from_db(revision)?,
            })
        })
        .transpose()
    }

    async fn put_tenant_config(&self, expected: u64, body: &str) -> Result<ConfigWrite> {
        let mut tx = self.begin_write().await?;
        let written = if expected == 0 {
            sqlx::query(
                "INSERT INTO tenant_config (tenant_id, body, revision, updated_at) \
                 VALUES ($1, $2, 1, $3) ON CONFLICT (tenant_id) DO NOTHING",
            )
            .bind(self.tenant())
            .bind(body)
            .bind(now_iso())
            .execute(&mut *tx)
            .await
            .map_err(db)?
            .rows_affected()
        } else {
            sqlx::query(
                "UPDATE tenant_config SET body = $2, revision = revision + 1, updated_at = $3 \
                 WHERE tenant_id = $1 AND revision = $4",
            )
            .bind(self.tenant())
            .bind(body)
            .bind(now_iso())
            .bind(to_db(expected)?)
            .execute(&mut *tx)
            .await
            .map_err(db)?
            .rows_affected()
        };
        let outcome = if written == 1 {
            ConfigWrite::Written(expected + 1)
        } else {
            let current: Option<i64> =
                sqlx::query_scalar("SELECT revision FROM tenant_config WHERE tenant_id = $1")
                    .bind(self.tenant())
                    .fetch_optional(&mut *tx)
                    .await
                    .map_err(db)?;
            ConfigWrite::Conflict(current.map(from_db).transpose()?.unwrap_or(0))
        };
        tx.commit().await.map_err(db)?;
        Ok(outcome)
    }
}

/// One tenant's settings document in its Postgres row (see the module
/// docs). Cheap to clone. The `*_blocking` methods serve the synchronous
/// settings registry of `intent-services`.
#[derive(Clone)]
pub struct TenantConfigStore {
    store: PgStore,
    runtime: tokio::runtime::Handle,
}

impl fmt::Debug for TenantConfigStore {
    fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
        f.debug_struct("TenantConfigStore")
            .field("tenant", &self.store.tenant())
            .finish_non_exhaustive()
    }
}

impl TenantConfigStore {
    /// The settings document of `store`'s tenant. Call it inside the
    /// multi-thread Tokio runtime the blocking methods will use; that
    /// runtime must outlive the store.
    ///
    /// # Errors
    /// `Error::Internal` when `store` is not a Postgres store, or no
    /// multi-thread Tokio runtime is running.
    pub fn new(store: &crate::Store) -> Result<Self> {
        let crate::Backend::Postgres(pg) = &store.backend else {
            return Err(Error::Internal(
                "tenant settings need a Postgres store; the SQLite daemon uses config.toml".into(),
            ));
        };
        let runtime = tokio::runtime::Handle::try_current()
            .map_err(|e| Error::Internal(format!("tenant settings need a Tokio runtime: {e}")))?;
        if runtime.runtime_flavor() != tokio::runtime::RuntimeFlavor::MultiThread {
            return Err(Error::Internal(
                "tenant settings need the multi-thread Tokio runtime".into(),
            ));
        }
        Ok(Self {
            store: (**pg).clone(),
            runtime,
        })
    }

    /// The tenant this document belongs to.
    #[must_use]
    pub fn tenant(&self) -> &str {
        self.store.tenant()
    }

    /// The stored document; `None` before the tenant's first write.
    ///
    /// # Errors
    /// `Error::Internal` when the database fails.
    pub async fn load(&self) -> Result<Option<StoredConfig>> {
        self.store.get_tenant_config().await
    }

    /// Write `body` if the row is still at `expected` (0: no row yet).
    ///
    /// # Errors
    /// `Error::NotOwner` when this process lost the tenant; `Error::StoreBusy`
    /// past the writer wait budget; `Error::Internal` when the database fails.
    pub async fn compare_and_set(&self, expected: u64, body: &str) -> Result<ConfigWrite> {
        self.store.put_tenant_config(expected, body).await
    }

    /// [`Self::load`] for synchronous callers.
    ///
    /// # Errors
    /// As [`Self::load`], and see [`Self::block`].
    pub fn load_blocking(&self) -> Result<Option<StoredConfig>> {
        self.block(self.load())
    }

    /// [`Self::compare_and_set`] for synchronous callers.
    ///
    /// # Errors
    /// As [`Self::compare_and_set`], and see [`Self::block`].
    pub fn compare_and_set_blocking(&self, expected: u64, body: &str) -> Result<ConfigWrite> {
        self.block(self.compare_and_set(expected, body))
    }

    /// Runs `op` to completion from synchronous code, as
    /// `TenantSecretStore` does: in place on a multi-thread runtime worker,
    /// on the store's runtime outside any runtime, and an error (not a
    /// deadlock) on a current-thread runtime.
    fn block<T>(&self, op: impl std::future::Future<Output = Result<T>>) -> Result<T> {
        use tokio::runtime::{Handle, RuntimeFlavor};
        match Handle::try_current() {
            Ok(handle) if handle.runtime_flavor() == RuntimeFlavor::MultiThread => {
                tokio::task::block_in_place(|| handle.block_on(op))
            }
            Ok(_) => Err(Error::Internal(
                "tenant settings cannot block on a current-thread Tokio runtime; \
                 run the orchestrator on the multi-thread runtime"
                    .into(),
            )),
            Err(_) => self.runtime.block_on(op),
        }
    }
}
