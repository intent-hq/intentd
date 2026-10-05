//! Read-only diagnostic access, independent of Store initialization and shutdown.

use std::path::Path;
use std::time::Duration;

use sqlx::sqlite::{SqliteConnectOptions, SqlitePoolOptions};
use sqlx::SqlitePool;

use crate::{Error, MigrationStatus, Result, MIGRATOR};

/// An existing database opened read-only, without migrations or maintenance.
/// Does not expose Store mutation or checkpoint methods.
pub struct DiagnosticStore {
    pool: SqlitePool,
}

impl DiagnosticStore {
    /// Open an existing DB without creating it, changing journal mode or schema.
    ///
    /// # Errors
    /// Returns an error when the database does not exist or cannot be read.
    pub async fn open(db_path: &Path) -> Result<Self> {
        let options = SqliteConnectOptions::new()
            .filename(db_path)
            .create_if_missing(false)
            .read_only(true)
            .pragma("query_only", "ON")
            .busy_timeout(Duration::from_secs(5));
        let pool = SqlitePoolOptions::new()
            .max_connections(1)
            .connect_with(options)
            .await
            .map_err(|e| Error::Internal(format!("read-only database open failed: {e}")))?;
        Ok(Self { pool })
    }

    /// `SQLite` enforces read-only access even for queries supplied by callers.
    #[must_use]
    pub fn read_pool(&self) -> &SqlitePool {
        &self.pool
    }

    /// Read the existing migration ledger without creating or repairing it.
    ///
    /// # Errors
    /// Returns an error for a missing or unreadable migration ledger.
    pub async fn migration_status(&self) -> Result<MigrationStatus> {
        let applied = sqlx::query_scalar("SELECT version FROM _sqlx_migrations ORDER BY version")
            .fetch_all(&self.pool)
            .await
            .map_err(|e| Error::Internal(format!("query migrations failed: {e}")))?;
        Ok(MigrationStatus {
            expected: MIGRATOR.iter().map(|m| m.version).collect(),
            applied,
        })
    }

    /// Close the read-only pool without checkpointing.
    pub async fn close(&self) {
        self.pool.close().await;
    }
}
