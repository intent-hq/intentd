//! intent-store — SQLite persistence (§9.2, §9.4).
//!
//! Depends on `intent-core` only (§3.2). Opens a WAL-mode SQLite pool with the
//! required PRAGMAs, runs the embedded migrations, and exposes minimal
//! repository methods for the vertical slice (insert/list workspace and note).

use std::path::Path;
use std::time::Duration;

use sqlx::sqlite::{SqliteConnectOptions, SqliteJournalMode, SqlitePoolOptions};
use sqlx::SqlitePool;

pub use intent_core::{Error, Result};

mod note_repo;
mod workspace_repo;

#[cfg(test)]
mod tests;

/// Embedded schema migrations (§9.4).
static MIGRATOR: sqlx::migrate::Migrator = sqlx::migrate!();

/// SQLite-backed persistence handle. Cheaply cloneable (the pool is `Arc`-ed).
#[derive(Clone)]
pub struct Store {
    pool: SqlitePool,
}

impl Store {
    /// Open (creating if needed) the database at `db_path` and run migrations.
    pub async fn open(db_path: &Path) -> Result<Self> {
        let pool = connect(db_path).await?;
        MIGRATOR
            .run(&pool)
            .await
            .map_err(|e| Error::Internal(format!("migrations failed: {e}")))?;
        Ok(Self { pool })
    }

    /// Wrap an already-configured pool (e.g. for tests or shared composition).
    pub fn from_pool(pool: SqlitePool) -> Self {
        Self { pool }
    }

    /// Borrow the underlying connection pool.
    pub fn pool(&self) -> &SqlitePool {
        &self.pool
    }
}

/// Open a WAL-mode SQLite pool with the required PRAGMAs (§9.4): `journal_mode
/// = WAL`, `foreign_keys = ON`, `busy_timeout = 5000`.
pub async fn connect(db_path: &Path) -> Result<SqlitePool> {
    let opts = SqliteConnectOptions::new()
        .filename(db_path)
        .create_if_missing(true)
        .journal_mode(SqliteJournalMode::Wal)
        .foreign_keys(true)
        .busy_timeout(Duration::from_millis(5000));

    SqlitePoolOptions::new()
        .connect_with(opts)
        .await
        .map_err(|e| Error::Internal(format!("failed to open database: {e}")))
}

/// Encode tags as a JSON-array TEXT column.
pub(crate) fn tags_to_db(tags: &[String]) -> Result<String> {
    serde_json::to_string(tags).map_err(|e| Error::Internal(format!("encode tags failed: {e}")))
}

/// Decode tags from a JSON-array TEXT column.
pub(crate) fn tags_from_db(s: &str) -> Result<Vec<String>> {
    serde_json::from_str(s).map_err(|e| Error::Internal(format!("decode tags failed: {e}")))
}

/// Encode an enum to its lowercase/snake_case string DB form via serde.
pub(crate) fn enum_to_db<T: serde::Serialize>(v: &T) -> Result<String> {
    serde_json::to_value(v)
        .ok()
        .and_then(|x| x.as_str().map(|s| s.to_string()))
        .ok_or_else(|| Error::Internal("failed to encode enum".to_string()))
}

/// Decode an enum from its string DB form via serde.
pub(crate) fn enum_from_db<T: serde::de::DeserializeOwned>(s: &str) -> Result<T> {
    serde_json::from_value(serde_json::Value::String(s.to_string()))
        .map_err(|e| Error::Internal(format!("failed to decode enum '{s}': {e}")))
}
