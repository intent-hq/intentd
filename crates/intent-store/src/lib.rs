//! intent-store — SQLite persistence (§9.2, §9.4).
//!
//! Depends on `intent-core` only (§3.2). Opens a WAL-mode SQLite pool with the
//! required PRAGMAs, runs the embedded migrations, and exposes minimal
//! repository methods for the vertical slice (insert/list workspace and note).

use std::path::Path;
use std::time::Duration;

use sqlx::sqlite::{SqliteConnectOptions, SqliteJournalMode, SqlitePoolOptions};
use sqlx::{Row, SqlitePool};

pub use intent_core::{Error, Result};

mod agent_repo;
mod client_repo;
mod comment_repo;
mod diffs_repo;
mod draft_repo;
mod event_repo;
mod idempotency_repo;
mod known_repo_repo;
mod mcp_oauth_repo;
mod memories_repo;
mod metrics_repo;
mod note_line_attribution_repo;
mod note_repo;
mod note_version_repo;
mod sandbox_repo;
mod script_repo;
mod settings_repo;
mod task_agent_link_repo;
mod tracked_changes_repo;
mod workspace_context_repo;
mod workspace_repo;
mod workspace_ui_context_repo;

pub use agent_repo::ReplaceMessage;
pub use diffs_repo::{DiffRow, NewDiff};
pub use event_repo::{EventQuery, NewEvent};
pub use metrics_repo::{AgentMetricsRow, WorkspaceMetricsRow};
pub use note_version_repo::MAX_NOTE_VERSIONS;
pub use sandbox_repo::{Sandbox, SandboxStatus};
pub use tracked_changes_repo::{NewTrackedChange, TrackedChangeRow};

/// Retry helper for write transactions that may hit SQLITE_BUSY during lock upgrade
/// (STAB-7). Executes the given async transaction closure up to MAX_ATTEMPTS times,
/// with jittered exponential backoff between attempts. Returns the result on success
/// or the last error after exhausting retries.
///
/// Use this for any write transaction that uses .begin() (DEFERRED mode) to eliminate
/// the intermittent "database is locked" (code 5) failures that occur when multiple
/// transactions try to upgrade from shared to exclusive lock simultaneously.
async fn with_write_txn_retry<F, Fut, T>(f: F) -> Result<T>
where
    F: Fn() -> Fut,
    Fut: std::future::Future<Output = Result<T>>,
{
    const MAX_ATTEMPTS: u32 = 10;
    const BASE_DELAY_MS: u64 = 50;

    let mut last_error = None;

    for attempt in 0..MAX_ATTEMPTS {
        match f().await {
            Ok(result) => return Ok(result),
            Err(e) => {
                // Only retry on SQLITE_BUSY (code 5: database is locked)
                let should_retry = matches!(&e, Error::Internal(msg) if msg.contains("code: 5"));

                if !should_retry || attempt == MAX_ATTEMPTS - 1 {
                    return Err(e);
                }

                last_error = Some(e);

                // Exponential backoff with jitter: 50ms, 100ms, 200ms, 400ms, ...
                let delay_ms = BASE_DELAY_MS * (1 << attempt);
                let jitter_ms = (delay_ms / 4) as i64; // ±25% jitter
                let jittered_delay =
                    delay_ms as i64 + (rand::random::<i64>() % (2 * jitter_ms + 1)) - jitter_ms;
                tokio::time::sleep(Duration::from_millis(jittered_delay.max(0) as u64)).await;
            }
        }
    }

    Err(last_error.unwrap_or_else(|| Error::Internal("retry exhausted".to_string())))
}

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

    /// Compare the migrations embedded in the binary against the versions
    /// recorded as applied in `_sqlx_migrations`, for `intentd doctor` (§5.7).
    pub async fn migration_status(&self) -> Result<MigrationStatus> {
        let expected: Vec<i64> = MIGRATOR.iter().map(|m| m.version).collect();
        let applied: Vec<i64> =
            sqlx::query("SELECT version FROM _sqlx_migrations ORDER BY version")
                .fetch_all(&self.pool)
                .await
                .map_err(|e| Error::Internal(format!("query migrations failed: {e}")))?
                .iter()
                .map(|row| row.get::<i64, _>("version"))
                .collect();
        Ok(MigrationStatus { expected, applied })
    }
}

/// Migration diagnostics: the versions embedded in the binary (`expected`) and
/// the versions recorded as applied in `_sqlx_migrations` (`applied`).
#[derive(Debug, Clone, PartialEq, Eq)]
pub struct MigrationStatus {
    pub expected: Vec<i64>,
    pub applied: Vec<i64>,
}

impl MigrationStatus {
    /// True when every embedded migration version has been applied.
    pub fn is_current(&self) -> bool {
        self.expected.iter().all(|v| self.applied.contains(v))
    }
}

/// Open a WAL-mode SQLite pool with the required PRAGMAs (§9.4): `journal_mode
/// = WAL`, `foreign_keys = ON`, `busy_timeout = 5000`.
///
/// Pool sizing: SQLite WAL mode supports many concurrent readers but only one
/// writer (serialized inside SQLite). We set `max_connections=20` so the
/// client-driven startup burst (FE rehydrating several workspaces at once) no
/// longer saturates the pool and trips sqlx's `slow_acquire_threshold`
/// warnings; `acquire_timeout=10s` ensures pool exhaustion surfaces a clear
/// error instead of silently queueing for 30s, which would exceed the sidecar
/// health probe's 3s timeout and risk a false-positive daemon kill (STAB-6).
pub async fn connect(db_path: &Path) -> Result<SqlitePool> {
    let opts = SqliteConnectOptions::new()
        .filename(db_path)
        .create_if_missing(true)
        .journal_mode(SqliteJournalMode::Wal)
        .foreign_keys(true)
        .busy_timeout(Duration::from_millis(5000));

    SqlitePoolOptions::new()
        .max_connections(20)
        .acquire_timeout(Duration::from_secs(10))
        .connect_with(opts)
        .await
        .map_err(|e| {
            // Match on the structured error variant for precision.
            // NOTE: This mapping applies only to initial pool creation. Runtime pool
            // exhaustion (pool.begin()/queries) will surface as the default sqlx error
            // message unless mapped at the point of acquisition. Consider adding a
            // shared error-mapping helper if runtime exhaustion diagnostics are needed.
            match e {
                sqlx::Error::PoolTimedOut => Error::Internal(
                    "database pool exhausted (acquire timeout exceeded)".to_string(),
                ),
                _ => Error::Internal(format!("failed to open database: {e}")),
            }
        })
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
