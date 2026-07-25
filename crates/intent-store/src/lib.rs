//! intent-store — SQLite persistence (§9.2, §9.4).
//!
//! Depends on `intent-core` only (§3.2). Opens a WAL-mode SQLite pool with the
//! required PRAGMAs, runs the embedded migrations, and exposes minimal
//! repository methods for the vertical slice (insert/list workspace and note).

use std::path::Path;
use std::time::Duration;

use sqlx::sqlite::{SqliteAutoVacuum, SqliteConnectOptions, SqliteJournalMode, SqlitePoolOptions};
use sqlx::{Row, SqlitePool};

pub use intent_core::{Error, Result};

mod agent_queue_repo;
mod agent_repo;
mod client_repo;
mod comment_repo;
mod completion_watch_repo;
mod delegation_group_repo;
mod diffs_repo;
mod draft_repo;
mod event_repo;
mod idempotency_repo;
mod known_repo_repo;
mod mcp_oauth_repo;
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

pub use agent_queue_repo::AgentQueueRow;
pub use agent_repo::{InterruptedAgent, ReplaceMessage};
pub use completion_watch_repo::PersistedCompletionWatch;
pub use delegation_group_repo::PersistedDelegationGroup;
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

/// COMMIT the transaction open on `conn`, guarding against a failed COMMIT
/// (monorepo#638 / #657 / #670): a failed COMMIT can leave the transaction
/// open on the pooled connection, so roll back explicitly so the connection
/// is not returned to the pool still holding the write lock. If the ROLLBACK
/// fails too, detach the connection from the pool and close it so the
/// poisoned handle is never reused (the pool opens a fresh replacement on
/// demand). On failure returns `Error::Internal` with the message
/// `"{context}: {commit error}"`.
///
/// Takes the connection by value: COMMIT must be the last statement of the
/// raw `BEGIN IMMEDIATE` transaction, and the detach path consumes it.
pub(crate) async fn commit_with_rollback_guard(
    mut conn: sqlx::pool::PoolConnection<sqlx::Sqlite>,
    context: &str,
) -> Result<()> {
    use sqlx::Connection;
    if let Err(e) = sqlx::query("COMMIT").execute(&mut *conn).await {
        if sqlx::query("ROLLBACK").execute(&mut *conn).await.is_err() {
            let _ = conn.detach().close().await;
        }
        return Err(Error::Internal(format!("{context}: {e}")));
    }
    Ok(())
}

#[cfg(test)]
mod tests;

/// Embedded schema migrations (§9.4).
static MIGRATOR: sqlx::migrate::Migrator = sqlx::migrate!();

/// SQLite-backed persistence handle. Cheaply cloneable (the pools are `Arc`-ed).
///
/// Holds two pools over the same DB file: a single-connection **write pool**
/// (max_connections=1) to serialize all mutations and eliminate in-process
/// writer-vs-writer busy_timeout contention, and a **read pool** (32 connections)
/// intended for concurrent read (SELECT) queries — a convention, not an enforced
/// constraint. See `connect_write` / `connect_read` for the pool configurations.
#[derive(Clone)]
pub struct Store {
    write_pool: SqlitePool,
    read_pool: SqlitePool,
}

impl Store {
    /// Open (creating if needed) the database at `db_path` and run migrations.
    /// Builds two pools: a single-connection write pool and a 32-connection read pool.
    pub async fn open(db_path: &Path) -> Result<Self> {
        let write_pool = connect_write(db_path).await?;
        let read_pool = connect_read(db_path).await?;
        // Run migrations on the write pool (migrations are write operations).
        MIGRATOR
            .run(&write_pool)
            .await
            .map_err(|e| Error::Internal(format!("migrations failed: {e}")))?;
        Ok(Self {
            write_pool,
            read_pool,
        })
    }

    /// Wrap an already-configured pool (e.g. for tests or shared composition).
    /// The single pool is used for both reads and writes in test scenarios.
    pub fn from_pool(pool: SqlitePool) -> Self {
        Self {
            write_pool: pool.clone(),
            read_pool: pool,
        }
    }

    /// Borrow the write pool (single connection, for INSERT/UPDATE/DELETE/BEGIN).
    pub fn write_pool(&self) -> &SqlitePool {
        &self.write_pool
    }

    /// Borrow the read pool (32 connections, intended for read/SELECT queries).
    pub fn read_pool(&self) -> &SqlitePool {
        &self.read_pool
    }

    /// Deprecated alias for `read_pool()`. External callers should migrate to
    /// explicit `read_pool()` / `write_pool()` usage.
    #[deprecated(since = "0.1.0", note = "use read_pool() or write_pool() explicitly")]
    pub fn pool(&self) -> &SqlitePool {
        &self.read_pool
    }

    /// Spawn a background task that periodically runs PRAGMA wal_checkpoint(PASSIVE)
    /// to prevent unbounded WAL growth when continuous readers hold long-lived
    /// transactions. Returns a handle that can be aborted to stop the task.
    ///
    /// Call this after `open()` in the daemon composition root; the task will run
    /// every ~60s until the returned handle is dropped or aborted.
    pub fn spawn_periodic_wal_checkpoint(&self) -> tokio::task::JoinHandle<()> {
        let write_pool = self.write_pool.clone();
        tokio::spawn(async move {
            let mut interval = tokio::time::interval(std::time::Duration::from_secs(60));
            interval.set_missed_tick_behavior(tokio::time::MissedTickBehavior::Skip);
            loop {
                interval.tick().await;
                // Best-effort PASSIVE checkpoint (does not block writers/readers).
                let _ = sqlx::query("PRAGMA wal_checkpoint(PASSIVE)")
                    .execute(&write_pool)
                    .await;
            }
        })
    }

    /// Number of pages on the database freelist (`PRAGMA freelist_count`):
    /// pages emptied by deletes that have not yet been reclaimed. On an
    /// incremental-auto-vacuum database these are the pages
    /// [`Self::incremental_vacuum`] can release back to the filesystem.
    pub async fn freelist_count(&self) -> Result<i64> {
        sqlx::query("PRAGMA freelist_count")
            .fetch_one(&self.write_pool)
            .await
            .map(|row| row.get::<i64, _>(0))
            .map_err(|e| Error::Internal(format!("freelist_count failed: {e}")))
    }

    /// Release up to `max_pages` freelist pages back to the filesystem via
    /// `PRAGMA incremental_vacuum(N)`. Only effective when the database has
    /// `auto_vacuum = INCREMENTAL` (a no-op otherwise — see [`connect_write`]
    /// for the activation story). Bounded `N` keeps each call short so the
    /// single-connection write pool is never held for long; call it
    /// periodically (e.g. after retention sweeps) to reclaim space
    /// incrementally. Returns the number of pages actually freed.
    pub async fn incremental_vacuum(&self, max_pages: u32) -> Result<u64> {
        let before = self.freelist_count().await?;
        sqlx::query(&format!("PRAGMA incremental_vacuum({max_pages})"))
            .execute(&self.write_pool)
            .await
            .map_err(|e| Error::Internal(format!("incremental_vacuum failed: {e}")))?;
        let after = self.freelist_count().await?;
        Ok((before - after).max(0) as u64)
    }

    /// Run `PRAGMA optimize` on the write connection. Cheap when called
    /// repeatedly (SQLite only re-analyzes tables whose content changed
    /// enough to matter); intended to run periodically after retention
    /// sweeps so the query planner statistics track the shrinking event
    /// table. See <https://sqlite.org/pragma.html#pragma_optimize>.
    pub async fn optimize(&self) -> Result<()> {
        sqlx::query("PRAGMA optimize")
            .execute(&self.write_pool)
            .await
            .map(|_| ())
            .map_err(|e| Error::Internal(format!("PRAGMA optimize failed: {e}")))
    }

    /// Close both pools gracefully, checkpointing the WAL and freeing resources.
    /// Call this during daemon shutdown to checkpoint the WAL and close the pools.
    /// This ensures WAL changes are visible to subsequent daemon instances
    /// (regression: persisted settings must survive app relaunches in sidecar mode).
    pub async fn close(&self) {
        // Best-effort WAL checkpoint before closing the pools (via write pool).
        let _ = sqlx::query("PRAGMA wal_checkpoint(TRUNCATE)")
            .execute(&self.write_pool)
            .await;
        self.write_pool.close().await;
        self.read_pool.close().await;
    }

    /// Compare the migrations embedded in the binary against the versions
    /// recorded as applied in `_sqlx_migrations`, for `intentd doctor` (§5.7).
    pub async fn migration_status(&self) -> Result<MigrationStatus> {
        let expected: Vec<i64> = MIGRATOR.iter().map(|m| m.version).collect();
        let applied: Vec<i64> =
            sqlx::query("SELECT version FROM _sqlx_migrations ORDER BY version")
                .fetch_all(&self.read_pool)
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

/// Open a WAL-mode SQLite **write pool** with `max_connections=1` (§9.4).
/// The single-connection write pool serializes all mutations (INSERT/UPDATE/DELETE)
/// and eliminates in-process writer-vs-writer busy_timeout contention.
///
/// **WAL + synchronous=NORMAL pairing** (finding F4, fsync half): WAL mode
/// ensures crash safety with only periodic WAL checkpoints needing full fsyncs;
/// `synchronous = NORMAL` (vs the default `FULL`) skips the extra fsync on every
/// transaction commit, relying on the WAL's crash-recovery guarantees instead.
/// This cuts fsync load dramatically on high-write workloads (ephemeral events)
/// while preserving durability — a crash may lose the last uncommitted
/// transaction but never corrupts the database. See
/// <https://sqlite.org/pragma.html#pragma_synchronous> and
/// <https://sqlite.org/wal.html#performance_considerations>.
///
/// `busy_timeout=5000` is kept to protect against cross-process access (e.g.,
/// manual sqlite3 CLI probes during development), though in-process contention
/// is eliminated by the single-writer design.
///
/// **auto_vacuum = INCREMENTAL**: without auto_vacuum, pages emptied by
/// deletes (retention sweeps) accumulate on the freelist forever and the file
/// only ever grows. sqlx applies the `auto_vacuum` pragma before
/// `journal_mode`, so a **new** database is created in incremental mode and
/// [`Store::incremental_vacuum`] can release freelist pages in bounded slices.
/// On an **existing** database created without auto_vacuum the pragma is
/// recorded but inert until a one-time `VACUUM` rebuilds the file — that is
/// deliberately NOT done automatically (a full VACUUM blocks all writes for
/// the duration, unacceptable on a live daemon). One-time activation, with
/// the daemon **stopped**:
///
/// ```sh
/// sqlite3 ~/.intentd/intentd.db "PRAGMA auto_vacuum=INCREMENTAL; VACUUM;"
/// ```
///
/// `intentd doctor` reports the current auto_vacuum mode and freelist size,
/// and prints this activation step when the database is still in NONE mode.
pub async fn connect_write(db_path: &Path) -> Result<SqlitePool> {
    let opts = SqliteConnectOptions::new()
        .filename(db_path)
        .create_if_missing(true)
        .auto_vacuum(SqliteAutoVacuum::Incremental)
        .journal_mode(SqliteJournalMode::Wal)
        .foreign_keys(true)
        .busy_timeout(Duration::from_millis(5000))
        .synchronous(sqlx::sqlite::SqliteSynchronous::Normal);

    SqlitePoolOptions::new()
        .max_connections(1)
        .acquire_timeout(Duration::from_secs(10))
        .connect_with(opts)
        .await
        .map_err(|e| match e {
            sqlx::Error::PoolTimedOut => {
                Error::Internal("write pool exhausted (acquire timeout exceeded)".to_string())
            }
            _ => Error::Internal(format!("failed to open write pool: {e}")),
        })
}

/// Open a WAL-mode SQLite **read pool** with `max_connections=32` (§9.4).
/// The read pool is intended for read (SELECT) queries — by convention, not an
/// enforced read-only configuration — and supports concurrent readers without
/// contention (SQLite WAL mode allows many simultaneous readers).
///
/// The PRAGMAs match the write pool: `journal_mode = WAL`, `foreign_keys = ON`,
/// `busy_timeout = 5000`, `synchronous = NORMAL`. The read pool size (32) is
/// sized to absorb the client-driven startup burst (FE rehydrating several
/// workspaces at once) without saturating the pool and tripping sqlx's
/// `slow_acquire_threshold` warnings (STAB-6, STAB-46). The original size of
/// 16 was tuned against the fixed agent process cap of 30; the RAM-based cap
/// raise to 56 (intent-hq/intentd#296) roughly doubles the potential
/// concurrent-agent read load, so the pool doubles to 32 to preserve the
/// pool/agent-cap headroom ratio.
pub async fn connect_read(db_path: &Path) -> Result<SqlitePool> {
    let opts = SqliteConnectOptions::new()
        .filename(db_path)
        .create_if_missing(true)
        .auto_vacuum(SqliteAutoVacuum::Incremental)
        .journal_mode(SqliteJournalMode::Wal)
        .foreign_keys(true)
        .busy_timeout(Duration::from_millis(5000))
        .synchronous(sqlx::sqlite::SqliteSynchronous::Normal);

    SqlitePoolOptions::new()
        .max_connections(32)
        .acquire_timeout(Duration::from_secs(10))
        .connect_with(opts)
        .await
        .map_err(|e| match e {
            sqlx::Error::PoolTimedOut => {
                Error::Internal("read pool exhausted (acquire timeout exceeded)".to_string())
            }
            _ => Error::Internal(format!("failed to open read pool: {e}")),
        })
}

/// Legacy test helper: builds a single pool (max_connections=20) for tests
/// that predate the write/read pool split. New tests should use `Store::open`.
#[cfg(test)]
pub async fn connect(db_path: &Path) -> Result<SqlitePool> {
    let opts = SqliteConnectOptions::new()
        .filename(db_path)
        .create_if_missing(true)
        .auto_vacuum(SqliteAutoVacuum::Incremental)
        .journal_mode(SqliteJournalMode::Wal)
        .foreign_keys(true)
        .busy_timeout(Duration::from_millis(5000))
        .synchronous(sqlx::sqlite::SqliteSynchronous::Normal);

    SqlitePoolOptions::new()
        .max_connections(20)
        .acquire_timeout(Duration::from_secs(10))
        .connect_with(opts)
        .await
        .map_err(|e| match e {
            sqlx::Error::PoolTimedOut => {
                Error::Internal("database pool exhausted (acquire timeout exceeded)".to_string())
            }
            _ => Error::Internal(format!("failed to open database: {e}")),
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
