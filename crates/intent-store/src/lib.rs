//! intent-store — `SQLite` persistence (§9.2, §9.4).
//!
//! Depends on `intent-core` only (§3.2). Opens a WAL-mode `SQLite` pool with the
//! required PRAGMAs, runs the embedded migrations, and exposes minimal
//! repository methods for the vertical slice (insert/list workspace and note).

use std::path::Path;
use std::time::Duration;

use sqlx::sqlite::{SqliteAutoVacuum, SqliteConnectOptions, SqliteJournalMode, SqlitePoolOptions};
use sqlx::{Row, SqlitePool};

pub use intent_core::{Error, Result};

mod agent_flipped_completion_repo;
mod agent_queue_repo;
mod agent_repo;
mod attachment_repo;
mod client_repo;
mod comment_repo;
mod completion_wake_delivery_repo;
mod completion_watch_repo;
mod delegation_group_repo;
mod diffs_repo;
mod draft_repo;
mod event_repo;
mod event_subscription_repo;
mod hook_repo;
mod idempotency_repo;
mod known_repo_repo;
mod mcp_oauth_repo;
mod message_thumbnails;
mod metrics_repo;
mod note_line_attribution_repo;
mod note_repo;
mod note_version_repo;
mod pr_monitor_repo;
mod sandbox_repo;
mod script_repo;
mod settings_repo;
mod stop_redelivery_repo;
mod task_agent_link_repo;
mod tracked_changes_repo;
mod transfer_repo;
mod usage_rate_repo;
mod usage_stats_repo;
mod workspace_context_repo;
mod workspace_git_root_repo;
mod workspace_repo;
mod workspace_ui_context_repo;

pub use agent_flipped_completion_repo::AGENT_FLIPPED_COMPLETIONS_CAP;
pub use agent_queue_repo::AgentQueueRow;
pub(crate) use agent_repo::AgentUsageRow;
pub use agent_repo::{
    ChildAgentCounts, MessageFtsMatch, ReplaceMessage, SessionMessageProjection,
    UserMessageIndexItem, PROJECTION_TEXT_BLOCK_CAP,
};
pub use attachment_repo::AttachmentRecord;
pub use completion_watch_repo::PersistedCompletionWatch;
pub use delegation_group_repo::PersistedDelegationGroup;
pub use diffs_repo::NewDiff;
pub use event_repo::{EventQuery, NewEvent};
pub use event_subscription_repo::PersistedEventSubscription;
pub use metrics_repo::{AgentMetricsRow, WorkspaceMetricsRow};
#[cfg(test)]
pub(crate) use note_version_repo::MAX_NOTE_VERSIONS;
pub use pr_monitor_repo::PrMonitorPollUpdate;
pub use sandbox_repo::{Sandbox, SandboxStatus};
pub use tracked_changes_repo::{NewTrackedChange, TrackedChangeRow};
pub use transfer_repo::TRANSFER_TABLES;
pub use usage_rate_repo::{UsageRateDelta, UsageRateRow};
pub use usage_stats_repo::{LocalStamp, UsageStatsDelta, UsageStatsRow};

/// Total retry window for the `SQLITE_BUSY` retry helpers (monorepo#1139).
const BUSY_RETRY_DEADLINE: Duration = Duration::from_secs(30);

/// True when an error message carries a SQLITE_BUSY-family result code.
/// sqlx formats `SQLite` errors as `(code: {extended_code}) …` where the code
/// is always the extended result code, so match the busy family explicitly:
/// 5 (`SQLITE_BUSY`), 261 (`SQLITE_BUSY_RECOVERY`), 517 (`SQLITE_BUSY_SNAPSHOT`),
/// 773 (`SQLITE_BUSY_TIMEOUT`). A bare `code: 5` substring would false-positive
/// on unrelated 5xx codes (e.g. 516 `SQLITE_ABORT_ROLLBACK`) and miss the
/// extended busy variants.
fn is_busy_message(msg: &str) -> bool {
    ["(code: 5)", "(code: 261)", "(code: 517)", "(code: 773)"]
        .iter()
        .any(|code| msg.contains(code))
}

/// Shared `SQLITE_BUSY` retry loop backing [`with_write_txn_retry`] and
/// [`with_read_retry`]. Executes the given async closure, retrying only when
/// the error is a transient `SQLITE_BUSY` (`Error::Internal` whose message
/// carries a busy-family result code, see [`is_busy_message`]). Backoff is
/// jittered exponential: ~50ms base doubling per attempt (±25% jitter), with
/// each sleep capped at 5s and clamped to the remaining `deadline` so the
/// loop degrades to steady polling until `deadline` is exhausted, at which
/// point the last error is returned. Non-busy errors are returned
/// immediately.
async fn with_busy_retry<F, Fut, T>(f: F, deadline: Duration) -> Result<T>
where
    F: Fn() -> Fut,
    Fut: std::future::Future<Output = Result<T>>,
{
    const BASE_DELAY_MS: u64 = 50;
    const MAX_DELAY_MS: u64 = 5_000;

    let start = std::time::Instant::now();
    let mut attempt: u32 = 0;
    loop {
        match f().await {
            Ok(result) => return Ok(result),
            Err(e) => {
                // Only retry on SQLITE_BUSY-family errors (database is locked)
                let busy = matches!(&e, Error::Internal(msg) if is_busy_message(msg));
                if !busy || start.elapsed() >= deadline {
                    return Err(e);
                }

                // Exponential backoff with symmetric ±25% jitter: 50ms,
                // 100ms, 200ms, ..., capped at MAX_DELAY_MS per sleep and
                // clamped to the remaining deadline.
                let delay_ms = (BASE_DELAY_MS << attempt.min(16)).min(MAX_DELAY_MS);
                let jitter_ms = delay_ms / 4; // ±25% jitter
                let jittered_delay = (delay_ms - jitter_ms
                    + rand::random::<u64>() % (2 * jitter_ms + 1))
                    .min(MAX_DELAY_MS);
                let remaining = deadline.saturating_sub(start.elapsed());
                let sleep = Duration::from_millis(jittered_delay).min(remaining);
                tokio::time::sleep(sleep).await;
                attempt += 1;
            }
        }
    }
}

/// Retry helper for write transactions that may hit `SQLITE_BUSY` during lock upgrade
/// (STAB-7). Executes the given async transaction closure via the shared
/// [`with_busy_retry`] loop (~30s total window, monorepo#1139). Returns the
/// result on success or the last error after the retry window is exhausted.
///
/// Use this for any write transaction that uses .`begin()` (DEFERRED mode) to eliminate
/// the intermittent "database is locked" (code 5) failures that occur when multiple
/// transactions try to upgrade from shared to exclusive lock simultaneously.
async fn with_write_txn_retry<F, Fut, T>(f: F) -> Result<T>
where
    F: Fn() -> Fut,
    Fut: std::future::Future<Output = Result<T>>,
{
    with_busy_retry(f, BUSY_RETRY_DEADLINE).await
}

/// Retry helper for single-shot idempotent reads that may hit transient
/// `SQLITE_BUSY` under heavy write load (monorepo#1139: "get note failed:
/// ... (code: 5) database is locked" surfaced to a production client).
/// Same shared [`with_busy_retry`] loop as [`with_write_txn_retry`]:
/// retries only `code: 5` errors, jittered exponential backoff, ~30s total
/// window, last error surfaced.
async fn with_read_retry<F, Fut, T>(f: F) -> Result<T>
where
    F: Fn() -> Fut,
    Fut: std::future::Future<Output = Result<T>>,
{
    with_busy_retry(f, BUSY_RETRY_DEADLINE).await
}

/// Roll back the transaction open on `conn` after a failed statement or
/// COMMIT (monorepo#680). If the ROLLBACK itself fails, detach the
/// connection from the pool and close it so a potentially poisoned handle
/// (open transaction + write lock on the sole write-pool connection,
/// `max_connections=1`) is never reused — the pool opens a fresh
/// replacement on demand.
///
/// Takes the connection by value: the transaction is over either way, and
/// the detach path consumes it. Emits a `tracing::warn!` when the
/// detach+close path fires so the poisoned-connection event is observable
/// in logs (monorepo#711). Implementation detail of
/// [`commit_with_rollback_guard`] — call that instead (monorepo#716).
async fn rollback_or_poison(mut conn: sqlx::pool::PoolConnection<sqlx::Sqlite>) {
    use sqlx::Connection;
    if let Err(rollback_err) = sqlx::query("ROLLBACK").execute(&mut *conn).await {
        match conn.detach().close().await {
            Ok(()) => tracing::warn!(
                rollback_error = %rollback_err,
                "ROLLBACK failed; detached and closed the potentially poisoned write-pool connection"
            ),
            Err(close_err) => tracing::warn!(
                rollback_error = %rollback_err,
                close_error = %close_err,
                "ROLLBACK failed; detached the potentially poisoned write-pool connection but close also failed"
            ),
        }
    }
}

/// Finish the raw `BEGIN IMMEDIATE` transaction open on `conn` given the
/// result of the transaction body — the single entry point for both the
/// commit and rollback paths (monorepo#716):
///
/// - `Ok(v)`: COMMIT, guarding against a failed COMMIT (monorepo#638 /
///   #657 / #670) — a failed COMMIT can leave the transaction open on the
///   pooled connection, so roll back explicitly so the connection is not
///   returned to the pool still holding the write lock (with detach+close
///   on a failed ROLLBACK, via [`rollback_or_poison`]), and return
///   `Error::Internal` with the message `"{context}: {commit error}"`.
/// - `Err(body_err)`: roll back the failed body via [`rollback_or_poison`]
///   (monorepo#680) and return the original `body_err`.
///
/// Takes the connection by value: COMMIT/ROLLBACK is the last statement of
/// the transaction either way, and the rollback path consumes it.
pub(crate) async fn commit_with_rollback_guard<T>(
    mut conn: sqlx::pool::PoolConnection<sqlx::Sqlite>,
    body_result: Result<T>,
    context: &str,
) -> Result<T> {
    match body_result {
        Ok(v) => {
            if let Err(e) = sqlx::query("COMMIT").execute(&mut *conn).await {
                rollback_or_poison(conn).await;
                return Err(Error::Internal(format!("{context}: {e}")));
            }
            Ok(v)
        }
        Err(e) => {
            rollback_or_poison(conn).await;
            Err(e)
        }
    }
}

#[cfg(test)]
mod tests;

/// Embedded schema migrations (§9.4).
///
/// **Adding workspace state? Transfer checklist.** A migration that adds a
/// table or column does not automatically ride the workspace-transfer
/// archive: tables must be listed in [`transfer_repo::TRANSFER_TABLES`] (with
/// a workspace-scoping predicate) or [`transfer_repo::TRANSFER_EXCLUDED_TABLES`]
/// (with a rationale) — schema-parity tests in `transfer_repo` fail until the
/// decision is made explicit. Machine-local column values (absolute paths,
/// client ids, daemon-local ids) need import-side rewriting in
/// `intent-services/src/transfer_import.rs`. Remember non-DB state too: the
/// assets dir and git bundle are exported separately, and runtime state
/// (hooks, PR monitors, agent queues) is rehydrated from imported rows on the
/// target.
static MIGRATOR: sqlx::migrate::Migrator = sqlx::migrate!();

/// SQLite-backed persistence handle. Cheaply cloneable (the pools are `Arc`-ed).
///
/// Holds two pools over the same DB file: a single-connection **write pool**
/// (`max_connections=1`) to serialize all mutations and eliminate in-process
/// writer-vs-writer `busy_timeout` contention, and a **read pool** (32 connections)
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
    ///
    /// # Errors
    ///
    /// Returns `Error::Internal` if the database cannot be opened or created, a migration fails, or the migration ledger records a version newer than this build (downgrade).
    pub async fn open(db_path: &Path) -> Result<Self> {
        let write_pool = connect_write(db_path).await?;
        let read_pool = connect_read(db_path).await?;
        // Run migrations on the write pool (migrations are write operations).
        MIGRATOR.run(&write_pool).await.map_err(|e| match e {
            sqlx::migrate::MigrateError::VersionMissing(version) => Error::Internal(format!(
                "database schema is newer than this intentd build (found applied \
                 migration {version} not known to this build); downgrades are \
                 unsupported — upgrade intentd to the version that created this database"
            )),
            _ => Error::Internal(format!("migrations failed: {e}")),
        })?;
        Ok(Self {
            write_pool,
            read_pool,
        })
    }

    /// Borrow the write pool (single connection, for INSERT/UPDATE/DELETE/BEGIN).
    #[must_use]
    pub fn write_pool(&self) -> &SqlitePool {
        &self.write_pool
    }

    /// Borrow the read pool (32 connections, intended for read/SELECT queries).
    #[must_use]
    pub fn read_pool(&self) -> &SqlitePool {
        &self.read_pool
    }

    /// Deprecated alias for `read_pool()`. External callers should migrate to
    /// explicit `read_pool()` / `write_pool()` usage.
    #[deprecated(since = "0.1.0", note = "use read_pool() or write_pool() explicitly")]
    #[must_use]
    pub fn pool(&self) -> &SqlitePool {
        &self.read_pool
    }

    /// Spawn a background task that periodically runs PRAGMA `wal_checkpoint(PASSIVE)`
    /// to prevent unbounded WAL growth when continuous readers hold long-lived
    /// transactions. Returns a handle that can be aborted to stop the task.
    ///
    /// Call this after `open()` in the daemon composition root; the task will run
    /// every ~60s until the returned handle is dropped or aborted.
    #[must_use]
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
    ///
    /// # Errors
    ///
    /// Returns `Error::Internal` if the database operation fails.
    pub async fn freelist_count(&self) -> Result<i64> {
        sqlx::query("PRAGMA freelist_count")
            .fetch_one(&self.write_pool)
            .await
            .map(|row| row.get::<i64, _>(0))
            .map_err(|e| Error::Internal(format!("freelist_count failed: {e}")))
    }

    /// Total number of pages in the database file (`PRAGMA page_count`).
    async fn page_count(&self) -> Result<i64> {
        sqlx::query("PRAGMA page_count")
            .fetch_one(&self.write_pool)
            .await
            .map(|row| row.get::<i64, _>(0))
            .map_err(|e| Error::Internal(format!("page_count failed: {e}")))
    }

    /// One-time activation of incremental auto-vacuum on a legacy database
    /// (monorepo#720 finding 1). New databases are created with
    /// `auto_vacuum = INCREMENTAL` (see [`connect_write`]), but on a database
    /// created before that pragma existed the setting is recorded yet inert
    /// until a full `VACUUM` rebuilds the file. This method checks
    /// `PRAGMA auto_vacuum`; when it reports NONE it runs `VACUUM` on the
    /// write pool so the recorded incremental setting takes effect and
    /// [`Self::incremental_vacuum`] can release freelist pages from then on.
    ///
    /// Intended to run once during daemon startup, after [`Store::open`] and
    /// before any listener serves: at that point the single write connection
    /// has no open transaction (a `VACUUM` requirement) and no client is
    /// blocked by the rebuild. Callers should treat a failure as non-fatal —
    /// the daemon runs exactly as before, it just cannot return freelist
    /// pages to the filesystem.
    ///
    /// # Errors
    ///
    /// Returns `Error::Internal` if the database operation fails.
    pub async fn activate_incremental_vacuum(&self) -> Result<AutoVacuumActivation> {
        let mode = sqlx::query("PRAGMA auto_vacuum")
            .fetch_one(&self.write_pool)
            .await
            .map(|row| row.get::<i64, _>(0))
            .map_err(|e| Error::Internal(format!("auto_vacuum query failed: {e}")))?;
        if mode != 0 {
            return Ok(AutoVacuumActivation::AlreadyIncremental);
        }
        let pages_before = self.page_count().await?;
        let started = std::time::Instant::now();
        sqlx::query("VACUUM")
            .execute(&self.write_pool)
            .await
            .map_err(|e| Error::Internal(format!("activation VACUUM failed: {e}")))?;
        // The full VACUUM may renumber `agent_message`'s implicit rowids
        // (TEXT primary key), which key the rowid-mapped `agent_message_fts`
        // index (0074) — rebuild it so the mapping stays correct.
        self.rebuild_agent_message_fts().await?;
        let duration = started.elapsed();
        let pages_after = self.page_count().await?;
        Ok(AutoVacuumActivation::Activated {
            duration,
            pages_before,
            pages_after,
        })
    }

    /// Release up to `max_pages` freelist pages back to the filesystem via
    /// `PRAGMA incremental_vacuum(N)`. Only effective when the database has
    /// `auto_vacuum = INCREMENTAL` (a no-op otherwise — see [`connect_write`]
    /// for the activation story). Bounded `N` keeps each call short so the
    /// single-connection write pool is never held for long; call it
    /// periodically (e.g. after retention sweeps) to reclaim space
    /// incrementally. Returns the number of pages actually freed.
    ///
    /// # Errors
    ///
    /// Returns `Error::Internal` if the database operation fails.
    pub async fn incremental_vacuum(&self, max_pages: u32) -> Result<u64> {
        let before = self.freelist_count().await?;
        sqlx::query(&format!("PRAGMA incremental_vacuum({max_pages})"))
            .execute(&self.write_pool)
            .await
            .map_err(|e| Error::Internal(format!("incremental_vacuum failed: {e}")))?;
        let after = self.freelist_count().await?;
        Ok((before - after).max(0).cast_unsigned())
    }

    /// Run `PRAGMA optimize` on the write connection. Cheap when called
    /// repeatedly (`SQLite` only re-analyzes tables whose content changed
    /// enough to matter); intended to run periodically after retention
    /// sweeps so the query planner statistics track the shrinking event
    /// table. See <https://sqlite.org/pragma.html#pragma_optimize>.
    ///
    /// # Errors
    ///
    /// Returns `Error::Internal` if the database operation fails.
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
    ///
    /// # Errors
    ///
    /// Returns `Error::Internal` if the database operation fails.
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

/// Outcome of [`Store::activate_incremental_vacuum`].
#[derive(Debug, Clone, PartialEq, Eq)]
pub enum AutoVacuumActivation {
    /// `PRAGMA auto_vacuum` already reports a non-NONE mode; nothing ran.
    AlreadyIncremental,
    /// The database was in NONE mode and a `VACUUM` rebuilt it in
    /// incremental mode, taking `duration` and shrinking the file from
    /// `pages_before` to `pages_after` pages.
    Activated {
        duration: std::time::Duration,
        pages_before: i64,
        pages_after: i64,
    },
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
    #[must_use]
    pub fn is_current(&self) -> bool {
        self.expected.iter().all(|v| self.applied.contains(v))
    }
}

/// Open a WAL-mode `SQLite` **write pool** with `max_connections=1` (§9.4).
/// The single-connection write pool serializes all mutations (INSERT/UPDATE/DELETE)
/// and eliminates in-process writer-vs-writer `busy_timeout` contention.
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
/// **`auto_vacuum` = INCREMENTAL**: without `auto_vacuum`, pages emptied by
/// deletes (retention sweeps) accumulate on the freelist forever and the file
/// only ever grows. sqlx applies the `auto_vacuum` pragma before
/// `journal_mode`, so a **new** database is created in incremental mode and
/// [`Store::incremental_vacuum`] can release freelist pages in bounded slices.
/// On an **existing** database created without `auto_vacuum` the pragma is
/// recorded but inert until a one-time `VACUUM` rebuilds the file. The daemon
/// performs that activation automatically at startup — `intentd serve` calls
/// [`Store::activate_incremental_vacuum`] right after [`Store::open`], before
/// any listener serves, so no client is blocked by the rebuild (monorepo#720
/// finding 1). Manual fallback, with the daemon **stopped**:
///
/// ```sh
/// sqlite3 ~/.intentd/intentd.db "PRAGMA auto_vacuum=INCREMENTAL; VACUUM;"
/// ```
///
/// `intentd doctor` reports the current `auto_vacuum` mode and freelist size,
/// and notes the next-start activation when the database is still in NONE
/// mode.
///
/// # Errors
///
/// Returns `Error::Internal` if the pool cannot be opened (or the acquire timeout is exceeded).
pub async fn connect_write(db_path: &Path) -> Result<SqlitePool> {
    let opts = SqliteConnectOptions::new()
        .filename(db_path)
        .create_if_missing(true)
        .auto_vacuum(SqliteAutoVacuum::Incremental)
        .journal_mode(SqliteJournalMode::Wal)
        .foreign_keys(true)
        .busy_timeout(Duration::from_secs(5))
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

/// Open a WAL-mode `SQLite` **read pool** with `max_connections=32` (§9.4).
/// The read pool is intended for read (SELECT) queries — by convention, not an
/// enforced read-only configuration — and supports concurrent readers without
/// contention (`SQLite` WAL mode allows many simultaneous readers).
///
/// The PRAGMAs match the write pool — `journal_mode = WAL`, `foreign_keys =
/// ON`, `busy_timeout = 5000`, `synchronous = NORMAL` — except `auto_vacuum`,
/// which is deliberately NOT set here: `auto_vacuum` is a property of the
/// database file, established at creation by [`connect_write`] ([`Store::open`]
/// opens the write pool first) or by the startup activation VACUUM
/// ([`Store::activate_incremental_vacuum`]), and only
/// [`Store::incremental_vacuum`] (write pool) ever acts on it. Setting it per
/// read connection re-executed `PRAGMA auto_vacuum = INCREMENTAL` on every
/// fresh pool connection, which surfaced as slow-statement WARNs inside hot
/// read RPCs under load and amplified write-pool contention
/// (intent-hq/monorepo#2673). For the same reason the read pool does NOT
/// create the file (`create_if_missing(false)`): the read pool never
/// establishes database-file properties, so it must never be the first to
/// create the file.
///
/// The read pool size (32) is
/// sized to absorb the client-driven startup burst (FE rehydrating several
/// workspaces at once) without saturating the pool and tripping sqlx's
/// `slow_acquire_threshold` warnings (STAB-6, STAB-46). The original size of
/// 16 was tuned against the fixed agent process cap of 30; the RAM-based cap
/// raise to 56 (intent-hq/intentd#296) roughly doubles the potential
/// concurrent-agent read load, so the pool doubles to 32 to preserve the
/// pool/agent-cap headroom ratio.
pub(crate) async fn connect_read(db_path: &Path) -> Result<SqlitePool> {
    let opts = SqliteConnectOptions::new()
        .filename(db_path)
        .create_if_missing(false)
        .journal_mode(SqliteJournalMode::Wal)
        .foreign_keys(true)
        .busy_timeout(Duration::from_secs(5))
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

/// Legacy test helper: builds a single pool (`max_connections=20`) for tests
/// that predate the write/read pool split. New tests should use `Store::open`.
///
/// # Errors
///
/// Returns `Error::Internal` if the pool cannot be opened (or the acquire timeout is exceeded).
#[cfg(test)]
pub async fn connect(db_path: &Path) -> Result<SqlitePool> {
    let opts = SqliteConnectOptions::new()
        .filename(db_path)
        .create_if_missing(true)
        .auto_vacuum(SqliteAutoVacuum::Incremental)
        .journal_mode(SqliteJournalMode::Wal)
        .foreign_keys(true)
        .busy_timeout(Duration::from_secs(5))
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

/// Encode an enum to its `lowercase/snake_case` string DB form via serde.
pub(crate) fn enum_to_db<T: serde::Serialize>(v: &T) -> Result<String> {
    serde_json::to_value(v)
        .ok()
        .and_then(|x| x.as_str().map(std::string::ToString::to_string))
        .ok_or_else(|| Error::Internal("failed to encode enum".to_string()))
}

/// Decode an enum from its string DB form via serde.
pub(crate) fn enum_from_db<T: serde::de::DeserializeOwned>(s: &str) -> Result<T> {
    serde_json::from_value(serde_json::Value::String(s.to_string()))
        .map_err(|e| Error::Internal(format!("failed to decode enum '{s}': {e}")))
}
