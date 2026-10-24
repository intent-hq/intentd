//! Post-startup removal of the two large tables left behind by the retired
//! note paging system (intent-hq/intent#7515).
//!
//! Migration 0164 drops every paging trigger and every small paging table, but
//! leaves `note_page_entry` (millions of rows on large databases) and its
//! parent `note_page_head`: dropping the entry table inside the migration held
//! startup for about two minutes on a 17 GB database. Nothing reads or writes
//! either table any more, so any partially cleared state is harmless. The
//! cleanup deletes rows in short batches on the shared write pool, then drops
//! each table once it is empty. Every step is one autocommit statement, so a
//! daemon that stops mid-cleanup simply resumes on its next start.
//!
//! Deleted rows' pages go to the freelist (the database runs with
//! `auto_vacuum = INCREMENTAL`); the retention loop's paced incremental vacuum
//! drain releases them to the filesystem (intent-hq/intent#7448).
use std::time::{Duration, Instant};

use intent_core::{Error, Result};

use crate::Store;

/// The retired tables, children first: deleting `note_page_head` rows while
/// entry rows remain would cascade into the entry table in one statement.
pub const RETIRED_NOTE_PAGING_TABLES: [&str; 2] = ["note_page_entry", "note_page_head"];

/// Rows deleted by the first batch.
pub const NOTE_PAGING_CLEANUP_BATCH_ROWS: u32 = 5_000;

/// Smallest batch the cleanup shrinks to when batches run slow.
pub const NOTE_PAGING_CLEANUP_MIN_BATCH_ROWS: u32 = 500;

/// Longest one batch should hold the single write connection.
pub const NOTE_PAGING_CLEANUP_HOLD_BUDGET: Duration = Duration::from_millis(100);

/// Floor on the pause between batches. The pause is otherwise the previous
/// batch's own duration, capping the cleanup's writer duty cycle at about 50%.
pub const NOTE_PAGING_CLEANUP_MIN_PAUSE: Duration = Duration::from_millis(50);

/// Interval between progress log lines.
pub const NOTE_PAGING_CLEANUP_PROGRESS_EVERY: Duration = Duration::from_secs(30);

/// First retry delay after a failed step; doubles per consecutive failure.
pub const NOTE_PAGING_CLEANUP_INITIAL_BACKOFF: Duration = Duration::from_secs(1);

/// Cap on the retry delay after consecutive failed steps.
pub const NOTE_PAGING_CLEANUP_MAX_BACKOFF: Duration = Duration::from_secs(300);

/// One step of the cleanup.
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub enum RetiredNotePagingStep {
    /// Deleted `rows` rows from `table`.
    Deleted {
        /// The table the rows came from.
        table: &'static str,
        /// Rows removed by this batch.
        rows: u64,
    },
    /// Dropped the now-empty `table`.
    Dropped {
        /// The dropped table.
        table: &'static str,
    },
    /// Both retired tables are gone.
    Done,
}

/// Pacing knobs for [`Store::run_retired_note_paging_cleanup`].
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub struct NotePagingCleanupPolicy {
    /// Rows in the first batch ([`NOTE_PAGING_CLEANUP_BATCH_ROWS`]).
    pub batch_rows: u32,
    /// Smallest batch ([`NOTE_PAGING_CLEANUP_MIN_BATCH_ROWS`]).
    pub min_batch_rows: u32,
    /// Target hold per batch ([`NOTE_PAGING_CLEANUP_HOLD_BUDGET`]).
    pub hold_budget: Duration,
    /// Floor on the pause between batches ([`NOTE_PAGING_CLEANUP_MIN_PAUSE`]).
    pub min_pause: Duration,
    /// Progress log interval ([`NOTE_PAGING_CLEANUP_PROGRESS_EVERY`]).
    pub progress_every: Duration,
    /// First retry delay ([`NOTE_PAGING_CLEANUP_INITIAL_BACKOFF`]).
    pub initial_backoff: Duration,
    /// Retry delay cap ([`NOTE_PAGING_CLEANUP_MAX_BACKOFF`]).
    pub max_backoff: Duration,
}

impl Default for NotePagingCleanupPolicy {
    fn default() -> Self {
        Self {
            batch_rows: NOTE_PAGING_CLEANUP_BATCH_ROWS,
            min_batch_rows: NOTE_PAGING_CLEANUP_MIN_BATCH_ROWS,
            hold_budget: NOTE_PAGING_CLEANUP_HOLD_BUDGET,
            min_pause: NOTE_PAGING_CLEANUP_MIN_PAUSE,
            progress_every: NOTE_PAGING_CLEANUP_PROGRESS_EVERY,
            initial_backoff: NOTE_PAGING_CLEANUP_INITIAL_BACKOFF,
            max_backoff: NOTE_PAGING_CLEANUP_MAX_BACKOFF,
        }
    }
}

/// What one [`Store::run_retired_note_paging_cleanup`] did.
#[derive(Debug, Clone, Copy, Default, PartialEq, Eq)]
pub struct NotePagingCleanupOutcome {
    /// Rows deleted across both tables.
    pub rows_removed: u64,
    /// Tables dropped by this run.
    pub tables_dropped: u32,
    /// Longest single step.
    pub max_step: Duration,
    /// Failed steps that were retried.
    pub failed_steps: u32,
    /// True when both tables are gone.
    pub finished: bool,
}

impl Store {
    /// Run one cleanup step: delete up to `batch_rows` rows from the first
    /// retired table that still exists, or drop it once it is empty.
    ///
    /// # Errors
    /// Returns `Error::Internal` if the schema read or the write fails.
    pub async fn retired_note_paging_step(&self, batch_rows: u32) -> Result<RetiredNotePagingStep> {
        for table in RETIRED_NOTE_PAGING_TABLES {
            let present: bool = sqlx::query_scalar(
                "SELECT EXISTS(SELECT 1 FROM sqlite_master WHERE type='table' AND name=?)",
            )
            .bind(table)
            .fetch_one(self.read_pool())
            .await
            .map_err(|e| Error::Internal(format!("read retired note paging schema: {e}")))?;
            if !present {
                continue;
            }
            // `table` is one of two constants, never caller input.
            let deleted = sqlx::query(&format!(
                "DELETE FROM {table} WHERE rowid IN (SELECT rowid FROM {table} LIMIT ?)"
            ))
            .bind(i64::from(batch_rows.max(1)))
            .execute(self.write_pool())
            .await
            .map_err(|e| Error::Internal(format!("delete retired {table} rows: {e}")))?
            .rows_affected();
            if deleted > 0 {
                return Ok(RetiredNotePagingStep::Deleted {
                    table,
                    rows: deleted,
                });
            }
            sqlx::query(&format!("DROP TABLE IF EXISTS {table}"))
                .execute(self.write_pool())
                .await
                .map_err(|e| Error::Internal(format!("drop retired {table}: {e}")))?;
            return Ok(RetiredNotePagingStep::Dropped { table });
        }
        Ok(RetiredNotePagingStep::Done)
    }

    /// Remove the retired note paging tables in short, paced batches until
    /// both are gone. Between batches the write connection goes back to the
    /// (FIFO-fair) pool and the cleanup pauses, so queued writes run first. A
    /// batch slower than `policy.hold_budget` halves the next one. Logs start,
    /// periodic progress and completion at info. A failed step is logged at
    /// warn with its attempt count and retried after a capped exponential
    /// backoff (`policy.initial_backoff` doubling to `policy.max_backoff`), so
    /// a transient failure such as a write-pool acquire timeout does not leave
    /// the tables until the next restart. Every wait is a cancellable sleep:
    /// aborting the task at shutdown stops it between statements.
    pub async fn run_retired_note_paging_cleanup(
        &self,
        policy: NotePagingCleanupPolicy,
    ) -> NotePagingCleanupOutcome {
        let started = Instant::now();
        let mut last_progress = started;
        let mut batch = policy.batch_rows.max(1);
        let mut outcome = NotePagingCleanupOutcome::default();
        let mut announced = false;
        let mut consecutive_failures = 0u32;
        loop {
            let step_started = Instant::now();
            let step = match self.retired_note_paging_step(batch).await {
                Ok(step) => {
                    consecutive_failures = 0;
                    step
                }
                Err(e) => {
                    consecutive_failures += 1;
                    outcome.failed_steps += 1;
                    let backoff = retry_backoff(&policy, consecutive_failures);
                    tracing::warn!(
                        error = %e,
                        attempt = consecutive_failures,
                        retry_in_ms = backoff.as_millis(),
                        rows_removed = outcome.rows_removed,
                        elapsed_ms = started.elapsed().as_millis(),
                        "retired note paging cleanup step failed; retrying"
                    );
                    tokio::time::sleep(backoff).await;
                    continue;
                }
            };
            let elapsed = step_started.elapsed();
            outcome.max_step = outcome.max_step.max(elapsed);
            match step {
                RetiredNotePagingStep::Done => {
                    outcome.finished = true;
                    if announced {
                        tracing::info!(
                            rows_removed = outcome.rows_removed,
                            tables_dropped = outcome.tables_dropped,
                            max_step_ms = outcome.max_step.as_millis(),
                            elapsed_ms = started.elapsed().as_millis(),
                            "retired note paging cleanup finished"
                        );
                    }
                    return outcome;
                }
                RetiredNotePagingStep::Deleted { table, rows } => {
                    if !announced {
                        announced = true;
                        tracing::info!(table, "retired note paging cleanup started");
                    }
                    outcome.rows_removed += rows;
                    if elapsed > policy.hold_budget {
                        batch = (batch / 2).max(policy.min_batch_rows.max(1));
                    }
                }
                RetiredNotePagingStep::Dropped { table } => {
                    if !announced {
                        announced = true;
                        tracing::info!(table, "retired note paging cleanup started");
                    }
                    outcome.tables_dropped += 1;
                    tracing::info!(table, "retired note paging table dropped");
                }
            }
            if last_progress.elapsed() >= policy.progress_every {
                last_progress = Instant::now();
                tracing::info!(
                    rows_removed = outcome.rows_removed,
                    batch_rows = batch,
                    elapsed_ms = started.elapsed().as_millis(),
                    "retired note paging cleanup progress"
                );
            }
            tokio::time::sleep(elapsed.max(policy.min_pause)).await;
        }
    }
}

/// Delay before retrying after `failures` consecutive failed steps: the
/// initial backoff doubled per extra failure, capped at the maximum.
fn retry_backoff(policy: &NotePagingCleanupPolicy, failures: u32) -> Duration {
    let doublings = failures.saturating_sub(1).min(31);
    policy
        .initial_backoff
        .saturating_mul(1u32 << doublings)
        .min(policy.max_backoff)
}

/// True when `error` reports a table the post-startup cleanup already dropped.
fn is_missing_table(error: &sqlx::Error) -> bool {
    error.to_string().contains("no such table")
}

impl Store {
    /// Delete one workspace's retired page-index rows in bounded batches
    /// before its notes are deleted, so a note batch never cascades
    /// note → `note_page_head` → `note_page_entry` over millions of rows in one
    /// statement. Each batch is its own autocommit statement of at most
    /// [`NOTE_PAGING_CLEANUP_BATCH_ROWS`] rows. A table the post-startup
    /// cleanup has already dropped (before or between batches) is skipped.
    ///
    /// # Errors
    /// Returns `Error::Internal` if a batch fails for any other reason.
    pub(crate) async fn delete_workspace_retired_note_paging(
        &self,
        workspace_id: &str,
    ) -> Result<()> {
        self.delete_workspace_retired_note_paging_in_batches(
            workspace_id,
            NOTE_PAGING_CLEANUP_BATCH_ROWS,
        )
        .await
        .map(drop)
    }

    /// [`Self::delete_workspace_retired_note_paging`] with an explicit batch
    /// size. Returns the rows each DELETE statement removed, in order.
    pub(crate) async fn delete_workspace_retired_note_paging_in_batches(
        &self,
        workspace_id: &str,
        batch_rows: u32,
    ) -> Result<Vec<u64>> {
        let batch = i64::from(batch_rows.max(1));
        let mut statements = Vec::new();
        for table in RETIRED_NOTE_PAGING_TABLES {
            // `table` is one of two constants, never caller input.
            let sql = format!(
                "DELETE FROM {table} WHERE rowid IN \
                 (SELECT rowid FROM {table} WHERE workspace_id = ? LIMIT ?)"
            );
            loop {
                // Busy-retry window: a saturated single writer must not fail
                // the workspace delete sweep (intent-hq/intent#7705).
                let removed = crate::with_write_txn_retry(|| async {
                    match sqlx::query(&sql)
                        .bind(workspace_id)
                        .bind(batch)
                        .execute(self.write_pool())
                        .await
                    {
                        Ok(done) => Ok(Some(done.rows_affected())),
                        Err(e) if is_missing_table(&e) => Ok(None),
                        Err(e) => Err(Error::Internal(format!(
                            "delete workspace retired {table} rows: {e}"
                        ))),
                    }
                })
                .await?;
                let Some(removed) = removed else { break };
                statements.push(removed);
                if removed < batch.unsigned_abs() {
                    break;
                }
                tokio::task::yield_now().await;
            }
        }
        Ok(statements)
    }
}
