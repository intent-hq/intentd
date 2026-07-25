//! Usage-stats repository: the global, time-bucketed `usage_stats_hourly`
//! counters behind the agentic usage-stats cards.
//!
//! One row per `(bucket_utc, model)` — `bucket_utc` is the RFC-3339 UTC hour
//! floor (e.g. `"2026-07-25T14:00:00Z"`), `model` the canonical display name
//! produced by the services-layer normalizer. Writers fold additive
//! [`UsageStatsDelta`]s into the bucket via [`Store::add_usage_stats`]; every
//! counter sums EXCEPT `longest_run_ms`, which takes the MAX (longest single
//! completed prompt-turn wall-clock duration in the bucket). Stats aggregate
//! globally: there is no workspace dimension.

use intent_core::{Error, Result};
use sqlx::Row;

use crate::Store;

/// One additive contribution to a `(bucket_utc, model)` bucket. All counters
/// default to 0, so writers set only the fields their path owns (turn end:
/// tokens + runs + longest_run_ms; session start: sessions_started;
/// lines-changed: lines_added/lines_deleted). `longest_run_ms` is folded in
/// with MAX semantics, not summed.
#[derive(Debug, Clone, Default, PartialEq, Eq)]
pub struct UsageStatsDelta {
    pub input_tokens: u64,
    pub output_tokens: u64,
    pub cache_read_tokens: u64,
    pub cache_creation_tokens: u64,
    pub runs: u64,
    pub sessions_started: u64,
    pub longest_run_ms: u64,
    pub lines_added: u64,
    pub lines_deleted: u64,
}

/// One persisted `usage_stats_hourly` row (bucket key + accumulated counters).
#[derive(Debug, Clone, Default, PartialEq, Eq)]
pub struct UsageStatsRow {
    pub bucket_utc: String,
    pub model: String,
    pub input_tokens: u64,
    pub output_tokens: u64,
    pub cache_read_tokens: u64,
    pub cache_creation_tokens: u64,
    pub runs: u64,
    pub sessions_started: u64,
    pub longest_run_ms: u64,
    pub lines_added: u64,
    pub lines_deleted: u64,
}

const COUNTER_COLUMNS: &str = "input_tokens, output_tokens, cache_read_tokens, \
    cache_creation_tokens, runs, sessions_started, longest_run_ms, lines_added, lines_deleted";

impl Store {
    /// Fold one [`UsageStatsDelta`] into the `(bucket_utc, model)` bucket,
    /// creating the row when absent: additive counters are summed while
    /// `longest_run_ms` takes `MAX(existing, delta)`. `bucket_utc` MUST be a
    /// UTC hour floor and `model` an already-normalized display name — this
    /// layer stores what it is given.
    pub async fn add_usage_stats(
        &self,
        bucket_utc: &str,
        model: &str,
        delta: &UsageStatsDelta,
    ) -> Result<()> {
        sqlx::query(
            "INSERT INTO usage_stats_hourly (
                bucket_utc, model, input_tokens, output_tokens, cache_read_tokens,
                cache_creation_tokens, runs, sessions_started, longest_run_ms,
                lines_added, lines_deleted
             ) VALUES (?,?,?,?,?,?,?,?,?,?,?)
             ON CONFLICT(bucket_utc, model) DO UPDATE SET
                input_tokens = input_tokens + excluded.input_tokens,
                output_tokens = output_tokens + excluded.output_tokens,
                cache_read_tokens = cache_read_tokens + excluded.cache_read_tokens,
                cache_creation_tokens = cache_creation_tokens + excluded.cache_creation_tokens,
                runs = runs + excluded.runs,
                sessions_started = sessions_started + excluded.sessions_started,
                longest_run_ms = MAX(longest_run_ms, excluded.longest_run_ms),
                lines_added = lines_added + excluded.lines_added,
                lines_deleted = lines_deleted + excluded.lines_deleted",
        )
        .bind(bucket_utc)
        .bind(model)
        .bind(delta.input_tokens as i64)
        .bind(delta.output_tokens as i64)
        .bind(delta.cache_read_tokens as i64)
        .bind(delta.cache_creation_tokens as i64)
        .bind(delta.runs as i64)
        .bind(delta.sessions_started as i64)
        .bind(delta.longest_run_ms as i64)
        .bind(delta.lines_added as i64)
        .bind(delta.lines_deleted as i64)
        .execute(self.write_pool())
        .await
        .map_err(|e| Error::Internal(format!("add usage stats failed: {e}")))?;
        Ok(())
    }

    /// List every `usage_stats_hourly` row ordered by `(bucket_utc, model)` —
    /// the read surface the `stats.getUsage` aggregation (and tests) build on;
    /// period filtering/grouping happens in the service layer.
    pub async fn list_usage_stats_hourly(&self) -> Result<Vec<UsageStatsRow>> {
        let rows = sqlx::query(&format!(
            "SELECT bucket_utc, model, {COUNTER_COLUMNS}
             FROM usage_stats_hourly
             ORDER BY bucket_utc ASC, model ASC"
        ))
        .fetch_all(self.read_pool())
        .await
        .map_err(|e| Error::Internal(format!("list usage stats failed: {e}")))?;
        Ok(rows
            .iter()
            .map(|row| UsageStatsRow {
                bucket_utc: row.get("bucket_utc"),
                model: row.get("model"),
                input_tokens: row.get::<i64, _>("input_tokens") as u64,
                output_tokens: row.get::<i64, _>("output_tokens") as u64,
                cache_read_tokens: row.get::<i64, _>("cache_read_tokens") as u64,
                cache_creation_tokens: row.get::<i64, _>("cache_creation_tokens") as u64,
                runs: row.get::<i64, _>("runs") as u64,
                sessions_started: row.get::<i64, _>("sessions_started") as u64,
                longest_run_ms: row.get::<i64, _>("longest_run_ms") as u64,
                lines_added: row.get::<i64, _>("lines_added") as u64,
                lines_deleted: row.get::<i64, _>("lines_deleted") as u64,
            })
            .collect())
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use std::path::PathBuf;

    /// A unique temp DB path cleaned up on drop (mirrors `crate::tests::TempDb`,
    /// which is private to that module).
    struct TempDb {
        path: PathBuf,
    }

    impl TempDb {
        fn new() -> Self {
            let path =
                std::env::temp_dir().join(format!("intentd-test-{}.db", uuid::Uuid::new_v4()));
            Self { path }
        }
    }

    impl Drop for TempDb {
        fn drop(&mut self) {
            for suffix in ["", "-wal", "-shm"] {
                let _ = std::fs::remove_file(format!("{}{suffix}", self.path.display()));
            }
        }
    }

    /// First write creates the bucket row; a second write into the same
    /// `(bucket_utc, model)` sums every additive counter while
    /// `longest_run_ms` keeps the MAX — a shorter later run must not
    /// regress it, a longer one must raise it.
    #[tokio::test]
    async fn upsert_accumulates_and_longest_run_takes_max() {
        let tmp = TempDb::new();
        let store = Store::open(&tmp.path).await.expect("open store");
        let bucket = "2026-07-25T14:00:00Z";
        let first = UsageStatsDelta {
            input_tokens: 100,
            output_tokens: 40,
            cache_read_tokens: 20,
            cache_creation_tokens: 10,
            runs: 1,
            longest_run_ms: 5_000,
            ..Default::default()
        };
        store
            .add_usage_stats(bucket, "Opus 4.8", &first)
            .await
            .expect("first add");
        let second = UsageStatsDelta {
            input_tokens: 50,
            output_tokens: 10,
            runs: 1,
            longest_run_ms: 2_000, // shorter run — MAX must keep 5000
            ..Default::default()
        };
        store
            .add_usage_stats(bucket, "Opus 4.8", &second)
            .await
            .expect("second add");

        let rows = store.list_usage_stats_hourly().await.expect("list");
        assert_eq!(rows.len(), 1);
        let row = &rows[0];
        assert_eq!(row.bucket_utc, bucket);
        assert_eq!(row.model, "Opus 4.8");
        assert_eq!(row.input_tokens, 150);
        assert_eq!(row.output_tokens, 50);
        assert_eq!(row.cache_read_tokens, 20);
        assert_eq!(row.cache_creation_tokens, 10);
        assert_eq!(row.runs, 2);
        assert_eq!(row.sessions_started, 0);
        assert_eq!(
            row.longest_run_ms, 5_000,
            "shorter run must not regress MAX"
        );
        assert_eq!(row.lines_added, 0);
        assert_eq!(row.lines_deleted, 0);

        // A longer run raises the MAX.
        let third = UsageStatsDelta {
            runs: 1,
            longest_run_ms: 9_000,
            ..Default::default()
        };
        store
            .add_usage_stats(bucket, "Opus 4.8", &third)
            .await
            .expect("third add");
        let rows = store.list_usage_stats_hourly().await.expect("list");
        assert_eq!(rows[0].longest_run_ms, 9_000);
        assert_eq!(rows[0].runs, 3);
    }

    /// Different hour buckets and different models land in separate rows,
    /// and the listing is ordered by (bucket, model).
    #[tokio::test]
    async fn buckets_and_models_are_isolated() {
        let tmp = TempDb::new();
        let store = Store::open(&tmp.path).await.expect("open store");
        let delta = UsageStatsDelta {
            input_tokens: 1,
            runs: 1,
            ..Default::default()
        };
        store
            .add_usage_stats("2026-07-25T15:00:00Z", "Sonnet 5", &delta)
            .await
            .expect("add");
        store
            .add_usage_stats("2026-07-25T14:00:00Z", "Sonnet 5", &delta)
            .await
            .expect("add");
        store
            .add_usage_stats("2026-07-25T14:00:00Z", "Opus 4.8", &delta)
            .await
            .expect("add");

        let rows = store.list_usage_stats_hourly().await.expect("list");
        let keys: Vec<(&str, &str)> = rows
            .iter()
            .map(|r| (r.bucket_utc.as_str(), r.model.as_str()))
            .collect();
        assert_eq!(
            keys,
            vec![
                ("2026-07-25T14:00:00Z", "Opus 4.8"),
                ("2026-07-25T14:00:00Z", "Sonnet 5"),
                ("2026-07-25T15:00:00Z", "Sonnet 5"),
            ]
        );
        assert!(rows.iter().all(|r| r.input_tokens == 1 && r.runs == 1));
    }
}
