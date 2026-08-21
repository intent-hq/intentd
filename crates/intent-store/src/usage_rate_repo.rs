//! Usage-rate repository: the global, per-minute `usage_rate_minutely`
//! counters behind `stats.getRateHistory` (the HUD TOK/MIN chart).
//!
//! One row per UTC minute bucket — `bucket_utc` is the RFC-3339 UTC minute
//! floor (e.g. `"2026-07-30T14:07:00Z"`). Writers fold additive
//! [`UsageRateDelta`]s into the bucket via [`Store::add_usage_rate`]; every
//! counter sums. Rates aggregate globally: there is deliberately no
//! workspace / model / provider dimension. The table is capped by retention:
//! [`Store::delete_usage_rate_before`] (driven by the hourly reaper) removes
//! buckets at or older than the 24h cutoff (inclusive), bounding the table
//! at ≤ 1440 rows even when a sweep lands exactly on a minute boundary.

use intent_core::{Error, Result};
use sqlx::Row;

use crate::Store;

/// One additive contribution to a `usage_rate_minutely` bucket: the token
/// counters of one per-turn delta (the same clamped delta that feeds
/// `usage_stats_hourly` — never a raw cumulative snapshot).
#[derive(Debug, Clone, Default, PartialEq, Eq)]
pub struct UsageRateDelta {
    pub input_tokens: u64,
    pub output_tokens: u64,
    pub cache_read_tokens: u64,
    pub cache_creation_tokens: u64,
    pub thought_tokens: u64,
}

impl UsageRateDelta {
    /// True when every counter is zero — such deltas are skipped by writers
    /// (an all-zero turn adds nothing and would only churn the table).
    pub fn is_zero(&self) -> bool {
        *self == Self::default()
    }
}

/// One persisted `usage_rate_minutely` row (minute bucket key + accumulated
/// counters).
#[derive(Debug, Clone, Default, PartialEq, Eq)]
pub struct UsageRateRow {
    pub bucket_utc: String,
    pub input_tokens: u64,
    pub output_tokens: u64,
    pub cache_read_tokens: u64,
    pub cache_creation_tokens: u64,
    pub thought_tokens: u64,
}

impl Store {
    /// Fold one [`UsageRateDelta`] into the UTC minute bucket, creating the
    /// row when absent: all counters are summed. `bucket_utc` MUST be a UTC
    /// minute floor (`"YYYY-MM-DDTHH:MM:00Z"`) — this layer stores what it
    /// is given.
    ///
    /// # Errors
    ///
    /// Returns `Error::Internal` if the database operation fails.
    pub async fn add_usage_rate(&self, bucket_utc: &str, delta: &UsageRateDelta) -> Result<()> {
        sqlx::query(
            "INSERT INTO usage_rate_minutely (
                bucket_utc, input_tokens, output_tokens, cache_read_tokens,
                cache_creation_tokens, thought_tokens
             ) VALUES (?,?,?,?,?,?)
             ON CONFLICT(bucket_utc) DO UPDATE SET
                input_tokens = input_tokens + excluded.input_tokens,
                output_tokens = output_tokens + excluded.output_tokens,
                cache_read_tokens = cache_read_tokens + excluded.cache_read_tokens,
                cache_creation_tokens = cache_creation_tokens + excluded.cache_creation_tokens,
                thought_tokens = thought_tokens + excluded.thought_tokens",
        )
        .bind(bucket_utc)
        .bind(delta.input_tokens as i64)
        .bind(delta.output_tokens as i64)
        .bind(delta.cache_read_tokens as i64)
        .bind(delta.cache_creation_tokens as i64)
        .bind(delta.thought_tokens as i64)
        .execute(self.write_pool())
        .await
        .map_err(|e| Error::Internal(format!("add usage rate failed: {e}")))?;
        Ok(())
    }

    /// List the `usage_rate_minutely` rows with `bucket_utc >= since`,
    /// ordered ascending — the read surface the `stats.getRateHistory`
    /// zero-fill builds on (RFC-3339 UTC keys compare lexicographically).
    ///
    /// # Errors
    ///
    /// Returns `Error::Internal` if the database operation fails.
    pub async fn list_usage_rate_since(&self, since: &str) -> Result<Vec<UsageRateRow>> {
        let rows = sqlx::query(
            "SELECT bucket_utc, input_tokens, output_tokens, cache_read_tokens,
                    cache_creation_tokens, thought_tokens
             FROM usage_rate_minutely
             WHERE bucket_utc >= ?
             ORDER BY bucket_utc ASC",
        )
        .bind(since)
        .fetch_all(self.read_pool())
        .await
        .map_err(|e| Error::Internal(format!("list usage rate failed: {e}")))?;
        Ok(rows
            .iter()
            .map(|row| UsageRateRow {
                bucket_utc: row.get("bucket_utc"),
                input_tokens: row.get::<i64, _>("input_tokens") as u64,
                output_tokens: row.get::<i64, _>("output_tokens") as u64,
                cache_read_tokens: row.get::<i64, _>("cache_read_tokens") as u64,
                cache_creation_tokens: row.get::<i64, _>("cache_creation_tokens") as u64,
                thought_tokens: row.get::<i64, _>("thought_tokens") as u64,
            })
            .collect())
    }

    /// Retention sweep: delete minute buckets with `bucket_utc <= cutoff`
    /// (an RFC-3339 UTC string) and return the number of rows removed.
    /// Inclusive so a sweep landing exactly on a minute boundary still leaves
    /// at most 1440 buckets (cutoff bucket removed, cutoff+1 .. now retained).
    /// Idempotent — a re-run with the same cutoff removes nothing more.
    ///
    /// # Errors
    ///
    /// Returns `Error::Internal` if the database operation fails.
    pub async fn delete_usage_rate_before(&self, cutoff: &str) -> Result<u64> {
        let result = sqlx::query("DELETE FROM usage_rate_minutely WHERE bucket_utc <= ?")
            .bind(cutoff)
            .execute(self.write_pool())
            .await
            .map_err(|e| Error::Internal(format!("prune usage rate failed: {e}")))?;
        Ok(result.rows_affected())
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

    /// First write creates the minute bucket; a second write into the same
    /// bucket sums every counter; a different minute creates a second row.
    #[tokio::test]
    async fn upsert_accumulates_per_minute_bucket() {
        let tmp = TempDb::new();
        let store = Store::open(&tmp.path).await.expect("open store");
        let bucket = "2026-07-30T14:07:00Z";
        store
            .add_usage_rate(
                bucket,
                &UsageRateDelta {
                    input_tokens: 100,
                    output_tokens: 40,
                    cache_read_tokens: 20,
                    cache_creation_tokens: 10,
                    thought_tokens: 5,
                },
            )
            .await
            .expect("first add");
        store
            .add_usage_rate(
                bucket,
                &UsageRateDelta {
                    input_tokens: 50,
                    output_tokens: 10,
                    thought_tokens: 3,
                    ..Default::default()
                },
            )
            .await
            .expect("second add");
        store
            .add_usage_rate(
                "2026-07-30T14:08:00Z",
                &UsageRateDelta {
                    input_tokens: 7,
                    ..Default::default()
                },
            )
            .await
            .expect("next-minute add");

        let rows = store.list_usage_rate_since("").await.expect("list");
        assert_eq!(rows.len(), 2);
        assert_eq!(rows[0].bucket_utc, bucket);
        assert_eq!(rows[0].input_tokens, 150);
        assert_eq!(rows[0].output_tokens, 50);
        assert_eq!(rows[0].cache_read_tokens, 20);
        assert_eq!(rows[0].cache_creation_tokens, 10);
        assert_eq!(rows[0].thought_tokens, 8);
        assert_eq!(rows[1].bucket_utc, "2026-07-30T14:08:00Z");
        assert_eq!(rows[1].input_tokens, 7);
        // A bucket that never saw a thought-token delta reads back as 0 (the
        // additive column's default), same as a pre-migration row.
        assert_eq!(rows[1].thought_tokens, 0);
    }

    /// `list_usage_rate_since` is an inclusive lower bound on the RFC-3339
    /// key (lexicographic compare), returning ascending order.
    #[tokio::test]
    async fn list_since_filters_inclusive_and_sorts_ascending() {
        let tmp = TempDb::new();
        let store = Store::open(&tmp.path).await.expect("open store");
        for (bucket, tokens) in [
            ("2026-07-30T14:09:00Z", 3u64),
            ("2026-07-30T14:07:00Z", 1),
            ("2026-07-30T14:08:00Z", 2),
        ] {
            store
                .add_usage_rate(
                    bucket,
                    &UsageRateDelta {
                        input_tokens: tokens,
                        ..Default::default()
                    },
                )
                .await
                .expect("add");
        }
        let rows = store
            .list_usage_rate_since("2026-07-30T14:08:00Z")
            .await
            .expect("list");
        assert_eq!(rows.len(), 2);
        assert_eq!(rows[0].bucket_utc, "2026-07-30T14:08:00Z");
        assert_eq!(rows[1].bucket_utc, "2026-07-30T14:09:00Z");
    }

    /// Retention sweep removes buckets at or older than the cutoff (inclusive,
    /// so a boundary-aligned sweep keeps ≤ 1440 buckets) and reports the
    /// removed count; a re-run is a no-op.
    #[tokio::test]
    async fn prune_deletes_before_cutoff_and_is_idempotent() {
        let tmp = TempDb::new();
        let store = Store::open(&tmp.path).await.expect("open store");
        for bucket in [
            "2026-07-29T13:59:00Z",
            "2026-07-29T14:00:00Z",
            "2026-07-30T14:00:00Z",
        ] {
            store
                .add_usage_rate(
                    bucket,
                    &UsageRateDelta {
                        input_tokens: 1,
                        ..Default::default()
                    },
                )
                .await
                .expect("add");
        }
        let removed = store
            .delete_usage_rate_before("2026-07-29T14:00:00Z")
            .await
            .expect("prune");
        assert_eq!(
            removed, 2,
            "cutoff bucket removed too (inclusive predicate)"
        );
        let rows = store.list_usage_rate_since("").await.expect("list");
        assert_eq!(rows.len(), 1);
        assert_eq!(rows[0].bucket_utc, "2026-07-30T14:00:00Z");
        let removed_again = store
            .delete_usage_rate_before("2026-07-29T14:00:00Z")
            .await
            .expect("re-prune");
        assert_eq!(removed_again, 0);
    }

    /// All-zero deltas are detectable so writers can skip them.
    #[test]
    fn is_zero_detects_empty_delta() {
        assert!(UsageRateDelta::default().is_zero());
        assert!(!UsageRateDelta {
            output_tokens: 1,
            ..Default::default()
        }
        .is_zero());
        // A thought-only turn is a real contribution, not a skippable no-op.
        assert!(!UsageRateDelta {
            thought_tokens: 1,
            ..Default::default()
        }
        .is_zero());
    }
}
