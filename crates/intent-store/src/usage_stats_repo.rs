//! Usage-stats repository: the global, time-bucketed `usage_stats_hourly`
//! counters behind the agentic usage-stats cards.
//!
//! One row per `(bucket_utc, model, provider)` — `bucket_utc` is the RFC-3339
//! UTC hour floor (e.g. `"2026-07-25T14:00:00Z"`), `model` the canonical
//! display name produced by the services-layer normalizer, `provider` the
//! resolved agent-provider id (`"unknown"` when unknowable, and for rows
//! written before the provider migration). Writers fold additive
//! [`UsageStatsDelta`]s into the bucket via [`Store::add_usage_stats`]; every
//! counter sums EXCEPT `longest_run_ms`, which takes the MAX (longest single
//! completed prompt-turn wall-clock duration in the bucket). Stats aggregate
//! globally: there is no workspace dimension.
//!
//! Next to the UTC key each row carries a [`LocalStamp`] — the daemon's local
//! wall-clock date/hour at recording time (D12) — written on INSERT only:
//! later deltas folding into an existing bucket keep the first-writer's
//! stamp, so a bucket-key collision across a DST fold skews at most one hour
//! of data by one hour.

use intent_core::{Error, Result};
use sqlx::Row;

use crate::Store;

/// One additive contribution to a `(bucket_utc, model, provider)` bucket. All counters
/// default to 0, so writers set only the fields their path owns (turn end:
/// tokens + runs + `longest_run_ms`; session start: `sessions_started`;
/// lines-changed: `lines_added/lines_deleted`). `longest_run_ms` is folded in
/// with MAX semantics, not summed.
#[derive(Debug, Clone, Default, PartialEq, Eq)]
pub struct UsageStatsDelta {
    pub input_tokens: u64,
    pub output_tokens: u64,
    pub cache_read_tokens: u64,
    pub cache_creation_tokens: u64,
    pub thought_tokens: u64,
    pub runs: u64,
    pub sessions_started: u64,
    pub longest_run_ms: u64,
    pub lines_added: u64,
    pub lines_deleted: u64,
}

/// The daemon's local wall-clock at recording time (D12): calendar date
/// (`"YYYY-MM-DD"`) and hour-of-day (0–23) under the system UTC offset in
/// effect when the bucket row was first created. Persisted next to the UTC
/// bucket key so read-side hour/month grouping is immune to later DST
/// transitions or timezone moves.
#[derive(Debug, Clone, PartialEq, Eq)]
pub struct LocalStamp {
    pub date: String,
    pub hour: u8,
}

/// One persisted `usage_stats_hourly` row (bucket key + accumulated
/// counters). `local_date` / `local_hour` are `None` only for rows written
/// before the D12 migration whose backfill did not apply (or values outside
/// the valid range, defensively) — readers fall back to shifting
/// `bucket_utc` by the client offset.
#[derive(Debug, Clone, Default, PartialEq, Eq)]
pub struct UsageStatsRow {
    pub bucket_utc: String,
    pub model: String,
    pub provider: String,
    pub local_date: Option<String>,
    pub local_hour: Option<u8>,
    pub input_tokens: u64,
    pub output_tokens: u64,
    pub cache_read_tokens: u64,
    pub cache_creation_tokens: u64,
    pub thought_tokens: u64,
    pub runs: u64,
    pub sessions_started: u64,
    pub longest_run_ms: u64,
    pub lines_added: u64,
    pub lines_deleted: u64,
}

const COUNTER_COLUMNS: &str = "input_tokens, output_tokens, cache_read_tokens, \
    cache_creation_tokens, thought_tokens, runs, sessions_started, longest_run_ms, \
    lines_added, lines_deleted";

impl Store {
    /// Fold one [`UsageStatsDelta`] into the `(bucket_utc, model, provider)`
    /// bucket, creating the row when absent: additive counters are summed
    /// while `longest_run_ms` takes `MAX(existing, delta)`. `bucket_utc` MUST
    /// be a UTC hour floor, `model` an already-normalized display name, and
    /// `provider` an already-resolved provider key (`"unknown"` when
    /// unknowable) — this layer stores what it is given. `local` stamps the
    /// row on INSERT only: the conflict-update deliberately leaves
    /// `local_date` / `local_hour` untouched, so an existing bucket keeps its
    /// first-writer's stamp. `None` (local offset indeterminate at record
    /// time) persists NULLs, which readers treat like pre-D12 rows.
    ///
    /// # Errors
    ///
    /// Returns `Error::Internal` if the database operation fails.
    pub async fn add_usage_stats(
        &self,
        bucket_utc: &str,
        model: &str,
        provider: &str,
        local: Option<&LocalStamp>,
        delta: &UsageStatsDelta,
    ) -> Result<()> {
        sqlx::query(
            "INSERT INTO usage_stats_hourly (
                bucket_utc, model, provider, local_date, local_hour, input_tokens, output_tokens,
                cache_read_tokens, cache_creation_tokens, thought_tokens, runs, sessions_started,
                longest_run_ms, lines_added, lines_deleted
             ) VALUES (?,?,?,?,?,?,?,?,?,?,?,?,?,?,?)
             ON CONFLICT(bucket_utc, model, provider) DO UPDATE SET
                input_tokens = input_tokens + excluded.input_tokens,
                output_tokens = output_tokens + excluded.output_tokens,
                cache_read_tokens = cache_read_tokens + excluded.cache_read_tokens,
                cache_creation_tokens = cache_creation_tokens + excluded.cache_creation_tokens,
                thought_tokens = thought_tokens + excluded.thought_tokens,
                runs = runs + excluded.runs,
                sessions_started = sessions_started + excluded.sessions_started,
                longest_run_ms = MAX(longest_run_ms, excluded.longest_run_ms),
                lines_added = lines_added + excluded.lines_added,
                lines_deleted = lines_deleted + excluded.lines_deleted",
        )
        .bind(bucket_utc)
        .bind(model)
        .bind(provider)
        .bind(local.map(|l| l.date.as_str()))
        .bind(local.map(|l| i64::from(l.hour)))
        .bind(delta.input_tokens.cast_signed())
        .bind(delta.output_tokens.cast_signed())
        .bind(delta.cache_read_tokens.cast_signed())
        .bind(delta.cache_creation_tokens.cast_signed())
        .bind(delta.thought_tokens.cast_signed())
        .bind(delta.runs.cast_signed())
        .bind(delta.sessions_started.cast_signed())
        .bind(delta.longest_run_ms.cast_signed())
        .bind(delta.lines_added.cast_signed())
        .bind(delta.lines_deleted.cast_signed())
        .execute(self.write_pool())
        .await
        .map_err(|e| Error::Internal(format!("add usage stats failed: {e}")))?;
        Ok(())
    }

    /// List every `usage_stats_hourly` row ordered by `(bucket_utc, model,
    /// provider)` — the read surface the `stats.getUsage` aggregation (and
    /// tests) build on; period filtering/grouping happens in the service layer.
    ///
    /// # Errors
    ///
    /// Returns `Error::Internal` if the database operation fails.
    pub async fn list_usage_stats_hourly(&self) -> Result<Vec<UsageStatsRow>> {
        let rows = sqlx::query(&format!(
            "SELECT bucket_utc, model, provider, local_date, local_hour, {COUNTER_COLUMNS}
             FROM usage_stats_hourly
             ORDER BY bucket_utc ASC, model ASC, provider ASC"
        ))
        .fetch_all(self.read_pool())
        .await
        .map_err(|e| Error::Internal(format!("list usage stats failed: {e}")))?;
        Ok(rows
            .iter()
            .map(|row| UsageStatsRow {
                bucket_utc: row.get("bucket_utc"),
                model: row.get("model"),
                provider: row.get("provider"),
                local_date: row.get("local_date"),
                local_hour: row
                    .get::<Option<i64>, _>("local_hour")
                    .and_then(|h| u8::try_from(h).ok())
                    .filter(|h| *h < 24),
                input_tokens: row.get::<i64, _>("input_tokens").cast_unsigned(),
                output_tokens: row.get::<i64, _>("output_tokens").cast_unsigned(),
                cache_read_tokens: row.get::<i64, _>("cache_read_tokens").cast_unsigned(),
                cache_creation_tokens: row.get::<i64, _>("cache_creation_tokens").cast_unsigned(),
                thought_tokens: row.get::<i64, _>("thought_tokens").cast_unsigned(),
                runs: row.get::<i64, _>("runs").cast_unsigned(),
                sessions_started: row.get::<i64, _>("sessions_started").cast_unsigned(),
                longest_run_ms: row.get::<i64, _>("longest_run_ms").cast_unsigned(),
                lines_added: row.get::<i64, _>("lines_added").cast_unsigned(),
                lines_deleted: row.get::<i64, _>("lines_deleted").cast_unsigned(),
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

    fn stamp(date: &str, hour: u8) -> LocalStamp {
        LocalStamp {
            date: date.to_string(),
            hour,
        }
    }

    /// First write creates the bucket row; a second write into the same
    /// `(bucket_utc, model, provider)` sums every additive counter while
    /// `longest_run_ms` keeps the MAX — a shorter later run must not
    /// regress it, a longer one must raise it.
    #[tokio::test]
    async fn upsert_accumulates_and_longest_run_takes_max() {
        let tmp = TempDb::new();
        let store = Store::open(&tmp.path).await.expect("open store");
        let bucket = "2026-07-25T14:00:00Z";
        let local = stamp("2026-07-25", 7);
        let first = UsageStatsDelta {
            input_tokens: 100,
            output_tokens: 40,
            cache_read_tokens: 20,
            cache_creation_tokens: 10,
            thought_tokens: 5,
            runs: 1,
            longest_run_ms: 5_000,
            ..Default::default()
        };
        store
            .add_usage_stats(bucket, "Opus 4.8", "claude-code", Some(&local), &first)
            .await
            .expect("first add");
        let second = UsageStatsDelta {
            input_tokens: 50,
            output_tokens: 10,
            thought_tokens: 2,
            runs: 1,
            longest_run_ms: 2_000, // shorter run — MAX must keep 5000
            ..Default::default()
        };
        store
            .add_usage_stats(bucket, "Opus 4.8", "claude-code", Some(&local), &second)
            .await
            .expect("second add");

        let rows = store.list_usage_stats_hourly().await.expect("list");
        assert_eq!(rows.len(), 1);
        let row = &rows[0];
        assert_eq!(row.bucket_utc, bucket);
        assert_eq!(row.model, "Opus 4.8");
        assert_eq!(row.provider, "claude-code");
        assert_eq!(row.input_tokens, 150);
        assert_eq!(row.output_tokens, 50);
        assert_eq!(row.cache_read_tokens, 20);
        assert_eq!(row.cache_creation_tokens, 10);
        assert_eq!(row.thought_tokens, 7);
        assert_eq!(row.runs, 2);
        assert_eq!(row.sessions_started, 0);
        assert_eq!(
            row.longest_run_ms, 5_000,
            "shorter run must not regress MAX"
        );
        assert_eq!(row.lines_added, 0);
        assert_eq!(row.lines_deleted, 0);
        assert_eq!(row.local_date.as_deref(), Some("2026-07-25"));
        assert_eq!(row.local_hour, Some(7));

        // A longer run raises the MAX.
        let third = UsageStatsDelta {
            runs: 1,
            longest_run_ms: 9_000,
            ..Default::default()
        };
        store
            .add_usage_stats(bucket, "Opus 4.8", "claude-code", Some(&local), &third)
            .await
            .expect("third add");
        let rows = store.list_usage_stats_hourly().await.expect("list");
        assert_eq!(rows[0].longest_run_ms, 9_000);
        assert_eq!(rows[0].runs, 3);
    }

    /// Different hour buckets, different models, and different providers land
    /// in separate rows, and the listing is ordered by (bucket, model,
    /// provider).
    #[tokio::test]
    async fn buckets_models_and_providers_are_isolated() {
        let tmp = TempDb::new();
        let store = Store::open(&tmp.path).await.expect("open store");
        let delta = UsageStatsDelta {
            input_tokens: 1,
            runs: 1,
            ..Default::default()
        };
        let local = stamp("2026-07-25", 8);
        store
            .add_usage_stats(
                "2026-07-25T15:00:00Z",
                "Sonnet 5",
                "claude-code",
                Some(&local),
                &delta,
            )
            .await
            .expect("add");
        store
            .add_usage_stats(
                "2026-07-25T14:00:00Z",
                "Sonnet 5",
                "claude-code",
                Some(&local),
                &delta,
            )
            .await
            .expect("add");
        store
            .add_usage_stats(
                "2026-07-25T14:00:00Z",
                "Opus 4.8",
                "claude-code",
                Some(&local),
                &delta,
            )
            .await
            .expect("add");
        // Same bucket + model via a different provider → its own row.
        store
            .add_usage_stats(
                "2026-07-25T14:00:00Z",
                "Opus 4.8",
                "auggie",
                Some(&local),
                &delta,
            )
            .await
            .expect("add");

        let rows = store.list_usage_stats_hourly().await.expect("list");
        let keys: Vec<(&str, &str, &str)> = rows
            .iter()
            .map(|r| (r.bucket_utc.as_str(), r.model.as_str(), r.provider.as_str()))
            .collect();
        assert_eq!(
            keys,
            vec![
                ("2026-07-25T14:00:00Z", "Opus 4.8", "auggie"),
                ("2026-07-25T14:00:00Z", "Opus 4.8", "claude-code"),
                ("2026-07-25T14:00:00Z", "Sonnet 5", "claude-code"),
                ("2026-07-25T15:00:00Z", "Sonnet 5", "claude-code"),
            ]
        );
        assert!(rows.iter().all(|r| r.input_tokens == 1 && r.runs == 1));
    }

    /// The local wall-clock stamp is written on INSERT only: a later delta
    /// folding into the same bucket with a different stamp (e.g. across a
    /// DST fold) keeps the first-writer's stamp while its counters still
    /// accumulate.
    #[tokio::test]
    async fn conflict_update_keeps_first_writer_local_stamp() {
        let tmp = TempDb::new();
        let store = Store::open(&tmp.path).await.expect("open store");
        let bucket = "2026-11-01T05:00:00Z";
        let delta = UsageStatsDelta {
            input_tokens: 1,
            ..Default::default()
        };
        store
            .add_usage_stats(
                bucket,
                "Opus 4.8",
                "claude-code",
                Some(&stamp("2026-11-01", 1)),
                &delta,
            )
            .await
            .expect("first add");
        store
            .add_usage_stats(
                bucket,
                "Opus 4.8",
                "claude-code",
                Some(&stamp("2026-11-01", 0)),
                &delta,
            )
            .await
            .expect("second add");

        let rows = store.list_usage_stats_hourly().await.expect("list");
        assert_eq!(rows.len(), 1);
        assert_eq!(rows[0].input_tokens, 2, "counters still accumulate");
        assert_eq!(rows[0].local_date.as_deref(), Some("2026-11-01"));
        assert_eq!(rows[0].local_hour, Some(1), "first-writer stamp wins");
    }

    /// Rows lacking local columns read back as `None` — both pre-D12 rows
    /// and rows written with `local: None` (offset indeterminate at record
    /// time) — and out-of-range `local_hour` values are defensively dropped
    /// to `None` instead of leaking into the aggregation.
    #[tokio::test]
    async fn null_and_out_of_range_local_columns_read_as_none() {
        let tmp = TempDb::new();
        let store = Store::open(&tmp.path).await.expect("open store");
        sqlx::query(
            "INSERT INTO usage_stats_hourly (bucket_utc, model, input_tokens)
             VALUES ('2026-07-25T14:00:00Z', 'Opus 4.8', 5)",
        )
        .execute(store.write_pool())
        .await
        .expect("insert pre-D12 row");
        sqlx::query(
            "INSERT INTO usage_stats_hourly (bucket_utc, model, local_date, local_hour)
             VALUES ('2026-07-25T15:00:00Z', 'Opus 4.8', '2026-07-25', 99)",
        )
        .execute(store.write_pool())
        .await
        .expect("insert out-of-range hour");
        store
            .add_usage_stats(
                "2026-07-25T16:00:00Z",
                "Opus 4.8",
                "claude-code",
                None,
                &UsageStatsDelta {
                    input_tokens: 1,
                    ..Default::default()
                },
            )
            .await
            .expect("insert unstamped row");

        let rows = store.list_usage_stats_hourly().await.expect("list");
        assert_eq!(rows[0].local_date, None);
        assert_eq!(rows[0].local_hour, None);
        assert_eq!(rows[0].provider, "unknown", "raw INSERT takes the default");
        assert_eq!(rows[1].local_date.as_deref(), Some("2026-07-25"));
        assert_eq!(rows[1].local_hour, None, "hour 99 must not surface");
        assert_eq!(rows[2].local_date, None, "None writes NULL stamps");
        assert_eq!(rows[2].local_hour, None);
        assert_eq!(rows[2].input_tokens, 1);
    }

    /// The 0057 migration backfill stamps pre-D12 rows (NULL local columns)
    /// from `bucket_utc` via `SQLite`'s system-timezone conversion and leaves
    /// already-stamped rows alone. Fresh DBs run the migration against an
    /// empty table, so re-execute the migration's UPDATE (the real embedded
    /// SQL, ALTERs skipped) against seeded pre-D12 rows and check it against
    /// `SQLite`'s own `'localtime'` conversion.
    #[tokio::test]
    async fn migration_backfill_stamps_pre_d12_rows_from_system_timezone() {
        let tmp = TempDb::new();
        let store = Store::open(&tmp.path).await.expect("open store");
        for bucket in ["2026-07-25T00:00:00Z", "2026-07-25T23:00:00Z"] {
            sqlx::query(
                "INSERT INTO usage_stats_hourly (bucket_utc, model, input_tokens)
                 VALUES (?, 'Opus 4.8', 1)",
            )
            .bind(bucket)
            .execute(store.write_pool())
            .await
            .expect("insert pre-D12 row");
        }
        store
            .add_usage_stats(
                "2026-07-25T14:00:00Z",
                "Opus 4.8",
                "claude-code",
                Some(&stamp("2026-07-26", 3)),
                &UsageStatsDelta::default(),
            )
            .await
            .expect("insert stamped row");

        // Re-run the backfill statement exactly as embedded in the 0057
        // migration (skipping the ALTERs, which already ran at open).
        let migration = crate::MIGRATOR
            .migrations
            .iter()
            .find(|m| m.version == 57)
            .expect("migration 0057 present");
        // Strip comment lines BEFORE splitting on ';' — comment prose may
        // itself contain semicolons.
        let sql: String = migration
            .sql
            .lines()
            .filter(|l| !l.trim_start().starts_with("--"))
            .collect::<Vec<_>>()
            .join("\n");
        for statement in sql.split(';') {
            let body = statement.trim();
            if body.is_empty() || body.starts_with("ALTER TABLE") {
                continue;
            }
            sqlx::query(body)
                .execute(store.write_pool())
                .await
                .expect("run backfill statement");
        }

        // (bucket_utc, local_date, local_hour, expected_date, expected_hour)
        type BackfillCheckRow = (String, Option<String>, Option<i64>, Option<String>, i64);
        let checked: Vec<BackfillCheckRow> = sqlx::query_as(
            "SELECT bucket_utc, local_date, local_hour,
                        date(bucket_utc, 'localtime'),
                        CAST(strftime('%H', bucket_utc, 'localtime') AS INTEGER)
                 FROM usage_stats_hourly ORDER BY bucket_utc",
        )
        .fetch_all(store.read_pool())
        .await
        .expect("read back");
        assert_eq!(checked.len(), 3);
        for (bucket, date, hour, expected_date, expected_hour) in &checked {
            if bucket == "2026-07-25T14:00:00Z" {
                // Already-stamped row: the WHERE guard must not overwrite it.
                assert_eq!(date.as_deref(), Some("2026-07-26"), "{bucket}");
                assert_eq!(*hour, Some(3), "{bucket}");
                continue;
            }
            assert_eq!(date.as_deref(), expected_date.as_deref(), "{bucket}");
            assert_eq!(*hour, Some(*expected_hour), "{bucket}");
            let d = date.as_deref().expect("backfilled date");
            assert_eq!(d.len(), 10, "YYYY-MM-DD: {d}");
            assert!(
                (0..24).contains(&hour.expect("backfilled hour")),
                "{bucket}"
            );
        }
    }

    /// The 0059 provider migration rebuilds the table (`SQLite` cannot alter a
    /// PK) and must preserve pre-existing rows with `provider = 'unknown'`.
    /// Fresh DBs run the migration against an empty table, so recreate the
    /// pre-0059 shape, seed rows, and re-execute the migration's embedded SQL.
    #[tokio::test]
    async fn provider_migration_preserves_rows_as_unknown() {
        let tmp = TempDb::new();
        let store = Store::open(&tmp.path).await.expect("open store");
        // Recreate the pre-0059 table shape (0056 schema + the 0057 ALTERs).
        sqlx::query("DROP TABLE usage_stats_hourly")
            .execute(store.write_pool())
            .await
            .expect("drop rebuilt table");
        sqlx::query(
            "CREATE TABLE usage_stats_hourly (
              bucket_utc            TEXT NOT NULL,
              model                 TEXT NOT NULL,
              input_tokens          INTEGER NOT NULL DEFAULT 0,
              output_tokens         INTEGER NOT NULL DEFAULT 0,
              cache_read_tokens     INTEGER NOT NULL DEFAULT 0,
              cache_creation_tokens INTEGER NOT NULL DEFAULT 0,
              runs                  INTEGER NOT NULL DEFAULT 0,
              sessions_started      INTEGER NOT NULL DEFAULT 0,
              longest_run_ms        INTEGER NOT NULL DEFAULT 0,
              lines_added           INTEGER NOT NULL DEFAULT 0,
              lines_deleted         INTEGER NOT NULL DEFAULT 0,
              local_date            TEXT,
              local_hour            INTEGER,
              PRIMARY KEY (bucket_utc, model)
            )",
        )
        .execute(store.write_pool())
        .await
        .expect("recreate pre-0059 table");
        sqlx::query(
            "INSERT INTO usage_stats_hourly
                (bucket_utc, model, input_tokens, runs, longest_run_ms, local_date, local_hour)
             VALUES ('2026-07-25T14:00:00Z', 'Opus 4.8', 100, 2, 5000, '2026-07-25', 7),
                    ('2026-07-25T15:00:00Z', 'Sonnet 5', 7, 1, 0, NULL, NULL)",
        )
        .execute(store.write_pool())
        .await
        .expect("seed pre-0059 rows");

        // Re-run the rebuild exactly as embedded in the 0059 migration.
        let migration = crate::MIGRATOR
            .migrations
            .iter()
            .find(|m| m.version == 59)
            .expect("migration 0059 present");
        // Strip comment lines BEFORE splitting on ';' — comment prose may
        // itself contain semicolons.
        let sql: String = migration
            .sql
            .lines()
            .filter(|l| !l.trim_start().starts_with("--"))
            .collect::<Vec<_>>()
            .join("\n");
        for statement in sql.split(';') {
            let body = statement.trim();
            if body.is_empty() {
                continue;
            }
            sqlx::query(body)
                .execute(store.write_pool())
                .await
                .expect("run migration statement");
        }
        // The rebuild recreated the pre-thought_tokens shape; replay the
        // shipped ALTER (single-statement migration, pulled from MIGRATOR so
        // it can never drift from the real file) so the reads below see the
        // current schema.
        let thought_migration = crate::MIGRATOR
            .migrations
            .iter()
            .find(|m| {
                m.sql
                    .contains("ALTER TABLE usage_stats_hourly ADD COLUMN thought_tokens")
            })
            .expect("usage_stats_hourly thought_tokens migration present");
        let alter: String = thought_migration
            .sql
            .lines()
            .filter(|l| !l.trim_start().starts_with("--"))
            .collect::<Vec<_>>()
            .join("\n");
        sqlx::query(alter.trim().trim_end_matches(';'))
            .execute(store.write_pool())
            .await
            .expect("re-apply thought_tokens ALTER");

        let rows = store.list_usage_stats_hourly().await.expect("list");
        assert_eq!(rows.len(), 2);
        assert_eq!(rows[0].model, "Opus 4.8");
        assert_eq!(rows[0].provider, "unknown");
        assert_eq!(rows[0].input_tokens, 100);
        assert_eq!(rows[0].runs, 2);
        assert_eq!(rows[0].longest_run_ms, 5_000);
        assert_eq!(rows[0].local_date.as_deref(), Some("2026-07-25"));
        assert_eq!(rows[0].local_hour, Some(7));
        assert_eq!(rows[1].model, "Sonnet 5");
        assert_eq!(rows[1].provider, "unknown");
        assert_eq!(rows[1].input_tokens, 7);
        assert_eq!(rows[1].local_date, None);

        // The rebuilt PK includes provider: the same (bucket, model) under a
        // real provider creates a second row instead of folding into
        // 'unknown'.
        store
            .add_usage_stats(
                "2026-07-25T14:00:00Z",
                "Opus 4.8",
                "claude-code",
                None,
                &UsageStatsDelta {
                    input_tokens: 1,
                    ..Default::default()
                },
            )
            .await
            .expect("add attributed row");
        let rows = store.list_usage_stats_hourly().await.expect("list");
        assert_eq!(rows.len(), 3);
        assert_eq!(rows[0].provider, "claude-code");
        assert_eq!(rows[0].input_tokens, 1);
        assert_eq!(rows[1].provider, "unknown");
        assert_eq!(rows[1].input_tokens, 100, "pre-migration row untouched");
    }
}
