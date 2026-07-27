-- Provider dimension for the usage-stats cards: each `usage_stats_hourly`
-- row additionally carries the resolved agent-provider id (`claude-code`,
-- `codex`, ...) so usage can be broken down by provider as well as by model.
-- `provider` joins the primary key — the same model reached via different
-- providers accrues in separate rows per bucket. SQLite cannot alter a
-- primary key in place, so the table is rebuilt; pre-existing rows (written
-- before this migration, with no provider attribution) are preserved with
-- `provider = 'unknown'` — the same sentinel writers use when the provider
-- is unknowable at record time. No backfill.
CREATE TABLE usage_stats_hourly_new (
  bucket_utc            TEXT NOT NULL,
  model                 TEXT NOT NULL,
  provider              TEXT NOT NULL DEFAULT 'unknown',
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
  PRIMARY KEY (bucket_utc, model, provider)
);
INSERT INTO usage_stats_hourly_new (
  bucket_utc, model, input_tokens, output_tokens, cache_read_tokens,
  cache_creation_tokens, runs, sessions_started, longest_run_ms,
  lines_added, lines_deleted, local_date, local_hour
)
SELECT
  bucket_utc, model, input_tokens, output_tokens, cache_read_tokens,
  cache_creation_tokens, runs, sessions_started, longest_run_ms,
  lines_added, lines_deleted, local_date, local_hour
FROM usage_stats_hourly;
DROP TABLE usage_stats_hourly;
ALTER TABLE usage_stats_hourly_new RENAME TO usage_stats_hourly;
