-- Global time-bucketed usage stats behind the agentic usage-stats cards:
-- one row per (UTC hour bucket, normalized model name). Counters accrue
-- live from feature launch (no backfill) and aggregate globally — there is
-- deliberately NO workspace dimension. `bucket_utc` is the RFC-3339 UTC
-- hour floor, e.g. "2026-07-25T14:00:00Z"; `model` is the canonical display
-- name produced by the services-layer normalizer ("unknown" fallback), so
-- the same model reached via different hosts shares one row per bucket.
--
-- All counters are additive per bucket EXCEPT `longest_run_ms`, which is a
-- MAX: the longest single completed prompt-turn wall-clock duration
-- observed in the bucket. `sessions_started` / `lines_added` /
-- `lines_deleted` are reserved for the session-start and lines-changed
-- recording paths and stay 0 until those land.
CREATE TABLE usage_stats_hourly (
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
  PRIMARY KEY (bucket_utc, model)
);
