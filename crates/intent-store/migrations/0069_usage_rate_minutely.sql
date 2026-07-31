-- Per-minute all-workspace token-rate history behind `stats.getRateHistory`
-- (the HUD TOK/MIN chart): one row per UTC minute bucket, aggregating the
-- same per-turn token DELTAS that feed `usage_stats_hourly` (§5.36) — never
-- raw cumulative session snapshots. `bucket_utc` is the RFC-3339 UTC minute
-- floor, e.g. "2026-07-30T14:07:00Z". There is deliberately NO workspace /
-- model / provider dimension: the surface answers "how many tokens per
-- minute, fleet-wide" and nothing else. Rows are capped by retention — the
-- hourly reaper deletes buckets older than 24h — so the table stays ≤ 1440
-- rows. All counters are additive per bucket.
CREATE TABLE usage_rate_minutely (
  bucket_utc            TEXT NOT NULL PRIMARY KEY,
  input_tokens          INTEGER NOT NULL DEFAULT 0,
  output_tokens         INTEGER NOT NULL DEFAULT 0,
  cache_read_tokens     INTEGER NOT NULL DEFAULT 0,
  cache_creation_tokens INTEGER NOT NULL DEFAULT 0
) WITHOUT ROWID;
