-- D12 local wall-clock bucketing: hour/month grouping must reflect the
-- daemon's local wall-clock AT RECORDING TIME, immune to later DST
-- transitions or timezone moves. `bucket_utc` stays the UTC hour floor and
-- remains the primary bucket key (the 24h rolling window, ordering, and
-- upsert semantics depend on it); `local_date` ("YYYY-MM-DD") and
-- `local_hour` (0-23) capture the local wall-clock when the bucket row was
-- first created. Writers stamp them on INSERT only — later writes folding
-- into the same bucket keep the first-writer's stamp (a bucket key can
-- collide across a DST fold; <=1h of skew for <=1h of data is acceptable).
ALTER TABLE usage_stats_hourly ADD COLUMN local_date TEXT;
ALTER TABLE usage_stats_hourly ADD COLUMN local_hour INTEGER;

-- Backfill: pre-D12 rows have no stamp, so derive one from `bucket_utc` via
-- SQLite's 'localtime' modifier (the daemon's system timezone). The table
-- accrues from feature launch with no historical backfill (D4), so this is
-- the offset in effect now — matching what the tzOffsetMinutes-shifted read
-- rendered before this migration; no visible history change.
UPDATE usage_stats_hourly SET
  local_date = date(bucket_utc, 'localtime'),
  local_hour = CAST(strftime('%H', bucket_utc, 'localtime') AS INTEGER)
WHERE local_date IS NULL;
