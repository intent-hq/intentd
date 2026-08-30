-- Hook schedule kinds (cron / exact-time): additive nullable columns. A hook
-- carries exactly ONE schedule kind — `cron` (standard 5-field expression,
-- evaluated in UTC, recurring) or `run_at` (RFC3339 timestamp, fires once) —
-- or neither, which is the legacy fixed `delay_ms` cadence. For the new
-- kinds `delay_ms` is stored as 0. Legacy rows read back with both NULL.

ALTER TABLE hook ADD COLUMN cron TEXT;
ALTER TABLE hook ADD COLUMN run_at TEXT;
