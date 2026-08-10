-- Reasoning ("thought") tokens in the hourly usage-stats buckets behind
-- `stats.getUsage` (PROTOCOL §5.36 additive `thoughtTokens` on totals and the
-- per-cell rollups), mirroring the counter already carried by the per-minute
-- rate history (0083) and the usage totals. Additive with a 0 default:
-- buckets recorded before this migration read back as 0, exactly like an
-- hour in which no provider broke reasoning out of `output_tokens`. Additive
-- per bucket like every other counter.
ALTER TABLE usage_stats_hourly ADD COLUMN thought_tokens INTEGER NOT NULL DEFAULT 0;
