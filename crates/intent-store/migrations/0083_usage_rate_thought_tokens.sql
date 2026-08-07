-- Reasoning ("thought") tokens in the per-minute token-rate history behind
-- `stats.getRateHistory` (PROTOCOL §5.39 additive `RateSample.thoughtTokens`),
-- mirroring the `thoughtTokens` counter already carried by the usage totals.
-- Additive with a 0 default: buckets recorded before this migration read back
-- as 0, exactly like a minute in which no provider broke reasoning out of
-- `output_tokens`. Additive per bucket like every other counter.
ALTER TABLE usage_rate_minutely ADD COLUMN thought_tokens INTEGER NOT NULL DEFAULT 0;
