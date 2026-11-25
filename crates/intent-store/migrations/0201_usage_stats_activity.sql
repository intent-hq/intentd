-- Activity counters in the hourly usage-stats buckets behind `stats.getUsage`
-- (PROTOCOL §5.36 additive `toolCalls`, `toolCallsFailed`, `totalRunMs`,
-- `subAgentSessions`). Additive with a 0 default and no backfill: buckets
-- recorded before this migration read back as 0. `total_run_ms` sums
-- completed-turn durations (next to the MAX'd `longest_run_ms`);
-- `sub_agent_sessions_started` is the subset of `sessions_started` created
-- with a parent agent.
ALTER TABLE usage_stats_hourly ADD COLUMN tool_calls INTEGER NOT NULL DEFAULT 0;
ALTER TABLE usage_stats_hourly ADD COLUMN tool_calls_failed INTEGER NOT NULL DEFAULT 0;
ALTER TABLE usage_stats_hourly ADD COLUMN total_run_ms INTEGER NOT NULL DEFAULT 0;
ALTER TABLE usage_stats_hourly ADD COLUMN sub_agent_sessions_started INTEGER NOT NULL DEFAULT 0;
