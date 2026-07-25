-- Per-session end-of-turn token-usage snapshot (§5.23). Additive only: a
-- nullable TEXT column holding the JSON-encoded cumulative
-- `TokenUsageTotals { inputTokens, outputTokens, cacheReadTokens,
-- cacheCreationTokens }` the agent reported at the end of its latest turn
-- (ACP `unstable_end_turn_token_usage`; counts are cumulative per ACP
-- session, so each turn REPLACES the previous snapshot — never summed).
-- NULL means the session has never reported end-of-turn usage.
ALTER TABLE agent_session ADD COLUMN token_usage TEXT;
