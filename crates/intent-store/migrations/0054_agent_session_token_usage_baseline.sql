-- Per-session token-usage baseline (§5.23, monorepo#737). Additive only: a
-- nullable TEXT column holding the JSON-encoded `TokenUsageTotals` accumulated
-- from snapshots of PRIOR ACP sessions of the same agent. When the
-- resume-impossible fallback recreates the ACP session (`replace_acp_session_id`
-- swaps the stored `acp_session_id`), the current cumulative `token_usage`
-- snapshot is folded into this baseline (component-wise sum, NULL treated as
-- zero) and the snapshot is cleared, so the fresh session's cumulative counts
-- do not erase usage already paid for under the old id. NULL means no ACP
-- session of this agent has ever been replaced while holding a snapshot.
ALTER TABLE agent_session ADD COLUMN token_usage_baseline TEXT;
