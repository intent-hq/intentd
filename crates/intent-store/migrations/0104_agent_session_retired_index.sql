-- Partial covering index for the soft-retire read paths (`retiredCount`
-- aggregate + `retiredOnly` rows filter on `agent.list`, §5.5): with only
-- idx_agent_workspace(workspace_id), `SELECT COUNT(*) FROM agent_session
-- WHERE workspace_id = ? AND retired_at IS NOT NULL` still visits every
-- session entry in the workspace to test `retired_at`. The partial index
-- contains ONLY retired sessions, so the count is an index-only range scan
-- over exactly the rows counted — O(retired rows), and O(1) for the common
-- empty-bin workspace — keeping the hot `agent.list` enrichment within the
-- RPC cost contract (same shape as 0101's covering-aggregate precedent).
CREATE INDEX idx_agent_workspace_retired ON agent_session(workspace_id)
    WHERE retired_at IS NOT NULL;
