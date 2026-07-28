-- Multi-agent attribution (monorepo#957): tracked_changes rows are now keyed
-- logically on (workspace_id, path, stage, agent_id) instead of
-- (workspace_id, path, stage) — one row per agent per file per stage, so a
-- later touch by agent B no longer overwrites agent A's attribution row.
-- No UNIQUE constraint (NULL agent_id rows key separately via IS, and the
-- audit trail keeps history across stages); this index backs the per-agent
-- upsert lookup and the attribution-filtered commit reads.
CREATE INDEX idx_tracked_changes_ws_path_stage_agent
    ON tracked_changes(workspace_id, path, stage, agent_id);
