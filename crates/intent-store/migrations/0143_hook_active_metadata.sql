-- Agent waiting visibility reads active identity/timing only. Scope indexes
-- containing retired history still scan that history before filtering; these
-- partial indexes bound reads to the scope's active hooks in creation order.
CREATE INDEX idx_hook_active_agent ON hook(agent_id, created_at)
WHERE state IN ('scheduled', 'running');

CREATE INDEX idx_hook_active_workspace ON hook(workspace_id, created_at)
WHERE state IN ('scheduled', 'running');
