-- Sandbox table for CoW agent isolation (direct-mode workspaces).
-- Each row represents a CoW-cloned copy of a workspace's repository directory
-- for a specific agent, enabling isolated parallel work.

CREATE TABLE sandbox (
  id                     TEXT PRIMARY KEY,
  workspace_id           TEXT NOT NULL REFERENCES workspace(id) ON DELETE CASCADE,
  agent_id               TEXT NOT NULL REFERENCES agent_session(id) ON DELETE CASCADE,
  path                   TEXT NOT NULL,                 -- <workspaces_root>/<workspaceId>/sandboxes/<agentId>/<repo-slug>
  branch                 TEXT NOT NULL,                 -- sb/<agentId> or shortened form
  base_commit_sha        TEXT NOT NULL,                 -- user's HEAD at provision time
  snapshot_commit_sha    TEXT,                          -- dirty-state snapshot commit (NULL if clean)
  status                 TEXT NOT NULL DEFAULT 'created', -- created|merged|discarded|conflict
  created_at             TEXT NOT NULL,
  updated_at             TEXT NOT NULL,
  UNIQUE(workspace_id, agent_id)                       -- one sandbox per agent per workspace
);

CREATE INDEX idx_sandbox_workspace ON sandbox(workspace_id);
CREATE INDEX idx_sandbox_agent ON sandbox(agent_id);
