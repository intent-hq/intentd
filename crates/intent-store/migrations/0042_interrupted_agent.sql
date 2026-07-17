-- Interrupted agent sessions persistence (INT-41, agent-resumption phase 1).
-- When intentd restarts, in-flight agent sessions (Active/Processing/Waiting) are
-- logged here before the heal sweep rewrites them to idle. The FE can then prompt
-- the user to resume or abandon them. Rows with `resolution='pending'` survive
-- further restarts (idempotent insert); resolved rows are kept for audit.

CREATE TABLE interrupted_agent (
  agent_id       TEXT PRIMARY KEY,
  workspace_id   TEXT NOT NULL,
  prev_status    TEXT NOT NULL,
  interrupted_at TEXT NOT NULL,
  resolution     TEXT NOT NULL DEFAULT 'pending', -- pending|resumed|abandoned
  resolved_at    TEXT
);

CREATE INDEX idx_interrupted_workspace ON interrupted_agent(workspace_id);
CREATE INDEX idx_interrupted_resolution ON interrupted_agent(resolution);
