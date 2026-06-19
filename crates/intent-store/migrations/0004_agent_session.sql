-- Agent runtime sessions + append-only conversation log (§9.1 / §9.2). Adds
-- only the `agent_session` and `agent_message` tables (+ index); 0001-0003 are
-- untouched. `name_explicitly_set` backs the §9.1 `AgentSession` field
-- (`nameExplicitlySet` in agent-session.ts). `acp_session_id` and `provider`
-- are write-once at the repository layer (§9.5 invariants); the message log is
-- insert-only with a monotonic per-agent `seq` enforced by UNIQUE(agent_id, seq).

CREATE TABLE agent_session (
  id                  TEXT PRIMARY KEY,
  workspace_id        TEXT NOT NULL REFERENCES workspace(id) ON DELETE CASCADE,
  backend_session_id  TEXT,
  acp_session_id      TEXT,
  name                TEXT NOT NULL,
  name_explicitly_set INTEGER NOT NULL DEFAULT 0,
  model               TEXT,
  provider            TEXT,
  status              TEXT NOT NULL,
  is_active           INTEGER NOT NULL DEFAULT 0,
  system_prompt       TEXT,
  created_at          TEXT NOT NULL,
  updated_at          TEXT NOT NULL
);
CREATE INDEX idx_agent_workspace ON agent_session(workspace_id);

-- Append-only conversation log (one row per message; never updated)
CREATE TABLE agent_message (
  id         TEXT PRIMARY KEY,
  agent_id   TEXT NOT NULL REFERENCES agent_session(id) ON DELETE CASCADE,
  seq        INTEGER NOT NULL,                    -- monotonic per agent
  role       TEXT NOT NULL,                       -- user|assistant|tool|system
  content    TEXT NOT NULL,                       -- JSON content blocks
  created_at TEXT NOT NULL,
  UNIQUE(agent_id, seq)
);
