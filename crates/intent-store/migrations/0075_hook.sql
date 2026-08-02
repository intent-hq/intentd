-- Background hooks: agent-owned scheduled scripts the daemon runs
-- periodically (fixed `delay_ms` between runs) until one signals a dispatch,
-- fails (evicted), or is cancelled. Persisted so schedules survive a daemon
-- restart — `scheduled`/`running` rows are rehydrated into the scheduler at
-- boot; `dispatched`/`evicted`/`cancelled` are terminal and kept for
-- inspection. Rows cascade with their agent session.

CREATE TABLE hook (
  hook_id      TEXT PRIMARY KEY,
  workspace_id TEXT NOT NULL REFERENCES workspace(id) ON DELETE CASCADE,
  agent_id     TEXT NOT NULL REFERENCES agent_session(id) ON DELETE CASCADE,
  name         TEXT NOT NULL,
  code         TEXT NOT NULL,
  delay_ms     INTEGER NOT NULL,
  state        TEXT NOT NULL CHECK (state IN
                 ('scheduled', 'running', 'dispatched', 'evicted', 'cancelled')),
  created_at   TEXT NOT NULL,
  last_run_at  TEXT,
  next_run_at  TEXT,
  run_count    INTEGER NOT NULL DEFAULT 0,
  last_error   TEXT
);

CREATE INDEX idx_hook_workspace ON hook(workspace_id);
CREATE INDEX idx_hook_agent ON hook(agent_id);
CREATE INDEX idx_hook_state ON hook(state);
