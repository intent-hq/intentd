-- Hook TTL: every hook expires at most 60 minutes after creation. The
-- schedule call persists `expires_at` (= created_at + clamped ttlMs); the
-- scheduler expires the hook (new terminal state `expired`) when the deadline
-- passes and wakes the owner so the model can consciously reschedule.
--
-- The 0075 CHECK constraint on `state` does not include 'expired', so the
-- table is rebuilt (SQLite cannot alter a CHECK in place), preserving all
-- columns (incl. the 0076 last_logs / 0077 last_state additions), rows, and
-- the three indexes. `expires_at` is NULL for pre-TTL rows — the scheduler
-- treats a missing deadline as "no expiry" (legacy rows only; every new hook
-- gets one). No `PRAGMA foreign_keys` toggling (no-op inside the migration
-- transaction): `hook` is a leaf child table, so the DROP violates nothing
-- and every copied row already satisfies the workspace/agent FKs.

CREATE TABLE hook_new (
  hook_id      TEXT PRIMARY KEY,
  workspace_id TEXT NOT NULL REFERENCES workspace(id) ON DELETE CASCADE,
  agent_id     TEXT NOT NULL REFERENCES agent_session(id) ON DELETE CASCADE,
  name         TEXT NOT NULL,
  code         TEXT NOT NULL,
  delay_ms     INTEGER NOT NULL,
  state        TEXT NOT NULL CHECK (state IN
                 ('scheduled', 'running', 'dispatched', 'evicted', 'cancelled',
                  'expired')),
  created_at   TEXT NOT NULL,
  last_run_at  TEXT,
  next_run_at  TEXT,
  run_count    INTEGER NOT NULL DEFAULT 0,
  last_error   TEXT,
  last_logs    TEXT,
  last_state   TEXT,
  expires_at   TEXT
);

INSERT INTO hook_new (hook_id, workspace_id, agent_id, name, code, delay_ms,
                      state, created_at, last_run_at, next_run_at, run_count,
                      last_error, last_logs, last_state, expires_at)
SELECT hook_id, workspace_id, agent_id, name, code, delay_ms,
       state, created_at, last_run_at, next_run_at, run_count,
       last_error, last_logs, last_state, NULL
FROM hook;

DROP TABLE hook;
ALTER TABLE hook_new RENAME TO hook;

CREATE INDEX idx_hook_workspace ON hook(workspace_id);
CREATE INDEX idx_hook_agent ON hook(agent_id);
CREATE INDEX idx_hook_state ON hook(state);
