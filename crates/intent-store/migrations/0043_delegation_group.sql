-- Delegation-group persistence for `after_all` fan-in across daemon restarts.
-- In-memory `DelegationGroup` state (agent_subscriptions.rs) is persisted
-- write-through so a restarted daemon can rehydrate undelivered groups and
-- deliver a single aggregated wake to the parent — including summaries from
-- children that completed before the restart.

CREATE TABLE delegation_group (
  group_id            TEXT PRIMARY KEY,
  workspace_id        TEXT NOT NULL REFERENCES workspace(id) ON DELETE CASCADE,
  parent_agent_id     TEXT NOT NULL,
  await_mode          TEXT NOT NULL,
  expected_agent_ids  TEXT NOT NULL,       -- JSON array
  completed_agent_ids TEXT NOT NULL DEFAULT '[]',  -- JSON array
  deleted_agent_ids   TEXT NOT NULL DEFAULT '[]',  -- JSON array
  sealed              INTEGER NOT NULL DEFAULT 0,
  delivered           INTEGER NOT NULL DEFAULT 0,
  event_summaries     TEXT NOT NULL DEFAULT '[]',  -- JSON array of summary strings
  raw_events          TEXT NOT NULL DEFAULT '[]',  -- JSON array of JSON-encoded event frames
  created_at          TEXT NOT NULL,
  updated_at          TEXT NOT NULL
);

CREATE INDEX idx_delegation_group_workspace ON delegation_group(workspace_id);
CREATE INDEX idx_delegation_group_parent ON delegation_group(parent_agent_id);
CREATE INDEX idx_delegation_group_delivered ON delegation_group(delivered);
