-- Centralized PR monitors (`ws.pr.monitor`): an agent subscribes to change
-- wakes on one pull request and the daemon polls it from a single loop.
-- Persisted so monitors survive a daemon restart — `active` rows are
-- rehydrated at boot and poll promptly (a baseline that moved while the
-- daemon was down delivers immediately, without debounce). `completed`
-- (PR merged/closed) is terminal and RETAINED so merged PRs stay visible;
-- `cancelled` is terminal and excluded from list surfaces. Rows cascade with
-- their agent session, so monitors die with their agent.

CREATE TABLE pr_monitor (
  monitor_id      TEXT PRIMARY KEY,
  workspace_id    TEXT NOT NULL REFERENCES workspace(id) ON DELETE CASCADE,
  agent_id        TEXT NOT NULL REFERENCES agent_session(id) ON DELETE CASCADE,
  repo_owner      TEXT NOT NULL,
  repo_name       TEXT NOT NULL,
  pr_number       INTEGER NOT NULL,
  state           TEXT NOT NULL CHECK (state IN ('active', 'completed', 'cancelled')),
  -- JSON `PrMonitorSnapshot` baseline the next poll diffs against.
  last_snapshot   TEXT,
  -- JSON array of consolidated change lines accumulated and awaiting emit.
  pending_changes TEXT,
  -- When the oldest un-emitted change was detected.
  pending_since   TEXT,
  -- When the most recent change was detected (the debounce quiet-window
  -- anchor: the wake fires once the PR has been quiet for the window).
  last_change_at  TEXT,
  last_polled_at  TEXT,
  last_error      TEXT,
  created_at      TEXT NOT NULL,
  updated_at      TEXT NOT NULL
);

CREATE INDEX idx_pr_monitor_workspace ON pr_monitor(workspace_id);
CREATE INDEX idx_pr_monitor_agent ON pr_monitor(agent_id);
CREATE INDEX idx_pr_monitor_state ON pr_monitor(state);

-- One ACTIVE monitor per (agent, repo, PR): re-registering the same triple
-- re-arms the existing row instead of creating a duplicate. Terminal rows are
-- exempt so a merged/cancelled monitor never blocks a fresh registration.
CREATE UNIQUE INDEX idx_pr_monitor_identity
  ON pr_monitor(agent_id, repo_owner, repo_name, pr_number)
  WHERE state = 'active';
