-- Completion-watch persistence: one-shot (and grouped) parent→child
-- completion watches survive daemon restarts. In-memory `CompletionWatch`
-- records (agent_subscriptions.rs) are written durable-before-observable on
-- registration and deleted when the watch fires, is cancelled, or expires,
-- so a restarted daemon can rehydrate still-armed watches and wake the
-- parent when the child completes after the restart.
--
-- No FK to workspace(id): `parent_workspace_id` may be the reserved
-- `__chief__` anchor, which has no workspace row.

CREATE TABLE completion_watch (
  id                  TEXT PRIMARY KEY,
  parent_workspace_id TEXT NOT NULL,
  child_workspace_id  TEXT NOT NULL,
  parent_agent_id     TEXT NOT NULL,
  parent_agent_name   TEXT NOT NULL DEFAULT '',
  child_agent_id      TEXT NOT NULL,
  one_shot            INTEGER NOT NULL DEFAULT 1,
  group_id            TEXT,
  report_delivered    INTEGER NOT NULL DEFAULT 0,
  -- Wall-clock leak-guard deadline (unix epoch ms); NULL = no timed cleanup.
  deadline_at_ms      INTEGER,
  created_at          TEXT NOT NULL
);

CREATE INDEX idx_completion_watch_parent ON completion_watch(parent_agent_id);
CREATE INDEX idx_completion_watch_child ON completion_watch(child_agent_id);
