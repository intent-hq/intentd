-- Once-per-episode advisory-wake marker: records, per (parent, child) pair,
-- that the parent has already received the one advisory wake for the child's
-- CURRENT hook-/PR-monitor-waiting episode (an `agent:idle` deferred only
-- because the child owns active background hooks or PR monitors). While the
-- row stands, subsequent monitoring-idles defer silently (a re-armed watch
-- stays armed for the genuine completion); the row is cleared when a genuine
-- completion/failure/deletion wake delivers, opening the next episode. Rows
-- cascade with either endpoint's agent session.

CREATE TABLE advisory_wake_delivery (
  parent_agent_id TEXT NOT NULL REFERENCES agent_session(id) ON DELETE CASCADE,
  child_agent_id  TEXT NOT NULL REFERENCES agent_session(id) ON DELETE CASCADE,
  delivered_at    TEXT NOT NULL,
  PRIMARY KEY (parent_agent_id, child_agent_id)
);
