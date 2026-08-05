-- Remove the oneShot/persistent distinction from completion watches: every
-- ungrouped watch is now deliver-once (retired at the child's completion), so
-- the `one_shot` mode column and the `deadline_at_ms` leak-guard deadline
-- (which only ever armed the queued-branch non-oneShot `wakeOrCreate` watch)
-- are dead. Drop both columns outright (user decision: no legacy column left
-- behind). SQLite's ALTER TABLE ... DROP COLUMN cannot be relied on across
-- bundled versions, so rebuild the table, preserving all other columns
-- (including the 0072 `wake_on_attention`), both indexes, and existing rows.

CREATE TABLE completion_watch_new (
  id                  TEXT PRIMARY KEY,
  parent_workspace_id TEXT NOT NULL,
  child_workspace_id  TEXT NOT NULL,
  parent_agent_id     TEXT NOT NULL,
  parent_agent_name   TEXT NOT NULL DEFAULT '',
  child_agent_id      TEXT NOT NULL,
  group_id            TEXT,
  report_delivered    INTEGER NOT NULL DEFAULT 0,
  wake_on_attention   INTEGER NOT NULL DEFAULT 0,
  created_at          TEXT NOT NULL
);

INSERT INTO completion_watch_new (
  id, parent_workspace_id, child_workspace_id, parent_agent_id,
  parent_agent_name, child_agent_id, group_id, report_delivered,
  wake_on_attention, created_at
)
SELECT id, parent_workspace_id, child_workspace_id, parent_agent_id,
       parent_agent_name, child_agent_id, group_id, report_delivered,
       wake_on_attention, created_at
FROM completion_watch;

DROP TABLE completion_watch;
ALTER TABLE completion_watch_new RENAME TO completion_watch;

CREATE INDEX idx_completion_watch_parent ON completion_watch(parent_agent_id);
CREATE INDEX idx_completion_watch_child ON completion_watch(child_agent_id);
