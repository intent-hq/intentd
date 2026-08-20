-- Agent-flipped completions: one row per (agent, workspace, task note)
-- recording that the agent transitioned that OTHER task note into `complete`
-- (previous status ≠ complete, new status = complete) via a status write
-- carrying a `caller_agent_id`. Wake composition later attributes these as
-- unblocked-hint triggers when the agent settles. The agent's own linked task
-- note is never recorded (its completion is already stamped as a wake trigger
-- separately), a transition back out of `complete` deletes the pair for every
-- recording agent, and the set is capped per agent at write time (oldest
-- evicted). Rows cascade with the recording agent's session.

CREATE TABLE agent_flipped_completion (
  agent_id     TEXT NOT NULL REFERENCES agent_session(id) ON DELETE CASCADE,
  workspace_id TEXT NOT NULL,
  task_note_id TEXT NOT NULL,
  recorded_at  TEXT NOT NULL,
  PRIMARY KEY (agent_id, workspace_id, task_note_id)
);

-- The un-complete removal path deletes by task, across agents.
CREATE INDEX idx_agent_flipped_completion_task
  ON agent_flipped_completion(workspace_id, task_note_id);
