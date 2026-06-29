-- Auto-commit linkage for agent sessions (LNI-1, §5.6/§9.1). Adds the
-- `task_note_id` linkage and `skip_auto_commit` opt-out so the daemon-side
-- `agent:idle` auto-commit subscriber can resolve the `Linked-Note-Id:` trailer
-- and honor delegations that asked to skip auto-commit. Both stay NULL/0 for
-- pre-existing rows; populated on `agent.delegate` going forward.
ALTER TABLE agent_session ADD COLUMN task_note_id TEXT;
ALTER TABLE agent_session ADD COLUMN skip_auto_commit INTEGER NOT NULL DEFAULT 0;
