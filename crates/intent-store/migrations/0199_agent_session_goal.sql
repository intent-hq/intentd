-- User-set conversation goal (PROTOCOL §5.5 "Conversation goal"): 1-1,000
-- characters, NULL when unset. Written only by `/goal` and `agent.setGoal`
-- through the narrow goal writer. A user preference like `pinned`: setting it
-- never advances `updated_at`, and ordinary full-row session writes leave it
-- alone.
ALTER TABLE agent_session ADD COLUMN goal TEXT;
