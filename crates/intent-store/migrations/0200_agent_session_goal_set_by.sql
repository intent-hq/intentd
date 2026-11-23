-- Conversation goal attribution (PROTOCOL §5.5 "Conversation goal" ->
-- "Attribution"): who wrote the current goal, as JSON
-- `{ principalId, displayName, login }` (a profile snapshot at write time).
-- NULL when no goal is set, the writer was not a known principal, or the goal
-- predates this column. Written only together with `goal` by the narrow goal
-- writer; full-row session writes leave it alone.
ALTER TABLE agent_session ADD COLUMN goal_set_by TEXT;
