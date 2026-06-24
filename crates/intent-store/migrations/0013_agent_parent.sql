-- Parent-agent linkage for delegated sessions (§9.1). Adds the nullable
-- `parent_agent_id` column to `agent_session` so a delegated agent records the
-- agent that created it (`parentAgentId` in agent-session.ts). The value stays
-- NULL for user-created agents; later tasks populate it on delegation. The
-- index supports "children of agent X" lookups.
ALTER TABLE agent_session ADD COLUMN parent_agent_id TEXT;
CREATE INDEX idx_agent_parent ON agent_session(parent_agent_id);
