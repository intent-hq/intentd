-- Specialist linkage for agent sessions (§9.1, PROTOCOL §5.5). Adds the nullable
-- `specialist` column so `agent.create`'s `specialistId` round-trips and surfaces
-- as `metadata.specialist` in the `AgentLite` projection consumed by clients
-- (e.g. the iOS coverflow). Stays NULL for plain (non-specialist) agents.
ALTER TABLE agent_session ADD COLUMN specialist TEXT;
