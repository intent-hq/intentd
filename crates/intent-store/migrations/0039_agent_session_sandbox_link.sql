-- Add sandbox linkage columns to agent_session for CoW containment (§18.2).
-- These track the sandbox associated with a delegated agent, enabling path
-- resolution to prefer the sandbox root over the user's workspace directory.

ALTER TABLE agent_session ADD COLUMN sandbox_id TEXT;
ALTER TABLE agent_session ADD COLUMN sandbox_path TEXT;
ALTER TABLE agent_session ADD COLUMN sandbox_branch TEXT;
