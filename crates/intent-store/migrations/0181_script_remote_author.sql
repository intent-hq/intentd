-- The remote agent that created a saved script: `{ "agentId", "nodeId" }`
-- as JSON, set by the backend when a remote agent creates the script and
-- kept by every upsert. Served as `Script.remoteAuthor`; it limits that
-- agent's definition changes to its own scripts. Existing rows were created
-- by the user or local agents (NULL).
ALTER TABLE script ADD COLUMN remote_author TEXT;
