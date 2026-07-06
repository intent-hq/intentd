-- Session metadata JSON for agent_session (C1d-10a, closes the metadata half of
-- the P2-12a deferral in `agent_ops::agent_create_op`). Persists the widened
-- `agent.create`/`agent.wakeOrCreate` `metadata` payload so children's
-- `agent.wakeOrCreate` can read back the parent's `delegationDepth`, and so
-- the daemon composite can carry `createdByAgentId` / `taskNoteId` /
-- `isBackground` / `source` / `skipAutoCommit` provenance without a follow-up
-- round-trip. Stored as a TEXT-encoded JSON object (NULL for pre-existing
-- rows and for creates that omit `metadata`). `agent_type` stays deferred.
ALTER TABLE agent_session ADD COLUMN metadata TEXT;
