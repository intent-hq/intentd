-- Durable per-agent zero-output stop-redelivery payload
-- (intent-hq/monorepo#1899): write-through mirror of the in-memory
-- `AgentManager.stop_redelivery` map armed by intent-hq/monorepo#1757, so a
-- daemon restart between the stop and the follow-up turn no longer loses the
-- stopped message's prompt-only redelivery. `payload` is the TEXT-encoded
-- JSON of the internal `QueuedPrepend` (content, imageBlocks, fileBlocks).
-- At most one payload per agent; rows cascade with their agent session.

CREATE TABLE agent_stop_redelivery (
  agent_id   TEXT PRIMARY KEY REFERENCES agent_session(id) ON DELETE CASCADE,
  payload    TEXT NOT NULL,
  created_at TEXT NOT NULL
);
