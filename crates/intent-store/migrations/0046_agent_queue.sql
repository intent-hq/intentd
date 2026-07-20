-- Durable per-agent send queue (write-through persistence of the in-memory
-- `Services.agent_queues` map). Queued messages survive daemon restarts —
-- graceful or crash — and are rehydrated into memory at startup. `payload` is
-- the TEXT-encoded JSON of the internal `QueuedMessage` (content, imageBlocks,
-- fileBlocks, queuedAt, editing, persisted, requeuedAfterFailure,
-- messageMetadata). `position` is the entry's 0-based index in the queue at
-- snapshot time (0 = next to be sent). Rows cascade with their agent session.

CREATE TABLE agent_queue (
  id         TEXT PRIMARY KEY,
  agent_id   TEXT NOT NULL REFERENCES agent_session(id) ON DELETE CASCADE,
  position   INTEGER NOT NULL,
  payload    TEXT NOT NULL,
  created_at TEXT NOT NULL
);

CREATE INDEX idx_agent_queue_agent ON agent_queue(agent_id, position);
