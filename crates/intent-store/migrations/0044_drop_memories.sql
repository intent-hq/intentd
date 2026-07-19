-- Drop memories table and index.
-- The memories feature (§9.2, §9.12, §18.5) had no production usage and is being
-- removed per user decision. This migration drops the storage layer; the corresponding
-- search.memories RPC method is removed in the same commit (now returns -32601).

DROP INDEX IF EXISTS idx_memories_ws;
DROP TABLE IF EXISTS memories;
