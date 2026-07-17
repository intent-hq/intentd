-- Drop memories table and index.
-- The memories feature (§9.2, §9.12, §18.5) was never exposed as a wire surface in v1
-- and had no production usage. This migration removes the storage layer and completes
-- the removal of the feature per user decision.

DROP INDEX IF EXISTS idx_memories_ws;
DROP TABLE IF EXISTS memories;
