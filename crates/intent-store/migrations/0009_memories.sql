-- Long-term agent memory (§9.2, §9.12, §18.5). Adds only the `memories` table;
-- 0001–0008 are untouched. DEFERRED as a wire surface — there is NO `memories.*`
-- RPC in v1; rows are written/read INTERNALLY and surfaced to agents through the
-- agent→BE MCP callback (§6.8) as a context source. `search.memories` (§5.15)
-- reads this table. Ports src/features/memories/main/memories.service.ts.

CREATE TABLE memories (
  id            TEXT PRIMARY KEY,
  workspace_id  TEXT REFERENCES workspace(id) ON DELETE CASCADE,
  content       TEXT NOT NULL,
  tags          TEXT NOT NULL DEFAULT '[]',   -- JSON array
  created_at    TEXT NOT NULL,
  updated_at    TEXT
);
CREATE INDEX idx_memories_ws ON memories(workspace_id);
