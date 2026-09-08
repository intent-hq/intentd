-- Partial covering index for the unread top-level session derivation (§5.1):
-- the workspace.list batch derivation (`SELECT DISTINCT workspace_id …`), the
-- single-workspace EXISTS probe, and the guarded settle-clear all filter
-- agent_session with UNREAD_TOP_LEVEL_SESSION_PREDICATE. With only
-- idx_agent_parent(parent_agent_id) the candidate rows' metadata JSON must be
-- fetched from the main table B-tree — ~5.6MB of scattered page reads per call
-- on a ~1GB dogfood DB, which cold-cache pushes the workspace.list dispatch
-- past its 1s budget (intent-hq/monorepo#4190). This partial index contains
-- ONLY unread-candidate rows and embeds the seen-marker expression, so all
-- three statements are answered entirely from the index (~86KB, <1ms measured
-- on the same DB). Two subtleties, both load-bearing:
--
-- - The expression MUST be `metadata ->> '$.lastSeenMessageId'`, not
--   json_extract(): json_extract() carries the SQLITE_RESULT_SUBTYPE property,
--   which makes SQLite refuse index-expression substitution, silently
--   degrading every probe back to per-row table fetches. The queries use the
--   same `->>` spelling (values are plain strings/NULL, so semantics match).
-- - The queries name this index via INDEXED BY: sqlite_stat1 estimates make
--   the planner prefer idx_agent_parent otherwise (`parent_agent_id IS NULL`
--   is costed like an equality match at ~4 rows). Same precedent as the
--   idx_agent_parent INDEXED BY uses in agent_repo.rs; a plan-shape test
--   guards the substitution.
CREATE INDEX idx_agent_session_unread_top_level ON agent_session(
    workspace_id,
    last_message_id,
    metadata ->> '$.lastSeenMessageId'
) WHERE parent_agent_id IS NULL
    AND is_background = 0
    AND status <> 'deleted'
    AND last_message_id IS NOT NULL
    AND last_message_role = 'assistant';
