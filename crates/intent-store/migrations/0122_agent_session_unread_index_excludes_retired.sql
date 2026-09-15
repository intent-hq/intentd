-- Recreate the 0114 partial covering index for the unread top-level session
-- derivation (§5.1) with `retired_at IS NULL` in its WHERE clause: a
-- soft-retired session (agent_session.retired_at set) is hidden from
-- agent.list but still counted by UNREAD_TOP_LEVEL_SESSION_PREDICATE, so a
-- retired top-level agent whose last message is an unseen assistant message
-- kept its workspace unread forever (the FE cannot focus a hidden agent to
-- clear it). The predicate now carries `AND retired_at IS NULL`; the index
-- WHERE must carry the same term so the three consumers' INDEXED BY keeps
-- planning (the query WHERE must imply the index WHERE) and the term stays
-- answered from the index instead of the main table. Index-only DDL: no row
-- is rewritten.
--
-- Everything else is byte-identical to 0114. Two subtleties, both
-- load-bearing:
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
DROP INDEX idx_agent_session_unread_top_level;
CREATE INDEX idx_agent_session_unread_top_level ON agent_session(
    workspace_id,
    last_message_id,
    metadata ->> '$.lastSeenMessageId'
) WHERE parent_agent_id IS NULL
    AND is_background = 0
    AND status <> 'deleted'
    AND last_message_id IS NOT NULL
    AND last_message_role = 'assistant'
    AND retired_at IS NULL;
