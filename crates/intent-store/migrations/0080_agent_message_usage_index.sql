-- Partial index for the bounded token-usage fallback read (monorepo#1571).
-- `fetch_agent_usage_rows` reads per-message usage metadata for sessions still
-- on the per-message fallback (no snapshot/baseline token report), filtering to
-- usage-bearing messages. The prior indexes on agent_message are
-- UNIQUE(agent_id, seq) (0004) and (agent_id, role, seq DESC) (0064), neither
-- of which indexes the JSON usage paths — so the filter had to load and parse
-- every message body in the session, defeating the bound (a legacy transcript
-- of large no-usage messages was still fully scanned, while the transactional
-- recompute held the write lock).
--
-- The WHERE clause must stay equivalent to MESSAGE_USAGE_PRESENT_SQL in
-- crates/intent-store/src/agent_repo.rs; SQLite only satisfies the query's
-- filter from a partial index when the predicates match.

CREATE INDEX idx_agent_message_usage
  ON agent_message(agent_id, seq)
  WHERE CASE WHEN json_valid(content)
    THEN (json_type(content, '$.usage') IS NOT NULL
        OR json_type(content, '$._meta.usage') IS NOT NULL) ELSE 0 END;
