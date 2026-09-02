-- Dense ranking-context side table for `search.messages`
-- (intent-hq/monorepo#4127). The 0074 FTS index is contentless, so the
-- ranking pass had to join `agent_message` (for `agent_id`) and
-- `agent_session` (for `workspace_id`) PER FTS CANDIDATE to evaluate the
-- workspace scope filter and the prefer/archived rank adjustments. Each of
-- those joins is a random page read into a fat table — `agent_message` rows
-- carry multi-KB content blobs — so a broad term over a large corpus paid
-- O(matches) fat-row page reads before the LIMIT and blew the 1s duration
-- budget even after the monorepo#3529 two-phase split kept `content` /
-- `created_at` out of the ranking pass.
--
-- This table gives the ranking pass everything it needs in a dense
-- rowid-keyed table of three short TEXT columns: candidate lookups become
-- small-table PK probes with many rows per page, and the fat tables are
-- only touched by the outer query for the LIMIT rows that survive ranking.
-- Precedent for denormalizing: 0108 denormalizes `agent_id` into
-- `agent_message_payload`. `agent_session.workspace_id` is write-once at
-- the repository layer, so a stored copy can never go stale.
--
-- Scope mirrors the FTS index exactly: only role IN ('user','assistant')
-- rows, keyed by `agent_message`'s implicit rowid, maintained by the same
-- trigger discipline as 0074 (append, replaceMessages swap, session-delete
-- cascade, role/agent_id UPDATEs) — keep both trigger sets in sync. Like
-- the FTS index it is rebuilt after the one-time activation VACUUM
-- (`Store::rebuild_agent_message_fts`), and it is derived state: transfers
-- exclude it (`TRANSFER_EXCLUDED_TABLES`) because the target daemon's
-- insert triggers regenerate it from the imported `agent_message` rows.
--
-- One asymmetry vs the 0074 backfill, which indexes messages
-- unconditionally: ctx rows only materialize when the owning
-- `agent_session` row exists (the INSERT…SELECT joins it for
-- `workspace_id`). Every live path guarantees that — `agent_id` is
-- NOT NULL REFERENCES agent_session(id) with foreign_keys=ON, and both
-- bulk writers insert the session before its messages — but a legacy DB
-- carrying pre-FK-enforcement orphaned messages would leave them
-- FTS-indexed yet unmatchable (the ranking join drops them), which is
-- acceptable: with no session there is no workspace to attribute them to.
--
-- No FK / no secondary indexes on purpose: rowid keys cannot carry an FK
-- (same as the FTS table), lifecycle is fully trigger-owned, and every read
-- probes the PK.

CREATE TABLE agent_message_search_ctx (
  message_rowid INTEGER PRIMARY KEY,  -- agent_message.rowid
  agent_id      TEXT NOT NULL,
  workspace_id  TEXT NOT NULL,
  role          TEXT NOT NULL
);

CREATE TRIGGER agent_message_search_ctx_after_insert AFTER INSERT ON agent_message
WHEN new.role IN ('user', 'assistant')
BEGIN
  INSERT INTO agent_message_search_ctx(message_rowid, agent_id, workspace_id, role)
  SELECT new.rowid, new.agent_id, s.workspace_id, new.role
  FROM agent_session s WHERE s.id = new.agent_id;
END;

CREATE TRIGGER agent_message_search_ctx_after_delete AFTER DELETE ON agent_message
WHEN old.role IN ('user', 'assistant')
BEGIN
  DELETE FROM agent_message_search_ctx WHERE message_rowid = old.rowid;
END;

-- `OF role, agent_id`: 0074's trigger only needs `role, content`, but ctx
-- additionally denormalizes `agent_id` (and, through it, `workspace_id`),
-- so a future re-parenting UPDATE must refresh the row rather than
-- silently stranding the stale agent/workspace.
CREATE TRIGGER agent_message_search_ctx_after_update
AFTER UPDATE OF role, agent_id ON agent_message
BEGIN
  DELETE FROM agent_message_search_ctx WHERE message_rowid = old.rowid;
  INSERT INTO agent_message_search_ctx(message_rowid, agent_id, workspace_id, role)
  SELECT new.rowid, new.agent_id, s.workspace_id, new.role
  FROM agent_session s
  WHERE s.id = new.agent_id AND new.role IN ('user', 'assistant');
END;

-- One-shot backfill of pre-existing rows: a plain indexed join over the
-- rows the FTS index already covers, far cheaper than 0074's text
-- extraction was on the same corpus.
INSERT INTO agent_message_search_ctx(message_rowid, agent_id, workspace_id, role)
SELECT m.rowid, m.agent_id, s.workspace_id, m.role
FROM agent_message m
JOIN agent_session s ON s.id = m.agent_id
WHERE m.role IN ('user', 'assistant');
