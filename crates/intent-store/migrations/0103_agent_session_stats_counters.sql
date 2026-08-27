-- Incrementally-maintained per-session message stats (intent-hq/monorepo#3587).
--
-- The per-workspace stats aggregate (COUNT + SUM(role='assistant') +
-- SUM(OCTET_LENGTH(content)) with a LEFT JOIN over agent_message) scaled with
-- total conversation bytes, not agent count — >3s on a message-heavy dogfood
-- DB, tripping the sqlx slow-statement threshold. Same class as #958/#1010:
-- unbounded-cost work on a hot read path. These counters move the cost to
-- write time (RPC cost contract ladder rung 1: invalidated only by
-- daemon-owned mutations → compute on write, persist, reads select columns).
--
-- Sync is trigger-based, mirroring the FTS index (0074) and for the same
-- reason: every write path maintains the counters in the same statement —
-- append (INSERT), the agent.replaceMessages swap (DELETE + re-INSERT),
-- agent.delete's ON DELETE CASCADE sweep of agent_message (cascade deletes
-- fire the AFTER DELETE trigger before the session row itself goes away),
-- and direct content/role UPDATEs.
--
-- Transfer note: agent_session rows ride the archive with these columns, but
-- the import transform zeroes them — the target re-inserts agent_message rows
-- through these triggers, which rebuild the counters from zero (mirroring the
-- FTS rebuild-on-target approach; see transfer_import.rs).

ALTER TABLE agent_session ADD COLUMN message_count INTEGER NOT NULL DEFAULT 0;
ALTER TABLE agent_session ADD COLUMN assistant_message_count INTEGER NOT NULL DEFAULT 0;
ALTER TABLE agent_session ADD COLUMN conversation_bytes INTEGER NOT NULL DEFAULT 0;

CREATE TRIGGER agent_session_stats_after_message_insert
AFTER INSERT ON agent_message
BEGIN
  UPDATE agent_session SET
    message_count = message_count + 1,
    assistant_message_count = assistant_message_count + (new.role = 'assistant'),
    conversation_bytes = conversation_bytes + OCTET_LENGTH(new.content)
  WHERE id = new.agent_id;
END;

CREATE TRIGGER agent_session_stats_after_message_delete
AFTER DELETE ON agent_message
BEGIN
  UPDATE agent_session SET
    message_count = message_count - 1,
    assistant_message_count = assistant_message_count - (old.role = 'assistant'),
    conversation_bytes = conversation_bytes - OCTET_LENGTH(old.content)
  WHERE id = old.agent_id;
END;

CREATE TRIGGER agent_session_stats_after_message_update
AFTER UPDATE OF role, content ON agent_message
BEGIN
  UPDATE agent_session SET
    assistant_message_count = assistant_message_count
      + (new.role = 'assistant') - (old.role = 'assistant'),
    conversation_bytes = conversation_bytes
      + OCTET_LENGTH(new.content) - OCTET_LENGTH(old.content)
  WHERE id = new.agent_id;
END;

-- One-shot backfill of pre-existing rows: one scan of agent_message grouped
-- by agent. Runs once inside the migration transaction at open time; reads
-- only row headers (OCTET_LENGTH never decodes content or loads overflow
-- pages), so it is bounded by the same one-time cost class as the 0074 FTS
-- backfill.
UPDATE agent_session SET
  message_count = COALESCE(b.n, 0),
  assistant_message_count = COALESCE(b.a, 0),
  conversation_bytes = COALESCE(b.bytes, 0)
FROM (
  SELECT agent_id,
         COUNT(*) AS n,
         SUM(role = 'assistant') AS a,
         SUM(OCTET_LENGTH(content)) AS bytes
  FROM agent_message
  GROUP BY agent_id
) AS b
WHERE agent_session.id = b.agent_id;
