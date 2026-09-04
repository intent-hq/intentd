-- Incremental block persistence for the turn transcript
-- (intent-hq/intent#3884 part 2).
--
-- Completed blocks' heavy payloads are now staged into
-- `agent_message_payload` DURING the turn, before the owning
-- `agent_message` envelope row exists (it is inserted once, at turn end,
-- under the message id minted at turn start — CS-0 D1). The 0108 composite
-- FK `(message_id, agent_id) -> agent_message(id, agent_id)` made that
-- impossible (`foreign_keys = ON` checks immediately, and mid-turn staging
-- commits in its own transactions, so deferral cannot help), so the table
-- is rebuilt without it. What the FK provided is re-established explicitly:
--
--   * delete cascade from `agent_message`  -> AFTER DELETE trigger below;
--   * agent_id consistency (a payload row can never name a different
--     session than the message it attaches to) -> the two RAISE(ABORT)
--     guard triggers below, checking whichever side lands second.
--
-- The `agent_id -> agent_session` FK (cascade on session delete) is kept:
-- staged rows always belong to a live session, envelope or not. The new
-- `agent_id` index backs that cascade and the orphan sweep (rows staged by
-- a turn that died without persisting ANY row are deleted at store open;
-- in-process failure paths delete by message id).
--
-- Re-staging (a re-patched block) upserts via ON CONFLICT DO UPDATE, so a
-- stats UPDATE trigger joins the 0108 insert/delete pair to keep the 0103
-- `conversation_bytes` counter balanced.

CREATE TABLE agent_message_payload_new (
  message_id    TEXT NOT NULL,
  agent_id      TEXT NOT NULL REFERENCES agent_session(id) ON DELETE CASCADE,
  block_ordinal INTEGER NOT NULL,               -- index into the content array; -1 = message-level
  kind          TEXT NOT NULL,                  -- tool_use_input | tool_result_output | thumbnails
  encoding      TEXT NOT NULL,                  -- none | zlib
  body          BLOB NOT NULL,                  -- serialized JSON, possibly compressed
  PRIMARY KEY (message_id, block_ordinal, kind)
);

-- Stats triggers are created AFTER the copy so the moved rows are not
-- double-counted in `conversation_bytes`.
INSERT INTO agent_message_payload_new
  SELECT message_id, agent_id, block_ordinal, kind, encoding, body
  FROM agent_message_payload;
DROP TABLE agent_message_payload;
ALTER TABLE agent_message_payload_new RENAME TO agent_message_payload;

-- Backs the `agent_session` delete cascade and the per-agent orphan sweep.
CREATE INDEX idx_agent_message_payload_agent ON agent_message_payload(agent_id);

-- 0103 parity (recreated from 0108, plus the UPDATE leg for upserts).
CREATE TRIGGER agent_session_stats_after_payload_insert
AFTER INSERT ON agent_message_payload
BEGIN
  UPDATE agent_session SET
    conversation_bytes = conversation_bytes + OCTET_LENGTH(new.body)
  WHERE id = new.agent_id;
END;

CREATE TRIGGER agent_session_stats_after_payload_delete
AFTER DELETE ON agent_message_payload
BEGIN
  UPDATE agent_session SET
    conversation_bytes = conversation_bytes - OCTET_LENGTH(old.body)
  WHERE id = old.agent_id;
END;

CREATE TRIGGER agent_session_stats_after_payload_update
AFTER UPDATE OF body ON agent_message_payload
BEGIN
  UPDATE agent_session SET
    conversation_bytes = conversation_bytes
      - OCTET_LENGTH(old.body) + OCTET_LENGTH(new.body)
  WHERE id = new.agent_id;
END;

-- Replaces the dropped composite FK's ON DELETE CASCADE. During a session
-- delete this overlaps the `agent_id` FK cascade — each payload row is
-- still deleted exactly once, so the stats trigger stays balanced.
CREATE TRIGGER agent_message_payload_after_message_delete
AFTER DELETE ON agent_message
BEGIN
  DELETE FROM agent_message_payload WHERE message_id = old.id;
END;

-- Replaces the dropped composite FK's agent_id-consistency guarantee.
-- Payload row landing second (adoption / hydration side):
CREATE TRIGGER agent_message_payload_agent_guard
BEFORE INSERT ON agent_message_payload
WHEN EXISTS (
  SELECT 1 FROM agent_message m
  WHERE m.id = new.message_id AND m.agent_id <> new.agent_id
)
BEGIN
  SELECT RAISE(ABORT, 'agent_message_payload.agent_id does not match the owning agent_message');
END;

-- Envelope landing second (the mid-turn staging order):
CREATE TRIGGER agent_message_agent_guard_for_payloads
BEFORE INSERT ON agent_message
WHEN EXISTS (
  SELECT 1 FROM agent_message_payload p
  WHERE p.message_id = new.id AND p.agent_id <> new.agent_id
)
BEGIN
  SELECT RAISE(ABORT, 'agent_message.agent_id does not match its staged agent_message_payload rows');
END;
