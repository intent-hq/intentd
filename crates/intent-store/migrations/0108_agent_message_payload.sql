-- Heavy-payload side table for `agent_message` (intent-hq/intent#3884).
--
-- Multi-MB `tool_result.output` / `tool_use.input` bodies used to ride the
-- `agent_message.content` JSON, so the turn-end INSERT wrote (and every
-- transcript read decoded) the whole blob. Bodies whose size exceeds the
-- inline ceiling (`message_payload::PAYLOAD_INLINE_MAX_BYTES`) are now
-- extracted into this table at write time — the content array keeps the
-- block envelope with a slim-projection preview plus `*Truncated`/`*Bytes`
-- flags in the field's position, so a slim page read serves straight from
-- the content column with NO side-table access — and full-fidelity reads
-- splice the body back (stripping the flags) transparently, so wire shapes
-- are unchanged. Small bodies stay inline (no join for tiny blocks). Legacy
-- rows (inline bodies, no side rows) are NOT backfilled and keep reading
-- as-is: hydration is driven purely by side-row presence.
--
-- The 0097 `thumbnails` column stops growing: new write-time thumbnail maps
-- land here as `kind = 'thumbnails'` rows (`block_ordinal` -1 — the map is
-- message-level, keyed internally by image ordinal). Reads fall back to the
-- legacy column for pre-0108 rows.
--
-- `body` holds the field's serialized JSON, zlib-compressed when that is
-- smaller (`encoding = 'zlib'`), raw otherwise (`encoding = 'none'`).
--
-- `agent_id` is denormalized from the owning `agent_message` row so the
-- stats triggers below can resolve the session on cascade deletes: when a
-- message row is deleted its payload rows cascade AFTER the parent row is
-- gone, so a `SELECT agent_id FROM agent_message WHERE id = old.message_id`
-- would find nothing and `conversation_bytes` would drift upward on every
-- `agent.replaceMessages` swap. The composite FK below ties the pair
-- `(message_id, agent_id)` to the SAME `agent_message` row (backed by the
-- unique index on `agent_message(id, agent_id)`), so the database itself
-- rejects a payload row whose denormalized `agent_id` names a different
-- session than the message it attaches to — hydration and the triggers can
-- never disagree about ownership.
--
-- Transfer: rides the archive (see `TRANSFER_TABLES`), scoped through
-- `agent_id` like `agent_message`; BLOB bodies serialize as `$base64`
-- objects in `rows/<table>.jsonl`.

-- Parent key for the composite FK (SQLite requires the referenced column
-- pair to be collectively unique; `id` alone is the PK, so this index is a
-- superset and cheap).
CREATE UNIQUE INDEX idx_agent_message_id_agent ON agent_message(id, agent_id);

CREATE TABLE agent_message_payload (
  message_id    TEXT NOT NULL,
  agent_id      TEXT NOT NULL REFERENCES agent_session(id) ON DELETE CASCADE,
  block_ordinal INTEGER NOT NULL,               -- index into the content array; -1 = message-level
  kind          TEXT NOT NULL,                  -- tool_use_input | tool_result_output | thumbnails
  encoding      TEXT NOT NULL,                  -- none | zlib
  body          BLOB NOT NULL,                  -- serialized JSON, possibly compressed
  PRIMARY KEY (message_id, block_ordinal, kind),
  FOREIGN KEY (message_id, agent_id)
    REFERENCES agent_message(id, agent_id) ON DELETE CASCADE
);

-- 0103 parity: externalized bytes keep counting toward the session's
-- incrementally-maintained `conversation_bytes` — as STORED (possibly
-- compressed) size, matching what the row actually carries. Cascade deletes
-- fire the AFTER DELETE trigger, same as `agent_message`'s own 0103 pair.
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
