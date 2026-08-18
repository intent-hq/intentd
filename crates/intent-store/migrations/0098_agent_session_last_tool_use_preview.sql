-- Persisted lastToolUse preview on agent_session: a JSON object describing
-- the LAST tool_use block of the session's newest user/assistant
-- agent_message row -- `{ name, input?, inputTruncated?, inputBytes? }` with
-- `input` bounded by SLIM_PROJECTION_BUDGET_BYTES (2048; the shared
-- derivation is `last_tool_use_preview` in intent-core) -- serving the
-- additive `AgentLite.lastToolUse` wire field and the `agent:last-message`
-- event payload so clients update card previews with zero follow-up RPCs.
-- NULL means "newest user/assistant message carries no tool_use block" (or
-- no such message yet); the wire field is omitted. Write paths maintain the
-- column in the same transaction as the message write, next to the 0066
-- preview columns, 0070 `last_message_role`, and 0088 `last_message_id`.
--
-- The backfill below pays the newest-row lookup once for existing rows. It
-- differs from the Rust write path in ONE bounded way: an over-budget input
-- stores only the truncation flags (no capped `input` preview -- the
-- structure-preserving capping is not expressible cheaply in SQL). Such rows
-- converge to the full capped form the next time a message is appended.

ALTER TABLE agent_session ADD COLUMN last_tool_use_preview TEXT;

UPDATE agent_session SET last_tool_use_preview = (
  SELECT CASE WHEN json_valid(m.content) AND json_type(m.content) = 'array' THEN
    (SELECT CASE
        WHEN b.value -> '$.input' IS NULL THEN
          json_object('name', COALESCE(b.value ->> '$.name', ''))
        WHEN length(b.value -> '$.input') <= 2048 THEN
          json_object('name', COALESCE(b.value ->> '$.name', ''),
                      'input', json(b.value -> '$.input'))
        ELSE
          json_object('name', COALESCE(b.value ->> '$.name', ''),
                      'inputTruncated', json('true'),
                      'inputBytes', length(b.value -> '$.input'))
      END
     FROM json_each(m.content) b
     WHERE b.value ->> '$.type' = 'tool_use'
     ORDER BY b.key DESC LIMIT 1)
  ELSE NULL END
  FROM agent_message m
  WHERE m.agent_id = agent_session.id AND m.role IN ('user', 'assistant')
  ORDER BY m.seq DESC LIMIT 1
);
