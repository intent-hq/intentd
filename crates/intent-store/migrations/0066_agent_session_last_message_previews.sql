-- Persisted last-message previews on agent_session (follow-up to monorepo#1010)
-- `last_assistant_preview` / `last_user_preview` mirror the projection built by
-- `projection_text_blocks_expr()` in agent_repo.rs -- a JSON array of the capped
-- `text`-block strings of the session's newest assistant / user message -- so
-- `agent.list` / `agent.get` previews no longer re-project each session's newest
-- message content on every read. NULL means "no such message yet"; a winner
-- whose content is not a valid JSON array stores '[]' -- the projection form
-- (zero text blocks) -- so such sessions never need the read-path self-heal.
-- Write paths maintain the columns in the same transaction as the message
-- write; the backfill below pays the projection cost once for existing rows
-- using the same expression (assistant blocks keep their TAIL, user blocks
-- their HEAD, 4096-char per-block cap -- PROJECTION_TEXT_BLOCK_CAP).

ALTER TABLE agent_session ADD COLUMN last_assistant_preview TEXT;
ALTER TABLE agent_session ADD COLUMN last_user_preview TEXT;

UPDATE agent_session SET last_assistant_preview = (
  SELECT CASE WHEN json_valid(m.content) AND json_type(m.content) = 'array' THEN
      (SELECT json_group_array(t ORDER BY k) FROM (
          SELECT b.key AS k, CASE WHEN b.type = 'object'
                  AND json_extract(b.value, '$.type') = 'text'
                  AND json_type(b.value, '$.text') = 'text'
              THEN substr(json_extract(b.value, '$.text'), -4096)
              END AS t
          FROM json_each(m.content) b)
      WHERE t IS NOT NULL)
  ELSE '[]' END
  FROM agent_message m
  WHERE m.agent_id = agent_session.id AND m.role = 'assistant'
  ORDER BY m.seq DESC LIMIT 1
);

UPDATE agent_session SET last_user_preview = (
  SELECT CASE WHEN json_valid(m.content) AND json_type(m.content) = 'array' THEN
      (SELECT json_group_array(t ORDER BY k) FROM (
          SELECT b.key AS k, CASE WHEN b.type = 'object'
                  AND json_extract(b.value, '$.type') = 'text'
                  AND json_type(b.value, '$.text') = 'text'
              THEN substr(json_extract(b.value, '$.text'), 1, 4096)
              END AS t
          FROM json_each(m.content) b)
      WHERE t IS NOT NULL)
  ELSE '[]' END
  FROM agent_message m
  WHERE m.agent_id = agent_session.id AND m.role = 'user'
  ORDER BY m.seq DESC LIMIT 1
);
