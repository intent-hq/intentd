-- Persisted last-message role on agent_session: the role ('user' /
-- 'assistant') of the session's newest user/assistant agent_message row,
-- serving the additive `AgentLite.lastMessageRole` wire field. System (and
-- any other non-user/assistant) rows are transparent — they never change the
-- column. NULL means "no user/assistant message yet" (the field is omitted
-- on the wire). Write paths maintain the column in the same transaction as
-- the message write, next to the 0066 preview columns; the backfill below
-- pays the newest-row lookup once for existing rows.

ALTER TABLE agent_session ADD COLUMN last_message_role TEXT;

UPDATE agent_session SET last_message_role = (
  SELECT m.role FROM agent_message m
  WHERE m.agent_id = agent_session.id AND m.role IN ('user', 'assistant')
  ORDER BY m.seq DESC LIMIT 1
);
