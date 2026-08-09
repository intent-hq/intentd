-- Persisted last-message id on agent_session: the row id of the session's
-- newest user/assistant agent_message row, serving the additive
-- `AgentLite.lastMessageId` wire field (intent-hq/monorepo#1597) so clients
-- can compute per-agent unread against `metadata.lastSeenMessageId` without
-- transcript reads. System (and any other non-user/assistant) rows are
-- transparent — they never change the column. NULL means "no user/assistant
-- message yet" (the field is omitted on the wire). Write paths maintain the
-- column in the same transaction as the message write, next to the 0066
-- preview columns and 0070 `last_message_role`; the backfill below pays the
-- newest-row lookup once for existing rows.

ALTER TABLE agent_session ADD COLUMN last_message_id TEXT;

UPDATE agent_session SET last_message_id = (
  SELECT m.id FROM agent_message m
  WHERE m.agent_id = agent_session.id AND m.role IN ('user', 'assistant')
  ORDER BY m.seq DESC LIMIT 1
);
