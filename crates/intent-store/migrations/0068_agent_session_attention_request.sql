-- Pending attention request raised by ws.agent.requestDiscussion /
-- ws.agent.reportBlocker (agent attention-requests feature).
-- `attention_request_kind` is "discussion" or "blocker"; `..._reason` is the
-- agent-supplied text; `..._timestamp` is the ISO time it was raised. All
-- three are NULL when no request is pending — the daemon clears them when the
-- agent next receives a message, so the FE indicator retires naturally.

ALTER TABLE agent_session ADD COLUMN attention_request_kind TEXT;
ALTER TABLE agent_session ADD COLUMN attention_request_reason TEXT;
ALTER TABLE agent_session ADD COLUMN attention_request_timestamp TEXT;
