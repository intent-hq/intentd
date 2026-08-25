-- Soft retire (ws.agent.retire): nullable ISO timestamp marking the session
-- as retired. NULL = active (every existing row). A retired session keeps its
-- full conversation (agent_message rows untouched, still covered by the FTS
-- index) but is INERT: excluded from default `agent.list` reads, unreachable
-- on the agent-facing MCP surface, and nothing may start a turn on it.
-- Cleared by the user/FE-initiated `agent.restore` wire method.

ALTER TABLE agent_session ADD COLUMN retired_at TEXT;
