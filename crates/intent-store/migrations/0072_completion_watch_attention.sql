-- Agent-scoped completion subscriptions (monorepo#1229): a completion watch
-- registered via the explicit `ws.agent.watch(agentId)` MCP tool also wakes
-- the watcher when the watched agent raises an attention request
-- (`agent.reportBlocker` / `agent.requestDiscussion`). Auto-registered
-- watches (delegation, SUB-1 sender watches) keep the default 0.
ALTER TABLE completion_watch ADD COLUMN wake_on_attention INTEGER NOT NULL DEFAULT 0;
