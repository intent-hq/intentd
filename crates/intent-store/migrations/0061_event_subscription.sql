-- Event-subscription persistence (monorepo#937): the deprecated-alias
-- `event.subscribe` / `agent.subscribe` service subscriptions survive daemon
-- restarts. In-memory `EventSubscription` records (event_subscriptions.rs)
-- are persisted via a best-effort async write-through on registration (NOT
-- durable-before-observable — a crash in the milliseconds between in-memory
-- registration and commit loses the row; the subscriber can re-subscribe)
-- and deleted on `event.unsubscribe` or when the subscriber agent is
-- deleted, so a restarted daemon can rehydrate still-armed subscriptions
-- and keep waking the subscriber on matching events.
--
-- Subscriptions registered by callers with no agent identity (FE front
-- door) have no wake target and are kept in-memory only — they are never
-- written here, so `subscriber_agent_id` is NOT NULL.
--
-- No FK to workspace(id): `workspace_id` may be the reserved `__chief__`
-- anchor, which has no workspace row.

CREATE TABLE event_subscription (
  id                  TEXT PRIMARY KEY,
  workspace_id        TEXT NOT NULL,
  subscriber_agent_id TEXT NOT NULL,
  -- JSON array of resolved event-type patterns (e.g. ["agent:*","file:changed"]).
  event_types         TEXT NOT NULL,
  exclude_self        INTEGER NOT NULL DEFAULT 1,
  -- Batch window in milliseconds (default 500).
  batch_window_ms     INTEGER NOT NULL DEFAULT 500,
  created_at          TEXT NOT NULL
);

CREATE INDEX idx_event_subscription_agent ON event_subscription(subscriber_agent_id);
