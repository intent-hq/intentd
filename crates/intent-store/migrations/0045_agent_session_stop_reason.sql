-- Add stop_reason column to agent_session (Phase 2 — Daemon-side persistence of agent failure text).
-- Stores the canonical stop/finish reason from the latest terminal stream/status event,
-- surfaced as top-level `stopReason` on both AgentSession and AgentLite serialization.
-- Nullable so existing rows and agents without a stop reason stay NULL.
ALTER TABLE agent_session ADD COLUMN stop_reason TEXT;
