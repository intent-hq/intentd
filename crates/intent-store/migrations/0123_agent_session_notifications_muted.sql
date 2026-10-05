-- Daemon-owned per-session notification mute flag, so every client (desktop,
-- HUD, iOS) sees the same state. Toggled through
-- `agent.update { notificationsMuted }`; served as `AgentLite.notificationsMuted`
-- and stamped (present only when true) on `agent:idle` /
-- `agent:attention-requested` payloads so notification clients can suppress
-- alerts without a follow-up read. Existing rows default to unmuted.
ALTER TABLE agent_session ADD COLUMN notifications_muted INTEGER NOT NULL DEFAULT 0;
