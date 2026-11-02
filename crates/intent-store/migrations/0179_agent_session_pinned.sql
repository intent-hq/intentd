-- Daemon-owned per-session sidebar pin, so every client (desktop, iOS) lists
-- the same agents first. Toggled through `agent.update { pinned }`; served as
-- `AgentLite.pinned`. A user preference, not agent activity: setting it never
-- advances `updated_at`, and ordinary full-row session writes leave it alone.
-- Existing rows default to unpinned.
ALTER TABLE agent_session ADD COLUMN pinned INTEGER NOT NULL DEFAULT 0;
