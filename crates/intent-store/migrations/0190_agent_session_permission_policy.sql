-- Per-session permission policy override (PROTOCOL §8): `interactive`,
-- `allow`, `auto` or `deny`; NULL follows the daemon default. Set through
-- `agent.update { permissionPolicy }`. A user preference like `pinned`:
-- setting it never advances `updated_at`, and ordinary full-row session
-- writes leave it alone.
ALTER TABLE agent_session ADD COLUMN permission_policy TEXT;
