-- Session-discovered reasoning-effort levels (PROTOCOL §5.5, Option C).
-- Additive nullable TEXT column: the JSON string array of effort levels the
-- provider's `thought_level` config option advertised at the most recent
-- session open, minus the adapter's `"default"` sentinel. Replaced wholesale
-- at every session open; NULL = the provider advertised no such option.
ALTER TABLE agent_session ADD COLUMN effort_levels TEXT;
