-- Persist `metadata.isBackground` on agent sessions (G-A1 blocker, P3-1.2c):
-- the FE branches production behavior on it (rehydration decisions, list
-- placement, retry/notification handling), so serving a hard-coded `false`
-- would flip every rehydrated background agent to foreground once the FE
-- reads daemon-canonical sessions. Defaults to 0 (foreground) for
-- pre-existing rows, matching the FE repair default.
ALTER TABLE agent_session ADD COLUMN is_background INTEGER NOT NULL DEFAULT 0;
