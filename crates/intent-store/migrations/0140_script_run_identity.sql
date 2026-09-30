-- Internal admission identity, not a run ledger or a portable process handle.
ALTER TABLE script ADD COLUMN pending_run_id TEXT;
ALTER TABLE script ADD COLUMN pending_started_at TEXT;
-- Recover commands admitted by an older daemon without inventing idle runs.
UPDATE script SET pending_run_id = lower(hex(randomblob(16)))
WHERE mode = 'command' AND was_running = 1;
