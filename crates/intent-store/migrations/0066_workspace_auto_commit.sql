-- Persisted per-workspace auto-commit override (intent-hq/monorepo spec
-- Diagnosis §3b): mirrors the global `git.autoCommit` at workspace-create
-- time and can be toggled per workspace afterwards. NULL for pre-migration
-- rows — resolved against the global setting at read time (no backfill).
ALTER TABLE workspace ADD COLUMN auto_commit_enabled INTEGER;
