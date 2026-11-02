-- Workspace auto-archive opt-out (PROTOCOL §5.1 "Auto-archive opt-out",
-- `Workspace.autoArchiveDisabled`): 1 exempts the workspace from the
-- automatic archive sweep. Off (0) by default. Additive only: 0 for every
-- existing row, so an older daemon ignores it.
ALTER TABLE workspace ADD COLUMN auto_archive_disabled INTEGER NOT NULL DEFAULT 0;
