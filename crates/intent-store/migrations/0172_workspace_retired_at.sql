-- Retired workspace lifecycle state (`workspace.retire`, PROTOCOL §5.1).
--
-- A retired workspace keeps every row (agents, conversations, notes, assets,
-- tasks) but its checkout folder on disk is deleted. The row carries
-- status = 'Retired', stays archived = 1, and records when it was retired.
-- Additive only: NULL for every existing row, so an older daemon ignores it.
ALTER TABLE workspace ADD COLUMN retired_at TEXT;
