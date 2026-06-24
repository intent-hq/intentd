-- Persist the workspace `path` and add `repository_path` (parity with the TS
-- `Workspace` shape). Additive only: 0001–0010 are frozen, so this migration
-- just appends two nullable TEXT columns. `path` was previously accepted on the
-- wire but never persisted (read back as NULL); `repository_path` is the TS
-- `repositoryPath` field, used as the worktree fallback. Both NULL means the
-- field is omitted from the wire.

ALTER TABLE workspace ADD COLUMN path TEXT;
ALTER TABLE workspace ADD COLUMN repository_path TEXT;
