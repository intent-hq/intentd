-- Workspace git roots (multi git root tracking, intent-hq/monorepo#2053):
-- secondary local git repositories tracked for a workspace — registered
-- explicitly by agents (`source = 'agent'`) or auto-detected from the
-- worktree's git submodules (`source = 'auto'`). The daemon runs the same
-- background PR discovery on each root as on the primary workspace root, so
-- the PR columns mirror the `workspace` PR columns: `pr_status` stores the
-- PascalCase `PullRequestStatus` wire word, `pull_requests` the serialized
-- `Vec<PullRequestInfo>` JSON (NULL = never populated, '[]' = explicitly
-- none). `path` is the canonicalized absolute root path (may live anywhere
-- on the host); registration is idempotent by (workspace_id, path) — a
-- second agent registering the same path is appended to the
-- `registered_by_agent_ids` JSON array instead of creating a new row.
-- Rows cascade with their workspace.

CREATE TABLE workspace_git_root (
  id                      TEXT PRIMARY KEY,
  workspace_id            TEXT NOT NULL REFERENCES workspace(id) ON DELETE CASCADE,
  path                    TEXT NOT NULL,
  source                  TEXT NOT NULL CHECK (source IN ('agent', 'auto')),
  repo_owner              TEXT,
  repo_name               TEXT,
  registered_by_agent_ids TEXT NOT NULL DEFAULT '[]', -- JSON array of agent ids
  pr_number               INTEGER,
  pr_url                  TEXT,
  pr_status               TEXT,
  pull_requests           TEXT,
  created_at              TEXT NOT NULL,
  updated_at              TEXT NOT NULL
);

CREATE INDEX idx_workspace_git_root_workspace ON workspace_git_root(workspace_id);

-- Registration is idempotent by canonical path within a workspace.
CREATE UNIQUE INDEX idx_workspace_git_root_identity
  ON workspace_git_root(workspace_id, path);
