-- Initial schema for the thin UDS vertical slice (§9.2).
-- Only the `workspace` and `note` tables are created in this slice; the
-- remaining tables (comment/agent/event/...) land with later waves.

CREATE TABLE workspace (
  id              TEXT PRIMARY KEY,
  title           TEXT NOT NULL,
  branch          TEXT NOT NULL,
  base_ref        TEXT,
  base_commit_sha TEXT,
  status          TEXT NOT NULL DEFAULT 'active',
  status_message  TEXT,
  attention       TEXT NOT NULL DEFAULT 'none', -- dismissible blue-dot state (server-owned; §9.9). activity is derived, not stored.
  repository_owner TEXT,
  repository_name  TEXT,
  worktree_path   TEXT,
  scope           TEXT,
  skip_worktree   INTEGER NOT NULL DEFAULT 0,
  is_remote       INTEGER NOT NULL DEFAULT 0,
  default_model   TEXT,
  pr_number       INTEGER,
  pr_url          TEXT,
  archived        INTEGER NOT NULL DEFAULT 0,
  archived_at     TEXT,
  tags            TEXT NOT NULL DEFAULT '[]',     -- JSON array
  created_at      TEXT NOT NULL,
  updated_at      TEXT NOT NULL,
  last_activity   TEXT
);

CREATE TABLE note (
  id           TEXT PRIMARY KEY,
  workspace_id TEXT NOT NULL REFERENCES workspace(id) ON DELETE CASCADE,
  title        TEXT NOT NULL,
  content      TEXT NOT NULL,
  content_type TEXT NOT NULL DEFAULT 'markdown',
  tags         TEXT NOT NULL DEFAULT '[]',
  is_pinned    INTEGER NOT NULL DEFAULT 0,
  is_archived  INTEGER NOT NULL DEFAULT 0,
  is_default   INTEGER NOT NULL DEFAULT 0,
  parent_id    TEXT REFERENCES note(id) ON DELETE SET NULL,
  visibility   TEXT NOT NULL DEFAULT 'workspace',
  task_json    TEXT,                              -- serialized TaskMetadata or NULL
  created_at   TEXT NOT NULL,
  updated_at   TEXT NOT NULL
);
CREATE INDEX idx_note_workspace ON note(workspace_id);
CREATE INDEX idx_note_parent    ON note(parent_id);
CREATE INDEX idx_note_task      ON note(workspace_id) WHERE task_json IS NOT NULL;
