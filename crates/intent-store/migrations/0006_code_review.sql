-- Code Changes Review state (§9.11, §17). Additive: 0001-0005 untouched. Backs
-- the BE-internal review pipeline (track-change attribution, diff storage, and
-- the metrics aggregator). Raw file content is lazy via git blob SHAs, never
-- inlined here, mirroring the TS `file-tracking-storage.ts` blob-SHA strategy.

-- Per-file agent-change audit trail (§9.11, §17.4). One row per file as it moves
-- through git stages; raw content is lazy via git blob SHAs, not inlined. Written
-- by the INTERNAL file-tracking pipeline (track-change), read over the wire.
CREATE TABLE tracked_changes (
  id            TEXT PRIMARY KEY,
  workspace_id  TEXT NOT NULL REFERENCES workspace(id) ON DELETE CASCADE,
  path          TEXT NOT NULL,                    -- repo-relative file path
  stage         TEXT NOT NULL,                    -- unstaged|staged|committed|pushed|pr|merged
  status        TEXT NOT NULL,                    -- added|modified|deleted|renamed
  agent_id      TEXT,                             -- attribution: which agent wrote it
  session_id    TEXT,                             -- attribution: ACP/session id
  turn          INTEGER,                          -- attribution: conversation turn
  commit_hash   TEXT,                             -- set once committed
  old_blob_sha  TEXT,                             -- lazy content via git blob SHAs
  new_blob_sha  TEXT,
  additions     INTEGER NOT NULL DEFAULT 0,
  deletions     INTEGER NOT NULL DEFAULT 0,
  created_at    TEXT NOT NULL,
  updated_at    TEXT NOT NULL
);
CREATE INDEX idx_tracked_changes_ws     ON tracked_changes(workspace_id);
CREATE INDEX idx_tracked_changes_path   ON tracked_changes(workspace_id, path);
CREATE INDEX idx_tracked_changes_commit ON tracked_changes(commit_hash);
CREATE INDEX idx_tracked_changes_agent  ON tracked_changes(agent_id);

-- Persistent diff storage (§9.11, §17.3), independent of raw git so a change's
-- before/after + hunks survive staging/commit churn. INTERNAL storage only:
-- there are NO `diffs.*` wire methods; diffs surface via file-tracking + events.
CREATE TABLE diffs (
  id            TEXT PRIMARY KEY,
  workspace_id  TEXT NOT NULL REFERENCES workspace(id) ON DELETE CASCADE,
  file_path     TEXT NOT NULL,
  staged        INTEGER NOT NULL DEFAULT 0,
  old_content   TEXT,                             -- nullable for adds/deletes; large blobs lazy via SHAs
  new_content   TEXT,
  hunks_json    TEXT NOT NULL DEFAULT '[]',       -- extracted change hunks
  created_at    TEXT NOT NULL,
  updated_at    TEXT NOT NULL,
  UNIQUE(workspace_id, file_path, staged)
);
CREATE INDEX idx_diffs_ws ON diffs(workspace_id);

-- Aggregated per-workspace change metrics (§9.11, §17.5). Durable. Updated by the
-- INTERNAL metrics aggregator; read via metrics.getWorkspaceStats (no calculate RPC).
CREATE TABLE workspace_metrics (
  workspace_id  TEXT PRIMARY KEY REFERENCES workspace(id) ON DELETE CASCADE,
  additions     INTEGER NOT NULL DEFAULT 0,
  deletions     INTEGER NOT NULL DEFAULT 0,
  files_changed INTEGER NOT NULL DEFAULT 0,
  updated_at    TEXT NOT NULL
);

-- Aggregated per-agent change metrics (§9.11, §17.5). Durable; powers the
-- "by agent" breakdown. Read via metrics.getAgentStats; reset via metrics.clearAgentStats.
CREATE TABLE agent_metrics (
  agent_id      TEXT NOT NULL,
  workspace_id  TEXT NOT NULL REFERENCES workspace(id) ON DELETE CASCADE,
  additions     INTEGER NOT NULL DEFAULT 0,
  deletions     INTEGER NOT NULL DEFAULT 0,
  files_changed INTEGER NOT NULL DEFAULT 0,
  updated_at    TEXT NOT NULL,
  PRIMARY KEY (workspace_id, agent_id)
);
CREATE INDEX idx_agent_metrics_agent ON agent_metrics(agent_id);
