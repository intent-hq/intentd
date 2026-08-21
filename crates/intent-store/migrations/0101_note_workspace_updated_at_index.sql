-- Covering index for the `max_note_updated_at` aggregate (monorepo#3058):
-- with only idx_note_workspace(workspace_id), `SELECT MAX(updated_at) FROM
-- note WHERE workspace_id = ?` still visits every matching note row (and its
-- overflow pages for large bodies) to read `updated_at`. The composite index
-- answers the aggregate from the index alone — a covering reverse lookup —
-- keeping the hot workspace.list/get enrichment O(1) per workspace.
CREATE INDEX idx_note_workspace_updated_at ON note(workspace_id, updated_at);
