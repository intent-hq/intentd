-- Per-workspace MCP server disable (§5.22). One row per (workspace, server)
-- pair means "this server is disabled in this workspace"; absence means the
-- workspace tracks the global `mcp.disabledServers` setting. Global disable
-- always wins — this table only narrows an otherwise-enabled server. Rows
-- cascade with their workspace; `server_id` is not FK-constrained because MCP
-- server configs live in the sensitive `mcp.servers` setting, not in SQLite.
CREATE TABLE workspace_mcp_disabled_server (
  workspace_id TEXT NOT NULL REFERENCES workspace(id) ON DELETE CASCADE,
  server_id    TEXT NOT NULL,
  created_at   TEXT NOT NULL,
  PRIMARY KEY (workspace_id, server_id)
);
