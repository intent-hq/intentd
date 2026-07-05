-- Per-MCP-server OAuth token bags (§18.3, PROTOCOL §5.22 companion). Ports the
-- FE `mcp-oauth-tokens` electron-store: one opaque JSON token bag keyed by MCP
-- server id. Values are secret material (access/refresh tokens); the daemon
-- persists them here so the `mcp.oauth.*` RPC surface can read/write them
-- without the token bag ever crossing the wire (list/get expose presence only).

CREATE TABLE mcp_oauth_tokens (
  server_id  TEXT PRIMARY KEY,
  token_bag  TEXT NOT NULL,                              -- JSON-encoded bag
  updated_at TEXT NOT NULL                               -- ISO-8601 UTC
);
