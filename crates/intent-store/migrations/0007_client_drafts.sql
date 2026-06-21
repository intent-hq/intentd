-- Stable client identity + per-client chat drafts (§9.2, §15, §16). Additive:
-- 0001-0006 untouched. Backs the `client.hello` handshake (logical, client-
-- supplied identity that survives reconnects) and the BE-persisted `drafts.*`
-- namespace that replaces the FE's per-client localStorage `chatDrafts`.

-- Logical clients (stable, client-supplied identity; §16). The ephemeral
-- per-connection id (ws-<ts>-<rand>) is transport-only and never stored here.
CREATE TABLE client (
  id            TEXT PRIMARY KEY,                  -- client-persisted UUID (or server-minted)
  name          TEXT,                              -- human label from client.hello
  capabilities  TEXT NOT NULL DEFAULT '{}',        -- JSON capability bag
  first_seen    TEXT NOT NULL,
  last_seen     TEXT NOT NULL
);

-- Per-client chat drafts (§9.10) — replaces FE localStorage chatDrafts.
CREATE TABLE draft (
  workspace_id TEXT NOT NULL REFERENCES workspace(id) ON DELETE CASCADE,
  agent_id     TEXT NOT NULL,                      -- target agent/session
  client_id    TEXT NOT NULL REFERENCES client(id) ON DELETE CASCADE,
  text         TEXT NOT NULL,
  updated_at   TEXT NOT NULL,
  PRIMARY KEY (workspace_id, agent_id, client_id)
);
CREATE INDEX idx_draft_client ON draft(client_id);
