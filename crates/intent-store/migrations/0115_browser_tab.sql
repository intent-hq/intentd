-- Daemon-owned browser tab registry (REV-2, intent-hq/intent#461). One row per
-- logical browser tab of a workspace; the daemon is the source of truth shared
-- by every connected client. `host_client_id` is the logical client (§5.17)
-- whose Electron process owns the live webview and reports navigation; every
-- other client is a viewer. Panel geometry stays client-local and is never
-- stored here. `tab_id` is minted by the host (`tab-<ts>-<rand>`) and unique
-- per daemon.
--
-- `closed_at` is a reconciliation tombstone: a tab closed daemon-side (by a
-- viewer / agent) while its host was offline keeps its row, hidden from every
-- list, until the host acknowledges the close — `browser.syncTabs` answers
-- `drop` for as long as the host still reports the id and purges the row once
-- a snapshot omits it. A host's own `browser.removeTab` deletes the row
-- outright.
CREATE TABLE browser_tab (
  tab_id           TEXT PRIMARY KEY,
  workspace_id     TEXT NOT NULL REFERENCES workspace(id) ON DELETE CASCADE,
  host_client_id   TEXT NOT NULL REFERENCES client(id) ON DELETE CASCADE,
  url              TEXT NOT NULL,
  requested_url    TEXT,
  title            TEXT,
  owner_agent_id   TEXT,
  owner_agent_name TEXT,
  visibility       TEXT NOT NULL DEFAULT 'visible',  -- 'visible' | 'hidden'
  emulated_width   INTEGER,
  emulated_height  INTEGER,
  created_at       TEXT NOT NULL,
  updated_at       TEXT NOT NULL,
  closed_at        TEXT
);
-- `browser.listTabs` hot path: the partial index carries exactly the open
-- rows of a workspace in wire order, so the list is one ordered index range
-- scan — no tombstones visited, no temporary sort.
CREATE INDEX idx_browser_tab_workspace_open
  ON browser_tab(workspace_id, created_at, tab_id) WHERE closed_at IS NULL;
-- Host-scoped reads (`syncTabs` sweep, list-by-host) in the same order;
-- covers tombstones too since the sweep must see them.
CREATE INDEX idx_browser_tab_host ON browser_tab(host_client_id, created_at, tab_id);
