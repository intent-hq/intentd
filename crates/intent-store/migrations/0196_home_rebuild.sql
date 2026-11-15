-- Cloud homes, orchestrator side (monorepo docs/protocol/methods/nodes.md,
-- "Cloud homes" 5): a cloud home whose sandbox was deleted gets a new homeId
-- with no sandbox, in the same transaction that writes this row. Until that
-- home opens, the row says to open it from the hub's home/head and home/wip
-- and to post a notice once it does. A row whose home_id is no longer the
-- workspace's home means nothing. The ids are this daemon's, so the table
-- never rides a workspace transfer.
CREATE TABLE home_rebuild (
    workspace_id TEXT PRIMARY KEY REFERENCES workspace(id) ON DELETE CASCADE,
    home_id TEXT NOT NULL,
    from_home_id TEXT NOT NULL,
    recorded_at TEXT NOT NULL
);
