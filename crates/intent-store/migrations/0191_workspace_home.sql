-- Workspace homes on an orchestrator (monorepo docs/protocol/methods/nodes.md,
-- "Workspace homes"): the machine holding each workspace's home checkout.
-- One row per workspace with a home; a regular daemon writes none. The ids
-- are this daemon's (exposed machine, cloud entry), so the table never rides
-- a workspace transfer. `backup_failing_since` is when home backups started
-- failing (cleared on the next success): `backupWarning` after 15 minutes.
CREATE TABLE workspace_home (
    workspace_id TEXT PRIMARY KEY REFERENCES workspace(id) ON DELETE CASCADE,
    home_id TEXT NOT NULL,
    node_id TEXT,
    cloud_machine_id TEXT,
    name TEXT NOT NULL,
    state TEXT NOT NULL CHECK (state IN ('attached', 'detached', 'moving')),
    since TEXT NOT NULL,
    last_backup_at TEXT,
    backup_failing_since TEXT
);
CREATE INDEX workspace_home_node ON workspace_home(node_id);
