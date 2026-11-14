-- Workspace homes, orchestrator side (monorepo docs/protocol/node-link.md,
-- "Home final stop"): home stops the machine has not acknowledged yet. A
-- row is written in the same transaction that archives or deletes the
-- workspace or switches its home, and is resent after every bind of
-- `node_id` until its ack, which deletes it; `remoteMachines.remove` deletes
-- the machine's rows. No foreign key to workspace: a deleted workspace's
-- home still gets its stop. The ids are this daemon's, so the table never
-- rides a workspace transfer.
CREATE TABLE home_stop (
    home_id TEXT PRIMARY KEY,
    workspace_id TEXT NOT NULL,
    node_id TEXT NOT NULL,
    stop_id TEXT NOT NULL,
    reason TEXT NOT NULL CHECK (reason IN ('retired', 'moved')),
    orphaned_at TEXT,
    recorded_at TEXT NOT NULL,
    CHECK ((reason = 'moved') = (orphaned_at IS NOT NULL))
);
CREATE INDEX home_stop_node ON home_stop(node_id);
CREATE INDEX home_stop_workspace ON home_stop(workspace_id);
