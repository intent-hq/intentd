-- A cloud home reports `starting` while its sandbox starts, resumes or is
-- rebuilt (monorepo docs/protocol/methods/nodes.md, "Cloud homes"). SQLite
-- cannot change a CHECK in place, so `workspace_home` and `home_move` (whose
-- `from_state` a move gives back) are rebuilt with every column, row and
-- index kept. No table references either one.

CREATE TABLE workspace_home_new (
    workspace_id TEXT PRIMARY KEY REFERENCES workspace(id) ON DELETE CASCADE,
    home_id TEXT NOT NULL,
    node_id TEXT,
    cloud_machine_id TEXT,
    name TEXT NOT NULL,
    state TEXT NOT NULL CHECK (state IN ('attached', 'detached', 'moving', 'starting')),
    since TEXT NOT NULL,
    last_backup_at TEXT,
    backup_failing_since TEXT,
    auto_start_lease_id TEXT,
    auto_start_seq INTEGER
);
INSERT INTO workspace_home_new (workspace_id, home_id, node_id, cloud_machine_id, name,
    state, since, last_backup_at, backup_failing_since, auto_start_lease_id, auto_start_seq)
SELECT workspace_id, home_id, node_id, cloud_machine_id, name, state, since,
    last_backup_at, backup_failing_since, auto_start_lease_id, auto_start_seq
FROM workspace_home;
DROP TABLE workspace_home;
ALTER TABLE workspace_home_new RENAME TO workspace_home;
CREATE INDEX workspace_home_node ON workspace_home(node_id);

CREATE TABLE home_move_new (
    workspace_id TEXT PRIMARY KEY REFERENCES workspace(id) ON DELETE CASCADE,
    home_id TEXT NOT NULL,
    node_id TEXT,
    cloud_machine_id TEXT,
    name TEXT NOT NULL,
    from_home_id TEXT NOT NULL,
    from_state TEXT NOT NULL CHECK (from_state IN ('attached', 'detached', 'starting')),
    from_since TEXT NOT NULL,
    pushed INTEGER NOT NULL DEFAULT 0 CHECK (pushed IN (0, 1)),
    setup_lease_id TEXT,
    setup_seq INTEGER,
    started_at TEXT NOT NULL
);
INSERT INTO home_move_new (workspace_id, home_id, node_id, cloud_machine_id, name,
    from_home_id, from_state, from_since, pushed, setup_lease_id, setup_seq, started_at)
SELECT workspace_id, home_id, node_id, cloud_machine_id, name, from_home_id, from_state,
    from_since, pushed, setup_lease_id, setup_seq, started_at
FROM home_move;
DROP TABLE home_move;
ALTER TABLE home_move_new RENAME TO home_move;
CREATE INDEX home_move_node ON home_move(node_id);
