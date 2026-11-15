-- Workspace homes, orchestrator side (monorepo docs/protocol/methods/nodes.md,
-- "Moving the home"): the move in progress of each workspace, so it goes on
-- after a restart. While a row exists the workspace's home (`from_home_id`)
-- is `moving`; the row goes with the switch to `home_id`, or with a
-- rollback that gives the old home back `from_state` and `from_since`.
-- `pushed` is set once the old home's last backup landed (step 1).
-- `setup_lease_id` and `setup_seq` name the new home's `setupFinished`
-- record when its setup ended before the switch: the switch claims its
-- `autoStart` with them (as `workspace_home.auto_start_*`). The ids
-- are this daemon's, so the table never rides a workspace transfer.
CREATE TABLE home_move (
    workspace_id TEXT PRIMARY KEY REFERENCES workspace(id) ON DELETE CASCADE,
    home_id TEXT NOT NULL,
    node_id TEXT,
    cloud_machine_id TEXT,
    name TEXT NOT NULL,
    from_home_id TEXT NOT NULL,
    from_state TEXT NOT NULL CHECK (from_state IN ('attached', 'detached')),
    from_since TEXT NOT NULL,
    pushed INTEGER NOT NULL DEFAULT 0 CHECK (pushed IN (0, 1)),
    setup_lease_id TEXT,
    setup_seq INTEGER,
    started_at TEXT NOT NULL
);
CREATE INDEX home_move_node ON home_move(node_id);

-- A stop of a move's target that never became the workspace's home (the
-- move rolled back, or the workspace was archived or deleted mid-move):
-- its last push lands in a sink, so the abandoned checkout never writes
-- `home/head` or `home/wip` over the home's own.
ALTER TABLE home_stop ADD COLUMN discard_push INTEGER NOT NULL DEFAULT 0
    CHECK (discard_push IN (0, 1));
