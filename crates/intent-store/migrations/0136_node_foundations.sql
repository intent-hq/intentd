-- Internal foundation only: admission and public APIs are integrated separately.
-- Durable tombstones intentionally outlive agent/workspace deletion.
CREATE TABLE execution_node (
    id TEXT PRIMARY KEY,
    head_id TEXT NOT NULL,
    node_identity TEXT NOT NULL UNIQUE,
    record_json TEXT NOT NULL
);
CREATE TABLE node_lease (
    id TEXT PRIMARY KEY,
    node_id TEXT NOT NULL REFERENCES execution_node(id),
    terminal INTEGER NOT NULL DEFAULT 0 CHECK (terminal IN (0, 1)),
    record_json TEXT NOT NULL,
    ack_seq TEXT NOT NULL DEFAULT '0'
);
CREATE UNIQUE INDEX node_lease_live ON node_lease(node_id) WHERE terminal = 0;
CREATE TABLE node_assignment (
    agent_id TEXT PRIMARY KEY,
    workspace_id TEXT NOT NULL,
    lease_id TEXT NOT NULL REFERENCES node_lease(id),
    record_json TEXT NOT NULL,
    current_checkpoint_id TEXT,
    capture_revision TEXT NOT NULL DEFAULT '0'
);
CREATE INDEX node_assignment_lease ON node_assignment(lease_id);
CREATE TABLE node_checkpoint (
    id TEXT PRIMARY KEY,
    agent_id TEXT NOT NULL REFERENCES node_assignment(agent_id),
    assignment_epoch TEXT NOT NULL,
    capture_revision TEXT NOT NULL,
    record_json TEXT NOT NULL,
    receipt_json TEXT NOT NULL,
    UNIQUE(agent_id, assignment_epoch, capture_revision)
);
-- Cover direct deletion and workspace FK cascades in the same transaction.
CREATE TRIGGER node_assignment_agent_deleted AFTER DELETE ON agent_session
BEGIN
    UPDATE node_assignment
       SET record_json = json_set(record_json, '$.active', json('false'), '$.tombstoned', json('true'))
     WHERE agent_id = OLD.id;
END;
