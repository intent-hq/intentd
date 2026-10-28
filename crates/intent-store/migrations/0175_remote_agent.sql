-- Simplified remote launch: which agents run on the configured execution
-- node, and under which lease. Head claims these agents when it binds; the
-- node stops any running agent its head does not claim. Node and lease
-- records (including the lease's link generation and journal ack) live in
-- the existing execution_node / node_lease tables.
CREATE TABLE remote_agent (
    agent_id TEXT PRIMARY KEY REFERENCES agent_session(id) ON DELETE CASCADE,
    workspace_id TEXT NOT NULL,
    node_id TEXT NOT NULL REFERENCES execution_node(id),
    lease_id TEXT NOT NULL REFERENCES node_lease(id),
    created_at TEXT NOT NULL
);
CREATE INDEX remote_agent_lease ON remote_agent(lease_id);
