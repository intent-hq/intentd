-- A receipt is written only for an accepted wire user submission. Keeping its
-- submission id makes reconnect retries idempotent even after queue delivery.
CREATE TABLE agent_browser_sender (
    agent_id TEXT NOT NULL REFERENCES agent_session(id) ON DELETE CASCADE,
    submission_id TEXT NOT NULL,
    received_order TEXT NOT NULL,
    client_id TEXT REFERENCES client(id) ON DELETE SET NULL,
    PRIMARY KEY (agent_id, submission_id)
);
CREATE INDEX agent_browser_sender_latest ON agent_browser_sender(agent_id, received_order DESC);
