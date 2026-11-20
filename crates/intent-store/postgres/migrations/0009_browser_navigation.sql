-- sqlite: 0197_browser_tab_navigation_only.sql
ALTER TABLE browser_tab ADD COLUMN IF NOT EXISTS navigation_only bigint NOT NULL DEFAULT 0;

-- sqlite: 0198_agent_browser_sender.sql
CREATE TABLE IF NOT EXISTS agent_browser_sender (
    tenant_id text NOT NULL,
    agent_id text NOT NULL,
    submission_id text NOT NULL,
    received_order text NOT NULL,
    client_id text,
    PRIMARY KEY (tenant_id, agent_id, submission_id),
    FOREIGN KEY (tenant_id, agent_id) REFERENCES agent_session (tenant_id, id) ON DELETE CASCADE,
    FOREIGN KEY (tenant_id, client_id) REFERENCES client (tenant_id, id) ON DELETE SET NULL (client_id)
);
CREATE INDEX IF NOT EXISTS agent_browser_sender_latest ON agent_browser_sender (tenant_id, agent_id, received_order COLLATE "C" DESC);
