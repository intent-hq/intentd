ALTER TABLE workspace_discussion_message ADD COLUMN agent_id TEXT;
ALTER TABLE workspace_discussion_message ADD COLUMN recipient_agent_ids_json TEXT NOT NULL DEFAULT '[]';
CREATE UNIQUE INDEX discussion_agent_retry ON workspace_discussion_message(workspace_id,agent_id,client_message_id);

-- Local authority, never restored from a workspace archive.
CREATE TABLE discussion_agent_grant (
    workspace_id TEXT NOT NULL REFERENCES workspace(id) ON DELETE CASCADE,
    conversation_id TEXT NOT NULL,
    agent_id TEXT NOT NULL REFERENCES agent_session(id) ON DELETE CASCADE,
    PRIMARY KEY(workspace_id,conversation_id,agent_id)
);
CREATE TABLE discussion_agent_delivery (
    message_id TEXT NOT NULL REFERENCES workspace_discussion_message(id) ON DELETE CASCADE,
    agent_id TEXT NOT NULL REFERENCES agent_session(id) ON DELETE CASCADE,
    workspace_id TEXT NOT NULL REFERENCES workspace(id) ON DELETE CASCADE,
    PRIMARY KEY(message_id,agent_id)
);
