CREATE TABLE discussion_conversation (
    id TEXT PRIMARY KEY,
    workspace_id TEXT NOT NULL REFERENCES workspace(id) ON DELETE CASCADE,
    kind TEXT NOT NULL CHECK(kind IN ('direct','group')),
    participants_json TEXT NOT NULL,
    created_at TEXT NOT NULL
);
CREATE INDEX discussion_conversation_workspace ON discussion_conversation(workspace_id,id);
CREATE TABLE discussion_participant (
    conversation_id TEXT NOT NULL REFERENCES discussion_conversation(id) ON DELETE CASCADE,
    workspace_id TEXT NOT NULL REFERENCES workspace(id) ON DELETE CASCADE,
    source_principal_id TEXT NOT NULL,
    -- Authorization is local. Import clears this grant, retaining source attribution.
    principal_id TEXT,
    PRIMARY KEY(conversation_id,source_principal_id)
);
CREATE INDEX discussion_participant_access ON discussion_participant(workspace_id,principal_id,conversation_id);
ALTER TABLE workspace_discussion_message RENAME TO workspace_discussion_message_old;
CREATE TABLE workspace_discussion_message (
    id TEXT PRIMARY KEY,
    workspace_id TEXT NOT NULL REFERENCES workspace(id) ON DELETE CASCADE,
    conversation_id TEXT NOT NULL DEFAULT 'default',
    sequence INTEGER NOT NULL CHECK(sequence > 0),
    principal_id TEXT,
    client_message_id TEXT NOT NULL,
    body TEXT NOT NULL,
    author_json TEXT NOT NULL,
    created_at TEXT NOT NULL,
    UNIQUE(workspace_id,conversation_id,sequence),
    UNIQUE(workspace_id,principal_id,client_message_id)
);
INSERT INTO workspace_discussion_message
    (id,workspace_id,sequence,principal_id,client_message_id,body,author_json,created_at)
    SELECT id,workspace_id,sequence,principal_id,client_message_id,body,author_json,created_at
    FROM workspace_discussion_message_old;
DROP TABLE workspace_discussion_message_old;
