-- Separate metadata preserves legacy private history and its local grants.
-- Tombstones prevent the implicit default discussion from reappearing after deletion.
CREATE TABLE workspace_discussion_conversation (
    workspace_id TEXT NOT NULL REFERENCES workspace(id) ON DELETE CASCADE,
    id TEXT NOT NULL,
    name TEXT,
    first_message_id TEXT,
    first_principal_id TEXT,
    first_client_message_id TEXT,
    created_at TEXT NOT NULL,
    deleted_at TEXT,
    PRIMARY KEY(workspace_id,id)
);
CREATE UNIQUE INDEX workspace_discussion_creation_retry ON workspace_discussion_conversation(workspace_id,first_principal_id,first_client_message_id);
