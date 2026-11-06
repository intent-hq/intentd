CREATE TABLE workspace_discussion_message (
    id TEXT PRIMARY KEY,
    workspace_id TEXT NOT NULL REFERENCES workspace(id) ON DELETE CASCADE,
    sequence INTEGER NOT NULL CHECK (sequence > 0),
    -- Local retry identity, cleared on import; historic identity is in author_json.
    principal_id TEXT,
    client_message_id TEXT NOT NULL,
    body TEXT NOT NULL,
    author_json TEXT NOT NULL,
    created_at TEXT NOT NULL,
    UNIQUE (workspace_id, sequence),
    UNIQUE (workspace_id, principal_id, client_message_id)
);
