ALTER TABLE workspace_discussion_message ADD COLUMN reply_to_message_id TEXT;
ALTER TABLE workspace_discussion_message ADD COLUMN reply_to_json TEXT;
ALTER TABLE workspace_discussion_message ADD COLUMN reactions_json TEXT NOT NULL DEFAULT '[]';
ALTER TABLE workspace_discussion_message ADD COLUMN reaction_version INTEGER NOT NULL DEFAULT 0 CHECK (reaction_version >= 0);

CREATE UNIQUE INDEX discussion_message_scope ON workspace_discussion_message(workspace_id,id);

CREATE TABLE discussion_reaction (
    id TEXT PRIMARY KEY,
    workspace_id TEXT NOT NULL REFERENCES workspace(id) ON DELETE CASCADE,
    message_id TEXT NOT NULL,
    -- Cleared on import. A source human identity must never bind to a local one.
    principal_id TEXT,
    reaction TEXT NOT NULL CHECK (reaction IN ('👍','🙂')),
    FOREIGN KEY(workspace_id,message_id) REFERENCES workspace_discussion_message(workspace_id,id) ON DELETE CASCADE,
    UNIQUE(message_id, principal_id, reaction)
);
CREATE INDEX discussion_reaction_workspace ON discussion_reaction(workspace_id);
