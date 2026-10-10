-- Prepared private native-artifact bookkeeping. No method/capability is enabled
-- by this migration. Jobs deliberately do not cascade with note deletion: their
-- physical storage and cleanup charges must outlive revoked source authority.
CREATE TABLE note_artifact_source (
    workspace_id TEXT NOT NULL,
    note_id TEXT NOT NULL,
    native_collection TEXT NOT NULL,
    source_collection TEXT NOT NULL,
    native_position INTEGER NOT NULL DEFAULT 0 CHECK (native_position=0),
    source_position INTEGER NOT NULL DEFAULT 0 CHECK (source_position=0),
    primitive TEXT NOT NULL CHECK (primitive IN ('diff','mermaid')),
    PRIMARY KEY (workspace_id,note_id,native_collection),
    FOREIGN KEY (workspace_id,note_id) REFERENCES note_page_head(workspace_id,note_id)
        ON DELETE CASCADE ON UPDATE CASCADE,
    FOREIGN KEY (workspace_id,note_id,native_collection,native_position)
        REFERENCES note_page_entry(workspace_id,note_id,collection,position) ON DELETE CASCADE,
    FOREIGN KEY (workspace_id,note_id,source_collection,source_position)
        REFERENCES note_page_entry(workspace_id,note_id,collection,position) ON DELETE CASCADE
);
