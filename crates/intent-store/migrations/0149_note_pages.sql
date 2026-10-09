-- Read indexes are derived transactionally from canonical note TEXT by the
-- existing writers. No snapshot pins a database transaction across RPCs.
CREATE TABLE note_page_backend (
    singleton INTEGER PRIMARY KEY CHECK (singleton = 1),
    backend_id TEXT NOT NULL,
    token_key BLOB NOT NULL
);
INSERT INTO note_page_backend VALUES (1, lower(hex(randomblob(16))), randomblob(32));
CREATE TABLE note_page_head (
    workspace_id TEXT NOT NULL,
    note_id TEXT NOT NULL,
    instance_id TEXT NOT NULL,
    indexed_rev INTEGER NOT NULL DEFAULT -1,
    profile_revision TEXT NOT NULL DEFAULT '',
    current_rev INTEGER NOT NULL,
    generation TEXT NOT NULL,
    task_count INTEGER NOT NULL DEFAULT 0,
    source_length INTEGER NOT NULL DEFAULT 0,
    source_bytes INTEGER NOT NULL DEFAULT 0,
    scalar_count INTEGER NOT NULL DEFAULT 0,
    lf_count INTEGER NOT NULL DEFAULT 0,
    PRIMARY KEY (workspace_id, note_id),
    FOREIGN KEY (note_id, workspace_id) REFERENCES note(id, workspace_id) ON DELETE CASCADE ON UPDATE CASCADE
);
INSERT INTO note_page_head(workspace_id,note_id,instance_id,current_rev,generation)
    SELECT workspace_id,id,lower(hex(randomblob(16))),rev,lower(hex(randomblob(16))) FROM note;
CREATE TRIGGER note_page_insert AFTER INSERT ON note BEGIN
    INSERT INTO note_page_head(workspace_id,note_id,instance_id,current_rev,generation)
        VALUES (new.workspace_id,new.id,lower(hex(randomblob(16))),new.rev,lower(hex(randomblob(16))));
END;
CREATE TABLE note_page_piece (
    workspace_id TEXT NOT NULL,
    note_id TEXT NOT NULL,
    start INTEGER NOT NULL,
    end INTEGER NOT NULL,
    text TEXT NOT NULL CHECK(length(CAST(text AS BLOB)) <= 4096),
    byte_start INTEGER NOT NULL,
    scalar_start INTEGER NOT NULL,
    lf_start INTEGER NOT NULL,
    PRIMARY KEY (workspace_id,note_id,start),
    FOREIGN KEY (workspace_id,note_id) REFERENCES note_page_head(workspace_id,note_id) ON DELETE CASCADE ON UPDATE CASCADE
);
CREATE TABLE note_page_entry (
    workspace_id TEXT NOT NULL,
    note_id TEXT NOT NULL,
    collection TEXT NOT NULL,
    position INTEGER NOT NULL,
    value TEXT NOT NULL CHECK(length(CAST(value AS BLOB)) <= 32768),
    source_start INTEGER,
    source_end INTEGER,
    PRIMARY KEY(workspace_id,note_id,collection,position),
    FOREIGN KEY(workspace_id,note_id) REFERENCES note_page_head(workspace_id,note_id) ON DELETE CASCADE ON UPDATE CASCADE
);
CREATE INDEX note_page_map_start ON note_page_entry(workspace_id,note_id,collection,source_start,position)
    WHERE source_start IS NOT NULL;
CREATE INDEX note_page_map_end ON note_page_entry(workspace_id,note_id,collection,source_end,position)
    WHERE source_end IS NOT NULL;

CREATE TRIGGER note_page_update AFTER UPDATE ON note BEGIN
    UPDATE note_page_head SET current_rev=new.rev,indexed_rev=-1,generation=lower(hex(randomblob(16)))
        WHERE workspace_id=new.workspace_id AND note_id=new.id;
END;
CREATE INDEX note_page_pending ON note_page_head(workspace_id,note_id) WHERE indexed_rev=-1;
