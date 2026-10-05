-- Backend-local staged source roots and append-only chunk storage.
-- Begin pins metadata only. The existing writer retains replaced pieces for
-- pinned generations; this is explicit writer/storage work, not begin/read work.
ALTER TABLE note_page_head ADD COLUMN content_generation TEXT NOT NULL DEFAULT '';
UPDATE note_page_head SET content_generation=lower(hex(randomblob(16)));
ALTER TABLE note_page_piece ADD COLUMN content_generation TEXT NOT NULL DEFAULT '';
UPDATE note_page_piece SET content_generation=(SELECT h.content_generation FROM note_page_head h
    WHERE h.workspace_id=note_page_piece.workspace_id AND h.note_id=note_page_piece.note_id);
CREATE INDEX note_page_piece_generation ON note_page_piece(workspace_id,note_id,content_generation,start);

CREATE TABLE note_stage_root (
    root_key TEXT PRIMARY KEY,
    workspace_id TEXT NOT NULL,
    note_id TEXT NOT NULL,
    instance_id TEXT NOT NULL,
    content_generation TEXT NOT NULL,
    source_length INTEGER NOT NULL CHECK(source_length>=0),
    source_bytes INTEGER NOT NULL CHECK(source_bytes>=0),
    UNIQUE(workspace_id,note_id,instance_id,content_generation)
);
CREATE INDEX note_stage_root_current ON note_stage_root(workspace_id,note_id,content_generation);
CREATE TABLE note_stage (
    operation_key TEXT PRIMARY KEY REFERENCES note_operation(operation_key) ON DELETE CASCADE,
    root_key TEXT NOT NULL REFERENCES note_stage_root(root_key),
    header_digest TEXT NOT NULL CHECK(length(header_digest)=64),
    header TEXT NOT NULL CHECK(length(CAST(header AS BLOB))<=8192),
    base_revision TEXT NOT NULL,
    phase TEXT NOT NULL CHECK(phase IN ('staging','sealed','cancelled','expired','committed')),
    manifest TEXT,
    payload_digest TEXT,
    view_length INTEGER,
    view_id TEXT
);
CREATE INDEX note_stage_root_pins ON note_stage(root_key,phase,operation_key);
CREATE TABLE note_stage_base_piece (
    root_key TEXT NOT NULL REFERENCES note_stage_root(root_key) ON DELETE CASCADE,
    start INTEGER NOT NULL,
    end INTEGER NOT NULL CHECK(end>=start),
    text TEXT NOT NULL CHECK(length(CAST(text AS BLOB))<=4096),
    PRIMARY KEY(root_key,start)
);
CREATE TRIGGER note_stage_preserve_deleted_piece BEFORE DELETE ON note_page_piece BEGIN
    INSERT OR IGNORE INTO note_stage_base_piece(root_key,start,end,text)
    SELECT r.root_key,old.start,old.end,old.text FROM note_stage_root r
    WHERE r.workspace_id=old.workspace_id AND r.note_id=old.note_id
      AND r.content_generation=old.content_generation
      AND EXISTS(SELECT 1 FROM note_stage s JOIN note_operation o USING(operation_key)
                 WHERE s.root_key=r.root_key AND ((s.phase IN ('staging','sealed')
                   AND o.admission_expires>=unixepoch()) OR (s.phase='committed' AND o.retain_until>unixepoch())));
END;
CREATE TRIGGER note_stage_preserve_moved_piece BEFORE UPDATE ON note_page_piece BEGIN
    SELECT CASE WHEN old.text IS NOT new.text OR old.start IS NOT new.start OR old.end IS NOT new.end
        OR old.content_generation IS NOT new.content_generation
        THEN RAISE(ABORT,'source pieces are replaced, never edited in place') END;
    INSERT OR IGNORE INTO note_stage_base_piece(root_key,start,end,text)
    SELECT r.root_key,old.start,old.end,old.text FROM note_stage_root r
    WHERE r.workspace_id=old.workspace_id AND r.note_id=old.note_id
      AND r.content_generation=old.content_generation
      AND EXISTS(SELECT 1 FROM note_stage s JOIN note_operation o USING(operation_key)
                 WHERE s.root_key=r.root_key AND ((s.phase IN ('staging','sealed')
                   AND o.admission_expires>=unixepoch()) OR (s.phase='committed' AND o.retain_until>unixepoch())));
END;
CREATE TABLE note_stage_chunk (
    operation_key TEXT NOT NULL REFERENCES note_stage(operation_key) ON DELETE CASCADE,
    stream TEXT NOT NULL CHECK(stream IN ('text','dirty','selection','mutation','live')),
    sequence INTEGER NOT NULL CHECK(sequence>=0),
    previous_digest TEXT,
    chunk_digest TEXT NOT NULL CHECK(length(chunk_digest)=64),
    record_count INTEGER NOT NULL CHECK(record_count BETWEEN 0 AND 128),
    PRIMARY KEY(operation_key,stream,sequence)
);
CREATE TABLE note_stage_record (
    operation_key TEXT NOT NULL,
    stream TEXT NOT NULL,
    chunk_sequence INTEGER NOT NULL,
    ordinal INTEGER NOT NULL,
    value TEXT NOT NULL CHECK(length(CAST(value AS BLOB))<=65536),
    PRIMARY KEY(operation_key,stream,chunk_sequence,ordinal),
    FOREIGN KEY(operation_key,stream,chunk_sequence) REFERENCES note_stage_chunk(operation_key,stream,sequence) ON DELETE CASCADE
);
ALTER TABLE note_operation ADD COLUMN method_kind TEXT NOT NULL DEFAULT 'inline' CHECK(method_kind IN ('inline','staged'));
CREATE TABLE note_stage_stream (
    operation_key TEXT NOT NULL REFERENCES note_stage(operation_key) ON DELETE CASCADE,
    stream TEXT NOT NULL CHECK(stream IN ('text','dirty','selection','mutation','live')),
    next_sequence INTEGER NOT NULL DEFAULT 0 CHECK(next_sequence>=0),
    last_digest TEXT,
    records INTEGER NOT NULL DEFAULT 0 CHECK(records>=0),
    tail TEXT NOT NULL CHECK(length(CAST(tail AS BLOB))<=1024),
    PRIMARY KEY(operation_key,stream)
);
CREATE TABLE note_stage_text (
    operation_key TEXT NOT NULL REFERENCES note_stage(operation_key) ON DELETE CASCADE,
    text_id TEXT NOT NULL,
    length INTEGER NOT NULL CHECK(length>=0),
    utf8_bytes INTEGER NOT NULL CHECK(utf8_bytes>=0),
    PRIMARY KEY(operation_key,text_id)
);
CREATE TABLE note_stage_text_piece (
    operation_key TEXT NOT NULL,
    text_id TEXT NOT NULL,
    start INTEGER NOT NULL,
    end INTEGER NOT NULL CHECK(end>start),
    text TEXT NOT NULL CHECK(length(CAST(text AS BLOB)) BETWEEN 1 AND 4096),
    PRIMARY KEY(operation_key,text_id,start),
    FOREIGN KEY(operation_key,text_id) REFERENCES note_stage_text(operation_key,text_id) ON DELETE CASCADE
);
