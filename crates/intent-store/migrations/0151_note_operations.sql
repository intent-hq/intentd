-- Durable mutation identity is independent of the live note foreign key: exact
-- retries survive deletion/recreation, but services must reauthorize the original
-- principal and workspace before reading any retained operation data.
CREATE TABLE note_operation (
    operation_key TEXT PRIMARY KEY,
    principal TEXT NOT NULL,
    backend_id TEXT NOT NULL,
    workspace_id TEXT NOT NULL,
    note_id TEXT NOT NULL,
    instance_id TEXT NOT NULL,
    operation_id TEXT NOT NULL,
    payload_digest TEXT NOT NULL,
    admission_expires INTEGER NOT NULL,
    retain_until INTEGER NOT NULL,
    converted_count INTEGER NOT NULL DEFAULT 0 CHECK(converted_count >= 0),
    outcome TEXT NOT NULL CHECK(length(CAST(outcome AS BLOB)) <= 4096),
    UNIQUE(principal,backend_id,workspace_id,note_id,instance_id,operation_id)
);
CREATE INDEX note_operation_expiry ON note_operation(retain_until,operation_key);

CREATE TABLE note_operation_item (
    operation_key TEXT NOT NULL REFERENCES note_operation(operation_key) ON DELETE CASCADE,
    kind TEXT NOT NULL CHECK(kind IN ('mapping','effects','inverse')),
    sequence INTEGER NOT NULL,
    value TEXT NOT NULL CHECK(length(CAST(value AS BLOB)) <= 32768),
    PRIMARY KEY(operation_key,kind,sequence)
);

-- Receipt-owned source for deleted text/inverse pages. Copy source pieces inside
-- the write transaction; no later read hydrates a whole source/version blob.
CREATE TABLE note_operation_source (
    operation_key TEXT NOT NULL REFERENCES note_operation(operation_key) ON DELETE CASCADE,
    phase TEXT NOT NULL CHECK(length(phase) BETWEEN 1 AND 128),
    start INTEGER NOT NULL,
    end INTEGER NOT NULL,
    text TEXT NOT NULL CHECK(length(CAST(text AS BLOB)) <= 4096),
    PRIMARY KEY(operation_key,phase,start)
);

-- Immutable receipt text views address retained scalar-safe source pieces.
CREATE TABLE note_operation_text (
    operation_key TEXT NOT NULL REFERENCES note_operation(operation_key) ON DELETE CASCADE,
    text_id TEXT NOT NULL,
    phase TEXT NOT NULL,
    start INTEGER NOT NULL CHECK(start >= 0),
    end INTEGER NOT NULL CHECK(end >= start),
    length INTEGER NOT NULL CHECK(length = end-start),
    utf8_bytes INTEGER NOT NULL CHECK(utf8_bytes >= 0),
    sha256 TEXT NOT NULL CHECK(length(sha256) = 64),
    PRIMARY KEY(operation_key,text_id)
);
CREATE TABLE note_operation_detail (
    operation_key TEXT NOT NULL REFERENCES note_operation(operation_key) ON DELETE CASCADE,
    reference TEXT NOT NULL,
    sequence INTEGER NOT NULL,
    value TEXT NOT NULL CHECK(length(CAST(value AS BLOB)) <= 32768),
    PRIMARY KEY(operation_key,reference,sequence)
);

CREATE INDEX note_operation_inverse_text ON note_operation_item(operation_key,json_extract(value,'$.replacement.textId')) WHERE kind='inverse';
-- Only references emitted by the committed writer are externally addressable.
CREATE TABLE note_operation_reference (
    operation_key TEXT NOT NULL REFERENCES note_operation(operation_key) ON DELETE CASCADE,
    reference TEXT NOT NULL,
    PRIMARY KEY(operation_key,reference)
);

CREATE TABLE note_operation_scalar (
    operation_key TEXT NOT NULL REFERENCES note_operation(operation_key) ON DELETE CASCADE,
    reference TEXT NOT NULL,
    id TEXT NOT NULL,
    field TEXT NOT NULL,
    phase TEXT NOT NULL,
    length INTEGER NOT NULL CHECK(length>=0),
    PRIMARY KEY(operation_key,reference)
);
