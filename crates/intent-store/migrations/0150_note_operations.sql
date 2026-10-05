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
    phase TEXT NOT NULL CHECK(phase IN ('base','final')),
    start INTEGER NOT NULL,
    end INTEGER NOT NULL,
    text TEXT NOT NULL CHECK(length(CAST(text AS BLOB)) <= 4096),
    PRIMARY KEY(operation_key,phase,start)
);
