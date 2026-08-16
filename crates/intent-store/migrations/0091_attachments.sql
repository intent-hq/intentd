-- Attachment registry (PROTOCOL §5.9): one row per
-- file placed by `file.placeAttachment`, keyed by a daemon-minted UUID so
-- agents can retrieve attachments by id (`ws.file.getAttachment`) and clients
-- can resolve metadata (`file.getAttachmentInfo`) without hardcoding paths.
-- `stored_path` is workspace-relative (under `.intent/attachments/`);
-- `mime_type` is the optional client-supplied MIME type. Rows are never
-- updated; the file on disk may be deleted out-of-band (the registry row
-- survives and reads report `exists: false`).
CREATE TABLE attachments (
    id TEXT PRIMARY KEY,
    workspace_id TEXT NOT NULL,
    file_name TEXT NOT NULL,
    mime_type TEXT,
    size INTEGER NOT NULL,
    uploaded_at TEXT NOT NULL,
    stored_path TEXT NOT NULL
);

CREATE INDEX idx_attachments_workspace ON attachments(workspace_id);
