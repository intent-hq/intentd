-- Idempotent attachment placement (PROTOCOL §5.9; intent-hq/intent#4691):
-- a client-minted `idempotencyKey` bound to the attachment it placed, so a
-- lost `file.placeAttachment` / `file.attachmentUpload.commit` reply is
-- recoverable (`file.getAttachmentInfo { workspaceId, idempotencyKey }`)
-- and a same-key retry replays the original result instead of placing a
-- second collision-suffixed copy. Scoped per workspace: the same key in
-- another workspace is a different binding. `fingerprint` is the payload
-- identity the key was bound to (`fileName` + size + SHA-256 for the base64
-- and chunked arms; `fileName` + size for the `sourcePath` arm) — a replay
-- presenting a different payload is rejected, never silently replayed.
-- Inserted in the SAME transaction as the `attachments` row; rows expire
-- 7 days after `created_at` (lazy sweep at begin/placement time + boot),
-- while the `attachments` row itself is never touched.
CREATE TABLE attachment_idempotency_keys (
    workspace_id TEXT NOT NULL,
    key TEXT NOT NULL,
    attachment_id TEXT NOT NULL REFERENCES attachments(id),
    fingerprint TEXT NOT NULL,
    created_at TEXT NOT NULL,
    PRIMARY KEY (workspace_id, key)
);

CREATE INDEX idx_attachment_idempotency_keys_created
    ON attachment_idempotency_keys(created_at);
