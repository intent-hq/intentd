-- Bound the NO ACTION FK probe when deleting attachment registry rows, and
-- seek only the imported attachment's retry keys during orphan reclamation.
CREATE INDEX idx_attachment_idempotency_keys_attachment
  ON attachment_idempotency_keys(attachment_id, workspace_id);
