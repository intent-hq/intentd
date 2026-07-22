-- Draft attachments (§5.16): opaque FE-authored JSON array serialized as TEXT,
-- NULL when the draft has none. Additive — existing rows read back with no
-- attachments.
ALTER TABLE draft ADD COLUMN attachments TEXT;
