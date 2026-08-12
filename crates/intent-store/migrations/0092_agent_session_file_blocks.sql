-- Session-level `fileBlocks` captured at spawn (PROTOCOL §5.5),
-- mirroring the existing `image_blocks` column
-- (migration 0022): an opaque JSON array stored as TEXT, NULL for
-- pre-existing rows and for creates that omit the field.
ALTER TABLE agent_session ADD COLUMN file_blocks TEXT;
