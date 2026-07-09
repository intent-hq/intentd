-- Note version history (PROTOCOL §5.2 version-history extensions).
-- Full-snapshot model: one row per captured version carrying the complete
-- note content at that version (deliberate divergence from the FE's
-- snapshot/diff `.versions.jsonl` — documented in PROTOCOL.md §5.2). `v` is
-- 1-based and strictly increasing per note; append prunes to the newest 50
-- (the FE's `VERSION_CONFIG.MAX_VERSIONS`).

CREATE TABLE note_version (
  note_id      TEXT NOT NULL REFERENCES note(id) ON DELETE CASCADE,
  v            INTEGER NOT NULL,
  date         TEXT NOT NULL,
  author_id    TEXT NOT NULL,
  author_name  TEXT NOT NULL,
  author_type  TEXT NOT NULL,                     -- 'user' | 'agent' | 'system'
  title        TEXT NOT NULL,
  content      TEXT NOT NULL,
  PRIMARY KEY (note_id, v)
);
