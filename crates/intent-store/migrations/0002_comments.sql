-- Comment persistence for the M1 core-CRUD surface (§9.2).
-- Adds only the `comment` table + its indexes. Per §9.2 the `workspace` and
-- `note` tables created by 0001 already match the spec exactly (no new
-- columns), tasks live in `note.task_json` (0001), and threads are derived
-- from `comment.thread_id` — so no `task`/`thread` tables are introduced here.

CREATE TABLE comment (
  id            TEXT PRIMARY KEY,
  thread_id     TEXT NOT NULL,
  note_id       TEXT REFERENCES note(id) ON DELETE CASCADE,
  kind          TEXT NOT NULL,
  content       TEXT NOT NULL,
  author        TEXT NOT NULL,
  author_type   TEXT NOT NULL,
  status        TEXT NOT NULL DEFAULT 'open',
  parent_id     TEXT,
  anchor_json   TEXT NOT NULL,
  anchor_text   TEXT,
  extra_json    TEXT,                             -- suggestion/session-specific fields
  created_at    TEXT NOT NULL,
  updated_at    TEXT NOT NULL
);
CREATE INDEX idx_comment_note   ON comment(note_id);
CREATE INDEX idx_comment_thread ON comment(thread_id);
