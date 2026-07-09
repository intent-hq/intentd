-- Per-workspace scoping for well-known note ids. The 0001 `note` table used a
-- global `id TEXT PRIMARY KEY`, which meant well-known ids like the workspace
-- spec (`id = "spec"`) could only exist in a single workspace and note-id
-- collisions across workspaces were impossible to represent. This migration
-- widens the primary key to `(id, workspace_id)` so every workspace can carry
-- its own spec, and generalises the invariant to all user-created notes.
--
-- Two ripples fall out of the composite key: `note.parent_id` and
-- `comment.note_id` can no longer FK a plain `note(id)`. Composite FKs are used
-- where representable; `parent_id` swaps its old `ON DELETE SET NULL` FK for a
-- trigger that clears children scoped to the same workspace (a composite
-- `ON DELETE SET NULL` would try to null the NOT NULL `workspace_id` column).
-- `comment` gains a `workspace_id` column plus a composite FK to `note`, so the
-- workspace-scoped cascade is preserved.

PRAGMA foreign_keys = OFF;

-- Rebuild `note` with the composite PK. The column order and defaults match
-- 0001+0016 (rev was appended in 0016) so re-encoding logic in `note_repo`
-- stays identical. The parent_id FK is dropped; a trigger below emulates the
-- previous ON DELETE SET NULL semantic scoped to the same workspace.
CREATE TABLE note_new (
  id           TEXT NOT NULL,
  workspace_id TEXT NOT NULL REFERENCES workspace(id) ON DELETE CASCADE,
  title        TEXT NOT NULL,
  content      TEXT NOT NULL,
  content_type TEXT NOT NULL DEFAULT 'markdown',
  tags         TEXT NOT NULL DEFAULT '[]',
  is_pinned    INTEGER NOT NULL DEFAULT 0,
  is_archived  INTEGER NOT NULL DEFAULT 0,
  is_default   INTEGER NOT NULL DEFAULT 0,
  parent_id    TEXT,
  visibility   TEXT NOT NULL DEFAULT 'workspace',
  task_json    TEXT,
  created_at   TEXT NOT NULL,
  updated_at   TEXT NOT NULL,
  rev          INTEGER NOT NULL DEFAULT 0,
  PRIMARY KEY (id, workspace_id)
);

INSERT INTO note_new (id, workspace_id, title, content, content_type, tags,
  is_pinned, is_archived, is_default, parent_id, visibility, task_json,
  created_at, updated_at, rev)
SELECT id, workspace_id, title, content, content_type, tags, is_pinned,
  is_archived, is_default, parent_id, visibility, task_json, created_at,
  updated_at, rev
FROM note;

DROP TABLE note;
ALTER TABLE note_new RENAME TO note;

CREATE INDEX idx_note_workspace ON note(workspace_id);
CREATE INDEX idx_note_parent    ON note(parent_id);
CREATE INDEX idx_note_task      ON note(workspace_id) WHERE task_json IS NOT NULL;

-- Emulate `parent_id ... ON DELETE SET NULL` scoped to the same workspace: a
-- composite `ON DELETE SET NULL` on `(parent_id, workspace_id)` would try to
-- null the NOT NULL `workspace_id` column.
CREATE TRIGGER note_parent_set_null_on_delete
AFTER DELETE ON note
BEGIN
  UPDATE note SET parent_id = NULL
  WHERE parent_id = OLD.id AND workspace_id = OLD.workspace_id;
END;

-- Rebuild `comment` with a `workspace_id` column plus a composite FK to
-- `note(id, workspace_id)`. Existing rows are backfilled via the join to
-- `note`; orphan comments (`note_id` NULL) or comments whose `note_id` no
-- longer resolves are dropped. Both are best-effort — orphan comments have no
-- caller path in intent-services and no such rows are expected in an
-- installed database at this point in the port.
CREATE TABLE comment_new (
  id            TEXT PRIMARY KEY,
  thread_id     TEXT NOT NULL,
  note_id       TEXT,
  workspace_id  TEXT NOT NULL,
  kind          TEXT NOT NULL,
  content       TEXT NOT NULL,
  author        TEXT NOT NULL,
  author_type   TEXT NOT NULL,
  status        TEXT NOT NULL DEFAULT 'open',
  parent_id     TEXT,
  anchor_json   TEXT NOT NULL,
  anchor_text   TEXT,
  extra_json    TEXT,
  created_at    TEXT NOT NULL,
  updated_at    TEXT NOT NULL,
  FOREIGN KEY (note_id, workspace_id) REFERENCES note(id, workspace_id) ON DELETE CASCADE
);

INSERT INTO comment_new (id, thread_id, note_id, workspace_id, kind, content,
  author, author_type, status, parent_id, anchor_json, anchor_text, extra_json,
  created_at, updated_at)
SELECT c.id, c.thread_id, c.note_id, n.workspace_id, c.kind, c.content,
  c.author, c.author_type, c.status, c.parent_id, c.anchor_json, c.anchor_text,
  c.extra_json, c.created_at, c.updated_at
FROM comment c JOIN note n ON n.id = c.note_id;

DROP TABLE comment;
ALTER TABLE comment_new RENAME TO comment;

CREATE INDEX idx_comment_note   ON comment(note_id, workspace_id);
CREATE INDEX idx_comment_thread ON comment(thread_id);

PRAGMA foreign_keys = ON;
