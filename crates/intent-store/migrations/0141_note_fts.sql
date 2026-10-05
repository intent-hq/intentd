-- Derived note search data. An explicit INTEGER PRIMARY KEY supplies stable
-- FTS rowids: unlike note.rowid, search_id cannot be renumbered by VACUUM.
-- Composite identity also keeps each workspace's `spec` independent. The small
-- context table supports ranking/filtering before loading winning note bodies.
CREATE TABLE note_search_ctx (
  search_id    INTEGER PRIMARY KEY,
  note_id      TEXT NOT NULL,
  workspace_id TEXT NOT NULL,
  is_archived  INTEGER NOT NULL,
  updated_at   TEXT NOT NULL,
  UNIQUE (note_id, workspace_id),
  FOREIGN KEY (note_id, workspace_id) REFERENCES note(id, workspace_id)
    ON DELETE CASCADE ON UPDATE CASCADE
);

CREATE VIRTUAL TABLE note_fts USING fts5(
  title, content, tags,
  content='',
  contentless_delete=1,
  tokenize='porter unicode61'
);

CREATE TRIGGER note_fts_after_insert AFTER INSERT ON note
BEGIN
  INSERT INTO note_search_ctx(note_id, workspace_id, is_archived, updated_at)
  VALUES (new.id, new.workspace_id, new.is_archived, new.updated_at);
  INSERT INTO note_fts(rowid, title, content, tags)
  SELECT search_id, new.title, new.content,
         COALESCE((SELECT group_concat(value, ' ') FROM json_each(new.tags) WHERE type = 'text'), '')
  FROM note_search_ctx WHERE note_id = new.id AND workspace_id = new.workspace_id;
END;

-- Cascades include direct note deletion, workspace deletion and import
-- replacement. A key rewrite (stray-spec adoption) preserves search_id via FK.
CREATE TRIGGER note_fts_after_delete AFTER DELETE ON note_search_ctx
BEGIN
  DELETE FROM note_fts WHERE rowid = old.search_id;
END;

CREATE TRIGGER note_search_ctx_after_update
AFTER UPDATE OF is_archived, updated_at ON note
BEGIN
  UPDATE note_search_ctx SET is_archived = new.is_archived, updated_at = new.updated_at
  WHERE note_id = new.id AND workspace_id = new.workspace_id;
END;

CREATE TRIGGER note_fts_after_update
AFTER UPDATE OF title, content, tags ON note
WHEN old.title IS NOT new.title OR old.content IS NOT new.content OR old.tags IS NOT new.tags
BEGIN
  DELETE FROM note_fts WHERE rowid = (
    SELECT search_id FROM note_search_ctx WHERE note_id = new.id AND workspace_id = new.workspace_id
  );
  INSERT INTO note_fts(rowid, title, content, tags)
  SELECT search_id, new.title, new.content,
         COALESCE((SELECT group_concat(value, ' ') FROM json_each(new.tags) WHERE type = 'text'), '')
  FROM note_search_ctx WHERE note_id = new.id AND workspace_id = new.workspace_id;
END;

-- SQLx runs schema creation and this backfill in one migration transaction.
INSERT INTO note_search_ctx(note_id, workspace_id, is_archived, updated_at)
SELECT id, workspace_id, is_archived, updated_at FROM note;

INSERT INTO note_fts(rowid, title, content, tags)
SELECT c.search_id, n.title, n.content,
       COALESCE((SELECT group_concat(value, ' ') FROM json_each(n.tags) WHERE type = 'text'), '')
FROM note n JOIN note_search_ctx c ON c.note_id = n.id AND c.workspace_id = n.workspace_id;
