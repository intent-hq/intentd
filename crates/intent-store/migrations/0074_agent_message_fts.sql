-- Full-text index over persisted chat transcripts (user/assistant rows only).
-- Contentless FTS5 (content='' + contentless_delete=1, SQLite 3.43+; the
-- bundled build is 3.46) keyed by `agent_message`'s implicit rowid, so no
-- message text is duplicated: matches join back to `agent_message` via rowid
-- for their id/agent_id/content. `porter unicode61` tokenization for
-- natural-language search.
--
-- The indexed text mirrors the search-side `message_text` extraction
-- (intent-services/src/search_ops.rs) so index and preview agree: a bare JSON
-- string is used as-is; an array of content blocks contributes each block's
-- string `text` field joined by single spaces; any other shape falls back to
-- its compact JSON encoding (non-JSON content — impossible from the store's
-- serde-encoded write paths — is indexed raw rather than aborting the write).
-- The same expression lives in `agent_repo::MESSAGE_FTS_TEXT_SQL` for the
-- post-VACUUM rebuild; keep them in sync.
--
-- Sync is trigger-based so every write path maintains the index in the same
-- statement: append (INSERT), the `agent.replaceMessages` swap (DELETE +
-- re-INSERT), `agent.delete`'s ON DELETE CASCADE sweep of `agent_message`
-- (cascade deletes fire the AFTER DELETE trigger), and direct content/role
-- UPDATEs. Only role IN ('user','assistant') rows are indexed.
--
-- NOTE: `agent_message` has a TEXT primary key, so a full VACUUM may renumber
-- its implicit rowids. The only full VACUUM the daemon ever runs is the
-- one-time auto-vacuum activation (`Store::activate_incremental_vacuum`),
-- which rebuilds this index immediately afterwards
-- (`Store::rebuild_agent_message_fts`).

CREATE VIRTUAL TABLE agent_message_fts USING fts5(
  text,
  content='',
  contentless_delete=1,
  tokenize='porter unicode61'
);

CREATE TRIGGER agent_message_fts_after_insert AFTER INSERT ON agent_message
WHEN new.role IN ('user', 'assistant')
BEGIN
  INSERT INTO agent_message_fts(rowid, text)
  VALUES (
    new.rowid,
    CASE
      WHEN json_valid(new.content) = 0 THEN new.content
      WHEN json_type(new.content) = 'text' THEN new.content ->> '$'
      -- Extract each block's `text` through the parent JSON (je.fullkey)
      -- rather than je.value: for non-object array elements (e.g. a bare
      -- string block) json_each's value column is raw SQL text, and feeding
      -- it to a json_* function would abort the whole message write.
      WHEN json_type(new.content) = 'array' THEN COALESCE(
        (SELECT group_concat(new.content ->> (je.fullkey || '.text'), ' ' ORDER BY je.key)
           FROM json_each(new.content) AS je
          WHERE json_type(new.content, je.fullkey || '.text') = 'text'),
        '')
      ELSE json(new.content)
    END
  );
END;

CREATE TRIGGER agent_message_fts_after_delete AFTER DELETE ON agent_message
WHEN old.role IN ('user', 'assistant')
BEGIN
  DELETE FROM agent_message_fts WHERE rowid = old.rowid;
END;

CREATE TRIGGER agent_message_fts_after_update
AFTER UPDATE OF role, content ON agent_message
BEGIN
  DELETE FROM agent_message_fts WHERE rowid = old.rowid;
  INSERT INTO agent_message_fts(rowid, text)
  SELECT
    new.rowid,
    CASE
      WHEN json_valid(new.content) = 0 THEN new.content
      WHEN json_type(new.content) = 'text' THEN new.content ->> '$'
      WHEN json_type(new.content) = 'array' THEN COALESCE(
        (SELECT group_concat(new.content ->> (je.fullkey || '.text'), ' ' ORDER BY je.key)
           FROM json_each(new.content) AS je
          WHERE json_type(new.content, je.fullkey || '.text') = 'text'),
        '')
      ELSE json(new.content)
    END
  WHERE new.role IN ('user', 'assistant');
END;

-- One-shot backfill of pre-existing rows. Runs once inside the migration
-- transaction at open time; measured at ~0.9s per 100k messages (~750B of
-- extracted text each, release build, dev machine), so even a large dogfood
-- transcript log stays well under a noticeable startup delay.
INSERT INTO agent_message_fts(rowid, text)
SELECT
  m.rowid,
  CASE
    WHEN json_valid(m.content) = 0 THEN m.content
    WHEN json_type(m.content) = 'text' THEN m.content ->> '$'
    WHEN json_type(m.content) = 'array' THEN COALESCE(
      (SELECT group_concat(m.content ->> (je.fullkey || '.text'), ' ' ORDER BY je.key)
         FROM json_each(m.content) AS je
        WHERE json_type(m.content, je.fullkey || '.text') = 'text'),
      '')
    ELSE json(m.content)
  END
FROM agent_message m
WHERE m.role IN ('user', 'assistant');
