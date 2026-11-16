-- Full-text search: generated tsvector columns replace the SQLite FTS5 tables
-- (note_fts, agent_message_fts) and their *_search_ctx side tables. The
-- 'english' configuration stems like FTS5's porter tokenizer. Ranking differs
-- from bm25, so tests compare match sets, not order.

-- Mirrors MESSAGE_FTS_TEXT_SQL in agent_repo.rs: plain text, a JSON string,
-- the concatenated `.text` fields of a content-block array, or the JSON itself.
CREATE FUNCTION intent_message_search_text(content text) RETURNS text
LANGUAGE sql IMMUTABLE PARALLEL SAFE AS $$
    SELECT CASE
        WHEN NOT sqlite_json_valid(content) THEN content
        WHEN sqlite_json_type(content) = 'text' THEN sqlite_json_extract(content, '$')
        WHEN sqlite_json_type(content) = 'array' THEN COALESCE((
            SELECT string_agg(e.value #>> '{text}', ' ' ORDER BY e.ord)
            FROM jsonb_array_elements(intent_json(content)) WITH ORDINALITY e(value, ord)
            WHERE jsonb_typeof(e.value -> 'text') = 'string'), '')
        ELSE intent_json(content)::text
    END
$$;

ALTER TABLE agent_message ADD COLUMN search_tsv tsvector GENERATED ALWAYS AS (
    CASE WHEN role IN ('user', 'assistant')
        THEN to_tsvector('english', intent_message_search_text(content)) END
) STORED;
CREATE INDEX agent_message_search_tsv ON agent_message USING gin (search_tsv);

ALTER TABLE note ADD COLUMN search_tsv tsvector GENERATED ALWAYS AS (
    to_tsvector('english',
        coalesce(title, '') || ' ' || coalesce(content, '') || ' ' || coalesce(tags, ''))
) STORED;
CREATE INDEX note_search_tsv ON note USING gin (search_tsv);
