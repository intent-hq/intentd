-- Full-text search that keeps stopwords, like SQLite FTS5 (porter unicode61,
-- no stopword list): the 'english' configuration drops "a", "the", "about" at
-- index and query time, so a search for one returned nothing. Each vector is
-- now 'english' (stemmed: "plans" -> "plan") plus 'simple' (every word as
-- written, stopwords included). Built-in configurations only: PlanetScale
-- Neki rejects user-created text-search dictionaries and configurations
-- (router errors 302/303). Postgres only; no SQLite counterpart.

ALTER TABLE agent_message DROP COLUMN search_tsv;
ALTER TABLE agent_message ADD COLUMN search_tsv tsvector GENERATED ALWAYS AS (
    CASE WHEN role IN ('user', 'assistant')
        THEN to_tsvector('english', intent_message_search_text(content))
            || to_tsvector('simple', intent_message_search_text(content)) END
) STORED;
CREATE INDEX agent_message_search_tsv ON agent_message USING gin (search_tsv);

ALTER TABLE note DROP COLUMN search_tsv;
ALTER TABLE note ADD COLUMN search_tsv tsvector GENERATED ALWAYS AS (
    to_tsvector('english',
        coalesce(title, '') || ' ' || coalesce(content, '') || ' ' || coalesce(tags, ''))
    || to_tsvector('simple',
        coalesce(title, '') || ' ' || coalesce(content, '') || ' ' || coalesce(tags, ''))
) STORED;
CREATE INDEX note_search_tsv ON note USING gin (search_tsv);
