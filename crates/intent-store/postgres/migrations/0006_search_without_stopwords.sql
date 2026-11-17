-- Full-text search without a stopword list, like SQLite FTS5 (porter
-- unicode61): the 'english' configuration drops stopwords ("a", "the",
-- "about") at index and query time, so a search for one returned nothing.
-- `intent_search` is 'english' with an English snowball stemmer that keeps
-- every word. Postgres only; no SQLite counterpart.

CREATE TEXT SEARCH DICTIONARY intent_english_stem (TEMPLATE = snowball, Language = english);

CREATE TEXT SEARCH CONFIGURATION intent_search (COPY = english);
ALTER TEXT SEARCH CONFIGURATION intent_search
    ALTER MAPPING FOR asciiword, asciihword, hword_asciipart, word, hword, hword_part
    WITH intent_english_stem;

-- Re-create the generated columns (and their GIN indexes) with the new
-- configuration; the expressions are otherwise those of 0004_search.sql.
ALTER TABLE agent_message DROP COLUMN search_tsv;
ALTER TABLE agent_message ADD COLUMN search_tsv tsvector GENERATED ALWAYS AS (
    CASE WHEN role IN ('user', 'assistant')
        THEN to_tsvector('intent_search', intent_message_search_text(content)) END
) STORED;
CREATE INDEX agent_message_search_tsv ON agent_message USING gin (search_tsv);

ALTER TABLE note DROP COLUMN search_tsv;
ALTER TABLE note ADD COLUMN search_tsv tsvector GENERATED ALWAYS AS (
    to_tsvector('intent_search',
        coalesce(title, '') || ' ' || coalesce(content, '') || ' ' || coalesce(tags, ''))
) STORED;
CREATE INDEX note_search_tsv ON note USING gin (search_tsv);
