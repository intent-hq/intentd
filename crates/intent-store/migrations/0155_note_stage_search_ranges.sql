-- Derived source-search selections. Uploaded records and digests are unchanged.
-- The operation owns these rows; no view FK so bounded view retirement may
-- precede the derived-index cleanup phase without cascading an unbounded set.
CREATE TABLE note_stage_search_input (
 operation_key TEXT NOT NULL REFERENCES note_stage(operation_key) ON DELETE CASCADE,
 generation INTEGER NOT NULL CHECK(generation BETWEEN 0 AND 9007199254740991),
 start INTEGER NOT NULL CHECK(start BETWEEN 0 AND 9007199254740991),
 end INTEGER NOT NULL CHECK(end>start AND end<=9007199254740991),
 ordinal INTEGER NOT NULL CHECK(ordinal BETWEEN 0 AND 9007199254740991),
 PRIMARY KEY(operation_key,generation,start,end,ordinal)
);
CREATE TABLE note_stage_search_range (
 operation_key TEXT NOT NULL REFERENCES note_stage(operation_key) ON DELETE CASCADE,
 generation INTEGER NOT NULL CHECK(generation BETWEEN 0 AND 9007199254740991),
 start INTEGER NOT NULL CHECK(start BETWEEN 0 AND 9007199254740991),
 end INTEGER NOT NULL CHECK(end>start AND end<=9007199254740991),
 PRIMARY KEY(operation_key,generation,start)
);
