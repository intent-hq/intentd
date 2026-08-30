-- Heavy-payload side table for agent_message (intent-hq/intent#3884). The
-- turn-end assistant persist lands the whole turn as ONE multi-MB
-- agent_message INSERT whose overflow-page chain holds the SQLite write lock
-- for seconds. New writes externalize the big byte carriers out of
-- agent_message.content into this table, one row per (message, block
-- ordinal, kind):
--
--   kind = 'tool_result_output' | 'tool_use_input' | 'thumbnails'
--     tool_result_output / tool_use_input: the block's heavy field value
--       (JSON bytes), keyed by the block's 0-based index in the content
--       array; the content array keeps the block envelope with a reference
--       marker in the field's place.
--     thumbnails: the 0097 per-message thumbnail JSON map (block_ordinal 0);
--       the 0097 agent_message.thumbnails column stays for legacy reads.
--   encoding = 'zstd' | 'none' — codec marker written by the payload codec
--     (src/payload_codec.rs): bodies are zstd-compressed unless small or
--     incompressible, and decompressed transparently on read. Future codecs
--     add markers; readers fail loudly on markers they do not know.
--
-- No backfill: legacy rows keep inline content/thumbnails and the read path
-- serves both layouts. The composite PRIMARY KEY doubles as the needed
-- index — its message_id prefix serves both the read path's
-- all-payloads-for-a-message lookup and the ON DELETE CASCADE scan.

CREATE TABLE agent_message_payload (
  message_id    TEXT    NOT NULL REFERENCES agent_message(id) ON DELETE CASCADE,
  block_ordinal INTEGER NOT NULL,
  kind          TEXT    NOT NULL,
  encoding      TEXT    NOT NULL,
  body          BLOB    NOT NULL,
  PRIMARY KEY (message_id, block_ordinal, kind)
);
