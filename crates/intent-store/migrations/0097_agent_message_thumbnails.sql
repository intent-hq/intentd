-- Write-time image thumbnails for the slim conversation projection
-- (intent-hq/monorepo -- connection-switch RPC frame mitigation). When a
-- persisted message contains an image block whose base64 `data` exceeds the
-- slim-projection budget, the write path decodes, downscales (longest edge
-- <= 256px), and re-encodes a small thumbnail, stored here as a JSON map
-- keyed by the block's image ordinal (the i-th image block in the message):
-- '{"0": {"data": "<base64>", "mimeType": "image/png"}}'. Slim reads serve
-- the thumbnail in place of the full image data; full reads never select
-- this column. NULL means "no oversized image in this message" (the common
-- case) or a pre-migration row / failed generation -- slim reads then serve
-- the image block with `data` omitted. No backfill: decoding every historic
-- image is prohibitively expensive, and legacy rows degrade gracefully.

ALTER TABLE agent_message ADD COLUMN thumbnails TEXT;
