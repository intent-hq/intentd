-- Turn correlation id for queued messages (monorepo#1022). Stable across
-- terminal-failure requeues: a requeued entry gets a NEW `id` but carries the
-- failed turn's ORIGINAL `turn_id`, so retries of the same logical turn share
-- one correlation id. Fresh enqueues set `turn_id = id`. Legacy rows are NULL
-- and default to the row `id` at load time (COALESCE in the repo query).

ALTER TABLE agent_queue ADD COLUMN turn_id TEXT;
