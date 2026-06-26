-- Add a monotonic `rev` version counter to the shared `note` row, covering both
-- Note and Task wire payloads (tasks are notes carrying `task_json`). Additive
-- only: 0001–0015 are frozen, so this migration just appends the column. Existing
-- rows default `0`; note-writing store methods bump `rev = rev + 1` on update, so
-- the first write moves an existing row to `1`. Backs the §8.3/§8.4 wire `rev`
-- and (later) optimistic-concurrency `expectedVersion`.
ALTER TABLE note ADD COLUMN rev INTEGER NOT NULL DEFAULT 0;
