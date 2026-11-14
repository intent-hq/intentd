-- The `setupFinished` record that started a home's `autoStart` scripts
-- (node-link.md § Home runs and setup): its lease (each lease has its own
-- journal) and its journal sequence. `autoStart` runs once per new
-- checkout, so a record replayed after an orchestrator restart (ingested,
-- not acked yet) starts nothing again. Both reset with a new `home_id`.
ALTER TABLE workspace_home ADD COLUMN auto_start_lease_id TEXT;
ALTER TABLE workspace_home ADD COLUMN auto_start_seq INTEGER;
