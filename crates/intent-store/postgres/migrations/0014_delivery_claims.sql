-- Batch G 2c: a delivery claim on completion watches and delegation
-- groups, so one completion wakes the parent once even when two processes
-- see it (two delivery loops, or a daemon still delivering while another
-- rehydrated the watch). A process claims the row before it sends the
-- terminal wake and skips when another holds it. The claim names the
-- tenant's epoch (`tenant_writer.epoch`, read under the writer lock) and
-- the claiming process (a token per process): it is stale only when a
-- newer epoch exists (a takeover), never by elapsed time, so a slow but
-- live holder keeps it. A retired watch or settled group drops its row and
-- with it the claim; a failed delivery releases the claim for its retry.
--
-- Postgres only; no SQLite counterpart (SQLite has one daemon per database
-- file). NULL: unclaimed.
ALTER TABLE completion_watch ADD COLUMN IF NOT EXISTS claim_epoch bigint;
ALTER TABLE completion_watch ADD COLUMN IF NOT EXISTS claim_token text;
ALTER TABLE delegation_group ADD COLUMN IF NOT EXISTS claim_epoch bigint;
ALTER TABLE delegation_group ADD COLUMN IF NOT EXISTS claim_token text;
