-- Batch G 2b: reserve an idempotency key before the operation runs. The
-- row used to be written only after the operation (ON CONFLICT DO
-- NOTHING), so a retry that reached another process while the first still
-- ran executed the operation twice (for example two agents). Now a keyed
-- operation inserts a pending row first (`result_json` NULL); a caller that
-- finds it pending waits, and one that finds a result replays it. A failed
-- operation deletes its pending row so a retry may run it.
--
-- A reservation names the tenant's epoch (`tenant_writer.epoch`, read
-- under the writer lock) and the reserving process (a token per process).
-- It is stale only when a newer epoch exists (a takeover), never by
-- elapsed time, so a slow but live holder keeps it. A process's first
-- unleased open of the tenant drops other processes' pending rows (an
-- unleased tenant has one process by design). Rows from before this
-- migration all carry a result.
--
-- Postgres only; no SQLite counterpart (SQLite has one daemon per database
-- file, and its in-process key lock orders callers).
ALTER TABLE idempotency_key ALTER COLUMN result_json DROP NOT NULL;
ALTER TABLE idempotency_key ADD COLUMN IF NOT EXISTS claim_epoch bigint;
ALTER TABLE idempotency_key ADD COLUMN IF NOT EXISTS claim_token text;
-- A pending row names its reservation; a completed one names none.
ALTER TABLE idempotency_key DROP CONSTRAINT IF EXISTS idempotency_key_pending_check;
ALTER TABLE idempotency_key ADD CONSTRAINT idempotency_key_pending_check
    CHECK ((result_json IS NULL) = (claim_token IS NOT NULL));
