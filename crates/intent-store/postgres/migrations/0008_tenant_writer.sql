-- The tenant writer lock as a row lock. Every Postgres writer transaction
-- locks its tenant's row here (`SELECT ... FOR UPDATE`) before it reads or
-- writes anything else. It replaces a transaction advisory lock, which reads
-- no table and so carries no shard key: on Neki sharded by `tenant_id`, the
-- router could not route it to the tenant's shard, and two writers of one
-- tenant were not guaranteed to exclude each other. A row keyed by
-- `tenant_id` routes to the tenant's shard like every other statement of the
-- transaction.
--
-- No overlap period with the advisory lock: release builds never enable the
-- postgres feature, so no deployed daemon takes the old lock. Once Postgres
-- stores ship, a lock change like this one needs one release that takes
-- both locks.
--
-- The store seeds a tenant's row on its first write (`INSERT ... ON
-- CONFLICT DO NOTHING`, then the lock), so existing tenants need no
-- backfill. Postgres only; no SQLite counterpart (SQLite has one writer per
-- database file).

CREATE TABLE IF NOT EXISTS tenant_writer (
    tenant_id text NOT NULL,
    PRIMARY KEY (tenant_id)
);
