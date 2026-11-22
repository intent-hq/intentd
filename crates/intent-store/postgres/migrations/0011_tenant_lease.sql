-- The fenced tenant lease (orchestrator mode). One orchestrator process
-- owns a tenant at a time. Postgres only; no SQLite counterpart (SQLite has
-- one daemon per database file).
--
-- tenant_writer carries the fence: the owner instance and the tenant's
-- epoch, on the row every writer already locks (`SELECT ... FOR UPDATE`).
-- A writer compares them with its process's claim after it takes the lock,
-- so a process that lost the tenant cannot write, even one that waited for
-- the lock behind the takeover (READ COMMITTED re-reads the locked row).
-- NULL owner and epoch 0: no process ever claimed the tenant.
ALTER TABLE tenant_writer ADD COLUMN IF NOT EXISTS owner_instance text;
ALTER TABLE tenant_writer ADD COLUMN IF NOT EXISTS epoch bigint NOT NULL DEFAULT 0;

-- The lease itself: renewed by the owner's heartbeat with a plain UPDATE of
-- this row, outside the writer lock, so a busy tenant cannot starve it.
-- A claim takes the writer lock, then this row's lock, and succeeds only
-- when there is no row or `expires_at` has passed by the database clock;
-- it sets the owner and bumps the epoch here and on tenant_writer in one
-- transaction. One row per tenant, keyed by tenant_id alone (no identity
-- columns), so it routes to the tenant's shard on Neki.
CREATE TABLE IF NOT EXISTS tenant_lease (
    tenant_id text NOT NULL,
    owner_instance text NOT NULL,
    epoch bigint NOT NULL,
    expires_at timestamptz NOT NULL,
    renewed_at timestamptz NOT NULL,
    PRIMARY KEY (tenant_id)
);
