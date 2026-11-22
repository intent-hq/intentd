-- Batch G 2e: a version per agent's persisted queue, so replacing the
-- queue is a compare-and-swap. The in-memory queue is the source of truth
-- and a persist replaces every `agent_queue` row of the agent with a
-- snapshot; without a version, a process holding an older snapshot (a
-- second handle, or a daemon that lost the tenant) could replace a newer
-- queue and lose a queued message. A replace bumps `version` under the
-- tenant writer lock after checking it is still the one its process last
-- loaded or wrote; otherwise it fails `Conflict` and writes nothing.
--
-- Postgres only; no SQLite counterpart (SQLite has one daemon per database
-- file). Keyed by tenant_id first, no identity column. One row per agent
-- that ever had its queue persisted; it goes with the agent's session row.
CREATE TABLE IF NOT EXISTS agent_queue_version (
    tenant_id text NOT NULL,
    agent_id text NOT NULL,
    version bigint NOT NULL,
    -- A fresh id per write, so a process whose commit failed can tell
    -- whether its own write landed.
    write_id text NOT NULL,
    PRIMARY KEY (tenant_id, agent_id),
    FOREIGN KEY (tenant_id, agent_id) REFERENCES agent_session (tenant_id, id) ON DELETE CASCADE
);
