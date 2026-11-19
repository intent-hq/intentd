-- Bounded, resumable workspace and agent deletes. A delete first records
-- itself here in a short writer transaction, then drains the heavy child
-- tables in bounded batches (each its own writer transaction, so other
-- writers of the tenant interleave), and finally deletes the workspace or
-- agent row and this record in one transaction. A record left by a crash or
-- a failed batch is finished by the next store open. `agent_id` is '' for a
-- workspace delete, whose record covers its agents. No foreign keys: the
-- record must outlive its rows until the final transaction removes it.
-- Postgres only; no SQLite counterpart (SQLite drains in bounded batches
-- without a record).

CREATE TABLE IF NOT EXISTS delete_purge (
    tenant_id text NOT NULL,
    kind text NOT NULL CHECK (kind IN ('workspace', 'agent')),
    workspace_id text NOT NULL,
    agent_id text NOT NULL DEFAULT '',
    started_at text NOT NULL,
    PRIMARY KEY (tenant_id, kind, workspace_id, agent_id)
);
