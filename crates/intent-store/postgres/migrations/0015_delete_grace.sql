-- Persisted delete grace deadlines (batch G, step 3). A `*.delete` with
-- `undoDelayMs > 0` arms an in-memory timer; on its own that pending
-- delete is lost when the process stops or loses the tenant, and the
-- entity the user deleted survives. Here the deadline is also stored: the
-- process that serves the tenant next re-arms it, and the commit takes the
-- row first (DELETE ... RETURNING), so the delete commits once even when
-- two processes reach it. A cancel deletes the row.
--
-- Postgres (orchestrator mode) only; SQLite (desktop) keeps the documented
-- behaviour that a restart drops pending deletions. Keyed by tenant_id
-- first, no identity column.
CREATE TABLE IF NOT EXISTS delete_grace (
    tenant_id text NOT NULL,
    subject_kind text NOT NULL,
    subject_id text NOT NULL,
    workspace_id text NOT NULL,
    delete_at text NOT NULL,
    created_at text NOT NULL,
    PRIMARY KEY (tenant_id, subject_kind, subject_id),
    CHECK (subject_kind IN ('workspace', 'agent'))
);
