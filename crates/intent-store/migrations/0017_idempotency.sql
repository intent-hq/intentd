-- Idempotency-key dedupe store (design note TB-0 §5.1). Keyed by
-- (workspace_id, idempotency_key) so a replayed create/commit/PR-merge returns
-- the original serialized result without re-executing or re-emitting events.
-- `workspace_id` uses the `""` sentinel for global methods that carry no
-- workspaceId (e.g. workspace.create). `created_at` is an RFC-3339 string; the
-- `idx_idempotency_created` index backs the ~hourly reaper sweep that deletes
-- rows older than ~24h. Additive only: 0001–0016 are frozen.
CREATE TABLE idempotency_key (
    workspace_id    TEXT NOT NULL,
    idempotency_key TEXT NOT NULL,
    method          TEXT NOT NULL,
    result_json     TEXT NOT NULL,
    created_at      TEXT NOT NULL,
    PRIMARY KEY (workspace_id, idempotency_key)
);
CREATE INDEX idx_idempotency_created ON idempotency_key(created_at);
