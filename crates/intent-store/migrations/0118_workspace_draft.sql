-- Durable, daemon-owned workspace drafts. Draft writes use `revision` for
-- optimistic concurrency and `operation_key` for exactly-once promotion.
CREATE TABLE workspace_draft (
    id TEXT PRIMARY KEY NOT NULL,
    owner_client_id TEXT NOT NULL,
    revision INTEGER NOT NULL DEFAULT 0 CHECK (revision >= 0),
    phase TEXT NOT NULL DEFAULT 'editing'
        CHECK (phase IN ('editing', 'promoting', 'promoted', 'failed')),
    title TEXT,
    intent_text TEXT NOT NULL DEFAULT '',
    source JSON,
    context_links JSON NOT NULL DEFAULT '[]',
    attachments JSON NOT NULL DEFAULT '[]',
    config JSON NOT NULL DEFAULT '{}',
    operation_key TEXT NOT NULL UNIQUE,
    promoted_workspace_id TEXT,
    initial_agent_id TEXT,
    delivery JSON NOT NULL DEFAULT '{"state":"none"}',
    last_error TEXT,
    created_at TEXT NOT NULL,
    updated_at TEXT NOT NULL
);

CREATE INDEX idx_workspace_draft_phase ON workspace_draft(phase);

-- Setup is externally observed state, materialized on write for cheap
-- workspace.list/workspace.get reads.
ALTER TABLE workspace ADD COLUMN setup_result JSON;