-- Per-workspace chat context items (§5.1 `workspace.getContext` /
-- `updateContext`, PROTOCOL §6.5 `workspace:context-changed`). Migrates the
-- renderer-only `localStorage["workspace:context:{workspaceId}"]` store —
-- attached notes/urls/linear/github/sentry issues surfaced in the chat
-- context panel — into daemon-owned rows so the surface is queryable by
-- other clients and MCP tools. The daemon treats each item's payload as an
-- opaque JSON blob authored by the FE (its `ContextItem` union); the row
-- pulls `id` out for keying and `ordinal` for stable ordering (insertion
-- order, matching the FE's `Collection<ContextItem, "id">` iteration order).
CREATE TABLE workspace_context_item (
    workspace_id TEXT NOT NULL REFERENCES workspace(id) ON DELETE CASCADE,
    id           TEXT NOT NULL,
    ordinal      INTEGER NOT NULL,
    payload      TEXT NOT NULL,
    PRIMARY KEY (workspace_id, id)
);

CREATE INDEX idx_workspace_context_item_workspace_ordinal
    ON workspace_context_item(workspace_id, ordinal);
