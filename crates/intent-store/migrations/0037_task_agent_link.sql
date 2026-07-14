-- Task↔agent linkage (§5.4 `task.linkAgent` / `unlinkAgent` /
-- `listAgentLinks`, PROTOCOL §6.5 `task:agent-linked` /
-- `task:agent-unlinked`). Migrates the renderer-only
-- `localStorage["task-agent-associations:{workspaceId}"]` store — the map
-- `byNoteId → byTaskKey → { taskText, taskKey?, agentId, noteId, createdAt }`
-- — into daemon-owned rows so MCP tools and other clients can ask "who is
-- working on this task?". `task_key` mirrors the FE key derivation
-- (`association.taskKey ?? association.taskText`); `task_text` records the
-- human-readable checkbox text at link time. `created_at` is epoch-ms
-- (FE parity with `TaskAgentAssociation.createdAt: number`).
CREATE TABLE task_agent_link (
    workspace_id TEXT NOT NULL REFERENCES workspace(id) ON DELETE CASCADE,
    note_id      TEXT NOT NULL,
    task_key     TEXT NOT NULL,
    task_text    TEXT NOT NULL,
    agent_id     TEXT NOT NULL,
    created_at   INTEGER NOT NULL,
    PRIMARY KEY (workspace_id, note_id, task_key)
);

CREATE INDEX idx_task_agent_link_workspace ON task_agent_link(workspace_id);
CREATE INDEX idx_task_agent_link_agent ON task_agent_link(workspace_id, agent_id);
