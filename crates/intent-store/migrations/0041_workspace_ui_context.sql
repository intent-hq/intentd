-- Per-workspace UI context (§5.1 `workspace.getUiContext` /
-- `updateUiContext`). Persists the FE's WorkspaceUIContext state (what content
-- is visible in the main pane: note, diff, browser preview, file) so the daemon
-- can restore it on reconnect. The daemon treats the payload as an opaque JSON
-- blob authored by the FE; no interpretation, no shape coercion — byte-for-byte
-- round-trip preservation is the correctness requirement.
--
-- This is distinct from workspace_context_item (chat-context attachments, §5.1
-- workspace.getContext / updateContext), which stores an array of ContextItem
-- payloads (linear issues, notes, files, URLs surfaced in the context panel).
CREATE TABLE workspace_ui_context (
    workspace_id TEXT PRIMARY KEY REFERENCES workspace(id) ON DELETE CASCADE,
    payload      TEXT NOT NULL
);
