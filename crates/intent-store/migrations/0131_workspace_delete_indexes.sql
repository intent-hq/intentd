-- Workspace cleanup must bound candidate lookup as well as affected rows.
-- The delivery-marker primary keys cover parent_agent_id; these cover the
-- other side of the OR batches and agent_session's child FK probes.
CREATE INDEX idx_completion_wake_delivery_child
  ON completion_wake_delivery(child_agent_id);
CREATE INDEX idx_advisory_wake_delivery_child
  ON advisory_wake_delivery(child_agent_id);

-- The open-tab index excludes tombstones. Deletion and workspace FK checks
-- must find closed tabs too, without scanning other workspaces' registries.
CREATE INDEX idx_browser_tab_workspace ON browser_tab(workspace_id);

-- Removing a parent link also removes the cleanup candidate, so later
-- batches never revisit cleared prefixes. Covers the parent-clear trigger.
CREATE INDEX idx_note_workspace_parented
  ON note(workspace_id, parent_id) WHERE parent_id IS NOT NULL;

-- Comments can be distributed across many notes. Seek remaining comments
-- directly, excluding note-less rows whose retention is independent.
CREATE INDEX idx_comment_workspace_note
  ON comment(workspace_id, note_id) WHERE note_id IS NOT NULL;
