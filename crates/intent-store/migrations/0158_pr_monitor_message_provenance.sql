-- Local evidence for fresh admission of an exact saved monitor envelope.
-- This is neither a delivery receipt nor portable authorization. No backfill.
CREATE TABLE pr_monitor_message_provenance (
    message_id TEXT PRIMARY KEY,
    agent_id TEXT NOT NULL,
    workspace_id TEXT NOT NULL REFERENCES workspace(id) ON DELETE CASCADE,
    binding_json TEXT NOT NULL,
    envelope_sha256 BLOB NOT NULL CHECK(length(envelope_sha256) = 32),
    version INTEGER NOT NULL CHECK(version = 1),
    FOREIGN KEY(message_id, agent_id) REFERENCES agent_message(id, agent_id) ON DELETE CASCADE
);

CREATE TRIGGER pr_monitor_message_provenance_insert_guard
BEFORE INSERT ON pr_monitor_message_provenance
BEGIN
    SELECT RAISE(ABORT, 'monitor message provenance is immutable')
    WHERE EXISTS(SELECT 1 FROM pr_monitor_message_provenance WHERE message_id = NEW.message_id);
    SELECT RAISE(ABORT, 'monitor message workspace mismatch')
    WHERE NOT EXISTS(SELECT 1 FROM agent_session WHERE id = NEW.agent_id AND workspace_id = NEW.workspace_id);
END;
CREATE TRIGGER pr_monitor_message_provenance_update_guard
BEFORE UPDATE ON pr_monitor_message_provenance
BEGIN
    SELECT RAISE(ABORT, 'monitor message provenance is immutable');
END;

-- Invalidate even byte-identical replacement/import attempts. An ID is not
-- evidence that a new message is the original local message.
CREATE TRIGGER pr_monitor_message_provenance_message_insert
BEFORE INSERT ON agent_message
BEGIN
    DELETE FROM pr_monitor_message_provenance WHERE message_id = NEW.id;
END;
CREATE TRIGGER pr_monitor_message_provenance_message_update
AFTER UPDATE OF id, agent_id, seq, role, content, metadata, created_at, thumbnails ON agent_message
WHEN NEW.id IS NOT OLD.id OR NEW.agent_id IS NOT OLD.agent_id OR NEW.seq IS NOT OLD.seq
  OR NEW.role IS NOT OLD.role OR NEW.content IS NOT OLD.content OR NEW.metadata IS NOT OLD.metadata
  OR NEW.created_at IS NOT OLD.created_at OR NEW.thumbnails IS NOT OLD.thumbnails
BEGIN
    DELETE FROM pr_monitor_message_provenance WHERE message_id IN (OLD.id, NEW.id);
END;
CREATE TRIGGER pr_monitor_message_provenance_payload_insert
AFTER INSERT ON agent_message_payload
BEGIN
    DELETE FROM pr_monitor_message_provenance WHERE message_id = NEW.message_id;
END;
CREATE TRIGGER pr_monitor_message_provenance_payload_update
AFTER UPDATE ON agent_message_payload
BEGIN
    DELETE FROM pr_monitor_message_provenance WHERE message_id IN (OLD.message_id, NEW.message_id);
END;
CREATE TRIGGER pr_monitor_message_provenance_payload_delete
AFTER DELETE ON agent_message_payload
BEGIN
    DELETE FROM pr_monitor_message_provenance WHERE message_id = OLD.message_id;
END;
CREATE TRIGGER pr_monitor_message_provenance_session_update
AFTER UPDATE OF id, workspace_id, provider, acp_session_id, backend_session_id ON agent_session
WHEN NEW.id IS NOT OLD.id OR NEW.workspace_id IS NOT OLD.workspace_id
  OR NEW.provider IS NOT OLD.provider OR NEW.acp_session_id IS NOT OLD.acp_session_id
  OR NEW.backend_session_id IS NOT OLD.backend_session_id
BEGIN
    DELETE FROM pr_monitor_message_provenance WHERE agent_id IN (OLD.id, NEW.id);
END;
