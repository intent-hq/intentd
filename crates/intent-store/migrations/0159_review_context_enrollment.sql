-- Installation-private native family selection. Never authority or a receipt.
-- No backfill from public agent/session identifiers.
CREATE TABLE review_context_enrollment (
    agent_id TEXT PRIMARY KEY REFERENCES agent_session(id) ON DELETE CASCADE,
    workspace_id TEXT NOT NULL REFERENCES workspace(id) ON DELETE CASCADE,
    acp_session_id TEXT NOT NULL CHECK(length(trim(acp_session_id)) BETWEEN 1 AND 256 AND instr(acp_session_id,char(0))=0),
    owner_key TEXT NOT NULL UNIQUE CHECK(
        length(owner_key)=36 AND substr(owner_key,9,1)='-' AND substr(owner_key,14,1)='-'
        AND substr(owner_key,19,1)='-' AND substr(owner_key,24,1)='-'
        AND length(replace(owner_key,'-',''))=32
        AND replace(owner_key,'-','') NOT GLOB '*[^0-9a-f]*'
        AND substr(owner_key,15,1)='4' AND substr(owner_key,20,1) GLOB '[89ab]'),
    version INTEGER NOT NULL CHECK(version=1)
);
CREATE TRIGGER review_context_enrollment_insert_guard
BEFORE INSERT ON review_context_enrollment
BEGIN
    SELECT RAISE(ABORT,'private native enrollment is immutable')
    WHERE EXISTS(SELECT 1 FROM review_context_enrollment WHERE agent_id=NEW.agent_id);
    SELECT RAISE(ABORT,'private native enrollment identity mismatch')
    WHERE length(trim(NEW.agent_id))=0 OR length(trim(NEW.workspace_id))=0
       OR NOT EXISTS(SELECT 1 FROM agent_session WHERE id=NEW.agent_id
                     AND workspace_id=NEW.workspace_id AND acp_session_id=NEW.acp_session_id);
END;
CREATE TRIGGER review_context_enrollment_update_guard
BEFORE UPDATE ON review_context_enrollment
BEGIN
    SELECT RAISE(ABORT,'private native enrollment is immutable');
END;
-- ABORT/ROLLBACK of a failed insert also rolls back this invalidation.
CREATE TRIGGER review_context_enrollment_agent_insert
BEFORE INSERT ON agent_session
BEGIN
    DELETE FROM review_context_enrollment WHERE agent_id=NEW.id;
END;
CREATE TRIGGER review_context_enrollment_agent_update
AFTER UPDATE OF id,workspace_id,provider,acp_session_id,backend_session_id,parent_agent_id ON agent_session
WHEN NEW.id IS NOT OLD.id OR NEW.workspace_id IS NOT OLD.workspace_id
  OR NEW.provider IS NOT OLD.provider OR NEW.acp_session_id IS NOT OLD.acp_session_id
  OR NEW.backend_session_id IS NOT OLD.backend_session_id OR NEW.parent_agent_id IS NOT OLD.parent_agent_id
BEGIN
    DELETE FROM review_context_enrollment WHERE agent_id IN (OLD.id,NEW.id);
END;
