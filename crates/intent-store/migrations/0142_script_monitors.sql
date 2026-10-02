-- Latest durable admission survives settlement for both commands and services.
ALTER TABLE script ADD COLUMN latest_run_id TEXT;
ALTER TABLE script ADD COLUMN latest_run_result TEXT;
UPDATE script SET latest_run_id = pending_run_id WHERE pending_run_id IS NOT NULL;

CREATE TABLE script_monitor (
    id TEXT PRIMARY KEY,
    workspace_id TEXT NOT NULL,
    agent_id TEXT NOT NULL,
    script_id TEXT NOT NULL,
    run_id TEXT NOT NULL,
    state TEXT NOT NULL CHECK(state IN ('active','completed','expired','triggered','cancelled')),
    row_json TEXT NOT NULL,
    created_at TEXT NOT NULL,
    settled_at TEXT,
    cancel_intent INTEGER NOT NULL DEFAULT 0,
    wake_state TEXT NOT NULL DEFAULT 'none' CHECK(wake_state IN ('none','pending','delivered','suppressed'))
);
CREATE UNIQUE INDEX script_monitor_owner ON script_monitor(workspace_id,script_id) WHERE state = 'active';
CREATE INDEX script_monitor_agent ON script_monitor(agent_id,state);
CREATE INDEX script_monitor_workspace ON script_monitor(workspace_id,created_at,id);
CREATE INDEX script_monitor_pending ON script_monitor(wake_state) WHERE wake_state = 'pending';

CREATE TRIGGER script_monitor_owner_retired AFTER UPDATE OF retired_at ON agent_session WHEN new.retired_at IS NOT NULL
BEGIN
    UPDATE script_monitor SET state='cancelled',cancel_intent=0,settled_at=strftime('%Y-%m-%dT%H:%M:%fZ','now'),
      row_json=json_set(row_json,'$.state','cancelled','$.reason','owner-retired','$.settledAt',strftime('%Y-%m-%dT%H:%M:%fZ','now'))
      WHERE agent_id = new.id AND state='active';
    UPDATE script_monitor SET wake_state='suppressed' WHERE agent_id = new.id AND wake_state IN ('pending','none','delivered');
    DELETE FROM agent_queue WHERE (CASE WHEN json_valid(payload) THEN json_extract(payload,'$.messageMetadata.type') END)='script_monitor_wake'
      AND (CASE WHEN json_valid(payload) THEN json_extract(payload,'$.messageMetadata.monitorId') END) IN (SELECT id FROM script_monitor WHERE agent_id = new.id);
END;

CREATE TRIGGER script_monitor_owner_deleted BEFORE DELETE ON agent_session
BEGIN
    UPDATE script_monitor SET state='cancelled',cancel_intent=0,settled_at=strftime('%Y-%m-%dT%H:%M:%fZ','now'),
      row_json=json_set(row_json,'$.state','cancelled','$.reason','owner-deleted','$.settledAt',strftime('%Y-%m-%dT%H:%M:%fZ','now'))
      WHERE agent_id = old.id AND state='active';
    UPDATE script_monitor SET wake_state='suppressed' WHERE agent_id = old.id AND wake_state IN ('pending','none','delivered');
    DELETE FROM agent_queue WHERE (CASE WHEN json_valid(payload) THEN json_extract(payload,'$.messageMetadata.type') END)='script_monitor_wake'
      AND (CASE WHEN json_valid(payload) THEN json_extract(payload,'$.messageMetadata.monitorId') END) IN (SELECT id FROM script_monitor WHERE agent_id = old.id);
END;

CREATE TRIGGER script_monitor_workspace_archived AFTER UPDATE OF archived ON workspace WHEN new.archived = 1
BEGIN
    UPDATE script_monitor SET state='cancelled',cancel_intent=0,settled_at=strftime('%Y-%m-%dT%H:%M:%fZ','now'),
      row_json=json_set(row_json,'$.state','cancelled','$.reason','workspace-archived','$.settledAt',strftime('%Y-%m-%dT%H:%M:%fZ','now'))
      WHERE workspace_id = new.id AND state='active';
    UPDATE script_monitor SET wake_state='suppressed' WHERE workspace_id = new.id AND wake_state IN ('pending','none','delivered');
    DELETE FROM agent_queue WHERE (CASE WHEN json_valid(payload) THEN json_extract(payload,'$.messageMetadata.type') END)='script_monitor_wake'
      AND (CASE WHEN json_valid(payload) THEN json_extract(payload,'$.messageMetadata.monitorId') END) IN (SELECT id FROM script_monitor WHERE workspace_id = new.id);
END;

CREATE TRIGGER script_monitor_workspace_deleted BEFORE DELETE ON workspace
BEGIN
    UPDATE script_monitor SET state='cancelled',cancel_intent=0,settled_at=strftime('%Y-%m-%dT%H:%M:%fZ','now'),
      row_json=json_set(row_json,'$.state','cancelled','$.reason','workspace-deleted','$.settledAt',strftime('%Y-%m-%dT%H:%M:%fZ','now'))
      WHERE workspace_id = old.id AND state='active';
    UPDATE script_monitor SET wake_state='suppressed' WHERE workspace_id = old.id AND wake_state IN ('pending','none','delivered');
    DELETE FROM agent_queue WHERE (CASE WHEN json_valid(payload) THEN json_extract(payload,'$.messageMetadata.type') END)='script_monitor_wake'
      AND (CASE WHEN json_valid(payload) THEN json_extract(payload,'$.messageMetadata.monitorId') END) IN (SELECT id FROM script_monitor WHERE workspace_id = old.id);
END;

CREATE TRIGGER script_monitor_registration_fence BEFORE INSERT ON script_monitor
WHEN NOT EXISTS (SELECT 1 FROM agent_session a JOIN workspace w ON w.id=a.workspace_id
 WHERE a.id=new.agent_id AND a.workspace_id=new.workspace_id AND a.retired_at IS NULL AND w.archived=0)
BEGIN SELECT RAISE(ABORT,'script monitor lifecycle fence'); END;

CREATE TRIGGER script_monitor_message_fence BEFORE INSERT ON agent_message
WHEN (CASE WHEN json_valid(new.metadata) THEN json_extract(new.metadata,'$.type') END)='script_monitor_wake'
 AND NOT EXISTS (SELECT 1 FROM script_monitor m JOIN agent_session a ON a.id=m.agent_id JOIN workspace w ON w.id=m.workspace_id
 WHERE m.id=(CASE WHEN json_valid(new.metadata) THEN json_extract(new.metadata,'$.monitorId') END) AND m.agent_id=new.agent_id AND m.wake_state='pending' AND a.retired_at IS NULL AND w.archived=0)
BEGIN SELECT RAISE(ABORT,'script monitor wake suppressed or delivered'); END;

CREATE TRIGGER script_monitor_message_delivered AFTER INSERT ON agent_message
WHEN (CASE WHEN json_valid(new.metadata) THEN json_extract(new.metadata,'$.type') END)='script_monitor_wake'
BEGIN
 UPDATE script_monitor SET wake_state='delivered' WHERE id=(CASE WHEN json_valid(new.metadata) THEN json_extract(new.metadata,'$.monitorId') END) AND wake_state='pending';
END;

-- A queue snapshot captured before cleanup may flush after it. Do not restore
-- suppressed monitor entries or reject unrelated entries in the same snapshot.
CREATE TRIGGER script_monitor_queue_fence BEFORE INSERT ON agent_queue
WHEN (CASE WHEN json_valid(new.payload) THEN json_extract(new.payload,'$.messageMetadata.type') END)='script_monitor_wake'
 AND NOT EXISTS (SELECT 1 FROM script_monitor m JOIN agent_session a ON a.id=m.agent_id JOIN workspace w ON w.id=m.workspace_id
 WHERE m.id=json_extract(new.payload,'$.messageMetadata.monitorId') AND m.agent_id=new.agent_id AND m.wake_state IN ('pending','delivered') AND a.retired_at IS NULL AND w.archived=0)
BEGIN SELECT RAISE(IGNORE); END;
