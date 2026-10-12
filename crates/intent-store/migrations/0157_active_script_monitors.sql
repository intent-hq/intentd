-- Retire monitor history only after preserving outstanding delivery obligations.
-- SQLx applies this migration in one transaction, including trigger replacement.
DROP TRIGGER script_monitor_owner_retired;
DROP TRIGGER script_monitor_owner_deleted;
DROP TRIGGER script_monitor_workspace_archived;
DROP TRIGGER script_monitor_workspace_deleted;
DROP TRIGGER script_monitor_registration_fence;
DROP TRIGGER script_monitor_message_fence;
DROP TRIGGER script_monitor_message_delivered;
DROP TRIGGER script_monitor_queue_fence;

CREATE TABLE script_notification_job (
 wake_id TEXT PRIMARY KEY, workspace_id TEXT NOT NULL, agent_id TEXT NOT NULL,
 script_id TEXT NOT NULL, run_id TEXT NOT NULL, metadata TEXT NOT NULL,
 body TEXT NOT NULL, phase TEXT NOT NULL CHECK(phase IN ('pending','message-persisted')),
 created_at TEXT NOT NULL
);
CREATE INDEX script_notification_owner ON script_notification_job(agent_id);
-- Ordinary durable delivery ownership survives queue snapshot replacement/dequeue.
-- Completed rows are payload-free cleanup work, removed by the retry sweep;
-- they never authorize delivery or form terminal monitor history.
CREATE TABLE agent_queue_admission (
 id TEXT PRIMARY KEY, agent_id TEXT NOT NULL REFERENCES agent_session(id) ON DELETE CASCADE,
 workspace_id TEXT NOT NULL, turn_id TEXT NOT NULL, payload TEXT NOT NULL,
 created_at TEXT NOT NULL, phase TEXT NOT NULL CHECK(phase IN ('pending','accepted','completed'))
);
INSERT INTO script_notification_job
 SELECT 'script-monitor:'||m.id,m.workspace_id,m.agent_id,m.script_id,m.run_id,
 json_set(json_remove(m.row_json,'$.state','$.createdAt','$.outputPattern','$.lineCount'),
   '$.type','script_monitor_wake','$.source','system'),
 'Script '||json_extract(m.row_json,'$.scriptName')||' ('||m.script_id||') run '||m.run_id||': '||json_extract(m.row_json,'$.reason')||'. Monitoring ended. Read available output explicitly by script ID.',
 CASE WHEN m.wake_state='delivered' THEN 'message-persisted' ELSE 'pending' END,
 m.settled_at
 FROM script_monitor m JOIN agent_session a ON a.id=m.agent_id JOIN workspace w ON w.id=m.workspace_id
 WHERE m.state IN ('completed','expired','triggered') AND a.retired_at IS NULL AND w.archived=0
 AND (m.wake_state='pending' OR (m.wake_state='delivered' AND EXISTS
 (SELECT 1 FROM agent_queue q WHERE q.agent_id=m.agent_id AND json_valid(q.payload)
 AND json_extract(q.payload,'$.messageMetadata.monitorId')=m.id)));
DELETE FROM agent_queue WHERE json_valid(payload) AND json_extract(payload,'$.messageMetadata.type')='script_monitor_wake'
 AND NOT EXISTS (SELECT 1 FROM script_notification_job j WHERE j.wake_id='script-monitor:'||json_extract(payload,'$.messageMetadata.monitorId') AND j.agent_id=agent_queue.agent_id);
ALTER TABLE script_monitor RENAME TO script_monitor_v1;
CREATE TABLE script_monitor (
 id TEXT PRIMARY KEY, workspace_id TEXT NOT NULL, agent_id TEXT NOT NULL,
 script_id TEXT NOT NULL, run_id TEXT NOT NULL,
 state TEXT NOT NULL CHECK(state='active'), row_json TEXT NOT NULL,
 created_at TEXT NOT NULL, cancel_intent INTEGER NOT NULL DEFAULT 0
);
INSERT INTO script_monitor SELECT m.id,m.workspace_id,m.agent_id,m.script_id,m.run_id,m.state,m.row_json,m.created_at,m.cancel_intent
 FROM script_monitor_v1 m JOIN agent_session a ON a.id=m.agent_id JOIN workspace w ON w.id=m.workspace_id
 WHERE m.state='active' AND a.retired_at IS NULL AND w.archived=0;
DROP TABLE script_monitor_v1;
CREATE UNIQUE INDEX script_monitor_owner ON script_monitor(workspace_id,script_id);
CREATE INDEX script_monitor_agent ON script_monitor(agent_id,state);
CREATE INDEX script_monitor_workspace ON script_monitor(workspace_id,created_at,id);
CREATE TRIGGER script_monitor_owner_retired AFTER UPDATE OF retired_at ON agent_session WHEN new.retired_at IS NOT NULL
BEGIN
 DELETE FROM script_monitor WHERE agent_id=new.id;
 DELETE FROM script_notification_job WHERE agent_id=new.id;
 DELETE FROM agent_queue_admission WHERE agent_id=new.id;
 DELETE FROM agent_queue WHERE json_valid(payload) AND json_extract(payload,'$.messageMetadata.type')='script_monitor_wake' AND agent_id=new.id;
END;
CREATE TRIGGER script_monitor_owner_deleted BEFORE DELETE ON agent_session
BEGIN
 DELETE FROM script_monitor WHERE agent_id=old.id;
 DELETE FROM script_notification_job WHERE agent_id=old.id;
 DELETE FROM agent_queue_admission WHERE agent_id=old.id;
 DELETE FROM agent_queue WHERE json_valid(payload) AND json_extract(payload,'$.messageMetadata.type')='script_monitor_wake' AND agent_id=old.id;
END;
CREATE TRIGGER script_monitor_workspace_archived AFTER UPDATE OF archived ON workspace WHEN new.archived=1
BEGIN
 DELETE FROM script_monitor WHERE workspace_id=new.id;
 DELETE FROM script_notification_job WHERE workspace_id=new.id;
 DELETE FROM agent_queue_admission WHERE workspace_id=new.id;
 DELETE FROM agent_queue WHERE json_valid(payload) AND json_extract(payload,'$.messageMetadata.type')='script_monitor_wake' AND json_extract(payload,'$.messageMetadata.workspaceId')=new.id;
END;
CREATE TRIGGER script_monitor_workspace_deleted BEFORE DELETE ON workspace
BEGIN
 DELETE FROM script_monitor WHERE workspace_id=old.id;
 DELETE FROM script_notification_job WHERE workspace_id=old.id;
 DELETE FROM agent_queue_admission WHERE workspace_id=old.id;
 DELETE FROM agent_queue WHERE json_valid(payload) AND json_extract(payload,'$.messageMetadata.type')='script_monitor_wake' AND json_extract(payload,'$.messageMetadata.workspaceId')=old.id;
END;
CREATE TRIGGER script_monitor_registration_fence BEFORE INSERT ON script_monitor
WHEN NOT EXISTS (SELECT 1 FROM agent_session a JOIN workspace w ON w.id=a.workspace_id
 WHERE a.id=new.agent_id AND a.workspace_id=new.workspace_id AND a.retired_at IS NULL AND w.archived=0)
BEGIN SELECT RAISE(ABORT,'script monitor lifecycle fence'); END;
CREATE TRIGGER script_monitor_message_fence BEFORE INSERT ON agent_message
WHEN (CASE WHEN json_valid(new.metadata) THEN json_extract(new.metadata,'$.type') END)='script_monitor_wake'
 AND NOT EXISTS (
 SELECT 1 FROM (SELECT wake_id AS id,agent_id,workspace_id FROM script_notification_job UNION ALL SELECT id,agent_id,workspace_id FROM agent_queue_admission WHERE phase != 'completed') d
 JOIN agent_session a ON a.id=d.agent_id JOIN workspace w ON w.id=d.workspace_id
 WHERE d.id='script-monitor:'||json_extract(new.metadata,'$.monitorId') AND d.agent_id=new.agent_id AND a.retired_at IS NULL AND w.archived=0)
BEGIN SELECT RAISE(ABORT,'script monitor wake suppressed'); END;
CREATE TRIGGER script_monitor_message_delivered AFTER INSERT ON agent_message
WHEN (CASE WHEN json_valid(new.metadata) THEN json_extract(new.metadata,'$.type') END)='script_monitor_wake'
BEGIN UPDATE script_notification_job SET phase='message-persisted' WHERE wake_id='script-monitor:'||json_extract(new.metadata,'$.monitorId'); END;
CREATE TRIGGER script_monitor_queue_fence BEFORE INSERT ON agent_queue
WHEN (CASE WHEN json_valid(new.payload) THEN json_extract(new.payload,'$.messageMetadata.type') END)='script_monitor_wake'
 AND NOT EXISTS (
 SELECT 1 FROM (SELECT wake_id AS id,agent_id,workspace_id FROM script_notification_job UNION ALL SELECT id,agent_id,workspace_id FROM agent_queue_admission WHERE phase='pending') d
 JOIN agent_session a ON a.id=d.agent_id JOIN workspace w ON w.id=d.workspace_id
 WHERE d.id='script-monitor:'||json_extract(new.payload,'$.messageMetadata.monitorId') AND d.agent_id=new.agent_id AND a.retired_at IS NULL AND w.archived=0)
BEGIN SELECT RAISE(IGNORE); END;
CREATE INDEX script_active_page ON script(workspace_id,created_at DESC,id DESC) WHERE archived_at IS NULL;
