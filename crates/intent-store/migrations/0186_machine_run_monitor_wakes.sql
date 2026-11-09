-- A remote agent's machine-run monitor wake carries script_monitor_wake
-- metadata plus remoteAgentId but has no notification job: it is delivered
-- as an ordinary message, so the message and queue fences skip it. Home-run
-- wakes stay fenced exactly as before.
DROP TRIGGER script_monitor_message_fence;
DROP TRIGGER script_monitor_queue_fence;
CREATE TRIGGER script_monitor_message_fence BEFORE INSERT ON agent_message
WHEN (CASE WHEN json_valid(new.metadata) THEN json_extract(new.metadata,'$.type') END)='script_monitor_wake'
 AND (CASE WHEN json_valid(new.metadata) THEN json_extract(new.metadata,'$.remoteAgentId') END) IS NULL
 AND NOT EXISTS (
 SELECT 1 FROM (SELECT wake_id AS id,agent_id,workspace_id FROM script_notification_job UNION ALL SELECT id,agent_id,workspace_id FROM agent_queue_admission WHERE phase != 'completed') d
 JOIN agent_session a ON a.id=d.agent_id JOIN workspace w ON w.id=d.workspace_id
 WHERE d.id='script-monitor:'||json_extract(new.metadata,'$.monitorId') AND d.agent_id=new.agent_id AND a.retired_at IS NULL AND w.archived=0)
BEGIN SELECT RAISE(ABORT,'script monitor wake suppressed'); END;
CREATE TRIGGER script_monitor_queue_fence BEFORE INSERT ON agent_queue
WHEN (CASE WHEN json_valid(new.payload) THEN json_extract(new.payload,'$.messageMetadata.type') END)='script_monitor_wake'
 AND (CASE WHEN json_valid(new.payload) THEN json_extract(new.payload,'$.messageMetadata.remoteAgentId') END) IS NULL
 AND NOT EXISTS (
 SELECT 1 FROM (SELECT wake_id AS id,agent_id,workspace_id FROM script_notification_job UNION ALL SELECT id,agent_id,workspace_id FROM agent_queue_admission WHERE phase='pending') d
 JOIN agent_session a ON a.id=d.agent_id JOIN workspace w ON w.id=d.workspace_id
 WHERE d.id='script-monitor:'||json_extract(new.payload,'$.messageMetadata.monitorId') AND d.agent_id=new.agent_id AND a.retired_at IS NULL AND w.archived=0)
BEGIN SELECT RAISE(IGNORE); END;
