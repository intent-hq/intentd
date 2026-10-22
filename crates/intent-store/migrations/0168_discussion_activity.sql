ALTER TABLE workspace_discussion_message ADD COLUMN agent_activity_json TEXT NOT NULL DEFAULT '[]';
CREATE TABLE discussion_agent_activity (
    message_id TEXT NOT NULL REFERENCES workspace_discussion_message(id) ON DELETE CASCADE,
    agent_id TEXT NOT NULL,
    attempt INTEGER NOT NULL CHECK(attempt > 0),
    version INTEGER NOT NULL CHECK(version > 0),
    state TEXT NOT NULL CHECK(state IN ('queued','working','completed','failed','stopped')),
    updated_at TEXT NOT NULL,
    error TEXT,
    turn_id TEXT NOT NULL UNIQUE,
    PRIMARY KEY(message_id,agent_id)
);
-- Only pending legacy deliveries have a reliable invocation identity.
INSERT INTO discussion_agent_activity
SELECT m.id,j.value,1,1,'queued',m.created_at,NULL,'discussion:'||m.id||':'||j.value
FROM workspace_discussion_message m,json_each(m.recipient_agent_ids_json) j
WHERE EXISTS(SELECT 1 FROM discussion_agent_delivery d WHERE d.message_id=m.id AND d.agent_id=j.value)
 OR EXISTS(SELECT 1 FROM agent_queue q WHERE q.id='discussion:'||m.id||':'||j.value);
UPDATE workspace_discussion_message SET agent_activity_json = (
 SELECT json_group_array(json_object('agentId',agent_id,'attempt',attempt,'version',version,'state',state,'updatedAt',updated_at,'error',error))
 FROM (SELECT * FROM discussion_agent_activity WHERE message_id=workspace_discussion_message.id ORDER BY agent_id)
);
ALTER TABLE workspace_discussion_message ADD COLUMN agent_mentions_json TEXT NOT NULL DEFAULT '[]';
