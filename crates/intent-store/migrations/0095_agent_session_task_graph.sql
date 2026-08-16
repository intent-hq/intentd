-- Daemon-owned snapshot of agentFeatures.taskGraph at session creation. Delivery-time
-- wake teaching reads this immutable value so later settings changes affect only new
-- sessions. Existing/imported sessions default off because the feature is opt-in.

ALTER TABLE agent_session ADD COLUMN task_graph_enabled INTEGER NOT NULL DEFAULT 0;