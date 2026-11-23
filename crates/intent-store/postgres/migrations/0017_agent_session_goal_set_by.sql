-- sqlite: 0200_agent_session_goal_set_by.sql
ALTER TABLE agent_session ADD COLUMN IF NOT EXISTS goal_set_by text;
