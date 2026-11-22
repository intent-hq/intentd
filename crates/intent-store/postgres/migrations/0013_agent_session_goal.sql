-- sqlite: 0199_agent_session_goal.sql
ALTER TABLE agent_session ADD COLUMN IF NOT EXISTS goal text;
