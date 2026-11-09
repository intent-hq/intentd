-- The per-section breakdown of a session's spawn context (agent.getSession
-- `promptBreakdown`, harness 3.10+). JSON, written together with
-- `system_prompt`; NULL when none was recorded. A rewrite of
-- `system_prompt` that does not compute a breakdown clears it.
ALTER TABLE agent_session ADD COLUMN prompt_breakdown TEXT;
