-- Move the stored spawn prompt out of the `agent_session` row. `SQLite`
-- keeps a row's columns in one record, so `system_prompt` (column 10,
-- ~70 KB per agent on a busy host) put every later column — `created_at`,
-- `metadata`, the status-adjacent fields every summary read selects —
-- behind a ~17-page overflow chain each read walked. `prompt_breakdown`
-- (0188) is written with the prompt and moves with it. A session without a
-- stored prompt has no row here; full reads LEFT JOIN it back
-- (`agent_repo::SESSION_FROM`).

CREATE TABLE agent_session_prompt (
  agent_id          TEXT PRIMARY KEY NOT NULL REFERENCES agent_session(id) ON DELETE CASCADE,
  system_prompt     TEXT,
  prompt_breakdown  TEXT
);

INSERT INTO agent_session_prompt (agent_id, system_prompt, prompt_breakdown)
SELECT id, system_prompt, prompt_breakdown FROM agent_session
WHERE system_prompt IS NOT NULL OR prompt_breakdown IS NOT NULL;

-- Each DROP rewrites the rows without the column, so the session rows
-- shrink here rather than on their next write.
ALTER TABLE agent_session DROP COLUMN system_prompt;
ALTER TABLE agent_session DROP COLUMN prompt_breakdown;
