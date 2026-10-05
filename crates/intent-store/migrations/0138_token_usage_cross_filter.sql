-- Persist exact message/model provenance and the materialized agent × model
-- cells used by workspace token-usage snapshots.

ALTER TABLE agent_message ADD COLUMN usage_model TEXT;
ALTER TABLE agent_message ADD COLUMN usage_origin TEXT;

UPDATE agent_message
SET usage_model = COALESCE(
      NULLIF((SELECT resolved_model FROM agent_session WHERE id = agent_message.agent_id), ''),
      NULLIF((SELECT model FROM agent_session WHERE id = agent_message.agent_id), ''),
      'unknown'
    ),
    usage_origin = CASE
      WHEN role = 'assistant' THEN 'agent'
      WHEN role = 'user' AND COALESCE(NULLIF(json_extract(metadata, '$.fromAgentId'), ''), '') != ''
        THEN 'agent'
      WHEN role = 'user' THEN 'human'
      ELSE 'excluded'
    END;

CREATE TABLE agent_usage_cell (
  agent_id             TEXT NOT NULL REFERENCES agent_session(id) ON DELETE CASCADE,
  model                TEXT NOT NULL,
  input_tokens         INTEGER NOT NULL DEFAULT 0,
  output_tokens        INTEGER NOT NULL DEFAULT 0,
  cache_read_tokens    INTEGER NOT NULL DEFAULT 0,
  cache_creation_tokens INTEGER NOT NULL DEFAULT 0,
  thought_tokens       INTEGER NOT NULL DEFAULT 0,
  costs_json           TEXT NOT NULL DEFAULT '{}',
  human_messages       INTEGER NOT NULL DEFAULT 0,
  agent_messages       INTEGER NOT NULL DEFAULT 0,
  PRIMARY KEY (agent_id, model)
);

INSERT INTO agent_usage_cell (
  agent_id, model, input_tokens, output_tokens, cache_read_tokens,
  cache_creation_tokens, thought_tokens, costs_json
)
SELECT id, COALESCE(NULLIF(resolved_model, ''), NULLIF(model, ''), 'unknown'),
  COALESCE(json_extract(token_usage_baseline, '$.inputTokens'), 0) + COALESCE(json_extract(token_usage, '$.inputTokens'), 0),
  COALESCE(json_extract(token_usage_baseline, '$.outputTokens'), 0) + COALESCE(json_extract(token_usage, '$.outputTokens'), 0),
  COALESCE(json_extract(token_usage_baseline, '$.cacheReadTokens'), 0) + COALESCE(json_extract(token_usage, '$.cacheReadTokens'), 0),
  COALESCE(json_extract(token_usage_baseline, '$.cacheCreationTokens'), 0) + COALESCE(json_extract(token_usage, '$.cacheCreationTokens'), 0),
  COALESCE(json_extract(token_usage_baseline, '$.thoughtTokens'), 0) + COALESCE(json_extract(token_usage, '$.thoughtTokens'), 0),
  CASE
    WHEN json_extract(token_usage_baseline, '$.cost.currency') IS NOT NULL
      AND json_extract(token_usage, '$.cost.currency') = json_extract(token_usage_baseline, '$.cost.currency')
      THEN json_object(json_extract(token_usage, '$.cost.currency'),
        COALESCE(json_extract(token_usage_baseline, '$.cost.amount'), 0) + COALESCE(json_extract(token_usage, '$.cost.amount'), 0))
    WHEN json_extract(token_usage_baseline, '$.cost.currency') IS NOT NULL
      AND json_extract(token_usage, '$.cost.currency') IS NOT NULL
      THEN json_object(
        json_extract(token_usage_baseline, '$.cost.currency'), json_extract(token_usage_baseline, '$.cost.amount'),
        json_extract(token_usage, '$.cost.currency'), json_extract(token_usage, '$.cost.amount'))
    WHEN json_extract(token_usage_baseline, '$.cost.currency') IS NOT NULL
      THEN json_object(json_extract(token_usage_baseline, '$.cost.currency'), json_extract(token_usage_baseline, '$.cost.amount'))
    WHEN json_extract(token_usage, '$.cost.currency') IS NOT NULL
      THEN json_object(json_extract(token_usage, '$.cost.currency'), json_extract(token_usage, '$.cost.amount'))
    ELSE '{}'
  END
FROM agent_session
WHERE token_usage IS NOT NULL OR token_usage_baseline IS NOT NULL;

INSERT INTO agent_usage_cell (agent_id, model, human_messages, agent_messages)
SELECT agent_id, COALESCE(NULLIF(usage_model, ''), 'unknown'),
  SUM(CASE WHEN usage_origin = 'human' THEN 1 ELSE 0 END),
  SUM(CASE WHEN role = 'assistant' OR usage_origin = 'agent' THEN 1 ELSE 0 END)
FROM agent_message
WHERE role IN ('user', 'assistant')
GROUP BY agent_id, COALESCE(NULLIF(usage_model, ''), 'unknown')
ON CONFLICT(agent_id, model) DO UPDATE SET
  human_messages = excluded.human_messages,
  agent_messages = excluded.agent_messages;

CREATE INDEX idx_agent_message_usage_projection
  ON agent_message(agent_id, usage_model, usage_origin, role);