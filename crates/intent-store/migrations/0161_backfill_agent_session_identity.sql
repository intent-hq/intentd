-- Every session pins its provider, model and reasoning effort; a NULL or blank
-- column is a legacy row written before that rule. Backfill each one from the
-- identity its last committed turn actually ran under, so a later
-- model.defaultProvider change can no longer move it. Data only, idempotent,
-- fill-only: set columns are never touched. Rows without evidence stay NULL
-- and are repaired at their next turn (provider) or session open (model /
-- effort) by the daemon, which alone can read the settings default.

-- Provider: the last turn's provider, else the provider recorded with the last
-- confirmed effort. Legacy default-provider aliases (`default` / `acp` /
-- `augment`) are left for the daemon's registry-aware repair.
UPDATE agent_session
SET provider = COALESCE(
        NULLIF(TRIM(last_turn_provider), ''),
        NULLIF(TRIM(CASE WHEN json_valid(last_turn_effort)
                         THEN json_extract(last_turn_effort, '$.provider') END), '')
    )
WHERE (provider IS NULL OR TRIM(provider) = '')
  AND COALESCE(
        NULLIF(TRIM(last_turn_provider), ''),
        NULLIF(TRIM(CASE WHEN json_valid(last_turn_effort)
                         THEN json_extract(last_turn_effort, '$.provider') END), '')
      ) IS NOT NULL
  AND LOWER(COALESCE(
        NULLIF(TRIM(last_turn_provider), ''),
        CASE WHEN json_valid(last_turn_effort)
             THEN json_extract(last_turn_effort, '$.provider') END
      )) NOT IN ('default', 'acp', 'augment');

-- Model: the spawn-resolved model the last turn ran (NULL there means the
-- provider default, which is not evidence of a concrete id).
UPDATE agent_session
SET model = TRIM(last_turn_model)
WHERE (model IS NULL OR TRIM(model) = '')
  AND NULLIF(TRIM(last_turn_model), '') IS NOT NULL;

-- Reasoning effort: the last confirmed level — the explicit selection, else
-- (Auto) the provider's reported default value. The adapter's "default"
-- sentinel is not a level and is skipped.
UPDATE agent_session
SET reasoning_effort = COALESCE(
        NULLIF(TRIM(json_extract(last_turn_effort, '$.effort')), ''),
        NULLIF(TRIM(json_extract(last_turn_effort, '$.default_value')), '')
    )
WHERE (reasoning_effort IS NULL OR TRIM(reasoning_effort) = '')
  AND json_valid(last_turn_effort)
  AND COALESCE(
        NULLIF(TRIM(json_extract(last_turn_effort, '$.effort')), ''),
        NULLIF(TRIM(json_extract(last_turn_effort, '$.default_value')), '')
      ) IS NOT NULL
  AND LOWER(COALESCE(
        NULLIF(TRIM(json_extract(last_turn_effort, '$.effort')), ''),
        NULLIF(TRIM(json_extract(last_turn_effort, '$.default_value')), '')
      )) <> 'default';
