-- Every session pins its provider, model and reasoning effort; a NULL or blank
-- column is a legacy row written before that rule. Backfill each one from the
-- identity its last committed turn actually ran under, so a later
-- model.defaultProvider change can no longer move it. The UPDATEs are
-- idempotent and fill-only: set columns are never touched. Rows without
-- evidence stay NULL and are repaired by the daemon, which alone can read the
-- settings default: provider at the next turn start, model at the next
-- session open, effort at the next confirmed turn.
-- Every json_extract sits behind CASE WHEN json_valid(...): SQLite does not
-- guarantee AND-operand order, and malformed JSON must never abort the
-- migration.

-- 1 when `reasoning_effort` is a level the daemon pinned from Auto (the
-- provider's confirmed default), not one anyone chose: a real model change
-- resets it to Auto so the new model's default applies. Any explicit effort
-- write clears it.
ALTER TABLE agent_session ADD COLUMN reasoning_effort_auto INTEGER NOT NULL DEFAULT 0;

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
  AND LOWER(COALESCE(
        NULLIF(TRIM(last_turn_provider), ''),
        NULLIF(TRIM(CASE WHEN json_valid(last_turn_effort)
                         THEN json_extract(last_turn_effort, '$.provider') END), ''),
        'default'
      )) NOT IN ('default', 'acp', 'augment');

-- Model: the spawn-resolved model the last turn ran — only when that turn ran
-- the session's (now pinned) provider, so one provider's model is never
-- written onto a session that has since moved (e.g. a disabled-provider
-- re-home cleared it). A legacy compound `provider:model` id is stripped to
-- its bare part; NULL there (provider default), whitespace-bearing display
-- names and the adapter's floating `default` alias are not concrete ids.
UPDATE agent_session
SET model = CASE WHEN INSTR(TRIM(last_turn_model), ':') > 0
                 THEN SUBSTR(TRIM(last_turn_model), INSTR(TRIM(last_turn_model), ':') + 1)
                 ELSE TRIM(last_turn_model) END
WHERE (model IS NULL OR TRIM(model) = '')
  AND provider IS NOT NULL
  AND provider = TRIM(last_turn_provider)
  AND NULLIF(TRIM(last_turn_model), '') IS NOT NULL
  AND INSTR(TRIM(last_turn_model), ' ') = 0
  AND LOWER(CASE WHEN INSTR(TRIM(last_turn_model), ':') > 0
                 THEN SUBSTR(TRIM(last_turn_model), INSTR(TRIM(last_turn_model), ':') + 1)
                 ELSE TRIM(last_turn_model) END) NOT IN ('', 'default');

-- Reasoning effort: the last confirmed level — the explicit selection, else
-- (Auto) the provider's reported default value, flagged as Auto-pinned — only
-- when it was confirmed on the session's provider. The adapter's "default"
-- sentinel is not a level.
UPDATE agent_session
SET reasoning_effort_auto = CASE
        WHEN NULLIF(TRIM(CASE WHEN json_valid(last_turn_effort)
                              THEN json_extract(last_turn_effort, '$.effort') END), '') IS NULL
        THEN 1 ELSE 0 END,
    reasoning_effort = COALESCE(
        NULLIF(TRIM(CASE WHEN json_valid(last_turn_effort)
                         THEN json_extract(last_turn_effort, '$.effort') END), ''),
        NULLIF(TRIM(CASE WHEN json_valid(last_turn_effort)
                         THEN json_extract(last_turn_effort, '$.default_value') END), '')
    )
WHERE (reasoning_effort IS NULL OR TRIM(reasoning_effort) = '')
  AND provider IS NOT NULL
  AND provider = TRIM(CASE WHEN json_valid(last_turn_effort)
                           THEN json_extract(last_turn_effort, '$.provider') END)
  AND LOWER(COALESCE(
        NULLIF(TRIM(CASE WHEN json_valid(last_turn_effort)
                         THEN json_extract(last_turn_effort, '$.effort') END), ''),
        NULLIF(TRIM(CASE WHEN json_valid(last_turn_effort)
                         THEN json_extract(last_turn_effort, '$.default_value') END), ''),
        'default'
      )) <> 'default';
