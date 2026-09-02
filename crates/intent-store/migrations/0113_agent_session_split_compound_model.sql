-- Retire compound `provider:model` ids from agent_session (model triple
-- refactor). The wire now hard-rejects compound model ids (-32602), so no
-- new compound rows can be written; this one-time normalization splits the
-- legacy rows so the stored identity is always the (provider, model) pair.
--
-- Split rule (matches the old prefix-wins runtime precedence): split on the
-- FIRST ':' — a non-empty prefix overwrites the provider column, the
-- remainder becomes the bare model id. A malformed leading-colon id
-- (`:model`) carries no provider information: the colon is stripped and the
-- provider column is left untouched. An empty remainder normalizes to NULL.
-- Unlike 0080 (the codex `{base}/{effort}` effort-suffix split), this does
-- NOT touch `reasoning_effort`: `provider:model` compounds never encoded
-- effort, and effort-lookalike bare ids (e.g. `opus[1m]`) contain no colon
-- and pass through untouched.
--
-- Columns audited for compound ids:
--   * `model` / `provider` — the stored session identity; compound in rows
--     written before the wire rejection. Split below.
--   * `last_turn_model` / `last_turn_provider` (0060) — the spawn-resolved
--     identity of the last committed turn; older daemons passed the stored
--     (possibly compound) model through. Split below.
--   * `resolved_model` (D13/D14) — a display identity resolved from the
--     provider's session-open option list (bare option values / display
--     labels), never compound by construction. Excluded.

-- Malformed leading-colon ids: strip the colon(s), keep the provider.
UPDATE agent_session
SET model = nullif(ltrim(model, ':'), '')
WHERE model IS NOT NULL
  AND substr(model, 1, 1) = ':';

-- Compound `provider:model`: prefix overwrites provider, remainder becomes
-- the model (after the statement above, every remaining ':' sits past
-- position 1, so the prefix is never empty).
UPDATE agent_session
SET provider = substr(model, 1, instr(model, ':') - 1),
    model = nullif(substr(model, instr(model, ':') + 1), '')
WHERE model IS NOT NULL
  AND instr(model, ':') > 1;

-- Same two passes for the last-turn identity pair.
UPDATE agent_session
SET last_turn_model = nullif(ltrim(last_turn_model, ':'), '')
WHERE last_turn_model IS NOT NULL
  AND substr(last_turn_model, 1, 1) = ':';

UPDATE agent_session
SET last_turn_provider = substr(last_turn_model, 1, instr(last_turn_model, ':') - 1),
    last_turn_model = nullif(substr(last_turn_model, instr(last_turn_model, ':') + 1), '')
WHERE last_turn_model IS NOT NULL
  AND instr(last_turn_model, ':') > 1;
