-- Reasoning effort as a first-class session field (PROTOCOL §5.5, Option B).
-- Additive nullable TEXT column: the effort level requested for the session
-- (e.g. `low` / `medium` / `high` / `xhigh`), stored as-is — providers
-- interpret the vocabulary, the daemon never normalizes it. NULL = provider
-- default.
ALTER TABLE agent_session ADD COLUMN reasoning_effort TEXT;

-- One-time normalization of legacy codex compound model ids: sessions stored
-- with a `{base}/{effort}` model (the pre-Option-B codex effort-variant rows,
-- e.g. `gpt-5.3-codex/high` or `codex:gpt-5.3-codex/high`) split into the
-- base model id + the new reasoning_effort column. Guarded three ways so
-- slash-bearing non-codex ids (e.g. HuggingFace-style `org/model` unsloth
-- ids) are never mangled: the suffix must be a known codex effort level, AND
-- the row must show codex evidence — provider = 'codex', a `codex:` compound
-- prefix, or a base model in the known codex effort-variant list.
UPDATE agent_session
SET reasoning_effort = substr(model, instr(model, '/') + 1),
    model = substr(model, 1, instr(model, '/') - 1)
WHERE model IS NOT NULL
  AND instr(model, '/') > 0
  AND substr(model, instr(model, '/') + 1) IN ('low', 'medium', 'high', 'xhigh')
  AND (
    provider = 'codex'
    OR model LIKE 'codex:%'
    OR substr(model, 1, instr(model, '/') - 1) IN
      ('gpt-5.3-codex', 'gpt-5.2-codex', 'gpt-5.1-codex-max')
  );
