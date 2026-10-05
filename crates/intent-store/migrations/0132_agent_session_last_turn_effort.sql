-- Last confirmed effort selection plus its provider/model default. SQL NULL
-- means no turn baseline; the JSON effort field is null for Auto. Do not seed
-- from reasoning_effort: that is desired state and may never have been applied.
ALTER TABLE agent_session ADD COLUMN last_turn_effort TEXT;
