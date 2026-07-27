-- Model/provider identity of the agent's last committed turn (model-change
-- transcript notice). Additive only: two nullable TEXT columns written at
-- turn start once the turn's child + ACP session are ready — NOT on
-- `agent.setModel`, so picker toggles reverted before any message never
-- commit. `last_turn_model` holds the spawn-resolved model id the turn ran
-- under (NULL for the provider default), `last_turn_provider` the provider
-- id. When a later turn starts under a different pair, the daemon persists
-- an informational `model_changed` system row in the transcript before the
-- turn proceeds. Both NULL until the agent's first turn commits.
ALTER TABLE agent_session ADD COLUMN last_turn_model TEXT;
ALTER TABLE agent_session ADD COLUMN last_turn_provider TEXT;
