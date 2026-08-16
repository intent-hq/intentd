-- Harness versioning (intent-hq/monorepo#2459): every agent session is
-- permanently stamped at creation with the harness version it was created
-- with (harness_version) and a JSON snapshot of the agentFeatures on/off
-- values it runs with (harness_features). Existing rows backfill to '1.0'
-- with NULL features (the service layer projects the current settings on
-- read for legacy rows). The per-session taskGraph pin (0095) folds into the
-- snapshot: readers prefer harness_features -> '$.taskGraph' and fall back
-- to the legacy task_graph_enabled column for pre-snapshot rows.

ALTER TABLE agent_session ADD COLUMN harness_version TEXT NOT NULL DEFAULT '1.0';
ALTER TABLE agent_session ADD COLUMN harness_features TEXT;
