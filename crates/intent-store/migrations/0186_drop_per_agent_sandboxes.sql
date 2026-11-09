-- Per-agent copy-on-write sandboxes are retired (protocol 16). No live
-- per-agent sandbox exists to carry over, so drop their store rows and the
-- session link columns. Files on disk are never touched. Workspace CoW
-- checkouts (`workspace.checkout_mode = 'cow'`) are unaffected.

DROP INDEX IF EXISTS idx_sandbox_workspace;
DROP INDEX IF EXISTS idx_sandbox_agent;
DROP TABLE IF EXISTS sandbox;

ALTER TABLE agent_session DROP COLUMN sandbox_id;
ALTER TABLE agent_session DROP COLUMN sandbox_path;
ALTER TABLE agent_session DROP COLUMN sandbox_branch;
