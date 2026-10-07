-- Empty after a deliberate purge is different from never having scripts.
-- Keep this internal marker on the workspace, so deleting the last definition
-- cannot re-enable repository-default bootstrap, including after a restart.
ALTER TABLE workspace ADD COLUMN scripts_initialized INTEGER NOT NULL DEFAULT 0;

-- Existing active and archived definitions both establish initialization.
-- Already-empty pre-upgrade workspaces have no retained evidence to backfill.
UPDATE workspace SET scripts_initialized = 1
WHERE EXISTS (SELECT 1 FROM script WHERE script.workspace_id = workspace.id);

-- Cover repository bootstrap, explicit creation, replacement and imports in
-- the same transaction as the definition. Subsequent writes are no-ops.
CREATE TRIGGER workspace_scripts_initialized_insert AFTER INSERT ON script
BEGIN
    UPDATE workspace SET scripts_initialized = 1
    WHERE id = new.workspace_id AND scripts_initialized = 0;
END;
