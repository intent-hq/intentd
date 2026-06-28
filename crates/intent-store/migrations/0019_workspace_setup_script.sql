-- Persist the durable `setupScript` workspace field (§5.25). Additive only:
-- 0001–0018 are frozen, so this migration appends a single nullable TEXT column
-- holding the JSON-encoded `SetupScript { script, projectType?, updatedAt,
-- generatedBy? }` record read/written via `workspace.getSetupScript` /
-- `saveSetupScript`. NULL means the field is omitted from the wire (no script has
-- been saved yet). Earlier slices carried `setupScript` only in memory; it was
-- never persisted, so there is no legacy column to migrate.
ALTER TABLE workspace ADD COLUMN setup_script TEXT;
