-- Workspace context links column (§5.1). Persists the issue/PR context links
-- supplied at workspace.create as a JSON TEXT column holding the serialized
-- `Vec<ContextLink>` (same shape precedent as `pull_requests`, 0035). NULL
-- denotes "created without links" (legacy rows and plain creates), which the
-- wire shape omits.
ALTER TABLE workspace ADD COLUMN context_links TEXT;
