-- Workspace status casing aligned with the TS `WorkspaceStatus` string enum
-- (PascalCase: Active/Inactive/Archived/Deleted, src/shared/types.ts). The serde
-- representation is both the wire shape and the stored DB word, matching the
-- existing `PullRequestStatus` precedent. Convert any pre-existing lowercase
-- rows. Inserts always bind `status` explicitly, so the column DEFAULT 'active'
-- is never exercised; it is left in place to avoid a full table rebuild.
UPDATE workspace SET status = 'Active'   WHERE status = 'active';
UPDATE workspace SET status = 'Inactive' WHERE status = 'inactive';
UPDATE workspace SET status = 'Archived' WHERE status = 'archived';
UPDATE workspace SET status = 'Deleted'  WHERE status = 'deleted';
