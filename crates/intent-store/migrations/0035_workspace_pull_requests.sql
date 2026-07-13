-- Workspace ↔ PR list column (§7.6). Adds the persisted list of PR snapshots
-- known for the workspace's baseRef, alongside the `active_pull_request` scalar
-- (0005). Stored as a JSON TEXT column holding the serialized
-- `Vec<PullRequestInfo>`. NULL denotes "never populated by the daemon" (legacy
-- rows and freshly created workspaces); `"[]"` is the distinct, explicit
-- "no discovered PRs" state written when `workspace.update` sets the field
-- to an empty list.

ALTER TABLE workspace ADD COLUMN pull_requests TEXT;
