-- Provisioning mode of the workspace checkout ('worktree' | 'cow'), set by
-- workspace.create. NULL for rows without a daemon-provisioned checkout
-- (skipWorktree, remote, caller-supplied worktreePath, non-git repo paths)
-- and for rows created before this column existed.
ALTER TABLE workspace ADD COLUMN checkout_mode TEXT;
