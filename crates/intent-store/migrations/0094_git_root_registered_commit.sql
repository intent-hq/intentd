-- Registered-commit SHA on workspace git roots: the root's HEAD commit SHA
-- captured when the root was first registered (agent `ws.git.registerRoot` or
-- the sweep's submodule auto-detect). Immutable once set — merges never touch
-- it. NULL when HEAD was unreadable at registration, or for rows that predate
-- this column; the background sweep best-effort-backfills NULL rows with the
-- root's current HEAD (a going-forward boundary, guarded IS NULL).

ALTER TABLE workspace_git_root ADD COLUMN registered_commit_sha TEXT;
