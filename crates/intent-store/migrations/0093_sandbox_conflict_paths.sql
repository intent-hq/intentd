-- Terminal 'conflict' sandbox state (isolation-lab follow-up): a merge that
-- hits deterministic conflicts with no live agent turn to bounce (retry cap
-- exhausted on the completion path, any sweep conflict, or a manual merge)
-- now lands in status 'conflict' with the conflicting paths persisted, so
-- clients and coordinators can see WHAT conflicted without re-running the
-- merge. Paths are a JSON string array; meaningful only while status is
-- 'conflict' (a later manual merge attempt supersedes them).

ALTER TABLE sandbox ADD COLUMN conflicting_paths TEXT;
