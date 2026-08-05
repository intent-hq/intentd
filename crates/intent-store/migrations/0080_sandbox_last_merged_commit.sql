-- Sandboxes are persistent for the agent's lifetime: merge-on-completion no
-- longer discards the sandbox, so repeat merges need the tip of the last
-- successfully merged range to compute the next incremental range
-- (last_merged..branch tip) instead of re-applying from base/snapshot.

ALTER TABLE sandbox ADD COLUMN last_merged_commit_sha TEXT;
