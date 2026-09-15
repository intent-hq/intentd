-- Forge repo slugs are case-insensitive (`Intent-HQ/IntentD` is the same
-- repository as `intent-hq/intentd`), so both PR-monitor identities compare
-- `repo_owner` / `repo_name` with `COLLATE NOCASE`. Stored values keep the
-- caller's casing; only the comparison changes.

-- Dedupe pre-existing case-variant duplicates before the stricter indexes
-- can be created: for each (workspace, repo, PR) with more than one ACTIVE
-- row under NOCASE, the oldest row (`created_at`, then `monitor_id` as the
-- tiebreak) keeps the watch and the rest become `cancelled`. Per-agent
-- duplicates are a subset of per-workspace duplicates, so one pass covers
-- both indexes.
UPDATE pr_monitor
SET state = 'cancelled',
    updated_at = strftime('%Y-%m-%dT%H:%M:%fZ', 'now')
WHERE state = 'active'
  AND monitor_id NOT IN (
    SELECT monitor_id FROM (
      SELECT monitor_id,
             ROW_NUMBER() OVER (
               PARTITION BY workspace_id,
                            lower(repo_owner),
                            lower(repo_name),
                            pr_number
               ORDER BY created_at, monitor_id
             ) AS rank
      FROM pr_monitor
      WHERE state = 'active'
    )
    WHERE rank = 1
  );

DROP INDEX IF EXISTS idx_pr_monitor_identity;
CREATE UNIQUE INDEX idx_pr_monitor_identity
  ON pr_monitor(agent_id, repo_owner COLLATE NOCASE, repo_name COLLATE NOCASE, pr_number)
  WHERE state = 'active';

DROP INDEX IF EXISTS idx_pr_monitor_workspace_identity;
CREATE UNIQUE INDEX idx_pr_monitor_workspace_identity
  ON pr_monitor(workspace_id, repo_owner COLLATE NOCASE, repo_name COLLATE NOCASE, pr_number)
  WHERE state = 'active';
