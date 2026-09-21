-- One ACTIVE monitor per (workspace, repo, PR): a workspace allows at most
-- one agent to monitor a given pull request, so a second agent's register is
-- refused (naming the owner) instead of mounting a duplicate that wakes twice
-- for every change. The per-agent `idx_pr_monitor_identity` stays for the
-- owner's idempotent re-register lookup.

-- Dedupe pre-existing duplicates before the index can be created: for each
-- (workspace, repo, PR) with more than one ACTIVE row, the oldest row
-- (`created_at`, then `monitor_id` as the tiebreak) keeps the watch and the
-- rest become `cancelled` (terminal, excluded from list surfaces).
UPDATE pr_monitor
SET state = 'cancelled',
    updated_at = strftime('%Y-%m-%dT%H:%M:%fZ', 'now')
WHERE state = 'active'
  AND monitor_id NOT IN (
    SELECT monitor_id FROM (
      SELECT monitor_id,
             ROW_NUMBER() OVER (
               PARTITION BY workspace_id, repo_owner, repo_name, pr_number
               ORDER BY created_at, monitor_id
             ) AS rank
      FROM pr_monitor
      WHERE state = 'active'
    )
    WHERE rank = 1
  );

CREATE UNIQUE INDEX idx_pr_monitor_workspace_identity
  ON pr_monitor(workspace_id, repo_owner, repo_name, pr_number)
  WHERE state = 'active';
