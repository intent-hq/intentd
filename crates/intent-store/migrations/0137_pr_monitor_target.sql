-- A legacy slug is evidence to retain, not enough to choose a forge. Target
-- qualification is a later explicit, guarded write; no old column is changed.
ALTER TABLE pr_monitor ADD COLUMN target_provider TEXT;
ALTER TABLE pr_monitor ADD COLUMN target_instance_base_url TEXT;
ALTER TABLE pr_monitor ADD COLUMN target_project_path TEXT;
ALTER TABLE pr_monitor ADD COLUMN target_kind TEXT;
ALTER TABLE pr_monitor ADD COLUMN target_provenance TEXT NOT NULL DEFAULT 'unresolved'
  CHECK (target_provenance IN ('unresolved', 'captured-request', 'legacy-github-writer', 'validated-transfer'));
ALTER TABLE pr_monitor ADD COLUMN target_unresolved_reason TEXT DEFAULT 'missing-provenance'
  CHECK (
    (target_provenance = 'unresolved' AND target_unresolved_reason IS NOT NULL)
    OR
    (target_provenance != 'unresolved' AND target_unresolved_reason IS NULL
      AND target_provider IS NOT NULL AND target_provider IN ('github', 'gitlab')
      AND target_instance_base_url IS NOT NULL AND length(target_instance_base_url) > 0
      AND target_project_path IS NOT NULL AND length(target_project_path) > 0
      AND target_kind IS NOT NULL
      AND ((target_provider = 'github' AND target_kind IN ('pull-request', 'issue'))
        OR (target_provider = 'gitlab' AND target_kind IN ('merge-request', 'issue')))
      AND pr_number > 0)
  );

-- Keep the legacy GitHub comparison/refusal boundary without treating an
-- unresolved slug as a resolved target. Qualified GitLab/issue/other-instance
-- rows must not participate in these unqualified lookups or uniqueness rules.
DROP INDEX idx_pr_monitor_identity;
CREATE UNIQUE INDEX idx_pr_monitor_identity
  ON pr_monitor(agent_id, repo_owner COLLATE NOCASE, repo_name COLLATE NOCASE, pr_number)
  WHERE state = 'active' AND ((target_provenance = 'unresolved'
    AND target_provider IS NULL AND target_instance_base_url IS NULL
    AND target_project_path IS NULL AND target_kind IS NULL)
    OR (target_provenance != 'unresolved' AND target_provider = 'github' AND target_instance_base_url = 'https://github.com'
      AND target_kind = 'pull-request'));

DROP INDEX idx_pr_monitor_workspace_identity;
CREATE UNIQUE INDEX idx_pr_monitor_workspace_identity
  ON pr_monitor(workspace_id, repo_owner COLLATE NOCASE, repo_name COLLATE NOCASE, pr_number)
  WHERE state = 'active' AND ((target_provenance = 'unresolved'
    AND target_provider IS NULL AND target_instance_base_url IS NULL
    AND target_project_path IS NULL AND target_kind IS NULL)
    OR (target_provenance != 'unresolved' AND target_provider = 'github' AND target_instance_base_url = 'https://github.com'
      AND target_kind = 'pull-request'));

-- Imported target fields remain unverified claims. Keep distinct claims (e.g.
-- a GH PR and GL MR with the same old slug/number) without losing either pending
-- record. This raw, binary discriminator is not a canonical provider identity.
-- json_array retains NULL versus empty text without delimiter collisions.
CREATE UNIQUE INDEX idx_pr_monitor_unresolved_claim_identity
  ON pr_monitor(agent_id, repo_owner COLLATE NOCASE, repo_name COLLATE NOCASE, pr_number,
    json_array(target_provider, target_instance_base_url, target_project_path, target_kind))
  WHERE state = 'active' AND target_provenance = 'unresolved'
    AND (target_provider IS NOT NULL OR target_instance_base_url IS NOT NULL
      OR target_project_path IS NOT NULL OR target_kind IS NOT NULL);
CREATE UNIQUE INDEX idx_pr_monitor_unresolved_claim_workspace_identity
  ON pr_monitor(workspace_id, repo_owner COLLATE NOCASE, repo_name COLLATE NOCASE, pr_number,
    json_array(target_provider, target_instance_base_url, target_project_path, target_kind))
  WHERE state = 'active' AND target_provenance = 'unresolved'
    AND (target_provider IS NOT NULL OR target_instance_base_url IS NOT NULL
      OR target_project_path IS NOT NULL OR target_kind IS NOT NULL);

-- Provider-canonical GitLab paths retain their case. GitHub keeps its existing
-- ASCII case-insensitive comparison without rewriting the captured spelling.
CREATE UNIQUE INDEX idx_pr_monitor_qualified_identity
  ON pr_monitor(agent_id, target_provider, target_instance_base_url,
    CASE WHEN target_provider = 'github' THEN lower(target_project_path) ELSE target_project_path END,
    target_kind, pr_number)
  WHERE state = 'active' AND target_provenance != 'unresolved';
CREATE UNIQUE INDEX idx_pr_monitor_qualified_workspace_identity
  ON pr_monitor(workspace_id, target_provider, target_instance_base_url,
    CASE WHEN target_provider = 'github' THEN lower(target_project_path) ELSE target_project_path END,
    target_kind, pr_number)
  WHERE state = 'active' AND target_provenance != 'unresolved';
