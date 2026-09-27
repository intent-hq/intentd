-- Data for the REAL pre-B schema created by 0085_pr_monitor.sql and
-- 0089_pr_monitor_baseline.sql. There are deliberately no provider,
-- instance, connection, target or new migration columns here.
--
-- Registration at intentd 56c7cfe3 (pr_monitor.rs) captured repo_owner,
-- repo_name and pr_number before writing this table. The production
-- registry only built GitHubSourceControl (registry.rs); current workspace
-- remotes were never part of that captured monitor identity.
INSERT INTO workspace (id) VALUES ('workspace-before-migration');
INSERT INTO agent_session (id) VALUES ('legacy-agent');

INSERT INTO pr_monitor (
    monitor_id, workspace_id, agent_id, repo_owner, repo_name, pr_number,
    state, last_snapshot, baseline_snapshot, pending_changes, pending_since,
    last_change_at, last_polled_at, last_error, created_at, updated_at
) VALUES (
    'prmon-legacy-pending', 'workspace-before-migration', 'legacy-agent',
    'Intent-HQ', 'Example', 42, 'active',
    '{"title":"Original GitHub review","url":"https://github.com/Intent-HQ/Example/pull/42","conversationCount":2,"reviewCommentCount":0,"requirements":{"state":"open","isDraft":false,"hasConflicts":false,"isBehind":false,"checks":{"total":0,"passed":0,"failed":0,"pending":0,"items":[],"failingRequired":[],"pendingRequired":[],"requiredKnown":false},"approvals":{"decision":"none","have":0,"changesRequested":0},"threads":{},"rulesKnown":false}}',
    '{"title":"Original GitHub review","url":"https://github.com/Intent-HQ/Example/pull/42","conversationCount":1,"reviewCommentCount":0,"requirements":{"state":"open","isDraft":false,"hasConflicts":false,"isBehind":false,"checks":{"total":0,"passed":0,"failed":0,"pending":0,"items":[],"failingRequired":[],"pendingRequired":[],"requiredKnown":false},"approvals":{"decision":"none","have":0,"changesRequested":0},"threads":{},"rulesKnown":false}}',
    '["Conversation comments: 1 → 2"]',
    '2026-09-20T12:00:01Z', '2026-09-20T12:00:02Z',
    '2026-09-20T12:00:02Z', 'temporary network failure',
    '2026-09-20T12:00:00Z', '2026-09-20T12:00:02Z'
);

-- An old incomplete row must stay inspectable/cancellable by its owner.
-- A current GitLab origin does not fill in its missing captured project.
INSERT INTO pr_monitor (
    monitor_id, workspace_id, agent_id, repo_owner, repo_name, pr_number,
    state, pending_changes, pending_since, created_at, updated_at
) VALUES (
    'prmon-legacy-unresolved', 'workspace-before-migration', 'legacy-agent',
    '', 'Example', 42, 'active', '["Unsent change"]',
    '2026-09-20T12:00:01Z', '2026-09-20T12:00:00Z', '2026-09-20T12:00:02Z'
);
