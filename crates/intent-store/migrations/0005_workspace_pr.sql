-- Workspace ↔ PR linkage metadata (§7.6). Adds the persisted PR snapshot
-- columns to `workspace`; 0001-0004 are untouched. `pr_number`/`pr_url` already
-- exist (0001); these add the lifecycle status and the full active-PR snapshot
-- so the background refresh can persist and diff linked PRs and emit `pr:*`
-- events. `pr_status` stores the PascalCase `PullRequestStatus` wire word
-- (`Open`/`Closed`/`Merged`/`Draft`); `active_pull_request` stores the
-- serialized `PullRequestInfo` JSON (NULL when the workspace has no linked PR).

ALTER TABLE workspace ADD COLUMN pr_status TEXT;
ALTER TABLE workspace ADD COLUMN active_pull_request TEXT;
