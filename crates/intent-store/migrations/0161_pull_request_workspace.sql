-- Pull request <-> workspace links backing `workspace.findByPullRequest`
-- (PROTOCOL §5.1 "Pull request workspace lookup"). One row per (PR, workspace):
-- the triggers below record a link whenever a local workspace row, or one of
-- its git roots, persists a changed PR snapshot (`pr_url`,
-- `active_pull_request`, `pull_requests`). Recording in triggers covers every
-- present and future writer of those columns. Links are never removed by the
-- PR leaving a pool, by archive, or by git-root unregistration; they go with
-- the workspace (bounded delete sweep + FK cascade backstop).
--
-- `pr_key` is the canonical PR URL: lowercased, trailing slashes dropped,
-- leading `www.` host prefix removed, and only the exact
-- `http(s)://<host>/<owner>/<repo>/pull/<number>` shape is accepted (anything
-- else is skipped). The service normalizes a clicked URL to the same key.
-- `linked_at` is when this workspace was first linked; the lookup orders by it
-- (most recent first). No backfill: links start with this migration.

CREATE TABLE pull_request_workspace (
  pr_key       TEXT NOT NULL,
  workspace_id TEXT NOT NULL REFERENCES workspace(id) ON DELETE CASCADE,
  source       TEXT NOT NULL CHECK (source IN ('workspace', 'git_root')),
  linked_at    TEXT NOT NULL,
  PRIMARY KEY (pr_key, workspace_id)
);

CREATE INDEX idx_pull_request_workspace_workspace
  ON pull_request_workspace(workspace_id);

CREATE TRIGGER pull_request_workspace_from_workspace_insert
AFTER INSERT ON workspace
WHEN NEW.is_remote = 0
BEGIN
  INSERT OR IGNORE INTO pull_request_workspace (pr_key, workspace_id, source, linked_at)
  SELECT k, NEW.id, 'workspace', strftime('%Y-%m-%dT%H:%M:%fZ', 'now') FROM (
    SELECT CASE
      WHEN l LIKE 'https://www.%' THEN 'https://' || substr(l, 13)
      WHEN l LIKE 'http://www.%' THEN 'http://' || substr(l, 12)
      ELSE l END AS k
    FROM (
      SELECT rtrim(lower(trim(u)), '/') AS l FROM (
        SELECT NEW.pr_url AS u
        UNION ALL
        SELECT CASE WHEN json_valid(NEW.active_pull_request)
          THEN NEW.active_pull_request ->> '$.url' END
        UNION ALL
        SELECT je.value ->> '$.url' FROM json_each(
          CASE WHEN json_valid(NEW.pull_requests)
            AND json_type(NEW.pull_requests) = 'array'
            THEN NEW.pull_requests ELSE '[]' END) AS je
      ) WHERE u IS NOT NULL
    )
  ) WHERE (k GLOB 'https://*' OR k GLOB 'http://*')
    AND rtrim(k, '0123456789') GLOB '*/pull/' AND k NOT GLOB '*/pull/'
    AND length(k) - length(replace(k, '/', '')) = 6;
END;

CREATE TRIGGER pull_request_workspace_from_workspace_update
AFTER UPDATE OF pr_url, active_pull_request, pull_requests ON workspace
WHEN NEW.is_remote = 0 AND (
  NEW.pr_url IS NOT OLD.pr_url
  OR NEW.active_pull_request IS NOT OLD.active_pull_request
  OR NEW.pull_requests IS NOT OLD.pull_requests)
BEGIN
  INSERT OR IGNORE INTO pull_request_workspace (pr_key, workspace_id, source, linked_at)
  SELECT k, NEW.id, 'workspace', strftime('%Y-%m-%dT%H:%M:%fZ', 'now') FROM (
    SELECT CASE
      WHEN l LIKE 'https://www.%' THEN 'https://' || substr(l, 13)
      WHEN l LIKE 'http://www.%' THEN 'http://' || substr(l, 12)
      ELSE l END AS k
    FROM (
      SELECT rtrim(lower(trim(u)), '/') AS l FROM (
        SELECT NEW.pr_url AS u
        UNION ALL
        SELECT CASE WHEN json_valid(NEW.active_pull_request)
          THEN NEW.active_pull_request ->> '$.url' END
        UNION ALL
        SELECT je.value ->> '$.url' FROM json_each(
          CASE WHEN json_valid(NEW.pull_requests)
            AND json_type(NEW.pull_requests) = 'array'
            THEN NEW.pull_requests ELSE '[]' END) AS je
      ) WHERE u IS NOT NULL
    )
  ) WHERE (k GLOB 'https://*' OR k GLOB 'http://*')
    AND rtrim(k, '0123456789') GLOB '*/pull/' AND k NOT GLOB '*/pull/'
    AND length(k) - length(replace(k, '/', '')) = 6;
END;

CREATE TRIGGER pull_request_workspace_from_git_root_insert
AFTER INSERT ON workspace_git_root
WHEN EXISTS (SELECT 1 FROM workspace w WHERE w.id = NEW.workspace_id AND w.is_remote = 0)
BEGIN
  INSERT OR IGNORE INTO pull_request_workspace (pr_key, workspace_id, source, linked_at)
  SELECT k, NEW.workspace_id, 'git_root', strftime('%Y-%m-%dT%H:%M:%fZ', 'now') FROM (
    SELECT CASE
      WHEN l LIKE 'https://www.%' THEN 'https://' || substr(l, 13)
      WHEN l LIKE 'http://www.%' THEN 'http://' || substr(l, 12)
      ELSE l END AS k
    FROM (
      SELECT rtrim(lower(trim(u)), '/') AS l FROM (
        SELECT NEW.pr_url AS u
        UNION ALL
        SELECT je.value ->> '$.url' FROM json_each(
          CASE WHEN json_valid(NEW.pull_requests)
            AND json_type(NEW.pull_requests) = 'array'
            THEN NEW.pull_requests ELSE '[]' END) AS je
      ) WHERE u IS NOT NULL
    )
  ) WHERE (k GLOB 'https://*' OR k GLOB 'http://*')
    AND rtrim(k, '0123456789') GLOB '*/pull/' AND k NOT GLOB '*/pull/'
    AND length(k) - length(replace(k, '/', '')) = 6;
END;

CREATE TRIGGER pull_request_workspace_from_git_root_update
AFTER UPDATE OF pr_url, pull_requests ON workspace_git_root
WHEN (NEW.pr_url IS NOT OLD.pr_url OR NEW.pull_requests IS NOT OLD.pull_requests)
  AND EXISTS (SELECT 1 FROM workspace w WHERE w.id = NEW.workspace_id AND w.is_remote = 0)
BEGIN
  INSERT OR IGNORE INTO pull_request_workspace (pr_key, workspace_id, source, linked_at)
  SELECT k, NEW.workspace_id, 'git_root', strftime('%Y-%m-%dT%H:%M:%fZ', 'now') FROM (
    SELECT CASE
      WHEN l LIKE 'https://www.%' THEN 'https://' || substr(l, 13)
      WHEN l LIKE 'http://www.%' THEN 'http://' || substr(l, 12)
      ELSE l END AS k
    FROM (
      SELECT rtrim(lower(trim(u)), '/') AS l FROM (
        SELECT NEW.pr_url AS u
        UNION ALL
        SELECT je.value ->> '$.url' FROM json_each(
          CASE WHEN json_valid(NEW.pull_requests)
            AND json_type(NEW.pull_requests) = 'array'
            THEN NEW.pull_requests ELSE '[]' END) AS je
      ) WHERE u IS NOT NULL
    )
  ) WHERE (k GLOB 'https://*' OR k GLOB 'http://*')
    AND rtrim(k, '0123456789') GLOB '*/pull/' AND k NOT GLOB '*/pull/'
    AND length(k) - length(replace(k, '/', '')) = 6;
END;
