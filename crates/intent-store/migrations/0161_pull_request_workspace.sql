-- Pull request <-> workspace links backing `workspace.findByPullRequest`
-- (PROTOCOL §5.1 "Pull request workspace lookup"). One row per (PR, workspace).
-- The triggers below record a link whenever a local workspace row, or one of
-- its git roots, persists a changed PR snapshot (`pr_url`,
-- `active_pull_request`, `pull_requests`), which covers every present and
-- future writer of those columns. Links are never removed by the PR leaving a
-- pool, by archive, or by git-root unregistration; they go with the workspace
-- (bounded delete sweep + FK cascade backstop) and ride workspace transfers.
--
-- `pr_key` is `<host>/<owner>/<repo>/pull/<number>`: scheme dropped, lowercased,
-- trailing slashes and a leading `www.` host prefix removed, number without
-- leading zeros, and nothing else accepted. `intent_core::pull_request_key`
-- produces the same key for a clicked URL; the intent-store parity test keeps
-- the two in step. `linked_at` is when this workspace was first linked; the
-- lookup orders by it (most recent first). No backfill: links start here.

CREATE TABLE pull_request_workspace (
  pr_key       TEXT NOT NULL,
  workspace_id TEXT NOT NULL REFERENCES workspace(id) ON DELETE CASCADE,
  source       TEXT NOT NULL CHECK (source IN ('workspace', 'git_root')),
  linked_at    TEXT NOT NULL,
  PRIMARY KEY (pr_key, workspace_id)
);

CREATE INDEX idx_pull_request_workspace_workspace
  ON pull_request_workspace(workspace_id);

-- Write-only funnel holding the single copy of the URL -> key normalization.
-- Its BEFORE INSERT trigger records the link and then RAISE(IGNORE)s the row,
-- so this table never stores anything; the source triggers just feed it.
CREATE TABLE pull_request_workspace_candidate (
  url          TEXT,
  workspace_id TEXT NOT NULL,
  source       TEXT NOT NULL
);

CREATE TRIGGER pull_request_workspace_candidate_record
BEFORE INSERT ON pull_request_workspace_candidate
BEGIN
  INSERT OR IGNORE INTO pull_request_workspace (pr_key, workspace_id, source, linked_at)
  SELECT k, NEW.workspace_id, NEW.source, strftime('%Y-%m-%dT%H:%M:%fZ', 'now') FROM (
    SELECT CASE WHEN s LIKE 'www.%' THEN substr(s, 5) ELSE s END AS k FROM (
      SELECT CASE
        WHEN l LIKE 'https://%' THEN substr(l, 9)
        WHEN l LIKE 'http://%' THEN substr(l, 8)
        END AS s
      FROM (SELECT rtrim(lower(trim(NEW.url)), '/') AS l)
    )
  ) WHERE k IS NOT NULL
    AND length(k) - length(replace(k, '/', '')) = 4
    AND substr(k, 1, 1) <> '/'
    AND instr(k, '//') = 0
    AND instr(k, '@') = 0
    AND instr(k, '?') = 0
    AND instr(k, '#') = 0
    AND rtrim(k, '0123456789') GLOB '*/pull/'
    AND k NOT GLOB '*/pull/'
    AND k NOT GLOB '*/pull/0*';
  SELECT RAISE(IGNORE);
END;

CREATE TRIGGER pull_request_workspace_from_workspace_insert
AFTER INSERT ON workspace
WHEN NEW.is_remote = 0
BEGIN
  INSERT INTO pull_request_workspace_candidate (url, workspace_id, source)
  SELECT u, NEW.id, 'workspace' FROM (
    SELECT NEW.pr_url AS u
    UNION ALL
    SELECT CASE WHEN json_valid(NEW.active_pull_request)
      THEN NEW.active_pull_request ->> '$.url' END
    UNION ALL
    SELECT je.value ->> '$.url' FROM json_each(
      CASE WHEN json_valid(NEW.pull_requests) AND json_type(NEW.pull_requests) = 'array'
        THEN NEW.pull_requests ELSE '[]' END) AS je
  ) WHERE u IS NOT NULL;
END;

CREATE TRIGGER pull_request_workspace_from_workspace_update
AFTER UPDATE OF pr_url, active_pull_request, pull_requests ON workspace
WHEN NEW.is_remote = 0 AND (
  NEW.pr_url IS NOT OLD.pr_url
  OR NEW.active_pull_request IS NOT OLD.active_pull_request
  OR NEW.pull_requests IS NOT OLD.pull_requests)
BEGIN
  INSERT INTO pull_request_workspace_candidate (url, workspace_id, source)
  SELECT u, NEW.id, 'workspace' FROM (
    SELECT NEW.pr_url AS u
    UNION ALL
    SELECT CASE WHEN json_valid(NEW.active_pull_request)
      THEN NEW.active_pull_request ->> '$.url' END
    UNION ALL
    SELECT je.value ->> '$.url' FROM json_each(
      CASE WHEN json_valid(NEW.pull_requests) AND json_type(NEW.pull_requests) = 'array'
        THEN NEW.pull_requests ELSE '[]' END) AS je
  ) WHERE u IS NOT NULL;
END;

CREATE TRIGGER pull_request_workspace_from_git_root_insert
AFTER INSERT ON workspace_git_root
WHEN EXISTS (SELECT 1 FROM workspace w WHERE w.id = NEW.workspace_id AND w.is_remote = 0)
BEGIN
  INSERT INTO pull_request_workspace_candidate (url, workspace_id, source)
  SELECT u, NEW.workspace_id, 'git_root' FROM (
    SELECT NEW.pr_url AS u
    UNION ALL
    SELECT je.value ->> '$.url' FROM json_each(
      CASE WHEN json_valid(NEW.pull_requests) AND json_type(NEW.pull_requests) = 'array'
        THEN NEW.pull_requests ELSE '[]' END) AS je
  ) WHERE u IS NOT NULL;
END;

CREATE TRIGGER pull_request_workspace_from_git_root_update
AFTER UPDATE OF pr_url, pull_requests ON workspace_git_root
WHEN (NEW.pr_url IS NOT OLD.pr_url OR NEW.pull_requests IS NOT OLD.pull_requests)
  AND EXISTS (SELECT 1 FROM workspace w WHERE w.id = NEW.workspace_id AND w.is_remote = 0)
BEGIN
  INSERT INTO pull_request_workspace_candidate (url, workspace_id, source)
  SELECT u, NEW.workspace_id, 'git_root' FROM (
    SELECT NEW.pr_url AS u
    UNION ALL
    SELECT je.value ->> '$.url' FROM json_each(
      CASE WHEN json_valid(NEW.pull_requests) AND json_type(NEW.pull_requests) = 'array'
        THEN NEW.pull_requests ELSE '[]' END) AS je
  ) WHERE u IS NOT NULL;
END;
