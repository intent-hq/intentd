-- Backfill `repository_name` from the `repository_path` basename for rows
-- created before workspace.create derived the name for local paths (parity
-- with the intent-services `known_repo_name` basename fallback). The substr/
-- rtrim/replace expression strips everything up to and including the last `/`:
-- `replace(path, '/', '')` is the set of the path's non-slash characters, so
-- `rtrim(path, ...)` trims trailing characters back to the last `/`. Only rows
-- with a NULL/empty name, a repository_path, and a non-empty derived basename
-- are touched; caller-supplied names are never overwritten.

UPDATE workspace
SET repository_name = substr(
        repository_path,
        length(rtrim(repository_path, replace(repository_path, '/', ''))) + 1
    )
WHERE (repository_name IS NULL OR repository_name = '')
  AND repository_path IS NOT NULL
  AND substr(
        repository_path,
        length(rtrim(repository_path, replace(repository_path, '/', ''))) + 1
      ) <> '';
