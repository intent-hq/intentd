-- Backfill `repository_name` from the `repository_path` basename for rows
-- created before workspace.create derived the name for local paths (parity
-- with the intent-services `derive_repo_name_from_path` basename fallback).
-- Windows-style `\` separators are normalized to `/` first, mirroring the
-- Rust helper's split on both separators. The substr/rtrim/replace expression
-- then strips everything up to and including the last `/`:
-- `replace(norm, '/', '')` is the set of the path's non-slash characters, so
-- `rtrim(norm, ...)` trims trailing characters back to the last `/`. Only rows
-- with a NULL/empty name, a repository_path, and a non-empty derived basename
-- are touched; caller-supplied names are never overwritten.

UPDATE workspace
SET repository_name = substr(
        replace(repository_path, '\', '/'),
        length(rtrim(
            replace(repository_path, '\', '/'),
            replace(replace(repository_path, '\', '/'), '/', '')
        )) + 1
    )
WHERE (repository_name IS NULL OR repository_name = '')
  AND repository_path IS NOT NULL
  AND substr(
        replace(repository_path, '\', '/'),
        length(rtrim(
            replace(repository_path, '\', '/'),
            replace(replace(repository_path, '\', '/'), '/', '')
        )) + 1
      ) <> '';
