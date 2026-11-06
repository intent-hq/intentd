-- Plain-folder data safety (PROTOCOL §5.1 "Plain-folder workspaces": the
-- folder is never deleted by Intent). The delete tombstone remembers a
-- deleted plain-folder workspace's folder (`repository_path`), so the
-- id-derived `<root>/<id>` cleanup sweeps keep skipping it after the row is
-- gone — a retried delete, or another workspace whose id matches the folder
-- name. NULL for every other tombstone. Additive only: an older daemon
-- ignores the column.
ALTER TABLE deleted_workspace_id ADD COLUMN plain_folder_path TEXT;
