-- Plain-folder workspaces (PROTOCOL §5.1 "Plain-folder workspaces",
-- `Workspace.plainFolder`): 1 marks a workspace that works directly in an
-- ordinary non-git folder. Written only at insert (`workspace.create
-- { plainFolder: true }`) and never updated. Additive only: 0 for every
-- existing row, so an older daemon ignores it.
ALTER TABLE workspace ADD COLUMN plain_folder INTEGER NOT NULL DEFAULT 0;
