-- Tombstones for deleted workspace ids. `workspace.create` consults this table
-- (plus live rows and on-disk directories) so a slug id is never recycled
-- across delete/recreate — reusing an id would collide the old workspace's
-- agent streams, IPC scopes, and file paths with the new one's (FE
-- `recentlyDeletedWorkspaces` parity, persisted across daemon restarts).
CREATE TABLE deleted_workspace_id (
  id         TEXT PRIMARY KEY,
  deleted_at TEXT NOT NULL
);
