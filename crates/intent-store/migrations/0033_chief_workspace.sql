-- Seed the daemon-known virtual "Chief of Staff" workspace (TS
-- `CHIEF_WORKSPACE_ID` = '__chief__' in `shared/types/branded-ids.ts`). Chief
-- has no repository/worktree on disk; the row exists only so `agent_session`'s
-- foreign key (`workspace_id REFERENCES workspace(id)`, §9.2) is satisfied when
-- Chief-of-Staff agents persist. The service layer synthesizes the wire shape
-- on read (title/branch/timestamps pinned; card aggregates omitted) and
-- filters Chief out of `workspace.list` — this row is never returned to
-- clients directly. `INSERT OR IGNORE` keeps the migration idempotent across
-- daemon restarts / re-migrations.
INSERT OR IGNORE INTO workspace (
  id, title, branch, status, attention, skip_worktree, is_remote,
  archived, tags, created_at, updated_at
) VALUES (
  '__chief__',
  'Chief of Staff',
  '',
  'Active',
  'none',
  0,
  0,
  0,
  '[]',
  '2026-01-01T00:00:00.000Z',
  '2026-01-01T00:00:00.000Z'
);
