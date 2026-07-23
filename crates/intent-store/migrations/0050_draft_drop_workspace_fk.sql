-- PROTOCOL §5.16 "Opaque keys & reserved sentinels": draft keys are opaque —
-- the daemon never validates `workspace_id` / `agent_id` against live
-- workspaces or agents. The FE saves the New Workspace modal's pre-creation
-- draft under the `__new-workspace__` / `__initializer__` sentinel pair before
-- any workspace row exists, so the 0007 `workspace_id REFERENCES workspace(id)
-- ON DELETE CASCADE` clause wrongly rejected those writes. Rebuild `draft`
-- without it (SQLite cannot drop an FK in place), preserving the
-- `(workspace_id, agent_id, client_id)` PK, the `client_id` FK, the 0048
-- `attachments` column, `idx_draft_client`, and all existing rows. The
-- workspace-delete cleanup the cascade used to provide moves to an explicit
-- `DELETE FROM draft` in `delete_workspace`.

PRAGMA foreign_keys = OFF;

CREATE TABLE draft_new (
  workspace_id TEXT NOT NULL,                      -- opaque key; sentinels allowed (§5.16)
  agent_id     TEXT NOT NULL,                      -- target agent/session (opaque)
  client_id    TEXT NOT NULL REFERENCES client(id) ON DELETE CASCADE,
  text         TEXT NOT NULL,
  updated_at   TEXT NOT NULL,
  attachments  TEXT,
  PRIMARY KEY (workspace_id, agent_id, client_id)
);

INSERT INTO draft_new (workspace_id, agent_id, client_id, text, updated_at, attachments)
SELECT workspace_id, agent_id, client_id, text, updated_at, attachments FROM draft;

DROP TABLE draft;
ALTER TABLE draft_new RENAME TO draft;

CREATE INDEX idx_draft_client ON draft(client_id);

PRAGMA foreign_keys = ON;
