-- Workspace invite links (multiplayer w4). An owner mints a single-use,
-- expiring invite; the invitee proves a GitHub identity through an
-- identity-only device flow and is added as a `collaborator`. The link
-- secret is stored only as its hex SHA-256; an optional pin restricts
-- redemption to one GitHub account and is stored as the stable account id
-- (the login is kept only for display). A redeemed or revoked invite keeps
-- its row so a replayed link is recognisably closed rather than unknown.
CREATE TABLE workspace_invite (
  id                        TEXT PRIMARY KEY,
  workspace_id              TEXT NOT NULL REFERENCES workspace(id) ON DELETE CASCADE,
  secret_hash               TEXT NOT NULL UNIQUE,
  created_by_principal_id   TEXT NOT NULL REFERENCES principal(id) ON DELETE CASCADE,
  pin_github_user_id        INTEGER,
  pin_login                 TEXT,
  created_at                TEXT NOT NULL,
  expires_at                TEXT NOT NULL,
  redeemed_at               TEXT,
  redeemed_by_principal_id  TEXT REFERENCES principal(id) ON DELETE SET NULL,
  revoked_at                TEXT
);

CREATE INDEX workspace_invite_workspace_idx ON workspace_invite (workspace_id);
