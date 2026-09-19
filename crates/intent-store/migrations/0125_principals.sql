-- Principals, workspace membership and per-principal bearer credentials
-- (multiplayer w1). A `principal` is a person, identified by their GitHub
-- account once linked; the daemon's existing single user is minted here as
-- the `is_primary` principal (GitHub identity NULL until the auth flow links
-- it) and becomes the owner of every workspace that already exists.
--
-- The legacy file bearer token (`FileTokenStore`) is deliberately NOT copied
-- into `principal_credential`: it stays the primary user's credential and is
-- honored by the service layer alongside the hashed rows stored here.

CREATE TABLE principal (
  id              TEXT PRIMARY KEY,
  github_user_id  INTEGER UNIQUE,
  login           TEXT,
  display_name    TEXT,
  avatar_url      TEXT,
  is_primary      INTEGER NOT NULL DEFAULT 0 CHECK (is_primary IN (0, 1)),
  created_at      TEXT NOT NULL,
  updated_at      TEXT NOT NULL
);

-- Exactly one primary principal per daemon.
CREATE UNIQUE INDEX principal_primary_uq ON principal (is_primary) WHERE is_primary = 1;

-- Membership: who may open a workspace and in which role. Forward-looking
-- per-member flags (e.g. invitation state, notification opt-outs) are added
-- as nullable columns by later migrations; none are defined yet.
CREATE TABLE workspace_member (
  workspace_id  TEXT NOT NULL REFERENCES workspace(id) ON DELETE CASCADE,
  principal_id  TEXT NOT NULL REFERENCES principal(id) ON DELETE CASCADE,
  role          TEXT NOT NULL CHECK (role IN ('owner', 'collaborator')),
  added_at      TEXT NOT NULL,
  PRIMARY KEY (workspace_id, principal_id)
);

CREATE INDEX workspace_member_principal_idx ON workspace_member (principal_id);

-- Bearer credentials, stored only as the hex SHA-256 of the presented token.
-- A revoked credential keeps its row (`revoked_at` set) so a replayed token
-- is recognisably revoked rather than merely unknown.
CREATE TABLE principal_credential (
  token_hash    TEXT PRIMARY KEY,
  principal_id  TEXT NOT NULL REFERENCES principal(id) ON DELETE CASCADE,
  created_at    TEXT NOT NULL,
  last_used_at  TEXT,
  revoked_at    TEXT
);

CREATE INDEX principal_credential_principal_idx ON principal_credential (principal_id);

-- `owner_principal_id`: the workspace's owner (mirrors the `owner` membership
-- row; kept on the workspace for cheap reads). `legacy_author_principal_id`:
-- the principal that pre-multiplayer, unattributed content in this workspace
-- is credited to — set to the primary principal for workspaces that predate
-- this migration and NULL for workspaces created afterwards. Neither column
-- carries a REFERENCES clause: transfer import nulls both, and the trigger
-- below re-derives the owner on the target daemon.
ALTER TABLE workspace ADD COLUMN legacy_author_principal_id TEXT;
ALTER TABLE workspace ADD COLUMN owner_principal_id TEXT;

-- Mint the primary principal (idempotent via the partial unique index on a
-- re-run against a database that already has one).
INSERT INTO principal (id, github_user_id, login, display_name, avatar_url, is_primary, created_at, updated_at)
SELECT
  lower(
    hex(randomblob(4)) || '-' || hex(randomblob(2)) || '-4' || substr(hex(randomblob(2)), 2)
    || '-' || substr('89ab', (abs(random()) % 4) + 1, 1) || substr(hex(randomblob(2)), 2)
    || '-' || hex(randomblob(6))
  ),
  NULL, NULL, NULL, NULL, 1,
  strftime('%Y-%m-%dT%H:%M:%fZ', 'now'),
  strftime('%Y-%m-%dT%H:%M:%fZ', 'now')
WHERE NOT EXISTS (SELECT 1 FROM principal WHERE is_primary = 1);

-- Every existing workspace: primary principal is owner and legacy author.
UPDATE workspace
SET owner_principal_id = (SELECT id FROM principal WHERE is_primary = 1)
WHERE owner_principal_id IS NULL;

UPDATE workspace
SET legacy_author_principal_id = (SELECT id FROM principal WHERE is_primary = 1)
WHERE legacy_author_principal_id IS NULL;

INSERT OR IGNORE INTO workspace_member (workspace_id, principal_id, role, added_at)
SELECT w.id, p.id, 'owner', strftime('%Y-%m-%dT%H:%M:%fZ', 'now')
FROM workspace w, principal p
WHERE p.is_primary = 1;

-- New workspaces default to the primary principal as owner unless the insert
-- names one explicitly; either way the matching `owner` membership row is
-- created in the same statement so membership and the column never diverge.
CREATE TRIGGER workspace_owner_default_ai
AFTER INSERT ON workspace
BEGIN
  UPDATE workspace
  SET owner_principal_id = (SELECT id FROM principal WHERE is_primary = 1)
  WHERE id = NEW.id AND owner_principal_id IS NULL;

  INSERT OR IGNORE INTO workspace_member (workspace_id, principal_id, role, added_at)
  SELECT NEW.id, p.id, 'owner', strftime('%Y-%m-%dT%H:%M:%fZ', 'now')
  FROM principal p
  WHERE p.id = COALESCE(NEW.owner_principal_id, (SELECT id FROM principal WHERE is_primary = 1));
END;
