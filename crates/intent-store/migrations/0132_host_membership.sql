-- Host grants are independent of workspace collaborators. Upgrading does
-- not promote existing guests, change the primary, or rewrite invite history.
CREATE TABLE host_membership_state (
  id INTEGER PRIMARY KEY CHECK (id = 1),
  revision INTEGER NOT NULL DEFAULT 0 CHECK (revision >= 0),
  member_count INTEGER NOT NULL DEFAULT 0 CHECK (member_count >= 0),
  authorization_generation INTEGER NOT NULL DEFAULT 0 CHECK (authorization_generation >= 0)
);
INSERT INTO host_membership_state (id) VALUES (1);

CREATE TABLE host_member (
  principal_id TEXT PRIMARY KEY REFERENCES principal(id) ON DELETE CASCADE,
  added_at TEXT NOT NULL
);
CREATE INDEX host_member_order_idx ON host_member (added_at, principal_id);

CREATE TRIGGER host_member_no_primary_bi BEFORE INSERT ON host_member
WHEN EXISTS (SELECT 1 FROM principal WHERE id = NEW.principal_id AND is_primary = 1)
BEGIN
  SELECT RAISE(ABORT, 'the primary principal is already the host owner');
END;
CREATE TRIGGER host_member_no_primary_bu BEFORE UPDATE OF principal_id ON host_member
WHEN EXISTS (SELECT 1 FROM principal WHERE id = NEW.principal_id AND is_primary = 1)
BEGIN
  SELECT RAISE(ABORT, 'the primary principal is already the host owner');
END;
CREATE TRIGGER host_member_added_ai AFTER INSERT ON host_member
BEGIN
  UPDATE host_membership_state SET revision = revision + 1, member_count = member_count + 1 WHERE id = 1;
END;
CREATE TRIGGER host_member_removed_ad AFTER DELETE ON host_member
BEGIN
  UPDATE host_membership_state SET revision = revision + 1, member_count = member_count - 1 WHERE id = 1;
END;

-- Retained after removal so a proof issued before revocation cannot recreate
-- credentials. The generation is allocated by the removal transaction.
CREATE TABLE principal_revocation (
  principal_id TEXT PRIMARY KEY REFERENCES principal(id) ON DELETE CASCADE,
  generation INTEGER NOT NULL CHECK (generation > 0)
);

-- A separate table makes the grant scope structural: no fake workspace and
-- no widening/nulling the existing workspace invite foreign key.
CREATE TABLE host_invite (
  id TEXT PRIMARY KEY,
  secret_hash TEXT NOT NULL UNIQUE,
  secret TEXT,
  created_by_principal_id TEXT NOT NULL REFERENCES principal(id),
  pin_identity_provider TEXT NOT NULL,
  pin_instance_host TEXT NOT NULL,
  pin_external_user_id TEXT NOT NULL,
  pin_login TEXT NOT NULL CHECK (length(trim(pin_login)) > 0),
  created_at TEXT NOT NULL,
  expires_at TEXT NOT NULL,
  redeemed_at TEXT,
  redeemed_by_principal_id TEXT REFERENCES principal(id),
  revoked_at TEXT,
  redemption_count INTEGER NOT NULL DEFAULT 0 CHECK (redemption_count IN (0, 1)),
  CHECK ((redemption_count = 0 AND redeemed_at IS NULL AND redeemed_by_principal_id IS NULL)
      OR (redemption_count = 1 AND redeemed_at IS NOT NULL AND redeemed_by_principal_id IS NOT NULL))
);
CREATE INDEX host_invite_order_idx ON host_invite (created_at, id);
CREATE INDEX workspace_invite_issuer_idx ON workspace_invite (created_by_principal_id);
