-- Local authority continuity, independent of host invite/revocation generation.
-- Tombstones deliberately have no foreign keys: cascades and same-ID recreation
-- must not reset a previously observed key. This table is not transferred between
-- daemons, and is not a grant or a replacement for existing permission checks.
-- Bootstrap current rows only; pre-upgrade admissions cannot claim this evidence.
CREATE TABLE repository_authority_revision (
  kind TEXT NOT NULL CHECK (kind IN ('workspace', 'principal', 'workspace_member', 'host_member', 'credential')),
  subject_id TEXT NOT NULL,
  member_id TEXT NOT NULL DEFAULT '',
  revision INTEGER NOT NULL CHECK (typeof(revision) = 'integer' AND revision > 0),
  PRIMARY KEY (kind, subject_id, member_id)
) WITHOUT ROWID;

-- Explicit RAISE checks are intentional: an outer INSERT OR IGNORE/REPLACE must
-- not override a constraint and commit an authority effect without provenance.
CREATE TRIGGER repository_authority_revision_no_delete BEFORE DELETE ON repository_authority_revision
BEGIN
  SELECT RAISE(ABORT, 'repository authority tombstone cannot be deleted');
END;
CREATE TRIGGER repository_authority_revision_no_reset BEFORE INSERT ON repository_authority_revision
WHEN EXISTS (SELECT 1 FROM repository_authority_revision
             WHERE kind = NEW.kind AND subject_id = NEW.subject_id AND member_id = NEW.member_id)
BEGIN
  SELECT RAISE(ABORT, 'repository authority tombstone cannot be replaced');
END;
CREATE TRIGGER repository_authority_revision_monotonic BEFORE UPDATE ON repository_authority_revision
WHEN NEW.kind IS NOT OLD.kind OR NEW.subject_id IS NOT OLD.subject_id OR NEW.member_id IS NOT OLD.member_id
  OR typeof(NEW.revision) <> 'integer' OR NEW.revision <= OLD.revision
BEGIN
  SELECT RAISE(ABORT, 'repository authority revision must increase without overflow');
END;

INSERT INTO repository_authority_revision (kind, subject_id, member_id, revision)
SELECT 'workspace', id, '', 1 FROM workspace;
INSERT INTO repository_authority_revision (kind, subject_id, member_id, revision)
SELECT 'principal', id, '', 1 FROM principal;
INSERT INTO repository_authority_revision (kind, subject_id, member_id, revision)
SELECT 'workspace_member', workspace_id, principal_id, 1 FROM workspace_member;
INSERT INTO repository_authority_revision (kind, subject_id, member_id, revision)
SELECT 'host_member', principal_id, '', 1 FROM host_member;
INSERT INTO repository_authority_revision (kind, subject_id, member_id, revision)
SELECT 'credential', token_hash, '', 1 FROM principal_credential;

-- workspace: insert/delete always retire; only meaningful updates advance.
CREATE TRIGGER repository_authority_workspace_ai AFTER INSERT ON workspace
BEGIN
  SELECT RAISE(ABORT, 'repository authority revision exhausted')
  FROM repository_authority_revision WHERE kind = 'workspace' AND subject_id = NEW.id AND member_id = ''
    AND (typeof(revision) <> 'integer' OR revision <= 0 OR revision = 9223372036854775807);
  UPDATE repository_authority_revision SET revision = revision + 1
  WHERE kind = 'workspace' AND subject_id = NEW.id AND member_id = '';
  INSERT INTO repository_authority_revision (kind, subject_id, member_id, revision)
  SELECT 'workspace', NEW.id, '', 1
  WHERE NOT EXISTS (SELECT 1 FROM repository_authority_revision WHERE kind = 'workspace' AND subject_id = NEW.id AND member_id = '');
END;
CREATE TRIGGER repository_authority_workspace_ad AFTER DELETE ON workspace
BEGIN
  SELECT RAISE(ABORT, 'repository authority revision exhausted')
  FROM repository_authority_revision WHERE kind = 'workspace' AND subject_id = OLD.id AND member_id = ''
    AND (typeof(revision) <> 'integer' OR revision <= 0 OR revision = 9223372036854775807);
  UPDATE repository_authority_revision SET revision = revision + 1
  WHERE kind = 'workspace' AND subject_id = OLD.id AND member_id = '';
  INSERT INTO repository_authority_revision (kind, subject_id, member_id, revision)
  SELECT 'workspace', OLD.id, '', 1
  WHERE NOT EXISTS (SELECT 1 FROM repository_authority_revision WHERE kind = 'workspace' AND subject_id = OLD.id AND member_id = '');
END;
CREATE TRIGGER repository_authority_workspace_au AFTER UPDATE OF id, owner_principal_id ON workspace
WHEN NEW.id IS NOT OLD.id OR NEW.owner_principal_id IS NOT OLD.owner_principal_id
BEGIN
  SELECT RAISE(ABORT, 'repository authority revision exhausted')
  FROM repository_authority_revision WHERE kind = 'workspace' AND subject_id = OLD.id AND member_id = ''
    AND (typeof(revision) <> 'integer' OR revision <= 0 OR revision = 9223372036854775807);
  UPDATE repository_authority_revision SET revision = revision + 1
  WHERE kind = 'workspace' AND subject_id = OLD.id AND member_id = '';
  INSERT INTO repository_authority_revision (kind, subject_id, member_id, revision)
  SELECT 'workspace', OLD.id, '', 1
  WHERE NOT EXISTS (SELECT 1 FROM repository_authority_revision WHERE kind = 'workspace' AND subject_id = OLD.id AND member_id = '');
  SELECT RAISE(ABORT, 'repository authority revision exhausted')
  FROM repository_authority_revision WHERE kind = 'workspace' AND subject_id = NEW.id AND member_id = ''
    AND (typeof(revision) <> 'integer' OR revision <= 0 OR revision = 9223372036854775807) AND (NEW.id IS NOT OLD.id);
  UPDATE repository_authority_revision SET revision = revision + 1
  WHERE kind = 'workspace' AND subject_id = NEW.id AND member_id = '' AND (NEW.id IS NOT OLD.id);
  INSERT INTO repository_authority_revision (kind, subject_id, member_id, revision)
  SELECT 'workspace', NEW.id, '', 1
  WHERE NOT EXISTS (SELECT 1 FROM repository_authority_revision WHERE kind = 'workspace' AND subject_id = NEW.id AND member_id = '') AND (NEW.id IS NOT OLD.id);
END;

-- principal: insert/delete always retire; only meaningful updates advance.
CREATE TRIGGER repository_authority_principal_ai AFTER INSERT ON principal
BEGIN
  SELECT RAISE(ABORT, 'repository authority revision exhausted')
  FROM repository_authority_revision WHERE kind = 'principal' AND subject_id = NEW.id AND member_id = ''
    AND (typeof(revision) <> 'integer' OR revision <= 0 OR revision = 9223372036854775807);
  UPDATE repository_authority_revision SET revision = revision + 1
  WHERE kind = 'principal' AND subject_id = NEW.id AND member_id = '';
  INSERT INTO repository_authority_revision (kind, subject_id, member_id, revision)
  SELECT 'principal', NEW.id, '', 1
  WHERE NOT EXISTS (SELECT 1 FROM repository_authority_revision WHERE kind = 'principal' AND subject_id = NEW.id AND member_id = '');
END;
CREATE TRIGGER repository_authority_principal_ad AFTER DELETE ON principal
BEGIN
  SELECT RAISE(ABORT, 'repository authority revision exhausted')
  FROM repository_authority_revision WHERE kind = 'principal' AND subject_id = OLD.id AND member_id = ''
    AND (typeof(revision) <> 'integer' OR revision <= 0 OR revision = 9223372036854775807);
  UPDATE repository_authority_revision SET revision = revision + 1
  WHERE kind = 'principal' AND subject_id = OLD.id AND member_id = '';
  INSERT INTO repository_authority_revision (kind, subject_id, member_id, revision)
  SELECT 'principal', OLD.id, '', 1
  WHERE NOT EXISTS (SELECT 1 FROM repository_authority_revision WHERE kind = 'principal' AND subject_id = OLD.id AND member_id = '');
END;
CREATE TRIGGER repository_authority_principal_au AFTER UPDATE OF id, is_primary, github_user_id, identity_provider, instance_host, external_user_id ON principal
WHEN NEW.id IS NOT OLD.id OR NEW.is_primary IS NOT OLD.is_primary OR NEW.github_user_id IS NOT OLD.github_user_id OR NEW.identity_provider IS NOT OLD.identity_provider OR NEW.instance_host IS NOT OLD.instance_host OR NEW.external_user_id IS NOT OLD.external_user_id
BEGIN
  SELECT RAISE(ABORT, 'repository authority revision exhausted')
  FROM repository_authority_revision WHERE kind = 'principal' AND subject_id = OLD.id AND member_id = ''
    AND (typeof(revision) <> 'integer' OR revision <= 0 OR revision = 9223372036854775807);
  UPDATE repository_authority_revision SET revision = revision + 1
  WHERE kind = 'principal' AND subject_id = OLD.id AND member_id = '';
  INSERT INTO repository_authority_revision (kind, subject_id, member_id, revision)
  SELECT 'principal', OLD.id, '', 1
  WHERE NOT EXISTS (SELECT 1 FROM repository_authority_revision WHERE kind = 'principal' AND subject_id = OLD.id AND member_id = '');
  SELECT RAISE(ABORT, 'repository authority revision exhausted')
  FROM repository_authority_revision WHERE kind = 'principal' AND subject_id = NEW.id AND member_id = ''
    AND (typeof(revision) <> 'integer' OR revision <= 0 OR revision = 9223372036854775807) AND (NEW.id IS NOT OLD.id);
  UPDATE repository_authority_revision SET revision = revision + 1
  WHERE kind = 'principal' AND subject_id = NEW.id AND member_id = '' AND (NEW.id IS NOT OLD.id);
  INSERT INTO repository_authority_revision (kind, subject_id, member_id, revision)
  SELECT 'principal', NEW.id, '', 1
  WHERE NOT EXISTS (SELECT 1 FROM repository_authority_revision WHERE kind = 'principal' AND subject_id = NEW.id AND member_id = '') AND (NEW.id IS NOT OLD.id);
END;

-- workspace_member: insert/delete always retire; only meaningful updates advance.
CREATE TRIGGER repository_authority_workspace_member_ai AFTER INSERT ON workspace_member
BEGIN
  SELECT RAISE(ABORT, 'repository authority revision exhausted')
  FROM repository_authority_revision WHERE kind = 'workspace_member' AND subject_id = NEW.workspace_id AND member_id = NEW.principal_id
    AND (typeof(revision) <> 'integer' OR revision <= 0 OR revision = 9223372036854775807);
  UPDATE repository_authority_revision SET revision = revision + 1
  WHERE kind = 'workspace_member' AND subject_id = NEW.workspace_id AND member_id = NEW.principal_id;
  INSERT INTO repository_authority_revision (kind, subject_id, member_id, revision)
  SELECT 'workspace_member', NEW.workspace_id, NEW.principal_id, 1
  WHERE NOT EXISTS (SELECT 1 FROM repository_authority_revision WHERE kind = 'workspace_member' AND subject_id = NEW.workspace_id AND member_id = NEW.principal_id);
END;
CREATE TRIGGER repository_authority_workspace_member_ad AFTER DELETE ON workspace_member
BEGIN
  SELECT RAISE(ABORT, 'repository authority revision exhausted')
  FROM repository_authority_revision WHERE kind = 'workspace_member' AND subject_id = OLD.workspace_id AND member_id = OLD.principal_id
    AND (typeof(revision) <> 'integer' OR revision <= 0 OR revision = 9223372036854775807);
  UPDATE repository_authority_revision SET revision = revision + 1
  WHERE kind = 'workspace_member' AND subject_id = OLD.workspace_id AND member_id = OLD.principal_id;
  INSERT INTO repository_authority_revision (kind, subject_id, member_id, revision)
  SELECT 'workspace_member', OLD.workspace_id, OLD.principal_id, 1
  WHERE NOT EXISTS (SELECT 1 FROM repository_authority_revision WHERE kind = 'workspace_member' AND subject_id = OLD.workspace_id AND member_id = OLD.principal_id);
END;
CREATE TRIGGER repository_authority_workspace_member_au AFTER UPDATE OF workspace_id, principal_id, role ON workspace_member
WHEN NEW.workspace_id IS NOT OLD.workspace_id OR NEW.principal_id IS NOT OLD.principal_id OR NEW.role IS NOT OLD.role
BEGIN
  SELECT RAISE(ABORT, 'repository authority revision exhausted')
  FROM repository_authority_revision WHERE kind = 'workspace_member' AND subject_id = OLD.workspace_id AND member_id = OLD.principal_id
    AND (typeof(revision) <> 'integer' OR revision <= 0 OR revision = 9223372036854775807);
  UPDATE repository_authority_revision SET revision = revision + 1
  WHERE kind = 'workspace_member' AND subject_id = OLD.workspace_id AND member_id = OLD.principal_id;
  INSERT INTO repository_authority_revision (kind, subject_id, member_id, revision)
  SELECT 'workspace_member', OLD.workspace_id, OLD.principal_id, 1
  WHERE NOT EXISTS (SELECT 1 FROM repository_authority_revision WHERE kind = 'workspace_member' AND subject_id = OLD.workspace_id AND member_id = OLD.principal_id);
  SELECT RAISE(ABORT, 'repository authority revision exhausted')
  FROM repository_authority_revision WHERE kind = 'workspace_member' AND subject_id = NEW.workspace_id AND member_id = NEW.principal_id
    AND (typeof(revision) <> 'integer' OR revision <= 0 OR revision = 9223372036854775807) AND (NEW.workspace_id IS NOT OLD.workspace_id OR NEW.principal_id IS NOT OLD.principal_id);
  UPDATE repository_authority_revision SET revision = revision + 1
  WHERE kind = 'workspace_member' AND subject_id = NEW.workspace_id AND member_id = NEW.principal_id AND (NEW.workspace_id IS NOT OLD.workspace_id OR NEW.principal_id IS NOT OLD.principal_id);
  INSERT INTO repository_authority_revision (kind, subject_id, member_id, revision)
  SELECT 'workspace_member', NEW.workspace_id, NEW.principal_id, 1
  WHERE NOT EXISTS (SELECT 1 FROM repository_authority_revision WHERE kind = 'workspace_member' AND subject_id = NEW.workspace_id AND member_id = NEW.principal_id) AND (NEW.workspace_id IS NOT OLD.workspace_id OR NEW.principal_id IS NOT OLD.principal_id);
END;

-- host_member: insert/delete always retire; only meaningful updates advance.
CREATE TRIGGER repository_authority_host_member_ai AFTER INSERT ON host_member
BEGIN
  SELECT RAISE(ABORT, 'repository authority revision exhausted')
  FROM repository_authority_revision WHERE kind = 'host_member' AND subject_id = NEW.principal_id AND member_id = ''
    AND (typeof(revision) <> 'integer' OR revision <= 0 OR revision = 9223372036854775807);
  UPDATE repository_authority_revision SET revision = revision + 1
  WHERE kind = 'host_member' AND subject_id = NEW.principal_id AND member_id = '';
  INSERT INTO repository_authority_revision (kind, subject_id, member_id, revision)
  SELECT 'host_member', NEW.principal_id, '', 1
  WHERE NOT EXISTS (SELECT 1 FROM repository_authority_revision WHERE kind = 'host_member' AND subject_id = NEW.principal_id AND member_id = '');
END;
CREATE TRIGGER repository_authority_host_member_ad AFTER DELETE ON host_member
BEGIN
  SELECT RAISE(ABORT, 'repository authority revision exhausted')
  FROM repository_authority_revision WHERE kind = 'host_member' AND subject_id = OLD.principal_id AND member_id = ''
    AND (typeof(revision) <> 'integer' OR revision <= 0 OR revision = 9223372036854775807);
  UPDATE repository_authority_revision SET revision = revision + 1
  WHERE kind = 'host_member' AND subject_id = OLD.principal_id AND member_id = '';
  INSERT INTO repository_authority_revision (kind, subject_id, member_id, revision)
  SELECT 'host_member', OLD.principal_id, '', 1
  WHERE NOT EXISTS (SELECT 1 FROM repository_authority_revision WHERE kind = 'host_member' AND subject_id = OLD.principal_id AND member_id = '');
END;
CREATE TRIGGER repository_authority_host_member_au AFTER UPDATE OF principal_id ON host_member
WHEN NEW.principal_id IS NOT OLD.principal_id
BEGIN
  SELECT RAISE(ABORT, 'repository authority revision exhausted')
  FROM repository_authority_revision WHERE kind = 'host_member' AND subject_id = OLD.principal_id AND member_id = ''
    AND (typeof(revision) <> 'integer' OR revision <= 0 OR revision = 9223372036854775807);
  UPDATE repository_authority_revision SET revision = revision + 1
  WHERE kind = 'host_member' AND subject_id = OLD.principal_id AND member_id = '';
  INSERT INTO repository_authority_revision (kind, subject_id, member_id, revision)
  SELECT 'host_member', OLD.principal_id, '', 1
  WHERE NOT EXISTS (SELECT 1 FROM repository_authority_revision WHERE kind = 'host_member' AND subject_id = OLD.principal_id AND member_id = '');
  SELECT RAISE(ABORT, 'repository authority revision exhausted')
  FROM repository_authority_revision WHERE kind = 'host_member' AND subject_id = NEW.principal_id AND member_id = ''
    AND (typeof(revision) <> 'integer' OR revision <= 0 OR revision = 9223372036854775807) AND (NEW.principal_id IS NOT OLD.principal_id);
  UPDATE repository_authority_revision SET revision = revision + 1
  WHERE kind = 'host_member' AND subject_id = NEW.principal_id AND member_id = '' AND (NEW.principal_id IS NOT OLD.principal_id);
  INSERT INTO repository_authority_revision (kind, subject_id, member_id, revision)
  SELECT 'host_member', NEW.principal_id, '', 1
  WHERE NOT EXISTS (SELECT 1 FROM repository_authority_revision WHERE kind = 'host_member' AND subject_id = NEW.principal_id AND member_id = '') AND (NEW.principal_id IS NOT OLD.principal_id);
END;

-- principal_credential: insert/delete always retire; only meaningful updates advance.
CREATE TRIGGER repository_authority_credential_ai AFTER INSERT ON principal_credential
BEGIN
  SELECT RAISE(ABORT, 'repository authority revision exhausted')
  FROM repository_authority_revision WHERE kind = 'credential' AND subject_id = NEW.token_hash AND member_id = ''
    AND (typeof(revision) <> 'integer' OR revision <= 0 OR revision = 9223372036854775807);
  UPDATE repository_authority_revision SET revision = revision + 1
  WHERE kind = 'credential' AND subject_id = NEW.token_hash AND member_id = '';
  INSERT INTO repository_authority_revision (kind, subject_id, member_id, revision)
  SELECT 'credential', NEW.token_hash, '', 1
  WHERE NOT EXISTS (SELECT 1 FROM repository_authority_revision WHERE kind = 'credential' AND subject_id = NEW.token_hash AND member_id = '');
END;
CREATE TRIGGER repository_authority_credential_ad AFTER DELETE ON principal_credential
BEGIN
  SELECT RAISE(ABORT, 'repository authority revision exhausted')
  FROM repository_authority_revision WHERE kind = 'credential' AND subject_id = OLD.token_hash AND member_id = ''
    AND (typeof(revision) <> 'integer' OR revision <= 0 OR revision = 9223372036854775807);
  UPDATE repository_authority_revision SET revision = revision + 1
  WHERE kind = 'credential' AND subject_id = OLD.token_hash AND member_id = '';
  INSERT INTO repository_authority_revision (kind, subject_id, member_id, revision)
  SELECT 'credential', OLD.token_hash, '', 1
  WHERE NOT EXISTS (SELECT 1 FROM repository_authority_revision WHERE kind = 'credential' AND subject_id = OLD.token_hash AND member_id = '');
END;
CREATE TRIGGER repository_authority_credential_au AFTER UPDATE OF token_hash, principal_id, revoked_at ON principal_credential
WHEN NEW.token_hash IS NOT OLD.token_hash OR NEW.principal_id IS NOT OLD.principal_id OR NEW.revoked_at IS NOT OLD.revoked_at
BEGIN
  SELECT RAISE(ABORT, 'repository authority revision exhausted')
  FROM repository_authority_revision WHERE kind = 'credential' AND subject_id = OLD.token_hash AND member_id = ''
    AND (typeof(revision) <> 'integer' OR revision <= 0 OR revision = 9223372036854775807);
  UPDATE repository_authority_revision SET revision = revision + 1
  WHERE kind = 'credential' AND subject_id = OLD.token_hash AND member_id = '';
  INSERT INTO repository_authority_revision (kind, subject_id, member_id, revision)
  SELECT 'credential', OLD.token_hash, '', 1
  WHERE NOT EXISTS (SELECT 1 FROM repository_authority_revision WHERE kind = 'credential' AND subject_id = OLD.token_hash AND member_id = '');
  SELECT RAISE(ABORT, 'repository authority revision exhausted')
  FROM repository_authority_revision WHERE kind = 'credential' AND subject_id = NEW.token_hash AND member_id = ''
    AND (typeof(revision) <> 'integer' OR revision <= 0 OR revision = 9223372036854775807) AND (NEW.token_hash IS NOT OLD.token_hash);
  UPDATE repository_authority_revision SET revision = revision + 1
  WHERE kind = 'credential' AND subject_id = NEW.token_hash AND member_id = '' AND (NEW.token_hash IS NOT OLD.token_hash);
  INSERT INTO repository_authority_revision (kind, subject_id, member_id, revision)
  SELECT 'credential', NEW.token_hash, '', 1
  WHERE NOT EXISTS (SELECT 1 FROM repository_authority_revision WHERE kind = 'credential' AND subject_id = NEW.token_hash AND member_id = '') AND (NEW.token_hash IS NOT OLD.token_hash);
END;

