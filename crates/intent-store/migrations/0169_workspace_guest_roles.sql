-- Persist the scoped Manager guest role without changing existing grants.
-- Rebuild the checked table and restore all referring triggers. Copy before
-- installing triggers so sharing counts and authority revisions stay unchanged.
DROP TRIGGER workspace_owner_default_ai;
DROP TRIGGER sharing_member_ai;
DROP TRIGGER sharing_member_ad;
DROP TRIGGER sharing_member_au;
DROP TRIGGER sharing_host_member_ai;
DROP TRIGGER sharing_host_member_ad;
DROP TRIGGER sharing_host_member_au;
DROP TRIGGER repository_authority_workspace_member_ai;
DROP TRIGGER repository_authority_workspace_member_ad;
DROP TRIGGER repository_authority_workspace_member_au;
CREATE TABLE workspace_member_next (
  workspace_id TEXT NOT NULL REFERENCES workspace(id) ON DELETE CASCADE,
  principal_id TEXT NOT NULL REFERENCES principal(id) ON DELETE CASCADE,
  role TEXT NOT NULL CHECK (role IN ('owner', 'collaborator', 'manager')),
  added_at TEXT NOT NULL,
  PRIMARY KEY (workspace_id, principal_id)
);
INSERT INTO workspace_member_next SELECT * FROM workspace_member;
DROP TABLE workspace_member;
ALTER TABLE workspace_member_next RENAME TO workspace_member;
CREATE INDEX workspace_member_principal_idx ON workspace_member (principal_id);
CREATE UNIQUE INDEX workspace_member_owner_uq ON workspace_member (workspace_id) WHERE role = 'owner';

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

CREATE TRIGGER sharing_member_ai AFTER INSERT ON workspace_member BEGIN
  INSERT INTO workspace_sharing_summary (workspace_id, direct_count, non_host_count, guest_count)
  VALUES (NEW.workspace_id, 1,
    NOT EXISTS(SELECT 1 FROM host_member WHERE principal_id = NEW.principal_id),
    NEW.role IN ('collaborator', 'manager') AND NOT EXISTS(SELECT 1 FROM host_member WHERE principal_id = NEW.principal_id))
  ON CONFLICT(workspace_id) DO UPDATE SET direct_count = direct_count + 1,
    non_host_count = non_host_count + excluded.non_host_count,
    guest_count = guest_count + excluded.guest_count;
END;

CREATE TRIGGER sharing_member_ad AFTER DELETE ON workspace_member BEGIN
  UPDATE workspace_sharing_summary SET direct_count = direct_count - 1,
    non_host_count = non_host_count - (NOT EXISTS(SELECT 1 FROM host_member WHERE principal_id = OLD.principal_id)),
    guest_count = guest_count - (OLD.role IN ('collaborator', 'manager') AND NOT EXISTS(SELECT 1 FROM host_member WHERE principal_id = OLD.principal_id))
  WHERE workspace_id = OLD.workspace_id;
END;

CREATE TRIGGER sharing_member_au AFTER UPDATE OF workspace_id, principal_id, role ON workspace_member BEGIN
  UPDATE workspace_sharing_summary SET direct_count = direct_count - 1,
    non_host_count = non_host_count - (NOT EXISTS(SELECT 1 FROM host_member WHERE principal_id = OLD.principal_id)),
    guest_count = guest_count - (OLD.role IN ('collaborator', 'manager') AND NOT EXISTS(SELECT 1 FROM host_member WHERE principal_id = OLD.principal_id))
  WHERE workspace_id = OLD.workspace_id;
  INSERT INTO workspace_sharing_summary (workspace_id, direct_count, non_host_count, guest_count)
  VALUES (NEW.workspace_id, 1,
    NOT EXISTS(SELECT 1 FROM host_member WHERE principal_id = NEW.principal_id),
    NEW.role IN ('collaborator', 'manager') AND NOT EXISTS(SELECT 1 FROM host_member WHERE principal_id = NEW.principal_id))
  ON CONFLICT(workspace_id) DO UPDATE SET direct_count = direct_count + 1,
    non_host_count = non_host_count + excluded.non_host_count,
    guest_count = guest_count + excluded.guest_count;
END;

CREATE TRIGGER sharing_host_member_ai AFTER INSERT ON host_member BEGIN
  UPDATE workspace_sharing_summary SET non_host_count = non_host_count - 1,
    guest_count = guest_count - EXISTS(SELECT 1 FROM workspace_member m
      WHERE m.workspace_id = workspace_sharing_summary.workspace_id AND m.principal_id = NEW.principal_id AND m.role IN ('collaborator', 'manager'))
  WHERE workspace_id IN (SELECT workspace_id FROM workspace_member WHERE principal_id = NEW.principal_id);
END;

CREATE TRIGGER sharing_host_member_ad AFTER DELETE ON host_member BEGIN
  UPDATE workspace_sharing_summary SET non_host_count = non_host_count + 1,
    guest_count = guest_count + EXISTS(SELECT 1 FROM workspace_member m
      WHERE m.workspace_id = workspace_sharing_summary.workspace_id AND m.principal_id = OLD.principal_id AND m.role IN ('collaborator', 'manager'))
  WHERE workspace_id IN (SELECT workspace_id FROM workspace_member WHERE principal_id = OLD.principal_id);
END;

CREATE TRIGGER sharing_host_member_au AFTER UPDATE OF principal_id ON host_member BEGIN
  UPDATE workspace_sharing_summary SET non_host_count = non_host_count + 1,
    guest_count = guest_count + EXISTS(SELECT 1 FROM workspace_member m
      WHERE m.workspace_id = workspace_sharing_summary.workspace_id AND m.principal_id = OLD.principal_id AND m.role IN ('collaborator', 'manager'))
  WHERE workspace_id IN (SELECT workspace_id FROM workspace_member WHERE principal_id = OLD.principal_id);
  UPDATE workspace_sharing_summary SET non_host_count = non_host_count - 1,
    guest_count = guest_count - EXISTS(SELECT 1 FROM workspace_member m
      WHERE m.workspace_id = workspace_sharing_summary.workspace_id AND m.principal_id = NEW.principal_id AND m.role IN ('collaborator', 'manager'))
  WHERE workspace_id IN (SELECT workspace_id FROM workspace_member WHERE principal_id = NEW.principal_id);
END;

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

