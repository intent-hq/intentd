-- Derived sharing state only: source grants/invitations and ownership are unchanged.
-- New workspaces are seeded regardless of the owner-default trigger's order.
CREATE TABLE workspace_sharing_summary (
  workspace_id TEXT PRIMARY KEY REFERENCES workspace(id) ON DELETE CASCADE,
  direct_count INTEGER NOT NULL DEFAULT 0 CHECK (direct_count >= 0),
  non_host_count INTEGER NOT NULL DEFAULT 0 CHECK (non_host_count >= 0),
  guest_count INTEGER NOT NULL DEFAULT 0 CHECK (guest_count >= 0),
  open_invite_count INTEGER NOT NULL DEFAULT 0 CHECK (open_invite_count >= 0)
);
INSERT INTO workspace_sharing_summary (workspace_id, direct_count, non_host_count, guest_count)
SELECT w.id, COUNT(m.principal_id),
  COALESCE(SUM(m.principal_id IS NOT NULL AND h.principal_id IS NULL), 0),
  COALESCE(SUM(m.role = 'collaborator' AND h.principal_id IS NULL), 0)
FROM workspace w LEFT JOIN workspace_member m ON m.workspace_id = w.id
LEFT JOIN host_member h ON h.principal_id = m.principal_id GROUP BY w.id;

-- An invitation that is otherwise open reserves a seat until its deadline.
-- Indexed reconciliation deletes due reservations, never the invitation history.
-- All count observations and admissions run that reconciliation at their own
-- observation time; no timer, restart, or normal write is needed for freshness.
CREATE TABLE workspace_invite_seat (
  invite_id TEXT PRIMARY KEY REFERENCES workspace_invite(id) ON DELETE CASCADE,
  workspace_id TEXT NOT NULL REFERENCES workspace(id) ON DELETE CASCADE,
  expires_at TEXT NOT NULL
);
CREATE INDEX workspace_invite_seat_expiry_idx ON workspace_invite_seat (workspace_id, expires_at);
INSERT INTO workspace_invite_seat
SELECT id, workspace_id, expires_at FROM workspace_invite
WHERE revoked_at IS NULL AND ((pin_identity_provider IS NULL AND pin_github_user_id IS NULL) OR redeemed_at IS NULL);
UPDATE workspace_sharing_summary SET open_invite_count = (
  SELECT COUNT(*) FROM workspace_invite_seat s WHERE s.workspace_id = workspace_sharing_summary.workspace_id
);

CREATE TRIGGER sharing_workspace_ai AFTER INSERT ON workspace BEGIN
  INSERT OR IGNORE INTO workspace_sharing_summary (workspace_id) VALUES (NEW.id);
END;

CREATE TRIGGER sharing_member_ai AFTER INSERT ON workspace_member BEGIN
  INSERT INTO workspace_sharing_summary (workspace_id, direct_count, non_host_count, guest_count)
  VALUES (NEW.workspace_id, 1,
    NOT EXISTS(SELECT 1 FROM host_member WHERE principal_id = NEW.principal_id),
    NEW.role = 'collaborator' AND NOT EXISTS(SELECT 1 FROM host_member WHERE principal_id = NEW.principal_id))
  ON CONFLICT(workspace_id) DO UPDATE SET direct_count = direct_count + 1,
    non_host_count = non_host_count + excluded.non_host_count,
    guest_count = guest_count + excluded.guest_count;
END;

CREATE TRIGGER sharing_member_ad AFTER DELETE ON workspace_member BEGIN
  UPDATE workspace_sharing_summary SET direct_count = direct_count - 1,
    non_host_count = non_host_count - (NOT EXISTS(SELECT 1 FROM host_member WHERE principal_id = OLD.principal_id)),
    guest_count = guest_count - (OLD.role = 'collaborator' AND NOT EXISTS(SELECT 1 FROM host_member WHERE principal_id = OLD.principal_id))
  WHERE workspace_id = OLD.workspace_id;
END;

CREATE TRIGGER sharing_member_au AFTER UPDATE OF workspace_id, principal_id, role ON workspace_member BEGIN
  UPDATE workspace_sharing_summary SET direct_count = direct_count - 1,
    non_host_count = non_host_count - (NOT EXISTS(SELECT 1 FROM host_member WHERE principal_id = OLD.principal_id)),
    guest_count = guest_count - (OLD.role = 'collaborator' AND NOT EXISTS(SELECT 1 FROM host_member WHERE principal_id = OLD.principal_id))
  WHERE workspace_id = OLD.workspace_id;
  INSERT INTO workspace_sharing_summary (workspace_id, direct_count, non_host_count, guest_count)
  VALUES (NEW.workspace_id, 1,
    NOT EXISTS(SELECT 1 FROM host_member WHERE principal_id = NEW.principal_id),
    NEW.role = 'collaborator' AND NOT EXISTS(SELECT 1 FROM host_member WHERE principal_id = NEW.principal_id))
  ON CONFLICT(workspace_id) DO UPDATE SET direct_count = direct_count + 1,
    non_host_count = non_host_count + excluded.non_host_count,
    guest_count = guest_count + excluded.guest_count;
END;

CREATE TRIGGER sharing_host_member_ai AFTER INSERT ON host_member BEGIN
  UPDATE workspace_sharing_summary SET non_host_count = non_host_count - 1,
    guest_count = guest_count - EXISTS(SELECT 1 FROM workspace_member m
      WHERE m.workspace_id = workspace_sharing_summary.workspace_id AND m.principal_id = NEW.principal_id AND m.role = 'collaborator')
  WHERE workspace_id IN (SELECT workspace_id FROM workspace_member WHERE principal_id = NEW.principal_id);
END;

CREATE TRIGGER sharing_host_member_ad AFTER DELETE ON host_member BEGIN
  UPDATE workspace_sharing_summary SET non_host_count = non_host_count + 1,
    guest_count = guest_count + EXISTS(SELECT 1 FROM workspace_member m
      WHERE m.workspace_id = workspace_sharing_summary.workspace_id AND m.principal_id = OLD.principal_id AND m.role = 'collaborator')
  WHERE workspace_id IN (SELECT workspace_id FROM workspace_member WHERE principal_id = OLD.principal_id);
END;

CREATE TRIGGER sharing_host_member_au AFTER UPDATE OF principal_id ON host_member BEGIN
  UPDATE workspace_sharing_summary SET non_host_count = non_host_count + 1,
    guest_count = guest_count + EXISTS(SELECT 1 FROM workspace_member m
      WHERE m.workspace_id = workspace_sharing_summary.workspace_id AND m.principal_id = OLD.principal_id AND m.role = 'collaborator')
  WHERE workspace_id IN (SELECT workspace_id FROM workspace_member WHERE principal_id = OLD.principal_id);
  UPDATE workspace_sharing_summary SET non_host_count = non_host_count - 1,
    guest_count = guest_count - EXISTS(SELECT 1 FROM workspace_member m
      WHERE m.workspace_id = workspace_sharing_summary.workspace_id AND m.principal_id = NEW.principal_id AND m.role = 'collaborator')
  WHERE workspace_id IN (SELECT workspace_id FROM workspace_member WHERE principal_id = NEW.principal_id);
END;

CREATE TRIGGER sharing_invite_seat_ai AFTER INSERT ON workspace_invite_seat BEGIN
  UPDATE workspace_sharing_summary SET open_invite_count = open_invite_count + 1 WHERE workspace_id = NEW.workspace_id;
END;
CREATE TRIGGER sharing_invite_seat_ad AFTER DELETE ON workspace_invite_seat BEGIN
  UPDATE workspace_sharing_summary SET open_invite_count = open_invite_count - 1 WHERE workspace_id = OLD.workspace_id;
END;
CREATE TRIGGER sharing_invite_ai AFTER INSERT ON workspace_invite
WHEN NEW.revoked_at IS NULL AND ((NEW.pin_identity_provider IS NULL AND NEW.pin_github_user_id IS NULL) OR NEW.redeemed_at IS NULL)
BEGIN
  INSERT INTO workspace_invite_seat VALUES (NEW.id, NEW.workspace_id, NEW.expires_at);
END;
CREATE TRIGGER sharing_invite_au AFTER UPDATE OF workspace_id, expires_at, revoked_at, redeemed_at, pin_identity_provider, pin_github_user_id ON workspace_invite BEGIN
  DELETE FROM workspace_invite_seat WHERE invite_id = OLD.id;
  INSERT INTO workspace_invite_seat
  SELECT NEW.id, NEW.workspace_id, NEW.expires_at
  WHERE NEW.revoked_at IS NULL AND ((NEW.pin_identity_provider IS NULL AND NEW.pin_github_user_id IS NULL) OR NEW.redeemed_at IS NULL);
END;
