-- Workspace invite link secret (multiplayer w4 follow-up). Keeping the
-- plaintext secret next to its hash lets the owner copy an open invite's
-- `intent://invite?…` link again from `workspace.invite.list` instead of
-- only once at mint time. Redemption keeps matching on `secret_hash`; the
-- secret never rides the wire except inside the rebuilt `url`. Nullable so
-- rows minted before this migration stay valid (they simply have no url).
ALTER TABLE workspace_invite ADD COLUMN secret TEXT;
