-- Exactly one owner per workspace (multiplayer w3). Migration 0123 minted the
-- primary principal as the single owner of every workspace and the insert
-- trigger keeps it that way for new rows, but nothing stopped the membership
-- APIs from adding a second `owner` row. There is no ownership transfer in
-- v1, so the invariant is a partial unique index: a second owner insert or a
-- promote-while-owned fails at the constraint, and the store maps that to a
-- client-facing error.
CREATE UNIQUE INDEX workspace_member_owner_uq
  ON workspace_member (workspace_id) WHERE role = 'owner';
