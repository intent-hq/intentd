-- Exactly one owner per workspace (multiplayer w3). Migration 0123 minted the
-- primary principal as the single owner of every workspace and the insert
-- trigger keeps it that way for new rows, but nothing stopped the membership
-- APIs from adding a second `owner` row. There is no ownership transfer in
-- v1, so the invariant is a partial unique index: a second owner insert or a
-- promote-while-owned fails at the constraint, and the store maps that to a
-- client-facing error.
--
-- Repair first: a database that already holds several owner rows for one
-- workspace would otherwise abort this migration and leave the daemon unable
-- to open its store. Keep one owner per workspace — the primary principal's
-- row when it is among them, else the earliest-added (rowid breaks ties) —
-- and demote the rest to collaborator, so nobody loses membership.
UPDATE workspace_member
   SET role = 'collaborator'
 WHERE role = 'owner'
   AND rowid NOT IN (
     SELECT keep_rowid FROM (
       SELECT wm.rowid AS keep_rowid,
              ROW_NUMBER() OVER (
                PARTITION BY wm.workspace_id
                ORDER BY p.is_primary DESC, wm.added_at ASC, wm.rowid ASC
              ) AS rn
         FROM workspace_member wm
         JOIN principal p ON p.id = wm.principal_id
        WHERE wm.role = 'owner'
     )
     WHERE rn = 1
   );

CREATE UNIQUE INDEX workspace_member_owner_uq
  ON workspace_member (workspace_id) WHERE role = 'owner';
