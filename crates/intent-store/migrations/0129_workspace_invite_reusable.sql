-- Reusable invite links (multiplayer w4 follow-up). An unpinned invite
-- (`pin_github_user_id IS NULL`) may now be redeemed by any number of
-- distinct GitHub accounts until it expires or is revoked; a pinned invite
-- still closes on its single redemption. `redeemed_at` /
-- `redeemed_by_principal_id` become the LAST redemption; `redemption_count`
-- counts the memberships the link created (a returning member re-joining
-- through the same link is not counted again). Rows redeemed before this
-- migration are backfilled to one redemption.
ALTER TABLE workspace_invite ADD COLUMN redemption_count INTEGER NOT NULL DEFAULT 0;

UPDATE workspace_invite SET redemption_count = 1 WHERE redeemed_at IS NOT NULL;

-- An unpinned link redeemed BEFORE this migration was single-use when its
-- owner shared it and must not come back to life on upgrade: close it by
-- revoking it at its redemption instant. Only links redeemed after the
-- upgrade (or never redeemed before it) follow the unpinned ⇒ reusable rule.
-- Idempotent: the `revoked_at IS NULL` guard leaves already-closed rows alone.
UPDATE workspace_invite SET revoked_at = redeemed_at
 WHERE pin_github_user_id IS NULL AND redeemed_at IS NOT NULL AND revoked_at IS NULL;
