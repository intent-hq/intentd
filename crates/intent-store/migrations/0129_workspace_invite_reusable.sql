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
