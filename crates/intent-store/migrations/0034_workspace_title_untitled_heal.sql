-- Heal legacy rows created before `workspace.create` stored `""` for missing
-- titles: previous versions of `intent-services::create_workspace` seeded the
-- title with the workspace id (slug) so the FE header would show something
-- readable immediately. That divergence from the reference `workspace.service`
-- shape (`title: request.title || ''`) broke Untitled parity — the FE renders
-- an empty title as "Untitled" but a slug-seeded row shows the raw slug.
--
-- The predicate `title = id` targets those slug-seeded rows. `WorkspaceId`
-- slugs are `adjective-noun` / prompt-derived tokens while user titles are
-- free-form text, so collisions are rare but not impossible: a user who types
-- a title that happens to equal the workspace id (e.g. `blue-heron`) will be
-- cleared to `""` by this heal. Clearing that edge case is an accepted
-- trade-off — the slug-seeded placeholder is indistinguishable from a
-- coincidentally-shaped user title at rest, and the reference contract is
-- Untitled-on-empty. The Chief-of-Staff row is explicitly exempted: its
-- canonical `Chief of Staff` title differs from its `__chief__` id anyway,
-- but the guard makes the intent obvious.
UPDATE workspace
SET title = ''
WHERE title = id
  AND id != '__chief__';
