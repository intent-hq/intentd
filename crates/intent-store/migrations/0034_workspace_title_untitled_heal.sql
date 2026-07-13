-- Heal legacy rows created before `workspace.create` stored `""` for missing
-- titles: previous versions of `intent-services::create_workspace` seeded the
-- title with the workspace id (slug) so the FE header would show something
-- readable immediately. That divergence from the reference `workspace.service`
-- shape (`title: request.title || ''`) broke Untitled parity — the FE renders
-- an empty title as "Untitled" but a slug-seeded row shows the raw slug.
--
-- The predicate `title = id` uniquely identifies those slug-seeded rows:
-- `WorkspaceId` slugs never collide with user-typed titles (user titles are
-- free-form text, slugs are `adjective-noun` / prompt-derived tokens), and any
-- accidental caller-supplied title equal to the id is indistinguishable from
-- the auto-seeded placeholder we want to clear. The Chief-of-Staff row is
-- explicitly exempted: its canonical `Chief of Staff` title differs from its
-- `__chief__` id anyway, but the guard makes the intent obvious.
UPDATE workspace
SET title = ''
WHERE title = id
  AND id != '__chief__';
