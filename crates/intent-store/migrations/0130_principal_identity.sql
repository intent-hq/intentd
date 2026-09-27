-- Provider-neutral identity key (Track 2, link 2). A principal is identified
-- by the triple (identity_provider, instance_host, external_user_id) —
-- e.g. ('github', 'github.com', '583231') or ('gitlab', 'gitlab.com', '42') —
-- so a GitLab account whose numeric id collides with a GitHub one is a
-- different principal. `github_user_id` stays and stays populated for
-- github principals (dual-write); every existing row is backfilled from it
-- without loss.
ALTER TABLE principal ADD COLUMN identity_provider TEXT;
ALTER TABLE principal ADD COLUMN instance_host TEXT;
ALTER TABLE principal ADD COLUMN external_user_id TEXT;

UPDATE principal
   SET identity_provider = 'github',
       instance_host = 'github.com',
       external_user_id = CAST(github_user_id AS TEXT)
 WHERE github_user_id IS NOT NULL AND identity_provider IS NULL;

CREATE UNIQUE INDEX principal_identity_uq
    ON principal (identity_provider, instance_host, external_user_id)
 WHERE identity_provider IS NOT NULL;

-- Invite pins carry the same triple; `pin_github_user_id` stays for the
-- github case and is backfilled the same way.
ALTER TABLE workspace_invite ADD COLUMN pin_identity_provider TEXT;
ALTER TABLE workspace_invite ADD COLUMN pin_instance_host TEXT;
ALTER TABLE workspace_invite ADD COLUMN pin_external_user_id TEXT;

UPDATE workspace_invite
   SET pin_identity_provider = 'github',
       pin_instance_host = 'github.com',
       pin_external_user_id = CAST(pin_github_user_id AS TEXT)
 WHERE pin_github_user_id IS NOT NULL AND pin_identity_provider IS NULL;
