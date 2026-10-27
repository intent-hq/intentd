-- Explicit local notification identities; imported messages clear this field.
ALTER TABLE workspace_discussion_message ADD COLUMN principal_mentions_json TEXT NOT NULL DEFAULT '[]';
