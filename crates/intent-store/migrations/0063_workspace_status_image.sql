-- Agent-authored workspace status screenshot (intent-hq/monorepo#997):
-- asset id of the image stored via the note.saveAsset machinery. NULL until
-- an agent sets one; cleared with an explicit wire null on workspace.update.
ALTER TABLE workspace ADD COLUMN status_image_asset_id TEXT;
