-- Workspace label (PROTOCOL §9.1 `Workspace.label`): the single Home
-- grouping label a workspace carries — the reserved value 'pinned' for the
-- built-in Pinned label, or a free-form 1–64 character name. NULL = no
-- label. There is no label registry: the labels that exist are exactly the
-- distinct values held by workspace rows. Additive only: NULL for every
-- existing row, so an older daemon ignores it.
ALTER TABLE workspace ADD COLUMN label TEXT;
