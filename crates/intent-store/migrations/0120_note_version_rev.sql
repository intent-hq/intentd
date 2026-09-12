-- Record the note's post-write `rev` on every version snapshot so a writer's
-- base content is recoverable by the `rev` it read (three-way merge base for
-- `note.setContent` convergence). Nullable: rows captured before this
-- migration carry no rev and never match a base lookup.
ALTER TABLE note_version ADD COLUMN rev INTEGER NULL;

CREATE INDEX idx_note_version_rev ON note_version(workspace_id, note_id, rev);
