-- Per-note line attribution (PROTOCOL §5.2.1). One row per note carries the
-- most recently computed `attribute_lines` output (author + timestamp per
-- 1-based line number), so `note.lineAttribution.load` is O(1) and survives
-- restart. Rows are upserted after each recompute; deleting a note cascades.

CREATE TABLE note_line_attribution (
  note_id           TEXT NOT NULL PRIMARY KEY REFERENCES note(id) ON DELETE CASCADE,
  workspace_id      TEXT NOT NULL,
  computed_at       TEXT NOT NULL,
  attributions_json TEXT NOT NULL                     -- serialized `LineAttributionData.attributions`
);

CREATE INDEX idx_note_line_attribution_workspace ON note_line_attribution(workspace_id);
