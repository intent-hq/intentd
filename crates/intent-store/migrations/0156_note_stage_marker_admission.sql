-- Metadata-only evidence captured atomically with staged source admission.
-- This is not marker authority by itself. Existing operations have no witness;
-- a future marker adapter must refuse them rather than infer past ownership.
ALTER TABLE note_stage ADD COLUMN marker_admission TEXT
    CHECK(marker_admission IS NULL OR length(CAST(marker_admission AS BLOB))<=1024);
