-- Emit baseline for PR monitors: the JSON `PrMonitorSnapshot` as of the last
-- delivered wake (or registration). Pending changes are recomputed as
-- diff(baseline_snapshot, fresh) each poll — a coalesced net set rather than
-- an accumulated log — so a field that reverts disappears and A→B→C renders
-- as a single A→C line. Distinct from `last_snapshot`, which tracks the most
-- recent poll and keeps anchoring per-poll activity detection.
ALTER TABLE pr_monitor ADD COLUMN baseline_snapshot TEXT;

-- Backfill existing rows from `last_snapshot` so active monitors keep working
-- across the upgrade: their baseline starts at the latest polled state, and
-- the next delivered wake advances it normally.
UPDATE pr_monitor SET baseline_snapshot = last_snapshot;
