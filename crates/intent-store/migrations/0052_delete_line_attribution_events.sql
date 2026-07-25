-- `line-attribution:updated` is now broadcast-only (published via the
-- transient event path, monorepo#720 finding 2): the durable snapshot lives in
-- `note_line_attribution` and is served by `note.lineAttribution.load`, so the
-- persisted event rows were pure write amplification. Remove the rows that
-- accumulated on existing installs while the event was routed through the
-- durable publish path.

DELETE FROM event WHERE event_type = 'line-attribution:updated';
