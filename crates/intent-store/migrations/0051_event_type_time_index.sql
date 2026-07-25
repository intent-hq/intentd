-- Composite index for the event-retention sweep. The sweep deletes rows by
-- event-type family + age; the existing single-column indexes
-- (idx_event_type, idx_event_ws_time) cannot serve the combined predicate, so
-- the old OR-of-LIKEs DELETE full-scanned the table on the write pool
-- (2-3.5s per tick on a 1.2GB dev-seat DB). With (event_type, timestamp) each
-- per-family delete is an index range scan: equality or half-open range on
-- event_type, then a timestamp upper bound within the same index.

CREATE INDEX idx_event_type_time ON event(event_type, timestamp);
