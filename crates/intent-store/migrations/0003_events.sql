-- Append-only event log (§9.2 / §10). Adds only the `event` table + its three
-- indexes; 0001/0002 are untouched. The store exposes insert + query paths only
-- (no update/delete), keeping the log append-only at the API layer.
--
-- `actor` holds the serialized `EventActor` JSON and `data_json` the
-- type-specific payload; both round-trip the full TS/iOS wire shape.

CREATE TABLE event (
  id              TEXT PRIMARY KEY,
  workspace_id    TEXT NOT NULL,
  timestamp       TEXT NOT NULL,
  event_type      TEXT NOT NULL,
  actor           TEXT NOT NULL,
  session_id      TEXT,
  correlation_id  TEXT,
  parent_event_id TEXT,
  data_json       TEXT NOT NULL DEFAULT '{}'
);
CREATE INDEX idx_event_ws_time ON event(workspace_id, timestamp);
CREATE INDEX idx_event_type    ON event(event_type);
CREATE INDEX idx_event_session ON event(session_id);
