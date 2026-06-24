-- Add the optional `metadata` field to the event log (parity with the TS
-- `WorkspaceEventBase.metadata`). Additive only: 0001–0009 are frozen, so this
-- migration just appends a nullable `metadata_json` column. NULL means the
-- event carries no metadata (the field is omitted from the wire); when present
-- it holds the serialized free-form JSON object, round-tripping the TS shape.

ALTER TABLE event ADD COLUMN metadata_json TEXT;
