-- When the persisted `stop_reason` was recorded (terminal agent failures).
-- Set whenever `stop_reason` is set, cleared (NULL) whenever `stop_reason`
-- clears, so the FE can render "failed N minutes ago" on parked-in-error
-- sessions. NULL for pre-existing rows and for sessions with no stop reason.

ALTER TABLE agent_session ADD COLUMN stop_reason_timestamp TEXT;
