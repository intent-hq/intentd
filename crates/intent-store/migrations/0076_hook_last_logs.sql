-- Captured `console.*` output from a hook's most recent completed run.
-- Overwritten on every run completion (last run only); capped/head-truncated
-- at the service layer. NULL when the last run logged nothing.

ALTER TABLE hook ADD COLUMN last_logs TEXT;
