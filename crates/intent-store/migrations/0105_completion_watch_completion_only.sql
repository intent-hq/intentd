-- Completion-bound Chief asks ignore attention requests while ordinary
-- completion watches retain their existing attention fan-out semantics.
ALTER TABLE completion_watch
ADD COLUMN completion_only INTEGER NOT NULL DEFAULT 0;