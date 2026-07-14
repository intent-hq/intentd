-- Migration: Add retry_count to sandbox table for conflict bounce retry tracking
-- The retry count tracks how many times a sandbox merge has been bounced due to conflicts.
-- When the cap (2) is hit, the completion propagates with merge-pending status.

ALTER TABLE sandbox ADD COLUMN retry_count INTEGER NOT NULL DEFAULT 0;
