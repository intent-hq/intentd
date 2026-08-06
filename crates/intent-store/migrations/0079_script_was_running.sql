-- Service-was-running marker (stored-on-write, ARCHITECTURE "derived fields"
-- rung 1): set when a service-mode script successfully starts, cleared on
-- user `script.stop`, natural exit, and `script.remove` (row deleted). Boot
-- hydration surfaces rows still carrying the marker as `previouslyRunning:
-- true` on the runtime state (PROTOCOL §5.8 additive field) so clients can
-- render tabs for scripts that were running when the previous daemon process
-- died. Command-mode scripts never set it.
ALTER TABLE script ADD COLUMN was_running INTEGER NOT NULL DEFAULT 0;
