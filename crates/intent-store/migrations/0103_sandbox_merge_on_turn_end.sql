-- Parent-controlled turn-end merges (fan-out/decide-later): when an agent is
-- delegated/created with mergeOnTurnEnd=false, the completion path must skip
-- the automatic sandbox merge-back entirely and the retry sweep must not pick
-- the sandbox up. The flag is set at provision time from the delegate/create
-- input and survives respawn/daemon restart. Default 1 preserves today's
-- merge-on-completion behavior for all existing sandboxes.

ALTER TABLE sandbox ADD COLUMN merge_on_turn_end INTEGER NOT NULL DEFAULT 1;
