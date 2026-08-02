-- JSON-serialized state carried over between a hook's runs (`hookState`
-- global in, returned `state` field out). Overwritten on every completed
-- run; capped (~16 KiB) at the service layer. NULL when the hook has never
-- returned state.

ALTER TABLE hook ADD COLUMN last_state TEXT;
