-- Execution-environment selection (PROTOCOL §5.1): which sandbox type the
-- workspace was created with (`direct` | `worktree` | `cow` | `microvm`).
-- Persisted from the `workspace.create` `executionEnvironment` param (or
-- derived from the legacy skipIsolation/CoW-settings path when omitted).
-- NULL for pre-existing rows — the field is simply omitted on the wire.

ALTER TABLE workspace ADD COLUMN execution_environment TEXT;
