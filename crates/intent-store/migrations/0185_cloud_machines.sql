-- Cloud machines (E2B): a machine row the backend started itself in a cloud
-- sandbox carries its sandbox state as JSON (`RemoteMachine.cloud`); NULL
-- for every machine the owner exposed. The provider settings (size, idle
-- pause, paused delete, template project) live in their own table; the API
-- key is in the secret store, never here.
ALTER TABLE exposed_machine ADD COLUMN cloud_json TEXT;

CREATE TABLE cloud_machine_provider (
    id TEXT PRIMARY KEY NOT NULL,
    config_json TEXT NOT NULL,
    updated_at TEXT NOT NULL
);
