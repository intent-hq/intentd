-- Persistent registry of script definitions (`script.*`, PROTOCOL §5.8).
-- Parity with the FE's `.workspace/scripts.json` persistence: definitions
-- survive a daemon restart and are hydrated into the in-memory runtime
-- registry on boot. Only the *definition* is persisted — runtime state
-- (status, pid, detected URL, restart count) is transient and rebuilt from
-- a fresh idle state. `env` is a JSON-encoded string map; `mode` and
-- `source` carry the wire words (`service`/`command`, `user`/...).
CREATE TABLE script (
    id           TEXT PRIMARY KEY,
    workspace_id TEXT NOT NULL,
    name         TEXT NOT NULL,
    command      TEXT NOT NULL,
    cwd          TEXT,
    env          TEXT,
    mode         TEXT NOT NULL,
    category     TEXT,
    source       TEXT NOT NULL,
    auto_start   INTEGER,
    created_at   TEXT NOT NULL,
    updated_at   TEXT
);

CREATE INDEX idx_script_workspace ON script(workspace_id);
