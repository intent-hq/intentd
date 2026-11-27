-- Intent apps (PROTOCOL §5.60, Labs). `intent_app` holds one row per
-- installed app: the daemon-owned record (manifest projection, approval,
-- install source, timestamps) as JSON. `intent_app_storage` is the durable
-- per-app key-value store behind `app.storage.*`; `size_bytes` is the key's
-- UTF-8 length plus the compact JSON value's length, summed for the 10 MiB
-- quota. Uninstall deletes both; replace installs and revokes keep storage.
CREATE TABLE IF NOT EXISTS intent_app (
    app_id TEXT PRIMARY KEY NOT NULL,
    record_json TEXT NOT NULL,
    updated_at TEXT NOT NULL
);

CREATE TABLE IF NOT EXISTS intent_app_storage (
    app_id TEXT NOT NULL,
    key TEXT NOT NULL,
    value_json TEXT NOT NULL,
    size_bytes INTEGER NOT NULL,
    PRIMARY KEY (app_id, key)
);
