-- Persistent registry of known repositories (parity with the TS electron-store
-- `repo-registry`). Repos survive workspace deletion so the Create-Workspace
-- picker can re-use them later. Keyed by absolute `path`; timestamps are stored
-- as ISO-8601 TEXT (the `KnownRepo` `addedAt`/`lastUsedAt` wire fields). `owner`
-- is the optional GitHub org/user and is the only nullable column.

CREATE TABLE known_repo (
    path         TEXT PRIMARY KEY,
    name         TEXT NOT NULL,
    owner        TEXT,
    added_at     TEXT NOT NULL,
    last_used_at TEXT NOT NULL
);
