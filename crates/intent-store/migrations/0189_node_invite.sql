-- Node invites (`intentd pair --node`, `executor.invite.create`): single-use,
-- expiring invites a backend claims on `/node/claim` for one executor
-- credential. Only the SHA-256 of the secret is stored. A claim sets
-- `used_at`, `used_by_backend_id` and `credential_id` in the transaction that
-- inserts the credential.
CREATE TABLE node_invite (
    id TEXT PRIMARY KEY,
    secret_hash TEXT NOT NULL UNIQUE,
    label TEXT,
    created_at TEXT NOT NULL,
    expires_at TEXT NOT NULL,
    used_at TEXT,
    used_by_backend_id TEXT,
    credential_id TEXT REFERENCES executor_credential(id) ON DELETE SET NULL,
    revoked_at TEXT
);
CREATE INDEX node_invite_open ON node_invite(created_at, id)
    WHERE used_at IS NULL AND revoked_at IS NULL;
