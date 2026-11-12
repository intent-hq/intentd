-- Local code pairing (§5.58): one credential per successful redemption of a
-- host-shown short code by another Intent on the same computer. Only the
-- SHA-256 of the token is stored. Admitted on `/ws` from loopback only, bound
-- to the primary principal; independent of the owner token and of
-- `principal_credential`. Revocation sets `revoked_at`; rows are kept.
CREATE TABLE local_pairing_credential (
    id TEXT PRIMARY KEY,
    token_hash TEXT NOT NULL UNIQUE,
    app_name TEXT NOT NULL,
    created_at TEXT NOT NULL,
    last_used_at TEXT,
    revoked_at TEXT
);
CREATE INDEX local_pairing_credential_active ON local_pairing_credential(created_at, id)
    WHERE revoked_at IS NULL;
