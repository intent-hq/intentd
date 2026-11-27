-- Outstanding `invite.challenge` nonces (PROTOCOL §5.37), until the first
-- `invite.prove` that names one consumes it or it expires. A row is keyed by
-- the nonce's SHA-256 (hex), never the nonce. `claimed_at` marks a nonce a
-- prove is verifying with the forge: it still counts against the caps
-- (MAX_NONCES_PER_INVITE per invite, MAX_OUTSTANDING_NONCES in all) until the
-- verdict deletes it or a forge outage releases it. Times are Unix
-- milliseconds. A new table: no existing data changes.
CREATE TABLE IF NOT EXISTS invite_nonce (
    nonce_hash TEXT PRIMARY KEY NOT NULL,
    invite_id TEXT NOT NULL,
    scope TEXT NOT NULL CHECK (scope IN ('workspace', 'host')),
    authorization_generation INTEGER NOT NULL,
    issued_at_ms INTEGER NOT NULL,
    expires_at_ms INTEGER NOT NULL,
    claimed_at_ms INTEGER
) WITHOUT ROWID;

CREATE INDEX IF NOT EXISTS invite_nonce_invite_idx ON invite_nonce (invite_id);
