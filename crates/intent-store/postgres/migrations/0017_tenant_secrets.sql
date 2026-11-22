-- Tenant secrets (orchestrator mode). The desktop keeps its secrets in the
-- file store (`intent_core::FileSecretStore`); an orchestrator stores each
-- tenant's secrets here, so they follow the tenant to the process that
-- takes it over. Postgres only; no SQLite counterpart.
--
-- Values are encrypted by the store before they reach the database
-- (`intent_store::postgres::TenantSecretStore`): XChaCha20-Poly1305 with a
-- random 24-byte nonce per value, and associated data that binds the
-- tenant, the account and the key id, so a row copied to another tenant or
-- account does not decrypt. `key_id` names the keyring key that sealed the
-- row, so keys can rotate: new writes use the active key, older rows stay
-- readable under theirs. No plaintext is stored.
--
-- Keyed by tenant_id first, no identity column, so it routes to the
-- tenant's shard on Neki.
CREATE TABLE IF NOT EXISTS tenant_secrets (
    tenant_id text NOT NULL,
    account text NOT NULL,
    key_id text NOT NULL,
    nonce bytea NOT NULL,
    ciphertext bytea NOT NULL,
    updated_at text NOT NULL,
    PRIMARY KEY (tenant_id, account),
    CHECK (octet_length(nonce) = 24),
    CHECK (octet_length(ciphertext) >= 16)
);
