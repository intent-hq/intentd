-- BE-owned settings store (§9.2 / §9.8). Adds only the `settings` table;
-- 0001–0007 are untouched. Non-secret values persist here as `key` → JSON
-- `value`; sensitive settings (§9.8) live in the OS keychain, never in this
-- table. The DB row is authoritative for non-secret settings (§9.8).

CREATE TABLE settings (
  key   TEXT PRIMARY KEY,
  value TEXT NOT NULL                              -- JSON-encoded value
);
