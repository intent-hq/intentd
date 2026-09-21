-- Client-side host identification mirrored from `client.hello` (REV-2,
-- `client.list`): the same `hostname` / `prettyHostname` / `deviceKind`
-- triple the daemon reports about itself in `host.status` /
-- `server.pairingInfo`, now supplied by the connecting client so a
-- browser-client pin can be labelled by device. All optional (clients that
-- pre-date the fields send none); refreshed on every hello.
ALTER TABLE client ADD COLUMN hostname TEXT;
ALTER TABLE client ADD COLUMN pretty_hostname TEXT;
ALTER TABLE client ADD COLUMN device_kind TEXT;
-- Set on every `client.hello`; NULL on a row minted only to satisfy the
-- `draft` FK for an anonymous (never-hello'd) connection. A
-- `workspace.setBrowserClient` pin requires a hello'd client — a draft-only
-- id can never resolve to a reverse connection.
ALTER TABLE client ADD COLUMN last_hello_at TEXT;
-- Backfill: pre-upgrade rows carry no hello provenance, but `name` is a
-- proxy — the anonymous-draft placeholder was always minted with a NULL
-- name, while `client.hello` carries the client's name. Rows with a name
-- count as hello'd at their last touch, so a previously connected but
-- currently offline client stays pinnable; nameless rows (draft-only
-- placeholders, or a hello'd row that somehow had no name) fail closed and
-- become pinnable on their next hello, which stamps them. Kept on one line:
-- the store test re-runs this statement alone.
UPDATE client SET last_hello_at = last_seen WHERE last_hello_at IS NULL AND name IS NOT NULL;
