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
