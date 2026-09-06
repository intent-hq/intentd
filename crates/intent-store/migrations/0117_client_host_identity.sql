-- Client-side host identification mirrored from `client.hello` (REV-2,
-- `client.list`): the same `hostname` / `prettyHostname` / `deviceKind`
-- triple the daemon reports about itself in `host.status` /
-- `server.pairingInfo`, now supplied by the connecting client so a
-- browser-client pin can be labelled by device. All optional (clients that
-- pre-date the fields send none); refreshed on every hello.
ALTER TABLE client ADD COLUMN hostname TEXT;
ALTER TABLE client ADD COLUMN pretty_hostname TEXT;
ALTER TABLE client ADD COLUMN device_kind TEXT;
