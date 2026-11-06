-- Daemon-to-daemon remote agents, backend side: the submodule repositories
-- a final stop admits the agent's last module pushes into, as a JSON array
-- of { path, moduleKey, gitDir } (nodeProtocol 10, prepared checkout).
ALTER TABLE remote_final_stop ADD COLUMN submodules TEXT NOT NULL DEFAULT '[]';
