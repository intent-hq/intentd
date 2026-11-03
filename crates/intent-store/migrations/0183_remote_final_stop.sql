-- Daemon-to-daemon remote agents, backend side: final stops (agent deleted
-- or retired, workspace retired or deleted, machine removed) the machine has
-- not acknowledged yet. A row is written before the agent's session or
-- workspace is torn down and is resent at every bind, so a head restart
-- never forgets it. While it exists, the machine's final WIP push for the
-- agent is accepted into `repo_path`, even after the session and its
-- remote_agent row are gone; hence no foreign key to agent_session. The row
-- is dropped on the stop's acknowledgement, or after the bound (7 days).
CREATE TABLE remote_final_stop (
    agent_id TEXT PRIMARY KEY,
    workspace_id TEXT NOT NULL,
    machine_id TEXT NOT NULL,
    lease_id TEXT NOT NULL,
    stop_id TEXT NOT NULL,
    reason TEXT NOT NULL CHECK (reason IN ('agent-deleted', 'agent-retired', 'workspace-retired', 'workspace-deleted', 'machine-removed')),
    repo_path TEXT NOT NULL,
    requested_at TEXT NOT NULL
);
CREATE INDEX remote_final_stop_lease ON remote_final_stop(lease_id);
