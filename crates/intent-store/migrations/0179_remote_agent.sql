-- Daemon-to-daemon remote agents, backend side: which agents run on which
-- exposed machine, and under which lease. The backend claims these agents
-- when it binds; the machine stops any running agent its backend does not
-- claim. Node and lease records (including the lease's link generation and
-- journal ack) live in the existing execution_node / node_lease tables.
CREATE TABLE remote_agent (
    agent_id TEXT PRIMARY KEY REFERENCES agent_session(id) ON DELETE CASCADE,
    workspace_id TEXT NOT NULL,
    node_id TEXT NOT NULL REFERENCES execution_node(id),
    lease_id TEXT NOT NULL REFERENCES node_lease(id),
    created_at TEXT NOT NULL
);
CREATE INDEX remote_agent_lease ON remote_agent(lease_id);

-- Daemon-to-daemon remote agents, machine side: the executor credentials this
-- daemon minted for backends, each scoped to one backend and usable only on
-- the /node/* executor link. Only the token's SHA-256 hash is stored.
-- Revocation sets revoked_at; rows are kept so a revoked token never admits
-- again.
CREATE TABLE executor_credential (
    id TEXT PRIMARY KEY,
    token_hash TEXT NOT NULL UNIQUE,
    backend_id TEXT NOT NULL,
    backend_name TEXT NOT NULL,
    backend_fingerprint TEXT,
    created_at TEXT NOT NULL,
    last_used_at TEXT,
    revoked_at TEXT
);
CREATE INDEX executor_credential_active ON executor_credential(created_at, id)
    WHERE revoked_at IS NULL;

-- One link binding per executor credential (replaces the single-node binding
-- file). `generation` is a u64 decimal string; only a strictly higher one
-- replaces the row.
CREATE TABLE executor_binding (
    credential_id TEXT PRIMARY KEY REFERENCES executor_credential(id) ON DELETE CASCADE,
    head_id TEXT NOT NULL,
    node_id TEXT NOT NULL,
    lease_id TEXT NOT NULL,
    generation TEXT NOT NULL,
    bound_at TEXT NOT NULL
);

-- Backend side: machines exposed to this daemon. The executor token is kept
-- in the secret store under remoteMachines.<id>.token, never here. `state`
-- is the resting state; `connected` is a live overlay and never persisted.
CREATE TABLE exposed_machine (
    id TEXT PRIMARY KEY,
    name TEXT NOT NULL,
    hosts_json TEXT NOT NULL,
    port INTEGER NOT NULL CHECK (port BETWEEN 1 AND 65535),
    tc_address TEXT,
    fingerprint TEXT NOT NULL,
    credential_id TEXT NOT NULL,
    platform_json TEXT,
    capabilities_json TEXT,
    observed_at TEXT,
    state TEXT NOT NULL CHECK (state IN ('disconnected', 'unreachable', 'revoked', 'update-required')),
    added_at TEXT NOT NULL,
    last_connected_at TEXT
);
