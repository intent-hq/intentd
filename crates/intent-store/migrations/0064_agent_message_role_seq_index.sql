-- Newest user/assistant per-session projection index (monorepo#1010). The
-- `agent.list` / `agent.get` projections window `agent_message` by
-- (agent_id, role) ordered by seq DESC; the only prior index was
-- UNIQUE(agent_id, seq) from 0004, so the role-partitioned window had to
-- sort every user/assistant row with a temp b-tree. With this index the
-- windowed subquery runs as a covering index seek (agent_id=? AND role=?)
-- and never touches the base table until the per-partition winners are
-- joined back for their content/metadata.

CREATE INDEX idx_agent_message_agent_role_seq
  ON agent_message(agent_id, role, seq DESC);
