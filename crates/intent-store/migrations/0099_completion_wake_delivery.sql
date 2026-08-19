-- Delivered-completion dedup marker (intent-hq/monorepo#2842): records, per
-- (parent, child) pair, the identity of the child's most recently DELIVERED
-- terminal completion — the child's `completion_report_timestamp` at delivery
-- time. Completion-watch delivery consults it so a restart-recovery replay
-- (the boot reconciliation synthesizing the child's historical completion) or
-- a watch re-armed on an already-completed child never re-delivers a
-- completion the parent already received; a FUTURE completion carries a new
-- report timestamp and delivers normally. One row per pair (a new identity
-- overwrites — identities are monotonic per pair); rows cascade with either
-- endpoint's agent session.

CREATE TABLE completion_wake_delivery (
  parent_agent_id     TEXT NOT NULL REFERENCES agent_session(id) ON DELETE CASCADE,
  child_agent_id      TEXT NOT NULL REFERENCES agent_session(id) ON DELETE CASCADE,
  completion_identity TEXT NOT NULL,
  delivered_at        TEXT NOT NULL,
  PRIMARY KEY (parent_agent_id, child_agent_id)
);
