-- Recreate the 0114/0122 partial covering index for the unread top-level
-- session derivation (§5.1) with `notifications_muted = 0` in its WHERE
-- clause: a muted session (agent_session.notifications_muted, 0123) must not
-- raise the workspace blue dot — that is what silences the sidebar, the HUD
-- counters, and the iOS workspace list, which all read daemon-derived
-- workspace state. The predicate now carries `AND notifications_muted = 0`;
-- the index WHERE must carry the same term so the three consumers' INDEXED
-- BY keeps planning (the query WHERE must imply the index WHERE) and the
-- term stays answered from the index instead of the main table. Index-only
-- DDL: no row is rewritten. Unmuting (`notifications_muted` back to 0) makes
-- the same row count again — the derivation is purely a read over the column.
--
-- Everything else is byte-identical to 0122; the `->>` spelling and the
-- INDEXED BY requirement documented there still apply, and the plan-shape
-- test in agent_repo.rs guards both.
DROP INDEX idx_agent_session_unread_top_level;
CREATE INDEX idx_agent_session_unread_top_level ON agent_session(
    workspace_id,
    last_message_id,
    metadata ->> '$.lastSeenMessageId'
) WHERE parent_agent_id IS NULL
    AND is_background = 0
    AND status <> 'deleted'
    AND last_message_id IS NOT NULL
    AND last_message_role = 'assistant'
    AND retired_at IS NULL
    AND notifications_muted = 0;
