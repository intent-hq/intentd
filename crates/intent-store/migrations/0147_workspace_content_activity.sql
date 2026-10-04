-- Recorded conversation/note activity, independent of metadata bookkeeping.
-- High-water mark: deletion does not erase the fact that content was recorded.
-- Old rows are reconstructed only from retained content, never updated_at or
-- last_activity on workspace/session (those may contain maintenance times).
ALTER TABLE workspace ADD COLUMN last_content_activity TEXT;

-- One metadata-only pass over each content table; no transcript hydration and
-- no per-workspace history scan on list/get. Compare instants, not ISO strings
-- (offsets and optional fractional seconds do not sort lexicographically).
CREATE TEMP TABLE workspace_content_backfill AS
SELECT workspace_id, stamp FROM (
  SELECT workspace_id, stamp,
         ROW_NUMBER() OVER (PARTITION BY workspace_id ORDER BY julianday(stamp) DESC, stamp DESC) AS rank
  FROM (
    SELECT workspace_id, updated_at AS stamp FROM note WHERE julianday(updated_at) IS NOT NULL
    UNION ALL
    SELECT s.workspace_id, m.created_at AS stamp
    FROM agent_message m JOIN agent_session s ON s.id = m.agent_id
    WHERE m.role IN ('user', 'assistant') AND julianday(m.created_at) IS NOT NULL
  )
) WHERE rank = 1;
CREATE UNIQUE INDEX workspace_content_backfill_id ON workspace_content_backfill(workspace_id);
UPDATE workspace SET last_content_activity = (
  SELECT stamp FROM workspace_content_backfill b WHERE b.workspace_id = workspace.id
);
DROP TABLE workspace_content_backfill;

CREATE TRIGGER workspace_content_note_insert AFTER INSERT ON note
WHEN julianday(new.updated_at) IS NOT NULL
BEGIN
  UPDATE workspace SET last_content_activity = new.updated_at
  WHERE id = new.workspace_id AND (last_content_activity IS NULL
    OR julianday(last_content_activity) IS NULL
    OR julianday(new.updated_at) > julianday(last_content_activity));
END;

CREATE TRIGGER workspace_content_note_update AFTER UPDATE OF updated_at, workspace_id ON note
WHEN julianday(new.updated_at) IS NOT NULL
BEGIN
  UPDATE workspace SET last_content_activity = new.updated_at
  WHERE id = new.workspace_id AND (last_content_activity IS NULL
    OR julianday(last_content_activity) IS NULL
    OR julianday(new.updated_at) > julianday(last_content_activity));
END;

CREATE TRIGGER workspace_content_message_insert AFTER INSERT ON agent_message
WHEN new.role IN ('user', 'assistant') AND julianday(new.created_at) IS NOT NULL
BEGIN
  UPDATE workspace SET last_content_activity = new.created_at
  WHERE id = (SELECT workspace_id FROM agent_session WHERE id = new.agent_id)
    AND (last_content_activity IS NULL OR julianday(last_content_activity) IS NULL
      OR julianday(new.created_at) > julianday(last_content_activity));
END;

CREATE TRIGGER workspace_content_message_update AFTER UPDATE OF created_at, role, agent_id ON agent_message
WHEN new.role IN ('user', 'assistant') AND julianday(new.created_at) IS NOT NULL
BEGIN
  UPDATE workspace SET last_content_activity = new.created_at
  WHERE id = (SELECT workspace_id FROM agent_session WHERE id = new.agent_id)
    AND (last_content_activity IS NULL OR julianday(last_content_activity) IS NULL
      OR julianday(new.created_at) > julianday(last_content_activity));
END;
