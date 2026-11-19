//! Targeted presence projection (Postgres). Mirrors `presence_focus_repo.rs`:
//! all access and resource checks share one SQL snapshot, and errors carry no
//! candidate metadata.

use intent_core::{Error, PrincipalId, Result, WorkspaceId, CHIEF_WORKSPACE_ID};
use serde_json::Value;

use super::PgStore;

fn read_failed<E>(_: E) -> Error {
    Error::Internal("presence focus read failed".into())
}

impl PgStore {
    pub async fn authorized_presence_focus(
        &self,
        source: &WorkspaceId,
        viewer: &PrincipalId,
        person: &PrincipalId,
        candidates: &[Value],
    ) -> Result<Value> {
        let encoded = serde_json::to_string(candidates)
            .map_err(|_| Error::Internal("presence focus encoding failed".into()))?;
        let mut tx = self.begin().await.map_err(read_failed)?;
        // Select the index of the winner, not its JSON: a jsonb round trip
        // would reorder its keys, and the object from the caller goes out on
        // the wire as given.
        let row: Option<(Option<i64>,)> = sqlx::query_as(
            "WITH candidates AS (SELECT e.key::bigint AS ord, e.value AS target, \
                 sqlite_json_extract(e.value, '$.workspaceId') AS workspace_id, \
                 sqlite_json_extract(e.value, '$.agentId') AS agent_id, \
                 sqlite_json_extract(e.value, '$.noteId') AS note_id FROM sqlite_json_each($2) e), \
             scopes AS (SELECT $3::text AS id UNION SELECT workspace_id FROM candidates), \
             access AS (SELECT w.id AS workspace_id, p.id AS principal_id FROM scopes s \
                 JOIN workspace w ON w.tenant_id = $1 AND w.id = s.id \
                 JOIN principal p ON p.tenant_id = $1 AND p.id IN ($4, $5) \
                 WHERE w.status <> 'Deleted' AND \
                 (p.is_primary = 1 OR CASE WHEN EXISTS (SELECT 1 FROM host_member h \
                     WHERE h.tenant_id = $1 AND h.principal_id = p.id) \
                  THEN w.id <> $6 ELSE EXISTS (SELECT 1 FROM workspace_member m \
                     WHERE m.tenant_id = $1 AND m.workspace_id = w.id AND m.principal_id = p.id) END)), \
             permitted AS (SELECT c.* FROM candidates c \
                 WHERE EXISTS (SELECT 1 FROM access WHERE workspace_id = c.workspace_id AND principal_id = $4) \
                   AND EXISTS (SELECT 1 FROM access WHERE workspace_id = c.workspace_id AND principal_id = $5) \
                   AND NOT (c.agent_id IS NOT NULL AND c.note_id IS NOT NULL) \
                   AND (c.agent_id IS NULL OR EXISTS (SELECT 1 FROM agent_session a \
                       WHERE a.tenant_id = $1 AND a.id = c.agent_id AND a.workspace_id = c.workspace_id)) \
                   AND (c.note_id IS NULL OR EXISTS (SELECT 1 FROM note n \
                       WHERE n.tenant_id = $1 AND n.id = c.note_id AND n.workspace_id = c.workspace_id))) \
             SELECT (SELECT ord FROM permitted ORDER BY (workspace_id = $3) DESC, \
                 workspace_id COLLATE \"C\", \
                 CASE WHEN agent_id IS NOT NULL THEN 0 WHEN note_id IS NOT NULL THEN 1 ELSE 2 END, \
                 COALESCE(agent_id, note_id, '') COLLATE \"C\", ord LIMIT 1) AS ord \
             WHERE EXISTS (SELECT 1 FROM access WHERE workspace_id = $3 AND principal_id = $4) \
               AND EXISTS (SELECT 1 FROM access WHERE workspace_id = $3 AND principal_id = $5)",
        )
        .bind(self.tenant())
        .bind(&encoded)
        .bind(source.as_str())
        .bind(viewer.as_str())
        .bind(person.as_str())
        .bind(CHIEF_WORKSPACE_ID)
        .fetch_optional(&mut *tx)
        .await
        .map_err(read_failed)?;
        tx.commit().await.map_err(read_failed)?;
        let (ord,) = row.ok_or_else(|| Error::NotFound("presence focus".into()))?;
        ord.map_or(Ok(Value::Null), |ord| {
            usize::try_from(ord)
                .ok()
                .and_then(|i| candidates.get(i))
                .cloned()
                .ok_or_else(|| Error::Internal("presence focus decoding failed".into()))
        })
    }
}
