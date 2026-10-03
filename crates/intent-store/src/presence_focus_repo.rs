//! Targeted presence projection. All access and resource checks share one SQL snapshot.
use crate::Store;
use intent_core::{Error, PrincipalId, Result, WorkspaceId, CHIEF_WORKSPACE_ID};
use serde_json::Value;
use sqlx::Row;

impl Store {
    /// Resolve one known source member's already-collected focus candidates.
    /// No global workspace/person scan; no profile or hidden target leaves SQL.
    ///
    /// # Errors
    /// `NotFound` is deliberately identical for unknown/forbidden source/person.
    /// `Internal` reports database failures without candidate metadata.
    pub async fn authorized_presence_focus(
        &self,
        source: &WorkspaceId,
        viewer: &PrincipalId,
        person: &PrincipalId,
        candidates: &[Value],
    ) -> Result<Value> {
        let row = sqlx::query(
            "WITH candidates AS (SELECT value AS target, json_extract(value, '$.workspaceId') AS workspace_id, \
                 json_extract(value, '$.agentId') AS agent_id, json_extract(value, '$.noteId') AS note_id FROM json_each(?1)), \
             scopes AS (SELECT ?2 AS id UNION SELECT workspace_id FROM candidates), \
             access AS (SELECT w.id AS workspace_id, p.id AS principal_id FROM scopes s JOIN workspace w ON w.id = s.id \
                 JOIN principal p ON p.id IN (?3, ?4) WHERE w.status <> 'Deleted' AND \
                 (p.is_primary = 1 OR CASE WHEN EXISTS (SELECT 1 FROM host_member h WHERE h.principal_id = p.id) \
                  THEN w.id <> ?5 ELSE EXISTS (SELECT 1 FROM workspace_member m WHERE m.workspace_id = w.id AND m.principal_id = p.id) END)), \
             permitted AS (SELECT c.* FROM candidates c \
                 WHERE EXISTS (SELECT 1 FROM access WHERE workspace_id = c.workspace_id AND principal_id = ?3) \
                   AND EXISTS (SELECT 1 FROM access WHERE workspace_id = c.workspace_id AND principal_id = ?4) \
                   AND NOT (c.agent_id IS NOT NULL AND c.note_id IS NOT NULL) \
                   AND (c.agent_id IS NULL OR EXISTS (SELECT 1 FROM agent_session a WHERE a.id = c.agent_id AND a.workspace_id = c.workspace_id)) \
                   AND (c.note_id IS NULL OR EXISTS (SELECT 1 FROM note n WHERE n.id = c.note_id AND n.workspace_id = c.workspace_id))) \
             SELECT (SELECT target FROM permitted ORDER BY (workspace_id = ?2) DESC, workspace_id, \
                 CASE WHEN agent_id IS NOT NULL THEN 0 WHEN note_id IS NOT NULL THEN 1 ELSE 2 END, \
                 COALESCE(agent_id, note_id, '') LIMIT 1) AS target \
             WHERE EXISTS (SELECT 1 FROM access WHERE workspace_id = ?2 AND principal_id = ?3) \
               AND EXISTS (SELECT 1 FROM access WHERE workspace_id = ?2 AND principal_id = ?4)",
        )
        .bind(serde_json::to_string(candidates).map_err(|_| Error::Internal("presence focus encoding failed".into()))?)
        .bind(source.as_str()).bind(viewer.as_str()).bind(person.as_str()).bind(CHIEF_WORKSPACE_ID)
        .fetch_optional(self.read_pool()).await
        .map_err(|_| Error::Internal("presence focus read failed".into()))?
        .ok_or_else(|| Error::NotFound("presence focus".into()))?;
        row.get::<Option<String>, _>("target")
            .map_or(Ok(Value::Null), |value| {
                serde_json::from_str(&value)
                    .map_err(|_| Error::Internal("presence focus decoding failed".into()))
            })
    }
}
