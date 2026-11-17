//! Script monitors, notification jobs and their durable queue admission
//! (Postgres). Mirrors `script_monitor_repo.rs`.
//!
//! Text ordering uses `COLLATE "C"` to match `SQLite`'s byte order.

use intent_core::{AgentId, Error, Result, ScriptMonitor, WorkspaceId};

use super::PgStore;
use crate::script_monitor_repo::ScriptNotificationJob;

fn db(error: impl std::fmt::Display) -> Error {
    Error::Internal(format!("script monitor storage: {error}"))
}

fn wake_key(id: &str) -> String {
    format!("script-monitor:{id}")
}

impl PgStore {
    pub async fn script_monitors(
        &self,
        ws: &WorkspaceId,
        agent: Option<&AgentId>,
    ) -> Result<Vec<ScriptMonitor>> {
        let mut tx = self.begin().await?;
        let rows: Vec<String> = sqlx::query_scalar(
            "SELECT row_json FROM script_monitor \
             WHERE tenant_id = $1 AND state = 'active' AND workspace_id = $2 \
             AND ($3::text IS NULL OR agent_id = $3) \
             ORDER BY created_at COLLATE \"C\", id COLLATE \"C\"",
        )
        .bind(self.tenant())
        .bind(ws.as_str())
        .bind(agent.map(AgentId::as_str))
        .fetch_all(&mut *tx)
        .await
        .map_err(db)?;
        tx.commit().await.map_err(db)?;
        rows.iter()
            .map(|s| serde_json::from_str(s).map_err(db))
            .collect()
    }

    pub async fn script_monitor_waiting(
        &self,
        ws: Option<&WorkspaceId>,
        agent: Option<&AgentId>,
    ) -> Result<Vec<(String, serde_json::Value)>> {
        let mut tx = self.begin().await?;
        let rows: Vec<(String, String)> = sqlx::query_as(
            "SELECT agent_id, json_build_object('monitorId', id, 'scriptId', script_id, \
               'runId', run_id, 'scriptName', row_json::jsonb -> 'scriptName', \
               'expiresAt', row_json::jsonb -> 'expiresAt')::text \
             FROM script_monitor WHERE tenant_id = $1 AND state = 'active' \
             AND ($2::text IS NULL OR workspace_id = $2) AND ($3::text IS NULL OR agent_id = $3) \
             ORDER BY created_at COLLATE \"C\", id COLLATE \"C\"",
        )
        .bind(self.tenant())
        .bind(ws.map(WorkspaceId::as_str))
        .bind(agent.map(AgentId::as_str))
        .fetch_all(&mut *tx)
        .await
        .map_err(db)?;
        tx.commit().await.map_err(db)?;
        rows.into_iter()
            .map(|(agent, json)| Ok((agent, serde_json::from_str(&json).map_err(db)?)))
            .collect()
    }

    pub async fn script_monitor_pending_for_agent(&self, agent: &AgentId) -> Result<bool> {
        self.exists(
            "SELECT EXISTS(SELECT 1 FROM script_notification_job \
               WHERE tenant_id = $1 AND agent_id = $2 \
             UNION ALL SELECT 1 FROM agent_queue_admission \
               WHERE tenant_id = $1 AND agent_id = $2 AND phase <> 'completed')",
            &[agent.as_str()],
        )
        .await
    }

    pub async fn script_monitor(&self, ws: &WorkspaceId, id: &str) -> Result<ScriptMonitor> {
        let mut tx = self.begin().await?;
        let value: Option<String> = sqlx::query_scalar(
            "SELECT row_json FROM script_monitor \
             WHERE tenant_id = $1 AND workspace_id = $2 AND id = $3",
        )
        .bind(self.tenant())
        .bind(ws.as_str())
        .bind(id)
        .fetch_optional(&mut *tx)
        .await
        .map_err(db)?;
        tx.commit().await.map_err(db)?;
        serde_json::from_str(
            &value.ok_or_else(|| Error::InvalidParams("unknown monitor ID".into()))?,
        )
        .map_err(db)
    }

    pub async fn insert_script_monitor(&self, row: &ScriptMonitor) -> Result<()> {
        if row.state != "active" {
            return Err(Error::InvalidParams(
                "only active monitors can be stored".into(),
            ));
        }
        let json = serde_json::to_string(row).map_err(db)?;
        let mut tx = self.begin_write().await?;
        sqlx::query(
            "INSERT INTO script_monitor \
             (tenant_id, id, workspace_id, agent_id, script_id, run_id, state, row_json, created_at) \
             VALUES ($1, $2, $3, $4, $5, $6, 'active', $7, $8)",
        )
        .bind(self.tenant())
        .bind(&row.monitor_id)
        .bind(row.workspace_id.as_str())
        .bind(row.agent_id.as_str())
        .bind(&row.script_id)
        .bind(&row.run_id)
        .bind(json)
        .bind(&row.created_at)
        .execute(&mut *tx)
        .await
        .map_err(db)?;
        tx.commit().await.map_err(db)
    }

    pub async fn settle_script_monitor(&self, row: &ScriptMonitor) -> Result<bool> {
        if !matches!(
            row.state.as_str(),
            "completed" | "expired" | "triggered" | "cancelled"
        ) {
            return Err(Error::InvalidParams(
                "terminal monitor decision required".into(),
            ));
        }
        let mut tx = self.begin_write().await?;
        let deleted = sqlx::query(
            "DELETE FROM script_monitor WHERE tenant_id = $1 AND id = $2 AND workspace_id = $3 \
             AND agent_id = $4 AND run_id = $5 AND state = 'active'",
        )
        .bind(self.tenant())
        .bind(&row.monitor_id)
        .bind(row.workspace_id.as_str())
        .bind(row.agent_id.as_str())
        .bind(&row.run_id)
        .execute(&mut *tx)
        .await
        .map_err(db)?
        .rows_affected();
        if deleted == 1 && matches!(row.state.as_str(), "completed" | "expired" | "triggered") {
            sqlx::query(
                "INSERT INTO script_notification_job \
                 (tenant_id, wake_id, workspace_id, agent_id, script_id, run_id, metadata, body, \
                  phase, created_at) \
                 VALUES ($1, $2, $3, $4, $5, $6, $7, $8, 'pending', $9)",
            )
            .bind(self.tenant())
            .bind(wake_key(&row.monitor_id))
            .bind(row.workspace_id.as_str())
            .bind(row.agent_id.as_str())
            .bind(&row.script_id)
            .bind(&row.run_id)
            .bind(intent_core::script_monitor::script_monitor_wake_metadata(row).to_string())
            .bind(intent_core::script_monitor::script_monitor_wake_text(row))
            .bind(&row.settled_at)
            .execute(&mut *tx)
            .await
            .map_err(db)?;
        }
        tx.commit().await.map_err(db)?;
        Ok(deleted == 1)
    }

    pub async fn script_monitor_cancel_intent(
        &self,
        ws: &WorkspaceId,
        id: &str,
        reserve: bool,
    ) -> Result<()> {
        let mut tx = self.begin_write().await?;
        sqlx::query(
            "UPDATE script_monitor SET cancel_intent = $4 \
             WHERE tenant_id = $1 AND workspace_id = $2 AND id = $3 AND state = 'active'",
        )
        .bind(self.tenant())
        .bind(ws.as_str())
        .bind(id)
        .bind(i64::from(reserve))
        .execute(&mut *tx)
        .await
        .map_err(db)?;
        tx.commit().await.map_err(db)
    }

    pub async fn pending_script_monitors(&self) -> Result<Vec<(ScriptMonitor, bool)>> {
        let mut tx = self.begin().await?;
        let rows: Vec<(String, i64)> = sqlx::query_as(
            "SELECT row_json, cancel_intent FROM script_monitor \
             WHERE tenant_id = $1 AND state = 'active' ORDER BY row_seq",
        )
        .bind(self.tenant())
        .fetch_all(&mut *tx)
        .await
        .map_err(db)?;
        tx.commit().await.map_err(db)?;
        rows.into_iter()
            .map(|(json, intent)| Ok((serde_json::from_str(&json).map_err(db)?, intent != 0)))
            .collect()
    }

    pub async fn script_notification_jobs(&self) -> Result<Vec<ScriptNotificationJob>> {
        let mut tx = self.begin().await?;
        let rows: Vec<(String, String, String, String, String)> = sqlx::query_as(
            "SELECT wake_id, workspace_id, agent_id, metadata, body FROM script_notification_job \
             WHERE tenant_id = $1 ORDER BY created_at COLLATE \"C\", wake_id COLLATE \"C\"",
        )
        .bind(self.tenant())
        .fetch_all(&mut *tx)
        .await
        .map_err(db)?;
        tx.commit().await.map_err(db)?;
        rows.into_iter()
            .map(|(wake_id, workspace_id, agent_id, metadata, body)| {
                Ok(ScriptNotificationJob {
                    wake_id,
                    workspace_id: WorkspaceId::from(workspace_id),
                    agent_id: AgentId::from(agent_id),
                    metadata: serde_json::from_str(&metadata).map_err(db)?,
                    body,
                })
            })
            .collect()
    }

    pub async fn script_monitor_wake_pending(&self, id: &str) -> Result<bool> {
        self.exists(
            "SELECT EXISTS(SELECT 1 FROM script_notification_job \
               WHERE tenant_id = $1 AND wake_id = $2)",
            &[&wake_key(id)],
        )
        .await
    }

    pub async fn script_monitor_wake_allowed(&self, id: &str) -> Result<bool> {
        self.exists(
            "SELECT EXISTS(SELECT 1 FROM script_notification_job \
               WHERE tenant_id = $1 AND wake_id = $2 \
             UNION ALL SELECT 1 FROM agent_queue_admission \
               WHERE tenant_id = $1 AND id = $2 AND phase <> 'completed')",
            &[&wake_key(id)],
        )
        .await
    }

    pub async fn script_monitor_wake_allowed_for_agent(
        &self,
        id: &str,
        agent: &AgentId,
        ws: &WorkspaceId,
    ) -> Result<bool> {
        self.exists(
            "SELECT EXISTS(SELECT 1 FROM ( \
               SELECT wake_id AS id, agent_id, workspace_id FROM script_notification_job \
                 WHERE tenant_id = $1 \
               UNION ALL SELECT id, agent_id, workspace_id FROM agent_queue_admission \
                 WHERE tenant_id = $1 AND phase <> 'completed') d \
             WHERE d.id = $2 AND d.agent_id = $3 AND d.workspace_id = $4)",
            &[&wake_key(id), agent.as_str(), ws.as_str()],
        )
        .await
    }

    pub async fn script_notification_delivery_owners(&self) -> Result<Vec<(AgentId, WorkspaceId)>> {
        let mut tx = self.begin().await?;
        let rows: Vec<(String, String)> = sqlx::query_as(
            "SELECT DISTINCT agent_id COLLATE \"C\", workspace_id COLLATE \"C\" \
             FROM agent_queue_admission WHERE tenant_id = $1 AND phase <> 'completed' \
             ORDER BY 1, 2",
        )
        .bind(self.tenant())
        .fetch_all(&mut *tx)
        .await
        .map_err(db)?;
        tx.commit().await.map_err(db)?;
        Ok(rows
            .into_iter()
            .map(|(agent, ws)| (AgentId::from(agent), WorkspaceId::from(ws)))
            .collect())
    }

    pub async fn finish_script_monitor_wake(&self, id: &str, suppress: bool) -> Result<()> {
        if !suppress {
            return Ok(());
        }
        let key = wake_key(id);
        let mut tx = self.begin_write().await?;
        for query in [
            "DELETE FROM script_notification_job WHERE tenant_id = $1 AND wake_id = $2",
            "DELETE FROM agent_queue_admission WHERE tenant_id = $1 AND id = $2",
            "DELETE FROM agent_queue WHERE tenant_id = $1 AND id = $2",
        ] {
            sqlx::query(query)
                .bind(self.tenant())
                .bind(&key)
                .execute(&mut *tx)
                .await
                .map_err(db)?;
        }
        tx.commit().await.map_err(db)
    }

    pub async fn workspaces_with_active_hooks_or_script_monitors(
        &self,
        workspace_ids: &[WorkspaceId],
    ) -> Result<std::collections::HashSet<WorkspaceId>> {
        if workspace_ids.is_empty() {
            return Ok(std::collections::HashSet::new());
        }
        let ids: Vec<&str> = workspace_ids.iter().map(WorkspaceId::as_str).collect();
        let mut tx = self.begin().await?;
        let rows: Vec<String> = sqlx::query_scalar(
            "SELECT workspace_id FROM hook WHERE tenant_id = $1 \
               AND workspace_id = ANY($2) AND state IN ('scheduled', 'running') \
             UNION SELECT workspace_id FROM script_monitor WHERE tenant_id = $1 \
               AND state = 'active' AND workspace_id = ANY($2)",
        )
        .bind(self.tenant())
        .bind(&ids)
        .fetch_all(&mut *tx)
        .await
        .map_err(db)?;
        tx.commit().await.map_err(db)?;
        Ok(rows.into_iter().map(WorkspaceId).collect())
    }

    pub async fn script_monitor_reservations(&self, ws: &WorkspaceId) -> Result<i64> {
        let mut tx = self.begin().await?;
        let count: i64 = sqlx::query_scalar(
            "SELECT (SELECT count(*) FROM script_monitor \
                 WHERE tenant_id = $1 AND workspace_id = $2) \
               + (SELECT count(*) FROM script_notification_job \
                 WHERE tenant_id = $1 AND workspace_id = $2)",
        )
        .bind(self.tenant())
        .bind(ws.as_str())
        .fetch_one(&mut *tx)
        .await
        .map_err(db)?;
        tx.commit().await.map_err(db)?;
        Ok(count)
    }

    pub async fn admit_script_notification(&self, row: &crate::AgentQueueRow) -> Result<bool> {
        let payload = row.payload.to_string();
        let mut tx = self.begin_write().await?;
        let inserted = sqlx::query(
            "INSERT INTO agent_queue_admission \
             (tenant_id, id, agent_id, workspace_id, turn_id, payload, created_at, phase) \
             SELECT $1, j.wake_id, j.agent_id, j.workspace_id, $2, $3, $4, 'pending' \
             FROM script_notification_job j \
             JOIN agent_session a ON a.tenant_id = $1 AND a.id = j.agent_id \
             JOIN workspace w ON w.tenant_id = $1 AND w.id = j.workspace_id \
             WHERE j.tenant_id = $1 AND j.wake_id = $5 AND j.agent_id = $6 \
             AND a.retired_at IS NULL AND w.archived = 0 \
             ON CONFLICT (tenant_id, id) DO NOTHING",
        )
        .bind(self.tenant())
        .bind(&row.turn_id)
        .bind(&payload)
        .bind(&row.created_at)
        .bind(&row.id)
        .bind(row.agent_id.as_str())
        .execute(&mut *tx)
        .await
        .map_err(db)?
        .rows_affected();
        if inserted == 1 {
            sqlx::query(
                "INSERT INTO agent_queue \
                 (tenant_id, id, agent_id, position, payload, created_at, turn_id) \
                 VALUES ($1, $2, $3, $4, $5, $6, $7) ON CONFLICT (tenant_id, id) DO NOTHING",
            )
            .bind(self.tenant())
            .bind(&row.id)
            .bind(row.agent_id.as_str())
            .bind(row.position)
            .bind(&payload)
            .bind(&row.created_at)
            .bind(&row.turn_id)
            .execute(&mut *tx)
            .await
            .map_err(db)?;
            sqlx::query(
                "DELETE FROM script_notification_job WHERE tenant_id = $1 AND wake_id = $2",
            )
            .bind(self.tenant())
            .bind(&row.id)
            .execute(&mut *tx)
            .await
            .map_err(db)?;
        }
        tx.commit().await.map_err(db)?;
        Ok(inserted == 1)
    }

    pub async fn accept_script_notification(&self, agent: &AgentId, id: &str) -> Result<bool> {
        let mut tx = self.begin_write().await?;
        let updated = sqlx::query(
            "UPDATE agent_queue_admission SET phase = 'accepted' \
             WHERE tenant_id = $1 AND id = $2 AND agent_id = $3 AND phase <> 'completed' \
             AND EXISTS(SELECT 1 FROM agent_session a \
               JOIN workspace w ON w.tenant_id = $1 AND w.id = a.workspace_id \
               WHERE a.tenant_id = $1 AND a.id = agent_queue_admission.agent_id \
               AND a.retired_at IS NULL AND w.archived = 0)",
        )
        .bind(self.tenant())
        .bind(wake_key(id))
        .bind(agent.as_str())
        .execute(&mut *tx)
        .await
        .map_err(db)?
        .rows_affected();
        tx.commit().await.map_err(db)?;
        Ok(updated == 1)
    }

    pub async fn complete_script_notification(&self, agent: &AgentId, id: &str) -> Result<()> {
        let mut tx = self.begin_write().await?;
        sqlx::query(
            "UPDATE agent_queue_admission SET phase = 'completed', payload = '{}' \
             WHERE tenant_id = $1 AND id = $2 AND agent_id = $3",
        )
        .bind(self.tenant())
        .bind(wake_key(id))
        .bind(agent.as_str())
        .execute(&mut *tx)
        .await
        .map_err(db)?;
        tx.commit().await.map_err(db)?;
        self.cleanup_completed_script_notifications().await
    }

    pub async fn continue_script_notification(
        &self,
        agent: &AgentId,
        id: &str,
        turn: &str,
        content: &str,
    ) -> Result<()> {
        let wake_id = wake_key(id);
        let mut tx = self.begin_write().await?;
        // Key order: the `sqlite_json_set` shim round-trips `payload` through
        // jsonb, so the rewritten object comes back with its keys sorted,
        // while SQLite's `json_set` keeps the original order and appends new
        // keys. `Value` equality and struct deserialization ignore the order;
        // only a caller re-emitting the raw `Value` would see it. The rewrite
        // stays in SQL (no Rust read-modify-write) so each row changes in one
        // statement with SQLite's `json_set` semantics.
        let updated = sqlx::query(
            "UPDATE agent_queue_admission SET turn_id = $4, payload = sqlite_json_set( \
               sqlite_json_set(payload, '$.turnId', $4::text, '$.content', $5::text), \
               '$.persisted', sqlite_json('true')) \
             WHERE tenant_id = $1 AND id = $2 AND agent_id = $3 AND phase = 'accepted'",
        )
        .bind(self.tenant())
        .bind(&wake_id)
        .bind(agent.as_str())
        .bind(turn)
        .bind(content)
        .execute(&mut *tx)
        .await
        .map_err(db)?
        .rows_affected();
        if updated != 0 {
            // A failed best-effort dequeue may leave the ordinary copy behind.
            // Preserve its other queue fields, but never recover its old turn.
            sqlx::query(
                "UPDATE agent_queue SET turn_id = $4, payload = sqlite_json_set( \
                   sqlite_json_set(payload, '$.turnId', $4::text, '$.content', $5::text), \
                   '$.persisted', sqlite_json('true')) \
                 WHERE tenant_id = $1 AND id = $2 AND agent_id = $3",
            )
            .bind(self.tenant())
            .bind(&wake_id)
            .bind(agent.as_str())
            .bind(turn)
            .bind(content)
            .execute(&mut *tx)
            .await
            .map_err(db)?;
        }
        tx.commit().await.map_err(db)
    }

    pub async fn cleanup_completed_script_notifications(&self) -> Result<()> {
        let mut tx = self.begin_write().await?;
        sqlx::query(
            "DELETE FROM agent_queue WHERE tenant_id = $1 AND id IN ( \
               SELECT id FROM agent_queue_admission WHERE tenant_id = $1 AND phase = 'completed')",
        )
        .bind(self.tenant())
        .execute(&mut *tx)
        .await
        .map_err(db)?;
        sqlx::query(
            "DELETE FROM agent_queue_admission WHERE tenant_id = $1 AND phase = 'completed'",
        )
        .bind(self.tenant())
        .execute(&mut *tx)
        .await
        .map_err(db)?;
        tx.commit().await.map_err(db)
    }

    pub async fn script_notification_owners(
        &self,
        ws: &WorkspaceId,
        agent: Option<&AgentId>,
    ) -> Result<Vec<AgentId>> {
        let mut tx = self.begin().await?;
        let ids: Vec<String> = sqlx::query_scalar(
            "SELECT agent_id FROM ( \
               SELECT workspace_id, agent_id FROM script_monitor WHERE tenant_id = $1 \
               UNION SELECT workspace_id, agent_id FROM script_notification_job WHERE tenant_id = $1 \
               UNION SELECT workspace_id, agent_id FROM agent_queue_admission WHERE tenant_id = $1) o \
             WHERE workspace_id = $2 AND ($3::text IS NULL OR agent_id = $3) \
             ORDER BY agent_id COLLATE \"C\"",
        )
        .bind(self.tenant())
        .bind(ws.as_str())
        .bind(agent.map(AgentId::as_str))
        .fetch_all(&mut *tx)
        .await
        .map_err(db)?;
        tx.commit().await.map_err(db)?;
        Ok(ids.into_iter().map(AgentId::from).collect())
    }

    pub async fn suppress_script_notifications(
        &self,
        ws: &WorkspaceId,
        agent: Option<&AgentId>,
    ) -> Result<()> {
        let mut tx = self.begin_write().await?;
        for query in [
            "DELETE FROM script_notification_job WHERE tenant_id = $1 AND workspace_id = $2 \
             AND ($3::text IS NULL OR agent_id = $3)",
            "DELETE FROM agent_queue_admission WHERE tenant_id = $1 AND workspace_id = $2 \
             AND ($3::text IS NULL OR agent_id = $3)",
            "DELETE FROM agent_queue WHERE tenant_id = $1 AND sqlite_json_valid(payload) \
             AND sqlite_json_extract(payload, '$.messageMetadata.type') = 'script_monitor_wake' \
             AND sqlite_json_extract(payload, '$.messageMetadata.workspaceId') = $2 \
             AND ($3::text IS NULL OR agent_id = $3)",
        ] {
            sqlx::query(query)
                .bind(self.tenant())
                .bind(ws.as_str())
                .bind(agent.map(AgentId::as_str))
                .execute(&mut *tx)
                .await
                .map_err(db)?;
        }
        tx.commit().await.map_err(db)
    }

    /// One `SELECT EXISTS(…)` with the tenant as `$1` and `binds` after it.
    async fn exists(&self, sql: &'static str, binds: &[&str]) -> Result<bool> {
        let mut tx = self.begin().await?;
        let mut query = sqlx::query_scalar(sql).bind(self.tenant());
        for value in binds {
            query = query.bind(*value);
        }
        let found: bool = query.fetch_one(&mut *tx).await.map_err(db)?;
        tx.commit().await.map_err(db)?;
        Ok(found)
    }
}
