//! Active watches, transient notification jobs and durable queue admission.
use crate::Store;
use intent_core::{AgentId, Error, Result, ScriptMonitor, WorkspaceId};

fn db(error: impl std::fmt::Display) -> Error {
    Error::Internal(format!("script monitor storage: {error}"))
}

impl Store {
    /// Read active monitor rows, in canonical order.
    /// # Errors
    /// Fails closed on database/decoding errors.
    pub async fn script_monitors(
        &self,
        ws: &WorkspaceId,
        agent: Option<&AgentId>,
    ) -> Result<Vec<ScriptMonitor>> {
        let rows: Vec<String> = sqlx::query_scalar("SELECT row_json FROM script_monitor WHERE state='active' AND workspace_id = ? AND (? IS NULL OR agent_id = ?) ORDER BY created_at,id")
            .bind(ws.as_str()).bind(agent.map(AgentId::as_str)).bind(agent.map(AgentId::as_str))
            .fetch_all(self.read_pool()).await.map_err(db)?;
        rows.into_iter()
            .map(|s| serde_json::from_str(&s).map_err(db))
            .collect()
    }

    /// Project active waiting metadata without hydrating output or terminal history.
    /// # Errors
    /// Returns storage/decoding errors.
    pub async fn script_monitor_waiting(
        &self,
        ws: Option<&WorkspaceId>,
        agent: Option<&AgentId>,
    ) -> Result<Vec<(String, serde_json::Value)>> {
        let rows:Vec<(String,String)>=sqlx::query_as("SELECT agent_id,json_object('monitorId',id,'scriptId',script_id,'runId',run_id,'scriptName',json_extract(row_json,'$.scriptName'),'expiresAt',json_extract(row_json,'$.expiresAt')) FROM script_monitor WHERE state='active' AND (? IS NULL OR workspace_id=?) AND (? IS NULL OR agent_id=?) ORDER BY created_at,id")
            .bind(ws.map(WorkspaceId::as_str)).bind(ws.map(WorkspaceId::as_str)).bind(agent.map(AgentId::as_str)).bind(agent.map(AgentId::as_str)).fetch_all(self.read_pool()).await.map_err(db)?;
        rows.into_iter()
            .map(|(agent, json)| Ok((agent, serde_json::from_str(&json).map_err(db)?)))
            .collect()
    }

    /// Whether an owner still has a committed notification awaiting delivery.
    /// # Errors
    /// Returns database failures so callers can defer settlement safely.
    pub async fn script_monitor_pending_for_agent(&self, agent: &AgentId) -> Result<bool> {
        sqlx::query_scalar(
            "SELECT EXISTS(SELECT 1 FROM script_notification_job WHERE agent_id=? UNION ALL SELECT 1 FROM agent_queue_admission WHERE agent_id=? AND phase != 'completed')",
        )
        .bind(agent.as_str())
        .bind(agent.as_str())
        .fetch_one(self.read_pool())
        .await
        .map_err(db)
    }

    /// Read one scoped row. Foreign IDs are indistinguishable from missing IDs.
    /// # Errors
    /// Returns invalid parameters for unknown IDs or a storage error.
    pub async fn script_monitor(&self, ws: &WorkspaceId, id: &str) -> Result<ScriptMonitor> {
        let value: Option<String> = sqlx::query_scalar(
            "SELECT row_json FROM script_monitor WHERE workspace_id = ? AND id = ?",
        )
        .bind(ws.as_str())
        .bind(id)
        .fetch_optional(self.read_pool())
        .await
        .map_err(db)?;
        serde_json::from_str(
            &value.ok_or_else(|| Error::InvalidParams("unknown monitor ID".into()))?,
        )
        .map_err(db)
    }

    /// Insert under the durable single-owner constraint; caller serializes admission.
    /// # Errors
    /// Storage errors never acknowledge a memory-only watch.
    pub async fn insert_script_monitor(&self, row: &ScriptMonitor) -> Result<()> {
        if row.state != "active" {
            return Err(Error::InvalidParams(
                "only active monitors can be stored".into(),
            ));
        }
        let json = serde_json::to_string(row).map_err(db)?;
        sqlx::query("INSERT INTO script_monitor(id,workspace_id,agent_id,script_id,run_id,state,row_json,created_at) VALUES(?,?,?,?,?,'active',?,?)")
            .bind(&row.monitor_id).bind(row.workspace_id.as_str()).bind(row.agent_id.as_str())
            .bind(&row.script_id).bind(&row.run_id).bind(json).bind(&row.created_at)
            .execute(self.write_pool()).await.map_err(db)?;
        Ok(())
    }

    /// Atomically retire an active watch and reserve its notification, without history.
    /// # Errors
    /// A failed write rolls back both changes.
    pub async fn settle_script_monitor(&self, row: &ScriptMonitor) -> Result<bool> {
        if !matches!(
            row.state.as_str(),
            "completed" | "expired" | "triggered" | "cancelled"
        ) {
            return Err(Error::InvalidParams(
                "terminal monitor decision required".into(),
            ));
        }
        let mut tx = self.write_pool().begin().await.map_err(db)?;
        let deleted = sqlx::query("DELETE FROM script_monitor WHERE id=? AND workspace_id=? AND agent_id=? AND run_id=? AND state='active'")
            .bind(&row.monitor_id).bind(row.workspace_id.as_str()).bind(row.agent_id.as_str()).bind(&row.run_id)
            .execute(&mut *tx).await.map_err(db)?.rows_affected();
        if deleted == 1 && matches!(row.state.as_str(), "completed" | "expired" | "triggered") {
            sqlx::query("INSERT INTO script_notification_job(wake_id,workspace_id,agent_id,script_id,run_id,metadata,body,phase,created_at) VALUES(?,?,?,?,?,?,?,'pending',?)")
                .bind(format!("script-monitor:{}",row.monitor_id)).bind(row.workspace_id.as_str()).bind(row.agent_id.as_str())
                .bind(&row.script_id).bind(&row.run_id)
                .bind(intent_core::script_monitor::script_monitor_wake_metadata(row).to_string())
                .bind(intent_core::script_monitor::script_monitor_wake_text(row)).bind(&row.settled_at)
                .execute(&mut *tx).await.map_err(db)?;
        }
        tx.commit().await.map_err(db)?;
        Ok(deleted == 1)
    }

    /// Reserve a guarded cancellation before touching the owned process.
    /// # Errors
    /// Returns database failures.
    pub async fn script_monitor_cancel_intent(
        &self,
        ws: &WorkspaceId,
        id: &str,
        reserve: bool,
    ) -> Result<()> {
        sqlx::query("UPDATE script_monitor SET cancel_intent=? WHERE workspace_id=? AND id=? AND state='active'")
            .bind(reserve).bind(ws.as_str()).bind(id).execute(self.write_pool()).await.map_err(db)?;
        Ok(())
    }

    /// Read recovery/outbox work; this is never a hot agent-list read.
    /// # Errors
    /// Returns storage/decoding failures.
    pub async fn pending_script_monitors(&self) -> Result<Vec<(ScriptMonitor, bool)>> {
        let rows: Vec<(String, bool)> = sqlx::query_as(
            "SELECT row_json,cancel_intent FROM script_monitor WHERE state='active'",
        )
        .fetch_all(self.read_pool())
        .await
        .map_err(db)?;
        rows.into_iter()
            .map(|(json, intent)| Ok((serde_json::from_str(&json).map_err(db)?, intent)))
            .collect()
    }

    /// Read pending notification work, never monitor history.
    /// # Errors
    /// Returns storage/decoding failures.
    pub async fn script_notification_jobs(&self) -> Result<Vec<ScriptNotificationJob>> {
        let rows: Vec<(String,String,String,String,String)> = sqlx::query_as("SELECT wake_id,workspace_id,agent_id,metadata,body FROM script_notification_job ORDER BY created_at,wake_id")
            .fetch_all(self.read_pool()).await.map_err(db)?;
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

    /// Read the pending notification fence.
    /// # Errors
    /// Returns database errors.
    pub async fn script_monitor_wake_pending(&self, id: &str) -> Result<bool> {
        sqlx::query_scalar("SELECT EXISTS(SELECT 1 FROM script_notification_job WHERE wake_id=?)")
            .bind(format!("script-monitor:{id}"))
            .fetch_one(self.read_pool())
            .await
            .map_err(db)
    }

    /// Delivery authority belongs to transient work, never a transcript alone.
    /// # Errors
    /// Returns database errors.
    pub async fn script_monitor_wake_allowed(&self, id: &str) -> Result<bool> {
        let key = format!("script-monitor:{id}");
        sqlx::query_scalar("SELECT EXISTS(SELECT 1 FROM script_notification_job WHERE wake_id=? UNION ALL SELECT 1 FROM agent_queue_admission WHERE id=? AND phase != 'completed')")
            .bind(&key).bind(&key).fetch_one(self.read_pool()).await.map_err(db)
    }

    /// Scope delivery authority to its original owner and workspace.
    /// # Errors
    /// Returns database errors.
    pub async fn script_monitor_wake_allowed_for_agent(
        &self,
        id: &str,
        agent: &AgentId,
        ws: &WorkspaceId,
    ) -> Result<bool> {
        let key = format!("script-monitor:{id}");
        sqlx::query_scalar("SELECT EXISTS(SELECT 1 FROM (SELECT wake_id AS id,agent_id,workspace_id FROM script_notification_job UNION ALL SELECT id,agent_id,workspace_id FROM agent_queue_admission WHERE phase != 'completed') WHERE id=? AND agent_id=? AND workspace_id=?)")
            .bind(key).bind(agent.as_str()).bind(ws.as_str()).fetch_one(self.read_pool()).await.map_err(db)
    }
    /// Durable ordinary queue owners to kick after restart or temporary holds.
    /// # Errors
    /// Returns database errors.
    pub async fn script_notification_delivery_owners(&self) -> Result<Vec<(AgentId, WorkspaceId)>> {
        let rows: Vec<(String, String)> =
            sqlx::query_as("SELECT DISTINCT agent_id,workspace_id FROM agent_queue_admission WHERE phase != 'completed'")
                .fetch_all(self.read_pool())
                .await
                .map_err(db)?;
        Ok(rows
            .into_iter()
            .map(|(agent, ws)| (AgentId::from(agent), WorkspaceId::from(ws)))
            .collect())
    }

    /// Suppress outstanding work. Message persistence alone cannot finish a job.
    /// # Errors
    /// Returns database errors.
    pub async fn finish_script_monitor_wake(&self, id: &str, suppress: bool) -> Result<()> {
        if !suppress {
            return Ok(());
        }
        let key = format!("script-monitor:{id}");
        let mut tx = self.write_pool().begin().await.map_err(db)?;
        for query in [
            "DELETE FROM script_notification_job WHERE wake_id=?",
            "DELETE FROM agent_queue_admission WHERE id=?",
            "DELETE FROM agent_queue WHERE id=?",
        ] {
            sqlx::query(query)
                .bind(&key)
                .execute(&mut *tx)
                .await
                .map_err(db)?;
        }
        tx.commit().await.map_err(db)
    }

    /// Requested workspaces that own an active hook or an `active` script
    /// monitor — the two row-backed `Workspace.waiting` signals (§5.1) — in
    /// ONE statement, so `workspace.list` / `workspace.subscribe` keep their
    /// constant statement budget. No hook or monitor rows are hydrated.
    /// # Errors
    /// Returns database errors.
    pub async fn workspaces_with_active_hooks_or_script_monitors(
        &self,
        workspace_ids: &[WorkspaceId],
    ) -> Result<std::collections::HashSet<WorkspaceId>> {
        if workspace_ids.is_empty() {
            return Ok(std::collections::HashSet::new());
        }
        let placeholders = vec!["?"; workspace_ids.len()].join(",");
        let sql = format!(
            "SELECT workspace_id FROM hook WHERE workspace_id IN ({placeholders}) \
             AND state IN ('scheduled', 'running') \
             UNION SELECT workspace_id FROM script_monitor WHERE state='active' \
             AND workspace_id IN ({placeholders})"
        );
        let mut query = sqlx::query_scalar::<_, String>(&sql);
        for _ in 0..2 {
            for id in workspace_ids {
                query = query.bind(id.as_str());
            }
        }
        let rows = query.fetch_all(self.read_pool()).await.map_err(db)?;
        Ok(rows.into_iter().map(WorkspaceId).collect())
    }

    /// Outstanding reservations include active watches and untransferred jobs.
    /// # Errors
    /// Returns database errors.
    pub async fn script_monitor_reservations(&self, ws: &WorkspaceId) -> Result<i64> {
        sqlx::query_scalar("SELECT (SELECT count(*) FROM script_monitor WHERE workspace_id=?)+(SELECT count(*) FROM script_notification_job WHERE workspace_id=?)")
            .bind(ws.as_str()).bind(ws.as_str()).fetch_one(self.read_pool()).await.map_err(db)
    }

    /// Transfer one job to durable ordinary queue ownership in a single commit.
    /// # Errors
    /// Any failure preserves the job and leaves no partial admission.
    pub async fn admit_script_notification(&self, row: &crate::AgentQueueRow) -> Result<bool> {
        let mut tx = self.write_pool().begin().await.map_err(db)?;
        let inserted=sqlx::query("INSERT INTO agent_queue_admission(id,agent_id,workspace_id,turn_id,payload,created_at,phase) SELECT wake_id,j.agent_id,j.workspace_id,?,?,?,'pending' FROM script_notification_job j JOIN agent_session a ON a.id=j.agent_id JOIN workspace w ON w.id=j.workspace_id WHERE wake_id=? AND j.agent_id=? AND a.retired_at IS NULL AND w.archived=0 ON CONFLICT(id) DO NOTHING")
            .bind(&row.turn_id).bind(row.payload.to_string()).bind(&row.created_at).bind(&row.id).bind(row.agent_id.as_str())
            .execute(&mut *tx).await.map_err(db)?.rows_affected();
        if inserted == 1 {
            sqlx::query("INSERT OR IGNORE INTO agent_queue(id,agent_id,position,payload,created_at,turn_id) VALUES(?,?,?,?,?,?)")
                .bind(&row.id).bind(row.agent_id.as_str()).bind(row.position).bind(row.payload.to_string()).bind(&row.created_at).bind(&row.turn_id)
                .execute(&mut *tx).await.map_err(db)?;
            sqlx::query("DELETE FROM script_notification_job WHERE wake_id=?")
                .bind(&row.id)
                .execute(&mut *tx)
                .await
                .map_err(db)?;
        }
        tx.commit().await.map_err(db)?;
        Ok(inserted == 1)
    }

    /// Repeated guards of the same durable turn are valid; completed receipts are gone.
    /// # Errors
    /// Returns database errors.
    pub async fn accept_script_notification(&self, agent: &AgentId, id: &str) -> Result<bool> {
        let result=sqlx::query("UPDATE agent_queue_admission SET phase='accepted' WHERE id=? AND agent_id=? AND phase != 'completed' AND EXISTS(SELECT 1 FROM agent_session a JOIN workspace w ON w.id=a.workspace_id WHERE a.id=agent_queue_admission.agent_id AND a.retired_at IS NULL AND w.archived=0)")
            .bind(format!("script-monitor:{id}")).bind(agent.as_str()).execute(self.write_pool()).await.map_err(db)?;
        Ok(result.rows_affected() == 1)
    }

    /// Durably settle delivery before best-effort physical cleanup. A failed
    /// DELETE cannot make a completed turn eligible for replay on restart.
    /// # Errors
    /// Returns database errors; a committed completion is still authoritative
    /// if the subsequent cleanup fails.
    pub async fn complete_script_notification(&self, agent: &AgentId, id: &str) -> Result<()> {
        sqlx::query("UPDATE agent_queue_admission SET phase='completed',payload='{}' WHERE id=? AND agent_id=?")
            .bind(format!("script-monitor:{id}")).bind(agent.as_str())
            .execute(self.write_pool()).await.map_err(db)?;
        self.cleanup_completed_script_notifications().await
    }

    /// Continue a notification on the worker's persisted warning turn. The
    /// durable recovery payload and any surviving queue snapshot follow that
    /// turn atomically before it is submitted.
    /// # Errors
    /// Returns database failures.
    pub async fn continue_script_notification(
        &self,
        agent: &AgentId,
        id: &str,
        turn: &str,
        content: &str,
    ) -> Result<()> {
        let wake_id = format!("script-monitor:{id}");
        let mut tx = self.write_pool().begin().await.map_err(db)?;
        let updated = sqlx::query("UPDATE agent_queue_admission SET turn_id=?,payload=json_set(payload,'$.turnId',?,'$.content',?,'$.persisted',json('true')) WHERE id=? AND agent_id=? AND phase='accepted'")
            .bind(turn).bind(turn).bind(content).bind(&wake_id).bind(agent.as_str())
            .execute(&mut *tx).await.map_err(db)?.rows_affected();
        if updated != 0 {
            // A failed best-effort dequeue may leave the ordinary copy behind.
            // Preserve its other queue fields, but never recover its old turn.
            sqlx::query("UPDATE agent_queue SET turn_id=?,payload=json_set(payload,'$.turnId',?,'$.content',?,'$.persisted',json('true')) WHERE id=? AND agent_id=?")
                .bind(turn).bind(turn).bind(content).bind(&wake_id).bind(agent.as_str())
                .execute(&mut *tx).await.map_err(db)?;
        }
        tx.commit().await.map_err(db)
    }

    /// Retry physical cleanup independently of delivery settlement.
    /// # Errors
    /// Returns database failures; completed rows remain non-runnable.
    pub async fn cleanup_completed_script_notifications(&self) -> Result<()> {
        let mut tx = self.write_pool().begin().await.map_err(db)?;
        sqlx::query("DELETE FROM agent_queue WHERE id IN (SELECT id FROM agent_queue_admission WHERE phase='completed')")
            .execute(&mut *tx).await.map_err(db)?;
        sqlx::query("DELETE FROM agent_queue_admission WHERE phase='completed'")
            .execute(&mut *tx)
            .await
            .map_err(db)?;
        tx.commit().await.map_err(db)
    }

    /// Owners affected by lifecycle cleanup, including already-retired watches.
    /// # Errors
    /// Returns database errors.
    pub async fn script_notification_owners(
        &self,
        ws: &WorkspaceId,
        agent: Option<&AgentId>,
    ) -> Result<Vec<AgentId>> {
        let ids:Vec<String>=sqlx::query_scalar("SELECT agent_id FROM (SELECT workspace_id,agent_id FROM script_monitor UNION SELECT workspace_id,agent_id FROM script_notification_job UNION SELECT workspace_id,agent_id FROM agent_queue_admission) WHERE workspace_id=? AND (? IS NULL OR agent_id=?)")
            .bind(ws.as_str()).bind(agent.map(AgentId::as_str)).bind(agent.map(AgentId::as_str)).fetch_all(self.read_pool()).await.map_err(db)?;
        Ok(ids.into_iter().map(AgentId::from).collect())
    }

    /// Suppress work even when its active watch has already been deleted.
    /// # Errors
    /// Returns database errors.
    pub async fn suppress_script_notifications(
        &self,
        ws: &WorkspaceId,
        agent: Option<&AgentId>,
    ) -> Result<()> {
        let mut tx = self.write_pool().begin().await.map_err(db)?;
        for query in [
            "DELETE FROM script_notification_job WHERE workspace_id=? AND (? IS NULL OR agent_id=?)",
            "DELETE FROM agent_queue_admission WHERE workspace_id=? AND (? IS NULL OR agent_id=?)",
            "DELETE FROM agent_queue WHERE json_valid(payload) AND json_extract(payload,'$.messageMetadata.type')='script_monitor_wake' AND json_extract(payload,'$.messageMetadata.workspaceId')=? AND (? IS NULL OR agent_id=?)"
        ] { sqlx::query(query).bind(ws.as_str()).bind(agent.map(AgentId::as_str)).bind(agent.map(AgentId::as_str)).execute(&mut *tx).await.map_err(db)?; }
        tx.commit().await.map_err(db)
    }

    /// Latest accepted token remains available after settlement and marker dismissal.
    /// # Errors
    /// Returns storage errors.
    pub async fn latest_script_run(
        &self,
        ws: &WorkspaceId,
        id: &str,
    ) -> Result<Option<(String, Option<intent_core::ScriptLastRun>)>> {
        let row: Option<(String, Option<String>, Option<String>)> = sqlx::query_as("SELECT latest_run_id,pending_run_id,latest_run_result FROM script WHERE workspace_id=? AND id=? AND latest_run_id IS NOT NULL")
            .bind(ws.as_str()).bind(id).fetch_optional(self.read_pool()).await.map_err(db)?;
        row.map(|(token, pending, result)| {
            Ok((
                token.clone(),
                if pending.is_none() {
                    result
                        .map(|s| serde_json::from_str::<intent_core::ScriptLastRun>(&s).map_err(db))
                        .transpose()?
                        .filter(|r| r.run_id.as_deref() == Some(token.as_str()))
                } else {
                    None
                },
            ))
        })
        .transpose()
    }
}

/// Private delivery work, containing no monitor configuration or process buffer.
#[derive(Debug, Clone)]
pub struct ScriptNotificationJob {
    pub wake_id: String,
    pub workspace_id: WorkspaceId,
    pub agent_id: AgentId,
    pub metadata: serde_json::Value,
    pub body: String,
}
