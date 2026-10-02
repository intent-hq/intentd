//! Durable monitor rows are also their transactional wake outbox.
use crate::Store;
use intent_core::{AgentId, Error, Result, ScriptMonitor, WorkspaceId};

fn db(error: impl std::fmt::Display) -> Error {
    Error::Internal(format!("script monitor storage: {error}"))
}

impl Store {
    /// Read bounded retained monitor rows, in canonical order.
    /// # Errors
    /// Fails closed on database/decoding errors.
    pub async fn script_monitors(
        &self,
        ws: &WorkspaceId,
        agent: Option<&AgentId>,
    ) -> Result<Vec<ScriptMonitor>> {
        let rows: Vec<String> = sqlx::query_scalar("SELECT row_json FROM script_monitor WHERE workspace_id = ? AND (? IS NULL OR agent_id = ?) ORDER BY created_at,id")
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
            "SELECT EXISTS(SELECT 1 FROM script_monitor WHERE agent_id=? AND wake_state='pending')",
        )
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
        let json = serde_json::to_string(row).map_err(db)?;
        sqlx::query("INSERT INTO script_monitor(id,workspace_id,agent_id,script_id,run_id,state,row_json,created_at) VALUES(?,?,?,?,?,'active',?,?)")
            .bind(&row.monitor_id).bind(row.workspace_id.as_str()).bind(row.agent_id.as_str())
            .bind(&row.script_id).bind(&row.run_id).bind(json).bind(&row.created_at)
            .execute(self.write_pool()).await.map_err(db)?;
        Ok(())
    }

    /// One CAS commits the terminal row and its stable wake identity together.
    /// # Errors
    /// Returns a persistence error without changing the row on failure.
    pub async fn settle_script_monitor(&self, row: &ScriptMonitor) -> Result<bool> {
        let wake = if matches!(row.state.as_str(), "completed" | "expired" | "triggered") {
            "pending"
        } else {
            "suppressed"
        };
        let result = sqlx::query("UPDATE script_monitor SET state=?,row_json=?,settled_at=?,wake_state=?,cancel_intent=0 WHERE id=? AND workspace_id=? AND state='active'")
            .bind(&row.state).bind(serde_json::to_string(row).map_err(db)?).bind(&row.settled_at).bind(wake)
            .bind(&row.monitor_id).bind(row.workspace_id.as_str()).execute(self.write_pool()).await.map_err(db)?;
        Ok(result.rows_affected() == 1)
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
        let rows: Vec<(String,bool)> = sqlx::query_as("SELECT row_json,cancel_intent FROM script_monitor WHERE state='active' OR wake_state='pending'")
            .fetch_all(self.read_pool()).await.map_err(db)?;
        rows.into_iter()
            .map(|(json, intent)| Ok((serde_json::from_str(&json).map_err(db)?, intent)))
            .collect()
    }

    /// Read the stable outbox fence immediately before admission or delivery.
    /// # Errors
    /// Returns storage errors.
    pub async fn script_monitor_wake_pending(&self, id: &str) -> Result<bool> {
        let pending: Option<bool> =
            sqlx::query_scalar("SELECT wake_state='pending' FROM script_monitor WHERE id=?")
                .bind(id)
                .fetch_optional(self.read_pool())
                .await
                .map_err(db)?;
        Ok(pending.unwrap_or(false))
    }

    /// A delivered wake may finish its admitted turn; suppression may never revive.
    /// # Errors
    /// Returns storage errors.
    pub async fn script_monitor_wake_allowed(&self, id: &str) -> Result<bool> {
        Ok(sqlx::query_scalar::<_, bool>(
            "SELECT wake_state IN ('pending','delivered') FROM script_monitor WHERE id=?",
        )
        .bind(id)
        .fetch_optional(self.read_pool())
        .await
        .map_err(db)?
        .unwrap_or(false))
    }

    /// Mark delivered only after the durable message exists; suppression is permanent.
    /// # Errors
    /// Returns storage errors.
    pub async fn finish_script_monitor_wake(&self, id: &str, suppress: bool) -> Result<()> {
        sqlx::query(
            "UPDATE script_monitor SET wake_state=? WHERE id=? AND (wake_state='pending' OR ?=1)",
        )
        .bind(if suppress { "suppressed" } else { "delivered" })
        .bind(id)
        .bind(suppress)
        .execute(self.write_pool())
        .await
        .map_err(db)?;
        Ok(())
    }

    /// Workspace keys for bounded-retention maintenance (never a UI read).
    /// # Errors
    /// Returns storage errors.
    pub async fn script_monitor_workspaces(&self) -> Result<Vec<WorkspaceId>> {
        let ids: Vec<String> =
            sqlx::query_scalar("SELECT DISTINCT workspace_id FROM script_monitor")
                .fetch_all(self.read_pool())
                .await
                .map_err(db)?;
        Ok(ids.into_iter().map(WorkspaceId::from).collect())
    }

    /// Prune only terminal rows with no promised undelivered notification.
    /// # Errors
    /// Returns storage errors.
    pub async fn prune_script_monitors(
        &self,
        ws: &WorkspaceId,
        cutoff: &str,
        for_admission: bool,
    ) -> Result<()> {
        sqlx::query("DELETE FROM script_monitor WHERE workspace_id=? AND state!='active' AND wake_state IN ('delivered','suppressed') AND (settled_at < ? OR id IN (SELECT id FROM script_monitor WHERE workspace_id=? AND state!='active' AND wake_state IN ('delivered','suppressed') ORDER BY settled_at,id LIMIT max(0,(SELECT count(*) FROM script_monitor WHERE workspace_id=?)-?)))")
            .bind(ws.as_str()).bind(cutoff).bind(ws.as_str()).bind(ws.as_str()).bind(if for_admission {999_i64} else {1000})
            .execute(self.write_pool()).await.map_err(db)?;
        Ok(())
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
