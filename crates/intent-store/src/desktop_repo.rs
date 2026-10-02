//! Private desktop state in the existing backend state-blob store. These keys
//! are not configuration/catalog entries and cannot be read/written by settings RPCs.
//! Transactions couple terminal outcomes with durable outbox identities. No schema
//! change or execution authority survives restart; only notification credentials do.
use crate::Store;
use intent_core::{AgentId, Error, PrincipalId, Result, WorkspaceId};
use serde_json::{json, Value};
use sqlx::{Row, Sqlite, Transaction};

#[cfg(test)]
#[derive(Default)]
pub(crate) struct DeleteBarrier {
    pub entered: tokio::sync::Notify,
    pub release: tokio::sync::Notify,
}

const PREFIX: &str = "desktop.v1/";
// Result::map_err supplies its owned SQL error to this shared adapter.
#[expect(clippy::needless_pass_by_value)]
fn db(error: sqlx::Error) -> Error {
    Error::Internal(format!("desktop journal: {error}"))
}
fn decode(text: &str) -> Result<Value> {
    serde_json::from_str(text).map_err(|e| Error::Internal(format!("desktop journal JSON: {e}")))
}
fn key(kind: &str, id: &str) -> String {
    format!("{PREFIX}{kind}/{id}")
}
fn consent_key(
    principal: &PrincipalId,
    workspace: &WorkspaceId,
    agent: &AgentId,
    computer: &str,
) -> String {
    key(
        "permission",
        &json!([principal, workspace, agent, computer]).to_string(),
    )
}
async fn insert(tx: &mut Transaction<'_, Sqlite>, key: &str, record: &Value) -> Result<()> {
    let changed=sqlx::query("INSERT INTO settings(key,value) SELECT ?,? WHERE EXISTS(SELECT 1 FROM agent_session a JOIN workspace w ON w.id=a.workspace_id WHERE a.id=? AND w.id=?) AND (? IS NULL OR EXISTS(SELECT 1 FROM principal WHERE id=?))")
        .bind(key).bind(record.to_string()).bind(record["agentId"].as_str()).bind(record["workspaceId"].as_str()).bind(record["principalId"].as_str()).bind(record["principalId"].as_str()).execute(&mut **tx).await.map_err(db)?.rows_affected();
    if changed == 0 {
        return Err(Error::NotFound("desktop target".into()));
    }
    Ok(())
}
async fn outbox(
    tx: &mut Transaction<'_, Sqlite>,
    id: &str,
    workspace: &str,
    agent: &str,
    payload: &Value,
) -> Result<()> {
    insert(
        tx,
        &key("outbox", id),
        &json!({"workspaceId":workspace,"agentId":agent,"payload":payload,"delivered":false}),
    )
    .await
}

/// Scope cleanup shares the agent/workspace deletion transaction. Other callers
/// cannot name this private namespace through the settings catalog.
pub(crate) async fn delete_desktop_scope(
    tx: &mut Transaction<'_, Sqlite>,
    workspace: &WorkspaceId,
    agent: Option<&AgentId>,
) -> Result<()> {
    sqlx::query("DELETE FROM settings WHERE key GLOB 'desktop.v1/*' AND json_extract(value,'$.workspaceId')=? AND (? IS NULL OR json_extract(value,'$.agentId')=?)")
        .bind(workspace.as_str()).bind(agent.map(AgentId::as_str)).bind(agent.map(AgentId::as_str)).execute(&mut **tx).await.map_err(db)?;
    Ok(())
}
impl Store {
    /// Atomically claim an unassigned primary for a live consent request.
    /// The owner and browser assignment predicates are checked in the write.
    /// # Errors
    /// Returns a storage error; false means the request or assignment changed.
    pub async fn desktop_claim_primary(
        &self,
        request: &str,
        binding: &Value,
        generation: &str,
        remember: bool,
    ) -> Result<bool> {
        let field = |name: &str| {
            binding[name]
                .as_str()
                .ok_or_else(|| Error::InvalidParams(format!("missing desktop claim {name}")))
        };
        let workspace = WorkspaceId::from(field("workspaceId")?);
        let agent = AgentId::from(field("agentId")?);
        let principal = PrincipalId::from(field("principalId")?);
        let client = field("clientId")?;
        let computer = field("computerId")?;
        let mut tx = self.write_pool().begin().await.map_err(db)?;
        let changed = sqlx::query("UPDATE workspace SET browser_client_id=? WHERE id=? AND owner_principal_id=? AND browser_client_id IS NULL AND NOT EXISTS(SELECT 1 FROM browser_tab WHERE workspace_id=? AND closed_at IS NULL AND owner_agent_id IS NOT NULL) AND EXISTS(SELECT 1 FROM settings WHERE key=? AND json_extract(value,'$.outcome') IS NULL AND json_extract(value,'$.claimed') IS NULL AND json_extract(value,'$.assignmentGeneration')=? AND julianday(json_extract(value,'$.expiresAt')) > julianday('now') AND json_extract(value,'$.agentId')=? AND json_extract(value,'$.workspaceId')=? AND json_extract(value,'$.principalId')=?)")
            .bind(client).bind(workspace.as_str()).bind(principal.as_str()).bind(workspace.as_str()).bind(key("request", request)).bind(generation).bind(agent.as_str()).bind(workspace.as_str()).bind(principal.as_str()).execute(&mut *tx).await.map_err(db)?.rows_affected();
        if changed == 0 {
            return Ok(false);
        }
        sqlx::query("UPDATE settings SET value=json_set(value,'$.claimed',json(?)) WHERE key=?")
            .bind(binding.to_string())
            .bind(key("request", request))
            .execute(&mut *tx)
            .await
            .map_err(db)?;
        if remember {
            let consent = consent_key(&principal, &workspace, &agent, computer);
            sqlx::query("DELETE FROM settings WHERE key=?")
                .bind(&consent)
                .execute(&mut *tx)
                .await
                .map_err(db)?;
            insert(&mut tx,&consent,&json!({"workspaceId":workspace,"agentId":agent,"principalId":principal,"computerId":computer})).await?;
        }
        tx.commit().await.map_err(db)?;
        Ok(true)
    }
    /// Read remembered consent for exactly one principal/workspace/agent/computer.
    /// # Errors
    /// Returns a storage error if the journal read fails.
    pub async fn desktop_permission(
        &self,
        principal: &PrincipalId,
        workspace: &WorkspaceId,
        agent: &AgentId,
        computer: &str,
    ) -> Result<bool> {
        Ok(self
            .get_setting(&consent_key(principal, workspace, agent, computer))
            .await?
            .is_some())
    }
    /// Persist or remove consent without changing an active session.
    /// # Errors
    /// Returns a storage error or not-found if the target was deleted.
    pub async fn desktop_set_permission(
        &self,
        principal: &PrincipalId,
        workspace: &WorkspaceId,
        agent: &AgentId,
        computer: &str,
        allowed: bool,
    ) -> Result<()> {
        let k = consent_key(principal, workspace, agent, computer);
        let mut tx = self.write_pool().begin().await.map_err(db)?;
        sqlx::query("DELETE FROM settings WHERE key=?")
            .bind(&k)
            .execute(&mut *tx)
            .await
            .map_err(db)?;
        if allowed {
            insert(&mut tx,&k,&json!({"principalId":principal,"workspaceId":workspace,"agentId":agent,"computerId":computer})).await?;
        }
        tx.commit().await.map_err(db)
    }
    /// Journal a request before publishing its consent prompt.
    /// # Errors
    /// Returns a storage error or not-found if the target was deleted.
    pub async fn desktop_insert_request(
        &self,
        id: &str,
        workspace: &WorkspaceId,
        agent: &AgentId,
        binding: &Value,
    ) -> Result<()> {
        let mut record = binding.clone();
        record["workspaceId"] = workspace.as_str().into();
        record["agentId"] = agent.as_str().into();
        record["outcome"] = Value::Null;
        let mut tx = self.write_pool().begin().await.map_err(db)?;
        insert(&mut tx, &key("request", id), &record).await?;
        tx.commit().await.map_err(db)
    }
    /// Retain only the hash of the executor's notification credential.
    /// # Errors
    /// Returns a storage error or not-found if the target was deleted.
    pub async fn desktop_insert_terminal(
        &self,
        id: &str,
        workspace: &WorkspaceId,
        agent: &AgentId,
        binding: &Value,
        token_hash: &str,
    ) -> Result<()> {
        let mut record = binding.clone();
        record["workspaceId"] = workspace.as_str().into();
        record["agentId"] = agent.as_str().into();
        record["tokenHash"] = token_hash.into();
        record["reason"] = Value::Null;
        record["reportId"] = Value::Null;
        let mut tx = self.write_pool().begin().await.map_err(db)?;
        insert(&mut tx, &key("terminal", id), &record).await?;
        tx.commit().await.map_err(db)
    }
    /// Read a retained credential only while its target still exists.
    /// # Errors
    /// Returns a storage error if the journal read or JSON decoding fails.
    pub async fn desktop_terminal(&self, id: &str) -> Result<Option<Value>> {
        let raw:Option<String>=sqlx::query_scalar("SELECT value FROM settings WHERE key=? AND EXISTS(SELECT 1 FROM agent_session a JOIN workspace w ON w.id=a.workspace_id WHERE a.id=json_extract(value,'$.agentId') AND w.id=json_extract(value,'$.workspaceId')) AND EXISTS(SELECT 1 FROM principal WHERE id=json_extract(value,'$.principalId'))").bind(key("terminal",id)).fetch_optional(self.read_pool()).await.map_err(db)?;
        raw.map(|raw| decode(&raw)).transpose()
    }
    /// Commit the terminal request outcome and deduplication identity together.
    /// # Errors
    /// Returns a storage error if either write fails; neither is committed alone.
    pub async fn desktop_resolve_request(
        &self,
        request: &str,
        workspace: &WorkspaceId,
        agent: &AgentId,
        outcome: &str,
        payload: &Value,
    ) -> Result<bool> {
        let mut tx = self.write_pool().begin().await.map_err(db)?;
        let changed=sqlx::query("UPDATE settings SET value=json_set(value,'$.outcome',?) WHERE key=? AND json_extract(value,'$.outcome') IS NULL AND json_extract(value,'$.workspaceId')=? AND json_extract(value,'$.agentId')=?")
            .bind(outcome).bind(key("request",request)).bind(workspace.as_str()).bind(agent.as_str()).execute(&mut *tx).await.map_err(db)?.rows_affected()==1;
        if changed {
            outbox(
                &mut tx,
                &format!("request:{request}"),
                workspace.as_str(),
                agent.as_str(),
                payload,
            )
            .await?;
        }
        tx.commit().await.map_err(db)?;
        Ok(changed)
    }
    /// User Stop supersedes an undelivered outcome for this session only.
    /// # Errors
    /// Returns a storage error or not-found; all state/outbox writes roll back together.
    pub async fn desktop_record_end(
        &self,
        session: &str,
        reason: &str,
        report: Option<&str>,
        payload: Option<&Value>,
    ) -> Result<bool> {
        let mut tx = self.write_pool().begin().await.map_err(db)?;
        let terminal = key("terminal", session);
        let raw: String = sqlx::query_scalar("SELECT value FROM settings WHERE key=?")
            .bind(&terminal)
            .fetch_optional(&mut *tx)
            .await
            .map_err(db)?
            .ok_or_else(|| Error::NotFound("desktop session".into()))?;
        let record = decode(&raw)?;
        if !record["reportId"].is_null() || (report.is_none() && !record["reason"].is_null()) {
            return Ok(false);
        }
        if let Some(report) = report {
            let duplicate:bool=sqlx::query_scalar("SELECT EXISTS(SELECT 1 FROM settings WHERE key GLOB 'desktop.v1/terminal/*' AND json_extract(value,'$.reportId')=?)").bind(report).fetch_one(&mut *tx).await.map_err(db)?;
            if duplicate {
                return Err(Error::InvalidParams(
                    "Stop report ID already belongs to a different session".into(),
                ));
            }
        }
        sqlx::query(
            "UPDATE settings SET value=json_set(value,'$.reason',?,'$.reportId',?) WHERE key=?",
        )
        .bind(reason)
        .bind(report)
        .bind(&terminal)
        .execute(&mut *tx)
        .await
        .map_err(db)?;
        if let Some(payload) = payload {
            if let Some(request) = record["requestId"].as_str() {
                sqlx::query("UPDATE settings SET value=json_set(value,'$.outcome','invalidated') WHERE key=? AND json_extract(value,'$.outcome') IS NULL").bind(key("request",request)).execute(&mut *tx).await.map_err(db)?;
                sqlx::query(
                    "DELETE FROM settings WHERE key=? AND json_extract(value,'$.delivered')=0",
                )
                .bind(key("outbox", &format!("request:{request}")))
                .execute(&mut *tx)
                .await
                .map_err(db)?;
            }
            sqlx::query("DELETE FROM settings WHERE key GLOB 'desktop.v1/outbox/*' AND json_extract(value,'$.delivered')=0 AND json_extract(value,'$.payload.sessionId')=?").bind(session).execute(&mut *tx).await.map_err(db)?;
            outbox(
                &mut tx,
                &format!("session:{session}:{reason}"),
                record["workspaceId"].as_str().unwrap_or_default(),
                record["agentId"].as_str().unwrap_or_default(),
                payload,
            )
            .await?;
        }
        tx.commit().await.map_err(db)?;
        Ok(true)
    }
    /// Read a bounded batch of undelivered outcomes.
    /// # Errors
    /// Returns a storage error if the journal read or JSON decoding fails.
    pub async fn desktop_outbox(&self) -> Result<Vec<(String, WorkspaceId, AgentId, Value)>> {
        sqlx::query("SELECT key,value FROM settings WHERE key GLOB 'desktop.v1/outbox/*' AND json_extract(value,'$.delivered')=0 ORDER BY rowid LIMIT 64").fetch_all(self.read_pool()).await.map_err(db)?.into_iter().map(|row| {
            let record=decode(row.get("value"))?;
            let workspace=record["workspaceId"].as_str().ok_or_else(||Error::Internal("desktop outbox missing workspace".into()))?;
            let agent=record["agentId"].as_str().ok_or_else(||Error::Internal("desktop outbox missing agent".into()))?;
            Ok((row.get("key"),WorkspaceId::from(workspace),AgentId::from(agent),record["payload"].clone()))
        }).collect()
    }
    /// Record a successful durable handoff.
    /// # Errors
    /// Returns a storage error if the journal write fails.
    pub async fn desktop_outbox_delivered(&self, id: &str) -> Result<()> {
        sqlx::query("UPDATE settings SET value=json_set(value,'$.delivered',json('true')) WHERE key=? AND key GLOB 'desktop.v1/outbox/*'").bind(id).execute(self.write_pool()).await.map_err(db)?;
        Ok(())
    }
    /// Probe durable transcript/queue handoff before retrying an outcome.
    /// # Errors
    /// Returns a storage error if the probe fails.
    pub async fn desktop_wake_exists(&self, agent: &AgentId, id: &str) -> Result<bool> {
        sqlx::query_scalar("SELECT EXISTS(SELECT 1 FROM agent_message WHERE agent_id=? AND json_extract(metadata,'$.desktopWakeId')=? UNION ALL SELECT 1 FROM agent_queue WHERE agent_id=? AND json_extract(payload,'$.messageMetadata.desktopWakeId')=?)").bind(agent.as_str()).bind(id).bind(agent.as_str()).bind(id).fetch_one(self.read_pool()).await.map_err(db)
    }
    /// Restart invalidates unfinished execution and retains one correlated wake.
    /// # Errors
    /// Returns a storage error if reconciliation fails; the transaction rolls back.
    ///
    /// # Panics
    /// The internal namespace assertion only fails if the fixed SQL prefix changes.
    pub async fn desktop_invalidate_restart(&self) -> Result<()> {
        let mut tx = self.write_pool().begin().await.map_err(db)?;
        sqlx::query("DELETE FROM settings WHERE key GLOB 'desktop.v1/*' AND (NOT EXISTS(SELECT 1 FROM agent_session a JOIN workspace w ON w.id=a.workspace_id WHERE a.id=json_extract(value,'$.agentId') AND w.id=json_extract(value,'$.workspaceId')) OR (json_extract(value,'$.principalId') IS NOT NULL AND NOT EXISTS(SELECT 1 FROM principal WHERE id=json_extract(value,'$.principalId'))))").execute(&mut *tx).await.map_err(db)?;
        let requests=sqlx::query("SELECT key,value FROM settings WHERE key GLOB 'desktop.v1/request/*' AND json_extract(value,'$.outcome') IS NULL").fetch_all(&mut *tx).await.map_err(db)?;
        for row in requests {
            let k: String = row.get("key");
            let request = k
                .strip_prefix("desktop.v1/request/")
                .expect("query namespace");
            let record = decode(row.get("value"))?;
            let payload = json!({"type":"desktop_control","requestId":request,"outcome":"invalidated","state":{"status":"inactive"},"message":"Daemon restarted; desktop control is not active. Do not automatically restart."});
            outbox(
                &mut tx,
                &format!("request:{request}"),
                record["workspaceId"].as_str().unwrap_or_default(),
                record["agentId"].as_str().unwrap_or_default(),
                &payload,
            )
            .await?;
            sqlx::query(
                "UPDATE settings SET value=json_set(value,'$.outcome','invalidated') WHERE key=?",
            )
            .bind(k)
            .execute(&mut *tx)
            .await
            .map_err(db)?;
        }
        let sessions=sqlx::query("SELECT key,value FROM settings WHERE key GLOB 'desktop.v1/terminal/*' AND json_extract(value,'$.reason') IS NULL").fetch_all(&mut *tx).await.map_err(db)?;
        for row in sessions {
            let k: String = row.get("key");
            let session = k
                .strip_prefix("desktop.v1/terminal/")
                .expect("query namespace");
            let record = decode(row.get("value"))?;
            sqlx::query("DELETE FROM settings WHERE key GLOB 'desktop.v1/outbox/*' AND json_extract(value,'$.delivered')=0 AND (json_extract(value,'$.payload.sessionId')=? OR key=?)").bind(session).bind(key("outbox",&format!("request:{}",record["requestId"].as_str().unwrap_or_default()))).execute(&mut *tx).await.map_err(db)?;
            let mut payload = json!({"type":"desktop_control","sessionId":session,"outcome":"revoked","state":{"status":"inactive"},"message":"Daemon restarted; desktop control is not active. Do not automatically restart."});
            if let Some(request) = record["requestId"].as_str() {
                payload["requestId"] = request.into();
            }
            outbox(
                &mut tx,
                &format!("session:{session}:disconnected"),
                record["workspaceId"].as_str().unwrap_or_default(),
                record["agentId"].as_str().unwrap_or_default(),
                &payload,
            )
            .await?;
            sqlx::query(
                "UPDATE settings SET value=json_set(value,'$.reason','disconnected') WHERE key=?",
            )
            .bind(k)
            .execute(&mut *tx)
            .await
            .map_err(db)?;
        }
        tx.commit().await.map_err(db)
    }
}
