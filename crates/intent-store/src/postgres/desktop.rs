//! Private desktop state in the settings table (Postgres). Mirrors
//! `desktop_repo.rs`: the same `desktop.v1/` keys and JSON records, read and
//! changed with the `sqlite_json_*` compatibility functions.

use intent_core::{AgentId, Error, PrincipalId, Result, WorkspaceId};
use serde_json::{json, Value};
use sqlx::Row;

use super::{PgStore, WriteTx};
use crate::desktop_repo::{consent_key, decode, key};

fn db(error: impl std::fmt::Display) -> Error {
    Error::Internal(format!("desktop journal: {error}"))
}

/// `settings.value` names a live agent of its workspace.
const TARGET_LIVE: &str = "EXISTS(SELECT 1 FROM agent_session a JOIN workspace w \
     ON w.tenant_id = a.tenant_id AND w.id = a.workspace_id \
     WHERE a.tenant_id = $1 AND w.tenant_id = $1 \
     AND a.id = sqlite_json_extract(s.value, '$.agentId') \
     AND w.id = sqlite_json_extract(s.value, '$.workspaceId'))";

impl PgStore {
    async fn desktop_insert(&self, tx: &mut WriteTx, key: &str, record: &Value) -> Result<()> {
        let principal = record["principalId"].as_str();
        let changed = sqlx::query(
            "INSERT INTO settings (tenant_id, key, value) SELECT $1, $2, $3 \
             WHERE EXISTS(SELECT 1 FROM agent_session a JOIN workspace w \
               ON w.tenant_id = a.tenant_id AND w.id = a.workspace_id \
               WHERE a.tenant_id = $1 AND w.tenant_id = $1 AND a.id = $4 AND w.id = $5) \
             AND ($6::text IS NULL \
               OR EXISTS(SELECT 1 FROM principal WHERE tenant_id = $1 AND id = $6))",
        )
        .bind(self.tenant())
        .bind(key)
        .bind(record.to_string())
        .bind(record["agentId"].as_str())
        .bind(record["workspaceId"].as_str())
        .bind(principal)
        .execute(&mut **tx)
        .await
        .map_err(db)?
        .rows_affected();
        if changed == 0 {
            return Err(Error::NotFound("desktop target".into()));
        }
        Ok(())
    }

    async fn desktop_outbox_insert(
        &self,
        tx: &mut WriteTx,
        id: &str,
        workspace: &str,
        agent: &str,
        payload: &Value,
    ) -> Result<()> {
        self.desktop_insert(
            tx,
            &key("outbox", id),
            &json!({"workspaceId":workspace,"agentId":agent,"payload":payload,"delivered":false}),
        )
        .await
    }

    async fn desktop_delete_key(&self, tx: &mut WriteTx, key: &str) -> Result<()> {
        sqlx::query("DELETE FROM settings WHERE tenant_id = $1 AND key = $2")
            .bind(self.tenant())
            .bind(key)
            .execute(&mut **tx)
            .await
            .map_err(db)?;
        Ok(())
    }

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
        let _write = self.desktop_writes.enter(&workspace)?;
        let agent = AgentId::from(field("agentId")?);
        let principal = PrincipalId::from(field("principalId")?);
        let client = field("clientId")?;
        let computer = field("computerId")?;
        let request_key = key("request", request);
        let mut tx = self.begin_write().await?;
        let changed = sqlx::query(
            "UPDATE workspace SET browser_client_id = $2 \
             WHERE tenant_id = $1 AND id = $3 AND owner_principal_id = $4 \
             AND browser_client_id IS NULL \
             AND NOT EXISTS(SELECT 1 FROM browser_tab WHERE tenant_id = $1 AND workspace_id = $3 \
               AND closed_at IS NULL AND owner_agent_id IS NOT NULL) \
             AND EXISTS(SELECT 1 FROM settings WHERE tenant_id = $1 AND key = $5 \
               AND sqlite_json_extract(value, '$.outcome') IS NULL \
               AND sqlite_json_extract(value, '$.claimed') IS NULL \
               AND sqlite_json_extract(value, '$.assignmentGeneration') = $6 \
               AND sqlite_julianday(sqlite_json_extract(value, '$.expiresAt')) \
                 > sqlite_julianday('now') \
               AND sqlite_json_extract(value, '$.agentId') = $7 \
               AND sqlite_json_extract(value, '$.workspaceId') = $3 \
               AND sqlite_json_extract(value, '$.principalId') = $4)",
        )
        .bind(self.tenant())
        .bind(client)
        .bind(workspace.as_str())
        .bind(principal.as_str())
        .bind(&request_key)
        .bind(generation)
        .bind(agent.as_str())
        .execute(&mut *tx)
        .await
        .map_err(db)?
        .rows_affected();
        if changed == 0 {
            return Ok(false);
        }
        sqlx::query(
            "UPDATE settings SET value = sqlite_json_set(value, '$.claimed', sqlite_json($3::text)) \
             WHERE tenant_id = $1 AND key = $2",
        )
        .bind(self.tenant())
        .bind(&request_key)
        .bind(binding.to_string())
        .execute(&mut *tx)
        .await
        .map_err(db)?;
        if remember {
            let consent = consent_key(&principal, &workspace, &agent, computer);
            self.desktop_delete_key(&mut tx, &consent).await?;
            self.desktop_insert(
                &mut tx,
                &consent,
                &json!({"workspaceId":workspace,"agentId":agent,"principalId":principal,"computerId":computer}),
            )
            .await?;
        }
        tx.commit().await.map_err(db)?;
        Ok(true)
    }

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

    pub async fn desktop_set_permission(
        &self,
        principal: &PrincipalId,
        workspace: &WorkspaceId,
        agent: &AgentId,
        computer: &str,
        allowed: bool,
    ) -> Result<()> {
        let _write = self.desktop_writes.enter(workspace)?;
        let k = consent_key(principal, workspace, agent, computer);
        let mut tx = self.begin_write().await?;
        self.desktop_delete_key(&mut tx, &k).await?;
        if allowed {
            self.desktop_insert(
                &mut tx,
                &k,
                &json!({"principalId":principal,"workspaceId":workspace,"agentId":agent,"computerId":computer}),
            )
            .await?;
        }
        tx.commit().await.map_err(db)
    }

    pub async fn desktop_insert_request(
        &self,
        id: &str,
        workspace: &WorkspaceId,
        agent: &AgentId,
        binding: &Value,
    ) -> Result<()> {
        let _write = self.desktop_writes.enter(workspace)?;
        let mut record = binding.clone();
        record["workspaceId"] = workspace.as_str().into();
        record["agentId"] = agent.as_str().into();
        record["outcome"] = Value::Null;
        let mut tx = self.begin_write().await?;
        self.desktop_insert(&mut tx, &key("request", id), &record)
            .await?;
        tx.commit().await.map_err(db)
    }

    pub async fn desktop_insert_terminal(
        &self,
        id: &str,
        workspace: &WorkspaceId,
        agent: &AgentId,
        binding: &Value,
        token_hash: &str,
    ) -> Result<()> {
        let _write = self.desktop_writes.enter(workspace)?;
        let mut record = binding.clone();
        record["workspaceId"] = workspace.as_str().into();
        record["agentId"] = agent.as_str().into();
        record["tokenHash"] = token_hash.into();
        record["reason"] = Value::Null;
        record["reportId"] = Value::Null;
        let mut tx = self.begin_write().await?;
        self.desktop_insert(&mut tx, &key("terminal", id), &record)
            .await?;
        tx.commit().await.map_err(db)
    }

    pub async fn desktop_terminal(&self, id: &str) -> Result<Option<Value>> {
        let mut tx = self.begin().await?;
        let raw: Option<String> = sqlx::query_scalar(sqlx::AssertSqlSafe(format!(
            "SELECT s.value FROM settings s WHERE s.tenant_id = $1 AND s.key = $2 \
             AND {TARGET_LIVE} \
             AND EXISTS(SELECT 1 FROM principal p WHERE p.tenant_id = $1 \
               AND p.id = sqlite_json_extract(s.value, '$.principalId'))"
        )))
        .bind(self.tenant())
        .bind(key("terminal", id))
        .fetch_optional(&mut *tx)
        .await
        .map_err(db)?;
        tx.commit().await.map_err(db)?;
        raw.map(|raw| decode(&raw)).transpose()
    }

    pub async fn desktop_resolve_request(
        &self,
        request: &str,
        workspace: &WorkspaceId,
        agent: &AgentId,
        outcome: &str,
        payload: &Value,
    ) -> Result<bool> {
        let _write = self.desktop_writes.enter(workspace)?;
        let mut tx = self.begin_write().await?;
        let changed = sqlx::query(
            "UPDATE settings SET value = sqlite_json_set(value, '$.outcome', $2::text) \
             WHERE tenant_id = $1 AND key = $3 \
             AND sqlite_json_extract(value, '$.outcome') IS NULL \
             AND sqlite_json_extract(value, '$.workspaceId') = $4 \
             AND sqlite_json_extract(value, '$.agentId') = $5",
        )
        .bind(self.tenant())
        .bind(outcome)
        .bind(key("request", request))
        .bind(workspace.as_str())
        .bind(agent.as_str())
        .execute(&mut *tx)
        .await
        .map_err(db)?
        .rows_affected()
            == 1;
        if changed {
            self.desktop_outbox_insert(
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

    pub async fn desktop_record_end(
        &self,
        session: &str,
        reason: &str,
        report: Option<&str>,
        payload: Option<&Value>,
    ) -> Result<bool> {
        // As on SQLite: resolve the scope, then re-read inside the guarded
        // transaction, since teardown may remove the credential in between.
        let scope = self
            .desktop_terminal(session)
            .await?
            .ok_or_else(|| Error::NotFound("desktop session".into()))?;
        let workspace = WorkspaceId::from(
            scope["workspaceId"]
                .as_str()
                .ok_or_else(|| Error::Internal("desktop session missing workspace".into()))?,
        );
        let _write = self.desktop_writes.enter(&workspace)?;
        let mut tx = self.begin_write().await?;
        let terminal = key("terminal", session);
        let raw: String =
            sqlx::query_scalar("SELECT value FROM settings WHERE tenant_id = $1 AND key = $2")
                .bind(self.tenant())
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
            let duplicate: bool = sqlx::query_scalar(
                "SELECT EXISTS(SELECT 1 FROM settings WHERE tenant_id = $1 \
                 AND key LIKE 'desktop.v1/terminal/%' \
                 AND sqlite_json_extract(value, '$.reportId') = $2)",
            )
            .bind(self.tenant())
            .bind(report)
            .fetch_one(&mut *tx)
            .await
            .map_err(db)?;
            if duplicate {
                return Err(Error::InvalidParams(
                    "Stop report ID already belongs to a different session".into(),
                ));
            }
        }
        sqlx::query(
            "UPDATE settings SET value = sqlite_json_set(value, '$.reason', $2::text, \
               '$.reportId', $3::text) \
             WHERE tenant_id = $1 AND key = $4",
        )
        .bind(self.tenant())
        .bind(reason)
        .bind(report)
        .bind(&terminal)
        .execute(&mut *tx)
        .await
        .map_err(db)?;
        if let Some(payload) = payload {
            if let Some(request) = record["requestId"].as_str() {
                sqlx::query(
                    "UPDATE settings \
                     SET value = sqlite_json_set(value, '$.outcome', 'invalidated'::text) \
                     WHERE tenant_id = $1 AND key = $2 \
                     AND sqlite_json_extract(value, '$.outcome') IS NULL",
                )
                .bind(self.tenant())
                .bind(key("request", request))
                .execute(&mut *tx)
                .await
                .map_err(db)?;
                sqlx::query(
                    "DELETE FROM settings WHERE tenant_id = $1 AND key = $2 \
                     AND sqlite_json_extract(value, '$.delivered') = '0'",
                )
                .bind(self.tenant())
                .bind(key("outbox", &format!("request:{request}")))
                .execute(&mut *tx)
                .await
                .map_err(db)?;
            }
            sqlx::query(
                "DELETE FROM settings WHERE tenant_id = $1 AND key LIKE 'desktop.v1/outbox/%' \
                 AND sqlite_json_extract(value, '$.delivered') = '0' \
                 AND sqlite_json_extract(value, '$.payload.sessionId') = $2",
            )
            .bind(self.tenant())
            .bind(session)
            .execute(&mut *tx)
            .await
            .map_err(db)?;
            self.desktop_outbox_insert(
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

    pub async fn desktop_outbox(&self) -> Result<Vec<(String, WorkspaceId, AgentId, Value)>> {
        let mut tx = self.begin().await?;
        let rows = sqlx::query(
            "SELECT key, value FROM settings WHERE tenant_id = $1 \
             AND key LIKE 'desktop.v1/outbox/%' \
             AND sqlite_json_extract(value, '$.delivered') = '0' \
             ORDER BY row_seq LIMIT 64",
        )
        .bind(self.tenant())
        .fetch_all(&mut *tx)
        .await
        .map_err(db)?;
        tx.commit().await.map_err(db)?;
        rows.into_iter()
            .map(|row| {
                let k: String = row.try_get("key").map_err(db)?;
                let raw: String = row.try_get("value").map_err(db)?;
                let record = decode(&raw)?;
                let workspace = record["workspaceId"]
                    .as_str()
                    .ok_or_else(|| Error::Internal("desktop outbox missing workspace".into()))?;
                let agent = record["agentId"]
                    .as_str()
                    .ok_or_else(|| Error::Internal("desktop outbox missing agent".into()))?;
                Ok((
                    k,
                    WorkspaceId::from(workspace),
                    AgentId::from(agent),
                    record["payload"].clone(),
                ))
            })
            .collect()
    }

    pub async fn desktop_outbox_delivered(&self, id: &str) -> Result<()> {
        let mut tx = self.begin_write().await?;
        sqlx::query(
            "UPDATE settings SET value = sqlite_json_set(value, '$.delivered', sqlite_json('true')) \
             WHERE tenant_id = $1 AND key = $2 AND key LIKE 'desktop.v1/outbox/%'",
        )
        .bind(self.tenant())
        .bind(id)
        .execute(&mut *tx)
        .await
        .map_err(db)?;
        tx.commit().await.map_err(db)
    }

    pub async fn desktop_wake_exists(&self, agent: &AgentId, id: &str) -> Result<bool> {
        let mut tx = self.begin().await?;
        let exists = sqlx::query_scalar(
            "SELECT EXISTS(SELECT 1 FROM agent_message WHERE tenant_id = $1 AND agent_id = $2 \
               AND sqlite_json_extract(metadata, '$.desktopWakeId') = $3 \
             UNION ALL SELECT 1 FROM agent_queue WHERE tenant_id = $1 AND agent_id = $2 \
               AND sqlite_json_extract(payload, '$.messageMetadata.desktopWakeId') = $3)",
        )
        .bind(self.tenant())
        .bind(agent.as_str())
        .bind(id)
        .fetch_one(&mut *tx)
        .await
        .map_err(db)?;
        tx.commit().await.map_err(db)?;
        Ok(exists)
    }

    pub async fn desktop_invalidate_restart(&self) -> Result<()> {
        let _maintenance = self.desktop_writes.maintenance.write().await;
        let mut tx = self.begin_write().await?;
        sqlx::query(sqlx::AssertSqlSafe(format!(
            "DELETE FROM settings s WHERE s.tenant_id = $1 AND s.key LIKE 'desktop.v1/%' \
             AND (NOT {TARGET_LIVE} \
               OR (sqlite_json_extract(s.value, '$.principalId') IS NOT NULL \
                 AND NOT EXISTS(SELECT 1 FROM principal p WHERE p.tenant_id = $1 \
                   AND p.id = sqlite_json_extract(s.value, '$.principalId'))))"
        )))
        .bind(self.tenant())
        .execute(&mut *tx)
        .await
        .map_err(db)?;
        // SQLite walks these through its key index: byte order.
        let requests = sqlx::query(
            "SELECT key, value FROM settings WHERE tenant_id = $1 \
             AND key LIKE 'desktop.v1/request/%' \
             AND sqlite_json_extract(value, '$.outcome') IS NULL ORDER BY key COLLATE \"C\"",
        )
        .bind(self.tenant())
        .fetch_all(&mut *tx)
        .await
        .map_err(db)?;
        for row in requests {
            let k: String = row.try_get("key").map_err(db)?;
            let raw: String = row.try_get("value").map_err(db)?;
            let request = k
                .strip_prefix("desktop.v1/request/")
                .expect("query namespace");
            let record = decode(&raw)?;
            let payload = json!({"type":"desktop_control","requestId":request,"outcome":"invalidated","state":{"status":"inactive"},"message":"Daemon restarted; desktop control is not active. Do not automatically restart."});
            self.desktop_outbox_insert(
                &mut tx,
                &format!("request:{request}"),
                record["workspaceId"].as_str().unwrap_or_default(),
                record["agentId"].as_str().unwrap_or_default(),
                &payload,
            )
            .await?;
            sqlx::query(
                "UPDATE settings \
                 SET value = sqlite_json_set(value, '$.outcome', 'invalidated'::text) \
                 WHERE tenant_id = $1 AND key = $2",
            )
            .bind(self.tenant())
            .bind(&k)
            .execute(&mut *tx)
            .await
            .map_err(db)?;
        }
        let sessions = sqlx::query(
            "SELECT key, value FROM settings WHERE tenant_id = $1 \
             AND key LIKE 'desktop.v1/terminal/%' \
             AND sqlite_json_extract(value, '$.reason') IS NULL ORDER BY key COLLATE \"C\"",
        )
        .bind(self.tenant())
        .fetch_all(&mut *tx)
        .await
        .map_err(db)?;
        for row in sessions {
            let k: String = row.try_get("key").map_err(db)?;
            let raw: String = row.try_get("value").map_err(db)?;
            let session = k
                .strip_prefix("desktop.v1/terminal/")
                .expect("query namespace");
            let record = decode(&raw)?;
            sqlx::query(
                "DELETE FROM settings WHERE tenant_id = $1 AND key LIKE 'desktop.v1/outbox/%' \
                 AND sqlite_json_extract(value, '$.delivered') = '0' \
                 AND (sqlite_json_extract(value, '$.payload.sessionId') = $2 OR key = $3)",
            )
            .bind(self.tenant())
            .bind(session)
            .bind(key(
                "outbox",
                &format!(
                    "request:{}",
                    record["requestId"].as_str().unwrap_or_default()
                ),
            ))
            .execute(&mut *tx)
            .await
            .map_err(db)?;
            let mut payload = json!({"type":"desktop_control","sessionId":session,"outcome":"revoked","state":{"status":"inactive"},"message":"Daemon restarted; desktop control is not active. Do not automatically restart."});
            if let Some(request) = record["requestId"].as_str() {
                payload["requestId"] = request.into();
            }
            self.desktop_outbox_insert(
                &mut tx,
                &format!("session:{session}:disconnected"),
                record["workspaceId"].as_str().unwrap_or_default(),
                record["agentId"].as_str().unwrap_or_default(),
                &payload,
            )
            .await?;
            sqlx::query(
                "UPDATE settings \
                 SET value = sqlite_json_set(value, '$.reason', 'disconnected'::text) \
                 WHERE tenant_id = $1 AND key = $2",
            )
            .bind(self.tenant())
            .bind(&k)
            .execute(&mut *tx)
            .await
            .map_err(db)?;
        }
        tx.commit().await.map_err(db)
    }
}
