//! Agent repository: session CRUD + append-only message log (§9.1 / §9.2).
//!
//! The `agent_session` row holds the mutable session state; `agent_message` is
//! insert-only (no update/delete path) with a monotonic per-agent `seq`. Two
//! invariants are enforced here rather than by the schema (§9.5): `acp_session_id`
//! is **write-once** (set by the provider's `session:created`, never overwritten)
//! and `provider` is **immutable** once set on first real use. `stats` (§19.2) is
//! a derived snapshot and is never persisted — sessions load with `stats: None`.

use intent_core::{AgentId, AgentMessage, AgentSession, AgentStatus, Error, Result, WorkspaceId};
use sqlx::sqlite::SqliteRow;
use sqlx::Row;
use uuid::Uuid;

use crate::{enum_from_db, enum_to_db, Store};

const SESSION_COLUMNS: &str = "id, workspace_id, backend_session_id, acp_session_id, name, \
    name_explicitly_set, model, provider, status, is_active, system_prompt, created_at, updated_at";

impl Store {
    /// Insert an agent-session row. `messages`/`stats` are not persisted here;
    /// append messages via [`Store::append_agent_message`].
    pub async fn insert_agent_session(&self, s: &AgentSession) -> Result<()> {
        let sql = format!(
            "INSERT INTO agent_session ({SESSION_COLUMNS}) VALUES \
             (?,?,?,?,?,?,?,?,?,?,?,?,?)"
        );
        sqlx::query(&sql)
            .bind(&s.id.0)
            .bind(&s.workspace_id.0)
            .bind(s.backend_session_id.as_ref().map(|b| b.0.clone()))
            .bind(&s.acp_session_id)
            .bind(&s.name)
            .bind(s.name_explicitly_set as i64)
            .bind(&s.model)
            .bind(&s.provider)
            .bind(enum_to_db(&s.status)?)
            .bind(s.is_active as i64)
            .bind(&s.system_prompt)
            .bind(&s.created_at)
            .bind(&s.updated_at)
            .execute(self.pool())
            .await
            .map_err(|e| Error::Internal(format!("insert agent session failed: {e}")))?;
        Ok(())
    }

    /// Fetch a session by id (with its message log), or `NotFound`.
    pub async fn get_agent_session(&self, id: &AgentId) -> Result<AgentSession> {
        let sql = format!("SELECT {SESSION_COLUMNS} FROM agent_session WHERE id = ?");
        let row = sqlx::query(&sql)
            .bind(&id.0)
            .fetch_optional(self.pool())
            .await
            .map_err(|e| Error::Internal(format!("get agent session failed: {e}")))?;
        match row {
            Some(r) => {
                let mut session = map_session_row(&r)?;
                session.messages = self.get_agent_messages(id, None).await?;
                Ok(session)
            }
            None => Err(Error::NotFound(format!("agent session {id}"))),
        }
    }

    /// List a workspace's sessions (each with its message log), oldest first.
    pub async fn list_agent_sessions(
        &self,
        workspace_id: &WorkspaceId,
    ) -> Result<Vec<AgentSession>> {
        let sql = format!(
            "SELECT {SESSION_COLUMNS} FROM agent_session WHERE workspace_id = ? ORDER BY created_at"
        );
        let rows = sqlx::query(&sql)
            .bind(&workspace_id.0)
            .fetch_all(self.pool())
            .await
            .map_err(|e| Error::Internal(format!("list agent sessions failed: {e}")))?;
        let mut sessions = Vec::with_capacity(rows.len());
        for r in &rows {
            let mut session = map_session_row(r)?;
            session.messages = self.get_agent_messages(&session.id, None).await?;
            sessions.push(session);
        }
        Ok(sessions)
    }

    /// Update mutable session state, enforcing the `acp_session_id` write-once
    /// and `provider` immutability invariants (§9.5). `NotFound` if absent.
    pub async fn update_agent_session(&self, s: &AgentSession) -> Result<()> {
        let current = self.get_agent_session(&s.id).await?;
        if current.provider.is_some() && s.provider != current.provider {
            return Err(Error::Internal(
                "agent provider is immutable once set".to_string(),
            ));
        }
        if current.acp_session_id.is_some() && s.acp_session_id != current.acp_session_id {
            return Err(Error::Internal("acpSessionId is write-once".to_string()));
        }
        sqlx::query(
            "UPDATE agent_session SET backend_session_id=?, acp_session_id=?, name=?, \
             name_explicitly_set=?, model=?, provider=?, status=?, is_active=?, system_prompt=?, \
             updated_at=? WHERE id=?",
        )
        .bind(s.backend_session_id.as_ref().map(|b| b.0.clone()))
        .bind(&s.acp_session_id)
        .bind(&s.name)
        .bind(s.name_explicitly_set as i64)
        .bind(&s.model)
        .bind(&s.provider)
        .bind(enum_to_db(&s.status)?)
        .bind(s.is_active as i64)
        .bind(&s.system_prompt)
        .bind(&s.updated_at)
        .bind(&s.id.0)
        .execute(self.pool())
        .await
        .map_err(|e| Error::Internal(format!("update agent session failed: {e}")))?;
        Ok(())
    }

    /// Set `acp_session_id` write-once (the provider `session:created` path).
    /// Errors if it is already set to a different value (§9.5). `NotFound` if
    /// the session is absent.
    pub async fn set_acp_session_id(&self, id: &AgentId, acp_session_id: &str) -> Result<()> {
        let current = self.get_agent_session(id).await?;
        match current.acp_session_id.as_deref() {
            Some(existing) if existing == acp_session_id => return Ok(()),
            Some(_) => return Err(Error::Internal("acpSessionId is write-once".to_string())),
            None => {}
        }
        sqlx::query("UPDATE agent_session SET acp_session_id=? WHERE id=?")
            .bind(acp_session_id)
            .bind(&id.0)
            .execute(self.pool())
            .await
            .map_err(|e| Error::Internal(format!("set acp session id failed: {e}")))?;
        Ok(())
    }

    /// Delete an agent session and its message log (the `agent_message` rows
    /// cascade). Returns whether a row was removed (`agent.delete`, §5.5).
    pub async fn delete_agent_session(&self, id: &AgentId) -> Result<bool> {
        let result = sqlx::query("DELETE FROM agent_session WHERE id = ?")
            .bind(&id.0)
            .execute(self.pool())
            .await
            .map_err(|e| Error::Internal(format!("delete agent session failed: {e}")))?;
        Ok(result.rows_affected() > 0)
    }
}

fn map_session_row(row: &SqliteRow) -> Result<AgentSession> {
    let backend: Option<String> = col(row, "backend_session_id")?;
    Ok(AgentSession {
        id: AgentId(col(row, "id")?),
        workspace_id: WorkspaceId(col(row, "workspace_id")?),
        backend_session_id: backend.map(AgentId),
        acp_session_id: col(row, "acp_session_id")?,
        name: col(row, "name")?,
        name_explicitly_set: col::<i64>(row, "name_explicitly_set")? != 0,
        model: col(row, "model")?,
        provider: col(row, "provider")?,
        status: enum_from_db::<AgentStatus>(&col::<String>(row, "status")?)?,
        is_active: col::<i64>(row, "is_active")? != 0,
        system_prompt: col(row, "system_prompt")?,
        // Loaded separately by the caller; derived `stats` is never persisted.
        messages: Vec::new(),
        stats: None,
        created_at: col(row, "created_at")?,
        updated_at: col(row, "updated_at")?,
    })
}

const MESSAGE_COLUMNS: &str = "id, agent_id, seq, role, content, created_at";

impl Store {
    /// Append a message to an agent's insert-only log, minting a UUIDv7 id and
    /// the next monotonic `seq`, and return the persisted [`AgentMessage`].
    pub async fn append_agent_message(
        &self,
        agent_id: &AgentId,
        role: &str,
        content: &serde_json::Value,
        created_at: &str,
    ) -> Result<AgentMessage> {
        let seq: i64 = sqlx::query(
            "SELECT COALESCE(MAX(seq), -1) + 1 AS next FROM agent_message WHERE agent_id = ?",
        )
        .bind(&agent_id.0)
        .fetch_one(self.pool())
        .await
        .map_err(|e| Error::Internal(format!("next agent message seq failed: {e}")))?
        .get::<i64, _>("next");
        let id = Uuid::now_v7().to_string();
        let content_json = serde_json::to_string(content)
            .map_err(|e| Error::Internal(format!("encode message content failed: {e}")))?;
        let sql = format!("INSERT INTO agent_message ({MESSAGE_COLUMNS}) VALUES (?,?,?,?,?,?)");
        sqlx::query(&sql)
            .bind(&id)
            .bind(&agent_id.0)
            .bind(seq)
            .bind(role)
            .bind(&content_json)
            .bind(created_at)
            .execute(self.pool())
            .await
            .map_err(|e| Error::Internal(format!("append agent message failed: {e}")))?;
        Ok(AgentMessage {
            id,
            agent_id: agent_id.clone(),
            seq,
            role: role.to_string(),
            content: content.clone(),
            created_at: created_at.to_string(),
        })
    }

    /// Read an agent's messages in chronological (`seq` ascending) order. When
    /// `limit` is set, returns the most-recent `limit` messages (still ordered
    /// oldest→newest), matching `agent.getConversation` (PROTOCOL §5.5).
    pub async fn get_agent_messages(
        &self,
        agent_id: &AgentId,
        limit: Option<i64>,
    ) -> Result<Vec<AgentMessage>> {
        let sql = match limit {
            Some(_) => format!(
                "SELECT {MESSAGE_COLUMNS} FROM (SELECT {MESSAGE_COLUMNS} FROM agent_message \
                 WHERE agent_id = ? ORDER BY seq DESC LIMIT ?) ORDER BY seq ASC"
            ),
            None => format!(
                "SELECT {MESSAGE_COLUMNS} FROM agent_message WHERE agent_id = ? ORDER BY seq ASC"
            ),
        };
        let mut query = sqlx::query(&sql).bind(&agent_id.0);
        if let Some(n) = limit {
            query = query.bind(n);
        }
        let rows = query
            .fetch_all(self.pool())
            .await
            .map_err(|e| Error::Internal(format!("get agent messages failed: {e}")))?;
        rows.iter().map(map_message_row).collect()
    }

    /// Total number of messages logged for an agent (`agent.getConversation`
    /// `totalMessages`).
    pub async fn count_agent_messages(&self, agent_id: &AgentId) -> Result<i64> {
        let n: i64 = sqlx::query("SELECT COUNT(*) AS n FROM agent_message WHERE agent_id = ?")
            .bind(&agent_id.0)
            .fetch_one(self.pool())
            .await
            .map_err(|e| Error::Internal(format!("count agent messages failed: {e}")))?
            .get::<i64, _>("n");
        Ok(n)
    }
}

fn map_message_row(row: &SqliteRow) -> Result<AgentMessage> {
    let content: serde_json::Value = serde_json::from_str(&col::<String>(row, "content")?)
        .map_err(|e| Error::Internal(format!("decode message content failed: {e}")))?;
    Ok(AgentMessage {
        id: col(row, "id")?,
        agent_id: AgentId(col(row, "agent_id")?),
        seq: col(row, "seq")?,
        role: col(row, "role")?,
        content,
        created_at: col(row, "created_at")?,
    })
}

fn col<'r, T>(row: &'r SqliteRow, name: &str) -> Result<T>
where
    T: sqlx::Decode<'r, sqlx::Sqlite> + sqlx::Type<sqlx::Sqlite>,
{
    row.try_get::<T, _>(name)
        .map_err(|e| Error::Internal(format!("column {name}: {e}")))
}
