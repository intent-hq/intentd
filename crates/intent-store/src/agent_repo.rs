//! Agent repository: session CRUD + append-only message log (§9.1 / §9.2).
//!
//! The `agent_session` row holds the mutable session state; `agent_message` is
//! insert-only (no update/delete path) with a monotonic per-agent `seq`. Two
//! invariants are enforced here rather than by the schema (§9.5): `acp_session_id`
//! is **write-once** (set by the provider's `session:created`, never overwritten)
//! and `provider` is **immutable** once set on first real use. `stats` (§19.2) is
//! a derived snapshot and is never persisted — sessions load with `stats: None`.

use intent_core::{
    AgentId, AgentMessage, AgentSession, AgentStatus, Error, NoteId, Result, WorkspaceId,
};
use sqlx::sqlite::SqliteRow;
use sqlx::Row;
use uuid::Uuid;

use crate::{enum_from_db, enum_to_db, Store};

const SESSION_COLUMNS: &str = "id, workspace_id, backend_session_id, acp_session_id, name, \
    name_explicitly_set, model, provider, status, is_active, system_prompt, created_at, updated_at, \
    parent_agent_id, specialist, task_note_id, skip_auto_commit, completion_report, \
    completion_report_timestamp, delegation_depth, initial_message, context_references, image_blocks, \
    is_background, metadata";

/// Encode an optional JSON payload column (`context_references` /
/// `image_blocks`) as its TEXT form, `None` staying NULL.
fn json_col_to_db(v: &Option<serde_json::Value>) -> Result<Option<String>> {
    v.as_ref()
        .map(|value| {
            serde_json::to_string(value)
                .map_err(|e| Error::Internal(format!("encode session json column failed: {e}")))
        })
        .transpose()
}

/// Decode an optional JSON payload column back into its `serde_json::Value`.
fn json_col_from_db(raw: Option<String>, name: &str) -> Result<Option<serde_json::Value>> {
    raw.map(|s| {
        serde_json::from_str(&s)
            .map_err(|e| Error::Internal(format!("decode session column {name} failed: {e}")))
    })
    .transpose()
}

impl Store {
    /// Insert an agent-session row. `messages`/`stats` are not persisted here;
    /// append messages via [`Store::append_agent_message`].
    pub async fn insert_agent_session(&self, s: &AgentSession) -> Result<()> {
        let sql = format!(
            "INSERT INTO agent_session ({SESSION_COLUMNS}) VALUES \
             (?,?,?,?,?,?,?,?,?,?,?,?,?,?,?,?,?,?,?,?,?,?,?,?,?)"
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
            .bind(s.parent_agent_id.as_ref().map(|b| b.0.clone()))
            .bind(&s.specialist)
            .bind(s.task_note_id.as_ref().map(|n| n.0.clone()))
            .bind(s.skip_auto_commit as i64)
            .bind(&s.completion_report)
            .bind(&s.completion_report_timestamp)
            .bind(s.delegation_depth)
            .bind(&s.initial_message)
            .bind(json_col_to_db(&s.context_references)?)
            .bind(json_col_to_db(&s.image_blocks)?)
            .bind(s.is_background as i64)
            .bind(encode_metadata(s.metadata.as_ref())?)
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

    /// Lightweight name-only lookup used by hot paths that just need the
    /// session's display name (e.g. note-version author stamping). Skips the
    /// full message-log fetch that `get_agent_session` performs.
    pub async fn get_agent_session_name(&self, id: &AgentId) -> Result<Option<String>> {
        let row = sqlx::query("SELECT name FROM agent_session WHERE id = ?")
            .bind(&id.0)
            .fetch_optional(self.pool())
            .await
            .map_err(|e| Error::Internal(format!("get agent session name failed: {e}")))?;
        Ok(row.map(|r| r.get::<String, _>("name")))
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

    /// List every persisted session across workspaces, oldest first. Backs the
    /// daemon-startup stale-session heal: a session left non-terminal across a
    /// crash has no live worker after restart, so the heal sweeps the whole
    /// table once before serving. Sessions are returned WITHOUT their message
    /// logs (the heal does not need them) to keep the sweep O(rows).
    pub async fn list_all_agent_sessions(&self) -> Result<Vec<AgentSession>> {
        let sql = format!("SELECT {SESSION_COLUMNS} FROM agent_session ORDER BY created_at");
        let rows = sqlx::query(&sql)
            .fetch_all(self.pool())
            .await
            .map_err(|e| Error::Internal(format!("list all agent sessions failed: {e}")))?;
        rows.iter().map(map_session_row).collect()
    }

    /// Update mutable session state, enforcing the `acp_session_id` write-once
    /// and `provider` immutability invariants (§9.5). Scoped to `workspace_id`
    /// as a defense-in-depth guard so a caller bound to workspace B cannot
    /// mutate an `agent_session` row that belongs to workspace A (mirrors the
    /// post-0022 `note_repo` pattern). `NotFound` if absent or the workspace
    /// does not match.
    pub async fn update_agent_session(
        &self,
        workspace_id: &WorkspaceId,
        s: &AgentSession,
    ) -> Result<()> {
        let current = self.get_agent_session(&s.id).await?;
        if current.workspace_id != *workspace_id {
            return Err(Error::NotFound(format!("agent session {}", s.id)));
        }
        if current.provider.is_some() && s.provider != current.provider {
            return Err(Error::Internal(
                "agent provider is immutable once set".to_string(),
            ));
        }
        if current.acp_session_id.is_some() && s.acp_session_id != current.acp_session_id {
            return Err(Error::Internal("acpSessionId is write-once".to_string()));
        }
        let rows = sqlx::query(
            "UPDATE agent_session SET backend_session_id=?, acp_session_id=?, name=?, \
             name_explicitly_set=?, model=?, provider=?, status=?, is_active=?, system_prompt=?, \
             updated_at=?, parent_agent_id=?, specialist=?, task_note_id=?, skip_auto_commit=?, \
             completion_report=?, completion_report_timestamp=?, delegation_depth=?, \
             initial_message=?, context_references=?, image_blocks=?, is_background=?, \
             metadata=? WHERE id=? AND workspace_id=?",
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
        .bind(s.parent_agent_id.as_ref().map(|b| b.0.clone()))
        .bind(&s.specialist)
        .bind(s.task_note_id.as_ref().map(|n| n.0.clone()))
        .bind(s.skip_auto_commit as i64)
        .bind(&s.completion_report)
        .bind(&s.completion_report_timestamp)
        .bind(s.delegation_depth)
        .bind(&s.initial_message)
        .bind(json_col_to_db(&s.context_references)?)
        .bind(json_col_to_db(&s.image_blocks)?)
        .bind(s.is_background as i64)
        .bind(encode_metadata(s.metadata.as_ref())?)
        .bind(&s.id.0)
        .bind(&workspace_id.0)
        .execute(self.pool())
        .await
        .map_err(|e| Error::Internal(format!("update agent session failed: {e}")))?
        .rows_affected();
        if rows == 0 {
            return Err(Error::NotFound(format!("agent session {}", s.id)));
        }
        Ok(())
    }

    /// Persist the runtime `status` + `is_active` transition for `agent_session`
    /// without touching the write-once `acp_session_id` / immutable `provider`
    /// (the broader [`Store::update_agent_session`] enforces those invariants).
    /// Drives the `pending → active → idle` lifecycle so a hydrated/reloaded
    /// chat reflects the live state (PROTOCOL §6.5 `agent:status-changed`).
    /// Scoped to `workspace_id` (defense-in-depth). `updated_at` is refreshed to
    /// the supplied timestamp. `NotFound` if the session is absent or the
    /// workspace does not match.
    pub async fn set_agent_session_status(
        &self,
        workspace_id: &WorkspaceId,
        id: &AgentId,
        status: AgentStatus,
        is_active: bool,
        updated_at: &str,
    ) -> Result<()> {
        let rows = sqlx::query(
            "UPDATE agent_session SET status=?, is_active=?, updated_at=? \
             WHERE id=? AND workspace_id=?",
        )
        .bind(enum_to_db(&status)?)
        .bind(is_active as i64)
        .bind(updated_at)
        .bind(&id.0)
        .bind(&workspace_id.0)
        .execute(self.pool())
        .await
        .map_err(|e| Error::Internal(format!("set agent session status failed: {e}")))?
        .rows_affected();
        if rows == 0 {
            return Err(Error::NotFound(format!("agent session {id}")));
        }
        Ok(())
    }

    /// Refresh `updated_at` to the current timestamp whenever a message is
    /// appended to an agent session (STAB-19 fix). The FE agent-card timestamp
    /// is derived from `agent_session.updated_at`, so bumping it on every message
    /// (both user and agent messages, including the dequeued-message path) ensures
    /// the UI reflects real activity, not just status transitions. Scoped to
    /// `workspace_id` (defense-in-depth guard — matches the pattern of
    /// `set_agent_session_status` and `update_agent_session`). `NotFound` if
    /// the session is absent or the workspace does not match.
    pub async fn refresh_agent_session_timestamp(
        &self,
        workspace_id: &WorkspaceId,
        id: &AgentId,
        updated_at: &str,
    ) -> Result<()> {
        let rows =
            sqlx::query("UPDATE agent_session SET updated_at=? WHERE id=? AND workspace_id=?")
                .bind(updated_at)
                .bind(&id.0)
                .bind(&workspace_id.0)
                .execute(self.pool())
                .await
                .map_err(|e| {
                    Error::Internal(format!("refresh agent session timestamp failed: {e}"))
                })?
                .rows_affected();
        if rows == 0 {
            return Err(Error::NotFound(format!("agent session {id}")));
        }
        Ok(())
    }

    /// Reset all `is_active=1` rows to `is_active=0` unconditionally (Wave B
    /// post-restart recovery). ACP sessions are process-local and cannot survive
    /// a daemon restart, so any `is_active=1` flag after boot is stale. Called
    /// early in startup (before listeners) to ensure no races with live turn
    /// spawns. Returns the count of rows reset.
    pub async fn reset_all_active_flags(&self) -> Result<usize> {
        let rows = sqlx::query("UPDATE agent_session SET is_active=0 WHERE is_active=1")
            .execute(self.pool())
            .await
            .map_err(|e| Error::Internal(format!("reset active flags failed: {e}")))?
            .rows_affected();
        Ok(rows as usize)
    }

    /// Set `acp_session_id` write-once (the provider `session:created` path).
    /// Scoped to `workspace_id` (defense-in-depth). Errors if it is already set
    /// to a different value (§9.5). `NotFound` if the session is absent or the
    /// workspace does not match.
    pub async fn set_acp_session_id(
        &self,
        workspace_id: &WorkspaceId,
        id: &AgentId,
        acp_session_id: &str,
    ) -> Result<()> {
        let current = self.get_agent_session(id).await?;
        if current.workspace_id != *workspace_id {
            return Err(Error::NotFound(format!("agent session {id}")));
        }
        match current.acp_session_id.as_deref() {
            Some(existing) if existing == acp_session_id => return Ok(()),
            Some(_) => return Err(Error::Internal("acpSessionId is write-once".to_string())),
            None => {}
        }
        sqlx::query("UPDATE agent_session SET acp_session_id=? WHERE id=? AND workspace_id=?")
            .bind(acp_session_id)
            .bind(&id.0)
            .bind(&workspace_id.0)
            .execute(self.pool())
            .await
            .map_err(|e| Error::Internal(format!("set acp session id failed: {e}")))?;
        Ok(())
    }

    /// Compare-and-swap `acp_session_id` on the resume-impossible fallback: swap
    /// the stored id for `new_acp_session_id` ONLY when it currently equals
    /// `expected_old` (the id we just failed to `session/load`). If it has since
    /// diverged — e.g. a concurrent recreate already swapped it — the stored
    /// value is left untouched and returned, so the canonical id is never
    /// clobbered. Scoped to `workspace_id` (defense-in-depth). Returns the
    /// canonical id after the operation. Unlike [`Store::set_acp_session_id`]
    /// (strict write-once for the first set), this is the ONLY relaxation,
    /// scoped to the fallback (§6.5). `NotFound` if the session is absent or
    /// the workspace does not match.
    pub async fn replace_acp_session_id(
        &self,
        workspace_id: &WorkspaceId,
        id: &AgentId,
        expected_old: &str,
        new_acp_session_id: &str,
    ) -> Result<String> {
        let current = self.get_agent_session(id).await?;
        if current.workspace_id != *workspace_id {
            return Err(Error::NotFound(format!("agent session {id}")));
        }
        match current.acp_session_id.as_deref() {
            // The id we failed to load is still canonical → swap in the fresh one.
            Some(existing) if existing == expected_old => {
                self.write_acp_session_id(workspace_id, id, new_acp_session_id)
                    .await?;
                Ok(new_acp_session_id.to_string())
            }
            // Diverged (a concurrent recreate already swapped) → reuse the stored
            // canonical value instead of clobbering it.
            Some(existing) => Ok(existing.to_string()),
            // Nothing stored to clobber → set the fresh id.
            None => {
                self.write_acp_session_id(workspace_id, id, new_acp_session_id)
                    .await?;
                Ok(new_acp_session_id.to_string())
            }
        }
    }

    /// Unconditional `acp_session_id` write helper shared by the CAS replace
    /// branches (callers gate the overwrite policy before invoking this).
    async fn write_acp_session_id(
        &self,
        workspace_id: &WorkspaceId,
        id: &AgentId,
        acp_session_id: &str,
    ) -> Result<()> {
        sqlx::query("UPDATE agent_session SET acp_session_id=? WHERE id=? AND workspace_id=?")
            .bind(acp_session_id)
            .bind(&id.0)
            .bind(&workspace_id.0)
            .execute(self.pool())
            .await
            .map_err(|e| Error::Internal(format!("replace acp session id failed: {e}")))?;
        Ok(())
    }

    /// Delete an agent session and its message log (the `agent_message` rows
    /// cascade). Scoped to `workspace_id` (defense-in-depth). Returns whether a
    /// row was removed (`agent.delete`, §5.5).
    pub async fn delete_agent_session(
        &self,
        workspace_id: &WorkspaceId,
        id: &AgentId,
    ) -> Result<bool> {
        let result = sqlx::query("DELETE FROM agent_session WHERE id = ? AND workspace_id = ?")
            .bind(&id.0)
            .bind(&workspace_id.0)
            .execute(self.pool())
            .await
            .map_err(|e| Error::Internal(format!("delete agent session failed: {e}")))?;
        Ok(result.rows_affected() > 0)
    }
}

fn map_session_row(row: &SqliteRow) -> Result<AgentSession> {
    let backend: Option<String> = col(row, "backend_session_id")?;
    let parent: Option<String> = col(row, "parent_agent_id")?;
    let task_note: Option<String> = col(row, "task_note_id")?;
    let metadata_raw: Option<String> = col(row, "metadata")?;
    let metadata = match metadata_raw {
        Some(raw) if !raw.is_empty() => Some(
            serde_json::from_str::<serde_json::Value>(&raw).map_err(|e| {
                Error::Internal(format!("decode agent session metadata failed: {e}"))
            })?,
        ),
        _ => None,
    };
    Ok(AgentSession {
        id: AgentId(col(row, "id")?),
        workspace_id: WorkspaceId(col(row, "workspace_id")?),
        parent_agent_id: parent.map(AgentId),
        backend_session_id: backend.map(AgentId),
        acp_session_id: col(row, "acp_session_id")?,
        name: col(row, "name")?,
        name_explicitly_set: col::<i64>(row, "name_explicitly_set")? != 0,
        model: col(row, "model")?,
        provider: col(row, "provider")?,
        specialist: col(row, "specialist")?,
        status: enum_from_db::<AgentStatus>(&col::<String>(row, "status")?)?,
        is_active: col::<i64>(row, "is_active")? != 0,
        system_prompt: col(row, "system_prompt")?,
        // Loaded separately by the caller; derived `stats` is never persisted.
        messages: Vec::new(),
        stats: None,
        task_note_id: task_note.map(NoteId::from),
        skip_auto_commit: col::<i64>(row, "skip_auto_commit")? != 0,
        completion_report: col(row, "completion_report")?,
        completion_report_timestamp: col(row, "completion_report_timestamp")?,
        delegation_depth: col(row, "delegation_depth")?,
        initial_message: col(row, "initial_message")?,
        context_references: json_col_from_db(
            col(row, "context_references")?,
            "context_references",
        )?,
        image_blocks: json_col_from_db(col(row, "image_blocks")?, "image_blocks")?,
        is_background: col::<i64>(row, "is_background")? != 0,
        metadata,
        created_at: col(row, "created_at")?,
        updated_at: col(row, "updated_at")?,
    })
}

/// Encode `agent_session.metadata` for persistence: `None` → SQL `NULL`,
/// `Some(value)` → the JSON-serialized string. Kept local to this module so
/// `insert_agent_session` and `update_agent_session` share the same shape.
fn encode_metadata(value: Option<&serde_json::Value>) -> Result<Option<String>> {
    match value {
        None => Ok(None),
        Some(v) => Ok(Some(serde_json::to_string(v).map_err(|e| {
            Error::Internal(format!("encode agent session metadata failed: {e}"))
        })?)),
    }
}

const MESSAGE_COLUMNS: &str = "id, agent_id, seq, role, content, metadata, created_at";
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
        let id = Uuid::now_v7().to_string();
        self.append_agent_message_with_id(agent_id, &id, role, content, None, created_at)
            .await
    }

    /// Append a message with a caller-supplied `metadata` payload (`agent.sendMessage`
    /// / `agent.forceMessage`'s `messageMetadata`, PROTOCOL §5.5). The metadata is
    /// stored verbatim as JSON on the row and round-trips on transcript reads;
    /// callers with no per-message metadata continue to use
    /// [`Store::append_agent_message`] which stores `NULL`.
    pub async fn append_agent_message_with_metadata(
        &self,
        agent_id: &AgentId,
        role: &str,
        content: &serde_json::Value,
        metadata: Option<&serde_json::Value>,
        created_at: &str,
    ) -> Result<AgentMessage> {
        let id = Uuid::now_v7().to_string();
        self.append_agent_message_with_id(agent_id, &id, role, content, metadata, created_at)
            .await
    }

    /// Append a message with a caller-supplied id (the assistant turn mints its
    /// `messageId` at turn start so streaming block ids `{messageId}:{index}`
    /// match the persisted blocks — CS-0 D1), allocating the next monotonic
    /// `seq` and returning the persisted [`AgentMessage`].
    ///
    /// ## Transaction boundary
    ///
    /// This operation executes TWO separate queries (SELECT next seq, INSERT
    /// message) without an explicit transaction wrapper. Each query runs in
    /// SQLite's autocommit mode as its own implicit transaction. The schema
    /// enforces `UNIQUE(agent_id, seq)`, so concurrent appends racing on the
    /// SELECT phase will cause one INSERT to fail with a constraint violation.
    ///
    /// **Crash safety**: A crash between SELECT and INSERT leaves a seq gap
    /// (the next caller sees a higher seq) but does NOT lose any committed
    /// message — only the INSERT commits data. Once INSERT completes, the
    /// message row is durable. Assistant-message append (the streaming path)
    /// is additionally protected by the AgentManager's per-agent single-flight
    /// slot, serializing turns for one agent and eliminating the seq-race
    /// window on that hot path. User-message appends (sendMessage, forceMessage,
    /// wake delivery) can still race if fired concurrently for one agent, but
    /// the UNIQUE constraint will reject duplicates rather than silently
    /// corrupting the seq order.
    pub async fn append_agent_message_with_id(
        &self,
        agent_id: &AgentId,
        id: &str,
        role: &str,
        content: &serde_json::Value,
        metadata: Option<&serde_json::Value>,
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
        let content_json = serde_json::to_string(content)
            .map_err(|e| Error::Internal(format!("encode message content failed: {e}")))?;
        let metadata_json = match metadata {
            Some(m) => Some(
                serde_json::to_string(m)
                    .map_err(|e| Error::Internal(format!("encode message metadata failed: {e}")))?,
            ),
            None => None,
        };
        let sql = format!("INSERT INTO agent_message ({MESSAGE_COLUMNS}) VALUES (?,?,?,?,?,?,?)");
        sqlx::query(&sql)
            .bind(id)
            .bind(&agent_id.0)
            .bind(seq)
            .bind(role)
            .bind(&content_json)
            .bind(metadata_json.as_deref())
            .bind(created_at)
            .execute(self.pool())
            .await
            .map_err(|e| Error::Internal(format!("append agent message failed: {e}")))?;
        Ok(AgentMessage {
            id: id.to_string(),
            agent_id: agent_id.clone(),
            seq,
            role: role.to_string(),
            content: content.clone(),
            metadata: metadata.cloned(),
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

    /// Atomically clear the agent's message log and reinsert `messages` under
    /// fresh 0-based monotonic `seq` values. Row ids are minted here (UUIDv7)
    /// so callers cannot smuggle stale ids across the swap; the returned
    /// [`AgentMessage`]s carry the new id/`seq` pairing. Used by the FE's
    /// edit-truncate transcript-mutation path (`agent.replaceMessages`,
    /// PROTOCOL §5.5). Callers are expected to reject busy sessions before
    /// invoking this (message-log mutations must not race an in-flight turn).
    pub async fn replace_agent_messages(
        &self,
        agent_id: &AgentId,
        messages: &[ReplaceMessage<'_>],
    ) -> Result<Vec<AgentMessage>> {
        let mut tx =
            self.pool().begin().await.map_err(|e| {
                Error::Internal(format!("replace agent messages begin failed: {e}"))
            })?;
        sqlx::query("DELETE FROM agent_message WHERE agent_id = ?")
            .bind(&agent_id.0)
            .execute(&mut *tx)
            .await
            .map_err(|e| Error::Internal(format!("replace agent messages clear failed: {e}")))?;
        let mut inserted = Vec::with_capacity(messages.len());
        let insert_sql =
            format!("INSERT INTO agent_message ({MESSAGE_COLUMNS}) VALUES (?,?,?,?,?,?,?)");
        for (idx, m) in messages.iter().enumerate() {
            let seq = idx as i64;
            let id = Uuid::now_v7().to_string();
            let content_json = serde_json::to_string(&m.content).map_err(|e| {
                Error::Internal(format!("encode replaced message content failed: {e}"))
            })?;
            let metadata_json = match m.metadata {
                Some(md) => Some(serde_json::to_string(md).map_err(|e| {
                    Error::Internal(format!("encode replaced message metadata failed: {e}"))
                })?),
                None => None,
            };
            sqlx::query(&insert_sql)
                .bind(&id)
                .bind(&agent_id.0)
                .bind(seq)
                .bind(m.role)
                .bind(&content_json)
                .bind(metadata_json.as_deref())
                .bind(m.created_at)
                .execute(&mut *tx)
                .await
                .map_err(|e| {
                    Error::Internal(format!("replace agent messages insert failed: {e}"))
                })?;
            inserted.push(AgentMessage {
                id,
                agent_id: agent_id.clone(),
                seq,
                role: m.role.to_string(),
                content: m.content.clone(),
                metadata: m.metadata.cloned(),
                created_at: m.created_at.to_string(),
            });
        }
        tx.commit()
            .await
            .map_err(|e| Error::Internal(format!("replace agent messages commit failed: {e}")))?;
        Ok(inserted)
    }
}

/// Input row for [`Store::replace_agent_messages`]: borrowed refs to the
/// caller-supplied `role`/`content`/`metadata`/`created_at`, so the service
/// layer can build the batch without cloning the transcript twice.
pub struct ReplaceMessage<'a> {
    pub role: &'a str,
    pub content: &'a serde_json::Value,
    pub metadata: Option<&'a serde_json::Value>,
    pub created_at: &'a str,
}

fn map_message_row(row: &SqliteRow) -> Result<AgentMessage> {
    let content: serde_json::Value = serde_json::from_str(&col::<String>(row, "content")?)
        .map_err(|e| Error::Internal(format!("decode message content failed: {e}")))?;
    let metadata: Option<serde_json::Value> = match col::<Option<String>>(row, "metadata")? {
        Some(raw) => Some(
            serde_json::from_str(&raw)
                .map_err(|e| Error::Internal(format!("decode message metadata failed: {e}")))?,
        ),
        None => None,
    };
    Ok(AgentMessage {
        id: col(row, "id")?,
        agent_id: AgentId(col(row, "agent_id")?),
        seq: col(row, "seq")?,
        role: col(row, "role")?,
        content,
        metadata,
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
