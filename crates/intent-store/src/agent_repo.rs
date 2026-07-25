//! Agent repository: session CRUD + append-only message log (§9.1 / §9.2).
//!
//! The `agent_session` row holds the mutable session state; `agent_message` is
//! insert-only (no update/delete path) with a monotonic per-agent `seq`. Two
//! invariants are enforced here rather than by the schema (§9.5): `acp_session_id`
//! is **write-once** (set by the provider's `session:created`, never overwritten)
//! and `provider` is **immutable** once set on first real use. `stats` (§19.2) is
//! a derived snapshot and is never persisted — sessions load with `stats: None`.

use intent_core::{
    AgentId, AgentMessage, AgentSession, AgentStatus, Error, NoteId, Result, TokenUsageTotals,
    WorkspaceId,
};
use sqlx::sqlite::SqliteRow;
use sqlx::Row;
use uuid::Uuid;

use crate::{enum_from_db, enum_to_db, Store};

const SESSION_COLUMNS: &str = "id, workspace_id, backend_session_id, acp_session_id, name, \
    name_explicitly_set, model, provider, status, is_active, system_prompt, created_at, updated_at, \
    parent_agent_id, specialist, task_note_id, skip_auto_commit, completion_report, \
    completion_report_timestamp, delegation_depth, initial_message, context_references, image_blocks, \
    is_background, metadata, sandbox_id, sandbox_path, sandbox_branch, stop_reason";

/// Interrupted agent record (INT-41). Returned by
/// [`Store::list_interrupted_agents`], joined with agent_session and workspace.
#[derive(Debug, Clone)]
pub struct InterruptedAgent {
    pub agent_id: AgentId,
    pub workspace_id: WorkspaceId,
    pub prev_status: String,
    pub interrupted_at: String,
    pub agent_name: Option<String>,
    pub workspace_name: Option<String>,
}

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

/// Bind the full 29-column `agent_session` insert value list onto `query`, in
/// [`SESSION_COLUMNS`] order. Shared by [`Store::insert_agent_session`] and
/// [`Store::insert_agent_session_with_messages`] so the column/bind pairing
/// lives in one place.
fn bind_session_insert<'q>(
    query: sqlx::query::Query<'q, sqlx::Sqlite, sqlx::sqlite::SqliteArguments<'q>>,
    s: &'q AgentSession,
) -> Result<sqlx::query::Query<'q, sqlx::Sqlite, sqlx::sqlite::SqliteArguments<'q>>> {
    Ok(query
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
        .bind(&s.sandbox_id)
        .bind(&s.sandbox_path)
        .bind(&s.sandbox_branch)
        .bind(&s.stop_reason))
}

impl Store {
    /// Insert an agent-session row. `messages`/`stats` are not persisted here;
    /// append messages via [`Store::append_agent_message`].
    ///
    /// A UNIQUE violation on the id (a concurrent create raced past the
    /// service-layer availability precheck) is `Error::InvalidParams` naming
    /// the id — the same `-32602` contract as the precheck — so the
    /// duplicate-id behavior stays robust under concurrency instead of
    /// degrading to an opaque `-32603`.
    pub async fn insert_agent_session(&self, s: &AgentSession) -> Result<()> {
        let sql = format!(
            "INSERT INTO agent_session ({SESSION_COLUMNS}) VALUES \
             (?,?,?,?,?,?,?,?,?,?,?,?,?,?,?,?,?,?,?,?,?,?,?,?,?,?,?,?,?)"
        );
        bind_session_insert(sqlx::query(&sql), s)?
            .execute(self.write_pool())
            .await
            .map_err(|e| {
                if e.as_database_error()
                    .is_some_and(|d| d.is_unique_violation())
                {
                    // Agent ids are server-minted (`agent-{uuid}`), so a
                    // UNIQUE(id) violation is a server-side anomaly, not a
                    // client params error.
                    Error::Internal(format!(
                        "server-minted agent id {} collided with an existing session",
                        s.id
                    ))
                } else {
                    Error::Internal(format!("insert agent session failed: {e}"))
                }
            })?;
        Ok(())
    }

    /// Insert an agent-session row together with its full message log in ONE
    /// write transaction: either the session and every message commit, or
    /// nothing does. Messages get minted UUIDv7 ids and 0-based monotonic
    /// `seq` values in slice order. Built for the legacy-transcript importer,
    /// whose idempotency check is session-id presence — a partially-persisted
    /// transcript would otherwise be skipped forever on re-runs. Uses
    /// whole-transaction retry to absorb SQLITE_BUSY (code 5) during lock
    /// upgrade (STAB-7).
    pub async fn insert_agent_session_with_messages(
        &self,
        s: &AgentSession,
        messages: &[ReplaceMessage<'_>],
    ) -> Result<()> {
        let pool = self.write_pool();
        // Clone messages into owned data for the retry closure.
        let owned_messages: Vec<(String, serde_json::Value, Option<serde_json::Value>, String)> =
            messages
                .iter()
                .map(|m| {
                    (
                        m.role.to_string(),
                        m.content.clone(),
                        m.metadata.cloned(),
                        m.created_at.to_string(),
                    )
                })
                .collect();

        crate::with_write_txn_retry(|| async {
            let mut tx = pool.begin().await.map_err(|e| {
                Error::Internal(format!("insert session with messages begin failed: {e}"))
            })?;
            let session_sql = format!(
                "INSERT INTO agent_session ({SESSION_COLUMNS}) VALUES \
                 (?,?,?,?,?,?,?,?,?,?,?,?,?,?,?,?,?,?,?,?,?,?,?,?,?,?,?,?,?)"
            );
            bind_session_insert(sqlx::query(&session_sql), s)?
                .execute(&mut *tx)
                .await
                .map_err(|e| Error::Internal(format!("insert agent session failed: {e}")))?;
            let insert_sql =
                format!("INSERT INTO agent_message ({MESSAGE_COLUMNS}) VALUES (?,?,?,?,?,?,?)");
            for (idx, (role, content, metadata, created_at)) in owned_messages.iter().enumerate() {
                let content_json = serde_json::to_string(content)
                    .map_err(|e| Error::Internal(format!("encode message content failed: {e}")))?;
                let metadata_json = match metadata {
                    Some(md) => Some(serde_json::to_string(md).map_err(|e| {
                        Error::Internal(format!("encode message metadata failed: {e}"))
                    })?),
                    None => None,
                };
                sqlx::query(&insert_sql)
                    .bind(Uuid::now_v7().to_string())
                    .bind(&s.id.0)
                    .bind(idx as i64)
                    .bind(role)
                    .bind(&content_json)
                    .bind(metadata_json.as_deref())
                    .bind(created_at)
                    .execute(&mut *tx)
                    .await
                    .map_err(|e| Error::Internal(format!("append agent message failed: {e}")))?;
            }
            tx.commit().await.map_err(|e| {
                Error::Internal(format!("insert session with messages commit failed: {e}"))
            })?;
            Ok(())
        })
        .await
    }

    /// Fetch a session by id (with its message log), or `NotFound`.
    pub async fn get_agent_session(&self, id: &AgentId) -> Result<AgentSession> {
        let sql = format!("SELECT {SESSION_COLUMNS} FROM agent_session WHERE id = ?");
        let row = sqlx::query(&sql)
            .bind(&id.0)
            .fetch_optional(self.read_pool())
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

    /// Lightweight status-only lookup used by hot paths that just need the
    /// session's lifecycle status (e.g. the STAB-52 queue-drain gate). Selects
    /// a single column and skips the full message-log fetch that
    /// `get_agent_session` performs. `NotFound` if the session is absent,
    /// matching `get_agent_session`.
    pub async fn get_agent_session_status(&self, id: &AgentId) -> Result<AgentStatus> {
        let row = sqlx::query("SELECT status FROM agent_session WHERE id = ?")
            .bind(&id.0)
            .fetch_optional(self.read_pool())
            .await
            .map_err(|e| Error::Internal(format!("get agent session status failed: {e}")))?;
        match row {
            Some(r) => enum_from_db::<AgentStatus>(&r.get::<String, _>("status")),
            None => Err(Error::NotFound(format!("agent session {id}"))),
        }
    }

    /// Lightweight name-only lookup used by hot paths that just need the
    /// session's display name (e.g. note-version author stamping). Skips the
    /// full message-log fetch that `get_agent_session` performs.
    pub async fn get_agent_session_name(&self, id: &AgentId) -> Result<Option<String>> {
        let row = sqlx::query("SELECT name FROM agent_session WHERE id = ?")
            .bind(&id.0)
            .fetch_optional(self.read_pool())
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
            .fetch_all(self.read_pool())
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

    /// List a workspace's sessions WITHOUT message logs, oldest first. Used by hot
    /// paths (`derive_last_activity`, `enrich_workspace_aggregates`) that only need
    /// session metadata (name, status, updated_at, etc.) and never read the message
    /// bodies (finding F1: eliminates full agent-message-log hydration from
    /// `workspace.list` / `workspace.get` emit). Reuses the `list_all_agent_sessions`
    /// row-mapping pattern (§9.1).
    pub async fn list_agent_session_summaries(
        &self,
        workspace_id: &WorkspaceId,
    ) -> Result<Vec<AgentSession>> {
        let sql = format!(
            "SELECT {SESSION_COLUMNS} FROM agent_session WHERE workspace_id = ? ORDER BY created_at"
        );
        let rows = sqlx::query(&sql)
            .bind(&workspace_id.0)
            .fetch_all(self.read_pool())
            .await
            .map_err(|e| Error::Internal(format!("list agent session summaries failed: {e}")))?;
        rows.iter().map(map_session_row).collect()
    }

    /// Get message count and whether any assistant message exists for each session in
    /// a workspace, without hydrating message bodies (finding F1/F3: lightweight
    /// alternative to full message-log fetch for `agent.diagnostics`). Returns a map
    /// keyed by agent_id with `(message_count, has_assistant)` tuples.
    pub async fn get_agent_session_message_stats(
        &self,
        workspace_id: &WorkspaceId,
    ) -> Result<std::collections::HashMap<String, (u64, bool)>> {
        // First, get all session IDs for this workspace
        let sql = "SELECT id FROM agent_session WHERE workspace_id = ?";
        let rows = sqlx::query(sql)
            .bind(&workspace_id.0)
            .fetch_all(self.read_pool())
            .await
            .map_err(|e| Error::Internal(format!("get agent session message stats failed: {e}")))?;

        let mut stats = std::collections::HashMap::new();

        for row in rows {
            let agent_id: String = row.get("id");

            // Count total messages
            let count_row =
                sqlx::query("SELECT COUNT(*) as count FROM agent_message WHERE agent_id = ?")
                    .bind(&agent_id)
                    .fetch_one(self.read_pool())
                    .await
                    .map_err(|e| Error::Internal(format!("count messages failed: {e}")))?;
            let message_count: i64 = count_row.get("count");

            // Check if any assistant message exists
            let assistant_row = sqlx::query(
                "SELECT EXISTS(SELECT 1 FROM agent_message WHERE agent_id = ? AND role = 'assistant') as has_assistant"
            )
            .bind(&agent_id)
            .fetch_one(self.read_pool())
            .await
            .map_err(|e| Error::Internal(format!("check assistant message failed: {e}")))?;
            let has_assistant: i64 = assistant_row.get("has_assistant");

            stats.insert(agent_id, (message_count as u64, has_assistant != 0));
        }

        Ok(stats)
    }

    /// Get the agent_message watermark for a workspace: the count of messages
    /// across all agents. This is used by the token-usage scan loop to skip
    /// workspaces that have not changed since the last scan (finding F2).
    /// Returns 0 for workspaces with no agents or no messages.
    pub async fn get_workspace_message_watermark(&self, workspace_id: &WorkspaceId) -> Result<u64> {
        let sql = r#"
            SELECT COUNT(*) as count
            FROM agent_message
            WHERE agent_id IN (
                SELECT id FROM agent_session WHERE workspace_id = ?
            )
        "#;
        let row = sqlx::query(sql)
            .bind(&workspace_id.0)
            .fetch_one(self.read_pool())
            .await
            .map_err(|e| Error::Internal(format!("get workspace message watermark failed: {e}")))?;
        let count: i64 = row.get("count");
        Ok(count as u64)
    }

    /// Upsert one session's cumulative end-of-turn token-usage snapshot
    /// (§5.23): the JSON-encoded [`TokenUsageTotals`] REPLACES any previous
    /// snapshot (ACP end-of-turn counts are cumulative per session, never
    /// summed). `NotFound` if the session row is absent.
    pub async fn set_agent_session_token_usage(
        &self,
        id: &AgentId,
        snapshot: &TokenUsageTotals,
    ) -> Result<()> {
        let json = serde_json::to_string(snapshot)
            .map_err(|e| Error::Internal(format!("encode session token_usage failed: {e}")))?;
        let res = sqlx::query("UPDATE agent_session SET token_usage=? WHERE id=?")
            .bind(json)
            .bind(&id.0)
            .execute(self.write_pool())
            .await
            .map_err(|e| Error::Internal(format!("set agent session token usage failed: {e}")))?;
        if res.rows_affected() == 0 {
            return Err(Error::NotFound(format!("agent session {id}")));
        }
        Ok(())
    }

    /// Get lightweight usage data for all agents in a workspace: for each agent,
    /// returns the agent_id, model, the persisted end-of-turn `token_usage`
    /// snapshot (if any), and all message content JSON (for tallying without
    /// full AgentSession hydration; finding F2). Used by the token-usage
    /// scan to avoid reading full message logs when only the usage metadata is needed.
    #[allow(clippy::type_complexity)]
    pub async fn get_workspace_agent_usage_data(
        &self,
        workspace_id: &WorkspaceId,
    ) -> Result<
        Vec<(
            String,
            Option<String>,
            Option<TokenUsageTotals>,
            Vec<serde_json::Value>,
        )>,
    > {
        // First get all sessions for this workspace with their models
        let session_sql = "SELECT id, model, token_usage FROM agent_session WHERE workspace_id = ?";
        let session_rows = sqlx::query(session_sql)
            .bind(&workspace_id.0)
            .fetch_all(self.read_pool())
            .await
            .map_err(|e| Error::Internal(format!("get agent sessions for usage failed: {e}")))?;

        let mut result = Vec::new();
        for session_row in session_rows {
            let agent_id: String = session_row.get("id");
            let model: Option<String> = session_row.get("model");
            // Best-effort decode (mirrors the content parse below): a malformed
            // snapshot degrades to None so the tally falls back to message sums.
            let snapshot: Option<TokenUsageTotals> = session_row
                .get::<Option<String>, _>("token_usage")
                .and_then(|s| serde_json::from_str(&s).ok());

            // Get all message content for this agent
            let message_sql =
                "SELECT content FROM agent_message WHERE agent_id = ? ORDER BY seq ASC";
            let message_rows = sqlx::query(message_sql)
                .bind(&agent_id)
                .fetch_all(self.read_pool())
                .await
                .map_err(|e| {
                    Error::Internal(format!("get agent messages for usage failed: {e}"))
                })?;

            let contents: Vec<serde_json::Value> = message_rows
                .iter()
                .map(|row| {
                    let content_str: String = row.get("content");
                    serde_json::from_str(&content_str).unwrap_or(serde_json::Value::Null)
                })
                .collect();

            result.push((agent_id, model, snapshot, contents));
        }
        Ok(result)
    }

    /// List every persisted session across workspaces, oldest first. Backs the
    /// daemon-startup stale-session heal: a session left non-terminal across a
    /// crash has no live worker after restart, so the heal sweeps the whole
    /// table once before serving. Sessions are returned WITHOUT their message
    /// logs (the heal does not need them) to keep the sweep O(rows).
    pub async fn list_all_agent_sessions(&self) -> Result<Vec<AgentSession>> {
        let sql = format!("SELECT {SESSION_COLUMNS} FROM agent_session ORDER BY created_at");
        let rows = sqlx::query(&sql)
            .fetch_all(self.read_pool())
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
    ///
    /// Provider immutability is enforced only after the first real use (once
    /// `acp_session_id` is set), allowing cross-provider model switches before
    /// the first turn.
    pub async fn update_agent_session(
        &self,
        workspace_id: &WorkspaceId,
        s: &AgentSession,
    ) -> Result<()> {
        // Lightweight invariant check: read only workspace_id, provider,
        // acp_session_id (finding F3: no message fetch). Workspace mismatch →
        // NotFound, provider immutable, acp_session_id write-once (§9.5).
        let row = sqlx::query(
            "SELECT workspace_id, provider, acp_session_id FROM agent_session WHERE id = ?",
        )
        .bind(&s.id.0)
        .fetch_optional(self.read_pool())
        .await
        .map_err(|e| Error::Internal(format!("update agent session invariant check failed: {e}")))?
        .ok_or_else(|| Error::NotFound(format!("agent session {}", s.id)))?;

        let current_workspace_id = WorkspaceId(row.get::<String, _>("workspace_id"));
        if current_workspace_id != *workspace_id {
            return Err(Error::NotFound(format!("agent session {}", s.id)));
        }
        let current_provider = row.get::<Option<String>, _>("provider");
        let current_acp_session_id = row.get::<Option<String>, _>("acp_session_id");
        // Provider is immutable only after first real use (once acp_session_id
        // is set). This allows cross-provider model switches before the first
        // turn spawns a provider process.
        if current_acp_session_id.is_some() && s.provider != current_provider {
            return Err(Error::Internal(
                "agent provider is immutable once set (first real use)".to_string(),
            ));
        }
        // Also reject provider changes in the same update that sets acp_session_id.
        if current_acp_session_id.is_none()
            && s.acp_session_id.is_some()
            && s.provider != current_provider
        {
            return Err(Error::Internal(
                "agent provider is immutable once set (first real use)".to_string(),
            ));
        }
        if current_acp_session_id.is_some() && s.acp_session_id != current_acp_session_id {
            return Err(Error::Internal("acpSessionId is write-once".to_string()));
        }
        let rows = sqlx::query(
            "UPDATE agent_session SET backend_session_id=?, acp_session_id=?, name=?, \
             name_explicitly_set=?, model=?, provider=?, status=?, is_active=?, system_prompt=?, \
             updated_at=?, parent_agent_id=?, specialist=?, task_note_id=?, skip_auto_commit=?, \
             completion_report=?, completion_report_timestamp=?, delegation_depth=?, \
             initial_message=?, context_references=?, image_blocks=?, is_background=?, \
             metadata=?, sandbox_id=?, sandbox_path=?, sandbox_branch=?, stop_reason=? \
             WHERE id=? AND workspace_id=?",
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
        .bind(&s.sandbox_id)
        .bind(&s.sandbox_path)
        .bind(&s.sandbox_branch)
        .bind(&s.stop_reason)
        .bind(&s.id.0)
        .bind(&workspace_id.0)
        .execute(self.write_pool())
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
    ///
    /// `stop_reason`: `None` leaves the column untouched; `Some(None)` clears it
    /// to NULL; `Some(Some(reason))` sets the new value. This three-way encoding
    /// allows callers to set, clear, or leave unchanged across a status update.
    pub async fn set_agent_session_status(
        &self,
        workspace_id: &WorkspaceId,
        id: &AgentId,
        status: AgentStatus,
        is_active: bool,
        updated_at: &str,
        stop_reason: Option<Option<String>>,
    ) -> Result<()> {
        let rows = match stop_reason {
            None => {
                // Leave stop_reason untouched.
                sqlx::query(
                    "UPDATE agent_session SET status=?, is_active=?, updated_at=? \
                     WHERE id=? AND workspace_id=?",
                )
                .bind(enum_to_db(&status)?)
                .bind(is_active as i64)
                .bind(updated_at)
                .bind(&id.0)
                .bind(&workspace_id.0)
                .execute(self.write_pool())
                .await
                .map_err(|e| Error::Internal(format!("set agent session status failed: {e}")))?
                .rows_affected()
            }
            Some(reason) => {
                // Set or clear stop_reason: Some(None) → NULL, Some(Some(x)) → x.
                sqlx::query(
                    "UPDATE agent_session SET status=?, is_active=?, updated_at=?, stop_reason=? \
                     WHERE id=? AND workspace_id=?",
                )
                .bind(enum_to_db(&status)?)
                .bind(is_active as i64)
                .bind(updated_at)
                .bind(reason)
                .bind(&id.0)
                .bind(&workspace_id.0)
                .execute(self.write_pool())
                .await
                .map_err(|e| Error::Internal(format!("set agent session status failed: {e}")))?
                .rows_affected()
            }
        };
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
                .execute(self.write_pool())
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

    /// Clear `completion_report` + `completion_report_timestamp` when a new turn
    /// begins for a delegated agent that previously called `report_to_parent`.
    /// Returns `true` if a report was present and cleared, `false` if no report
    /// was set (the common case — no write, no event). Scoped to `workspace_id`
    /// (defense-in-depth). `updated_at` is refreshed to the supplied timestamp.
    /// `NotFound` if the session is absent or the workspace does not match.
    pub async fn clear_completion_report(
        &self,
        workspace_id: &WorkspaceId,
        id: &AgentId,
        updated_at: &str,
    ) -> Result<bool> {
        // Conditional UPDATE: only modify rows where completion_report IS NOT NULL.
        // This avoids the expensive get_agent_session call (which loads the full
        // message log) at the start of every turn. rows_affected tells us whether
        // a report was present and cleared.
        let rows = sqlx::query(
            "UPDATE agent_session SET completion_report=NULL, \
             completion_report_timestamp=NULL, updated_at=? \
             WHERE id=? AND workspace_id=? AND completion_report IS NOT NULL",
        )
        .bind(updated_at)
        .bind(&id.0)
        .bind(&workspace_id.0)
        .execute(self.write_pool())
        .await
        .map_err(|e| Error::Internal(format!("clear completion report failed: {e}")))?
        .rows_affected();
        // rows_affected > 0 means a report was cleared; 0 means either no session
        // found, workspace mismatch, or no report was set. Distinguish the error
        // case (session not found / workspace mismatch) with a lightweight lookup.
        if rows == 0 {
            // Verify the session exists and workspace matches. Only SELECT id to
            // avoid loading the full message log.
            let exists = sqlx::query_scalar::<_, String>(
                "SELECT id FROM agent_session WHERE id=? AND workspace_id=?",
            )
            .bind(&id.0)
            .bind(&workspace_id.0)
            .fetch_optional(self.read_pool())
            .await
            .map_err(|e| Error::Internal(format!("verify agent session failed: {e}")))?;
            if exists.is_none() {
                return Err(Error::NotFound(format!("agent session {id}")));
            }
            // Session exists but no report was set — the common case.
            return Ok(false);
        }
        Ok(true)
    }

    /// Reset all `is_active=1` rows to `is_active=0` unconditionally (Wave B
    /// post-restart recovery). ACP sessions are process-local and cannot survive
    /// a daemon restart, so any `is_active=1` flag after boot is stale. Called
    /// early in startup (before listeners) to ensure no races with live turn
    /// spawns. Returns the count of rows reset.
    pub async fn reset_all_active_flags(&self) -> Result<usize> {
        let rows = sqlx::query("UPDATE agent_session SET is_active=0 WHERE is_active=1")
            .execute(self.write_pool())
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
            .execute(self.write_pool())
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
            .execute(self.write_pool())
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
            .execute(self.write_pool())
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
        stop_reason: col(row, "stop_reason")?,
        created_at: col(row, "created_at")?,
        updated_at: col(row, "updated_at")?,
        sandbox_id: col(row, "sandbox_id")?,
        sandbox_path: col(row, "sandbox_path")?,
        sandbox_branch: col(row, "sandbox_branch")?,
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
    /// **Crash safety**: Because `seq` is computed as `COALESCE(MAX(seq), -1) + 1`
    /// rather than a persisted counter, a crash between SELECT and INSERT does
    /// NOT create a durable gap — the next caller recomputes seq from the same
    /// MAX. Only the INSERT commits data, so once INSERT completes the message
    /// row is durable. No committed message can be lost. Assistant-message append
    /// (the streaming path) is additionally protected by the AgentManager's
    /// per-agent single-flight slot, serializing turns for one agent and
    /// eliminating the seq-race window on that hot path. User-message appends
    /// (sendMessage, forceMessage, wake delivery) can still race if fired
    /// concurrently for one agent, but the UNIQUE constraint will reject
    /// duplicates rather than silently corrupting the seq order.
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
        .fetch_one(self.read_pool())
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
            .execute(self.write_pool())
            .await
            .map_err(|e| Error::Internal(format!("append agent message failed: {e}")))?;
        Ok(AgentMessage {
            id: id.to_string(),
            agent_id: agent_id.clone(),
            seq,
            role: role.to_string(),
            content: content.clone(),
            app_message_id: intent_core::lift_app_message_id(metadata),
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
            .fetch_all(self.read_pool())
            .await
            .map_err(|e| Error::Internal(format!("get agent messages failed: {e}")))?;
        rows.iter().map(map_message_row).collect()
    }

    /// Total number of messages logged for an agent (`agent.getConversation`
    /// `totalMessages`).
    pub async fn count_agent_messages(&self, agent_id: &AgentId) -> Result<i64> {
        let n: i64 = sqlx::query("SELECT COUNT(*) AS n FROM agent_message WHERE agent_id = ?")
            .bind(&agent_id.0)
            .fetch_one(self.read_pool())
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
    /// Uses whole-transaction retry to eliminate SQLITE_BUSY (code 5) failures
    /// during lock upgrade under concurrent load (STAB-7).
    pub async fn replace_agent_messages(
        &self,
        agent_id: &AgentId,
        messages: &[ReplaceMessage<'_>],
    ) -> Result<Vec<AgentMessage>> {
        let pool = self.write_pool();
        let agent_id = agent_id.clone();
        // Clone messages into owned data for retry closure
        let owned_messages: Vec<(String, serde_json::Value, Option<serde_json::Value>, String)> =
            messages
                .iter()
                .map(|m| {
                    (
                        m.role.to_string(),
                        m.content.clone(),
                        m.metadata.cloned(),
                        m.created_at.to_string(),
                    )
                })
                .collect();

        crate::with_write_txn_retry(|| async {
            let mut tx = pool.begin().await.map_err(|e| {
                Error::Internal(format!("replace agent messages begin failed: {e}"))
            })?;
            sqlx::query("DELETE FROM agent_message WHERE agent_id = ?")
                .bind(&agent_id.0)
                .execute(&mut *tx)
                .await
                .map_err(|e| {
                    Error::Internal(format!("replace agent messages clear failed: {e}"))
                })?;
            let mut inserted = Vec::with_capacity(owned_messages.len());
            let insert_sql =
                format!("INSERT INTO agent_message ({MESSAGE_COLUMNS}) VALUES (?,?,?,?,?,?,?)");
            for (idx, (role, content, metadata, created_at)) in owned_messages.iter().enumerate() {
                let seq = idx as i64;
                let id = Uuid::now_v7().to_string();
                let content_json = serde_json::to_string(content).map_err(|e| {
                    Error::Internal(format!("encode replaced message content failed: {e}"))
                })?;
                let metadata_json = match metadata {
                    Some(md) => Some(serde_json::to_string(md).map_err(|e| {
                        Error::Internal(format!("encode replaced message metadata failed: {e}"))
                    })?),
                    None => None,
                };
                sqlx::query(&insert_sql)
                    .bind(&id)
                    .bind(&agent_id.0)
                    .bind(seq)
                    .bind(role)
                    .bind(&content_json)
                    .bind(metadata_json.as_deref())
                    .bind(created_at)
                    .execute(&mut *tx)
                    .await
                    .map_err(|e| {
                        Error::Internal(format!("replace agent messages insert failed: {e}"))
                    })?;
                inserted.push(AgentMessage {
                    id,
                    agent_id: agent_id.clone(),
                    seq,
                    role: role.clone(),
                    content: content.clone(),
                    app_message_id: intent_core::lift_app_message_id(metadata.as_ref()),
                    metadata: metadata.clone(),
                    created_at: created_at.clone(),
                });
            }
            tx.commit().await.map_err(|e| {
                Error::Internal(format!("replace agent messages commit failed: {e}"))
            })?;
            Ok(inserted)
        })
        .await
    }

    /// Record an interrupted in-flight agent. Upserts: if a pending row exists
    /// (daemon restarted before resumption), updates to the latest state. Returns
    /// `true` if inserted/updated.
    pub async fn insert_interrupted_agent(
        &self,
        agent_id: &AgentId,
        workspace_id: &WorkspaceId,
        prev_status: &str,
        interrupted_at: &str,
    ) -> Result<bool> {
        let sql =
            "INSERT INTO interrupted_agent (agent_id, workspace_id, prev_status, interrupted_at) \
                   VALUES (?, ?, ?, ?) \
                   ON CONFLICT(agent_id) DO UPDATE SET \
                       prev_status = excluded.prev_status, \
                       interrupted_at = excluded.interrupted_at, \
                       resolution = 'pending', \
                       resolved_at = NULL";
        let res = sqlx::query(sql)
            .bind(&agent_id.0)
            .bind(&workspace_id.0)
            .bind(prev_status)
            .bind(interrupted_at)
            .execute(self.write_pool())
            .await
            .map_err(|e| Error::Internal(format!("insert interrupted_agent failed: {e}")))?;
        Ok(res.rows_affected() > 0)
    }

    /// List pending interrupted agents, joined with agent_session (name) and
    /// workspace (title). Sessions deleted since interruption are excluded (INNER JOIN).
    pub async fn list_interrupted_agents(&self) -> Result<Vec<InterruptedAgent>> {
        let sql = "SELECT ia.agent_id, ia.workspace_id, ia.prev_status, ia.interrupted_at, \
                          ag.name AS agent_name, w.title AS workspace_name \
                   FROM interrupted_agent ia \
                   INNER JOIN agent_session ag ON ia.agent_id = ag.id \
                   LEFT JOIN workspace w ON ia.workspace_id = w.id \
                   WHERE ia.resolution = 'pending'";
        sqlx::query(sql)
            .fetch_all(self.read_pool())
            .await
            .map_err(|e| Error::Internal(format!("list interrupted_agent failed: {e}")))?
            .iter()
            .map(|row| {
                Ok(InterruptedAgent {
                    agent_id: AgentId(col(row, "agent_id")?),
                    workspace_id: WorkspaceId(col(row, "workspace_id")?),
                    prev_status: col(row, "prev_status")?,
                    interrupted_at: col(row, "interrupted_at")?,
                    agent_name: col(row, "agent_name")?,
                    workspace_name: col(row, "workspace_name")?,
                })
            })
            .collect()
    }

    /// Get a single pending interrupted agent by ID. Returns None if not found or not pending.
    pub async fn get_interrupted_agent(
        &self,
        agent_id: &AgentId,
    ) -> Result<Option<InterruptedAgent>> {
        let sql = "SELECT ia.agent_id, ia.workspace_id, ia.prev_status, ia.interrupted_at, \
                          ag.name AS agent_name, w.title AS workspace_name \
                   FROM interrupted_agent ia \
                   INNER JOIN agent_session ag ON ia.agent_id = ag.id \
                   LEFT JOIN workspace w ON ia.workspace_id = w.id \
                   WHERE ia.agent_id = ? AND ia.resolution = 'pending'";
        let row = sqlx::query(sql)
            .bind(&agent_id.0)
            .fetch_optional(self.read_pool())
            .await
            .map_err(|e| Error::Internal(format!("get interrupted_agent failed: {e}")))?;
        match row {
            None => Ok(None),
            Some(ref r) => Ok(Some(InterruptedAgent {
                agent_id: AgentId(col(r, "agent_id")?),
                workspace_id: WorkspaceId(col(r, "workspace_id")?),
                prev_status: col(r, "prev_status")?,
                interrupted_at: col(r, "interrupted_at")?,
                agent_name: col(r, "agent_name")?,
                workspace_name: col(r, "workspace_name")?,
            })),
        }
    }

    /// Set the resolution (resumed|abandoned) for an interrupted agent. Returns
    /// `true` if a pending row was updated, `false` if the agent was not found or
    /// already resolved (caller should fail the operation).
    pub async fn set_interrupted_resolution(
        &self,
        agent_id: &AgentId,
        resolution: &str,
        resolved_at: &str,
    ) -> Result<bool> {
        let sql = "UPDATE interrupted_agent SET resolution = ?, resolved_at = ? \
                   WHERE agent_id = ? AND resolution = 'pending'";
        let res = sqlx::query(sql)
            .bind(resolution)
            .bind(resolved_at)
            .bind(&agent_id.0)
            .execute(self.write_pool())
            .await
            .map_err(|e| Error::Internal(format!("set interrupted resolution failed: {e}")))?;
        Ok(res.rows_affected() > 0)
    }

    /// Reset an interrupted agent row back to pending (resolution=NULL, resolved_at=NULL).
    /// Used when a resume attempt claimed the row but failed post-claim, to restore
    /// retryability. Returns `true` if a row was updated.
    pub async fn reset_interrupted_resolution(&self, agent_id: &AgentId) -> Result<bool> {
        let sql = "UPDATE interrupted_agent SET resolution = 'pending', resolved_at = NULL \
                   WHERE agent_id = ?";
        let res = sqlx::query(sql)
            .bind(&agent_id.0)
            .execute(self.write_pool())
            .await
            .map_err(|e| Error::Internal(format!("reset interrupted resolution failed: {e}")))?;
        Ok(res.rows_affected() > 0)
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
        app_message_id: intent_core::lift_app_message_id(metadata.as_ref()),
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

#[cfg(test)]
mod tests {
    use super::*;
    use crate::Store;

    /// A UNIQUE violation on the session id maps to `Internal` naming the
    /// colliding id. Agent ids are server-minted (`agent-{uuid}`), so a
    /// duplicate insert is a server-side anomaly — never a client params
    /// error — and must not surface the retired "supply a fresh id" guidance.
    #[tokio::test]
    async fn insert_agent_session_duplicate_id_is_internal_error() {
        use intent_core::{
            now_iso, Workspace, WorkspaceActivity, WorkspaceAttention, WorkspaceStatus,
        };
        use std::path::PathBuf;
        use uuid::Uuid;
        let tmp = PathBuf::from("/tmp").join(format!("test-agent-repo-{}.db", Uuid::new_v4()));
        let store = Store::open(&tmp).await.expect("create test store");
        let ts = now_iso();
        let ws_id = WorkspaceId("ws-test".to_string());
        let workspace = Workspace {
            id: ws_id.clone(),
            title: "Test".to_string(),
            branch: "main".to_string(),
            base_ref: None,
            base_commit_sha: None,
            status: WorkspaceStatus::Active,
            status_message: None,
            activity: WorkspaceActivity::Idle,
            attention: WorkspaceAttention::None,
            created_at: ts.clone(),
            updated_at: ts.clone(),
            last_activity: None,
            tags: vec![],
            path: None,
            repository_path: None,
            repository_owner: None,
            repository_name: None,
            worktree_path: None,
            scope: None,
            skip_worktree: false,
            setup_script: None,
            is_remote: false,
            default_model: None,
            pr_number: None,
            pr_url: None,
            pr_status: None,
            active_pull_request: None,
            pull_requests: None,
            archived: false,
            archived_at: None,
            task_stats: None,
            agent_summary: None,
            diff_summary: None,
            token_usage: None,
            cow_supported: None,
        };
        store
            .insert_workspace(&workspace)
            .await
            .expect("insert workspace");
        let session = AgentSession {
            id: AgentId(format!("agent-{}", Uuid::new_v4())),
            workspace_id: ws_id,
            backend_session_id: None,
            acp_session_id: None,
            name: "First".to_string(),
            name_explicitly_set: false,
            model: None,
            provider: None,
            status: AgentStatus::Idle,
            is_active: false,
            system_prompt: None,
            created_at: ts.clone(),
            updated_at: ts,
            messages: vec![],
            parent_agent_id: None,
            specialist: None,
            task_note_id: None,
            skip_auto_commit: false,
            stats: None,
            completion_report: None,
            completion_report_timestamp: None,
            delegation_depth: None,
            initial_message: None,
            context_references: None,
            image_blocks: None,
            is_background: false,
            metadata: None,
            sandbox_id: None,
            sandbox_path: None,
            sandbox_branch: None,
            stop_reason: None,
        };
        store.insert_agent_session(&session).await.expect("insert");
        let err = store
            .insert_agent_session(&session)
            .await
            .expect_err("duplicate id must be rejected");
        match &err {
            Error::Internal(msg) => assert!(
                msg.contains(session.id.0.as_str()) && msg.contains("collided"),
                "error must name the colliding server-minted id, got: {msg}"
            ),
            other => panic!("expected Internal, got {other:?}"),
        }
        let _ = std::fs::remove_file(&tmp);
    }

    /// `set_agent_session_token_usage` replaces (never sums) the persisted
    /// snapshot, `get_workspace_agent_usage_data` surfaces it, and an unknown
    /// session id maps to `NotFound`.
    #[tokio::test]
    async fn token_usage_snapshot_roundtrip_and_replace() {
        use intent_core::{
            now_iso, Workspace, WorkspaceActivity, WorkspaceAttention, WorkspaceStatus,
        };
        use std::path::PathBuf;
        use uuid::Uuid;
        let tmp = PathBuf::from("/tmp").join(format!("test-agent-repo-{}.db", Uuid::new_v4()));
        let store = Store::open(&tmp).await.expect("create test store");
        let ts = now_iso();
        let ws_id = WorkspaceId("ws-test".to_string());
        let workspace = Workspace {
            id: ws_id.clone(),
            title: "Test".to_string(),
            branch: "main".to_string(),
            base_ref: None,
            base_commit_sha: None,
            status: WorkspaceStatus::Active,
            status_message: None,
            activity: WorkspaceActivity::Idle,
            attention: WorkspaceAttention::None,
            created_at: ts.clone(),
            updated_at: ts.clone(),
            last_activity: None,
            tags: vec![],
            path: None,
            repository_path: None,
            repository_owner: None,
            repository_name: None,
            worktree_path: None,
            scope: None,
            skip_worktree: false,
            setup_script: None,
            is_remote: false,
            default_model: None,
            pr_number: None,
            pr_url: None,
            pr_status: None,
            active_pull_request: None,
            pull_requests: None,
            archived: false,
            archived_at: None,
            task_stats: None,
            agent_summary: None,
            diff_summary: None,
            token_usage: None,
            cow_supported: None,
        };
        store
            .insert_workspace(&workspace)
            .await
            .expect("insert workspace");
        let agent_id = AgentId(format!("agent-{}", Uuid::new_v4()));
        let session = AgentSession {
            id: agent_id.clone(),
            workspace_id: ws_id.clone(),
            backend_session_id: None,
            acp_session_id: None,
            name: "Usage".to_string(),
            name_explicitly_set: false,
            model: Some("opus-4.8".to_string()),
            provider: None,
            status: AgentStatus::Idle,
            is_active: false,
            system_prompt: None,
            created_at: ts.clone(),
            updated_at: ts,
            messages: vec![],
            parent_agent_id: None,
            specialist: None,
            task_note_id: None,
            skip_auto_commit: false,
            stats: None,
            completion_report: None,
            completion_report_timestamp: None,
            delegation_depth: None,
            initial_message: None,
            context_references: None,
            image_blocks: None,
            is_background: false,
            metadata: None,
            sandbox_id: None,
            sandbox_path: None,
            sandbox_branch: None,
            stop_reason: None,
        };
        store.insert_agent_session(&session).await.expect("insert");

        // No snapshot yet.
        let rows = store
            .get_workspace_agent_usage_data(&ws_id)
            .await
            .expect("usage data");
        assert_eq!(rows.len(), 1);
        assert!(rows[0].2.is_none(), "no snapshot before first set");

        // First snapshot, then a REPLACING second snapshot.
        let first = TokenUsageTotals {
            input_tokens: 70,
            output_tokens: 50,
            cache_read_tokens: 30,
            cache_creation_tokens: 4,
        };
        store
            .set_agent_session_token_usage(&agent_id, &first)
            .await
            .expect("set first");
        let second = TokenUsageTotals {
            input_tokens: 100,
            output_tokens: 80,
            cache_read_tokens: 45,
            cache_creation_tokens: 6,
        };
        store
            .set_agent_session_token_usage(&agent_id, &second)
            .await
            .expect("set second");
        let rows = store
            .get_workspace_agent_usage_data(&ws_id)
            .await
            .expect("usage data after set");
        assert_eq!(
            rows[0].2.as_ref(),
            Some(&second),
            "latest snapshot replaces the previous one"
        );

        // Unknown session → NotFound.
        let missing = AgentId("agent-missing".to_string());
        let err = store
            .set_agent_session_token_usage(&missing, &first)
            .await
            .expect_err("unknown session rejected");
        assert!(matches!(err, Error::NotFound(_)), "got {err:?}");

        let _ = std::fs::remove_file(&tmp);
    }

    #[tokio::test]
    async fn re_interruption_resets_resolution() {
        use intent_core::{
            now_iso, Workspace, WorkspaceActivity, WorkspaceAttention, WorkspaceStatus,
        };
        use std::path::PathBuf;
        use uuid::Uuid;
        let tmp = PathBuf::from("/tmp").join(format!("test-agent-repo-{}.db", Uuid::new_v4()));
        let store = Store::open(&tmp).await.expect("create test store");
        let agent_id = AgentId("agent-test".to_string());
        let ws_id = WorkspaceId("ws-test".to_string());

        // Seed workspace
        let ts = now_iso();
        let workspace = Workspace {
            id: ws_id.clone(),
            title: "Test".to_string(),
            branch: "main".to_string(),
            base_ref: None,
            base_commit_sha: None,
            status: WorkspaceStatus::Active,
            status_message: None,
            activity: WorkspaceActivity::Idle,
            attention: WorkspaceAttention::None,
            created_at: ts.clone(),
            updated_at: ts,
            last_activity: None,
            tags: vec![],
            path: None,
            repository_path: None,
            repository_owner: None,
            repository_name: None,
            worktree_path: None,
            scope: None,
            skip_worktree: false,
            setup_script: None,
            is_remote: false,
            default_model: None,
            pr_number: None,
            pr_url: None,
            pr_status: None,
            active_pull_request: None,
            pull_requests: None,
            archived: false,
            archived_at: None,
            task_stats: None,
            agent_summary: None,
            diff_summary: None,
            token_usage: None,
            cow_supported: None,
        };
        store
            .insert_workspace(&workspace)
            .await
            .expect("insert workspace");

        // Initial interruption
        store
            .insert_interrupted_agent(&agent_id, &ws_id, "active", "2026-01-01T00:00:00Z")
            .await
            .expect("initial insert");

        // Verify the row exists (raw SQL check since get_interrupted_agent requires agent_session)
        let count: i64 = sqlx::query_scalar(
            "SELECT COUNT(*) FROM interrupted_agent WHERE agent_id = ? AND resolution = 'pending'",
        )
        .bind(&agent_id.0)
        .fetch_one(store.read_pool())
        .await
        .expect("count");
        assert_eq!(count, 1, "should have one pending row");

        // Resolve it (resumed)
        let updated = store
            .set_interrupted_resolution(&agent_id, "resumed", "2026-01-01T00:01:00Z")
            .await
            .expect("resolve");
        assert!(updated, "should update pending row");

        // Verify no longer pending
        let count2: i64 = sqlx::query_scalar(
            "SELECT COUNT(*) FROM interrupted_agent WHERE agent_id = ? AND resolution = 'pending'",
        )
        .bind(&agent_id.0)
        .fetch_one(store.read_pool())
        .await
        .expect("count after resolve");
        assert_eq!(count2, 0, "resolved row should not be pending");

        // Re-interrupt (daemon crash again, same agent)
        store
            .insert_interrupted_agent(&agent_id, &ws_id, "processing", "2026-01-01T00:02:00Z")
            .await
            .expect("re-interrupt");

        // Verify row is pending again (resolution reset)
        let count3: i64 = sqlx::query_scalar(
            "SELECT COUNT(*) FROM interrupted_agent WHERE agent_id = ? AND resolution = 'pending'",
        )
        .bind(&agent_id.0)
        .fetch_one(store.read_pool())
        .await
        .expect("count after re-interrupt");
        assert_eq!(count3, 1, "re-interrupted row should be pending again");

        // Verify updated fields
        let row: (String, String) = sqlx::query_as(
            "SELECT prev_status, interrupted_at FROM interrupted_agent WHERE agent_id = ?",
        )
        .bind(&agent_id.0)
        .fetch_one(store.read_pool())
        .await
        .expect("fetch row");
        assert_eq!(row.0, "processing");
        assert_eq!(row.1, "2026-01-01T00:02:00Z");

        // Attempt to resolve a non-existent agent
        let unknown_id = AgentId("agent-unknown".to_string());
        let updated2 = store
            .set_interrupted_resolution(&unknown_id, "resumed", "2026-01-01T00:03:00Z")
            .await
            .expect("resolve unknown");
        assert!(!updated2, "resolving unknown agent should return false");

        let _ = std::fs::remove_file(&tmp);
    }

    #[tokio::test]
    async fn double_claim_race() {
        use intent_core::{
            now_iso, Workspace, WorkspaceActivity, WorkspaceAttention, WorkspaceStatus,
        };
        use std::path::PathBuf;
        use uuid::Uuid;
        let tmp = PathBuf::from("/tmp").join(format!("test-agent-repo-{}.db", Uuid::new_v4()));
        let store = Store::open(&tmp).await.expect("create test store");
        let agent_id = AgentId("agent-double".to_string());
        let ws_id = WorkspaceId("ws-double".to_string());

        // Seed workspace
        let ts = now_iso();
        let workspace = Workspace {
            id: ws_id.clone(),
            title: "Test".to_string(),
            branch: "main".to_string(),
            base_ref: None,
            base_commit_sha: None,
            status: WorkspaceStatus::Active,
            status_message: None,
            activity: WorkspaceActivity::Idle,
            attention: WorkspaceAttention::None,
            created_at: ts.clone(),
            updated_at: ts,
            last_activity: None,
            tags: vec![],
            path: None,
            repository_path: None,
            repository_owner: None,
            repository_name: None,
            worktree_path: None,
            scope: None,
            skip_worktree: false,
            setup_script: None,
            is_remote: false,
            default_model: None,
            pr_number: None,
            pr_url: None,
            pr_status: None,
            active_pull_request: None,
            pull_requests: None,
            archived: false,
            archived_at: None,
            task_stats: None,
            agent_summary: None,
            diff_summary: None,
            token_usage: None,
            cow_supported: None,
        };
        store
            .insert_workspace(&workspace)
            .await
            .expect("insert workspace");

        // Interrupt the agent
        store
            .insert_interrupted_agent(&agent_id, &ws_id, "active", "2026-01-01T00:00:00Z")
            .await
            .expect("initial insert");

        // First claim succeeds
        let claim1 = store
            .set_interrupted_resolution(&agent_id, "resumed", "2026-01-01T00:01:00Z")
            .await
            .expect("first claim");
        assert!(claim1, "first claim should succeed");

        // Second concurrent claim fails (row already resolved)
        let claim2 = store
            .set_interrupted_resolution(&agent_id, "resumed", "2026-01-01T00:01:01Z")
            .await
            .expect("second claim");
        assert!(!claim2, "second claim should fail (already resolved)");

        let _ = std::fs::remove_file(&tmp);
    }

    #[tokio::test]
    async fn list_agent_session_summaries_excludes_messages() {
        use intent_core::{
            now_iso, AgentSession, AgentStatus, Workspace, WorkspaceActivity, WorkspaceAttention,
            WorkspaceStatus,
        };
        use std::path::PathBuf;
        use uuid::Uuid;

        let tmp = PathBuf::from("/tmp").join(format!("test-summaries-{}.db", Uuid::new_v4()));
        let store = Store::open(&tmp).await.expect("create test store");
        let ws_id = WorkspaceId("ws-summaries".to_string());

        // Seed workspace
        let ts = now_iso();
        let workspace = Workspace {
            id: ws_id.clone(),
            title: "Test".to_string(),
            branch: "main".to_string(),
            base_ref: None,
            base_commit_sha: None,
            status: WorkspaceStatus::Active,
            status_message: None,
            activity: WorkspaceActivity::Idle,
            attention: WorkspaceAttention::None,
            created_at: ts.clone(),
            updated_at: ts.clone(),
            last_activity: None,
            tags: vec![],
            path: None,
            repository_path: None,
            repository_owner: None,
            repository_name: None,
            worktree_path: None,
            scope: None,
            skip_worktree: false,
            setup_script: None,
            is_remote: false,
            default_model: None,
            pr_number: None,
            pr_url: None,
            pr_status: None,
            active_pull_request: None,
            pull_requests: None,
            archived: false,
            archived_at: None,
            task_stats: None,
            agent_summary: None,
            diff_summary: None,
            token_usage: None,
            cow_supported: None,
        };
        store
            .insert_workspace(&workspace)
            .await
            .expect("insert workspace");

        // Insert agent session
        let agent_id = AgentId("agent-summary-test".to_string());
        let session = AgentSession {
            id: agent_id.clone(),
            workspace_id: ws_id.clone(),
            backend_session_id: None,
            acp_session_id: None,
            name: "Test Agent".to_string(),
            name_explicitly_set: false,
            model: None,
            provider: None,
            status: AgentStatus::Idle,
            is_active: false,
            system_prompt: None,
            created_at: ts.clone(),
            updated_at: ts.clone(),
            messages: vec![],
            parent_agent_id: None,
            specialist: None,
            task_note_id: None,
            skip_auto_commit: false,
            stats: None,
            completion_report: None,
            completion_report_timestamp: None,
            delegation_depth: None,
            initial_message: None,
            context_references: None,
            image_blocks: None,
            is_background: false,
            metadata: None,
            sandbox_id: None,
            sandbox_path: None,
            sandbox_branch: None,
            stop_reason: None,
        };
        store
            .insert_agent_session(&session)
            .await
            .expect("insert session");

        // Insert message
        store
            .append_agent_message(
                &agent_id,
                "user",
                &serde_json::json!([{"type": "text", "text": "hello"}]),
                &ts,
            )
            .await
            .expect("append message");

        // list_agent_sessions should include messages
        let full = store
            .list_agent_sessions(&ws_id)
            .await
            .expect("list_agent_sessions");
        assert_eq!(full.len(), 1, "should have one session");
        assert_eq!(
            full[0].messages.len(),
            1,
            "list_agent_sessions should include messages"
        );

        // list_agent_session_summaries should exclude messages
        let summaries = store
            .list_agent_session_summaries(&ws_id)
            .await
            .expect("list_agent_session_summaries");
        assert_eq!(summaries.len(), 1, "should have one session");
        assert_eq!(
            summaries[0].messages.len(),
            0,
            "summaries should exclude messages"
        );
        assert_eq!(summaries[0].id, agent_id, "id should match");
        assert_eq!(summaries[0].name, "Test Agent", "name should match");

        let _ = std::fs::remove_file(&tmp);
    }

    #[tokio::test]
    async fn update_agent_session_invariants_without_messages() {
        use intent_core::{
            now_iso, AgentSession, AgentStatus, Workspace, WorkspaceActivity, WorkspaceAttention,
            WorkspaceStatus,
        };
        use std::path::PathBuf;
        use uuid::Uuid;

        let tmp = PathBuf::from("/tmp").join(format!("test-update-inv-{}.db", Uuid::new_v4()));
        let store = Store::open(&tmp).await.expect("create test store");
        let ws_id = WorkspaceId("ws-update-inv".to_string());
        let wrong_ws_id = WorkspaceId("ws-wrong".to_string());

        // Seed workspaces
        let ts = now_iso();
        for id in [&ws_id, &wrong_ws_id] {
            let workspace = Workspace {
                id: id.clone(),
                title: "Test".to_string(),
                branch: "main".to_string(),
                base_ref: None,
                base_commit_sha: None,
                status: WorkspaceStatus::Active,
                status_message: None,
                activity: WorkspaceActivity::Idle,
                attention: WorkspaceAttention::None,
                created_at: ts.clone(),
                updated_at: ts.clone(),
                last_activity: None,
                tags: vec![],
                path: None,
                repository_path: None,
                repository_owner: None,
                repository_name: None,
                worktree_path: None,
                scope: None,
                skip_worktree: false,
                setup_script: None,
                is_remote: false,
                default_model: None,
                pr_number: None,
                pr_url: None,
                pr_status: None,
                active_pull_request: None,
                pull_requests: None,
                archived: false,
                archived_at: None,
                task_stats: None,
                agent_summary: None,
                diff_summary: None,
                token_usage: None,
                cow_supported: None,
            };
            store.insert_workspace(&workspace).await.expect("insert");
        }

        // Insert agent session with provider and acp_session_id
        let agent_id = AgentId("agent-inv-test".to_string());
        let mut session = AgentSession {
            id: agent_id.clone(),
            workspace_id: ws_id.clone(),
            backend_session_id: None,
            acp_session_id: Some("acp-123".to_string()),
            name: "Test Agent".to_string(),
            name_explicitly_set: false,
            model: None,
            provider: Some("auggie".to_string()),
            status: AgentStatus::Idle,
            is_active: false,
            system_prompt: None,
            created_at: ts.clone(),
            updated_at: ts.clone(),
            messages: vec![],
            parent_agent_id: None,
            specialist: None,
            task_note_id: None,
            skip_auto_commit: false,
            stats: None,
            completion_report: None,
            completion_report_timestamp: None,
            delegation_depth: None,
            initial_message: None,
            context_references: None,
            image_blocks: None,
            is_background: false,
            metadata: None,
            sandbox_id: None,
            sandbox_path: None,
            sandbox_branch: None,
            stop_reason: None,
        };
        store.insert_agent_session(&session).await.expect("insert");

        // Insert messages (to verify invariant check doesn't fetch them)
        for _ in 0..10 {
            store
                .append_agent_message(
                    &agent_id,
                    "user",
                    &serde_json::json!([{"type": "text", "text": "msg"}]),
                    &ts,
                )
                .await
                .expect("append");
        }

        // Test workspace mismatch → NotFound
        session.name = "Updated".to_string();
        let result = store.update_agent_session(&wrong_ws_id, &session).await;
        assert!(result.is_err(), "workspace mismatch should fail");
        assert!(
            matches!(result, Err(Error::NotFound(_))),
            "should be NotFound"
        );

        // Test provider immutability
        session.provider = Some("different".to_string());
        let result2 = store.update_agent_session(&ws_id, &session).await;
        assert!(result2.is_err(), "provider change should fail");
        assert!(
            matches!(result2, Err(Error::Internal(_))),
            "should be Internal"
        );

        // Test acp_session_id write-once
        session.provider = Some("auggie".to_string()); // restore
        session.acp_session_id = Some("different-acp".to_string());
        let result3 = store.update_agent_session(&ws_id, &session).await;
        assert!(result3.is_err(), "acp_session_id change should fail");
        assert!(
            matches!(result3, Err(Error::Internal(_))),
            "should be Internal"
        );

        // Test successful update (name change OK)
        session.acp_session_id = Some("acp-123".to_string()); // restore
        session.name = "New Name".to_string();
        let result4 = store.update_agent_session(&ws_id, &session).await;
        assert!(result4.is_ok(), "name change should succeed");

        let _ = std::fs::remove_file(&tmp);
    }

    #[tokio::test]
    async fn get_agent_session_message_stats() {
        use intent_core::{
            now_iso, AgentSession, AgentStatus, Workspace, WorkspaceActivity, WorkspaceAttention,
            WorkspaceStatus,
        };
        use std::path::PathBuf;
        use uuid::Uuid;

        let tmp = PathBuf::from("/tmp").join(format!("test-msg-stats-{}.db", Uuid::new_v4()));
        let store = Store::open(&tmp).await.expect("create test store");
        let ws_id = WorkspaceId("ws-stats".to_string());

        // Seed workspace
        let ts = now_iso();
        let workspace = Workspace {
            id: ws_id.clone(),
            title: "Test".to_string(),
            branch: "main".to_string(),
            base_ref: None,
            base_commit_sha: None,
            status: WorkspaceStatus::Active,
            status_message: None,
            activity: WorkspaceActivity::Idle,
            attention: WorkspaceAttention::None,
            created_at: ts.clone(),
            updated_at: ts.clone(),
            last_activity: None,
            tags: vec![],
            path: None,
            repository_path: None,
            repository_owner: None,
            repository_name: None,
            worktree_path: None,
            scope: None,
            skip_worktree: false,
            setup_script: None,
            is_remote: false,
            default_model: None,
            pr_number: None,
            pr_url: None,
            pr_status: None,
            active_pull_request: None,
            pull_requests: None,
            archived: false,
            archived_at: None,
            task_stats: None,
            agent_summary: None,
            diff_summary: None,
            token_usage: None,
            cow_supported: None,
        };
        store.insert_workspace(&workspace).await.expect("insert");

        // Create two agents
        let agent1 = AgentId("agent-stats-1".to_string());
        let agent2 = AgentId("agent-stats-2".to_string());

        for agent_id in [&agent1, &agent2] {
            let session = AgentSession {
                id: agent_id.clone(),
                workspace_id: ws_id.clone(),
                backend_session_id: None,
                acp_session_id: None,
                name: format!("Agent {}", agent_id.0),
                name_explicitly_set: false,
                model: None,
                provider: None,
                status: AgentStatus::Idle,
                is_active: false,
                system_prompt: None,
                created_at: ts.clone(),
                updated_at: ts.clone(),
                messages: vec![],
                parent_agent_id: None,
                specialist: None,
                task_note_id: None,
                skip_auto_commit: false,
                stats: None,
                completion_report: None,
                completion_report_timestamp: None,
                delegation_depth: None,
                initial_message: None,
                context_references: None,
                image_blocks: None,
                is_background: false,
                metadata: None,
                sandbox_id: None,
                sandbox_path: None,
                sandbox_branch: None,
                stop_reason: None,
            };
            store.insert_agent_session(&session).await.expect("insert");
        }

        // agent1: 1 user message only (no assistant)
        store
            .append_agent_message(
                &agent1,
                "user",
                &serde_json::json!([{"type": "text", "text": "hello"}]),
                &ts,
            )
            .await
            .expect("append user");

        // agent2: 3 messages (user, assistant, user)
        for (role, text) in [("user", "q1"), ("assistant", "a1"), ("user", "q2")] {
            store
                .append_agent_message(
                    &agent2,
                    role,
                    &serde_json::json!([{"type": "text", "text": text}]),
                    &ts,
                )
                .await
                .expect("append");
        }

        // Get stats
        let stats = store
            .get_agent_session_message_stats(&ws_id)
            .await
            .expect("get_agent_session_message_stats");

        assert_eq!(stats.len(), 2, "should have stats for both agents");

        let (count1, has_assistant1) = stats.get(&agent1.0).expect("agent1 stats");
        assert_eq!(*count1, 1, "agent1 should have 1 message");
        assert!(!has_assistant1, "agent1 should have no assistant message");

        let (count2, has_assistant2) = stats.get(&agent2.0).expect("agent2 stats");
        assert_eq!(*count2, 3, "agent2 should have 3 messages");
        assert!(has_assistant2, "agent2 should have assistant message");

        let _ = std::fs::remove_file(&tmp);
    }
}
