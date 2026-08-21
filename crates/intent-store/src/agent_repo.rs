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
    UsageCost, WorkspaceId,
};
use sqlx::sqlite::SqliteRow;
use sqlx::Row;
use uuid::Uuid;

use crate::{enum_from_db, enum_to_db, Store};

const SESSION_COLUMNS: &str = "id, workspace_id, backend_session_id, acp_session_id, name, \
    name_explicitly_set, model, provider, status, is_active, system_prompt, created_at, updated_at, \
    parent_agent_id, specialist, task_note_id, skip_auto_commit, completion_report, \
    completion_report_timestamp, attention_request_kind, attention_request_reason, \
    attention_request_timestamp, delegation_depth, initial_message, context_references, image_blocks, \
    file_blocks, is_background, metadata, sandbox_id, sandbox_path, sandbox_branch, stop_reason, \
    stop_reason_timestamp, reasoning_effort, effort_levels, task_graph_enabled, harness_version, \
    harness_features";

/// Session metadata needed by the `AgentLite` summary projection.
/// `system_prompt` and `image_blocks` are intentionally omitted:
/// `AgentLite::from_session` strips them from the wire, and loading them made
/// `agent.list` scale with the stored prompt/base64-image bytes.
const SESSION_SUMMARY_COLUMNS: &str = "id, workspace_id, backend_session_id, acp_session_id, name, \
    name_explicitly_set, model, provider, status, is_active, created_at, updated_at, parent_agent_id, \
    specialist, task_note_id, skip_auto_commit, completion_report, completion_report_timestamp, \
    attention_request_kind, attention_request_reason, attention_request_timestamp, delegation_depth, \
    initial_message, context_references, file_blocks, is_background, metadata, sandbox_id, \
    sandbox_path, sandbox_branch, stop_reason, stop_reason_timestamp, reasoning_effort, \
    effort_levels, harness_version, harness_features";

/// One agent session's usage inputs for the workspace token-usage tally
/// (§5.23): `(agent_id, model, snapshot, baseline, message_usage)`.
/// `message_usage` is non-empty only for sessions whose decoded snapshot and
/// baseline carry no token report (the per-message fallback path — see
/// [`Store::get_workspace_agent_usage_data`]), and carries only the usage
/// metadata of usage-bearing messages — never message bodies.
pub(crate) type AgentUsageRow = (
    String,
    Option<String>,
    Option<TokenUsageTotals>,
    Option<TokenUsageTotals>,
    Vec<serde_json::Value>,
);

/// SQL scalar expression projecting an `agent_message` row's usage object into
/// the `{"usage": {...}}` shape the tally's per-message fallback consumes.
/// Mirrors intent-services' `extract_message_usage` precedence: top-level
/// `usage` whenever that key is *present*, else `_meta.usage` — `json_type` is
/// SQL-NULL only for an absent path, not for a JSON `null`, so an explicit
/// `"usage": null` shadows `_meta.usage` here exactly as it does in Rust.
/// Only evaluated on rows passing [`MESSAGE_USAGE_PRESENT_SQL`], which
/// guarantees the selected value is a JSON object.
const MESSAGE_USAGE_JSON_SQL: &str = "json_object('usage', \
    CASE WHEN json_type(content, '$.usage') IS NOT NULL \
        THEN content -> '$.usage' ELSE content -> '$._meta.usage' END)";

/// Row filter pairing with [`MESSAGE_USAGE_JSON_SQL`]: keeps only messages
/// whose selected usage value is a JSON **object**, so the fallback read
/// materializes usage-bearing messages rather than whole transcripts. Dropping
/// non-object values is tally-preserving — `extract_message_usage`'s
/// `usage.get(key)` yields nothing on a non-object, contributing all-zero
/// counters — and it keeps `->` from ever selecting a value the projection
/// cannot represent. `_meta` is ACP provider passthrough, so a non-object
/// `usage` there is reachable even though `content` itself is always valid
/// JSON from the store's serde-encoded write paths; the `json_valid` guard
/// covers pre-existing rows that are not.
///
/// Must stay equivalent to the `WHERE` clause of the partial index
/// `idx_agent_message_usage` (migration 0081) — SQLite only satisfies the
/// filter from that index when the predicates match, and without the index
/// the filter would load and JSON-parse every message body in the session.
const MESSAGE_USAGE_PRESENT_SQL: &str = "CASE WHEN json_valid(content) THEN \
    (CASE WHEN json_type(content, '$.usage') IS NOT NULL \
        THEN json_type(content, '$.usage') = 'object' \
        ELSE json_type(content, '$._meta.usage') = 'object' END) ELSE 0 END";

/// Read the per-session usage rows for one workspace over an explicit
/// connection, so [`Store::get_workspace_agent_usage_data`] (read pool) and
/// the transactional recompute in `workspace_repo.rs` (write transaction,
/// monorepo#738) share one implementation. Per-message usage is read only when
/// neither the decoded snapshot nor the baseline carries a token report
/// ([`intent_core::token_usage_reported`], which also treats the all-zero
/// counters of a cost-only persist as "no report", keeping this in lockstep
/// with `agent_token_tally`'s fallback rule); a malformed snapshot/baseline
/// decodes to `None` and stays on the fallback too.
///
/// That fallback read is bounded (monorepo#1571): SQLite projects each
/// message's usage object ([`MESSAGE_USAGE_JSON_SQL`]) and drops rows carrying
/// none ([`MESSAGE_USAGE_PRESENT_SQL`], satisfied from the partial index
/// `idx_agent_message_usage`), so the bytes crossing the store boundary — and
/// the rows serde parses on this side — are O(usage-bearing messages) rather
/// than O(transcript); no message body is materialized. A presence-only
/// hydration guard (`snapshot.is_none() && baseline.is_none()`) would instead
/// have zeroed a cost-only session's counters, since such a session is still
/// on the tally's per-message fallback and needs its per-message usage.
pub(crate) async fn fetch_agent_usage_rows(
    conn: &mut sqlx::SqliteConnection,
    workspace_id: &WorkspaceId,
) -> Result<Vec<AgentUsageRow>> {
    let session_sql = "SELECT id, model, token_usage, token_usage_baseline \
        FROM agent_session WHERE workspace_id = ?";
    let session_rows = sqlx::query(session_sql)
        .bind(&workspace_id.0)
        .fetch_all(&mut *conn)
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
        // Same best-effort decode for the recreate baseline (monorepo#737).
        let baseline: Option<TokenUsageTotals> = session_row
            .get::<Option<String>, _>("token_usage_baseline")
            .and_then(|s| serde_json::from_str(&s).ok());

        // Per-message usage feeds the tally only when neither snapshot nor
        // baseline carries a token report (`agent_token_tally`'s fallback
        // rule), so the per-session message read is skipped otherwise
        // (monorepo#738) — and when it does run it projects usage metadata in
        // SQL rather than message bodies (monorepo#1571).
        let contents: Vec<serde_json::Value> =
            if !intent_core::token_usage_reported(baseline.as_ref(), snapshot.as_ref()) {
                let message_sql = format!(
                    "SELECT {MESSAGE_USAGE_JSON_SQL} AS usage_json FROM agent_message \
                     WHERE agent_id = ? AND {MESSAGE_USAGE_PRESENT_SQL} ORDER BY seq ASC"
                );
                let message_rows = sqlx::query(&message_sql)
                    .bind(&agent_id)
                    .fetch_all(&mut *conn)
                    .await
                    .map_err(|e| {
                        Error::Internal(format!("get agent messages for usage failed: {e}"))
                    })?;
                message_rows
                    .iter()
                    .filter_map(|row| {
                        let usage_json: Option<String> = row.get("usage_json");
                        usage_json.and_then(|s| serde_json::from_str(&s).ok())
                    })
                    .collect()
            } else {
                Vec::new()
            };

        result.push((agent_id, model, snapshot, baseline, contents));
    }
    Ok(result)
}

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
    /// Machine-readable interruption reason (the `InterruptReason` wire string,
    /// e.g. `system_suspend`), or `None` for rows enrolled without one (the
    /// daemon-restart / heal paths). The wake-resume orchestrator resumes ONLY
    /// `system_suspend` rows.
    pub reason: Option<String>,
}

/// Per-session message inputs for the `AgentLite` projection (monorepo#958):
/// everything the projection needs without hydrating the full transcript.
/// The last-rows fields carry only the capped `text`-block strings of each
/// session's highest-`seq` `user` / `assistant` row, served from the
/// persisted `agent_session.last_assistant_preview` / `last_user_preview`
/// columns maintained at message-write time (0066) — message bodies and
/// `metadata` are never fetched or decoded on the read path — and are `None`
/// when the session has no such message, or when the persisted column is
/// NULL/corrupt (degrade-only: no repair is attempted; the column converges
/// naturally the next time a message is appended). An existing message with
/// no text blocks yields `Some(vec![])`. Returned by
/// [`Store::get_agent_session_message_projections`] (workspace-wide) and
/// [`Store::get_agent_session_message_projection`] (single session).
#[derive(Debug, Clone, Default, PartialEq, Eq)]
pub struct SessionMessageProjection {
    pub message_count: u64,
    pub last_assistant_text_blocks: Option<Vec<String>>,
    pub last_user_text_blocks: Option<Vec<String>>,
    /// Role (`"user"` / `"assistant"`) of the session's newest
    /// user/assistant message, served from the persisted
    /// `agent_session.last_message_role` column (0070) maintained at
    /// message-write time. Other roles (system/tool) are transparent.
    /// `None` when the session has no user/assistant message — the wire
    /// field is omitted.
    pub last_message_role: Option<String>,
    /// Row id of the session's newest user/assistant message, served from
    /// the persisted `agent_session.last_message_id` column (0088)
    /// maintained at message-write time alongside `last_message_role`.
    /// Other roles (system/tool) are transparent. `None` when the session
    /// has no user/assistant message — the wire field is omitted.
    pub last_message_id: Option<String>,
    /// Preview of the newest user/assistant message's LAST `tool_use` block
    /// (`{ name, input?, inputTruncated?, inputBytes? }`, the
    /// [`intent_core::last_tool_use_preview`] shape), served from the
    /// persisted `agent_session.last_tool_use_preview` column (0098)
    /// maintained at message-write time. `None` when that message carries no
    /// `tool_use` block, the session has no user/assistant message, or the
    /// column is corrupt (degrade-only, like the 0066 previews) — the wire
    /// field is omitted.
    pub last_tool_use: Option<serde_json::Value>,
}

/// Per-block character cap applied inside SQLite when extracting projection
/// text (monorepo#1010 P1b). Assistant blocks keep their TAIL — the
/// `AgentLite` preview is the last non-empty line and the `<agent_digest>`
/// span is appended at the end — while user blocks keep their HEAD
/// (`lastUserMessage` reads from the start). Sized so the digest and
/// suggested-prompts markers plus the final response line survive intact for
/// any realistic message.
pub const PROJECTION_TEXT_BLOCK_CAP: u32 = 4096;

/// Applied at message-write time to maintain the persisted
/// `agent_session.last_assistant_preview` / `last_user_preview` columns
/// (0066): `None` when `content` is not a JSON array; otherwise the
/// `type: "text"` blocks with a string `text`, in block order, each capped at
/// [`PROJECTION_TEXT_BLOCK_CAP`] characters — assistant blocks keep their
/// TAIL, any other role its HEAD (counted in `char`s, not bytes).
fn preview_text_blocks(role: &str, content: &serde_json::Value) -> Option<Vec<String>> {
    let cap = PROJECTION_TEXT_BLOCK_CAP as usize;
    let blocks = content.as_array()?;
    Some(
        blocks
            .iter()
            .filter_map(|block| {
                let obj = block.as_object()?;
                if obj.get("type")?.as_str()? != "text" {
                    return None;
                }
                let text = obj.get("text")?.as_str()?;
                Some(if role == "assistant" {
                    let len = text.chars().count();
                    if len <= cap {
                        text.to_string()
                    } else {
                        text.chars().skip(len - cap).collect()
                    }
                } else {
                    text.chars().take(cap).collect()
                })
            })
            .collect(),
    )
}

/// TEXT column value for a preview: the JSON-encoded block array in the
/// projection form the read path serves. A winner whose content is not a
/// JSON array encodes as `"[]"` — the projection maps such winners to zero
/// text blocks. NULL is reserved for "no message of this role".
fn preview_col_value(role: &str, content: &serde_json::Value) -> Result<String> {
    let blocks = preview_text_blocks(role, content).unwrap_or_default();
    serde_json::to_string(&blocks)
        .map_err(|e| Error::Internal(format!("encode message preview failed: {e}")))
}

/// TEXT column value for the `last_tool_use_preview` column (0098): the
/// JSON-encoded [`intent_core::last_tool_use_preview`] of the message
/// content, `None` (NULL) when the content carries no `tool_use` block —
/// a user/assistant append always overwrites the column (it IS the newest
/// user/assistant message), so NULL actively clears a stale preview.
fn last_tool_use_col_value(content: &serde_json::Value) -> Result<Option<String>> {
    intent_core::last_tool_use_preview(content)
        .map(|preview| {
            serde_json::to_string(&preview)
                .map_err(|e| Error::Internal(format!("encode last tool use preview failed: {e}")))
        })
        .transpose()
}

/// Decode the persisted `last_tool_use_preview` column (0098): NULL stays
/// `None`; a corrupt value degrades to `None` (with a warning) like
/// [`decode_preview_col`] — no repair is attempted; the column converges the
/// next time a user/assistant message is appended.
fn decode_last_tool_use_col(raw: Option<String>) -> Option<serde_json::Value> {
    let raw = raw?;
    match serde_json::from_str(&raw) {
        Ok(preview) => Some(preview),
        Err(e) => {
            tracing::warn!(
                error = %e,
                "corrupt agent_session last_tool_use_preview column; degrading to None"
            );
            None
        }
    }
}

/// Decode a persisted preview column back into the projection's block vec:
/// NULL stays `None`, non-NULL is the JSON string array written by
/// [`preview_col_value`] or the 0066 backfill. A corrupt value degrades to
/// `None` (with a warning) instead of failing the whole projection read — no
/// repair is attempted; the column converges naturally the next time a
/// message of that role is appended.
fn decode_preview_col(raw: Option<String>) -> Option<Vec<String>> {
    let raw = raw?;
    match serde_json::from_str(&raw) {
        Ok(blocks) => Some(blocks),
        Err(e) => {
            tracing::warn!(
                error = %e,
                "corrupt agent_session preview column; degrading to None"
            );
            None
        }
    }
}

/// Preview column values `(last_assistant_preview, last_user_preview,
/// last_message_role, last_tool_use_preview)` for a whole message batch
/// written in seq order: the
/// projection of the LAST message of each role wins (a non-array winner
/// stores `"[]"`, the projection form), `None` when the batch has no message
/// of that role — matching the newest-row window query.
/// `last_message_role` is the role of the batch's LAST user/assistant
/// message (0070); other roles are transparent, as they are for
/// `last_tool_use_preview` (0098, derived from that same newest
/// user/assistant message's content). The batch insert loops track
/// `last_message_id` (0088) inline with the same newest-user/assistant
/// definition (ids are minted in-loop) — keep the two in sync if the
/// transparent-role set ever changes.
type OwnedBatchMessage = (String, serde_json::Value, Option<serde_json::Value>, String);
type BatchPreviewColValues = (
    Option<String>,
    Option<String>,
    Option<String>,
    Option<String>,
);
fn batch_preview_col_values(messages: &[OwnedBatchMessage]) -> Result<BatchPreviewColValues> {
    let mut assistant_preview = None;
    let mut user_preview = None;
    let mut last_message_role = None;
    let mut last_tool_use = None;
    for (role, content, _, _) in messages {
        match role.as_str() {
            "assistant" => {
                assistant_preview = Some(preview_col_value(role, content)?);
                last_message_role = Some(role.clone());
                last_tool_use = last_tool_use_col_value(content)?;
            }
            "user" => {
                user_preview = Some(preview_col_value(role, content)?);
                last_message_role = Some(role.clone());
                last_tool_use = last_tool_use_col_value(content)?;
            }
            _ => {}
        }
    }
    Ok((
        assistant_preview,
        user_preview,
        last_message_role,
        last_tool_use,
    ))
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

/// Encode the `effort_levels` string array as its JSON TEXT form, `None`
/// staying NULL. `serde_json` string-array encoding is deterministic, so the
/// encoded form doubles as the change-detection comparand in
/// [`Store::set_agent_effort_levels`].
fn effort_levels_to_db(levels: &Option<Vec<String>>) -> Result<Option<String>> {
    levels
        .as_ref()
        .map(|v| {
            serde_json::to_string(v)
                .map_err(|e| Error::Internal(format!("encode effort_levels failed: {e}")))
        })
        .transpose()
}

/// Decode the `effort_levels` JSON TEXT column back into its string array.
fn effort_levels_from_db(raw: Option<String>) -> Result<Option<Vec<String>>> {
    raw.map(|s| {
        serde_json::from_str(&s)
            .map_err(|e| Error::Internal(format!("decode effort_levels failed: {e}")))
    })
    .transpose()
}

/// Bind the full 39-column `agent_session` insert value list onto `query`, in
/// [`SESSION_COLUMNS`] order. Shared by [`Store::insert_agent_session`] and
/// [`Store::insert_agent_session_with_messages`] so the column/bind pairing
/// lives in one place. The harness stamp (`harness_version` /
/// `harness_features`) binds from the session struct itself.
fn bind_session_insert<'q>(
    query: sqlx::query::Query<'q, sqlx::Sqlite, sqlx::sqlite::SqliteArguments<'q>>,
    s: &'q AgentSession,
    task_graph_enabled: bool,
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
        .bind(&s.attention_request_kind)
        .bind(&s.attention_request_reason)
        .bind(&s.attention_request_timestamp)
        .bind(s.delegation_depth)
        .bind(&s.initial_message)
        .bind(json_col_to_db(&s.context_references)?)
        .bind(json_col_to_db(&s.image_blocks)?)
        .bind(json_col_to_db(&s.file_blocks)?)
        .bind(s.is_background as i64)
        .bind(encode_metadata(s.metadata.as_ref())?)
        .bind(&s.sandbox_id)
        .bind(&s.sandbox_path)
        .bind(&s.sandbox_branch)
        .bind(&s.stop_reason)
        .bind(&s.stop_reason_timestamp)
        .bind(&s.reasoning_effort)
        .bind(effort_levels_to_db(&s.effort_levels)?)
        .bind(task_graph_enabled as i64)
        .bind(&s.harness_version)
        .bind(json_col_to_db(&s.harness_features)?))
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
        self.insert_agent_session_with_task_graph(s, false).await
    }

    /// Insert a session with the daemon-owned task-graph feature snapshot that
    /// governs delivery-time teaching for the lifetime of this session.
    pub async fn insert_agent_session_with_task_graph(
        &self,
        s: &AgentSession,
        task_graph_enabled: bool,
    ) -> Result<()> {
        let sql = format!(
            "INSERT INTO agent_session ({SESSION_COLUMNS}) VALUES \
             (?,?,?,?,?,?,?,?,?,?,?,?,?,?,?,?,?,?,?,?,?,?,?,?,?,?,?,?,?,?,?,?,?,?,?,?,?,?,?)"
        );
        bind_session_insert(sqlx::query(&sql), s, task_graph_enabled)?
            .execute(self.write_pool())
            .await
            .map_err(|e| {
                if e.as_database_error()
                    .is_some_and(sqlx::error::DatabaseError::is_unique_violation)
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
    /// transcript would otherwise be skipped forever on re-runs. The session's
    /// last-message preview columns (0066), `last_message_role` (0070), and
    /// `last_message_id` (0088) are computed from the batch inside
    /// the same transaction. Uses
    /// whole-transaction retry to absorb SQLITE_BUSY (code 5) during lock
    /// upgrade (STAB-7).
    pub async fn insert_agent_session_with_messages(
        &self,
        s: &AgentSession,
        messages: &[ReplaceMessage<'_>],
    ) -> Result<()> {
        let pool = self.write_pool();
        // Clone messages into owned data for the retry closure.
        let owned_messages: Vec<OwnedBatchMessage> = messages
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
        // Thumbnail generation (CPU-bound image work) runs BEFORE the write
        // transaction opens — never inside it or the retry closure.
        let thumbnails = batch_thumbnails_col_values(&owned_messages).await;

        crate::with_write_txn_retry(|| async {
            let mut tx = pool.begin().await.map_err(|e| {
                Error::Internal(format!("insert session with messages begin failed: {e}"))
            })?;
            let session_sql = format!(
                "INSERT INTO agent_session ({SESSION_COLUMNS}) VALUES \
                 (?,?,?,?,?,?,?,?,?,?,?,?,?,?,?,?,?,?,?,?,?,?,?,?,?,?,?,?,?,?,?,?,?,?,?,?,?,?,?)"
            );
            bind_session_insert(sqlx::query(&session_sql), s, false)?
                .execute(&mut *tx)
                .await
                .map_err(|e| Error::Internal(format!("insert agent session failed: {e}")))?;
            let insert_sql = format!(
                "INSERT INTO agent_message ({MESSAGE_INSERT_COLUMNS}) VALUES (?,?,?,?,?,?,?,?)"
            );
            let mut last_message_id: Option<String> = None;
            for (idx, (role, content, metadata, created_at)) in owned_messages.iter().enumerate() {
                let content_json = serde_json::to_string(content)
                    .map_err(|e| Error::Internal(format!("encode message content failed: {e}")))?;
                let metadata_json = match metadata {
                    Some(md) => Some(serde_json::to_string(md).map_err(|e| {
                        Error::Internal(format!("encode message metadata failed: {e}"))
                    })?),
                    None => None,
                };
                let thumbnails_json = thumbnails[idx].as_deref();
                let id = Uuid::now_v7().to_string();
                if role == "user" || role == "assistant" {
                    last_message_id = Some(id.clone());
                }
                sqlx::query(&insert_sql)
                    .bind(&id)
                    .bind(&s.id.0)
                    .bind(idx as i64)
                    .bind(role)
                    .bind(&content_json)
                    .bind(metadata_json.as_deref())
                    .bind(created_at)
                    .bind(thumbnails_json)
                    .execute(&mut *tx)
                    .await
                    .map_err(|e| Error::Internal(format!("append agent message failed: {e}")))?;
            }
            let (assistant_preview, user_preview, last_message_role, last_tool_use) =
                batch_preview_col_values(&owned_messages)?;
            sqlx::query(
                "UPDATE agent_session SET last_assistant_preview = ?, last_user_preview = ?, \
                 last_message_role = ?, last_message_id = ?, last_tool_use_preview = ? \
                 WHERE id = ?",
            )
            .bind(assistant_preview.as_deref())
            .bind(user_preview.as_deref())
            .bind(last_message_role.as_deref())
            .bind(last_message_id.as_deref())
            .bind(last_tool_use.as_deref())
            .bind(&s.id.0)
            .execute(&mut *tx)
            .await
            .map_err(|e| Error::Internal(format!("update session message previews failed: {e}")))?;
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

    /// Metadata-only session lookup: the session row WITHOUT its message log
    /// (`messages` stays empty), or `NotFound` — the single-session shape of
    /// [`Store::list_agent_session_summaries`]. Used by paths that never read
    /// message bodies (`agent.get` `AgentLite` projection, workspace scope
    /// checks — monorepo#958), skipping the full transcript hydration that
    /// `get_agent_session` performs.
    pub async fn get_agent_session_summary(&self, id: &AgentId) -> Result<AgentSession> {
        let sql = format!("SELECT {SESSION_SUMMARY_COLUMNS} FROM agent_session WHERE id = ?");
        let row = sqlx::query(&sql)
            .bind(&id.0)
            .fetch_optional(self.read_pool())
            .await
            .map_err(|e| Error::Internal(format!("get agent session summary failed: {e}")))?;
        match row {
            Some(r) => map_session_summary_row(&r),
            None => Err(Error::NotFound(format!("agent session {id}"))),
        }
    }

    /// Batched status projection: the persisted [`AgentStatus`] of every id
    /// in `ids`, in ONE `IN`-list query (the `agent.getSubscriptions`
    /// `agentStatuses` map — intent-hq/monorepo#3018). Replaces a per-agent
    /// `get_agent_session` loop that hydrated each agent's full message log
    /// just to read `status`, so the caller stays at one statement and zero
    /// transcript bytes regardless of how many agents are present. Ids
    /// without a session row are simply absent from the result, and a row
    /// whose stored `status` fails to decode (e.g. a daemon downgrade after
    /// a newer build persisted a new variant) is skipped with a WARN — both
    /// best-effort, matching the old loop's skip-on-error behavior; order is
    /// unspecified. The id list is chunked well under SQLite's 32766
    /// bind-variable cap (same defense as `bulk_upsert_scripts`), so an
    /// implausibly large `ids` costs extra statements instead of erroring.
    pub async fn get_agent_statuses(&self, ids: &[AgentId]) -> Result<Vec<(AgentId, AgentStatus)>> {
        const IDS_PER_STATEMENT: usize = 32_000;
        let mut out = Vec::with_capacity(ids.len());
        for chunk in ids.chunks(IDS_PER_STATEMENT) {
            let placeholders = vec!["?"; chunk.len()].join(",");
            let sql = format!("SELECT id, status FROM agent_session WHERE id IN ({placeholders})");
            let mut query = sqlx::query(&sql);
            for id in chunk {
                query = query.bind(&id.0);
            }
            let rows = query
                .fetch_all(self.read_pool())
                .await
                .map_err(|e| Error::Internal(format!("get agent statuses failed: {e}")))?;
            for row in &rows {
                let id: String = row.get("id");
                match enum_from_db::<AgentStatus>(&row.get::<String, _>("status")) {
                    Ok(status) => out.push((AgentId(id), status)),
                    Err(e) => {
                        tracing::warn!(
                            agent = %id,
                            error = %e,
                            "decode agent status failed; omitting agent from status batch"
                        );
                    }
                }
            }
        }
        Ok(out)
    }

    /// Read the daemon-owned task-graph snapshot captured when this agent was
    /// created. Prefers the whole-harness `harness_features` snapshot's
    /// `taskGraph` value (0096) and falls back to the legacy
    /// `task_graph_enabled` column (0095) for pre-snapshot rows — behavior
    /// identical for every row written before the fold.
    pub async fn get_agent_session_task_graph_enabled(&self, id: &AgentId) -> Result<bool> {
        sqlx::query_scalar::<_, i64>(
            "SELECT COALESCE(harness_features ->> '$.taskGraph', task_graph_enabled) \
             FROM agent_session WHERE id = ?",
        )
        .bind(&id.0)
        .fetch_optional(self.read_pool())
        .await
        .map_err(|e| Error::Internal(format!("get agent taskGraph snapshot failed: {e}")))?
        .map(|value| value != 0)
        .ok_or_else(|| Error::NotFound(format!("agent session {id}")))
    }

    /// Lazy legacy freeze (intent-hq/monorepo#2459): persist `snapshot` as the
    /// session's `harness_features` ONLY if the column is still NULL — the
    /// one-time materialization a legacy (pre-0096) row gets at its first
    /// activation. The `IS NULL` guard makes the write idempotent at the DB
    /// level (a second activation, or a concurrent one, never rewrites), and
    /// `updated_at` is deliberately untouched so the freeze is invisible to
    /// list ordering. Returns the persisted value after the conditional write
    /// (a lost race returns the winner's snapshot). `NotFound` if the session
    /// is absent.
    pub async fn materialize_agent_session_harness_features(
        &self,
        id: &AgentId,
        snapshot: &serde_json::Value,
    ) -> Result<Option<serde_json::Value>> {
        let encoded = serde_json::to_string(snapshot)
            .map_err(|e| Error::Internal(format!("encode agentFeatures snapshot failed: {e}")))?;
        sqlx::query(
            "UPDATE agent_session SET harness_features = ? \
             WHERE id = ? AND harness_features IS NULL",
        )
        .bind(&encoded)
        .bind(&id.0)
        .execute(self.write_pool())
        .await
        .map_err(|e| Error::Internal(format!("materialize harness_features failed: {e}")))?;
        let raw = sqlx::query_scalar::<_, Option<String>>(
            "SELECT harness_features FROM agent_session WHERE id = ?",
        )
        .bind(&id.0)
        .fetch_optional(self.read_pool())
        .await
        .map_err(|e| Error::Internal(format!("read back harness_features failed: {e}")))?
        .ok_or_else(|| Error::NotFound(format!("agent session {id}")))?;
        json_col_from_db(raw, "harness_features")
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

    /// Lightweight timestamp-only lookup for daemon-global active-agent probes.
    /// Selects no session metadata or transcript content.
    pub async fn get_agent_session_updated_at(&self, id: &AgentId) -> Result<String> {
        let row = sqlx::query("SELECT updated_at FROM agent_session WHERE id = ?")
            .bind(&id.0)
            .fetch_optional(self.read_pool())
            .await
            .map_err(|e| Error::Internal(format!("get agent session updated_at failed: {e}")))?;
        match row {
            Some(r) => Ok(r.get("updated_at")),
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
            "SELECT {SESSION_SUMMARY_COLUMNS} FROM agent_session WHERE workspace_id = ? ORDER BY created_at"
        );
        let rows = sqlx::query(&sql)
            .bind(&workspace_id.0)
            .fetch_all(self.read_pool())
            .await
            .map_err(|e| Error::Internal(format!("list agent session summaries failed: {e}")))?;
        rows.iter().map(map_session_summary_row).collect()
    }

    /// Get message count, whether any assistant message exists, and the total
    /// persisted conversation size in bytes for each session in a workspace,
    /// without hydrating message bodies (finding F1/F3: lightweight
    /// alternative to full message-log fetch for `agent.diagnostics`). One aggregate
    /// statement — the LEFT JOIN keeps zero-message sessions — instead of two
    /// queries per session (monorepo#958). The byte total sums
    /// `OCTET_LENGTH(content)` — raw stored bytes read from the row header,
    /// never decoded as JSON and never loading overflow pages — so
    /// coordinators can see session-size pressure before turns start dying
    /// under context bloat (intent-hq/monorepo#2669). Returns a map keyed
    /// by agent_id with `(message_count, has_assistant, conversation_bytes)`
    /// tuples.
    pub async fn get_agent_session_message_stats(
        &self,
        workspace_id: &WorkspaceId,
    ) -> Result<std::collections::HashMap<String, (u64, bool, u64)>> {
        let sql = "SELECT s.id AS agent_id, COUNT(m.id) AS message_count, \
            COALESCE(SUM(m.role = 'assistant'), 0) AS assistant_count, \
            COALESCE(SUM(OCTET_LENGTH(m.content)), 0) AS conversation_bytes \
            FROM agent_session s \
            LEFT JOIN agent_message m ON m.agent_id = s.id \
            WHERE s.workspace_id = ? \
            GROUP BY s.id";
        let rows = sqlx::query(sql)
            .bind(&workspace_id.0)
            .fetch_all(self.read_pool())
            .await
            .map_err(|e| Error::Internal(format!("get agent session message stats failed: {e}")))?;
        let mut stats = std::collections::HashMap::with_capacity(rows.len());
        for row in rows {
            let agent_id: String = row.get("agent_id");
            let message_count: i64 = row.get("message_count");
            let assistant_count: i64 = row.get("assistant_count");
            let conversation_bytes: i64 = row.get("conversation_bytes");
            stats.insert(
                agent_id,
                (
                    message_count as u64,
                    assistant_count != 0,
                    conversation_bytes.max(0) as u64,
                ),
            );
        }
        Ok(stats)
    }

    /// Per-session `AgentLite` projection inputs for every session in a
    /// workspace, in one bounded statement (monorepo#958, 0066): an aggregate
    /// message count (LEFT JOIN, so zero-message sessions still appear with
    /// count 0) plus the persisted `last_assistant_preview` /
    /// `last_user_preview` columns maintained at message-write time — read
    /// cost no longer scales with transcript or message size, and
    /// `agent_message.content` is never touched on this path. A NULL or
    /// corrupt column degrades to `None` ([`decode_preview_col`]) rather than
    /// being repaired; it converges the next time a message is appended.
    /// Returns a map keyed by agent_id with one entry per session in the
    /// workspace.
    pub async fn get_agent_session_message_projections(
        &self,
        workspace_id: &WorkspaceId,
    ) -> Result<std::collections::HashMap<String, SessionMessageProjection>> {
        let sql = "SELECT s.id AS agent_id, COUNT(m.id) AS message_count, \
            s.last_assistant_preview, s.last_user_preview, s.last_message_role, \
            s.last_message_id, s.last_tool_use_preview \
            FROM agent_session s \
            LEFT JOIN agent_message m ON m.agent_id = s.id \
            WHERE s.workspace_id = ? \
            GROUP BY s.id";
        let rows = sqlx::query(sql)
            .bind(&workspace_id.0)
            .fetch_all(self.read_pool())
            .await
            .map_err(|e| Error::Internal(format!("session message projections failed: {e}")))?;
        let mut projections = std::collections::HashMap::with_capacity(rows.len());
        for row in &rows {
            let agent_id: String = row.get("agent_id");
            let message_count: i64 = row.get("message_count");
            projections.insert(
                agent_id,
                SessionMessageProjection {
                    message_count: message_count as u64,
                    last_assistant_text_blocks: decode_preview_col(
                        row.get("last_assistant_preview"),
                    ),
                    last_user_text_blocks: decode_preview_col(row.get("last_user_preview")),
                    last_message_role: row.get("last_message_role"),
                    last_message_id: row.get("last_message_id"),
                    last_tool_use: decode_last_tool_use_col(row.get("last_tool_use_preview")),
                },
            );
        }
        Ok(projections)
    }

    /// Bounded `AgentLite` projection inputs for a single session
    /// (monorepo#981): the per-session variant of
    /// [`Store::get_agent_session_message_projections`] — one row reading the
    /// persisted preview columns (0066) plus a correlated message count. A
    /// session with no messages (or an unknown agent id) returns the zero
    /// projection.
    pub async fn get_agent_session_message_projection(
        &self,
        agent_id: &AgentId,
    ) -> Result<SessionMessageProjection> {
        let sql = "SELECT s.last_assistant_preview, s.last_user_preview, s.last_message_role, \
            s.last_message_id, s.last_tool_use_preview, \
            (SELECT COUNT(*) FROM agent_message m WHERE m.agent_id = s.id) AS message_count \
            FROM agent_session s WHERE s.id = ?";
        let Some(row) = sqlx::query(sql)
            .bind(&agent_id.0)
            .fetch_optional(self.read_pool())
            .await
            .map_err(|e| Error::Internal(format!("session message projection failed: {e}")))?
        else {
            return Ok(SessionMessageProjection::default());
        };
        let message_count: i64 = row.get("message_count");
        Ok(SessionMessageProjection {
            message_count: message_count as u64,
            last_assistant_text_blocks: decode_preview_col(row.get("last_assistant_preview")),
            last_user_text_blocks: decode_preview_col(row.get("last_user_preview")),
            last_message_role: row.get("last_message_role"),
            last_message_id: row.get("last_message_id"),
            last_tool_use: decode_last_tool_use_col(row.get("last_tool_use_preview")),
        })
    }

    /// Get the agent_message watermark for a workspace: the count of messages
    /// across all agents. This is used by the token-usage scan loop to skip
    /// workspaces that have not changed since the last scan (finding F2).
    /// Returns 0 for workspaces with no agents or no messages.
    pub async fn get_workspace_message_watermark(&self, workspace_id: &WorkspaceId) -> Result<u64> {
        let sql = r"
            SELECT COUNT(*) as count
            FROM agent_message
            WHERE agent_id IN (
                SELECT id FROM agent_session WHERE workspace_id = ?
            )
        ";
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
    /// summed). Scoped to `workspace_id` (defense-in-depth — matches the
    /// pattern of the other `agent_session` update helpers). `NotFound` if
    /// the session row is absent or the workspace does not match.
    pub async fn set_agent_session_token_usage(
        &self,
        workspace_id: &WorkspaceId,
        id: &AgentId,
        snapshot: &TokenUsageTotals,
    ) -> Result<()> {
        let json = serde_json::to_string(snapshot)
            .map_err(|e| Error::Internal(format!("encode session token_usage failed: {e}")))?;
        let res =
            sqlx::query("UPDATE agent_session SET token_usage=? WHERE id=? AND workspace_id=?")
                .bind(json)
                .bind(&id.0)
                .bind(&workspace_id.0)
                .execute(self.write_pool())
                .await
                .map_err(|e| {
                    Error::Internal(format!("set agent session token usage failed: {e}"))
                })?;
        if res.rows_affected() == 0 {
            return Err(Error::NotFound(format!("agent session {id}")));
        }
        Ok(())
    }

    /// Guarded write of a session's *resolved* display model (D13/D14): the
    /// display identity resolved against the provider's
    /// `configOptions[id="model"]` option list at session open — for an
    /// explicit pick (D14) and for a placeholder/NULL model whose effective
    /// model the provider reported (D13). This never touches `model` — the
    /// stored id (or placeholder) keeps driving provider configuration
    /// (spawn flags / `session/set_config_option`); the resolution is used
    /// ONLY for usage-stats attribution. `resolved` is written as given,
    /// `None` included — an id that no longer resolves must overwrite (not
    /// orphan) a previously persisted resolution, or a stale display name
    /// would keep mis-attributing stats. Writes only while `model` still
    /// equals `expected_model` (the value read before the ACP call — `None`
    /// matches NULL), so a resolution is never attached to a model a
    /// concurrent `agent.setModel` changed. Returns whether the write
    /// landed. Scoped to `workspace_id` (defense-in-depth).
    pub async fn set_agent_session_resolved_model(
        &self,
        workspace_id: &WorkspaceId,
        id: &AgentId,
        expected_model: Option<&str>,
        resolved: Option<&str>,
    ) -> Result<bool> {
        let res = match expected_model {
            Some(expected) => {
                sqlx::query(
                    "UPDATE agent_session SET resolved_model=? \
                     WHERE id=? AND workspace_id=? AND model=?",
                )
                .bind(resolved)
                .bind(&id.0)
                .bind(&workspace_id.0)
                .bind(expected)
                .execute(self.write_pool())
                .await
            }
            None => {
                sqlx::query(
                    "UPDATE agent_session SET resolved_model=? \
                     WHERE id=? AND workspace_id=? AND model IS NULL",
                )
                .bind(resolved)
                .bind(&id.0)
                .bind(&workspace_id.0)
                .execute(self.write_pool())
                .await
            }
        }
        .map_err(|e| Error::Internal(format!("set agent session resolved model failed: {e}")))?;
        Ok(res.rows_affected() > 0)
    }

    /// Clear a session's resolved display model (D14). Called by
    /// `agent.setModel` when the stored model changes so a stale resolution
    /// never mis-attributes stats; the next session open re-resolves against
    /// the new model. Idempotent: an absent row or an already-NULL column is
    /// a no-op, not an error — the caller just updated the session, so
    /// nothing actionable hides behind a zero row count.
    pub async fn clear_agent_session_resolved_model(
        &self,
        workspace_id: &WorkspaceId,
        id: &AgentId,
    ) -> Result<()> {
        sqlx::query("UPDATE agent_session SET resolved_model=NULL WHERE id=? AND workspace_id=?")
            .bind(&id.0)
            .bind(&workspace_id.0)
            .execute(self.write_pool())
            .await
            .map_err(|e| {
                Error::Internal(format!("clear agent session resolved model failed: {e}"))
            })?;
        Ok(())
    }

    /// Read the model/provider identity of the agent's last committed turn
    /// (model-change transcript notice): `(last_turn_model, last_turn_provider)`.
    /// Both are NULL until the agent's first turn commits its identity via
    /// [`Store::set_agent_session_last_turn_model`]. Scoped to `workspace_id`
    /// (defense-in-depth). `NotFound` if the session row is absent or the
    /// workspace does not match.
    pub async fn get_agent_session_last_turn_model(
        &self,
        workspace_id: &WorkspaceId,
        id: &AgentId,
    ) -> Result<(Option<String>, Option<String>)> {
        let row = sqlx::query(
            "SELECT last_turn_model, last_turn_provider FROM agent_session \
             WHERE id=? AND workspace_id=?",
        )
        .bind(&id.0)
        .bind(&workspace_id.0)
        .fetch_optional(self.read_pool())
        .await
        .map_err(|e| Error::Internal(format!("get agent session last turn model failed: {e}")))?;
        let Some(row) = row else {
            return Err(Error::NotFound(format!("agent session {id}")));
        };
        Ok((
            row.get::<Option<String>, _>("last_turn_model"),
            row.get::<Option<String>, _>("last_turn_provider"),
        ))
    }

    /// Persist the model/provider identity the CURRENT turn runs under
    /// (model-change transcript notice). Written at turn start once the
    /// turn's spawn identity is resolved — NOT on `agent.setModel` — so
    /// picker toggles reverted before any message never commit a "last
    /// turn" identity. `model` is the spawn-resolved model id (`None` for
    /// the provider default). Scoped to `workspace_id` (defense-in-depth);
    /// an absent row is a no-op (the caller just read the session).
    pub async fn set_agent_session_last_turn_model(
        &self,
        workspace_id: &WorkspaceId,
        id: &AgentId,
        model: Option<&str>,
        provider: &str,
    ) -> Result<()> {
        sqlx::query(
            "UPDATE agent_session SET last_turn_model=?, last_turn_provider=? \
             WHERE id=? AND workspace_id=?",
        )
        .bind(model)
        .bind(provider)
        .bind(&id.0)
        .bind(&workspace_id.0)
        .execute(self.write_pool())
        .await
        .map_err(|e| Error::Internal(format!("set agent session last turn model failed: {e}")))?;
        Ok(())
    }

    /// Read one session's `model`, `resolved_model` (D14 display identity of
    /// an explicit pick, if any), `provider`, and its persisted cumulative
    /// end-of-turn `token_usage` snapshot (§5.23) in a single row read. This
    /// is the pre-turn state the global usage-stats recorder diffs the new
    /// snapshot against, so it MUST be read BEFORE
    /// [`set_agent_session_token_usage`](Store::set_agent_session_token_usage)
    /// replaces the snapshot. The snapshot is scoped to the CURRENT ACP
    /// session (a recreate folds it into `token_usage_baseline` and clears
    /// it), which is exactly the baseline a per-turn delta needs. The
    /// `provider` rides along as the stats model-key fallback for
    /// placeholder/absent models (D13). Scoped to `workspace_id`
    /// (defense-in-depth). `NotFound` if the session row is absent or the
    /// workspace does not match.
    #[allow(clippy::type_complexity)]
    pub async fn get_agent_session_token_usage(
        &self,
        workspace_id: &WorkspaceId,
        id: &AgentId,
    ) -> Result<(
        Option<String>,
        Option<String>,
        Option<String>,
        Option<TokenUsageTotals>,
    )> {
        let row = sqlx::query(
            "SELECT model, resolved_model, provider, token_usage FROM agent_session \
             WHERE id=? AND workspace_id=?",
        )
        .bind(&id.0)
        .bind(&workspace_id.0)
        .fetch_optional(self.read_pool())
        .await
        .map_err(|e| Error::Internal(format!("get agent session token usage failed: {e}")))?;
        let Some(row) = row else {
            return Err(Error::NotFound(format!("agent session {id}")));
        };
        let model = row.get::<Option<String>, _>("model");
        let resolved_model = row.get::<Option<String>, _>("resolved_model");
        let provider = row.get::<Option<String>, _>("provider");
        let snapshot = row
            .get::<Option<String>, _>("token_usage")
            .map(|json| {
                serde_json::from_str(&json)
                    .map_err(|e| Error::Internal(format!("decode session token_usage failed: {e}")))
            })
            .transpose()?;
        Ok((model, resolved_model, provider, snapshot))
    }

    /// Get lightweight usage data for all agents in a workspace: for each agent,
    /// returns the agent_id, model, the persisted end-of-turn `token_usage`
    /// snapshot (if any), the persisted `token_usage_baseline` folded from
    /// prior ACP sessions (if any, monorepo#737), and per-message usage
    /// metadata (for tallying without full AgentSession hydration; finding F2).
    /// Messages are read ONLY for sessions whose decoded snapshot and baseline
    /// carry no token report — the tally falls back to message sums for those
    /// alone (see `agent_token_tally`), so report-backed sessions return an
    /// empty list (monorepo#738) — and that read is bounded: SQLite projects
    /// each message's usage metadata and drops rows carrying none, so message
    /// bodies never cross the boundary (monorepo#1571). A malformed
    /// snapshot/baseline decodes to `None` and therefore stays on the fallback.
    pub async fn get_workspace_agent_usage_data(
        &self,
        workspace_id: &WorkspaceId,
    ) -> Result<Vec<AgentUsageRow>> {
        let mut conn = self
            .read_pool()
            .acquire()
            .await
            .map_err(|e| Error::Internal(format!("acquire read connection failed: {e}")))?;
        fetch_agent_usage_rows(&mut conn, workspace_id).await
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
    /// `acp_session_id` is set), catching accidental provider drift from
    /// general session writers. The one intentional provider change — a
    /// cross-provider `agent.setModel` — goes through the narrow
    /// [`Store::set_agent_session_model`] instead (monorepo#882).
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
        // The attention_request_* columns are deliberately ABSENT from this
        // full-row UPDATE: a long-lived in-memory `AgentSession` persisted
        // here mid-race must not resurrect a request that
        // `clear_attention_request` already NULLed (or clobber one that
        // `set_attention_request` just wrote). `effort_levels` is excluded on
        // the same grounds: `set_agent_effort_levels` (called at session
        // open) is its only post-insert mutator, so a stale in-memory session
        // persisted here cannot wipe freshly discovered levels.
        // Those two attention writers are
        // the only post-insert mutators of the attention columns.
        let rows = sqlx::query(
            "UPDATE agent_session SET backend_session_id=?, acp_session_id=?, name=?, \
             name_explicitly_set=?, model=?, provider=?, status=?, is_active=?, system_prompt=?, \
             updated_at=?, parent_agent_id=?, specialist=?, task_note_id=?, skip_auto_commit=?, \
             completion_report=?, completion_report_timestamp=?, delegation_depth=?, \
             initial_message=?, context_references=?, image_blocks=?, file_blocks=?, \
             is_background=?, metadata=?, sandbox_id=?, sandbox_path=?, sandbox_branch=?, \
             stop_reason=?, stop_reason_timestamp=?, reasoning_effort=? \
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
        .bind(json_col_to_db(&s.file_blocks)?)
        .bind(s.is_background as i64)
        .bind(encode_metadata(s.metadata.as_ref())?)
        .bind(&s.sandbox_id)
        .bind(&s.sandbox_path)
        .bind(&s.sandbox_branch)
        .bind(&s.stop_reason)
        .bind(&s.stop_reason_timestamp)
        .bind(&s.reasoning_effort)
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

    /// Persist a model switch (`agent.setModel`): a narrow write of `model`,
    /// `provider`, and `updated_at` only. This is the ONE writer allowed to
    /// change `provider` after first real use — an intentional cross-provider
    /// model switch must reconcile `provider` to the compound id's prefix so
    /// the next spawn tears down the old child and runs the new provider's
    /// binary (monorepo#882). Accidental provider drift from every other
    /// writer is still rejected by [`Store::update_agent_session`]'s
    /// immutability guard. Scoped to `workspace_id` (defense-in-depth).
    /// `NotFound` if the session is absent or the workspace does not match.
    pub async fn set_agent_session_model(
        &self,
        workspace_id: &WorkspaceId,
        id: &AgentId,
        model: &str,
        provider: Option<&str>,
        updated_at: &str,
    ) -> Result<()> {
        let rows = sqlx::query(
            "UPDATE agent_session SET model=?, provider=?, updated_at=? \
             WHERE id=? AND workspace_id=?",
        )
        .bind(model)
        .bind(provider)
        .bind(updated_at)
        .bind(&id.0)
        .bind(&workspace_id.0)
        .execute(self.write_pool())
        .await
        .map_err(|e| Error::Internal(format!("set agent session model failed: {e}")))?
        .rows_affected();
        if rows == 0 {
            return Err(Error::NotFound(format!("agent session {id}")));
        }
        Ok(())
    }

    /// Persist the assembled system prompt (the spawn path): a narrow write
    /// of `system_prompt` only. The spawn path previously persisted its
    /// long-held session snapshot through the full-row
    /// [`Store::update_agent_session`], silently reverting a concurrent
    /// `agent.setModel` that landed between the spawn's session read and the
    /// prompt persist — the lost update behind the flaky respawn e2e
    /// (monorepo#1936). Unlike sibling narrow writers, this deliberately does
    /// NOT bump `updated_at`: that timestamp belongs to the concurrent
    /// `agent.setModel` write this call must not stomp. Scoped to
    /// `workspace_id` (defense-in-depth). `NotFound` if the session is absent
    /// or the workspace does not match.
    pub async fn set_agent_session_system_prompt(
        &self,
        workspace_id: &WorkspaceId,
        id: &AgentId,
        system_prompt: &str,
    ) -> Result<()> {
        let rows =
            sqlx::query("UPDATE agent_session SET system_prompt=? WHERE id=? AND workspace_id=?")
                .bind(system_prompt)
                .bind(&id.0)
                .bind(&workspace_id.0)
                .execute(self.write_pool())
                .await
                .map_err(|e| {
                    Error::Internal(format!("set agent session system prompt failed: {e}"))
                })?
                .rows_affected();
        if rows == 0 {
            return Err(Error::NotFound(format!("agent session {id}")));
        }
        Ok(())
    }

    /// Persist a metadata change: a narrow write of `metadata` and
    /// `updated_at` only. Callers that load a session via the summary
    /// projection (no `system_prompt` — see [`SESSION_SUMMARY_COLUMNS`]) must
    /// use this instead of [`Store::update_agent_session`], whose full-row
    /// write would clear every column absent from the summary. Scoped to
    /// `workspace_id` (defense-in-depth). `NotFound` if the session is absent
    /// or the workspace does not match.
    pub async fn update_agent_session_metadata(
        &self,
        workspace_id: &WorkspaceId,
        id: &AgentId,
        metadata: Option<&serde_json::Value>,
        updated_at: &str,
    ) -> Result<()> {
        let rows = sqlx::query(
            "UPDATE agent_session SET metadata=?, updated_at=? WHERE id=? AND workspace_id=?",
        )
        .bind(encode_metadata(metadata)?)
        .bind(updated_at)
        .bind(&id.0)
        .bind(&workspace_id.0)
        .execute(self.write_pool())
        .await
        .map_err(|e| Error::Internal(format!("update agent session metadata failed: {e}")))?
        .rows_affected();
        if rows == 0 {
            return Err(Error::NotFound(format!("agent session {id}")));
        }
        Ok(())
    }

    /// Atomically set ONE key in `agent_session.metadata` in SQL (`json_set`),
    /// preserving every sibling key — unlike
    /// [`Store::update_agent_session_metadata`], whose whole-column replacement
    /// loses keys written concurrently by another metadata writer (e.g.
    /// `agent.dismissQuestions` racing `agent.markSeen`). A NULL column starts
    /// from `{}`; a non-object value (should one ever land there) is preserved
    /// under `priorNonObjectMetadata` (monorepo#751 review), matching the
    /// service-side defensive shape. `key` must be a trusted compile-time
    /// constant (it is spliced into the JSON path). `expected` is a three-way
    /// compare-and-set guard on the key's CURRENT value (same encoding as
    /// `stop_reason` on [`Store::set_agent_session_status`]): `None` writes
    /// unconditionally, `Some(None)` writes only when the key is absent,
    /// `Some(Some(v))` only when it currently equals `v`. Returns `Ok(false)`
    /// when the guard failed (the session exists but the key's value moved —
    /// callers re-read and retry); `NotFound` when the session is absent or
    /// the workspace does not match. `updated_at` is refreshed on a successful
    /// write only.
    pub async fn set_agent_session_metadata_key(
        &self,
        workspace_id: &WorkspaceId,
        id: &AgentId,
        key: &str,
        value: &str,
        expected: Option<Option<&str>>,
        updated_at: &str,
    ) -> Result<bool> {
        let guarded = expected.is_some();
        let expected_value = expected.flatten();
        let rows = sqlx::query(
            "UPDATE agent_session SET \
             metadata = json_set(\
                 CASE \
                     WHEN metadata IS NULL THEN '{}' \
                     WHEN json_type(metadata) = 'object' THEN metadata \
                     ELSE json_object('priorNonObjectMetadata', json(metadata)) \
                 END, \
                 '$.' || ?, ?), \
             updated_at = ? \
             WHERE id = ? AND workspace_id = ? \
               AND (? = 0 OR json_extract(metadata, '$.' || ?) IS ?)",
        )
        .bind(key)
        .bind(value)
        .bind(updated_at)
        .bind(&id.0)
        .bind(&workspace_id.0)
        .bind(guarded as i64)
        .bind(key)
        .bind(expected_value)
        .execute(self.write_pool())
        .await
        .map_err(|e| Error::Internal(format!("set agent session metadata key failed: {e}")))?
        .rows_affected();
        if rows == 0 {
            if guarded {
                // Distinguish a CAS-guard miss (session exists, marker moved)
                // from a missing / workspace-mismatched session.
                let exists =
                    sqlx::query("SELECT 1 FROM agent_session WHERE id = ? AND workspace_id = ?")
                        .bind(&id.0)
                        .bind(&workspace_id.0)
                        .fetch_optional(self.read_pool())
                        .await
                        .map_err(|e| {
                            Error::Internal(format!("agent session existence check failed: {e}"))
                        })?;
                if exists.is_some() {
                    return Ok(false);
                }
            }
            return Err(Error::NotFound(format!("agent session {id}")));
        }
        Ok(true)
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
    /// `stop_reason_timestamp` is coupled to `stop_reason`: setting a reason
    /// stamps the column with `updated_at`, clearing the reason clears it, and
    /// leaving the reason untouched leaves the timestamp untouched.
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
                // Leave stop_reason (and its timestamp) untouched.
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
                // stop_reason_timestamp is stamped with `updated_at` when a
                // reason is set and cleared to NULL alongside a cleared reason.
                let stop_reason_timestamp = reason.is_some().then_some(updated_at);
                sqlx::query(
                    "UPDATE agent_session SET status=?, is_active=?, updated_at=?, \
                     stop_reason=?, stop_reason_timestamp=? \
                     WHERE id=? AND workspace_id=?",
                )
                .bind(enum_to_db(&status)?)
                .bind(is_active as i64)
                .bind(updated_at)
                .bind(reason)
                .bind(stop_reason_timestamp)
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

    /// Persist a pending attention request (`attention_request_kind` /
    /// `..._reason` / `..._timestamp`): a narrow write of the three attention
    /// columns plus `updated_at` (refreshed to `timestamp`). Together with
    /// [`Store::clear_attention_request`] this is the ONLY writer of the
    /// attention columns after insert — the full-row
    /// [`Store::update_agent_session`] deliberately excludes them so a stale
    /// in-memory session persisted mid-race can neither resurrect a cleared
    /// request nor clobber a fresh one. Scoped to `workspace_id`
    /// (defense-in-depth). `NotFound` if the session is absent or the
    /// workspace does not match.
    pub async fn set_attention_request(
        &self,
        workspace_id: &WorkspaceId,
        id: &AgentId,
        kind: &str,
        reason: &str,
        timestamp: &str,
    ) -> Result<()> {
        let rows = sqlx::query(
            "UPDATE agent_session SET attention_request_kind=?, \
             attention_request_reason=?, attention_request_timestamp=?, updated_at=? \
             WHERE id=? AND workspace_id=?",
        )
        .bind(kind)
        .bind(reason)
        .bind(timestamp)
        .bind(timestamp)
        .bind(&id.0)
        .bind(&workspace_id.0)
        .execute(self.write_pool())
        .await
        .map_err(|e| Error::Internal(format!("set attention request failed: {e}")))?
        .rows_affected();
        if rows == 0 {
            return Err(Error::NotFound(format!("agent session {id}")));
        }
        Ok(())
    }

    /// Clear the pending attention request (`attention_request_kind` /
    /// `..._reason` / `..._timestamp`) when the agent next receives a message.
    /// Returns `true` if a request was present and cleared, `false` if none
    /// was set (the common case — no write, no event). Scoped to
    /// `workspace_id` (defense-in-depth); `updated_at` is refreshed to the
    /// supplied timestamp. `NotFound` if the session is absent or the
    /// workspace does not match. Mirrors [`Store::clear_completion_report`].
    pub async fn clear_attention_request(
        &self,
        workspace_id: &WorkspaceId,
        id: &AgentId,
        updated_at: &str,
    ) -> Result<bool> {
        let rows = sqlx::query(
            "UPDATE agent_session SET attention_request_kind=NULL, \
             attention_request_reason=NULL, attention_request_timestamp=NULL, updated_at=? \
             WHERE id=? AND workspace_id=? AND attention_request_kind IS NOT NULL",
        )
        .bind(updated_at)
        .bind(&id.0)
        .bind(&workspace_id.0)
        .execute(self.write_pool())
        .await
        .map_err(|e| Error::Internal(format!("clear attention request failed: {e}")))?
        .rows_affected();
        if rows == 0 {
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
            // Session exists but no request was pending — the common case.
            return Ok(false);
        }
        Ok(true)
    }

    /// Replace the session's `effort_levels` wholesale (PROTOCOL §5.5,
    /// Option C): the levels the provider's `thought_level` option advertised
    /// at session open, `None` when it advertised none. Returns `true` when
    /// the stored value actually changed — the write is guarded by an
    /// `IS NOT` comparison against the deterministic JSON encoding, so an
    /// unchanged set is a no-op (no write, no `updated_at` bump, no event
    /// from the caller). The ONLY post-insert mutator of the column (the
    /// full-row [`Store::update_agent_session`] deliberately excludes it).
    /// `NotFound` if the session is absent or the workspace does not match.
    pub async fn set_agent_effort_levels(
        &self,
        workspace_id: &WorkspaceId,
        id: &AgentId,
        levels: Option<&[String]>,
        updated_at: &str,
    ) -> Result<bool> {
        let encoded = effort_levels_to_db(&levels.map(<[String]>::to_vec))?;
        let rows = sqlx::query(
            "UPDATE agent_session SET effort_levels=?, updated_at=? \
             WHERE id=? AND workspace_id=? AND effort_levels IS NOT ?",
        )
        .bind(&encoded)
        .bind(updated_at)
        .bind(&id.0)
        .bind(&workspace_id.0)
        .bind(&encoded)
        .execute(self.write_pool())
        .await
        .map_err(|e| Error::Internal(format!("set effort levels failed: {e}")))?
        .rows_affected();
        if rows == 0 {
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
            // Session exists with the identical set — the common case.
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
    ///
    /// Both branches that write a new id also fold the current cumulative
    /// `token_usage` snapshot into `token_usage_baseline` and clear the
    /// snapshot, atomically with the id swap (see [`Store::write_acp_session_id`],
    /// monorepo#737): the recreated ACP session restarts its cumulative counts
    /// from zero, so the old session's totals must be banked first. The
    /// CAS-loss (diverged) branch writes nothing and therefore never folds;
    /// the CAS predicate is re-checked inside the write transaction, so a
    /// swap that races between this read and the write also loses cleanly.
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
                self.write_acp_session_id(workspace_id, id, Some(expected_old), new_acp_session_id)
                    .await
            }
            // Diverged (a concurrent recreate already swapped) → reuse the stored
            // canonical value instead of clobbering it.
            Some(existing) => Ok(existing.to_string()),
            // Nothing stored to clobber → set the fresh id.
            None => {
                self.write_acp_session_id(workspace_id, id, None, new_acp_session_id)
                    .await
            }
        }
    }

    /// CAS-guarded `acp_session_id` write helper shared by the writing
    /// branches of [`Store::replace_acp_session_id`]. Re-reads the stored id
    /// inside the write transaction and compares it (NULL-safe) against
    /// `expected_old` so the caller's read → write window cannot clobber a
    /// concurrent swap; on a mismatch it writes nothing and returns the
    /// stored canonical id. Returns the id that is canonical after the call.
    ///
    /// In the same transaction as the id write it folds the session's current
    /// `token_usage` snapshot into `token_usage_baseline` (component-wise
    /// saturating sum, NULL treated as zero) and clears the snapshot
    /// (monorepo#737). Malformed
    /// stored JSON decodes to `None` (mirroring
    /// [`Store::get_workspace_agent_usage_data`]) and is treated as zero. The
    /// write-once first set ([`Store::set_acp_session_id`]) does NOT share this
    /// helper and never touches the baseline.
    ///
    /// Uses raw `BEGIN IMMEDIATE` (same pattern as
    /// `Store::update_workspace_token_usage` / `insert_events`, monorepo#783):
    /// IMMEDIATE mode acquires the RESERVED (write) lock upfront — readers may
    /// still proceed, especially in WAL mode — avoiding the
    /// DEFERRED-mode lock-upgrade race (read → write inside one transaction)
    /// that intermittently fails with SQLITE_BUSY (code 5). With
    /// `max_connections=1` on the write pool, concurrent writers serialize at
    /// `pool.acquire()` instead. The CAS-loss early return happens before any
    /// write statement, so the guard's COMMIT closes a read-only transaction —
    /// there is no partial write to undo and the connection never returns to
    /// the pool with a transaction open.
    async fn write_acp_session_id(
        &self,
        workspace_id: &WorkspaceId,
        id: &AgentId,
        expected_old: Option<&str>,
        acp_session_id: &str,
    ) -> Result<String> {
        let mut conn =
            self.write_pool().acquire().await.map_err(|e| {
                Error::Internal(format!("replace acp session id acquire failed: {e}"))
            })?;
        sqlx::query("BEGIN IMMEDIATE")
            .execute(&mut *conn)
            .await
            .map_err(|e| Error::Internal(format!("replace acp session id begin failed: {e}")))?;

        let body_result = async {
            let row = sqlx::query(
                "SELECT acp_session_id, token_usage, token_usage_baseline FROM agent_session \
                 WHERE id=? AND workspace_id=?",
            )
            .bind(&id.0)
            .bind(&workspace_id.0)
            .fetch_optional(&mut *conn)
            .await
            .map_err(|e| Error::Internal(format!("replace acp session id read failed: {e}")))?;
            let stored_id = row
                .as_ref()
                .and_then(|r| r.get::<Option<String>, _>("acp_session_id"));
            if stored_id.as_deref() != expected_old {
                // The stored id changed between the caller's CAS read and this
                // transaction: treat it as a CAS loss and keep the canonical
                // value (falling back to the fresh id only if the row
                // vanished). Nothing has been written, so committing below is
                // a no-op close of a read-only transaction.
                return Ok(stored_id.unwrap_or_else(|| acp_session_id.to_string()));
            }
            let (snapshot, baseline): (Option<TokenUsageTotals>, Option<TokenUsageTotals>) = row
                .map_or((None, None), |r| {
                    (
                        r.get::<Option<String>, _>("token_usage")
                            .and_then(|s| serde_json::from_str(&s).ok()),
                        r.get::<Option<String>, _>("token_usage_baseline")
                            .and_then(|s| serde_json::from_str(&s).ok()),
                    )
                });
            let folded = match (&baseline, &snapshot) {
                (None, None) => None,
                (b, s) => {
                    let b = b.clone().unwrap_or_default();
                    let s = s.clone().unwrap_or_default();
                    Some(TokenUsageTotals {
                        input_tokens: b.input_tokens.saturating_add(s.input_tokens),
                        output_tokens: b.output_tokens.saturating_add(s.output_tokens),
                        cache_read_tokens: b.cache_read_tokens.saturating_add(s.cache_read_tokens),
                        cache_creation_tokens: b
                            .cache_creation_tokens
                            .saturating_add(s.cache_creation_tokens),
                        thought_tokens: b.thought_tokens.saturating_add(s.thought_tokens),
                        // Cost is cumulative per ACP session exactly like the
                        // counters, so the fold banks it the same way (§5.23).
                        cost: UsageCost::merge(b.cost.as_ref(), s.cost.as_ref()),
                    })
                }
            };
            let folded_json = folded
                .as_ref()
                .map(serde_json::to_string)
                .transpose()
                .map_err(|e| Error::Internal(format!("encode token_usage_baseline failed: {e}")))?;
            sqlx::query(
                "UPDATE agent_session SET acp_session_id=?, token_usage_baseline=?, \
                 token_usage=NULL WHERE id=? AND workspace_id=?",
            )
            .bind(acp_session_id)
            .bind(folded_json)
            .bind(&id.0)
            .bind(&workspace_id.0)
            .execute(&mut *conn)
            .await
            .map_err(|e| Error::Internal(format!("replace acp session id failed: {e}")))?;
            Ok(acp_session_id.to_string())
        }
        .await;

        crate::commit_with_rollback_guard(conn, body_result, "replace acp session id commit failed")
            .await
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
    map_session_row_with_heavy_cols(
        row,
        col(row, "system_prompt")?,
        json_col_from_db(col(row, "image_blocks")?, "image_blocks")?,
    )
}

fn map_session_summary_row(row: &SqliteRow) -> Result<AgentSession> {
    map_session_row_with_heavy_cols(row, None, None)
}

fn map_session_row_with_heavy_cols(
    row: &SqliteRow,
    system_prompt: Option<String>,
    image_blocks: Option<serde_json::Value>,
) -> Result<AgentSession> {
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
        reasoning_effort: col(row, "reasoning_effort")?,
        effort_levels: effort_levels_from_db(col(row, "effort_levels")?)?,
        provider: col(row, "provider")?,
        specialist: col(row, "specialist")?,
        status: enum_from_db::<AgentStatus>(&col::<String>(row, "status")?)?,
        is_active: col::<i64>(row, "is_active")? != 0,
        system_prompt,
        // Loaded separately by the caller; derived `stats` is never persisted.
        messages: Vec::new(),
        stats: None,
        task_note_id: task_note.map(NoteId::from),
        skip_auto_commit: col::<i64>(row, "skip_auto_commit")? != 0,
        completion_report: col(row, "completion_report")?,
        completion_report_timestamp: col(row, "completion_report_timestamp")?,
        attention_request_kind: col(row, "attention_request_kind")?,
        attention_request_reason: col(row, "attention_request_reason")?,
        attention_request_timestamp: col(row, "attention_request_timestamp")?,
        delegation_depth: col(row, "delegation_depth")?,
        initial_message: col(row, "initial_message")?,
        context_references: json_col_from_db(
            col(row, "context_references")?,
            "context_references",
        )?,
        image_blocks,
        file_blocks: json_col_from_db(col(row, "file_blocks")?, "file_blocks")?,
        is_background: col::<i64>(row, "is_background")? != 0,
        metadata,
        stop_reason: col(row, "stop_reason")?,
        stop_reason_timestamp: col(row, "stop_reason_timestamp")?,
        // Derived on emit by the service layer (monorepo#940); never persisted.
        session_corrupted: false,
        pending_delete_at: None,
        harness_version: col(row, "harness_version")?,
        harness_features: json_col_from_db(col(row, "harness_features")?, "harness_features")?,
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

/// Insert-side superset of [`MESSAGE_COLUMNS`]: message writes also persist
/// the write-time image `thumbnails` map (0097, slim projection). Reads keep
/// selecting [`MESSAGE_COLUMNS`] only — the full-fidelity read path never
/// pays for the column; slim reads fetch it separately via
/// [`Store::get_agent_message_thumbnails_page`].
const MESSAGE_INSERT_COLUMNS: &str =
    "id, agent_id, seq, role, content, metadata, created_at, thumbnails";

/// Serialize the write-time thumbnail map for `content` (0097), or `None`
/// when the message needs none. Generation failure inside is non-fatal
/// (logged per block); a serialization failure of the map itself is also
/// non-fatal — thumbnails are an optimization, never worth failing the
/// message write.
///
/// Generation decodes + downscales + re-encodes the image (hundreds of ms of
/// CPU for a multi-MB screenshot), so callers MUST await this BEFORE opening
/// the write transaction / entering the `with_write_txn_retry` closure — never
/// inside — and the work itself runs on a blocking thread, off the tokio
/// worker. The cheap `needs_thumbnails` pre-check keeps the common
/// no-oversized-image message free of the clone + thread hop.
async fn thumbnails_col_value(content: &serde_json::Value) -> Option<String> {
    if !crate::message_thumbnails::needs_thumbnails(content) {
        return None;
    }
    let owned = content.clone();
    let map = match tokio::task::spawn_blocking(move || {
        crate::message_thumbnails::generate_message_thumbnails(&owned)
    })
    .await
    {
        Ok(map) => map?,
        Err(e) => {
            tracing::warn!(error = %e, "thumbnail generation task failed; persisting none");
            return None;
        }
    };
    match serde_json::to_string(&map) {
        Ok(s) => Some(s),
        Err(e) => {
            tracing::warn!(error = %e, "encode message thumbnails failed; persisting none");
            None
        }
    }
}

/// [`thumbnails_col_value`] for a whole batch, positionally aligned with
/// `messages`. Awaited before the batch write transaction opens, so a
/// SQLITE_BUSY retry re-runs only the SQL, never the image work.
async fn batch_thumbnails_col_values(messages: &[OwnedBatchMessage]) -> Vec<Option<String>> {
    let mut out = Vec::with_capacity(messages.len());
    for (_, content, _, _) in messages {
        out.push(thumbnails_col_value(content).await);
    }
    out
}

/// SQL scalar expression extracting the searchable plain text of an
/// `agent_message` row (aliased `m`) for the `agent_message_fts` index
/// (0074). Mirrors the search-side `message_text` extraction
/// (intent-services `search_ops`) so index and preview agree: a bare JSON
/// string is used as-is, an array of content blocks contributes each block's
/// string `text` field joined by single spaces (order pinned to the array
/// index via aggregate `ORDER BY`, SQLite 3.44+), and any other shape falls
/// back to its compact JSON encoding. The `json_valid` guard keeps non-JSON
/// content (impossible from the store's serde-encoded write paths) from
/// erroring the statement. The 0074 migration's triggers/backfill embed the
/// same expression; keep them in sync.
const MESSAGE_FTS_TEXT_SQL: &str = "CASE \
    WHEN json_valid(m.content) = 0 THEN m.content \
    WHEN json_type(m.content) = 'text' THEN m.content ->> '$' \
    WHEN json_type(m.content) = 'array' THEN COALESCE(\
        (SELECT group_concat(m.content ->> (je.fullkey || '.text'), ' ' ORDER BY je.key) \
           FROM json_each(m.content) AS je \
          WHERE json_type(m.content, je.fullkey || '.text') = 'text'), '') \
    ELSE json(m.content) END";

/// `search.messages` `preferWorkspaceId` ranking boost, in bm25 units,
/// subtracted from the bm25 rank (lower = better) of matches owned by the
/// preferred workspace. Large enough to lift a preferred-workspace match above
/// equally-relevant (and modestly better-scoring) matches elsewhere, small
/// enough that a decisively better match from another workspace still wins.
const PREFER_WORKSPACE_BOOST: f64 = 1.0;

/// `search.messages` archived-workspace ranking penalty, in bm25 units,
/// added to the bm25 rank (lower = better) of matches owned by an archived
/// workspace. Mirrors [`PREFER_WORKSPACE_BOOST`] so equally-relevant matches
/// tier as preferred workspace → other active workspaces → archived
/// workspaces, while a decisively better match from an archived workspace
/// still wins.
const ARCHIVED_WORKSPACE_PENALTY: f64 = 1.0;

/// One `search.messages` hit from [`Store::search_agent_messages_fts`]: the
/// message/agent ids and result-row context (owning workspace, agent display
/// name, role, decoded content, timestamp) plus the adjusted bm25 rank
/// (lower = more relevant).
#[derive(Debug, Clone)]
pub struct MessageFtsMatch {
    pub message_id: String,
    pub agent_id: String,
    pub workspace_id: String,
    pub agent_name: String,
    pub role: String,
    pub content: serde_json::Value,
    pub created_at: String,
    pub rank: f64,
}

impl Store {
    /// Rebuild the `agent_message_fts` full-text index (0074) from scratch:
    /// delete-all, then re-insert the extracted text of every
    /// user/assistant `agent_message` row keyed by its current rowid.
    ///
    /// The index is trigger-maintained on every message write, so this is
    /// only needed after an operation that renumbers `agent_message`'s
    /// implicit rowids — in practice the one-time activation `VACUUM` in
    /// [`Store::activate_incremental_vacuum`] (`agent_message` has a TEXT
    /// primary key, so `VACUUM` may reassign its rowids and silently desync
    /// the rowid-keyed index). Runs in one write transaction.
    pub(crate) async fn rebuild_agent_message_fts(&self) -> Result<()> {
        let pool = self.write_pool();
        crate::with_write_txn_retry(|| async {
            let mut tx = pool
                .begin()
                .await
                .map_err(|e| Error::Internal(format!("fts rebuild begin failed: {e}")))?;
            sqlx::query("INSERT INTO agent_message_fts(agent_message_fts) VALUES('delete-all')")
                .execute(&mut *tx)
                .await
                .map_err(|e| Error::Internal(format!("fts rebuild delete-all failed: {e}")))?;
            let backfill = format!(
                "INSERT INTO agent_message_fts(rowid, text) \
                 SELECT m.rowid, {MESSAGE_FTS_TEXT_SQL} FROM agent_message m \
                 WHERE m.role IN ('user', 'assistant')"
            );
            sqlx::query(&backfill)
                .execute(&mut *tx)
                .await
                .map_err(|e| Error::Internal(format!("fts rebuild backfill failed: {e}")))?;
            tx.commit()
                .await
                .map_err(|e| Error::Internal(format!("fts rebuild commit failed: {e}")))?;
            Ok(())
        })
        .await
    }

    /// `search.messages` backing query: bm25-ranked hits from the
    /// `agent_message_fts` index (0074), joined back to `agent_message` /
    /// `agent_session` for the ids and result-row context. `match_expr` must
    /// already be valid FTS5 query syntax (callers sanitize user input —
    /// see `intent_search::fts_match_expr`). `workspace_id` is a hard scope
    /// filter (`None` → all workspaces); `agent_id`/`role` narrow further.
    /// `prefer_workspace_id` is a soft ranking boost: matches from that
    /// workspace get [`PREFER_WORKSPACE_BOOST`] subtracted from their bm25
    /// rank (lower = better), so they outrank equally-relevant matches from
    /// other workspaces without excluding anyone. Matches owned by an
    /// archived workspace get [`ARCHIVED_WORKSPACE_PENALTY`] added to their
    /// rank regardless of `prefer_workspace_id`, tiering equally-relevant
    /// matches preferred → other active → archived — both adjustments are
    /// soft, so a decisively better bm25 match still wins. Rows order by
    /// adjusted rank, then newest-first, one row per matching message.
    /// `limit` `None` → no cap.
    pub async fn search_agent_messages_fts(
        &self,
        match_expr: &str,
        workspace_id: Option<&WorkspaceId>,
        agent_id: Option<&str>,
        role: Option<&str>,
        prefer_workspace_id: Option<&WorkspaceId>,
        limit: Option<i64>,
    ) -> Result<Vec<MessageFtsMatch>> {
        let rows = sqlx::query(
            "SELECT m.id AS message_id, m.agent_id, m.role, m.content, m.created_at, \
                    s.workspace_id, s.name AS agent_name, \
                    bm25(agent_message_fts) \
                      - (CASE WHEN s.workspace_id = ? THEN ? ELSE 0.0 END) \
                      + (CASE WHEN w.archived <> 0 THEN ? ELSE 0.0 END) AS adjusted_rank \
             FROM agent_message_fts \
             JOIN agent_message m ON m.rowid = agent_message_fts.rowid \
             JOIN agent_session s ON s.id = m.agent_id \
             JOIN workspace w ON w.id = s.workspace_id \
             WHERE agent_message_fts MATCH ? \
               AND (? IS NULL OR s.workspace_id = ?) \
               AND (? IS NULL OR m.agent_id = ?) \
               AND (? IS NULL OR m.role = ?) \
             ORDER BY adjusted_rank ASC, m.created_at DESC, m.id ASC \
             LIMIT ?",
        )
        .bind(prefer_workspace_id.map(|w| w.0.as_str()))
        .bind(PREFER_WORKSPACE_BOOST)
        .bind(ARCHIVED_WORKSPACE_PENALTY)
        .bind(match_expr)
        .bind(workspace_id.map(|w| w.0.as_str()))
        .bind(workspace_id.map(|w| w.0.as_str()))
        .bind(agent_id)
        .bind(agent_id)
        .bind(role)
        .bind(role)
        .bind(limit.map_or(-1, |n| n.max(0)))
        .fetch_all(self.read_pool())
        .await
        .map_err(|e| Error::Internal(format!("search agent messages failed: {e}")))?;
        rows.iter()
            .map(|row| {
                let content: serde_json::Value =
                    serde_json::from_str(&col::<String>(row, "content")?).map_err(|e| {
                        Error::Internal(format!("decode message content failed: {e}"))
                    })?;
                Ok(MessageFtsMatch {
                    message_id: col(row, "message_id")?,
                    agent_id: col(row, "agent_id")?,
                    workspace_id: col(row, "workspace_id")?,
                    agent_name: col(row, "agent_name")?,
                    role: col(row, "role")?,
                    content,
                    created_at: col(row, "created_at")?,
                    rank: col(row, "adjusted_rank")?,
                })
            })
            .collect()
    }

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

    /// Append a message with a caller-supplied `metadata` payload
    /// (`agent.sendMessage`'s `messageMetadata`, PROTOCOL §5.5). The metadata is
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
    /// The next-seq SELECT runs first in autocommit mode; the message INSERT
    /// then commits together with the matching `agent_session` last-message
    /// preview column update (0066) — plus `last_message_role` (0070) and
    /// `last_message_id` (0088) for user/assistant appends — in one write
    /// transaction — `user` and
    /// `assistant` appends are by construction the session's newest message of
    /// their role, so the columns are overwritten unconditionally (other roles
    /// keep the bare INSERT). The schema enforces `UNIQUE(agent_id, seq)`, so
    /// concurrent appends racing on the SELECT phase will cause one INSERT to
    /// fail with a constraint violation.
    ///
    /// **Crash safety**: Because `seq` is computed as `COALESCE(MAX(seq), -1) + 1`
    /// rather than a persisted counter, a crash between SELECT and INSERT does
    /// NOT create a durable gap — the next caller recomputes seq from the same
    /// MAX. Only the INSERT transaction commits data, so once it completes the
    /// message row (and its preview column) is durable. No committed message can
    /// be lost. Assistant-message append
    /// (the streaming path) is additionally protected by the AgentManager's
    /// per-agent single-flight slot, serializing turns for one agent and
    /// eliminating the seq-race window on that hot path. User-message appends
    /// (sendMessage, sendQueuedMessageNow, wake delivery) can still race if fired
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
        let preview_update = match role {
            "assistant" => Some(("last_assistant_preview", preview_col_value(role, content)?)),
            "user" => Some(("last_user_preview", preview_col_value(role, content)?)),
            _ => None,
        };
        // 0098: the newest user/assistant message's lastToolUse preview —
        // NULL actively clears a stale preview when this newest message
        // carries no tool_use block. Only computed alongside a preview
        // update (system/tool rows are transparent).
        let last_tool_use = match &preview_update {
            Some(_) => last_tool_use_col_value(content)?,
            None => None,
        };
        // Awaited before any write transaction below opens: thumbnail
        // generation is CPU-bound image work and runs on a blocking thread.
        let thumbnails_json = thumbnails_col_value(content).await;
        let sql = format!(
            "INSERT INTO agent_message ({MESSAGE_INSERT_COLUMNS}) VALUES (?,?,?,?,?,?,?,?)"
        );
        match &preview_update {
            None => {
                sqlx::query(&sql)
                    .bind(id)
                    .bind(&agent_id.0)
                    .bind(seq)
                    .bind(role)
                    .bind(&content_json)
                    .bind(metadata_json.as_deref())
                    .bind(created_at)
                    .bind(thumbnails_json.as_deref())
                    .execute(self.write_pool())
                    .await
                    .map_err(|e| Error::Internal(format!("append agent message failed: {e}")))?;
            }
            Some((column, value)) => {
                let pool = self.write_pool();
                // A user/assistant append is by construction the session's
                // newest message of any role, so `last_message_role` (0070),
                // `last_message_id` (0088), and `last_tool_use_preview`
                // (0098) are overwritten unconditionally alongside the
                // preview.
                let update_sql = format!(
                    "UPDATE agent_session SET {column} = ?, last_message_role = ?, \
                     last_message_id = ?, last_tool_use_preview = ? WHERE id = ?"
                );
                crate::with_write_txn_retry(|| async {
                    let mut tx = pool.begin().await.map_err(|e| {
                        Error::Internal(format!("append agent message begin failed: {e}"))
                    })?;
                    sqlx::query(&sql)
                        .bind(id)
                        .bind(&agent_id.0)
                        .bind(seq)
                        .bind(role)
                        .bind(&content_json)
                        .bind(metadata_json.as_deref())
                        .bind(created_at)
                        .bind(thumbnails_json.as_deref())
                        .execute(&mut *tx)
                        .await
                        .map_err(|e| {
                            Error::Internal(format!("append agent message failed: {e}"))
                        })?;
                    sqlx::query(&update_sql)
                        .bind(value.as_str())
                        .bind(role)
                        .bind(id)
                        .bind(last_tool_use.as_deref())
                        .bind(&agent_id.0)
                        .execute(&mut *tx)
                        .await
                        .map_err(|e| {
                            Error::Internal(format!("update session message preview failed: {e}"))
                        })?;
                    tx.commit().await.map_err(|e| {
                        Error::Internal(format!("append agent message commit failed: {e}"))
                    })?;
                    Ok(())
                })
                .await?;
            }
        }
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

    /// The newest non-`system` message of an agent's log, hydrated as a
    /// single row — the question-hold tail anchor (PROTOCOL §5.5). Trailing
    /// `system` rows are skipped inside SQL (a backwards walk over the
    /// `UNIQUE(agent_id, seq)` index), so per-call cost is one statement and
    /// at most ONE decoded message regardless of transcript size — the
    /// service-layer alternative pages full rows (content blobs included)
    /// back until it finds a non-system row. `None` when the log is empty or
    /// all-system.
    pub async fn get_last_non_system_message(
        &self,
        agent_id: &AgentId,
    ) -> Result<Option<AgentMessage>> {
        let sql = format!(
            "SELECT {MESSAGE_COLUMNS} FROM agent_message \
             WHERE agent_id = ? AND role != 'system' ORDER BY seq DESC LIMIT 1"
        );
        let row = sqlx::query(&sql)
            .bind(&agent_id.0)
            .fetch_optional(self.read_pool())
            .await
            .map_err(|e| Error::Internal(format!("get last non-system message failed: {e}")))?;
        row.as_ref().map(map_message_row).transpose()
    }

    /// One message of an agent's log by row id, hydrated as a single row —
    /// the pending-questions marker resolver (PROTOCOL §5.5). One statement
    /// over the primary key, at most ONE decoded message regardless of
    /// transcript size. `None` when the id is unknown or belongs to a
    /// different agent.
    pub async fn get_agent_message_by_id(
        &self,
        agent_id: &AgentId,
        message_id: &str,
    ) -> Result<Option<AgentMessage>> {
        let sql =
            format!("SELECT {MESSAGE_COLUMNS} FROM agent_message WHERE agent_id = ? AND id = ?");
        let row = sqlx::query(&sql)
            .bind(&agent_id.0)
            .bind(message_id)
            .fetch_optional(self.read_pool())
            .await
            .map_err(|e| Error::Internal(format!("get agent message by id failed: {e}")))?;
        row.as_ref().map(map_message_row).transpose()
    }

    /// Number of live delegated children of `parent_agent_id`: sessions
    /// carrying this `parent_agent_id` whose status is still non-terminal
    /// (not `Completed`/`error`/`deleted`). Deliberately UNSCOPED by
    /// workspace — a Chief parent can delegate into another workspace
    /// (`agent.delegate` cross-workspace), and `parent_agent_id` is globally
    /// unique. One aggregate statement over `idx_agent_parent`, so cost is
    /// O(this agent's children), never O(workspace sessions). Backs the
    /// `runningSubAgents` snapshot field.
    pub async fn count_unsettled_child_agents(&self, parent_agent_id: &AgentId) -> Result<u64> {
        let terminal = [
            AgentStatus::Completed,
            AgentStatus::Error,
            AgentStatus::Deleted,
        ]
        .iter()
        .map(enum_to_db)
        .collect::<Result<Vec<_>>>()?;
        let n: i64 = sqlx::query(
            "SELECT COUNT(*) AS n FROM agent_session \
             WHERE parent_agent_id = ? AND status NOT IN (?, ?, ?)",
        )
        .bind(&parent_agent_id.0)
        .bind(&terminal[0])
        .bind(&terminal[1])
        .bind(&terminal[2])
        .fetch_one(self.read_pool())
        .await
        .map_err(|e| Error::Internal(format!("count unsettled child agents failed: {e}")))?
        .get::<i64, _>("n");
        Ok(n as u64)
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

    /// 0-based chronological (`seq` ascending) position of one message within
    /// an agent's log — the `aroundMessageId` seek anchor for
    /// `agent.getConversation` (PROTOCOL §5.5). Metadata-only: counts earlier
    /// rows over the `UNIQUE(agent_id, seq)` index without fetching or
    /// decoding any `content`. An unknown message id (or a row belonging to a
    /// different agent) resolves to `None`.
    pub async fn get_agent_message_index(
        &self,
        agent_id: &AgentId,
        message_id: &str,
    ) -> Result<Option<i64>> {
        let row = sqlx::query(
            "SELECT (SELECT COUNT(*) FROM agent_message p \
             WHERE p.agent_id = m.agent_id AND p.seq < m.seq) AS idx \
             FROM agent_message m WHERE m.agent_id = ? AND m.id = ?",
        )
        .bind(&agent_id.0)
        .bind(message_id)
        .fetch_optional(self.read_pool())
        .await
        .map_err(|e| Error::Internal(format!("get agent message index failed: {e}")))?;
        Ok(row.map(|r| r.get::<i64, _>("idx")))
    }

    /// Read one offset window of an agent's messages in chronological (`seq`
    /// ascending) order — the rows at positions `offset..offset+limit` counted
    /// from the oldest message, matching a `start..end` window from the
    /// service-layer pagination contract (`page_window`). Unlike
    /// [`Store::get_agent_messages`], which hydrates the whole log before the
    /// caller slices it, this selects and decodes only the requested page.
    /// Out-of-range windows (offset at/past the end, or an empty log) return
    /// an empty vec. Negative inputs are clamped to zero.
    pub async fn get_agent_messages_page(
        &self,
        agent_id: &AgentId,
        offset: i64,
        limit: i64,
    ) -> Result<Vec<AgentMessage>> {
        let sql = format!(
            "SELECT {MESSAGE_COLUMNS} FROM agent_message WHERE agent_id = ? \
             ORDER BY seq ASC LIMIT ? OFFSET ?"
        );
        let rows = sqlx::query(&sql)
            .bind(&agent_id.0)
            .bind(limit.max(0))
            .bind(offset.max(0))
            .fetch_all(self.read_pool())
            .await
            .map_err(|e| Error::Internal(format!("get agent messages page failed: {e}")))?;
        rows.iter().map(map_message_row).collect()
    }

    /// Read the persisted image-thumbnail maps (0097) for the message ids in
    /// `message_ids`, keyed by message id. Only rows with a non-NULL
    /// `thumbnails` column appear in the result — the common all-text page
    /// returns an empty map. One bounded SELECT sized by the page (slim reads
    /// only; the full-fidelity read path never calls this). Each value is the
    /// stored JSON map `{"<image ordinal>": {"data", "mimeType"}}`; a row
    /// whose stored JSON fails to decode is skipped with a WARN (slim reads
    /// then degrade to data-omitted flags, same as a legacy row).
    pub async fn get_agent_message_thumbnails(
        &self,
        agent_id: &AgentId,
        message_ids: &[String],
    ) -> Result<std::collections::HashMap<String, serde_json::Value>> {
        if message_ids.is_empty() {
            return Ok(std::collections::HashMap::new());
        }
        let placeholders = vec!["?"; message_ids.len()].join(",");
        let sql = format!(
            "SELECT id, thumbnails FROM agent_message \
             WHERE agent_id = ? AND thumbnails IS NOT NULL AND id IN ({placeholders})"
        );
        let mut query = sqlx::query(&sql).bind(&agent_id.0);
        for id in message_ids {
            query = query.bind(id);
        }
        let rows = query
            .fetch_all(self.read_pool())
            .await
            .map_err(|e| Error::Internal(format!("get agent message thumbnails failed: {e}")))?;
        let mut map = std::collections::HashMap::with_capacity(rows.len());
        for row in &rows {
            let id: String = col(row, "id")?;
            let raw: String = col(row, "thumbnails")?;
            match serde_json::from_str::<serde_json::Value>(&raw) {
                Ok(v) => {
                    map.insert(id, v);
                }
                Err(e) => {
                    tracing::warn!(
                        message = %id,
                        error = %e,
                        "decode message thumbnails failed; serving block with data omitted"
                    );
                }
            }
        }
        Ok(map)
    }

    /// Atomically clear the agent's message log and reinsert `messages` under
    /// fresh 0-based monotonic `seq` values. Row ids are minted here (UUIDv7)
    /// so callers cannot smuggle stale ids across the swap; the returned
    /// [`AgentMessage`]s carry the new id/`seq` pairing. Used by the FE's
    /// edit-truncate transcript-mutation path (`agent.replaceMessages`,
    /// PROTOCOL §5.5). Callers are expected to reject busy sessions before
    /// invoking this (message-log mutations must not race an in-flight turn).
    /// Both `agent_session` last-message preview columns (0066),
    /// `last_message_role` (0070), and `last_message_id` (0088) are
    /// recomputed from the replacement batch inside the same transaction
    /// (NULL when the batch has no message of that role / no user/assistant
    /// message).
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
        let (assistant_preview, user_preview, last_message_role, last_tool_use) =
            batch_preview_col_values(&owned_messages)?;
        // Thumbnail generation (CPU-bound image work) runs BEFORE the write
        // transaction opens — never inside it or the retry closure.
        let thumbnails = batch_thumbnails_col_values(&owned_messages).await;

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
            let mut last_message_id: Option<String> = None;
            let insert_sql = format!(
                "INSERT INTO agent_message ({MESSAGE_INSERT_COLUMNS}) VALUES (?,?,?,?,?,?,?,?)"
            );
            for (idx, (role, content, metadata, created_at)) in owned_messages.iter().enumerate() {
                let seq = idx as i64;
                let id = Uuid::now_v7().to_string();
                if role == "user" || role == "assistant" {
                    last_message_id = Some(id.clone());
                }
                let content_json = serde_json::to_string(content).map_err(|e| {
                    Error::Internal(format!("encode replaced message content failed: {e}"))
                })?;
                let metadata_json = match metadata {
                    Some(md) => Some(serde_json::to_string(md).map_err(|e| {
                        Error::Internal(format!("encode replaced message metadata failed: {e}"))
                    })?),
                    None => None,
                };
                let thumbnails_json = thumbnails[idx].as_deref();
                sqlx::query(&insert_sql)
                    .bind(&id)
                    .bind(&agent_id.0)
                    .bind(seq)
                    .bind(role)
                    .bind(&content_json)
                    .bind(metadata_json.as_deref())
                    .bind(created_at)
                    .bind(thumbnails_json)
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
            sqlx::query(
                "UPDATE agent_session SET last_assistant_preview = ?, last_user_preview = ?, \
                 last_message_role = ?, last_message_id = ?, last_tool_use_preview = ? \
                 WHERE id = ?",
            )
            .bind(assistant_preview.as_deref())
            .bind(user_preview.as_deref())
            .bind(last_message_role.as_deref())
            .bind(last_message_id.as_deref())
            .bind(last_tool_use.as_deref())
            .bind(&agent_id.0)
            .execute(&mut *tx)
            .await
            .map_err(|e| Error::Internal(format!("update session message previews failed: {e}")))?;
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
        self.insert_interrupted_agent_with_reason(
            agent_id,
            workspace_id,
            prev_status,
            interrupted_at,
            None,
        )
        .await
    }

    /// Like [`insert_interrupted_agent`], but tags the row with a
    /// machine-readable interruption `reason` (the `InterruptReason` wire
    /// string). The wake-resume orchestrator enumerates only `system_suspend`
    /// rows, so the sleep-induced enrollment path supplies `Some("system_suspend")`
    /// while the daemon-restart / heal paths pass `None` (untouched by
    /// auto-resume). The idempotent upsert refreshes the reason too, so a row
    /// re-enrolled under a new reason carries the latest one.
    pub async fn insert_interrupted_agent_with_reason(
        &self,
        agent_id: &AgentId,
        workspace_id: &WorkspaceId,
        prev_status: &str,
        interrupted_at: &str,
        reason: Option<&str>,
    ) -> Result<bool> {
        let sql =
            "INSERT INTO interrupted_agent (agent_id, workspace_id, prev_status, interrupted_at, reason) \
                   VALUES (?, ?, ?, ?, ?) \
                   ON CONFLICT(agent_id) DO UPDATE SET \
                       prev_status = excluded.prev_status, \
                       interrupted_at = excluded.interrupted_at, \
                       reason = excluded.reason, \
                       resolution = 'pending', \
                       resolved_at = NULL";
        let res = sqlx::query(sql)
            .bind(&agent_id.0)
            .bind(&workspace_id.0)
            .bind(prev_status)
            .bind(interrupted_at)
            .bind(reason)
            .execute(self.write_pool())
            .await
            .map_err(|e| Error::Internal(format!("insert interrupted_agent failed: {e}")))?;
        Ok(res.rows_affected() > 0)
    }

    /// List pending interrupted agents, joined with agent_session (name) and
    /// workspace (title). Sessions deleted since interruption are excluded (INNER JOIN).
    pub async fn list_interrupted_agents(&self) -> Result<Vec<InterruptedAgent>> {
        let sql = "SELECT ia.agent_id, ia.workspace_id, ia.prev_status, ia.interrupted_at, \
                          ia.reason, ag.name AS agent_name, w.title AS workspace_name \
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
                    reason: col(row, "reason")?,
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
                          ia.reason, ag.name AS agent_name, w.title AS workspace_name \
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
                reason: col(r, "reason")?,
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

    /// A unique temp DB path whose `.db`/`-wal`/`-shm` files are removed on
    /// drop, including on panic (mirrors `crate::tests::TempDb`, which is
    /// private to that module). Set `INTENTD_TEST_KEEP_TMP` (non-empty) to
    /// keep the files around for debugging. Derefs to the DB path, like
    /// `tempfile::TempPath`.
    struct TempDb {
        path: std::path::PathBuf,
    }

    impl TempDb {
        fn new(tag: &str) -> Self {
            let path = std::env::temp_dir().join(format!("{tag}-{}.db", uuid::Uuid::new_v4()));
            Self { path }
        }
    }

    impl std::ops::Deref for TempDb {
        type Target = std::path::Path;
        fn deref(&self) -> &std::path::Path {
            &self.path
        }
    }

    impl Drop for TempDb {
        fn drop(&mut self) {
            if std::env::var_os("INTENTD_TEST_KEEP_TMP").is_some_and(|v| !v.is_empty()) {
                return;
            }
            for suffix in ["", "-wal", "-shm"] {
                let mut sidecar = self.path.clone().into_os_string();
                sidecar.push(suffix);
                let _ = std::fs::remove_file(&sidecar);
            }
        }
    }

    /// A UNIQUE violation on the session id maps to `Internal` naming the
    /// colliding id. Agent ids are server-minted (`agent-{uuid}`), so a
    /// duplicate insert is a server-side anomaly — never a client params
    /// error — and must not surface the retired "supply a fresh id" guidance.
    #[tokio::test]
    async fn insert_agent_session_duplicate_id_is_internal_error() {
        use intent_core::{
            now_iso, Workspace, WorkspaceActivity, WorkspaceAttention, WorkspaceStatus,
        };

        use uuid::Uuid;
        let tmp = TempDb::new("test-agent-repo");
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
            status_image_asset_id: None,
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
            display_status: None,
            waiting: false,
            checkout_mode: None,
            disk_usage: None,
            pending_delete_at: None,
        };
        store
            .insert_workspace(&workspace)
            .await
            .expect("insert workspace");
        let session = AgentSession {
            harness_version: intent_core::CURRENT_HARNESS_VERSION.to_string(),
            harness_features: None,
            id: AgentId(format!("agent-{}", Uuid::new_v4())),
            workspace_id: ws_id,
            backend_session_id: None,
            acp_session_id: None,
            name: "First".to_string(),
            name_explicitly_set: false,
            model: None,
            reasoning_effort: None,
            effort_levels: None,
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
            attention_request_kind: None,
            attention_request_reason: None,
            attention_request_timestamp: None,
            delegation_depth: None,
            initial_message: None,
            context_references: None,
            image_blocks: None,
            file_blocks: None,
            is_background: false,
            metadata: None,
            sandbox_id: None,
            sandbox_path: None,
            sandbox_branch: None,
            stop_reason: None,
            stop_reason_timestamp: None,
            session_corrupted: false,
            pending_delete_at: None,
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
    }

    /// `set_agent_session_token_usage` replaces (never sums) the persisted
    /// snapshot, `get_workspace_agent_usage_data` surfaces it, and an unknown
    /// session id maps to `NotFound`.
    #[tokio::test]
    async fn token_usage_snapshot_roundtrip_and_replace() {
        use intent_core::{
            now_iso, Workspace, WorkspaceActivity, WorkspaceAttention, WorkspaceStatus,
        };

        use uuid::Uuid;
        let tmp = TempDb::new("test-agent-repo");
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
            status_image_asset_id: None,
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
            display_status: None,
            waiting: false,
            checkout_mode: None,
            disk_usage: None,
            pending_delete_at: None,
        };
        store
            .insert_workspace(&workspace)
            .await
            .expect("insert workspace");
        let agent_id = AgentId(format!("agent-{}", Uuid::new_v4()));
        let session = AgentSession {
            harness_version: intent_core::CURRENT_HARNESS_VERSION.to_string(),
            harness_features: None,
            id: agent_id.clone(),
            workspace_id: ws_id.clone(),
            backend_session_id: None,
            acp_session_id: None,
            name: "Usage".to_string(),
            name_explicitly_set: false,
            model: Some("opus-4.8".to_string()),
            reasoning_effort: None,
            effort_levels: None,
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
            attention_request_kind: None,
            attention_request_reason: None,
            attention_request_timestamp: None,
            delegation_depth: None,
            initial_message: None,
            context_references: None,
            image_blocks: None,
            file_blocks: None,
            is_background: false,
            metadata: None,
            sandbox_id: None,
            sandbox_path: None,
            sandbox_branch: None,
            stop_reason: None,
            stop_reason_timestamp: None,
            session_corrupted: false,
            pending_delete_at: None,
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
            thought_tokens: 0,
            cost: None,
        };
        store
            .set_agent_session_token_usage(&ws_id, &agent_id, &first)
            .await
            .expect("set first");
        let second = TokenUsageTotals {
            input_tokens: 100,
            output_tokens: 80,
            cache_read_tokens: 45,
            cache_creation_tokens: 6,
            thought_tokens: 0,
            cost: None,
        };
        store
            .set_agent_session_token_usage(&ws_id, &agent_id, &second)
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
            .set_agent_session_token_usage(&ws_id, &missing, &first)
            .await
            .expect_err("unknown session rejected");
        assert!(matches!(err, Error::NotFound(_)), "got {err:?}");

        // Workspace mismatch → NotFound (defense-in-depth scoping).
        let other_ws = WorkspaceId("ws-other".to_string());
        let err = store
            .set_agent_session_token_usage(&other_ws, &agent_id, &first)
            .await
            .expect_err("cross-workspace write rejected");
        assert!(matches!(err, Error::NotFound(_)), "got {err:?}");
    }

    /// Minimal workspace literal for the baseline-fold tests below.
    fn baseline_test_workspace(ws_id: &WorkspaceId, ts: &str) -> intent_core::Workspace {
        use intent_core::{Workspace, WorkspaceActivity, WorkspaceAttention, WorkspaceStatus};
        Workspace {
            id: ws_id.clone(),
            title: "Test".to_string(),
            branch: "main".to_string(),
            base_ref: None,
            base_commit_sha: None,
            status: WorkspaceStatus::Active,
            status_message: None,
            status_image_asset_id: None,
            activity: WorkspaceActivity::Idle,
            attention: WorkspaceAttention::None,
            created_at: ts.to_string(),
            updated_at: ts.to_string(),
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
            display_status: None,
            waiting: false,
            checkout_mode: None,
            disk_usage: None,
            pending_delete_at: None,
        }
    }

    /// Minimal session literal for the baseline-fold tests below.
    fn baseline_test_session(
        agent_id: &AgentId,
        ws_id: &WorkspaceId,
        ts: &str,
        acp_session_id: Option<&str>,
    ) -> AgentSession {
        AgentSession {
            harness_version: intent_core::CURRENT_HARNESS_VERSION.to_string(),
            harness_features: None,
            id: agent_id.clone(),
            workspace_id: ws_id.clone(),
            backend_session_id: None,
            acp_session_id: acp_session_id.map(str::to_string),
            name: "Baseline".to_string(),
            name_explicitly_set: false,
            model: Some("opus-4.8".to_string()),
            reasoning_effort: None,
            effort_levels: None,
            provider: None,
            status: AgentStatus::Idle,
            is_active: false,
            system_prompt: None,
            created_at: ts.to_string(),
            updated_at: ts.to_string(),
            messages: vec![],
            parent_agent_id: None,
            specialist: None,
            task_note_id: None,
            skip_auto_commit: false,
            stats: None,
            completion_report: None,
            completion_report_timestamp: None,
            attention_request_kind: None,
            attention_request_reason: None,
            attention_request_timestamp: None,
            delegation_depth: None,
            initial_message: None,
            context_references: None,
            image_blocks: None,
            file_blocks: None,
            is_background: false,
            metadata: None,
            sandbox_id: None,
            sandbox_path: None,
            sandbox_branch: None,
            stop_reason: None,
            stop_reason_timestamp: None,
            session_corrupted: false,
            pending_delete_at: None,
        }
    }

    /// Write-time thumbnails (0097): appending a message with an oversized
    /// image block persists a thumbnail map on the row, the page-scoped
    /// getter returns it keyed by message id, and text-only / under-budget
    /// rows persist NULL (absent from the getter's map). Legacy rows
    /// (thumbnails column NULL) are simply absent — the slim read then
    /// serves the block with data omitted.
    #[tokio::test]
    async fn append_persists_image_thumbnails_and_page_getter_reads_them() {
        use intent_core::now_iso;

        let tmp = TempDb::new("test-thumbnails");
        let store = Store::open(&tmp).await.expect("create test store");
        let ts = now_iso();
        let ws_id = WorkspaceId("ws-thumbs".to_string());
        store
            .insert_workspace(&baseline_test_workspace(&ws_id, &ts))
            .await
            .expect("insert workspace");
        let agent_id = AgentId("agent-thumbs".to_string());
        store
            .insert_agent_session(&baseline_test_session(&agent_id, &ws_id, &ts, None))
            .await
            .expect("insert session");

        // A 512x384 noise PNG comfortably exceeds the slim budget.
        let img = image::RgbImage::from_fn(512, 384, |x, y| {
            let v = (x.wrapping_mul(31).wrapping_add(y.wrapping_mul(17)) % 251) as u8;
            image::Rgb([v, v.wrapping_add(97), v.wrapping_add(193)])
        });
        let mut buf = Vec::new();
        image::DynamicImage::ImageRgb8(img)
            .write_to(&mut std::io::Cursor::new(&mut buf), image::ImageFormat::Png)
            .expect("encode test png");
        use base64::Engine as _;
        let data = base64::engine::general_purpose::STANDARD.encode(&buf);

        let with_image = store
            .append_agent_message(
                &agent_id,
                "user",
                &serde_json::json!([
                    { "type": "text", "text": "see screenshot" },
                    { "type": "image", "data": data, "mimeType": "image/png" },
                ]),
                &ts,
            )
            .await
            .expect("append image message");
        let text_only = store
            .append_agent_message(
                &agent_id,
                "assistant",
                &serde_json::json!([{ "type": "text", "text": "looks good" }]),
                &ts,
            )
            .await
            .expect("append text message");

        let ids = vec![with_image.id.clone(), text_only.id.clone()];
        let thumbs = store
            .get_agent_message_thumbnails(&agent_id, &ids)
            .await
            .expect("read thumbnails");
        let entry = thumbs
            .get(&with_image.id)
            .expect("image row has a persisted thumbnail map");
        let thumb = entry.get("0").expect("keyed by image ordinal");
        assert!(thumb
            .get("data")
            .and_then(serde_json::Value::as_str)
            .is_some());
        assert!(
            !thumbs.contains_key(&text_only.id),
            "text-only row persists NULL and is absent from the map"
        );
        assert!(
            store
                .get_agent_message_thumbnails(&agent_id, &[])
                .await
                .expect("empty read")
                .is_empty(),
            "empty id list short-circuits"
        );
    }

    /// The CAS-swap branch of `replace_acp_session_id` folds the current
    /// snapshot into `token_usage_baseline`, clears the snapshot, and swaps
    /// the id (monorepo#737); a second recreate accumulates onto the existing
    /// baseline, and a recreate with no snapshot leaves the baseline as-is.
    #[tokio::test]
    async fn replace_acp_session_id_folds_snapshot_into_baseline() {
        use intent_core::now_iso;

        use uuid::Uuid;
        let tmp = TempDb::new("test-agent-repo");
        let store = Store::open(&tmp).await.expect("create test store");
        let ts = now_iso();
        let ws_id = WorkspaceId("ws-baseline".to_string());
        store
            .insert_workspace(&baseline_test_workspace(&ws_id, &ts))
            .await
            .expect("insert workspace");
        let agent_id = AgentId(format!("agent-{}", Uuid::new_v4()));
        store
            .insert_agent_session(&baseline_test_session(
                &agent_id,
                &ws_id,
                &ts,
                Some("acp-1"),
            ))
            .await
            .expect("insert");
        let snap1 = TokenUsageTotals {
            input_tokens: 100,
            output_tokens: 40,
            cache_read_tokens: 20,
            cache_creation_tokens: 5,
            thought_tokens: 0,
            cost: None,
        };
        store
            .set_agent_session_token_usage(&ws_id, &agent_id, &snap1)
            .await
            .expect("set snapshot");

        // Recreate #1: id swaps, snapshot folds into the (previously NULL)
        // baseline, snapshot clears.
        let canonical = store
            .replace_acp_session_id(&ws_id, &agent_id, "acp-1", "acp-2")
            .await
            .expect("swap");
        assert_eq!(canonical, "acp-2");
        let sess = store.get_agent_session(&agent_id).await.expect("get");
        assert_eq!(sess.acp_session_id.as_deref(), Some("acp-2"));
        let rows = store
            .get_workspace_agent_usage_data(&ws_id)
            .await
            .expect("usage data");
        assert!(rows[0].2.is_none(), "snapshot cleared on swap");
        assert_eq!(
            rows[0].3.as_ref(),
            Some(&snap1),
            "baseline holds old totals"
        );

        // Recreate #2 with a fresh snapshot: baseline accumulates.
        let snap2 = TokenUsageTotals {
            input_tokens: 10,
            output_tokens: 20,
            cache_read_tokens: 30,
            cache_creation_tokens: 40,
            thought_tokens: 0,
            cost: None,
        };
        store
            .set_agent_session_token_usage(&ws_id, &agent_id, &snap2)
            .await
            .expect("set second snapshot");
        let canonical = store
            .replace_acp_session_id(&ws_id, &agent_id, "acp-2", "acp-3")
            .await
            .expect("second swap");
        assert_eq!(canonical, "acp-3");
        let rows = store
            .get_workspace_agent_usage_data(&ws_id)
            .await
            .expect("usage data");
        assert!(rows[0].2.is_none(), "snapshot cleared again");
        assert_eq!(
            rows[0].3.as_ref(),
            Some(&TokenUsageTotals {
                input_tokens: 110,
                output_tokens: 60,
                cache_read_tokens: 50,
                cache_creation_tokens: 45,
                thought_tokens: 0,
                cost: None,
            }),
            "second recreate accumulates onto the baseline"
        );

        // Recreate #3 with NO snapshot: baseline carries over unchanged.
        let canonical = store
            .replace_acp_session_id(&ws_id, &agent_id, "acp-3", "acp-4")
            .await
            .expect("third swap");
        assert_eq!(canonical, "acp-4");
        let rows = store
            .get_workspace_agent_usage_data(&ws_id)
            .await
            .expect("usage data");
        assert!(rows[0].2.is_none());
        assert_eq!(
            rows[0].3.as_ref().map(|t| t.input_tokens),
            Some(110),
            "no snapshot to fold leaves the baseline unchanged"
        );
    }

    /// The CAS-loss branch (stored id diverged from `expected_old`) writes
    /// nothing: id, snapshot, and baseline are all untouched.
    #[tokio::test]
    async fn replace_acp_session_id_cas_loss_does_not_fold() {
        use intent_core::now_iso;

        use uuid::Uuid;
        let tmp = TempDb::new("test-agent-repo");
        let store = Store::open(&tmp).await.expect("create test store");
        let ts = now_iso();
        let ws_id = WorkspaceId("ws-baseline".to_string());
        store
            .insert_workspace(&baseline_test_workspace(&ws_id, &ts))
            .await
            .expect("insert workspace");
        let agent_id = AgentId(format!("agent-{}", Uuid::new_v4()));
        store
            .insert_agent_session(&baseline_test_session(
                &agent_id,
                &ws_id,
                &ts,
                Some("acp-current"),
            ))
            .await
            .expect("insert");
        let snap = TokenUsageTotals {
            input_tokens: 7,
            output_tokens: 8,
            cache_read_tokens: 9,
            cache_creation_tokens: 10,
            thought_tokens: 0,
            cost: None,
        };
        store
            .set_agent_session_token_usage(&ws_id, &agent_id, &snap)
            .await
            .expect("set snapshot");

        let canonical = store
            .replace_acp_session_id(&ws_id, &agent_id, "acp-stale", "acp-x")
            .await
            .expect("cas loss returns canonical id");
        assert_eq!(canonical, "acp-current", "stored id not clobbered");
        let rows = store
            .get_workspace_agent_usage_data(&ws_id)
            .await
            .expect("usage data");
        assert_eq!(rows[0].2.as_ref(), Some(&snap), "snapshot untouched");
        assert!(rows[0].3.is_none(), "baseline untouched on CAS loss");
    }

    /// The nothing-stored branch of `replace_acp_session_id` also writes the
    /// id via the folding helper, while the strict write-once first set
    /// (`set_acp_session_id`) never touches snapshot or baseline.
    #[tokio::test]
    async fn first_set_paths_and_baseline() {
        use intent_core::now_iso;

        use uuid::Uuid;
        let tmp = TempDb::new("test-agent-repo");
        let store = Store::open(&tmp).await.expect("create test store");
        let ts = now_iso();
        let ws_id = WorkspaceId("ws-baseline".to_string());
        store
            .insert_workspace(&baseline_test_workspace(&ws_id, &ts))
            .await
            .expect("insert workspace");
        let snap = TokenUsageTotals {
            input_tokens: 3,
            output_tokens: 4,
            cache_read_tokens: 5,
            cache_creation_tokens: 6,
            thought_tokens: 0,
            cost: None,
        };

        // Write-once first set: snapshot and baseline stay untouched.
        let first_set = AgentId(format!("agent-{}", Uuid::new_v4()));
        store
            .insert_agent_session(&baseline_test_session(&first_set, &ws_id, &ts, None))
            .await
            .expect("insert");
        store
            .set_agent_session_token_usage(&ws_id, &first_set, &snap)
            .await
            .expect("set snapshot");
        store
            .set_acp_session_id(&ws_id, &first_set, "acp-first")
            .await
            .expect("first set");
        let sess = store.get_agent_session(&first_set).await.expect("get");
        assert_eq!(sess.acp_session_id.as_deref(), Some("acp-first"));

        // Replace with nothing stored: sets the id and folds the snapshot.
        let replace_none = AgentId(format!("agent-{}", Uuid::new_v4()));
        store
            .insert_agent_session(&baseline_test_session(&replace_none, &ws_id, &ts, None))
            .await
            .expect("insert");
        store
            .set_agent_session_token_usage(&ws_id, &replace_none, &snap)
            .await
            .expect("set snapshot");
        let canonical = store
            .replace_acp_session_id(&ws_id, &replace_none, "acp-anything", "acp-new")
            .await
            .expect("replace with nothing stored");
        assert_eq!(canonical, "acp-new");

        let rows = store
            .get_workspace_agent_usage_data(&ws_id)
            .await
            .expect("usage data");
        let by_id = |id: &AgentId| rows.iter().find(|r| r.0 == id.0).expect("row");
        let first_row = by_id(&first_set);
        assert_eq!(
            first_row.2.as_ref(),
            Some(&snap),
            "first set keeps snapshot"
        );
        assert!(first_row.3.is_none(), "first set never touches baseline");
        let replace_row = by_id(&replace_none);
        assert!(replace_row.2.is_none(), "replace clears the snapshot");
        assert_eq!(replace_row.3.as_ref(), Some(&snap), "replace folds it");
    }

    /// Malformed stored JSON in `token_usage` / `token_usage_baseline`
    /// decodes to `None` and folds as zero: a malformed snapshot folds
    /// nothing (an existing baseline carries over), and a malformed baseline
    /// is overwritten by the folded (valid-snapshot) totals rather than
    /// preserved.
    #[tokio::test]
    async fn replace_acp_session_id_treats_malformed_json_as_zero() {
        use intent_core::now_iso;

        use uuid::Uuid;
        let tmp = TempDb::new("test-agent-repo");
        let store = Store::open(&tmp).await.expect("create test store");
        let ts = now_iso();
        let ws_id = WorkspaceId("ws-baseline".to_string());
        store
            .insert_workspace(&baseline_test_workspace(&ws_id, &ts))
            .await
            .expect("insert workspace");
        let snap = TokenUsageTotals {
            input_tokens: 11,
            output_tokens: 22,
            cache_read_tokens: 33,
            cache_creation_tokens: 44,
            thought_tokens: 0,
            cost: None,
        };

        // Malformed snapshot + valid baseline: the fold treats the snapshot
        // as zero, so the baseline carries over unchanged (not dropped).
        let bad_snap = AgentId(format!("agent-{}", Uuid::new_v4()));
        store
            .insert_agent_session(&baseline_test_session(
                &bad_snap,
                &ws_id,
                &ts,
                Some("acp-1"),
            ))
            .await
            .expect("insert");
        sqlx::query(
            "UPDATE agent_session SET token_usage='not json', token_usage_baseline=? \
             WHERE id=?",
        )
        .bind(serde_json::to_string(&snap).unwrap())
        .bind(&bad_snap.0)
        .execute(store.write_pool())
        .await
        .expect("inject malformed snapshot");
        let canonical = store
            .replace_acp_session_id(&ws_id, &bad_snap, "acp-1", "acp-2")
            .await
            .expect("swap with malformed snapshot");
        assert_eq!(canonical, "acp-2");

        // Malformed baseline + valid snapshot: the fold treats the baseline
        // as zero and overwrites it with the snapshot totals.
        let bad_base = AgentId(format!("agent-{}", Uuid::new_v4()));
        store
            .insert_agent_session(&baseline_test_session(
                &bad_base,
                &ws_id,
                &ts,
                Some("acp-1"),
            ))
            .await
            .expect("insert");
        store
            .set_agent_session_token_usage(&ws_id, &bad_base, &snap)
            .await
            .expect("set snapshot");
        sqlx::query("UPDATE agent_session SET token_usage_baseline='{broken' WHERE id=?")
            .bind(&bad_base.0)
            .execute(store.write_pool())
            .await
            .expect("inject malformed baseline");
        let canonical = store
            .replace_acp_session_id(&ws_id, &bad_base, "acp-1", "acp-2")
            .await
            .expect("swap with malformed baseline");
        assert_eq!(canonical, "acp-2");

        let rows = store
            .get_workspace_agent_usage_data(&ws_id)
            .await
            .expect("usage data");
        let by_id = |id: &AgentId| rows.iter().find(|r| r.0 == id.0).expect("row");
        let bad_snap_row = by_id(&bad_snap);
        assert!(bad_snap_row.2.is_none(), "malformed snapshot cleared");
        assert_eq!(
            bad_snap_row.3.as_ref(),
            Some(&snap),
            "valid baseline carries over when the snapshot is malformed"
        );
        let bad_base_row = by_id(&bad_base);
        assert!(bad_base_row.2.is_none(), "snapshot cleared");
        assert_eq!(
            bad_base_row.3.as_ref(),
            Some(&snap),
            "malformed baseline overwritten by the folded snapshot"
        );
    }

    /// The in-transaction CAS re-check in `write_acp_session_id`: when the
    /// stored id diverges from `expected_old` between the caller's read and
    /// the write transaction, nothing is written — id, snapshot, and baseline
    /// are untouched and the stored canonical id is returned.
    #[tokio::test]
    async fn write_acp_session_id_recheck_loses_cleanly() {
        use intent_core::now_iso;

        use uuid::Uuid;
        let tmp = TempDb::new("test-agent-repo");
        let store = Store::open(&tmp).await.expect("create test store");
        let ts = now_iso();
        let ws_id = WorkspaceId("ws-baseline".to_string());
        store
            .insert_workspace(&baseline_test_workspace(&ws_id, &ts))
            .await
            .expect("insert workspace");
        let agent_id = AgentId(format!("agent-{}", Uuid::new_v4()));
        store
            .insert_agent_session(&baseline_test_session(
                &agent_id,
                &ws_id,
                &ts,
                Some("acp-winner"),
            ))
            .await
            .expect("insert");
        let snap = TokenUsageTotals {
            input_tokens: 1,
            output_tokens: 2,
            cache_read_tokens: 3,
            cache_creation_tokens: 4,
            thought_tokens: 0,
            cost: None,
        };
        store
            .set_agent_session_token_usage(&ws_id, &agent_id, &snap)
            .await
            .expect("set snapshot");

        // Simulate a caller whose CAS read saw "acp-stale" before a
        // concurrent recreate stored "acp-winner".
        let canonical = store
            .write_acp_session_id(&ws_id, &agent_id, Some("acp-stale"), "acp-loser")
            .await
            .expect("recheck loss returns canonical id");
        assert_eq!(canonical, "acp-winner", "stored id not clobbered");
        let rows = store
            .get_workspace_agent_usage_data(&ws_id)
            .await
            .expect("usage data");
        assert_eq!(rows[0].2.as_ref(), Some(&snap), "snapshot untouched");
        assert!(rows[0].3.is_none(), "baseline untouched on recheck loss");
    }

    /// Post-conversion to raw `BEGIN IMMEDIATE` (monorepo#783): the CAS-loss
    /// early return leaves id, snapshot, and baseline untouched AND leaves no
    /// transaction open on the sole write-pool connection — a subsequent raw
    /// `BEGIN IMMEDIATE` on the write pool succeeds (it would fail with
    /// "cannot start a transaction within a transaction" on a leaked one).
    #[tokio::test]
    async fn write_acp_session_id_cas_loss_leaves_no_open_transaction() {
        use intent_core::now_iso;

        use uuid::Uuid;
        let tmp = TempDb::new("test-agent-repo");
        let store = Store::open(&tmp).await.expect("create test store");
        let ts = now_iso();
        let ws_id = WorkspaceId("ws-baseline".to_string());
        store
            .insert_workspace(&baseline_test_workspace(&ws_id, &ts))
            .await
            .expect("insert workspace");
        let agent_id = AgentId(format!("agent-{}", Uuid::new_v4()));
        store
            .insert_agent_session(&baseline_test_session(
                &agent_id,
                &ws_id,
                &ts,
                Some("acp-current"),
            ))
            .await
            .expect("insert");
        let snap = TokenUsageTotals {
            input_tokens: 5,
            output_tokens: 6,
            cache_read_tokens: 7,
            cache_creation_tokens: 8,
            thought_tokens: 0,
            cost: None,
        };
        store
            .set_agent_session_token_usage(&ws_id, &agent_id, &snap)
            .await
            .expect("set snapshot");

        let canonical = store
            .write_acp_session_id(&ws_id, &agent_id, Some("acp-stale"), "acp-x")
            .await
            .expect("cas loss returns canonical id");
        assert_eq!(canonical, "acp-current");

        // The write pool has max_connections=1, so this acquires the SAME
        // connection the CAS-loss path used; a leaked open transaction would
        // make BEGIN IMMEDIATE fail here.
        let mut conn = store.write_pool().acquire().await.expect("acquire");
        sqlx::query("BEGIN IMMEDIATE")
            .execute(&mut *conn)
            .await
            .expect("no transaction left open by the CAS-loss path");
        sqlx::query("COMMIT")
            .execute(&mut *conn)
            .await
            .expect("commit probe txn");
        drop(conn);

        let sess = store.get_agent_session(&agent_id).await.expect("get");
        assert_eq!(sess.acp_session_id.as_deref(), Some("acp-current"));
        let rows = store
            .get_workspace_agent_usage_data(&ws_id)
            .await
            .expect("usage data");
        assert_eq!(rows[0].2.as_ref(), Some(&snap), "snapshot untouched");
        assert!(rows[0].3.is_none(), "baseline untouched on CAS loss");
    }

    /// Stress loop for the `BEGIN IMMEDIATE` conversion (monorepo#783,
    /// mirroring the #738 verification loop shape): each iteration races
    /// `replace_acp_session_id` (fold + id swap) against a concurrent
    /// write-pool writer (the token-usage recompute) and asserts ZERO
    /// SQLITE_BUSY failures — IMMEDIATE mode serializes writers at
    /// `pool.acquire()` instead of racing the DEFERRED lock upgrade.
    ///
    /// Run standalone with:
    /// `cargo test -p intent-store replace_acp_session_id_racing_writer_no_busy -- --nocapture`
    #[tokio::test]
    async fn replace_acp_session_id_racing_writer_no_busy() {
        use intent_core::{now_iso, TokenUsage};

        use uuid::Uuid;
        let tmp = TempDb::new("test-agent-repo");
        let store = Store::open(&tmp).await.expect("create test store");
        let ts = now_iso();
        let ws_id = WorkspaceId("ws-baseline".to_string());
        store
            .insert_workspace(&baseline_test_workspace(&ws_id, &ts))
            .await
            .expect("insert workspace");
        let agent_id = AgentId(format!("agent-{}", Uuid::new_v4()));
        store
            .insert_agent_session(&baseline_test_session(
                &agent_id,
                &ws_id,
                &ts,
                Some("acp-0"),
            ))
            .await
            .expect("insert");

        for i in 0..120u64 {
            // A fresh snapshot each round forces the swap to fold.
            let snap = TokenUsageTotals {
                input_tokens: i + 1,
                output_tokens: 1,
                cache_read_tokens: 0,
                cache_creation_tokens: 0,
                thought_tokens: 0,
                cost: None,
            };
            store
                .set_agent_session_token_usage(&ws_id, &agent_id, &snap)
                .await
                .expect("set snapshot");
            let old = format!("acp-{i}");
            let new = format!("acp-{}", i + 1);
            let swap = store.replace_acp_session_id(&ws_id, &agent_id, &old, &new);
            let recompute = store.update_workspace_token_usage(&ws_id, |_, _| {
                Some(TokenUsage {
                    totals: snap.clone(),
                    ..TokenUsage::default()
                })
            });
            let (swapped, recomputed) = tokio::join!(swap, recompute);
            let canonical = swapped.expect("swap must never hit SQLITE_BUSY");
            assert_eq!(canonical, new, "iteration {i} swaps to the fresh id");
            recomputed.expect("recompute must never hit SQLITE_BUSY");
        }

        // 120 folds of (i+1)/1 input/output accumulated into the baseline.
        let rows = store
            .get_workspace_agent_usage_data(&ws_id)
            .await
            .expect("usage data");
        assert!(rows[0].2.is_none(), "snapshot cleared by the last fold");
        assert_eq!(
            rows[0].3.as_ref(),
            Some(&TokenUsageTotals {
                input_tokens: (1..=120).sum(),
                output_tokens: 120,
                cache_read_tokens: 0,
                cache_creation_tokens: 0,
                thought_tokens: 0,
                cost: None,
            }),
            "every fold landed exactly once"
        );
    }

    /// Migration 0054 applies cleanly on an existing, populated DB: reverting
    /// it (drop column + forget the version) and reopening the store re-runs
    /// it against rows written under the 0053 schema.
    #[tokio::test]
    async fn token_usage_baseline_migration_applies_on_existing_db() {
        use intent_core::now_iso;

        use uuid::Uuid;
        let tmp = TempDb::new("test-agent-repo");
        let ts = now_iso();
        let ws_id = WorkspaceId("ws-baseline".to_string());
        let agent_id = AgentId(format!("agent-{}", Uuid::new_v4()));
        let snap = TokenUsageTotals {
            input_tokens: 1,
            output_tokens: 2,
            cache_read_tokens: 3,
            cache_creation_tokens: 4,
            thought_tokens: 0,
            cost: None,
        };
        {
            let store = Store::open(&tmp).await.expect("create test store");
            sqlx::query("DELETE FROM _sqlx_migrations WHERE version = 54")
                .execute(store.write_pool())
                .await
                .expect("forget 0054");
            sqlx::query("ALTER TABLE agent_session DROP COLUMN token_usage_baseline")
                .execute(store.write_pool())
                .await
                .expect("drop baseline column");
            store
                .insert_workspace(&baseline_test_workspace(&ws_id, &ts))
                .await
                .expect("insert workspace");
            store
                .insert_agent_session(&baseline_test_session(&agent_id, &ws_id, &ts, None))
                .await
                .expect("insert");
            store
                .set_agent_session_token_usage(&ws_id, &agent_id, &snap)
                .await
                .expect("set snapshot");
            store.close().await;
        }

        let store = Store::open(&tmp).await.expect("reopen applies 0054");
        let status = store.migration_status().await.expect("status");
        assert!(status.is_current(), "all migrations applied: {status:?}");
        let rows = store
            .get_workspace_agent_usage_data(&ws_id)
            .await
            .expect("usage data");
        assert_eq!(rows[0].2.as_ref(), Some(&snap), "snapshot survives");
        assert!(rows[0].3.is_none(), "baseline NULL for pre-existing rows");
    }

    /// `reasoning_effort` persists across insert → read (full + summary
    /// projections) and clears via `update_agent_session` (PROTOCOL §5.5,
    /// Option B: stored as-is, cleared when absent).
    #[tokio::test]
    async fn reasoning_effort_roundtrip_and_clear() {
        use intent_core::now_iso;

        use uuid::Uuid;
        let tmp = TempDb::new("test-agent-repo");
        let store = Store::open(&tmp).await.expect("create test store");
        let ts = now_iso();
        let ws_id = WorkspaceId("ws-effort".to_string());
        let agent_id = AgentId(format!("agent-{}", Uuid::new_v4()));
        store
            .insert_workspace(&baseline_test_workspace(&ws_id, &ts))
            .await
            .expect("insert workspace");
        let mut session = baseline_test_session(&agent_id, &ws_id, &ts, None);
        session.reasoning_effort = Some("high".to_string());
        store.insert_agent_session(&session).await.expect("insert");

        let full = store.get_agent_session(&agent_id).await.expect("get");
        assert_eq!(full.reasoning_effort.as_deref(), Some("high"));
        let summary = store
            .get_agent_session_summary(&agent_id)
            .await
            .expect("summary");
        assert_eq!(summary.reasoning_effort.as_deref(), Some("high"));

        session.reasoning_effort = None;
        store
            .update_agent_session(&ws_id, &session)
            .await
            .expect("update");
        let cleared = store.get_agent_session(&agent_id).await.expect("get");
        assert_eq!(cleared.reasoning_effort, None, "cleared on update");
    }

    /// `effort_levels` (PROTOCOL §5.5, Option C) round-trips through insert →
    /// read (full + summary projections), is replaced wholesale by
    /// `set_agent_effort_levels` (change-detecting: `true` on change, `false`
    /// on the identical set), clears to NULL, survives the full-row
    /// `update_agent_session` (which deliberately excludes the column), and
    /// is `NotFound` for an absent session.
    #[tokio::test]
    async fn effort_levels_roundtrip_set_and_clear() {
        use intent_core::now_iso;

        use uuid::Uuid;
        let tmp = TempDb::new("test-agent-repo");
        let store = Store::open(&tmp).await.expect("create test store");
        let ts = now_iso();
        let ws_id = WorkspaceId("ws-effort-levels".to_string());
        let agent_id = AgentId(format!("agent-{}", Uuid::new_v4()));
        store
            .insert_workspace(&baseline_test_workspace(&ws_id, &ts))
            .await
            .expect("insert workspace");
        let levels = vec!["low".to_string(), "medium".to_string(), "high".to_string()];
        let mut session = baseline_test_session(&agent_id, &ws_id, &ts, None);
        session.effort_levels = Some(levels.clone());
        store.insert_agent_session(&session).await.expect("insert");

        let full = store.get_agent_session(&agent_id).await.expect("get");
        assert_eq!(full.effort_levels.as_deref(), Some(levels.as_slice()));
        let summary = store
            .get_agent_session_summary(&agent_id)
            .await
            .expect("summary");
        assert_eq!(summary.effort_levels.as_deref(), Some(levels.as_slice()));

        // The identical set is a no-op (`false`); a different set writes.
        let unchanged = store
            .set_agent_effort_levels(&ws_id, &agent_id, Some(&levels), &now_iso())
            .await
            .expect("set identical");
        assert!(!unchanged, "identical set must not report a change");
        let with_max: Vec<String> = ["low", "medium", "high", "max"].map(String::from).to_vec();
        let changed = store
            .set_agent_effort_levels(&ws_id, &agent_id, Some(&with_max), &now_iso())
            .await
            .expect("set changed");
        assert!(changed, "different set must report a change");
        let read = store.get_agent_session(&agent_id).await.expect("get");
        assert_eq!(read.effort_levels.as_deref(), Some(with_max.as_slice()));

        // The full-row update excludes the column — a stale in-memory session
        // (still carrying the insert-time levels) must not clobber it.
        store
            .update_agent_session(&ws_id, &session)
            .await
            .expect("full-row update");
        let preserved = store.get_agent_session(&agent_id).await.expect("get");
        assert_eq!(
            preserved.effort_levels.as_deref(),
            Some(with_max.as_slice()),
            "full-row update must not clobber effort_levels"
        );

        // `None` clears to NULL (`true` — a change), then repeats as a no-op.
        let cleared = store
            .set_agent_effort_levels(&ws_id, &agent_id, None, &now_iso())
            .await
            .expect("clear");
        assert!(cleared, "clearing a present set is a change");
        let read = store.get_agent_session(&agent_id).await.expect("get");
        assert_eq!(read.effort_levels, None);
        let cleared_again = store
            .set_agent_effort_levels(&ws_id, &agent_id, None, &now_iso())
            .await
            .expect("clear again");
        assert!(!cleared_again, "clearing NULL is a no-op");

        // Absent session → NotFound.
        let missing = AgentId("agent-missing".to_string());
        let err = store
            .set_agent_effort_levels(&ws_id, &missing, Some(&levels), &now_iso())
            .await
            .expect_err("missing session");
        assert!(matches!(err, Error::NotFound(_)), "got {err:?}");
    }

    /// Migration 0080 splits legacy codex `{base}/{effort}` compound model
    /// ids into base model + `reasoning_effort`, guarded on codex evidence
    /// (provider column, `codex:` prefix, or known effort-variant base) AND a
    /// known effort suffix — slash-bearing non-codex ids stay untouched.
    #[tokio::test]
    async fn migration_0080_splits_codex_compound_model_ids() {
        use intent_core::now_iso;

        use uuid::Uuid;
        let tmp = TempDb::new("test-agent-repo");
        let ts = now_iso();
        let ws_id = WorkspaceId("ws-0080".to_string());
        let mk_id = || AgentId(format!("agent-{}", Uuid::new_v4()));
        // (model, provider, expected model, expected effort after 0080)
        let cases: Vec<(&str, Option<&str>, &str, Option<&str>)> = vec![
            // codex provider evidence → split.
            (
                "gpt-5.3-codex/high",
                Some("codex"),
                "gpt-5.3-codex",
                Some("high"),
            ),
            // codex compound prefix evidence → split.
            (
                "codex:gpt-5.3-codex/xhigh",
                None,
                "codex:gpt-5.3-codex",
                Some("xhigh"),
            ),
            // known effort-variant base evidence → split.
            (
                "gpt-5.2-codex/medium",
                None,
                "gpt-5.2-codex",
                Some("medium"),
            ),
            // bare codex model: no slash → untouched.
            ("gpt-5.3-codex", Some("codex"), "gpt-5.3-codex", None),
            // HuggingFace-style slash id, suffix not an effort level → untouched.
            (
                "unsloth/Qwen3-32B",
                Some("unsloth"),
                "unsloth/Qwen3-32B",
                None,
            ),
            // effort-shaped suffix but no codex evidence → untouched.
            ("some-org/high", None, "some-org/high", None),
        ];
        let ids: Vec<AgentId> = cases.iter().map(|_| mk_id()).collect();
        {
            let store = Store::open(&tmp).await.expect("create test store");
            store
                .insert_workspace(&baseline_test_workspace(&ws_id, &ts))
                .await
                .expect("insert workspace");
            for (id, (model, provider, _, _)) in ids.iter().zip(&cases) {
                let mut session = baseline_test_session(id, &ws_id, &ts, None);
                session.model = Some(model.to_string());
                session.provider = provider.map(str::to_string);
                store.insert_agent_session(&session).await.expect("insert");
            }
            // Rewind to the pre-0080 schema so the reopen re-runs the
            // migration against these rows (same pattern as the 0054 test).
            sqlx::query("DELETE FROM _sqlx_migrations WHERE version = 80")
                .execute(store.write_pool())
                .await
                .expect("forget 0080");
            sqlx::query("ALTER TABLE agent_session DROP COLUMN reasoning_effort")
                .execute(store.write_pool())
                .await
                .expect("drop reasoning_effort column");
            store.close().await;
        }

        let store = Store::open(&tmp).await.expect("reopen applies 0080");
        for (id, (model, _, expected_model, expected_effort)) in ids.iter().zip(&cases) {
            let session = store.get_agent_session(id).await.expect("get");
            assert_eq!(
                session.model.as_deref(),
                Some(*expected_model),
                "model after 0080 for legacy {model}"
            );
            assert_eq!(
                session.reasoning_effort.as_deref(),
                *expected_effort,
                "reasoning_effort after 0080 for legacy {model}"
            );
        }
    }

    /// Hydration-skip matrix (monorepo#738): `get_workspace_agent_usage_data`
    /// hydrates message contents ONLY when both the decoded snapshot and
    /// baseline are absent — snapshot- and/or baseline-backed sessions return
    /// empty `contents`; a malformed-JSON snapshot decodes to `None` and
    /// still hydrates (per-message fallback preserved).
    #[tokio::test]
    async fn usage_data_skips_hydration_for_snapshot_backed_sessions() {
        use intent_core::now_iso;

        use uuid::Uuid;
        let tmp = TempDb::new("test-agent-repo");
        let store = Store::open(&tmp).await.expect("create test store");
        let ts = now_iso();
        let ws_id = WorkspaceId("ws-hydration".to_string());
        store
            .insert_workspace(&baseline_test_workspace(&ws_id, &ts))
            .await
            .expect("insert workspace");
        let usage_msg = serde_json::json!({ "usage": { "inputTokens": 9, "outputTokens": 1 } });
        let snap = TokenUsageTotals {
            input_tokens: 70,
            output_tokens: 50,
            cache_read_tokens: 30,
            cache_creation_tokens: 4,
            thought_tokens: 0,
            cost: None,
        };

        // Each session gets one message with usage metadata; only the
        // fallback-eligible sessions must surface it.
        let with_snapshot = AgentId(format!("agent-{}", Uuid::new_v4()));
        let with_baseline = AgentId(format!("agent-{}", Uuid::new_v4()));
        let with_neither = AgentId(format!("agent-{}", Uuid::new_v4()));
        let with_malformed = AgentId(format!("agent-{}", Uuid::new_v4()));
        for id in [
            &with_snapshot,
            &with_baseline,
            &with_neither,
            &with_malformed,
        ] {
            store
                .insert_agent_session(&baseline_test_session(id, &ws_id, &ts, None))
                .await
                .expect("insert");
            store
                .append_agent_message(id, "assistant", &usage_msg, &ts)
                .await
                .expect("append message");
        }
        store
            .set_agent_session_token_usage(&ws_id, &with_snapshot, &snap)
            .await
            .expect("set snapshot");
        sqlx::query("UPDATE agent_session SET token_usage_baseline=? WHERE id=?")
            .bind(serde_json::to_string(&snap).unwrap())
            .bind(&with_baseline.0)
            .execute(store.write_pool())
            .await
            .expect("inject baseline");
        sqlx::query("UPDATE agent_session SET token_usage='not json' WHERE id=?")
            .bind(&with_malformed.0)
            .execute(store.write_pool())
            .await
            .expect("inject malformed snapshot");

        let rows = store
            .get_workspace_agent_usage_data(&ws_id)
            .await
            .expect("usage data");
        let by_id = |id: &AgentId| rows.iter().find(|r| r.0 == id.0).expect("row");
        let snap_row = by_id(&with_snapshot);
        assert_eq!(snap_row.2.as_ref(), Some(&snap), "snapshot surfaced");
        assert!(snap_row.4.is_empty(), "snapshot-backed: no hydration");
        let base_row = by_id(&with_baseline);
        assert_eq!(base_row.3.as_ref(), Some(&snap), "baseline surfaced");
        assert!(base_row.4.is_empty(), "baseline-backed: no hydration");
        let neither_row = by_id(&with_neither);
        assert_eq!(
            neither_row.4,
            vec![usage_msg.clone()],
            "both-absent session still reads per-message usage"
        );
        let malformed_row = by_id(&with_malformed);
        assert!(malformed_row.2.is_none(), "malformed snapshot decodes None");
        assert_eq!(
            malformed_row.4,
            vec![usage_msg],
            "malformed snapshot still reads per-message usage (fallback preserved)"
        );
    }

    /// Bounded fallback read (monorepo#1571): a session on the per-message
    /// fallback path — including one whose snapshot is present but carries
    /// zero counters (the cost-only persist shape, which MUST stay on the
    /// fallback) — never materializes message bodies. Only usage objects cross
    /// the boundary, in `seq` order; every shape that `extract_message_usage`
    /// tallies as zero (no usage, a null/scalar `usage` or `_meta.usage`, and
    /// non-JSON content) is dropped instead of erroring the statement. Also
    /// asserts the filter is satisfied from the partial index, since without it
    /// the read would still load and parse every message body.
    #[tokio::test]
    async fn usage_data_fallback_read_projects_usage_without_message_bodies() {
        use intent_core::now_iso;

        use uuid::Uuid;
        let tmp = TempDb::new("test-agent-repo");
        let store = Store::open(&tmp).await.expect("create test store");
        let ts = now_iso();
        let ws_id = WorkspaceId("ws-bounded".to_string());
        store
            .insert_workspace(&baseline_test_workspace(&ws_id, &ts))
            .await
            .expect("insert workspace");

        // A body large enough that its presence in the row is unmistakable.
        let blob = "x".repeat(64 * 1024);
        let usage_msg = serde_json::json!({
            "text": blob,
            "usage": { "inputTokens": 9, "outputTokens": 1 },
        });
        let meta_usage_msg = serde_json::json!({
            "text": blob,
            "_meta": { "usage": { "cacheReadTokens": 4 } },
        });
        // Non-object usage values all contribute zero counters in
        // `extract_message_usage`, so the read must drop them rather than
        // error or (for the null case, where the present-but-null top-level key
        // shadows `_meta.usage` in Rust) fall through and count 7.
        let null_usage_msg = serde_json::json!({
            "text": blob,
            "usage": serde_json::Value::Null,
            "_meta": { "usage": { "inputTokens": 7 } },
        });
        let scalar_usage_msg = serde_json::json!({ "text": blob, "usage": "legacy" });
        let scalar_meta_usage_msg = serde_json::json!({
            "text": blob,
            "_meta": { "usage": "n/a" },
        });
        // No usage metadata anywhere: pure payload, must not be read at all.
        let plain_msg = serde_json::json!([{ "type": "text", "text": blob }]);

        let cost_only = AgentId(format!("agent-{}", Uuid::new_v4()));
        let never_reported = AgentId(format!("agent-{}", Uuid::new_v4()));
        for id in [&cost_only, &never_reported] {
            store
                .insert_agent_session(&baseline_test_session(id, &ws_id, &ts, None))
                .await
                .expect("insert");
            for msg in [
                &usage_msg,
                &meta_usage_msg,
                &null_usage_msg,
                &scalar_usage_msg,
                &scalar_meta_usage_msg,
                &plain_msg,
            ] {
                store
                    .append_agent_message(id, "assistant", msg, &ts)
                    .await
                    .expect("append message");
            }
            // Non-JSON content cannot arrive through the serde-encoded write
            // paths, but the filter must tolerate it rather than error the
            // whole statement.
            sqlx::query(
                "INSERT INTO agent_message (id, agent_id, seq, role, content, created_at) \
                 VALUES (?, ?, 99, 'assistant', 'not json', ?)",
            )
            .bind(format!("msg-{}", Uuid::new_v4()))
            .bind(&id.0)
            .bind(&ts)
            .execute(store.write_pool())
            .await
            .expect("inject non-JSON content");
        }
        // Cost-only persist shape: zero counters, cost present.
        store
            .set_agent_session_token_usage(
                &ws_id,
                &cost_only,
                &TokenUsageTotals {
                    cost: Some(UsageCost {
                        amount: 0.4,
                        currency: "USD".to_string(),
                    }),
                    ..Default::default()
                },
            )
            .await
            .expect("set cost-only snapshot");

        let rows = store
            .get_workspace_agent_usage_data(&ws_id)
            .await
            .expect("usage data");
        // Only the two object-usage messages contribute; the null, scalar,
        // scalar-`_meta`, no-usage and non-JSON rows all tally zero in
        // `extract_message_usage` and so are dropped.
        let expected = vec![
            serde_json::json!({ "usage": { "inputTokens": 9, "outputTokens": 1 } }),
            serde_json::json!({ "usage": { "cacheReadTokens": 4 } }),
        ];
        for id in [&cost_only, &never_reported] {
            let row = rows.iter().find(|r| r.0 == id.0).expect("row");
            // Compare/report on the encoded length first so a regression that
            // materializes the 64 KiB bodies does not dump them into the
            // failure output.
            let encoded = serde_json::to_string(&row.4).expect("encode usage");
            assert!(
                !encoded.contains(&blob),
                "no message body materialized for {id} ({} bytes)",
                encoded.len()
            );
            assert_eq!(
                row.4, expected,
                "fallback session {id} surfaces usage objects only, in seq order"
            );
        }

        // The filter must be satisfied from the partial index (migration 0081);
        // a plan that scans agent_message loads and parses every body, which is
        // exactly the cost this read is supposed to avoid.
        let plan_sql = format!(
            "EXPLAIN QUERY PLAN SELECT {MESSAGE_USAGE_JSON_SQL} AS usage_json FROM agent_message \
             WHERE agent_id = ? AND {MESSAGE_USAGE_PRESENT_SQL} ORDER BY seq ASC"
        );
        let plan: String = sqlx::query(&plan_sql)
            .bind(&cost_only.0)
            .fetch_all(store.read_pool())
            .await
            .expect("query plan")
            .iter()
            .map(|r| r.get::<String, _>("detail"))
            .collect::<Vec<_>>()
            .join("; ");
        assert!(
            plan.contains("idx_agent_message_usage"),
            "fallback read must use the usage partial index, plan was: {plan}"
        );
    }

    /// `update_workspace_token_usage` (monorepo#738): the closure sees the
    /// in-transaction usage rows and stored usage; a `Some` return performs a
    /// SCOPED `token_usage` + `updated_at` write (a title changed between
    /// reads survives — no full-row replace); `None` skips the write; a
    /// missing workspace maps to `NotFound`.
    #[tokio::test]
    async fn update_workspace_token_usage_scoped_write_and_decline() {
        use intent_core::{now_iso, TokenUsage};

        use uuid::Uuid;
        let tmp = TempDb::new("test-agent-repo");
        let store = Store::open(&tmp).await.expect("create test store");
        let ts = now_iso();
        let ws_id = WorkspaceId("ws-scoped".to_string());
        store
            .insert_workspace(&baseline_test_workspace(&ws_id, &ts))
            .await
            .expect("insert workspace");
        let agent_id = AgentId(format!("agent-{}", Uuid::new_v4()));
        store
            .insert_agent_session(&baseline_test_session(&agent_id, &ws_id, &ts, None))
            .await
            .expect("insert");
        let snap = TokenUsageTotals {
            input_tokens: 70,
            output_tokens: 50,
            cache_read_tokens: 30,
            cache_creation_tokens: 4,
            thought_tokens: 0,
            cost: None,
        };
        store
            .set_agent_session_token_usage(&ws_id, &agent_id, &snap)
            .await
            .expect("set snapshot");

        // Simulate a title update landing after the recompute's decision
        // point would have read the workspace under the OLD full-row-replace
        // scheme: change the title first, then run the scoped recompute and
        // assert the title survives.
        sqlx::query("UPDATE workspace SET title='Renamed by user' WHERE id=?")
            .bind(&ws_id.0)
            .execute(store.write_pool())
            .await
            .expect("rename");

        let written = store
            .update_workspace_token_usage(&ws_id, |rows, current| {
                assert_eq!(rows.len(), 1, "closure sees the session rows");
                assert_eq!(rows[0].2.as_ref(), Some(&snap), "snapshot visible");
                assert!(current.is_none(), "no stored usage yet");
                Some(TokenUsage {
                    totals: snap.clone(),
                    ..TokenUsage::default()
                })
            })
            .await
            .expect("recompute ok");
        assert_eq!(
            written.as_ref().map(|u| &u.totals),
            Some(&snap),
            "written usage returned"
        );
        let ws = store.get_workspace(&ws_id).await.expect("get");
        assert_eq!(ws.title, "Renamed by user", "scoped write keeps the title");
        assert_eq!(
            ws.token_usage.as_ref().map(|u| &u.totals),
            Some(&snap),
            "token_usage persisted"
        );

        // Closure declines → nothing written, stored usage passed in.
        let declined = store
            .update_workspace_token_usage(&ws_id, |_, current| {
                assert_eq!(
                    current.map(|u| &u.totals),
                    Some(&snap),
                    "stored usage visible on the next recompute"
                );
                None
            })
            .await
            .expect("decline ok");
        assert!(declined.is_none(), "None return skips the write");

        // Missing workspace → NotFound.
        let missing = WorkspaceId("ws-missing".to_string());
        let err = store
            .update_workspace_token_usage(&missing, |_, _| Some(TokenUsage::default()))
            .await
            .expect_err("missing workspace rejected");
        assert!(matches!(err, Error::NotFound(_)), "got {err:?}");
    }

    #[tokio::test]
    async fn re_interruption_resets_resolution() {
        use intent_core::{
            now_iso, Workspace, WorkspaceActivity, WorkspaceAttention, WorkspaceStatus,
        };

        let tmp = TempDb::new("test-agent-repo");
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
            status_image_asset_id: None,
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
            display_status: None,
            waiting: false,
            checkout_mode: None,
            disk_usage: None,
            pending_delete_at: None,
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
    }

    #[tokio::test]
    async fn double_claim_race() {
        use intent_core::{
            now_iso, Workspace, WorkspaceActivity, WorkspaceAttention, WorkspaceStatus,
        };

        let tmp = TempDb::new("test-agent-repo");
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
            status_image_asset_id: None,
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
            display_status: None,
            waiting: false,
            checkout_mode: None,
            disk_usage: None,
            pending_delete_at: None,
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
    }

    #[tokio::test]
    async fn list_agent_session_summaries_excludes_messages() {
        use intent_core::{
            now_iso, AgentSession, AgentStatus, Workspace, WorkspaceActivity, WorkspaceAttention,
            WorkspaceStatus,
        };

        let tmp = TempDb::new("test-summaries");
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
            status_image_asset_id: None,
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
            display_status: None,
            waiting: false,
            checkout_mode: None,
            disk_usage: None,
            pending_delete_at: None,
        };
        store
            .insert_workspace(&workspace)
            .await
            .expect("insert workspace");

        // Insert agent session
        let agent_id = AgentId("agent-summary-test".to_string());
        let system_prompt = "large system prompt".repeat(4096);
        let image_blocks = serde_json::json!([
            {"type": "image", "data": "A".repeat(4096), "mimeType": "image/png"}
        ]);
        let session = AgentSession {
            harness_version: intent_core::CURRENT_HARNESS_VERSION.to_string(),
            harness_features: None,
            id: agent_id.clone(),
            workspace_id: ws_id.clone(),
            backend_session_id: None,
            acp_session_id: None,
            name: "Test Agent".to_string(),
            name_explicitly_set: false,
            model: None,
            reasoning_effort: None,
            effort_levels: None,
            provider: None,
            status: AgentStatus::Idle,
            is_active: false,
            system_prompt: Some(system_prompt.clone()),
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
            attention_request_kind: None,
            attention_request_reason: None,
            attention_request_timestamp: None,
            delegation_depth: None,
            initial_message: None,
            context_references: None,
            image_blocks: Some(image_blocks.clone()),
            file_blocks: None,
            is_background: false,
            metadata: None,
            sandbox_id: None,
            sandbox_path: None,
            sandbox_branch: None,
            stop_reason: None,
            stop_reason_timestamp: None,
            session_corrupted: false,
            pending_delete_at: None,
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
            full[0].system_prompt.as_deref(),
            Some(system_prompt.as_str()),
            "full session reads should retain system_prompt"
        );
        assert_eq!(
            full[0].image_blocks.as_ref(),
            Some(&image_blocks),
            "full session reads should retain image_blocks"
        );
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
        assert_eq!(
            summaries[0].system_prompt, None,
            "summary reads should not load system_prompt"
        );
        assert_eq!(
            summaries[0].image_blocks, None,
            "summary reads should not load image_blocks"
        );
        assert!(
            !SESSION_SUMMARY_COLUMNS.contains("system_prompt"),
            "the summary SELECT must not mention system_prompt"
        );
        assert!(
            !SESSION_SUMMARY_COLUMNS.contains("image_blocks"),
            "the summary SELECT must not mention image_blocks"
        );
    }

    #[tokio::test]
    async fn update_agent_session_invariants_without_messages() {
        use intent_core::{
            now_iso, AgentSession, AgentStatus, Workspace, WorkspaceActivity, WorkspaceAttention,
            WorkspaceStatus,
        };

        let tmp = TempDb::new("test-update-inv");
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
                status_image_asset_id: None,
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
                display_status: None,
                waiting: false,
                checkout_mode: None,
                disk_usage: None,
                pending_delete_at: None,
            };
            store.insert_workspace(&workspace).await.expect("insert");
        }

        // Insert agent session with provider and acp_session_id
        let agent_id = AgentId("agent-inv-test".to_string());
        let mut session = AgentSession {
            harness_version: intent_core::CURRENT_HARNESS_VERSION.to_string(),
            harness_features: None,
            id: agent_id.clone(),
            workspace_id: ws_id.clone(),
            backend_session_id: None,
            acp_session_id: Some("acp-123".to_string()),
            name: "Test Agent".to_string(),
            name_explicitly_set: false,
            model: None,
            reasoning_effort: None,
            effort_levels: None,
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
            attention_request_kind: None,
            attention_request_reason: None,
            attention_request_timestamp: None,
            delegation_depth: None,
            initial_message: None,
            context_references: None,
            image_blocks: None,
            file_blocks: None,
            is_background: false,
            metadata: None,
            sandbox_id: None,
            sandbox_path: None,
            sandbox_branch: None,
            stop_reason: None,
            stop_reason_timestamp: None,
            session_corrupted: false,
            pending_delete_at: None,
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
    }

    /// The narrow `set_agent_session_model` writer (`agent.setModel`,
    /// monorepo#882): a cross-provider switch lands even after first real use
    /// (`acp_session_id` set) — the case `update_agent_session`'s
    /// immutability guard rejects — while `acp_session_id` and unrelated
    /// columns stay untouched. Wrong-workspace writes are `NotFound` and
    /// mutate nothing.
    #[tokio::test]
    async fn set_agent_session_model_allows_cross_provider_after_first_use() {
        use intent_core::now_iso;

        use uuid::Uuid;
        let tmp = TempDb::new("test-agent-repo");
        let store = Store::open(&tmp).await.expect("create test store");
        let ts = now_iso();
        let ws_id = WorkspaceId("ws-setmodel".to_string());
        store
            .insert_workspace(&baseline_test_workspace(&ws_id, &ts))
            .await
            .expect("insert workspace");
        let agent_id = AgentId(format!("agent-{}", Uuid::new_v4()));
        let mut session = baseline_test_session(&agent_id, &ws_id, &ts, Some("acp-live"));
        session.provider = Some("mock".to_string());
        session.model = Some("mock:default".to_string());
        store.insert_agent_session(&session).await.expect("insert");

        // Wrong workspace → NotFound, nothing written.
        let err = store
            .set_agent_session_model(
                &WorkspaceId("ws-other".to_string()),
                &agent_id,
                "grok:grok-4-fast",
                Some("grok"),
                &now_iso(),
            )
            .await
            .expect_err("cross-workspace write must not mutate");
        assert!(matches!(err, Error::NotFound(_)), "got: {err:?}");
        let unchanged = store.get_agent_session(&agent_id).await.expect("get");
        assert_eq!(unchanged.model.as_deref(), Some("mock:default"));
        assert_eq!(unchanged.provider.as_deref(), Some("mock"));

        // Cross-provider switch AFTER first real use (acp_session_id set).
        let updated_at = now_iso();
        store
            .set_agent_session_model(
                &ws_id,
                &agent_id,
                "grok:grok-4-fast",
                Some("grok"),
                &updated_at,
            )
            .await
            .expect("intentional cross-provider switch");
        let after = store.get_agent_session(&agent_id).await.expect("get after");
        assert_eq!(after.model.as_deref(), Some("grok:grok-4-fast"));
        assert_eq!(after.provider.as_deref(), Some("grok"));
        assert_eq!(after.updated_at, updated_at);
        assert_eq!(
            after.acp_session_id.as_deref(),
            Some("acp-live"),
            "acp session id untouched"
        );
        assert_eq!(after.name, "Baseline", "unrelated columns untouched");
    }

    /// monorepo#1936 regression: the spawn path's system-prompt persist is a
    /// narrow write, so an `agent.setModel` that lands between the spawn's
    /// session read and the prompt persist survives — the prompt write must
    /// not revert `model`/`provider` (or touch `updated_at`/other columns).
    #[tokio::test]
    async fn set_agent_session_system_prompt_preserves_concurrent_model_switch() {
        use intent_core::now_iso;

        use uuid::Uuid;
        let tmp = TempDb::new("test-agent-repo");
        let store = Store::open(&tmp).await.expect("create test store");
        let ts = now_iso();
        let ws_id = WorkspaceId("ws-sysprompt".to_string());
        store
            .insert_workspace(&baseline_test_workspace(&ws_id, &ts))
            .await
            .expect("insert workspace");
        let agent_id = AgentId(format!("agent-{}", Uuid::new_v4()));
        let mut session = baseline_test_session(&agent_id, &ws_id, &ts, Some("acp-live"));
        session.provider = Some("mock".to_string());
        session.model = Some("auggie:sonnet4.5".to_string());
        store.insert_agent_session(&session).await.expect("insert");

        // Wrong workspace → NotFound, nothing written.
        let err = store
            .set_agent_session_system_prompt(
                &WorkspaceId("ws-other".to_string()),
                &agent_id,
                "prompt",
            )
            .await
            .expect_err("cross-workspace write must not mutate");
        assert!(matches!(err, Error::NotFound(_)), "got: {err:?}");
        let unchanged = store.get_agent_session(&agent_id).await.expect("get");
        assert_eq!(unchanged.system_prompt, None);

        // A model switch lands mid-spawn (after the spawn path read its
        // session snapshot)…
        let switched_at = now_iso();
        store
            .set_agent_session_model(
                &ws_id,
                &agent_id,
                "auggie:haiku",
                Some("mock"),
                &switched_at,
            )
            .await
            .expect("model switch");

        // …then the spawn path persists the assembled prompt. The switch
        // must survive.
        store
            .set_agent_session_system_prompt(&ws_id, &agent_id, "assembled prompt")
            .await
            .expect("persist system prompt");
        let after = store.get_agent_session(&agent_id).await.expect("get after");
        assert_eq!(after.system_prompt.as_deref(), Some("assembled prompt"));
        assert_eq!(
            after.model.as_deref(),
            Some("auggie:haiku"),
            "concurrent setModel must not be reverted by the prompt persist"
        );
        assert_eq!(after.provider.as_deref(), Some("mock"));
        assert_eq!(after.updated_at, switched_at, "updated_at untouched");
        assert_eq!(after.acp_session_id.as_deref(), Some("acp-live"));
    }

    #[tokio::test]
    async fn get_agent_session_message_stats() {
        use intent_core::{
            now_iso, AgentSession, AgentStatus, Workspace, WorkspaceActivity, WorkspaceAttention,
            WorkspaceStatus,
        };

        let tmp = TempDb::new("test-msg-stats");
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
            status_image_asset_id: None,
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
            display_status: None,
            waiting: false,
            checkout_mode: None,
            disk_usage: None,
            pending_delete_at: None,
        };
        store.insert_workspace(&workspace).await.expect("insert");

        // Create two agents
        let agent1 = AgentId("agent-stats-1".to_string());
        let agent2 = AgentId("agent-stats-2".to_string());

        for agent_id in [&agent1, &agent2] {
            let session = AgentSession {
                harness_version: intent_core::CURRENT_HARNESS_VERSION.to_string(),
                harness_features: None,
                id: agent_id.clone(),
                workspace_id: ws_id.clone(),
                backend_session_id: None,
                acp_session_id: None,
                name: format!("Agent {}", agent_id.0),
                name_explicitly_set: false,
                model: None,
                reasoning_effort: None,
                effort_levels: None,
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
                attention_request_kind: None,
                attention_request_reason: None,
                attention_request_timestamp: None,
                delegation_depth: None,
                initial_message: None,
                context_references: None,
                image_blocks: None,
                file_blocks: None,
                is_background: false,
                metadata: None,
                sandbox_id: None,
                sandbox_path: None,
                sandbox_branch: None,
                stop_reason: None,
                stop_reason_timestamp: None,
                session_corrupted: false,
                pending_delete_at: None,
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

        let (count1, has_assistant1, bytes1) = stats.get(&agent1.0).expect("agent1 stats");
        assert_eq!(*count1, 1, "agent1 should have 1 message");
        assert!(!has_assistant1, "agent1 should have no assistant message");
        assert!(*bytes1 > 0, "agent1 conversation bytes should be non-zero");

        let (count2, has_assistant2, bytes2) = stats.get(&agent2.0).expect("agent2 stats");
        assert_eq!(*count2, 3, "agent2 should have 3 messages");
        assert!(has_assistant2, "agent2 should have assistant message");
        assert!(
            bytes2 > bytes1,
            "agent2's 3-message conversation should outweigh agent1's 1 message"
        );
    }

    /// `get_agent_messages_page` returns exactly the `offset..offset+limit`
    /// window of the chronological log — matching what a caller would get by
    /// slicing `get_agent_messages` — with correct boundary behavior (first
    /// page, last partial page, out-of-range, empty log).
    #[tokio::test]
    async fn get_agent_messages_page_matches_full_read_windows() {
        use intent_core::now_iso;

        use uuid::Uuid;
        let tmp = TempDb::new("test-agent-repo");
        let store = Store::open(&tmp).await.expect("create test store");
        let ts = now_iso();
        let ws_id = WorkspaceId("ws-msg-page".to_string());
        store
            .insert_workspace(&baseline_test_workspace(&ws_id, &ts))
            .await
            .expect("insert workspace");
        let agent_id = AgentId(format!("agent-{}", Uuid::new_v4()));
        store
            .insert_agent_session(&baseline_test_session(&agent_id, &ws_id, &ts, None))
            .await
            .expect("insert session");

        for i in 0..5 {
            store
                .append_agent_message(
                    &agent_id,
                    if i % 2 == 0 { "user" } else { "assistant" },
                    &serde_json::json!([{"type": "text", "text": format!("msg-{i}")}]),
                    &ts,
                )
                .await
                .expect("append");
        }

        let full = store
            .get_agent_messages(&agent_id, None)
            .await
            .expect("full read");
        assert_eq!(full.len(), 5);
        assert_eq!(
            store.count_agent_messages(&agent_id).await.expect("count"),
            5
        );

        // Every window matches the equivalent slice of the full read
        // (same rows, same chronological order).
        for (offset, limit) in [(0, 2), (2, 2), (4, 2), (0, 5), (1, 3), (0, 100)] {
            let page = store
                .get_agent_messages_page(&agent_id, offset, limit)
                .await
                .expect("page read");
            let start = (offset as usize).min(full.len());
            let end = (start + limit as usize).min(full.len());
            let expected: Vec<_> = full[start..end].iter().map(|m| (m.seq, &m.id)).collect();
            let got: Vec<_> = page.iter().map(|m| (m.seq, &m.id)).collect();
            assert_eq!(got, expected, "window offset={offset} limit={limit}");
        }

        // Out-of-range offsets and a zero limit yield empty pages.
        for (offset, limit) in [(5, 2), (10, 3), (0, 0)] {
            let page = store
                .get_agent_messages_page(&agent_id, offset, limit)
                .await
                .expect("page read");
            assert!(page.is_empty(), "offset={offset} limit={limit}");
        }

        // Negative inputs clamp to zero rather than erroring.
        let page = store
            .get_agent_messages_page(&agent_id, -1, 2)
            .await
            .expect("negative offset");
        assert_eq!(page.len(), 2);
        assert_eq!(page[0].seq, full[0].seq);
        let page = store
            .get_agent_messages_page(&agent_id, 0, -1)
            .await
            .expect("negative limit");
        assert!(page.is_empty());

        // An agent with no messages pages to empty.
        let empty_agent = AgentId(format!("agent-{}", Uuid::new_v4()));
        store
            .insert_agent_session(&baseline_test_session(&empty_agent, &ws_id, &ts, None))
            .await
            .expect("insert empty session");
        let page = store
            .get_agent_messages_page(&empty_agent, 0, 10)
            .await
            .expect("empty page");
        assert!(page.is_empty());
        assert_eq!(
            store
                .count_agent_messages(&empty_agent)
                .await
                .expect("count empty"),
            0
        );
    }

    /// `get_agent_session_summary` returns the session row with `messages`
    /// empty even when a transcript exists, and maps an unknown id to
    /// `NotFound` (matching `get_agent_session`).
    #[tokio::test]
    async fn get_agent_session_summary_excludes_messages() {
        use intent_core::now_iso;

        use uuid::Uuid;
        let tmp = TempDb::new("test-agent-repo");
        let store = Store::open(&tmp).await.expect("create test store");
        let ts = now_iso();
        let ws_id = WorkspaceId("ws-summary".to_string());
        store
            .insert_workspace(&baseline_test_workspace(&ws_id, &ts))
            .await
            .expect("insert workspace");
        let agent_id = AgentId(format!("agent-{}", Uuid::new_v4()));
        store
            .insert_agent_session(&baseline_test_session(&agent_id, &ws_id, &ts, None))
            .await
            .expect("insert session");
        for role in ["user", "assistant"] {
            store
                .append_agent_message(
                    &agent_id,
                    role,
                    &serde_json::json!([{"type": "text", "text": role}]),
                    &ts,
                )
                .await
                .expect("append");
        }

        let summary = store
            .get_agent_session_summary(&agent_id)
            .await
            .expect("summary");
        assert_eq!(summary.id, agent_id);
        assert_eq!(summary.workspace_id, ws_id);
        assert_eq!(summary.name, "Baseline");
        assert!(
            summary.messages.is_empty(),
            "summary must not hydrate the message log"
        );

        let missing = AgentId("agent-missing".to_string());
        match store.get_agent_session_summary(&missing).await {
            Err(Error::NotFound(_)) => {}
            other => panic!("expected NotFound, got {other:?}"),
        }
    }

    /// `get_agent_statuses` returns every requested id's persisted status in
    /// one batched query, skips ids without a session row, skips (rather
    /// than fails on) a row whose stored status does not decode, and returns
    /// empty for an empty id list (intent-hq/monorepo#3018 — the
    /// `agent.getSubscriptions` `agentStatuses` projection).
    #[tokio::test]
    async fn get_agent_statuses_batches_ids_and_skips_missing() {
        use intent_core::now_iso;

        use uuid::Uuid;
        let tmp = TempDb::new("test-agent-repo");
        let store = Store::open(&tmp).await.expect("create test store");
        let ts = now_iso();
        let ws_id = WorkspaceId("ws-statuses".to_string());
        store
            .insert_workspace(&baseline_test_workspace(&ws_id, &ts))
            .await
            .expect("insert workspace");
        let idle = AgentId(format!("agent-{}", Uuid::new_v4()));
        let active = AgentId(format!("agent-{}", Uuid::new_v4()));
        store
            .insert_agent_session(&baseline_test_session(&idle, &ws_id, &ts, None))
            .await
            .expect("insert idle session");
        let mut active_session = baseline_test_session(&active, &ws_id, &ts, None);
        active_session.status = AgentStatus::Active;
        store
            .insert_agent_session(&active_session)
            .await
            .expect("insert active session");

        // A row whose persisted status no longer decodes (e.g. a daemon
        // downgrade after a newer build wrote a new variant) must be
        // skipped, not fail the whole batch.
        let corrupt = AgentId(format!("agent-{}", Uuid::new_v4()));
        store
            .insert_agent_session(&baseline_test_session(&corrupt, &ws_id, &ts, None))
            .await
            .expect("insert corrupt session");
        sqlx::query("UPDATE agent_session SET status = 'from-the-future' WHERE id = ?")
            .bind(&corrupt.0)
            .execute(store.write_pool())
            .await
            .expect("write undecodable status");

        let missing = AgentId("agent-missing".to_string());
        let mut statuses = store
            .get_agent_statuses(&[idle.clone(), active.clone(), missing, corrupt])
            .await
            .expect("batched statuses");
        statuses.sort_by(|a, b| a.0 .0.cmp(&b.0 .0));
        let mut expected = vec![(idle, AgentStatus::Idle), (active, AgentStatus::Active)];
        expected.sort_by(|a, b| a.0 .0.cmp(&b.0 .0));
        assert_eq!(statuses, expected);

        assert!(store
            .get_agent_statuses(&[])
            .await
            .expect("empty id list")
            .is_empty());
    }

    /// `update_agent_session_metadata` writes only `metadata` + `updated_at`:
    /// columns absent from the summary projection (`system_prompt`) survive,
    /// and the write is workspace-scoped (`NotFound` on mismatch or missing).
    #[tokio::test]
    async fn update_agent_session_metadata_targets_only_metadata() {
        use intent_core::now_iso;

        use uuid::Uuid;
        let tmp = TempDb::new("test-agent-repo");
        let store = Store::open(&tmp).await.expect("create test store");
        let ts = now_iso();
        let ws_id = WorkspaceId("ws-meta".to_string());
        store
            .insert_workspace(&baseline_test_workspace(&ws_id, &ts))
            .await
            .expect("insert workspace");
        let agent_id = AgentId(format!("agent-{}", Uuid::new_v4()));
        let mut session = baseline_test_session(&agent_id, &ws_id, &ts, None);
        session.system_prompt = Some("keep this prompt".to_string());
        store
            .insert_agent_session(&session)
            .await
            .expect("insert session");

        let metadata = serde_json::json!({ "dismissedQuestionsMessageId": "msg-1" });
        let later = now_iso();
        store
            .update_agent_session_metadata(&ws_id, &agent_id, Some(&metadata), &later)
            .await
            .expect("update metadata");

        let after = store.get_agent_session(&agent_id).await.expect("get");
        assert_eq!(after.metadata, Some(metadata.clone()));
        assert_eq!(after.updated_at, later);
        assert_eq!(
            after.system_prompt.as_deref(),
            Some("keep this prompt"),
            "targeted metadata write must not touch system_prompt"
        );

        // Workspace mismatch and unknown id both surface as NotFound.
        let other_ws = WorkspaceId("ws-meta-other".to_string());
        match store
            .update_agent_session_metadata(&other_ws, &agent_id, Some(&metadata), &later)
            .await
        {
            Err(Error::NotFound(_)) => {}
            other => panic!("expected NotFound on workspace mismatch, got {other:?}"),
        }
        let missing = AgentId("agent-meta-missing".to_string());
        match store
            .update_agent_session_metadata(&ws_id, &missing, Some(&metadata), &later)
            .await
        {
            Err(Error::NotFound(_)) => {}
            other => panic!("expected NotFound on unknown id, got {other:?}"),
        }
    }

    /// `set_agent_session_metadata_key` writes exactly one key in SQL:
    /// sibling keys survive (no whole-column clobber), a NULL column starts
    /// from `{}`, a non-object column is preserved under
    /// `priorNonObjectMetadata`, `system_prompt` is untouched, the CAS guard
    /// enforces expected-absent / expected-value semantics (guard miss →
    /// `Ok(false)`, no write), and missing/mismatched sessions are NotFound.
    #[tokio::test]
    async fn set_agent_session_metadata_key_atomic_and_guarded() {
        use intent_core::now_iso;

        use uuid::Uuid;
        let tmp = TempDb::new("test-agent-repo");
        let store = Store::open(&tmp).await.expect("create test store");
        let ts = now_iso();
        let ws_id = WorkspaceId("ws-meta-key".to_string());
        store
            .insert_workspace(&baseline_test_workspace(&ws_id, &ts))
            .await
            .expect("insert workspace");
        let agent_id = AgentId(format!("agent-{}", Uuid::new_v4()));
        let mut session = baseline_test_session(&agent_id, &ws_id, &ts, None);
        session.system_prompt = Some("keep this prompt".to_string());
        store
            .insert_agent_session(&session)
            .await
            .expect("insert session");

        // NULL column + expected-absent guard: writes, starting from {}.
        let wrote = store
            .set_agent_session_metadata_key(
                &ws_id,
                &agent_id,
                "lastSeenMessageId",
                "msg-1",
                Some(None),
                &now_iso(),
            )
            .await
            .expect("first write");
        assert!(wrote, "expected-absent guard must pass on a NULL column");

        // Sibling key written unconditionally: both keys coexist.
        let wrote = store
            .set_agent_session_metadata_key(
                &ws_id,
                &agent_id,
                "dismissedQuestionsMessageId",
                "msg-q",
                None,
                &now_iso(),
            )
            .await
            .expect("sibling write");
        assert!(wrote);
        let after = store.get_agent_session(&agent_id).await.expect("get");
        let metadata = after.metadata.as_ref().expect("metadata");
        assert_eq!(metadata["lastSeenMessageId"], serde_json::json!("msg-1"));
        assert_eq!(
            metadata["dismissedQuestionsMessageId"],
            serde_json::json!("msg-q")
        );
        assert_eq!(
            after.system_prompt.as_deref(),
            Some("keep this prompt"),
            "single-key metadata write must not touch system_prompt"
        );

        // CAS guard miss: expected value no longer current → Ok(false), no write.
        let wrote = store
            .set_agent_session_metadata_key(
                &ws_id,
                &agent_id,
                "lastSeenMessageId",
                "msg-stale",
                Some(Some("msg-0")),
                &now_iso(),
            )
            .await
            .expect("guard miss is not an error");
        assert!(!wrote, "stale expected value must not write");
        let after = store.get_agent_session(&agent_id).await.expect("get");
        assert_eq!(
            after.metadata.as_ref().expect("metadata")["lastSeenMessageId"],
            serde_json::json!("msg-1"),
            "guard miss must leave the key untouched"
        );

        // CAS guard hit: expected current value → writes.
        let wrote = store
            .set_agent_session_metadata_key(
                &ws_id,
                &agent_id,
                "lastSeenMessageId",
                "msg-2",
                Some(Some("msg-1")),
                &now_iso(),
            )
            .await
            .expect("guard hit");
        assert!(wrote);
        let after = store.get_agent_session(&agent_id).await.expect("get");
        let metadata = after.metadata.as_ref().expect("metadata");
        assert_eq!(metadata["lastSeenMessageId"], serde_json::json!("msg-2"));
        assert_eq!(
            metadata["dismissedQuestionsMessageId"],
            serde_json::json!("msg-q"),
            "sibling key must survive the guarded write"
        );

        // Non-object column: preserved under `priorNonObjectMetadata`.
        let legacy = AgentId(format!("agent-{}", Uuid::new_v4()));
        let mut legacy_session = baseline_test_session(&legacy, &ws_id, &ts, None);
        legacy_session.metadata = Some(serde_json::json!("legacy-string"));
        store
            .insert_agent_session(&legacy_session)
            .await
            .expect("insert legacy session");
        let wrote = store
            .set_agent_session_metadata_key(
                &ws_id,
                &legacy,
                "lastSeenMessageId",
                "msg-1",
                None,
                &now_iso(),
            )
            .await
            .expect("legacy write");
        assert!(wrote);
        let after = store.get_agent_session(&legacy).await.expect("get");
        let metadata = after.metadata.as_ref().expect("metadata");
        assert_eq!(metadata["lastSeenMessageId"], serde_json::json!("msg-1"));
        assert_eq!(
            metadata["priorNonObjectMetadata"],
            serde_json::json!("legacy-string"),
            "prior non-object metadata must be preserved, not dropped"
        );

        // Workspace mismatch / unknown id: NotFound (guarded and unguarded).
        let other_ws = WorkspaceId("ws-meta-key-other".to_string());
        for expected in [None, Some(Some("msg-2"))] {
            match store
                .set_agent_session_metadata_key(
                    &other_ws,
                    &agent_id,
                    "lastSeenMessageId",
                    "msg-3",
                    expected,
                    &now_iso(),
                )
                .await
            {
                Err(Error::NotFound(_)) => {}
                other => panic!("expected NotFound on workspace mismatch, got {other:?}"),
            }
        }
        let missing = AgentId("agent-meta-key-missing".to_string());
        match store
            .set_agent_session_metadata_key(
                &ws_id,
                &missing,
                "lastSeenMessageId",
                "msg-3",
                Some(None),
                &now_iso(),
            )
            .await
        {
            Err(Error::NotFound(_)) => {}
            other => panic!("expected NotFound on unknown id, got {other:?}"),
        }
    }

    /// `get_agent_session_message_projections` returns one entry per session
    /// (zero-message sessions included) with the correct count and the
    /// highest-`seq` user/assistant rows, scoped to the workspace, without
    /// decoding any `content` beyond those last rows — proven by a
    /// malformed-JSON content row parked mid-transcript, which would error
    /// the query if it were ever decoded.
    #[tokio::test]
    async fn session_message_projections_bounded_and_correct() {
        use intent_core::now_iso;

        use uuid::Uuid;
        let tmp = TempDb::new("test-agent-repo");
        let store = Store::open(&tmp).await.expect("create test store");
        let ts = now_iso();
        let ws_id = WorkspaceId("ws-proj".to_string());
        let other_ws = WorkspaceId("ws-proj-other".to_string());
        for ws in [&ws_id, &other_ws] {
            store
                .insert_workspace(&baseline_test_workspace(ws, &ts))
                .await
                .expect("insert workspace");
        }

        // empty: session with no messages at all.
        let empty = AgentId("agent-proj-empty".to_string());
        // user_only: a single user message, no assistant.
        let user_only = AgentId("agent-proj-user-only".to_string());
        // full: user/assistant/tool traffic, including a malformed-content
        // row that is neither the last user nor the last assistant message.
        let full = AgentId("agent-proj-full".to_string());
        for id in [&empty, &user_only, &full] {
            store
                .insert_agent_session(&baseline_test_session(id, &ws_id, &ts, None))
                .await
                .expect("insert session");
        }
        // foreign: identical traffic in another workspace, must not appear.
        let foreign = AgentId("agent-proj-foreign".to_string());
        store
            .insert_agent_session(&baseline_test_session(&foreign, &other_ws, &ts, None))
            .await
            .expect("insert session");

        store
            .append_agent_message(
                &user_only,
                "user",
                &serde_json::json!([{"type": "text", "text": "only"}]),
                &ts,
            )
            .await
            .expect("append");

        for (role, text) in [
            ("user", "q1"),
            ("assistant", "a1"),
            ("tool", "t1"),
            ("user", "q2"),
            ("assistant", "a2"),
        ] {
            for agent in [&full, &foreign] {
                store
                    .append_agent_message(
                        agent,
                        role,
                        &serde_json::json!([{"type": "text", "text": text}]),
                        &ts,
                    )
                    .await
                    .expect("append");
            }
        }
        // Park a NON-last user row with malformed content JSON between the
        // real rows (seq 5 < the final user/assistant appends below): the
        // projection must never touch content beyond the last user/assistant
        // rows.
        sqlx::query(
            "INSERT INTO agent_message (id, agent_id, seq, role, content, created_at) \
             VALUES (?,?,?,?,?,?)",
        )
        .bind(Uuid::now_v7().to_string())
        .bind(&full.0)
        .bind(5_i64)
        .bind("user")
        .bind("{not-valid-json")
        .bind(&ts)
        .execute(store.write_pool())
        .await
        .expect("insert malformed row");
        for (role, text) in [("user", "q3"), ("assistant", "a3")] {
            store
                .append_agent_message(
                    &full,
                    role,
                    &serde_json::json!([{"type": "text", "text": text}]),
                    &ts,
                )
                .await
                .expect("append last rows");
        }

        let projections = store
            .get_agent_session_message_projections(&ws_id)
            .await
            .expect("projections");
        assert_eq!(
            projections.len(),
            3,
            "one entry per session in the workspace, foreign excluded"
        );

        let p = projections.get(&empty.0).expect("empty entry");
        assert_eq!(p.message_count, 0);
        assert!(p.last_assistant_text_blocks.is_none());
        assert!(p.last_user_text_blocks.is_none());

        let p = projections.get(&user_only.0).expect("user-only entry");
        assert_eq!(p.message_count, 1);
        assert!(p.last_assistant_text_blocks.is_none());
        assert_eq!(
            p.last_user_text_blocks,
            Some(vec!["only".to_string()]),
            "last user text blocks"
        );

        let p = projections.get(&full.0).expect("full entry");
        // 5 appended + 1 malformed + last user + last assistant (tool rows count).
        assert_eq!(p.message_count, 8);
        assert_eq!(
            p.last_user_text_blocks,
            Some(vec!["q3".to_string()]),
            "last user text blocks"
        );
        assert_eq!(
            p.last_assistant_text_blocks,
            Some(vec!["a3".to_string()]),
            "last assistant text blocks"
        );
    }

    /// `get_agent_session_message_projection` (per-session, monorepo#981)
    /// returns the same shape/values as the workspace-wide variant's entry
    /// for that agent — for an empty session, a user-only session, and a full
    /// session with mixed traffic (including a malformed non-last content row
    /// proving nothing beyond the last user/assistant rows is decoded).
    #[tokio::test]
    async fn per_session_message_projection_matches_workspace_variant() {
        use intent_core::now_iso;

        use uuid::Uuid;
        let tmp = TempDb::new("test-agent-repo");
        let store = Store::open(&tmp).await.expect("create test store");
        let ts = now_iso();
        let ws_id = WorkspaceId("ws-proj-single".to_string());
        store
            .insert_workspace(&baseline_test_workspace(&ws_id, &ts))
            .await
            .expect("insert workspace");

        let empty = AgentId("agent-single-empty".to_string());
        let user_only = AgentId("agent-single-user-only".to_string());
        let full = AgentId("agent-single-full".to_string());
        for id in [&empty, &user_only, &full] {
            store
                .insert_agent_session(&baseline_test_session(id, &ws_id, &ts, None))
                .await
                .expect("insert session");
        }

        store
            .append_agent_message(
                &user_only,
                "user",
                &serde_json::json!([{"type": "text", "text": "only"}]),
                &ts,
            )
            .await
            .expect("append");

        for (role, text) in [
            ("user", "q1"),
            ("assistant", "a1"),
            ("tool", "t1"),
            ("user", "q2"),
            ("assistant", "a2"),
        ] {
            store
                .append_agent_message(
                    &full,
                    role,
                    &serde_json::json!([{"type": "text", "text": text}]),
                    &ts,
                )
                .await
                .expect("append");
        }
        // Malformed non-last user row: the projection must never touch
        // content beyond the last user/assistant rows.
        sqlx::query(
            "INSERT INTO agent_message (id, agent_id, seq, role, content, created_at) \
             VALUES (?,?,?,?,?,?)",
        )
        .bind(Uuid::now_v7().to_string())
        .bind(&full.0)
        .bind(5_i64)
        .bind("user")
        .bind("{not-valid-json")
        .bind(&ts)
        .execute(store.write_pool())
        .await
        .expect("insert malformed row");
        for (role, text) in [("user", "q3"), ("assistant", "a3")] {
            store
                .append_agent_message(
                    &full,
                    role,
                    &serde_json::json!([{"type": "text", "text": text}]),
                    &ts,
                )
                .await
                .expect("append");
        }

        let workspace_wide = store
            .get_agent_session_message_projections(&ws_id)
            .await
            .expect("workspace projections");
        for agent in [&empty, &user_only, &full] {
            let per_session = store
                .get_agent_session_message_projection(agent)
                .await
                .expect("per-session projection");
            let expected = workspace_wide.get(&agent.0).expect("workspace entry");
            assert_eq!(
                per_session.message_count, expected.message_count,
                "message_count mismatch for {agent:?}"
            );
            assert_eq!(per_session, *expected, "projection mismatch for {agent:?}");
        }
    }

    /// P1b: the projection never returns unbounded text. Multi-MB winner
    /// text blocks come back capped at [`PROJECTION_TEXT_BLOCK_CAP`] — the
    /// user block's HEAD and the assistant block's TAIL (so the final
    /// response line and trailing `<agent_digest>` span survive) — while
    /// short blocks pass through unchanged. Non-text / non-object blocks are
    /// skipped. A row written via raw SQL (bypassing write-time preview
    /// maintenance) leaves the preview column NULL regardless of its
    /// content, so its projection degrades to `None` rather than being
    /// computed from `agent_message.content`.
    #[tokio::test]
    async fn projection_text_blocks_bounded_and_tolerant() {
        use intent_core::now_iso;

        use uuid::Uuid;
        let tmp = TempDb::new("test-agent-repo");
        let store = Store::open(&tmp).await.expect("create test store");
        let ts = now_iso();
        let ws_id = WorkspaceId("ws-proj-bounded".to_string());
        store
            .insert_workspace(&baseline_test_workspace(&ws_id, &ts))
            .await
            .expect("insert workspace");

        let big = AgentId("agent-proj-big".to_string());
        let malformed = AgentId("agent-proj-malformed".to_string());
        let non_array = AgentId("agent-proj-non-array".to_string());
        for id in [&big, &malformed, &non_array] {
            store
                .insert_agent_session(&baseline_test_session(id, &ws_id, &ts, None))
                .await
                .expect("insert session");
        }

        let cap = PROJECTION_TEXT_BLOCK_CAP as usize;
        let big_user = format!("user-head-{}", "u".repeat(2 * 1024 * 1024));
        let big_assistant = format!(
            "{}\nFinal answer line\n<agent_digest>big digest</agent_digest>",
            "a".repeat(2 * 1024 * 1024)
        );
        store
            .append_agent_message(
                &big,
                "user",
                &serde_json::json!([
                    {"type": "text", "text": big_user},
                    {"type": "tool_use", "name": "t", "toolCallId": "c1"},
                    "bare-string-block",
                    {"type": "text"},
                    {"type": "text", "text": "short tail block"},
                ]),
                &ts,
            )
            .await
            .expect("append big user");
        store
            .append_agent_message(
                &big,
                "assistant",
                &serde_json::json!([{"type": "text", "text": big_assistant}]),
                &ts,
            )
            .await
            .expect("append big assistant");

        sqlx::query(
            "INSERT INTO agent_message (id, agent_id, seq, role, content, created_at) \
             VALUES (?,?,?,?,?,?)",
        )
        .bind(Uuid::now_v7().to_string())
        .bind(&malformed.0)
        .bind(0_i64)
        .bind("user")
        .bind("{not-valid-json")
        .bind(&ts)
        .execute(store.write_pool())
        .await
        .expect("insert malformed winner");
        store
            .append_agent_message(&non_array, "user", &serde_json::json!("plain string"), &ts)
            .await
            .expect("append non-array");

        let projections = store
            .get_agent_session_message_projections(&ws_id)
            .await
            .expect("projections");

        let p = projections.get(&big.0).expect("big entry");
        let user_blocks = p.last_user_text_blocks.as_ref().expect("user blocks");
        assert_eq!(user_blocks.len(), 2, "only text blocks with string text");
        assert_eq!(user_blocks[0].chars().count(), cap, "user block capped");
        assert!(
            user_blocks[0].starts_with("user-head-"),
            "user block keeps its head"
        );
        assert_eq!(user_blocks[1], "short tail block");
        let assistant_blocks = p
            .last_assistant_text_blocks
            .as_ref()
            .expect("assistant blocks");
        assert_eq!(assistant_blocks.len(), 1);
        assert_eq!(
            assistant_blocks[0].chars().count(),
            cap,
            "assistant block capped"
        );
        assert!(
            assistant_blocks[0].ends_with("</agent_digest>"),
            "assistant block keeps its tail"
        );
        assert!(
            assistant_blocks[0].contains("Final answer line"),
            "final response line survives the cap"
        );

        // The malformed row was inserted via raw SQL, bypassing write-time
        // preview maintenance: its preview column is still NULL, so the
        // projection degrades to `None` rather than reading the row's
        // (malformed) content.
        let p = projections.get(&malformed.0).expect("malformed entry");
        assert_eq!(p.message_count, 1, "malformed count");
        assert!(
            p.last_user_text_blocks.is_none(),
            "malformed winner's untouched NULL column degrades to None"
        );
        let per_agent = store
            .get_agent_session_message_projection(&malformed)
            .await
            .expect("per-agent projection");
        assert_eq!(per_agent, *p, "malformed per-agent parity");

        // The non-array append went through the write path, which stores the
        // projection form `"[]"` for a non-array winner.
        let p = projections.get(&non_array.0).expect("non-array entry");
        assert_eq!(p.message_count, 1, "non-array count");
        assert_eq!(
            p.last_user_text_blocks,
            Some(vec![]),
            "non-array winner projects to no text blocks"
        );
        let per_agent = store
            .get_agent_session_message_projection(&non_array)
            .await
            .expect("per-agent projection");
        assert_eq!(per_agent, *p, "non-array per-agent parity");
    }

    /// The single-aggregate `get_agent_session_message_stats` never decodes
    /// message content: a workspace whose only transcript rows are malformed
    /// JSON still returns correct counts and assistant detection.
    #[tokio::test]
    async fn message_stats_do_not_decode_content() {
        use intent_core::now_iso;

        use uuid::Uuid;
        let tmp = TempDb::new("test-agent-repo");
        let store = Store::open(&tmp).await.expect("create test store");
        let ts = now_iso();
        let ws_id = WorkspaceId("ws-stats-raw".to_string());
        store
            .insert_workspace(&baseline_test_workspace(&ws_id, &ts))
            .await
            .expect("insert workspace");
        let agent_id = AgentId("agent-stats-raw".to_string());
        store
            .insert_agent_session(&baseline_test_session(&agent_id, &ws_id, &ts, None))
            .await
            .expect("insert session");
        for (seq, role) in [(0_i64, "user"), (1, "assistant")] {
            sqlx::query(
                "INSERT INTO agent_message (id, agent_id, seq, role, content, created_at) \
                 VALUES (?,?,?,?,?,?)",
            )
            .bind(Uuid::now_v7().to_string())
            .bind(&agent_id.0)
            .bind(seq)
            .bind(role)
            .bind("{not-valid-json")
            .bind(&ts)
            .execute(store.write_pool())
            .await
            .expect("insert malformed row");
        }

        let stats = store
            .get_agent_session_message_stats(&ws_id)
            .await
            .expect("stats");
        let raw_len = "{not-valid-json".len() as u64;
        assert_eq!(stats.get(&agent_id.0), Some(&(2, true, 2 * raw_len)));
    }

    /// Raw `(last_assistant_preview, last_user_preview)` column values for a
    /// session (0066).
    async fn read_preview_columns(
        store: &Store,
        agent_id: &AgentId,
    ) -> (Option<String>, Option<String>) {
        let row = sqlx::query(
            "SELECT last_assistant_preview, last_user_preview FROM agent_session WHERE id = ?",
        )
        .bind(&agent_id.0)
        .fetch_one(store.read_pool())
        .await
        .expect("read preview columns");
        (
            row.get("last_assistant_preview"),
            row.get("last_user_preview"),
        )
    }

    /// Decode a preview column into the block vec it mirrors (NULL → `None`).
    fn decode_preview(col: Option<String>) -> Option<Vec<String>> {
        col.map(|raw| serde_json::from_str(&raw).expect("decode preview column"))
    }

    /// [`preview_text_blocks`] semantics: text blocks only (string `text`,
    /// string `type` equal to `text`), block order preserved, per-block char
    /// cap with assistant=TAIL / user=HEAD, non-array content → `None`,
    /// empty array → `Some(vec![])`.
    #[test]
    fn preview_text_blocks_matches_sql_expression_semantics() {
        let cap = PROJECTION_TEXT_BLOCK_CAP as usize;

        // Plain text blocks: order preserved, short blocks pass through.
        let content = serde_json::json!([
            {"type": "text", "text": "first"},
            {"type": "text", "text": "second"},
        ]);
        assert_eq!(
            preview_text_blocks("user", &content),
            Some(vec!["first".to_string(), "second".to_string()])
        );

        // Mixed block types: only objects with type == "text" and a string
        // `text` survive (bare strings, tool blocks, missing/non-string text,
        // non-string type are all skipped — same as the SQL guards).
        let content = serde_json::json!([
            {"type": "tool_use", "name": "t", "toolCallId": "c1"},
            "bare-string-block",
            {"type": "text"},
            {"type": "text", "text": 42},
            {"type": 7, "text": "not-a-text-type"},
            {"type": "text", "text": "kept"},
        ]);
        assert_eq!(
            preview_text_blocks("assistant", &content),
            Some(vec!["kept".to_string()])
        );

        // Non-array content → None (the SQL CASE's NULL).
        for content in [
            serde_json::json!("plain string"),
            serde_json::json!({"type": "text", "text": "object, not array"}),
            serde_json::json!(3),
            serde_json::Value::Null,
        ] {
            assert_eq!(preview_text_blocks("user", &content), None);
            assert_eq!(preview_text_blocks("assistant", &content), None);
        }

        // Empty array → Some(vec![]) (json_group_array over zero rows is '[]').
        assert_eq!(
            preview_text_blocks("user", &serde_json::json!([])),
            Some(vec![])
        );

        // Oversized blocks: assistant keeps the TAIL, user the HEAD, counted
        // in chars (multi-byte safe) like SQLite substr.
        let big = format!("head-marker-{}-tail-marker", "é".repeat(2 * cap));
        let content = serde_json::json!([{"type": "text", "text": big}]);
        let tail = &preview_text_blocks("assistant", &content).unwrap()[0];
        assert_eq!(tail.chars().count(), cap, "assistant block capped");
        assert!(tail.ends_with("-tail-marker"), "assistant keeps its tail");
        let head = &preview_text_blocks("user", &content).unwrap()[0];
        assert_eq!(head.chars().count(), cap, "user block capped");
        assert!(head.starts_with("head-marker-"), "user keeps its head");
    }

    /// `append_agent_message_with_id` maintains the matching preview column in
    /// the same transaction as the INSERT: user/assistant appends overwrite
    /// their column (non-array content stores `"[]"`, the projection form),
    /// other roles leave both columns untouched.
    #[tokio::test]
    async fn preview_columns_maintained_on_append() {
        use intent_core::now_iso;

        let tmp = TempDb::new("test-agent-repo");
        let store = Store::open(&tmp).await.expect("create test store");
        let ts = now_iso();
        let ws_id = WorkspaceId("ws-preview-append".to_string());
        store
            .insert_workspace(&baseline_test_workspace(&ws_id, &ts))
            .await
            .expect("insert workspace");
        let agent_id = AgentId("agent-preview-append".to_string());
        store
            .insert_agent_session(&baseline_test_session(&agent_id, &ws_id, &ts, None))
            .await
            .expect("insert session");

        assert_eq!(
            read_preview_columns(&store, &agent_id).await,
            (None, None),
            "fresh session has NULL previews"
        );

        store
            .append_agent_message(
                &agent_id,
                "user",
                &serde_json::json!([{"type": "text", "text": "q1"}]),
                &ts,
            )
            .await
            .expect("append user");
        let (assistant, user) = read_preview_columns(&store, &agent_id).await;
        assert_eq!(assistant, None, "no assistant message yet");
        assert_eq!(decode_preview(user), Some(vec!["q1".to_string()]));

        store
            .append_agent_message(
                &agent_id,
                "assistant",
                &serde_json::json!([{"type": "text", "text": "a1"}]),
                &ts,
            )
            .await
            .expect("append assistant");
        let (assistant, user) = read_preview_columns(&store, &agent_id).await;
        assert_eq!(decode_preview(assistant), Some(vec!["a1".to_string()]));
        assert_eq!(decode_preview(user), Some(vec!["q1".to_string()]));

        store
            .append_agent_message(
                &agent_id,
                "tool",
                &serde_json::json!([{"type": "text", "text": "tool noise"}]),
                &ts,
            )
            .await
            .expect("append tool");
        let (assistant, user) = read_preview_columns(&store, &agent_id).await;
        assert_eq!(
            decode_preview(assistant),
            Some(vec!["a1".to_string()]),
            "tool append leaves previews untouched"
        );
        assert_eq!(decode_preview(user), Some(vec!["q1".to_string()]));

        // A newer user message with non-array content overwrites to '[]' —
        // the projection form (zero text blocks).
        store
            .append_agent_message(&agent_id, "user", &serde_json::json!("plain string"), &ts)
            .await
            .expect("append non-array user");
        let (assistant, user) = read_preview_columns(&store, &agent_id).await;
        assert_eq!(decode_preview(assistant), Some(vec!["a1".to_string()]));
        assert_eq!(
            user,
            Some("[]".to_string()),
            "non-array winner stores the projection form"
        );
    }

    /// `replace_agent_messages` recomputes both preview columns from the
    /// replacement batch: truncating to before the last assistant message
    /// rewinds the assistant preview, and an empty batch clears both.
    #[tokio::test]
    async fn preview_columns_recomputed_on_replace() {
        use intent_core::now_iso;

        let tmp = TempDb::new("test-agent-repo");
        let store = Store::open(&tmp).await.expect("create test store");
        let ts = now_iso();
        let ws_id = WorkspaceId("ws-preview-replace".to_string());
        store
            .insert_workspace(&baseline_test_workspace(&ws_id, &ts))
            .await
            .expect("insert workspace");
        let agent_id = AgentId("agent-preview-replace".to_string());
        store
            .insert_agent_session(&baseline_test_session(&agent_id, &ws_id, &ts, None))
            .await
            .expect("insert session");
        for (role, text) in [
            ("user", "q1"),
            ("assistant", "a1"),
            ("user", "q2"),
            ("assistant", "a2"),
        ] {
            store
                .append_agent_message(
                    &agent_id,
                    role,
                    &serde_json::json!([{"type": "text", "text": text}]),
                    &ts,
                )
                .await
                .expect("append");
        }
        let (assistant, user) = read_preview_columns(&store, &agent_id).await;
        assert_eq!(decode_preview(assistant), Some(vec!["a2".to_string()]));
        assert_eq!(decode_preview(user), Some(vec!["q2".to_string()]));

        // Truncate to before the last assistant message: the assistant
        // preview rewinds to a1, the user preview to q2.
        let batch = [("user", "q1"), ("assistant", "a1"), ("user", "q2")]
            .map(|(role, text)| (role, serde_json::json!([{"type": "text", "text": text}])));
        let replace: Vec<ReplaceMessage<'_>> = batch
            .iter()
            .map(|(role, content)| ReplaceMessage {
                role,
                content,
                metadata: None,
                created_at: &ts,
            })
            .collect();
        store
            .replace_agent_messages(&agent_id, &replace)
            .await
            .expect("replace truncated");
        let (assistant, user) = read_preview_columns(&store, &agent_id).await;
        assert_eq!(
            decode_preview(assistant),
            Some(vec!["a1".to_string()]),
            "assistant preview rewound to the batch's last assistant message"
        );
        assert_eq!(decode_preview(user), Some(vec!["q2".to_string()]));

        store
            .replace_agent_messages(&agent_id, &[])
            .await
            .expect("replace with empty batch");
        assert_eq!(
            read_preview_columns(&store, &agent_id).await,
            (None, None),
            "empty batch clears both previews"
        );
    }

    /// Equivalence: for sessions with messages — written via append, replace,
    /// and the importer's `insert_agent_session_with_messages` — the persisted
    /// preview columns decode to exactly what [`preview_text_blocks`] applied
    /// to the newest message of each role produces, including capped
    /// oversized blocks and mixed block types.
    #[tokio::test]
    async fn preview_columns_equal_last_message_projection() {
        use intent_core::now_iso;

        let tmp = TempDb::new("test-agent-repo");
        let store = Store::open(&tmp).await.expect("create test store");
        let ts = now_iso();
        let ws_id = WorkspaceId("ws-preview-equiv".to_string());
        store
            .insert_workspace(&baseline_test_workspace(&ws_id, &ts))
            .await
            .expect("insert workspace");

        let cap = PROJECTION_TEXT_BLOCK_CAP as usize;
        let big_user = format!("user-head-{}", "é".repeat(2 * cap));
        let big_assistant = format!("{}<agent_digest>d</agent_digest>", "a".repeat(2 * cap));
        let batch = [
            ("user", serde_json::json!([{"type": "text", "text": "q1"}])),
            (
                "assistant",
                serde_json::json!([{"type": "text", "text": "a1"}]),
            ),
            ("tool", serde_json::json!([{"type": "text", "text": "t1"}])),
            (
                "user",
                serde_json::json!([
                    {"type": "text", "text": big_user},
                    {"type": "tool_use", "name": "t", "toolCallId": "c1"},
                    {"type": "text", "text": "short block"},
                ]),
            ),
            (
                "assistant",
                serde_json::json!([{"type": "text", "text": big_assistant}]),
            ),
        ];

        // Session 1: written through the append funnel.
        let appended = AgentId("agent-equiv-append".to_string());
        store
            .insert_agent_session(&baseline_test_session(&appended, &ws_id, &ts, None))
            .await
            .expect("insert session");
        for (role, content) in &batch {
            store
                .append_agent_message(&appended, role, content, &ts)
                .await
                .expect("append");
        }

        // Session 2: written through the importer's batched insert.
        let imported = AgentId("agent-equiv-import".to_string());
        let replace: Vec<ReplaceMessage<'_>> = batch
            .iter()
            .map(|(role, content)| ReplaceMessage {
                role,
                content,
                metadata: None,
                created_at: &ts,
            })
            .collect();
        store
            .insert_agent_session_with_messages(
                &baseline_test_session(&imported, &ws_id, &ts, None),
                &replace,
            )
            .await
            .expect("insert session with messages");

        // Session 3: written through the replace path.
        let replaced = AgentId("agent-equiv-replace".to_string());
        store
            .insert_agent_session(&baseline_test_session(&replaced, &ws_id, &ts, None))
            .await
            .expect("insert session");
        store
            .replace_agent_messages(&replaced, &replace)
            .await
            .expect("replace");

        // Expected previews computed straight from the batch's last
        // user/assistant messages via `preview_text_blocks` — the same
        // reference the write path itself uses.
        let expected_assistant = preview_text_blocks("assistant", &batch[4].1);
        let expected_user = preview_text_blocks("user", &batch[3].1);
        for agent_id in [&appended, &imported, &replaced] {
            let (assistant_col, user_col) = read_preview_columns(&store, agent_id).await;
            assert_eq!(
                decode_preview(assistant_col),
                expected_assistant,
                "assistant preview matches last assistant message for {agent_id:?}"
            );
            assert_eq!(
                decode_preview(user_col),
                expected_user,
                "user preview matches last user message for {agent_id:?}"
            );
        }
    }

    /// The 0066 migration backfill stamps columns matching
    /// [`preview_text_blocks`] applied to the newest message of each role:
    /// message rows inserted raw (bypassing write-time maintenance, like
    /// pre-migration rows) get correct previews after running the migration —
    /// including `'[]'` for a non-array winner (the projection form) and NULL
    /// for sessions with no message of a role.
    #[tokio::test]
    async fn migration_backfill_matches_last_message_projection() {
        use intent_core::now_iso;

        use uuid::Uuid;
        let tmp = TempDb::new("test-agent-repo");
        let store = Store::open(&tmp).await.expect("create test store");
        let ts = now_iso();
        let ws_id = WorkspaceId("ws-preview-backfill".to_string());
        store
            .insert_workspace(&baseline_test_workspace(&ws_id, &ts))
            .await
            .expect("insert workspace");

        let cap = PROJECTION_TEXT_BLOCK_CAP as usize;
        let full = AgentId("agent-backfill-full".to_string());
        let non_array = AgentId("agent-backfill-non-array".to_string());
        let empty = AgentId("agent-backfill-empty".to_string());
        for id in [&full, &non_array, &empty] {
            store
                .insert_agent_session(&baseline_test_session(id, &ws_id, &ts, None))
                .await
                .expect("insert session");
        }
        let big_user = format!("user-head-{}", "é".repeat(2 * cap));
        let full_rows = [
            ("user", serde_json::json!([{"type": "text", "text": "q1"}])),
            (
                "assistant",
                serde_json::json!([{"type": "text", "text": "a1"}]),
            ),
            (
                "user",
                serde_json::json!([
                    {"type": "text", "text": big_user},
                    {"type": "tool_use", "name": "t"},
                    {"type": "text", "text": "short block"},
                ]),
            ),
        ];
        for (seq, (role, content)) in full_rows.iter().enumerate() {
            sqlx::query(
                "INSERT INTO agent_message (id, agent_id, seq, role, content, created_at) \
                 VALUES (?,?,?,?,?,?)",
            )
            .bind(Uuid::now_v7().to_string())
            .bind(&full.0)
            .bind(seq as i64)
            .bind(*role)
            .bind(serde_json::to_string(content).expect("encode"))
            .bind(&ts)
            .execute(store.write_pool())
            .await
            .expect("insert raw row");
        }
        sqlx::query(
            "INSERT INTO agent_message (id, agent_id, seq, role, content, created_at) \
             VALUES (?,?,?,?,?,?)",
        )
        .bind(Uuid::now_v7().to_string())
        .bind(&non_array.0)
        .bind(0_i64)
        .bind("user")
        .bind("\"plain string\"")
        .bind(&ts)
        .execute(store.write_pool())
        .await
        .expect("insert non-array row");

        // Raw inserts bypass write-time maintenance: columns are still NULL.
        assert_eq!(read_preview_columns(&store, &full).await, (None, None));

        // Recreate the pre-0066 shape (drop the columns the open-time
        // migration added), then re-run the migration file verbatim via
        // raw_sql — no statement re-parsing (PR #742 review).
        for column in ["last_assistant_preview", "last_user_preview"] {
            sqlx::query(&format!("ALTER TABLE agent_session DROP COLUMN {column}"))
                .execute(store.write_pool())
                .await
                .expect("drop preview column");
        }
        sqlx::raw_sql(include_str!(
            "../migrations/0066_agent_session_last_message_previews.sql"
        ))
        .execute(store.write_pool())
        .await
        .expect("re-run 0066 migration");

        let (assistant_col, user_col) = read_preview_columns(&store, &full).await;
        assert_eq!(
            decode_preview(assistant_col),
            preview_text_blocks("assistant", &full_rows[1].1),
            "backfilled assistant preview matches the newest assistant message"
        );
        assert_eq!(
            decode_preview(user_col),
            preview_text_blocks("user", &full_rows[2].1),
            "backfilled user preview matches the newest user message"
        );

        assert_eq!(
            read_preview_columns(&store, &non_array).await,
            (None, Some("[]".to_string())),
            "non-array winner backfills to '[]' (assistant NULL: no such message)"
        );
        assert_eq!(
            read_preview_columns(&store, &empty).await,
            (None, None),
            "message-less session stays NULL"
        );
    }

    /// Degrade-only semantics: a NULL preview column for a role that HAS
    /// messages (e.g. rows written by a pre-0066 daemon after a downgrade)
    /// projects to `None` — no repair is attempted, and neither read path
    /// writes to `agent_session`. The column converges only once a new
    /// message of that role is appended.
    #[tokio::test]
    async fn projection_degrades_null_preview_columns_without_repair() {
        use intent_core::now_iso;

        let tmp = TempDb::new("test-agent-repo");
        let store = Store::open(&tmp).await.expect("create test store");
        let ts = now_iso();
        let ws_id = WorkspaceId("ws-preview-degrade".to_string());
        store
            .insert_workspace(&baseline_test_workspace(&ws_id, &ts))
            .await
            .expect("insert workspace");

        let full = AgentId("agent-degrade-full".to_string());
        let user_only = AgentId("agent-degrade-user-only".to_string());
        for id in [&full, &user_only] {
            store
                .insert_agent_session(&baseline_test_session(id, &ws_id, &ts, None))
                .await
                .expect("insert session");
        }
        for (role, text) in [("user", "q1"), ("assistant", "a1")] {
            store
                .append_agent_message(
                    &full,
                    role,
                    &serde_json::json!([{"type": "text", "text": text}]),
                    &ts,
                )
                .await
                .expect("append");
        }
        store
            .append_agent_message(
                &user_only,
                "user",
                &serde_json::json!([{"type": "text", "text": "only"}]),
                &ts,
            )
            .await
            .expect("append user-only");

        // Simulate downgrade-era writes: wipe every preview column.
        sqlx::query(
            "UPDATE agent_session SET last_assistant_preview = NULL, last_user_preview = NULL",
        )
        .execute(store.write_pool())
        .await
        .expect("wipe preview columns");
        assert_eq!(read_preview_columns(&store, &full).await, (None, None));

        let projections = store
            .get_agent_session_message_projections(&ws_id)
            .await
            .expect("projections");
        let p = projections.get(&full.0).expect("full entry");
        assert!(
            p.last_assistant_text_blocks.is_none(),
            "NULL column degrades to None even though the session has an assistant message"
        );
        assert!(
            p.last_user_text_blocks.is_none(),
            "NULL column degrades to None even though the session has a user message"
        );
        assert_eq!(
            read_preview_columns(&store, &full).await,
            (None, None),
            "workspace read never writes to agent_session"
        );

        let p = projections.get(&user_only.0).expect("user-only entry");
        assert!(p.last_assistant_text_blocks.is_none());
        assert!(p.last_user_text_blocks.is_none());

        // The per-session variant degrades the same way and is equally
        // read-only.
        let per_session = store
            .get_agent_session_message_projection(&full)
            .await
            .expect("per-session projection");
        assert_eq!(per_session, *projections.get(&full.0).expect("full entry"));
        assert_eq!(
            read_preview_columns(&store, &full).await,
            (None, None),
            "per-session read never writes to agent_session"
        );

        // Convergence: appending a new message of a role stamps its column,
        // independent of the other (still-NULL) column.
        store
            .append_agent_message(
                &full,
                "assistant",
                &serde_json::json!([{"type": "text", "text": "a2"}]),
                &ts,
            )
            .await
            .expect("append converging message");
        let converged = store
            .get_agent_session_message_projection(&full)
            .await
            .expect("converged projection");
        assert_eq!(
            converged.last_assistant_text_blocks,
            Some(vec!["a2".to_string()]),
            "assistant column converges on next append"
        );
        assert!(
            converged.last_user_text_blocks.is_none(),
            "user column stays degraded until a user message is appended"
        );
    }

    /// A NULL preview column for a role with NO messages projects to `None`
    /// exactly as for any session without such a message — it is not treated
    /// as "damaged" and reads never touch `agent_message.content` to decide
    /// otherwise. Mutating the newest message's content behind the columns'
    /// back does not change the projection, because the column — not the
    /// message table — is always the source of truth.
    #[tokio::test]
    async fn projection_missing_role_column_stays_none_without_reading_content() {
        use intent_core::now_iso;

        let tmp = TempDb::new("test-agent-repo");
        let store = Store::open(&tmp).await.expect("create test store");
        let ts = now_iso();
        let ws_id = WorkspaceId("ws-preview-missing-role".to_string());
        store
            .insert_workspace(&baseline_test_workspace(&ws_id, &ts))
            .await
            .expect("insert workspace");
        let user_only = AgentId("agent-missing-role-user-only".to_string());
        store
            .insert_agent_session(&baseline_test_session(&user_only, &ws_id, &ts, None))
            .await
            .expect("insert session");
        store
            .append_agent_message(
                &user_only,
                "user",
                &serde_json::json!([{"type": "text", "text": "only"}]),
                &ts,
            )
            .await
            .expect("append user");

        let first = store
            .get_agent_session_message_projection(&user_only)
            .await
            .expect("first read");
        assert_eq!(first.last_user_text_blocks, Some(vec!["only".to_string()]));
        assert!(first.last_assistant_text_blocks.is_none());

        // Mutate the newest message's content behind the columns' back: if
        // any read path decoded `agent_message.content` instead of serving
        // the persisted column, the projection would change.
        sqlx::query("UPDATE agent_message SET content = ? WHERE agent_id = ?")
            .bind("[{\"type\":\"text\",\"text\":\"mutated\"}]")
            .bind(&user_only.0)
            .execute(store.write_pool())
            .await
            .expect("mutate winner content");

        for _ in 0..2 {
            let per_session = store
                .get_agent_session_message_projection(&user_only)
                .await
                .expect("repeat per-session read");
            assert_eq!(
                per_session, first,
                "repeat per-session reads are column-served and stable"
            );
            let workspace = store
                .get_agent_session_message_projections(&ws_id)
                .await
                .expect("repeat workspace read");
            assert_eq!(
                workspace.get(&user_only.0),
                Some(&first),
                "repeat workspace reads are column-served and stable"
            );
        }
        assert_eq!(
            read_preview_columns(&store, &user_only).await,
            (None, Some("[\"only\"]".to_string())),
            "columns unchanged after repeated reads"
        );
    }

    /// Raw `last_message_role` column value for a session (0070).
    async fn read_role_column(store: &Store, agent_id: &AgentId) -> Option<String> {
        sqlx::query("SELECT last_message_role FROM agent_session WHERE id = ?")
            .bind(&agent_id.0)
            .fetch_one(store.read_pool())
            .await
            .expect("read role column")
            .get("last_message_role")
    }

    /// `last_message_role` (0070) is maintained at message-write time:
    /// user/assistant appends overwrite it, other roles (system/tool) are
    /// transparent, and `replace_agent_messages` recomputes it from the
    /// batch (NULL when the batch has no user/assistant message).
    #[tokio::test]
    async fn last_message_role_maintained_on_writes() {
        use intent_core::now_iso;

        let tmp = TempDb::new("test-agent-repo");
        let store = Store::open(&tmp).await.expect("create test store");
        let ts = now_iso();
        let ws_id = WorkspaceId("ws-role-writes".to_string());
        store
            .insert_workspace(&baseline_test_workspace(&ws_id, &ts))
            .await
            .expect("insert workspace");
        let agent_id = AgentId("agent-role-writes".to_string());
        store
            .insert_agent_session(&baseline_test_session(&agent_id, &ws_id, &ts, None))
            .await
            .expect("insert session");

        assert_eq!(
            read_role_column(&store, &agent_id).await,
            None,
            "fresh session has NULL role"
        );

        let text = |t: &str| serde_json::json!([{"type": "text", "text": t}]);
        store
            .append_agent_message(&agent_id, "user", &text("q1"), &ts)
            .await
            .expect("append user");
        assert_eq!(
            read_role_column(&store, &agent_id).await,
            Some("user".to_string())
        );

        store
            .append_agent_message(&agent_id, "assistant", &text("a1"), &ts)
            .await
            .expect("append assistant");
        assert_eq!(
            read_role_column(&store, &agent_id).await,
            Some("assistant".to_string())
        );

        // System/tool appends are transparent: the role stays "assistant".
        for role in ["system", "tool"] {
            store
                .append_agent_message(&agent_id, role, &text("noise"), &ts)
                .await
                .expect("append transparent role");
        }
        assert_eq!(
            read_role_column(&store, &agent_id).await,
            Some("assistant".to_string()),
            "system/tool appends leave the role untouched"
        );

        // Replace recomputes from the batch: newest user/assistant row wins
        // even with a trailing system row.
        let batch =
            [("user", "q1"), ("assistant", "a1"), ("user", "q2")].map(|(role, t)| (role, text(t)));
        let replace: Vec<ReplaceMessage<'_>> = batch
            .iter()
            .map(|(role, content)| ReplaceMessage {
                role,
                content,
                metadata: None,
                created_at: &ts,
            })
            .collect();
        store
            .replace_agent_messages(&agent_id, &replace)
            .await
            .expect("replace");
        assert_eq!(
            read_role_column(&store, &agent_id).await,
            Some("user".to_string()),
            "replace recomputes the role from the batch"
        );

        store
            .replace_agent_messages(&agent_id, &[])
            .await
            .expect("replace with empty batch");
        assert_eq!(
            read_role_column(&store, &agent_id).await,
            None,
            "empty batch clears the role"
        );

        // Both projection read paths serve the column.
        store
            .append_agent_message(&agent_id, "user", &text("again"), &ts)
            .await
            .expect("append user again");
        let per_session = store
            .get_agent_session_message_projection(&agent_id)
            .await
            .expect("per-session projection");
        assert_eq!(per_session.last_message_role, Some("user".to_string()));
        let workspace = store
            .get_agent_session_message_projections(&ws_id)
            .await
            .expect("workspace projections");
        assert_eq!(
            workspace
                .get(&agent_id.0)
                .and_then(|p| p.last_message_role.clone()),
            Some("user".to_string())
        );
    }

    /// The 0070 migration backfill stamps `last_message_role` from the
    /// newest user/assistant row (system tails are transparent), leaving
    /// NULL for sessions with no such message; a NULL column left by a
    /// pre-0070 daemon (or any other cause) degrades to `None` on read
    /// without repair — both projection paths never write to
    /// `agent_session`, and the column converges only on the next
    /// user/assistant append.
    #[tokio::test]
    async fn last_message_role_backfill_and_degrades_without_repair() {
        use intent_core::now_iso;

        use uuid::Uuid;
        let tmp = TempDb::new("test-agent-repo");
        let store = Store::open(&tmp).await.expect("create test store");
        let ts = now_iso();
        let ws_id = WorkspaceId("ws-role-backfill".to_string());
        store
            .insert_workspace(&baseline_test_workspace(&ws_id, &ts))
            .await
            .expect("insert workspace");

        let user_newest = AgentId("agent-role-user-newest".to_string());
        let system_tail = AgentId("agent-role-system-tail".to_string());
        let empty = AgentId("agent-role-empty".to_string());
        let system_only = AgentId("agent-role-system-only".to_string());
        for id in [&user_newest, &system_tail, &empty, &system_only] {
            store
                .insert_agent_session(&baseline_test_session(id, &ws_id, &ts, None))
                .await
                .expect("insert session");
        }
        let raw_insert = |agent: &AgentId, seq: i64, role: &str| {
            let agent = agent.clone();
            let role = role.to_string();
            let ts = ts.clone();
            let store = &store;
            async move {
                sqlx::query(
                    "INSERT INTO agent_message (id, agent_id, seq, role, content, created_at) \
                     VALUES (?,?,?,?,?,?)",
                )
                .bind(Uuid::now_v7().to_string())
                .bind(&agent.0)
                .bind(seq)
                .bind(&role)
                .bind("[{\"type\":\"text\",\"text\":\"x\"}]")
                .bind(&ts)
                .execute(store.write_pool())
                .await
                .expect("insert raw row");
            }
        };
        for (seq, role) in [(0_i64, "assistant"), (1, "user")] {
            raw_insert(&user_newest, seq, role).await;
        }
        for (seq, role) in [(0_i64, "user"), (1, "assistant"), (2, "system")] {
            raw_insert(&system_tail, seq, role).await;
        }
        raw_insert(&system_only, 0, "system").await;

        // Raw inserts bypass write-time maintenance: still NULL. Recreate
        // the pre-0070 shape and re-run the migration file verbatim.
        assert_eq!(read_role_column(&store, &user_newest).await, None);
        sqlx::query("ALTER TABLE agent_session DROP COLUMN last_message_role")
            .execute(store.write_pool())
            .await
            .expect("drop role column");
        sqlx::raw_sql(include_str!(
            "../migrations/0070_agent_session_last_message_role.sql"
        ))
        .execute(store.write_pool())
        .await
        .expect("re-run 0070 migration");

        assert_eq!(
            read_role_column(&store, &user_newest).await,
            Some("user".to_string())
        );
        assert_eq!(
            read_role_column(&store, &system_tail).await,
            Some("assistant".to_string()),
            "system tail is transparent to the backfill"
        );
        assert_eq!(read_role_column(&store, &empty).await, None);
        assert_eq!(
            read_role_column(&store, &system_only).await,
            None,
            "system-only transcript backfills to NULL"
        );

        // Wipe the column and read through both projection paths: a NULL
        // column degrades to `None` without repair, even for a session with
        // a qualifying (transparently-tailed) message, and neither read
        // writes to `agent_session`.
        sqlx::query("UPDATE agent_session SET last_message_role = NULL")
            .execute(store.write_pool())
            .await
            .expect("wipe role column");
        let per_session = store
            .get_agent_session_message_projection(&system_tail)
            .await
            .expect("per-session read");
        assert_eq!(
            per_session.last_message_role, None,
            "NULL column degrades to None without repair"
        );
        assert_eq!(
            read_role_column(&store, &system_tail).await,
            None,
            "per-session read never writes to agent_session"
        );
        let workspace = store
            .get_agent_session_message_projections(&ws_id)
            .await
            .expect("workspace read");
        assert_eq!(
            workspace
                .get(&user_newest.0)
                .and_then(|p| p.last_message_role.clone()),
            None,
            "NULL column degrades to None for the workspace path too"
        );
        assert_eq!(
            workspace
                .get(&system_only.0)
                .and_then(|p| p.last_message_role.clone()),
            None,
            "system-only session stays None"
        );
        assert_eq!(
            read_role_column(&store, &user_newest).await,
            None,
            "workspace read never writes to agent_session"
        );

        // Convergence: a new user/assistant append stamps the column.
        store
            .append_agent_message(
                &system_tail,
                "assistant",
                &serde_json::json!([{"type": "text", "text": "converge"}]),
                &ts,
            )
            .await
            .expect("append converging message");
        assert_eq!(
            read_role_column(&store, &system_tail).await,
            Some("assistant".to_string()),
            "column converges on next append"
        );
    }

    /// Raw `last_message_id` column value for a session (0088).
    async fn read_id_column(store: &Store, agent_id: &AgentId) -> Option<String> {
        sqlx::query("SELECT last_message_id FROM agent_session WHERE id = ?")
            .bind(&agent_id.0)
            .fetch_one(store.read_pool())
            .await
            .expect("read id column")
            .get("last_message_id")
    }

    /// `last_message_id` (0088) is maintained at message-write time:
    /// user/assistant appends stamp the appended row's id, other roles
    /// (system/tool) are transparent, `replace_agent_messages` recomputes it
    /// from the batch (NULL when the batch has no user/assistant message),
    /// and the session-with-messages insert stamps the batch's newest
    /// user/assistant row.
    #[tokio::test]
    async fn last_message_id_maintained_on_writes() {
        use intent_core::now_iso;

        let tmp = TempDb::new("test-agent-repo");
        let store = Store::open(&tmp).await.expect("create test store");
        let ts = now_iso();
        let ws_id = WorkspaceId("ws-id-writes".to_string());
        store
            .insert_workspace(&baseline_test_workspace(&ws_id, &ts))
            .await
            .expect("insert workspace");
        let agent_id = AgentId("agent-id-writes".to_string());
        store
            .insert_agent_session(&baseline_test_session(&agent_id, &ws_id, &ts, None))
            .await
            .expect("insert session");

        assert_eq!(
            read_id_column(&store, &agent_id).await,
            None,
            "fresh session has NULL id"
        );

        let text = |t: &str| serde_json::json!([{"type": "text", "text": t}]);
        let user_msg = store
            .append_agent_message(&agent_id, "user", &text("q1"), &ts)
            .await
            .expect("append user");
        assert_eq!(
            read_id_column(&store, &agent_id).await,
            Some(user_msg.id.clone())
        );

        let assistant_msg = store
            .append_agent_message(&agent_id, "assistant", &text("a1"), &ts)
            .await
            .expect("append assistant");
        assert_eq!(
            read_id_column(&store, &agent_id).await,
            Some(assistant_msg.id.clone())
        );

        // System/tool appends are transparent: the id stays on the assistant
        // row.
        for role in ["system", "tool"] {
            store
                .append_agent_message(&agent_id, role, &text("noise"), &ts)
                .await
                .expect("append transparent role");
        }
        assert_eq!(
            read_id_column(&store, &agent_id).await,
            Some(assistant_msg.id.clone()),
            "system/tool appends leave the id untouched"
        );

        // Replace recomputes from the batch: newest user/assistant row wins
        // even with a trailing system row.
        let batch = [
            ("user", "q1"),
            ("assistant", "a1"),
            ("user", "q2"),
            ("system", "tail"),
        ]
        .map(|(role, t)| (role, text(t)));
        let replace: Vec<ReplaceMessage<'_>> = batch
            .iter()
            .map(|(role, content)| ReplaceMessage {
                role,
                content,
                metadata: None,
                created_at: &ts,
            })
            .collect();
        let inserted = store
            .replace_agent_messages(&agent_id, &replace)
            .await
            .expect("replace");
        assert_eq!(
            read_id_column(&store, &agent_id).await,
            Some(inserted[2].id.clone()),
            "replace stamps the batch's newest user/assistant row id"
        );

        store
            .replace_agent_messages(&agent_id, &[])
            .await
            .expect("replace with empty batch");
        assert_eq!(
            read_id_column(&store, &agent_id).await,
            None,
            "empty batch clears the id"
        );

        // Both projection read paths serve the column.
        let again = store
            .append_agent_message(&agent_id, "user", &text("again"), &ts)
            .await
            .expect("append user again");
        let per_session = store
            .get_agent_session_message_projection(&agent_id)
            .await
            .expect("per-session projection");
        assert_eq!(per_session.last_message_id, Some(again.id.clone()));
        let workspace = store
            .get_agent_session_message_projections(&ws_id)
            .await
            .expect("workspace projections");
        assert_eq!(
            workspace
                .get(&agent_id.0)
                .and_then(|p| p.last_message_id.clone()),
            Some(again.id.clone())
        );

        // The session-with-messages insert stamps the batch's newest
        // user/assistant row (the trailing system row is transparent). Ids
        // are minted by the store, so assert against the reloaded rows.
        let seeded = AgentId("agent-id-seeded".to_string());
        let session = baseline_test_session(&seeded, &ws_id, &ts, None);
        let seed_batch = [("user", "q1"), ("assistant", "a1"), ("system", "tail")]
            .map(|(role, t)| (role, text(t)));
        let seed_messages: Vec<ReplaceMessage<'_>> = seed_batch
            .iter()
            .map(|(role, content)| ReplaceMessage {
                role,
                content,
                metadata: None,
                created_at: &ts,
            })
            .collect();
        store
            .insert_agent_session_with_messages(&session, &seed_messages)
            .await
            .expect("insert seeded session");
        let stamped = read_id_column(&store, &seeded).await;
        let persisted = store
            .get_agent_session(&seeded)
            .await
            .expect("reload seeded session");
        assert_eq!(persisted.messages.len(), 3);
        assert_eq!(
            stamped.as_deref(),
            Some(persisted.messages[1].id.as_str()),
            "seeded insert stamps the newest user/assistant row id"
        );
    }

    /// The 0088 migration backfill stamps `last_message_id` from the newest
    /// user/assistant row (system tails are transparent), leaving NULL for
    /// sessions with no such message; a NULL column left by a pre-0088
    /// daemon (or any other cause) degrades to `None` on read without
    /// repair — both projection paths never write to `agent_session`, and
    /// the column converges only on the next user/assistant append.
    #[tokio::test]
    async fn last_message_id_backfill_and_degrades_without_repair() {
        use intent_core::now_iso;

        let tmp = TempDb::new("test-agent-repo");
        let store = Store::open(&tmp).await.expect("create test store");
        let ts = now_iso();
        let ws_id = WorkspaceId("ws-id-backfill".to_string());
        store
            .insert_workspace(&baseline_test_workspace(&ws_id, &ts))
            .await
            .expect("insert workspace");

        let user_newest = AgentId("agent-id-user-newest".to_string());
        let system_tail = AgentId("agent-id-system-tail".to_string());
        let empty = AgentId("agent-id-empty".to_string());
        let system_only = AgentId("agent-id-system-only".to_string());
        for id in [&user_newest, &system_tail, &empty, &system_only] {
            store
                .insert_agent_session(&baseline_test_session(id, &ws_id, &ts, None))
                .await
                .expect("insert session");
        }
        let text = |t: &str| serde_json::json!([{"type": "text", "text": t}]);
        store
            .append_agent_message(&user_newest, "assistant", &text("a"), &ts)
            .await
            .expect("append assistant");
        let un_user = store
            .append_agent_message(&user_newest, "user", &text("q"), &ts)
            .await
            .expect("append user");
        store
            .append_agent_message(&system_tail, "user", &text("q"), &ts)
            .await
            .expect("append user");
        let st_assistant = store
            .append_agent_message(&system_tail, "assistant", &text("a"), &ts)
            .await
            .expect("append assistant");
        store
            .append_agent_message(&system_tail, "system", &text("s"), &ts)
            .await
            .expect("append system");
        store
            .append_agent_message(&system_only, "system", &text("s"), &ts)
            .await
            .expect("append system");

        // Recreate the pre-0088 shape and re-run the migration file
        // verbatim: the backfill stamps from the newest user/assistant row.
        sqlx::query("ALTER TABLE agent_session DROP COLUMN last_message_id")
            .execute(store.write_pool())
            .await
            .expect("drop id column");
        sqlx::raw_sql(include_str!(
            "../migrations/0088_agent_session_last_message_id.sql"
        ))
        .execute(store.write_pool())
        .await
        .expect("re-run 0088 migration");

        assert_eq!(
            read_id_column(&store, &user_newest).await,
            Some(un_user.id.clone())
        );
        assert_eq!(
            read_id_column(&store, &system_tail).await,
            Some(st_assistant.id.clone()),
            "system tail is transparent to the backfill"
        );
        assert_eq!(read_id_column(&store, &empty).await, None);
        assert_eq!(
            read_id_column(&store, &system_only).await,
            None,
            "system-only transcript backfills to NULL"
        );

        // Wipe the column and read through both projection paths: a NULL
        // column degrades to `None` without repair, even for a session with
        // a qualifying (transparently-tailed) message, and neither read
        // writes to `agent_session`.
        sqlx::query("UPDATE agent_session SET last_message_id = NULL")
            .execute(store.write_pool())
            .await
            .expect("wipe id column");
        let per_session = store
            .get_agent_session_message_projection(&system_tail)
            .await
            .expect("per-session read");
        assert_eq!(
            per_session.last_message_id, None,
            "NULL column degrades to None without repair"
        );
        assert_eq!(
            read_id_column(&store, &system_tail).await,
            None,
            "per-session read never writes to agent_session"
        );
        let workspace = store
            .get_agent_session_message_projections(&ws_id)
            .await
            .expect("workspace read");
        assert_eq!(
            workspace
                .get(&user_newest.0)
                .and_then(|p| p.last_message_id.clone()),
            None,
            "NULL column degrades to None for the workspace path too"
        );
        assert_eq!(
            workspace
                .get(&system_only.0)
                .and_then(|p| p.last_message_id.clone()),
            None,
            "system-only session stays None"
        );
        assert_eq!(
            read_id_column(&store, &user_newest).await,
            None,
            "workspace read never writes to agent_session"
        );

        // Convergence: a new user/assistant append stamps the column.
        let converge = store
            .append_agent_message(
                &system_tail,
                "assistant",
                &serde_json::json!([{"type": "text", "text": "converge"}]),
                &ts,
            )
            .await
            .expect("append converging message");
        assert_eq!(
            read_id_column(&store, &system_tail).await,
            Some(converge.id),
            "column converges on next append"
        );
    }

    /// Raw `last_tool_use_preview` column value for a session (0098),
    /// decoded as JSON.
    async fn read_tool_use_column(store: &Store, agent_id: &AgentId) -> Option<serde_json::Value> {
        let raw: Option<String> =
            sqlx::query("SELECT last_tool_use_preview FROM agent_session WHERE id = ?")
                .bind(&agent_id.0)
                .fetch_one(store.read_pool())
                .await
                .expect("read tool use column")
                .get("last_tool_use_preview");
        raw.map(|s| serde_json::from_str(&s).expect("column is valid JSON"))
    }

    /// `last_tool_use_preview` (0098) is maintained at message-write time:
    /// a user/assistant append stamps the row's last tool_use block preview
    /// (NULL actively clears when the row carries no tool_use), system/tool
    /// appends are transparent, an over-budget input is capped with the
    /// additive truncation flags, and `replace_agent_messages` recomputes
    /// from the batch. Both projection read paths serve the column.
    #[tokio::test]
    async fn last_tool_use_preview_maintained_on_writes() {
        use intent_core::now_iso;

        let tmp = TempDb::new("test-agent-repo");
        let store = Store::open(&tmp).await.expect("create test store");
        let ts = now_iso();
        let ws_id = WorkspaceId("ws-tooluse-writes".to_string());
        store
            .insert_workspace(&baseline_test_workspace(&ws_id, &ts))
            .await
            .expect("insert workspace");
        let agent_id = AgentId("agent-tooluse-writes".to_string());
        store
            .insert_agent_session(&baseline_test_session(&agent_id, &ws_id, &ts, None))
            .await
            .expect("insert session");

        assert_eq!(
            read_tool_use_column(&store, &agent_id).await,
            None,
            "fresh session has NULL preview"
        );

        let text = |t: &str| serde_json::json!([{"type": "text", "text": t}]);
        store
            .append_agent_message(&agent_id, "user", &text("q1"), &ts)
            .await
            .expect("append user");
        assert_eq!(
            read_tool_use_column(&store, &agent_id).await,
            None,
            "text-only row keeps NULL"
        );

        // Assistant row with two tool_use blocks: the LAST one wins, small
        // input passes through whole (no flags).
        let with_tools = serde_json::json!([
            {"type": "tool_use", "id": "m:0", "name": "first", "input": {"a": 1}, "toolCallId": "tc-0"},
            {"type": "text", "text": "between"},
            {"type": "tool_use", "id": "m:2", "name": "view", "input": {"path": "/tmp/f"}, "toolCallId": "tc-2"},
        ]);
        store
            .append_agent_message(&agent_id, "assistant", &with_tools, &ts)
            .await
            .expect("append assistant with tools");
        assert_eq!(
            read_tool_use_column(&store, &agent_id).await,
            Some(serde_json::json!({"name": "view", "input": {"path": "/tmp/f"}})),
            "last tool_use block wins, small input whole, no flags"
        );

        // System/tool appends are transparent.
        for role in ["system", "tool"] {
            store
                .append_agent_message(&agent_id, role, &text("noise"), &ts)
                .await
                .expect("append transparent role");
        }
        assert!(
            read_tool_use_column(&store, &agent_id).await.is_some(),
            "system/tool appends leave the preview untouched"
        );

        // A newer user/assistant row WITHOUT a tool_use actively clears.
        store
            .append_agent_message(&agent_id, "assistant", &text("plain"), &ts)
            .await
            .expect("append plain assistant");
        assert_eq!(
            read_tool_use_column(&store, &agent_id).await,
            None,
            "tool-less user/assistant row clears the preview"
        );

        // Over-budget input is capped with the additive flags; the small
        // scalar keys classifyTool reads survive the giant sibling.
        let big = serde_json::json!([{
            "type": "tool_use", "id": "m:0", "name": "write_file",
            "input": {
                "path": "/tmp/big.txt",
                "content": "x".repeat(intent_core::SLIM_PROJECTION_BUDGET_BYTES * 4),
            },
            "toolCallId": "tc-big",
        }]);
        store
            .append_agent_message(&agent_id, "assistant", &big, &ts)
            .await
            .expect("append oversized tool input");
        let preview = read_tool_use_column(&store, &agent_id)
            .await
            .expect("preview present");
        assert_eq!(preview["name"], "write_file");
        assert_eq!(preview["inputTruncated"], true);
        assert!(preview["inputBytes"].as_u64().unwrap() > 0);
        assert_eq!(preview["input"]["path"], "/tmp/big.txt");
        let capped = preview["input"]["content"].as_str().unwrap();
        assert!(capped.len() < intent_core::SLIM_PROJECTION_BUDGET_BYTES * 4);

        // Replace recomputes from the batch (newest user/assistant wins,
        // trailing system rows transparent), and an empty batch clears.
        let tool_row = serde_json::json!([
            {"type": "tool_use", "id": "m:0", "name": "grep", "input": {"q": "x"}, "toolCallId": "tc-r"},
        ]);
        let batch = vec![
            ReplaceMessage {
                role: "assistant",
                content: &tool_row,
                metadata: None,
                created_at: &ts,
            },
            ReplaceMessage {
                role: "user",
                content: &tool_row,
                metadata: None,
                created_at: &ts,
            },
            ReplaceMessage {
                role: "system",
                content: &tool_row,
                metadata: None,
                created_at: &ts,
            },
        ];
        store
            .replace_agent_messages(&agent_id, &batch)
            .await
            .expect("replace");
        assert_eq!(
            read_tool_use_column(&store, &agent_id).await,
            Some(serde_json::json!({"name": "grep", "input": {"q": "x"}})),
            "replace stamps the batch's newest user/assistant row's preview"
        );
        store
            .replace_agent_messages(&agent_id, &[])
            .await
            .expect("replace empty");
        assert_eq!(read_tool_use_column(&store, &agent_id).await, None);

        // Both projection read paths serve the column; a corrupt value
        // degrades to None without repair.
        store
            .append_agent_message(&agent_id, "assistant", &tool_row, &ts)
            .await
            .expect("re-append tool row");
        let per_session = store
            .get_agent_session_message_projection(&agent_id)
            .await
            .expect("per-session projection");
        assert_eq!(
            per_session.last_tool_use,
            Some(serde_json::json!({"name": "grep", "input": {"q": "x"}}))
        );
        let workspace = store
            .get_agent_session_message_projections(&ws_id)
            .await
            .expect("workspace projections");
        assert_eq!(
            workspace
                .get(&agent_id.0)
                .and_then(|p| p.last_tool_use.clone()),
            Some(serde_json::json!({"name": "grep", "input": {"q": "x"}}))
        );
        sqlx::query("UPDATE agent_session SET last_tool_use_preview = '{corrupt' WHERE id = ?")
            .bind(&agent_id.0)
            .execute(store.write_pool())
            .await
            .expect("corrupt column");
        let degraded = store
            .get_agent_session_message_projection(&agent_id)
            .await
            .expect("read survives corrupt column");
        assert_eq!(
            degraded.last_tool_use, None,
            "corrupt column degrades to None without repair"
        );
    }

    /// The 0098 migration backfill stamps `last_tool_use_preview` from the
    /// newest user/assistant row's LAST tool_use block: a small input is
    /// stored whole, an over-budget input stores only the truncation flags
    /// (the SQL backfill's one bounded divergence from the Rust write path),
    /// and tool-less / empty transcripts stay NULL.
    #[tokio::test]
    async fn last_tool_use_preview_backfill() {
        use intent_core::now_iso;

        let tmp = TempDb::new("test-agent-repo");
        let store = Store::open(&tmp).await.expect("create test store");
        let ts = now_iso();
        let ws_id = WorkspaceId("ws-tooluse-backfill".to_string());
        store
            .insert_workspace(&baseline_test_workspace(&ws_id, &ts))
            .await
            .expect("insert workspace");

        let with_tool = AgentId("agent-bf-tool".to_string());
        let big_tool = AgentId("agent-bf-big".to_string());
        let boundary = AgentId("agent-bf-boundary".to_string());
        let no_tool = AgentId("agent-bf-plain".to_string());
        let empty = AgentId("agent-bf-empty".to_string());
        for id in [&with_tool, &big_tool, &boundary, &no_tool, &empty] {
            store
                .insert_agent_session(&baseline_test_session(id, &ws_id, &ts, None))
                .await
                .expect("insert session");
        }
        let text = |t: &str| serde_json::json!([{"type": "text", "text": t}]);
        store
            .append_agent_message(
                &with_tool,
                "assistant",
                &serde_json::json!([
                    {"type": "tool_use", "id": "m:0", "name": "old", "input": {}, "toolCallId": "t0"},
                    {"type": "tool_use", "id": "m:1", "name": "view", "input": {"path": "/x"}, "toolCallId": "t1"},
                ]),
                &ts,
            )
            .await
            .expect("append tool row");
        store
            .append_agent_message(&with_tool, "system", &text("tail"), &ts)
            .await
            .expect("append system tail");
        store
            .append_agent_message(
                &big_tool,
                "assistant",
                &serde_json::json!([{
                    "type": "tool_use", "id": "m:0", "name": "write_file",
                    "input": {"content": "x".repeat(intent_core::SLIM_PROJECTION_BUDGET_BYTES * 4)},
                    "toolCallId": "tb",
                }]),
                &ts,
            )
            .await
            .expect("append big tool row");
        // Exactly-at-budget STRING input: the Rust write path measures a
        // string body by its raw byte length (`slim_body_size`), so this row
        // must backfill whole — measuring the JSON form (`->`) would count
        // the surrounding quotes and flag it truncated (off-by-two).
        let boundary_str = "y".repeat(intent_core::SLIM_PROJECTION_BUDGET_BYTES);
        store
            .append_agent_message(
                &boundary,
                "assistant",
                &serde_json::json!([{
                    "type": "tool_use", "id": "m:0", "name": "bash",
                    "input": boundary_str,
                    "toolCallId": "ty",
                }]),
                &ts,
            )
            .await
            .expect("append boundary tool row");
        store
            .append_agent_message(&no_tool, "assistant", &text("plain"), &ts)
            .await
            .expect("append plain row");

        // Recreate the pre-0098 shape and re-run the migration verbatim.
        sqlx::query("ALTER TABLE agent_session DROP COLUMN last_tool_use_preview")
            .execute(store.write_pool())
            .await
            .expect("drop column");
        sqlx::raw_sql(include_str!(
            "../migrations/0098_agent_session_last_tool_use_preview.sql"
        ))
        .execute(store.write_pool())
        .await
        .expect("re-run 0098 migration");

        assert_eq!(
            read_tool_use_column(&store, &with_tool).await,
            Some(serde_json::json!({"name": "view", "input": {"path": "/x"}})),
            "backfill stamps the last tool_use of the newest user/assistant row (system tail transparent)"
        );
        let big = read_tool_use_column(&store, &big_tool)
            .await
            .expect("big preview present");
        assert_eq!(big["name"], "write_file");
        assert_eq!(big["inputTruncated"], true);
        assert!(big["inputBytes"].as_u64().unwrap() > 0);
        assert!(
            big.get("input").is_none(),
            "SQL backfill stores flags only for over-budget inputs"
        );
        assert_eq!(
            read_tool_use_column(&store, &boundary).await,
            Some(serde_json::json!({"name": "bash", "input": boundary_str})),
            "an exactly-at-budget string input backfills whole — the SQL size \
             accounting matches slim_body_size (raw bytes, not quoted JSON)"
        );
        assert_eq!(read_tool_use_column(&store, &no_tool).await, None);
        assert_eq!(read_tool_use_column(&store, &empty).await, None);
    }

    /// A corrupt (non-JSON) preview column value never fails the projection
    /// read: [`decode_preview_col`] degrades it to `None` (with a warning)
    /// instead of failing the whole projection — no repair is attempted, the
    /// corrupt column is left untouched, and both read paths return the
    /// degraded projection.
    #[tokio::test]
    async fn projection_degrades_corrupt_preview_columns_without_repair() {
        use intent_core::now_iso;

        let tmp = TempDb::new("test-agent-repo");
        let store = Store::open(&tmp).await.expect("create test store");
        let ts = now_iso();
        let ws_id = WorkspaceId("ws-preview-corrupt".to_string());
        store
            .insert_workspace(&baseline_test_workspace(&ws_id, &ts))
            .await
            .expect("insert workspace");
        let agent_id = AgentId("agent-corrupt".to_string());
        store
            .insert_agent_session(&baseline_test_session(&agent_id, &ws_id, &ts, None))
            .await
            .expect("insert session");
        for (role, text) in [("user", "q1"), ("assistant", "a1")] {
            store
                .append_agent_message(
                    &agent_id,
                    role,
                    &serde_json::json!([{"type": "text", "text": text}]),
                    &ts,
                )
                .await
                .expect("append");
        }

        // Corrupt both columns (truncated JSON).
        sqlx::query(
            "UPDATE agent_session SET last_assistant_preview = '[\"trunc', \
             last_user_preview = '{not json' WHERE id = ?",
        )
        .bind(&agent_id.0)
        .execute(store.write_pool())
        .await
        .expect("corrupt preview columns");

        let per_session = store
            .get_agent_session_message_projection(&agent_id)
            .await
            .expect("per-session read survives corrupt columns");
        assert!(
            per_session.last_assistant_text_blocks.is_none(),
            "corrupt assistant column degrades to None"
        );
        assert!(
            per_session.last_user_text_blocks.is_none(),
            "corrupt user column degrades to None"
        );
        let (assistant_col, user_col) = read_preview_columns(&store, &agent_id).await;
        assert_eq!(
            assistant_col,
            Some("[\"trunc".to_string()),
            "corrupt assistant column left untouched (no repair)"
        );
        assert_eq!(
            user_col,
            Some("{not json".to_string()),
            "corrupt user column left untouched (no repair)"
        );

        // The workspace path degrades the same way.
        let projections = store
            .get_agent_session_message_projections(&ws_id)
            .await
            .expect("workspace read survives corrupt columns");
        let p = projections.get(&agent_id.0).expect("agent entry");
        assert!(p.last_assistant_text_blocks.is_none());
        assert!(p.last_user_text_blocks.is_none());

        // Convergence: a new append overwrites the corrupt column.
        store
            .append_agent_message(
                &agent_id,
                "user",
                &serde_json::json!([{"type": "text", "text": "q2"}]),
                &ts,
            )
            .await
            .expect("append converging message");
        let (_, user_col) = read_preview_columns(&store, &agent_id).await;
        assert_eq!(
            decode_preview(user_col),
            Some(vec!["q2".to_string()]),
            "corrupt column overwritten by the next append"
        );
    }

    /// Message ids whose `agent_message_fts` (0074) row matches `query`,
    /// joined back through the rowid mapping (the FTS table is contentless,
    /// so text is never read back directly — matches are the observable).
    async fn fts_match_ids(store: &Store, query: &str) -> Vec<String> {
        sqlx::query(
            "SELECT m.id FROM agent_message_fts JOIN agent_message m \
             ON m.rowid = agent_message_fts.rowid \
             WHERE agent_message_fts MATCH ? ORDER BY m.id",
        )
        .bind(query)
        .fetch_all(store.read_pool())
        .await
        .expect("fts match query")
        .iter()
        .map(|row| row.get::<String, _>("id"))
        .collect()
    }

    async fn fts_row_count(store: &Store) -> i64 {
        sqlx::query("SELECT COUNT(*) AS n FROM agent_message_fts")
            .fetch_one(store.read_pool())
            .await
            .expect("fts count")
            .get("n")
    }

    /// Append-path FTS maintenance (0074): user/assistant appends are
    /// indexed with the search-side `message_text` extraction semantics —
    /// bare-string content as-is, content-block arrays as their string
    /// `text` fields joined by single spaces (non-text blocks contribute
    /// nothing), other shapes as compact JSON — while tool/system rows are
    /// never indexed.
    #[tokio::test]
    async fn fts_appends_indexed_with_message_text_semantics() {
        use intent_core::now_iso;
        let tmp = TempDb::new("test-agent-repo");
        let store = Store::open(&tmp).await.expect("create test store");
        let ts = now_iso();
        let ws_id = WorkspaceId("ws-fts-append".to_string());
        store
            .insert_workspace(&baseline_test_workspace(&ws_id, &ts))
            .await
            .expect("insert workspace");
        let agent = AgentId("agent-fts-append".to_string());
        store
            .insert_agent_session(&baseline_test_session(&agent, &ws_id, &ts, None))
            .await
            .expect("insert session");

        let user = store
            .append_agent_message(
                &agent,
                "user",
                &serde_json::json!([
                    {"type": "text", "text": "please deploy the"},
                    {"type": "tool_use", "name": "excludedblockterm"},
                    {"type": "text", "text": "staging environment"},
                ]),
                &ts,
            )
            .await
            .expect("append user");
        let assistant = store
            .append_agent_message(
                &agent,
                "assistant",
                &serde_json::json!("bare string reply zebrafish"),
                &ts,
            )
            .await
            .expect("append assistant");
        let fallback = store
            .append_agent_message(
                &agent,
                "user",
                &serde_json::json!({"unexpected": "shape", "word": "quokka"}),
                &ts,
            )
            .await
            .expect("append fallback-shape user");
        store
            .append_agent_message(
                &agent,
                "tool",
                &serde_json::json!([{"type": "text", "text": "toolonlyterm"}]),
                &ts,
            )
            .await
            .expect("append tool");
        store
            .append_agent_message(
                &agent,
                "system",
                &serde_json::json!([{"type": "text", "text": "systemonlyterm"}]),
                &ts,
            )
            .await
            .expect("append system");

        assert_eq!(
            fts_row_count(&store).await,
            3,
            "only the user/assistant rows are indexed"
        );
        // Phrase across the block boundary proves single-space joining.
        assert_eq!(
            fts_match_ids(&store, "\"the staging\"").await,
            vec![user.id.clone()]
        );
        assert_eq!(
            fts_match_ids(&store, "zebrafish").await,
            vec![assistant.id.clone()]
        );
        // Porter stemming: "deploy" indexed, query with a stemmed variant.
        assert_eq!(
            fts_match_ids(&store, "deploying").await,
            vec![user.id.clone()]
        );
        // Non-string/array content falls back to its compact JSON encoding.
        assert_eq!(fts_match_ids(&store, "quokka").await, vec![fallback.id]);
        // Non-text blocks and non-user/assistant roles contribute nothing.
        assert!(fts_match_ids(&store, "excludedblockterm").await.is_empty());
        assert!(fts_match_ids(&store, "toolonlyterm").await.is_empty());
        assert!(fts_match_ids(&store, "systemonlyterm").await.is_empty());
    }

    /// The `agent.replaceMessages` swap (DELETE + re-INSERT) drops every
    /// stale FTS row and indexes only the replacement batch's
    /// user/assistant rows; deleting the session cascades `agent_message`
    /// away and the cascade fires the FTS delete trigger, emptying the
    /// index.
    #[tokio::test]
    async fn fts_synced_on_replace_and_session_delete() {
        use intent_core::now_iso;
        let tmp = TempDb::new("test-agent-repo");
        let store = Store::open(&tmp).await.expect("create test store");
        let ts = now_iso();
        let ws_id = WorkspaceId("ws-fts-swap".to_string());
        store
            .insert_workspace(&baseline_test_workspace(&ws_id, &ts))
            .await
            .expect("insert workspace");
        let agent = AgentId("agent-fts-swap".to_string());
        store
            .insert_agent_session(&baseline_test_session(&agent, &ws_id, &ts, None))
            .await
            .expect("insert session");
        for text in ["staleterm one", "staleterm two"] {
            store
                .append_agent_message(
                    &agent,
                    "user",
                    &serde_json::json!([{"type": "text", "text": text}]),
                    &ts,
                )
                .await
                .expect("append original");
        }
        assert_eq!(fts_match_ids(&store, "staleterm").await.len(), 2);

        let replacement_user = serde_json::json!([{"type": "text", "text": "freshterm"}]);
        let replacement_tool = serde_json::json!([{"type": "text", "text": "swappedtoolterm"}]);
        let replaced = store
            .replace_agent_messages(
                &agent,
                &[
                    ReplaceMessage {
                        role: "user",
                        content: &replacement_user,
                        metadata: None,
                        created_at: &ts,
                    },
                    ReplaceMessage {
                        role: "tool",
                        content: &replacement_tool,
                        metadata: None,
                        created_at: &ts,
                    },
                ],
            )
            .await
            .expect("replace messages");
        assert!(
            fts_match_ids(&store, "staleterm").await.is_empty(),
            "stale rows are gone after the swap"
        );
        assert_eq!(
            fts_match_ids(&store, "freshterm").await,
            vec![replaced[0].id.clone()]
        );
        assert!(fts_match_ids(&store, "swappedtoolterm").await.is_empty());
        assert_eq!(fts_row_count(&store).await, 1);

        assert!(store
            .delete_agent_session(&ws_id, &agent)
            .await
            .expect("delete session"));
        assert_eq!(
            fts_row_count(&store).await,
            0,
            "cascade delete of agent_message empties the index"
        );
    }

    /// The 0074 migration backfills pre-existing rows: raw-inserted messages
    /// (triggers dropped to simulate a pre-0074 database) become searchable
    /// after re-running the migration file verbatim, with the same role
    /// filter and extraction semantics as the write-time triggers.
    #[tokio::test]
    async fn fts_migration_backfills_existing_rows() {
        use intent_core::now_iso;
        let tmp = TempDb::new("test-agent-repo");
        let store = Store::open(&tmp).await.expect("create test store");
        let ts = now_iso();
        let ws_id = WorkspaceId("ws-fts-backfill".to_string());
        store
            .insert_workspace(&baseline_test_workspace(&ws_id, &ts))
            .await
            .expect("insert workspace");
        let agent = AgentId("agent-fts-backfill".to_string());
        store
            .insert_agent_session(&baseline_test_session(&agent, &ws_id, &ts, None))
            .await
            .expect("insert session");

        // Recreate the pre-0074 shape (no FTS table, no triggers), then
        // raw-insert rows the way a pre-0074 daemon would have.
        for stmt in [
            "DROP TRIGGER agent_message_fts_after_insert",
            "DROP TRIGGER agent_message_fts_after_delete",
            "DROP TRIGGER agent_message_fts_after_update",
            "DROP TABLE agent_message_fts",
        ] {
            sqlx::query(stmt)
                .execute(store.write_pool())
                .await
                .expect("drop 0074 objects");
        }
        for (seq, (role, content)) in [
            ("user", "[{\"type\":\"text\",\"text\":\"backfilledterm\"}]"),
            (
                "tool",
                "[{\"type\":\"text\",\"text\":\"backfilltoolterm\"}]",
            ),
        ]
        .iter()
        .enumerate()
        {
            sqlx::query(
                "INSERT INTO agent_message (id, agent_id, seq, role, content, created_at) \
                 VALUES (?,?,?,?,?,?)",
            )
            .bind(Uuid::now_v7().to_string())
            .bind(&agent.0)
            .bind(seq as i64)
            .bind(*role)
            .bind(*content)
            .bind(&ts)
            .execute(store.write_pool())
            .await
            .expect("insert raw pre-0074 row");
        }

        sqlx::raw_sql(include_str!("../migrations/0074_agent_message_fts.sql"))
            .execute(store.write_pool())
            .await
            .expect("re-run 0074 migration");

        assert_eq!(fts_match_ids(&store, "backfilledterm").await.len(), 1);
        assert!(fts_match_ids(&store, "backfilltoolterm").await.is_empty());
        assert_eq!(fts_row_count(&store).await, 1);
    }

    /// The one-time activation VACUUM (`activate_incremental_vacuum`) may
    /// renumber `agent_message`'s implicit rowids (TEXT primary key), so it
    /// rebuilds the rowid-keyed FTS index afterwards: matches keep resolving
    /// to the correct message rows even when rowids actually shifted.
    #[tokio::test]
    async fn fts_rebuilt_after_vacuum_activation() {
        use crate::AutoVacuumActivation;
        use intent_core::now_iso;
        let tmp = TempDb::new("test-agent-repo");
        // Legacy DB in auto_vacuum=NONE mode so activation runs a real VACUUM
        // (Store::open's pragma is recorded but inert on an existing file).
        {
            let opts = sqlx::sqlite::SqliteConnectOptions::new()
                .filename(&*tmp)
                .create_if_missing(true);
            let pool = sqlx::sqlite::SqlitePoolOptions::new()
                .max_connections(1)
                .connect_with(opts)
                .await
                .expect("open legacy pool");
            sqlx::query("CREATE TABLE filler (id INTEGER PRIMARY KEY, data BLOB)")
                .execute(&pool)
                .await
                .expect("create filler");
            pool.close().await;
        }
        let store = Store::open(&tmp).await.expect("open store");
        let ts = now_iso();
        let ws_id = WorkspaceId("ws-fts-vacuum".to_string());
        store
            .insert_workspace(&baseline_test_workspace(&ws_id, &ts))
            .await
            .expect("insert workspace");
        let agent = AgentId("agent-fts-vacuum".to_string());
        store
            .insert_agent_session(&baseline_test_session(&agent, &ws_id, &ts, None))
            .await
            .expect("insert session");
        // A deleted low-rowid row leaves a gap, so the VACUUM renumbers the
        // survivor's implicit rowid — the desync the rebuild guards against.
        let doomed = store
            .append_agent_message(
                &agent,
                "user",
                &serde_json::json!([{"type": "text", "text": "doomed"}]),
                &ts,
            )
            .await
            .expect("append doomed");
        let survivor = store
            .append_agent_message(
                &agent,
                "user",
                &serde_json::json!([{"type": "text", "text": "survivorterm"}]),
                &ts,
            )
            .await
            .expect("append survivor");
        sqlx::query("DELETE FROM agent_message WHERE id = ?")
            .bind(&doomed.id)
            .execute(store.write_pool())
            .await
            .expect("delete doomed row");

        match store
            .activate_incremental_vacuum()
            .await
            .expect("activation")
        {
            AutoVacuumActivation::Activated { .. } => {}
            AutoVacuumActivation::AlreadyIncremental => {
                panic!("first activation on a NONE DB should run VACUUM")
            }
        }

        assert_eq!(
            fts_match_ids(&store, "survivorterm").await,
            vec![survivor.id.clone()],
            "post-VACUUM matches resolve to the correct message row"
        );
        assert_eq!(fts_row_count(&store).await, 1);
        // And the rebuilt index keeps tracking subsequent writes.
        let after = store
            .append_agent_message(
                &agent,
                "assistant",
                &serde_json::json!([{"type": "text", "text": "postvacuumterm"}]),
                &ts,
            )
            .await
            .expect("append after rebuild");
        assert_eq!(
            fts_match_ids(&store, "postvacuumterm").await,
            vec![after.id]
        );
    }

    /// `search.messages` workspace tiering: with equally-relevant matches in
    /// a preferred active workspace, another active workspace, and an
    /// archived workspace, adjusted ranks order preferred → other active →
    /// archived. The [`ARCHIVED_WORKSPACE_PENALTY`] applies with or without
    /// `prefer_workspace_id`, and hard workspace scoping still returns
    /// archived rows (the penalty is a ranking adjustment, never a filter).
    #[tokio::test]
    async fn search_messages_fts_tiers_active_above_archived() {
        use intent_core::now_iso;
        let tmp = TempDb::new("test-agent-repo");
        let store = Store::open(&tmp).await.expect("create test store");
        let ts = now_iso();
        let ws_pref = WorkspaceId("ws-tier-pref".to_string());
        let ws_active = WorkspaceId("ws-tier-active".to_string());
        let ws_arch = WorkspaceId("ws-tier-arch".to_string());
        for ws in [&ws_pref, &ws_active] {
            store
                .insert_workspace(&baseline_test_workspace(ws, &ts))
                .await
                .expect("insert workspace");
        }
        let mut archived_ws = baseline_test_workspace(&ws_arch, &ts);
        archived_ws.archived = true;
        archived_ws.archived_at = Some(ts.clone());
        store
            .insert_workspace(&archived_ws)
            .await
            .expect("insert archived workspace");
        for (agent, ws) in [
            ("agent-tier-pref", &ws_pref),
            ("agent-tier-active", &ws_active),
            ("agent-tier-arch", &ws_arch),
        ] {
            let agent = AgentId(agent.to_string());
            store
                .insert_agent_session(&baseline_test_session(&agent, ws, &ts, None))
                .await
                .expect("insert session");
            store
                .append_agent_message(
                    &agent,
                    "user",
                    &serde_json::json!("tierterm equally relevant"),
                    &ts,
                )
                .await
                .expect("append message");
        }
        let ws_of = |hits: &[MessageFtsMatch]| {
            hits.iter()
                .map(|h| h.workspace_id.clone())
                .collect::<Vec<_>>()
        };

        // preferred → other active → archived under prefer_workspace_id.
        let hits = store
            .search_agent_messages_fts("tierterm", None, None, None, Some(&ws_pref), None)
            .await
            .expect("tiered search");
        assert_eq!(
            ws_of(&hits),
            vec![ws_pref.0.clone(), ws_active.0.clone(), ws_arch.0.clone()]
        );

        // No prefer_workspace_id: the archived penalty still applies, by
        // exactly ARCHIVED_WORKSPACE_PENALTY bm25 units on equal content.
        let hits = store
            .search_agent_messages_fts("tierterm", None, None, None, None, None)
            .await
            .expect("unpreferred search");
        assert_eq!(hits.len(), 3);
        assert_eq!(hits[2].workspace_id, ws_arch.0, "archived match ranks last");
        assert!((hits[2].rank - hits[0].rank - ARCHIVED_WORKSPACE_PENALTY).abs() < 1e-9);

        // Hard workspace scoping to the archived workspace still matches.
        let hits = store
            .search_agent_messages_fts("tierterm", Some(&ws_arch), None, None, None, None)
            .await
            .expect("scoped search");
        assert_eq!(ws_of(&hits), vec![ws_arch.0.clone()]);
    }

    /// The archived-workspace penalty is a soft boost, not strict tiering: a
    /// decisively better bm25 match from an archived workspace (short
    /// document dense in the query term) still outranks a weak match from an
    /// active workspace (single occurrence in a long document).
    #[tokio::test]
    async fn search_messages_fts_archived_penalty_is_soft() {
        use intent_core::now_iso;
        let tmp = TempDb::new("test-agent-repo");
        let store = Store::open(&tmp).await.expect("create test store");
        let ts = now_iso();
        let ws_active = WorkspaceId("ws-soft-active".to_string());
        let ws_arch = WorkspaceId("ws-soft-arch".to_string());
        store
            .insert_workspace(&baseline_test_workspace(&ws_active, &ts))
            .await
            .expect("insert workspace");
        let mut archived_ws = baseline_test_workspace(&ws_arch, &ts);
        archived_ws.archived = true;
        archived_ws.archived_at = Some(ts.clone());
        store
            .insert_workspace(&archived_ws)
            .await
            .expect("insert archived workspace");
        let active_agent = AgentId("agent-soft-active".to_string());
        let arch_agent = AgentId("agent-soft-arch".to_string());
        store
            .insert_agent_session(&baseline_test_session(&active_agent, &ws_active, &ts, None))
            .await
            .expect("insert active session");
        store
            .insert_agent_session(&baseline_test_session(&arch_agent, &ws_arch, &ts, None))
            .await
            .expect("insert archived session");
        // Filler documents keep the query term rare in the corpus (IDF up).
        for _ in 0..8 {
            store
                .append_agent_message(
                    &active_agent,
                    "user",
                    &serde_json::json!("padword ".repeat(8).trim()),
                    &ts,
                )
                .await
                .expect("append filler");
        }
        // Weak active match: one occurrence buried in a long document.
        store
            .append_agent_message(
                &active_agent,
                "user",
                &serde_json::json!(format!("needleterm {}", "padword ".repeat(60).trim())),
                &ts,
            )
            .await
            .expect("append weak match");
        // Decisively better archived match: short and dense in the term.
        store
            .append_agent_message(
                &arch_agent,
                "user",
                &serde_json::json!("needleterm ".repeat(8).trim()),
                &ts,
            )
            .await
            .expect("append strong match");

        let hits = store
            .search_agent_messages_fts("needleterm", None, None, None, None, None)
            .await
            .expect("soft-boost search");
        assert_eq!(hits.len(), 2, "penalty never excludes archived matches");
        assert_eq!(
            hits[0].workspace_id, ws_arch.0,
            "decisively better archived match overcomes the penalty: {hits:?}"
        );
        assert_eq!(hits[1].workspace_id, ws_active.0);
    }

    /// Harness stamp round-trip (0096): a session inserted with a harness
    /// version + captured agentFeatures snapshot reads both back verbatim on
    /// the full and summary lookups.
    #[tokio::test]
    async fn harness_stamp_roundtrip() {
        let tmp = TempDb::new("test-agent-repo");
        let store = Store::open(&tmp).await.expect("create test store");
        let ws_id = WorkspaceId("ws-test".to_string());
        insert_test_workspace(&store, &ws_id).await;
        let snapshot = serde_json::json!({ "taskGraph": true, "backgroundHooks": false });
        let mut session = test_harness_session(&ws_id);
        session.harness_version = "2.3".to_string();
        session.harness_features = Some(snapshot.clone());
        store.insert_agent_session(&session).await.expect("insert");

        let full = store.get_agent_session(&session.id).await.expect("get");
        assert_eq!(full.harness_version, "2.3");
        assert_eq!(full.harness_features, Some(snapshot.clone()));
        let summary = store
            .get_agent_session_summary(&session.id)
            .await
            .expect("summary");
        assert_eq!(summary.harness_version, "2.3");
        assert_eq!(summary.harness_features, Some(snapshot));
    }

    /// Migration-0096 backfill: a row written without the harness columns
    /// (simulating a pre-feature session) reads back harnessVersion "1.0"
    /// with no captured features.
    #[tokio::test]
    async fn harness_backfill_defaults_to_one_dot_zero() {
        let tmp = TempDb::new("test-agent-repo");
        let store = Store::open(&tmp).await.expect("create test store");
        let ws_id = WorkspaceId("ws-test".to_string());
        insert_test_workspace(&store, &ws_id).await;
        let session = test_harness_session(&ws_id);
        // Insert bypassing the harness binds — only the pre-0096 column set —
        // so the schema defaults apply exactly as the migration backfill did.
        sqlx::query(
            "INSERT INTO agent_session (id, workspace_id, name, name_explicitly_set, status, \
             is_active, created_at, updated_at) VALUES (?,?,?,0,'idle',0,?,?)",
        )
        .bind(&session.id.0)
        .bind(&ws_id.0)
        .bind(&session.name)
        .bind(&session.created_at)
        .bind(&session.updated_at)
        .execute(store.write_pool())
        .await
        .expect("raw legacy insert");

        let full = store.get_agent_session(&session.id).await.expect("get");
        assert_eq!(full.harness_version, "1.0", "backfill pins 1.0");
        assert_eq!(full.harness_features, None, "legacy rows carry no snapshot");
        let summary = store
            .get_agent_session_summary(&session.id)
            .await
            .expect("summary");
        assert_eq!(summary.harness_version, "1.0");
        assert_eq!(summary.harness_features, None);
    }

    /// The taskGraph pin folds into the harness snapshot: the reader prefers
    /// `harness_features -> '$.taskGraph'` and falls back to the legacy
    /// `task_graph_enabled` column for pre-snapshot rows (behavior identical).
    #[tokio::test]
    async fn task_graph_reader_prefers_snapshot_with_legacy_fallback() {
        let tmp = TempDb::new("test-agent-repo");
        let store = Store::open(&tmp).await.expect("create test store");
        let ws_id = WorkspaceId("ws-test".to_string());
        insert_test_workspace(&store, &ws_id).await;

        // Snapshot present: taskGraph true wins even with the legacy pin off.
        let mut with_snapshot = test_harness_session(&ws_id);
        with_snapshot.harness_features = Some(serde_json::json!({ "taskGraph": true }));
        store
            .insert_agent_session_with_task_graph(&with_snapshot, false)
            .await
            .expect("insert with snapshot");
        assert!(
            store
                .get_agent_session_task_graph_enabled(&with_snapshot.id)
                .await
                .expect("read snapshot pin"),
            "snapshot taskGraph=true wins over legacy column 0"
        );

        // No snapshot (legacy row): the 0095 column still governs.
        let legacy = test_harness_session(&ws_id);
        store
            .insert_agent_session_with_task_graph(&legacy, true)
            .await
            .expect("insert legacy");
        assert!(
            store
                .get_agent_session_task_graph_enabled(&legacy.id)
                .await
                .expect("read legacy pin"),
            "NULL snapshot falls back to task_graph_enabled"
        );
    }

    /// Minimal valid session for the harness tests; harness fields at their
    /// legacy defaults (callers override as needed).
    fn test_harness_session(ws_id: &WorkspaceId) -> AgentSession {
        let ts = intent_core::now_iso();
        AgentSession {
            id: AgentId(format!("agent-{}", uuid::Uuid::new_v4())),
            workspace_id: ws_id.clone(),
            parent_agent_id: None,
            backend_session_id: None,
            acp_session_id: None,
            name: "Harness".to_string(),
            name_explicitly_set: false,
            model: None,
            reasoning_effort: None,
            effort_levels: None,
            provider: None,
            system_prompt: None,
            specialist: None,
            status: AgentStatus::Idle,
            is_active: false,
            messages: vec![],
            stats: None,
            task_note_id: None,
            skip_auto_commit: false,
            completion_report: None,
            completion_report_timestamp: None,
            attention_request_kind: None,
            attention_request_reason: None,
            attention_request_timestamp: None,
            delegation_depth: None,
            initial_message: None,
            context_references: None,
            image_blocks: None,
            file_blocks: None,
            is_background: false,
            metadata: None,
            stop_reason: None,
            stop_reason_timestamp: None,
            session_corrupted: false,
            pending_delete_at: None,
            harness_version: intent_core::CURRENT_HARNESS_VERSION.to_string(),
            harness_features: None,
            created_at: ts.clone(),
            updated_at: ts,
            sandbox_id: None,
            sandbox_path: None,
            sandbox_branch: None,
        }
    }

    /// Insert a minimal workspace row so agent-session FKs resolve.
    async fn insert_test_workspace(store: &Store, ws_id: &WorkspaceId) {
        use intent_core::{
            now_iso, Workspace, WorkspaceActivity, WorkspaceAttention, WorkspaceStatus,
        };
        let ts = now_iso();
        let workspace = Workspace {
            id: ws_id.clone(),
            title: "Test".to_string(),
            branch: "main".to_string(),
            base_ref: None,
            base_commit_sha: None,
            status: WorkspaceStatus::Active,
            status_message: None,
            status_image_asset_id: None,
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
            display_status: None,
            waiting: false,
            checkout_mode: None,
            disk_usage: None,
            pending_delete_at: None,
        };
        store
            .insert_workspace(&workspace)
            .await
            .expect("insert workspace");
    }
}
