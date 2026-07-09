//! Session lifecycle (new/load/prompt/cancel) and `session/update` → event
//! mapping (§6.5/§6.6).
//!
//! The lifecycle helpers drive an ACP turn over a [`Connection`]; the pure
//! mapping turns each provider `session/update` into a [`MappedUpdate`] the
//! service layer publishes onto the M2 event bus and accumulates into the
//! append-only transcript (publish/accumulate live in `intent-services`, which
//! owns the store + bus; the mapping stays side-effect free here). Variants
//! without a canonical `WorkspaceEvent` in `events/types.ts`
//! (plan/mode/thought/commands/usage/…) map to `None`: emitting invented event
//! strings would break wire parity with the live iOS client.

use std::path::PathBuf;
use std::time::Duration;

use agent_client_protocol::schema::{
    CancelNotification, LoadSessionRequest, NewSessionRequest, PromptRequest, PromptResponse,
    SessionId, SessionNotification, ToolCall, ToolCallStatus, ToolCallUpdate, ToolKind,
};
use serde_json::Value;

use crate::error::{AcpError, AcpResult};
use crate::transport::Connection;
use crate::IncomingNotification;

// Re-export the schema types used in this module's public signatures so the
// service layer can consume them without depending on `agent-client-protocol`
// directly (§3.2 keeps that crate an `intent-acp` implementation detail).
pub use agent_client_protocol::schema::{
    ContentBlock, InitializeResponse, LoadSessionResponse, McpServer, NewSessionResponse,
    SessionMode, SessionModeState, SessionUpdate, StopReason,
};

/// Timeout for session setup requests (`session/new`, `session/load`). Generous
/// relative to `initialize` because the agent may connect MCP servers here.
const SESSION_SETUP_TIMEOUT: Duration = Duration::from_secs(60);
/// Timeout for a full prompt turn. A turn can run for minutes, so this is large;
/// real cancellation flows through `session/cancel`, not the timeout.
const PROMPT_TIMEOUT: Duration = Duration::from_secs(60 * 60);

/// `session/new` with `{ cwd, mcpServers }` → the agent's session id and initial
/// state. The caller persists `response.session_id` as `AgentSession.acpSessionId`
/// (write-once) for later resume (§6.5).
pub async fn new_session(
    conn: &Connection,
    cwd: impl Into<PathBuf>,
    mcp_servers: Vec<McpServer>,
) -> AcpResult<NewSessionResponse> {
    let request = NewSessionRequest::new(cwd).mcp_servers(mcp_servers);
    let params = serde_json::to_value(&request)?;
    let result = conn
        .request_timeout("session/new", params, SESSION_SETUP_TIMEOUT)
        .await?;
    serde_json::from_value(result)
        .map_err(|e| AcpError::Protocol(format!("invalid session/new response: {e}")))
}

/// `session/load` to resume an existing `acpSessionId` after a restart. Only
/// valid when the agent advertised the `loadSession` capability — check with
/// [`supports_load_session`] first (§6.5).
pub async fn load_session(
    conn: &Connection,
    session_id: &str,
    cwd: impl Into<PathBuf>,
    mcp_servers: Vec<McpServer>,
) -> AcpResult<LoadSessionResponse> {
    let request = LoadSessionRequest::new(SessionId::new(session_id), cwd).mcp_servers(mcp_servers);
    let params = serde_json::to_value(&request)?;
    let result = conn
        .request_timeout("session/load", params, SESSION_SETUP_TIMEOUT)
        .await?;
    serde_json::from_value(result)
        .map_err(|e| AcpError::Protocol(format!("invalid session/load response: {e}")))
}

/// `session/prompt` with the user content blocks → drives a turn; the agent
/// streams `session/update`s then returns a [`StopReason`] (§6.5).
pub async fn prompt(
    conn: &Connection,
    session_id: &str,
    prompt: Vec<ContentBlock>,
) -> AcpResult<StopReason> {
    let request = PromptRequest::new(SessionId::new(session_id), prompt);
    let params = serde_json::to_value(&request)?;
    let result = conn
        .request_timeout("session/prompt", params, PROMPT_TIMEOUT)
        .await?;
    let response: PromptResponse = serde_json::from_value(result)
        .map_err(|e| AcpError::Protocol(format!("invalid session/prompt response: {e}")))?;
    Ok(response.stop_reason)
}

/// `session/cancel` to interrupt the current turn (fire-and-forget notification;
/// the agent then resolves the in-flight `session/prompt` with
/// `StopReason::Cancelled`). Hard-cancel/reap process-tree kill is
/// `SpawnedAgent::kill` (orchestrated by the AgentManager, M3.6) (§6.5).
pub async fn cancel(conn: &Connection, session_id: &str) -> AcpResult<()> {
    let notification = CancelNotification::new(SessionId::new(session_id));
    let params = serde_json::to_value(&notification)?;
    conn.notify("session/cancel", params).await
}

/// Whether the agent advertised the `loadSession` capability in its handshake.
pub fn supports_load_session(init: &InitializeResponse) -> bool {
    init.agent_capabilities.load_session
}

/// A `session/update` mapped to the data the service layer needs to publish a
/// canonical `WorkspaceEvent` and accumulate the assistant transcript (§6.6).
#[derive(Debug, Clone, PartialEq)]
pub enum MappedUpdate {
    /// `agent_message_chunk` → `agent:stream:chunk` + transcript accumulation.
    /// `content` is the event payload (`content: any`); `text` is the extracted
    /// text for text blocks (used to coalesce the accumulated transcript).
    Chunk {
        /// The `data.content` carried on the `agent:stream:chunk` event.
        content: Value,
        /// Extracted text for text blocks; `None` for non-text content.
        text: Option<String>,
    },
    /// `tool_call` / `tool_call_update` → `agent:tool:call`.
    ToolCall(MappedToolCall),
}

/// A tool call mapped to the `agent:tool:call` event payload taxonomy (§6.6).
#[derive(Debug, Clone, PartialEq)]
pub struct MappedToolCall {
    /// The agent-assigned tool call id.
    pub tool_call_id: String,
    /// Human-readable tool name/title (`data.toolName`).
    pub tool_name: String,
    /// `data.toolKind`: one of file|terminal|search|note|git|other.
    pub tool_kind: &'static str,
    /// Raw tool input (`data.input`); `Null` when absent.
    pub input: Value,
    /// Raw tool output (`data.output`); omitted when `None`.
    pub output: Option<Value>,
    /// `data.status`: one of started|completed|error.
    pub status: &'static str,
}

/// Map a `session/update` to a [`MappedUpdate`], or `None` when the variant has
/// no canonical `WorkspaceEvent` (plan/mode/thought/commands/usage/…) (§6.6).
pub fn map_session_update(update: &SessionUpdate) -> Option<MappedUpdate> {
    match update {
        SessionUpdate::AgentMessageChunk(chunk) => {
            let (content, text) = map_content(&chunk.content);
            Some(MappedUpdate::Chunk { content, text })
        }
        SessionUpdate::ToolCall(tool_call) => {
            Some(MappedUpdate::ToolCall(map_tool_call(tool_call)))
        }
        SessionUpdate::ToolCallUpdate(update) => {
            Some(MappedUpdate::ToolCall(map_tool_call_update(update)))
        }
        _ => None,
    }
}

/// Parse a `session/update` notification and map it. Returns `None` when the
/// method is not `session/update`, the params fail to parse, or the variant has
/// no canonical event. Keeps schema parsing inside `intent-acp`.
pub fn map_notification(note: &IncomingNotification) -> Option<MappedUpdate> {
    if note.method != "session/update" {
        return None;
    }
    let parsed: SessionNotification = serde_json::from_value(note.params.clone()).ok()?;
    map_session_update(&parsed.update)
}

/// Extract `(event content, accumulated text)` from a streamed content block.
/// Text blocks carry their string both as the event payload and the transcript
/// text; other blocks pass through as the full JSON block with no text.
fn map_content(block: &ContentBlock) -> (Value, Option<String>) {
    match block {
        ContentBlock::Text(text) => (Value::String(text.text.clone()), Some(text.text.clone())),
        other => (serde_json::to_value(other).unwrap_or(Value::Null), None),
    }
}

/// Map a fresh `tool_call` (status defaults to "started").
fn map_tool_call(tool_call: &ToolCall) -> MappedToolCall {
    MappedToolCall {
        tool_call_id: tool_call.tool_call_id.0.to_string(),
        tool_name: tool_call.title.clone(),
        tool_kind: tool_kind_word(tool_call.kind, &tool_call.title),
        input: tool_call.raw_input.clone().unwrap_or(Value::Null),
        output: tool_call.raw_output.clone(),
        status: tool_status_word(tool_call.status),
    }
}

/// Map a `tool_call_update` (only changed fields are present).
fn map_tool_call_update(update: &ToolCallUpdate) -> MappedToolCall {
    let fields = &update.fields;
    let title = fields.title.clone().unwrap_or_default();
    MappedToolCall {
        tool_kind: tool_kind_word(fields.kind.unwrap_or_default(), &title),
        // A bare progress update (no status) is still mid-flight → "started".
        status: fields.status.map_or("started", tool_status_word),
        tool_call_id: update.tool_call_id.0.to_string(),
        tool_name: title,
        input: fields.raw_input.clone().unwrap_or(Value::Null),
        output: fields.raw_output.clone(),
    }
}

/// Map ACP's `ToolKind` (+ tool name) to the intentd taxonomy
/// (file|terminal|search|note|git|other). `note`/`git` are not expressible via
/// `ToolKind` alone, so they are inferred from the tool name (§6.6).
fn tool_kind_word(kind: ToolKind, name: &str) -> &'static str {
    let lower = name.to_lowercase();
    if lower.starts_with("git") || lower.contains(" git ") {
        return "git";
    }
    if lower.contains("note") {
        return "note";
    }
    match kind {
        ToolKind::Read | ToolKind::Edit | ToolKind::Delete | ToolKind::Move => "file",
        ToolKind::Search => "search",
        ToolKind::Execute => "terminal",
        _ => "other",
    }
}

/// Map ACP's `ToolCallStatus` to the intentd status (started|completed|error).
fn tool_status_word(status: ToolCallStatus) -> &'static str {
    match status {
        ToolCallStatus::Completed => "completed",
        ToolCallStatus::Failed => "error",
        _ => "started",
    }
}
