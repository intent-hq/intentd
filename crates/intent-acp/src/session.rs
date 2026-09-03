//! Session lifecycle (new/load/prompt/cancel) and `session/update` → event
//! mapping (§6.5/§6.6).
//!
//! The lifecycle helpers drive an ACP turn over a [`Connection`]; the pure
//! mapping turns each provider `session/update` into a [`MappedUpdate`] the
//! service layer publishes onto the M2 event bus and accumulates into the
//! append-only transcript (publish/accumulate live in `intent-services`, which
//! owns the store + bus; the mapping stays side-effect free here). Variants
//! without a canonical `WorkspaceEvent` in `events/types.ts`
//! (plan/mode/commands/…) map to `None`: emitting invented event
//! strings would break wire parity with the live iOS client. `usage_update`
//! emits no event either, but it maps: `used`/`size` carry the live session's
//! context-window occupancy (a latest-wins signal, never a token-tally input)
//! and the cumulative `cost`, when reported, is folded into the workspace
//! `TokenUsage` tally by the service layer (§5.23).
//!
//! ## Session lifetime semantics
//!
//! ACP session ids are **process-local** — they exist only for the lifetime of
//! the provider child process. When the daemon restarts (or the provider crashes),
//! stored session ids become stale. For providers that do not persist session state
//! across restarts, post-restart `session/load` will fail (typically `-32602`
//! invalid params) because the provider has no record of the stale id. This is a
//! limitation of process-local session state in such providers, not a bug. The
//! daemon implements a recreate+resend fallback (see `AgentManager::start_session`):
//! when `session/load` fails (or the agent lacks `loadSession` capability), the
//! daemon creates a fresh session via `session/new` (CAS-replacing the stale id)
//! and prepends the prior conversation history as `<supervisor>` XML on the next
//! prompt turn so the fresh session has context.

use std::path::PathBuf;
use std::sync::atomic::{AtomicU64, Ordering};
use std::sync::Arc;
use std::time::{Duration, Instant};

use agent_client_protocol::schema::v1::{
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
pub use agent_client_protocol::schema::v1::{
    ContentBlock, InitializeResponse, LoadSessionResponse, McpServer, Meta, NewSessionResponse,
    SessionConfigKind, SessionConfigOption, SessionConfigOptionCategory, SessionConfigSelect,
    SessionConfigSelectOption, SessionConfigSelectOptions, SessionMode, SessionModeState,
    SessionUpdate, StopReason, Usage,
};

/// Timeout for session setup requests (`session/new`, `session/load`). Generous
/// relative to `initialize` because the agent may connect MCP servers here.
/// Overridable via `INTENTD_SESSION_SETUP_TIMEOUT_MS` (primarily for tests/CI).
fn session_setup_timeout() -> Duration {
    if let Ok(val) = std::env::var("INTENTD_SESSION_SETUP_TIMEOUT_MS") {
        if let Ok(ms) = val.parse::<u64>() {
            return Duration::from_millis(ms);
        }
    }
    Duration::from_secs(60)
}

/// Idle timeout for a prompt turn: the turn times out only after this period
/// of silence (no `session/update` traffic). Actively-streaming turns reset
/// the timer on every update and never time out. Overridable via
/// `INTENTD_PROMPT_IDLE_TIMEOUT_MS`.
///
/// The 30-minute default aligns with the FE contract: cloudlands-fe's
/// `SESSION_TIMEOUT`, `abandonedStreamTimeout`, and `inactiveThreshold` are
/// all 30 minutes, so the daemon must not cut idle turns earlier than the FE
/// stops waiting (STAB-49; the previous 15-minute default killed healthy
/// long-running implementor turns).
///
/// Do NOT raise this timeout (or advise bumping
/// `INTENTD_PROMPT_IDLE_TIMEOUT_MS`) to accommodate one long-running silent
/// turn. It applies to EVERY agent harness/provider driving a
/// `session/prompt` turn through intentd — it is not per-agent or
/// per-workload — so widening it affects all harnesses and breaks the FE
/// 30-minute contract above. The remedy for long silent operations (e.g. an
/// agent blocking on `sleep` / `gh pr checks --watch` loops for >30 min) is
/// agent-side: emit periodic activity by polling in shorter intervals.
/// Context: intent-hq/monorepo#1106 diagnosis (2026-07-29), where a
/// 30-minute silent watch loop tripped this timeout.
///
/// Public so the service layer's warn-and-continue path can render the
/// actual configured window in the timeout-warning message instead of a
/// hardcoded literal.
#[must_use]
pub fn prompt_idle_timeout() -> Duration {
    if let Ok(val) = std::env::var("INTENTD_PROMPT_IDLE_TIMEOUT_MS") {
        if let Ok(ms) = val.parse::<u64>() {
            return Duration::from_millis(ms);
        }
    }
    Duration::from_secs(30 * 60)
}

/// Shared last-activity timestamp for idle-timeout tracking. The caller updates
/// this on every `session/update` notification; the prompt logic polls it to
/// enforce the idle window.
#[derive(Clone)]
pub struct ActivityTracker {
    /// Elapsed milliseconds since an arbitrary epoch (e.g. `Instant::now()`),
    /// atomically updated on each activity.
    last_active_ms: Arc<AtomicU64>,
}

impl ActivityTracker {
    /// Create a new tracker initialized to "now".
    #[must_use]
    pub fn new() -> Self {
        Self {
            last_active_ms: Arc::new(AtomicU64::new(elapsed_ms())),
        }
    }

    /// Record activity now.
    pub fn touch(&self) {
        self.last_active_ms.store(elapsed_ms(), Ordering::SeqCst);
    }

    /// Milliseconds since the last activity.
    #[must_use]
    pub fn idle_ms(&self) -> u64 {
        elapsed_ms().saturating_sub(self.last_active_ms.load(Ordering::SeqCst))
    }
}

impl Default for ActivityTracker {
    fn default() -> Self {
        Self::new()
    }
}

/// Milliseconds elapsed since an arbitrary epoch (monotonic).
fn elapsed_ms() -> u64 {
    static START: std::sync::OnceLock<Instant> = std::sync::OnceLock::new();
    let start = START.get_or_init(Instant::now);
    u64::try_from(start.elapsed().as_millis()).unwrap_or(u64::MAX)
}

/// `session/new` with `{ cwd, mcpServers, _meta? }` → the agent's session id and initial
/// state. The caller persists `response.session_id` as `AgentSession.acpSessionId`
/// (write-once) for later resume (§6.5). `meta` (if present) is provider-specific
/// metadata for system-prompt injection or other extensions.
///
/// # Errors
///
/// Returns [`AcpError::Protocol`] if the response does not deserialize; otherwise propagates the transport/RPC error from the request.
pub async fn new_session(
    conn: &Connection,
    cwd: impl Into<PathBuf>,
    mcp_servers: Vec<McpServer>,
    meta: Option<Meta>,
) -> AcpResult<NewSessionResponse> {
    let mut request = NewSessionRequest::new(cwd).mcp_servers(mcp_servers);
    request.meta = meta;
    let params = serde_json::to_value(&request)?;
    let result = conn
        .request_timeout("session/new", params, session_setup_timeout())
        .await?;
    serde_json::from_value(result)
        .map_err(|e| AcpError::Protocol(format!("invalid session/new response: {e}")))
}

/// `session/load` to resume an existing `acpSessionId` after a restart. Only
/// valid when the agent advertised the `loadSession` capability — check with
/// [`supports_load_session`] first (§6.5). `meta` (if present) is provider-specific
/// metadata for system-prompt injection or other extensions.
///
/// # Errors
///
/// Returns [`AcpError::Protocol`] if the response does not deserialize; otherwise propagates the transport/RPC error from the request.
pub async fn load_session(
    conn: &Connection,
    session_id: &str,
    cwd: impl Into<PathBuf>,
    mcp_servers: Vec<McpServer>,
    meta: Option<Meta>,
) -> AcpResult<LoadSessionResponse> {
    let mut request =
        LoadSessionRequest::new(SessionId::new(session_id), cwd).mcp_servers(mcp_servers);
    request.meta = meta;
    let params = serde_json::to_value(&request)?;
    let result = conn
        .request_timeout("session/load", params, session_setup_timeout())
        .await?;
    serde_json::from_value(result)
        .map_err(|e| AcpError::Protocol(format!("invalid session/load response: {e}")))
}

/// Outcome of a completed `session/prompt` turn: why the agent stopped plus
/// the end-of-turn token-usage snapshot when the agent reports one (the
/// `unstable_end_turn_token_usage` extension; counts are cumulative per ACP
/// session). `usage` is `None` when the agent omits the field or sends a
/// malformed payload (the schema deserializes it best-effort to `None`).
#[derive(Debug, Clone)]
pub struct PromptOutcome {
    /// Why the agent stopped processing the turn (§6.5).
    pub stop_reason: StopReason,
    /// Cumulative-per-session token usage reported at end of turn, if any.
    pub usage: Option<Usage>,
    /// The response's raw `_meta` extension payload, if any. Some providers
    /// report usage only here instead of the standard `usage` field (grok's
    /// `_meta.usage` whole-prompt bill, intent-hq/intent#3803); the service
    /// layer owns interpreting it. Best-effort like `usage`: a malformed
    /// `_meta` deserializes to `None`.
    pub meta: Option<Meta>,
}

/// `session/prompt` with the user content blocks → drives a turn; the agent
/// streams `session/update`s then returns a [`PromptOutcome`] carrying the
/// [`StopReason`] and optional end-of-turn usage snapshot (§6.5).
///
/// Uses an activity-based idle timeout: the turn times out only after a
/// sustained period of silence (no `session/update` traffic). The caller must
/// update `activity` on every incoming notification; the prompt loop polls it
/// to enforce the idle window. Actively-streaming turns never time out.
///
/// # Errors
///
/// Returns [`AcpError::PromptIdleTimeout`] after a sustained period with no `session/update` traffic; [`AcpError::Protocol`] if the response does not deserialize; otherwise propagates the transport/RPC error.
pub async fn prompt(
    conn: &Connection,
    session_id: &str,
    prompt: Vec<ContentBlock>,
    activity: &ActivityTracker,
) -> AcpResult<PromptOutcome> {
    let request = PromptRequest::new(SessionId::new(session_id), prompt);
    let params = serde_json::to_value(&request)?;
    let idle_window = prompt_idle_timeout();

    // Use a very large fallback timeout (24h) to catch agent-died-without-
    // closing-stdout edge cases, but the idle timeout below provides the real
    // bound. Returning early on idle timeout (below) drops req_fut, whose
    // pending-map entry is removed by the transport's drop guard (see
    // `Connection::request_timeout`); a late response from the agent is then
    // dispatched into the void harmlessly.
    let fallback_timeout = Duration::from_secs(24 * 60 * 60);
    let req_fut = conn.request_timeout("session/prompt", params, fallback_timeout);
    tokio::pin!(req_fut);
    let poll_interval = Duration::from_secs(1);
    loop {
        tokio::select! {
            res = &mut req_fut => {
                let result = res?;
                let response: PromptResponse = serde_json::from_value(result)
                    .map_err(|e| AcpError::Protocol(format!("invalid session/prompt response: {e}")))?;
                return Ok(PromptOutcome {
                    stop_reason: response.stop_reason,
                    usage: response.usage,
                    meta: response.meta,
                });
            }
            () = tokio::time::sleep(poll_interval) => {
                let idle = Duration::from_millis(activity.idle_ms());
                if idle >= idle_window {
                    // Early return drops req_fut; its pending-map entry is
                    // cleaned by the transport's drop guard (comment above).
                    return Err(AcpError::PromptIdleTimeout(idle_window));
                }
            }
        }
    }
}

/// `session/set_model` to select the session's model after creation. Used for
/// providers whose ACP subcommand has no CLI model flag
/// ([`ProviderConfig::supports_set_model`](intent_providers::ProviderConfig),
/// grok today; parity with the reference acp-provider's post-session
/// `session/set_model`). The request shape is `{ sessionId, modelId }` — the
/// pinned `agent-client-protocol` schema has no typed request for it yet.
///
/// # Errors
///
/// Propagates the transport/RPC error if the request fails.
pub async fn set_session_model(
    conn: &Connection,
    session_id: &str,
    model_id: &str,
) -> AcpResult<()> {
    let params = serde_json::json!({ "sessionId": session_id, "modelId": model_id });
    conn.request("session/set_model", params).await?;
    Ok(())
}

/// `session/set_config_option` to change a session config option after
/// establishment. Used for providers that expose the model as a
/// `configOptions[id="model"]` select in the `session/new` result
/// ([`ProviderConfig::supports_config_option_model`](intent_providers::ProviderConfig),
/// claude-code, pi, and codex today). The request shape is
/// `{ sessionId, configId, value }` —
/// verified live against claude-agent-acp@0.60.0 (2026-07-22), whose response
/// echoes the updated `configOptions` list; the pinned `agent-client-protocol`
/// schema has no typed request for it yet.
///
/// # Errors
///
/// Propagates the transport/RPC error if the request fails.
pub async fn set_session_config_option(
    conn: &Connection,
    session_id: &str,
    config_id: &str,
    value: &str,
) -> AcpResult<()> {
    set_session_config_option_response(conn, session_id, config_id, value).await?;
    Ok(())
}

/// Apply a config option and retain the response for providers that require
/// confirmation of the exact selected value before a prompt may run.
///
/// # Errors
///
/// Propagates the transport/RPC error if the request fails.
pub async fn set_session_config_option_response(
    conn: &Connection,
    session_id: &str,
    config_id: &str,
    value: &str,
) -> AcpResult<Value> {
    let params =
        serde_json::json!({ "sessionId": session_id, "configId": config_id, "value": value });
    conn.request("session/set_config_option", params).await
}

/// `session/cancel` to interrupt the current turn (fire-and-forget notification;
/// the agent then resolves the in-flight `session/prompt` with
/// `StopReason::Cancelled`). Hard-cancel/reap process-tree kill is
/// `SpawnedAgent::kill` (orchestrated by the `AgentManager`, M3.6) (§6.5).
///
/// # Errors
///
/// Returns [`AcpError::Transport`] if sending the notification fails.
pub async fn cancel(conn: &Connection, session_id: &str) -> AcpResult<()> {
    let notification = CancelNotification::new(SessionId::new(session_id));
    let params = serde_json::to_value(&notification)?;
    conn.notify("session/cancel", params).await
}

/// Whether the agent advertised the `loadSession` capability in its handshake.
#[must_use]
pub fn supports_load_session(init: &InitializeResponse) -> bool {
    init.agent_capabilities.load_session
}

/// A `session/update` mapped to the data the service layer needs to publish a
/// canonical `WorkspaceEvent` and accumulate the assistant transcript (§6.6).
#[derive(Debug, Clone, PartialEq)]
pub enum MappedUpdate {
    /// `agent_message_chunk` / `agent_thought_chunk` → `chat:stream:delta` (+
    /// the throttled `agent:stream:activity` signal) + transcript
    /// accumulation. `content` is the event payload (`content: any`); `text` is
    /// the extracted text for text blocks (used to coalesce the accumulated
    /// transcript).
    Chunk {
        /// The `data.content` carried on the `chat:stream:delta` event.
        content: Value,
        /// Extracted text for text blocks; `None` for non-text content.
        text: Option<String>,
        /// `true` for `agent_thought_chunk` (streamed reasoning), `false` for
        /// `agent_message_chunk`. Thought chunks travel the same path as
        /// message chunks with this marker set, mirroring Zed's `is_thought`.
        thought: bool,
    },
    /// `tool_call` / `tool_call_update` → `agent:tool:call`.
    ToolCall(MappedToolCall),
    /// `usage_update` → no canonical `WorkspaceEvent`. The context-window
    /// occupancy (`used`/`size`) is recorded latest-wins per live session
    /// (never folded into token tallies), and the cumulative per-ACP-session
    /// `cost`, when reported, feeds the workspace `TokenUsage` tally (§5.23).
    Usage(MappedUsage),
}

/// The context-window occupancy and optional cumulative cost carried by an
/// ACP `usage_update` (§5.23). `used`/`size` are point-in-time context
/// occupancy (input + cache vs. the model's window) — a signal, not a
/// billing counter; only `cost` ever contributes to the token tally.
#[derive(Debug, Clone, PartialEq)]
pub struct MappedUsage {
    /// Tokens currently in the session's context window.
    pub used: u64,
    /// Total context window size in tokens.
    pub size: u64,
    /// Cumulative session cost, when the provider reports one — absence is
    /// never coerced to a zero figure.
    pub cost: Option<MappedUsageCost>,
}

/// The cumulative session cost carried by an ACP `usage_update` (§5.23).
#[derive(Debug, Clone, PartialEq)]
pub struct MappedUsageCost {
    /// Cumulative spend for the ACP session so far.
    pub amount: f64,
    /// ISO 4217 currency code (e.g. `"USD"`).
    pub currency: String,
}

/// A tool call mapped to the `agent:tool:call` event payload taxonomy (§6.6).
#[derive(Debug, Clone, PartialEq)]
pub struct MappedToolCall {
    /// The agent-assigned tool call id.
    pub tool_call_id: String,
    /// The real tool name (`data.toolName`), derived from the ACP title via
    /// [`derive_tool_name`].
    pub tool_name: String,
    /// The raw human-readable ACP title (`data.title`), verbatim.
    pub title: String,
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
/// no canonical `WorkspaceEvent` and nothing else to accumulate
/// (plan/mode/commands/…) (§6.6). `usage_update` always maps: `used`/`size`
/// are required schema fields (the context-occupancy signal), and `cost`
/// rides along when reported (§5.23).
pub(crate) fn map_session_update(update: &SessionUpdate) -> Option<MappedUpdate> {
    match update {
        SessionUpdate::AgentMessageChunk(chunk) => {
            let (content, text) = map_content(&chunk.content);
            Some(MappedUpdate::Chunk {
                content,
                text,
                thought: false,
            })
        }
        SessionUpdate::AgentThoughtChunk(chunk) => {
            let (content, text) = map_content(&chunk.content);
            Some(MappedUpdate::Chunk {
                content,
                text,
                thought: true,
            })
        }
        SessionUpdate::ToolCall(tool_call) => {
            Some(MappedUpdate::ToolCall(map_tool_call(tool_call)))
        }
        SessionUpdate::ToolCallUpdate(update) => {
            Some(MappedUpdate::ToolCall(map_tool_call_update(update)))
        }
        SessionUpdate::UsageUpdate(usage) => Some(MappedUpdate::Usage(MappedUsage {
            used: usage.used,
            size: usage.size,
            cost: usage.cost.as_ref().map(|cost| MappedUsageCost {
                amount: cost.amount,
                currency: cost.currency.clone(),
            }),
        })),
        _ => None,
    }
}

/// Parse a `session/update` notification and map it. Returns `None` when the
/// method is not `session/update`, the params fail to parse, or the variant has
/// no canonical event. Keeps schema parsing inside `intent-acp`.
#[must_use]
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

/// Codex delivers MCP tool calls with the model's parameters nested one level
/// down (reference `acp-provider-streaming.ts` ~L1580–1606):
///
/// ```json
/// { "arguments": { "noteId": "spec" }, "server": "workspace-mcp", "tool": "read_note" }
/// ```
///
/// Detect that shape — `arguments` an object (not an array) plus string
/// `tool` and `server` — and return the unwrapped top-level input together
/// with the rewritten `{server}_{tool}` tool name (e.g.
/// `workspace-mcp_read_note`). A non-null `_acpTitle` on the outer object is
/// preserved onto the unwrapped input, as in the reference. Returns `None`
/// for any other shape (input passes through verbatim).
fn unwrap_codex_mcp_input(raw_input: Option<&Value>) -> Option<(Value, String)> {
    let obj = raw_input?.as_object()?;
    let args = obj.get("arguments")?.as_object()?;
    let tool = obj.get("tool")?.as_str()?;
    let server = obj.get("server")?.as_str()?;
    let mut unwrapped = args.clone();
    if let Some(title) = obj.get("_acpTitle") {
        if !title.is_null() {
            unwrapped.insert("_acpTitle".to_string(), title.clone());
        }
    }
    Some((Value::Object(unwrapped), format!("{server}_{tool}")))
}

/// Resolve the mapped `input` and tool name for a tool call. When the codex
/// nested-MCP shape is detected ([`unwrap_codex_mcp_input`]) the arguments
/// are hoisted to the top level and the rewritten `{server}_{tool}` name is
/// fed through [`derive_tool_name`]'s title path — the title-prefix rule and
/// `workspace-mcp` affix strip still apply, so `workspace-mcp_read_note`
/// yields `read_note` while other servers keep the `{server}_{tool}` name.
/// Otherwise the input passes through verbatim and the name derives from the
/// ACP `title`.
fn resolve_input_and_name(
    title: &str,
    raw_input: Option<&Value>,
    meta: Option<&serde_json::Map<String, Value>>,
) -> (Value, String) {
    // Antigravity wraps MCP arguments and identifies the tool in ACP metadata.
    // Require both captured markers and the matching title to avoid unwrapping
    // an unrelated provider's legitimate `arguments` parameter.
    if let Some(meta) = meta.filter(|meta| meta.get("is_mcp_tool_call") == Some(&Value::Bool(true)))
    {
        if let (Some(server), Some(tool), Some(arguments)) = (
            meta.get("mcp")
                .and_then(|m| m.get("server"))
                .and_then(Value::as_str),
            meta.get("mcp")
                .and_then(|m| m.get("tool"))
                .and_then(Value::as_str),
            raw_input
                .and_then(|v| v.get("arguments"))
                .and_then(Value::as_object),
        ) {
            if title == format!("{server}_{tool}") {
                let mut input = arguments.clone();
                if let Some(acp_title) = raw_input.and_then(|v| v.get("_acpTitle")) {
                    input.insert("_acpTitle".into(), acp_title.clone());
                }
                return (Value::Object(input), strip_workspace_mcp_affix(title));
            }
        }
    }
    if let Some((input, rewritten)) = unwrap_codex_mcp_input(raw_input) {
        let name = derive_tool_name(&rewritten, Some(&input));
        return (input, name);
    }
    let name = derive_tool_name(title, raw_input);
    (raw_input.cloned().unwrap_or(Value::Null), name)
}

/// Map a fresh `tool_call` (status defaults to "started").
fn map_tool_call(tool_call: &ToolCall) -> MappedToolCall {
    let title = tool_call.title.clone();
    let (input, tool_name) = resolve_input_and_name(
        &title,
        tool_call.raw_input.as_ref(),
        tool_call.meta.as_ref(),
    );
    MappedToolCall {
        tool_call_id: tool_call.tool_call_id.0.to_string(),
        tool_kind: tool_kind_word(tool_call.kind, &tool_name),
        input,
        output: tool_call.raw_output.clone(),
        status: tool_status_word(tool_call.status),
        tool_name,
        title,
    }
}

/// Map a `tool_call_update` (only changed fields are present).
fn map_tool_call_update(update: &ToolCallUpdate) -> MappedToolCall {
    let fields = &update.fields;
    let title = fields.title.clone().unwrap_or_default();
    let (input, tool_name) =
        resolve_input_and_name(&title, fields.raw_input.as_ref(), update.meta.as_ref());
    MappedToolCall {
        tool_kind: tool_kind_word(fields.kind.unwrap_or_default(), &tool_name),
        // A bare progress update (no status) is still mid-flight → "started".
        status: fields.status.map_or("started", tool_status_word),
        tool_call_id: update.tool_call_id.0.to_string(),
        input,
        output: fields.raw_output.clone(),
        tool_name,
        title,
    }
}

/// Derive the "real" tool name from a human-readable ACP `title` and, when the
/// title carries no identifier, the shape of the `raw_input` parameters (§6.6).
///
/// ACP providers (auggie, codex, opencode, …) deliver a prose `title` (e.g.
/// `"sub-agent-explore: Explore the AI agent system…"`) rather than the raw
/// tool name the model invoked. Rules, in order:
///  1. A title of the form `<name>: <description>` (`<name>` a bare identifier
///     of `[A-Za-z0-9_-]+`, followed by `": "` or `":\t"`) is split; the prefix
///     becomes the name.
///  2. Codex titles MCP tools `mcp.<server>.<tool>` (dot-separated, no
///     whitespace — prose titles containing dots never match); the title is
///     rewritten to `{server}_{tool}` and fed through the affix strip below,
///     the same downstream treatment as [`unwrap_codex_mcp_input`]'s
///     rewritten name (`mcp.workspace-mcp.workspace_api` → `workspace_api`,
///     `mcp.other-server.some_tool` → `other-server_some_tool`).
///  3. Claude Code titles MCP tools `mcp__<server>__<tool>`
///     (double-underscore-separated, no whitespace — prose titles containing
///     `mcp__` never match); the title is rewritten to `{server}_{tool}` and
///     fed through the affix strip below, the same downstream treatment as
///     the codex dot rule (`mcp__workspace-mcp__workspace_api` →
///     `workspace_api`, `mcp__github__list_issues` → `github_list_issues`).
///  4. `workspace-mcp` server affixes are stripped — auggie names an MCP tool
///     `<tool>_<server>` (trailing `_workspace-mcp` suffix), opencode names it
///     `<server>_<tool>` (leading `workspace-mcp_` prefix); stripping either
///     (repeatedly) recovers the registry name (§18.4).
///  5. A bare `webfetch` title (opencode's fetch tool) is normalized to the
///     canonical `web-fetch` builtin name.
///  6. When none of the above yielded an identifier (the title is prose
///     like `"Read"` or `"Edit foo.rs"`), inspect `raw_input` for unambiguous
///     shapes. Evaluated in the same order as the reference
///     (`acp-provider-streaming.ts` ~L1635–1666), first match wins:
///       - `command ∈ {str_replace, insert, create}` → `str-replace-editor`
///       - `file_content` + `path` + `instructions_reminder` → `save-file`
///       - `path` + `view_range` → `view`
///       - `information_request` → `codebase-retrieval`
///         (or `conversation-retrieval` when the title mentions `conversation`)
///       - `file_paths` array → `remove-files`
///       - `input` string containing `*** Begin Patch` → `apply_patch`
///
///     Then opencode's camelCase shapes (captured from opencode 1.18.3,
///     which titles its calls with raw prose — the command line, a file
///     path, a regex — once arguments stream in):
///       - `filePath` + `oldString`/`newString` → `edit`
///       - `filePath` + `content` → `write`
///       - `filePath` alone → `read`
///       - string `command` + string `cwd` (no `wait`/`max_wait_seconds`,
///         which would mean auggie's `launch-process`) → `bash`
///       - `url` → `web-fetch`
///  7. Otherwise the title passes through as-is.
///
/// The `conversation`-vs-`codebase` split keys off the passed-in ACP `title`.
/// The reference keys off its local `toolName` variable, which may have been
/// reassigned by an upstream codex-input unwrap (reference ~L1580–1606). That
/// unwrap now exists upstream here too: [`unwrap_codex_mcp_input`] (via the
/// tool-call mappers' [`resolve_input_and_name`]) feeds the rewritten
/// `{server}_{tool}` string in as `title`, keeping the two equivalent on
/// every path.
#[must_use]
pub fn derive_tool_name(title: &str, raw_input: Option<&Value>) -> String {
    if let Some(name) = split_name_prefix(title) {
        return strip_workspace_mcp_affix(name);
    }
    if let Some(rewritten) = split_codex_mcp_title(title) {
        return strip_workspace_mcp_affix(&rewritten);
    }
    if let Some(rewritten) = split_claude_mcp_title(title) {
        return strip_workspace_mcp_affix(&rewritten);
    }
    let stripped = strip_workspace_mcp_affix(title);
    if stripped != title {
        return stripped;
    }
    // Opencode's fetch tool is titled `webfetch`; normalize to the canonical
    // builtin name so downstream consumers match on one spelling.
    if title == "webfetch" {
        return "web-fetch".to_string();
    }
    if let Some(input) = raw_input {
        if let Some(from_input) = derive_tool_name_from_input(title, input) {
            return from_input;
        }
    }
    stripped
}

/// Inspect an ACP `raw_input` object for shapes that unambiguously identify a
/// well-known tool. Mirrors the reference in
/// `acp-provider-streaming.ts` ~L1635–1666. Returns `None` when no pattern
/// matches; the caller falls back to the title. Order matches the reference —
/// first match wins.
fn derive_tool_name_from_input(title: &str, input: &Value) -> Option<String> {
    let obj = input.as_object()?;
    // Official Antigravity ACP frames. Require the captured input shapes;
    // do not infer tool identity from arbitrary prose titles.
    if is_non_empty_string(obj.get("CommandLine")) && is_non_empty_string(obj.get("Cwd")) {
        return Some("run_command".to_string());
    }
    if title == "Running client_view_file" && is_non_empty_string(obj.get("absolute_path")) {
        return Some("client_view_file".to_string());
    }
    if matches!(
        title,
        "Run client_create_file?" | "Running client_create_file"
    ) && is_non_empty_string(obj.get("target_file"))
        && obj.get("code_content").and_then(Value::as_str).is_some()
    {
        return Some("client_create_file".to_string());
    }
    // command ∈ {str_replace, insert, create} → str-replace-editor
    if let Some(cmd) = obj.get("command").and_then(Value::as_str) {
        if matches!(cmd, "str_replace" | "insert" | "create") {
            return Some("str-replace-editor".to_string());
        }
    }
    // file_content + path + instructions_reminder → save-file. The reference
    // uses JS truthy on `path` (rejects null / empty); we match by requiring
    // a non-empty string, so `{ path: null }` falls through as it does in JS.
    if obj.contains_key("file_content")
        && is_non_empty_string(obj.get("path"))
        && obj.contains_key("instructions_reminder")
    {
        return Some("save-file".to_string());
    }
    // path + view_range → view. Same JS-truthy semantics on `path` as above.
    if is_non_empty_string(obj.get("path")) && obj.contains_key("view_range") {
        return Some("view".to_string());
    }
    // information_request → codebase-retrieval / conversation-retrieval
    if obj.contains_key("information_request") {
        if title.to_lowercase().contains("conversation") {
            return Some("conversation-retrieval".to_string());
        }
        return Some("codebase-retrieval".to_string());
    }
    // file_paths array → remove-files
    if obj.get("file_paths").and_then(Value::as_array).is_some() {
        return Some("remove-files".to_string());
    }
    // input: string containing "*** Begin Patch" → apply_patch
    if let Some(s) = obj.get("input").and_then(Value::as_str) {
        if s.contains("*** Begin Patch") {
            return Some("apply_patch".to_string());
        }
    }
    // Opencode camelCase shapes (captured from opencode 1.18.3). These keys
    // don't collide with the snake_case auggie/codex shapes above, so they
    // are checked last. filePath + oldString/newString → edit; filePath +
    // content → write; filePath alone → read.
    if is_non_empty_string(obj.get("filePath")) {
        if obj.contains_key("oldString") || obj.contains_key("newString") {
            return Some("edit".to_string());
        }
        if obj.contains_key("content") {
            return Some("write".to_string());
        }
        return Some("read".to_string());
    }
    // command (string) + cwd (string) → bash. Auggie's launch-process also
    // carries a string `command` + `cwd` but always with
    // `wait`/`max_wait_seconds`; codex sends `command` as an array — both
    // are excluded here.
    if is_non_empty_string(obj.get("command"))
        && is_non_empty_string(obj.get("cwd"))
        && !obj.contains_key("wait")
        && !obj.contains_key("max_wait_seconds")
    {
        return Some("bash".to_string());
    }
    // url → web-fetch (opencode's webfetch and auggie's web-fetch both carry
    // a bare url input).
    if is_non_empty_string(obj.get("url")) {
        return Some("web-fetch".to_string());
    }
    None
}

/// JS-truthy on a `path`-style field: present, a string, and non-empty.
/// Rejects `null`, `""`, missing keys, and non-string types so
/// `{ path: null, view_range: [] }` does not misclassify as `view`.
fn is_non_empty_string(v: Option<&Value>) -> bool {
    v.and_then(Value::as_str).is_some_and(|s| !s.is_empty())
}

fn split_name_prefix(title: &str) -> Option<&str> {
    let colon = title.find(':')?;
    let name = &title[..colon];
    if name.is_empty() {
        return None;
    }
    if !name
        .chars()
        .all(|c| c.is_ascii_alphanumeric() || c == '_' || c == '-')
    {
        return None;
    }
    let after = title[colon + 1..].chars().next()?;
    if after != ' ' && after != '\t' {
        return None;
    }
    Some(name)
}

/// Recognize codex's dot-separated ACP title form for MCP tools —
/// `mcp.<server>.<tool>`, where the server segment contains no dots and the
/// title carries no whitespace (so prose titles containing dots never match)
/// — and rewrite it to the `{server}_{tool}` name shared with
/// [`unwrap_codex_mcp_input`]. Returns `None` for any other title.
fn split_codex_mcp_title(title: &str) -> Option<String> {
    if title.contains(char::is_whitespace) {
        return None;
    }
    let rest = title.strip_prefix("mcp.")?;
    let (server, tool) = rest.split_once('.')?;
    if server.is_empty() || tool.is_empty() {
        return None;
    }
    Some(format!("{server}_{tool}"))
}

/// Recognize Claude Code's double-underscore-separated ACP title form for MCP
/// tools — `mcp__<server>__<tool>`, where the server segment runs to the
/// first `__` and the title carries no whitespace (so prose titles containing
/// `mcp__` never match) — and rewrite it to the `{server}_{tool}` name shared
/// with [`split_codex_mcp_title`]. Returns `None` for any other title.
fn split_claude_mcp_title(title: &str) -> Option<String> {
    if title.contains(char::is_whitespace) {
        return None;
    }
    let rest = title.strip_prefix("mcp__")?;
    let (server, tool) = rest.split_once("__")?;
    if server.is_empty() || tool.is_empty() {
        return None;
    }
    Some(format!("{server}_{tool}"))
}

/// Strip `workspace-mcp` server affixes from an MCP tool name: auggie appends
/// a trailing `_workspace-mcp` suffix (`add_to_note_workspace-mcp`), opencode
/// prepends a leading `workspace-mcp_` prefix (`workspace-mcp_add_to_note`).
/// Either is stripped repeatedly until the bare registry name remains.
fn strip_workspace_mcp_affix(name: &str) -> String {
    const SUFFIX: &str = "_workspace-mcp";
    const PREFIX: &str = "workspace-mcp_";
    let mut cur = name;
    loop {
        if let Some(stripped) = cur.strip_suffix(SUFFIX) {
            if !stripped.is_empty() {
                cur = stripped;
                continue;
            }
        }
        if let Some(stripped) = cur.strip_prefix(PREFIX) {
            if !stripped.is_empty() {
                cur = stripped;
                continue;
            }
        }
        break;
    }
    cur.to_string()
}

/// Map ACP's `ToolKind` (+ tool name) to the intentd taxonomy
/// (file|terminal|search|note|git|other). `note`/`git` and the context-engine
/// retrievals are not expressible via `ToolKind` alone, so they are inferred
/// from the tool name (§6.6).
fn tool_kind_word(kind: ToolKind, name: &str) -> &'static str {
    let lower = name.to_lowercase();
    if lower.starts_with("git") || lower.contains(" git ") {
        return "git";
    }
    if lower.contains("note") {
        return "note";
    }
    if lower == "codebase-retrieval" || lower == "conversation-retrieval" {
        return "search";
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

#[cfg(test)]
mod tests {
    use super::*;

    /// Restores an env var to its prior state on drop so tests stay hermetic.
    struct EnvGuard {
        key: &'static str,
        prev: Option<String>,
    }

    impl EnvGuard {
        fn new(key: &'static str) -> Self {
            Self {
                key,
                prev: std::env::var(key).ok(),
            }
        }
    }

    impl Drop for EnvGuard {
        fn drop(&mut self) {
            match &self.prev {
                Some(v) => std::env::set_var(self.key, v),
                None => std::env::remove_var(self.key),
            }
        }
    }

    /// Default idle window is 30 minutes, matching the FE contract
    /// (`SESSION_TIMEOUT` / `abandonedStreamTimeout` / `inactiveThreshold` are
    /// all 30 min), and the `INTENTD_PROMPT_IDLE_TIMEOUT_MS` override still
    /// applies (STAB-49). Default, override, and the structured idle-timeout
    /// return of `prompt()` are checked in one test because the env var is
    /// process-global and tests run in parallel.
    #[tokio::test]
    async fn prompt_idle_timeout_default_and_env_override() {
        let _guard = EnvGuard::new("INTENTD_PROMPT_IDLE_TIMEOUT_MS");

        std::env::remove_var("INTENTD_PROMPT_IDLE_TIMEOUT_MS");
        assert_eq!(prompt_idle_timeout(), Duration::from_secs(30 * 60));

        std::env::set_var("INTENTD_PROMPT_IDLE_TIMEOUT_MS", "100");
        assert_eq!(prompt_idle_timeout(), Duration::from_millis(100));

        // A silent agent (remote duplex ends held open, never responding):
        // `prompt()` must resolve with the structured `PromptIdleTimeout`
        // variant and leave no leaked pending-map entry behind (the
        // transport's drop guard cleans the abandoned correlation entry).
        let (c2a_client, _c2a_agent) = tokio::io::duplex(4096);
        let (_a2c_agent, a2c_client) = tokio::io::duplex(4096);
        let conn = Connection::new(
            c2a_client,
            a2c_client,
            None,
            crate::ConnectionHooks::default(),
        );
        let activity = ActivityTracker::new();
        let err = prompt(&conn, "sess-1", Vec::new(), &activity)
            .await
            .expect_err("silent agent must idle-time-out");
        assert!(
            matches!(err, AcpError::PromptIdleTimeout(w) if w == Duration::from_millis(100)),
            "unexpected error: {err:?}"
        );
        assert_eq!(conn.pending_len(), 0, "abandoned prompt entry cleaned up");

        std::env::remove_var("INTENTD_PROMPT_IDLE_TIMEOUT_MS");
        assert_eq!(prompt_idle_timeout(), Duration::from_secs(30 * 60));
    }
}
