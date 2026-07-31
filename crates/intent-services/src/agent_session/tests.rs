//! Driver tests over a temp SQLite store + a mock ACP agent: a prompt turn
//! accumulates chunks, publishes events in order with a single terminal
//! `stream:end`, persists `acpSessionId`, and gates resume on the capability.

use std::path::PathBuf;
use std::time::Duration;

use intent_acp::session::{ContentBlock, InitializeResponse};
use intent_acp::{Connection, ConnectionHooks, IncomingNotification};
use intent_core::{
    now_iso, AgentId, AgentSession, AgentStatus, Event, Workspace, WorkspaceActivity,
    WorkspaceAttention, WorkspaceId, WorkspaceStatus,
};
use intent_store::Store;
use serde_json::{json, Value};
use tokio::io::{AsyncBufReadExt, AsyncRead, AsyncWrite, AsyncWriteExt, BufReader};
use tokio::sync::mpsc;
use tokio::task::JoinHandle;
use tokio::time::timeout;

use crate::events::{EventBus, SubscriptionFilter};
use crate::Services;

struct TempDb {
    path: PathBuf,
}

impl TempDb {
    fn new() -> Self {
        let path = std::env::temp_dir().join(format!("intentd-agent-{}.db", uuid::Uuid::new_v4()));
        Self { path }
    }
}

impl Drop for TempDb {
    fn drop(&mut self) {
        for suffix in ["", "-wal", "-shm"] {
            let _ = std::fs::remove_file(PathBuf::from(format!("{}{suffix}", self.path.display())));
        }
    }
}

const ACP_SID: &str = "acp-session-1";

/// Mock agent that answers the lifecycle methods; `session/prompt` streams the
/// caller-supplied `session/update` burst, then resolves with the supplied
/// result — e.g. `end_turn`, optionally carrying an end-of-turn `usage`
/// snapshot (the ACP `unstable_end_turn_token_usage` extension).
fn spawn_mock_agent_with_prompt_result<R, W>(
    read: R,
    write: W,
    updates: Vec<String>,
    prompt_result: Value,
) -> JoinHandle<()>
where
    R: AsyncRead + Unpin + Send + 'static,
    W: AsyncWrite + Unpin + Send + 'static,
{
    tokio::spawn(async move {
        let mut lines = BufReader::new(read).lines();
        let mut write = write;
        while let Ok(Some(line)) = lines.next_line().await {
            if line.trim().is_empty() {
                continue;
            }
            let value: Value = serde_json::from_str(&line).expect("valid JSON");
            let (Some(id), Some(method)) =
                (value.get("id"), value.get("method").and_then(Value::as_str))
            else {
                continue;
            };
            if method == "session/prompt" {
                for note in &updates {
                    write
                        .write_all(format!("{note}\n").as_bytes())
                        .await
                        .unwrap();
                }
            }
            let result = match method {
                "initialize" => {
                    json!({ "protocolVersion": 1, "agentCapabilities": { "loadSession": true } })
                }
                "session/new" => json!({ "sessionId": ACP_SID }),
                "session/load" => json!({}),
                "session/prompt" => prompt_result.clone(),
                _ => json!({}),
            };
            let resp = json!({ "jsonrpc": "2.0", "id": id, "result": result });
            write
                .write_all(format!("{resp}\n").as_bytes())
                .await
                .unwrap();
            write.flush().await.unwrap();
        }
    })
}

/// The `session/update` notifications a prompt turn streams before completing.
fn prompt_updates() -> Vec<String> {
    let chunk = |text: &str| {
        json!({
            "jsonrpc": "2.0",
            "method": "session/update",
            "params": {
                "sessionId": ACP_SID,
                "update": { "sessionUpdate": "agent_message_chunk",
                    "content": { "type": "text", "text": text } }
            }
        })
        .to_string()
    };
    let tool = json!({
        "jsonrpc": "2.0",
        "method": "session/update",
        "params": {
            "sessionId": ACP_SID,
            "update": { "sessionUpdate": "tool_call", "toolCallId": "t1",
                "title": "Edit src/lib.rs", "kind": "edit", "status": "in_progress",
                "rawInput": { "path": "src/lib.rs" } }
        }
    })
    .to_string();
    vec![chunk("Hello "), chunk("world"), tool]
}

/// A prompt turn that streams one text chunk, a `tool_call` (started), then a
/// `tool_call_update` that completes it with output — exercises tool_use +
/// tool_result block accumulation (CS-0 D6).
fn prompt_updates_with_tool_result() -> Vec<String> {
    let chunk = json!({
        "jsonrpc": "2.0",
        "method": "session/update",
        "params": {
            "sessionId": ACP_SID,
            "update": { "sessionUpdate": "agent_message_chunk",
                "content": { "type": "text", "text": "Working " } }
        }
    })
    .to_string();
    let tool_call = json!({
        "jsonrpc": "2.0",
        "method": "session/update",
        "params": {
            "sessionId": ACP_SID,
            "update": { "sessionUpdate": "tool_call", "toolCallId": "t1",
                "title": "Run tests", "kind": "execute", "status": "in_progress",
                "rawInput": { "path": "." } }
        }
    })
    .to_string();
    let tool_done = json!({
        "jsonrpc": "2.0",
        "method": "session/update",
        "params": {
            "sessionId": ACP_SID,
            "update": { "sessionUpdate": "tool_call_update", "toolCallId": "t1",
                "status": "completed", "rawOutput": { "summary": "12 passed" } }
        }
    })
    .to_string();
    vec![chunk, tool_call, tool_done]
}

/// A prompt turn whose tool completes with an MCP content-item array output
/// carrying a proposal-MIME resource item — exercises the standalone
/// proposal-resource block extraction (§7.1).
fn prompt_updates_with_proposal_resource() -> Vec<String> {
    let tool_call = json!({
        "jsonrpc": "2.0",
        "method": "session/update",
        "params": {
            "sessionId": ACP_SID,
            "update": { "sessionUpdate": "tool_call", "toolCallId": "t1",
                "title": "workspace_api", "kind": "other", "status": "in_progress",
                "rawInput": { "code": "ws.app.proposal.show(p)" } }
        }
    })
    .to_string();
    let tool_done = json!({
        "jsonrpc": "2.0",
        "method": "session/update",
        "params": {
            "sessionId": ACP_SID,
            "update": { "sessionUpdate": "tool_call_update", "toolCallId": "t1",
                "status": "completed",
                "rawOutput": [
                    { "type": "text", "text": "Proposal shown" },
                    { "type": "resource", "resource": {
                        "uri": "intent-proposal://settings-change/Update",
                        "name": "Update",
                        "mimeType": "application/vnd.intent.proposal+json",
                        "text": "{\"kind\":\"settings-change\"}" } }
                ] }
        }
    })
    .to_string();
    vec![tool_call, tool_done]
}

/// A prompt turn whose tool completes with a provider-collapsed output —
/// auggie flattens the MCP content items into `{ "output": "<stringified
/// {ok, proposal}>" }`, dropping the resource item — exercising the fallback
/// proposal lift (§7.1).
fn prompt_updates_with_collapsed_proposal() -> Vec<String> {
    let proposal = json!({
        "kind": "settings-change",
        "preview": { "title": "Update Setting" },
        "payload": { "key": "k", "value": "v" },
    });
    let text = serde_json::to_string_pretty(&json!({ "ok": true, "proposal": proposal }))
        .expect("serialize");
    let tool_call = json!({
        "jsonrpc": "2.0",
        "method": "session/update",
        "params": {
            "sessionId": ACP_SID,
            "update": { "sessionUpdate": "tool_call", "toolCallId": "t1",
                "title": "workspace_api", "kind": "other", "status": "in_progress",
                "rawInput": { "code": "ws.app.proposal.show(p)" } }
        }
    })
    .to_string();
    let tool_done = json!({
        "jsonrpc": "2.0",
        "method": "session/update",
        "params": {
            "sessionId": ACP_SID,
            "update": { "sessionUpdate": "tool_call_update", "toolCallId": "t1",
                "status": "completed",
                "rawOutput": { "output": text } }
        }
    })
    .to_string();
    vec![tool_call, tool_done]
}

/// A prompt turn whose FIRST update is a stale `tool_call_update` for a
/// toolCallId this turn never saw (the shape a cancelled child emits after an
/// interrupt: no title, no rawInput → derived name ""), followed by a real
/// text chunk. STAB-124: the stale update must be dropped, not fabricated into
/// an anonymous `tool_use` block.
fn prompt_updates_stale_anonymous_tool() -> Vec<String> {
    let stale = json!({
        "jsonrpc": "2.0",
        "method": "session/update",
        "params": {
            "sessionId": ACP_SID,
            "update": { "sessionUpdate": "tool_call_update", "toolCallId": "stale-1",
                "status": "failed",
                "rawOutput": { "error": "The operation was aborted" } }
        }
    })
    .to_string();
    let chunk = json!({
        "jsonrpc": "2.0",
        "method": "session/update",
        "params": {
            "sessionId": ACP_SID,
            "update": { "sessionUpdate": "agent_message_chunk",
                "content": { "type": "text", "text": "Resumed" } }
        }
    })
    .to_string();
    vec![stale, chunk]
}

/// Shared crate-wide env guard (defined in `agent_manager::tests`): tests
/// here that pin `INTENTD_PROMPT_IDLE_TIMEOUT_MS` must serialize with the
/// worker-level tests that pin the same var.
use crate::agent_manager::tests::EnvGuard;

/// Mock agent that goes SILENT on `session/prompt`: it answers the lifecycle
/// methods, streams the caller-supplied `session/update` burst, then never
/// resolves the prompt — while keeping both pipe ends open (the child is
/// alive, merely quiet). With a short `INTENTD_PROMPT_IDLE_TIMEOUT_MS` this
/// drives the `AcpError::PromptIdleTimeout` return of `session::prompt`.
fn spawn_silent_mock_agent<R, W>(read: R, write: W, updates: Vec<String>) -> JoinHandle<()>
where
    R: AsyncRead + Unpin + Send + 'static,
    W: AsyncWrite + Unpin + Send + 'static,
{
    tokio::spawn(async move {
        let mut lines = BufReader::new(read).lines();
        let mut write = write;
        while let Ok(Some(line)) = lines.next_line().await {
            if line.trim().is_empty() {
                continue;
            }
            let value: Value = serde_json::from_str(&line).expect("valid JSON");
            let (Some(id), Some(method)) =
                (value.get("id"), value.get("method").and_then(Value::as_str))
            else {
                continue;
            };
            if method == "session/prompt" {
                for note in &updates {
                    write
                        .write_all(format!("{note}\n").as_bytes())
                        .await
                        .unwrap();
                }
                write.flush().await.unwrap();
                // Never resolve the prompt; keep reading so the pipes stay
                // open (a `session/cancel` notification may still arrive).
                continue;
            }
            let result = match method {
                "initialize" => {
                    json!({ "protocolVersion": 1, "agentCapabilities": { "loadSession": true } })
                }
                "session/new" => json!({ "sessionId": ACP_SID }),
                _ => json!({}),
            };
            let resp = json!({ "jsonrpc": "2.0", "id": id, "result": result });
            write
                .write_all(format!("{resp}\n").as_bytes())
                .await
                .unwrap();
            write.flush().await.unwrap();
        }
    })
}

/// [`connect`] against the silent mock: `session/prompt` streams `updates`
/// then never resolves, with the child pipes held open.
fn connect_silent(
    updates: Vec<String>,
) -> (
    Connection,
    mpsc::UnboundedReceiver<IncomingNotification>,
    JoinHandle<()>,
) {
    let (c2a_client, c2a_agent) = tokio::io::duplex(16 * 1024);
    let (a2c_agent, a2c_client) = tokio::io::duplex(16 * 1024);
    let agent = spawn_silent_mock_agent(c2a_agent, a2c_agent, updates);
    let (note_tx, note_rx) = mpsc::unbounded_channel();
    let hooks = ConnectionHooks {
        notifications: Some(note_tx),
        ..ConnectionHooks::default()
    };
    let conn = Connection::new(c2a_client, a2c_client, None, hooks);
    (conn, note_rx, agent)
}

/// Mock agent that dies mid-`session/prompt`: it streams the caller-supplied
/// `session/update` burst, then drops both pipe ends WITHOUT answering the
/// prompt — the daemon's reader hits EOF and fails the pending request with
/// the code-0 "agent stdout closed" JSON-RPC error (the monorepo#764
/// transport-death shape).
fn spawn_dying_mock_agent<R, W>(read: R, write: W, updates: Vec<String>) -> JoinHandle<()>
where
    R: AsyncRead + Unpin + Send + 'static,
    W: AsyncWrite + Unpin + Send + 'static,
{
    tokio::spawn(async move {
        let mut lines = BufReader::new(read).lines();
        let mut write = write;
        while let Ok(Some(line)) = lines.next_line().await {
            if line.trim().is_empty() {
                continue;
            }
            let value: Value = serde_json::from_str(&line).expect("valid JSON");
            if value.get("method").and_then(Value::as_str) == Some("session/prompt") {
                for note in &updates {
                    write
                        .write_all(format!("{note}\n").as_bytes())
                        .await
                        .unwrap();
                }
                write.flush().await.unwrap();
                return;
            }
        }
    })
}

/// Wire a `Connection` to a fresh mock agent, returning the connection, its
/// notification receiver, and the agent task handle.
fn connect() -> (
    Connection,
    mpsc::UnboundedReceiver<IncomingNotification>,
    JoinHandle<()>,
) {
    connect_with(prompt_updates())
}

/// [`connect`] against the dying mock: `session/prompt` streams `updates`,
/// then the child's pipes close with the request still pending.
fn connect_dying(
    updates: Vec<String>,
) -> (
    Connection,
    mpsc::UnboundedReceiver<IncomingNotification>,
    JoinHandle<()>,
) {
    let (c2a_client, c2a_agent) = tokio::io::duplex(16 * 1024);
    let (a2c_agent, a2c_client) = tokio::io::duplex(16 * 1024);
    let agent = spawn_dying_mock_agent(c2a_agent, a2c_agent, updates);
    let (note_tx, note_rx) = mpsc::unbounded_channel();
    let hooks = ConnectionHooks {
        notifications: Some(note_tx),
        ..ConnectionHooks::default()
    };
    let conn = Connection::new(c2a_client, a2c_client, None, hooks);
    (conn, note_rx, agent)
}

/// [`connect`] with a caller-supplied prompt-update burst.
fn connect_with(
    updates: Vec<String>,
) -> (
    Connection,
    mpsc::UnboundedReceiver<IncomingNotification>,
    JoinHandle<()>,
) {
    connect_with_prompt_result(updates, json!({ "stopReason": "end_turn" }))
}

/// [`connect_with`] with a caller-supplied `session/prompt` result.
fn connect_with_prompt_result(
    updates: Vec<String>,
    prompt_result: Value,
) -> (
    Connection,
    mpsc::UnboundedReceiver<IncomingNotification>,
    JoinHandle<()>,
) {
    let (c2a_client, c2a_agent) = tokio::io::duplex(16 * 1024);
    let (a2c_agent, a2c_client) = tokio::io::duplex(16 * 1024);
    let agent = spawn_mock_agent_with_prompt_result(c2a_agent, a2c_agent, updates, prompt_result);
    let (note_tx, note_rx) = mpsc::unbounded_channel();
    let hooks = ConnectionHooks {
        notifications: Some(note_tx),
        ..ConnectionHooks::default()
    };
    let conn = Connection::new(c2a_client, a2c_client, None, hooks);
    (conn, note_rx, agent)
}

/// Mock agent whose `session/new` / `session/load` responses carry a
/// caller-supplied payload (e.g. `configOptions` for the D13 effective-model
/// resolution); other methods answer like the standard mock.
fn spawn_mock_agent_with_session_result<R, W>(
    read: R,
    write: W,
    session_result: Value,
) -> JoinHandle<()>
where
    R: AsyncRead + Unpin + Send + 'static,
    W: AsyncWrite + Unpin + Send + 'static,
{
    tokio::spawn(async move {
        let mut lines = BufReader::new(read).lines();
        let mut write = write;
        while let Ok(Some(line)) = lines.next_line().await {
            if line.trim().is_empty() {
                continue;
            }
            let value: Value = serde_json::from_str(&line).expect("valid JSON");
            let (Some(id), Some(method)) =
                (value.get("id"), value.get("method").and_then(Value::as_str))
            else {
                continue;
            };
            let result = match method {
                "initialize" => {
                    json!({ "protocolVersion": 1, "agentCapabilities": { "loadSession": true } })
                }
                "session/new" | "session/load" => session_result.clone(),
                _ => json!({}),
            };
            let resp = json!({ "jsonrpc": "2.0", "id": id, "result": result });
            write
                .write_all(format!("{resp}\n").as_bytes())
                .await
                .unwrap();
            write.flush().await.unwrap();
        }
    })
}

/// [`connect`] against a mock whose session-open responses carry `result`.
fn connect_with_session_result(
    result: Value,
) -> (
    Connection,
    mpsc::UnboundedReceiver<IncomingNotification>,
    JoinHandle<()>,
) {
    let (c2a_client, c2a_agent) = tokio::io::duplex(16 * 1024);
    let (a2c_agent, a2c_client) = tokio::io::duplex(16 * 1024);
    let agent = spawn_mock_agent_with_session_result(c2a_agent, a2c_agent, result);
    let (note_tx, note_rx) = mpsc::unbounded_channel();
    let hooks = ConnectionHooks {
        notifications: Some(note_tx),
        ..ConnectionHooks::default()
    };
    let conn = Connection::new(c2a_client, a2c_client, None, hooks);
    (conn, note_rx, agent)
}

async fn setup() -> (TempDb, Services, EventBus, AgentId, WorkspaceId) {
    let tmp = TempDb::new();
    let store = Store::open(&tmp.path).await.expect("open store");
    let bus = EventBus::new(store.clone());
    let services = Services::new(store.clone()).with_event_bus(bus.clone());
    let workspace_id = WorkspaceId::from("ws-1");
    let agent_id = AgentId::from("agent-1");
    store
        .insert_workspace(&workspace(&workspace_id))
        .await
        .expect("insert workspace");
    store
        .insert_agent_session(&new_session(&agent_id, &workspace_id))
        .await
        .expect("insert agent session");
    (tmp, services, bus, agent_id, workspace_id)
}

fn workspace(id: &WorkspaceId) -> Workspace {
    let ts = now_iso();
    Workspace {
        id: id.clone(),
        title: "WS".to_string(),
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
        checkout_mode: None,
    }
}

fn new_session(agent_id: &AgentId, workspace_id: &WorkspaceId) -> AgentSession {
    let ts = now_iso();
    AgentSession {
        id: agent_id.clone(),
        workspace_id: workspace_id.clone(),
        parent_agent_id: None,
        backend_session_id: None,
        acp_session_id: None,
        name: "Builder".to_string(),
        name_explicitly_set: false,
        model: None,
        provider: None,
        system_prompt: None,
        specialist: None,
        status: AgentStatus::Pending,
        is_active: true,
        messages: Vec::new(),
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
        is_background: false,
        metadata: None,
        created_at: ts.clone(),
        updated_at: ts,
        sandbox_id: None,
        sandbox_path: None,
        sandbox_branch: None,
        stop_reason: None,
        session_corrupted: false,
    }
}

fn text_block(text: &str) -> ContentBlock {
    serde_json::from_value(json!({ "type": "text", "text": text })).unwrap()
}

fn init_caps(load_session: bool) -> InitializeResponse {
    serde_json::from_value(
        json!({ "protocolVersion": 1, "agentCapabilities": { "loadSession": load_session } }),
    )
    .unwrap()
}

#[tokio::test]
async fn prompt_turn_streams_events_and_accumulates() {
    let (_tmp, services, bus, agent_id, workspace_id) = setup().await;
    let (conn, mut note_rx, _agent) = connect();
    let mut sub = bus.subscribe(SubscriptionFilter::default());

    let stop = services
        .run_prompt_turn(
            &conn,
            &mut note_rx,
            &agent_id,
            &workspace_id,
            ACP_SID,
            vec![text_block("hi")],
            None,
        )
        .await
        .expect("turn completes");
    assert_eq!(serde_json::to_value(stop).unwrap(), json!("end_turn"));

    // Collect the published events (default filter → one event per batch).
    // The turn also emits a `prompt` status hint before the first chunk
    // (STAT-1 / PROTOCOL §7 pre-first-token status family), so expect one
    // extra frame ahead of the delta/activity/tool/end/idle sequence. The
    // first chunk additionally emits the content-free `agent:stream:activity`
    // signal (leading edge of the per-agent throttle); the second chunk
    // normally lands inside the 1s window and produces a delta only, but
    // under CI load it can slip past the window and emit a second activity
    // signal, so the sequence assertion below drops any activity events
    // after the first instead of hard-coding exactly one.
    let mut events: Vec<Event> = Vec::new();
    while !events.iter().any(|e| e.event_type == "agent:idle") {
        let batch = timeout(Duration::from_secs(2), sub.recv())
            .await
            .expect("recv timed out")
            .expect("subscription open");
        events.extend(batch);
    }
    let mut seen_activity = false;
    let types: Vec<&str> = events
        .iter()
        .map(|e| e.event_type.as_str())
        .filter(|t| {
            if *t == "agent:stream:activity" {
                if seen_activity {
                    return false;
                }
                seen_activity = true;
            }
            true
        })
        .collect();
    assert_eq!(
        types,
        vec![
            "agent:stream:status",
            "chat:stream:delta",
            "agent:stream:activity",
            "chat:stream:delta",
            "agent:tool:call",
            "agent:stream:end",
            "agent:idle",
        ],
        "a normal turn emits the `prompt` status hint before the first chunk, the leading-edge activity signal on the first chunk, and exactly one agent:idle after the terminal stream:end"
    );

    // The activity signal carries identifiers plus the server-derived live
    // preview — never the raw transcript content. The first activity fires
    // after the first chunk ("Hello ") landed, but that text has no newline
    // yet, so the mid-turn preview clips it as a still-streaming partial line
    // and omits `lastAgentResponse` entirely (it surfaces on the terminal
    // stream:end below instead).
    let activity = events
        .iter()
        .find(|e| e.event_type == "agent:stream:activity")
        .expect("turn emits at least one activity signal");
    assert_eq!(activity.data["agentId"], json!("agent-1"));
    assert!(
        activity.data["messageId"].is_string(),
        "activity carries the turn's messageId"
    );
    assert!(
        activity.data.get("content").is_none(),
        "activity payload never carries transcript content"
    );
    assert!(
        activity.data.get("lastAgentResponse").is_none(),
        "mid-turn preview omitted until a completed (newline-terminated) line streams"
    );
    assert!(
        activity.data.get("digest").is_none(),
        "digest omitted when the streamed text has no <agent_digest> span"
    );

    // The pre-first-token status hint carries the "Sent prompt…" phrase and
    // arrives BEFORE any `chat:stream:delta` so the FE spinner can render it
    // while the turn is starting.
    let status = &events[0];
    assert_eq!(status.event_type, "agent:stream:status");
    assert_eq!(status.data["agentId"], json!("agent-1"));
    assert_eq!(status.data["workspaceId"], json!("ws-1"));
    assert_eq!(status.data["phase"], json!("prompt"));
    assert_eq!(status.data["message"], json!("Sent prompt\u{2026}"));
    assert_eq!(status.data["level"], json!("info"));
    assert!(
        status.data["timestamp"].is_u64(),
        "status timestamp is an epoch-ms integer"
    );
    assert_eq!(
        events
            .iter()
            .filter(|e| e.event_type == "agent:stream:end")
            .count(),
        1
    );
    assert_eq!(
        events
            .iter()
            .filter(|e| e.event_type == "agent:idle")
            .count(),
        1,
        "exactly one agent:idle per normal turn"
    );

    // The agent:idle payload carries the session-completion signal.
    let idle = events
        .iter()
        .find(|e| e.event_type == "agent:idle")
        .unwrap();
    assert_eq!(idle.data["agentId"], json!("agent-1"));
    assert_eq!(idle.data["reason"], json!("stream_complete"));
    assert_eq!(idle.data["finishReason"], json!("end_turn"));
    assert_eq!(idle.data["status"], json!("idle"));
    assert_eq!(idle.data["lastResponseSummary"], json!("Hello world"));
    // DELIV-1: `agent:idle` MUST carry `agentName` so subscribers don't fall
    // back to a generic label; `completion_report` is `None` on this session,
    // so `completionReport` / `report` are absent (only present when a
    // delegated child called `agent.reportToParent`). `isBackground` rides
    // along from the session row — `false` for this foreground session.
    assert_eq!(idle.data["agentName"], json!("Builder"));
    assert_eq!(idle.data["isBackground"], json!(false));
    // Emit-time waiting flag: this session parents no pending completion
    // watches, so the idle payload reports `false`.
    assert_eq!(idle.data["isWaitingForOtherAgents"], json!(false));
    assert!(
        idle.data.get("report").is_none(),
        "no completion_report was set on this session"
    );
    assert!(
        idle.data.get("completionReport").is_none(),
        "no completion_report was set on this session"
    );

    let tool = events
        .iter()
        .find(|e| e.event_type == "agent:tool:call")
        .unwrap();
    assert_eq!(tool.data["toolKind"], json!("file"));
    assert_eq!(tool.data["status"], json!("started"));
    assert_eq!(tool.data["input"], json!({ "path": "src/lib.rs" }));

    // Chunks accumulate into one assistant message (coalesced text + tool block).
    let messages = bus
        .store()
        .get_agent_messages(&agent_id, None)
        .await
        .expect("read messages");
    assert_eq!(messages.len(), 1);
    assert_eq!(messages[0].role, "assistant");
    let mid = &messages[0].id;
    // The coalesced text block is block 0; the tool_use block is block 1, each
    // carrying the stable id `{messageId}:{index}` (CS-0 D1/D6).
    assert_eq!(
        messages[0].content,
        json!([
            { "type": "text", "id": format!("{mid}:0"), "text": "Hello world" },
            { "type": "tool_use", "id": format!("{mid}:1"), "name": "Edit src/lib.rs",
              "input": { "path": "src/lib.rs", "_acpTitle": "Edit src/lib.rs" },
              "toolCallId": "t1",
              "metadata": { "toolKind": "file", "status": "started" } },
        ])
    );

    // The streaming delta events carry the SAME stable block id across both
    // text chunks; the persisted message id is the block-id prefix (D1/D4).
    let chunks: Vec<_> = events
        .iter()
        .filter(|e| e.event_type == "chat:stream:delta")
        .collect();
    assert_eq!(chunks.len(), 2);
    assert_eq!(chunks[0].data["blockId"], json!(format!("{mid}:0")));
    assert_eq!(chunks[1].data["blockId"], json!(format!("{mid}:0")));
    assert_eq!(chunks[0].data["blockIndex"], json!(0));
    assert_eq!(chunks[1].data["blockIndex"], json!(0));
    // The tool block gets its own (next) id, and the tool event carries it.
    assert_eq!(tool.data["blockId"], json!(format!("{mid}:1")));
    assert_eq!(tool.data["blockIndex"], json!(1));
    assert_eq!(tool.data["messageId"], json!(mid));

    // The terminal stream:end carries the turn's messageId; `trailingBlocks`
    // is omitted when no AtTurnEnd attachments were drained (monorepo#732).
    // It also carries the FINAL preview values (the last throttled activity
    // may have missed the response tail).
    let end = events
        .iter()
        .find(|e| e.event_type == "agent:stream:end")
        .unwrap();
    assert_eq!(end.data["messageId"], json!(mid));
    assert!(
        end.data.get("trailingBlocks").is_none(),
        "trailingBlocks omitted on a turn without AtTurnEnd attachments"
    );
    assert_eq!(
        end.data["lastAgentResponse"],
        json!("Hello world"),
        "terminal stream:end carries the final preview from the full turn text"
    );
}

/// Mid-turn `agent:stream:activity` clips the preview at the last newline:
/// the first chunk carries a completed line plus the start of the next one,
/// so the activity serves only the completed line; a partial
/// `<agent_digest>` opener streamed later never surfaces mid-turn. The
/// terminal `agent:stream:end` re-derives from the full (complete) turn text
/// and is unaffected by the clipping.
#[tokio::test]
async fn prompt_turn_activity_preview_clips_partial_line_and_digest() {
    let (_tmp, services, bus, agent_id, workspace_id) = setup().await;
    let chunk = |text: &str| {
        json!({
            "jsonrpc": "2.0",
            "method": "session/update",
            "params": {
                "sessionId": ACP_SID,
                "update": { "sessionUpdate": "agent_message_chunk",
                    "content": { "type": "text", "text": text } }
            }
        })
        .to_string()
    };
    let updates = vec![
        chunk("Completed line\nNext par"),
        chunk("tial\n<agent_digest>sum"),
        chunk("mary</agent_digest>"),
    ];
    let (conn, mut note_rx, _agent) = connect_with(updates);
    let mut sub = bus.subscribe(SubscriptionFilter::default());

    let stop = services
        .run_prompt_turn(
            &conn,
            &mut note_rx,
            &agent_id,
            &workspace_id,
            ACP_SID,
            vec![text_block("hi")],
            None,
        )
        .await
        .expect("turn completes");
    assert_eq!(serde_json::to_value(stop).unwrap(), json!("end_turn"));

    let mut events: Vec<Event> = Vec::new();
    while !events.iter().any(|e| e.event_type == "agent:idle") {
        let batch = timeout(Duration::from_secs(2), sub.recv())
            .await
            .expect("recv timed out")
            .expect("subscription open");
        events.extend(batch);
    }

    // First activity fires on the first chunk: the trailing "Next par" is a
    // still-streaming partial line and is excluded.
    let activity = events
        .iter()
        .find(|e| e.event_type == "agent:stream:activity")
        .expect("turn emits at least one activity signal");
    assert_eq!(
        activity.data["lastAgentResponse"],
        json!("Completed line"),
        "activity preview serves only the completed (newline-terminated) line"
    );
    assert!(
        activity.data.get("digest").is_none(),
        "no digest streamed by the first chunk"
    );
    // No mid-turn frame ever surfaces a partially-streamed digest span.
    for ev in events
        .iter()
        .filter(|e| e.event_type == "agent:stream:activity")
    {
        if let Some(d) = ev.data.get("digest").and_then(|d| d.as_str()) {
            assert_eq!(d, "summary", "only a fully-streamed digest may surface");
        }
        if let Some(r) = ev.data.get("lastAgentResponse").and_then(|r| r.as_str()) {
            assert!(
                !r.contains("<agent_digest>"),
                "digest markup never leaks into the preview: {r}"
            );
        }
    }

    // The terminal stream:end derives from the full turn text: the final line
    // is complete by definition and the completed digest is extracted.
    let end = events
        .iter()
        .find(|e| e.event_type == "agent:stream:end")
        .expect("terminal stream:end");
    assert_eq!(
        end.data["lastAgentResponse"],
        json!("Next partial"),
        "terminal preview keeps the turn-end (unclipped) semantics"
    );
    assert_eq!(
        end.data["digest"],
        json!("summary"),
        "terminal frame carries the completed digest"
    );
}

/// DELIV-1: when the session carries a `completion_report` (persisted by
/// `agent.reportToParent` on a delegated child), the terminal
/// `agent:idle` payload includes it under both `completionReport`
/// (canonical) and `report` (back-compat) alongside the enriched
/// `agentName`, so subscribers see the child's report without a
/// follow-up `agent.get` round-trip.
#[tokio::test]
async fn agent_idle_payload_carries_agent_name_and_completion_report() {
    let (_tmp, services, bus, agent_id, workspace_id) = setup().await;
    // Seed a completion_report on the session BEFORE the turn (the FE-side
    // ordering: a delegated child calls `agent.reportToParent` before its
    // last turn ends).
    let mut session = services
        .store()
        .get_agent_session(&agent_id)
        .await
        .expect("load session");
    let saved = now_iso();
    session.completion_report = Some("wrote fix + tests, green".into());
    session.completion_report_timestamp = Some(saved.clone());
    session.updated_at = saved;
    services
        .store()
        .update_agent_session(&workspace_id, &session)
        .await
        .expect("persist completion_report");

    let (conn, mut note_rx, _agent) = connect();
    let mut sub = bus.subscribe(SubscriptionFilter {
        event_types: vec!["agent:idle".to_string()],
        ..Default::default()
    });

    services
        .run_prompt_turn(
            &conn,
            &mut note_rx,
            &agent_id,
            &workspace_id,
            ACP_SID,
            vec![text_block("hi")],
            None,
        )
        .await
        .expect("turn completes");

    let batch = timeout(Duration::from_secs(2), sub.recv())
        .await
        .expect("recv timed out")
        .expect("subscription open");
    let idle = batch
        .iter()
        .find(|e| e.event_type == "agent:idle")
        .expect("agent:idle emitted");
    assert_eq!(idle.data["agentId"], json!("agent-1"));
    assert_eq!(idle.data["agentName"], json!("Builder"));
    assert_eq!(
        idle.data["completionReport"],
        json!("wrote fix + tests, green")
    );
    assert_eq!(idle.data["report"], json!("wrote fix + tests, green"));
}

/// A `session/update` notification shaped like the prior-conversation replay
/// `session/load` buffers before it returns (built directly, not via the wire).
fn replay_chunk(text: &str) -> IncomingNotification {
    IncomingNotification {
        method: "session/update".to_string(),
        params: json!({
            "sessionId": ACP_SID,
            "update": { "sessionUpdate": "agent_message_chunk",
                "content": { "type": "text", "text": text } }
        }),
    }
}

fn replay_tool() -> IncomingNotification {
    IncomingNotification {
        method: "session/update".to_string(),
        params: json!({
            "sessionId": ACP_SID,
            "update": { "sessionUpdate": "tool_call", "toolCallId": "old",
                "title": "Edit src/old.rs", "kind": "edit", "status": "in_progress",
                "rawInput": { "path": "src/old.rs" } }
        }),
    }
}

/// The `session/load` replay burst buffered in the handle's channel is discarded
/// (no events published, transcript untouched), while a subsequent real turn
/// still streams its updates and accumulates the assistant message.
#[tokio::test]
async fn resume_replay_burst_is_dropped_then_real_turn_streams() {
    let (_tmp, services, bus, agent_id, workspace_id) = setup().await;
    let mut sub = bus.subscribe(SubscriptionFilter::default());

    // Simulate the post-resume buffered replay: a burst already in the channel.
    let (replay_tx, mut replay_rx) = mpsc::unbounded_channel();
    replay_tx
        .send(replay_chunk("Hi, I am the prior "))
        .expect("buffer replay update");
    replay_tx
        .send(replay_chunk("greeting from last session"))
        .expect("buffer replay update");
    replay_tx.send(replay_tool()).expect("buffer replay tool");
    drop(replay_tx);

    // The bounded drain empties the burst and cannot hang.
    timeout(
        Duration::from_secs(2),
        Services::drain_replay_notifications(&mut replay_rx),
    )
    .await
    .expect("drain settles within the cap");
    assert!(replay_rx.try_recv().is_err(), "replay channel emptied");

    // Dropping the replay produced no events and no transcript message.
    assert!(
        timeout(Duration::from_millis(100), sub.recv())
            .await
            .is_err(),
        "no events published for the dropped replay burst"
    );
    assert!(
        bus.store()
            .get_agent_messages(&agent_id, None)
            .await
            .unwrap()
            .is_empty(),
        "replay is not appended to the transcript"
    );

    // A subsequent real turn still streams + accumulates normally.
    let (conn, mut note_rx, _agent) = connect();
    let stop = services
        .run_prompt_turn(
            &conn,
            &mut note_rx,
            &agent_id,
            &workspace_id,
            ACP_SID,
            vec![text_block("hi")],
            None,
        )
        .await
        .expect("turn completes");
    assert_eq!(serde_json::to_value(stop).unwrap(), json!("end_turn"));

    let mut events = Vec::new();
    while events.len() < 7 {
        let batch = timeout(Duration::from_secs(2), sub.recv())
            .await
            .expect("recv timed out")
            .expect("subscription open");
        events.extend(batch);
    }
    let types: Vec<&str> = events.iter().map(|e| e.event_type.as_str()).collect();
    assert_eq!(
        types,
        vec![
            "agent:stream:status",
            "chat:stream:delta",
            "agent:stream:activity",
            "chat:stream:delta",
            "agent:tool:call",
            "agent:stream:end",
            "agent:idle",
        ],
        "the real turn emits the pre-first-token `prompt` status hint, streams its own updates (then goes idle) after the replay was dropped"
    );

    let messages = bus
        .store()
        .get_agent_messages(&agent_id, None)
        .await
        .expect("read messages");
    assert_eq!(messages.len(), 1, "only the real turn is accumulated");
    let mid = &messages[0].id;
    assert_eq!(
        messages[0].content,
        json!([
            { "type": "text", "id": format!("{mid}:0"), "text": "Hello world" },
            { "type": "tool_use", "id": format!("{mid}:1"), "name": "Edit src/lib.rs",
              "input": { "path": "src/lib.rs", "_acpTitle": "Edit src/lib.rs" },
              "toolCallId": "t1",
              "metadata": { "toolKind": "file", "status": "started" } },
        ])
    );
}

/// A tool that streams `tool_call` (started) then `tool_call_update` (completed
/// with output) persists BOTH a `tool_use` and a `tool_result` block, each with
/// a stable id, and the second event carries the matching block identity while
/// preserving the legacy fields additively (CS-0 D4/D6).
#[tokio::test]
async fn tool_call_then_update_persists_use_and_result_blocks() {
    let (_tmp, services, bus, agent_id, workspace_id) = setup().await;
    let (conn, mut note_rx, _agent) = connect_with(prompt_updates_with_tool_result());
    let mut sub = bus.subscribe(SubscriptionFilter::default());

    services
        .run_prompt_turn(
            &conn,
            &mut note_rx,
            &agent_id,
            &workspace_id,
            ACP_SID,
            vec![text_block("go")],
            None,
        )
        .await
        .expect("turn completes");

    // status, chunk, tool_call, tool_call_update, stream:end, idle.
    let mut events = Vec::new();
    while events.len() < 6 {
        let batch = timeout(Duration::from_secs(2), sub.recv())
            .await
            .expect("recv timed out")
            .expect("subscription open");
        events.extend(batch);
    }

    let messages = bus
        .store()
        .get_agent_messages(&agent_id, None)
        .await
        .expect("read messages");
    assert_eq!(messages.len(), 1);
    let mid = &messages[0].id;
    // text(0) → tool_use(1) patched to completed → tool_result(2) with output.
    assert_eq!(
        messages[0].content,
        json!([
            { "type": "text", "id": format!("{mid}:0"), "text": "Working " },
            { "type": "tool_use", "id": format!("{mid}:1"), "name": "Run tests",
              "input": { "path": ".", "_acpTitle": "Run tests" }, "toolCallId": "t1",
              "metadata": { "toolKind": "terminal", "status": "completed" } },
            { "type": "tool_result", "id": format!("{mid}:2"), "tool_use_id": "t1",
              "output": { "summary": "12 passed" }, "is_error": false },
        ])
    );

    // Both tool events target the SAME tool_use block id (the result update
    // patches the existing block in place rather than re-indexing).
    let tool_events: Vec<_> = events
        .iter()
        .filter(|e| e.event_type == "agent:tool:call")
        .collect();
    assert_eq!(tool_events.len(), 2);
    for e in &tool_events {
        assert_eq!(e.data["blockId"], json!(format!("{mid}:1")));
        assert_eq!(e.data["blockIndex"], json!(1));
        assert_eq!(e.data["toolCallId"], json!("t1"));
    }
    // The completing update carries status + output additively.
    assert_eq!(tool_events[1].data["status"], json!("completed"));
    assert_eq!(
        tool_events[1].data["output"],
        json!({ "summary": "12 passed" })
    );
}

/// A prompt turn that streams a sparse `tool_call` (short title, no input),
/// then a `tool_call_update` carrying the richer title + input, then a
/// status-only completing update — the Claude shape that collapsed rows to a
/// bare "Run" (title arrives on an update, later updates are status-only).
fn prompt_updates_sparse_then_richer_title() -> Vec<String> {
    let tool_call = json!({
        "jsonrpc": "2.0",
        "method": "session/update",
        "params": {
            "sessionId": ACP_SID,
            "update": { "sessionUpdate": "tool_call", "toolCallId": "t1",
                "title": "Run", "kind": "execute", "status": "in_progress" }
        }
    })
    .to_string();
    let richer = json!({
        "jsonrpc": "2.0",
        "method": "session/update",
        "params": {
            "sessionId": ACP_SID,
            "update": { "sessionUpdate": "tool_call_update", "toolCallId": "t1",
                "title": "Run: cargo test --all",
                "rawInput": { "command": "cargo test --all" } }
        }
    })
    .to_string();
    let tool_done = json!({
        "jsonrpc": "2.0",
        "method": "session/update",
        "params": {
            "sessionId": ACP_SID,
            "update": { "sessionUpdate": "tool_call_update", "toolCallId": "t1",
                "status": "completed", "rawOutput": { "summary": "ok" } }
        }
    })
    .to_string();
    vec![tool_call, richer, tool_done]
}

/// Rebuild the `tool_use` block from an `agent:tool:call` event exactly the
/// way `tool_delta` does (§7.1 shared factory) so tests can assert the live
/// delta stays byte-identical to the persisted block.
fn rebuild_block_from_event(data: &Value) -> Value {
    crate::tool_block::build_tool_use_block(
        data["blockId"].as_str().expect("blockId"),
        data["toolName"].as_str().expect("toolName"),
        data["title"].as_str().expect("title"),
        data["input"].clone(),
        data["toolCallId"].as_str().expect("toolCallId"),
        data["toolKind"].as_str().expect("toolKind"),
        data["status"].as_str().expect("status"),
    )
}

/// A status-only `tool_call_update` after a titled `tool_call` must not wipe
/// the tool name/title/input: the persisted block keeps them, and the
/// published `agent:tool:call` event carries the MERGED fields (backfilled
/// from the transcript block) so `tool_delta`'s rebuilt block stays
/// byte-identical to the persisted one (§7.1).
#[tokio::test]
async fn status_only_update_keeps_title_name_and_input_on_event() {
    let (_tmp, services, bus, agent_id, workspace_id) = setup().await;
    let (conn, mut note_rx, _agent) = connect_with(prompt_updates_with_tool_result());
    let mut sub = bus.subscribe(SubscriptionFilter::default());

    services
        .run_prompt_turn(
            &conn,
            &mut note_rx,
            &agent_id,
            &workspace_id,
            ACP_SID,
            vec![text_block("go")],
            None,
        )
        .await
        .expect("turn completes");

    // status, chunk, tool_call, tool_call_update, stream:end, idle.
    let mut events = Vec::new();
    while events.len() < 6 {
        let batch = timeout(Duration::from_secs(2), sub.recv())
            .await
            .expect("recv timed out")
            .expect("subscription open");
        events.extend(batch);
    }
    let tool_events: Vec<_> = events
        .iter()
        .filter(|e| e.event_type == "agent:tool:call")
        .collect();
    assert_eq!(tool_events.len(), 2);

    // The status-only completing update is sparse on the wire (no title, no
    // input) — the published event must backfill the merged fields from the
    // transcript block instead of shipping empties that wipe the row live.
    let update = &tool_events[1].data;
    assert_eq!(update["toolName"], json!("Run tests"));
    assert_eq!(update["title"], json!("Run tests"));
    assert_eq!(update["toolKind"], json!("terminal"));
    assert_eq!(
        update["input"],
        json!({ "path": ".", "_acpTitle": "Run tests" })
    );
    assert_eq!(update["status"], json!("completed"));

    // Byte-identical invariant: rebuilding the block from the event the way
    // `tool_delta` does yields exactly the persisted block.
    let messages = bus
        .store()
        .get_agent_messages(&agent_id, None)
        .await
        .expect("read messages");
    let persisted = &messages[0].content[1];
    assert_eq!(persisted["type"], json!("tool_use"));
    assert_eq!(&rebuild_block_from_event(update), persisted);
}

/// A richer title/input arriving on a later `tool_call_update` must be
/// persisted into the existing `tool_use` block (not just the first-sight
/// sparse title), and subsequent status-only updates must not undo it.
#[tokio::test]
async fn richer_title_update_is_merged_into_block_and_event() {
    let (_tmp, services, bus, agent_id, workspace_id) = setup().await;
    let (conn, mut note_rx, _agent) = connect_with(prompt_updates_sparse_then_richer_title());
    let mut sub = bus.subscribe(SubscriptionFilter::default());

    services
        .run_prompt_turn(
            &conn,
            &mut note_rx,
            &agent_id,
            &workspace_id,
            ACP_SID,
            vec![text_block("go")],
            None,
        )
        .await
        .expect("turn completes");

    // status, tool_call, richer update, completing update, stream:end, idle.
    let mut events = Vec::new();
    while events.len() < 6 {
        let batch = timeout(Duration::from_secs(2), sub.recv())
            .await
            .expect("recv timed out")
            .expect("subscription open");
        events.extend(batch);
    }
    let tool_events: Vec<_> = events
        .iter()
        .filter(|e| e.event_type == "agent:tool:call")
        .collect();
    assert_eq!(tool_events.len(), 3);

    // The persisted block carries the RICHER title/input from the mid-flight
    // update, with the completing status patched on top.
    let messages = bus
        .store()
        .get_agent_messages(&agent_id, None)
        .await
        .expect("read messages");
    assert_eq!(messages.len(), 1);
    let mid = &messages[0].id;
    assert_eq!(
        messages[0].content,
        json!([
            { "type": "tool_use", "id": format!("{mid}:0"), "name": "Run",
              "input": { "command": "cargo test --all",
                         "_acpTitle": "Run: cargo test --all" },
              "toolCallId": "t1",
              "metadata": { "toolKind": "terminal", "status": "completed" } },
            { "type": "tool_result", "id": format!("{mid}:1"), "tool_use_id": "t1",
              "output": { "summary": "ok" }, "is_error": false },
        ])
    );

    // The richer-update event carries the merged title/input; the final
    // status-only event keeps them (backfilled from the block).
    let richer = &tool_events[1].data;
    assert_eq!(richer["title"], json!("Run: cargo test --all"));
    assert_eq!(richer["toolName"], json!("Run"));
    assert_eq!(richer["toolKind"], json!("terminal"));
    assert_eq!(
        richer["input"],
        json!({ "command": "cargo test --all", "_acpTitle": "Run: cargo test --all" })
    );
    let done = &tool_events[2].data;
    assert_eq!(done["title"], json!("Run: cargo test --all"));
    assert_eq!(done["toolName"], json!("Run"));
    assert_eq!(done["toolKind"], json!("terminal"));
    assert_eq!(done["status"], json!("completed"));
    assert_eq!(&rebuild_block_from_event(done), &messages[0].content[0]);
}

/// A tool completing with a proposal-MIME resource item in its output array
/// persists the `tool_result` (output unchanged) AND a standalone
/// proposal-resource block right after it (§7.1).
#[tokio::test]
async fn tool_output_with_proposal_resource_appends_standalone_block() {
    let (_tmp, services, bus, agent_id, workspace_id) = setup().await;
    let (conn, mut note_rx, _agent) = connect_with(prompt_updates_with_proposal_resource());

    services
        .run_prompt_turn(
            &conn,
            &mut note_rx,
            &agent_id,
            &workspace_id,
            ACP_SID,
            vec![text_block("go")],
            None,
        )
        .await
        .expect("turn completes");

    let messages = bus
        .store()
        .get_agent_messages(&agent_id, None)
        .await
        .expect("read messages");
    assert_eq!(messages.len(), 1);
    let mid = &messages[0].id;
    let output = json!([
        { "type": "text", "text": "Proposal shown" },
        { "type": "resource", "resource": {
            "uri": "intent-proposal://settings-change/Update",
            "name": "Update",
            "mimeType": "application/vnd.intent.proposal+json",
            "text": "{\"kind\":\"settings-change\"}" } }
    ]);
    // tool_use(0) → tool_result(1, output unchanged) → standalone proposal(2).
    assert_eq!(
        messages[0].content,
        json!([
            { "type": "tool_use", "id": format!("{mid}:0"), "name": "workspace_api",
              "input": { "code": "ws.app.proposal.show(p)", "_acpTitle": "workspace_api" },
              "toolCallId": "t1",
              "metadata": { "toolKind": "other", "status": "completed" } },
            { "type": "tool_result", "id": format!("{mid}:1"), "tool_use_id": "t1",
              "output": output, "is_error": false },
            { "type": "resource", "id": format!("{mid}:2"), "resource": {
                "uri": "intent-proposal://settings-change/Update",
                "name": "Update",
                "mimeType": "application/vnd.intent.proposal+json",
                "text": "{\"kind\":\"settings-change\"}" } },
        ])
    );
}

/// A tool completing with a provider-collapsed output (auggie flattens the
/// MCP content items into `{ "output": "<stringified {ok, proposal}>" }`,
/// dropping the resource item) still persists the standalone
/// proposal-resource block via the fallback lift (§7.1).
#[tokio::test]
async fn tool_output_with_collapsed_proposal_appends_standalone_block() {
    let (_tmp, services, bus, agent_id, workspace_id) = setup().await;
    let (conn, mut note_rx, _agent) = connect_with(prompt_updates_with_collapsed_proposal());

    services
        .run_prompt_turn(
            &conn,
            &mut note_rx,
            &agent_id,
            &workspace_id,
            ACP_SID,
            vec![text_block("go")],
            None,
        )
        .await
        .expect("turn completes");

    let messages = bus
        .store()
        .get_agent_messages(&agent_id, None)
        .await
        .expect("read messages");
    assert_eq!(messages.len(), 1);
    let mid = &messages[0].id;
    let blocks = messages[0].content.as_array().expect("content is array");
    assert_eq!(
        blocks.len(),
        3,
        "tool_use + tool_result + standalone proposal"
    );
    // tool_result output is the collapsed object, unchanged.
    assert_eq!(blocks[1]["type"], "tool_result");
    assert!(
        blocks[1]["output"]["output"].is_string(),
        "output unchanged"
    );
    // The standalone proposal block is rebuilt from the collapsed payload.
    let proposal = &blocks[2];
    assert_eq!(proposal["type"], "resource");
    assert_eq!(proposal["id"], format!("{mid}:2"));
    assert_eq!(
        proposal["resource"]["mimeType"],
        "application/vnd.intent.proposal+json"
    );
    assert_eq!(
        proposal["resource"]["uri"],
        "intent-proposal://settings-change/Update%20Setting"
    );
    let text = proposal["resource"]["text"].as_str().expect("text");
    let parsed: Value = serde_json::from_str(text).expect("text parses");
    assert_eq!(parsed["kind"], "settings-change");
    assert_eq!(parsed["preview"]["title"], "Update Setting");
}

/// A prompt turn whose `workspace_api` tool completes with a GARBLED echo —
/// truncated/corrupted so neither the array path nor the collapsed-output
/// lift can recover anything (§7.1 deterministic-attach scenario).
fn prompt_updates_with_garbled_tool_output() -> Vec<String> {
    let tool_call = json!({
        "jsonrpc": "2.0",
        "method": "session/update",
        "params": {
            "sessionId": ACP_SID,
            "update": { "sessionUpdate": "tool_call", "toolCallId": "t1",
                "title": "workspace_api", "kind": "other", "status": "in_progress",
                "rawInput": { "code": "ws.app.proposal.show(p)" } }
        }
    })
    .to_string();
    let tool_done = json!({
        "jsonrpc": "2.0",
        "method": "session/update",
        "params": {
            "sessionId": ACP_SID,
            "update": { "sessionUpdate": "tool_call_update", "toolCallId": "t1",
                "status": "completed",
                "rawOutput": { "output": "[tool ran] {\"ok\": tru…(truncated)" } }
        }
    })
    .to_string();
    vec![tool_call, tool_done]
}

fn test_attachment(id: &str, policy: intent_core::AttachmentPolicy) -> intent_core::TurnAttachment {
    intent_core::TurnAttachment {
        id: id.to_string(),
        policy,
        mime_type: "application/vnd.intent.proposal+json".to_string(),
        uri: "intent-proposal://settings-change/Registered".to_string(),
        name: "Registered".to_string(),
        text: format!("{{\"kind\":\"settings-change\",\"attachmentId\":\"{id}\"}}"),
    }
}

/// §7.1 deterministic attach: an `AtToolResult` attachment registered before
/// the tool's completion echo arrives is attached as the standalone resource
/// block even when the echo is garbled beyond what the lift fallback can
/// recover — the canonical registry payload wins, no echo parsing.
#[tokio::test]
async fn registered_attachment_survives_garbled_tool_echo() {
    let (_tmp, services, bus, agent_id, workspace_id) = setup().await;
    let (conn, mut note_rx, _agent) = connect_with(prompt_updates_with_garbled_tool_output());
    services.turn_attachments().register(
        &agent_id,
        test_attachment("tar-reg1", intent_core::AttachmentPolicy::AtToolResult),
    );

    services
        .run_prompt_turn(
            &conn,
            &mut note_rx,
            &agent_id,
            &workspace_id,
            ACP_SID,
            vec![text_block("go")],
            None,
        )
        .await
        .expect("turn completes");

    let messages = bus
        .store()
        .get_agent_messages(&agent_id, None)
        .await
        .expect("read messages");
    assert_eq!(messages.len(), 1);
    let mid = &messages[0].id;
    let blocks = messages[0].content.as_array().expect("content is array");
    assert_eq!(blocks.len(), 3, "tool_use + tool_result + registered block");
    // tool_result echo preserved verbatim (garbled).
    assert_eq!(blocks[1]["type"], "tool_result");
    assert_eq!(
        blocks[1]["output"]["output"],
        "[tool ran] {\"ok\": tru…(truncated)"
    );
    // The standalone block carries the CANONICAL registry payload.
    assert_eq!(
        blocks[2],
        json!({ "type": "resource", "id": format!("{mid}:2"), "resource": {
            "uri": "intent-proposal://settings-change/Registered",
            "name": "Registered",
            "mimeType": "application/vnd.intent.proposal+json",
            "text": "{\"kind\":\"settings-change\",\"attachmentId\":\"tar-reg1\"}" } })
    );
    // The claim consumed the entry — nothing drains at the next turn end.
    assert!(services
        .turn_attachments()
        .finish_turn(&agent_id)
        .is_empty());
}

/// §7.1 `AtTurnEnd` policy: attachments registered with the turn-end policy
/// are appended as trailing resource blocks when the turn finalizes, and
/// unclaimed `AtToolResult` leftovers are dropped (not attached, not leaked
/// to a later turn). The terminal `agent:stream:end` carries the drained
/// blocks live as `trailingBlocks` — byte-identical to the persisted blocks —
/// plus the turn's `messageId` (monorepo#732 fix wave).
#[tokio::test]
async fn turn_end_attachments_append_trailing_blocks_and_leftovers_drop() {
    let (_tmp, services, bus, agent_id, workspace_id) = setup().await;
    let (conn, mut note_rx, _agent) = connect_with(prompt_updates());
    let mut sub = bus.subscribe(SubscriptionFilter::default());
    let registry = services.turn_attachments();
    registry.register(
        &agent_id,
        test_attachment("tar-end1", intent_core::AttachmentPolicy::AtTurnEnd),
    );
    // An orphaned AtToolResult entry (its tool echo never arrives this turn).
    registry.register(
        &agent_id,
        test_attachment("tar-orphan", intent_core::AttachmentPolicy::AtToolResult),
    );

    services
        .run_prompt_turn(
            &conn,
            &mut note_rx,
            &agent_id,
            &workspace_id,
            ACP_SID,
            vec![text_block("go")],
            None,
        )
        .await
        .expect("turn completes");

    let messages = bus
        .store()
        .get_agent_messages(&agent_id, None)
        .await
        .expect("read messages");
    assert_eq!(messages.len(), 1);
    let blocks = messages[0].content.as_array().expect("content is array");
    // The AtTurnEnd attachment is the trailing block; the orphan is nowhere.
    let last = blocks.last().expect("blocks non-empty");
    assert_eq!(last["type"], "resource");
    assert!(last["resource"]["text"]
        .as_str()
        .is_some_and(|t| t.contains("tar-end1")));
    assert!(
        !blocks.iter().any(|b| b["resource"]["text"]
            .as_str()
            .is_some_and(|t| t.contains("tar-orphan"))),
        "unclaimed AtToolResult leftover must be dropped at turn end"
    );
    // Registry fully drained — nothing leaks into a later turn.
    assert!(registry.finish_turn(&agent_id).is_empty());

    // The terminal stream:end delivers the drained blocks LIVE: the FE
    // finalizes the in-flight message from accumulated chunks at stream-end,
    // so blocks appended only after the stream loop would otherwise never
    // reach it without a refetch.
    let mut end_event = None;
    while end_event.is_none() {
        let batch = timeout(Duration::from_secs(2), sub.recv())
            .await
            .expect("recv timed out")
            .expect("subscription open");
        end_event = batch
            .into_iter()
            .find(|e| e.event_type == "agent:stream:end");
    }
    let end = end_event.unwrap();
    assert_eq!(end.data["messageId"], json!(messages[0].id));
    assert_eq!(
        end.data["trailingBlocks"],
        json!([last.clone()]),
        "trailingBlocks are byte-identical to the persisted trailing blocks"
    );
}

/// STAB-124 regression: a stale `tool_call_update` for a toolCallId this turn
/// never saw (the abort echo a cancelled child emits after an interrupt: no
/// title/rawInput → derived name "") must NOT fabricate an anonymous
/// `tool_use` block in the persisted message, and no `agent:tool:call` event
/// is published for it. The turn's real text still persists normally.
#[tokio::test]
async fn stale_anonymous_tool_update_is_dropped_not_persisted() {
    let (_tmp, services, bus, agent_id, workspace_id) = setup().await;
    let (conn, mut note_rx, _agent) = connect_with(prompt_updates_stale_anonymous_tool());
    let mut sub = bus.subscribe(SubscriptionFilter::default());

    services
        .run_prompt_turn(
            &conn,
            &mut note_rx,
            &agent_id,
            &workspace_id,
            ACP_SID,
            vec![text_block("go")],
            None,
        )
        .await
        .expect("turn completes");

    // status, chunk, stream:end, idle — and NO agent:tool:call for the stale update.
    let mut events = Vec::new();
    while events.len() < 4 {
        let batch = timeout(Duration::from_secs(2), sub.recv())
            .await
            .expect("recv timed out")
            .expect("subscription open");
        events.extend(batch);
    }
    assert!(
        !events.iter().any(|e| e.event_type == "agent:tool:call"),
        "no tool event published for a dropped anonymous tool update"
    );

    let messages = bus
        .store()
        .get_agent_messages(&agent_id, None)
        .await
        .expect("read messages");
    assert_eq!(messages.len(), 1);
    let mid = &messages[0].id;
    assert_eq!(
        messages[0].content,
        json!([
            { "type": "text", "id": format!("{mid}:0"), "text": "Resumed" },
        ]),
        "the anonymous tool_use block (and its errored tool_result) are never persisted"
    );
}

#[tokio::test]
async fn open_acp_session_persists_id() {
    let (_tmp, services, bus, agent_id, _ws) = setup().await;
    let (conn, _rx, _agent) = connect();
    let mut sub = bus.subscribe(SubscriptionFilter::default());
    let opened = services
        .open_acp_session(&conn, &agent_id, "/tmp/ws", Vec::new())
        .await
        .expect("open session");
    assert_eq!(opened.session_id, ACP_SID);
    let stored = bus.store().get_agent_session(&agent_id).await.unwrap();
    assert_eq!(stored.acp_session_id.as_deref(), Some(ACP_SID));

    // `session-create` status hint fires ahead of the `session/new` wire call
    // (STAT-1 / PROTOCOL §7) so the FE spinner can render "Creating session…"
    // while the provider spins up the session.
    let batch = timeout(Duration::from_secs(2), sub.recv())
        .await
        .expect("recv timed out")
        .expect("subscription open");
    let status = batch
        .iter()
        .find(|e| e.event_type == "agent:stream:status")
        .expect("status event on the wire");
    assert_eq!(status.data["agentId"], json!("agent-1"));
    assert_eq!(status.data["workspaceId"], json!("ws-1"));
    assert_eq!(status.data["phase"], json!("session-create"));
    assert_eq!(status.data["message"], json!("Creating session\u{2026}"));
    assert_eq!(status.data["level"], json!("info"));
}

#[tokio::test]
async fn resume_requires_capability_and_stored_id() {
    let (_tmp, services, bus, agent_id, ws) = setup().await;
    let (conn, _rx, _agent) = connect();

    // No stored acpSessionId yet → None even with the capability.
    assert!(services
        .resume_acp_session(&conn, &init_caps(true), &agent_id, "/tmp/ws", Vec::new())
        .await
        .unwrap()
        .is_none());

    bus.store()
        .set_acp_session_id(&ws, &agent_id, ACP_SID)
        .await
        .unwrap();

    // Stored id but the agent lacks loadSession → None.
    assert!(services
        .resume_acp_session(&conn, &init_caps(false), &agent_id, "/tmp/ws", Vec::new())
        .await
        .unwrap()
        .is_none());

    // Stored id + capability → resumes.
    let opened = services
        .resume_acp_session(&conn, &init_caps(true), &agent_id, "/tmp/ws", Vec::new())
        .await
        .unwrap()
        .expect("resume yields opened session");
    assert_eq!(opened.session_id, ACP_SID);

    // A successful resume keeps the stored id canonical (no overwrite).
    let stored = bus.store().get_agent_session(&agent_id).await.unwrap();
    assert_eq!(stored.acp_session_id.as_deref(), Some(ACP_SID));
}

/// monorepo#907 regression: a COMMITTED cross-provider `agent.setModel`
/// leaves the old provider's `acp_session_id` in place (deferred-commit
/// semantics), so the resume path must never offer that foreign id to the
/// NEW provider's binary via `session/load` — a provider that silently
/// accepted it would skip the history replay entirely. The stored id's owner
/// is the committed `last_turn_provider`; when it differs from the provider
/// this turn resolves to, resume is skipped outright (the manager then falls
/// into the recreate + supervisor-XML-replay branch). Reverting the switch
/// before the next message restores the match, so the original id resumes.
#[tokio::test]
async fn resume_skips_foreign_session_after_cross_provider_switch() {
    let (_tmp, services, bus, agent_id, ws) = setup().await;
    let (conn, _rx, _agent) = connect();

    // Turn 1 ran on claude-code: session id persisted, identity committed.
    bus.store()
        .set_agent_session_model(
            &ws,
            &agent_id,
            "claude-code:opus",
            Some("claude-code"),
            &now_iso(),
        )
        .await
        .unwrap();
    bus.store()
        .set_acp_session_id(&ws, &agent_id, ACP_SID)
        .await
        .unwrap();
    bus.store()
        .set_agent_session_last_turn_model(&ws, &agent_id, Some("opus"), "claude-code")
        .await
        .unwrap();

    // Committed cross-provider switch (the narrow setModel writer): model +
    // provider now say grok, but the stored acp_session_id is claude-code's.
    bus.store()
        .set_agent_session_model(&ws, &agent_id, "grok:grok-4-fast", Some("grok"), &now_iso())
        .await
        .unwrap();
    assert!(
        services
            .resume_acp_session(&conn, &init_caps(true), &agent_id, "/tmp/ws", Vec::new())
            .await
            .unwrap()
            .is_none(),
        "foreign session/load must be skipped after a committed cross-provider switch"
    );
    // The skip must not destroy the stored id: revert depends on it.
    let stored = bus.store().get_agent_session(&agent_id).await.unwrap();
    assert_eq!(stored.acp_session_id.as_deref(), Some(ACP_SID));

    // Switch-and-revert before the next message: the identity matches the
    // stored id's owner again, so the original session resumes via load.
    bus.store()
        .set_agent_session_model(
            &ws,
            &agent_id,
            "claude-code:opus",
            Some("claude-code"),
            &now_iso(),
        )
        .await
        .unwrap();
    let opened = services
        .resume_acp_session(&conn, &init_caps(true), &agent_id, "/tmp/ws", Vec::new())
        .await
        .unwrap()
        .expect("revert-before-send resumes the original session");
    assert_eq!(opened.session_id, ACP_SID);
}

/// Same-provider model switches keep `session/load` resume: only a provider
/// identity change gates the monorepo#907 skip, never the model. An agent
/// with no committed last-turn identity (legacy rows / a crash between the
/// session open and the identity commit) also keeps today's resume behavior.
#[tokio::test]
async fn resume_survives_same_provider_model_switch() {
    let (_tmp, services, bus, agent_id, ws) = setup().await;
    let (conn, _rx, _agent) = connect();
    bus.store()
        .set_agent_session_model(
            &ws,
            &agent_id,
            "claude-code:opus",
            Some("claude-code"),
            &now_iso(),
        )
        .await
        .unwrap();
    bus.store()
        .set_acp_session_id(&ws, &agent_id, ACP_SID)
        .await
        .unwrap();

    // No committed last-turn identity yet → resume proceeds as before.
    let opened = services
        .resume_acp_session(&conn, &init_caps(true), &agent_id, "/tmp/ws", Vec::new())
        .await
        .unwrap()
        .expect("no committed identity keeps resume");
    assert_eq!(opened.session_id, ACP_SID);

    bus.store()
        .set_agent_session_last_turn_model(&ws, &agent_id, Some("opus"), "claude-code")
        .await
        .unwrap();
    // Same-provider model switch: the session id's owner is unchanged.
    bus.store()
        .set_agent_session_model(
            &ws,
            &agent_id,
            "claude-code:sonnet",
            Some("claude-code"),
            &now_iso(),
        )
        .await
        .unwrap();
    let opened = services
        .resume_acp_session(&conn, &init_caps(true), &agent_id, "/tmp/ws", Vec::new())
        .await
        .unwrap()
        .expect("same-provider switch keeps session/load resume");
    assert_eq!(opened.session_id, ACP_SID);
}

/// Legacy default-provider aliases (`acp`/`augment`/`default`) spawn the same
/// default binary as the canonical id, so an alias-vs-canonical difference
/// between the persisted row and the committed `last_turn_provider` must not
/// read as a cross-provider switch: both sides canonicalize through the
/// registry before the monorepo#907 comparison.
#[tokio::test]
async fn resume_survives_default_provider_alias_mismatch() {
    let (_tmp, services, bus, agent_id, ws) = setup().await;
    let (conn, _rx, _agent) = connect();
    // Legacy row: bare model + alias provider → resolve_provider_id yields
    // the alias verbatim ("acp"), while the turn-start commit always stores
    // the spawn-resolved canonical id ("auggie").
    bus.store()
        .set_agent_session_model(&ws, &agent_id, "opus4.7", Some("acp"), &now_iso())
        .await
        .unwrap();
    bus.store()
        .set_acp_session_id(&ws, &agent_id, ACP_SID)
        .await
        .unwrap();
    bus.store()
        .set_agent_session_last_turn_model(&ws, &agent_id, Some("opus4.7"), "auggie")
        .await
        .unwrap();
    let opened = services
        .resume_acp_session(&conn, &init_caps(true), &agent_id, "/tmp/ws", Vec::new())
        .await
        .unwrap()
        .expect("alias-vs-canonical default provider ids keep resume");
    assert_eq!(opened.session_id, ACP_SID);
}

#[tokio::test]
async fn recreate_acp_session_replaces_stored_id() {
    let (_tmp, services, bus, agent_id, ws) = setup().await;
    let (conn, _rx, _agent) = connect();

    // A stale id is persisted (the resume-impossible fallback case).
    bus.store()
        .set_acp_session_id(&ws, &agent_id, "stale-id")
        .await
        .unwrap();

    // recreate opens a fresh session and CAS-swaps the lost id for the new one.
    let opened = services
        .recreate_acp_session(&conn, &agent_id, "stale-id", "/tmp/ws", Vec::new())
        .await
        .expect("recreate session");
    assert_eq!(opened.session_id, ACP_SID);
    let stored = bus.store().get_agent_session(&agent_id).await.unwrap();
    assert_eq!(stored.acp_session_id.as_deref(), Some(ACP_SID));

    // No-clobber: recreating again with a stale expected-old reuses the stored
    // canonical id rather than overwriting it (a second session/new is opened
    // but the CAS declines to swap).
    let opened = services
        .recreate_acp_session(&conn, &agent_id, "stale-id", "/tmp/ws", Vec::new())
        .await
        .expect("recreate session");
    assert_eq!(
        opened.session_id, ACP_SID,
        "diverged expected-old keeps the canonical id"
    );
    // CAS loss: the new session's modes must not be surfaced (they belong to a
    // session we didn't open).
    assert!(
        opened.modes.is_none(),
        "CAS loss must not surface modes captured from a session we didn't own"
    );
    let stored = bus.store().get_agent_session(&agent_id).await.unwrap();
    assert_eq!(stored.acp_session_id.as_deref(), Some(ACP_SID));
}

/// `resume_acp_session` on the happy path emits the `session-load` status
/// hint ahead of the `session/load` wire call so the pre-first-token spinner
/// can render "Resuming session…" (STAT-1 / PROTOCOL §7).
#[tokio::test]
async fn resume_acp_session_emits_session_load_status() {
    let (_tmp, services, bus, agent_id, ws) = setup().await;
    let (conn, _rx, _agent) = connect();
    bus.store()
        .set_acp_session_id(&ws, &agent_id, ACP_SID)
        .await
        .unwrap();
    let mut sub = bus.subscribe(SubscriptionFilter::default());

    let opened = services
        .resume_acp_session(&conn, &init_caps(true), &agent_id, "/tmp/ws", Vec::new())
        .await
        .unwrap()
        .expect("resume yields opened session");
    assert_eq!(opened.session_id, ACP_SID);

    let batch = timeout(Duration::from_secs(2), sub.recv())
        .await
        .expect("recv timed out")
        .expect("subscription open");
    let status = batch
        .iter()
        .find(|e| e.event_type == "agent:stream:status")
        .expect("status event on the wire");
    assert_eq!(status.data["phase"], json!("session-load"));
    assert_eq!(status.data["message"], json!("Resuming session\u{2026}"));
    assert_eq!(status.data["level"], json!("info"));
    assert_eq!(status.data["agentId"], json!("agent-1"));
    assert_eq!(status.data["workspaceId"], json!("ws-1"));
}

/// `recreate_acp_session` (the resume-impossible fallback) emits the
/// `session-create` status hint ahead of the `session/new` wire call — the
/// FE renders the same "Creating session…" phase as the brand-new-agent path.
#[tokio::test]
async fn recreate_acp_session_emits_session_create_status() {
    let (_tmp, services, bus, agent_id, ws) = setup().await;
    let (conn, _rx, _agent) = connect();
    bus.store()
        .set_acp_session_id(&ws, &agent_id, "stale-id")
        .await
        .unwrap();
    let mut sub = bus.subscribe(SubscriptionFilter::default());

    let opened = services
        .recreate_acp_session(&conn, &agent_id, "stale-id", "/tmp/ws", Vec::new())
        .await
        .expect("recreate session");
    assert_eq!(opened.session_id, ACP_SID);

    let batch = timeout(Duration::from_secs(2), sub.recv())
        .await
        .expect("recv timed out")
        .expect("subscription open");
    let status = batch
        .iter()
        .find(|e| e.event_type == "agent:stream:status")
        .expect("status event on the wire");
    assert_eq!(status.data["phase"], json!("session-create"));
    assert_eq!(status.data["message"], json!("Creating session\u{2026}"));
}

/// monorepo#764: a transport death BEFORE any streamed output must resolve
/// the turn with the pre-output marker error and SUPPRESS the terminal
/// `agent:failed` + `agent:stream:end` pair — the worker either redrives the
/// prompt silently or emits the pair via the terminal-failure path.
#[tokio::test]
async fn pre_output_transport_death_marks_error_and_suppresses_terminal_events() {
    let (_tmp, services, bus, agent_id, workspace_id) = setup().await;
    // The dying mock streams NOTHING before dropping its pipes.
    let (conn, mut note_rx, _agent) = connect_dying(Vec::new());
    let mut sub = bus.subscribe(SubscriptionFilter::default());

    let err = services
        .run_prompt_turn(
            &conn,
            &mut note_rx,
            &agent_id,
            &workspace_id,
            ACP_SID,
            vec![text_block("hi")],
            None,
        )
        .await
        .expect_err("transport death fails the turn");
    assert!(
        matches!(
            &err,
            intent_core::Error::Internal(msg)
                if msg.starts_with(crate::agent_session::PROMPT_PRE_OUTPUT_TRANSPORT_PREFIX)
        ),
        "pre-output transport failure carries the redrive marker: {err}"
    );

    // Only the pre-first-token status hint reached the bus — no agent:failed,
    // no agent:stream:end (the worker owns the terminal decision).
    let mut events = Vec::new();
    while let Ok(Some(batch)) = timeout(Duration::from_millis(300), sub.recv()).await {
        events.extend(batch);
    }
    let types: Vec<&str> = events.iter().map(|e| e.event_type.as_str()).collect();
    assert_eq!(
        types,
        vec!["agent:stream:status"],
        "terminal events suppressed for a pre-output transport failure"
    );
    // Nothing streamed → no assistant row persisted.
    assert!(
        bus.store()
            .get_agent_messages(&agent_id, None)
            .await
            .unwrap()
            .is_empty(),
        "no transcript row for an output-free failed attempt"
    );
}

/// monorepo#764 inverse: the SAME transport death AFTER a streamed chunk is
/// NOT redrive-eligible — the ordinary `session/prompt failed:` wrapper and
/// the terminal `agent:failed` + `agent:stream:end` pair are unchanged
/// (STAB-6 semantics preserved, including event ordering).
#[tokio::test]
async fn post_output_transport_death_keeps_terminal_events() {
    let (_tmp, services, bus, agent_id, workspace_id) = setup().await;
    let chunk = json!({
        "jsonrpc": "2.0",
        "method": "session/update",
        "params": {
            "sessionId": ACP_SID,
            "update": { "sessionUpdate": "agent_message_chunk",
                "content": { "type": "text", "text": "partial" } }
        }
    })
    .to_string();
    let (conn, mut note_rx, _agent) = connect_dying(vec![chunk]);
    let mut sub = bus.subscribe(SubscriptionFilter::default());

    let err = services
        .run_prompt_turn(
            &conn,
            &mut note_rx,
            &agent_id,
            &workspace_id,
            ACP_SID,
            vec![text_block("hi")],
            None,
        )
        .await
        .expect_err("transport death fails the turn");
    assert!(
        matches!(
            &err,
            intent_core::Error::Internal(msg) if msg.starts_with("session/prompt failed:")
        ),
        "post-output failure keeps the ordinary wrapper: {err}"
    );

    let mut events = Vec::new();
    while let Ok(Some(batch)) = timeout(Duration::from_millis(300), sub.recv()).await {
        events.extend(batch);
    }
    let types: Vec<&str> = events.iter().map(|e| e.event_type.as_str()).collect();
    assert_eq!(
        types,
        vec![
            "agent:stream:status",
            "chat:stream:delta",
            "agent:stream:activity",
            "agent:stream:end",
            "agent:failed",
        ],
        "post-output transport death keeps the terminal pair and ordering"
    );
    // The streamed partial persists as the turn's assistant row.
    let messages = bus
        .store()
        .get_agent_messages(&agent_id, None)
        .await
        .unwrap();
    assert_eq!(messages.len(), 1, "partial output persisted");
}

/// Warn-and-continue (idle timeout, silent turn): a prompt that goes the
/// whole idle window with zero `session/update` traffic resolves with the
/// idle-timeout error under the ORDINARY wrapper (no streamed-output suffix),
/// emits a normal `agent:stream:end`, and SUPPRESSES `agent:failed` +
/// `agent:idle` — the turn worker owns the warn/terminal decision.
#[tokio::test]
async fn idle_timeout_silent_turn_suppresses_agent_failed() {
    let _env = EnvGuard::set_all(&[("INTENTD_PROMPT_IDLE_TIMEOUT_MS", "100")]);
    let (_tmp, services, bus, agent_id, workspace_id) = setup().await;
    // The silent mock streams NOTHING and never resolves the prompt.
    let (conn, mut note_rx, _agent) = connect_silent(Vec::new());
    let mut sub = bus.subscribe(SubscriptionFilter::default());

    let err = services
        .run_prompt_turn(
            &conn,
            &mut note_rx,
            &agent_id,
            &workspace_id,
            ACP_SID,
            vec![text_block("hi")],
            Some("turn-idle-1"),
        )
        .await
        .expect_err("idle timeout fails the turn");
    let intent_core::Error::Internal(msg) = &err else {
        panic!("Internal error expected: {err}");
    };
    assert!(
        msg.starts_with("session/prompt failed: session/prompt idle timeout"),
        "idle timeout keeps the ordinary wrapper: {msg}"
    );
    assert!(
        !msg.ends_with(crate::agent_session::PROMPT_IDLE_TIMEOUT_STREAMED_SUFFIX),
        "a silent turn carries no streamed-output suffix: {msg}"
    );

    let mut events = Vec::new();
    while let Ok(Some(batch)) = timeout(Duration::from_millis(300), sub.recv()).await {
        events.extend(batch);
    }
    let types: Vec<&str> = events.iter().map(|e| e.event_type.as_str()).collect();
    assert_eq!(
        types,
        vec!["agent:stream:status", "agent:stream:end"],
        "normal stream:end, no agent:failed / agent:idle on idle timeout"
    );
    // The stream:end is the NORMAL turn close (turn correlation intact, no
    // messageId since nothing streamed).
    let end = events
        .iter()
        .find(|e| e.event_type == "agent:stream:end")
        .unwrap();
    assert_eq!(end.data["turnId"], json!("turn-idle-1"));
    assert!(end.data.get("messageId").is_none());
    // Nothing streamed → no assistant row persisted.
    assert!(
        bus.store()
            .get_agent_messages(&agent_id, None)
            .await
            .unwrap()
            .is_empty(),
        "no transcript row for a fully silent timed-out turn"
    );
}

/// Warn-and-continue (idle timeout after streamed output): the partial
/// assistant row is flushed to the transcript, the normal `agent:stream:end`
/// carries its `messageId`, `agent:failed` stays suppressed, and the wrapped
/// error carries the streamed-output suffix (the worker restarts its
/// consecutive-timeout counter on it).
#[tokio::test]
async fn idle_timeout_after_output_flushes_partial_and_marks_streamed() {
    let _env = EnvGuard::set_all(&[("INTENTD_PROMPT_IDLE_TIMEOUT_MS", "100")]);
    let (_tmp, services, bus, agent_id, workspace_id) = setup().await;
    let chunk = json!({
        "jsonrpc": "2.0",
        "method": "session/update",
        "params": {
            "sessionId": ACP_SID,
            "update": { "sessionUpdate": "agent_message_chunk",
                "content": { "type": "text", "text": "partial work" } }
        }
    })
    .to_string();
    let (conn, mut note_rx, _agent) = connect_silent(vec![chunk]);
    let mut sub = bus.subscribe(SubscriptionFilter::default());

    let err = services
        .run_prompt_turn(
            &conn,
            &mut note_rx,
            &agent_id,
            &workspace_id,
            ACP_SID,
            vec![text_block("hi")],
            None,
        )
        .await
        .expect_err("idle timeout fails the turn");
    let intent_core::Error::Internal(msg) = &err else {
        panic!("Internal error expected: {err}");
    };
    assert!(
        msg.starts_with("session/prompt failed: session/prompt idle timeout"),
        "idle timeout keeps the ordinary wrapper: {msg}"
    );
    assert!(
        msg.ends_with(crate::agent_session::PROMPT_IDLE_TIMEOUT_STREAMED_SUFFIX),
        "streamed output stamps the activity suffix: {msg}"
    );

    let mut events = Vec::new();
    while let Ok(Some(batch)) = timeout(Duration::from_millis(300), sub.recv()).await {
        events.extend(batch);
    }
    let types: Vec<&str> = events.iter().map(|e| e.event_type.as_str()).collect();
    assert_eq!(
        types,
        vec![
            "agent:stream:status",
            "chat:stream:delta",
            "agent:stream:activity",
            "agent:stream:end"
        ],
        "partial streams, normal stream:end, no agent:failed / agent:idle"
    );
    // The partial persists as the turn's assistant row (interrupt-flush
    // semantics) and the stream:end advertises it.
    let messages = bus
        .store()
        .get_agent_messages(&agent_id, None)
        .await
        .unwrap();
    assert_eq!(messages.len(), 1, "partial output persisted");
    assert_eq!(messages[0].role, "assistant");
    assert_eq!(messages[0].content[0]["text"], json!("partial work"));
    let end = events
        .iter()
        .find(|e| e.event_type == "agent:stream:end")
        .unwrap();
    assert_eq!(end.data["messageId"], json!(messages[0].id));
}

/// Warn-and-continue (idle timeout after an UNMAPPED update): a
/// `session/update` variant with no canonical turn mapping (here a thought
/// chunk — same class as plan/mode/usage) still reset the idle timer, so it
/// counts as intervening activity: the wrapped error carries the
/// streamed-output suffix even though nothing was applied to the transcript.
#[tokio::test]
async fn idle_timeout_after_unmapped_update_marks_streamed() {
    let _env = EnvGuard::set_all(&[("INTENTD_PROMPT_IDLE_TIMEOUT_MS", "100")]);
    let (_tmp, services, bus, agent_id, workspace_id) = setup().await;
    let thought = json!({
        "jsonrpc": "2.0",
        "method": "session/update",
        "params": {
            "sessionId": ACP_SID,
            "update": { "sessionUpdate": "agent_thought_chunk",
                "content": { "type": "text", "text": "thinking" } }
        }
    })
    .to_string();
    let (conn, mut note_rx, _agent) = connect_silent(vec![thought]);
    let _sub = bus.subscribe(SubscriptionFilter::default());

    let err = services
        .run_prompt_turn(
            &conn,
            &mut note_rx,
            &agent_id,
            &workspace_id,
            ACP_SID,
            vec![text_block("hi")],
            None,
        )
        .await
        .expect_err("idle timeout fails the turn");
    let intent_core::Error::Internal(msg) = &err else {
        panic!("Internal error expected: {err}");
    };
    assert!(
        msg.starts_with("session/prompt failed: session/prompt idle timeout"),
        "idle timeout keeps the ordinary wrapper: {msg}"
    );
    assert!(
        msg.ends_with(crate::agent_session::PROMPT_IDLE_TIMEOUT_STREAMED_SUFFIX),
        "an unmapped update still stamps the activity suffix: {msg}"
    );
    // Nothing mapped → no assistant row persisted.
    assert!(
        bus.store()
            .get_agent_messages(&agent_id, None)
            .await
            .unwrap()
            .is_empty(),
        "no transcript row when only unmapped updates arrived"
    );
}

/// Turn correlation (monorepo#1022): the failure-arm `agent:failed` emitted by
/// `run_prompt_turn` carries the caller-supplied `turnId`; when the caller
/// passes `None` (bare wiring) the field is omitted, never `null`.
#[tokio::test]
async fn prompt_turn_failure_stamps_turn_id_on_agent_failed() {
    let (_tmp, services, bus, agent_id, workspace_id) = setup().await;
    let chunk = json!({
        "jsonrpc": "2.0",
        "method": "session/update",
        "params": {
            "sessionId": ACP_SID,
            "update": { "sessionUpdate": "agent_message_chunk",
                "content": { "type": "text", "text": "partial" } }
        }
    })
    .to_string();
    let (conn, mut note_rx, _agent) = connect_dying(vec![chunk]);
    let mut sub = bus.subscribe(SubscriptionFilter::default());

    services
        .run_prompt_turn(
            &conn,
            &mut note_rx,
            &agent_id,
            &workspace_id,
            ACP_SID,
            vec![text_block("hi")],
            Some("turn-corr-1"),
        )
        .await
        .expect_err("transport death fails the turn");

    let mut events = Vec::new();
    while let Ok(Some(batch)) = timeout(Duration::from_millis(300), sub.recv()).await {
        events.extend(batch);
    }
    let failed = events
        .iter()
        .find(|e| e.event_type == "agent:failed")
        .expect("agent:failed event");
    assert_eq!(
        failed.data["turnId"],
        json!("turn-corr-1"),
        "agent:failed carries the turn correlation id: {:?}",
        failed.data
    );
}

/// Omit-when-absent counterpart: a `None` turn id leaves the `agent:failed`
/// payload without a `turnId` key (the wire contract forbids emitting null).
#[tokio::test]
async fn prompt_turn_failure_omits_turn_id_when_absent() {
    let (_tmp, services, bus, agent_id, workspace_id) = setup().await;
    let chunk = json!({
        "jsonrpc": "2.0",
        "method": "session/update",
        "params": {
            "sessionId": ACP_SID,
            "update": { "sessionUpdate": "agent_message_chunk",
                "content": { "type": "text", "text": "partial" } }
        }
    })
    .to_string();
    let (conn, mut note_rx, _agent) = connect_dying(vec![chunk]);
    let mut sub = bus.subscribe(SubscriptionFilter::default());

    services
        .run_prompt_turn(
            &conn,
            &mut note_rx,
            &agent_id,
            &workspace_id,
            ACP_SID,
            vec![text_block("hi")],
            None,
        )
        .await
        .expect_err("transport death fails the turn");

    let mut events = Vec::new();
    while let Ok(Some(batch)) = timeout(Duration::from_millis(300), sub.recv()).await {
        events.extend(batch);
    }
    let failed = events
        .iter()
        .find(|e| e.event_type == "agent:failed")
        .expect("agent:failed event");
    assert!(
        failed.data.get("turnId").is_none(),
        "no turnId key when the turn has none: {:?}",
        failed.data
    );
}

/// Detached turn-end bookkeeping (monorepo#738): a prompt whose result carries
/// an end-of-turn `usage` snapshot still lands the session snapshot and emits
/// `workspace:tokenUsage-changed`, even though the bookkeeping now runs in a
/// spawned task off the stream path. The turn's terminal `agent:stream:end`
/// must NOT wait on it — `workspace:tokenUsage-changed` has no ordering
/// guarantee relative to `agent:stream:end`, so the effects are awaited by
/// polling after the turn resolves.
#[tokio::test]
async fn detached_turn_end_usage_bookkeeping_still_lands() {
    let (_tmp, services, bus, agent_id, workspace_id) = setup().await;
    let (conn, mut note_rx, _agent) = connect_with_prompt_result(
        prompt_updates(),
        json!({
            "stopReason": "end_turn",
            "usage": {
                "totalTokens": 154,
                "inputTokens": 70,
                "outputTokens": 50,
                "cachedReadTokens": 30,
                "cachedWriteTokens": 4
            }
        }),
    );
    let mut sub = bus.subscribe(SubscriptionFilter {
        event_types: vec!["workspace:tokenUsage-changed".to_string()],
        ..Default::default()
    });

    let stop = services
        .run_prompt_turn(
            &conn,
            &mut note_rx,
            &agent_id,
            &workspace_id,
            ACP_SID,
            vec![text_block("hi")],
            None,
        )
        .await
        .expect("turn completes");
    assert_eq!(serde_json::to_value(stop).unwrap(), json!("end_turn"));

    // The detached task persists the snapshot and emits the event; await it.
    let batch = timeout(Duration::from_secs(5), sub.recv())
        .await
        .expect("tokenUsage-changed delivered")
        .expect("subscription open");
    let ev = batch
        .iter()
        .find(|e| e.event_type == "workspace:tokenUsage-changed")
        .expect("usage event");
    assert_eq!(ev.data["tokenUsage"]["totals"]["inputTokens"], json!(70));
    assert_eq!(ev.data["tokenUsage"]["totals"]["outputTokens"], json!(50));
    assert_eq!(
        ev.data["tokenUsage"]["totals"]["cacheReadTokens"],
        json!(30)
    );
    assert_eq!(
        ev.data["tokenUsage"]["totals"]["cacheCreationTokens"],
        json!(4)
    );

    // Durable effects: session snapshot + workspace tally both landed.
    let rows = bus
        .store()
        .get_workspace_agent_usage_data(&workspace_id)
        .await
        .expect("usage rows");
    let snapshot = rows[0].2.as_ref().expect("session snapshot persisted");
    assert_eq!(snapshot.input_tokens, 70);
    assert_eq!(snapshot.output_tokens, 50);
    let ws = bus
        .store()
        .get_workspace(&workspace_id)
        .await
        .expect("reload workspace");
    let usage = ws.token_usage.expect("workspace tally persisted");
    assert_eq!(usage.totals.input_tokens, 70);
    assert_eq!(usage.by_agent_id[&agent_id.0].input_tokens, 70);
}

/// Cross-turn bookkeeping ordering (monorepo#738): detached turn-end
/// bookkeeping tasks for one agent are CHAINED — each awaits its predecessor
/// — so a delayed task from turn N can neither skew turn N+1's usage-stats
/// delta nor overwrite its newer cumulative snapshot. Seed the chain with a
/// slow predecessor that lands the prior turn's snapshot (70 input tokens)
/// late; the next turn (cumulative 100) must wait for it, record a stats
/// delta of 30 (not a double-counting 100) into `usage_stats_hourly`, and
/// leave its own newer snapshot in place.
#[tokio::test]
async fn detached_bookkeeping_chains_per_agent_across_turns() {
    let (_tmp, services, bus, agent_id, workspace_id) = setup().await;
    let (conn, mut note_rx, _agent) = connect_with_prompt_result(
        prompt_updates(),
        json!({
            "stopReason": "end_turn",
            "usage": {
                "totalTokens": 100,
                "inputTokens": 100,
                "outputTokens": 0,
                "cachedReadTokens": 0,
                "cachedWriteTokens": 0
            }
        }),
    );

    // Seed the per-agent chain with a slow "previous turn" bookkeeping task
    // that persists the earlier cumulative snapshot late.
    let store = bus.store().clone();
    let prev_agent = agent_id.clone();
    let prev_ws = workspace_id.clone();
    let prev = tokio::spawn(async move {
        tokio::time::sleep(Duration::from_millis(300)).await;
        let snapshot = intent_core::TokenUsageTotals {
            input_tokens: 70,
            ..Default::default()
        };
        store
            .set_agent_session_token_usage(&prev_ws, &prev_agent, &snapshot)
            .await
            .expect("late snapshot persists");
    });
    services
        .turn_bookkeeping
        .lock()
        .unwrap()
        .insert(agent_id.clone(), prev);

    services
        .run_prompt_turn(
            &conn,
            &mut note_rx,
            &agent_id,
            &workspace_id,
            ACP_SID,
            vec![text_block("hi")],
            None,
        )
        .await
        .expect("turn completes");

    // Await the chained bookkeeping task the turn registered.
    let handle = services
        .turn_bookkeeping
        .lock()
        .unwrap()
        .remove(&agent_id)
        .expect("turn registered a bookkeeping task");
    handle.await.expect("bookkeeping task completes");

    // The turn's newer snapshot survives — the late predecessor did not win.
    let (_model, _resolved, _provider, snapshot) = bus
        .store()
        .get_agent_session_token_usage(&workspace_id, &agent_id)
        .await
        .expect("read snapshot");
    assert_eq!(snapshot.expect("snapshot persisted").input_tokens, 100);

    // The stats delta was computed against the predecessor's snapshot
    // (100 - 70 = 30), not the pre-chain state (a double-counting 100).
    let rows = bus
        .store()
        .list_usage_stats_hourly()
        .await
        .expect("stats rows");
    let input: u64 = rows.iter().map(|r| r.input_tokens).sum();
    assert_eq!(input, 30);
}

/// A `session/new` result canned from a live claude-agent-acp@0.60.0 session:
/// the model select's `currentValue` is the `default` placeholder whose
/// option entry carries the real family in its description.
fn claude_code_session_result() -> Value {
    json!({
        "sessionId": ACP_SID,
        "modes": { "currentModeId": "acceptEdits", "availableModes": [] },
        "configOptions": [
            { "id": "mode", "name": "Mode", "category": "mode", "type": "select",
              "currentValue": "acceptEdits",
              "options": [ { "value": "auto", "name": "Auto" },
                           { "value": "acceptEdits", "name": "Accept Edits" } ] },
            { "id": "model", "name": "Model", "description": "AI model to use",
              "category": "model", "type": "select", "currentValue": "default",
              "options": [
                { "value": "default", "name": "Default (recommended)",
                  "description": "Opus 4.8 with 1M context · Best for everyday, complex tasks" },
                { "value": "opus[1m]", "name": "Opus",
                  "description": "Opus 4.8 with 1M context · Best for everyday, complex tasks" },
                { "value": "claude-fable-5[1m]", "name": "Fable",
                  "description": "Fable 5 with 1M context · Powerful model for complex work" },
                { "value": "sonnet", "name": "Sonnet",
                  "description": "Sonnet 5 · Efficient for routine tasks" }
              ] }
        ]
    })
}

/// D13: opening a session with a placeholder stored model resolves the
/// effective model from the `session/new` response's
/// `configOptions[id="model"]` (currentValue `"default"` → its option's
/// description "Opus 4.8 with 1M context · …" → "Opus 4.8") and persists it
/// compound (`claude-code:Opus 4.8`) on `agent_session.model`.
#[tokio::test]
async fn open_session_resolves_and_persists_effective_model() {
    let (_tmp, services, bus, agent_id, ws) = setup().await;
    let mut session = new_session(&agent_id, &ws);
    session.id = AgentId::from("agent-d13");
    session.model = Some("claude-code:default".to_string());
    bus.store()
        .insert_agent_session(&session)
        .await
        .expect("insert");
    let (conn, _rx, _agent) = connect_with_session_result(claude_code_session_result());
    let opened = services
        .open_acp_session(&conn, &session.id, "/tmp/ws", Vec::new())
        .await
        .expect("open session");
    assert_eq!(opened.effective_model.as_deref(), Some("Opus 4.8"));
    let stored = bus.store().get_agent_session(&session.id).await.unwrap();
    assert_eq!(
        stored.model.as_deref(),
        Some("claude-code:Opus 4.8"),
        "placeholder model replaced by the resolved effective model (compound)"
    );
}

/// D13: an explicitly selected (non-placeholder) stored model is NEVER
/// overwritten by the session-open resolution. D14: its display identity IS
/// resolved from the same option list into the separate `resolved_model`
/// column (raw id `sonnet` → option description "Sonnet 5 · …" → "Sonnet 5").
#[tokio::test]
async fn open_session_never_overwrites_explicit_model() {
    let (_tmp, services, bus, agent_id, ws) = setup().await;
    let mut session = new_session(&agent_id, &ws);
    session.id = AgentId::from("agent-d13-explicit");
    session.model = Some("claude-code:sonnet".to_string());
    bus.store()
        .insert_agent_session(&session)
        .await
        .expect("insert");
    let (conn, _rx, _agent) = connect_with_session_result(claude_code_session_result());
    let opened = services
        .open_acp_session(&conn, &session.id, "/tmp/ws", Vec::new())
        .await
        .expect("open session");
    assert!(opened.effective_model.is_none());
    let stored = bus.store().get_agent_session(&session.id).await.unwrap();
    assert_eq!(
        stored.model.as_deref(),
        Some("claude-code:sonnet"),
        "explicit model untouched"
    );
    let (_, resolved, _, _) = bus
        .store()
        .get_agent_session_token_usage(&ws, &session.id)
        .await
        .expect("read resolved model");
    assert_eq!(
        resolved.as_deref(),
        Some("Sonnet 5"),
        "explicit pick's display identity persisted separately (D14)"
    );
}

/// D14: a bracketed explicit pick (`claude-code:claude-fable-5[1m]`) resolves
/// its display identity ("Fable 5") from the matching option entry — the
/// version-less name "Fable" is skipped for the version-bearing description —
/// while the raw stored id keeps driving provider configuration.
#[tokio::test]
async fn open_session_resolves_explicit_bracketed_pick_display_model() {
    let (_tmp, services, bus, agent_id, ws) = setup().await;
    let mut session = new_session(&agent_id, &ws);
    session.id = AgentId::from("agent-d14-fable");
    session.model = Some("claude-code:claude-fable-5[1m]".to_string());
    bus.store()
        .insert_agent_session(&session)
        .await
        .expect("insert");
    let (conn, _rx, _agent) = connect_with_session_result(claude_code_session_result());
    let opened = services
        .open_acp_session(&conn, &session.id, "/tmp/ws", Vec::new())
        .await
        .expect("open session");
    assert!(opened.effective_model.is_none(), "D13 branch not taken");
    let stored = bus.store().get_agent_session(&session.id).await.unwrap();
    assert_eq!(
        stored.model.as_deref(),
        Some("claude-code:claude-fable-5[1m]"),
        "raw explicit id untouched — still drives provider configuration"
    );
    let (_, resolved, _, _) = bus
        .store()
        .get_agent_session_token_usage(&ws, &session.id)
        .await
        .expect("read resolved model");
    assert_eq!(resolved.as_deref(), Some("Fable 5"));
}

/// D14: an explicit id with no matching option entry persists no resolution —
/// stats fall back to normalizing the raw id. A PREVIOUS resolution is
/// overwritten (cleared), not orphaned: a stale display name from an older
/// option list must not keep mis-attributing stats.
#[tokio::test]
async fn open_session_unmatched_explicit_pick_clears_previous_resolution() {
    let (_tmp, services, bus, agent_id, ws) = setup().await;
    let mut session = new_session(&agent_id, &ws);
    session.id = AgentId::from("agent-d14-unmatched");
    session.model = Some("claude-code:claude-haiku-4-5".to_string());
    bus.store()
        .insert_agent_session(&session)
        .await
        .expect("insert");
    // A resolution persisted by an earlier open against an older option list.
    let landed = bus
        .store()
        .set_agent_session_resolved_model(
            &ws,
            &session.id,
            "claude-code:claude-haiku-4-5",
            Some("Haiku 4.5"),
        )
        .await
        .expect("seed stale resolution");
    assert!(landed);
    let (conn, _rx, _agent) = connect_with_session_result(claude_code_session_result());
    services
        .open_acp_session(&conn, &session.id, "/tmp/ws", Vec::new())
        .await
        .expect("open session");
    let (model, resolved, _, _) = bus
        .store()
        .get_agent_session_token_usage(&ws, &session.id)
        .await
        .expect("read resolved model");
    assert_eq!(model.as_deref(), Some("claude-code:claude-haiku-4-5"));
    assert_eq!(resolved, None, "stale resolution overwritten by None");
}

/// D13: a NULL stored model resolves too, persisting the compound id with
/// the session's resolved provider so `resolve_provider_id` keeps working.
#[tokio::test]
async fn open_session_resolves_effective_model_for_null_model() {
    let (_tmp, services, bus, agent_id, ws) = setup().await;
    let mut session = new_session(&agent_id, &ws);
    session.id = AgentId::from("agent-d13-null");
    session.provider = Some("claude-code".to_string());
    bus.store()
        .insert_agent_session(&session)
        .await
        .expect("insert");
    let (conn, _rx, _agent) = connect_with_session_result(claude_code_session_result());
    let opened = services
        .open_acp_session(&conn, &session.id, "/tmp/ws", Vec::new())
        .await
        .expect("open session");
    assert_eq!(opened.effective_model.as_deref(), Some("Opus 4.8"));
    let stored = bus.store().get_agent_session(&session.id).await.unwrap();
    assert_eq!(stored.model.as_deref(), Some("claude-code:Opus 4.8"));
}

/// D13: a response without a resolvable model select (e.g. the plain mock's
/// bare `{ sessionId }`) leaves the placeholder model untouched.
#[tokio::test]
async fn open_session_without_config_options_keeps_placeholder() {
    let (_tmp, services, bus, agent_id, ws) = setup().await;
    let mut session = new_session(&agent_id, &ws);
    session.id = AgentId::from("agent-d13-none");
    session.model = Some("claude-code:default".to_string());
    bus.store()
        .insert_agent_session(&session)
        .await
        .expect("insert");
    let (conn, _rx, _agent) = connect();
    let opened = services
        .open_acp_session(&conn, &session.id, "/tmp/ws", Vec::new())
        .await
        .expect("open session");
    assert!(opened.effective_model.is_none());
    let stored = bus.store().get_agent_session(&session.id).await.unwrap();
    assert_eq!(stored.model.as_deref(), Some("claude-code:default"));
}

/// D13: `session/load` (resume) resolves the effective model the same way as
/// `session/new`.
#[tokio::test]
async fn resume_session_resolves_and_persists_effective_model() {
    let (_tmp, services, bus, agent_id, ws) = setup().await;
    let mut session = new_session(&agent_id, &ws);
    session.id = AgentId::from("agent-d13-resume");
    session.model = Some("claude-code:default".to_string());
    session.acp_session_id = Some(ACP_SID.to_string());
    bus.store()
        .insert_agent_session(&session)
        .await
        .expect("insert");
    let (conn, _rx, _agent) = connect_with_session_result(claude_code_session_result());
    let opened = services
        .resume_acp_session(&conn, &init_caps(true), &session.id, "/tmp/ws", Vec::new())
        .await
        .expect("resume")
        .expect("resume yields opened session");
    assert_eq!(opened.effective_model.as_deref(), Some("Opus 4.8"));
    let stored = bus.store().get_agent_session(&session.id).await.unwrap();
    assert_eq!(stored.model.as_deref(), Some("claude-code:Opus 4.8"));
}

/// `agent:stream:activity` leading-edge throttle (PROTOCOL §7): the first
/// activity of a turn emits immediately, activity inside the 1s window is
/// suppressed, the window elapsing re-opens the gate, and clearing the
/// live-turn slot (stream end/failure/abort) resets the state so the next
/// turn's first activity is immediate again.
#[tokio::test]
async fn activity_throttle_leading_edge_window_and_reset() {
    let (_tmp, services, _bus, agent_id, _ws) = setup().await;
    let other = AgentId::from("agent-2");

    // No live-turn slot open → nothing to signal for.
    assert!(
        !services.should_emit_activity(&agent_id),
        "no slot → no emission"
    );

    // Leading edge: the turn's first activity emits immediately…
    services.set_live_turn(&agent_id, "m1", Vec::new());
    assert!(
        services.should_emit_activity(&agent_id),
        "first activity of a turn emits immediately"
    );
    // …and activity inside the window is suppressed.
    assert!(
        !services.should_emit_activity(&agent_id),
        "second activity within 1s is suppressed"
    );
    assert!(
        !services.should_emit_activity(&agent_id),
        "still suppressed within the window"
    );

    // Per-agent state: another agent's turn throttles independently.
    services.set_live_turn(&other, "m2", Vec::new());
    assert!(
        services.should_emit_activity(&other),
        "another agent's first activity is unaffected"
    );

    // The window elapsing re-opens the gate.
    tokio::time::sleep(super::ACTIVITY_THROTTLE + Duration::from_millis(50)).await;
    assert!(
        services.should_emit_activity(&agent_id),
        "activity after the window emits again"
    );
    assert!(
        !services.should_emit_activity(&agent_id),
        "the re-emission re-arms the window"
    );

    // Turn end/failure clears the slot → the next turn's first activity is
    // immediate again (no leftover window from the previous turn).
    services.clear_live_turn(&agent_id);
    assert!(
        !services.should_emit_activity(&agent_id),
        "no slot after clear"
    );
    services.set_live_turn(&agent_id, "m3", Vec::new());
    assert!(
        services.should_emit_activity(&agent_id),
        "throttle state reset with the slot: next turn leads immediately"
    );
}
