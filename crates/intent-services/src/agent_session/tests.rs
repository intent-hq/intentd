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

/// A tool-ONLY prompt turn: three distinct tool calls and not a single
/// assistant text chunk — the shape an implementor sub-agent spends most of a
/// turn in (monorepo#1414).
fn prompt_updates_tools_only() -> Vec<String> {
    let tool = |id: &str, title: &str, status: &str| {
        json!({
            "jsonrpc": "2.0",
            "method": "session/update",
            "params": {
                "sessionId": ACP_SID,
                "update": { "sessionUpdate": "tool_call", "toolCallId": id,
                    "title": title, "kind": "execute", "status": status,
                    "rawInput": { "cmd": "x" } }
            }
        })
        .to_string()
    };
    vec![
        tool("t1", "bash: cargo test", "in_progress"),
        tool("t2", "view: src/lib.rs", "in_progress"),
        tool("t3", "bash: cargo fmt", "in_progress"),
    ]
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

/// [`connect_with`] whose mock HOLDS the `session/prompt` response until the
/// caller fires the returned release sender — freezing the turn mid-flight so
/// a test can act in the window where the live-turn slot is open (e.g. pin it
/// the way the teardown paths do) before the real turn end runs.
fn connect_gated_prompt(
    updates: Vec<String>,
) -> (
    Connection,
    mpsc::UnboundedReceiver<IncomingNotification>,
    JoinHandle<()>,
    tokio::sync::oneshot::Sender<()>,
) {
    let (release_tx, release_rx) = tokio::sync::oneshot::channel();
    let (c2a_client, c2a_agent) = tokio::io::duplex(16 * 1024);
    let (a2c_agent, a2c_client) = tokio::io::duplex(16 * 1024);
    let agent = spawn_gated_mock_agent(c2a_agent, a2c_agent, updates, release_rx);
    let (note_tx, note_rx) = mpsc::unbounded_channel();
    let hooks = ConnectionHooks {
        notifications: Some(note_tx),
        ..ConnectionHooks::default()
    };
    let conn = Connection::new(c2a_client, a2c_client, None, hooks);
    (conn, note_rx, agent, release_tx)
}

/// Mock agent for [`connect_gated_prompt`]: streams `updates` on
/// `session/prompt`, then awaits the release signal before answering
/// `end_turn`; other methods answer like the standard mock.
fn spawn_gated_mock_agent<R, W>(
    read: R,
    write: W,
    updates: Vec<String>,
    release: tokio::sync::oneshot::Receiver<()>,
) -> JoinHandle<()>
where
    R: AsyncRead + Unpin + Send + 'static,
    W: AsyncWrite + Unpin + Send + 'static,
{
    tokio::spawn(async move {
        let mut lines = BufReader::new(read).lines();
        let mut write = write;
        let mut release = Some(release);
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
                if let Some(release) = release.take() {
                    let _ = release.await;
                }
            }
            let result = match method {
                "initialize" => {
                    json!({ "protocolVersion": 1, "agentCapabilities": { "loadSession": true } })
                }
                "session/new" => json!({ "sessionId": ACP_SID }),
                "session/load" => json!({}),
                "session/prompt" => json!({ "stopReason": "end_turn" }),
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
        disk_usage: None,
        pending_delete_at: None,
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
        reasoning_effort: None,
        effort_levels: None,
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
        file_blocks: None,
        is_background: false,
        metadata: None,
        created_at: ts.clone(),
        updated_at: ts,
        sandbox_id: None,
        sandbox_path: None,
        sandbox_branch: None,
        stop_reason: None,
        stop_reason_timestamp: None,
        session_corrupted: false,
        pending_delete_at: None,
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
    // Normal `end_turn` endings stay metadata-free: no `finishReason` on the
    // terminal stream:end and no metadata on the persisted assistant row.
    assert!(
        end.data.get("finishReason").is_none(),
        "finishReason omitted on a normal end_turn ending"
    );
    assert!(
        messages[0].metadata.is_none(),
        "no row metadata on a normal end_turn ending"
    );
}

/// Abnormal turn endings (PROTOCOL §7): a turn resolving with a non-`end_turn`
/// stop reason (here `refusal`) persists `metadata.finishReason` on the turn's
/// assistant row — durable across reload — and the terminal `agent:stream:end`
/// carries the same `finishReason` so live clients render it without a
/// transcript re-fetch. The turn still completes normally (`agent:idle`, no
/// `agent:failed`).
#[tokio::test]
async fn abnormal_stop_reason_persists_finish_reason_on_row_and_stream_end() {
    let (_tmp, services, bus, agent_id, workspace_id) = setup().await;
    let (conn, mut note_rx, _agent) =
        connect_with_prompt_result(prompt_updates(), json!({ "stopReason": "refusal" }));
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
    assert_eq!(serde_json::to_value(stop).unwrap(), json!("refusal"));

    let mut events: Vec<Event> = Vec::new();
    while !events.iter().any(|e| e.event_type == "agent:idle") {
        let batch = timeout(Duration::from_secs(2), sub.recv())
            .await
            .expect("recv timed out")
            .expect("subscription open");
        events.extend(batch);
    }

    // The assistant row carries the durable finishReason tag.
    let messages = bus
        .store()
        .get_agent_messages(&agent_id, None)
        .await
        .expect("read messages");
    assert_eq!(messages.len(), 1);
    assert_eq!(
        messages[0].metadata.as_ref().expect("row metadata")["finishReason"],
        json!("refusal")
    );

    // The terminal stream:end carries the same finishReason live.
    let end = events
        .iter()
        .find(|e| e.event_type == "agent:stream:end")
        .expect("terminal stream:end");
    assert_eq!(end.data["finishReason"], json!("refusal"));
    assert_eq!(end.data["messageId"], json!(messages[0].id));

    // The turn is a completion, not a failure: `agent:idle` fires (its
    // `finishReason` is the existing lifecycle field) and `agent:failed`
    // never does.
    let idle = events
        .iter()
        .find(|e| e.event_type == "agent:idle")
        .expect("agent:idle");
    assert_eq!(idle.data["finishReason"], json!("refusal"));
    assert!(
        !events.iter().any(|e| e.event_type == "agent:failed"),
        "an abnormal stop reason is a completion, not a failure"
    );
}

/// A ZERO-OUTPUT abnormal ending (PROTOCOL §7.3): the prompt resolves with an
/// abnormal stop reason before emitting a single `session/update`. Because
/// `agent:idle` / `agent:stream:end` are ephemeral, the turn must still
/// persist an empty marker row (`contentBlocks: []`) tagged with
/// `metadata.finishReason` so the ending survives a reload — mirroring the
/// §7.2 pre-first-token interrupt marker row.
#[tokio::test]
async fn zero_output_abnormal_stop_reason_persists_empty_marker_row() {
    let (_tmp, services, bus, agent_id, workspace_id) = setup().await;
    let (conn, mut note_rx, _agent) =
        connect_with_prompt_result(Vec::new(), json!({ "stopReason": "refusal" }));
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
    assert_eq!(serde_json::to_value(stop).unwrap(), json!("refusal"));

    let mut events: Vec<Event> = Vec::new();
    while !events.iter().any(|e| e.event_type == "agent:idle") {
        let batch = timeout(Duration::from_secs(2), sub.recv())
            .await
            .expect("recv timed out")
            .expect("subscription open");
        events.extend(batch);
    }

    // The empty marker row is persisted: no content blocks, but the durable
    // finishReason tag.
    let messages = bus
        .store()
        .get_agent_messages(&agent_id, None)
        .await
        .expect("read messages");
    assert_eq!(messages.len(), 1);
    assert_eq!(messages[0].content, json!([]));
    assert_eq!(
        messages[0].metadata.as_ref().expect("row metadata")["finishReason"],
        json!("refusal")
    );

    // The terminal stream:end carries the finishReason and the marker row's id.
    let end = events
        .iter()
        .find(|e| e.event_type == "agent:stream:end")
        .expect("terminal stream:end");
    assert_eq!(end.data["finishReason"], json!("refusal"));
    assert_eq!(end.data["messageId"], json!(messages[0].id));
    assert!(
        !events.iter().any(|e| e.event_type == "agent:failed"),
        "an abnormal stop reason is a completion, not a failure"
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

/// §6.5 step-0 turn-end trigger: a turn whose persisted assistant tail
/// carries a question resource block promotes the workspace's derived
/// `displayStatus` to `needs_attention` (and emits the transition); once the
/// user's ANSWER (a row tagged `question_answers` for that message) clears the
/// pending-questions marker, the next turn's question-free tail lets the same
/// turn-end recompute retire it back to `idle`. A plain user row and a
/// question-free turn alone do NOT retire it — pendingness is persisted until
/// answered or dismissed.
#[tokio::test]
async fn question_tail_at_turn_end_raises_then_retires_needs_attention() {
    let (_tmp, services, bus, agent_id, workspace_id) = setup().await;
    // Seed the last-observed baseline (a first observation never emits).
    services
        .maybe_emit_display_status_changed(&workspace_id)
        .await;
    let mut sub = bus.subscribe(SubscriptionFilter {
        workspace_id: Some(workspace_id.0.clone()),
        event_types: vec!["workspace:displayStatus-changed".to_string()],
        ..Default::default()
    });

    // Turn 1: an AtTurnEnd question attachment lands as the trailing
    // resource block of the persisted assistant message (the shape
    // `ws.app.question.ask` produces).
    services.turn_attachments().register(
        &agent_id,
        intent_core::TurnAttachment {
            id: "tar-q1".to_string(),
            policy: intent_core::AttachmentPolicy::AtTurnEnd,
            mime_type: intent_acp::mcp_server::QUESTION_RESOURCE_MIME_TYPE.to_string(),
            uri: "question://q-1".to_string(),
            name: "Question".to_string(),
            text: "{\"questions\":[]}".to_string(),
        },
    );
    let (conn, mut note_rx, _agent) = connect_with(prompt_updates());
    services
        .run_prompt_turn(
            &conn,
            &mut note_rx,
            &agent_id,
            &workspace_id,
            ACP_SID,
            vec![text_block("ask")],
            None,
        )
        .await
        .expect("turn 1 completes");
    let batch = timeout(Duration::from_secs(2), sub.recv())
        .await
        .expect("raise event delivered")
        .expect("subscription open");
    assert_eq!(batch.len(), 1);
    assert_eq!(
        batch[0].data,
        json!({ "workspaceId": workspace_id.0, "displayStatus": "needs_attention" })
    );

    // The user's ANSWER resolves the questions (appended via the op so the
    // pending-questions marker clears — the send paths carry the same
    // resolution, exercised elsewhere), then turn 2 persists a question-free
    // tail: the turn-end recompute retires the hold and emits the demotion.
    let asked_id = bus
        .store()
        .get_agent_messages(&agent_id, None)
        .await
        .expect("messages")
        .last()
        .expect("assistant row")
        .id
        .clone();
    services
        .agent_append_message_op(
            agent_id.clone(),
            "user".to_string(),
            json!([{ "type": "text", "text": "answer" }]),
            Some(json!({
                "type": "question_answers",
                "answeredQuestionsMessageId": asked_id,
            })),
        )
        .await
        .expect("append answer");
    let (conn, mut note_rx, _agent) = connect_with(prompt_updates());
    services
        .run_prompt_turn(
            &conn,
            &mut note_rx,
            &agent_id,
            &workspace_id,
            ACP_SID,
            vec![text_block("continue")],
            None,
        )
        .await
        .expect("turn 2 completes");
    let batch = timeout(Duration::from_secs(2), sub.recv())
        .await
        .expect("retire event delivered")
        .expect("subscription open");
    assert_eq!(batch.len(), 1);
    assert_eq!(
        batch[0].data,
        json!({ "workspaceId": workspace_id.0, "displayStatus": "idle" })
    );
}

/// Stored-on-write pending-questions marker (PROTOCOL §5.5, question hold):
/// the turn-end persist writes the marker under the turn's message id when the
/// assistant tail bears question blocks, and a subsequent question-FREE turn
/// leaves it in place (pendingness survives the agent's own later turns).
#[tokio::test]
async fn turn_end_writes_pending_marker_and_question_free_turn_keeps_it() {
    let (_tmp, services, bus, agent_id, workspace_id) = setup().await;
    services.turn_attachments().register(
        &agent_id,
        intent_core::TurnAttachment {
            id: "tar-q1".to_string(),
            policy: intent_core::AttachmentPolicy::AtTurnEnd,
            mime_type: intent_acp::mcp_server::QUESTION_RESOURCE_MIME_TYPE.to_string(),
            uri: "question://q-1".to_string(),
            name: "Question".to_string(),
            text: "{\"questions\":[]}".to_string(),
        },
    );
    let (conn, mut note_rx, _agent) = connect_with(prompt_updates());
    services
        .run_prompt_turn(
            &conn,
            &mut note_rx,
            &agent_id,
            &workspace_id,
            ACP_SID,
            vec![text_block("ask")],
            None,
        )
        .await
        .expect("turn 1 completes");

    let asked_id = bus
        .store()
        .get_agent_messages(&agent_id, None)
        .await
        .expect("messages")
        .last()
        .expect("assistant row")
        .id
        .clone();
    let session = bus
        .store()
        .get_agent_session(&agent_id)
        .await
        .expect("session");
    assert_eq!(
        session.pending_questions_message_id(),
        Some(asked_id.as_str()),
        "turn end persists the pending-questions marker"
    );
    assert!(services.question_hold_active(&agent_id).await);

    // A question-free turn must NOT clear the marker.
    let (conn, mut note_rx, _agent) = connect_with(prompt_updates());
    services
        .run_prompt_turn(
            &conn,
            &mut note_rx,
            &agent_id,
            &workspace_id,
            ACP_SID,
            vec![text_block("continue")],
            None,
        )
        .await
        .expect("turn 2 completes");
    let session = bus
        .store()
        .get_agent_session(&agent_id)
        .await
        .expect("session");
    assert_eq!(
        session.pending_questions_message_id(),
        Some(asked_id.as_str()),
        "a question-free turn end must not clear the marker"
    );
    assert!(
        services.question_hold_active(&agent_id).await,
        "hold survives the agent's later turn"
    );
}

/// Question resource content-block array — the tail shape
/// `ws.app.question.ask` persists — for the monorepo#1266 regression tests
/// over the transcript-mutation ops below.
fn question_blocks() -> Value {
    json!([{
        "type": "resource",
        "resource": {
            "uri": "intent-question://q-1266",
            "mimeType": intent_acp::mcp_server::QUESTION_RESOURCE_MIME_TYPE,
            "text": "{\"questions\":[]}"
        }
    }])
}

/// The `workspace:displayStatus-changed`-only subscription the monorepo#1266
/// regression tests below assert against.
fn display_status_sub(bus: &EventBus, workspace_id: &WorkspaceId) -> crate::events::Subscription {
    bus.subscribe(SubscriptionFilter {
        workspace_id: Some(workspace_id.0.clone()),
        event_types: vec!["workspace:displayStatus-changed".to_string()],
        ..Default::default()
    })
}

/// monorepo#1266 regression (retire): the ANSWER row appended via
/// `agent.appendMessage` — tagged `question_answers` for the marked question
/// message — resolves the pending Q&A, so the op's own recompute must retire
/// the workspace's `needs_attention` displayStatus rather than leave it stale
/// until the next trigger or snapshot. A PLAIN user row does not: pendingness
/// now survives it, so the status stays `needs_attention` and nothing emits.
#[tokio::test]
async fn append_message_op_answer_row_retires_needs_attention() {
    let (_tmp, services, bus, agent_id, workspace_id) = setup().await;
    // Seed a question-bearing assistant tail through the op so the
    // pending-questions marker is armed, then the last-observed baseline
    // (needs_attention; a first observation never emits).
    let asked = services
        .agent_append_message_op(
            agent_id.clone(),
            "assistant".to_string(),
            question_blocks(),
            None,
        )
        .await
        .expect("append question tail");
    let asked_id = asked["message"]["id"]
        .as_str()
        .expect("question row id")
        .to_string();
    services
        .maybe_emit_display_status_changed(&workspace_id)
        .await;
    let mut sub = display_status_sub(&bus, &workspace_id);

    // A plain user row leaves the Q&A pending — no transition, no emit.
    services
        .agent_append_message_op(
            agent_id.clone(),
            "user".to_string(),
            json!([{ "type": "text", "text": "unrelated aside" }]),
            None,
        )
        .await
        .expect("plain appendMessage succeeds");
    assert!(
        services.question_hold_active(&agent_id).await,
        "a plain user row must not resolve the pending Q&A"
    );
    assert!(
        timeout(Duration::from_millis(300), sub.recv())
            .await
            .is_err(),
        "no displayStatus transition for a plain user row"
    );

    services
        .agent_append_message_op(
            agent_id.clone(),
            "user".to_string(),
            json!([{ "type": "text", "text": "answer" }]),
            Some(json!({
                "type": "question_answers",
                "answeredQuestionsMessageId": asked_id,
            })),
        )
        .await
        .expect("appendMessage succeeds");

    let batch = timeout(Duration::from_secs(2), sub.recv())
        .await
        .expect("retire event delivered")
        .expect("subscription open");
    assert_eq!(batch.len(), 1);
    assert_eq!(
        batch[0].data,
        json!({ "workspaceId": workspace_id.0, "displayStatus": "idle" })
    );
}

/// monorepo#1266 regression (raise): an assistant row with a trailing
/// question resource block appended via `agent.appendMessage` activates the
/// question hold, so the op's own recompute must promote the workspace's
/// displayStatus to `needs_attention` and emit the transition.
#[tokio::test]
async fn append_message_op_question_row_raises_needs_attention() {
    let (_tmp, services, bus, agent_id, workspace_id) = setup().await;
    // Seed the last-observed baseline (idle; a first observation never emits).
    services
        .maybe_emit_display_status_changed(&workspace_id)
        .await;
    let mut sub = display_status_sub(&bus, &workspace_id);

    services
        .agent_append_message_op(
            agent_id.clone(),
            "assistant".to_string(),
            question_blocks(),
            None,
        )
        .await
        .expect("appendMessage succeeds");

    let batch = timeout(Duration::from_secs(2), sub.recv())
        .await
        .expect("raise event delivered")
        .expect("subscription open");
    assert_eq!(batch.len(), 1);
    assert_eq!(
        batch[0].data,
        json!({ "workspaceId": workspace_id.0, "displayStatus": "needs_attention" })
    );
}

/// monorepo#1266 regression: `agent.replaceMessages` swaps the whole
/// transcript, which can move the question-hold derivation in either
/// direction — a swap whose question row is answered retires
/// `needs_attention`, a swap ending on an unanswered question-bearing
/// assistant row raises it again. Both flips must emit. The swap re-mints row
/// ids, so the pending-questions marker is re-derived from the new transcript
/// rather than left dangling.
#[tokio::test]
async fn replace_messages_op_moves_needs_attention_both_ways() {
    let (_tmp, services, bus, agent_id, workspace_id) = setup().await;
    // Seed a question-bearing tail, then the baseline (needs_attention).
    bus.store()
        .append_agent_message(&agent_id, "assistant", &question_blocks(), &now_iso())
        .await
        .expect("append question tail");
    services
        .maybe_emit_display_status_changed(&workspace_id)
        .await;
    let mut sub = display_status_sub(&bus, &workspace_id);

    // Retire: the swapped transcript's question row is answered.
    services
        .agent_replace_messages_op(
            agent_id.clone(),
            json!([
                { "role": "assistant", "contentBlocks": question_blocks() },
                {
                    "role": "user",
                    "contentBlocks": [{ "type": "text", "text": "answer" }],
                    "metadata": {
                        "type": "question_answers",
                        "answeredQuestionsMessageId": "pre-swap-id",
                    },
                },
            ]),
        )
        .await
        .expect("replaceMessages succeeds");
    let batch = timeout(Duration::from_secs(2), sub.recv())
        .await
        .expect("retire event delivered")
        .expect("subscription open");
    assert_eq!(batch.len(), 1);
    assert_eq!(
        batch[0].data,
        json!({ "workspaceId": workspace_id.0, "displayStatus": "idle" })
    );

    // Raise: swap back to a transcript ending on a question-bearing row.
    services
        .agent_replace_messages_op(
            agent_id.clone(),
            json!([{ "role": "assistant", "contentBlocks": question_blocks() }]),
        )
        .await
        .expect("replaceMessages succeeds");
    let batch = timeout(Duration::from_secs(2), sub.recv())
        .await
        .expect("raise event delivered")
        .expect("subscription open");
    assert_eq!(batch.len(), 1);
    assert_eq!(
        batch[0].data,
        json!({ "workspaceId": workspace_id.0, "displayStatus": "needs_attention" })
    );
}

/// monorepo#1266 transition-only guard: an `agent.appendMessage` mutation
/// that does NOT move the derivation (a user row onto an already-hold-free
/// transcript) recomputes silently — no `workspace:displayStatus-changed`.
#[tokio::test]
async fn append_message_op_without_derivation_change_emits_nothing() {
    let (_tmp, services, bus, agent_id, workspace_id) = setup().await;
    // Seed the last-observed baseline (idle; a first observation never emits).
    services
        .maybe_emit_display_status_changed(&workspace_id)
        .await;
    let mut sub = display_status_sub(&bus, &workspace_id);

    services
        .agent_append_message_op(
            agent_id.clone(),
            "user".to_string(),
            json!([{ "type": "text", "text": "note to self" }]),
            None,
        )
        .await
        .expect("appendMessage succeeds");

    assert!(
        timeout(Duration::from_millis(750), sub.recv())
            .await
            .is_err(),
        "no displayStatus event for a derivation-preserving mutation"
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

/// Mock agent whose `session/prompt` streams `updates`, then resolves with a
/// JSON-RPC ERROR object (rather than a result) carrying `message` — used to
/// force a deterministic, classifier-shaped `AcpError::Rpc` (e.g. a transient
/// connection reset vs a terminal 4xx) for the sleep-resume enrollment path.
fn spawn_mock_agent_with_prompt_rpc_error<R, W>(
    read: R,
    write: W,
    updates: Vec<String>,
    error_message: String,
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
                let resp = json!({
                    "jsonrpc": "2.0",
                    "id": id,
                    "error": { "code": -32603, "message": error_message },
                });
                write
                    .write_all(format!("{resp}\n").as_bytes())
                    .await
                    .unwrap();
                write.flush().await.unwrap();
                continue;
            }
            let result = match method {
                "initialize" => {
                    json!({ "protocolVersion": 1, "agentCapabilities": { "loadSession": true } })
                }
                "session/new" => json!({ "sessionId": ACP_SID }),
                "session/load" => json!({}),
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

/// [`connect`] against a mock whose `session/prompt` fails with a JSON-RPC
/// error carrying `error_message` (after streaming `updates`).
fn connect_with_prompt_rpc_error(
    updates: Vec<String>,
    error_message: &str,
) -> (
    Connection,
    mpsc::UnboundedReceiver<IncomingNotification>,
    JoinHandle<()>,
) {
    let (c2a_client, c2a_agent) = tokio::io::duplex(16 * 1024);
    let (a2c_agent, a2c_client) = tokio::io::duplex(16 * 1024);
    let agent = spawn_mock_agent_with_prompt_rpc_error(
        c2a_agent,
        a2c_agent,
        updates,
        error_message.to_string(),
    );
    let (note_tx, note_rx) = mpsc::unbounded_channel();
    let hooks = ConnectionHooks {
        notifications: Some(note_tx),
        ..ConnectionHooks::default()
    };
    let conn = Connection::new(c2a_client, a2c_client, None, hooks);
    (conn, note_rx, agent)
}

/// Mock agent whose `session/prompt` resolves with a JSON-RPC ERROR object
/// carrying an explicit `code` (and message) — used to force a specific
/// classifier-shaped `AcpError::Rpc`, e.g. the benign `-32800`
/// request-cancelled code (monorepo#2050 streaming benign-cancel path).
fn spawn_mock_agent_with_prompt_rpc_error_code<R, W>(
    read: R,
    write: W,
    code: i64,
    error_message: String,
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
                let resp = json!({
                    "jsonrpc": "2.0",
                    "id": id,
                    "error": { "code": code, "message": error_message },
                });
                write
                    .write_all(format!("{resp}\n").as_bytes())
                    .await
                    .unwrap();
                write.flush().await.unwrap();
                continue;
            }
            let result = match method {
                "initialize" => {
                    json!({ "protocolVersion": 1, "agentCapabilities": { "loadSession": true } })
                }
                "session/new" => json!({ "sessionId": ACP_SID }),
                "session/load" => json!({}),
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

/// [`connect`] against a mock whose `session/prompt` fails with a JSON-RPC
/// error carrying an explicit `code` + `message`.
fn connect_with_prompt_rpc_error_code(
    code: i64,
    error_message: &str,
) -> (
    Connection,
    mpsc::UnboundedReceiver<IncomingNotification>,
    JoinHandle<()>,
) {
    let (c2a_client, c2a_agent) = tokio::io::duplex(16 * 1024);
    let (a2c_agent, a2c_client) = tokio::io::duplex(16 * 1024);
    let agent = spawn_mock_agent_with_prompt_rpc_error_code(
        c2a_agent,
        a2c_agent,
        code,
        error_message.to_string(),
    );
    let (note_tx, note_rx) = mpsc::unbounded_channel();
    let hooks = ConnectionHooks {
        notifications: Some(note_tx),
        ..ConnectionHooks::default()
    };
    let conn = Connection::new(c2a_client, a2c_client, None, hooks);
    (conn, note_rx, agent)
}

/// Durable-before-observable on the STREAMING terminal-failure path
/// (monorepo#2050): an ordinary mid-turn `session/prompt failed:` error is
/// emitted by `run_prompt_turn` itself (terminal `agent:stream:end` +
/// `agent:failed`). The `status = error` + `stop_reason` store write must land
/// BEFORE either event reaches the bus, so a client reading `agent.getSession`
/// upon observing `agent:failed` is guaranteed the persisted Error. Runs
/// `run_prompt_turn` on its own task and reads the store the moment
/// `agent:failed` arrives — with the persist ordered first the read
/// deterministically sees `error`. Analogous to the manager-side
/// `terminal_failure_persists_error_before_publishing_events`.
#[tokio::test]
async fn streaming_terminal_failure_persists_error_before_publishing_events() {
    let (_tmp, services, bus, agent_id, workspace_id) = setup().await;
    let (conn, note_rx, _agent) = connect_with_prompt_rpc_error(Vec::new(), "backend exploded");
    let mut sub = bus.subscribe(SubscriptionFilter::default());

    let handle = {
        let (services, agent_id, workspace_id) =
            (services.clone(), agent_id.clone(), workspace_id.clone());
        tokio::spawn(async move {
            let mut note_rx = note_rx;
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
                .expect_err("an ordinary prompt error fails the turn")
        })
    };

    // Read the store the moment agent:failed lands: the persisted Error must
    // already be visible.
    'observed: loop {
        let batch = timeout(Duration::from_secs(10), sub.recv())
            .await
            .expect("agent:failed within 10s")
            .expect("bus open");
        for event in batch {
            if event.event_type == "agent:failed" {
                break 'observed;
            }
        }
    }
    let stored = bus.store().get_agent_session(&agent_id).await.unwrap();
    assert_eq!(
        stored.status,
        AgentStatus::Error,
        "status is durably error when agent:failed is observable (monorepo#2050)"
    );
    assert!(
        stored
            .stop_reason
            .as_deref()
            .is_some_and(|r| r.contains("backend exploded")),
        "stop_reason persisted alongside the status: {:?}",
        stored.stop_reason
    );

    let err = handle.await.unwrap();
    assert!(
        matches!(&err, intent_core::Error::Internal(msg) if msg.starts_with("session/prompt failed:")),
        "ordinary terminal error keeps the wrapper: {err}"
    );
    // The persisted context was stashed for the turn worker to reuse (so the
    // failure streak / Error status are written exactly once, not twice).
    assert!(
        services.take_pending_terminal_error(&agent_id).is_some(),
        "streaming path stashed the persisted terminal error for the worker"
    );
}

/// A benign provider-resolved cancel (JSON-RPC `-32800` request-cancelled) on
/// the streaming path must NOT persist an Error status (monorepo#2050): it is
/// the expected outcome of a concurrent stop/cancel, classified benign by the
/// same predicate the worker uses. `run_prompt_turn` still emits its terminal
/// `agent:failed` (the worker suppresses the terminal-failure path), but no
/// Error is persisted and nothing is stashed for the worker.
#[tokio::test]
async fn streaming_benign_cancel_does_not_persist_error() {
    let (_tmp, services, _bus, agent_id, workspace_id) = setup().await;
    let (conn, mut note_rx, _agent) =
        connect_with_prompt_rpc_error_code(-32800, "request cancelled");

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
        .expect_err("a cancel still returns Err");
    assert!(
        crate::agent_manager::prompt_cancellation_error(&err),
        "the -32800 cancel classifies benign: {err}"
    );

    let stored = services.store.get_agent_session(&agent_id).await.unwrap();
    assert_ne!(
        stored.status,
        AgentStatus::Error,
        "a benign cancel must not park the session in Error (monorepo#2050)"
    );
    assert!(
        stored.stop_reason.is_none(),
        "no error stop_reason for a benign cancel: {:?}",
        stored.stop_reason
    );
    assert!(
        services.take_pending_terminal_error(&agent_id).is_none(),
        "nothing stashed for a benign cancel"
    );
}

/// Injectable [`SuspendOverlapQuery`](crate::SuspendOverlapQuery) for the
/// sleep-resume enrollment tests: reports a fixed overlap answer regardless of
/// the queried window.
struct FakeSuspend(Option<Duration>);

impl crate::agent_session::SuspendOverlapQuery for FakeSuspend {
    fn did_suspend_overlap(
        &self,
        _start: std::time::Instant,
        _end: std::time::Instant,
    ) -> Option<Duration> {
        self.0
    }
}

/// A `session/update` text chunk for the sleep-resume tests.
fn suspend_chunk(text: &str) -> String {
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
}

/// Task C happy path: a transient upstream disconnect whose active window
/// overlapped a detected host suspend is ENROLLED as interrupted, not surfaced
/// terminally. `run_prompt_turn` returns the suspend-interrupt marker error,
/// emits the interrupted terminal `agent:stream:end` (`stopReason:
/// "interrupted"`, `interruptReason: "system_suspend"`) and NO `agent:failed`,
/// persists the partial turn tagged with the interrupt reason, and writes an
/// `interrupted_agent` row for the wake orchestrator (Task D).
#[tokio::test]
async fn suspend_interrupt_enrolls_transient_failure_and_suppresses_terminal_failure() {
    let (_tmp, services, bus, agent_id, workspace_id) = setup().await;
    let services = services.with_suspend_tracker(std::sync::Arc::new(FakeSuspend(Some(
        Duration::from_secs(120),
    ))));
    // Stream a partial chunk, then fail with a connection-reset RPC error.
    let (conn, mut note_rx, _agent) =
        connect_with_prompt_rpc_error(vec![suspend_chunk("partial ")], "Connection reset by peer");
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
        .expect_err("a suspend-overlapping transient failure still returns Err");
    assert!(
        matches!(
            &err,
            intent_core::Error::Internal(msg)
                if msg.starts_with(crate::agent_session::PROMPT_SUSPEND_INTERRUPT_PREFIX)
        ),
        "sleep-induced failure carries the suspend-interrupt marker: {err}"
    );

    let mut events = Vec::new();
    while let Ok(Some(batch)) = timeout(Duration::from_millis(300), sub.recv()).await {
        events.extend(batch);
    }
    assert!(
        !events.iter().any(|e| e.event_type == "agent:failed"),
        "no agent:failed for a sleep-induced interruption"
    );
    let end = events
        .iter()
        .find(|e| e.event_type == "agent:stream:end")
        .expect("interrupted terminal stream:end emitted");
    assert_eq!(end.data["stopReason"], json!("interrupted"));
    assert_eq!(end.data["interruptReason"], json!("system_suspend"));

    // The partial turn persisted, tagged with the interrupt reason.
    let messages = bus
        .store()
        .get_agent_messages(&agent_id, None)
        .await
        .unwrap();
    assert_eq!(
        messages.len(),
        1,
        "partial turn persisted on suspend enroll"
    );
    assert_eq!(
        messages[0]
            .metadata
            .as_ref()
            .and_then(|m| m.get("interruptReason").and_then(Value::as_str)),
        Some("system_suspend"),
        "persisted row tagged suspend-induced"
    );

    // The interrupted_agent row is written for the wake orchestrator (Task D).
    let interrupted = bus.store().list_interrupted_agents().await.unwrap();
    assert!(
        interrupted.iter().any(|ia| ia.agent_id == agent_id),
        "interrupted_agent row enrolled for wake-resume"
    );
}

/// Task C boundary: the SAME transient failure with NO overlapping suspend
/// (the tracker reports `None`) is NOT enrolled — it keeps today's terminal
/// behavior (ordinary `session/prompt failed:` wrapper, `agent:failed` emitted,
/// no `interrupted_agent` row).
#[tokio::test]
async fn suspend_interrupt_awake_transient_failure_surfaces_terminally() {
    let (_tmp, services, bus, agent_id, workspace_id) = setup().await;
    let services = services.with_suspend_tracker(std::sync::Arc::new(FakeSuspend(None)));
    let (conn, mut note_rx, _agent) =
        connect_with_prompt_rpc_error(vec![suspend_chunk("partial ")], "Connection reset by peer");
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
        .expect_err("an awake-time transient failure fails the turn");
    assert!(
        matches!(
            &err,
            intent_core::Error::Internal(msg) if msg.starts_with("session/prompt failed:")
        ),
        "awake-time failure keeps the ordinary wrapper: {err}"
    );

    let mut events = Vec::new();
    while let Ok(Some(batch)) = timeout(Duration::from_millis(300), sub.recv()).await {
        events.extend(batch);
    }
    assert!(
        events.iter().any(|e| e.event_type == "agent:failed"),
        "awake-time failure surfaces agent:failed"
    );
    let interrupted = bus.store().list_interrupted_agents().await.unwrap();
    assert!(
        interrupted.is_empty(),
        "no interrupted_agent row for an awake-time failure"
    );
}

/// Task C boundary: a NON-transient error (a terminal 4xx) is NOT enrolled even
/// when a suspend overlapped — the classifier rejects it, so the turn surfaces
/// terminally with `agent:failed` and no `interrupted_agent` row.
#[tokio::test]
async fn suspend_interrupt_ignores_non_transient_error_during_suspend() {
    let (_tmp, services, bus, agent_id, workspace_id) = setup().await;
    let services = services.with_suspend_tracker(std::sync::Arc::new(FakeSuspend(Some(
        Duration::from_secs(120),
    ))));
    let (conn, mut note_rx, _agent) =
        connect_with_prompt_rpc_error(Vec::new(), "HTTP 401 Unauthorized");
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
        .expect_err("a terminal 4xx fails the turn");
    assert!(
        matches!(
            &err,
            intent_core::Error::Internal(msg) if msg.starts_with("session/prompt failed:")
        ),
        "a non-transient error keeps the ordinary wrapper even during suspend: {err}"
    );

    let mut events = Vec::new();
    while let Ok(Some(batch)) = timeout(Duration::from_millis(300), sub.recv()).await {
        events.extend(batch);
    }
    assert!(
        events.iter().any(|e| e.event_type == "agent:failed"),
        "a non-transient error surfaces agent:failed even during suspend"
    );
    let interrupted = bus.store().list_interrupted_agents().await.unwrap();
    assert!(
        interrupted.is_empty(),
        "no interrupted_agent row for a non-transient error"
    );
}

/// Task D end-to-end: a turn ENROLLED by Task C's classifier (a suspend-
/// overlapping transient disconnect via the real `run_prompt_turn` path) is
/// resumed by the wake-triggered sweep. The enrolled row is tagged
/// `system_suspend`, the sweep resumes exactly it, and its atomic claim leaves
/// the row resolved (no longer pending).
#[tokio::test]
async fn wake_resume_resumes_turn_enrolled_by_suspend_classifier() {
    let (_tmp, services, bus, agent_id, workspace_id) = setup().await;
    let services = services.with_suspend_tracker(std::sync::Arc::new(FakeSuspend(Some(
        Duration::from_secs(120),
    ))));

    // Task C: a transient disconnect overlapping a suspend enrolls the turn.
    let (conn, mut note_rx, _agent) =
        connect_with_prompt_rpc_error(vec![suspend_chunk("partial ")], "Connection reset by peer");
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
        .expect_err("suspend-overlapping transient failure returns the interrupt marker");

    // The enrolled row carries the system_suspend reason (only these auto-resume).
    let row = bus
        .store()
        .get_interrupted_agent(&agent_id)
        .await
        .unwrap()
        .expect("interrupted_agent row enrolled");
    assert_eq!(row.reason.as_deref(), Some("system_suspend"));

    // The mid-turn session had an open ACP session id (required to reload).
    bus.store()
        .set_acp_session_id(&workspace_id, &agent_id, ACP_SID)
        .await
        .expect("mark session resumable");

    // Simulated host wake: the sweep resumes the enrolled row.
    let resumed = services.resume_suspend_interrupted_agents().await;
    assert_eq!(resumed, 1, "the enrolled suspend turn is resumed on wake");
    assert!(
        bus.store()
            .get_interrupted_agent(&agent_id)
            .await
            .unwrap()
            .is_none(),
        "the enrolled row is claimed and resolved by the wake sweep"
    );
}

/// Finding 2 (late-enrollment self-heal): a row enrolled AFTER the one-shot wake
/// sweep must not stay stranded until the next host wake. Enrollment fires a
/// gated, debounced resume directly, so WITHOUT any wake broadcast (the test
/// never calls `resume_suspend_interrupted_agents`) the enrolled row still
/// resolves on its own. The `INTENTD_WAKE_RESUME_SELF_HEAL_MS` seam compresses
/// the debounce so the test need not wait the production window.
#[tokio::test]
async fn suspend_enrollment_self_heals_resume_without_wake_broadcast() {
    // Compress the self-heal debounce for the test (guard restores the env).
    let _env = EnvGuard::set_all(&[("INTENTD_WAKE_RESUME_SELF_HEAL_MS", "50")]);

    let (_tmp, services, bus, agent_id, workspace_id) = setup().await;
    let services = services.with_suspend_tracker(std::sync::Arc::new(FakeSuspend(Some(
        Duration::from_secs(120),
    ))));

    // The mid-turn session had an open ACP session id (required for the
    // self-heal sweep to consider the row resumable). Set it BEFORE the turn so
    // the debounced resume — which may fire moments after enrollment — sees it.
    bus.store()
        .set_acp_session_id(&workspace_id, &agent_id, ACP_SID)
        .await
        .expect("mark session resumable");

    // A transient disconnect overlapping a suspend enrolls the turn AND schedules
    // the self-heal resume.
    let (conn, mut note_rx, _agent) =
        connect_with_prompt_rpc_error(vec![suspend_chunk("partial ")], "Connection reset by peer");
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
        .expect_err("suspend-overlapping transient failure returns the interrupt marker");

    // The row is enrolled and (initially) pending — the wake broadcast has NOT run.
    let row = bus
        .store()
        .get_interrupted_agent(&agent_id)
        .await
        .unwrap()
        .expect("interrupted_agent row enrolled");
    assert_eq!(row.reason.as_deref(), Some("system_suspend"));

    // Poll: the enrollment-driven self-heal resumes the row on its own, with no
    // call to `resume_suspend_interrupted_agents` from the test.
    let mut resolved = false;
    for _ in 0..40 {
        if bus
            .store()
            .get_interrupted_agent(&agent_id)
            .await
            .unwrap()
            .is_none()
        {
            resolved = true;
            break;
        }
        tokio::time::sleep(Duration::from_millis(50)).await;
    }
    assert!(
        resolved,
        "the row enrolled after the sweep still resumes via the enrollment self-heal (no wake broadcast)"
    );
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
/// `session/update` variant with no canonical turn mapping (here a plan
/// update — same class as mode/commands) still reset the idle timer, so it
/// counts as intervening activity: the wrapped error carries the
/// streamed-output suffix even though nothing was applied to the transcript.
#[tokio::test]
async fn idle_timeout_after_unmapped_update_marks_streamed() {
    let _env = EnvGuard::set_all(&[("INTENTD_PROMPT_IDLE_TIMEOUT_MS", "100")]);
    let (_tmp, services, bus, agent_id, workspace_id) = setup().await;
    let plan = json!({
        "jsonrpc": "2.0",
        "method": "session/update",
        "params": {
            "sessionId": ACP_SID,
            "update": { "sessionUpdate": "plan", "entries": [] }
        }
    })
    .to_string();
    let (conn, mut note_rx, _agent) = connect_silent(vec![plan]);
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
/// to the separate `resolved_model` column — `agent_session.model` is NEVER
/// rewritten (monorepo#1534: a display name is not a selectable option id,
/// so persisting it on `model` made the FE flag it unavailable and fall
/// back, re-triggering the rewrite forever).
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
    services
        .open_acp_session(&conn, &session.id, "/tmp/ws", Vec::new())
        .await
        .expect("open session");
    let stored = bus.store().get_agent_session(&session.id).await.unwrap();
    assert_eq!(
        stored.model.as_deref(),
        Some("claude-code:default"),
        "placeholder model untouched — never rewritten to a display name"
    );
    let (_, resolved, _, _) = bus
        .store()
        .get_agent_session_token_usage(&ws, &session.id)
        .await
        .expect("read resolved model");
    assert_eq!(
        resolved.as_deref(),
        Some("Opus 4.8"),
        "effective display identity persisted to resolved_model (D13)"
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
    services
        .open_acp_session(&conn, &session.id, "/tmp/ws", Vec::new())
        .await
        .expect("open session");
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
    services
        .open_acp_session(&conn, &session.id, "/tmp/ws", Vec::new())
        .await
        .expect("open session");
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
            Some("claude-code:claude-haiku-4-5"),
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

/// D13: a NULL stored model resolves too — `model` stays NULL (provider
/// resolution keeps riding the `provider` field) and the display identity
/// lands in `resolved_model`.
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
    services
        .open_acp_session(&conn, &session.id, "/tmp/ws", Vec::new())
        .await
        .expect("open session");
    let stored = bus.store().get_agent_session(&session.id).await.unwrap();
    assert_eq!(stored.model, None, "NULL model stays NULL");
    let (_, resolved, _, _) = bus
        .store()
        .get_agent_session_token_usage(&ws, &session.id)
        .await
        .expect("read resolved model");
    assert_eq!(resolved.as_deref(), Some("Opus 4.8"));
}

/// D13: a response without a resolvable model select (e.g. the plain mock's
/// bare `{ sessionId }`) leaves the placeholder model untouched and persists
/// no resolution.
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
    services
        .open_acp_session(&conn, &session.id, "/tmp/ws", Vec::new())
        .await
        .expect("open session");
    let stored = bus.store().get_agent_session(&session.id).await.unwrap();
    assert_eq!(stored.model.as_deref(), Some("claude-code:default"));
    let (_, resolved, _, _) = bus
        .store()
        .get_agent_session_token_usage(&ws, &session.id)
        .await
        .expect("read resolved model");
    assert_eq!(resolved, None);
}

/// D13: a placeholder session whose new open resolves nothing clears a
/// previously persisted resolution (same anti-staleness contract as D14) —
/// a display name from an older option list must not keep mis-attributing
/// stats.
#[tokio::test]
async fn open_session_placeholder_unresolvable_clears_stale_resolution() {
    let (_tmp, services, bus, agent_id, ws) = setup().await;
    let mut session = new_session(&agent_id, &ws);
    session.id = AgentId::from("agent-d13-stale");
    session.model = Some("claude-code:default".to_string());
    bus.store()
        .insert_agent_session(&session)
        .await
        .expect("insert");
    let landed = bus
        .store()
        .set_agent_session_resolved_model(
            &ws,
            &session.id,
            Some("claude-code:default"),
            Some("Opus 4.8"),
        )
        .await
        .expect("seed stale resolution");
    assert!(landed);
    let (conn, _rx, _agent) = connect();
    services
        .open_acp_session(&conn, &session.id, "/tmp/ws", Vec::new())
        .await
        .expect("open session");
    let (model, resolved, _, _) = bus
        .store()
        .get_agent_session_token_usage(&ws, &session.id)
        .await
        .expect("read resolved model");
    assert_eq!(model.as_deref(), Some("claude-code:default"));
    assert_eq!(resolved, None, "stale resolution overwritten by None");
}

/// D13: `session/load` (resume) resolves the effective model the same way as
/// `session/new` — into `resolved_model`, never onto `model`.
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
    services
        .resume_acp_session(&conn, &init_caps(true), &session.id, "/tmp/ws", Vec::new())
        .await
        .expect("resume")
        .expect("resume yields opened session");
    let stored = bus.store().get_agent_session(&session.id).await.unwrap();
    assert_eq!(stored.model.as_deref(), Some("claude-code:default"));
    let (_, resolved, _, _) = bus
        .store()
        .get_agent_session_token_usage(&ws, &session.id)
        .await
        .expect("read resolved model");
    assert_eq!(resolved.as_deref(), Some("Opus 4.8"));
}

/// A `session/new` result whose `configOptions` carry a `thought_level`
/// select under an adapter-specific id (`effort`, as claude-agent-acp names
/// it) — the shape the generic reasoning-effort discovery keys on.
fn thought_level_session_result() -> Value {
    json!({
        "sessionId": ACP_SID,
        "configOptions": [
            { "id": "model", "name": "Model", "category": "model", "type": "select",
              "currentValue": "sonnet",
              "options": [ { "value": "sonnet", "name": "Sonnet" } ] },
            { "id": "effort", "name": "Effort", "category": "thought_level",
              "type": "select", "currentValue": "medium",
              "options": [ { "value": "low", "name": "Low" },
                           { "value": "medium", "name": "Medium" },
                           { "value": "high", "name": "High" } ] }
        ]
    })
}

/// PROTOCOL §5.5: the reasoning-effort selector is discovered by CATEGORY, so
/// the adapter's own config id (`effort` here, `reasoning_effort` for
/// codex-acp) is carried back for the later `session/set_config_option`,
/// together with its current value and accepted values.
#[tokio::test]
async fn open_session_discovers_thought_level_option() {
    let (_tmp, services, bus, agent_id, ws) = setup().await;
    let mut session = new_session(&agent_id, &ws);
    session.id = AgentId::from("agent-effort-discover");
    bus.store()
        .insert_agent_session(&session)
        .await
        .expect("insert");
    let (conn, _rx, _agent) = connect_with_session_result(thought_level_session_result());
    let opened = services
        .open_acp_session(&conn, &session.id, "/tmp/ws", Vec::new())
        .await
        .expect("open session");
    let tl = opened.thought_level.expect("thought_level discovered");
    assert_eq!(tl.config_id, "effort", "adapter's own config id is carried");
    assert_eq!(tl.current_value, "medium");
    assert_eq!(tl.values, vec!["low", "medium", "high"]);
}

/// A provider that advertises no `thought_level` option yields `None`, so the
/// session's `reasoningEffort` is silently ignored for it.
#[tokio::test]
async fn open_session_without_thought_level_option_yields_none() {
    let (_tmp, services, bus, agent_id, ws) = setup().await;
    let mut session = new_session(&agent_id, &ws);
    session.id = AgentId::from("agent-effort-absent");
    bus.store()
        .insert_agent_session(&session)
        .await
        .expect("insert");
    let (conn, _rx, _agent) = connect_with_session_result(claude_code_session_result());
    let opened = services
        .open_acp_session(&conn, &session.id, "/tmp/ws", Vec::new())
        .await
        .expect("open session");
    assert!(opened.thought_level.is_none());
}

/// `session/load` (resume) discovers the selector the same way as
/// `session/new` — a resumed session must honor `reasoningEffort` too.
#[tokio::test]
async fn resume_session_discovers_thought_level_option() {
    let (_tmp, services, bus, agent_id, ws) = setup().await;
    let mut session = new_session(&agent_id, &ws);
    session.id = AgentId::from("agent-effort-resume");
    session.acp_session_id = Some(ACP_SID.to_string());
    bus.store()
        .insert_agent_session(&session)
        .await
        .expect("insert");
    let (conn, _rx, _agent) = connect_with_session_result(thought_level_session_result());
    let opened = services
        .resume_acp_session(&conn, &init_caps(true), &session.id, "/tmp/ws", Vec::new())
        .await
        .expect("resume")
        .expect("resume yields opened session");
    assert_eq!(
        opened.thought_level.expect("discovered").config_id,
        "effort"
    );
}

/// A `session/new` result shaped like claude-agent-acp's: the `thought_level`
/// select lists a `default` sentinel alongside the real levels
/// (default/low/medium/high/max).
fn claude_shaped_thought_level_session_result() -> Value {
    json!({
        "sessionId": ACP_SID,
        "configOptions": [
            { "id": "effort", "name": "Effort", "category": "thought_level",
              "type": "select", "currentValue": "default",
              "options": [ { "value": "default", "name": "Default" },
                           { "value": "low", "name": "Low" },
                           { "value": "medium", "name": "Medium" },
                           { "value": "high", "name": "High" },
                           { "value": "max", "name": "Max" } ] }
        ]
    })
}

/// [`ThoughtLevelOption::surfaced_levels`] drops the case-insensitive
/// `"default"` sentinel and returns `None` (not `Some(empty)`) when nothing
/// remains.
#[test]
fn surfaced_levels_filters_default_sentinel() {
    let mut tl = super::ThoughtLevelOption {
        config_id: "effort".to_string(),
        initial_value: "Default".to_string(),
        current_value: "Default".to_string(),
        values: ["Default", "low", "medium", "high", "max"]
            .map(String::from)
            .to_vec(),
    };
    assert_eq!(
        tl.surfaced_levels().as_deref(),
        Some(
            ["low", "medium", "high", "max"]
                .map(String::from)
                .as_slice()
        )
    );
    tl.values = vec!["DEFAULT".to_string()];
    assert_eq!(tl.surfaced_levels(), None, "all-sentinel list yields None");
    tl.values = Vec::new();
    assert_eq!(tl.surfaced_levels(), None, "empty list yields None");
}

/// Regression (PROTOCOL §5.5, Option C): a claude-code-shaped `configOptions`
/// (thought_level select with default/low/medium/high/max) yields
/// `effortLevels: ["low","medium","high","max"]` on the wire — persisted by
/// `open_acp_session` itself, carried by the `AgentLite` projection, and
/// announced by ONE `agent:updated`; the identical re-discovery on the next
/// session open (a `resume_acp_session` here) emits nothing.
#[tokio::test]
async fn session_open_persists_effort_levels_and_emits_on_change_only() {
    let (_tmp, services, bus, agent_id, ws) = setup().await;
    let mut session = new_session(&agent_id, &ws);
    session.id = AgentId::from("agent-effort-persist");
    bus.store()
        .insert_agent_session(&session)
        .await
        .expect("insert");
    let mut sub = bus.subscribe(SubscriptionFilter::default());
    let (conn, _rx, _agent) =
        connect_with_session_result(claude_shaped_thought_level_session_result());
    services
        .open_acp_session(&conn, &session.id, "/tmp/ws", Vec::new())
        .await
        .expect("open session");

    let expected = ["low", "medium", "high", "max"].map(String::from).to_vec();
    let stored = bus.store().get_agent_session(&session.id).await.unwrap();
    assert_eq!(stored.effort_levels.as_deref(), Some(expected.as_slice()));
    // The wire projection carries the camelCase field.
    let lite = intent_core::AgentLite::from_session(stored, 0, None, None, None, None, None);
    assert_eq!(
        serde_json::to_value(&lite).unwrap()["effortLevels"],
        json!(["low", "medium", "high", "max"])
    );
    // One agent:updated announced the change (the open also emits
    // session-create status hints, so scan batches until it shows up).
    let updated = 'found: {
        for _ in 0..5 {
            let batch = timeout(Duration::from_secs(2), sub.recv())
                .await
                .expect("recv timed out")
                .expect("subscription open");
            if let Some(e) = batch.iter().find(|e| e.event_type == "agent:updated") {
                break 'found e.clone();
            }
        }
        panic!("agent:updated on the wire");
    };
    assert_eq!(updated.data["agentId"], json!(session.id.0));
    assert_eq!(
        updated.data["effortLevels"],
        json!(["low", "medium", "high", "max"])
    );

    // The next session open discovers the identical set — a no-op: no event.
    let (conn2, _rx2, _agent2) =
        connect_with_session_result(claude_shaped_thought_level_session_result());
    services
        .resume_acp_session(&conn2, &init_caps(true), &session.id, "/tmp/ws", Vec::new())
        .await
        .expect("resume")
        .expect("resume yields opened session");
    services
        .publish_agent_event(&ws, &session.id, "test:flush", json!({}))
        .await;
    let mut saw_flush = false;
    while !saw_flush {
        let batch = timeout(Duration::from_secs(2), sub.recv())
            .await
            .expect("recv timed out")
            .expect("subscription open");
        assert!(
            !batch.iter().any(|e| e.event_type == "agent:updated"),
            "unchanged set must not re-emit agent:updated"
        );
        saw_flush = batch.iter().any(|e| e.event_type == "test:flush");
    }
}

/// A provider that advertises no `thought_level` selector CLEARS a previously
/// persisted set (a provider switch must not leave the old provider's levels
/// on the wire), and the `agent:updated` announcing it carries
/// `effortLevels: null`.
#[tokio::test]
async fn session_open_without_selector_clears_persisted_effort_levels() {
    let (_tmp, services, bus, agent_id, ws) = setup().await;
    let mut session = new_session(&agent_id, &ws);
    session.id = AgentId::from("agent-effort-clear");
    session.effort_levels = Some(vec!["low".to_string(), "high".to_string()]);
    bus.store()
        .insert_agent_session(&session)
        .await
        .expect("insert");

    let mut sub = bus.subscribe(SubscriptionFilter::default());
    // The open discovers no `thought_level` selector (bare `{ sessionId }`
    // result) and clears the stale set itself.
    let (conn, _rx, _agent) = connect_with_session_result(json!({ "sessionId": ACP_SID }));
    services
        .open_acp_session(&conn, &session.id, "/tmp/ws", Vec::new())
        .await
        .expect("open session");
    let stored = bus.store().get_agent_session(&session.id).await.unwrap();
    assert_eq!(stored.effort_levels, None, "cleared on none advertised");
    // The wire projection omits the field entirely.
    let lite = intent_core::AgentLite::from_session(stored, 0, None, None, None, None, None);
    assert!(
        serde_json::to_value(&lite)
            .unwrap()
            .get("effortLevels")
            .is_none(),
        "cleared levels must be omitted from the wire"
    );
    let updated = 'found: {
        for _ in 0..5 {
            let batch = timeout(Duration::from_secs(2), sub.recv())
                .await
                .expect("recv timed out")
                .expect("subscription open");
            if let Some(e) = batch.iter().find(|e| e.event_type == "agent:updated") {
                break 'found e.clone();
            }
        }
        panic!("agent:updated on the wire");
    };
    assert_eq!(updated.data["effortLevels"], Value::Null);
}

/// Regression (PR #992 review): a `recreate_acp_session` that LOSES its CAS
/// (a concurrent recreate already swapped the stored id) must not touch the
/// persisted `effort_levels` — its `thought_level: None` means "CAS lost /
/// unknown", not "the provider advertised no selector". The loser leaves the
/// winner's set intact and emits no `agent:updated`.
#[tokio::test]
async fn cas_losing_recreate_leaves_effort_levels_untouched() {
    let (_tmp, services, bus, agent_id, ws) = setup().await;
    // The canonical session id was already swapped by a concurrent recreate
    // (the CAS winner), which also persisted the discovered levels.
    bus.store()
        .set_acp_session_id(&ws, &agent_id, "winner-sid")
        .await
        .unwrap();
    let winner_levels = ["low", "medium", "high", "max"].map(String::from).to_vec();
    bus.store()
        .set_agent_effort_levels(&ws, &agent_id, Some(&winner_levels), &now_iso())
        .await
        .unwrap();

    let mut sub = bus.subscribe(SubscriptionFilter::default());
    // The loser recreates with a mismatched expected-old: the CAS declines to
    // swap, and — even though its own session advertised a selector — the
    // discovered levels belong to a session we didn't keep.
    let (conn, _rx, _agent) =
        connect_with_session_result(claude_shaped_thought_level_session_result());
    let opened = services
        .recreate_acp_session(&conn, &agent_id, "stale-id", "/tmp/ws", Vec::new())
        .await
        .expect("recreate session");
    assert_eq!(
        opened.session_id, "winner-sid",
        "CAS loss keeps canonical id"
    );
    assert!(
        opened.thought_level.is_none(),
        "CAS loss surfaces no selector"
    );

    // The winner's persisted set survives.
    let stored = bus.store().get_agent_session(&agent_id).await.unwrap();
    assert_eq!(
        stored.effort_levels.as_deref(),
        Some(winner_levels.as_slice()),
        "CAS loser must not clear the winner's effort levels"
    );

    // And no agent:updated fired (only the session-create status hint).
    services
        .publish_agent_event(&ws, &agent_id, "test:flush", json!({}))
        .await;
    let mut saw_flush = false;
    while !saw_flush {
        let batch = timeout(Duration::from_secs(2), sub.recv())
            .await
            .expect("recv timed out")
            .expect("subscription open");
        assert!(
            !batch.iter().any(|e| e.event_type == "agent:updated"),
            "CAS loser must not emit agent:updated"
        );
        saw_flush = batch.iter().any(|e| e.event_type == "test:flush");
    }
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

/// The partial blocks a mid-turn interrupt has streamed so far.
fn partial_blocks() -> Vec<Value> {
    vec![json!({ "id": "m1:0", "type": "text", "text": "I'll run " })]
}

/// monorepo#2056 — the interrupt teardown's abort→flush gap must not unpublish
/// the in-flight turn. `pin_live_turn` (called before `worker.abort()`) makes
/// the slot survive the `LiveTurnGuard` drop the abort triggers, so
/// `agent_live_turn` — the `chat.subscribe` snapshot's in-flight source — keeps
/// serving the streamed-so-far content until the flush persists it. Without the
/// pin the content is neither published nor durable for the width of that
/// INSERT, and a snapshot taken there drops the whole partial turn.
#[tokio::test]
async fn pinned_live_turn_survives_the_guard_drop_until_the_interrupt_flush_clears_it() {
    use intent_core::WorkspaceApi;

    let (_tmp, services, _bus, agent_id, _ws) = setup().await;
    let blocks = partial_blocks();

    {
        // Mid-turn: the worker holds the guard, the slot carries the partial
        // content.
        let _guard = services.begin_live_turn(&agent_id, "m1");
        services.set_live_turn(&agent_id, "m1", blocks.clone());
        // The teardown path pins the slot, then aborts the worker — which
        // drops the guard, as this scope end does.
        assert!(
            services.live_turn(&agent_id).is_some(),
            "slot open mid-turn"
        );
        services.pin_live_turn(&agent_id);
    }

    // The window: the guard has dropped and the interrupted row is not durable
    // yet — the pin keeps the slot published, so a snapshot landing here still
    // reconstructs the partial turn.
    assert_eq!(
        services.agent_live_turn(agent_id.clone()),
        Some(json!({ "messageId": "m1", "contentBlocks": blocks })),
        "the pinned slot stays published across the abort→flush gap"
    );

    // The flush lands: the row is durable and the slot (pin included) clears,
    // so later snapshots serve the persisted row alone — no duplicate overlay.
    let flushed = services
        .flush_pinned_turn_on_interruption(&agent_id, super::InterruptReason::UserStop, None)
        .await
        .expect("the pinned slot is still there to flush");
    assert_eq!(flushed.message_id.as_deref(), Some("m1"));
    assert!(
        services.agent_live_turn(agent_id.clone()).is_none(),
        "the flush releases the pin with the slot"
    );
    let messages = services
        .store
        .get_agent_messages(&agent_id, None)
        .await
        .expect("read messages");
    assert_eq!(messages.len(), 1);
    assert_eq!(messages[0].id, "m1");
    assert_eq!(messages[0].content, json!(blocks));
}

/// The other half of the monorepo#2056 contract: an UNPINNED slot still clears
/// on the guard drop. A turn that dies with no teardown flush behind it (worker
/// panic, a plain abort) leaves no orphan slot, so no phantom in-flight message
/// can be merged into a later snapshot — the reason the guard exists.
#[tokio::test]
async fn unpinned_live_turn_slot_still_clears_on_the_guard_drop() {
    use intent_core::WorkspaceApi;

    let (_tmp, services, _bus, agent_id, _ws) = setup().await;
    {
        let _guard = services.begin_live_turn(&agent_id, "m1");
        services.set_live_turn(&agent_id, "m1", partial_blocks());
        assert!(
            services.agent_live_turn(agent_id.clone()).is_some(),
            "the turn is published while it streams"
        );
    }
    assert!(
        services.live_turn(&agent_id).is_none(),
        "an unpinned slot is cleared by the guard drop"
    );
    assert!(
        services.agent_live_turn(agent_id.clone()).is_none(),
        "…so no orphan overlay survives the aborted turn"
    );
}

/// The pin is released on the flush's collision path too: when the worker
/// already persisted the full turn under the minted id, the flush's append
/// loses on the `agent_message.id` UNIQUE constraint and drops the now-stale
/// slot. Otherwise the pin would keep a superseded partial overlay published
/// for the rest of the turn's absence.
#[tokio::test]
async fn interrupt_flush_releases_the_pin_when_the_worker_already_persisted_the_turn() {
    use intent_core::WorkspaceApi;

    let (_tmp, services, _bus, agent_id, _ws) = setup().await;
    let full_blocks = json!([
        { "id": "m1:0", "type": "text", "text": "I'll run the tests." }
    ]);

    {
        let _guard = services.begin_live_turn(&agent_id, "m1");
        services.set_live_turn(&agent_id, "m1", partial_blocks());
        assert!(
            services.live_turn(&agent_id).is_some(),
            "slot open mid-turn"
        );
        services.pin_live_turn(&agent_id);
    }
    // The aborted worker's own append won the race: the full row is durable.
    services
        .store
        .append_agent_message_with_id(
            &agent_id,
            "m1",
            "assistant",
            &full_blocks,
            None,
            &intent_core::now_iso(),
        )
        .await
        .expect("worker append");

    let flushed = services
        .flush_pinned_turn_on_interruption(&agent_id, super::InterruptReason::AgentStopped, None)
        .await
        .expect("the pinned slot is still there to flush");
    assert!(
        flushed.message_id.is_none(),
        "the durable full row won the collision"
    );
    assert!(
        services.agent_live_turn(agent_id.clone()).is_none(),
        "the collision path releases the pin with the stale slot"
    );
    let messages = services
        .store
        .get_agent_messages(&agent_id, None)
        .await
        .expect("read messages");
    assert_eq!(messages.len(), 1, "exactly one row survives the race");
    assert_eq!(messages[0].content, full_blocks, "the full row is intact");
}

/// monorepo#2110 — the flush persists the slot AS OF FLUSH TIME, not the clone
/// the teardown path used to take before `worker.abort()`. The abort cannot
/// recall a `session/update` already being routed, so a final chunk can land
/// after the pin: flushing the pre-abort clone trimmed it out of the durable
/// row even though every subscriber had already been sent it, leaving the
/// reloaded transcript short of what clients watched stream.
#[tokio::test]
async fn interrupt_flush_persists_the_update_routed_after_the_pin() {
    let (_tmp, services, _bus, agent_id, workspace_id) = setup().await;
    let mut transcript = super::Transcript::new("m1".to_string());
    let chunk = |text: &str| IncomingNotification {
        method: "session/update".to_string(),
        params: json!({
            "sessionId": ACP_SID,
            "update": { "sessionUpdate": "agent_message_chunk",
                "content": { "type": "text", "text": text } }
        }),
    };

    {
        // Mid-turn: the worker holds the guard and streams the first chunk.
        let _guard = services.begin_live_turn(&agent_id, "m1");
        services
            .route_notification(
                &chunk("I'll run "),
                &agent_id,
                &workspace_id,
                &mut transcript,
            )
            .await;
        // The teardown path pins the slot…
        assert!(
            services.live_turn(&agent_id).is_some(),
            "slot open mid-turn"
        );
        services.pin_live_turn(&agent_id);
        // …and the notification already in flight is routed in the pin→abort
        // gap: broadcast to every subscriber AND folded into the live slot.
        services
            .route_notification(
                &chunk("the tests.\n"),
                &agent_id,
                &workspace_id,
                &mut transcript,
            )
            .await;
        // Scope end = the `worker.abort()` that drops the LiveTurnGuard.
    }

    let flushed = services
        .flush_pinned_turn_on_interruption(&agent_id, super::InterruptReason::UserStop, None)
        .await
        .expect("the pinned slot is still there to flush");
    assert_eq!(flushed.message_id.as_deref(), Some("m1"));
    assert!(flushed.had_output, "the flushed slot carried blocks");
    assert_eq!(
        flushed.text_blocks,
        vec!["I'll run the tests.\n".to_string()],
        "the terminal preview reads the flush-time content too"
    );

    let messages = services
        .store
        .get_agent_messages(&agent_id, None)
        .await
        .expect("read messages");
    assert_eq!(messages.len(), 1);
    assert_eq!(
        messages[0].content,
        json!([{ "id": "m1:0", "type": "text", "text": "I'll run the tests.\n" }]),
        "the durable row carries the chunk routed after the pin, \
         not the pre-abort clone's \"I'll run \""
    );
}

/// monorepo#2110 review — a ZERO-OUTPUT normal completion landing in the
/// abort gap must not look like a turn that produced output.
///
/// `run_prompt_turn` persists an assistant row only when the turn produced
/// blocks (`if !blocks.is_empty()`), but its turn-end slot clear is
/// unconditional. So a turn that completes normally having streamed nothing
/// writes NO row and drops the slot. If the teardown path treats every
/// vanished-but-pinned slot as "the worker persisted a full row", a user stop
/// racing that completion loses both halves of the zero-output contract: no
/// interrupted marker row for the FE to anchor the Stopped indicator on, and
/// no prompt-only stop-redelivery armed for the next turn (#1757) — the
/// stopped prompt is silently dropped.
///
/// The pin is what prevents it: a pinned slot survives the turn-end clear
/// exactly as it survives the `LiveTurnGuard` drop, so the flush still finds
/// it, still records the empty-blocks marker row, and still reports
/// `had_output: false`.
#[tokio::test]
async fn zero_output_completion_in_the_abort_gap_still_flushes_a_marker_row() {
    let (_tmp, services, _bus, agent_id, _ws) = setup().await;

    {
        // Mid-turn: a slot is open but nothing has streamed into it yet.
        let _guard = services.begin_live_turn(&agent_id, "m1");
        // The teardown path pins the slot…
        assert!(
            services.live_turn(&agent_id).is_some(),
            "slot open mid-turn"
        );
        services.pin_live_turn(&agent_id);
        // …and in the pin→abort gap the turn completes normally with zero
        // blocks: `run_prompt_turn` persists nothing and clears the slot.
        services.clear_unpinned_live_turn(&agent_id);
    }

    let flushed = services
        .flush_pinned_turn_on_interruption(&agent_id, super::InterruptReason::UserStop, None)
        .await
        .expect("the pin keeps the slot alive through a zero-output turn end");
    assert!(
        !flushed.had_output,
        "a zero-block turn produced no output — the stop-redelivery arm depends on this"
    );
    assert_eq!(
        flushed.message_id.as_deref(),
        Some("m1"),
        "the interrupted marker row is still recorded"
    );

    let messages = services
        .store
        .get_agent_messages(&agent_id, None)
        .await
        .expect("read messages");
    assert_eq!(messages.len(), 1, "exactly the marker row");
    assert_eq!(messages[0].content, json!([]), "empty blocks, as flushed");
    assert_eq!(
        messages[0].metadata.as_ref().expect("metadata")["interrupted"],
        json!(true)
    );
}

/// monorepo#2110 — the REAL prompt-turn end must leave a pinned slot to the
/// teardown flush, not just the `clear_unpinned_live_turn` helper the
/// simulation above calls directly. Drives `run_prompt_turn` against a mock
/// whose `session/prompt` response is held open, pins in that window (as the
/// teardown paths do before `worker.abort()`), then releases the turn to
/// complete normally with zero output: `run_prompt_turn` persists no row and
/// its turn-end clear runs for real — the pinned slot must survive it for the
/// flush that owns it.
#[tokio::test]
async fn normal_zero_output_turn_end_leaves_the_pinned_slot_to_the_teardown_flush() {
    let (_tmp, services, _bus, agent_id, workspace_id) = setup().await;
    let (conn, mut note_rx, _agent, release) = connect_gated_prompt(Vec::new());

    let turn = {
        let services = services.clone();
        let agent_id = agent_id.clone();
        let workspace_id = workspace_id.clone();
        tokio::spawn(async move {
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
        })
    };
    // The slot opens at turn start; the gate holds the turn there.
    timeout(Duration::from_secs(2), async {
        while services.live_turn(&agent_id).is_none() {
            tokio::task::yield_now().await;
        }
    })
    .await
    .expect("the turn opens its live slot");
    // The teardown path pins…
    services.pin_live_turn(&agent_id);
    // …and the turn completes normally with zero blocks.
    release.send(()).expect("mock alive");
    let stop = timeout(Duration::from_secs(2), turn)
        .await
        .expect("turn completes")
        .expect("worker task")
        .expect("prompt turn ok");
    assert_eq!(serde_json::to_value(stop).unwrap(), json!("end_turn"));

    let slot = services
        .live_turn(&agent_id)
        .expect("the real turn-end clear leaves the pinned slot to the teardown flush");
    assert!(slot.flush_pending, "the pin survives the turn end");
    assert!(slot.blocks.is_empty(), "zero-output turn");

    let flushed = services
        .flush_pinned_turn_on_interruption(&agent_id, super::InterruptReason::UserStop, None)
        .await
        .expect("the pinned slot is still there to flush");
    assert!(!flushed.had_output, "a zero-block turn produced no output");
    assert!(
        flushed.message_id.is_some(),
        "the interrupted marker row is recorded"
    );
}

/// The teardown flush only owns a PINNED slot. An unpinned slot present at
/// flush time is not this teardown's turn — it is the NEXT turn's, begun in
/// the pin→flush window (`begin_live_turn` replaces the slot wholesale,
/// unpinned) — and flushing it would persist a live turn as interrupted under
/// its freshly minted id, poisoning the id the worker's own append still
/// needs. The flush must leave it untouched and report nothing pinned.
#[tokio::test]
async fn interrupt_flush_leaves_an_unpinned_slot_alone() {
    let (_tmp, services, _bus, agent_id, _ws) = setup().await;
    // The next turn's slot: begun after the teardown's pinned slot was
    // replaced — never pinned by this teardown.
    services.set_live_turn(&agent_id, "m2", partial_blocks());

    let flushed = services
        .flush_pinned_turn_on_interruption(&agent_id, super::InterruptReason::UserStop, None)
        .await;
    assert!(
        flushed.is_none(),
        "an unpinned slot is not this flush's to persist"
    );
    assert!(
        services.live_turn(&agent_id).is_some(),
        "the live turn's slot stays published"
    );
    let messages = services
        .store
        .get_agent_messages(&agent_id, None)
        .await
        .expect("read messages");
    assert!(
        messages.is_empty(),
        "no interrupted row was recorded for the live turn"
    );
}

/// monorepo#2140 — the suspend-enrollment flush must not release a pin a
/// concurrent teardown holds. It flushes caller-held content (its synthetic
/// turn never enters the slots map), so its slot clear is pin-respecting: the
/// pinned slot survives to the teardown's flush, which absorbs the
/// `agent_message.id` UNIQUE collision with the enrollment's row and keeps the
/// slot-derived `had_output` — instead of reading `None` as "no turn in
/// flight" and arming a zero-output stop-redelivery for a turn that DID
/// produce output.
#[tokio::test]
async fn suspend_enrollment_flush_leaves_a_foreign_pin_to_its_teardown() {
    let (_tmp, services, _bus, agent_id, _ws) = setup().await;

    // A teardown pinned the in-flight slot…
    services.set_live_turn(&agent_id, "m1", partial_blocks());
    services.pin_live_turn(&agent_id);
    // …and in the abort gap the worker classifies a suspend interrupt and
    // flushes its caller-held content (the enroll path: `owns_slot: false`).
    let live = super::LiveTurn {
        message_id: "m1".to_string(),
        blocks: partial_blocks(),
        final_text_block_open: false,
        last_activity_at: "2026-01-01T00:00:00Z".to_string(),
        last_activity_emit: None,
        flush_pending: false,
        flush_failed: false,
    };
    let enrolled = services
        .flush_partial_turn_on_interruption(
            &agent_id,
            live,
            super::InterruptReason::SystemSuspend,
            None,
            false,
        )
        .await;
    assert_eq!(
        enrolled.as_deref(),
        Some("m1"),
        "the enrollment row is durable"
    );
    assert!(
        services
            .live_turn(&agent_id)
            .is_some_and(|l| l.flush_pending),
        "the enrollment flush leaves the foreign pin in place"
    );

    // The teardown's own flush still finds its pinned slot: the UNIQUE
    // collision with the enrollment row is absorbed and `had_output` keeps
    // the truth — the turn produced output, so no stop-redelivery.
    let flushed = services
        .flush_pinned_turn_on_interruption(&agent_id, super::InterruptReason::UserStop, None)
        .await
        .expect("the pinned slot survived to its owner");
    assert!(
        flushed.message_id.is_none(),
        "the enrollment row won the collision"
    );
    assert!(flushed.had_output, "the turn really did produce output");
    assert!(
        services.live_turn(&agent_id).is_none(),
        "the owning flush releases the pin"
    );
}

/// The abandoned mark must not widen either pin-respecting clear
/// ([`LiveTurn::flush_failed`]'s contract): the aborted worker's
/// [`LiveTurnGuard`] drop can run AFTER the flush gave up — `worker.abort()`
/// cancels at the next await point, unordered against the interrupt path's
/// flush — and both it and the normal turn-end clear key on `flush_pending`
/// alone. A slot that is pinned AND abandoned is the only copy of the
/// content, so both must leave it; only a new turn's claim may reap it
/// (`try_begin_drops_a_slot_whose_flush_already_gave_up`).
#[tokio::test]
async fn abandoned_slot_survives_guard_drop_and_turn_end_clear() {
    let (_tmp, services, _bus, _seeded, _ws) = setup().await;
    // Deliberately NOT the seeded agent: no agent_session row, so the flush's
    // INSERT fails the `agent_message.agent_id` foreign key — the genuine
    // store error that drives the give-up arm.
    let agent_id = AgentId::from("agent-abandoned");

    let guard = services.begin_live_turn(&agent_id, "m-abandoned");
    services.set_live_turn(&agent_id, "m-abandoned", partial_blocks());
    services.pin_live_turn(&agent_id);
    let flushed = services
        .flush_pinned_turn_on_interruption(&agent_id, super::InterruptReason::UserStop, None)
        .await
        .expect("the pinned slot was there to flush");
    assert!(
        flushed.message_id.is_none(),
        "precondition: the store rejected the append, so nothing was persisted"
    );
    assert!(
        services
            .live_turn(&agent_id)
            .is_some_and(|s| s.flush_pending && s.flush_failed),
        "precondition: the give-up arm kept the slot pinned and marked it abandoned"
    );

    // The aborted worker's delayed guard drop…
    drop(guard);
    assert!(
        services.live_turn(&agent_id).is_some(),
        "the guard drop leaves an abandoned slot alone — it is the only copy of the content"
    );

    // …and a turn end reaching the pin-respecting clear.
    services.clear_unpinned_live_turn(&agent_id);
    assert!(
        services.live_turn(&agent_id).is_some(),
        "the turn-end clear leaves an abandoned slot alone too"
    );
}

/// A tool-ONLY turn keeps ticking (monorepo#1414): a turn that streams three
/// tool calls and no assistant text still emits `agent:stream:activity`, each
/// ping carrying `lastToolUse { name, status }` for the call just recorded.
/// The throttle still applies — the burst lands inside one 1 s window, so the
/// leading-edge ping is the only guaranteed one.
#[tokio::test]
async fn tool_only_turn_emits_throttled_activity_with_last_tool_use() {
    let (_tmp, services, bus, agent_id, workspace_id) = setup().await;
    let (conn, mut note_rx, _agent) = connect_with(prompt_updates_tools_only());
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

    let activities: Vec<&Event> = events
        .iter()
        .filter(|e| e.event_type == "agent:stream:activity")
        .collect();
    assert!(
        !activities.is_empty(),
        "a turn with no assistant text still emits activity from the tool arm"
    );
    // Sanity cap, not throttle verification: the turn has exactly three
    // potential emitters, so this only guards against a future fixture change
    // multiplying the emit count. The deterministic window-boundary coverage
    // lives in `activity_throttle_window_is_shared_between_chunk_and_tool_arms`
    // below. Deliberately not tightened to `< 3` — the throttle is real-clock
    // based, so a >1s gap between updates on a loaded machine legitimately
    // produces up to three emits.
    assert!(
        activities.len() <= 3,
        "at most one ping per tool call: {}",
        activities.len()
    );
    // The leading-edge ping describes the FIRST tool call and carries no
    // preview text (nothing streamed).
    let first = activities[0];
    assert_eq!(first.data["agentId"], json!("agent-1"));
    assert!(
        first.data["messageId"].is_string(),
        "activity carries the turn's messageId"
    );
    assert_eq!(
        first.data["lastToolUse"],
        json!({ "name": "bash", "status": "started" }),
        "tool-arm activity carries the just-recorded call's name + status"
    );
    assert!(
        first.data.get("lastAgentResponse").is_none(),
        "no assistant text streamed → no preview field"
    );
    for ev in &activities {
        assert!(
            ev.data["lastToolUse"]["name"].is_string(),
            "every tool-arm ping names the tool"
        );
        assert!(
            ev.data.get("content").is_none(),
            "activity payload never carries transcript content"
        );
    }
}

/// Both emit arms share ONE per-agent throttle window: a text chunk's ping
/// suppresses an immediately following tool call's ping (and vice versa),
/// while the window elapsing re-opens the gate for whichever arm comes next.
/// Drives `route_notification` directly so the window boundaries are
/// deterministic. The throttle reads a real `Instant` (tokio paused time cannot
/// help), so the first two routes are adjacent awaits deliberately: if >1s of
/// wall clock elapsed between them the `t1` ping would no longer be suppressed
/// and the count would read 3 — a rare failure here is host scheduling noise,
/// not a regression.
#[tokio::test]
async fn activity_throttle_window_is_shared_between_chunk_and_tool_arms() {
    let (_tmp, services, bus, agent_id, workspace_id) = setup().await;
    let mut sub = bus.subscribe(SubscriptionFilter::default());
    let mut transcript = super::Transcript::new("m1".to_string());
    services.set_live_turn(&agent_id, "m1", Vec::new());

    let chunk_note = |text: &str| IncomingNotification {
        method: "session/update".to_string(),
        params: json!({
            "sessionId": ACP_SID,
            "update": { "sessionUpdate": "agent_message_chunk",
                "content": { "type": "text", "text": text } }
        }),
    };
    let tool_note = |id: &str, title: &str| IncomingNotification {
        method: "session/update".to_string(),
        params: json!({
            "sessionId": ACP_SID,
            "update": { "sessionUpdate": "tool_call", "toolCallId": id,
                "title": title, "kind": "execute", "status": "in_progress",
                "rawInput": { "cmd": "x" } }
        }),
    };

    // Leading edge on the chunk arm.
    services
        .route_notification(
            &chunk_note("Hello\n"),
            &agent_id,
            &workspace_id,
            &mut transcript,
        )
        .await;
    // Same window → the tool arm is suppressed.
    services
        .route_notification(
            &tool_note("t1", "bash: one"),
            &agent_id,
            &workspace_id,
            &mut transcript,
        )
        .await;
    // Window elapses → the tool arm emits.
    tokio::time::sleep(super::ACTIVITY_THROTTLE + Duration::from_millis(50)).await;
    services
        .route_notification(
            &tool_note("t2", "view: src/lib.rs"),
            &agent_id,
            &workspace_id,
            &mut transcript,
        )
        .await;
    // Same window again → the chunk arm is now the suppressed one.
    services
        .route_notification(
            &chunk_note("more\n"),
            &agent_id,
            &workspace_id,
            &mut transcript,
        )
        .await;

    let mut events: Vec<Event> = Vec::new();
    while let Ok(Some(batch)) = timeout(Duration::from_millis(300), sub.recv()).await {
        events.extend(batch);
    }
    let activities: Vec<&Event> = events
        .iter()
        .filter(|e| e.event_type == "agent:stream:activity")
        .collect();
    assert_eq!(
        activities.len(),
        2,
        "one ping per shared window, regardless of which arm opened it: {:?}",
        events
            .iter()
            .map(|e| e.event_type.as_str())
            .collect::<Vec<_>>()
    );
    assert!(
        activities[0].data.get("lastToolUse").is_none(),
        "the chunk-arm ping carries no lastToolUse"
    );
    assert_eq!(
        activities[1].data["lastToolUse"],
        json!({ "name": "view", "status": "started" }),
        "the second window's ping came from the tool arm"
    );
    assert_eq!(
        activities[1].data["lastAgentResponse"],
        json!("Hello"),
        "tool-arm pings carry the same live preview fields as chunk-arm pings"
    );
}

// --- Thinking blocks (streamed reasoning) ---------------------------------

fn thought_note(text: &str) -> IncomingNotification {
    IncomingNotification {
        method: "session/update".to_string(),
        params: json!({
            "sessionId": ACP_SID,
            "update": { "sessionUpdate": "agent_thought_chunk",
                "content": { "type": "text", "text": text } }
        }),
    }
}

fn message_note(text: &str) -> IncomingNotification {
    IncomingNotification {
        method: "session/update".to_string(),
        params: json!({
            "sessionId": ACP_SID,
            "update": { "sessionUpdate": "agent_message_chunk",
                "content": { "type": "text", "text": text } }
        }),
    }
}

/// Consecutive thought chunks coalesce into ONE `thinking` block, and a
/// thought↔text switch (either direction) closes the open block and starts a
/// new one — so thought → text → thought persists three interleaved blocks in
/// stream order.
#[tokio::test]
async fn thought_chunks_coalesce_and_interleave_with_text() {
    let (_tmp, services, _bus, agent_id, workspace_id) = setup().await;
    let mut transcript = super::Transcript::new("m1".to_string());
    services.set_live_turn(&agent_id, "m1", Vec::new());

    for note in [
        thought_note("Let me "),
        thought_note("think."),
        message_note("Answer: "),
        message_note("42."),
        thought_note("Double-checking."),
    ] {
        assert!(
            services
                .route_notification(&note, &agent_id, &workspace_id, &mut transcript)
                .await,
            "every chunk (thought or text) is a turn update"
        );
    }

    let blocks = transcript.into_blocks();
    assert_eq!(
        blocks,
        vec![
            json!({ "type": "thinking", "id": "m1:0", "text": "Let me think." }),
            json!({ "type": "text", "id": "m1:1", "text": "Answer: 42." }),
            json!({ "type": "thinking", "id": "m1:2", "text": "Double-checking." }),
        ],
        "consecutive thoughts merge; each thought↔text switch opens a new block"
    );
}

/// A tool call breaks an open `thinking` block exactly as it breaks a text
/// block, and the thought resumed after it lands in a fresh block.
#[tokio::test]
async fn tool_call_breaks_an_open_thinking_block() {
    let (_tmp, services, _bus, agent_id, workspace_id) = setup().await;
    let mut transcript = super::Transcript::new("m1".to_string());
    services.set_live_turn(&agent_id, "m1", Vec::new());

    let tool = IncomingNotification {
        method: "session/update".to_string(),
        params: json!({
            "sessionId": ACP_SID,
            "update": { "sessionUpdate": "tool_call", "toolCallId": "t1",
                "title": "bash: ls", "kind": "execute", "status": "in_progress",
                "rawInput": { "cmd": "ls" } }
        }),
    };
    for note in [thought_note("Plan it."), tool, thought_note("Read it.")] {
        services
            .route_notification(&note, &agent_id, &workspace_id, &mut transcript)
            .await;
    }

    let blocks = transcript.into_blocks();
    let types: Vec<&str> = blocks
        .iter()
        .map(|b| b["type"].as_str().unwrap_or(""))
        .collect();
    assert_eq!(types, vec!["thinking", "tool_use", "thinking"]);
    assert_eq!(blocks[0]["text"], json!("Plan it."));
    assert_eq!(blocks[2]["text"], json!("Read it."));
}

/// Thought chunks stream as `chat:stream:delta` with `blockType: "thinking"`
/// on their own stable block id, and never leak into the
/// `agent:stream:activity` live preview (`lastAgentResponse`/`digest`).
#[tokio::test]
async fn thought_deltas_carry_thinking_block_type_and_skip_live_previews() {
    let (_tmp, services, bus, agent_id, workspace_id) = setup().await;
    let mut sub = bus.subscribe(SubscriptionFilter::default());
    let mut transcript = super::Transcript::new("m1".to_string());
    services.set_live_turn(&agent_id, "m1", Vec::new());

    for note in [thought_note("Reasoning...\n"), thought_note("more\n")] {
        services
            .route_notification(&note, &agent_id, &workspace_id, &mut transcript)
            .await;
    }

    let mut events: Vec<Event> = Vec::new();
    while let Ok(Some(batch)) = timeout(Duration::from_millis(300), sub.recv()).await {
        events.extend(batch);
    }
    let deltas: Vec<&Event> = events
        .iter()
        .filter(|e| e.event_type == "chat:stream:delta")
        .collect();
    assert_eq!(deltas.len(), 2, "one delta per thought chunk");
    for d in &deltas {
        assert_eq!(d.data["blockType"], json!("thinking"));
        assert_eq!(d.data["blockIndex"], json!(0), "coalesced onto one block");
        assert_eq!(d.data["blockId"], json!("m1:0"));
    }
    assert_eq!(deltas[0].data["content"], json!("Reasoning...\n"));

    for a in events
        .iter()
        .filter(|e| e.event_type == "agent:stream:activity")
    {
        assert!(
            a.data.get("lastAgentResponse").is_none() && a.data.get("digest").is_none(),
            "thought text never feeds the live preview: {:?}",
            a.data
        );
    }
}

/// Thought text is excluded from the text-block extraction the agent-list
/// previews read, while assistant text in the same turn still counts.
#[tokio::test]
async fn thought_text_is_absent_from_text_block_extraction() {
    let (_tmp, services, _bus, agent_id, workspace_id) = setup().await;
    let mut transcript = super::Transcript::new("m1".to_string());
    services.set_live_turn(&agent_id, "m1", Vec::new());

    for note in [thought_note("secret reasoning"), message_note("Done.")] {
        services
            .route_notification(&note, &agent_id, &workspace_id, &mut transcript)
            .await;
    }
    assert_eq!(transcript.text_block_strings(), vec!["Done.".to_string()]);

    // A turn that streamed ONLY reasoning contributes no preview text and
    // leaves no open final text block.
    let mut thought_only = super::Transcript::new("m2".to_string());
    services
        .route_notification(
            &thought_note("only reasoning"),
            &agent_id,
            &workspace_id,
            &mut thought_only,
        )
        .await;
    assert!(thought_only.text_block_strings().is_empty());
    assert!(!thought_only.final_text_block_open());
    assert_eq!(
        thought_only.snapshot_blocks(),
        vec![json!({ "type": "thinking", "id": "m2:0", "text": "only reasoning" })],
        "the live-turn snapshot carries the in-flight thinking block"
    );
}

// --- Group-tag-bearing turns (monorepo#2029 audit) -------------------------

/// The FE opens a `<group:Name>` display section on the text block carrying the
/// tag and swallows every LATER block in the message, so the daemon's ordering
/// invariant is load-bearing for grouping: the text block that opened the group
/// must sit at an index BEFORE the tool blocks it should contain, on BOTH the
/// mid-turn snapshot path ([`Transcript::snapshot_blocks`], what a
/// `chat.subscribe` arriving mid-turn reconstructs the in-flight message from)
/// and the persisted path ([`Transcript::into_blocks`]) — with block ids stable
/// (`{messageId}:{index}`, never renumbered) as the turn grows.
///
/// Drives the real shape: a group-opening text chunk (split mid-tag, as it
/// streams) → `tool_call` → `tool_call_update(completed)` → more text → two
/// more tool calls, asserting the invariant after EVERY step.
#[tokio::test]
async fn group_opening_text_block_precedes_its_tool_blocks_in_snapshot_and_persist() {
    let (_tmp, services, bus, agent_id, workspace_id) = setup().await;
    let mut sub = bus.subscribe(SubscriptionFilter::default());
    let mut transcript = super::Transcript::new("m1".to_string());
    services.set_live_turn(&agent_id, "m1", Vec::new());

    const GROUP_TEXT: &str =
        "<group:Prepping>\nI'll set the workspace title and dig into the debug bundle.";

    let tool_note = |id: &str, title: &str| IncomingNotification {
        method: "session/update".to_string(),
        params: json!({
            "sessionId": ACP_SID,
            "update": { "sessionUpdate": "tool_call", "toolCallId": id,
                "title": title, "kind": "execute", "status": "in_progress",
                "rawInput": { "cmd": "x" } }
        }),
    };
    let done_note = |id: &str| IncomingNotification {
        method: "session/update".to_string(),
        params: json!({
            "sessionId": ACP_SID,
            "update": { "sessionUpdate": "tool_call_update", "toolCallId": id,
                "status": "completed", "rawOutput": { "summary": "ok" } }
        }),
    };

    // Index 0 must stay the group-opening text block for the whole turn, and
    // every tool block must land after it.
    let assert_group_invariant = |blocks: &[Value], step: &str| {
        assert_eq!(
            blocks[0]["type"], "text",
            "{step}: block 0 is the group-opening text block: {blocks:?}"
        );
        assert_eq!(
            blocks[0]["id"], "m1:0",
            "{step}: the group-opening block keeps its id"
        );
        assert_eq!(
            blocks[0]["text"], GROUP_TEXT,
            "{step}: the group tag + header text is intact"
        );
        for (index, block) in blocks.iter().enumerate() {
            assert_eq!(
                block["id"],
                json!(format!("m1:{index}")),
                "{step}: block ids stay {{messageId}}:{{index}}"
            );
            if matches!(
                block["type"].as_str(),
                Some("tool_use") | Some("tool_result")
            ) {
                assert!(index > 0, "{step}: no tool block precedes the group open");
            }
        }
    };

    // The live-turn slot (the `chat.subscribe` mid-turn reconstruction source)
    // must carry exactly the snapshot the transcript reports.
    let assert_slot_matches = |step: &str, expected: &[Value]| {
        let live = services.live_turn(&agent_id).expect("live turn slot open");
        assert_eq!(
            live.blocks, expected,
            "{step}: the live-turn slot mirrors snapshot_blocks()"
        );
    };

    // Step 1 — the group-opening text, streamed split across the `>` so the
    // tag itself spans two chunks (they coalesce onto one block).
    for chunk in ["<group:Prep", "ping>\nI'll set the workspace title "] {
        services
            .route_notification(
                &message_note(chunk),
                &agent_id,
                &workspace_id,
                &mut transcript,
            )
            .await;
    }
    services
        .route_notification(
            &message_note("and dig into the debug bundle."),
            &agent_id,
            &workspace_id,
            &mut transcript,
        )
        .await;
    let snap = transcript.snapshot_blocks();
    assert_eq!(
        snap,
        vec![json!({ "type": "text", "id": "m1:0", "text": GROUP_TEXT })],
        "the pending chunk buffer surfaces as the block index it will flush into"
    );
    assert_group_invariant(&snap, "after group-opening text");
    assert_slot_matches("after group-opening text", &snap);

    // Step 2 — first tool call: flushes the open text block at index 0 and
    // appends `tool_use` at index 1.
    services
        .route_notification(
            &tool_note("t1", "bash: title"),
            &agent_id,
            &workspace_id,
            &mut transcript,
        )
        .await;
    let snap = transcript.snapshot_blocks();
    let types: Vec<&str> = snap.iter().map(|b| b["type"].as_str().unwrap()).collect();
    assert_eq!(types, vec!["text", "tool_use"]);
    assert_group_invariant(&snap, "after first tool_call");
    assert_slot_matches("after first tool_call", &snap);

    // Step 3 — completion appends the `tool_result`; the text block does not move.
    services
        .route_notification(&done_note("t1"), &agent_id, &workspace_id, &mut transcript)
        .await;
    let snap = transcript.snapshot_blocks();
    let types: Vec<&str> = snap.iter().map(|b| b["type"].as_str().unwrap()).collect();
    assert_eq!(types, vec!["text", "tool_use", "tool_result"]);
    assert_group_invariant(&snap, "after tool_call_update(completed)");
    assert_slot_matches("after tool_call_update(completed)", &snap);

    // Step 4 — more text lands in a NEW block after the tool blocks (the group
    // opened at index 0 still spans everything after it).
    services
        .route_notification(
            &message_note("Now reading the bundle."),
            &agent_id,
            &workspace_id,
            &mut transcript,
        )
        .await;
    // Step 5 — two more tool calls after that text.
    for (id, title) in [("t2", "view: bundle.json"), ("t3", "bash: grep")] {
        services
            .route_notification(
                &tool_note(id, title),
                &agent_id,
                &workspace_id,
                &mut transcript,
            )
            .await;
    }
    let snap = transcript.snapshot_blocks();
    let types: Vec<&str> = snap.iter().map(|b| b["type"].as_str().unwrap()).collect();
    assert_eq!(
        types,
        vec![
            "text",
            "tool_use",
            "tool_result",
            "text",
            "tool_use",
            "tool_use"
        ],
        "blocks accumulate in stream order"
    );
    assert_group_invariant(&snap, "after trailing text + tool calls");
    assert_slot_matches("after trailing text + tool calls", &snap);

    // The live `chat:stream:delta` / `agent:tool:call` block identities agree
    // with the snapshot positions: the group-opening chunks all name `m1:0`,
    // and every tool event names a strictly later index.
    let mut events: Vec<Event> = Vec::new();
    while let Ok(Some(batch)) = timeout(Duration::from_millis(300), sub.recv()).await {
        events.extend(batch);
    }
    let group_deltas: Vec<&Event> = events
        .iter()
        .filter(|e| e.event_type == "chat:stream:delta" && e.data["blockIndex"] == json!(0))
        .collect();
    assert_eq!(
        group_deltas.len(),
        3,
        "the three group-opening chunks coalesce onto block 0"
    );
    for d in &group_deltas {
        assert_eq!(d.data["blockId"], json!("m1:0"));
        assert_eq!(d.data["blockType"], json!("text"));
    }
    for e in events.iter().filter(|e| e.event_type == "agent:tool:call") {
        assert!(
            e.data["blockIndex"].as_u64().unwrap() > 0,
            "no tool event claims the group-opening block index: {:?}",
            e.data
        );
    }

    // The persisted block sequence matches the last snapshot exactly — the
    // delta-accumulated, snapshot-reconstructed, and persisted views agree.
    let persisted = transcript.into_blocks();
    assert_eq!(
        persisted, snap,
        "into_blocks() equals the final snapshot_blocks()"
    );
    assert_group_invariant(&persisted, "persisted");
}

/// PARALLEL tools (Claude Code fires several before any completes) push a
/// completion's `tool_result` to the CURRENT end of the block list — NOT to its
/// `tool_use` index + 1 — because a later call's `tool_use` already took that
/// slot. Pins the transcript-side indices the `chat.subscribe` forwarder must
/// reproduce live; it no longer predicts them (monorepo#2029), it reads the
/// real ids off the event — see
/// [`tool_call_events_carry_real_result_block_ids_for_parallel_completions`].
/// The group-opening text block at index 0 is unaffected either way.
#[tokio::test]
async fn interleaved_tool_completions_append_results_at_the_current_end() {
    let (_tmp, services, _bus, agent_id, workspace_id) = setup().await;
    let mut transcript = super::Transcript::new("m1".to_string());
    services.set_live_turn(&agent_id, "m1", Vec::new());

    let tool_note = |id: &str| IncomingNotification {
        method: "session/update".to_string(),
        params: json!({
            "sessionId": ACP_SID,
            "update": { "sessionUpdate": "tool_call", "toolCallId": id,
                "title": "bash: x", "kind": "execute", "status": "in_progress",
                "rawInput": { "cmd": "x" } }
        }),
    };
    let done_note = |id: &str| IncomingNotification {
        method: "session/update".to_string(),
        params: json!({
            "sessionId": ACP_SID,
            "update": { "sessionUpdate": "tool_call_update", "toolCallId": id,
                "status": "completed", "rawOutput": { "summary": "ok" } }
        }),
    };

    for note in [
        message_note("<group:Prepping>\nHeader."),
        tool_note("t1"),
        tool_note("t2"),
        done_note("t1"),
        done_note("t2"),
    ] {
        services
            .route_notification(&note, &agent_id, &workspace_id, &mut transcript)
            .await;
    }

    let blocks = transcript.into_blocks();
    let shape: Vec<(&str, &str)> = blocks
        .iter()
        .map(|b| {
            (
                b["type"].as_str().unwrap(),
                b["id"].as_str().unwrap_or_default(),
            )
        })
        .collect();
    assert_eq!(
        shape,
        vec![
            ("text", "m1:0"),
            ("tool_use", "m1:1"),
            ("tool_use", "m1:2"),
            ("tool_result", "m1:3"),
            ("tool_result", "m1:4"),
        ],
        "t1's result lands at index 3 (the end), not at its tool_use index + 1"
    );
    assert_eq!(blocks[3]["tool_use_id"], json!("t1"));
    assert_eq!(blocks[4]["tool_use_id"], json!("t2"));
}

/// monorepo#2029: the `agent:tool:call` event names the REAL `tool_result`
/// block id (`resultBlockId`), so the live `chat.subscribe` delta path never
/// has to predict it. Shape (a): assistant text INTERLEAVES between a call and
/// its completion, so the durable transcript flushes that text into the index
/// the old `tool_use + 1` prediction would have claimed — the prediction landed
/// on a legitimate `text` block (the one that can carry a `<group:Name>`
/// opener) and clobbered it on every id-keyed client until `stream:end`.
#[tokio::test]
async fn tool_call_event_carries_the_real_result_block_id_when_text_interleaves() {
    let (_tmp, services, bus, agent_id, workspace_id) = setup().await;
    let mut sub = bus.subscribe(SubscriptionFilter::default());
    let mut transcript = super::Transcript::new("m1".to_string());
    services.set_live_turn(&agent_id, "m1", Vec::new());

    for note in [
        message_note("I'll run the tests. "),
        tool_call_note("t1"),
        message_note("<group:Setup>\nChecking output. "),
        tool_done_note("t1"),
    ] {
        services
            .route_notification(&note, &agent_id, &workspace_id, &mut transcript)
            .await;
    }

    let blocks = transcript.into_blocks();
    let shape: Vec<(&str, &str)> = blocks
        .iter()
        .map(|b| (b["type"].as_str().unwrap(), b["id"].as_str().unwrap()))
        .collect();
    assert_eq!(
        shape,
        vec![
            ("text", "m1:0"),
            ("tool_use", "m1:1"),
            ("text", "m1:2"),
            ("tool_result", "m1:3"),
        ],
        "the interleaved text owns m1:2; the real result is m1:3"
    );

    let events = drain_tool_call_events(&mut sub).await;
    assert_result_ids_match_transcript(&events, &blocks);
    let completed = events
        .iter()
        .find(|e| e.data["status"] == json!("completed"))
        .expect("a completed agent:tool:call event");
    assert_eq!(completed.data["blockId"], json!("m1:1"));
    assert_eq!(
        completed.data["resultBlockId"],
        json!("m1:3"),
        "the event names the real result id, NOT the tool_use index + 1 (m1:2)"
    );
    assert_eq!(completed.data["resultBlockIndex"], json!(3));
    let started = events
        .iter()
        .find(|e| e.data["status"] != json!("completed"))
        .expect("a started agent:tool:call event");
    assert!(
        started.data.get("resultBlockId").is_none(),
        "a call with no result block yet carries no resultBlockId: {:?}",
        started.data
    );
}

/// monorepo#2029, shape (b): PARALLEL calls (Claude Code's normal mode). Both
/// `tool_use` blocks land before either result, so `tool_use + 1` named the
/// SECOND call's `tool_use` — live, t2's tool row was overwritten by t1's
/// result. Each completion event now names the result id its own block took.
#[tokio::test]
async fn tool_call_events_carry_real_result_block_ids_for_parallel_completions() {
    let (_tmp, services, bus, agent_id, workspace_id) = setup().await;
    let mut sub = bus.subscribe(SubscriptionFilter::default());
    let mut transcript = super::Transcript::new("m1".to_string());
    services.set_live_turn(&agent_id, "m1", Vec::new());

    for note in [
        message_note("Running both. "),
        tool_call_note("t1"),
        tool_call_note("t2"),
        tool_done_note("t1"),
        tool_done_note("t2"),
    ] {
        services
            .route_notification(&note, &agent_id, &workspace_id, &mut transcript)
            .await;
    }

    let blocks = transcript.into_blocks();
    let shape: Vec<(&str, &str)> = blocks
        .iter()
        .map(|b| (b["type"].as_str().unwrap(), b["id"].as_str().unwrap()))
        .collect();
    assert_eq!(
        shape,
        vec![
            ("text", "m1:0"),
            ("tool_use", "m1:1"),
            ("tool_use", "m1:2"),
            ("tool_result", "m1:3"),
            ("tool_result", "m1:4"),
        ]
    );

    let events = drain_tool_call_events(&mut sub).await;
    assert_result_ids_match_transcript(&events, &blocks);
    let result_id_for = |tool_call_id: &str| -> Value {
        events
            .iter()
            .find(|e| {
                e.data["toolCallId"] == json!(tool_call_id)
                    && e.data["status"] == json!("completed")
            })
            .expect("completion event")
            .data["resultBlockId"]
            .clone()
    };
    assert_eq!(
        result_id_for("t1"),
        json!("m1:3"),
        "t1's result is m1:3 — m1:2 belongs to t2's tool_use"
    );
    assert_eq!(result_id_for("t2"), json!("m1:4"));
}

fn tool_call_note(id: &str) -> IncomingNotification {
    IncomingNotification {
        method: "session/update".to_string(),
        params: json!({
            "sessionId": ACP_SID,
            "update": { "sessionUpdate": "tool_call", "toolCallId": id,
                "title": "bash: x", "kind": "execute", "status": "in_progress",
                "rawInput": { "cmd": "x" } }
        }),
    }
}

fn tool_done_note(id: &str) -> IncomingNotification {
    IncomingNotification {
        method: "session/update".to_string(),
        params: json!({
            "sessionId": ACP_SID,
            "update": { "sessionUpdate": "tool_call_update", "toolCallId": id,
                "status": "completed", "rawOutput": { "summary": "ok" } }
        }),
    }
}

async fn drain_tool_call_events(sub: &mut crate::Subscription) -> Vec<Event> {
    let mut events: Vec<Event> = Vec::new();
    while let Ok(Some(batch)) = timeout(Duration::from_millis(300), sub.recv()).await {
        events.extend(batch);
    }
    events.retain(|e| e.event_type == "agent:tool:call");
    assert!(!events.is_empty(), "the turn published tool call events");
    events
}

/// Every id an `agent:tool:call` event carries resolves to the block of the
/// matching type in the DURABLE transcript — the live/persisted parity the
/// `chat.subscribe` mapper relies on now that it stamps these ids verbatim.
fn assert_result_ids_match_transcript(events: &[Event], blocks: &[Value]) {
    for e in events {
        let block_id = e.data["blockId"].as_str().expect("blockId");
        let use_block = blocks
            .iter()
            .find(|b| b["id"] == json!(block_id))
            .unwrap_or_else(|| panic!("blockId {block_id} exists in the transcript"));
        assert_eq!(use_block["type"], json!("tool_use"));
        assert_eq!(use_block["toolCallId"], e.data["toolCallId"]);
        let Some(result_id) = e.data.get("resultBlockId").and_then(Value::as_str) else {
            continue;
        };
        let result_block = blocks
            .iter()
            .find(|b| b["id"] == json!(result_id))
            .unwrap_or_else(|| panic!("resultBlockId {result_id} exists in the transcript"));
        assert_eq!(
            result_block["type"],
            json!("tool_result"),
            "resultBlockId {result_id} names a tool_result, not {:?}",
            result_block["type"]
        );
        assert_eq!(
            result_block["tool_use_id"], e.data["toolCallId"],
            "resultBlockId {result_id} names THIS call's result"
        );
    }
}
