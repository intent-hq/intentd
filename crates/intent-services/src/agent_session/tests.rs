//! Driver tests over a temp SQLite store + a mock ACP agent: a prompt turn
//! accumulates chunks, publishes events in order with a single terminal
//! `stream:end`, persists `acpSessionId`, and gates resume on the capability.

use std::path::PathBuf;
use std::time::Duration;

use intent_acp::session::{ContentBlock, InitializeResponse};
use intent_acp::{Connection, ConnectionHooks, IncomingNotification};
use intent_core::{
    now_iso, AgentId, AgentSession, AgentStatus, Workspace, WorkspaceActivity, WorkspaceAttention,
    WorkspaceId, WorkspaceStatus,
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
/// caller-supplied `session/update` burst, then resolves with `end_turn`.
fn spawn_mock_agent_with<R, W>(read: R, write: W, updates: Vec<String>) -> JoinHandle<()>
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

/// Wire a `Connection` to a fresh mock agent, returning the connection, its
/// notification receiver, and the agent task handle.
fn connect() -> (
    Connection,
    mpsc::UnboundedReceiver<IncomingNotification>,
    JoinHandle<()>,
) {
    connect_with(prompt_updates())
}

/// [`connect`] with a caller-supplied prompt-update burst.
fn connect_with(
    updates: Vec<String>,
) -> (
    Connection,
    mpsc::UnboundedReceiver<IncomingNotification>,
    JoinHandle<()>,
) {
    let (c2a_client, c2a_agent) = tokio::io::duplex(16 * 1024);
    let (a2c_agent, a2c_client) = tokio::io::duplex(16 * 1024);
    let agent = spawn_mock_agent_with(c2a_agent, a2c_agent, updates);
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
        )
        .await
        .expect("turn completes");
    assert_eq!(serde_json::to_value(stop).unwrap(), json!("end_turn"));

    // Collect the published events (default filter → one event per batch).
    // The turn also emits a `prompt` status hint before the first chunk
    // (STAT-1 / PROTOCOL §7 pre-first-token status family), so expect one
    // extra frame ahead of the chunk/tool/end/idle sequence.
    let mut events = Vec::new();
    while events.len() < 6 {
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
            "agent:stream:chunk",
            "agent:stream:chunk",
            "agent:tool:call",
            "agent:stream:end",
            "agent:idle",
        ],
        "a normal turn emits the `prompt` status hint before the first chunk and exactly one agent:idle after the terminal stream:end"
    );

    // The pre-first-token status hint carries the "Sent prompt…" phrase and
    // arrives BEFORE any `agent:stream:chunk` so the FE spinner can render it
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
    // so `report` is absent (only present when a delegated child called
    // `agent.reportToParent`).
    assert_eq!(idle.data["agentName"], json!("Builder"));
    assert!(
        idle.data.get("report").is_none(),
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

    // The streaming chunk events carry the SAME stable block id across both
    // text chunks; the persisted message id is the block-id prefix (D1/D4).
    let chunks: Vec<_> = events
        .iter()
        .filter(|e| e.event_type == "agent:stream:chunk")
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
}

/// DELIV-1: when the session carries a `completion_report` (persisted by
/// `agent.reportToParent` on a delegated child), the terminal
/// `agent:idle` payload includes it as `report` alongside the enriched
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
        )
        .await
        .expect("turn completes");
    assert_eq!(serde_json::to_value(stop).unwrap(), json!("end_turn"));

    let mut events = Vec::new();
    while events.len() < 6 {
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
            "agent:stream:chunk",
            "agent:stream:chunk",
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
        .resume_acp_session(
            &conn,
            &init_caps(true),
            &agent_id,
            "/tmp/ws",
            Vec::new()
        )
        .await
        .unwrap()
        .is_none());

    bus.store()
        .set_acp_session_id(&ws, &agent_id, ACP_SID)
        .await
        .unwrap();

    // Stored id but the agent lacks loadSession → None.
    assert!(services
        .resume_acp_session(
            &conn,
            &init_caps(false),
            &agent_id,
            "/tmp/ws",
            Vec::new()
        )
        .await
        .unwrap()
        .is_none());

    // Stored id + capability → resumes.
    let opened = services
        .resume_acp_session(
            &conn,
            &init_caps(true),
            &agent_id,
            "/tmp/ws",
            Vec::new(),
        )
        .await
        .unwrap()
        .expect("resume yields opened session");
    assert_eq!(opened.session_id, ACP_SID);

    // A successful resume keeps the stored id canonical (no overwrite).
    let stored = bus.store().get_agent_session(&agent_id).await.unwrap();
    assert_eq!(stored.acp_session_id.as_deref(), Some(ACP_SID));
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
        .recreate_acp_session(
            &conn,
            &agent_id,
            "stale-id",
            "/tmp/ws",
            Vec::new(),
        )
        .await
        .expect("recreate session");
    assert_eq!(opened.session_id, ACP_SID);
    let stored = bus.store().get_agent_session(&agent_id).await.unwrap();
    assert_eq!(stored.acp_session_id.as_deref(), Some(ACP_SID));

    // No-clobber: recreating again with a stale expected-old reuses the stored
    // canonical id rather than overwriting it (a second session/new is opened
    // but the CAS declines to swap).
    let opened = services
        .recreate_acp_session(
            &conn,
            &agent_id,
            "stale-id",
            "/tmp/ws",
            Vec::new(),
        )
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
        .resume_acp_session(
            &conn,
            &init_caps(true),
            &agent_id,
            "/tmp/ws",
            Vec::new(),
        )
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
        .recreate_acp_session(
            &conn,
            &agent_id,
            "stale-id",
            "/tmp/ws",
            Vec::new(),
        )
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
