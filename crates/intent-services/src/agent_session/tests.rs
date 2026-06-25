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

/// Mock agent: answers the lifecycle methods; `session/prompt` streams two text
/// chunks and one tool call, then resolves with `end_turn`.
fn spawn_mock_agent<R, W>(read: R, write: W) -> JoinHandle<()>
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
                for note in prompt_updates() {
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

/// Wire a `Connection` to a fresh mock agent, returning the connection, its
/// notification receiver, and the agent task handle.
fn connect() -> (
    Connection,
    mpsc::UnboundedReceiver<IncomingNotification>,
    JoinHandle<()>,
) {
    let (c2a_client, c2a_agent) = tokio::io::duplex(16 * 1024);
    let (a2c_agent, a2c_client) = tokio::io::duplex(16 * 1024);
    let agent = spawn_mock_agent(c2a_agent, a2c_agent);
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
        archived: false,
        archived_at: None,
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
        status: AgentStatus::Pending,
        is_active: true,
        messages: Vec::new(),
        stats: None,
        created_at: ts.clone(),
        updated_at: ts,
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
    let mut events = Vec::new();
    while events.len() < 5 {
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
            "agent:stream:chunk",
            "agent:stream:chunk",
            "agent:tool:call",
            "agent:stream:end",
            "agent:idle",
        ],
        "a normal turn emits exactly one agent:idle after the terminal stream:end"
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

    let tool = events
        .iter()
        .find(|e| e.event_type == "agent:tool:call")
        .unwrap();
    assert_eq!(tool.data["toolKind"], json!("file"));
    assert_eq!(tool.data["status"], json!("started"));
    assert_eq!(tool.data["input"], json!({ "path": "src/lib.rs" }));

    // Chunks accumulate into one assistant message (coalesced text).
    let messages = bus
        .store()
        .get_agent_messages(&agent_id, None)
        .await
        .expect("read messages");
    assert_eq!(messages.len(), 1);
    assert_eq!(messages[0].role, "assistant");
    assert_eq!(
        messages[0].content,
        json!([{ "type": "text", "text": "Hello world" }])
    );
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
    while events.len() < 5 {
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
            "agent:stream:chunk",
            "agent:stream:chunk",
            "agent:tool:call",
            "agent:stream:end",
            "agent:idle",
        ],
        "the real turn streams its own updates (then goes idle) after the replay was dropped"
    );

    let messages = bus
        .store()
        .get_agent_messages(&agent_id, None)
        .await
        .expect("read messages");
    assert_eq!(messages.len(), 1, "only the real turn is accumulated");
    assert_eq!(
        messages[0].content,
        json!([{ "type": "text", "text": "Hello world" }])
    );
}

#[tokio::test]
async fn open_acp_session_persists_id() {
    let (_tmp, services, bus, agent_id, _ws) = setup().await;
    let (conn, _rx, _agent) = connect();
    let sid = services
        .open_acp_session(&conn, &agent_id, "/tmp/ws", Vec::new())
        .await
        .expect("open session");
    assert_eq!(sid, ACP_SID);
    let stored = bus.store().get_agent_session(&agent_id).await.unwrap();
    assert_eq!(stored.acp_session_id.as_deref(), Some(ACP_SID));
}

#[tokio::test]
async fn resume_requires_capability_and_stored_id() {
    let (_tmp, services, bus, agent_id, _ws) = setup().await;
    let (conn, _rx, _agent) = connect();

    // No stored acpSessionId yet → None even with the capability.
    assert_eq!(
        services
            .resume_acp_session(&conn, &init_caps(true), &agent_id, "/tmp/ws", Vec::new())
            .await
            .unwrap(),
        None
    );

    bus.store()
        .set_acp_session_id(&agent_id, ACP_SID)
        .await
        .unwrap();

    // Stored id but the agent lacks loadSession → None.
    assert_eq!(
        services
            .resume_acp_session(&conn, &init_caps(false), &agent_id, "/tmp/ws", Vec::new())
            .await
            .unwrap(),
        None
    );

    // Stored id + capability → resumes.
    assert_eq!(
        services
            .resume_acp_session(&conn, &init_caps(true), &agent_id, "/tmp/ws", Vec::new())
            .await
            .unwrap(),
        Some(ACP_SID.to_string())
    );

    // A successful resume keeps the stored id canonical (no overwrite).
    let stored = bus.store().get_agent_session(&agent_id).await.unwrap();
    assert_eq!(stored.acp_session_id.as_deref(), Some(ACP_SID));
}

#[tokio::test]
async fn recreate_acp_session_replaces_stored_id() {
    let (_tmp, services, bus, agent_id, _ws) = setup().await;
    let (conn, _rx, _agent) = connect();

    // A stale id is persisted (the resume-impossible fallback case).
    bus.store()
        .set_acp_session_id(&agent_id, "stale-id")
        .await
        .unwrap();

    // recreate opens a fresh session and CAS-swaps the lost id for the new one.
    let sid = services
        .recreate_acp_session(&conn, &agent_id, "stale-id", "/tmp/ws", Vec::new())
        .await
        .expect("recreate session");
    assert_eq!(sid, ACP_SID);
    let stored = bus.store().get_agent_session(&agent_id).await.unwrap();
    assert_eq!(stored.acp_session_id.as_deref(), Some(ACP_SID));

    // No-clobber: recreating again with a stale expected-old reuses the stored
    // canonical id rather than overwriting it (a second session/new is opened
    // but the CAS declines to swap).
    let sid = services
        .recreate_acp_session(&conn, &agent_id, "stale-id", "/tmp/ws", Vec::new())
        .await
        .expect("recreate session");
    assert_eq!(sid, ACP_SID, "diverged expected-old keeps the canonical id");
    let stored = bus.store().get_agent_session(&agent_id).await.unwrap();
    assert_eq!(stored.acp_session_id.as_deref(), Some(ACP_SID));
}
