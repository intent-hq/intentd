//! Transport + handshake tests against an in-memory mock agent and (on unix) a
//! real `sh` mock child process (§6.2–§6.4 DoD).

use std::time::Duration;

use serde_json::{json, Value};
use tokio::io::{AsyncBufReadExt, AsyncRead, AsyncWrite, AsyncWriteExt, BufReader};
use tokio::sync::mpsc;
use tokio::task::JoinHandle;

use crate::transport::{Connection, ConnectionHooks};

/// Mock agent: read JSON-RPC lines, assert each frame parses cleanly (whole-line
/// atomicity), and echo a result for any request. Returns every frame it saw.
fn spawn_responder<R, W>(read: R, write: W) -> JoinHandle<Vec<Value>>
where
    R: AsyncRead + Unpin + Send + 'static,
    W: AsyncWrite + Unpin + Send + 'static,
{
    tokio::spawn(async move {
        let mut lines = BufReader::new(read).lines();
        let mut write = write;
        let mut seen = Vec::new();
        while let Ok(Some(line)) = lines.next_line().await {
            if line.trim().is_empty() {
                continue;
            }
            let value: Value =
                serde_json::from_str(&line).expect("agent received a non-JSON (interleaved) line");
            seen.push(value.clone());
            if let (Some(id), Some(method)) =
                (value.get("id"), value.get("method").and_then(Value::as_str))
            {
                let result = match method {
                    "initialize" => json!({
                        "protocolVersion": 1,
                        "agentCapabilities": { "loadSession": true }
                    }),
                    _ => json!({}),
                };
                let resp = json!({ "jsonrpc": "2.0", "id": id, "result": result });
                let frame = format!("{resp}\n");
                write.write_all(frame.as_bytes()).await.unwrap();
                write.flush().await.unwrap();
            }
        }
        seen
    })
}

/// Build a `Connection` wired to a fresh in-memory mock agent. Returns the
/// connection, the responder handle, and the agent-side stderr writer.
fn connect_mock(
    hooks: ConnectionHooks,
) -> (Connection, JoinHandle<Vec<Value>>, tokio::io::DuplexStream) {
    let (c2a_client, c2a_agent) = tokio::io::duplex(8 * 1024);
    let (a2c_agent, a2c_client) = tokio::io::duplex(8 * 1024);
    let (stderr_w, stderr_r) = tokio::io::duplex(8 * 1024);
    let responder = spawn_responder(c2a_agent, a2c_agent);
    let conn = Connection::new(c2a_client, a2c_client, Some(Box::new(stderr_r)), hooks);
    (conn, responder, stderr_w)
}

#[tokio::test]
async fn handshake_completes() {
    let provider = intent_providers::find_provider("auggie").unwrap();
    let (conn, _responder, _stderr) = connect_mock(ConnectionHooks::default());

    let result = crate::handshake::handshake(&conn, provider).await.unwrap();
    assert!(result.authenticated, "auggie supports authenticate");
    assert_eq!(
        serde_json::to_value(result.initialize.protocol_version).unwrap(),
        json!(1)
    );
}

#[tokio::test]
async fn concurrent_writes_do_not_interleave() {
    let (conn, responder, _stderr) = connect_mock(ConnectionHooks::default());
    let conn = std::sync::Arc::new(conn);

    // Large params (> PIPE_BUF) fired concurrently must each arrive as one
    // intact line — the responder's parse asserts this.
    let big = "x".repeat(16 * 1024);
    let mut handles = Vec::new();
    for i in 0..24 {
        let conn = std::sync::Arc::clone(&conn);
        let big = big.clone();
        handles.push(tokio::spawn(async move {
            conn.request("session/ping", json!({ "i": i, "blob": big }))
                .await
        }));
    }
    for handle in handles {
        handle.await.unwrap().expect("each request resolves");
    }

    // Drop the connection to close the agent's read side, then collect frames.
    drop(conn);
    let seen = responder.await.unwrap();
    assert_eq!(seen.len(), 24, "every request line was received intact");
}

#[tokio::test]
async fn routes_requests_and_notifications() {
    let (req_tx, mut req_rx) = mpsc::unbounded_channel();
    let (note_tx, mut note_rx) = mpsc::unbounded_channel();
    let hooks = ConnectionHooks {
        requests: Some(req_tx),
        notifications: Some(note_tx),
        auth_error_patterns: Vec::new(),
    };

    let (c2a_client, _c2a_agent) = tokio::io::duplex(4096);
    let (mut a2c_agent, a2c_client) = tokio::io::duplex(4096);
    let conn = Connection::new(c2a_client, a2c_client, None, hooks);

    // Agent → client request, then a notification.
    a2c_agent
        .write_all(b"{\"jsonrpc\":\"2.0\",\"id\":\"p1\",\"method\":\"fs/read_text_file\",\"params\":{\"path\":\"/x\"}}\n")
        .await
        .unwrap();
    a2c_agent
        .write_all(b"{\"jsonrpc\":\"2.0\",\"method\":\"session/update\",\"params\":{\"k\":1}}\n")
        .await
        .unwrap();
    a2c_agent.flush().await.unwrap();

    let req = tokio::time::timeout(Duration::from_secs(2), req_rx.recv())
        .await
        .unwrap()
        .unwrap();
    assert_eq!(req.method, "fs/read_text_file");
    assert_eq!(req.id, json!("p1"));

    let note = tokio::time::timeout(Duration::from_secs(2), note_rx.recv())
        .await
        .unwrap()
        .unwrap();
    assert_eq!(note.method, "session/update");
    let _ = conn;
}

#[tokio::test]
async fn stderr_captured_and_auth_flagged() {
    let hooks = ConnectionHooks {
        auth_error_patterns: vec!["authentication required".to_string()],
        ..ConnectionHooks::default()
    };
    let (conn, _responder, mut stderr_w) = connect_mock(hooks);

    stderr_w.write_all(b"loading model\n").await.unwrap();
    stderr_w
        .write_all(b"error: Authentication Required: run auggie login\n")
        .await
        .unwrap();
    stderr_w.flush().await.unwrap();

    // Poll briefly for the stderr task to drain.
    let mut captured = Vec::new();
    for _ in 0..50 {
        captured = conn.recent_stderr();
        if conn.auth_error_detected() && captured.len() >= 2 {
            break;
        }
        tokio::time::sleep(Duration::from_millis(10)).await;
    }
    assert!(conn.auth_error_detected(), "auth pattern matched on stderr");
    assert_eq!(captured.len(), 2);
    assert!(captured[1].contains("auggie login"));
}

/// Minimal portable ACP responder: echoes a `{protocolVersion:1}` result for
/// every line carrying an integer `id`. Used as a real mock child process.
#[cfg(unix)]
const MOCK_SH: &str = r#"while IFS= read -r line; do
  id=$(printf '%s' "$line" | sed -n 's/.*"id":\([0-9][0-9]*\).*/\1/p')
  if [ -n "$id" ]; then
    printf '{"jsonrpc":"2.0","id":%s,"result":{"protocolVersion":1}}\n' "$id"
  fi
done"#;

#[cfg(unix)]
#[tokio::test]
async fn spawn_real_child_handshake() {
    use crate::spawn::{spawn_provider, SpawnOptions};

    // Base the config on auggie (supports authenticate) but run `sh` with our
    // inline responder script as the command.
    let base = *intent_providers::find_provider("auggie").unwrap();
    let provider = intent_providers::ProviderConfig {
        command: "sh",
        base_args: &["-c", MOCK_SH],
        model_flag: None,
        rules_flag: None,
        mcp_config_flag: None,
        quiet_flag: None,
        supports_mcp_config: false,
        supports_rules_file: false,
        ..base
    };

    let opts = SpawnOptions::new(&provider);
    let mut agent = spawn_provider(&opts, ConnectionHooks::default()).expect("spawn sh mock");

    let result = crate::handshake::handshake(agent.connection(), &provider)
        .await
        .expect("handshake over real child");
    assert!(result.authenticated);
    assert_eq!(
        serde_json::to_value(result.initialize.protocol_version).unwrap(),
        json!(1)
    );

    agent.kill().await.ok();
}

/// Session-lifecycle + streaming-mapping tests (§6.5/§6.6 DoD).
mod session_tests {
    use super::*;

    use agent_client_protocol::schema::{
        ContentBlock, SessionNotification, SessionUpdate, TextContent, ToolCall, ToolCallStatus,
        ToolCallUpdate, ToolCallUpdateFields, ToolKind,
    };

    use crate::session::{self, MappedUpdate};
    use crate::IncomingNotification;

    /// Mock agent that answers the session lifecycle methods and records every
    /// frame it received. `session/cancel` is a notification (no id) → no reply.
    fn spawn_session_responder<R, W>(read: R, write: W) -> JoinHandle<Vec<Value>>
    where
        R: AsyncRead + Unpin + Send + 'static,
        W: AsyncWrite + Unpin + Send + 'static,
    {
        tokio::spawn(async move {
            let mut lines = BufReader::new(read).lines();
            let mut write = write;
            let mut seen = Vec::new();
            while let Ok(Some(line)) = lines.next_line().await {
                if line.trim().is_empty() {
                    continue;
                }
                let value: Value = serde_json::from_str(&line).expect("agent received valid JSON");
                seen.push(value.clone());
                let (Some(id), Some(method)) =
                    (value.get("id"), value.get("method").and_then(Value::as_str))
                else {
                    continue;
                };
                let result = match method {
                    "initialize" => {
                        json!({ "protocolVersion": 1, "agentCapabilities": { "loadSession": true } })
                    }
                    "session/new" => json!({ "sessionId": "acp-session-1" }),
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
            seen
        })
    }

    fn connect_session() -> (Connection, JoinHandle<Vec<Value>>) {
        let (c2a_client, c2a_agent) = tokio::io::duplex(8 * 1024);
        let (a2c_agent, a2c_client) = tokio::io::duplex(8 * 1024);
        let responder = spawn_session_responder(c2a_agent, a2c_agent);
        let conn = Connection::new(c2a_client, a2c_client, None, ConnectionHooks::default());
        (conn, responder)
    }

    #[tokio::test]
    async fn new_session_returns_acp_session_id() {
        let (conn, _responder) = connect_session();
        let resp = session::new_session(&conn, "/tmp/ws", Vec::new())
            .await
            .expect("session/new succeeds");
        assert_eq!(resp.session_id.0.as_ref(), "acp-session-1");
    }

    #[tokio::test]
    async fn prompt_returns_stop_reason_and_load_caps_detected() {
        let provider = intent_providers::find_provider("auggie").unwrap();
        let (conn, _responder) = connect_session();
        let handshake = crate::handshake::handshake(&conn, provider).await.unwrap();
        assert!(session::supports_load_session(&handshake.initialize));

        session::load_session(&conn, "acp-session-1", "/tmp/ws", Vec::new())
            .await
            .expect("session/load succeeds when capability present");

        let block = ContentBlock::Text(TextContent::new("hello"));
        let stop = session::prompt(&conn, "acp-session-1", vec![block])
            .await
            .expect("session/prompt resolves");
        assert_eq!(
            serde_json::to_value(stop).unwrap(),
            json!("end_turn"),
            "stop reason round-trips as end_turn"
        );
    }

    #[tokio::test]
    async fn cancel_sends_session_cancel_notification() {
        let (conn, responder) = connect_session();
        session::cancel(&conn, "acp-session-1")
            .await
            .expect("cancel notification sent");
        // Await a follow-up request: the single FIFO writer guarantees the
        // cancel line was flushed before this response arrives (avoids racing
        // the writer task against the drop below).
        session::new_session(&conn, "/tmp/ws", Vec::new())
            .await
            .expect("follow-up request flushes the cancel line");
        // Drop the connection to close the agent read side, then inspect frames.
        drop(conn);
        let seen = responder.await.unwrap();
        let cancel = seen
            .iter()
            .find(|f| f.get("method").and_then(Value::as_str) == Some("session/cancel"))
            .expect("agent received session/cancel");
        assert!(cancel.get("id").is_none(), "cancel is a notification");
        assert_eq!(cancel["params"]["sessionId"], json!("acp-session-1"));
    }

    #[test]
    fn maps_message_chunk_to_stream_chunk() {
        let update =
            SessionUpdate::AgentMessageChunk(agent_client_protocol::schema::ContentChunk::new(
                ContentBlock::Text(TextContent::new("tok")),
            ));
        assert_eq!(
            session::map_session_update(&update),
            Some(MappedUpdate::Chunk {
                content: json!("tok"),
                text: Some("tok".to_string()),
            })
        );
    }

    #[test]
    fn maps_tool_call_started_with_tool_kind() {
        let call = ToolCall::new("t1", "Edit src/lib.rs")
            .kind(ToolKind::Edit)
            .raw_input(json!({ "path": "src/lib.rs" }));
        let mapped = session::map_session_update(&SessionUpdate::ToolCall(call)).unwrap();
        let MappedUpdate::ToolCall(tc) = mapped else {
            panic!("expected tool call");
        };
        assert_eq!(tc.tool_kind, "file");
        assert_eq!(tc.status, "started");
        assert_eq!(tc.tool_name, "Edit src/lib.rs");
        assert_eq!(tc.input, json!({ "path": "src/lib.rs" }));
    }

    #[test]
    fn maps_tool_call_update_completed_and_error() {
        let done = ToolCallUpdate::new(
            "t1",
            ToolCallUpdateFields::new()
                .status(ToolCallStatus::Completed)
                .kind(ToolKind::Search),
        );
        let MappedUpdate::ToolCall(tc) =
            session::map_session_update(&SessionUpdate::ToolCallUpdate(done)).unwrap()
        else {
            panic!("expected tool call");
        };
        assert_eq!(tc.status, "completed");
        assert_eq!(tc.tool_kind, "search");

        let failed = ToolCallUpdate::new(
            "t1",
            ToolCallUpdateFields::new().status(ToolCallStatus::Failed),
        );
        let MappedUpdate::ToolCall(tc) =
            session::map_session_update(&SessionUpdate::ToolCallUpdate(failed)).unwrap()
        else {
            panic!("expected tool call");
        };
        assert_eq!(tc.status, "error");
    }

    #[test]
    fn unmapped_variants_return_none() {
        let thought =
            SessionUpdate::AgentThoughtChunk(agent_client_protocol::schema::ContentChunk::new(
                ContentBlock::Text(TextContent::new("thinking")),
            ));
        assert_eq!(session::map_session_update(&thought), None);
    }

    #[test]
    fn map_notification_parses_session_update() {
        let note = SessionNotification::new(
            "acp-session-1",
            SessionUpdate::AgentMessageChunk(agent_client_protocol::schema::ContentChunk::new(
                ContentBlock::Text(TextContent::new("hi")),
            )),
        );
        let incoming = IncomingNotification {
            method: "session/update".to_string(),
            params: serde_json::to_value(&note).unwrap(),
        };
        assert_eq!(
            session::map_notification(&incoming),
            Some(MappedUpdate::Chunk {
                content: json!("hi"),
                text: Some("hi".to_string()),
            })
        );

        let other = IncomingNotification {
            method: "session/other".to_string(),
            params: json!({}),
        };
        assert_eq!(session::map_notification(&other), None);
    }
}

/// Agent→BE MCP server, config conversions, env baseline/redaction, and the
/// per-agent-type tool denylist (§6.8 / §18.4 DoD).
mod mcp_tests {
    use std::sync::{Arc, Mutex};

    use intent_core::{
        AgentId, BoxFuture, GitAgentCommitResult, Note, NoteAddInput, NoteAddResult, NoteCreate,
        NoteId, Result, WorkspaceApi, WorkspaceId,
    };
    use serde_json::{json, Value};

    use crate::mcp_config::{
        apply_baseline_env_to_stdio_servers, normalize_mcp_servers, to_acp_mcp_servers,
        to_auggie_mcp_config, to_claude_mcp_json, to_codex_mcp_overrides, to_opencode_mcp_config,
        NormalizedMcpServer,
    };
    use crate::mcp_env::{
        build_baseline_mcp_env, is_likely_secret_env_key, merge_mcp_env,
        redact_mcp_env_for_logging, EnvMap, REDACTED_VALUE,
    };
    use crate::tool_restrictions::{
        get_tool_denylist_for_agent_type, AGENT_CREATION_TOOLS, SUBAGENT_TOOLS,
    };
    use crate::WorkspaceMcpServer;

    /// A `WorkspaceApi` that records the `add_to_note` calls it receives so a tool
    /// call through the MCP server can be observed as a state change.
    /// A recorded `git_agent_commit` call: (message, agent_id, linked_note_id).
    type CommitRecord = (String, Option<String>, Option<String>);

    #[derive(Default)]
    pub(super) struct MockApi {
        pub(super) added: Mutex<Vec<(String, String)>>,
        pub(super) committed: Mutex<Vec<CommitRecord>>,
        /// Recorded `create_note` calls: (title, idempotency_key).
        pub(super) created: Mutex<Vec<(String, Option<String>)>>,
    }

    impl WorkspaceApi for MockApi {
        fn create_note(
            &self,
            workspace_id: WorkspaceId,
            input: NoteCreate,
            idempotency_key: Option<String>,
        ) -> BoxFuture<'_, Result<Note>> {
            self.created
                .lock()
                .unwrap()
                .push((input.title.clone(), idempotency_key));
            Box::pin(async move {
                Ok(Note {
                    id: NoteId::from_string("n-created"),
                    workspace_id,
                    title: input.title,
                    content: input.content.unwrap_or_default(),
                    content_type: Default::default(),
                    tags: input.tags.unwrap_or_default(),
                    is_pinned: false,
                    is_archived: false,
                    is_default: false,
                    parent_id: None,
                    visibility: Default::default(),
                    task: None,
                    created_at: "2026-01-01T00:00:00Z".to_string(),
                    rev: 0,
                    updated_at: "2026-01-01T00:00:00Z".to_string(),
                })
            })
        }

        fn list_notes<'a>(
            &'a self,
            _workspace_id: &'a WorkspaceId,
        ) -> BoxFuture<'a, Result<Vec<Note>>> {
            Box::pin(async { Ok(Vec::new()) })
        }

        fn git_agent_commit(
            &self,
            _workspace_id: WorkspaceId,
            message: String,
            agent_id: Option<AgentId>,
            linked_note_id: Option<NoteId>,
            _files: Option<Vec<String>>,
            _user_requested: bool,
        ) -> BoxFuture<'_, Result<GitAgentCommitResult>> {
            self.committed.lock().unwrap().push((
                message,
                agent_id.as_ref().map(|a| a.as_str().to_string()),
                linked_note_id.as_ref().map(|n| n.as_str().to_string()),
            ));
            Box::pin(async move {
                Ok(GitAgentCommitResult {
                    hash: "deadbeef".to_string(),
                    files: vec!["a.txt".to_string()],
                    file_count: 1,
                })
            })
        }

        fn add_to_note(
            &self,
            _workspace_id: WorkspaceId,
            note_id: NoteId,
            input: NoteAddInput,
        ) -> BoxFuture<'_, Result<NoteAddResult>> {
            self.added
                .lock()
                .unwrap()
                .push((note_id.to_string(), input.content.clone()));
            Box::pin(async move {
                let len = input.content.len();
                Ok(NoteAddResult {
                    ok: true,
                    note_id,
                    added_length: len,
                    total_length: len,
                    position: "at end".to_string(),
                    old_content: String::new(),
                    new_content: input.content,
                    converted_count: 0,
                    created_task_note_ids: Vec::new(),
                })
            })
        }
    }

    fn server(api: Arc<MockApi>) -> WorkspaceMcpServer {
        WorkspaceMcpServer::new(api, WorkspaceId::from_string("ws-1"))
    }

    /// Sibling test modules build their own `MockApi` instances through this helper
    /// so the unit-test fixture lives in exactly one place.
    pub(super) fn mock_api() -> Arc<MockApi> {
        Arc::new(MockApi::default())
    }

    #[tokio::test]
    async fn initialize_advertises_protocol_and_tools() {
        let srv = server(Arc::new(MockApi::default()));
        let resp = srv
            .handle_message(&json!({ "jsonrpc": "2.0", "id": 1, "method": "initialize" }))
            .await
            .unwrap();
        assert_eq!(resp["result"]["protocolVersion"], json!("2024-11-05"));
        assert_eq!(
            resp["result"]["capabilities"]["tools"]["listChanged"],
            json!(false)
        );
    }

    #[tokio::test]
    async fn tool_call_dispatches_to_workspace_api() {
        let api = Arc::new(MockApi::default());
        let srv = server(api.clone());
        let resp = srv
            .handle_message(&json!({
                "jsonrpc": "2.0",
                "id": 2,
                "method": "tools/call",
                "params": {
                    "name": "add_to_note_workspace-mcp",
                    "arguments": { "noteId": "n1", "content": "hello" }
                }
            }))
            .await
            .unwrap();
        // The BE state changed: the mock recorded the add.
        assert_eq!(
            *api.added.lock().unwrap(),
            vec![("n1".to_string(), "hello".to_string())]
        );
        // The MCP result wraps the service result as text content.
        assert_eq!(resp["result"]["isError"], json!(false));
        let text = resp["result"]["content"][0]["text"].as_str().unwrap();
        let parsed: Value = serde_json::from_str(text).unwrap();
        assert_eq!(parsed["ok"], json!(true));
        assert_eq!(parsed["newContent"], json!("hello"));
    }

    #[tokio::test]
    async fn git_commit_threads_caller_agent_id_as_attribution() {
        let api = Arc::new(MockApi::default());
        let srv = WorkspaceMcpServer::new(api.clone(), WorkspaceId::from_string("ws-1"))
            .with_caller_agent_id(Some(AgentId::from_string("agent-77")));
        let resp = srv
            .handle_message(&json!({
                "jsonrpc": "2.0", "id": 9, "method": "tools/call",
                "params": {
                    "name": "git_commit_workspace-mcp",
                    "arguments": { "message": "Add a" }
                }
            }))
            .await
            .unwrap();
        assert_eq!(resp["result"]["isError"], json!(false));
        let committed = api.committed.lock().unwrap();
        assert_eq!(
            *committed,
            vec![("Add a".to_string(), Some("agent-77".to_string()), None)]
        );
    }

    #[tokio::test]
    async fn git_commit_without_agent_context_errors() {
        let api = Arc::new(MockApi::default());
        let srv = server(api.clone());
        let resp = srv
            .handle_message(&json!({
                "jsonrpc": "2.0", "id": 10, "method": "tools/call",
                "params": {
                    "name": "git_commit_workspace-mcp",
                    "arguments": { "message": "Add a" }
                }
            }))
            .await
            .unwrap();
        assert_eq!(resp["error"]["code"], json!(-32602));
        assert!(resp["error"]["message"]
            .as_str()
            .unwrap()
            .contains("This tool must be called by an agent."));
        assert!(api.committed.lock().unwrap().is_empty());
    }

    #[tokio::test]
    async fn notification_yields_no_response() {
        let srv = server(Arc::new(MockApi::default()));
        let resp = srv
            .handle_message(&json!({ "jsonrpc": "2.0", "method": "notifications/initialized" }))
            .await;
        assert!(resp.is_none());
    }

    #[tokio::test]
    async fn unknown_tool_is_rejected() {
        let srv = server(Arc::new(MockApi::default()));
        let resp = srv
            .handle_message(&json!({
                "jsonrpc": "2.0", "id": 3, "method": "tools/call",
                "params": { "name": "does_not_exist", "arguments": {} }
            }))
            .await
            .unwrap();
        assert_eq!(resp["error"]["code"], json!(-32602));
    }

    #[tokio::test]
    async fn denylist_filters_tools_list_and_blocks_calls() {
        // commit-message denies every write category, including note writes.
        let srv = WorkspaceMcpServer::for_agent_type(
            Arc::new(MockApi::default()),
            WorkspaceId::from_string("ws-1"),
            "commit-message",
        );
        let names: Vec<String> = srv
            .available_tools()
            .iter()
            .map(|t| t.name.to_string())
            .collect();
        assert!(!names.contains(&"add_to_note_workspace-mcp".to_string()));
        assert!(!names.contains(&"delegate_task_workspace-mcp".to_string()));
        // Read tools remain available.
        assert!(names.contains(&"get_note_workspace-mcp".to_string()));

        let resp = srv
            .handle_message(&json!({
                "jsonrpc": "2.0", "id": 4, "method": "tools/call",
                "params": { "name": "add_to_note_workspace-mcp", "arguments": { "noteId": "n1", "content": "x" } }
            }))
            .await
            .unwrap();
        assert_eq!(resp["error"]["code"], json!(-32602));
    }

    #[test]
    fn task_loop_denies_only_subagent_tools() {
        let deny = get_tool_denylist_for_agent_type("task-loop");
        assert_eq!(deny, SUBAGENT_TOOLS.to_vec());
        // Interactive/foreground agents are unrestricted.
        assert!(get_tool_denylist_for_agent_type("interactive").is_empty());
        // Pure text agents deny note writes and file writes.
        let cm = get_tool_denylist_for_agent_type("commit-message");
        assert!(cm.contains(&"add_to_note_workspace-mcp"));
        assert!(cm.contains(&"str-replace-editor"));
    }

    #[test]
    fn report_to_parent_is_an_agent_creation_tool_and_denied_for_background_types() {
        // It lives in the agent-orchestration group alongside delegate/send.
        assert!(AGENT_CREATION_TOOLS.contains(&"report_to_parent_workspace-mcp"));
        // Pure-text background agents (full denylist) cannot call it.
        let cm = get_tool_denylist_for_agent_type("commit-message");
        assert!(cm.contains(&"report_to_parent_workspace-mcp"));
        // Interactive/foreground agents stay unrestricted.
        assert!(get_tool_denylist_for_agent_type("interactive").is_empty());
    }

    #[test]
    fn secret_env_detection_matches_ts() {
        for key in [
            "GITHUB_TOKEN",
            "OPENAI_API_KEY",
            "MY_SECRET",
            "AUTH_TOKEN",
            "DB_PASSWORD",
        ] {
            assert!(is_likely_secret_env_key(key), "{key} should be secret");
        }
        for key in ["PATH", "HOME", "LANG", "TOKENIZER"] {
            assert!(!is_likely_secret_env_key(key), "{key} should not be secret");
        }
    }

    #[test]
    fn baseline_env_drops_secrets_and_controlled_keys() {
        let mut parent = EnvMap::new();
        parent.insert("PATH".into(), "/usr/bin".into());
        parent.insert("HOME".into(), "/home/u".into());
        parent.insert("GITHUB_TOKEN".into(), "ghp_x".into());
        parent.insert("ELECTRON_RUN_AS_NODE".into(), "1".into());
        let baseline = build_baseline_mcp_env(&parent);
        assert_eq!(baseline.get("PATH").map(String::as_str), Some("/usr/bin"));
        assert!(baseline.contains_key("HOME"));
        assert!(!baseline.contains_key("GITHUB_TOKEN"));
        assert!(!baseline.contains_key("ELECTRON_RUN_AS_NODE"));
    }

    #[test]
    fn merge_env_later_layers_win() {
        let mut a = EnvMap::new();
        a.insert("K".into(), "1".into());
        a.insert("ONLY_A".into(), "a".into());
        let mut b = EnvMap::new();
        b.insert("K".into(), "2".into());
        let merged = merge_mcp_env(&[&a, &b]);
        assert_eq!(merged.get("K").map(String::as_str), Some("2"));
        assert_eq!(merged.get("ONLY_A").map(String::as_str), Some("a"));
    }

    #[test]
    fn redaction_masks_env_and_header_values() {
        let config = json!({
            "mcpServers": {
                "ws": { "command": "node", "args": ["x"], "env": { "TOKEN": "secret" } },
                "remote": { "type": "http", "url": "https://h", "headers": { "Authorization": "Bearer z" } }
            }
        });
        let redacted = redact_mcp_env_for_logging(&config);
        assert_eq!(
            redacted["mcpServers"]["ws"]["env"]["TOKEN"],
            json!(REDACTED_VALUE)
        );
        assert_eq!(redacted["mcpServers"]["ws"]["command"], json!("node"));
        assert_eq!(
            redacted["mcpServers"]["remote"]["headers"]["Authorization"],
            json!(REDACTED_VALUE)
        );
        // §11.3 secret hygiene: the raw secret values must never survive into the
        // serialized, log-bound form (keys + structure are preserved, values are not).
        let serialized = serde_json::to_string(&redacted).unwrap();
        assert!(
            !serialized.contains("secret"),
            "redacted MCP config leaked an env secret value: {serialized}"
        );
        assert!(
            !serialized.contains("Bearer z"),
            "redacted MCP config leaked a header secret value: {serialized}"
        );
    }

    fn stdio_servers() -> Value {
        json!({
            "ws": { "command": "node", "args": ["server.js"], "env": { "A": "1" } },
            "remote": { "type": "sse", "url": "https://h", "headers": { "X": "y" } }
        })
    }

    #[test]
    fn normalize_and_convert_to_provider_formats() {
        let normalized = normalize_mcp_servers(&stdio_servers());
        assert!(matches!(
            normalized.get("ws"),
            Some(NormalizedMcpServer::Stdio { .. })
        ));

        let acp = to_acp_mcp_servers(&normalized);
        let ws = acp.iter().find(|s| s["name"] == json!("ws")).unwrap();
        assert_eq!(ws["command"], json!("node"));
        assert_eq!(ws["env"], json!([{ "name": "A", "value": "1" }]));

        let opencode = to_opencode_mcp_config(&normalized);
        assert_eq!(opencode["ws"]["type"], json!("local"));
        assert_eq!(opencode["ws"]["command"], json!(["node", "server.js"]));

        let claude = to_claude_mcp_json(&normalized);
        assert_eq!(claude["mcpServers"]["ws"]["type"], json!("stdio"));

        let auggie = to_auggie_mcp_config(&normalized);
        assert_eq!(auggie["mcpServers"]["ws"]["command"], json!("node"));

        let codex = to_codex_mcp_overrides(&normalized);
        assert!(codex
            .iter()
            .any(|o| o.key == "mcp_servers.ws.command" && o.toml_value == "\"node\""));
        assert!(codex.iter().any(|o| o.key == "mcp_servers.ws.enabled"));
    }

    #[test]
    fn baseline_env_injected_into_stdio_servers_existing_wins() {
        let normalized = normalize_mcp_servers(&stdio_servers());
        let mut baseline = EnvMap::new();
        baseline.insert("PATH".into(), "/usr/bin".into());
        baseline.insert("A".into(), "baseline".into());
        let injected = apply_baseline_env_to_stdio_servers(&normalized, &baseline);
        let NormalizedMcpServer::Stdio { env, .. } = injected.get("ws").unwrap() else {
            panic!("ws is stdio");
        };
        assert_eq!(env.get("PATH").map(String::as_str), Some("/usr/bin"));
        // Server's own env wins over the baseline.
        assert_eq!(env.get("A").map(String::as_str), Some("1"));
        // Remote servers are untouched.
        assert!(matches!(
            injected.get("remote"),
            Some(NormalizedMcpServer::Sse { .. })
        ));
    }
}

/// Client-served handler tests: fs sandbox + events, permission resolve/timeout,
/// and the terminal stub (§6.7 / PROTOCOL §8 DoD).
mod client_served_tests {
    use super::*;

    use std::path::PathBuf;
    use std::sync::atomic::{AtomicU64, Ordering};
    use std::sync::{Arc, Mutex};

    use intent_core::{AgentId, BoxFuture, WorkspaceId};
    use tokio::io::DuplexStream;

    use crate::permission::{PermissionOutcome, PermissionPolicy, PermissionRegistry};
    use crate::transport::IncomingRequest;
    use crate::{ClientRequestHandler, EventSink, FileService, SinkEvent};

    /// Records every event published through the sink for assertions.
    #[derive(Default)]
    struct MockSink {
        events: Mutex<Vec<SinkEvent>>,
    }

    impl MockSink {
        fn types(&self) -> Vec<String> {
            self.events
                .lock()
                .unwrap()
                .iter()
                .map(|e| e.event_type.clone())
                .collect()
        }

        fn last_of(&self, event_type: &str) -> Option<Value> {
            self.events
                .lock()
                .unwrap()
                .iter()
                .rfind(|e| e.event_type == event_type)
                .map(|e| e.data.clone())
        }
    }

    impl EventSink for MockSink {
        fn publish(&self, event: SinkEvent) -> BoxFuture<'_, ()> {
            Box::pin(async move {
                self.events.lock().unwrap().push(event);
            })
        }
    }

    /// A unique, freshly created temp directory for sandbox tests.
    fn temp_dir() -> PathBuf {
        static COUNTER: AtomicU64 = AtomicU64::new(0);
        let n = COUNTER.fetch_add(1, Ordering::SeqCst);
        let nanos = std::time::SystemTime::now()
            .duration_since(std::time::UNIX_EPOCH)
            .unwrap()
            .as_nanos();
        let dir = std::env::temp_dir().join(format!("intent-acp-test-{nanos}-{n}"));
        std::fs::create_dir_all(&dir).unwrap();
        dir
    }

    /// Wire a `Connection` whose agent→client requests land on `req_rx`. Returns
    /// the connection, the request receiver, the agent-side writer (to send
    /// requests), and a buffered reader over the agent-side stdin (to read the
    /// handler's responses).
    fn connect_handler() -> (
        Connection,
        mpsc::UnboundedReceiver<IncomingRequest>,
        DuplexStream,
        BufReader<DuplexStream>,
    ) {
        let (c2a_client, c2a_agent) = tokio::io::duplex(16 * 1024);
        let (a2c_agent, a2c_client) = tokio::io::duplex(16 * 1024);
        let (req_tx, req_rx) = mpsc::unbounded_channel();
        let hooks = ConnectionHooks {
            requests: Some(req_tx),
            ..Default::default()
        };
        let conn = Connection::new(c2a_client, a2c_client, None, hooks);
        (conn, req_rx, a2c_agent, BufReader::new(c2a_agent))
    }

    fn build_handler(
        root: &std::path::Path,
        policy: PermissionPolicy,
        sink: Arc<MockSink>,
        registry: PermissionRegistry,
    ) -> (ClientRequestHandler, Arc<PermissionRegistry>) {
        let registry = Arc::new(registry);
        let handler = ClientRequestHandler::new(
            WorkspaceId::from_string("ws-1"),
            AgentId::from_string("agent-1"),
            "auggie",
            FileService::new(root),
            registry.clone(),
            policy,
            sink,
        );
        (handler, registry)
    }

    async fn send(
        writer: &mut DuplexStream,
        req_rx: &mut mpsc::UnboundedReceiver<IncomingRequest>,
        id: i64,
        method: &str,
        params: Value,
    ) -> IncomingRequest {
        let frame = json!({ "jsonrpc": "2.0", "id": id, "method": method, "params": params });
        writer
            .write_all(format!("{frame}\n").as_bytes())
            .await
            .unwrap();
        writer.flush().await.unwrap();
        tokio::time::timeout(Duration::from_secs(2), req_rx.recv())
            .await
            .unwrap()
            .unwrap()
    }

    async fn read_frame(reader: &mut BufReader<DuplexStream>) -> Value {
        let mut line = String::new();
        tokio::time::timeout(Duration::from_secs(2), reader.read_line(&mut line))
            .await
            .unwrap()
            .unwrap();
        serde_json::from_str(line.trim()).unwrap()
    }

    #[test]
    fn sandbox_rejects_traversal_but_allows_in_scope() {
        let root = temp_dir();
        let svc = FileService::new(&root);
        // §11.3 path sandboxing: every shape of `..` escape outside the worktree
        // must be rejected, including nested and trailing traversal that lexically
        // climbs above the root.
        for escape in [
            "../escape.txt",
            "../../escape.txt",
            "sub/../../escape.txt",
            "a/b/../../../escape.txt",
            "./../escape.txt",
        ] {
            assert!(
                svc.resolve(std::path::Path::new(escape)).is_err(),
                "traversal `{escape}` must escape the worktree and be rejected"
            );
        }
        assert!(
            svc.resolve(std::path::Path::new("/etc/passwd")).is_err(),
            "absolute path outside the worktree is rejected"
        );
        // In-scope `..` that stays within the root resolves and stays inside it.
        let ok = svc.resolve(std::path::Path::new("sub/ok.txt")).unwrap();
        assert!(ok.starts_with(&root), "in-scope path resolves inside root");
        let in_scope = svc
            .resolve(std::path::Path::new("a/b/../c.txt"))
            .expect("in-bounds traversal resolves");
        assert!(
            in_scope.starts_with(&root),
            "traversal that stays within the root remains sandboxed"
        );
    }

    #[test]
    fn headless_policy_default_denies_destructive_and_medium() {
        use crate::permission::{assess_risk_level, RiskLevel};

        // §11.3 permission gating: the headless default (`AutoByRisk`) auto-allows
        // only low-risk reads and DENIES anything destructive or unclassified.
        assert_eq!(
            PermissionPolicy::AutoByRisk.auto_allow(RiskLevel::Low),
            Some(true)
        );
        assert_eq!(
            PermissionPolicy::AutoByRisk.auto_allow(RiskLevel::Medium),
            Some(false),
            "medium/unclassified prompts default-deny in headless"
        );
        assert_eq!(
            PermissionPolicy::AutoByRisk.auto_allow(RiskLevel::High),
            Some(false),
            "destructive prompts default-deny in headless"
        );
        // `DenyAll` denies every risk; `Interactive` surfaces every prompt.
        for risk in [RiskLevel::Low, RiskLevel::Medium, RiskLevel::High] {
            assert_eq!(PermissionPolicy::DenyAll.auto_allow(risk), Some(false));
            assert_eq!(PermissionPolicy::Interactive.auto_allow(risk), None);
        }
        // A representative destructive title classifies High (→ denied above).
        assert_eq!(assess_risk_level("Delete file"), RiskLevel::High);
        assert_eq!(assess_risk_level("Run command"), RiskLevel::Medium);
    }

    #[tokio::test]
    async fn write_then_read_round_trip_fires_file_changed() {
        let root = temp_dir();
        let sink = Arc::new(MockSink::default());
        let (handler, _reg) = build_handler(
            &root,
            PermissionPolicy::Interactive,
            sink.clone(),
            PermissionRegistry::new(),
        );
        let (conn, mut req_rx, mut writer, mut reader) = connect_handler();
        let path = root.join("notes/hello.txt");

        let req = send(
            &mut writer,
            &mut req_rx,
            1,
            "fs/write_text_file",
            json!({ "sessionId": "acp-1", "path": path, "content": "hi there" }),
        )
        .await;
        handler.serve(&conn, req).await.unwrap();
        let resp = read_frame(&mut reader).await;
        assert_eq!(resp["id"], json!(1));
        assert!(resp.get("result").is_some(), "write returns a result");
        assert_eq!(std::fs::read_to_string(&path).unwrap(), "hi there");

        let changed = sink
            .last_of("file:changed")
            .expect("write fires file:changed");
        assert_eq!(changed["relativePath"], json!("notes/hello.txt"));
        assert_eq!(changed["action"], json!("create"));

        let req = send(
            &mut writer,
            &mut req_rx,
            2,
            "fs/read_text_file",
            json!({ "sessionId": "acp-1", "path": path }),
        )
        .await;
        handler.serve(&conn, req).await.unwrap();
        let resp = read_frame(&mut reader).await;
        assert_eq!(resp["result"]["content"], json!("hi there"));
    }

    fn permission_params(title: &str) -> Value {
        json!({
            "sessionId": "acp-1",
            "toolCall": { "toolCallId": "t1", "title": title, "rawInput": { "command": "x" } },
            "options": [
                { "optionId": "allow_once", "name": "Allow", "kind": "allow_once" },
                { "optionId": "reject_once", "name": "Deny", "kind": "reject_once" }
            ]
        })
    }

    #[tokio::test]
    async fn permission_request_resolves_via_registry() {
        let root = temp_dir();
        let sink = Arc::new(MockSink::default());
        let (handler, registry) = build_handler(
            &root,
            PermissionPolicy::Interactive,
            sink.clone(),
            PermissionRegistry::new(),
        );
        let (conn, mut req_rx, mut writer, mut reader) = connect_handler();

        let req = send(
            &mut writer,
            &mut req_rx,
            7,
            "session/request_permission",
            permission_params("Write file"),
        )
        .await;

        let handler = Arc::new(handler);
        let conn = Arc::new(conn);
        let task = {
            let handler = handler.clone();
            let conn = conn.clone();
            tokio::spawn(async move { handler.serve(&conn, req).await.unwrap() })
        };

        // The prompt is registered + recoverable; resolve it from "the client".
        let request_id = loop {
            if let Some(data) = registry.pending().first() {
                break data.request_id.clone();
            }
            tokio::time::sleep(Duration::from_millis(5)).await;
        };
        assert!(
            registry.resolve(
                &request_id,
                PermissionOutcome::Selected {
                    option_id: "allow_once".to_string()
                }
            ),
            "registry delivers the outcome to the waiter"
        );
        task.await.unwrap();

        let resp = read_frame(&mut reader).await;
        assert_eq!(resp["result"]["outcome"]["outcome"], json!("selected"));
        assert_eq!(resp["result"]["outcome"]["optionId"], json!("allow_once"));
        assert!(registry.pending().is_empty(), "resolved prompt is removed");

        let resolved = sink.last_of("agent:permission:resolved").unwrap();
        assert_eq!(resolved["requestId"], json!(request_id));
        assert!(sink
            .types()
            .contains(&"agent:permission:request".to_string()));
    }

    #[tokio::test]
    async fn permission_times_out_to_cancelled() {
        let root = temp_dir();
        let sink = Arc::new(MockSink::default());
        let (handler, _reg) = build_handler(
            &root,
            PermissionPolicy::Interactive,
            sink.clone(),
            PermissionRegistry::with_timeout(Duration::from_millis(50)),
        );
        let (conn, mut req_rx, mut writer, mut reader) = connect_handler();

        let req = send(
            &mut writer,
            &mut req_rx,
            8,
            "session/request_permission",
            permission_params("Run command"),
        )
        .await;
        handler.serve(&conn, req).await.unwrap();
        let resp = read_frame(&mut reader).await;
        assert_eq!(resp["result"]["outcome"]["outcome"], json!("cancelled"));
        assert_eq!(
            sink.last_of("agent:permission:resolved").unwrap()["outcome"]["outcome"],
            json!("cancelled")
        );
    }

    #[tokio::test]
    async fn headless_policy_allows_reads_and_denies_destructive() {
        let root = temp_dir();
        let sink = Arc::new(MockSink::default());
        let (handler, _reg) = build_handler(
            &root,
            PermissionPolicy::AutoByRisk,
            sink.clone(),
            PermissionRegistry::new(),
        );
        let (conn, mut req_rx, mut writer, mut reader) = connect_handler();

        let req = send(
            &mut writer,
            &mut req_rx,
            9,
            "session/request_permission",
            permission_params("Read file"),
        )
        .await;
        handler.serve(&conn, req).await.unwrap();
        let allow = read_frame(&mut reader).await;
        assert_eq!(allow["result"]["outcome"]["optionId"], json!("allow_once"));

        let req = send(
            &mut writer,
            &mut req_rx,
            10,
            "session/request_permission",
            permission_params("Delete file"),
        )
        .await;
        handler.serve(&conn, req).await.unwrap();
        let deny = read_frame(&mut reader).await;
        assert_eq!(deny["result"]["outcome"]["optionId"], json!("reject_once"));
    }

    /// With no terminal host wired, `terminal/*` falls back to a clean
    /// "method not found" stub (the host is wired in production by the service
    /// layer; see the `acp_integration` host-backed scenario).
    #[tokio::test]
    async fn terminal_methods_return_unsupported_stub() {
        let root = temp_dir();
        let sink = Arc::new(MockSink::default());
        let (handler, _reg) = build_handler(
            &root,
            PermissionPolicy::Interactive,
            sink,
            PermissionRegistry::new(),
        );
        let (conn, mut req_rx, mut writer, mut reader) = connect_handler();

        let req = send(
            &mut writer,
            &mut req_rx,
            11,
            "terminal/create",
            json!({ "sessionId": "acp-1" }),
        )
        .await;
        handler.serve(&conn, req).await.unwrap();
        let resp = read_frame(&mut reader).await;
        assert_eq!(resp["error"]["code"], json!(-32601));
        assert!(resp["error"]["message"]
            .as_str()
            .unwrap()
            .contains("no terminal host wired"));
    }
}

/// Display / `From` coverage for `error::{AcpError, JsonRpcError}`.
mod error_tests {
    use crate::{AcpError, JsonRpcError};
    use serde_json::json;

    #[test]
    fn json_rpc_error_display_format() {
        let e = JsonRpcError {
            code: -32000,
            message: "boom".to_string(),
            data: Some(json!({"k": 1})),
        };
        assert_eq!(e.to_string(), "JSON-RPC error -32000: boom");
        // Cloned equality and Debug both exercise their derives.
        let cloned = e.clone();
        assert_eq!(cloned, e);
        assert!(format!("{e:?}").contains("boom"));
    }

    #[test]
    fn acp_error_display_for_every_variant() {
        let cases: Vec<(AcpError, &str)> = vec![
            (
                AcpError::Spawn("pipe".into()),
                "failed to spawn provider: pipe",
            ),
            (AcpError::Transport("eof".into()), "transport closed: eof"),
            (AcpError::Timeout("foo".into()), "request `foo` timed out"),
            (AcpError::Serde("bad".into()), "serialization error: bad"),
            (AcpError::Auth("login".into()), "login"),
            (AcpError::Protocol("xx".into()), "protocol error: xx"),
            (AcpError::Fs("io".into()), "filesystem error: io"),
            (AcpError::Terminal("pty".into()), "terminal error: pty"),
        ];
        for (err, expected) in cases {
            assert_eq!(err.to_string(), expected, "variant {err:?}");
        }
        let rpc = AcpError::Rpc(JsonRpcError {
            code: 7,
            message: "m".into(),
            data: None,
        });
        // `#[error("{0}")]` delegates to JsonRpcError::Display.
        assert_eq!(rpc.to_string(), "JSON-RPC error 7: m");
    }

    #[test]
    fn from_serde_error_into_acp_serde_variant() {
        let serde_err = serde_json::from_str::<serde_json::Value>("not json").unwrap_err();
        let raw_msg = serde_err.to_string();
        let acp: AcpError = serde_err.into();
        match &acp {
            AcpError::Serde(msg) => assert_eq!(msg, &raw_msg),
            other => panic!("expected Serde, got {other:?}"),
        }
        // And Display matches the formatted variant string.
        assert_eq!(acp.to_string(), format!("serialization error: {raw_msg}"));
    }
}

/// Exhaustive branches of `tool_restrictions::*` (denylist tables + agent-type
/// predicates).
mod tool_restriction_tests {
    use crate::tool_restrictions::{
        background_agent_types, get_tool_denylist_for_agent_type, is_background_agent_type,
        AGENT_CREATION_TOOLS, CONFLICTING_BUILTIN_TOOLS, EXECUTION_TOOLS, EXTERNAL_TOOLS,
        FILE_WRITE_TOOLS, GIT_TOOLS, NOTE_WRITE_TOOLS, SUBAGENT_TOOLS, UNIFIED_WORKSPACE_TOOLS,
        WORKSPACE_WRITE_TOOLS,
    };

    #[test]
    fn pure_text_agents_share_full_denylist() {
        let pure = [
            "commit-message",
            "pr-description",
            "code-review",
            "code-walkthrough",
        ];
        let mut prev: Option<Vec<&'static str>> = None;
        for ty in pure {
            let deny = get_tool_denylist_for_agent_type(ty);
            // Every category appears in the denylist for every pure-text agent.
            for cat in [
                FILE_WRITE_TOOLS,
                GIT_TOOLS,
                AGENT_CREATION_TOOLS,
                NOTE_WRITE_TOOLS,
                WORKSPACE_WRITE_TOOLS,
                UNIFIED_WORKSPACE_TOOLS,
                EXECUTION_TOOLS,
                EXTERNAL_TOOLS,
                SUBAGENT_TOOLS,
            ] {
                for name in cat {
                    assert!(deny.contains(name), "{ty} denylist missing {name}");
                }
            }
            if let Some(p) = &prev {
                assert_eq!(p, &deny, "pure-text denylists should be identical");
            }
            prev = Some(deny);
        }
    }

    #[test]
    fn working_agents_deny_only_subagents() {
        for ty in ["task-loop", "ralph-loop", "chat"] {
            let deny = get_tool_denylist_for_agent_type(ty);
            assert_eq!(deny, SUBAGENT_TOOLS.to_vec(), "{ty}");
        }
    }

    #[test]
    fn unknown_agent_types_have_empty_denylist() {
        for ty in ["interactive", "", "foreground", "implementor", "verifier"] {
            assert!(
                get_tool_denylist_for_agent_type(ty).is_empty(),
                "expected empty denylist for {ty}"
            );
        }
    }

    #[test]
    fn background_agent_predicate_matches_listed_types() {
        for ty in background_agent_types() {
            assert!(is_background_agent_type(ty), "{ty} should be background");
        }
        for ty in ["interactive", "", "foo", "implementor"] {
            assert!(
                !is_background_agent_type(ty),
                "{ty} should not be background"
            );
        }
        // The list is the closed set of seven names — fix it here so any
        // accidental addition/removal trips a test.
        assert_eq!(background_agent_types().len(), 7);
    }

    #[test]
    fn denylist_categories_are_non_empty_and_distinct() {
        // Sanity: each constant table actually exposes names; CONFLICTING_BUILTIN_TOOLS
        // is exported but not part of the per-type denylist computation.
        for cat in [
            FILE_WRITE_TOOLS,
            GIT_TOOLS,
            AGENT_CREATION_TOOLS,
            NOTE_WRITE_TOOLS,
            WORKSPACE_WRITE_TOOLS,
            UNIFIED_WORKSPACE_TOOLS,
            EXECUTION_TOOLS,
            EXTERNAL_TOOLS,
            SUBAGENT_TOOLS,
            CONFLICTING_BUILTIN_TOOLS,
        ] {
            assert!(!cat.is_empty(), "empty category {cat:?}");
        }
        // The unified workspace API tool comes in two flavours (bare + suffixed).
        assert!(UNIFIED_WORKSPACE_TOOLS.contains(&"workspace_api"));
        assert!(UNIFIED_WORKSPACE_TOOLS.contains(&"workspace_api_workspace-mcp"));
    }
}

/// Schema synthesis + tool-registry shape.
mod tool_registry_tests {
    use crate::mcp_server::ToolDef;
    use crate::tool_restrictions::background_agent_types;
    // `tools::all_tools` is `pub(crate)` via the `mcp_server::tools` mod — re-export
    // it through the public façade by walking through `WorkspaceMcpServer::new`'s
    // `available_tools()` with an empty denylist.
    use crate::WorkspaceMcpServer;

    fn unrestricted_tools() -> Vec<&'static ToolDef> {
        let srv = WorkspaceMcpServer::new(
            super::mcp_tests::mock_api(),
            intent_core::WorkspaceId::from_string("ws-1"),
        );
        srv.available_tools()
    }

    #[test]
    fn schema_marks_required_and_injects_array_items() {
        // Pick a tool with mixed required/optional/array params: create_note has
        // required `title`, optional `content`, and an `array` `tags`.
        let tools = unrestricted_tools();
        let def = tools
            .iter()
            .find(|t| t.name == "create_note_workspace-mcp")
            .expect("create_note tool present");
        let schema = def.schema();
        assert_eq!(schema["type"], serde_json::json!("object"));
        let required = schema["required"].as_array().expect("required array");
        let req_names: Vec<&str> = required.iter().filter_map(|v| v.as_str()).collect();
        assert_eq!(req_names, vec!["title"]);
        // Array params get `items: { type: "string" }`.
        assert_eq!(
            schema["properties"]["tags"]["items"]["type"],
            serde_json::json!("string")
        );
        // Scalars do not get `items` synthesized.
        assert!(schema["properties"]["content"].get("items").is_none());
        assert_eq!(
            schema["properties"]["title"]["type"],
            serde_json::json!("string")
        );
    }

    #[test]
    fn schema_for_tool_without_params_is_empty_object() {
        let tools = unrestricted_tools();
        let def = tools
            .iter()
            .find(|t| t.name == "list_notes_workspace-mcp")
            .expect("list_notes tool present");
        let schema = def.schema();
        assert_eq!(schema["type"], serde_json::json!("object"));
        assert!(schema["properties"]
            .as_object()
            .map(|m| m.is_empty())
            .unwrap_or(false));
        assert_eq!(schema["required"], serde_json::json!([]));
    }

    #[test]
    fn registry_has_unique_tool_names_and_includes_expected_set() {
        let tools = unrestricted_tools();
        let names: Vec<&'static str> = tools.iter().map(|t| t.name).collect();
        let mut sorted = names.clone();
        sorted.sort_unstable();
        sorted.dedup();
        assert_eq!(
            sorted.len(),
            names.len(),
            "duplicate tool names in registry"
        );
        // Every name must end with `_workspace-mcp` per the §18.4 naming convention.
        for n in &names {
            assert!(
                n.ends_with("_workspace-mcp"),
                "tool name {n} missing workspace-mcp suffix"
            );
        }
        // A representative read, write, agent-creation, and git tool are all present.
        for required in [
            "list_notes_workspace-mcp",
            "add_to_note_workspace-mcp",
            "delegate_task_workspace-mcp",
            "git_commit_workspace-mcp",
            "report_to_parent_workspace-mcp",
            // Agent coordination surface (D-batch 3): send/wake/list/read tools
            // that let agents cooperate over the MCP front door.
            "send_message_to_agent_workspace-mcp",
            "send_message_to_task_agent_workspace-mcp",
            "wake_or_create_task_agent_workspace-mcp",
            "list_agents_workspace-mcp",
            "get_agent_status_workspace-mcp",
            "read_agent_conversation_workspace-mcp",
            "get_agent_summary_workspace-mcp",
            "get_agent_diagnostics_workspace-mcp",
            "subscribe_to_events_workspace-mcp",
            "unsubscribe_from_events_workspace-mcp",
        ] {
            assert!(names.contains(&required), "missing {required}");
        }
    }

    #[test]
    fn pure_text_agent_types_strictly_shrink_registry() {
        // Pure-text background agents (commit-message, pr-description, code-review,
        // code-walkthrough) deny note/task/agent/git/workspace-write tools that DO
        // live in the workspace-MCP registry, so their available set must be
        // strictly smaller than the unrestricted set. The other background types
        // (task-loop / ralph-loop / chat) only deny auggie subagent tools that
        // are intentionally absent from this registry.
        let full = unrestricted_tools().len();
        for ty in [
            "commit-message",
            "pr-description",
            "code-review",
            "code-walkthrough",
        ] {
            let srv = WorkspaceMcpServer::for_agent_type(
                super::mcp_tests::mock_api(),
                intent_core::WorkspaceId::from_string("ws-1"),
                ty,
            );
            let available = srv.available_tools().len();
            assert!(
                available < full,
                "{ty} did not filter any tools ({available}/{full})"
            );
            // Read tools survive.
            let names: Vec<&str> = srv.available_tools().iter().map(|t| t.name).collect();
            assert!(names.contains(&"get_note_workspace-mcp"), "{ty}");
            assert!(names.contains(&"list_notes_workspace-mcp"), "{ty}");
        }
    }

    #[test]
    fn working_agent_types_keep_full_registry() {
        // task-loop / ralph-loop / chat only deny auggie subagent tools that are
        // not in the workspace-MCP registry, so the available set is unchanged.
        let full = unrestricted_tools().len();
        for ty in ["task-loop", "ralph-loop", "chat"] {
            let srv = WorkspaceMcpServer::for_agent_type(
                super::mcp_tests::mock_api(),
                intent_core::WorkspaceId::from_string("ws-1"),
                ty,
            );
            assert_eq!(srv.available_tools().len(), full, "{ty}");
        }
        // Sanity: background_agent_types() enumerates exactly these seven types
        // (4 pure-text + 3 working), keeping the two branches of this pair test
        // exhaustive over the predicate.
        let bg: Vec<&str> = background_agent_types().to_vec();
        assert_eq!(bg.len(), 7);
    }
}

/// Additional dispatch coverage: each tool arm hits its `WorkspaceApi` method
/// (default `Internal` from the mock when unoverridden, `InvalidParams` for
/// missing required parameters) and the catch-all `Tool not found` branch.
mod dispatch_unit_tests {
    use serde_json::json;
    use std::sync::Arc;

    use intent_core::WorkspaceId;

    use super::mcp_tests::{mock_api, MockApi};
    use crate::WorkspaceMcpServer;

    fn server() -> (WorkspaceMcpServer, Arc<MockApi>) {
        let api = mock_api();
        let srv = WorkspaceMcpServer::new(api.clone(), WorkspaceId::from_string("ws-1"));
        (srv, api)
    }

    async fn call(
        srv: &WorkspaceMcpServer,
        name: &str,
        args: serde_json::Value,
    ) -> serde_json::Value {
        srv.handle_message(&json!({
            "jsonrpc": "2.0", "id": 1, "method": "tools/call",
            "params": { "name": name, "arguments": args }
        }))
        .await
        .expect("tools/call must produce a response")
    }

    #[tokio::test]
    async fn tools_call_without_name_is_invalid_params() {
        let (srv, _) = server();
        let resp = srv
            .handle_message(&json!({
                "jsonrpc": "2.0", "id": 1, "method": "tools/call",
                "params": { "arguments": {} }
            }))
            .await
            .unwrap();
        assert_eq!(resp["error"]["code"], json!(-32602));
        assert!(resp["error"]["message"]
            .as_str()
            .unwrap()
            .contains("Missing tool name"));
    }

    #[tokio::test]
    async fn tools_call_without_arguments_defaults_to_empty_object() {
        let (srv, _) = server();
        // get_note requires noteId. With no `arguments`, dispatch defaults args to
        // `{}` and the required-parameter check fires (InvalidParams).
        let resp = srv
            .handle_message(&json!({
                "jsonrpc": "2.0", "id": 1, "method": "tools/call",
                "params": { "name": "get_note_workspace-mcp" }
            }))
            .await
            .unwrap();
        assert_eq!(resp["error"]["code"], json!(-32602));
        assert!(resp["error"]["message"]
            .as_str()
            .unwrap()
            .contains("noteId"));
    }

    #[tokio::test]
    async fn unknown_top_level_method_returns_method_not_found() {
        let (srv, _) = server();
        let resp = srv
            .handle_message(&json!({
                "jsonrpc": "2.0", "id": 9, "method": "frobnicate"
            }))
            .await
            .unwrap();
        assert_eq!(resp["error"]["code"], json!(-32601));
    }

    #[tokio::test]
    async fn handle_message_ignores_message_without_method() {
        let (srv, _) = server();
        let resp = srv.handle_message(&json!({ "id": 1 })).await;
        assert!(resp.is_none(), "no method => no response");
    }

    #[tokio::test]
    async fn missing_required_parameters_surface_invalid_params() {
        let (srv, _) = server();
        // (tool, args, expected substring in error message)
        let cases: &[(&str, serde_json::Value, &str)] = &[
            ("get_note_workspace-mcp", json!({}), "noteId"),
            ("list_note_tasks_workspace-mcp", json!({}), "noteId"),
            ("create_note_workspace-mcp", json!({}), "title"),
            (
                "add_to_note_workspace-mcp",
                json!({ "noteId": "n" }),
                "content",
            ),
            (
                "set_note_content_workspace-mcp",
                json!({ "noteId": "n" }),
                "content",
            ),
            (
                "edit_note_workspace-mcp",
                json!({ "noteId": "n", "old": "x" }),
                "new",
            ),
            (
                "edit_note_lines_workspace-mcp",
                json!({ "noteId": "n", "start": 1, "end": 2 }),
                "content",
            ),
            (
                "edit_note_lines_workspace-mcp",
                json!({ "noteId": "n", "end": 2, "content": "x" }),
                "start",
            ),
            ("delete_note_workspace-mcp", json!({}), "noteId"),
            (
                "update_task_status_workspace-mcp",
                json!({ "noteId": "n", "taskText": "t" }),
                "status",
            ),
            (
                "update_note_task_status_workspace-mcp",
                json!({ "noteId": "n" }),
                "status",
            ),
            (
                "update_task_workspace-mcp",
                json!({ "noteId": "n" }),
                "line",
            ),
            (
                "mark_as_task_workspace-mcp",
                json!({ "noteId": "n" }),
                "status",
            ),
            ("convert_task_blocks_workspace-mcp", json!({}), "noteId"),
            (
                "create_prerequisite_workspace-mcp",
                json!({ "noteId": "n" }),
                "title",
            ),
            (
                "assign_agent_workspace-mcp",
                json!({ "noteId": "n" }),
                "agentId",
            ),
            (
                "add_note_comment_workspace-mcp",
                json!({ "noteId": "n", "searchContext": "s", "commentTarget": "t" }),
                "comment",
            ),
            (
                "respond_to_comment_thread_workspace-mcp",
                json!({ "noteId": "n" }),
                "comment",
            ),
            ("git_commit_workspace-mcp", json!({}), "agent"),
            ("report_to_parent_workspace-mcp", json!({}), "report"),
            (
                "send_message_to_agent_workspace-mcp",
                json!({ "message": "hi" }),
                "agentId",
            ),
            (
                "send_message_to_agent_workspace-mcp",
                json!({ "agentId": "agent-1" }),
                "message",
            ),
            (
                "send_message_to_task_agent_workspace-mcp",
                json!({ "message": "hi" }),
                "taskNoteId",
            ),
            (
                "wake_or_create_task_agent_workspace-mcp",
                json!({ "contextMessage": "ctx" }),
                "taskNoteId",
            ),
            (
                "wake_or_create_task_agent_workspace-mcp",
                json!({ "taskNoteId": "n" }),
                "contextMessage",
            ),
            ("get_agent_status_workspace-mcp", json!({}), "agentId"),
            (
                "read_agent_conversation_workspace-mcp",
                json!({}),
                "agentId",
            ),
            ("get_agent_summary_workspace-mcp", json!({}), "agentId"),
            ("subscribe_to_events_workspace-mcp", json!({}), "eventTypes"),
            (
                "unsubscribe_from_events_workspace-mcp",
                json!({}),
                "subscriptionId",
            ),
        ];
        for (tool, args, needle) in cases {
            let resp = call(&srv, tool, args.clone()).await;
            assert_eq!(
                resp["error"]["code"],
                json!(-32602),
                "{tool}: expected InvalidParams, got {resp}"
            );
            let msg = resp["error"]["message"].as_str().unwrap();
            assert!(
                msg.contains(needle),
                "{tool}: error {msg:?} does not mention {needle:?}"
            );
        }
    }

    #[tokio::test]
    async fn each_dispatch_arm_reaches_workspace_api_default() {
        // For tools not overridden on MockApi, the default trait method returns
        // `Error::Internal(...)` → JSON-RPC -32603. The catch-all branch would
        // return -32602 "Tool not found", so seeing -32603 proves each arm is
        // wired through to the real method.
        let (srv, _) = server();
        let valid_args: &[(&str, serde_json::Value)] = &[
            ("get_note_workspace-mcp", json!({ "noteId": "n" })),
            ("list_note_tasks_workspace-mcp", json!({ "noteId": "n" })),
            (
                "set_note_content_workspace-mcp",
                json!({ "noteId": "n", "content": "c", "confirmReplacement": true, "expectedVersion": 3 }),
            ),
            (
                "edit_note_workspace-mcp",
                json!({ "noteId": "n", "old": "a", "new": "b" }),
            ),
            (
                "edit_note_lines_workspace-mcp",
                json!({ "noteId": "n", "start": 1, "end": 2, "content": "x" }),
            ),
            (
                "update_note_metadata_workspace-mcp",
                json!({ "noteId": "n", "title": "T", "tags": ["a", "b"] }),
            ),
            ("delete_note_workspace-mcp", json!({ "noteId": "n" })),
            (
                "update_task_status_workspace-mcp",
                json!({ "noteId": "n", "taskText": "t", "status": "done" }),
            ),
            (
                "update_note_task_status_workspace-mcp",
                json!({ "noteId": "n", "status": "complete", "expectedVersion": 1 }),
            ),
            (
                "update_task_workspace-mcp",
                json!({ "noteId": "n", "line": 2, "text": "T", "status": "todo", "expected": "old" }),
            ),
            (
                "mark_as_task_workspace-mcp",
                json!({ "noteId": "n", "status": "in_progress", "acceptanceCriteria": ["a"], "effort": "small" }),
            ),
            (
                "convert_task_blocks_workspace-mcp",
                json!({ "noteId": "n" }),
            ),
            (
                "create_prerequisite_workspace-mcp",
                json!({ "noteId": "n", "title": "t", "content": "c", "status": "todo" }),
            ),
            (
                "assign_agent_workspace-mcp",
                json!({ "noteId": "n", "agentId": "agent-1" }),
            ),
            (
                "add_note_comment_workspace-mcp",
                json!({ "noteId": "n", "searchContext": "s", "commentTarget": "t", "comment": "c", "type": "comment", "author": "me" }),
            ),
            (
                "respond_to_comment_thread_workspace-mcp",
                json!({ "noteId": "n", "threadId": "th", "comment": "c", "type": "comment", "author": "me", "suggestionOriginal": "o", "suggestionProposed": "p" }),
            ),
            (
                "delegate_task_workspace-mcp",
                json!({ "taskNoteId": "tn", "noteId": "n", "taskText": "t", "agentInstructions": "i",
                        "specialist": "implementor", "model": "opus", "behaviorPrompt": "b",
                        "waitMode": "after_all", "skipAutoCommit": true }),
            ),
            (
                "report_to_parent_workspace-mcp",
                json!({ "report": "all done" }),
            ),
            (
                "send_message_to_agent_workspace-mcp",
                json!({ "agentId": "agent-1", "message": "hi", "priority": "normal" }),
            ),
            (
                "send_message_to_task_agent_workspace-mcp",
                json!({ "taskNoteId": "tn", "message": "hi" }),
            ),
            (
                "wake_or_create_task_agent_workspace-mcp",
                json!({ "taskNoteId": "tn", "contextMessage": "ctx", "model": "opus" }),
            ),
            (
                "list_agents_workspace-mcp",
                json!({ "includeCompleted": true }),
            ),
            (
                "get_agent_status_workspace-mcp",
                json!({ "agentId": "agent-1" }),
            ),
            (
                "read_agent_conversation_workspace-mcp",
                json!({ "agentId": "agent-1", "lastN": 20 }),
            ),
            (
                "get_agent_summary_workspace-mcp",
                json!({ "agentId": "agent-1" }),
            ),
            (
                "get_agent_diagnostics_workspace-mcp",
                json!({ "agentId": "agent-1", "taskNoteId": "n", "staleRespondingAfterMs": 60000 }),
            ),
            (
                "subscribe_to_events_workspace-mcp",
                json!({ "eventTypes": ["agent:idle"], "excludeSelf": true, "batchWindow": 250 }),
            ),
            (
                "unsubscribe_from_events_workspace-mcp",
                json!({ "subscriptionId": "sub-1" }),
            ),
        ];
        for (tool, args) in valid_args {
            let resp = call(&srv, tool, args.clone()).await;
            assert_eq!(
                resp["error"]["code"],
                json!(-32603),
                "{tool}: expected Internal default, got {resp}"
            );
        }
    }

    #[tokio::test]
    async fn create_note_mints_idempotency_key_when_absent() {
        // Daemon-internal `note.create`: agents do not pass an idempotencyKey,
        // so the dispatch arm mints one — the services soft-launch warn
        // ("idempotencyKey missing on idempotent method") must never fire for
        // MCP tool calls.
        let (srv, api) = server();
        let resp = call(&srv, "create_note_workspace-mcp", json!({ "title": "t" })).await;
        assert!(resp.get("error").is_none(), "unexpected error: {resp}");
        let created = api.created.lock().unwrap();
        assert_eq!(created.len(), 1);
        let (title, key) = &created[0];
        assert_eq!(title, "t");
        let key = key
            .as_deref()
            .expect("dispatch must mint an idempotencyKey");
        assert!(
            uuid::Uuid::parse_str(key).is_ok(),
            "minted key {key:?} is not a UUID"
        );
    }

    #[tokio::test]
    async fn create_note_passes_caller_idempotency_key_through() {
        // A caller-supplied key is adopted verbatim (no re-mint), preserving
        // dedupe across retries of the same tool call.
        let (srv, api) = server();
        let resp = call(
            &srv,
            "create_note_workspace-mcp",
            json!({ "title": "t", "idempotencyKey": "key-from-caller" }),
        )
        .await;
        assert!(resp.get("error").is_none(), "unexpected error: {resp}");
        let created = api.created.lock().unwrap();
        assert_eq!(created.len(), 1);
        assert_eq!(created[0].1.as_deref(), Some("key-from-caller"));
    }

    #[tokio::test]
    async fn dispatch_more_catchall_reports_tool_not_found() {
        // A name that does not exist in the registry: the `tools/call` filter
        // catches it first (-32602 "Tool not found"). The internal `dispatch_more`
        // catch-all is reached when bypassing the filter via `dispatch` directly,
        // but here we just confirm the public surface returns the registry error.
        let (srv, _) = server();
        let resp = srv
            .handle_message(&json!({
                "jsonrpc": "2.0", "id": 5, "method": "tools/call",
                "params": { "name": "totally_made_up_workspace-mcp", "arguments": {} }
            }))
            .await
            .unwrap();
        assert_eq!(resp["error"]["code"], json!(-32602));
        assert!(resp["error"]["message"]
            .as_str()
            .unwrap()
            .contains("Tool not found"));
    }

    #[tokio::test]
    async fn with_denylist_blocks_individual_tool() {
        // The custom-denylist builder removes a single tool without affecting
        // the rest of the registry.
        let api = mock_api();
        let srv = WorkspaceMcpServer::new(api, WorkspaceId::from_string("ws-1"))
            .with_denylist(["get_note_workspace-mcp"]);
        assert!(srv.is_denied("get_note_workspace-mcp"));
        assert!(!srv.is_denied("list_notes_workspace-mcp"));
        let resp = srv
            .handle_message(&json!({
                "jsonrpc": "2.0", "id": 6, "method": "tools/call",
                "params": { "name": "get_note_workspace-mcp", "arguments": { "noteId": "n" } }
            }))
            .await
            .unwrap();
        assert_eq!(resp["error"]["code"], json!(-32602));
        assert!(resp["error"]["message"]
            .as_str()
            .unwrap()
            .contains("Tool not available"));
    }

    #[tokio::test]
    async fn tools_list_excludes_denied_entries() {
        let api = mock_api();
        let srv = WorkspaceMcpServer::new(api, WorkspaceId::from_string("ws-1"))
            .with_denylist(["delete_note_workspace-mcp"]);
        let resp = srv
            .handle_message(&json!({ "jsonrpc": "2.0", "id": 1, "method": "tools/list" }))
            .await
            .unwrap();
        let tools = resp["result"]["tools"].as_array().unwrap();
        assert!(!tools
            .iter()
            .any(|t| t["name"] == json!("delete_note_workspace-mcp")));
        assert!(tools
            .iter()
            .any(|t| t["name"] == json!("list_notes_workspace-mcp")));
    }
}

/// `mcp_bridge`: real loopback TCP listener + line-framed JSON-RPC proxy.
mod mcp_bridge_tests {
    use std::sync::Arc;
    use std::time::Duration;

    use intent_core::WorkspaceId;
    use serde_json::{json, Value};
    use tokio::io::{AsyncBufReadExt, AsyncWriteExt, BufReader};
    use tokio::net::TcpStream;
    use tokio::time::timeout;

    use super::mcp_tests::mock_api;
    use crate::mcp_bridge::serve_workspace_mcp_tcp;
    use crate::WorkspaceMcpServer;

    fn build_bridge_server() -> Arc<WorkspaceMcpServer> {
        Arc::new(WorkspaceMcpServer::new(
            mock_api(),
            WorkspaceId::from_string("ws-1"),
        ))
    }

    async fn send_line(stream: &mut TcpStream, line: &str) {
        stream.write_all(line.as_bytes()).await.unwrap();
        stream.write_all(b"\n").await.unwrap();
        stream.flush().await.unwrap();
    }

    async fn read_next_line(reader: &mut BufReader<tokio::net::tcp::OwnedReadHalf>) -> String {
        let mut buf = String::new();
        timeout(Duration::from_secs(2), reader.read_line(&mut buf))
            .await
            .expect("read_line timed out")
            .expect("read_line ok");
        buf
    }

    #[tokio::test]
    async fn connect_addr_is_localhost_port() {
        let bridge = serve_workspace_mcp_tcp(build_bridge_server())
            .await
            .unwrap();
        let addr = bridge.connect_addr();
        assert!(
            addr.starts_with("127.0.0.1:"),
            "unexpected connect_addr: {addr}"
        );
        assert_eq!(addr, format!("127.0.0.1:{}", bridge.addr().port()));
    }

    #[tokio::test]
    async fn request_is_proxied_and_response_returns() {
        let bridge = serve_workspace_mcp_tcp(build_bridge_server())
            .await
            .unwrap();
        let stream = TcpStream::connect(bridge.connect_addr()).await.unwrap();
        let (read, mut write) = stream.into_split();
        let mut reader = BufReader::new(read);

        write
            .write_all(b"{\"jsonrpc\":\"2.0\",\"id\":1,\"method\":\"initialize\"}\n")
            .await
            .unwrap();
        write.flush().await.unwrap();

        let mut line = String::new();
        timeout(Duration::from_secs(2), reader.read_line(&mut line))
            .await
            .unwrap()
            .unwrap();
        let resp: Value = serde_json::from_str(line.trim()).unwrap();
        assert_eq!(resp["id"], json!(1));
        assert_eq!(resp["result"]["protocolVersion"], json!("2024-11-05"));
    }

    #[tokio::test]
    async fn empty_and_malformed_lines_are_tolerated() {
        let bridge = serve_workspace_mcp_tcp(build_bridge_server())
            .await
            .unwrap();
        let mut stream = TcpStream::connect(bridge.connect_addr()).await.unwrap();
        // A blank line and a non-JSON line both produce no response and must not
        // close the connection; the follow-up request still gets answered.
        send_line(&mut stream, "").await;
        send_line(&mut stream, "not-valid-json{").await;
        send_line(
            &mut stream,
            "{\"jsonrpc\":\"2.0\",\"id\":42,\"method\":\"initialize\"}",
        )
        .await;
        let (read, _w) = stream.into_split();
        let mut reader = BufReader::new(read);
        let line = read_next_line(&mut reader).await;
        let resp: Value = serde_json::from_str(line.trim()).unwrap();
        assert_eq!(resp["id"], json!(42));
    }

    #[tokio::test]
    async fn notification_yields_no_response_line() {
        let bridge = serve_workspace_mcp_tcp(build_bridge_server())
            .await
            .unwrap();
        let mut stream = TcpStream::connect(bridge.connect_addr()).await.unwrap();
        // A notification (no `id`) yields no response, then a request still works.
        send_line(
            &mut stream,
            "{\"jsonrpc\":\"2.0\",\"method\":\"notifications/initialized\"}",
        )
        .await;
        send_line(
            &mut stream,
            "{\"jsonrpc\":\"2.0\",\"id\":7,\"method\":\"initialize\"}",
        )
        .await;
        let (read, _w) = stream.into_split();
        let mut reader = BufReader::new(read);
        let line = read_next_line(&mut reader).await;
        let resp: Value = serde_json::from_str(line.trim()).unwrap();
        assert_eq!(resp["id"], json!(7), "notification must not consume a line");
    }

    #[tokio::test]
    async fn dropping_bridge_aborts_listener() {
        let bridge = serve_workspace_mcp_tcp(build_bridge_server())
            .await
            .unwrap();
        let addr = bridge.connect_addr();
        drop(bridge);
        // Give the abort a moment to tear down the listener, then a fresh
        // connection attempt must fail (refused or reset). We retry briefly.
        let mut last_err = None;
        for _ in 0..20 {
            match TcpStream::connect(&addr).await {
                Ok(_) => {
                    tokio::time::sleep(Duration::from_millis(50)).await;
                    continue;
                }
                Err(e) => {
                    last_err = Some(e);
                    break;
                }
            }
        }
        assert!(
            last_err.is_some(),
            "listener still accepting after McpBridge drop"
        );
    }
}
