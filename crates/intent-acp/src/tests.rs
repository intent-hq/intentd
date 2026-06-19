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
        assert!(
            svc.resolve(std::path::Path::new("../escape.txt")).is_err(),
            "relative traversal escapes the worktree"
        );
        assert!(
            svc.resolve(std::path::Path::new("/etc/passwd")).is_err(),
            "absolute path outside the worktree is rejected"
        );
        let ok = svc.resolve(std::path::Path::new("sub/ok.txt")).unwrap();
        assert!(ok.starts_with(&root), "in-scope path resolves inside root");
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
            .contains("until M6"));
    }
}
