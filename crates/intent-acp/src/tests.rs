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
