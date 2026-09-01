//! Transport + handshake tests against an in-memory mock agent and (on unix) a
//! real `sh` mock child process (§6.2–§6.4 `DoD`).

use std::time::Duration;

use serde_json::{json, Value};
use tokio::io::{AsyncBufReadExt, AsyncRead, AsyncWrite, AsyncWriteExt, BufReader};
use tokio::sync::mpsc;
use tokio::task::JoinHandle;

use crate::transport::{Connection, ConnectionHooks};

/// A fresh RAII temp directory with `prefix` under the system temp root. The
/// returned guard removes the dir on drop (including on panic); set
/// `INTENTD_TEST_KEEP_TMP` (non-empty) to keep it around for debugging.
fn test_temp_dir(prefix: &str) -> tempfile::TempDir {
    let mut dir = tempfile::Builder::new()
        .prefix(prefix)
        .tempdir()
        .expect("create test temp dir");
    if std::env::var_os("INTENTD_TEST_KEEP_TMP").is_some_and(|v| !v.is_empty()) {
        dir.disable_cleanup(true);
    }
    dir
}

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
        stderr_log_dir: None,
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

/// monorepo#764: `is_alive()` is true on a fresh connection and flips to
/// false once the writer task dies on a broken pipe (the child side of stdin
/// dropped) — the writer's exit drops its receiver, closing the channel.
#[tokio::test]
async fn is_alive_flips_after_writer_hits_closed_stdin() {
    let (c2a_client, c2a_agent) = tokio::io::duplex(4096);
    let (_a2c_agent, a2c_client) = tokio::io::duplex(4096);
    let conn = Connection::new(c2a_client, a2c_client, None, ConnectionHooks::default());
    assert!(conn.is_alive(), "fresh connection reports alive");

    // Drop the child end of stdin, then attempt a send: the enqueue succeeds
    // (channel still open) but the writer's flush hits the broken pipe and
    // the task exits, dropping `writer_rx`.
    drop(c2a_agent);
    let _ = conn.notify("session/ping", json!({})).await;
    let mut alive = true;
    for _ in 0..200 {
        alive = conn.is_alive();
        if !alive {
            break;
        }
        tokio::time::sleep(Duration::from_millis(10)).await;
    }
    assert!(!alive, "writer task exit closes the channel → not alive");
}

/// Dropping an in-flight request future removes its pending-map entry (the
/// transport's cancel-safety drop guard): abandoning a `session/prompt` on
/// idle timeout must not leak the correlation entry until stdout closes.
#[tokio::test]
async fn dropped_request_future_cleans_pending_map_entry() {
    // An "agent" that never responds: keep both remote ends alive so the
    // request stays pending until the caller drops the future.
    let (c2a_client, _c2a_agent) = tokio::io::duplex(4096);
    let (_a2c_agent, a2c_client) = tokio::io::duplex(4096);
    let conn = Connection::new(c2a_client, a2c_client, None, ConnectionHooks::default());

    let mut fut =
        Box::pin(conn.request_timeout("session/prompt", json!({}), Duration::from_secs(60)));
    // Drive the future far enough to write the request and insert the entry.
    tokio::select! {
        _ = &mut fut => panic!("request must still be pending"),
        () = tokio::time::sleep(Duration::from_millis(50)) => {}
    }
    assert_eq!(conn.pending_len(), 1, "in-flight request is correlated");
    drop(fut);
    assert_eq!(
        conn.pending_len(),
        0,
        "dropping the future removes the entry"
    );
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

/// STAB-53: with `stderr_log_dir` set, every stderr line a (real, exiting)
/// child emits lands in `<dir>/<YYYY-MM-DD>.log`.
#[cfg(unix)]
#[tokio::test]
async fn stderr_capture_written_to_daily_log_file() {
    // STAB-56: the capture dir and daily log file are created owner-only.
    use crate::spawn::{spawn_provider, SpawnOptions};
    use std::os::unix::fs::PermissionsExt;

    let tmp = test_temp_dir("intent-acp-stderr-");
    let dir = tmp.path().join("logs");

    let base = *intent_providers::find_provider("auggie").unwrap();
    let provider = intent_providers::ProviderConfig {
        command: "sh",
        base_args: &[
            "-c",
            "echo 'boom: child crashed' >&2; echo 'second line' >&2",
        ],
        model_flag: None,
        rules_flag: None,
        mcp_config_flag: None,
        quiet_flag: None,
        supports_mcp_config: false,
        supports_rules_file: false,
        ..base
    };

    let opts = SpawnOptions::new(&provider);
    let hooks = ConnectionHooks {
        stderr_log_dir: Some(dir.clone()),
        ..ConnectionHooks::default()
    };
    let mut agent = spawn_provider(&opts, hooks).expect("spawn sh child");

    // Concatenate every daily file in the capture dir rather than assuming
    // today's name: the writer rotates by UTC date, so a rollover between
    // emit and read must not flake the test.
    let mut content = String::new();
    for _ in 0..100 {
        content.clear();
        if let Ok(mut entries) = tokio::fs::read_dir(&dir).await {
            while let Ok(Some(entry)) = entries.next_entry().await {
                if let Ok(c) = tokio::fs::read_to_string(entry.path()).await {
                    content.push_str(&c);
                }
            }
        }
        if content.contains("second line") {
            break;
        }
        tokio::time::sleep(Duration::from_millis(10)).await;
    }
    assert!(
        content.contains("boom: child crashed"),
        "first stderr line captured; got: {content:?}"
    );
    assert!(
        content.contains("second line"),
        "second stderr line captured; got: {content:?}"
    );

    let dir_mode = std::fs::metadata(&dir).unwrap().permissions().mode() & 0o777;
    assert_eq!(dir_mode, 0o700, "capture dir must be owner-only");
    let mut entries = tokio::fs::read_dir(&dir).await.unwrap();
    let mut checked = 0;
    while let Some(entry) = entries.next_entry().await.unwrap() {
        let mode = entry.metadata().await.unwrap().permissions().mode() & 0o777;
        assert_eq!(
            mode,
            0o600,
            "log file {} must be owner-only",
            entry.path().display()
        );
        checked += 1;
    }
    assert!(checked > 0, "at least one daily log file was checked");

    agent.kill().await.ok();
}

/// Concatenate every daily capture file under `dir` (empty when the dir does
/// not exist yet). Rotation-proof like the daily-log test above.
async fn read_capture_dir(dir: &std::path::Path) -> String {
    let mut content = String::new();
    if let Ok(mut entries) = tokio::fs::read_dir(dir).await {
        while let Ok(Some(entry)) = entries.next_entry().await {
            if let Ok(c) = tokio::fs::read_to_string(entry.path()).await {
                content.push_str(&c);
            }
        }
    }
    content
}

/// Poll `dir` until the concatenated capture content contains `needle` (or
/// ~2s elapse), returning the last content read.
async fn poll_capture_dir_for(dir: &std::path::Path, needle: &str) -> String {
    let mut content = String::new();
    for _ in 0..200 {
        content = read_capture_dir(dir).await;
        if content.contains(needle) {
            break;
        }
        tokio::time::sleep(Duration::from_millis(10)).await;
    }
    content
}

/// monorepo#3570 regression: a child's dying words — stderr still undrained
/// when the terminal-failure teardown drops the `Connection` — must still
/// reach the capture file. Pre-fix, `Connection::drop` aborted the stderr
/// drain task, losing everything not yet read from the pipe (and when the
/// dying words were the child's ONLY stderr, the lazily-created capture file
/// never existed at all, despite the WARN naming its path).
#[tokio::test]
async fn stderr_dying_words_survive_connection_drop() {
    let tmp = test_temp_dir("intent-acp-stderr-drop-");
    let dir = tmp.path().join("logs");
    let hooks = ConnectionHooks {
        stderr_log_dir: Some(dir.clone()),
        ..ConnectionHooks::default()
    };
    let (conn, _responder, mut stderr_w) = connect_mock(hooks);

    // Teardown races the dying words: the connection is dropped (terminal
    // turn failure → kill_child_only → handle drop) BEFORE the child's final
    // stderr is written/drained. The drain must run to stderr EOF regardless.
    drop(conn);
    let _ = stderr_w.write_all(b"fatal: child dying words\n").await;
    let _ = stderr_w.flush().await;
    drop(stderr_w); // child exit → stderr EOF

    let content = poll_capture_dir_for(&dir, "dying words").await;
    assert!(
        content.contains("fatal: child dying words"),
        "stderr written after connection drop must still be captured; got: {content:?}"
    );
}

/// monorepo#3570: `await_stderr_settled` resolves `true` once the drain hits
/// stderr EOF and the capture file is flushed — the content is readable
/// immediately after, no polling needed. Before EOF it times out (`false`);
/// with no stderr pipe at all it resolves `true` immediately.
#[tokio::test]
async fn await_stderr_settled_signals_flushed_capture() {
    let tmp = test_temp_dir("intent-acp-stderr-settle-");
    let dir = tmp.path().join("logs");
    let hooks = ConnectionHooks {
        stderr_log_dir: Some(dir.clone()),
        ..ConnectionHooks::default()
    };
    let (conn, _responder, mut stderr_w) = connect_mock(hooks);

    stderr_w.write_all(b"last words\n").await.unwrap();
    stderr_w.flush().await.unwrap();
    // Pipe still open → not settled yet.
    assert!(
        !conn.await_stderr_settled(Duration::from_millis(50)).await,
        "not settled while the stderr pipe is open"
    );
    drop(stderr_w); // EOF
    assert!(
        conn.await_stderr_settled(Duration::from_secs(2)).await,
        "settled after stderr EOF"
    );
    // Settled means flushed: readable without polling.
    let content = read_capture_dir(&dir).await;
    assert!(
        content.contains("last words"),
        "capture flushed by settle time; got: {content:?}"
    );

    // No stderr pipe → settled immediately.
    let (c2a_client, _c2a_agent) = tokio::io::duplex(4096);
    let (_a2c_agent, a2c_client) = tokio::io::duplex(4096);
    let no_stderr = Connection::new(c2a_client, a2c_client, None, ConnectionHooks::default());
    assert!(
        no_stderr
            .await_stderr_settled(Duration::from_millis(50))
            .await,
        "no stderr pipe settles immediately"
    );
}

/// monorepo#3570 regression: one invalid-UTF-8 blob on stderr must not kill
/// the drain for the rest of the child's life. Pre-fix the drain loop exited
/// silently on the first `next_line()` UTF-8 error, so everything after the
/// bad bytes (including later dying words) was lost.
#[tokio::test]
async fn stderr_capture_survives_invalid_utf8() {
    let tmp = test_temp_dir("intent-acp-stderr-utf8-");
    let dir = tmp.path().join("logs");
    let hooks = ConnectionHooks {
        stderr_log_dir: Some(dir.clone()),
        ..ConnectionHooks::default()
    };
    let (conn, _responder, mut stderr_w) = connect_mock(hooks);

    stderr_w.write_all(b"before bad bytes\n").await.unwrap();
    stderr_w
        .write_all(&[0xFF, 0xFE, 0xFD, b'\n'])
        .await
        .unwrap();
    stderr_w.write_all(b"after bad bytes\n").await.unwrap();
    stderr_w.flush().await.unwrap();
    drop(stderr_w);

    let content = poll_capture_dir_for(&dir, "after bad bytes").await;
    assert!(
        content.contains("before bad bytes"),
        "line before invalid UTF-8 captured; got: {content:?}"
    );
    assert!(
        content.contains("after bad bytes"),
        "line after invalid UTF-8 captured (drain survived); got: {content:?}"
    );
    drop(conn);
}

/// monorepo#3570: `stderr_captured` is per-connection — `true` only when THIS
/// child wrote at least one stderr line the capture sink accepted, so stale
/// daily files an earlier run left in the same per-agent dir can't turn a
/// silent child into a misleading "stderr captured at …" WARN claim.
#[tokio::test]
async fn stderr_captured_flag_is_per_connection() {
    let tmp = test_temp_dir("intent-acp-stderr-flag-");
    let dir = tmp.path().join("logs");
    // Stale file from a "previous run" of the same agent.
    std::fs::create_dir_all(&dir).unwrap();
    std::fs::write(dir.join("2020-01-01.log"), "old run\n").unwrap();

    // A child that writes nothing: the flag stays false despite the
    // populated dir.
    let hooks = ConnectionHooks {
        stderr_log_dir: Some(dir.clone()),
        ..ConnectionHooks::default()
    };
    let (conn, _responder, stderr_w) = connect_mock(hooks);
    drop(stderr_w); // EOF, no output
    assert!(conn.await_stderr_settled(Duration::from_secs(2)).await);
    assert!(
        !conn.stderr_captured(),
        "silent child must not claim capture (stale dir entries exist)"
    );
    drop(conn);

    // A child that writes: the flag flips.
    let hooks = ConnectionHooks {
        stderr_log_dir: Some(dir.clone()),
        ..ConnectionHooks::default()
    };
    let (conn, _responder, mut stderr_w) = connect_mock(hooks);
    stderr_w.write_all(b"fresh crash output\n").await.unwrap();
    stderr_w.flush().await.unwrap();
    drop(stderr_w);
    assert!(conn.await_stderr_settled(Duration::from_secs(2)).await);
    assert!(
        conn.stderr_captured(),
        "child that wrote stderr reports capture"
    );
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

/// Session-lifecycle + streaming-mapping tests (§6.5/§6.6 `DoD`).
mod session_tests {
    use super::*;

    use agent_client_protocol::schema::v1::{
        ContentBlock, SessionNotification, SessionUpdate, TextContent, ToolCall, ToolCallStatus,
        ToolCallUpdate, ToolCallUpdateFields, ToolKind,
    };

    use crate::session::{self, MappedUpdate};
    use crate::IncomingNotification;

    /// Mock agent that answers the session lifecycle methods and records every
    /// frame it received. `session/cancel` is a notification (no id) → no reply.
    /// `prompt_result` is the JSON returned for `session/prompt` (tests vary it
    /// to exercise the optional end-of-turn `usage` snapshot).
    fn spawn_session_responder_with<R, W>(
        read: R,
        write: W,
        prompt_result: Value,
    ) -> JoinHandle<Vec<Value>>
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
            seen
        })
    }

    fn connect_session() -> (Connection, JoinHandle<Vec<Value>>) {
        connect_session_with_prompt_result(json!({ "stopReason": "end_turn" }))
    }

    /// Like [`connect_session`] but with a caller-supplied `session/prompt`
    /// result payload.
    fn connect_session_with_prompt_result(
        prompt_result: Value,
    ) -> (Connection, JoinHandle<Vec<Value>>) {
        let (c2a_client, c2a_agent) = tokio::io::duplex(8 * 1024);
        let (a2c_agent, a2c_client) = tokio::io::duplex(8 * 1024);
        let responder = spawn_session_responder_with(c2a_agent, a2c_agent, prompt_result);
        let conn = Connection::new(c2a_client, a2c_client, None, ConnectionHooks::default());
        (conn, responder)
    }

    #[tokio::test]
    async fn new_session_returns_acp_session_id() {
        let (conn, _responder) = connect_session();
        let resp = session::new_session(&conn, "/tmp/ws", Vec::new(), None)
            .await
            .expect("session/new succeeds");
        assert_eq!(resp.session_id.0.as_ref(), "acp-session-1");
    }

    #[tokio::test]
    async fn claude_code_new_session_injects_disallowed_tools_and_system_prompt() {
        use crate::session::Meta;
        use serde_json::json;
        let (conn, responder) = connect_session();

        // Build meta with both disallowedTools and systemPrompt (as build_session_meta does)
        let mut meta = Meta::new();
        meta.insert(
            "claudeCode".to_string(),
            json!({
                "options": {
                    "disallowedTools": ["Task"]
                }
            }),
        );
        meta.insert("systemPrompt".to_string(), json!("test prompt"));

        let resp = session::new_session(&conn, "/tmp/ws", Vec::new(), Some(meta))
            .await
            .expect("session/new with _meta succeeds");
        assert_eq!(resp.session_id.0.as_ref(), "acp-session-1");

        drop(conn);
        let seen = responder.await.expect("responder completes");
        let new_req = seen
            .iter()
            .find(|f| f.get("method").and_then(|v| v.as_str()) == Some("session/new"))
            .expect("agent received session/new");
        let meta_payload = &new_req["params"]["_meta"];
        assert_eq!(
            meta_payload["claudeCode"]["options"]["disallowedTools"],
            json!(["Task"]),
            "session/new for claude-code must inject disallowedTools"
        );
        assert_eq!(
            meta_payload["systemPrompt"],
            json!("test prompt"),
            "session/new systemPrompt string (full replacement) preserved"
        );
    }

    #[tokio::test]
    async fn claude_code_load_session_injects_disallowed_tools() {
        use crate::session::Meta;
        use serde_json::json;
        let (conn, responder) = connect_session();

        // Build meta with disallowedTools only (this test omits systemPrompt)
        let mut meta = Meta::new();
        meta.insert(
            "claudeCode".to_string(),
            json!({
                "options": {
                    "disallowedTools": ["Task"]
                }
            }),
        );

        session::load_session(&conn, "acp-session-1", "/tmp/ws", Vec::new(), Some(meta))
            .await
            .expect("session/load succeeds");
        drop(conn);
        let seen = responder.await.unwrap();
        let load_req = seen
            .iter()
            .find(|f| f.get("method").and_then(|v| v.as_str()) == Some("session/load"))
            .expect("agent received session/load");
        let meta_payload = &load_req["params"]["_meta"];
        assert_eq!(
            meta_payload["claudeCode"]["options"]["disallowedTools"],
            json!(["Task"]),
            "session/load for claude-code must inject disallowedTools"
        );
    }

    #[tokio::test]
    async fn no_meta_param_omits_meta_field() {
        let (conn, responder) = connect_session();
        // Passing meta=None omits the _meta field from JSON-RPC params
        session::new_session(&conn, "/tmp/ws", Vec::new(), None)
            .await
            .expect("session/new succeeds");
        drop(conn);
        let seen = responder.await.unwrap();
        let new_req = seen
            .iter()
            .find(|f| f.get("method").and_then(|v| v.as_str()) == Some("session/new"))
            .expect("agent received session/new");
        let params = new_req["params"]
            .as_object()
            .expect("params must be an object");
        assert!(
            !params.contains_key("_meta"),
            "session/new with meta=None must not inject _meta field"
        );
    }

    #[tokio::test]
    async fn claude_code_load_session_with_prompt_injects_both_disallowed_tools_and_system_prompt()
    {
        use crate::session::Meta;
        use serde_json::json;
        let (conn, responder) = connect_session();

        // Build meta with both disallowedTools and systemPrompt (as build_session_meta does on resume)
        let mut meta = Meta::new();
        meta.insert(
            "claudeCode".to_string(),
            json!({
                "options": {
                    "disallowedTools": ["Task"]
                }
            }),
        );
        meta.insert("systemPrompt".to_string(), json!("Resumed prompt"));

        session::load_session(&conn, "acp-session-1", "/tmp/ws", Vec::new(), Some(meta))
            .await
            .expect("session/load succeeds");
        drop(conn);
        let seen = responder.await.unwrap();
        let load_req = seen
            .iter()
            .find(|f| f.get("method").and_then(|v| v.as_str()) == Some("session/load"))
            .expect("agent received session/load");
        let meta_payload = &load_req["params"]["_meta"];
        assert_eq!(
            meta_payload["claudeCode"]["options"]["disallowedTools"],
            json!(["Task"]),
            "session/load for claude-code must inject disallowedTools"
        );
        assert_eq!(
            meta_payload["systemPrompt"],
            json!("Resumed prompt"),
            "session/load systemPrompt string (full replacement) preserved"
        );
    }

    #[tokio::test]
    async fn new_session_passes_through_top_level_meta_keys() {
        use crate::session::Meta;
        use serde_json::json;
        let (conn, responder) = connect_session();

        // Bare top-level _meta keys must ride session/new unchanged (codex used
        // this shape for developerInstructions before moving to the first-turn
        // prepend fallback, #479).
        let mut meta = Meta::new();
        meta.insert("customKey".to_string(), json!("custom value"));

        session::new_session(&conn, "/tmp/ws", Vec::new(), Some(meta))
            .await
            .expect("session/new succeeds");
        drop(conn);
        let seen = responder.await.unwrap();
        let new_req = seen
            .iter()
            .find(|f| f.get("method").and_then(|v| v.as_str()) == Some("session/new"))
            .expect("agent received session/new");
        let meta_payload = &new_req["params"]["_meta"];
        assert_eq!(
            meta_payload["customKey"],
            json!("custom value"),
            "session/new must pass top-level _meta keys through unchanged"
        );
    }

    #[tokio::test]
    async fn prompt_returns_stop_reason_and_load_caps_detected() {
        let provider = intent_providers::find_provider("auggie").unwrap();
        let (conn, _responder) = connect_session();
        let handshake = crate::handshake::handshake(&conn, provider).await.unwrap();
        assert!(session::supports_load_session(&handshake.initialize));

        session::load_session(&conn, "acp-session-1", "/tmp/ws", Vec::new(), None)
            .await
            .expect("session/load succeeds when capability present");

        let block = ContentBlock::Text(TextContent::new("hello"));
        let activity = session::ActivityTracker::new();
        let outcome = session::prompt(&conn, "acp-session-1", vec![block], &activity)
            .await
            .expect("session/prompt resolves");
        assert_eq!(
            serde_json::to_value(outcome.stop_reason).unwrap(),
            json!("end_turn"),
            "stop reason round-trips as end_turn"
        );
        assert!(
            outcome.usage.is_none(),
            "no usage field in the response → usage is None"
        );
    }

    #[tokio::test]
    async fn prompt_captures_end_of_turn_usage_snapshot() {
        let (conn, _responder) = connect_session_with_prompt_result(json!({
            "stopReason": "end_turn",
            "usage": {
                "totalTokens": 120,
                "inputTokens": 70,
                "outputTokens": 50,
                "thoughtTokens": 8,
                "cachedReadTokens": 30,
                "cachedWriteTokens": 4
            }
        }));
        let block = ContentBlock::Text(TextContent::new("hello"));
        let activity = session::ActivityTracker::new();
        let outcome = session::prompt(&conn, "acp-session-1", vec![block], &activity)
            .await
            .expect("session/prompt resolves");
        assert_eq!(
            serde_json::to_value(outcome.stop_reason).unwrap(),
            json!("end_turn")
        );
        let usage = outcome.usage.expect("usage snapshot parsed");
        assert_eq!(usage.total_tokens, 120);
        assert_eq!(usage.input_tokens, 70);
        assert_eq!(usage.output_tokens, 50);
        assert_eq!(usage.thought_tokens, Some(8));
        assert_eq!(usage.cached_read_tokens, Some(30));
        assert_eq!(usage.cached_write_tokens, Some(4));
    }

    #[tokio::test]
    async fn prompt_captures_response_meta() {
        // Providers may attach extension payloads at `_meta` (grok reports
        // its whole-prompt usage bill only there, intent-hq/intent#3803);
        // the outcome carries the raw map for the service layer.
        let (conn, _responder) = connect_session_with_prompt_result(json!({
            "stopReason": "end_turn",
            "_meta": {
                "modelId": "grok-code-1",
                "usage": { "inputTokens": 5000, "outputTokens": 1200 }
            }
        }));
        let block = ContentBlock::Text(TextContent::new("hello"));
        let activity = session::ActivityTracker::new();
        let outcome = session::prompt(&conn, "acp-session-1", vec![block], &activity)
            .await
            .expect("session/prompt resolves");
        assert!(outcome.usage.is_none(), "no standard usage field");
        let meta = outcome.meta.expect("_meta captured");
        assert_eq!(meta["modelId"], json!("grok-code-1"));
        assert_eq!(meta["usage"]["inputTokens"], json!(5000));
    }

    #[tokio::test]
    async fn prompt_with_malformed_usage_yields_none() {
        // The schema deserializes `usage` best-effort (`DefaultOnError`), so a
        // malformed payload degrades to None instead of failing the turn.
        let (conn, _responder) = connect_session_with_prompt_result(json!({
            "stopReason": "end_turn",
            "usage": { "totalTokens": "not-a-number" }
        }));
        let block = ContentBlock::Text(TextContent::new("hello"));
        let activity = session::ActivityTracker::new();
        let outcome = session::prompt(&conn, "acp-session-1", vec![block], &activity)
            .await
            .expect("session/prompt resolves despite malformed usage");
        assert_eq!(
            serde_json::to_value(outcome.stop_reason).unwrap(),
            json!("end_turn")
        );
        assert!(outcome.usage.is_none(), "malformed usage degrades to None");
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
        session::new_session(&conn, "/tmp/ws", Vec::new(), None)
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
            SessionUpdate::AgentMessageChunk(agent_client_protocol::schema::v1::ContentChunk::new(
                ContentBlock::Text(TextContent::new("tok")),
            ));
        assert_eq!(
            session::map_session_update(&update),
            Some(MappedUpdate::Chunk {
                content: json!("tok"),
                text: Some("tok".to_string()),
                thought: false,
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
        // The title has no `<name>: <description>` prefix shape, so the
        // derived tool name is the title verbatim.
        assert_eq!(tc.tool_name, "Edit src/lib.rs");
        assert_eq!(tc.title, "Edit src/lib.rs");
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
    fn derive_tool_name_splits_prefix_and_strips_server_suffix() {
        // `<name>: <description>` titles yield the bare tool name; the registry
        // names are bare and the ACP provider (auggie) appends `_workspace-mcp`
        // on its side, so any number of trailing server suffixes is stripped.
        assert_eq!(
            session::derive_tool_name("add_to_note_workspace-mcp: Append content", None),
            "add_to_note"
        );
        assert_eq!(
            session::derive_tool_name("add_to_note_workspace-mcp_workspace-mcp: x", None),
            "add_to_note"
        );
        assert_eq!(
            session::derive_tool_name("add_to_note_workspace-mcp", None),
            "add_to_note"
        );
        assert_eq!(
            session::derive_tool_name("sub-agent-explore: Explore the AI agent system", None),
            "sub-agent-explore"
        );
        // Titles without the `<name>: ` prefix shape pass through unchanged.
        assert_eq!(
            session::derive_tool_name("Edit src/lib.rs", None),
            "Edit src/lib.rs"
        );
        assert_eq!(
            session::derive_tool_name("https://example.com/x", None),
            "https://example.com/x"
        );
        assert_eq!(session::derive_tool_name("10:15 sync", None), "10:15 sync");
    }

    #[test]
    fn derive_tool_name_uses_raw_input_when_title_is_prose() {
        // information_request → codebase-retrieval; "conversation" in the
        // title routes to conversation-retrieval instead.
        assert_eq!(
            session::derive_tool_name(
                "Search codebase",
                Some(&json!({ "information_request": "where is auth?" })),
            ),
            "codebase-retrieval"
        );
        assert_eq!(
            session::derive_tool_name(
                "Search Conversation history",
                Some(&json!({ "information_request": "when did we discuss auth?" })),
            ),
            "conversation-retrieval"
        );
        // command ∈ {str_replace, insert, create} → str-replace-editor.
        for cmd in ["str_replace", "insert", "create"] {
            assert_eq!(
                session::derive_tool_name(
                    "Edit file",
                    Some(&json!({ "command": cmd, "path": "a.rs" })),
                ),
                "str-replace-editor",
                "command={cmd}"
            );
        }
        // file_content + path + instructions_reminder → save-file.
        assert_eq!(
            session::derive_tool_name(
                "Create",
                Some(&json!({
                    "file_content": "hi",
                    "path": "a.rs",
                    "instructions_reminder": "…",
                })),
            ),
            "save-file"
        );
        // path + view_range → view.
        assert_eq!(
            session::derive_tool_name(
                "Read",
                Some(&json!({ "path": "a.rs", "view_range": [1, 10] })),
            ),
            "view"
        );
        // file_paths array → remove-files.
        assert_eq!(
            session::derive_tool_name("Delete", Some(&json!({ "file_paths": ["a.rs", "b.rs"] })),),
            "remove-files"
        );
        // input string containing "*** Begin Patch" → apply_patch.
        assert_eq!(
            session::derive_tool_name(
                "Apply patch",
                Some(&json!({ "input": "*** Begin Patch\n*** End Patch" })),
            ),
            "apply_patch"
        );
    }

    #[test]
    fn derive_tool_name_strips_opencode_mcp_prefix() {
        // Opencode names MCP tools `<server>_<tool>` (leading prefix), the
        // mirror image of auggie's trailing suffix. Captured from opencode
        // 1.18.3: `workspace-mcp_echo` with kind "other" and empty rawInput.
        assert_eq!(
            session::derive_tool_name("workspace-mcp_echo", Some(&json!({}))),
            "echo"
        );
        assert_eq!(
            session::derive_tool_name("workspace-mcp_add_to_note", None),
            "add_to_note"
        );
        // Doubled prefix strips repeatedly, like the doubled suffix.
        assert_eq!(
            session::derive_tool_name("workspace-mcp_workspace-mcp_read_note", None),
            "read_note"
        );
        // A bare affix never strips to the empty string.
        assert_eq!(
            session::derive_tool_name("workspace-mcp_", None),
            "workspace-mcp_"
        );
    }

    #[test]
    fn derive_tool_name_rewrites_codex_dot_separated_mcp_titles() {
        // Codex titles MCP tools `mcp.<server>.<tool>`; the title is
        // rewritten to `{server}_{tool}` and fed through the affix strip —
        // the same treatment as the codex nested-input unwrap.
        assert_eq!(
            session::derive_tool_name("mcp.workspace-mcp.workspace_api", None),
            "workspace_api"
        );
        assert_eq!(
            session::derive_tool_name("mcp.some-server.read_note", None),
            "some-server_read_note"
        );
        // Prose titles containing dots (with whitespace) never match.
        assert_eq!(
            session::derive_tool_name("Read config.toml and summarize", None),
            "Read config.toml and summarize"
        );
        assert_eq!(
            session::derive_tool_name("mcp.workspace-mcp.some tool", None),
            "mcp.workspace-mcp.some tool"
        );
        // Missing server or tool segment → no match, title passes through.
        assert_eq!(session::derive_tool_name("mcp.server", None), "mcp.server");
        assert_eq!(session::derive_tool_name("mcp..tool", None), "mcp..tool");
        assert_eq!(
            session::derive_tool_name("mcp.server.", None),
            "mcp.server."
        );
    }

    #[test]
    fn derive_tool_name_rewrites_claude_code_mcp_titles() {
        // Claude Code titles MCP tools `mcp__<server>__<tool>`; the title is
        // rewritten to `{server}_{tool}` and fed through the affix strip —
        // the same treatment as the codex dot rule.
        assert_eq!(
            session::derive_tool_name("mcp__workspace-mcp__workspace_api", None),
            "workspace_api"
        );
        assert_eq!(
            session::derive_tool_name("mcp__github__list_issues", None),
            "github_list_issues"
        );
        // Tool names containing single underscores survive intact; the
        // server segment ends at the first `__`.
        assert_eq!(
            session::derive_tool_name("mcp__endara__execute_tools_Endara", None),
            "endara_execute_tools_Endara"
        );
        // Prose titles containing `mcp__` mid-string (with whitespace) never
        // match.
        assert_eq!(
            session::derive_tool_name("Call mcp__github__list_issues now", None),
            "Call mcp__github__list_issues now"
        );
        assert_eq!(
            session::derive_tool_name("mcp__workspace-mcp__some tool", None),
            "mcp__workspace-mcp__some tool"
        );
        // Missing server or tool segment → no match, title passes through.
        assert_eq!(
            session::derive_tool_name("mcp__server", None),
            "mcp__server"
        );
        assert_eq!(
            session::derive_tool_name("mcp____tool", None),
            "mcp____tool"
        );
        assert_eq!(
            session::derive_tool_name("mcp__server__", None),
            "mcp__server__"
        );
    }

    #[test]
    fn derive_tool_name_recognizes_opencode_input_shapes() {
        // Captured from opencode 1.18.3: once arguments stream in, titles
        // turn into raw prose (the command line, a file path, a regex), so
        // the camelCase rawInput shapes identify the tool.
        // filePath + oldString/newString → edit.
        assert_eq!(
            session::derive_tool_name(
                "edit",
                Some(&json!({
                    "filePath": "/tmp/sandbox/notes.txt",
                    "oldString": "test note",
                    "newString": "edited note",
                })),
            ),
            "edit"
        );
        // filePath + content → write.
        assert_eq!(
            session::derive_tool_name(
                "write",
                Some(&json!({ "filePath": "/tmp/sandbox/notes.txt", "content": "test note" })),
            ),
            "write"
        );
        // filePath alone → read, even when the title is the raw path.
        assert_eq!(
            session::derive_tool_name(
                "private/tmp/sandbox/sample.txt",
                Some(&json!({ "filePath": "/private/tmp/sandbox/sample.txt" })),
            ),
            "read"
        );
        // command (string) + cwd → bash; opencode titles the call with the
        // raw command line itself.
        assert_eq!(
            session::derive_tool_name(
                "echo hello-from-bash",
                Some(&json!({ "command": "echo hello-from-bash", "cwd": "/tmp/sandbox" })),
            ),
            "bash"
        );
        // url → web-fetch.
        assert_eq!(
            session::derive_tool_name(
                "https://example.com/ (text/html)",
                Some(&json!({ "url": "https://example.com/" })),
            ),
            "web-fetch"
        );
        // A bare `webfetch` title (opencode's first tool_call frame carries
        // an empty rawInput) normalizes to the canonical builtin name.
        assert_eq!(
            session::derive_tool_name("webfetch", Some(&json!({}))),
            "web-fetch"
        );
    }

    #[test]
    fn derive_tool_name_opencode_shapes_do_not_capture_other_providers() {
        // Auggie's launch-process also carries a string `command` + `cwd`,
        // but always with `wait`/`max_wait_seconds` — it must not become
        // `bash`.
        assert_eq!(
            session::derive_tool_name(
                "Run tests",
                Some(&json!({
                    "command": "cargo test",
                    "cwd": "/repo",
                    "wait": true,
                    "max_wait_seconds": 300,
                })),
            ),
            "Run tests"
        );
        // Codex sends `command` as an array — the string check rejects it.
        assert_eq!(
            session::derive_tool_name(
                "Run tests",
                Some(&json!({ "command": ["bash", "-lc", "cargo test"], "cwd": "/repo" })),
            ),
            "Run tests"
        );
        // Auggie's snake_case `path` shapes stay on the existing rules and
        // never hit the camelCase branch.
        assert_eq!(
            session::derive_tool_name("Edit src/lib.rs", Some(&json!({ "path": "src/lib.rs" }))),
            "Edit src/lib.rs"
        );
        // JS-truthy semantics on filePath: null/empty falls through.
        assert_eq!(
            session::derive_tool_name("read", Some(&json!({ "filePath": null }))),
            "read"
        );
        // cwd must be a non-empty string too; a null cwd is not bash.
        assert_eq!(
            session::derive_tool_name(
                "echo hi",
                Some(&json!({ "command": "echo hi", "cwd": null })),
            ),
            "echo hi"
        );
        // Pattern-only inputs (grep/glob) intentionally derive nothing; the
        // bare title already names the tool.
        assert_eq!(
            session::derive_tool_name("grep", Some(&json!({ "pattern": "findme" }))),
            "grep"
        );
    }

    #[test]
    fn maps_opencode_tool_calls_with_names_and_kinds() {
        // Full mapping for the captured opencode frames: real tool names AND
        // FE-taxonomy kinds so the chat stops rendering wrench-icon `other`
        // entries with raw prose titles.
        let read = ToolCall::new("t1", "read")
            .kind(ToolKind::Read)
            .raw_input(json!({ "filePath": "/tmp/sandbox/sample.txt" }));
        let MappedUpdate::ToolCall(tc) =
            session::map_session_update(&SessionUpdate::ToolCall(read)).unwrap()
        else {
            panic!("expected tool call");
        };
        assert_eq!(tc.tool_name, "read");
        assert_eq!(tc.tool_kind, "file");

        let edit = ToolCall::new("t2", "edit").kind(ToolKind::Edit).raw_input(
            json!({ "filePath": "/tmp/sandbox/notes.txt", "oldString": "a", "newString": "b" }),
        );
        let MappedUpdate::ToolCall(tc) =
            session::map_session_update(&SessionUpdate::ToolCall(edit)).unwrap()
        else {
            panic!("expected tool call");
        };
        assert_eq!(tc.tool_name, "edit");
        assert_eq!(tc.tool_kind, "file");

        // Mid-flight bash update: the title is the raw command line.
        let bash = ToolCallUpdate::new(
            "t3",
            ToolCallUpdateFields::new()
                .title("echo hello-from-bash")
                .kind(ToolKind::Execute)
                .raw_input(json!({ "command": "echo hello-from-bash", "cwd": "/tmp/sandbox" })),
        );
        let MappedUpdate::ToolCall(tc) =
            session::map_session_update(&SessionUpdate::ToolCallUpdate(bash)).unwrap()
        else {
            panic!("expected tool call");
        };
        assert_eq!(tc.tool_name, "bash");
        assert_eq!(tc.tool_kind, "terminal");

        let grep = ToolCall::new("t4", "grep")
            .kind(ToolKind::Search)
            .raw_input(json!({ "pattern": "findme" }));
        let MappedUpdate::ToolCall(tc) =
            session::map_session_update(&SessionUpdate::ToolCall(grep)).unwrap()
        else {
            panic!("expected tool call");
        };
        assert_eq!(tc.tool_name, "grep");
        assert_eq!(tc.tool_kind, "search");

        // webfetch: name normalizes to web-fetch; ToolKind::Fetch has no
        // dedicated word in the file|terminal|search|note|git|other taxonomy
        // and stays "other" (the FE picks its fetch display by tool name).
        let fetch = ToolCall::new("t5", "webfetch")
            .kind(ToolKind::Fetch)
            .raw_input(json!({}));
        let MappedUpdate::ToolCall(tc) =
            session::map_session_update(&SessionUpdate::ToolCall(fetch)).unwrap()
        else {
            panic!("expected tool call");
        };
        assert_eq!(tc.tool_name, "web-fetch");
        assert_eq!(tc.tool_kind, "other");

        // Opencode-prefixed MCP tool: name recovers the registry tool and the
        // note-name inference kicks in for the kind.
        let mcp = ToolCall::new("t6", "workspace-mcp_add_to_note")
            .kind(ToolKind::Other)
            .raw_input(json!({}));
        let MappedUpdate::ToolCall(tc) =
            session::map_session_update(&SessionUpdate::ToolCall(mcp)).unwrap()
        else {
            panic!("expected tool call");
        };
        assert_eq!(tc.tool_name, "add_to_note");
        assert_eq!(tc.tool_kind, "note");
    }

    #[test]
    fn derive_tool_name_title_prefix_wins_over_input_derivation() {
        // Even when raw_input matches an input-derivation shape, an
        // unambiguous `<name>: <desc>` title prefix takes precedence — the
        // ACP provider (or MCP registry name) is authoritative for the tool
        // identity when it is explicit.
        assert_eq!(
            session::derive_tool_name(
                "read_note_workspace-mcp: Read the spec",
                Some(&json!({ "information_request": "spec contents" })),
            ),
            "read_note"
        );
        // Same for the `_workspace-mcp` suffix without a `<name>: ` prefix:
        // the suffix-strip already identified this as an MCP tool.
        assert_eq!(
            session::derive_tool_name(
                "read_note_workspace-mcp",
                Some(&json!({ "information_request": "spec" })),
            ),
            "read_note"
        );
    }

    #[test]
    fn derive_tool_name_falls_back_when_input_has_no_match() {
        // Prose title + raw_input that doesn't match any pattern → title
        // passes through verbatim.
        assert_eq!(
            session::derive_tool_name("Edit src/lib.rs", Some(&json!({ "path": "src/lib.rs" })),),
            "Edit src/lib.rs"
        );
        // Non-object raw_input (e.g. bare string) is ignored.
        assert_eq!(
            session::derive_tool_name("Do something", Some(&json!("hello"))),
            "Do something"
        );
        // Empty raw_input object → title passes through verbatim.
        assert_eq!(
            session::derive_tool_name("Edit src/lib.rs", Some(&json!({}))),
            "Edit src/lib.rs"
        );
        // Path is present-but-null (or empty) → does not misclassify as
        // `view` / `save-file`; JS-truthy semantics reject null/"".
        assert_eq!(
            session::derive_tool_name(
                "Read",
                Some(&json!({ "path": null, "view_range": [1, 10] })),
            ),
            "Read"
        );
        assert_eq!(
            session::derive_tool_name(
                "Create",
                Some(&json!({
                    "file_content": "x",
                    "path": "",
                    "instructions_reminder": "…",
                })),
            ),
            "Create"
        );
    }

    #[test]
    fn derive_tool_name_input_derivation_order_matches_reference() {
        // First matching pattern wins, in the reference's order
        // (acp-provider-streaming.ts ~L1635–1666):
        // command → save-file → view → retrieval → remove-files → apply_patch.
        //
        // `command=str_replace` + `information_request` + `view_range`:
        // command wins.
        assert_eq!(
            session::derive_tool_name(
                "Do stuff",
                Some(&json!({
                    "command": "str_replace",
                    "path": "a.rs",
                    "information_request": "…",
                    "view_range": [1, 10],
                })),
            ),
            "str-replace-editor"
        );
        // `path` + `view_range` + `information_request`: view wins over
        // retrieval (view is checked first).
        assert_eq!(
            session::derive_tool_name(
                "Do stuff",
                Some(&json!({
                    "path": "a.rs",
                    "view_range": [1, 10],
                    "information_request": "…",
                })),
            ),
            "view"
        );
        // `information_request` + `file_paths`: retrieval wins over
        // remove-files (retrieval is checked first).
        assert_eq!(
            session::derive_tool_name(
                "Do stuff",
                Some(&json!({
                    "information_request": "…",
                    "file_paths": ["a.rs"],
                })),
            ),
            "codebase-retrieval"
        );
        // `file_paths` + `input: "*** Begin Patch"`: remove-files wins
        // over apply_patch (file_paths is checked first).
        assert_eq!(
            session::derive_tool_name(
                "Do stuff",
                Some(&json!({
                    "file_paths": ["a.rs"],
                    "input": "*** Begin Patch\n*** End Patch",
                })),
            ),
            "remove-files"
        );
    }

    #[test]
    fn input_derived_retrieval_names_classify_as_search() {
        // A prose title + information_request must yield tool_name
        // "codebase-retrieval" AND tool_kind "search" so the FE renders the
        // context-engine card even when the ACP provider set ToolKind::Other.
        let call = ToolCall::new("t1", "Search codebase")
            .kind(ToolKind::Other)
            .raw_input(json!({ "information_request": "where is auth?" }));
        let MappedUpdate::ToolCall(tc) =
            session::map_session_update(&SessionUpdate::ToolCall(call)).unwrap()
        else {
            panic!("expected tool call");
        };
        assert_eq!(tc.tool_name, "codebase-retrieval");
        assert_eq!(tc.title, "Search codebase");
        assert_eq!(tc.tool_kind, "search");
    }

    #[test]
    fn codex_nested_mcp_input_unwraps_arguments_and_derives_name() {
        // Codex nests the model's parameters under `arguments` alongside
        // `server`/`tool`. The mapper must hoist them to the top level and
        // derive the name from `{server}_{tool}` — the workspace-mcp prefix
        // strip then yields the bare registry name.
        let call = ToolCall::new("t1", "workspace_api").kind(ToolKind::Other).raw_input(json!({
            "arguments": { "summary": "Read spec", "code": "return await ws.note.read('spec')" },
            "server": "workspace-mcp",
            "tool": "workspace_api",
        }));
        let MappedUpdate::ToolCall(tc) =
            session::map_session_update(&SessionUpdate::ToolCall(call)).unwrap()
        else {
            panic!("expected tool call");
        };
        assert_eq!(tc.tool_name, "workspace_api");
        assert_eq!(tc.input["summary"], json!("Read spec"));
        assert_eq!(tc.input["code"], json!("return await ws.note.read('spec')"));
        assert!(
            tc.input.get("arguments").is_none(),
            "unwrapped input must not retain the nested arguments object"
        );
        assert!(tc.input.get("server").is_none());
        assert!(tc.input.get("tool").is_none());
    }

    #[test]
    fn codex_unwrap_preserves_acp_title_and_keeps_other_server_names() {
        // `_acpTitle` on the outer object rides the unwrap; a server other
        // than workspace-mcp has no affix to strip, so the derived name stays
        // `{server}_{tool}`.
        let call = ToolCall::new("t1", "list_issues")
            .kind(ToolKind::Other)
            .raw_input(json!({
                "arguments": { "repo": "intent-hq/monorepo" },
                "server": "github",
                "tool": "list_issues",
                "_acpTitle": "List issues",
            }));
        let MappedUpdate::ToolCall(tc) =
            session::map_session_update(&SessionUpdate::ToolCall(call)).unwrap()
        else {
            panic!("expected tool call");
        };
        assert_eq!(tc.tool_name, "github_list_issues");
        assert_eq!(tc.input["repo"], json!("intent-hq/monorepo"));
        assert_eq!(tc.input["_acpTitle"], json!("List issues"));
    }

    #[test]
    fn non_codex_shapes_pass_through_verbatim() {
        // `arguments` not an object → no unwrap.
        let array_args = json!({
            "arguments": ["not", "an", "object"],
            "server": "workspace-mcp",
            "tool": "workspace_api",
        });
        let call = ToolCall::new("t1", "Some tool")
            .kind(ToolKind::Other)
            .raw_input(array_args.clone());
        let MappedUpdate::ToolCall(tc) =
            session::map_session_update(&SessionUpdate::ToolCall(call)).unwrap()
        else {
            panic!("expected tool call");
        };
        assert_eq!(tc.input, array_args);
        assert_eq!(tc.tool_name, "Some tool");

        // Missing `tool` → no unwrap.
        let no_tool = json!({ "arguments": { "a": 1 }, "server": "workspace-mcp" });
        let call = ToolCall::new("t2", "Some tool")
            .kind(ToolKind::Other)
            .raw_input(no_tool.clone());
        let MappedUpdate::ToolCall(tc) =
            session::map_session_update(&SessionUpdate::ToolCall(call)).unwrap()
        else {
            panic!("expected tool call");
        };
        assert_eq!(tc.input, no_tool);

        // Missing `server` → no unwrap.
        let no_server = json!({ "arguments": { "a": 1 }, "tool": "workspace_api" });
        let call = ToolCall::new("t3", "Some tool")
            .kind(ToolKind::Other)
            .raw_input(no_server.clone());
        let MappedUpdate::ToolCall(tc) =
            session::map_session_update(&SessionUpdate::ToolCall(call)).unwrap()
        else {
            panic!("expected tool call");
        };
        assert_eq!(tc.input, no_server);
    }

    #[test]
    fn codex_unwrap_applies_to_tool_call_updates() {
        // Sparse updates may re-deliver the raw input; the same unwrap must
        // apply on the `tool_call_update` path.
        let update = ToolCallUpdate::new(
            "t1",
            ToolCallUpdateFields::new()
                .status(ToolCallStatus::Completed)
                .raw_input(json!({
                    "arguments": { "summary": "Read spec", "code": "return 1" },
                    "server": "workspace-mcp",
                    "tool": "workspace_api",
                    "_acpTitle": "Workspace API",
                })),
        );
        let MappedUpdate::ToolCall(tc) =
            session::map_session_update(&SessionUpdate::ToolCallUpdate(update)).unwrap()
        else {
            panic!("expected tool call");
        };
        assert_eq!(tc.tool_name, "workspace_api");
        assert_eq!(tc.status, "completed");
        assert_eq!(tc.input["summary"], json!("Read spec"));
        assert_eq!(tc.input["code"], json!("return 1"));
        assert_eq!(tc.input["_acpTitle"], json!("Workspace API"));
        assert!(tc.input.get("arguments").is_none());

        // Non-codex update input passes through verbatim.
        let plain = ToolCallUpdate::new(
            "t2",
            ToolCallUpdateFields::new().raw_input(json!({ "arguments": "text", "tool": "x" })),
        );
        let MappedUpdate::ToolCall(tc) =
            session::map_session_update(&SessionUpdate::ToolCallUpdate(plain)).unwrap()
        else {
            panic!("expected tool call");
        };
        assert_eq!(tc.input, json!({ "arguments": "text", "tool": "x" }));
    }

    /// `agent_thought_chunk` travels the same path as a message chunk with the
    /// `thought` marker set (Zed's `is_thought`), carrying the extracted text.
    #[test]
    fn maps_thought_chunk_with_thought_marker() {
        let thought =
            SessionUpdate::AgentThoughtChunk(agent_client_protocol::schema::v1::ContentChunk::new(
                ContentBlock::Text(TextContent::new("thinking")),
            ));
        assert_eq!(
            session::map_session_update(&thought),
            Some(MappedUpdate::Chunk {
                content: json!("thinking"),
                text: Some("thinking".to_string()),
                thought: true,
            })
        );
    }

    #[test]
    fn unmapped_variants_return_none() {
        let plan: SessionUpdate = serde_json::from_value(json!({
            "sessionUpdate": "plan",
            "entries": []
        }))
        .expect("plan update deserializes");
        assert_eq!(session::map_session_update(&plan), None);
    }

    #[test]
    fn map_notification_parses_session_update() {
        let note = SessionNotification::new(
            "acp-session-1",
            SessionUpdate::AgentMessageChunk(agent_client_protocol::schema::v1::ContentChunk::new(
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
                thought: false,
            })
        );

        let other = IncomingNotification {
            method: "session/other".to_string(),
            params: json!({}),
        };
        assert_eq!(session::map_notification(&other), None);
    }

    /// `usage_update` maps to [`MappedUpdate::Usage`] carrying the required
    /// context-occupancy fields (`used`/`size`, intent-hq/intent#3797) plus
    /// the `cost` object when reported (§5.23) — a cost-less provider must
    /// never fabricate a zero cost figure.
    #[test]
    fn usage_update_maps_used_size_and_optional_cost() {
        let with_cost: SessionUpdate = serde_json::from_value(json!({
            "sessionUpdate": "usage_update",
            "used": 53_000,
            "size": 200_000,
            "cost": { "amount": 1.25, "currency": "USD" }
        }))
        .expect("usage_update with cost deserializes");
        assert_eq!(
            session::map_session_update(&with_cost),
            Some(MappedUpdate::Usage(session::MappedUsage {
                used: 53_000,
                size: 200_000,
                cost: Some(session::MappedUsageCost {
                    amount: 1.25,
                    currency: "USD".to_string(),
                }),
            }))
        );

        let without_cost: SessionUpdate = serde_json::from_value(json!({
            "sessionUpdate": "usage_update",
            "used": 53_000,
            "size": 200_000
        }))
        .expect("usage_update without cost deserializes");
        assert_eq!(
            session::map_session_update(&without_cost),
            Some(MappedUpdate::Usage(session::MappedUsage {
                used: 53_000,
                size: 200_000,
                cost: None,
            }))
        );
    }
}

/// Agent→BE MCP server, config conversions, env baseline/redaction, and the
/// per-agent-type tool denylist (§6.8 / §18.4 `DoD`).
mod mcp_tests {
    use std::ffi::OsStr;
    use std::path::Path;
    use std::sync::{Arc, Mutex};

    use intent_core::{
        AgentCreateExtra, AgentId, BoxFuture, Error, GitAgentCommitResult, Note, NoteAddInput,
        NoteAddResult, NoteCreate, NoteCreateResult, NoteId, Result, TaskAssignAgentResult,
        TaskGetMyTaskResult, TaskMetadata, TaskStatus, TaskSubtask, WorkspaceApi, WorkspaceId,
    };
    use serde_json::{json, Value};

    use crate::mcp_config::{
        apply_baseline_env_to_stdio_servers, normalize_mcp_servers,
        normalize_spaced_bridge_command, to_acp_mcp_servers, to_acp_session_mcp_servers,
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
    /// A recorded `git_agent_commit` call: (message, `agent_id`, `linked_note_id`).
    type CommitRecord = (String, Option<String>, Option<String>);
    /// A recorded `agent_create` call:
    /// (name, specialist, `parent_agent_id`, `idempotency_key`, metadata).
    type AgentCreateRecord = (
        Option<String>,
        Option<String>,
        Option<String>,
        Option<String>,
        Option<Value>,
    );

    #[derive(Default)]
    pub(super) struct MockApi {
        pub(super) added: Mutex<Vec<(String, String)>>,
        pub(super) committed: Mutex<Vec<CommitRecord>>,
        /// Recorded `create_note` calls: (title, `idempotency_key`).
        pub(super) created: Mutex<Vec<(String, Option<String>)>>,
        pub(super) agent_creates: Mutex<Vec<AgentCreateRecord>>,
        /// Recorded `agent_send_message` calls: (`agent_id`, content).
        pub(super) sent: Mutex<Vec<(String, String)>>,
        /// Recorded `assign_agent` calls: (`note_id`, `agent_id`).
        pub(super) assigned: Mutex<Vec<(String, String)>>,
        /// Recorded `agent_watch_completion` calls: (`parent_id`, `child_id`).
        pub(super) watched: Mutex<Vec<(String, String)>>,
        /// Recorded `agent_watch_completion_for_sender` calls: (`caller_id`, `target_id`).
        pub(super) sender_watched: Mutex<Vec<(String, String)>>,
        /// Recorded `get_my_task` calls: `task_note_id`.
        pub(super) get_my_task_calls: Mutex<Vec<String>>,
    }

    impl WorkspaceApi for MockApi {
        fn create_note(
            &self,
            workspace_id: WorkspaceId,
            input: NoteCreate,
            idempotency_key: Option<String>,
            _caller_agent_id: Option<AgentId>,
        ) -> BoxFuture<'_, Result<NoteCreateResult>> {
            self.created
                .lock()
                .unwrap()
                .push((input.title.clone(), idempotency_key));
            Box::pin(async move {
                Ok(NoteCreateResult {
                    note: Note {
                        id: NoteId::from_string("n-created"),
                        workspace_id,
                        title: input.title,
                        content: input.content.unwrap_or_default(),
                        content_type: intent_core::ContentType::default(),
                        tags: input.tags.unwrap_or_default(),
                        is_pinned: false,
                        is_archived: false,
                        is_default: false,
                        parent_id: None,
                        visibility: intent_core::NoteVisibility::default(),
                        metadata: intent_core::NoteMetadata::default(),
                        created_at: "2026-01-01T00:00:00Z".to_string(),
                        rev: 0,
                        updated_at: "2026-01-01T00:00:00Z".to_string(),
                    },
                    converted_count: 0,
                    created_task_note_ids: Vec::new(),
                    created_tasks: Vec::new(),
                    warnings: Vec::new(),
                })
            })
        }

        fn list_notes<'a>(
            &'a self,
            _workspace_id: &'a WorkspaceId,
        ) -> BoxFuture<'a, Result<Vec<Note>>> {
            Box::pin(async { Ok(Vec::new()) })
        }

        #[allow(clippy::too_many_arguments)]
        fn git_agent_commit(
            &self,
            _workspace_id: WorkspaceId,
            message: String,
            agent_id: Option<AgentId>,
            linked_note_id: Option<NoteId>,
            _files: Option<Vec<String>>,
            _user_requested: bool,
            _git_root_id: Option<intent_core::WorkspaceGitRootId>,
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
            _caller_agent_id: Option<AgentId>,
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
                    created_tasks: Vec::new(),
                    warnings: Vec::new(),
                })
            })
        }

        #[allow(clippy::too_many_arguments)]
        fn agent_create(
            &self,
            _workspace_id: WorkspaceId,
            name: Option<String>,
            _model: Option<String>,
            specialist_id: Option<String>,
            parent_agent_id: Option<AgentId>,
            idempotency_key: Option<String>,
            extra: AgentCreateExtra,
        ) -> BoxFuture<'_, Result<Value>> {
            self.agent_creates.lock().unwrap().push((
                name.clone(),
                specialist_id,
                parent_agent_id.as_ref().map(|a| a.as_str().to_string()),
                idempotency_key,
                extra.metadata,
            ));
            Box::pin(async move {
                Ok(json!({
                    "agent": { "id": "agent-child", "name": name.unwrap_or_default() }
                }))
            })
        }

        #[allow(clippy::too_many_arguments)]
        fn agent_send_message(
            &self,
            _workspace_id: WorkspaceId,
            agent_id: AgentId,
            content: String,
            _message_id: Option<String>,
            _image_blocks: Option<Value>,
            _file_blocks: Option<Value>,
            _priority: Option<String>,
            _note_ids: Option<Value>,
            _stdin_context: Option<String>,
            _context_references: Option<Value>,
            _message_metadata: Option<Value>,
            _origin: intent_core::MessageOrigin,
        ) -> BoxFuture<'_, Result<Value>> {
            self.sent
                .lock()
                .unwrap()
                .push((agent_id.as_str().to_string(), content));
            Box::pin(async { Ok(json!({ "ok": true })) })
        }

        fn assign_agent(
            &self,
            _workspace_id: WorkspaceId,
            note_id: NoteId,
            agent_id: String,
            _force: Option<bool>,
        ) -> BoxFuture<'_, Result<TaskAssignAgentResult>> {
            self.assigned
                .lock()
                .unwrap()
                .push((note_id.as_str().to_string(), agent_id.clone()));
            Box::pin(async move {
                Ok(TaskAssignAgentResult {
                    ok: true,
                    note_id,
                    agent_id: AgentId::from_string(agent_id),
                })
            })
        }

        fn agent_watch_completion(
            &self,
            _workspace_id: WorkspaceId,
            parent_agent_id: AgentId,
            child_agent_id: AgentId,
        ) -> BoxFuture<'_, Result<Value>> {
            self.watched.lock().unwrap().push((
                parent_agent_id.as_str().to_string(),
                child_agent_id.as_str().to_string(),
            ));
            Box::pin(async { Ok(json!({ "ok": true, "subscriptionId": "sub-watch-1" })) })
        }

        fn agent_watch_completion_for_sender(
            &self,
            _workspace_id: WorkspaceId,
            caller_agent_id: AgentId,
            target_agent_id: AgentId,
        ) -> BoxFuture<'_, Result<Value>> {
            self.sender_watched.lock().unwrap().push((
                caller_agent_id.as_str().to_string(),
                target_agent_id.as_str().to_string(),
            ));
            Box::pin(async { Ok(json!({ "ok": true, "subscriptionId": "sub-sender-1" })) })
        }

        fn agent_send_to_task(
            &self,
            _workspace_id: WorkspaceId,
            _task_note_id: NoteId,
            _message: String,
            _priority: Option<String>,
            _message_metadata: Option<Value>,
        ) -> BoxFuture<'_, Result<Value>> {
            Box::pin(async {
                Ok(json!({ "ok": true, "agentId": "agent-assignee", "result": { "ok": true } }))
            })
        }

        fn get_my_task(
            &self,
            _workspace_id: WorkspaceId,
            task_note_id: NoteId,
        ) -> BoxFuture<'_, Result<TaskGetMyTaskResult>> {
            let key = task_note_id.as_str().to_string();
            self.get_my_task_calls.lock().unwrap().push(key.clone());
            Box::pin(async move {
                if key == "task-missing" {
                    // Mirrors the services impl, which wraps the store's
                    // NotFound as Error::Internal("Task note not found").
                    return Err(Error::Internal("Task note not found".to_string()));
                }
                let task = TaskMetadata {
                    status: TaskStatus::InProgress,
                    assigned_agent_ids: vec![AgentId::from_string("agent-77")],
                    acceptance_criteria: vec!["criterion-a".to_string()],
                    estimated_effort: Some("small".to_string()),
                    ..TaskMetadata::default()
                };
                Ok(TaskGetMyTaskResult {
                    note_id: task_note_id.clone(),
                    title: "My Task".to_string(),
                    content: "task body".to_string(),
                    status: TaskStatus::InProgress,
                    parent_id: Some(NoteId::from_string("spec")),
                    subtasks: vec![TaskSubtask {
                        id: NoteId::from_string("child-1"),
                        title: "Child".to_string(),
                        status: "not_started".to_string(),
                    }],
                    assigned_agents: task.assigned_agent_ids.clone(),
                    task_metadata: task,
                    rev: 3,
                    unmet_depends_on: Vec::new(),
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

    // Discrete-tool dispatch tests (`add_to_note`, `git_commit`,
    // `create_agent`, `send_message_to_*`) were removed in WSAPI-8: those
    // tools are no longer registered, and their bindings are exercised
    // through `workspace_api` in the `wsapi{3,4,5,6}_bindings_tests`
    // modules. The tests below are gated out via `cfg(any())` so the file
    // history stays legible; they will be removed outright once the
    // cutover has settled.
    #[cfg(all(test, any()))]
    async fn _dead_create_agent_creates_assigns_and_starts_first_turn() {
        let api = Arc::new(MockApi::default());
        let srv = WorkspaceMcpServer::new(api.clone(), WorkspaceId::from_string("ws-1"))
            .with_caller_agent_id(Some(AgentId::from_string("agent-77")));
        let resp = srv
            .handle_message(&json!({
                "jsonrpc": "2.0", "id": 11, "method": "tools/call",
                "params": {
                    "name": "create_agent",
                    "arguments": {
                        "name": "Bug Fixer",
                        "initialMessage": "fix the bug",
                        "specialist": "implementor",
                        "taskNoteId": "n-task"
                    }
                }
            }))
            .await
            .unwrap();
        assert_eq!(resp["result"]["isError"], json!(false));
        let text = resp["result"]["content"][0]["text"].as_str().unwrap();
        let parsed: Value = serde_json::from_str(text).unwrap();
        assert_eq!(parsed["ok"], json!(true));
        assert_eq!(parsed["agentId"], json!("agent-child"));
        assert_eq!(parsed["name"], json!("Bug Fixer"));
        assert_eq!(parsed["subscriptionId"], json!("sub-watch-1"));

        // The create op was parent-attributed to the MCP caller, carried the
        // specialist through, and minted a fresh idempotency key.
        let creates = api.agent_creates.lock().unwrap();
        assert_eq!(creates.len(), 1);
        let (name, specialist, parent, idem, metadata) = &creates[0];
        assert_eq!(name.as_deref(), Some("Bug Fixer"));
        assert_eq!(specialist.as_deref(), Some("implementor"));
        assert_eq!(parent.as_deref(), Some("agent-77"));
        assert!(idem.is_some(), "a fresh idempotency key is minted");
        let meta = metadata.as_ref().expect("metadata block persisted");
        assert_eq!(meta["createdByAgentId"], json!("agent-77"));
        assert_eq!(meta["delegationDepth"], json!(1));
        assert_eq!(meta["initialMessage"], json!("fix the bug"));
        assert_eq!(meta["taskNoteId"], json!("n-task"));
        assert_eq!(meta["isBackground"], json!(true));
        drop(creates);

        // The child was assigned to the supplied task note…
        assert_eq!(
            *api.assigned.lock().unwrap(),
            vec![("n-task".to_string(), "agent-child".to_string())]
        );
        // …the caller was auto-subscribed to the child's completion (AS-5)…
        assert_eq!(
            *api.watched.lock().unwrap(),
            vec![("agent-77".to_string(), "agent-child".to_string())]
        );
        // …and its first turn was started via the initial message.
        assert_eq!(
            *api.sent.lock().unwrap(),
            vec![("agent-child".to_string(), "fix the bug".to_string())]
        );
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
    async fn denylist_filters_workspace_api_and_blocks_calls() {
        // Pure-text background agents (commit-message here) deny the unified
        // `workspace_api` tool via `UNIFIED_WORKSPACE_TOOLS`, so the exposed
        // registry drops to empty and a tools/call attempt is refused with
        // the standard "Tool not available" error.
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
        assert!(!names.contains(&"workspace_api".to_string()));

        let resp = srv
            .handle_message(&json!({
                "jsonrpc": "2.0", "id": 4, "method": "tools/call",
                "params": {
                    "name": "workspace_api",
                    "arguments": { "code": "return 1;", "summary": "x" }
                }
            }))
            .await
            .unwrap();
        assert_eq!(resp["error"]["code"], json!(-32602));
        assert!(resp["error"]["message"]
            .as_str()
            .unwrap()
            .contains("Tool not available"));
    }

    #[test]
    fn task_loop_denies_only_subagent_tools() {
        let deny = get_tool_denylist_for_agent_type("task-loop");
        assert_eq!(deny, SUBAGENT_TOOLS.to_vec());
        // Interactive/foreground agents are unrestricted.
        assert!(get_tool_denylist_for_agent_type("interactive").is_empty());
        // Pure text agents deny the unified workspace tool and file writes.
        let cm = get_tool_denylist_for_agent_type("commit-message");
        assert!(cm.contains(&"workspace_api"));
        assert!(cm.contains(&"str-replace-editor"));
    }

    #[test]
    fn report_to_parent_is_an_agent_creation_tool_and_denied_for_background_types() {
        // It lives in the agent-orchestration group alongside delegate/send.
        assert!(AGENT_CREATION_TOOLS.contains(&"report_to_parent"));
        // Pure-text background agents (full denylist) cannot call it.
        let cm = get_tool_denylist_for_agent_type("commit-message");
        assert!(cm.contains(&"report_to_parent"));
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

    #[test]
    fn opencode_permission_survives_mcp_merge() {
        use intent_providers::{build_provider_env, find_provider};
        use std::collections::BTreeMap;

        // Prepare a normalized MCP server list and its opencode `mcp` block.
        let mut servers = BTreeMap::new();
        servers.insert(
            "ws".to_string(),
            NormalizedMcpServer::Stdio {
                command: "node".into(),
                args: vec!["server.js".into()],
                env: BTreeMap::new(),
            },
        );
        let mcp_block = to_opencode_mcp_config(&servers);
        let mcp_json = serde_json::to_string(&mcp_block).unwrap();

        // Build opencode's env with the mcp block merged at spawn time — the
        // real daemon path (agent_manager passes the serialized block through
        // SpawnOptions::env_mcp_config into build_provider_env).
        let opencode = find_provider("opencode").unwrap();
        let env = build_provider_env(opencode, Some("claude-sonnet-4"), None, Some(&mcp_json));
        let config_content = env
            .get("OPENCODE_CONFIG_CONTENT")
            .expect("OPENCODE_CONFIG_CONTENT must be set");

        let config: Value = serde_json::from_str(config_content)
            .expect("OPENCODE_CONFIG_CONTENT must be valid JSON");

        // Assert: permission.task=deny must still be present after the merge
        assert_eq!(
            config["permission"]["task"],
            json!("deny"),
            "permission.task=deny must survive workspace-MCP merge"
        );
        assert_eq!(
            config["model"],
            json!("claude-sonnet-4"),
            "model must survive workspace-MCP merge"
        );
        // And the mcp block should be populated
        assert!(
            config["mcp"]["ws"].is_object(),
            "mcp.ws server config should be present"
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

    /// The typed `session/new` `mcpServers` list serializes to the same wire
    /// shape as [`to_acp_mcp_servers`]: stdio entries untagged (no `type`
    /// field) with `{ name, value }` env pairs, remotes tagged `http`/`sse`
    /// with `{ name, value }` header pairs.
    #[test]
    fn typed_session_mcp_servers_match_acp_wire_shape() {
        let normalized = normalize_mcp_servers(&stdio_servers());
        let typed = to_acp_session_mcp_servers(&normalized);
        assert_eq!(typed.len(), 2);

        let wire: Vec<Value> = typed
            .iter()
            .map(|s| serde_json::to_value(s).unwrap())
            .collect();
        let ws = wire.iter().find(|s| s["name"] == json!("ws")).unwrap();
        assert!(
            ws.get("type").is_none(),
            "stdio entries serialize untagged (no `type` field): {ws}"
        );
        assert_eq!(ws["command"], json!("node"));
        assert_eq!(ws["args"], json!(["server.js"]));
        assert_eq!(ws["env"], json!([{ "name": "A", "value": "1" }]));

        let remote = wire.iter().find(|s| s["name"] == json!("remote")).unwrap();
        assert_eq!(remote["type"], json!("sse"));
        assert_eq!(remote["url"], json!("https://h"));
        assert_eq!(remote["headers"], json!([{ "name": "X", "value": "y" }]));

        // Pin the parity invariant directly: the two converters are parallel
        // implementations of the same wire shape, so serialized output must be
        // byte-equivalent (Value equality is key-order-insensitive).
        assert_eq!(
            wire,
            to_acp_mcp_servers(&normalized),
            "typed converter must not drift from to_acp_mcp_servers"
        );
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

    /// Spaced bridge path (monorepo#1049): the command becomes the basename
    /// and the PATH override prepends the parent dir to the inherited PATH so
    /// the server-env-wins baseline merge never loses the inherited PATH.
    #[test]
    fn spaced_bridge_path_uses_basename_and_prepends_parent_to_path() {
        let inherited = std::env::join_paths([Path::new("/usr/bin"), Path::new("/bin")]).unwrap();
        let (command, path) = normalize_spaced_bridge_command(
            Path::new("/opt/App Support/bin/intentd"),
            Some(inherited.as_os_str()),
        );
        assert_eq!(command, "intentd");
        let expected = std::env::join_paths([
            Path::new("/opt/App Support/bin"),
            Path::new("/usr/bin"),
            Path::new("/bin"),
        ])
        .unwrap();
        assert_eq!(path.as_deref(), Some(&*expected.to_string_lossy()));
    }

    #[test]
    fn spaced_bridge_path_without_inherited_path_is_parent_only() {
        let (command, path) =
            normalize_spaced_bridge_command(Path::new("/opt/App Support/bin/intentd"), None);
        assert_eq!(command, "intentd");
        assert_eq!(path.as_deref(), Some("/opt/App Support/bin"));
    }

    /// Empty inherited-PATH segments (e.g. an empty `PATH` string) must be
    /// dropped, not joined as trailing separators that would implicitly add
    /// the current directory to lookup on Unix.
    #[test]
    fn spaced_bridge_path_drops_empty_inherited_path_segments() {
        let (command, path) = normalize_spaced_bridge_command(
            Path::new("/opt/App Support/bin/intentd"),
            Some(OsStr::new("")),
        );
        assert_eq!(command, "intentd");
        assert_eq!(path.as_deref(), Some("/opt/App Support/bin"));
    }

    /// Whitespace-free bridge path stays verbatim with no PATH override.
    #[test]
    fn unspaced_bridge_path_is_left_verbatim() {
        let inherited = std::env::join_paths([Path::new("/usr/bin"), Path::new("/bin")]).unwrap();
        let (command, path) = normalize_spaced_bridge_command(
            Path::new("/usr/local/bin/intentd"),
            Some(inherited.as_os_str()),
        );
        assert_eq!(command, "/usr/local/bin/intentd");
        assert_eq!(path, None);
    }

    /// A relative spaced path can't be normalized: the prepended parent dir
    /// would resolve against the launcher child's cwd, not the daemon's.
    #[test]
    fn relative_spaced_bridge_path_falls_back_to_verbatim_command() {
        let (command, path) = normalize_spaced_bridge_command(
            Path::new("my dir/intentd"),
            Some(OsStr::new("/usr/bin")),
        );
        assert_eq!(command, "my dir/intentd");
        assert_eq!(path, None);
    }

    /// A spaced basename can't be fixed by PATH lookup — keep the absolute
    /// path verbatim rather than emitting a still-broken command.
    #[test]
    fn spaced_basename_falls_back_to_verbatim_command() {
        let (command, path) = normalize_spaced_bridge_command(
            Path::new("/usr/local/bin/intent daemon"),
            Some(OsStr::new("/usr/bin")),
        );
        assert_eq!(command, "/usr/local/bin/intent daemon");
        assert_eq!(path, None);
    }
}

/// Client-served handler tests: fs sandbox + events, permission resolve/timeout,
/// and the terminal stub (§6.7 / PROTOCOL §8 `DoD`).
mod client_served_tests {
    use super::*;

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

    /// A unique, freshly created temp directory for sandbox tests; removed
    /// when the returned guard drops.
    fn temp_dir() -> tempfile::TempDir {
        super::test_temp_dir("intent-acp-test-")
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
        let svc = FileService::new(root.path());
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
        assert!(
            ok.starts_with(root.path()),
            "in-scope path resolves inside root"
        );
        let in_scope = svc
            .resolve(std::path::Path::new("a/b/../c.txt"))
            .expect("in-bounds traversal resolves");
        assert!(
            in_scope.starts_with(root.path()),
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
            root.path(),
            PermissionPolicy::Interactive,
            sink.clone(),
            PermissionRegistry::new(),
        );
        let (conn, mut req_rx, mut writer, mut reader) = connect_handler();
        let path = root.path().join("notes/hello.txt");

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

    /// Regression (intent-hq/monorepo#1144): `fs/write_text_file` must fully
    /// await the `file:changed` sink publish BEFORE the write response is
    /// sent, otherwise the agent can observe the write as done — and end its
    /// turn — before the attribution pipeline (which the service-layer sink
    /// awaits inside `publish`) records the change.
    ///
    /// Deterministic, no sleeps: a gated sink suspends `publish` mid-flight;
    /// cooperative yields on the current-thread runtime give the connection's
    /// background writer task every chance to flush anything already
    /// enqueued, so a single poll of the reader proves whether a response
    /// frame went out while the publish was still pending. Under the old
    /// respond-then-emit order the frame is enqueued before `publish` starts,
    /// the yields flush it, and the poll finds it — failing the test.
    #[tokio::test]
    async fn write_response_waits_for_file_changed_publish() {
        use std::future::Future;

        struct GatedSink {
            entered: tokio::sync::Notify,
            release: tokio::sync::Notify,
            events: Mutex<Vec<SinkEvent>>,
        }
        impl EventSink for GatedSink {
            fn publish(&self, event: SinkEvent) -> BoxFuture<'_, ()> {
                Box::pin(async move {
                    self.entered.notify_one();
                    self.release.notified().await;
                    self.events.lock().unwrap().push(event);
                })
            }
        }

        let root = temp_dir();
        let sink = Arc::new(GatedSink {
            entered: tokio::sync::Notify::new(),
            release: tokio::sync::Notify::new(),
            events: Mutex::new(Vec::new()),
        });
        let handler = ClientRequestHandler::new(
            WorkspaceId::from_string("ws-1"),
            AgentId::from_string("agent-1"),
            "auggie",
            FileService::new(root.path()),
            Arc::new(PermissionRegistry::new()),
            PermissionPolicy::Interactive,
            sink.clone(),
        );
        let (conn, mut req_rx, mut writer, mut reader) = connect_handler();
        let path = root.path().join("ordered.txt");

        let req = send(
            &mut writer,
            &mut req_rx,
            7,
            "fs/write_text_file",
            json!({ "sessionId": "acp-1", "path": path, "content": "attributed" }),
        )
        .await;
        // Return `conn` so it outlives the serve future (dropping it closes
        // the wire before the response flushes).
        let serve = tokio::spawn(async move {
            handler.serve(&conn, req).await.unwrap();
            conn
        });

        // Wait until the handler is inside the (suspended) sink publish.
        sink.entered.notified().await;
        // Cooperative yields: let the connection's writer task flush anything
        // that was enqueued before the publish started.
        for _ in 0..8 {
            tokio::task::yield_now().await;
        }

        let mut line = String::new();
        {
            let read = reader.read_line(&mut line);
            tokio::pin!(read);
            let early =
                std::future::poll_fn(|cx| std::task::Poll::Ready(read.as_mut().poll(cx))).await;
            assert!(
                early.is_pending(),
                "fs/write_text_file response was sent before the file:changed \
                 publish completed (intent-hq/monorepo#1144): {line:?}"
            );
        }

        // Release the publish; only now may the response go out.
        sink.release.notify_one();
        let _conn = serve.await.unwrap();
        let resp = read_frame(&mut reader).await;
        assert_eq!(resp["id"], json!(7));
        assert!(resp.get("result").is_some(), "write returns a result");

        let events = sink.events.lock().unwrap();
        assert_eq!(events.len(), 1, "exactly one sink publish");
        assert_eq!(events[0].event_type, "file:changed");
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
            root.path(),
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
            root.path(),
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
            root.path(),
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
            root.path(),
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
    use std::time::Duration;

    #[test]
    fn json_rpc_error_display_format() {
        let e = JsonRpcError {
            code: -32000,
            message: "boom".to_string(),
            data: None,
        };
        assert_eq!(e.to_string(), "JSON-RPC error -32000: boom");
        // Cloned equality and Debug both exercise their derives.
        let cloned = e.clone();
        assert_eq!(cloned, e);
        assert!(format!("{e:?}").contains("boom"));
    }

    #[test]
    fn json_rpc_error_display_appends_string_data_without_quotes() {
        let e = JsonRpcError {
            code: -32603,
            message: "Internal error".to_string(),
            data: Some(json!("failed to send response, receiver dropped")),
        };
        assert_eq!(
            e.to_string(),
            "JSON-RPC error -32603: Internal error: failed to send response, receiver dropped"
        );
    }

    #[test]
    fn json_rpc_error_display_appends_object_data_as_compact_json() {
        let e = JsonRpcError {
            code: -32000,
            message: "boom".to_string(),
            data: Some(json!({"k": 1})),
        };
        assert_eq!(e.to_string(), r#"JSON-RPC error -32000: boom: {"k":1}"#);
    }

    #[test]
    fn json_rpc_error_display_keeps_data_at_cap_untruncated() {
        let at_cap = "x".repeat(crate::error::MAX_RENDERED_DATA_BYTES);
        let e = JsonRpcError {
            code: -32603,
            message: "Internal error".to_string(),
            data: Some(json!(at_cap.clone())),
        };
        let rendered = e.to_string();
        assert_eq!(
            rendered,
            format!("JSON-RPC error -32603: Internal error: {at_cap}")
        );
        assert!(!rendered.contains("[truncated]"));
    }

    #[test]
    fn json_rpc_error_display_truncates_oversized_data_with_marker() {
        // monorepo#519: `data` is provider-controlled and unbounded, and the
        // rendered string flows into stop_reason persistence, agent:failed
        // events, and logs — cap it, keeping the leading detail intact.
        let cap = crate::error::MAX_RENDERED_DATA_BYTES;
        let big = "x".repeat(cap + 4096);
        let e = JsonRpcError {
            code: -32603,
            message: "Internal error".to_string(),
            data: Some(json!(big)),
        };
        let rendered = e.to_string();
        let data_portion = rendered
            .strip_prefix("JSON-RPC error -32603: Internal error: ")
            .expect("code/message prefix intact");
        assert!(data_portion.starts_with(&"x".repeat(cap)));
        assert!(data_portion.ends_with("… [truncated]"));
        assert_eq!(data_portion.len(), cap + "… [truncated]".len());
        // Oversized object data is bounded too (compact-JSON rendering).
        let e = JsonRpcError {
            code: -32000,
            message: "boom".to_string(),
            data: Some(json!({ "detail": "y".repeat(cap + 4096) })),
        };
        assert!(e.to_string().ends_with("… [truncated]"));
    }

    #[test]
    fn json_rpc_error_display_truncation_respects_char_boundaries() {
        // A multi-byte char straddling the byte cap must not split (no panic,
        // valid UTF-8): truncation backs off to the previous char boundary.
        // '€' is 3 bytes, so a cap that is not a multiple of 3 lands mid-char
        // and the back-off loop genuinely runs.
        let cap = crate::error::MAX_RENDERED_DATA_BYTES;
        let char_len = '€'.len_utf8();
        assert_ne!(cap % char_len, 0, "cap must land mid-char for this test");
        let big = "€".repeat(cap); // 3·cap bytes, well past the cap
        let e = JsonRpcError {
            code: -32603,
            message: "Internal error".to_string(),
            data: Some(json!(big)),
        };
        let rendered = e.to_string();
        assert!(rendered.ends_with("… [truncated]"));
        assert!(rendered.chars().all(|c| c != char::REPLACEMENT_CHARACTER));
        let data_portion = rendered
            .strip_prefix("JSON-RPC error -32603: Internal error: ")
            .expect("code/message prefix intact")
            .strip_suffix("… [truncated]")
            .expect("truncation marker suffix");
        // Backed off from the cap to the previous char boundary.
        assert_eq!(data_portion.len(), cap - cap % char_len);
        assert!(data_portion.chars().all(|c| c == '€'));
    }

    #[test]
    fn json_rpc_error_display_keeps_codex_backend_400_detail_untruncated() {
        // monorepo#479 regression guard: the codex-acp -32603 whose `data`
        // object nests the ChatGPT backend 400 (as a JSON string) must still
        // render in full — that detail is the actionable failure cause.
        let backend_400 = r#"{"type":"error","status":400,"error":{"type":"invalid_request_error","message":"The 'gpt-5.6-sol' model requires a newer version of Codex. Please upgrade to the latest app or CLI and try again."}}"#;
        let e = JsonRpcError {
            code: -32603,
            message: "Internal error".to_string(),
            data: Some(json!({ "message": backend_400, "codex_error_info": "other" })),
        };
        let rendered = e.to_string();
        assert!(rendered.contains("requires a newer version of Codex"));
        assert!(rendered.contains("Please upgrade to the latest app or CLI and try again."));
        assert!(!rendered.contains("[truncated]"));
    }

    #[test]
    fn json_rpc_error_display_omits_null_and_empty_string_data() {
        for data in [Some(json!(null)), Some(json!(""))] {
            let e = JsonRpcError {
                code: -32000,
                message: "boom".to_string(),
                data,
            };
            assert_eq!(e.to_string(), "JSON-RPC error -32000: boom");
        }
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
            (
                AcpError::PromptIdleTimeout(Duration::from_secs(1800)),
                "session/prompt idle timeout (1800s of silence)",
            ),
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
    fn acp_error_prompt_idle_timeout_display_is_prefix_anchored() {
        // The service layer classifies the idle timeout AFTER the error is
        // flattened to a string (`session/prompt failed: {e}`), matching
        // prefix-anchored on PROMPT_IDLE_TIMEOUT_PREFIX — pin the Display
        // rendering to the exported const so the contract cannot drift.
        let err = AcpError::PromptIdleTimeout(Duration::from_secs(1800));
        assert!(err
            .to_string()
            .starts_with(crate::PROMPT_IDLE_TIMEOUT_PREFIX));
        // The plain request timeout must NOT carry the marker even when the
        // timed-out method is `session/prompt` (the 24h fallback timeout).
        let err = AcpError::Timeout("session/prompt".into());
        assert!(!err
            .to_string()
            .starts_with(crate::PROMPT_IDLE_TIMEOUT_PREFIX));
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

/// Schema synthesis + tool-registry shape after the WSAPI-8 cutover: the
/// daemon exposes exactly one MCP tool (`workspace_api`), so the assertions
/// here reduce to "the registry is a singleton with the expected schema"
/// and denylist filtering is tested against that single entry.
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
    fn registry_is_workspace_api_singleton_with_required_params() {
        // Post-cutover the daemon exposes exactly one MCP tool. Both `code`
        // and `summary` are declared required by the schema; the bare tool
        // name carries no `_workspace-mcp` suffix (the ACP provider appends
        // that server suffix on its side).
        let tools = unrestricted_tools();
        assert_eq!(tools.len(), 1, "registry should be a single-tool surface");
        let def = tools[0];
        assert_eq!(def.name, "workspace_api");
        assert!(
            !def.name.ends_with("_workspace-mcp"),
            "tool name must be bare (provider appends the server suffix)"
        );
        let schema = def.schema();
        assert_eq!(schema["type"], serde_json::json!("object"));
        let required = schema["required"].as_array().expect("required array");
        let req_names: Vec<&str> = required.iter().filter_map(|v| v.as_str()).collect();
        assert!(req_names.contains(&"code"));
        assert!(req_names.contains(&"summary"));
        assert_eq!(
            schema["properties"]["code"]["type"],
            serde_json::json!("string")
        );
        assert_eq!(
            schema["properties"]["summary"]["type"],
            serde_json::json!("string")
        );
    }

    #[test]
    fn pure_text_agent_types_deny_the_unified_workspace_tool() {
        // Pure-text background agents (commit-message, pr-description,
        // code-review, code-walkthrough) deny the unified `workspace_api`
        // surface, so their available set is empty. The other background
        // types (task-loop / ralph-loop / chat) only deny auggie subagent
        // tools that are intentionally absent from this registry.
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
            let names: Vec<&str> = srv.available_tools().iter().map(|t| t.name).collect();
            assert!(
                names.is_empty(),
                "{ty} must expose no tools (denies workspace_api), got {names:?}"
            );
        }
    }

    #[test]
    fn working_agent_types_keep_the_workspace_api_tool() {
        // task-loop / ralph-loop / chat only deny auggie subagent tools that
        // are not in the workspace-MCP registry, so `workspace_api` stays
        // available.
        for ty in ["task-loop", "ralph-loop", "chat"] {
            let srv = WorkspaceMcpServer::for_agent_type(
                super::mcp_tests::mock_api(),
                intent_core::WorkspaceId::from_string("ws-1"),
                ty,
            );
            let names: Vec<&str> = srv.available_tools().iter().map(|t| t.name).collect();
            assert_eq!(
                names.len(),
                1,
                "{ty} must expose exactly one tool (singleton registry), got {names:?}"
            );
            assert_eq!(
                names[0], "workspace_api",
                "{ty} must keep workspace_api as the sole registered tool, got {names:?}"
            );
        }
        // Sanity: background_agent_types() enumerates exactly these seven types
        // (4 pure-text + 3 working), keeping the two branches of this pair test
        // exhaustive over the predicate.
        let bg: Vec<&str> = background_agent_types().to_vec();
        assert_eq!(bg.len(), 7);
    }
}

// Discrete-tool dispatch coverage removed in WSAPI-8: `dispatch()` and its
// per-tool arms no longer exist. Coverage for tool-call error framing now
// lives in `workspace_api_tool_tests`; parameter parsing is covered by the
// per-namespace binding tests (`wsapi3_bindings_tests` etc.).
#[cfg(all(test, any()))]
mod _dead_dispatch_unit_tests {
    use serde_json::json;
    use std::sync::Arc;

    use intent_core::{TaskGetMyTaskResult, TaskStatus, WorkspaceId};

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
                "params": { "name": "get_note" }
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
            ("get_note", json!({}), "noteId"),
            ("list_note_tasks", json!({}), "noteId"),
            ("get_my_task", json!({}), "taskNoteId"),
            ("create_note", json!({}), "title"),
            ("add_to_note", json!({ "noteId": "n" }), "content"),
            ("set_note_content", json!({ "noteId": "n" }), "content"),
            ("edit_note", json!({ "noteId": "n", "old": "x" }), "new"),
            (
                "edit_note_lines",
                json!({ "noteId": "n", "start": 1, "end": 2 }),
                "content",
            ),
            (
                "edit_note_lines",
                json!({ "noteId": "n", "end": 2, "content": "x" }),
                "start",
            ),
            ("delete_note", json!({}), "noteId"),
            (
                "update_task_status",
                json!({ "noteId": "n", "taskText": "t" }),
                "status",
            ),
            (
                "update_note_task_status",
                json!({ "noteId": "n" }),
                "status",
            ),
            ("update_task", json!({ "noteId": "n" }), "line"),
            ("mark_as_task", json!({ "noteId": "n" }), "status"),
            ("convert_task_blocks", json!({}), "noteId"),
            ("create_prerequisite", json!({ "noteId": "n" }), "title"),
            ("assign_agent", json!({ "noteId": "n" }), "agentId"),
            (
                "add_note_comment",
                json!({ "noteId": "n", "searchContext": "s", "commentTarget": "t" }),
                "comment",
            ),
            (
                "respond_to_comment_thread",
                json!({ "noteId": "n" }),
                "comment",
            ),
            ("git_commit", json!({}), "agent"),
            ("report_to_parent", json!({}), "report"),
            (
                "send_message_to_agent",
                json!({ "message": "hi" }),
                "agentId",
            ),
            (
                "send_message_to_agent",
                json!({ "agentId": "agent-1" }),
                "message",
            ),
            (
                "send_message_to_task_agent",
                json!({ "message": "hi" }),
                "taskNoteId",
            ),
            (
                "wake_or_create_task_agent",
                json!({ "contextMessage": "ctx" }),
                "taskNoteId",
            ),
            (
                "wake_or_create_task_agent",
                json!({ "taskNoteId": "n" }),
                "contextMessage",
            ),
            ("get_agent_status", json!({}), "agentId"),
            ("read_agent_conversation", json!({}), "agentId"),
            ("get_agent_summary", json!({}), "agentId"),
            ("subscribe_to_events", json!({}), "eventTypes"),
            ("unsubscribe_from_events", json!({}), "subscriptionId"),
            ("set_workspace_title", json!({}), "title"),
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
            ("get_note", json!({ "noteId": "n" })),
            ("list_note_tasks", json!({ "noteId": "n" })),
            // `get_my_task` is absent here: `MockApi` overrides it (for the
            // dedicated happy-path / not-found tests below), so its arm is
            // proven by `get_my_task_arm_reaches_workspace_api`.
            (
                "set_note_content",
                json!({ "noteId": "n", "content": "c", "confirmReplacement": true, "expectedVersion": 3 }),
            ),
            (
                "edit_note",
                json!({ "noteId": "n", "old": "a", "new": "b" }),
            ),
            (
                "edit_note_lines",
                json!({ "noteId": "n", "start": 1, "end": 2, "content": "x" }),
            ),
            (
                "update_note_metadata",
                json!({ "noteId": "n", "title": "T", "tags": ["a", "b"] }),
            ),
            ("delete_note", json!({ "noteId": "n" })),
            (
                "update_task_status",
                json!({ "noteId": "n", "taskText": "t", "status": "done" }),
            ),
            (
                "update_note_task_status",
                json!({ "noteId": "n", "status": "complete", "expectedVersion": 1 }),
            ),
            (
                "update_task",
                json!({ "noteId": "n", "line": 2, "text": "T", "status": "todo", "expected": "old" }),
            ),
            (
                "mark_as_task",
                json!({ "noteId": "n", "status": "in_progress", "acceptanceCriteria": ["a"], "effort": "small" }),
            ),
            ("convert_task_blocks", json!({ "noteId": "n" })),
            (
                "create_prerequisite",
                json!({ "noteId": "n", "title": "t", "content": "c", "status": "todo" }),
            ),
            // `assign_agent` is absent here: `MockApi` overrides
            // `assign_agent` (for the create_agent tests), so its arm is proven
            // by `assign_agent_arm_reaches_workspace_api` below instead.
            (
                "add_note_comment",
                json!({ "noteId": "n", "searchContext": "s", "commentTarget": "t", "comment": "c", "type": "comment", "author": "me" }),
            ),
            (
                "respond_to_comment_thread",
                json!({ "noteId": "n", "threadId": "th", "comment": "c", "type": "comment", "author": "me", "suggestionOriginal": "o", "suggestionProposed": "p" }),
            ),
            (
                "delegate_task",
                json!({ "taskNoteId": "tn", "noteId": "n", "taskText": "t", "agentInstructions": "i",
                        "specialist": "implementor", "model": "opus", "behaviorPrompt": "b",
                        "waitMode": "after_all", "skipAutoCommit": true,
                        "scope": ["crates/intent-core", "crates/intent-services"] }),
            ),
            ("report_to_parent", json!({ "report": "all done" })),
            // `send_message_to_agent` is absent here: `MockApi`
            // overrides `agent_send_message` (for the create_agent tests), so
            // its arm is proven by `send_message_arm_reaches_workspace_api`.
            // `send_message_to_task_agent` is absent here: `MockApi` overrides
            // `agent_send_to_task` (for the SUB-1 sender-watch tests), so its
            // arm is proven by `send_to_task_arm_subscribes_caller_to_assignee`.
            (
                "wake_or_create_task_agent",
                json!({ "taskNoteId": "tn", "contextMessage": "ctx", "model": "opus" }),
            ),
            ("list_agents", json!({ "includeCompleted": true })),
            ("get_agent_status", json!({ "agentId": "agent-1" })),
            (
                "read_agent_conversation",
                json!({ "agentId": "agent-1", "lastN": 20 }),
            ),
            ("get_agent_summary", json!({ "agentId": "agent-1" })),
            (
                "get_agent_diagnostics",
                json!({ "agentId": "agent-1", "taskNoteId": "n", "staleRespondingAfterMs": 60000 }),
            ),
            (
                "subscribe_to_events",
                json!({ "eventTypes": ["agent:idle"], "excludeSelf": true, "batchWindow": 250 }),
            ),
            (
                "unsubscribe_from_events",
                json!({ "subscriptionId": "sub-1" }),
            ),
            ("get_workspace_details", json!({})),
            (
                "set_workspace_status_message",
                json!({ "statusMessage": "current progress" }),
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
    async fn assign_agent_arm_reaches_workspace_api() {
        let (srv, api) = server();
        let resp = call(
            &srv,
            "assign_agent",
            json!({ "noteId": "n", "agentId": "agent-1" }),
        )
        .await;
        assert_eq!(resp["result"]["isError"], json!(false));
        assert_eq!(
            *api.assigned.lock().unwrap(),
            vec![("n".to_string(), "agent-1".to_string())]
        );
    }

    #[tokio::test]
    async fn send_message_arm_reaches_workspace_api() {
        let (srv, api) = server();
        let resp = call(
            &srv,
            "send_message_to_agent",
            json!({ "agentId": "agent-1", "message": "hi", "priority": "normal" }),
        )
        .await;
        assert_eq!(resp["result"]["isError"], json!(false));
        assert_eq!(
            *api.sent.lock().unwrap(),
            vec![("agent-1".to_string(), "hi".to_string())]
        );
    }

    /// SUB-1: a caller-fronted `send_message_to_agent` registers the sender
    /// watch via `agent_watch_completion_for_sender` and surfaces the
    /// subscription id in the tool payload.
    #[tokio::test]
    async fn send_message_arm_subscribes_caller_to_target_completion() {
        let api = mock_api();
        let srv = WorkspaceMcpServer::new(api.clone(), WorkspaceId::from_string("ws-1"))
            .with_caller_agent_id(Some(intent_core::AgentId::from_string("agent-77")));
        let resp = call(
            &srv,
            "send_message_to_agent",
            json!({ "agentId": "agent-1", "message": "hi" }),
        )
        .await;
        assert_eq!(resp["result"]["isError"], json!(false));
        let text = resp["result"]["content"][0]["text"].as_str().unwrap();
        let payload: serde_json::Value = serde_json::from_str(text).unwrap();
        assert_eq!(payload["subscriptionId"], json!("sub-sender-1"));
        assert_eq!(
            payload["message"],
            json!("You will be notified when the agent responds.")
        );
        assert_eq!(
            *api.sender_watched.lock().unwrap(),
            vec![("agent-77".to_string(), "agent-1".to_string())]
        );
    }

    /// SUB-1: the caller-less front door (FE/RPC) registers no sender watch
    /// and the payload stays in the pre-SUB-1 shape (no `subscriptionId` /
    /// `message` keys).
    #[tokio::test]
    async fn send_message_arm_skips_sender_watch_without_caller() {
        let (srv, api) = server();
        let resp = call(
            &srv,
            "send_message_to_agent",
            json!({ "agentId": "agent-1", "message": "hi" }),
        )
        .await;
        assert_eq!(resp["result"]["isError"], json!(false));
        let text = resp["result"]["content"][0]["text"].as_str().unwrap();
        let payload: serde_json::Value = serde_json::from_str(text).unwrap();
        assert!(payload.get("subscriptionId").is_none());
        assert!(payload.get("message").is_none());
        assert!(api.sender_watched.lock().unwrap().is_empty());
    }

    /// SUB-1: `send_message_to_task_agent` resolves the assignee from the op
    /// result and registers the same sender watch against it.
    #[tokio::test]
    async fn send_to_task_arm_subscribes_caller_to_assignee() {
        let api = mock_api();
        let srv = WorkspaceMcpServer::new(api.clone(), WorkspaceId::from_string("ws-1"))
            .with_caller_agent_id(Some(intent_core::AgentId::from_string("agent-77")));
        let resp = call(
            &srv,
            "send_message_to_task_agent",
            json!({ "taskNoteId": "tn-1", "message": "hi" }),
        )
        .await;
        assert_eq!(resp["result"]["isError"], json!(false));
        let text = resp["result"]["content"][0]["text"].as_str().unwrap();
        let payload: serde_json::Value = serde_json::from_str(text).unwrap();
        assert_eq!(payload["subscriptionId"], json!("sub-sender-1"));
        assert_eq!(
            payload["message"],
            json!("You will be notified when the agent responds.")
        );
        assert_eq!(
            *api.sender_watched.lock().unwrap(),
            vec![("agent-77".to_string(), "agent-assignee".to_string())]
        );
    }

    #[tokio::test]
    async fn get_my_task_arm_reaches_workspace_api() {
        let (srv, api) = server();
        let resp = call(&srv, "get_my_task", json!({ "taskNoteId": "tn-1" })).await;
        assert_eq!(resp["result"]["isError"], json!(false));
        let text = resp["result"]["content"][0]["text"].as_str().unwrap();
        let result: TaskGetMyTaskResult = serde_json::from_str(text).unwrap();
        assert_eq!(result.note_id.as_str(), "tn-1");
        assert_eq!(result.status, TaskStatus::InProgress);
        assert_eq!(result.title, "My Task");
        assert_eq!(
            *api.get_my_task_calls.lock().unwrap(),
            vec!["tn-1".to_string()]
        );
    }

    #[tokio::test]
    async fn get_my_task_arm_surfaces_not_found_error() {
        let (srv, api) = server();
        let resp = call(&srv, "get_my_task", json!({ "taskNoteId": "task-missing" })).await;
        // Error::Internal → -32603 (matches services impl wrapping).
        assert_eq!(resp["error"]["code"], json!(-32603));
        assert!(resp["error"]["message"]
            .as_str()
            .unwrap()
            .contains("Task note not found"));
        assert_eq!(
            *api.get_my_task_calls.lock().unwrap(),
            vec!["task-missing".to_string()]
        );
    }

    #[tokio::test]
    async fn create_note_mints_idempotency_key_when_absent() {
        // Daemon-internal `note.create`: agents do not pass an idempotencyKey,
        // so the dispatch arm mints one — the services soft-launch warn
        // ("idempotencyKey missing on idempotent method") must never fire for
        // MCP tool calls.
        let (srv, api) = server();
        let resp = call(&srv, "create_note", json!({ "title": "t" })).await;
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
            "create_note",
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
                "params": { "name": "totally_made_up", "arguments": {} }
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
            .with_denylist(["get_note"]);
        assert!(srv.is_denied("get_note"));
        assert!(!srv.is_denied("list_notes"));
        let resp = srv
            .handle_message(&json!({
                "jsonrpc": "2.0", "id": 6, "method": "tools/call",
                "params": { "name": "get_note", "arguments": { "noteId": "n" } }
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
            .with_denylist(["delete_note"]);
        let resp = srv
            .handle_message(&json!({ "jsonrpc": "2.0", "id": 1, "method": "tools/list" }))
            .await
            .unwrap();
        let tools = resp["result"]["tools"].as_array().unwrap();
        assert!(!tools.iter().any(|t| t["name"] == json!("delete_note")));
        assert!(tools.iter().any(|t| t["name"] == json!("list_notes")));
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
        // Sanity: the listener accepts while the bridge handle is alive.
        TcpStream::connect(&addr).await.unwrap();
        let accept_loop = bridge.accept_loop_handle();
        drop(bridge);
        // Drop aborts the accept-loop task; await its completion, which drops
        // (and closes) the TcpListener it owns. Probing the port with fresh
        // connects instead is racy under a parallel test run: once the
        // ephemeral port is freed, a concurrent test binding 127.0.0.1:0 can
        // be handed the same port, making connects succeed against an
        // unrelated listener.
        let deadline = tokio::time::Instant::now() + Duration::from_secs(5);
        while !accept_loop.is_finished() {
            assert!(
                tokio::time::Instant::now() < deadline,
                "accept loop still running 5s after McpBridge drop"
            );
            tokio::time::sleep(Duration::from_millis(10)).await;
        }
    }

    /// Concurrent per-request dispatch (monorepo#871): a long `tools/call`
    /// must never head-of-line block a liveness ping on the same connection.
    /// These tests drive `serve_mcp_tcp` with a gated mock dispatch so
    /// completion timing is fully controlled.
    mod concurrency {
        use std::collections::HashMap;
        use std::future::Future;
        use std::pin::Pin;
        use std::sync::atomic::{AtomicUsize, Ordering};
        use std::sync::{Arc, Mutex};
        use std::time::Duration;

        use serde_json::{json, Value};
        use tokio::io::{AsyncBufReadExt, AsyncWriteExt, BufReader};
        use tokio::net::tcp::{OwnedReadHalf, OwnedWriteHalf};
        use tokio::net::TcpStream;
        use tokio::sync::Notify;
        use tokio::time::timeout;

        use crate::mcp_bridge::{serve_mcp_tcp, BridgeDispatch, McpBridge};

        /// Mock dispatch: `slow` requests park on a per-id gate until
        /// `release(id)`; any other request with an id answers immediately;
        /// notifications (no id) are recorded and yield `None`. A drop-guard
        /// counts `slow` futures cancelled before completion, which is how
        /// teardown-abort is observed.
        #[derive(Default)]
        struct TestDispatch {
            gates: Mutex<HashMap<i64, Arc<Notify>>>,
            started: Mutex<Vec<i64>>,
            notifications: Mutex<Vec<String>>,
            cancelled: Arc<AtomicUsize>,
        }

        impl TestDispatch {
            fn gate(&self, id: i64) -> Arc<Notify> {
                self.gates.lock().unwrap().entry(id).or_default().clone()
            }

            fn release(&self, id: i64) {
                self.gate(id).notify_one();
            }

            async fn wait_started(&self, id: i64) {
                let deadline = tokio::time::Instant::now() + Duration::from_secs(2);
                while !self.started.lock().unwrap().contains(&id) {
                    assert!(
                        tokio::time::Instant::now() < deadline,
                        "request {id} never reached dispatch"
                    );
                    tokio::time::sleep(Duration::from_millis(5)).await;
                }
            }
        }

        struct AbortGuard {
            cancelled: Arc<AtomicUsize>,
            armed: bool,
        }

        impl Drop for AbortGuard {
            fn drop(&mut self) {
                if self.armed {
                    self.cancelled.fetch_add(1, Ordering::SeqCst);
                }
            }
        }

        impl BridgeDispatch for TestDispatch {
            fn dispatch(
                self: Arc<Self>,
                message: Value,
            ) -> Pin<Box<dyn Future<Output = Option<Value>> + Send>> {
                Box::pin(async move {
                    let method = message["method"].as_str().unwrap_or_default().to_string();
                    let Some(id) = message.get("id").and_then(Value::as_i64) else {
                        self.notifications.lock().unwrap().push(method);
                        return None;
                    };
                    self.started.lock().unwrap().push(id);
                    if method == "slow" {
                        let gate = self.gate(id);
                        let mut guard = AbortGuard {
                            cancelled: self.cancelled.clone(),
                            armed: true,
                        };
                        gate.notified().await;
                        guard.armed = false;
                    }
                    let payload_len =
                        usize::try_from(message["params"]["payload_len"].as_u64().unwrap_or(0))
                            .expect("value fits in usize");
                    Some(json!({
                        "jsonrpc": "2.0", "id": id,
                        "result": { "ok": true, "payload": "x".repeat(payload_len) }
                    }))
                })
            }
        }

        async fn start() -> (
            Arc<TestDispatch>,
            McpBridge,
            OwnedWriteHalf,
            BufReader<OwnedReadHalf>,
        ) {
            let dispatch = Arc::new(TestDispatch::default());
            let bridge = serve_mcp_tcp(dispatch.clone()).await.unwrap();
            let stream = TcpStream::connect(bridge.connect_addr()).await.unwrap();
            let (read, write) = stream.into_split();
            (dispatch, bridge, write, BufReader::new(read))
        }

        async fn send(write: &mut OwnedWriteHalf, line: &str) {
            write.write_all(line.as_bytes()).await.unwrap();
            write.write_all(b"\n").await.unwrap();
            write.flush().await.unwrap();
        }

        async fn read_response(reader: &mut BufReader<OwnedReadHalf>) -> Value {
            let mut buf = String::new();
            timeout(Duration::from_secs(2), reader.read_line(&mut buf))
                .await
                .expect("read_line timed out")
                .expect("read_line ok");
            serde_json::from_str(buf.trim()).expect("response line must be whole JSON")
        }

        #[tokio::test]
        async fn slow_request_does_not_delay_subsequent_request() {
            let (dispatch, _bridge, mut write, mut reader) = start().await;
            send(&mut write, r#"{"jsonrpc":"2.0","id":1,"method":"slow"}"#).await;
            dispatch.wait_started(1).await;
            send(&mut write, r#"{"jsonrpc":"2.0","id":2,"method":"ping"}"#).await;
            let first = read_response(&mut reader).await;
            assert_eq!(
                first["id"],
                json!(2),
                "ping must not wait behind the stalled call"
            );
            dispatch.release(1);
            let second = read_response(&mut reader).await;
            assert_eq!(second["id"], json!(1));
        }

        #[tokio::test]
        async fn responses_complete_out_of_order() {
            let (dispatch, _bridge, mut write, mut reader) = start().await;
            for id in 1..=3 {
                send(
                    &mut write,
                    &format!(r#"{{"jsonrpc":"2.0","id":{id},"method":"slow"}}"#),
                )
                .await;
                dispatch.wait_started(id).await;
            }
            for id in [3, 1, 2] {
                dispatch.release(id);
                let resp = read_response(&mut reader).await;
                assert_eq!(resp["id"], json!(id));
            }
        }

        #[tokio::test]
        async fn notification_yields_no_response_while_request_in_flight() {
            let (dispatch, _bridge, mut write, mut reader) = start().await;
            send(&mut write, r#"{"jsonrpc":"2.0","id":1,"method":"slow"}"#).await;
            dispatch.wait_started(1).await;
            send(
                &mut write,
                r#"{"jsonrpc":"2.0","method":"notifications/initialized"}"#,
            )
            .await;
            send(&mut write, r#"{"jsonrpc":"2.0","id":2,"method":"ping"}"#).await;
            let first = read_response(&mut reader).await;
            assert_eq!(
                first["id"],
                json!(2),
                "notification must not consume a response line"
            );
            dispatch.release(1);
            let second = read_response(&mut reader).await;
            assert_eq!(second["id"], json!(1));
            assert_eq!(
                dispatch.notifications.lock().unwrap().as_slice(),
                ["notifications/initialized"]
            );
        }

        #[tokio::test]
        async fn concurrent_responses_are_whole_uninterleaved_lines() {
            let (dispatch, _bridge, mut write, mut reader) = start().await;
            let n = 8i64;
            for id in 1..=n {
                let payload_len = 64 * 1024 + id;
                send(
                    &mut write,
                    &format!(
                        r#"{{"jsonrpc":"2.0","id":{id},"method":"slow","params":{{"payload_len":{payload_len}}}}}"#
                    ),
                )
                .await;
                dispatch.wait_started(id).await;
            }
            for id in 1..=n {
                dispatch.release(id);
            }
            let mut seen = std::collections::HashSet::new();
            for _ in 0..n {
                let resp = read_response(&mut reader).await;
                let id = resp["id"].as_i64().expect("id must survive intact");
                let expected_len = usize::try_from(64 * 1024 + id).expect("value fits in usize");
                assert_eq!(
                    resp["result"]["payload"].as_str().unwrap().len(),
                    expected_len,
                    "payload for id {id} must be whole"
                );
                assert!(seen.insert(id), "duplicate response for id {id}");
            }
            assert_eq!(seen.len(), usize::try_from(n).expect("value fits in usize"));
        }

        #[tokio::test]
        async fn disconnect_mid_request_aborts_in_flight_task() {
            let (dispatch, _bridge, mut write, reader) = start().await;
            send(&mut write, r#"{"jsonrpc":"2.0","id":1,"method":"slow"}"#).await;
            dispatch.wait_started(1).await;
            drop(write);
            drop(reader);
            let deadline = tokio::time::Instant::now() + Duration::from_secs(2);
            while dispatch.cancelled.load(Ordering::SeqCst) == 0 {
                assert!(
                    tokio::time::Instant::now() < deadline,
                    "in-flight request task not aborted after disconnect"
                );
                tokio::time::sleep(Duration::from_millis(5)).await;
            }
        }
    }

    /// Dispatch watchdog (monorepo#2709): a dispatch wedged forever must
    /// still produce an error response line within the watchdog deadline —
    /// the deadline is enforced from a separate task, so it fires even when
    /// the dispatch future is stuck inside a single synchronous poll. These
    /// tests drive `serve_mcp_tcp_with_timeout` with a shortened deadline
    /// and a mock whose `wedge` method never resolves.
    mod watchdog {
        use std::future::Future;
        use std::pin::Pin;
        use std::sync::atomic::{AtomicUsize, Ordering};
        use std::sync::{Arc, Mutex};
        use std::time::Duration;

        use serde_json::{json, Value};
        use tokio::io::{AsyncBufReadExt, AsyncWriteExt, BufReader};
        use tokio::net::tcp::{OwnedReadHalf, OwnedWriteHalf};
        use tokio::net::TcpStream;
        use tokio::time::timeout;

        use crate::mcp_bridge::{
            effective_dispatch_timeout, serve_mcp_tcp_with_timeout, BridgeDispatch, McpBridge,
            BRIDGE_DISPATCH_TIMEOUT_CODE, BRIDGE_DISPATCH_TIMEOUT_MESSAGE,
            DISPATCH_WATCHDOG_TIMEOUT, MAX_IN_FLIGHT_REQUESTS,
        };

        /// Generous relative to the per-line read budget below so a stalled
        /// CI runner cannot invert response ordering into a flake.
        const TEST_DEADLINE: Duration = Duration::from_millis(500);

        /// Mock dispatch: `wedge` parks forever (request or notification);
        /// any other request answers immediately; non-wedged notifications
        /// are recorded and yield `None`. A drop-guard counts wedged futures
        /// cancelled before completion, which is how the watchdog abort is
        /// observed.
        #[derive(Default)]
        struct WedgeDispatch {
            notifications: Mutex<Vec<String>>,
            cancelled: Arc<AtomicUsize>,
        }

        struct AbortGuard(Arc<AtomicUsize>);

        impl Drop for AbortGuard {
            fn drop(&mut self) {
                self.0.fetch_add(1, Ordering::SeqCst);
            }
        }

        impl BridgeDispatch for WedgeDispatch {
            fn dispatch(
                self: Arc<Self>,
                message: Value,
            ) -> Pin<Box<dyn Future<Output = Option<Value>> + Send>> {
                Box::pin(async move {
                    let method = message["method"].as_str().unwrap_or_default().to_string();
                    if method == "wedge" {
                        let _guard = AbortGuard(self.cancelled.clone());
                        std::future::pending::<()>().await;
                        unreachable!("wedge dispatch never resolves");
                    }
                    let Some(id) = message.get("id") else {
                        self.notifications.lock().unwrap().push(method);
                        return None;
                    };
                    Some(json!({ "jsonrpc": "2.0", "id": id, "result": { "ok": true } }))
                })
            }
        }

        async fn start() -> (
            Arc<WedgeDispatch>,
            McpBridge,
            OwnedWriteHalf,
            BufReader<OwnedReadHalf>,
        ) {
            let dispatch = Arc::new(WedgeDispatch::default());
            let bridge = serve_mcp_tcp_with_timeout(dispatch.clone(), TEST_DEADLINE)
                .await
                .unwrap();
            let stream = TcpStream::connect(bridge.connect_addr()).await.unwrap();
            let (read, write) = stream.into_split();
            (dispatch, bridge, write, BufReader::new(read))
        }

        async fn send(write: &mut OwnedWriteHalf, line: &str) {
            write.write_all(line.as_bytes()).await.unwrap();
            write.write_all(b"\n").await.unwrap();
            write.flush().await.unwrap();
        }

        async fn read_response(reader: &mut BufReader<OwnedReadHalf>) -> Value {
            let mut buf = String::new();
            timeout(Duration::from_secs(2), reader.read_line(&mut buf))
                .await
                .expect("read_line timed out")
                .expect("read_line ok");
            serde_json::from_str(buf.trim()).expect("response line must be whole JSON")
        }

        async fn wait_cancelled(dispatch: &WedgeDispatch, n: usize) {
            let deadline = tokio::time::Instant::now() + Duration::from_secs(2);
            while dispatch.cancelled.load(Ordering::SeqCst) < n {
                assert!(
                    tokio::time::Instant::now() < deadline,
                    "wedged dispatch task not aborted by the watchdog"
                );
                tokio::time::sleep(Duration::from_millis(5)).await;
            }
        }

        #[tokio::test]
        async fn wedged_request_gets_timeout_error_and_concurrent_fast_request_succeeds() {
            let (dispatch, _bridge, mut write, mut reader) = start().await;
            send(&mut write, r#"{"jsonrpc":"2.0","id":1,"method":"wedge"}"#).await;
            send(&mut write, r#"{"jsonrpc":"2.0","id":2,"method":"ping"}"#).await;
            // Collect both responses id-keyed rather than asserting arrival
            // order (CI-scheduler tolerant). The wedged dispatch parks
            // forever, so the fast response arriving at all — within the
            // read budget — already proves it did not queue behind it.
            let mut by_id = std::collections::HashMap::new();
            for _ in 0..2 {
                let response = read_response(&mut reader).await;
                by_id.insert(response["id"].as_i64().unwrap(), response);
            }
            assert_eq!(by_id[&2]["result"]["ok"], json!(true));
            let timed_out = &by_id[&1];
            assert_eq!(
                timed_out["error"]["code"],
                json!(BRIDGE_DISPATCH_TIMEOUT_CODE)
            );
            assert_eq!(
                timed_out["error"]["message"],
                json!(BRIDGE_DISPATCH_TIMEOUT_MESSAGE)
            );
            assert_eq!(timed_out["error"]["data"]["retryable"], json!(false));
            wait_cancelled(&dispatch, 1).await;
            // The connection keeps serving after the watchdog fired.
            send(&mut write, r#"{"jsonrpc":"2.0","id":3,"method":"ping"}"#).await;
            let third = read_response(&mut reader).await;
            assert_eq!(third["id"], json!(3));
        }

        /// `id: null` marks a request (matching `handle_message` and the
        /// stdio proxy's `request_id`), so a wedged null-id request gets a
        /// synthesized error line carrying `id: null`.
        #[tokio::test]
        async fn wedged_null_id_request_gets_timeout_error_with_null_id() {
            let (dispatch, _bridge, mut write, mut reader) = start().await;
            send(
                &mut write,
                r#"{"jsonrpc":"2.0","id":null,"method":"wedge"}"#,
            )
            .await;
            let response = read_response(&mut reader).await;
            assert!(response["id"].is_null());
            assert_eq!(
                response["error"]["code"],
                json!(BRIDGE_DISPATCH_TIMEOUT_CODE)
            );
            wait_cancelled(&dispatch, 1).await;
        }

        /// Saturation: with every in-flight permit held by wedged requests,
        /// the watchdog path must release the permits — the one queued
        /// request behind the full semaphore can only be answered if it does.
        #[tokio::test]
        async fn watchdog_releases_semaphore_permits_at_saturation() {
            let (dispatch, _bridge, mut write, mut reader) = start().await;
            for i in 0..MAX_IN_FLIGHT_REQUESTS {
                send(
                    &mut write,
                    &format!(r#"{{"jsonrpc":"2.0","id":{i},"method":"wedge"}}"#),
                )
                .await;
            }
            send(&mut write, r#"{"jsonrpc":"2.0","id":100,"method":"ping"}"#).await;
            let mut timeout_errors = 0;
            let mut ping_ok = false;
            for _ in 0..=MAX_IN_FLIGHT_REQUESTS {
                let response = read_response(&mut reader).await;
                if response["id"] == json!(100) {
                    assert_eq!(response["result"]["ok"], json!(true));
                    ping_ok = true;
                } else {
                    assert_eq!(
                        response["error"]["code"],
                        json!(BRIDGE_DISPATCH_TIMEOUT_CODE)
                    );
                    timeout_errors += 1;
                }
            }
            assert!(ping_ok, "queued request must run once a permit frees");
            assert_eq!(timeout_errors, MAX_IN_FLIGHT_REQUESTS);
            wait_cancelled(&dispatch, MAX_IN_FLIGHT_REQUESTS).await;
        }

        /// The production deadline keeps clear of a raised eval budget: the
        /// 120s floor holds for the 30s default, and an
        /// `INTENTD_WORKSPACE_API_TIMEOUT_MS` override past half the floor
        /// scales the deadline to twice the budget.
        #[test]
        fn effective_dispatch_timeout_floors_and_scales() {
            assert_eq!(
                effective_dispatch_timeout(Duration::from_secs(30)),
                DISPATCH_WATCHDOG_TIMEOUT
            );
            assert_eq!(
                effective_dispatch_timeout(Duration::from_secs(60)),
                DISPATCH_WATCHDOG_TIMEOUT
            );
            assert_eq!(
                effective_dispatch_timeout(Duration::from_secs(90)),
                Duration::from_secs(180)
            );
        }

        #[tokio::test]
        async fn wedged_notification_is_aborted_without_a_response_line() {
            let (dispatch, _bridge, mut write, mut reader) = start().await;
            send(&mut write, r#"{"jsonrpc":"2.0","method":"wedge"}"#).await;
            // Wait for the watchdog to abort the wedged notification, then
            // prove no error line was synthesized for it: the next response
            // on the wire belongs to the ping sent afterwards.
            wait_cancelled(&dispatch, 1).await;
            send(&mut write, r#"{"jsonrpc":"2.0","id":7,"method":"ping"}"#).await;
            let response = read_response(&mut reader).await;
            assert_eq!(
                response["id"],
                json!(7),
                "a wedged notification must not synthesize a response line"
            );
        }
    }

    /// Stdio-bridge resilience (monorepo#871, monorepo#908): initial-connect
    /// retry with stdin buffering, mid-session reconnect, and synthesized
    /// errors while disconnected mid-session — retryable for ids never
    /// written to the socket, non-retryable outcome-unknown for ids already
    /// delivered to the listener (monorepo#1530). `run_bridge` is driven
    /// with in-memory duplex streams in place of stdin/stdout and shrunk retry
    /// knobs so tests stay fast.
    mod stdio_bridge_resilience {
        use std::net::SocketAddr;
        use std::pin::Pin;
        use std::sync::atomic::{AtomicU32, Ordering};
        use std::sync::Arc;
        use std::task::{Context, Poll};
        use std::time::Duration;

        use serde_json::{json, Value};
        use tokio::io::{
            AsyncBufReadExt, AsyncReadExt, AsyncWrite, AsyncWriteExt, BufReader, DuplexStream,
        };
        use tokio::net::{TcpListener, TcpStream};
        use tokio::sync::Notify;
        use tokio::task::JoinHandle;
        use tokio::time::{sleep, timeout};

        use crate::mcp_bridge::{
            pump_session, run_bridge, run_bridge_with, BridgeRetryConfig, SessionEnd,
            BRIDGE_DISCONNECTED_CODE, BRIDGE_DISCONNECTED_MESSAGE, BRIDGE_OUTCOME_UNKNOWN_CODE,
            BRIDGE_OUTCOME_UNKNOWN_MESSAGE, INITIAL_BUFFER_MAX_BYTES, INITIAL_BUFFER_MAX_LINES,
        };

        fn fast_cfg() -> BridgeRetryConfig {
            BridgeRetryConfig {
                initial_attempts: 50,
                reconnect_window: Duration::from_secs(2),
                backoff_start: Duration::from_millis(10),
                backoff_cap: Duration::from_millis(20),
                // Generous so gated test connectors (monorepo#906 regressions)
                // never hit the per-attempt bound.
                connect_timeout: Duration::from_secs(5),
            }
        }

        struct BridgeHarness {
            stdin: DuplexStream,
            stdout: BufReader<DuplexStream>,
            handle: JoinHandle<std::io::Result<()>>,
        }

        fn spawn_bridge(addr: SocketAddr, cfg: BridgeRetryConfig) -> BridgeHarness {
            let (stdin, stdin_remote) = tokio::io::duplex(64 * 1024);
            let (stdout_remote, stdout) = tokio::io::duplex(64 * 1024);
            let handle = tokio::spawn(async move {
                run_bridge(&addr.to_string(), stdin_remote, stdout_remote, cfg).await
            });
            BridgeHarness {
                stdin,
                stdout: BufReader::new(stdout),
                handle,
            }
        }

        /// [`spawn_bridge`] with an injected connector (monorepo#906 tests).
        fn spawn_bridge_with_connector<C, Fut>(
            addr: SocketAddr,
            cfg: BridgeRetryConfig,
            connect: C,
        ) -> BridgeHarness
        where
            C: FnMut() -> Fut + Send + 'static,
            Fut: std::future::Future<Output = std::io::Result<TcpStream>> + Send,
        {
            let (stdin, stdin_remote) = tokio::io::duplex(64 * 1024);
            let (stdout_remote, stdout) = tokio::io::duplex(64 * 1024);
            let handle = tokio::spawn(async move {
                run_bridge_with(&addr.to_string(), stdin_remote, stdout_remote, cfg, connect).await
            });
            BridgeHarness {
                stdin,
                stdout: BufReader::new(stdout),
                handle,
            }
        }

        impl BridgeHarness {
            async fn send_request(&mut self, id: i64) {
                let line = format!("{{\"jsonrpc\":\"2.0\",\"id\":{id},\"method\":\"ping\"}}\n");
                self.stdin.write_all(line.as_bytes()).await.unwrap();
                self.stdin.flush().await.unwrap();
            }

            async fn read_response(&mut self) -> Value {
                let mut line = String::new();
                timeout(Duration::from_secs(5), self.stdout.read_line(&mut line))
                    .await
                    .expect("bridge stdout read timed out")
                    .expect("bridge stdout read failed");
                serde_json::from_str(line.trim()).expect("bridge stdout line is JSON")
            }
        }

        fn assert_disconnected_error(resp: &Value, id: i64) {
            assert_eq!(resp["id"], json!(id));
            assert_eq!(resp["error"]["code"], json!(BRIDGE_DISCONNECTED_CODE));
            assert_eq!(resp["error"]["message"], json!(BRIDGE_DISCONNECTED_MESSAGE));
            assert_eq!(resp["error"]["data"]["retryable"], json!(true));
        }

        fn assert_outcome_unknown_error(resp: &Value, id: i64) {
            assert_eq!(resp["id"], json!(id));
            assert_eq!(resp["error"]["code"], json!(BRIDGE_OUTCOME_UNKNOWN_CODE));
            assert_eq!(
                resp["error"]["message"],
                json!(BRIDGE_OUTCOME_UNKNOWN_MESSAGE)
            );
            assert_eq!(resp["error"]["data"]["retryable"], json!(false));
        }

        /// Answer every request line on `stream` with `{"ok":true}`.
        async fn answer_requests(stream: TcpStream) {
            let (read, mut write) = stream.into_split();
            let mut lines = BufReader::new(read).lines();
            while let Ok(Some(line)) = lines.next_line().await {
                let msg: Value = serde_json::from_str(&line).unwrap();
                let resp = json!({"jsonrpc":"2.0","id":msg["id"],"result":{"ok":true}});
                let out = format!("{resp}\n");
                if write.write_all(out.as_bytes()).await.is_err() {
                    break;
                }
            }
        }

        /// Bind a listener on an ephemeral port, then free the port so nothing
        /// is listening at that address (yet).
        async fn reserve_free_addr() -> SocketAddr {
            let probe = TcpListener::bind(("127.0.0.1", 0)).await.unwrap();
            let addr = probe.local_addr().unwrap();
            drop(probe);
            addr
        }

        #[tokio::test]
        async fn initial_connect_retries_until_listener_appears() {
            let addr = reserve_free_addr().await;
            let mut bridge = spawn_bridge(addr, fast_cfg());
            // Bind only after the bridge has started (and failed) connecting.
            sleep(Duration::from_millis(50)).await;
            let listener = TcpListener::bind(addr).await.unwrap();
            let (conn, _) = timeout(Duration::from_secs(5), listener.accept())
                .await
                .expect("bridge never retried the connect")
                .unwrap();
            tokio::spawn(answer_requests(conn));

            bridge.send_request(1).await;
            let resp = bridge.read_response().await;
            assert_eq!(resp["id"], json!(1));
            assert_eq!(resp["result"]["ok"], json!(true));
        }

        /// A request written before the listener is reachable is buffered
        /// during the initial connect window (monorepo#908) — never answered
        /// with `-32001` — and gets the real server response once the connect
        /// succeeds a couple of backoff cycles later.
        #[tokio::test]
        async fn initial_window_buffers_requests_until_listener_appears() {
            let addr = reserve_free_addr().await;
            let mut bridge = spawn_bridge(addr, fast_cfg());
            // Written while nothing is listening: must be buffered, not
            // answered with the retryable error.
            bridge.send_request(1).await;
            // Let the bridge burn a couple of backoff cycles first.
            sleep(Duration::from_millis(50)).await;
            let listener = TcpListener::bind(addr).await.unwrap();
            let (conn, _) = timeout(Duration::from_secs(5), listener.accept())
                .await
                .expect("bridge never retried the connect")
                .unwrap();
            tokio::spawn(answer_requests(conn));
            // The FIRST line on stdout is the real response for id 1; a
            // pre-fix bridge writes the -32001 reject here instead.
            let resp = bridge.read_response().await;
            assert_eq!(resp["id"], json!(1));
            assert_eq!(
                resp["result"]["ok"],
                json!(true),
                "buffered request must get the real response, got: {resp}"
            );
        }

        /// Initial-window exhaustion still surfaces `Err` (the bridge exits
        /// non-zero) and the buffered request is never answered — no `-32001`
        /// is written for it (monorepo#908).
        #[tokio::test]
        async fn initial_window_exhaustion_errors_without_answering_buffered_requests() {
            let addr = reserve_free_addr().await;
            let cfg = BridgeRetryConfig {
                initial_attempts: 5,
                ..fast_cfg()
            };
            let mut bridge = spawn_bridge(addr, cfg);
            bridge.send_request(7).await;
            // The bounded retry gives up with the connect error.
            let result = timeout(Duration::from_secs(5), bridge.handle)
                .await
                .expect("bridge did not give up in time")
                .unwrap();
            assert!(
                result.is_err(),
                "initial-connect give-up must surface an error"
            );
            // Nothing was written to stdout for the buffered request.
            let mut leftover = String::new();
            timeout(
                Duration::from_secs(5),
                bridge.stdout.read_to_string(&mut leftover),
            )
            .await
            .expect("stdout drain timed out")
            .expect("stdout drain failed");
            assert!(
                leftover.is_empty(),
                "no response may be written for buffered requests on give-up: {leftover}"
            );
        }

        /// Lines past the defensive initial-window buffer cap fall back to
        /// the retryable disconnected error instead of growing the buffer
        /// unboundedly (monorepo#908).
        #[tokio::test]
        async fn initial_buffer_overflow_falls_back_to_retryable_error() {
            let addr = reserve_free_addr().await;
            let mut bridge = spawn_bridge(addr, fast_cfg());
            for id in 0..(i64::try_from(INITIAL_BUFFER_MAX_LINES).expect("small const")) {
                bridge.send_request(id).await;
            }
            // The line past the cap is rejected with the retryable error.
            bridge.send_request(9999).await;
            let resp = bridge.read_response().await;
            assert_disconnected_error(&resp, 9999);
        }

        /// The byte cap rejects an oversized line, and overflow is sticky:
        /// once any line has been rejected, later lines that would fit are
        /// rejected too, so a later request can never be served after an
        /// earlier one failed (monorepo#908).
        #[tokio::test]
        async fn initial_buffer_byte_cap_overflow_is_sticky() {
            let addr = reserve_free_addr().await;
            let mut bridge = spawn_bridge(addr, fast_cfg());
            // One line larger than the whole byte cap is rejected outright.
            let big = format!(
                "{{\"jsonrpc\":\"2.0\",\"id\":1,\"method\":\"ping\",\"params\":{{\"pad\":\"{}\"}}}}\n",
                "x".repeat(INITIAL_BUFFER_MAX_BYTES)
            );
            bridge.stdin.write_all(big.as_bytes()).await.unwrap();
            bridge.stdin.flush().await.unwrap();
            let resp = bridge.read_response().await;
            assert_disconnected_error(&resp, 1);
            // A small line that would fit is rejected too — overflow is sticky.
            bridge.send_request(2).await;
            let resp = bridge.read_response().await;
            assert_disconnected_error(&resp, 2);
        }

        /// Buffered lines participate in `pending` tracking once flushed: a
        /// TCP drop right after the flush still synthesizes an error for the
        /// buffered id (monorepo#908) — and since the line was delivered to
        /// the listener, it is the non-retryable outcome-unknown error
        /// (monorepo#1530).
        #[tokio::test]
        async fn buffered_request_gets_outcome_unknown_error_if_tcp_drops_after_flush() {
            let addr = reserve_free_addr().await;
            let mut bridge = spawn_bridge(addr, fast_cfg());
            bridge.send_request(9).await;
            sleep(Duration::from_millis(50)).await;
            let listener = TcpListener::bind(addr).await.unwrap();
            let (conn, _) = timeout(Duration::from_secs(5), listener.accept())
                .await
                .expect("bridge never connected")
                .unwrap();
            // Read the flushed request, then drop the connection without
            // answering it.
            let (read, write) = conn.into_split();
            let mut lines = BufReader::new(read).lines();
            let line = timeout(Duration::from_secs(5), lines.next_line())
                .await
                .expect("buffered request never flushed to the connection")
                .unwrap()
                .unwrap();
            let msg: Value = serde_json::from_str(&line).unwrap();
            assert_eq!(msg["id"], json!(9), "buffered line is flushed in order");
            drop(write);
            drop(lines);
            let resp = bridge.read_response().await;
            assert_outcome_unknown_error(&resp, 9);
        }

        #[tokio::test]
        async fn reconnects_after_mid_session_drop() {
            let listener = TcpListener::bind(("127.0.0.1", 0)).await.unwrap();
            let addr = listener.local_addr().unwrap();
            let mut bridge = spawn_bridge(addr, fast_cfg());

            // First connection answers one request, then the server drops it.
            let (conn1, _) = timeout(Duration::from_secs(5), listener.accept())
                .await
                .unwrap()
                .unwrap();
            let (read1, mut write1) = conn1.into_split();
            bridge.send_request(1).await;
            let mut lines1 = BufReader::new(read1).lines();
            let line = timeout(Duration::from_secs(5), lines1.next_line())
                .await
                .unwrap()
                .unwrap()
                .unwrap();
            let msg: Value = serde_json::from_str(&line).unwrap();
            let resp = json!({"jsonrpc":"2.0","id":msg["id"],"result":{"ok":true}});
            write1
                .write_all(format!("{resp}\n").as_bytes())
                .await
                .unwrap();
            let resp = bridge.read_response().await;
            assert_eq!(resp["id"], json!(1));
            drop(write1);
            drop(lines1);

            // The bridge reconnects to the same addr; the second connection
            // serves the follow-up request and stdio never closed in between.
            let (conn2, _) = timeout(Duration::from_secs(5), listener.accept())
                .await
                .expect("bridge never reconnected")
                .unwrap();
            tokio::spawn(answer_requests(conn2));
            bridge.send_request(2).await;
            let resp = bridge.read_response().await;
            assert_eq!(resp["id"], json!(2));
            assert_eq!(resp["result"]["ok"], json!(true));
        }

        /// Regression for monorepo#906: a stdin line that races a *pending*
        /// mid-session reconnect attempt must be decided by that attempt's
        /// outcome, not rejected against possibly stale readiness. The gated
        /// connector pins the bridge in "attempt in flight" while the test
        /// writes the request; a pre-fix bridge answers `-32001` here, the
        /// fixed bridge holds the line and forwards it on the fresh
        /// connection.
        #[tokio::test]
        async fn line_racing_pending_reconnect_is_forwarded_when_attempt_succeeds() {
            let listener = TcpListener::bind(("127.0.0.1", 0)).await.unwrap();
            let addr = listener.local_addr().unwrap();
            let attempts = Arc::new(AtomicU32::new(0));
            let gate = Arc::new(Notify::new());
            let connect = {
                let attempts = attempts.clone();
                let gate = gate.clone();
                move || {
                    let n = attempts.fetch_add(1, Ordering::SeqCst) + 1;
                    let gate = gate.clone();
                    async move {
                        if n >= 2 {
                            gate.notified().await;
                        }
                        TcpStream::connect(addr).await
                    }
                }
            };
            let mut bridge = spawn_bridge_with_connector(addr, fast_cfg(), connect);

            // Session 1: answer one request cleanly, then drop the TCP side.
            let (conn1, _) = timeout(Duration::from_secs(5), listener.accept())
                .await
                .unwrap()
                .unwrap();
            let (read1, mut write1) = conn1.into_split();
            bridge.send_request(1).await;
            let mut lines1 = BufReader::new(read1).lines();
            let line = timeout(Duration::from_secs(5), lines1.next_line())
                .await
                .unwrap()
                .unwrap()
                .unwrap();
            let msg: Value = serde_json::from_str(&line).unwrap();
            let resp = json!({"jsonrpc":"2.0","id":msg["id"],"result":{"ok":true}});
            write1
                .write_all(format!("{resp}\n").as_bytes())
                .await
                .unwrap();
            let resp = bridge.read_response().await;
            assert_eq!(resp["id"], json!(1));
            drop(write1);
            drop(lines1);

            // Wait until reconnect attempt 2 is in flight (parked on the gate).
            timeout(Duration::from_secs(5), async {
                while attempts.load(Ordering::SeqCst) < 2 {
                    sleep(Duration::from_millis(5)).await;
                }
            })
            .await
            .expect("bridge never started the reconnect attempt");

            // This request races the pending attempt: it must be held, not
            // rejected with -32001. While the gate keeps the attempt pending,
            // stdout must stay silent — a pre-fix bridge writes the reject in
            // this window (this wait also gives the bridge time to consume
            // the line before the gate opens, so the race is exercised).
            bridge.send_request(2).await;
            let mut peek = String::new();
            let silent = timeout(
                Duration::from_millis(200),
                bridge.stdout.read_line(&mut peek),
            )
            .await;
            assert!(
                silent.is_err(),
                "no reject may be written while the attempt is pending, got: {peek}"
            );
            gate.notify_one();

            let (conn2, _) = timeout(Duration::from_secs(5), listener.accept())
                .await
                .expect("bridge never completed the reconnect")
                .unwrap();
            tokio::spawn(answer_requests(conn2));

            let resp = bridge.read_response().await;
            assert_eq!(resp["id"], json!(2));
            assert_eq!(
                resp["result"]["ok"],
                json!(true),
                "request racing a successful reconnect must be forwarded, got: {resp}"
            );
        }

        /// Companion to the race regression (monorepo#906): when the pending
        /// attempt the line raced *fails*, the held line gets the retryable
        /// disconnected error — held lines are decided by the attempt's
        /// outcome, never silently dropped.
        #[tokio::test]
        async fn line_racing_pending_reconnect_gets_retryable_error_when_attempt_fails() {
            let listener = TcpListener::bind(("127.0.0.1", 0)).await.unwrap();
            let addr = listener.local_addr().unwrap();
            let attempts = Arc::new(AtomicU32::new(0));
            let gate = Arc::new(Notify::new());
            let connect = {
                let attempts = attempts.clone();
                let gate = gate.clone();
                move || {
                    let n = attempts.fetch_add(1, Ordering::SeqCst) + 1;
                    let gate = gate.clone();
                    async move {
                        if n == 1 {
                            TcpStream::connect(addr).await
                        } else {
                            gate.notified().await;
                            Err(std::io::Error::new(
                                std::io::ErrorKind::ConnectionRefused,
                                "gated refuse",
                            ))
                        }
                    }
                }
            };
            let mut bridge = spawn_bridge_with_connector(addr, fast_cfg(), connect);

            // Session 1: answer one request cleanly, then drop the TCP side.
            let (conn1, _) = timeout(Duration::from_secs(5), listener.accept())
                .await
                .unwrap()
                .unwrap();
            let (read1, mut write1) = conn1.into_split();
            bridge.send_request(1).await;
            let mut lines1 = BufReader::new(read1).lines();
            let line = timeout(Duration::from_secs(5), lines1.next_line())
                .await
                .unwrap()
                .unwrap()
                .unwrap();
            let msg: Value = serde_json::from_str(&line).unwrap();
            let resp = json!({"jsonrpc":"2.0","id":msg["id"],"result":{"ok":true}});
            write1
                .write_all(format!("{resp}\n").as_bytes())
                .await
                .unwrap();
            let resp = bridge.read_response().await;
            assert_eq!(resp["id"], json!(1));
            drop(write1);
            drop(lines1);

            timeout(Duration::from_secs(5), async {
                while attempts.load(Ordering::SeqCst) < 2 {
                    sleep(Duration::from_millis(5)).await;
                }
            })
            .await
            .expect("bridge never started the reconnect attempt");

            // Held while the gated attempt is pending: stdout stays silent
            // until the attempt fails (this wait also gives the bridge time
            // to consume the line before the gate opens).
            bridge.send_request(2).await;
            let mut peek = String::new();
            let silent = timeout(
                Duration::from_millis(200),
                bridge.stdout.read_line(&mut peek),
            )
            .await;
            assert!(
                silent.is_err(),
                "no reject may be written while the attempt is pending, got: {peek}"
            );
            gate.notify_one();

            let resp = bridge.read_response().await;
            assert_disconnected_error(&resp, 2);
        }

        /// An in-flight id delivered to the listener before the drop is
        /// answered with the non-retryable outcome-unknown error — the
        /// listener may have executed it, so a blind retry could double-apply
        /// a non-idempotent call (monorepo#1530).
        #[tokio::test]
        async fn delivered_in_flight_ids_get_outcome_unknown_error_when_connection_drops() {
            let listener = TcpListener::bind(("127.0.0.1", 0)).await.unwrap();
            let addr = listener.local_addr().unwrap();
            let mut bridge = spawn_bridge(addr, fast_cfg());

            let (conn1, _) = timeout(Duration::from_secs(5), listener.accept())
                .await
                .unwrap()
                .unwrap();
            let (read1, write1) = conn1.into_split();
            // Forwarded but never answered: the server reads the request and
            // then drops the connection.
            bridge.send_request(9).await;
            let mut lines1 = BufReader::new(read1).lines();
            timeout(Duration::from_secs(5), lines1.next_line())
                .await
                .unwrap()
                .unwrap()
                .unwrap();
            drop(write1);
            drop(lines1);

            let resp = bridge.read_response().await;
            assert_outcome_unknown_error(&resp, 9);
        }

        /// An `AsyncWrite` that forwards the first `remaining` `poll_write`
        /// calls to `inner` and then fails, injecting a deterministic
        /// mid-flush TCP drop. `write_line` issues two writes per line
        /// (payload + newline), so `remaining = 2 * n` delivers exactly `n`
        /// lines before the drop.
        struct FailAfterWrites<W> {
            inner: W,
            remaining: usize,
        }

        impl<W: AsyncWrite + Unpin> AsyncWrite for FailAfterWrites<W> {
            fn poll_write(
                mut self: Pin<&mut Self>,
                cx: &mut Context<'_>,
                buf: &[u8],
            ) -> Poll<std::io::Result<usize>> {
                if self.remaining == 0 {
                    return Poll::Ready(Err(std::io::Error::new(
                        std::io::ErrorKind::BrokenPipe,
                        "injected tcp drop",
                    )));
                }
                self.remaining -= 1;
                Pin::new(&mut self.inner).poll_write(cx, buf)
            }

            fn poll_flush(
                mut self: Pin<&mut Self>,
                cx: &mut Context<'_>,
            ) -> Poll<std::io::Result<()>> {
                Pin::new(&mut self.inner).poll_flush(cx)
            }

            fn poll_shutdown(
                mut self: Pin<&mut Self>,
                cx: &mut Context<'_>,
            ) -> Poll<std::io::Result<()>> {
                Pin::new(&mut self.inner).poll_shutdown(cx)
            }
        }

        /// A TCP drop *mid-flush* classifies the buffered batch precisely:
        /// the prefix already written to the socket gets the non-retryable
        /// outcome-unknown error, the never-written suffix keeps the
        /// retryable disconnected error (monorepo#1530).
        #[tokio::test]
        async fn partial_flush_splits_outcome_unknown_from_retryable() {
            let (tcp_write, _tcp_sink) = tokio::io::duplex(4096);
            // Two writes per line: line 1 is delivered, line 2 hits the drop.
            let tcp_write = FailAfterWrites {
                inner: tcp_write,
                remaining: 2,
            };
            let (_stdin_write, stdin_read) = tokio::io::duplex(4096);
            let mut input = BufReader::new(stdin_read).lines();
            let (mut out_write, out_read) = tokio::io::duplex(4096);
            let buffered = vec![
                json!({"jsonrpc":"2.0","id":1,"method":"tools/call"}).to_string(),
                json!({"jsonrpc":"2.0","id":2,"method":"tools/call"}).to_string(),
            ];
            let end = timeout(
                Duration::from_secs(5),
                pump_session(
                    tokio::io::empty(),
                    tcp_write,
                    buffered,
                    &mut input,
                    &mut out_write,
                ),
            )
            .await
            .expect("pump_session hung")
            .unwrap();
            assert!(matches!(end, SessionEnd::TcpDropped));
            drop(out_write);
            let mut out_lines = BufReader::new(out_read).lines();
            let mut by_id = std::collections::HashMap::new();
            for _ in 0..2 {
                let line = out_lines.next_line().await.unwrap().unwrap();
                let resp: Value = serde_json::from_str(&line).unwrap();
                by_id.insert(resp["id"].as_i64().unwrap(), resp);
            }
            assert_outcome_unknown_error(&by_id[&1], 1);
            assert_disconnected_error(&by_id[&2], 2);
            assert_eq!(out_lines.next_line().await.unwrap(), None);
        }

        /// Notifications carry no `id` and get no synthesized response on a
        /// drop: after a delivered notification and a delivered request, only
        /// the request is answered.
        #[tokio::test]
        async fn notification_gets_no_synthesized_response_on_drop() {
            let (tcp_write, _tcp_sink) = tokio::io::duplex(4096);
            let (_stdin_write, stdin_read) = tokio::io::duplex(4096);
            let mut input = BufReader::new(stdin_read).lines();
            let (mut out_write, out_read) = tokio::io::duplex(4096);
            let buffered = vec![
                json!({"jsonrpc":"2.0","method":"notifications/initialized"}).to_string(),
                json!({"jsonrpc":"2.0","id":5,"method":"tools/call"}).to_string(),
            ];
            // The empty TCP read side yields EOF right after the flush, so
            // the session ends as TcpDropped with both lines delivered.
            let end = timeout(
                Duration::from_secs(5),
                pump_session(
                    tokio::io::empty(),
                    tcp_write,
                    buffered,
                    &mut input,
                    &mut out_write,
                ),
            )
            .await
            .expect("pump_session hung")
            .unwrap();
            assert!(matches!(end, SessionEnd::TcpDropped));
            drop(out_write);
            let mut out_lines = BufReader::new(out_read).lines();
            let line = out_lines.next_line().await.unwrap().unwrap();
            let resp: Value = serde_json::from_str(&line).unwrap();
            assert_outcome_unknown_error(&resp, 5);
            assert_eq!(out_lines.next_line().await.unwrap(), None);
        }

        #[tokio::test]
        async fn gap_requests_error_and_bridge_exits_cleanly_after_window() {
            let listener = TcpListener::bind(("127.0.0.1", 0)).await.unwrap();
            let addr = listener.local_addr().unwrap();
            let cfg = BridgeRetryConfig {
                reconnect_window: Duration::from_millis(300),
                ..fast_cfg()
            };
            let mut bridge = spawn_bridge(addr, cfg);

            let (conn1, _) = timeout(Duration::from_secs(5), listener.accept())
                .await
                .unwrap()
                .unwrap();
            // Kill the connection and the listener: the daemon is gone for good.
            drop(conn1);
            drop(listener);

            // Let the bridge observe the drop before sending: a line racing
            // the not-yet-noticed drop can be written to the dead socket and
            // classified delivered (-32002) instead of exercising the gap
            // path this test is about.
            sleep(Duration::from_millis(50)).await;

            // A request during the reconnect gap gets the retryable error.
            // Tolerate one -32002 in case the request still raced the
            // not-yet-noticed dead socket (safe at-most-once direction); the
            // follow-up request is then guaranteed to hit the gap path.
            bridge.send_request(2).await;
            let resp = bridge.read_response().await;
            if resp["error"]["code"] == json!(BRIDGE_OUTCOME_UNKNOWN_CODE) {
                bridge.send_request(3).await;
                let resp = bridge.read_response().await;
                assert_disconnected_error(&resp, 3);
            } else {
                assert_disconnected_error(&resp, 2);
            }

            // Once the reconnect window is exhausted the bridge exits cleanly.
            let result = timeout(Duration::from_secs(5), bridge.handle)
                .await
                .expect("bridge did not exit after reconnect window")
                .unwrap();
            assert!(
                result.is_ok(),
                "reconnect give-up must exit cleanly: {result:?}"
            );
        }

        #[tokio::test]
        async fn stdin_eof_ends_bridge_cleanly() {
            let listener = TcpListener::bind(("127.0.0.1", 0)).await.unwrap();
            let addr = listener.local_addr().unwrap();
            let bridge = spawn_bridge(addr, fast_cfg());
            let (conn, _) = timeout(Duration::from_secs(5), listener.accept())
                .await
                .unwrap()
                .unwrap();
            tokio::spawn(answer_requests(conn));
            drop(bridge.stdin);
            let result = timeout(Duration::from_secs(5), bridge.handle)
                .await
                .expect("bridge did not exit on stdin EOF")
                .unwrap();
            assert!(result.is_ok(), "stdin EOF must exit cleanly: {result:?}");
        }
    }
}

// Discrete workspace-metadata tool tests removed in WSAPI-8: the daemon no
// longer registers `get_workspace_details` / `set_workspace_title` /
// `set_workspace_status_message`. Their bindings are exercised through
// `workspace_api` inside `wsapi3_bindings_tests` and friends.
#[cfg(test)]
mod _removed_workspace_metadata_tool_tests {
    // Intentionally empty — module preserved as a landing pad for the docs
    // comment above. The original tests targeted discrete daemon tools that
    // no longer exist post-cutover.
}
#[cfg(all(test, any()))]
mod _dead_workspace_metadata_tool_tests {
    use std::sync::{Arc, Mutex};

    use intent_core::{
        BoxFuture, Result, Workspace, WorkspaceActivity, WorkspaceApi, WorkspaceAttention,
        WorkspaceId, WorkspaceStatus, WorkspaceUpdate,
    };
    use serde_json::{json, Value};

    use crate::WorkspaceMcpServer;

    struct WorkspaceMockApi {
        ws: Mutex<Workspace>,
        updates: Mutex<Vec<WorkspaceUpdate>>,
    }

    impl WorkspaceMockApi {
        fn new(id: &str, title: &str, branch: &str) -> Arc<Self> {
            let now = "2026-01-01T00:00:00Z".to_string();
            let ws = Workspace {
                id: WorkspaceId::from_string(id),
                title: title.to_string(),
                branch: branch.to_string(),
                base_ref: None,
                base_commit_sha: None,
                status: WorkspaceStatus::Active,
                status_message: None,
                status_image_asset_id: None,
                activity: WorkspaceActivity::Idle,
                attention: WorkspaceAttention::None,
                created_at: now.clone(),
                updated_at: now,
                last_activity: None,
                tags: vec!["demo".to_string()],
                path: None,
                repository_path: None,
                repository_owner: None,
                repository_name: Some("intent-hq/intentd".to_string()),
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
            };
            Arc::new(Self {
                ws: Mutex::new(ws),
                updates: Mutex::new(Vec::new()),
            })
        }
    }

    impl WorkspaceApi for WorkspaceMockApi {
        fn get_workspace(&self, _id: WorkspaceId) -> BoxFuture<'_, Result<Workspace>> {
            let snapshot = self.ws.lock().unwrap().clone();
            Box::pin(async move { Ok(snapshot) })
        }

        fn update_workspace(
            &self,
            _id: WorkspaceId,
            update: WorkspaceUpdate,
        ) -> BoxFuture<'_, Result<Workspace>> {
            self.updates.lock().unwrap().push(update.clone());
            let mut ws = self.ws.lock().unwrap();
            if let Some(t) = update.title {
                ws.title = t;
            }
            if let Some(m) = update.status_message {
                // Mirror `intent-services::update_workspace`: empty or
                // whitespace-only `status_message` clears to `None` so the
                // discrete MCP tool's clear semantics read back correctly.
                ws.status_message = if m.trim().is_empty() { None } else { Some(m) };
            }
            let snapshot = ws.clone();
            Box::pin(async move { Ok(snapshot) })
        }
    }

    async fn call(srv: &WorkspaceMcpServer, name: &str, args: Value) -> Value {
        srv.handle_message(&json!({
            "jsonrpc": "2.0", "id": 1, "method": "tools/call",
            "params": { "name": name, "arguments": args }
        }))
        .await
        .expect("tools/call must produce a response")
    }

    fn parse_content(resp: &Value) -> Value {
        let text = resp["result"]["content"][0]["text"].as_str().unwrap();
        serde_json::from_str(text).unwrap()
    }

    #[tokio::test]
    async fn get_workspace_details_returns_reduced_metadata() {
        // `title == id` is the daemon's "still a slug" marker (created via
        // `create_workspace` when no title is supplied); `hasTitle` reflects
        // this by treating the placeholder as still-untitled.
        let api = WorkspaceMockApi::new("amber-forest", "amber-forest", "amber-forest");
        let srv = WorkspaceMcpServer::new(api.clone(), WorkspaceId::from_string("amber-forest"));
        let resp = call(&srv, "get_workspace_details", json!({})).await;
        assert_eq!(resp["result"]["isError"], json!(false));
        let body = parse_content(&resp);
        assert_eq!(body["id"], json!("amber-forest"));
        assert_eq!(body["title"], json!("amber-forest"));
        assert_eq!(body["hasTitle"], json!(false));
        assert_eq!(body["status"], json!("Active"));
        assert_eq!(body["statusMessage"], Value::Null);
        assert_eq!(body["branch"], json!("amber-forest"));
        assert_eq!(body["repositoryName"], json!("intent-hq/intentd"));
        assert_eq!(body["tags"], json!(["demo"]));
    }

    #[tokio::test]
    async fn get_workspace_details_reports_has_title_for_custom_titles() {
        let api = WorkspaceMockApi::new("amber-forest", "Add dark mode", "amber-forest");
        let srv = WorkspaceMcpServer::new(api, WorkspaceId::from_string("amber-forest"));
        let body = parse_content(&call(&srv, "get_workspace_details", json!({})).await);
        assert_eq!(body["hasTitle"], json!(true));
        assert_eq!(body["title"], json!("Add dark mode"));
    }

    #[tokio::test]
    async fn set_workspace_title_updates_when_title_still_matches_id() {
        // Fresh workspace where the seeded title equals the id — a
        // still-untitled slug per the reference `setTitle` guard.
        let api = WorkspaceMockApi::new("amber-forest", "amber-forest", "amber-forest");
        let srv = WorkspaceMcpServer::new(api.clone(), WorkspaceId::from_string("amber-forest"));
        let resp = call(
            &srv,
            "set_workspace_title",
            json!({ "title": "  Add dark mode support  " }),
        )
        .await;
        assert_eq!(resp["result"]["isError"], json!(false));
        let body = parse_content(&resp);
        assert_eq!(body["ok"], json!(true));
        assert_eq!(body["title"], json!("Add dark mode support"));
        assert_eq!(body["branch"], json!("amber-forest"));
        assert!(body.get("skipped").is_none(), "should not skip: {body}");
        // Underlying update_workspace was called with only the title populated
        // (branch rename is deferred until the daemon owns a rename path).
        let updates = api.updates.lock().unwrap();
        assert_eq!(updates.len(), 1);
        assert_eq!(updates[0].title.as_deref(), Some("Add dark mode support"));
        assert!(updates[0].branch.is_none());
        assert!(updates[0].status_message.is_none());
    }

    #[tokio::test]
    async fn set_workspace_title_skips_when_custom_title_already_set() {
        let api = WorkspaceMockApi::new("amber-forest", "Add dark mode", "auth-refactor");
        let srv = WorkspaceMcpServer::new(api.clone(), WorkspaceId::from_string("amber-forest"));
        let resp = call(
            &srv,
            "set_workspace_title",
            json!({ "title": "Something else" }),
        )
        .await;
        assert_eq!(resp["result"]["isError"], json!(false));
        let body = parse_content(&resp);
        assert_eq!(body["ok"], json!(true));
        assert_eq!(body["skipped"], json!(true));
        assert_eq!(body["title"], json!("Add dark mode"));
        assert_eq!(body["branch"], json!("auth-refactor"));
        // No update reached the API on the skip path.
        assert!(api.updates.lock().unwrap().is_empty());
    }

    #[tokio::test]
    async fn set_workspace_title_rejects_empty_title() {
        let api = WorkspaceMockApi::new("amber-forest", "amber-forest", "amber-forest");
        let srv = WorkspaceMcpServer::new(api, WorkspaceId::from_string("amber-forest"));
        let resp = call(&srv, "set_workspace_title", json!({ "title": "   " })).await;
        assert_eq!(resp["error"]["code"], json!(-32602));
    }

    #[tokio::test]
    async fn set_workspace_status_message_updates_and_serializes_string() {
        let api = WorkspaceMockApi::new("amber-forest", "Add dark mode", "amber-forest");
        let srv = WorkspaceMcpServer::new(api.clone(), WorkspaceId::from_string("amber-forest"));
        let resp = call(
            &srv,
            "set_workspace_status_message",
            json!({ "statusMessage": " Implementing dark mode toggle " }),
        )
        .await;
        assert_eq!(resp["result"]["isError"], json!(false));
        let body = parse_content(&resp);
        assert_eq!(body["ok"], json!(true));
        assert_eq!(
            body["statusMessage"],
            json!("Implementing dark mode toggle")
        );
        let updates = api.updates.lock().unwrap();
        assert_eq!(updates.len(), 1);
        assert_eq!(
            updates[0].status_message.as_deref(),
            Some("Implementing dark mode toggle")
        );
    }

    #[tokio::test]
    async fn set_workspace_status_message_clears_on_empty_string() {
        let api = WorkspaceMockApi::new("amber-forest", "Add dark mode", "amber-forest");
        let srv = WorkspaceMcpServer::new(api.clone(), WorkspaceId::from_string("amber-forest"));
        let resp = call(
            &srv,
            "set_workspace_status_message",
            json!({ "statusMessage": "" }),
        )
        .await;
        let body = parse_content(&resp);
        assert_eq!(body["ok"], json!(true));
        assert_eq!(body["statusMessage"], Value::Null);
        let updates = api.updates.lock().unwrap();
        assert_eq!(updates.len(), 1);
        // The MCP tool still passes the empty string through in the delta;
        // the services layer (and the mock, mirroring it) is what normalizes
        // to `None` on write. Pin both: the input delta stays `Some("")`,
        // and the stored `status_message` reads back as `None`.
        assert_eq!(updates[0].status_message.as_deref(), Some(""));
        assert!(api.ws.lock().unwrap().status_message.is_none());
    }

    #[tokio::test]
    async fn set_workspace_status_message_enforces_length_cap() {
        let api = WorkspaceMockApi::new("amber-forest", "Add dark mode", "amber-forest");
        let srv = WorkspaceMcpServer::new(api.clone(), WorkspaceId::from_string("amber-forest"));
        let too_long = "x".repeat(intent_core::WORKSPACE_STATUS_MESSAGE_MAX_LENGTH + 1);
        let resp = call(
            &srv,
            "set_workspace_status_message",
            json!({ "statusMessage": too_long }),
        )
        .await;
        assert_eq!(resp["error"]["code"], json!(-32602));
        let msg = resp["error"]["message"].as_str().unwrap();
        assert!(
            msg.contains("statusMessage")
                && msg.contains(&intent_core::WORKSPACE_STATUS_MESSAGE_MAX_LENGTH.to_string()),
            "unexpected error message: {msg}",
        );
        assert!(api.updates.lock().unwrap().is_empty());
    }

    #[tokio::test]
    async fn workspace_metadata_tools_are_registered_in_tools_list() {
        // Sanity check that the three tools appear in `tools/list` — agents
        // discover them via the standard MCP handshake.
        let api = WorkspaceMockApi::new("amber-forest", "amber-forest", "amber-forest");
        let srv = WorkspaceMcpServer::new(api, WorkspaceId::from_string("amber-forest"));
        let resp = srv
            .handle_message(&json!({ "jsonrpc": "2.0", "id": 1, "method": "tools/list" }))
            .await
            .unwrap();
        let names: Vec<String> = resp["result"]["tools"]
            .as_array()
            .unwrap()
            .iter()
            .map(|t| t["name"].as_str().unwrap().to_string())
            .collect();
        for required in [
            "get_workspace_details",
            "set_workspace_title",
            "set_workspace_status_message",
        ] {
            assert!(
                names.contains(&required.to_string()),
                "tools/list missing {required}: {names:?}"
            );
        }
    }
}

/// WSAPI-2 `workspace_api` MCP tool: agent-supplied JavaScript against the
/// workspace API. This module exercises the tool registration, the
/// dispatch's envelope semantics (success / undefined / JS error) and the
/// `ws.workspace.info()` binding proof.
#[cfg(test)]
mod workspace_api_tool_tests {
    use std::sync::{Arc, Mutex};
    use std::time::Duration;

    use intent_core::{
        BoxFuture, Result, Workspace, WorkspaceActivity, WorkspaceApi, WorkspaceAttention,
        WorkspaceId, WorkspaceStatus,
    };
    use serde_json::{json, Value};

    use crate::WorkspaceMcpServer;

    struct WorkspaceInfoMockApi {
        ws: Mutex<Workspace>,
    }

    impl WorkspaceInfoMockApi {
        fn new(id: &str, path: Option<&str>) -> Arc<Self> {
            let now = "2026-01-01T00:00:00Z".to_string();
            let ws = Workspace {
                id: WorkspaceId::from_string(id),
                title: id.to_string(),
                branch: id.to_string(),
                base_ref: None,
                base_commit_sha: None,
                status: WorkspaceStatus::Active,
                status_message: None,
                status_image_asset_id: None,
                activity: WorkspaceActivity::Idle,
                attention: WorkspaceAttention::None,
                created_at: now.clone(),
                updated_at: now,
                last_activity: None,
                tags: Vec::new(),
                path: path.map(str::to_string),
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
                context_links: None,
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
            Arc::new(Self { ws: Mutex::new(ws) })
        }
    }

    impl WorkspaceApi for WorkspaceInfoMockApi {
        fn get_workspace(&self, _id: WorkspaceId) -> BoxFuture<'_, Result<Workspace>> {
            let snapshot = self.ws.lock().unwrap().clone();
            Box::pin(async move { Ok(snapshot) })
        }

        // Pin the `workspaceApi.*` output knobs to the legacy behavior (plain
        // pretty JSON, no size limit) so this fixture keeps asserting raw JSON
        // bodies; the TOON/limit paths are covered by
        // `workspace_api_output_limit_tests`.
        fn settings_get(&self, path: String) -> BoxFuture<'_, Result<Value>> {
            Box::pin(async move {
                let value = match path.as_str() {
                    "workspaceApi.toonOutput" => json!(false),
                    "workspaceApi.maxOutputChars" => json!(0),
                    _ => Value::Null,
                };
                Ok(json!({ "path": path, "value": value }))
            })
        }
    }

    fn server(id: &str, path: Option<&str>) -> WorkspaceMcpServer {
        let api = WorkspaceInfoMockApi::new(id, path);
        WorkspaceMcpServer::new(api, WorkspaceId::from_string(id))
    }

    fn git_repo() -> tempfile::TempDir {
        let dir = tempfile::tempdir().unwrap();
        let repo = git2::Repository::init(dir.path()).unwrap();
        std::fs::write(dir.path().join("README.md"), "test\n").unwrap();
        let mut index = repo.index().unwrap();
        index.add_path(std::path::Path::new("README.md")).unwrap();
        let tree_id = index.write_tree().unwrap();
        let tree = repo.find_tree(tree_id).unwrap();
        let sig = git2::Signature::now("Test", "test@example.com").unwrap();
        let commit_id = repo
            .commit(Some("HEAD"), &sig, &sig, "initial", &tree, &[])
            .unwrap();
        let commit = repo.find_commit(commit_id).unwrap();
        repo.branch("main", &commit, true).unwrap();
        repo.branch("feature/ready", &commit, false).unwrap();
        repo.reference_symbolic(
            "refs/remotes/origin/HEAD",
            "refs/remotes/origin/main",
            false,
            "test default branch",
        )
        .unwrap();
        repo.set_head("refs/heads/feature/ready").unwrap();
        drop(commit);
        drop(tree);
        drop(repo);
        dir
    }

    fn server_with_repo(id: &str, repo: &std::path::Path) -> WorkspaceMcpServer {
        let api = WorkspaceInfoMockApi::new(id, None);
        {
            let mut workspace = api.ws.lock().unwrap();
            workspace.repository_path = Some(repo.to_string_lossy().into_owned());
            workspace.repository_owner = Some("intent-hq".to_string());
            workspace.repository_name = Some("intentd".to_string());
        }
        WorkspaceMcpServer::new(api, WorkspaceId::from_string(id))
    }

    async fn call_workspace_api(srv: &WorkspaceMcpServer, code: &str) -> Value {
        srv.handle_message(&json!({
            "jsonrpc": "2.0", "id": 1, "method": "tools/call",
            "params": {
                "name": "workspace_api",
                "arguments": { "code": code, "summary": "unit test" }
            }
        }))
        .await
        .expect("tools/call must produce a response")
    }

    fn tool_text(resp: &Value) -> &str {
        resp["result"]["content"][0]["text"].as_str().unwrap()
    }

    fn tool_json(resp: &Value) -> Value {
        serde_json::from_str(tool_text(resp)).unwrap()
    }

    #[tokio::test]
    async fn propose_sibling_emits_scoped_exact_workspace_create_request() {
        let repo = git_repo();
        let srv = server_with_repo("amber-forest", repo.path());
        let code = r#"return await ws.workspace.proposeSibling({
            title: "Follow up",
            initialPrompt: "Implement the isolated follow-up and test it.",
            specialist: "implementor"
        });"#;
        let first_response = call_workspace_api(&srv, code).await;
        let first = tool_json(&first_response);
        let second = tool_json(&call_workspace_api(&srv, code).await);
        let proposal = &first["proposal"];
        let create = &proposal["preview"]["workspaceCreate"];
        let params = &proposal["payload"]["params"];

        assert_eq!(proposal["kind"], "workspace-create");
        assert_eq!(proposal["payload"]["operation"], "workspace.create");
        assert_eq!(create["mode"], "sibling");
        assert_eq!(create["title"], "Follow up");
        assert_eq!(
            create["initialPrompt"],
            "Implement the isolated follow-up and test it."
        );
        assert_eq!(create["branch"], "main");
        assert_eq!(create["specialist"], "implementor");
        assert_eq!(create["repoPath"], repo.path().to_string_lossy().as_ref());
        assert_eq!(create["githubUrl"], "https://github.com/intent-hq/intentd");
        assert_eq!(
            params["repositoryPath"],
            repo.path().to_string_lossy().as_ref()
        );
        assert_eq!(params["repositoryOwner"], "intent-hq");
        assert_eq!(params["repositoryName"], "intentd");
        assert_eq!(params["baseRef"], "main");
        assert_eq!(params["baseRef"], create["branch"]);
        assert_eq!(params["initialAgent"]["prompt"], create["initialPrompt"]);
        assert_eq!(params["initialAgent"]["specialist"], "implementor");
        assert_eq!(params["initialAgent"]["metadata"]["isInitialAgent"], true);
        assert_ne!(
            params["idempotencyKey"], second["proposal"]["payload"]["params"]["idempotencyKey"],
            "each proposal invocation must represent a new creation intent"
        );
        assert!(params["idempotencyKey"]
            .as_str()
            .unwrap()
            .starts_with("sibling-workspace-"));
        let resource = &first_response["result"]["content"][1]["resource"];
        assert_eq!(resource["mimeType"], "application/vnd.intent.proposal+json");
        let proposal_text = resource["text"].as_str().unwrap();
        let request_for_apply =
            || serde_json::from_str::<Value>(proposal_text).unwrap()["payload"]["params"].clone();
        let first_apply = request_for_apply();
        let retry_apply = request_for_apply();
        assert_eq!(first_apply, retry_apply);
        assert_eq!(first_apply["idempotencyKey"], params["idempotencyKey"]);
    }

    #[tokio::test]
    async fn propose_sibling_preserves_valid_and_invalid_named_refs_without_fallback() {
        let repo = git_repo();
        let srv = server_with_repo("amber-forest", repo.path());
        let valid = tool_json(
            &call_workspace_api(
                &srv,
                "return await ws.workspace.proposeSibling({ title: 'Valid', initialPrompt: 'Do it.', baseRef: 'feature/ready' });",
            )
            .await,
        );
        assert_eq!(
            valid["proposal"]["preview"]["workspaceCreate"]["branch"],
            "feature/ready"
        );
        assert_eq!(
            valid["proposal"]["payload"]["params"]["baseRef"],
            "feature/ready"
        );
        assert!(valid["proposal"]["preview"].get("warnings").is_none());

        for named_ref in ["missing", "stale/deleted"] {
            let code = format!(
                "return await ws.workspace.proposeSibling({{ title: 'Invalid', initialPrompt: 'Do it.', baseRef: '{named_ref}' }});"
            );
            let invalid = tool_json(&call_workspace_api(&srv, &code).await);
            assert_eq!(
                invalid["proposal"]["preview"]["workspaceCreate"]["branch"],
                named_ref
            );
            assert_eq!(
                invalid["proposal"]["payload"]["params"]["baseRef"],
                named_ref
            );
            assert!(invalid["proposal"]["preview"]["warnings"][0]
                .as_str()
                .unwrap()
                .contains(named_ref));
        }
    }

    #[tokio::test]
    async fn propose_sibling_rejects_unknown_fields_bad_types_and_missing_repository() {
        let repo = git_repo();
        let srv = server_with_repo("amber-forest", repo.path());
        for code in [
            "return await ws.workspace.proposeSibling({ title: 'X', initialPrompt: 'Y', repositoryPath: '/tmp/other' });",
            "return await ws.workspace.proposeSibling({ title: 'X', initialPrompt: 1 });",
            "return await ws.workspace.proposeSibling({ title: 'X' });",
        ] {
            let response = call_workspace_api(&srv, code).await;
            assert_eq!(response["result"]["isError"], true, "{response}");
        }

        let missing = server("amber-forest", None);
        let response = call_workspace_api(
            &missing,
            "return await ws.workspace.proposeSibling({ title: 'X', initialPrompt: 'Y' });",
        )
        .await;
        assert_eq!(response["result"]["isError"], true);
        assert!(tool_text(&response).contains("no usable repository"));
    }

    #[tokio::test]
    async fn propose_sibling_is_hidden_and_raw_dispatch_denied_for_sub_agents() {
        let top = server("amber-forest", None);
        let top_list = top
            .handle_message(&json!({ "jsonrpc": "2.0", "id": 1, "method": "tools/list" }))
            .await
            .unwrap();
        assert!(top_list["result"]["tools"][0]["description"]
            .as_str()
            .unwrap()
            .contains("ws.workspace.proposeSibling("));
        let top_type = call_workspace_api(&top, "return typeof ws.workspace.proposeSibling;").await;
        assert_eq!(tool_text(&top_type), "\"function\"");

        let sub = server("amber-forest", None).with_sub_agent(true);
        let sub_list = sub
            .handle_message(&json!({ "jsonrpc": "2.0", "id": 1, "method": "tools/list" }))
            .await
            .unwrap();
        assert!(!sub_list["result"]["tools"][0]["description"]
            .as_str()
            .unwrap()
            .contains("ws.workspace.proposeSibling("));
        let sub_type = call_workspace_api(&sub, "return typeof ws.workspace.proposeSibling;").await;
        assert_eq!(tool_text(&sub_type), "\"undefined\"");
        let raw = call_workspace_api(
            &sub,
            "return await host({ method: 'workspace.proposeSibling', args: { title: 'X', initialPrompt: 'Y' } });",
        )
        .await;
        assert_eq!(raw["result"]["isError"], true);
        assert!(tool_text(&raw).contains("only available to foreground top-level agents"));
    }

    #[tokio::test]
    async fn workspace_api_tool_is_registered_in_tools_list() {
        let srv = server("amber-forest", None);
        let resp = srv
            .handle_message(&json!({ "jsonrpc": "2.0", "id": 1, "method": "tools/list" }))
            .await
            .unwrap();
        let tools = resp["result"]["tools"].as_array().unwrap();
        let tool = tools
            .iter()
            .find(|t| t["name"] == "workspace_api")
            .expect("workspace_api must appear in tools/list");
        // Both `code` and `summary` are required per the reference schema.
        let required = tool["inputSchema"]["required"].as_array().unwrap();
        assert!(required.iter().any(|v| v == "code"));
        assert!(required.iter().any(|v| v == "summary"));
    }

    #[tokio::test]
    async fn workspace_api_returns_plain_expression_as_pretty_json() {
        // Sanity: an ordinary `return 1 + 1;` round-trips as JSON text.
        let srv = server("amber-forest", None);
        let resp = call_workspace_api(&srv, "return 1 + 1;").await;
        assert_eq!(resp["result"]["isError"], json!(false));
        assert_eq!(tool_text(&resp), "2");
    }

    #[tokio::test]
    async fn workspace_api_undefined_return_reports_no_return_value() {
        // Reference parity: user code with no return prints the sentinel
        // string instead of `null` / `undefined`.
        let srv = server("amber-forest", None);
        let resp = call_workspace_api(&srv, "/* nothing */").await;
        assert_eq!(resp["result"]["isError"], json!(false));
        assert_eq!(tool_text(&resp), "(no return value)");
    }

    #[tokio::test]
    async fn workspace_api_explicit_null_return_prints_json_null() {
        // Reference parity: `return null;` prints JSON `null`, not the
        // "(no return value)" sentinel reserved for `undefined`.
        let srv = server("amber-forest", None);
        let resp = call_workspace_api(&srv, "return null;").await;
        assert_eq!(resp["result"]["isError"], json!(false));
        assert_eq!(tool_text(&resp), "null");
    }

    #[tokio::test]
    async fn workspace_api_info_binding_returns_real_workspace_id_and_path() {
        // The `ws.workspace.info()` binding is the WSAPI-2 proof point:
        // it must return the real per-call workspace id + path threaded
        // through the dispatch.
        let srv = server("amber-forest", Some("/tmp/amber-forest"));
        let resp = call_workspace_api(&srv, "return await ws.workspace.info();").await;
        assert_eq!(resp["result"]["isError"], json!(false));
        let body: Value = serde_json::from_str(tool_text(&resp)).unwrap();
        assert_eq!(body["id"], json!("amber-forest"));
        assert_eq!(body["path"], json!("/tmp/amber-forest"));
    }

    #[tokio::test]
    async fn workspace_api_info_binding_returns_null_path_when_workspace_has_none() {
        // `Workspace.path` is optional (§9.1); a workspace without a
        // resolved on-disk path returns `path: null` rather than erroring.
        let srv = server("amber-forest", None);
        let resp = call_workspace_api(&srv, "return await ws.workspace.info();").await;
        assert_eq!(resp["result"]["isError"], json!(false));
        let body: Value = serde_json::from_str(tool_text(&resp)).unwrap();
        assert_eq!(body["id"], json!("amber-forest"));
        assert_eq!(body["path"], Value::Null);
    }

    #[tokio::test]
    async fn workspace_api_info_binding_falls_back_to_repository_path() {
        // Direct-checkout workspaces (`skipIsolation`) may persist only
        // `repositoryPath` (monorepo#3778): `info()` must resolve it as the
        // final fallback instead of returning `path: null`.
        let api = WorkspaceInfoMockApi::new("amber-forest", None);
        api.ws.lock().unwrap().repository_path = Some("/tmp/amber-repo".to_string());
        let srv = WorkspaceMcpServer::new(api, WorkspaceId::from_string("amber-forest"));
        let resp = call_workspace_api(&srv, "return await ws.workspace.info();").await;
        assert_eq!(resp["result"]["isError"], json!(false));
        let body: Value = serde_json::from_str(tool_text(&resp)).unwrap();
        assert_eq!(body["id"], json!("amber-forest"));
        assert_eq!(body["path"], json!("/tmp/amber-repo"));
    }

    #[tokio::test]
    async fn workspace_api_syntax_error_returns_readable_is_error_result() {
        let srv = server("amber-forest", None);
        let resp = call_workspace_api(&srv, "return (").await;
        assert_eq!(resp["result"]["isError"], json!(true));
        let text = tool_text(&resp);
        assert!(
            text.contains("SyntaxError"),
            "expected SyntaxError message, got: {text}"
        );
    }

    #[tokio::test]
    async fn workspace_api_thrown_error_returns_readable_is_error_result() {
        let srv = server("amber-forest", None);
        let resp = call_workspace_api(&srv, "throw new Error('boom');").await;
        assert_eq!(resp["result"]["isError"], json!(true));
        let text = tool_text(&resp);
        assert!(
            text.contains("boom"),
            "expected thrown message in output, got: {text}"
        );
    }

    #[tokio::test]
    async fn workspace_api_missing_code_returns_is_error_result() {
        // `code` is declared required by the input schema; the dispatch also
        // guards defensively so an omitted argument surfaces as a friendly
        // tool-result error instead of a protocol error.
        let srv = server("amber-forest", None);
        let resp = srv
            .handle_message(&json!({
                "jsonrpc": "2.0", "id": 1, "method": "tools/call",
                "params": { "name": "workspace_api", "arguments": { "summary": "" } }
            }))
            .await
            .unwrap();
        assert_eq!(resp["result"]["isError"], json!(true));
        let text = tool_text(&resp);
        assert!(
            text.contains("`code` is required"),
            "expected friendly missing-code message, got: {text}"
        );
    }

    #[tokio::test]
    async fn workspace_api_hot_loop_hits_timeout() {
        // Compressed tool budget so the timeout path runs in milliseconds
        // instead of the 30s production default. Fail-safe: if the engine's
        // interrupt handler regresses, the test must bail out well before the
        // tool timeout instead of hanging.
        let srv =
            server("amber-forest", None).with_workspace_api_timeout(Duration::from_millis(250));
        let resp = tokio::time::timeout(
            Duration::from_secs(5),
            call_workspace_api(&srv, "while (true) {}"),
        )
        .await
        .expect("workspace_api hot loop must return before the test-level fail-safe");
        assert_eq!(resp["result"]["isError"], json!(true));
        let text = tool_text(&resp);
        assert!(
            text.contains("timed out"),
            "expected timeout message, got: {text}"
        );
    }

    #[tokio::test]
    async fn tools_call_unknown_name_is_rejected_as_tool_not_found() {
        // Regression for the WSAPI-8 singleton assumption: a tool name that
        // is not `workspace_api` must be rejected as `Tool not found` and
        // must NOT be silently mis-dispatched to `dispatch_workspace_api`.
        let srv = server("amber-forest", None);
        let resp = srv
            .handle_message(&json!({
                "jsonrpc": "2.0", "id": 1, "method": "tools/call",
                "params": {
                    "name": "not_a_real_tool",
                    "arguments": { "code": "return 1;", "summary": "unit test" }
                }
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
    async fn tools_call_denylisted_but_unregistered_name_reports_tool_not_found() {
        // A denylist may carry legacy discrete tool names or agent-provider
        // built-ins that were never registered in the WSAPI-8 singleton
        // registry. The registration check runs before the denylist check,
        // so those names surface the accurate "Tool not found" MCP error
        // instead of the misleading "Tool not available".
        let api = WorkspaceInfoMockApi::new("amber-forest", None);
        let srv = WorkspaceMcpServer::new(api, WorkspaceId::from_string("amber-forest"))
            .with_denylist(["get_note"]);
        assert!(srv.is_denied("get_note"));
        let resp = srv
            .handle_message(&json!({
                "jsonrpc": "2.0", "id": 1, "method": "tools/call",
                "params": { "name": "get_note", "arguments": { "noteId": "n" } }
            }))
            .await
            .unwrap();
        assert_eq!(resp["error"]["code"], json!(-32602));
        let msg = resp["error"]["message"].as_str().unwrap();
        assert!(
            msg.contains("Tool not found"),
            "expected `Tool not found` for denylisted-but-unregistered name, got: {msg}"
        );
        assert!(
            !msg.contains("Tool not available"),
            "denylisted-but-unregistered name must not surface `Tool not available`, got: {msg}"
        );
    }

    #[tokio::test]
    async fn workspace_api_missing_summary_returns_invalid_params() {
        // The schema declares `summary` required alongside `code`; a call
        // without `summary` must fail with a clear MCP error before the JS
        // engine is invoked.
        let srv = server("amber-forest", None);
        let resp = srv
            .handle_message(&json!({
                "jsonrpc": "2.0", "id": 1, "method": "tools/call",
                "params": {
                    "name": "workspace_api",
                    "arguments": { "code": "return 1;" }
                }
            }))
            .await
            .unwrap();
        assert_eq!(resp["error"]["code"], json!(-32602));
        assert!(resp["error"]["message"]
            .as_str()
            .unwrap()
            .contains("`summary` is required"));
    }

    #[tokio::test]
    async fn workspace_api_non_string_summary_returns_invalid_params() {
        // Reject non-string `summary` values with the same MCP error rather
        // than coercing or silently ignoring them.
        let srv = server("amber-forest", None);
        let resp = srv
            .handle_message(&json!({
                "jsonrpc": "2.0", "id": 1, "method": "tools/call",
                "params": {
                    "name": "workspace_api",
                    "arguments": { "code": "return 1;", "summary": 42 }
                }
            }))
            .await
            .unwrap();
        assert_eq!(resp["error"]["code"], json!(-32602));
        assert!(resp["error"]["message"]
            .as_str()
            .unwrap()
            .contains("`summary` is required and must be a string"));
    }

    // ---- [agentFeatures] surface gating (description / prelude / dispatch) --

    fn no_hooks_features() -> intent_core::settings_file::AgentFeaturesSettings {
        intent_core::settings_file::AgentFeaturesSettings {
            background_hooks: false,
            ..Default::default()
        }
    }

    #[tokio::test]
    async fn tools_list_description_prunes_disabled_feature() {
        // Layer (a): a bridge created with `backgroundHooks = false` must not
        // advertise `ws.hook.*` in its `workspace_api` description, while an
        // all-defaults bridge advertises it verbatim.
        let srv = server("amber-forest", None).with_agent_features(no_hooks_features());
        let resp = srv
            .handle_message(&json!({ "jsonrpc": "2.0", "id": 1, "method": "tools/list" }))
            .await
            .unwrap();
        let desc = resp["result"]["tools"][0]["description"].as_str().unwrap();
        assert!(
            !desc.contains("ws.hook."),
            "pruned description still advertises ws.hook.*"
        );
        assert!(
            desc.contains("ws.note.read("),
            "un-gated surface must stay advertised"
        );

        // Byte-identity needs every gate open (the defaults plus the opt-in
        // `peerAgents` toggle, the default-off gate).
        let all_on_srv = server("amber-forest", None).with_agent_features(
            intent_core::settings_file::AgentFeaturesSettings {
                peer_agents: true,
                ..Default::default()
            },
        );
        let resp = all_on_srv
            .handle_message(&json!({ "jsonrpc": "2.0", "id": 1, "method": "tools/list" }))
            .await
            .unwrap();
        let desc = resp["result"]["tools"][0]["description"].as_str().unwrap();
        assert_eq!(
            desc,
            crate::mcp_server::WORKSPACE_API_DESCRIPTION,
            "every-gate-open tools/list description must be byte-identical to the static const"
        );
    }

    #[tokio::test]
    async fn tools_list_description_advertises_specialist_model_options() {
        // A bridge wired with specialist `modelOptions` advertises them in
        // the delegate docs; the wiring composes with feature pruning on the
        // same server. (The no-options byte-parity case is covered by
        // `tools_list_description_prunes_disabled_feature` above.)
        use crate::mcp_server::{SpecialistModelOption, SpecialistModelOptions};
        let srv = server("amber-forest", None)
            .with_agent_features(no_hooks_features())
            .with_specialist_model_options(vec![SpecialistModelOptions {
                specialist: "implementor".to_string(),
                default_model: Some("auggie:claude-opus-5".to_string()),
                options: vec![SpecialistModelOption {
                    model: "opencode:kimi-k3".to_string(),
                    hint: "cheap".to_string(),
                    reasoning_effort: String::new(),
                }],
            }]);
        let resp = srv
            .handle_message(&json!({ "jsonrpc": "2.0", "id": 1, "method": "tools/list" }))
            .await
            .unwrap();
        let desc = resp["result"]["tools"][0]["description"].as_str().unwrap();
        assert!(
            desc.contains(
                "implementor: default `auggie:claude-opus-5`, `opencode:kimi-k3` (cheap)"
            ),
            "delegate docs must list the specialist's default model and options"
        );
        assert!(
            !desc.contains("ws.hook."),
            "feature pruning must still apply alongside the options injection"
        );
    }

    #[tokio::test]
    async fn tools_list_compact_flag_serves_compact_description() {
        // A bridge flagged for a truncating provider serves the compact
        // `workspace_api` description (whole text under the ~2k cutoff, no
        // API sections), while `full_workspace_api_description()` returns
        // exactly what the unflagged bridge's tools/list serves, and
        // `condensed_workspace_api_description()` — the text the spawn path
        // appends to the system prompt — matches the tools module rendering
        // for the same gating.
        let compact_srv = server("amber-forest", None)
            .with_agent_features(no_hooks_features())
            .with_compact_tool_descriptions(true);
        let resp = compact_srv
            .handle_message(&json!({ "jsonrpc": "2.0", "id": 1, "method": "tools/list" }))
            .await
            .unwrap();
        let compact = resp["result"]["tools"][0]["description"].as_str().unwrap();
        assert!(
            compact.len() <= 2000,
            "compact description is {} bytes, over the ~2k truncation cutoff",
            compact.len()
        );
        assert!(
            !compact.contains("\nAPI:"),
            "compact description must not carry the API sections"
        );
        assert!(
            !compact.contains("ws.hook."),
            "feature pruning must still apply to the compact description"
        );
        assert!(
            compact.contains("system-prompt \"Workspace API Reference\""),
            "compact description must point at the system-prompt reference"
        );

        let full_srv = server("amber-forest", None).with_agent_features(no_hooks_features());
        let resp = full_srv
            .handle_message(&json!({ "jsonrpc": "2.0", "id": 1, "method": "tools/list" }))
            .await
            .unwrap();
        let full = resp["result"]["tools"][0]["description"].as_str().unwrap();
        assert_eq!(
            compact_srv.full_workspace_api_description(),
            full,
            "full_workspace_api_description must match the unflagged tools/list text"
        );

        // The bridge-level condensed rendering (what the spawn path appends
        // to the system prompt) applies this bridge's gating: derived from
        // the same assembly, so the pruned namespace stays absent and the
        // text is well under the full reference's size.
        let condensed = compact_srv.condensed_workspace_api_description();
        assert!(
            !condensed.contains("ws.hook."),
            "feature pruning must apply to the condensed system-prompt reference"
        );
        assert!(
            condensed.len() < full.len(),
            "condensed reference must be smaller than the full text"
        );
        assert!(
            condensed.contains("\nAPI:"),
            "condensed reference must keep the API section"
        );
    }

    #[tokio::test]
    async fn disabled_namespace_is_absent_from_prelude() {
        // Layer (b): with the toggle off, `ws.hook` is not installed, so
        // touching it fails with the clear namespace-missing TypeError.
        let srv = server("amber-forest", None).with_agent_features(no_hooks_features());
        let resp = call_workspace_api(&srv, "return await ws.hook.list();").await;
        assert_eq!(resp["result"]["isError"], json!(true));
        let text = tool_text(&resp);
        assert!(
            text.contains("undefined"),
            "expected undefined-namespace error, got: {text}"
        );
    }

    #[tokio::test]
    async fn disabled_method_is_denied_at_dispatch() {
        // Layer (c): a raw `host({...})` frame cannot bypass the pruned
        // prelude — dispatch denies it with the explicit settings error.
        let srv = server("amber-forest", None).with_agent_features(no_hooks_features());
        let resp = call_workspace_api(
            &srv,
            "return await host({ method: 'hook.list', args: {} });",
        )
        .await;
        assert_eq!(resp["result"]["isError"], json!(true));
        let text = tool_text(&resp);
        assert!(
            text.contains("disabled in settings") && text.contains("agentFeatures.backgroundHooks"),
            "expected explicit disabled-in-settings denial, got: {text}"
        );
    }

    #[tokio::test]
    async fn enabled_features_dispatch_unaffected_by_other_toggles() {
        // Disabling hooks must not disturb un-gated namespaces on the same
        // bridge — `ws.workspace.info()` still round-trips.
        let srv = server("amber-forest", Some("/tmp/amber-forest"))
            .with_agent_features(no_hooks_features());
        let resp = call_workspace_api(&srv, "return await ws.workspace.info();").await;
        assert_eq!(resp["result"]["isError"], json!(false));
        let body: Value = serde_json::from_str(tool_text(&resp)).unwrap();
        assert_eq!(body["id"], json!("amber-forest"));
    }

    #[tokio::test]
    async fn structured_questions_off_denies_question_ask() {
        let srv = server("amber-forest", None).with_agent_features(
            intent_core::settings_file::AgentFeaturesSettings {
                structured_questions: false,
                ..Default::default()
            },
        );
        // Prelude: ws.app.question is not installed.
        let resp = call_workspace_api(
            &srv,
            "return await ws.app.question.ask({ question: 'q', header: 'h', options: [{label:'a'},{label:'b'}] });",
        )
        .await;
        assert_eq!(resp["result"]["isError"], json!(true));
        // Dispatch: the raw frame is denied with the settings error.
        let resp = call_workspace_api(
            &srv,
            "return await host({ method: 'app.question.ask', args: {} });",
        )
        .await;
        assert_eq!(resp["result"]["isError"], json!(true));
        let text = tool_text(&resp);
        assert!(
            text.contains("disabled in settings")
                && text.contains("agentFeatures.structuredQuestions"),
            "expected explicit disabled-in-settings denial, got: {text}"
        );
    }

    #[tokio::test]
    async fn attention_requests_off_denies_blocker_and_discussion() {
        let srv = server("amber-forest", None).with_agent_features(
            intent_core::settings_file::AgentFeaturesSettings {
                attention_requests: false,
                ..Default::default()
            },
        );
        // Prelude: the two attention-request installers are not present on
        // `ws.agent`, while the rest of the namespace survives.
        let resp = call_workspace_api(
            &srv,
            "return { rb: typeof ws.agent.reportBlocker, rd: typeof ws.agent.requestDiscussion, rtp: typeof ws.agent.reportToParent };",
        )
        .await;
        assert_eq!(resp["result"]["isError"], json!(false));
        let body: Value = serde_json::from_str(tool_text(&resp)).unwrap();
        assert_eq!(body["rb"], json!("undefined"));
        assert_eq!(body["rd"], json!("undefined"));
        assert_eq!(body["rtp"], json!("function"));
        // Dispatch: the raw frames are denied with the settings error.
        for method in ["agent.reportBlocker", "agent.requestDiscussion"] {
            let resp = call_workspace_api(
                &srv,
                &format!("return await host({{ method: '{method}', args: {{ reason: 'r' }} }});"),
            )
            .await;
            assert_eq!(resp["result"]["isError"], json!(true));
            let text = tool_text(&resp);
            assert!(
                text.contains("disabled in settings")
                    && text.contains("agentFeatures.attentionRequests"),
                "expected explicit disabled-in-settings denial for {method}, got: {text}"
            );
        }
    }

    #[tokio::test]
    async fn peer_agents_off_denies_retire() {
        // `peerAgents` defaults OFF (the one opt-in toggle), so the default
        // bridge prunes/denies `ws.agent.retire` on all three layers.
        let srv = server("amber-forest", None)
            .with_agent_features(intent_core::settings_file::AgentFeaturesSettings::default());
        // Layer (a): the description does not advertise it.
        let resp = srv
            .handle_message(&json!({ "jsonrpc": "2.0", "id": 1, "method": "tools/list" }))
            .await
            .unwrap();
        let desc = resp["result"]["tools"][0]["description"].as_str().unwrap();
        assert!(
            !desc.contains("ws.agent.retire"),
            "default description must not advertise ws.agent.retire"
        );
        // Layer (b): the installer is not in the prelude; sibling ws.agent.*
        // methods survive.
        let resp = call_workspace_api(
            &srv,
            "return { r: typeof ws.agent.retire, rb: typeof ws.agent.reportBlocker };",
        )
        .await;
        assert_eq!(resp["result"]["isError"], json!(false));
        let body: Value = serde_json::from_str(tool_text(&resp)).unwrap();
        assert_eq!(body["r"], json!("undefined"));
        assert_eq!(body["rb"], json!("function"));
        // Layer (c): the raw frame is denied with the settings error.
        let resp = call_workspace_api(
            &srv,
            "return await host({ method: 'agent.retire', args: {} });",
        )
        .await;
        assert_eq!(resp["result"]["isError"], json!(true));
        let text = tool_text(&resp);
        assert!(
            text.contains("disabled in settings") && text.contains("agentFeatures.peerAgents"),
            "expected explicit disabled-in-settings denial, got: {text}"
        );
    }

    #[tokio::test]
    async fn peer_agents_on_installs_and_advertises_retire() {
        // Opting in installs the binding and advertises the doc line.
        let srv = server("amber-forest", None).with_agent_features(
            intent_core::settings_file::AgentFeaturesSettings {
                peer_agents: true,
                ..Default::default()
            },
        );
        let resp = srv
            .handle_message(&json!({ "jsonrpc": "2.0", "id": 1, "method": "tools/list" }))
            .await
            .unwrap();
        let desc = resp["result"]["tools"][0]["description"].as_str().unwrap();
        assert!(desc.contains("ws.agent.retire(reason?)"));
        let resp = call_workspace_api(&srv, "return typeof ws.agent.retire;").await;
        assert_eq!(resp["result"]["isError"], json!(false));
        assert_eq!(tool_text(&resp), "\"function\"");
    }

    // ---- sub-agent question gating (description / prelude / dispatch) ------

    #[tokio::test]
    async fn sub_agent_description_prunes_question_docs() {
        // Layer (a): a sub-agent bridge must not advertise ws.app.question.*
        // in its description (index entry and doc line both pruned), while
        // the rest of the surface stays advertised.
        let srv = server("amber-forest", None).with_sub_agent(true);
        let resp = srv
            .handle_message(&json!({ "jsonrpc": "2.0", "id": 1, "method": "tools/list" }))
            .await
            .unwrap();
        let desc = resp["result"]["tools"][0]["description"].as_str().unwrap();
        assert!(
            !desc.contains("ws.app.question."),
            "sub-agent description still advertises ws.app.question.*"
        );
        assert!(
            desc.contains("ws.note.read(") && desc.contains("ws.agent.requestDiscussion("),
            "un-gated surface must stay advertised"
        );

        // A top-level bridge with every gate open (the defaults plus the
        // opt-in `peerAgents` toggle) stays byte-identical to the static
        // const.
        let top = server("amber-forest", None)
            .with_sub_agent(false)
            .with_agent_features(intent_core::settings_file::AgentFeaturesSettings {
                peer_agents: true,
                ..Default::default()
            });
        let resp = top
            .handle_message(&json!({ "jsonrpc": "2.0", "id": 1, "method": "tools/list" }))
            .await
            .unwrap();
        let desc = resp["result"]["tools"][0]["description"].as_str().unwrap();
        assert_eq!(
            desc,
            crate::mcp_server::WORKSPACE_API_DESCRIPTION,
            "top-level tools/list description must be byte-identical to the static const"
        );
    }

    #[tokio::test]
    async fn sub_agent_prelude_omits_question_installer() {
        // Layer (b): ws.app.question is not installed, so touching it fails
        // with the clear namespace-missing TypeError; the rest of ws.app.*
        // and the attention-request bindings stay installed.
        let srv = server("amber-forest", None).with_sub_agent(true);
        let resp = call_workspace_api(
            &srv,
            "return { q: typeof ws.app.question, w: typeof ws.app.workspaces, rd: typeof ws.agent.requestDiscussion };",
        )
        .await;
        assert_eq!(resp["result"]["isError"], json!(false));
        let body: Value = serde_json::from_str(tool_text(&resp)).unwrap();
        assert_eq!(body["q"], json!("undefined"));
        assert_eq!(body["w"], json!("object"));
        assert_eq!(body["rd"], json!("function"));
    }

    #[tokio::test]
    async fn sub_agent_dispatch_denies_question_ask_with_redirect() {
        // Layer (c): a raw `host({...})` frame cannot bypass the pruned
        // prelude — dispatch denies it with the top-level-only redirect
        // error (NOT the misleading "disabled in settings").
        let srv = server("amber-forest", None).with_sub_agent(true);
        let resp = call_workspace_api(
            &srv,
            "return await host({ method: 'app.question.ask', args: { question: { header: 'h', question: 'q', options: [{label:'a'},{label:'b'}] } } });",
        )
        .await;
        assert_eq!(resp["result"]["isError"], json!(true));
        let text = tool_text(&resp);
        assert!(
            text.contains("only available to top-level agents")
                && text.contains("ws.agent.requestDiscussion")
                && text.contains("ws.agent.reportToParent"),
            "expected top-level-only redirect denial, got: {text}"
        );
        assert!(
            !text.contains("disabled in settings"),
            "sub-agent denial must not masquerade as a settings gate: {text}"
        );
    }

    #[tokio::test]
    async fn sub_agent_gate_leaves_other_namespaces_alone() {
        // The sub-agent flag prunes ONLY ws.app.question.* — un-gated
        // namespaces still dispatch on the same bridge.
        let srv = server("amber-forest", Some("/tmp/amber-forest")).with_sub_agent(true);
        let resp = call_workspace_api(&srv, "return await ws.workspace.info();").await;
        assert_eq!(resp["result"]["isError"], json!(false));
        let body: Value = serde_json::from_str(tool_text(&resp)).unwrap();
        assert_eq!(body["id"], json!("amber-forest"));
    }
}

/// WSAPI-3 per-namespace bindings: `ws.note.*`, `ws.task.*`, `ws.comment.*`,
/// `ws.primitive.*`. Each namespace is exercised through the real JS engine
/// (no engine mocking) — the tool call round-trips a `workspace_api` request
/// against a fake [`WorkspaceApi`] that only stubs the trait methods the
/// bindings touch, so a happy-path and one error-path per namespace prove
/// the JS→Rust dispatch, argument peel, and result serialization. A
/// `Promise.all` batching test proves multiple bindings can share one
/// `workspace_api` invocation.
#[cfg(test)]
mod wsapi3_bindings_tests {
    use std::sync::{Arc, Mutex};

    use intent_core::{
        AgentId, BoxFuture, CommentAddResult, CommentDeleteResult, CommentGetThreadResult,
        CommentListResult, CommentLocation, CommentRespondResult, CommentRespondThread,
        CommentType, CommentWire, Error, Note, NoteAddInput, NoteAddResult, NoteCreate,
        NoteCreateResult, NoteDeleteResult, NoteEditInput, NoteEditLinesInput, NoteEditLinesResult,
        NoteEditResult, NoteId, NoteMetadata, NoteSetContentResult, NoteTaskRow,
        NoteUpdateMetadataResult, ReadAssetResult, Result, TaskAssignAgentResult,
        TaskConvertBlocksResult, TaskCreatePrerequisiteResult, TaskGetMyTaskResult,
        TaskMarkAsTaskResult, TaskMetadata, TaskStatus, TaskUpdateNoteStatusResult,
        TaskUpdateResult, TaskUpdateStatusResult, WorkspaceApi, WorkspaceId,
    };
    use serde_json::{json, Value};

    use crate::WorkspaceMcpServer;

    // Type aliases keep clippy's `type_complexity` lint quiet for the many
    // per-method argument-tuple recorders below.
    /// `(title, content, tags, idempotency_key)`.
    type CreateNoteCall = (String, Option<String>, Option<Vec<String>>, Option<String>);
    type AddCall = (String, String, Option<String>, Option<String>);
    type EditLinesCall = (String, i64, i64, String);
    type UpdateMetadataCall = (String, Option<String>, Option<Vec<String>>);
    type TaskUpdateCall = (String, i64, Option<String>, Option<String>, Option<String>);
    type MarkAsTaskCall = (String, String, Vec<String>, Option<String>);
    type CreatePrereqCall = (String, String, Option<String>, Option<String>);
    type CommentAddCall = (String, String, String, String);
    type CommentGetThreadCall = (String, Option<String>, Option<String>);
    type CommentRespondCall = (String, Option<String>, String);

    /// A fake `WorkspaceApi` that stubs just enough for the WSAPI-3 host
    /// frames to complete. Every method records the arguments it saw so a
    /// test can inspect the peel result; unknown noteIds surface `NotFound`
    /// so the error-path tests can prove JS-visible failures.
    #[derive(Default)]
    #[allow(clippy::struct_field_names)] // fields mirror the recorded method names
    struct FakeApi {
        get_note_calls: Mutex<Vec<String>>,
        create_note_calls: Mutex<Vec<CreateNoteCall>>,
        list_note_tasks_calls: Mutex<Vec<String>>,
        read_asset_calls: Mutex<Vec<String>>,
        set_content_calls: Mutex<Vec<(String, String, bool)>>,
        add_calls: Mutex<Vec<AddCall>>,
        edit_calls: Mutex<Vec<(String, String, String)>>,
        edit_lines_calls: Mutex<Vec<EditLinesCall>>,
        update_metadata_calls: Mutex<Vec<UpdateMetadataCall>>,
        delete_calls: Mutex<Vec<String>>,
        list_notes_calls: Mutex<u32>,
        task_update_status_calls: Mutex<Vec<(String, String, String)>>,
        task_update_note_status_calls: Mutex<Vec<(String, String)>>,
        task_update_calls: Mutex<Vec<TaskUpdateCall>>,
        get_my_task_calls: Mutex<Vec<String>>,
        mark_as_task_calls: Mutex<Vec<MarkAsTaskCall>>,
        convert_blocks_calls: Mutex<Vec<String>>,
        create_prereq_calls: Mutex<Vec<CreatePrereqCall>>,
        assign_agent_calls: Mutex<Vec<(String, String)>>,
        comment_add_calls: Mutex<Vec<CommentAddCall>>,
        comment_list_calls: Mutex<Vec<String>>,
        comment_get_thread_calls: Mutex<Vec<CommentGetThreadCall>>,
        comment_respond_calls: Mutex<Vec<CommentRespondCall>>,
        comment_delete_calls: Mutex<Vec<(String, String)>>,
        primitive_calls: Mutex<Vec<String>>,
    }

    fn stub_note(id: &str, ws: &WorkspaceId, task: Option<TaskMetadata>) -> Note {
        Note {
            id: NoteId::from_string(id),
            workspace_id: ws.clone(),
            title: format!("title-{id}"),
            content: "line one\nline two\n![alt](workspace-asset://ws/asset-1)".to_string(),
            content_type: intent_core::ContentType::default(),
            tags: vec!["a".to_string()],
            is_pinned: false,
            is_archived: false,
            is_default: false,
            parent_id: None,
            visibility: intent_core::NoteVisibility::default(),
            metadata: NoteMetadata { task },
            created_at: "2026-01-01T00:00:00Z".to_string(),
            rev: 1,
            updated_at: "2026-01-01T00:00:00Z".to_string(),
        }
    }

    impl WorkspaceApi for FakeApi {
        // Pin the `workspaceApi.*` output knobs to the legacy behavior (plain
        // pretty JSON, no size limit) so this fixture keeps asserting raw JSON
        // bodies; the TOON/limit paths are covered by
        // `workspace_api_output_limit_tests`.
        fn settings_get(&self, path: String) -> BoxFuture<'_, Result<Value>> {
            Box::pin(async move {
                let value = match path.as_str() {
                    "workspaceApi.toonOutput" => json!(false),
                    "workspaceApi.maxOutputChars" => json!(0),
                    _ => Value::Null,
                };
                Ok(json!({ "path": path, "value": value }))
            })
        }

        // ---- note.* ----
        fn get_note(
            &self,
            workspace_id: WorkspaceId,
            note_id: NoteId,
        ) -> BoxFuture<'_, Result<Note>> {
            let id = note_id.as_str().to_string();
            self.get_note_calls.lock().unwrap().push(id.clone());
            Box::pin(async move {
                if id == "missing" {
                    return Err(Error::NotFound(format!("Note not found: {id}")));
                }
                let mut task = None;
                if id == "task-1" {
                    task = Some(TaskMetadata {
                        status: TaskStatus::InProgress,
                        acceptance_criteria: vec!["A".to_string(), "B".to_string()],
                        estimated_effort: Some("1h".to_string()),
                        ..Default::default()
                    });
                }
                Ok(stub_note(&id, &workspace_id, task))
            })
        }

        fn list_notes<'a>(&'a self, _ws: &'a WorkspaceId) -> BoxFuture<'a, Result<Vec<Note>>> {
            *self.list_notes_calls.lock().unwrap() += 1;
            let ws = WorkspaceId::from_string("amber-forest");
            Box::pin(async move {
                Ok(vec![
                    Note {
                        id: NoteId::from_string("n-1"),
                        workspace_id: ws.clone(),
                        title: "First".to_string(),
                        content: String::new(),
                        content_type: intent_core::ContentType::default(),
                        tags: vec!["red".to_string()],
                        is_pinned: false,
                        is_archived: false,
                        is_default: false,
                        parent_id: None,
                        visibility: intent_core::NoteVisibility::default(),
                        metadata: intent_core::NoteMetadata::default(),
                        created_at: "2026-01-01T00:00:00Z".to_string(),
                        rev: 1,
                        updated_at: "2026-01-01T00:00:00Z".to_string(),
                    },
                    Note {
                        id: NoteId::from_string("n-2"),
                        workspace_id: ws,
                        title: "Second".to_string(),
                        content: String::new(),
                        content_type: intent_core::ContentType::default(),
                        tags: vec!["blue".to_string()],
                        is_pinned: false,
                        is_archived: false,
                        is_default: false,
                        parent_id: None,
                        visibility: intent_core::NoteVisibility::default(),
                        metadata: intent_core::NoteMetadata::default(),
                        created_at: "2026-01-01T00:00:00Z".to_string(),
                        rev: 1,
                        updated_at: "2026-01-01T00:00:00Z".to_string(),
                    },
                ])
            })
        }

        fn create_note(
            &self,
            workspace_id: WorkspaceId,
            input: NoteCreate,
            idempotency_key: Option<String>,
            _caller_agent_id: Option<AgentId>,
        ) -> BoxFuture<'_, Result<NoteCreateResult>> {
            self.create_note_calls.lock().unwrap().push((
                input.title.clone(),
                input.content.clone(),
                input.tags.clone(),
                idempotency_key,
            ));
            Box::pin(async move {
                Ok(NoteCreateResult {
                    note: Note {
                        id: NoteId::from_string("n-new"),
                        workspace_id,
                        title: input.title,
                        content: input.content.unwrap_or_default(),
                        content_type: intent_core::ContentType::default(),
                        tags: input.tags.unwrap_or_default(),
                        is_pinned: false,
                        is_archived: false,
                        is_default: false,
                        parent_id: None,
                        visibility: intent_core::NoteVisibility::default(),
                        metadata: intent_core::NoteMetadata::default(),
                        created_at: "2026-01-01T00:00:00Z".to_string(),
                        rev: 1,
                        updated_at: "2026-01-01T00:00:00Z".to_string(),
                    },
                    converted_count: 0,
                    created_task_note_ids: Vec::new(),
                    created_tasks: Vec::new(),
                    warnings: Vec::new(),
                })
            })
        }

        fn list_note_tasks(
            &self,
            _ws: WorkspaceId,
            note_id: NoteId,
        ) -> BoxFuture<'_, Result<Vec<NoteTaskRow>>> {
            self.list_note_tasks_calls
                .lock()
                .unwrap()
                .push(note_id.as_str().to_string());
            Box::pin(async move {
                Ok(vec![NoteTaskRow {
                    line_number: 3,
                    text: "do the thing".to_string(),
                    status: "todo".to_string(),
                    task_note_id: None,
                    linked_task_note_id: None,
                    depends_on: Vec::new(),
                    conflicts_with: Vec::new(),
                    unmet_depends_on: Vec::new(),
                }])
            })
        }

        fn read_asset(
            &self,
            _ws: WorkspaceId,
            asset: String,
        ) -> BoxFuture<'_, Result<ReadAssetResult>> {
            self.read_asset_calls.lock().unwrap().push(asset.clone());
            Box::pin(async move {
                Ok(ReadAssetResult {
                    asset_id: asset,
                    mime_type: "image/png".to_string(),
                    data: "AAAA".to_string(),
                    size_kb: 1,
                })
            })
        }

        fn set_note_content(
            &self,
            _ws: WorkspaceId,
            note_id: NoteId,
            content: String,
            confirm_replacement: bool,
            _expected_version: Option<i64>,
            _caller_agent_id: Option<AgentId>,
        ) -> BoxFuture<'_, Result<NoteSetContentResult>> {
            self.set_content_calls.lock().unwrap().push((
                note_id.as_str().to_string(),
                content.clone(),
                confirm_replacement,
            ));
            Box::pin(async move {
                Ok(NoteSetContentResult {
                    ok: true,
                    note_id,
                    title: "t".to_string(),
                    previous_title: None,
                    updated_at: "2026-01-01T00:00:00Z".to_string(),
                    old_content: None,
                    new_content: content,
                    converted_count: 0,
                    created_task_note_ids: Vec::new(),
                    created_tasks: Vec::new(),
                    warnings: Vec::new(),
                })
            })
        }

        fn add_to_note(
            &self,
            _ws: WorkspaceId,
            note_id: NoteId,
            input: NoteAddInput,
            _caller_agent_id: Option<AgentId>,
        ) -> BoxFuture<'_, Result<NoteAddResult>> {
            self.add_calls.lock().unwrap().push((
                note_id.as_str().to_string(),
                input.content.clone(),
                input.heading.clone(),
                input.position.clone(),
            ));
            Box::pin(async move {
                let n = input.content.len();
                Ok(NoteAddResult {
                    ok: true,
                    note_id,
                    added_length: n,
                    total_length: n,
                    position: input.position.unwrap_or_else(|| "at end".to_string()),
                    old_content: String::new(),
                    new_content: input.content,
                    converted_count: 0,
                    created_task_note_ids: Vec::new(),
                    created_tasks: Vec::new(),
                    warnings: Vec::new(),
                })
            })
        }

        fn edit_note(
            &self,
            _ws: WorkspaceId,
            note_id: NoteId,
            input: NoteEditInput,
            _caller_agent_id: Option<AgentId>,
        ) -> BoxFuture<'_, Result<NoteEditResult>> {
            self.edit_calls.lock().unwrap().push((
                note_id.as_str().to_string(),
                input.old.clone(),
                input.new.clone(),
            ));
            Box::pin(async move {
                Ok(NoteEditResult {
                    ok: true,
                    note_id,
                    old_text_length: input.old.len(),
                    new_text_length: input.new.len(),
                    match_position: 0,
                    old_content: input.old,
                    new_content: input.new,
                    converted_count: 0,
                    created_task_note_ids: Vec::new(),
                    created_tasks: Vec::new(),
                    warnings: Vec::new(),
                })
            })
        }

        fn edit_note_lines(
            &self,
            _ws: WorkspaceId,
            note_id: NoteId,
            input: NoteEditLinesInput,
            _caller_agent_id: Option<AgentId>,
        ) -> BoxFuture<'_, Result<NoteEditLinesResult>> {
            self.edit_lines_calls.lock().unwrap().push((
                note_id.as_str().to_string(),
                input.start,
                input.end,
                input.content.clone(),
            ));
            Box::pin(async move {
                Ok(NoteEditLinesResult {
                    ok: true,
                    note_id,
                    start_line: input.start,
                    end_line: input.end,
                    total_lines_before: 3,
                    total_lines_after: 3,
                    old_content: String::new(),
                    new_content: input.content,
                    converted_count: 0,
                    created_task_note_ids: Vec::new(),
                    created_tasks: Vec::new(),
                    warnings: Vec::new(),
                })
            })
        }

        fn update_note_metadata(
            &self,
            _ws: WorkspaceId,
            note_id: NoteId,
            title: Option<String>,
            tags: Option<Vec<String>>,
            _expected_version: Option<i64>,
            _caller_agent_id: Option<AgentId>,
        ) -> BoxFuture<'_, Result<NoteUpdateMetadataResult>> {
            self.update_metadata_calls.lock().unwrap().push((
                note_id.as_str().to_string(),
                title.clone(),
                tags.clone(),
            ));
            Box::pin(async move {
                Ok(NoteUpdateMetadataResult {
                    ok: true,
                    note_id,
                    title,
                    tags,
                    updated_at: Some("2026-01-01T00:00:00Z".to_string()),
                    skipped: None,
                    reason: None,
                })
            })
        }

        fn delete_note(
            &self,
            _ws: WorkspaceId,
            note_id: NoteId,
            _expected_version: Option<i64>,
        ) -> BoxFuture<'_, Result<NoteDeleteResult>> {
            self.delete_calls
                .lock()
                .unwrap()
                .push(note_id.as_str().to_string());
            Box::pin(async move {
                Ok(NoteDeleteResult {
                    ok: true,
                    note_id,
                    deleted: true,
                })
            })
        }

        // ---- task.* ----
        fn task_update_status(
            &self,
            _ws: WorkspaceId,
            note_id: NoteId,
            task_text: String,
            status: String,
        ) -> BoxFuture<'_, Result<TaskUpdateStatusResult>> {
            self.task_update_status_calls.lock().unwrap().push((
                note_id.as_str().to_string(),
                task_text.clone(),
                status.clone(),
            ));
            Box::pin(async move {
                Ok(TaskUpdateStatusResult {
                    ok: true,
                    note_id,
                    task_text,
                    status,
                })
            })
        }

        fn task_update_note_status(
            &self,
            workspace_id: WorkspaceId,
            note_id: NoteId,
            status: String,
            _expected_version: Option<i64>,
            _caller_agent_id: Option<AgentId>,
        ) -> BoxFuture<'_, Result<TaskUpdateNoteStatusResult>> {
            self.task_update_note_status_calls
                .lock()
                .unwrap()
                .push((note_id.as_str().to_string(), status.clone()));
            let note = stub_note(note_id.as_str(), &workspace_id, None);
            Box::pin(async move {
                let parsed: TaskStatus =
                    serde_json::from_value(Value::String(status)).unwrap_or_default();
                Ok(TaskUpdateNoteStatusResult {
                    ok: true,
                    note_id,
                    status: parsed,
                    note,
                })
            })
        }

        fn task_update(
            &self,
            _ws: WorkspaceId,
            note_id: NoteId,
            line: i64,
            text: Option<String>,
            status: Option<String>,
            expected: Option<String>,
        ) -> BoxFuture<'_, Result<TaskUpdateResult>> {
            self.task_update_calls.lock().unwrap().push((
                note_id.as_str().to_string(),
                line,
                text.clone(),
                status.clone(),
                expected,
            ));
            Box::pin(async move {
                Ok(TaskUpdateResult {
                    ok: true,
                    note_id,
                    line_number: line,
                    previous_text: "old".to_string(),
                    new_text: text.unwrap_or_else(|| "old".to_string()),
                    status: status.unwrap_or_else(|| "todo".to_string()),
                })
            })
        }

        fn get_my_task(
            &self,
            _workspace_id: WorkspaceId,
            task_note_id: NoteId,
        ) -> BoxFuture<'_, Result<TaskGetMyTaskResult>> {
            self.get_my_task_calls
                .lock()
                .unwrap()
                .push(task_note_id.as_str().to_string());
            Box::pin(async move {
                Ok(TaskGetMyTaskResult {
                    note_id: task_note_id,
                    title: "task title".to_string(),
                    content: "body".to_string(),
                    status: TaskStatus::InProgress,
                    task_metadata: TaskMetadata {
                        status: TaskStatus::InProgress,
                        ..Default::default()
                    },
                    parent_id: None,
                    subtasks: Vec::new(),
                    assigned_agents: Vec::new(),
                    rev: 1,
                    unmet_depends_on: Vec::new(),
                })
            })
        }

        fn mark_as_task(
            &self,
            _ws: WorkspaceId,
            note_id: NoteId,
            status: String,
            acceptance_criteria: Vec<String>,
            effort: Option<String>,
            _depends_on: Option<Vec<NoteId>>,
            _conflicts_with: Option<Vec<NoteId>>,
            _caller_agent_id: Option<AgentId>,
        ) -> BoxFuture<'_, Result<TaskMarkAsTaskResult>> {
            self.mark_as_task_calls.lock().unwrap().push((
                note_id.as_str().to_string(),
                status.clone(),
                acceptance_criteria,
                effort,
            ));
            let parsed: TaskStatus =
                serde_json::from_value(Value::String(status)).unwrap_or_default();
            Box::pin(async move {
                Ok(TaskMarkAsTaskResult {
                    ok: true,
                    note_id,
                    status: parsed,
                })
            })
        }

        fn convert_task_blocks(
            &self,
            _ws: WorkspaceId,
            note_id: NoteId,
            _caller_agent_id: Option<AgentId>,
        ) -> BoxFuture<'_, Result<TaskConvertBlocksResult>> {
            self.convert_blocks_calls
                .lock()
                .unwrap()
                .push(note_id.as_str().to_string());
            Box::pin(async move {
                Ok(TaskConvertBlocksResult {
                    ok: true,
                    converted_count: 0,
                    created_note_ids: Vec::new(),
                    created_tasks: Vec::new(),
                    warnings: Vec::new(),
                })
            })
        }

        fn create_prerequisite(
            &self,
            _ws: WorkspaceId,
            dependent_note_id: NoteId,
            title: String,
            content: Option<String>,
            status: Option<String>,
            _caller_agent_id: Option<AgentId>,
        ) -> BoxFuture<'_, Result<TaskCreatePrerequisiteResult>> {
            self.create_prereq_calls.lock().unwrap().push((
                dependent_note_id.as_str().to_string(),
                title.clone(),
                content,
                status,
            ));
            Box::pin(async move {
                Ok(TaskCreatePrerequisiteResult {
                    ok: true,
                    prerequisite_note_id: NoteId::from_string("prereq-1"),
                    dependent_note_id,
                    title,
                })
            })
        }

        fn assign_agent(
            &self,
            _ws: WorkspaceId,
            note_id: NoteId,
            agent_id: String,
            _force: Option<bool>,
        ) -> BoxFuture<'_, Result<TaskAssignAgentResult>> {
            self.assign_agent_calls
                .lock()
                .unwrap()
                .push((note_id.as_str().to_string(), agent_id.clone()));
            Box::pin(async move {
                Ok(TaskAssignAgentResult {
                    ok: true,
                    note_id,
                    agent_id: AgentId::from(agent_id.as_str()),
                })
            })
        }

        // ---- comment.* ----
        #[allow(clippy::too_many_arguments)]
        fn comment_add(
            &self,
            _ws: WorkspaceId,
            note_id: NoteId,
            search_context: String,
            comment_target: String,
            comment: String,
            _kind: Option<String>,
            _author: Option<String>,
            _author_type: Option<String>,
            _idempotency_key: Option<String>,
            _comment_id: Option<String>,
        ) -> BoxFuture<'_, Result<CommentAddResult>> {
            self.comment_add_calls.lock().unwrap().push((
                note_id.as_str().to_string(),
                search_context,
                comment_target.clone(),
                comment,
            ));
            Box::pin(async move {
                Ok(CommentAddResult {
                    success: true,
                    message: format!("Comment anchored to \"{comment_target}\""),
                    comment_id: "c-1".to_string(),
                    anchored: true,
                    note_rev: 1,
                    location: CommentLocation {
                        line: 1,
                        anchored_text: comment_target,
                    },
                })
            })
        }

        fn comment_list(
            &self,
            _ws: WorkspaceId,
            note_id: NoteId,
            _since: Option<String>,
            _author_type: Option<String>,
            _status: Option<String>,
            _include_comments: bool,
        ) -> BoxFuture<'_, Result<CommentListResult>> {
            self.comment_list_calls
                .lock()
                .unwrap()
                .push(note_id.as_str().to_string());
            Box::pin(async move {
                Ok(CommentListResult {
                    threads: Vec::new(),
                    total_threads: 0,
                    total_comments: 0,
                })
            })
        }

        fn comment_get_thread(
            &self,
            _ws: WorkspaceId,
            note_id: NoteId,
            thread_id: Option<String>,
            comment_id: Option<String>,
        ) -> BoxFuture<'_, Result<CommentGetThreadResult>> {
            self.comment_get_thread_calls.lock().unwrap().push((
                note_id.as_str().to_string(),
                thread_id.clone(),
                comment_id,
            ));
            Box::pin(async move {
                let tid = thread_id.unwrap_or_else(|| "t-1".to_string());
                Ok(CommentGetThreadResult {
                    thread_id: tid.clone(),
                    note_id,
                    root_comment: CommentWire {
                        id: "c-root".to_string(),
                        thread_id: tid,
                        note_id: None,
                        kind: CommentType::Comment,
                        content: "root".to_string(),
                        author: "Agent".to_string(),
                        author_type: intent_core::AuthorType::default(),
                        status: intent_core::CommentStatus::default(),
                        parent_id: None,
                        anchor: Option::default(),
                        anchor_text: None,
                        anchor_context: None,
                        suggestion_diff: None,
                        agent_id: None,
                        is_orphaned: None,
                        created_at: "2026-01-01T00:00:00Z".to_string(),
                        updated_at: "2026-01-01T00:00:00Z".to_string(),
                    },
                    replies: Vec::new(),
                    total_comments: 1,
                    status: "open".to_string(),
                })
            })
        }

        #[allow(clippy::too_many_arguments)]
        fn comment_respond(
            &self,
            _ws: WorkspaceId,
            note_id: NoteId,
            thread_id: Option<String>,
            _comment_id: Option<String>,
            comment: String,
            _kind: Option<String>,
            _author: Option<String>,
            _author_type: Option<String>,
            _suggestion_original: Option<String>,
            _suggestion_proposed: Option<String>,
        ) -> BoxFuture<'_, Result<CommentRespondResult>> {
            self.comment_respond_calls.lock().unwrap().push((
                note_id.as_str().to_string(),
                thread_id.clone(),
                comment.clone(),
            ));
            Box::pin(async move {
                let tid = thread_id.unwrap_or_else(|| "t-1".to_string());
                Ok(CommentRespondResult {
                    success: true,
                    message: "reply".to_string(),
                    comment: CommentWire {
                        id: "c-reply".to_string(),
                        thread_id: tid.clone(),
                        note_id: None,
                        kind: CommentType::Comment,
                        content: comment,
                        author: "Agent".to_string(),
                        author_type: intent_core::AuthorType::default(),
                        status: intent_core::CommentStatus::default(),
                        parent_id: None,
                        anchor: Option::default(),
                        anchor_text: None,
                        anchor_context: None,
                        suggestion_diff: None,
                        agent_id: None,
                        is_orphaned: None,
                        created_at: "2026-01-01T00:00:00Z".to_string(),
                        updated_at: "2026-01-01T00:00:00Z".to_string(),
                    },
                    thread: CommentRespondThread {
                        thread_id: tid,
                        total_comments: 2,
                    },
                })
            })
        }

        fn comment_delete(
            &self,
            _ws: WorkspaceId,
            note_id: NoteId,
            comment_id: String,
        ) -> BoxFuture<'_, Result<CommentDeleteResult>> {
            self.comment_delete_calls
                .lock()
                .unwrap()
                .push((note_id.as_str().to_string(), comment_id.clone()));
            Box::pin(async move {
                Ok(CommentDeleteResult {
                    success: true,
                    message: format!("Comment {comment_id} deleted"),
                })
            })
        }

        // ---- primitive.* ----
        fn primitive_add_reference(
            &self,
            _ws: WorkspaceId,
            note_id: NoteId,
            _semantic_id: String,
            _description: String,
            _snapshot: Option<String>,
        ) -> BoxFuture<'_, Result<Value>> {
            self.primitive_calls
                .lock()
                .unwrap()
                .push(format!("reference:{}", note_id.as_str()));
            Box::pin(async move {
                Ok(
                    json!({ "ok": true, "primitiveId": "p-1", "noteId": note_id.as_str(), "content": "" }),
                )
            })
        }

        fn primitive_add_cli(
            &self,
            _ws: WorkspaceId,
            note_id: NoteId,
            _command: String,
            _description: String,
            _working_directory: Option<String>,
        ) -> BoxFuture<'_, Result<Value>> {
            self.primitive_calls
                .lock()
                .unwrap()
                .push(format!("cli:{}", note_id.as_str()));
            Box::pin(async move {
                Ok(
                    json!({ "ok": true, "primitiveId": "p-2", "noteId": note_id.as_str(), "content": "" }),
                )
            })
        }

        fn primitive_add_patch(
            &self,
            _ws: WorkspaceId,
            note_id: NoteId,
            _file_path: String,
            _diff: String,
            _description: String,
        ) -> BoxFuture<'_, Result<Value>> {
            self.primitive_calls
                .lock()
                .unwrap()
                .push(format!("patch:{}", note_id.as_str()));
            Box::pin(async move {
                Ok(
                    json!({ "ok": true, "primitiveId": "p-3", "noteId": note_id.as_str(), "content": "" }),
                )
            })
        }

        fn primitive_add_agent_action(
            &self,
            _ws: WorkspaceId,
            note_id: NoteId,
            _agent_id: String,
            _goal: String,
            _description: String,
        ) -> BoxFuture<'_, Result<Value>> {
            self.primitive_calls
                .lock()
                .unwrap()
                .push(format!("agent_action:{}", note_id.as_str()));
            Box::pin(async move {
                Ok(
                    json!({ "ok": true, "primitiveId": "p-4", "noteId": note_id.as_str(), "content": "" }),
                )
            })
        }
    }

    fn server() -> (WorkspaceMcpServer, Arc<FakeApi>) {
        let api = Arc::new(FakeApi::default());
        let srv = WorkspaceMcpServer::new(api.clone(), WorkspaceId::from_string("amber-forest"));
        (srv, api)
    }

    async fn call(srv: &WorkspaceMcpServer, code: &str) -> Value {
        srv.handle_message(&json!({
            "jsonrpc": "2.0", "id": 1, "method": "tools/call",
            "params": {
                "name": "workspace_api",
                "arguments": { "code": code, "summary": "wsapi3 unit test" }
            }
        }))
        .await
        .expect("tools/call must produce a response")
    }

    fn body(resp: &Value) -> Value {
        let text = resp["result"]["content"][0]["text"].as_str().unwrap();
        serde_json::from_str(text).expect("workspace_api body must be JSON")
    }

    fn text(resp: &Value) -> String {
        resp["result"]["content"][0]["text"]
            .as_str()
            .unwrap()
            .to_string()
    }

    // ================================================================
    // note.*
    // ================================================================

    #[tokio::test]
    async fn note_read_happy_returns_shaped_body_with_line_numbers() {
        let (srv, api) = server();
        let resp = call(&srv, "return await ws.note.read('n-1');").await;
        assert_eq!(resp["result"]["isError"], json!(false));
        let v = body(&resp);
        assert_eq!(v["id"], json!("n-1"));
        assert_eq!(v["title"], json!("title-n-1"));
        assert_eq!(v["totalLines"], json!(3));
        assert_eq!(v["imageCount"], json!(0));
        let content = v["content"].as_str().unwrap();
        // First line is padded to width 4 (`   1 | line one`).
        assert!(content.starts_with("   1 | line one"), "content: {content}");
        assert!(content.contains("   2 | line two"));
        assert_eq!(api.get_note_calls.lock().unwrap().len(), 1);
    }

    #[tokio::test]
    async fn note_read_task_note_appends_task_metadata_footer() {
        let (srv, _api) = server();
        let resp = call(&srv, "return await ws.note.read('task-1');").await;
        assert_eq!(resp["result"]["isError"], json!(false));
        let v = body(&resp);
        assert_eq!(v["isTask"], json!(true));
        assert_eq!(v["taskStatus"], json!("in_progress"));
        // taskStatus / taskMetadata must be fully serialized (never a silent null
        // fallback): the serializer errors would surface as a JS-visible error.
        assert!(v["taskStatus"].is_string(), "taskStatus should be a string");
        assert!(
            v["taskMetadata"].is_object(),
            "taskMetadata should be an object"
        );
        let content = v["content"].as_str().unwrap();
        assert!(content.contains("--- Task Metadata ---"));
        assert!(content.contains("Acceptance Criteria:"));
    }

    #[tokio::test]
    async fn note_read_missing_id_surfaces_js_error() {
        let (srv, _api) = server();
        let resp = call(&srv, "return await ws.note.read();").await;
        assert_eq!(resp["result"]["isError"], json!(true));
        assert!(text(&resp).contains("Note ID is required"));
    }

    #[tokio::test]
    async fn note_read_not_found_surfaces_daemon_error_verbatim() {
        let (srv, _api) = server();
        let resp = call(&srv, "return await ws.note.read('missing');").await;
        assert_eq!(resp["result"]["isError"], json!(true));
        assert!(text(&resp).contains("Note not found"));
    }

    #[tokio::test]
    async fn note_create_returns_link_and_markdown_link() {
        let (srv, api) = server();
        let resp = call(
            &srv,
            "return await ws.note.create('Hello', 'world', ['a', 'b']);",
        )
        .await;
        assert_eq!(resp["result"]["isError"], json!(false));
        let v = body(&resp);
        assert_eq!(v["id"], json!("n-new"));
        assert_eq!(v["title"], json!("Hello"));
        assert_eq!(v["tags"], json!(["a", "b"]));
        assert_eq!(v["link"], json!("intent://local/amber-forest/note/n-new"));
        assert_eq!(
            v["markdownLink"],
            json!("[Hello](intent://local/amber-forest/note/n-new)")
        );
        // Conversion outcome is appended to the binding result (parity with
        // the content-write ops).
        assert_eq!(v["convertedCount"], json!(0));
        assert_eq!(v["createdTaskNoteIds"], json!([]));
        assert_eq!(v["createdTasks"], json!([]));
        assert_eq!(v["warnings"], json!([]));
        let created = api.create_note_calls.lock().unwrap();
        assert_eq!(created.len(), 1);
        assert_eq!(created[0].0, "Hello");
        assert_eq!(created[0].1.as_deref(), Some("world"));
        let key = created[0]
            .3
            .as_deref()
            .expect("note.create must mint an idempotencyKey");
        assert!(
            uuid::Uuid::parse_str(key).is_ok(),
            "minted key {key:?} is not a UUID"
        );
    }

    #[tokio::test]
    async fn note_create_passes_caller_idempotency_key_through() {
        // Caller-supplied `idempotencyKey` is adopted verbatim, matching
        // `comment.add`. The JS prelude is positional so the key
        // must be supplied via a raw `host({...})` invocation.
        let (srv, api) = server();
        let resp = call(
            &srv,
            "return await host({ method: 'note.create', args: { title: 'H', content: 'W', idempotencyKey: 'key-from-caller' } });",
        )
        .await;
        assert_eq!(resp["result"]["isError"], json!(false));
        let created = api.create_note_calls.lock().unwrap();
        assert_eq!(created.len(), 1);
        assert_eq!(created[0].3.as_deref(), Some("key-from-caller"));
    }

    #[tokio::test]
    async fn note_create_treats_blank_idempotency_key_as_absent() {
        // A whitespace-only key must be treated as absent (parity with
        // `comment.add`) so it cannot collapse dedupe across unrelated
        // requests. The binding mints a fresh UUID instead.
        let (srv, api) = server();
        let resp = call(
            &srv,
            "return await host({ method: 'note.create', args: { title: 'H', content: 'W', idempotencyKey: '   ' } });",
        )
        .await;
        assert_eq!(resp["result"]["isError"], json!(false));
        let created = api.create_note_calls.lock().unwrap();
        assert_eq!(created.len(), 1);
        let key = created[0]
            .3
            .as_deref()
            .expect("note.create must mint an idempotencyKey");
        assert!(
            uuid::Uuid::parse_str(key).is_ok(),
            "minted key {key:?} is not a UUID (got blank passthrough)"
        );
    }

    #[tokio::test]
    async fn note_create_missing_content_surfaces_reference_error() {
        let (srv, _api) = server();
        let resp = call(&srv, "return await ws.note.create('Hello');").await;
        assert_eq!(resp["result"]["isError"], json!(true));
        assert!(text(&resp).contains("Title and content are required"));
    }

    #[tokio::test]
    async fn note_list_filters_by_tag_and_projects_summary() {
        let (srv, _api) = server();
        let resp = call(&srv, "return await ws.note.list();").await;
        let v = body(&resp);
        let arr = v.as_array().unwrap();
        assert_eq!(arr.len(), 2);
        assert_eq!(arr[0]["id"], json!("n-1"));

        let resp = call(&srv, "return await ws.note.list('blue');").await;
        let arr = body(&resp);
        let arr = arr.as_array().unwrap();
        assert_eq!(arr.len(), 1);
        assert_eq!(arr[0]["id"], json!("n-2"));
    }

    #[tokio::test]
    async fn note_list_tasks_returns_task_rows() {
        let (srv, _api) = server();
        let resp = call(&srv, "return await ws.note.listTasks('n-1');").await;
        let v = body(&resp);
        let arr = v.as_array().unwrap();
        assert_eq!(arr.len(), 1);
        assert_eq!(arr[0]["lineNumber"], json!(3));
        assert_eq!(arr[0]["status"], json!("todo"));
    }

    #[tokio::test]
    async fn note_read_asset_forwards_asset_id_to_daemon() {
        let (srv, api) = server();
        let resp = call(
            &srv,
            "return await ws.note.readAsset('workspace-asset://amber-forest/img-1');",
        )
        .await;
        let v = body(&resp);
        assert_eq!(v["mimeType"], json!("image/png"));
        assert_eq!(v["sizeKb"], json!(1));
        assert_eq!(
            api.read_asset_calls.lock().unwrap()[0],
            "workspace-asset://amber-forest/img-1"
        );
    }

    #[tokio::test]
    async fn note_set_content_threads_confirm_replacement_true() {
        let (srv, api) = server();
        let resp = call(
            &srv,
            "return await ws.note.setContent('n-1', 'new body', true);",
        )
        .await;
        assert_eq!(resp["result"]["isError"], json!(false));
        let calls = api.set_content_calls.lock().unwrap();
        assert_eq!(calls[0], ("n-1".to_string(), "new body".to_string(), true));
    }

    #[tokio::test]
    async fn note_add_threads_position_option_object() {
        let (srv, api) = server();
        let resp = call(
            &srv,
            "return await ws.note.add('n-1', { content: 'X', position: 'start' });",
        )
        .await;
        assert_eq!(resp["result"]["isError"], json!(false));
        let calls = api.add_calls.lock().unwrap();
        assert_eq!(calls[0].2, None);
        assert_eq!(calls[0].3.as_deref(), Some("start"));
    }

    #[tokio::test]
    async fn note_edit_missing_new_surfaces_js_error() {
        let (srv, _api) = server();
        let resp = call(&srv, "return await ws.note.edit('n-1', { old: 'a' });").await;
        assert_eq!(resp["result"]["isError"], json!(true));
        assert!(text(&resp).contains("new is required"));
    }

    #[tokio::test]
    async fn note_edit_lines_rejects_reversed_range() {
        let (srv, _api) = server();
        let resp = call(
            &srv,
            "return await ws.note.editLines('n-1', { start: 5, end: 2, content: 'x' });",
        )
        .await;
        assert_eq!(resp["result"]["isError"], json!(true));
        assert!(text(&resp).contains("start cannot be greater than end"));
    }

    #[tokio::test]
    async fn note_update_metadata_requires_title_or_tags() {
        let (srv, _api) = server();
        let resp = call(&srv, "return await ws.note.updateMetadata('n-1', {});").await;
        assert_eq!(resp["result"]["isError"], json!(true));
        assert!(text(&resp).contains("At least one of title or tags"));
    }

    #[tokio::test]
    async fn note_delete_records_daemon_call() {
        let (srv, api) = server();
        let resp = call(&srv, "return await ws.note.delete('n-1');").await;
        assert_eq!(resp["result"]["isError"], json!(false));
        let v = body(&resp);
        assert_eq!(v["ok"], json!(true));
        assert_eq!(api.delete_calls.lock().unwrap()[0], "n-1");
    }

    // ================================================================
    // task.*
    // ================================================================

    #[tokio::test]
    async fn task_update_status_happy_path() {
        let (srv, api) = server();
        let resp = call(
            &srv,
            "return await ws.task.updateStatus('n-1', 'do it', 'done');",
        )
        .await;
        assert_eq!(resp["result"]["isError"], json!(false));
        let v = body(&resp);
        assert_eq!(v["status"], json!("done"));
        assert_eq!(
            api.task_update_status_calls.lock().unwrap()[0],
            ("n-1".to_string(), "do it".to_string(), "done".to_string())
        );
    }

    #[tokio::test]
    async fn task_update_status_rejects_invalid_status() {
        let (srv, _api) = server();
        let resp = call(
            &srv,
            "return await ws.task.updateStatus('n-1', 'do it', 'unknown');",
        )
        .await;
        assert_eq!(resp["result"]["isError"], json!(true));
        assert!(text(&resp).contains("'done', 'todo', or 'in-progress'"));
    }

    #[tokio::test]
    async fn task_update_note_status_forwards_status() {
        let (srv, api) = server();
        let resp = call(
            &srv,
            "return await ws.task.updateNoteStatus('n-1', 'complete');",
        )
        .await;
        assert_eq!(resp["result"]["isError"], json!(false));
        let v = body(&resp);
        assert_eq!(v["status"], json!("complete"));
        assert_eq!(
            api.task_update_note_status_calls.lock().unwrap()[0],
            ("n-1".to_string(), "complete".to_string())
        );
    }

    #[tokio::test]
    async fn task_update_requires_text_or_status() {
        let (srv, _api) = server();
        let resp = call(&srv, "return await ws.task.update('n-1', 3, {});").await;
        assert_eq!(resp["result"]["isError"], json!(true));
        assert!(text(&resp).contains("Either text or status"));
    }

    #[tokio::test]
    async fn task_update_happy_path() {
        let (srv, api) = server();
        let resp = call(
            &srv,
            "return await ws.task.update('n-1', 3, { text: 'new text', status: 'done' });",
        )
        .await;
        assert_eq!(resp["result"]["isError"], json!(false));
        assert_eq!(api.task_update_calls.lock().unwrap()[0].1, 3);
    }

    #[tokio::test]
    async fn task_get_my_task_returns_shaped_body() {
        let (srv, api) = server();
        let resp = call(&srv, "return await ws.task.getMyTask('task-1');").await;
        let v = body(&resp);
        assert_eq!(v["title"], json!("task title"));
        assert_eq!(v["status"], json!("in_progress"));
        assert_eq!(api.get_my_task_calls.lock().unwrap()[0], "task-1");
    }

    #[tokio::test]
    async fn task_mark_as_task_forwards_acceptance_criteria() {
        let (srv, api) = server();
        let resp = call(
            &srv,
            "return await ws.task.markAsTask('n-1', 'in_progress', { acceptanceCriteria: ['A', 'B'], effort: '2h' });",
        )
        .await;
        assert_eq!(resp["result"]["isError"], json!(false));
        let calls = api.mark_as_task_calls.lock().unwrap();
        assert_eq!(calls[0].2, vec!["A".to_string(), "B".to_string()]);
        assert_eq!(calls[0].3.as_deref(), Some("2h"));
    }

    #[tokio::test]
    async fn task_convert_blocks_forwards_note_id() {
        let (srv, api) = server();
        let resp = call(&srv, "return await ws.task.convertBlocks('n-1');").await;
        assert_eq!(resp["result"]["isError"], json!(false));
        assert_eq!(api.convert_blocks_calls.lock().unwrap()[0], "n-1");
    }

    #[tokio::test]
    async fn task_create_prerequisite_forwards_content_and_status() {
        let (srv, api) = server();
        let resp = call(
            &srv,
            "return await ws.task.createPrerequisite('n-1', 'Prereq', { content: 'body', status: 'not_started' });",
        )
        .await;
        assert_eq!(resp["result"]["isError"], json!(false));
        let calls = api.create_prereq_calls.lock().unwrap();
        assert_eq!(calls[0].2.as_deref(), Some("body"));
        assert_eq!(calls[0].3.as_deref(), Some("not_started"));
    }

    #[tokio::test]
    async fn task_assign_agent_rejects_malformed_id() {
        let (srv, _api) = server();
        let resp = call(
            &srv,
            "return await ws.task.assignAgent('n-1', 'not-a-uuid');",
        )
        .await;
        assert_eq!(resp["result"]["isError"], json!(true));
        assert!(text(&resp).contains("Invalid agentId format"));
    }

    #[tokio::test]
    async fn task_assign_agent_happy_path() {
        let (srv, api) = server();
        let resp = call(
            &srv,
            "return await ws.task.assignAgent('n-1', 'agent-b0a8044a-5eac-4b52-8456-15d3b784decb');",
        )
        .await;
        assert_eq!(resp["result"]["isError"], json!(false));
        assert_eq!(
            api.assign_agent_calls.lock().unwrap()[0].1,
            "agent-b0a8044a-5eac-4b52-8456-15d3b784decb"
        );
    }

    // ================================================================
    // comment.*
    // ================================================================

    #[tokio::test]
    async fn comment_add_happy_path() {
        let (srv, api) = server();
        let resp = call(
            &srv,
            "return await ws.comment.add('n-1', { searchContext: 'ctx', commentTarget: 'ctx', comment: 'looks good' });",
        )
        .await;
        assert_eq!(resp["result"]["isError"], json!(false));
        let v = body(&resp);
        assert_eq!(v["commentId"], json!("c-1"));
        assert_eq!(api.comment_add_calls.lock().unwrap()[0].3, "looks good");
    }

    #[tokio::test]
    async fn comment_add_empty_comment_surfaces_reference_error() {
        let (srv, _api) = server();
        let resp = call(
            &srv,
            "return await ws.comment.add('n-1', { searchContext: 'ctx', commentTarget: 'ctx', comment: '   ' });",
        )
        .await;
        assert_eq!(resp["result"]["isError"], json!(true));
        assert!(text(&resp).contains("Comment text is required"));
    }

    #[tokio::test]
    async fn comment_list_forwards_note_id() {
        let (srv, api) = server();
        let resp = call(&srv, "return await ws.comment.list('n-1');").await;
        assert_eq!(resp["result"]["isError"], json!(false));
        assert_eq!(api.comment_list_calls.lock().unwrap()[0], "n-1");
    }

    #[tokio::test]
    async fn comment_get_thread_requires_id() {
        let (srv, _api) = server();
        let resp = call(&srv, "return await ws.comment.getThread('n-1', {});").await;
        assert_eq!(resp["result"]["isError"], json!(true));
        assert!(text(&resp).contains("Either threadId or commentId"));
    }

    #[tokio::test]
    async fn comment_respond_happy_path() {
        let (srv, api) = server();
        let resp = call(
            &srv,
            "return await ws.comment.respond('n-1', { threadId: 't-1', comment: 'ok' });",
        )
        .await;
        assert_eq!(resp["result"]["isError"], json!(false));
        assert_eq!(
            api.comment_respond_calls.lock().unwrap()[0].1.as_deref(),
            Some("t-1")
        );
    }

    #[tokio::test]
    async fn comment_delete_forwards_ids() {
        let (srv, api) = server();
        let resp = call(&srv, "return await ws.comment.delete('n-1', 'c-1');").await;
        assert_eq!(resp["result"]["isError"], json!(false));
        assert_eq!(
            api.comment_delete_calls.lock().unwrap()[0],
            ("n-1".to_string(), "c-1".to_string())
        );
    }

    // ================================================================
    // primitive.*
    // ================================================================

    #[tokio::test]
    async fn primitive_add_reference_happy_path() {
        let (srv, api) = server();
        let resp = call(
            &srv,
            "return await ws.primitive.addReference('n-1', 'src/foo.rs#L1-2', 'a range');",
        )
        .await;
        assert_eq!(resp["result"]["isError"], json!(false));
        assert!(api.primitive_calls.lock().unwrap()[0].starts_with("reference:"));
    }

    #[tokio::test]
    async fn primitive_add_cli_forwards_working_directory() {
        let (srv, api) = server();
        let resp = call(
            &srv,
            "return await ws.primitive.addCli('n-1', 'ls', 'listing', '/tmp');",
        )
        .await;
        assert_eq!(resp["result"]["isError"], json!(false));
        assert!(api.primitive_calls.lock().unwrap()[0].starts_with("cli:"));
    }

    #[tokio::test]
    async fn primitive_add_patch_missing_description_errors() {
        let (srv, _api) = server();
        let resp = call(
            &srv,
            "return await ws.primitive.addPatch('n-1', 'src/foo.rs', 'diff');",
        )
        .await;
        assert_eq!(resp["result"]["isError"], json!(true));
        assert!(text(&resp).contains("description is required"));
    }

    #[tokio::test]
    async fn primitive_add_agent_action_happy_path() {
        let (srv, api) = server();
        let resp = call(
            &srv,
            "return await ws.primitive.addAgentAction('n-1', 'agent-1', 'do it', 'why');",
        )
        .await;
        assert_eq!(resp["result"]["isError"], json!(false));
        assert!(api.primitive_calls.lock().unwrap()[0].starts_with("agent_action:"));
    }

    // ================================================================
    // Batching — one `workspace_api` call, multiple concurrent host frames.
    // ================================================================

    #[tokio::test]
    async fn promise_all_batches_multiple_bindings_in_one_invocation() {
        let (srv, api) = server();
        let resp = call(
            &srv,
            r"
            const [a, b, c] = await Promise.all([
                ws.note.list(),
                ws.note.listTasks('n-1'),
                ws.task.getMyTask('task-1'),
            ]);
            return { listLen: a.length, taskRows: b.length, taskTitle: c.title };
            ",
        )
        .await;
        assert_eq!(resp["result"]["isError"], json!(false));
        let v = body(&resp);
        assert_eq!(v["listLen"], json!(2));
        assert_eq!(v["taskRows"], json!(1));
        assert_eq!(v["taskTitle"], json!("task title"));
        assert_eq!(*api.list_notes_calls.lock().unwrap(), 1);
        assert_eq!(api.list_note_tasks_calls.lock().unwrap().len(), 1);
        assert_eq!(api.get_my_task_calls.lock().unwrap().len(), 1);
    }
}

/// WSAPI-6 per-namespace bindings: `ws.pr.*`, `ws.crossWorkspace.*`,
/// `ws.browser.*`. Each namespace round-trips a `workspace_api` request
/// against a fake [`WorkspaceApi`] that mocks the trait methods the
/// bindings touch. For `ws.browser.exec` the fake stands in for the FE-served
/// reverse channel — it records the forwarded envelope and returns the
/// pre-shaped `Value` the transport layer would echo back.
#[cfg(test)]
mod wsapi6_bindings_tests {
    use std::sync::{Arc, Mutex};

    use intent_core::{AgentId, BoxFuture, NoteId, Result, WorkspaceApi, WorkspaceId};
    use serde_json::{json, Value};

    use crate::WorkspaceMcpServer;

    type PrSnapshotCall = (u64, Option<String>);
    type CrossReadCall = (String, String);
    type BrowserExecCall = (Vec<Value>, Option<String>, Option<String>);

    #[derive(Default)]
    struct FakeApi {
        pr_snapshot_calls: Mutex<Vec<PrSnapshotCall>>,
        cross_list_siblings_calls: Mutex<u32>,
        cross_read_note_calls: Mutex<Vec<CrossReadCall>>,
        cross_list_notes_calls: Mutex<Vec<String>>,
        browser_exec_calls: Mutex<Vec<BrowserExecCall>>,
        /// When set, `browser_exec` shapes this raw FE envelope through the
        /// real `intent_services::browser_ops::shape_agent_result` — the same
        /// seam the production `Services::browser_exec` uses — so the JS
        /// dispatch tests exercise real shaping instead of canned replies.
        browser_exec_fe_envelope: Mutex<Option<Value>>,
    }

    impl WorkspaceApi for FakeApi {
        // Pin the `workspaceApi.*` output knobs to the legacy behavior (plain
        // pretty JSON, no size limit) so this fixture keeps asserting raw JSON
        // bodies; the TOON/limit paths are covered by
        // `workspace_api_output_limit_tests`.
        fn settings_get(&self, path: String) -> BoxFuture<'_, Result<Value>> {
            Box::pin(async move {
                let value = match path.as_str() {
                    "workspaceApi.toonOutput" => json!(false),
                    "workspaceApi.maxOutputChars" => json!(0),
                    _ => Value::Null,
                };
                Ok(json!({ "path": path, "value": value }))
            })
        }

        fn pr_state(
            &self,
            _ws: WorkspaceId,
            pr_number: u64,
            repo: Option<String>,
        ) -> BoxFuture<'_, Result<Value>> {
            self.pr_snapshot_calls
                .lock()
                .unwrap()
                .push((pr_number, repo.clone()));
            Box::pin(async move {
                Ok(json!({
                    "repo": repo.unwrap_or_else(|| "o/r".to_string()),
                    "prNumber": pr_number,
                    "state": "open",
                    "isMerged": false,
                    "mergeBlockedReason": null,
                    "checks": { "total": 0, "passed": 0, "failed": 0, "pending": 0, "failedNames": [] },
                    "reviews": { "decision": "review_required", "approvals": 0, "changesRequested": 0 },
                    "comments": { "conversationCount": 0, "reviewCommentCount": 0, "unresolvedThreadCount": 0, "totalCount": 0 },
                }))
            })
        }

        fn cross_workspace_list_siblings(&self, _ws: WorkspaceId) -> BoxFuture<'_, Result<Value>> {
            *self.cross_list_siblings_calls.lock().unwrap() += 1;
            Box::pin(async {
                Ok(json!([
                    { "id": "sibling-1", "title": "Sib One", "branch": "b/1", "status": "active" },
                ]))
            })
        }

        fn cross_workspace_read_note(
            &self,
            _ws: WorkspaceId,
            target: WorkspaceId,
            note_id: NoteId,
        ) -> BoxFuture<'_, Result<Value>> {
            self.cross_read_note_calls
                .lock()
                .unwrap()
                .push((target.as_str().to_string(), note_id.as_str().to_string()));
            Box::pin(async move {
                Ok(json!({
                    "id": note_id.as_str(),
                    "title": "cross-title",
                    "content": "hello\nworld",
                    "numberedContent": "   1 | hello\n   2 | world",
                    "sourceWorkspaceId": target.as_str(),
                    "sourceWorkspaceTitle": "Sib One",
                    "branch": "b/1",
                    "lineCount": 2,
                }))
            })
        }

        fn cross_workspace_list_notes(
            &self,
            _ws: WorkspaceId,
            target: WorkspaceId,
        ) -> BoxFuture<'_, Result<Value>> {
            self.cross_list_notes_calls
                .lock()
                .unwrap()
                .push(target.as_str().to_string());
            Box::pin(async {
                Ok(json!([
                    { "id": "n-1", "title": "t1", "createdAt": "2026-01-01T00:00:00Z", "updatedAt": "2026-01-01T00:00:00Z" },
                ]))
            })
        }

        fn browser_exec(
            &self,
            _ws: WorkspaceId,
            actions: Vec<Value>,
            tab_id: Option<String>,
            agent_id: Option<AgentId>,
        ) -> BoxFuture<'_, Result<Value>> {
            self.browser_exec_calls.lock().unwrap().push((
                actions.clone(),
                tab_id,
                agent_id.map(|a| a.as_str().to_string()),
            ));
            let fe_envelope = self.browser_exec_fe_envelope.lock().unwrap().clone();
            // Reference parity: a single-action batch yields the sole action's
            // envelope; multi-action yields `{ results: [...] }`. The fake
            // stands in for what the reverse channel would have returned.
            Box::pin(async move {
                if let Some(envelope) = fe_envelope {
                    return intent_services::browser_ops::shape_agent_result(
                        &envelope,
                        actions.len(),
                    )
                    .map_err(|e| intent_core::Error::Internal(e.message));
                }
                if actions.len() == 1 {
                    Ok(json!({
                        "action": "listTabs",
                        "success": true,
                        "result": { "tabs": [] }
                    }))
                } else {
                    Ok(json!({
                        "results": actions
                            .iter()
                            .map(|a| json!({
                                "action": a.get("action").cloned().unwrap_or(Value::Null),
                                "success": true,
                                "result": {}
                            }))
                            .collect::<Vec<_>>()
                    }))
                }
            })
        }
    }

    fn server() -> (WorkspaceMcpServer, Arc<FakeApi>) {
        let api = Arc::new(FakeApi::default());
        let srv = WorkspaceMcpServer::new(api.clone(), WorkspaceId::from_string("amber-forest"));
        (srv, api)
    }

    fn server_with_caller(agent_id: &str) -> (WorkspaceMcpServer, Arc<FakeApi>) {
        let api = Arc::new(FakeApi::default());
        let srv = WorkspaceMcpServer::new(api.clone(), WorkspaceId::from_string("amber-forest"))
            .with_caller_agent_id(Some(AgentId::from_string(agent_id)));
        (srv, api)
    }

    async fn call(srv: &WorkspaceMcpServer, code: &str) -> Value {
        srv.handle_message(&json!({
            "jsonrpc": "2.0", "id": 1, "method": "tools/call",
            "params": {
                "name": "workspace_api",
                "arguments": { "code": code, "summary": "wsapi6 unit test" }
            }
        }))
        .await
        .expect("tools/call must produce a response")
    }

    fn body(resp: &Value) -> Value {
        let text = resp["result"]["content"][0]["text"].as_str().unwrap();
        serde_json::from_str(text).expect("workspace_api body must be JSON")
    }

    fn text(resp: &Value) -> String {
        resp["result"]["content"][0]["text"]
            .as_str()
            .unwrap()
            .to_string()
    }

    // ================================================================
    // crossWorkspace.*
    // ================================================================

    #[tokio::test]
    async fn cross_workspace_list_siblings_returns_sibling_array() {
        let (srv, api) = server();
        let resp = call(&srv, "return await ws.crossWorkspace.listSiblings();").await;
        assert_eq!(resp["result"]["isError"], json!(false));
        let v = body(&resp);
        assert!(v.is_array());
        assert_eq!(v[0]["id"], json!("sibling-1"));
        assert_eq!(*api.cross_list_siblings_calls.lock().unwrap(), 1);
    }

    #[tokio::test]
    async fn cross_workspace_read_note_forwards_target_and_note_ids() {
        let (srv, api) = server();
        let resp = call(
            &srv,
            "return await ws.crossWorkspace.readNote('sibling-1', 'n-1');",
        )
        .await;
        assert_eq!(resp["result"]["isError"], json!(false));
        let v = body(&resp);
        assert_eq!(v["id"], json!("n-1"));
        assert_eq!(v["sourceWorkspaceId"], json!("sibling-1"));
        assert_eq!(v["lineCount"], json!(2));
        assert_eq!(
            *api.cross_read_note_calls.lock().unwrap(),
            vec![("sibling-1".to_string(), "n-1".to_string())]
        );
    }

    #[tokio::test]
    async fn cross_workspace_read_note_missing_note_id_errors() {
        let (srv, _api) = server();
        // Reference parity: both fields required, single error string.
        let resp = call(
            &srv,
            "return await ws.crossWorkspace.readNote('sib', null);",
        )
        .await;
        assert_eq!(resp["result"]["isError"], json!(true));
        assert!(text(&resp).contains("Both workspaceId and noteId are required"));
    }

    #[tokio::test]
    async fn cross_workspace_list_notes_forwards_target_workspace_id() {
        let (srv, api) = server();
        let resp = call(
            &srv,
            "return await ws.crossWorkspace.listNotes('sibling-1');",
        )
        .await;
        assert_eq!(resp["result"]["isError"], json!(false));
        let v = body(&resp);
        assert!(v.is_array());
        assert_eq!(v[0]["id"], json!("n-1"));
        assert_eq!(
            *api.cross_list_notes_calls.lock().unwrap(),
            vec!["sibling-1".to_string()]
        );
    }

    // ================================================================
    // pr.*
    // ================================================================

    #[tokio::test]
    async fn pr_snapshot_forwards_pr_number_and_returns_shape() {
        let (srv, api) = server();
        let resp = call(&srv, "return await ws.pr.snapshot(42);").await;
        assert_eq!(resp["result"]["isError"], json!(false));
        let v = body(&resp);
        assert_eq!(v["repo"], json!("o/r"));
        assert_eq!(v["prNumber"], json!(42));
        assert_eq!(v["isMerged"], json!(false));
        assert_eq!(v["mergeBlockedReason"], json!(null));
        assert_eq!(*api.pr_snapshot_calls.lock().unwrap(), vec![(42u64, None)]);
    }

    #[tokio::test]
    async fn pr_snapshot_forwards_cross_repo_arg_and_echoes_repo() {
        let (srv, api) = server();
        let resp = call(
            &srv,
            "return await ws.pr.snapshot(7, { repo: 'acme/widgets' });",
        )
        .await;
        assert_eq!(resp["result"]["isError"], json!(false));
        let v = body(&resp);
        assert_eq!(v["repo"], json!("acme/widgets"));
        assert_eq!(v["prNumber"], json!(7));
        assert_eq!(
            *api.pr_snapshot_calls.lock().unwrap(),
            vec![(7u64, Some("acme/widgets".to_string()))]
        );
    }

    #[tokio::test]
    async fn pr_snapshot_rejects_non_string_repo() {
        // A present-but-non-string `repo` fails fast instead of silently
        // falling back to the workspace repo.
        let (srv, api) = server();
        for code in [
            "return await ws.pr.snapshot(7, { repo: 123 });",
            "return await ws.pr.snapshot(7, { repo: { owner: 'a', name: 'b' } });",
        ] {
            let resp = call(&srv, code).await;
            assert_eq!(resp["result"]["isError"], json!(true), "code: {code}");
            assert!(text(&resp).contains("repo must be an \"owner/name\" string"));
        }
        assert!(api.pr_snapshot_calls.lock().unwrap().is_empty());
    }

    #[tokio::test]
    async fn pr_snapshot_requires_numeric_pr_number() {
        // Missing, non-positive, and non-numeric prNumber all surface the
        // same validation error before the trait method is called.
        let (srv, api) = server();
        for code in [
            "return await ws.pr.snapshot();",
            "return await ws.pr.snapshot(0);",
            "return await ws.pr.snapshot(-1);",
            "return await ws.pr.snapshot('abc');",
        ] {
            let resp = call(&srv, code).await;
            assert_eq!(resp["result"]["isError"], json!(true), "code: {code}");
            assert!(
                text(&resp).contains("prNumber is required and must be a number"),
                "code: {code}"
            );
        }
        assert!(api.pr_snapshot_calls.lock().unwrap().is_empty());
    }

    #[tokio::test]
    async fn pr_removed_methods_error_as_unknown() {
        // The non-snapshot `ws.pr.*` surface was removed in favor of the `gh`
        // CLI; raw `host({...})` frames for the old methods must fail with the
        // standard unknown-binding error, not a validation or trait error.
        let (srv, _api) = server();
        for method in [
            "status",
            "merge",
            "updateBranch",
            "listReviewComments",
            "replyToReviewComment",
            "resolveThread",
            "listComments",
            "postComment",
        ] {
            let code = format!("return await host({{ method: 'pr.{method}', args: {{}} }});");
            let resp = call(&srv, &code).await;
            assert_eq!(resp["result"]["isError"], json!(true), "pr.{method}");
            assert!(
                text(&resp).contains(&format!("unknown method `pr.{method}`")),
                "pr.{method} must surface the unknown-binding error"
            );
        }
    }

    // ================================================================
    // browser.*
    // ================================================================

    #[tokio::test]
    async fn browser_exec_forwards_actions_and_shapes_single_result() {
        let (srv, api) = server_with_caller("agent-77");
        let resp = call(
            &srv,
            r"return await ws.browser.exec([{ action: 'listTabs' }], 'tab-1');",
        )
        .await;
        assert_eq!(resp["result"]["isError"], json!(false));
        let v = body(&resp);
        // Reference parity for a 1-action batch: single envelope, not wrapped.
        assert_eq!(v["action"], json!("listTabs"));
        assert_eq!(v["success"], json!(true));

        let calls = api.browser_exec_calls.lock().unwrap();
        assert_eq!(calls.len(), 1);
        let (actions, tab_id, agent_id) = &calls[0];
        assert_eq!(actions.len(), 1);
        assert_eq!(actions[0]["action"], json!("listTabs"));
        assert_eq!(tab_id.as_deref(), Some("tab-1"));
        // caller_agent_id is threaded through for FE-side attribution.
        assert_eq!(agent_id.as_deref(), Some("agent-77"));
    }

    #[tokio::test]
    async fn browser_exec_multi_action_batch_returns_results_array() {
        let (srv, _api) = server();
        let resp = call(
            &srv,
            r"return await ws.browser.exec([
                { action: 'listTabs' },
                { action: 'screenshot' }
            ]);",
        )
        .await;
        assert_eq!(resp["result"]["isError"], json!(false));
        let v = body(&resp);
        // Multi-action batch → { results: [...] } passthrough from the fake
        // reverse channel; matches transport `shape_result` behaviour.
        assert_eq!(v["results"].as_array().unwrap().len(), 2);
    }

    // Regression (monorepo#3042): structured per-action ownership errors
    // (not-owner / already-claimed) must reach the JS caller as data —
    // `errorCode` / `ownerAgentId` readable from the returned object — not
    // flattened into a thrown prose error.
    #[tokio::test]
    async fn browser_exec_single_action_ownership_failure_is_structured_in_js() {
        let (srv, api) = server();
        *api.browser_exec_fe_envelope.lock().unwrap() = Some(json!({
            "success": false,
            "error": "Tab tab-9 is not owned by you",
            "results": [{
                "action": "resizeTab",
                "success": false,
                "errorCode": "not-owner",
                "ownerAgentId": null,
                "error": "Tab tab-9 is not owned by you (owner: none). Claim it with claimTab first."
            }],
        }));
        let resp = call(
            &srv,
            r"
            const r = await ws.browser.exec([{ action: 'resizeTab', tabId: 'tab-9', width: 375 }]);
            return { code: r.errorCode, owner: r.ownerAgentId, ok: r.success };
            ",
        )
        .await;
        assert_eq!(resp["result"]["isError"], json!(false));
        let v = body(&resp);
        assert_eq!(v["code"], json!("not-owner"));
        assert_eq!(v["owner"], json!(Value::Null));
        assert_eq!(v["ok"], json!(false));
    }

    #[tokio::test]
    async fn browser_exec_multi_action_partial_failure_is_structured_in_js() {
        let (srv, api) = server();
        *api.browser_exec_fe_envelope.lock().unwrap() = Some(json!({
            "success": false,
            "error": "1 of 2 actions failed",
            "results": [
                { "action": "listTabs", "success": true, "result": [] },
                {
                    "action": "claimTab",
                    "success": false,
                    "errorCode": "already-claimed",
                    "ownerAgentId": "agent-42",
                    "error": "Tab tab-3 is owned by agent agent-42"
                }
            ],
        }));
        let resp = call(
            &srv,
            r"
            const r = await ws.browser.exec([
                { action: 'listTabs' },
                { action: 'claimTab', tabId: 'tab-3', width: 1280 }
            ]);
            const failed = r.results.find(x => !x.success);
            return { ok: r.success, code: failed.errorCode, owner: failed.ownerAgentId };
            ",
        )
        .await;
        assert_eq!(resp["result"]["isError"], json!(false));
        let v = body(&resp);
        assert_eq!(v["ok"], json!(false));
        assert_eq!(v["code"], json!("already-claimed"));
        assert_eq!(v["owner"], json!("agent-42"));
    }

    // The FE aborts a batch on the first failing action, so the reply's
    // partial `results` can be shorter than the request. A multi-action JS
    // caller must still receive the `{ success, results, error }` envelope
    // (never a bare action envelope), so `r.results.find(...)` always works.
    #[tokio::test]
    async fn browser_exec_multi_action_first_failure_keeps_envelope_in_js() {
        let (srv, api) = server();
        *api.browser_exec_fe_envelope.lock().unwrap() = Some(json!({
            "success": false,
            "error": "Tab tab-9 is not owned by you",
            "results": [{
                "action": "resizeTab",
                "success": false,
                "errorCode": "not-owner",
                "ownerAgentId": "agent-7",
                "error": "Tab tab-9 is owned by agent agent-7"
            }],
        }));
        let resp = call(
            &srv,
            r"
            const r = await ws.browser.exec([
                { action: 'resizeTab', tabId: 'tab-9', width: 375 },
                { action: 'screenshot' },
                { action: 'listTabs' }
            ]);
            const failed = r.results.find(x => !x.success);
            return { ok: r.success, count: r.results.length, code: failed.errorCode, owner: failed.ownerAgentId };
            ",
        )
        .await;
        assert_eq!(resp["result"]["isError"], json!(false));
        let v = body(&resp);
        assert_eq!(v["ok"], json!(false));
        assert_eq!(v["count"], json!(1));
        assert_eq!(v["code"], json!("not-owner"));
        assert_eq!(v["owner"], json!("agent-7"));
    }

    #[tokio::test]
    async fn browser_exec_failure_without_results_still_throws_in_js() {
        let (srv, api) = server();
        *api.browser_exec_fe_envelope.lock().unwrap() = Some(json!({
            "success": false,
            "error": "CDP not attached",
        }));
        let resp = call(
            &srv,
            "return await ws.browser.exec([{ action: 'listTabs' }]);",
        )
        .await;
        assert_eq!(resp["result"]["isError"], json!(true));
        assert!(text(&resp).contains("CDP not attached"));
    }

    #[tokio::test]
    async fn browser_exec_missing_actions_errors_without_calling_trait() {
        let (srv, api) = server();
        let resp = call(&srv, "return await ws.browser.exec();").await;
        assert_eq!(resp["result"]["isError"], json!(true));
        assert!(text(&resp).contains("actions parameter is required"));
        assert!(api.browser_exec_calls.lock().unwrap().is_empty());
    }

    #[tokio::test]
    async fn browser_exec_empty_actions_errors_without_calling_trait() {
        let (srv, api) = server();
        let resp = call(&srv, "return await ws.browser.exec([]);").await;
        assert_eq!(resp["result"]["isError"], json!(true));
        assert!(text(&resp).contains("actions array cannot be empty"));
        assert!(api.browser_exec_calls.lock().unwrap().is_empty());
    }

    #[tokio::test]
    async fn browser_docs_returns_topic_text_verbatim() {
        let (srv, _api) = server();
        for topic in ["overview", "capture", "examples"] {
            let resp = call(&srv, &format!("return await ws.browser.docs('{topic}');")).await;
            assert_eq!(
                resp["result"]["isError"],
                json!(false),
                "topic {topic} should succeed"
            );
            let v = body(&resp);
            let doc = v.as_str().expect("docs return string");
            assert!(!doc.is_empty(), "topic {topic} should return non-empty");
            // First line is the topic heading (`# Browser ...`).
            assert!(doc.starts_with("# Browser"), "topic {topic}: {doc:.40}");
        }
    }

    #[tokio::test]
    async fn browser_docs_rejects_unknown_topic() {
        let (srv, _api) = server();
        let resp = call(&srv, "return await ws.browser.docs('bogus');").await;
        assert_eq!(resp["result"]["isError"], json!(true));
        assert!(text(&resp).contains("Unknown topic"));
        assert!(text(&resp).contains("overview, capture, examples"));
    }
}
/// WSAPI-4 per-namespace bindings: `ws.agent.*` and `ws.event.*`. Each
/// namespace is exercised through the real JS engine — the tool call
/// round-trips a `workspace_api` request against a fake [`WorkspaceApi`]
/// that only stubs the trait methods the bindings touch. Caller
/// attribution (SUB-1 sender auto-subscribe on `agent.send`) is exercised
/// by wiring the server through `with_caller_agent_id`.
#[cfg(test)]
mod wsapi4_bindings_tests {
    use std::sync::{Arc, Mutex};

    use intent_core::{
        AgentDelegateInput, AgentId, AgentLite, AgentMetadata, AgentStatus, BoxFuture, Error,
        EventQueryParams, EventSubscribeResult, EventUnsubscribeResult, Result,
        TaskGetMyTaskResult, TaskMetadata, TaskStatus, WorkspaceApi, WorkspaceId,
    };
    use serde_json::{json, Value};

    use crate::WorkspaceMcpServer;

    type SendCall = (String, String, Option<String>, Option<Value>);
    type SendToTaskCall = (String, String, Option<String>, Option<Value>);
    type WatchSenderCall = (String, String);
    type SubscribeCall = (Vec<String>, Option<bool>, Option<i64>);
    type DelegateCall = (Option<String>, Option<String>);
    type WakeOrCreateCall = (String, String, Option<String>, Option<Value>);
    type AttentionCall = (String, String, Option<String>);
    type RetireCall = (String, Option<String>, Option<String>);
    /// `(name, specialist_id, parent_agent_id, idempotency_key, extra.metadata,
    /// extra.is_background)`.
    type CreateCall = (
        Option<String>,
        Option<String>,
        Option<String>,
        Option<String>,
        Option<Value>,
        Option<bool>,
    );

    #[derive(Default)]
    struct FakeApi {
        agent_list_calls: Mutex<u32>,
        agent_get_calls: Mutex<Vec<String>>,
        agent_send_calls: Mutex<Vec<SendCall>>,
        agent_send_to_task_calls: Mutex<Vec<SendToTaskCall>>,
        agent_delegate_calls: Mutex<Vec<DelegateCall>>,
        agent_wake_or_create_calls: Mutex<Vec<WakeOrCreateCall>>,
        agent_subscribe_calls: Mutex<Vec<SubscribeCall>>,
        agent_unsubscribe_calls: Mutex<Vec<String>>,
        event_subscribe_calls: Mutex<Vec<SubscribeCall>>,
        watch_sender_calls: Mutex<Vec<WatchSenderCall>>,
        report_to_parent_calls: Mutex<Vec<Option<String>>>,
        request_attention_calls: Mutex<Vec<AttentionCall>>,
        agent_delete_calls: Mutex<Vec<(String, Option<String>)>>,
        agent_retire_calls: Mutex<Vec<RetireCall>>,
        agent_create_calls: Mutex<Vec<CreateCall>>,
        /// Count of `agent_watch_completion` (create-path auto-subscribe)
        /// calls — `create({ topLevel: true })` must never make one.
        agent_watch_completion_calls: Mutex<u32>,
        /// When set, `agent_list` serves these rows instead of the two
        /// default top-level stubs (topLevel cap-counting tests).
        agent_list_rows: Mutex<Option<Vec<AgentLite>>>,
        /// When set, `settings_get("agents.maxTopLevelAgents")` serves this
        /// value (topLevel cap tests; the real settings layer normalizes
        /// numbers to the float wire shape).
        max_top_level_agents_setting: Mutex<Option<Value>>,
        /// Agent ids `agent_get` serves with `isBackground: true` metadata
        /// (background-caller denial tests for `create({ topLevel: true })`).
        background_agent_ids: Mutex<Vec<String>>,
        /// Agent ids `agent_is_retired` reports as retired (same-turn
        /// dispatch-guard tests).
        retired_agent_ids: Mutex<Vec<String>>,
        event_query_calls: Mutex<Vec<EventQueryParams>>,
        /// When set, `agent_get` fails with this error (name-lookup failure path).
        agent_get_error: Mutex<Option<String>>,
        /// Raw `agent.getQueue` entries served by `agent_get_queue`.
        queue_entries: Mutex<Vec<Value>>,
        remove_queued_owned_calls: Mutex<Vec<(String, String, String)>>,
        /// Interleaved order of send/removal calls, for asserting the
        /// send-first replacePending sequence.
        call_order: Mutex<Vec<&'static str>>,
        /// When set, `agent_remove_queued_message_owned` fails with
        /// `Error::NotFound` (replacePending drained-race path).
        remove_queued_error: Mutex<Option<String>>,
        /// When set, `agent_remove_queued_message_owned` fails with
        /// `Error::Internal` (replacePending "error" outcome path).
        remove_queued_internal_error: Mutex<Option<String>>,
        /// When set, `agent_send_message` fails with `Error::Internal`
        /// (lossless replacePending path: a failed send retracts nothing).
        agent_send_error: Mutex<Option<String>>,
        /// When set, `get_my_task` reports this agent as the task's assignee
        /// (sendToTask guard-path tests). Unset → `get_my_task` errors, so
        /// the guard falls through as on a resolution failure.
        task_assignee: Mutex<Option<String>>,
        /// When set, overrides the `agent_send_message` result (delivery-shape tests).
        agent_send_result: Mutex<Option<Value>>,
        /// When set, overrides the `agent_send_to_task` result (delivery-shape tests).
        agent_send_to_task_result: Mutex<Option<Value>>,
    }

    fn stub_agent(id: &str, ws: &WorkspaceId) -> AgentLite {
        AgentLite {
            harness_version: intent_core::CURRENT_HARNESS_VERSION.to_string(),
            harness_features: None,
            id: AgentId::from(id),
            workspace_id: ws.clone(),
            parent_agent_id: None,
            backend_session_id: None,
            acp_session_id: None,
            name: format!("agent-{id}"),
            name_explicitly_set: false,
            model: None,
            reasoning_effort: None,
            effort_levels: None,
            provider: None,
            status: AgentStatus::Idle,
            is_active: false,
            is_streaming: false,
            is_processing: false,
            is_responding: false,
            is_waiting_on_tool: false,
            is_waiting_for_other_agents: false,
            waiting_for_agent_ids: vec![],
            waiting_on_hooks: vec![],
            waiting_on_pr_monitors: vec![],
            turn_in_flight: false,
            last_stream_activity_at: None,
            context_usage: None,
            stats: None,
            created_at: "2026-01-01T00:00:00Z".to_string(),
            updated_at: "2026-01-01T00:00:00Z".to_string(),
            last_activity: None,
            message_count: 0,
            digest: None,
            last_agent_response: None,
            last_user_message: None,
            last_message_role: None,
            last_message_id: None,
            last_tool_use: None,
            context_references: None,
            file_blocks: None,
            stop_reason: None,
            stop_reason_timestamp: None,
            session_corrupted: false,
            pending_delete_at: None,
            retired_at: None,
            metadata: AgentMetadata {
                is_background: false,
                specialist: None,
                created_by_agent_id: None,
                task_note_id: None,
                completion_report: None,
                completion_report_timestamp: None,
                attention_request_kind: None,
                attention_request_reason: None,
                attention_request_timestamp: None,
                delegation_depth: None,
                sandbox_id: None,
                sandbox_path: None,
                sandbox_branch: None,
                dismissed_questions_message_id: None,
                pending_questions_message_id: None,
                pending_proposals: Vec::new(),
                proposal_resolutions: serde_json::Map::new(),
                last_seen_message_id: None,
                is_initial_agent: None,
                sponsor_agent_id: None,
            },
        }
    }

    impl WorkspaceApi for FakeApi {
        // Pin the `workspaceApi.*` output knobs to the legacy behavior (plain
        // pretty JSON, no size limit) so this fixture keeps asserting raw JSON
        // bodies; the TOON/limit paths are covered by
        // `workspace_api_output_limit_tests`.
        fn settings_get(&self, path: String) -> BoxFuture<'_, Result<Value>> {
            Box::pin(async move {
                let value = match path.as_str() {
                    "workspaceApi.toonOutput" => json!(false),
                    "workspaceApi.maxOutputChars" => json!(0),
                    "agents.maxTopLevelAgents" => self
                        .max_top_level_agents_setting
                        .lock()
                        .unwrap()
                        .clone()
                        .unwrap_or(Value::Null),
                    _ => Value::Null,
                };
                Ok(json!({ "path": path, "value": value }))
            })
        }

        fn agent_list(&self, ws: WorkspaceId) -> BoxFuture<'_, Result<Vec<AgentLite>>> {
            *self.agent_list_calls.lock().unwrap() += 1;
            let rows = self.agent_list_rows.lock().unwrap().clone();
            Box::pin(async move {
                Ok(rows.unwrap_or_else(|| vec![stub_agent("a-1", &ws), stub_agent("a-2", &ws)]))
            })
        }

        fn agent_get(
            &self,
            agent_id: AgentId,
            workspace_id: Option<WorkspaceId>,
        ) -> BoxFuture<'_, Result<AgentLite>> {
            let id = agent_id.as_str().to_string();
            self.agent_get_calls.lock().unwrap().push(id.clone());
            let ws = workspace_id.unwrap_or_else(|| WorkspaceId::from_string("amber-forest"));
            let error = self.agent_get_error.lock().unwrap().clone();
            let is_background = self.background_agent_ids.lock().unwrap().contains(&id);
            Box::pin(async move {
                if let Some(e) = error {
                    return Err(Error::NotFound(e));
                }
                let mut agent = stub_agent(&id, &ws);
                agent.metadata.is_background = is_background;
                Ok(agent)
            })
        }

        fn agent_get_queue(
            &self,
            _agent_id: AgentId,
            _workspace_id: Option<WorkspaceId>,
        ) -> BoxFuture<'_, Result<Value>> {
            let queue = self.queue_entries.lock().unwrap().clone();
            Box::pin(async move { Ok(json!({ "success": true, "queue": queue })) })
        }

        fn agent_remove_queued_message_owned(
            &self,
            agent_id: AgentId,
            message_id: String,
            caller_agent_id: AgentId,
        ) -> BoxFuture<'_, Result<Value>> {
            self.remove_queued_owned_calls.lock().unwrap().push((
                agent_id.as_str().to_string(),
                message_id.clone(),
                caller_agent_id.as_str().to_string(),
            ));
            self.call_order.lock().unwrap().push("remove");
            let error = self.remove_queued_error.lock().unwrap().clone();
            let internal = self.remove_queued_internal_error.lock().unwrap().clone();
            Box::pin(async move {
                if let Some(e) = error {
                    return Err(Error::NotFound(e));
                }
                if let Some(e) = internal {
                    return Err(Error::Internal(e));
                }
                Ok(json!({ "success": true, "messageId": message_id }))
            })
        }

        fn get_my_task(
            &self,
            _workspace_id: WorkspaceId,
            task_note_id: intent_core::NoteId,
        ) -> BoxFuture<'_, Result<TaskGetMyTaskResult>> {
            let assignee = self.task_assignee.lock().unwrap().clone();
            Box::pin(async move {
                let Some(assignee) = assignee else {
                    return Err(Error::NotFound("no task".to_string()));
                };
                Ok(TaskGetMyTaskResult {
                    note_id: task_note_id,
                    title: "task title".to_string(),
                    content: "body".to_string(),
                    status: TaskStatus::InProgress,
                    task_metadata: TaskMetadata {
                        status: TaskStatus::InProgress,
                        ..Default::default()
                    },
                    parent_id: None,
                    subtasks: Vec::new(),
                    assigned_agents: vec![AgentId::from(assignee.as_str())],
                    rev: 1,
                    unmet_depends_on: Vec::new(),
                })
            })
        }

        #[allow(clippy::too_many_arguments)]
        fn agent_send_message(
            &self,
            _ws: WorkspaceId,
            agent_id: AgentId,
            content: String,
            _message_id: Option<String>,
            _image_blocks: Option<Value>,
            _file_blocks: Option<Value>,
            priority: Option<String>,
            _note_ids: Option<Value>,
            _stdin_context: Option<String>,
            _context_references: Option<Value>,
            message_metadata: Option<Value>,
            _origin: intent_core::MessageOrigin,
        ) -> BoxFuture<'_, Result<Value>> {
            self.agent_send_calls.lock().unwrap().push((
                agent_id.as_str().to_string(),
                content,
                priority,
                message_metadata,
            ));
            self.call_order.lock().unwrap().push("send");
            let error = self.agent_send_error.lock().unwrap().clone();
            let result = self
                .agent_send_result
                .lock()
                .unwrap()
                .clone()
                .unwrap_or_else(
                    || json!({ "success": true, "queued": false, "turnId": "turn-fake-1" }),
                );
            Box::pin(async move {
                if let Some(e) = error {
                    return Err(Error::Internal(e));
                }
                Ok(result)
            })
        }

        fn agent_send_to_task(
            &self,
            _ws: WorkspaceId,
            task_note_id: intent_core::NoteId,
            message: String,
            priority: Option<String>,
            message_metadata: Option<Value>,
        ) -> BoxFuture<'_, Result<Value>> {
            self.agent_send_to_task_calls.lock().unwrap().push((
                task_note_id.as_str().to_string(),
                message,
                priority,
                message_metadata,
            ));
            self.call_order.lock().unwrap().push("send");
            let result = self.agent_send_to_task_result.lock().unwrap().clone();
            Box::pin(async move {
                Ok(result.unwrap_or_else(|| {
                    json!({
                        "ok": true,
                        "agentId": "agent-assignee",
                        "result": { "success": true, "queued": false, "turnId": "turn-fake-1" },
                    })
                }))
            })
        }

        fn agent_wake_or_create(
            &self,
            _ws: WorkspaceId,
            task_note_id: intent_core::NoteId,
            context_message: String,
            input: intent_core::AgentWakeOrCreateInput,
        ) -> BoxFuture<'_, Result<Value>> {
            self.agent_wake_or_create_calls.lock().unwrap().push((
                task_note_id.as_str().to_string(),
                context_message,
                input.model,
                input.message_metadata,
            ));
            Box::pin(async move {
                Ok(json!({ "ok": true, "agentId": "agent-woken", "action": "resumed" }))
            })
        }

        #[allow(clippy::too_many_arguments)]
        fn agent_create(
            &self,
            _ws: WorkspaceId,
            name: Option<String>,
            _model: Option<String>,
            specialist_id: Option<String>,
            parent_agent_id: Option<AgentId>,
            idempotency_key: Option<String>,
            extra: intent_core::AgentCreateExtra,
        ) -> BoxFuture<'_, Result<Value>> {
            self.agent_create_calls.lock().unwrap().push((
                name,
                specialist_id,
                parent_agent_id.map(|p| p.as_str().to_string()),
                idempotency_key,
                extra.metadata,
                extra.is_background,
            ));
            Box::pin(async move { Ok(json!({ "agent": { "id": "child-1", "name": "child" } })) })
        }

        fn agent_watch_completion(
            &self,
            _ws: WorkspaceId,
            _parent_agent_id: AgentId,
            _child_agent_id: AgentId,
        ) -> BoxFuture<'_, Result<Value>> {
            *self.agent_watch_completion_calls.lock().unwrap() += 1;
            Box::pin(async move { Ok(json!({ "ok": true, "subscriptionId": "watch-1" })) })
        }

        fn agent_delegate(
            &self,
            _ws: WorkspaceId,
            input: AgentDelegateInput,
            caller: Option<AgentId>,
        ) -> BoxFuture<'_, Result<Value>> {
            self.agent_delegate_calls.lock().unwrap().push((
                input.wait_mode.clone(),
                caller.as_ref().map(|c| c.as_str().to_string()),
            ));
            Box::pin(async move { Ok(json!({ "agent": { "id": "child-1", "name": "child" } })) })
        }

        fn agent_subscribe(
            &self,
            _ws: WorkspaceId,
            _subscriber: Option<AgentId>,
            event_types: Vec<String>,
            exclude_self: Option<bool>,
            batch_window: Option<i64>,
        ) -> BoxFuture<'_, Result<Value>> {
            self.agent_subscribe_calls.lock().unwrap().push((
                event_types.clone(),
                exclude_self,
                batch_window,
            ));
            Box::pin(
                async move { Ok(json!({ "subscriptionId": "sub-1", "eventTypes": event_types })) },
            )
        }

        fn agent_unsubscribe(
            &self,
            _ws: WorkspaceId,
            subscription_id: String,
        ) -> BoxFuture<'_, Result<Value>> {
            self.agent_unsubscribe_calls
                .lock()
                .unwrap()
                .push(subscription_id);
            Box::pin(async move { Ok(json!({ "ok": true })) })
        }

        fn agent_watch_completion_for_sender(
            &self,
            _ws: WorkspaceId,
            caller: AgentId,
            target: AgentId,
        ) -> BoxFuture<'_, Result<Value>> {
            self.watch_sender_calls
                .lock()
                .unwrap()
                .push((caller.as_str().to_string(), target.as_str().to_string()));
            Box::pin(async move { Ok(json!({ "ok": true, "subscriptionId": "sender-watch-1" })) })
        }

        fn agent_report_to_parent(
            &self,
            _ws: WorkspaceId,
            report: Value,
            caller: Option<AgentId>,
        ) -> BoxFuture<'_, Result<Value>> {
            self.report_to_parent_calls
                .lock()
                .unwrap()
                .push(caller.as_ref().map(|c| c.as_str().to_string()));
            let _ = report;
            Box::pin(async move { Ok(json!({ "success": true })) })
        }

        fn agent_request_attention(
            &self,
            _ws: WorkspaceId,
            kind: String,
            reason: String,
            caller: Option<AgentId>,
        ) -> BoxFuture<'_, Result<Value>> {
            self.request_attention_calls.lock().unwrap().push((
                kind.clone(),
                reason.clone(),
                caller.as_ref().map(|c| c.as_str().to_string()),
            ));
            Box::pin(async move {
                Ok(json!({
                    "ok": true,
                    "kind": kind,
                    "reason": reason,
                    "savedAt": "2026-01-01T00:00:00Z",
                }))
            })
        }

        fn agent_delete(
            &self,
            agent_id: AgentId,
            workspace_id: Option<WorkspaceId>,
        ) -> BoxFuture<'_, Result<Value>> {
            self.agent_delete_calls.lock().unwrap().push((
                agent_id.as_str().to_string(),
                workspace_id.as_ref().map(|w| w.as_str().to_string()),
            ));
            Box::pin(async move { Ok(json!({ "success": true })) })
        }

        fn agent_retire(
            &self,
            agent_id: AgentId,
            workspace_id: Option<WorkspaceId>,
            reason: Option<String>,
        ) -> BoxFuture<'_, Result<Value>> {
            self.agent_retire_calls.lock().unwrap().push((
                agent_id.as_str().to_string(),
                workspace_id.as_ref().map(|w| w.as_str().to_string()),
                reason,
            ));
            Box::pin(
                async move { Ok(json!({ "success": true, "retiredAt": "2026-01-01T00:00:00Z" })) },
            )
        }

        fn agent_is_retired(&self, agent_id: AgentId) -> BoxFuture<'_, bool> {
            let retired = self
                .retired_agent_ids
                .lock()
                .unwrap()
                .contains(&agent_id.as_str().to_string());
            Box::pin(async move { retired })
        }

        fn event_query(
            &self,
            _ws: WorkspaceId,
            params: EventQueryParams,
        ) -> BoxFuture<'_, Result<Value>> {
            self.event_query_calls.lock().unwrap().push(params);
            Box::pin(async move { Ok(json!([])) })
        }

        fn event_subscribe(
            &self,
            _ws: WorkspaceId,
            _subscriber: Option<AgentId>,
            event_types: Vec<String>,
            exclude_self: Option<bool>,
            batch_window: Option<i64>,
        ) -> BoxFuture<'_, Result<EventSubscribeResult>> {
            self.event_subscribe_calls.lock().unwrap().push((
                event_types.clone(),
                exclude_self,
                batch_window,
            ));
            Box::pin(async move {
                Ok(EventSubscribeResult {
                    subscription_id: "sub-1".to_string(),
                    event_types,
                })
            })
        }

        fn event_unsubscribe(
            &self,
            _ws: WorkspaceId,
            subscription_id: String,
        ) -> BoxFuture<'_, Result<EventUnsubscribeResult>> {
            Box::pin(async move {
                Ok(EventUnsubscribeResult {
                    ok: true,
                    subscription_id,
                })
            })
        }
    }

    fn server() -> (WorkspaceMcpServer, Arc<FakeApi>) {
        let api = Arc::new(FakeApi::default());
        let srv = WorkspaceMcpServer::new(api.clone(), WorkspaceId::from_string("amber-forest"));
        (srv, api)
    }

    fn server_with_caller(caller: &str) -> (WorkspaceMcpServer, Arc<FakeApi>) {
        let api = Arc::new(FakeApi::default());
        let srv = WorkspaceMcpServer::new(api.clone(), WorkspaceId::from_string("amber-forest"))
            .with_caller_agent_id(Some(AgentId::from(caller)));
        (srv, api)
    }

    async fn call(srv: &WorkspaceMcpServer, code: &str) -> Value {
        srv.handle_message(&json!({
            "jsonrpc": "2.0", "id": 1, "method": "tools/call",
            "params": {
                "name": "workspace_api",
                "arguments": { "code": code, "summary": "wsapi4 unit test" }
            }
        }))
        .await
        .expect("tools/call must produce a response")
    }

    fn body(resp: &Value) -> Value {
        let text = resp["result"]["content"][0]["text"].as_str().unwrap();
        serde_json::from_str(text).expect("workspace_api body must be JSON")
    }

    fn text(resp: &Value) -> String {
        resp["result"]["content"][0]["text"]
            .as_str()
            .unwrap()
            .to_string()
    }

    // ================================================================
    // agent.*
    // ================================================================

    #[tokio::test]
    async fn agent_list_returns_projected_rows() {
        let (srv, api) = server();
        let resp = call(&srv, "return await ws.agent.list();").await;
        assert_eq!(resp["result"]["isError"], json!(false));
        let v = body(&resp);
        let arr = v.as_array().unwrap();
        assert_eq!(arr.len(), 2);
        assert_eq!(arr[0]["id"], json!("a-1"));
        assert_eq!(*api.agent_list_calls.lock().unwrap(), 1);
    }

    /// Seed `agent_list_rows` with a live/completed top-level pair plus a
    /// live/errored child of `a-top` (the `ws.agent.list` filter tests).
    fn seed_agent_list_filter_rows(api: &FakeApi) {
        let ws = WorkspaceId::from_string("amber-forest");
        let top = stub_agent("a-top", &ws);
        let mut top_done = stub_agent("a-top-done", &ws);
        top_done.status = AgentStatus::Completed;
        let mut child = stub_agent("a-child", &ws);
        child.parent_agent_id = Some(AgentId::from("a-top"));
        let mut child_err = stub_agent("a-child-err", &ws);
        child_err.parent_agent_id = Some(AgentId::from("a-top"));
        child_err.status = AgentStatus::Error;
        *api.agent_list_rows.lock().unwrap() = Some(vec![top, top_done, child, child_err]);
    }

    async fn agent_list_ids(srv: &WorkspaceMcpServer, code: &str) -> Vec<String> {
        let resp = call(srv, code).await;
        assert_eq!(resp["result"]["isError"], json!(false), "{}", text(&resp));
        body(&resp)
            .as_array()
            .unwrap()
            .iter()
            .map(|r| r["id"].as_str().unwrap().to_string())
            .collect()
    }

    #[tokio::test]
    async fn agent_list_omits_terminal_rows_unless_include_completed() {
        let (srv, api) = server();
        seed_agent_list_filter_rows(&api);
        assert_eq!(
            agent_list_ids(&srv, "return await ws.agent.list();").await,
            ["a-top", "a-child"]
        );
        // Legacy bare-boolean form.
        assert_eq!(
            agent_list_ids(&srv, "return await ws.agent.list(true);").await,
            ["a-top", "a-top-done", "a-child", "a-child-err"]
        );
        // Object form.
        assert_eq!(
            agent_list_ids(
                &srv,
                "return await ws.agent.list({ includeCompleted: true });"
            )
            .await,
            ["a-top", "a-top-done", "a-child", "a-child-err"]
        );
    }

    #[tokio::test]
    async fn agent_list_scope_and_parent_filters() {
        let (srv, api) = server();
        seed_agent_list_filter_rows(&api);
        assert_eq!(
            agent_list_ids(&srv, "return await ws.agent.list({ scope: 'top-level' });").await,
            ["a-top"]
        );
        assert_eq!(
            agent_list_ids(&srv, "return await ws.agent.list({ scope: 'subagents' });").await,
            ["a-child"]
        );
        assert_eq!(
            agent_list_ids(
                &srv,
                "return await ws.agent.list({ parentAgentId: 'a-top', includeCompleted: true });"
            )
            .await,
            ["a-child", "a-child-err"]
        );
        assert_eq!(
            agent_list_ids(
                &srv,
                "return await ws.agent.list({ parentAgentId: 'a-other' });"
            )
            .await,
            Vec::<String>::new()
        );
    }

    #[tokio::test]
    async fn agent_list_rejects_invalid_filter_combos() {
        let (srv, _api) = server();
        let resp = call(&srv, "return await ws.agent.list({ scope: 'bogus' });").await;
        assert_eq!(resp["result"]["isError"], json!(true));
        let t = text(&resp);
        assert!(
            t.contains("\"top-level\"") && t.contains("\"subagents\""),
            "error must name the valid scope values: {t}"
        );

        let resp = call(
            &srv,
            "return await ws.agent.list({ scope: 'top-level', parentAgentId: 'a-top' });",
        )
        .await;
        assert_eq!(resp["result"]["isError"], json!(true));
        assert!(
            text(&resp).contains("cannot be combined"),
            "{}",
            text(&resp)
        );
    }

    #[tokio::test]
    async fn agent_status_forwards_agent_id() {
        let (srv, api) = server();
        let resp = call(&srv, "return await ws.agent.status('a-42');").await;
        assert_eq!(resp["result"]["isError"], json!(false));
        let v = body(&resp);
        assert_eq!(v["id"], json!("a-42"));
        assert_eq!(api.agent_get_calls.lock().unwrap()[0], "a-42");
        // The status result carries the merged (empty) queue view.
        assert_eq!(v["queueLength"], json!(0));
        assert_eq!(v["queue"], json!([]));
    }

    #[tokio::test]
    async fn agent_status_merges_truncated_queue() {
        let (srv, api) = server();
        *api.queue_entries.lock().unwrap() = vec![json!({
            "id": "q-1",
            "content": "y".repeat(300),
            "queuedAt": "2026-01-01T00:00:00Z",
            "position": 0,
            "messageMetadata": { "fromAgentId": "a-9", "fromAgentName": "Nine" },
        })];
        let resp = call(&srv, "return await ws.agent.status('a-42');").await;
        assert_eq!(resp["result"]["isError"], json!(false));
        let v = body(&resp);
        assert_eq!(v["queueLength"], json!(1));
        let content = v["queue"][0]["content"].as_str().unwrap();
        assert_eq!(content.chars().count(), 201, "200 chars + ellipsis");
        assert!(content.ends_with('…'));
        assert_eq!(v["queue"][0]["fromAgentId"], json!("a-9"));
        assert_eq!(v["queue"][0]["fromAgentName"], json!("Nine"));
    }

    #[tokio::test]
    async fn agent_get_queue_presents_drain_order_and_attribution() {
        let (srv, api) = server();
        *api.queue_entries.lock().unwrap() = vec![
            json!({
                "id": "q-normal",
                "content": "normal",
                "queuedAt": "2026-01-01T00:00:00Z",
                "position": 0,
                "messageMetadata": { "fromAgentId": "a-9", "fromAgentName": "Nine" },
            }),
            json!({
                "id": "q-interrupt",
                "content": "urgent",
                "queuedAt": "2026-01-01T00:00:01Z",
                "position": 1,
                "interruptPriority": true,
            }),
        ];
        let resp = call(&srv, "return await ws.agent.getQueue('a-42');").await;
        assert_eq!(resp["result"]["isError"], json!(false));
        let v = body(&resp);
        assert_eq!(v["ok"], json!(true));
        assert_eq!(v["agentId"], json!("a-42"));
        assert_eq!(v["queueLength"], json!(2));
        // Next delivery first: the interrupt entry ahead of normal FIFO.
        assert_eq!(v["queue"][0]["id"], json!("q-interrupt"));
        assert_eq!(v["queue"][0]["position"], json!(0));
        assert_eq!(v["queue"][1]["id"], json!("q-normal"));
        assert_eq!(v["queue"][1]["fromAgentId"], json!("a-9"));
        assert!(v["queue"][1].get("messageMetadata").is_none());
    }

    #[tokio::test]
    async fn agent_remove_queued_message_forwards_caller_identity() {
        let (srv, api) = server_with_caller("caller-1");
        let resp = call(
            &srv,
            "return await ws.agent.removeQueuedMessage('a-42', 'q-7');",
        )
        .await;
        assert_eq!(resp["result"]["isError"], json!(false));
        let v = body(&resp);
        assert_eq!(v["ok"], json!(true));
        assert_eq!(v["agentId"], json!("a-42"));
        assert_eq!(v["messageId"], json!("q-7"));
        let calls = api.remove_queued_owned_calls.lock().unwrap();
        assert_eq!(
            calls[0],
            (
                "a-42".to_string(),
                "q-7".to_string(),
                "caller-1".to_string()
            )
        );
    }

    #[tokio::test]
    async fn agent_remove_queued_message_requires_caller() {
        let (srv, api) = server();
        let resp = call(
            &srv,
            "return await ws.agent.removeQueuedMessage('a-42', 'q-7');",
        )
        .await;
        assert_eq!(resp["result"]["isError"], json!(true));
        assert!(api.remove_queued_owned_calls.lock().unwrap().is_empty());
    }

    #[tokio::test]
    async fn agent_send_priority_interrupt_forwards_to_daemon() {
        let (srv, api) = server();
        let resp = call(
            &srv,
            "return await ws.agent.send('a-1', 'stop', 'interrupt');",
        )
        .await;
        assert_eq!(resp["result"]["isError"], json!(false));
        let v = body(&resp);
        assert_eq!(v["agentId"], json!("a-1"));
        assert_eq!(v["ok"], json!(true));
        let calls = api.agent_send_calls.lock().unwrap();
        assert_eq!(calls[0].2.as_deref(), Some("interrupt"));
    }

    /// Self-describing success: a delivered-now send (turn-driving, carries
    /// `turnId`) reports the top-level `delivery: "delivered"` outcome.
    #[tokio::test]
    async fn agent_send_delivered_shape_carries_delivery_field() {
        let (srv, _api) = server();
        let resp = call(&srv, "return await ws.agent.send('a-1', 'hi');").await;
        assert_eq!(resp["result"]["isError"], json!(false));
        let v = body(&resp);
        assert_eq!(v["ok"], json!(true));
        assert_eq!(v["delivery"], json!("delivered"));
    }

    /// The store-only fallback's persist-only success (`queued: false`, no
    /// `turnId`) drives no turn — the binding makes no `delivery` claim
    /// rather than reporting a false "delivered".
    #[tokio::test]
    async fn agent_send_persist_only_shape_makes_no_delivery_claim() {
        let (srv, api) = server();
        *api.agent_send_result.lock().unwrap() = Some(json!({
            "success": true,
            "queued": false,
            "messageId": "m-1",
        }));
        let resp = call(&srv, "return await ws.agent.send('a-1', 'hi');").await;
        assert_eq!(resp["result"]["isError"], json!(false));
        let v = body(&resp);
        assert_eq!(v["ok"], json!(true));
        assert!(v.get("delivery").is_none(), "{v}");
    }

    /// Self-describing success: a queued-because-busy send is explicit —
    /// `delivery: "queued"` — so `ok: true` is not read as "delivered now".
    #[tokio::test]
    async fn agent_send_queued_shape_carries_delivery_field() {
        let (srv, api) = server();
        *api.agent_send_result.lock().unwrap() = Some(json!({
            "success": true,
            "queued": true,
            "queuedMessage": { "id": "qmsg-1" },
        }));
        let resp = call(&srv, "return await ws.agent.send('a-1', 'hi');").await;
        assert_eq!(resp["result"]["isError"], json!(false));
        let v = body(&resp);
        assert_eq!(v["ok"], json!(true));
        assert_eq!(v["delivery"], json!("queued"));
    }

    /// Self-describing success: a held-for-questions park reports
    /// `delivery: "held"`.
    #[tokio::test]
    async fn agent_send_held_shape_carries_delivery_field() {
        let (srv, api) = server();
        *api.agent_send_result.lock().unwrap() = Some(json!({
            "success": true,
            "queued": true,
            "heldForQuestions": true,
            "queuedMessage": { "id": "qmsg-1" },
        }));
        let resp = call(&srv, "return await ws.agent.send('a-1', 'hi');").await;
        assert_eq!(resp["result"]["isError"], json!(false));
        let v = body(&resp);
        assert_eq!(v["ok"], json!(true));
        assert_eq!(v["delivery"], json!("held"));
    }

    /// `sendToTask` classifies the nested `result` envelope the op returns.
    #[tokio::test]
    async fn agent_send_to_task_delivery_field_reads_nested_result() {
        let (srv, api) = server();
        let resp = call(&srv, "return await ws.agent.sendToTask('tn-1', 'hi');").await;
        assert_eq!(resp["result"]["isError"], json!(false));
        let v = body(&resp);
        assert_eq!(v["delivery"], json!("delivered"));

        *api.agent_send_to_task_result.lock().unwrap() = Some(json!({
            "ok": true,
            "agentId": "agent-assignee",
            "result": { "success": true, "queued": true, "heldForQuestions": true },
        }));
        let resp = call(&srv, "return await ws.agent.sendToTask('tn-1', 'hi');").await;
        assert_eq!(resp["result"]["isError"], json!(false));
        let v = body(&resp);
        assert_eq!(v["delivery"], json!("held"));
    }

    /// A non-success `sendToTask` result (e.g. no assignee) gains no
    /// `delivery` claim — nothing was sent.
    #[tokio::test]
    async fn agent_send_to_task_failure_has_no_delivery_field() {
        let (srv, api) = server();
        *api.agent_send_to_task_result.lock().unwrap() = Some(json!({
            "ok": false,
            "delivered": false,
            "error": "No agent assigned to task",
        }));
        let resp = call(&srv, "return await ws.agent.sendToTask('tn-1', 'hi');").await;
        assert_eq!(resp["result"]["isError"], json!(false));
        let v = body(&resp);
        assert_eq!(v["ok"], json!(false));
        assert!(v.get("delivery").is_none(), "{v}");
    }

    /// Single-pending-message refusal: `refused: true` discriminator, an
    /// `error` naming the rule, and an `instruction` pointing at the atomic
    /// `replacePending` option (with manual removeQueuedMessage as the
    /// non-atomic fallback).
    #[tokio::test]
    async fn agent_send_refusal_is_self_describing() {
        let (srv, api) = server_with_caller("caller-1");
        *api.queue_entries.lock().unwrap() = vec![json!({
            "id": "qmsg-pending",
            "content": "earlier send",
            "queuedAt": "2026-01-01T00:00:00Z",
            "position": 0,
            "messageMetadata": { "fromAgentId": "caller-1", "fromAgentName": "Caller" },
        })];
        let resp = call(&srv, "return await ws.agent.send('a-1', 'hi');").await;
        assert_eq!(resp["result"]["isError"], json!(false));
        let v = body(&resp);
        assert_eq!(v["ok"], json!(false));
        assert_eq!(v["refused"], json!(true));
        assert_eq!(v["pendingMessageId"], json!("qmsg-pending"));
        let error = v["error"].as_str().unwrap();
        assert!(
            error.contains("only one pending message per target"),
            "error names the rule: {error}"
        );
        let instruction = v["instruction"].as_str().unwrap();
        assert!(
            instruction.contains("replacePending"),
            "instruction recommends the atomic replace: {instruction}"
        );
        assert!(instruction.contains("removeQueuedMessage"), "{instruction}");
        assert!(
            instruction.contains("NOT atomic"),
            "instruction warns manual remove + re-send is not atomic: {instruction}"
        );
        assert!(
            api.agent_send_calls.lock().unwrap().is_empty(),
            "refused send never reaches the service layer"
        );
    }

    /// `replacePending: true` with a pending entry present: the entry is
    /// retracted (ownership-guarded removal with the caller's identity), the
    /// send proceeds, and the result reports `replaced: true` +
    /// `replacedMessageId`.
    #[tokio::test]
    async fn agent_send_replace_pending_retracts_and_sends() {
        let (srv, api) = server_with_caller("caller-1");
        *api.queue_entries.lock().unwrap() = vec![json!({
            "id": "qmsg-pending",
            "content": "earlier send",
            "queuedAt": "2026-01-01T00:00:00Z",
            "position": 0,
            "messageMetadata": { "fromAgentId": "caller-1", "fromAgentName": "Caller" },
        })];
        let resp = call(
            &srv,
            "return await ws.agent.send('a-1', 'hi', { replacePending: true });",
        )
        .await;
        assert_eq!(resp["result"]["isError"], json!(false));
        let v = body(&resp);
        assert_eq!(v["ok"], json!(true), "{v}");
        assert_eq!(v["replaced"], json!(true), "{v}");
        assert_eq!(v["replacedMessageId"], json!("qmsg-pending"), "{v}");
        assert!(v.get("refused").is_none(), "{v}");
        let removals = api.remove_queued_owned_calls.lock().unwrap();
        assert_eq!(
            removals[0],
            (
                "a-1".to_string(),
                "qmsg-pending".to_string(),
                "caller-1".to_string()
            )
        );
        let sends = api.agent_send_calls.lock().unwrap();
        assert_eq!(sends[0].1, "hi");
        // Options-object third argument without `priority` keeps the
        // interrupt default.
        assert_eq!(sends[0].2.as_deref(), Some("interrupt"));
        // Lossless ordering: the new message is sent BEFORE the pending
        // entry is retracted, so a failed send never discards the entry.
        assert_eq!(*api.call_order.lock().unwrap(), vec!["send", "remove"]);
    }

    /// Lossless replace: when the send itself fails, the pending entry is
    /// never retracted — the caller gets the send error and the original
    /// message stays in the target's queue.
    #[tokio::test]
    async fn agent_send_replace_pending_failed_send_retracts_nothing() {
        let (srv, api) = server_with_caller("caller-1");
        *api.queue_entries.lock().unwrap() = vec![json!({
            "id": "qmsg-pending",
            "content": "earlier send",
            "queuedAt": "2026-01-01T00:00:00Z",
            "position": 0,
            "messageMetadata": { "fromAgentId": "caller-1", "fromAgentName": "Caller" },
        })];
        *api.agent_send_error.lock().unwrap() = Some("service failure".to_string());
        let resp = call(
            &srv,
            "return await ws.agent.send('a-1', 'hi', { replacePending: true });",
        )
        .await;
        assert_eq!(resp["result"]["isError"], json!(true), "{resp}");
        assert!(
            api.remove_queued_owned_calls.lock().unwrap().is_empty(),
            "a failed send must not retract the pending entry"
        );
    }

    /// `replacePending: true` when the pending entry drains between the guard
    /// check and the retraction (removal fails with NotFound): the new
    /// message was already sent — graceful degradation — and the result
    /// reports `replaced: false` + `replaceOutcome: "drained"`.
    #[tokio::test]
    async fn agent_send_replace_pending_drained_entry_degrades_to_plain_send() {
        let (srv, api) = server_with_caller("caller-1");
        *api.queue_entries.lock().unwrap() = vec![json!({
            "id": "qmsg-pending",
            "content": "earlier send",
            "queuedAt": "2026-01-01T00:00:00Z",
            "position": 0,
            "messageMetadata": { "fromAgentId": "caller-1", "fromAgentName": "Caller" },
        })];
        *api.remove_queued_error.lock().unwrap() = Some("message not found".to_string());
        let resp = call(
            &srv,
            "return await ws.agent.send('a-1', 'hi', { replacePending: true });",
        )
        .await;
        assert_eq!(resp["result"]["isError"], json!(false));
        let v = body(&resp);
        assert_eq!(v["ok"], json!(true), "{v}");
        assert_eq!(v["replaced"], json!(false), "{v}");
        assert_eq!(v["replaceOutcome"], json!("drained"), "{v}");
        assert!(v.get("replacedMessageId").is_none(), "{v}");
        assert_eq!(api.agent_send_calls.lock().unwrap().len(), 1);
    }

    /// A non-NotFound removal failure (infrastructure error) does not
    /// masquerade as a drained race: the send already succeeded and the
    /// result reports `replaceOutcome: "error"`.
    #[tokio::test]
    async fn agent_send_replace_pending_removal_error_reports_error_outcome() {
        let (srv, api) = server_with_caller("caller-1");
        *api.queue_entries.lock().unwrap() = vec![json!({
            "id": "qmsg-pending",
            "content": "earlier send",
            "queuedAt": "2026-01-01T00:00:00Z",
            "position": 0,
            "messageMetadata": { "fromAgentId": "caller-1", "fromAgentName": "Caller" },
        })];
        *api.remove_queued_internal_error.lock().unwrap() = Some("db unavailable".to_string());
        let resp = call(
            &srv,
            "return await ws.agent.send('a-1', 'hi', { replacePending: true });",
        )
        .await;
        assert_eq!(resp["result"]["isError"], json!(false));
        let v = body(&resp);
        assert_eq!(v["ok"], json!(true), "{v}");
        assert_eq!(v["replaced"], json!(false), "{v}");
        assert_eq!(v["replaceOutcome"], json!("error"), "{v}");
        assert_eq!(api.agent_send_calls.lock().unwrap().len(), 1);
    }

    /// `replacePending: true` with no pending entry at all: nothing to
    /// retract, the send proceeds, and the result reports `replaced: false`
    /// + `replaceOutcome: "none"`.
    #[tokio::test]
    async fn agent_send_replace_pending_without_entry_reports_none() {
        let (srv, api) = server_with_caller("caller-1");
        let resp = call(
            &srv,
            "return await ws.agent.send('a-1', 'hi', { replacePending: true });",
        )
        .await;
        assert_eq!(resp["result"]["isError"], json!(false));
        let v = body(&resp);
        assert_eq!(v["ok"], json!(true), "{v}");
        assert_eq!(v["replaced"], json!(false), "{v}");
        assert_eq!(v["replaceOutcome"], json!("none"), "{v}");
        assert!(api.remove_queued_owned_calls.lock().unwrap().is_empty());
        assert_eq!(api.agent_send_calls.lock().unwrap().len(), 1);
    }

    /// `replacePending` from a caller-less (user/FE) context is a no-op: the
    /// guard never applies, no replace report is attached.
    #[tokio::test]
    async fn agent_send_replace_pending_without_caller_is_noop() {
        let (srv, api) = server();
        let resp = call(
            &srv,
            "return await ws.agent.send('a-1', 'hi', { replacePending: true });",
        )
        .await;
        assert_eq!(resp["result"]["isError"], json!(false));
        let v = body(&resp);
        assert_eq!(v["ok"], json!(true), "{v}");
        assert!(v.get("replaced").is_none(), "{v}");
        assert!(v.get("replaceOutcome").is_none(), "{v}");
        assert!(api.remove_queued_owned_calls.lock().unwrap().is_empty());
    }

    /// Options-object third argument carries `priority` alongside
    /// `replacePending` — the `"queue"` opt-out still maps to `"normal"`.
    #[tokio::test]
    async fn agent_send_options_object_priority_queue_opts_out() {
        let (srv, api) = server();
        let resp = call(
            &srv,
            "return await ws.agent.send('a-1', 'hi', { priority: 'queue' });",
        )
        .await;
        assert_eq!(resp["result"]["isError"], json!(false));
        let calls = api.agent_send_calls.lock().unwrap();
        assert_eq!(calls[0].2.as_deref(), Some("normal"));
    }

    /// `sendToTask` with `replacePending: true` against an assigned target
    /// holding the caller's pending entry: same send-then-retract as `send`,
    /// tagged with the task note id.
    #[tokio::test]
    async fn agent_send_to_task_replace_pending_retracts_and_sends() {
        let (srv, api) = server_with_caller("caller-1");
        *api.task_assignee.lock().unwrap() = Some("agent-assignee".to_string());
        *api.queue_entries.lock().unwrap() = vec![json!({
            "id": "qmsg-pending",
            "content": "earlier send",
            "queuedAt": "2026-01-01T00:00:00Z",
            "position": 0,
            "messageMetadata": { "fromAgentId": "caller-1", "fromAgentName": "Caller" },
        })];
        let resp = call(
            &srv,
            "return await ws.agent.sendToTask('tn-1', 'hi', { replacePending: true });",
        )
        .await;
        assert_eq!(resp["result"]["isError"], json!(false));
        let v = body(&resp);
        assert_eq!(v["ok"], json!(true), "{v}");
        assert_eq!(v["taskNoteId"], json!("tn-1"), "{v}");
        assert_eq!(v["replaced"], json!(true), "{v}");
        assert_eq!(v["replacedMessageId"], json!("qmsg-pending"), "{v}");
        let removals = api.remove_queued_owned_calls.lock().unwrap();
        assert_eq!(
            removals[0],
            (
                "agent-assignee".to_string(),
                "qmsg-pending".to_string(),
                "caller-1".to_string()
            )
        );
        assert_eq!(api.agent_send_to_task_calls.lock().unwrap().len(), 1);
        // Lossless ordering: send before retract.
        assert_eq!(*api.call_order.lock().unwrap(), vec!["send", "remove"]);
    }

    /// `sendToTask` with `replacePending: true` when the op resolves a
    /// different assignee than the guard did (mid-call reassignment): the
    /// pending entry in the old assignee's queue is NOT retracted and the
    /// result reports `replaceOutcome: "reassigned"`.
    #[tokio::test]
    async fn agent_send_to_task_replace_pending_reassigned_retracts_nothing() {
        let (srv, api) = server_with_caller("caller-1");
        *api.task_assignee.lock().unwrap() = Some("agent-old-assignee".to_string());
        *api.queue_entries.lock().unwrap() = vec![json!({
            "id": "qmsg-pending",
            "content": "earlier send",
            "queuedAt": "2026-01-01T00:00:00Z",
            "position": 0,
            "messageMetadata": { "fromAgentId": "caller-1", "fromAgentName": "Caller" },
        })];
        // The op resolves "agent-assignee" (fixture default) — a different
        // agent than the guard's "agent-old-assignee".
        let resp = call(
            &srv,
            "return await ws.agent.sendToTask('tn-1', 'hi', { replacePending: true });",
        )
        .await;
        assert_eq!(resp["result"]["isError"], json!(false));
        let v = body(&resp);
        assert_eq!(v["ok"], json!(true), "{v}");
        assert_eq!(v["replaced"], json!(false), "{v}");
        assert_eq!(v["replaceOutcome"], json!("reassigned"), "{v}");
        assert!(v.get("replacedMessageId").is_none(), "{v}");
        assert!(
            api.remove_queued_owned_calls.lock().unwrap().is_empty(),
            "a reassigned target must not have the old assignee's entry retracted"
        );
        assert_eq!(api.agent_send_to_task_calls.lock().unwrap().len(), 1);
    }

    /// `sendToTask` with `replacePending: true` when the guard's target
    /// resolution falls through (no task assignee): the op still runs and an
    /// agent caller gets an explicit `replaceOutcome: "none"` rather than a
    /// silently ignored option.
    #[tokio::test]
    async fn agent_send_to_task_replace_pending_guard_fallthrough_reports_none() {
        let (srv, api) = server_with_caller("caller-1");
        // task_assignee unset → get_my_task errors → guard falls through.
        let resp = call(
            &srv,
            "return await ws.agent.sendToTask('tn-1', 'hi', { replacePending: true });",
        )
        .await;
        assert_eq!(resp["result"]["isError"], json!(false));
        let v = body(&resp);
        assert_eq!(v["ok"], json!(true), "{v}");
        assert_eq!(v["replaced"], json!(false), "{v}");
        assert_eq!(v["replaceOutcome"], json!("none"), "{v}");
        assert!(api.remove_queued_owned_calls.lock().unwrap().is_empty());
        assert_eq!(api.agent_send_to_task_calls.lock().unwrap().len(), 1);
    }

    /// `sendToTask` still refuses without `replacePending` when the caller
    /// has a pending entry in the assignee's queue, and the refusal carries
    /// the task tag.
    #[tokio::test]
    async fn agent_send_to_task_refusal_without_replace_pending() {
        let (srv, api) = server_with_caller("caller-1");
        *api.task_assignee.lock().unwrap() = Some("agent-assignee".to_string());
        *api.queue_entries.lock().unwrap() = vec![json!({
            "id": "qmsg-pending",
            "content": "earlier send",
            "queuedAt": "2026-01-01T00:00:00Z",
            "position": 0,
            "messageMetadata": { "fromAgentId": "caller-1", "fromAgentName": "Caller" },
        })];
        let resp = call(&srv, "return await ws.agent.sendToTask('tn-1', 'hi');").await;
        assert_eq!(resp["result"]["isError"], json!(false));
        let v = body(&resp);
        assert_eq!(v["ok"], json!(false), "{v}");
        assert_eq!(v["refused"], json!(true), "{v}");
        assert_eq!(v["taskNoteId"], json!("tn-1"), "{v}");
        assert!(api.agent_send_to_task_calls.lock().unwrap().is_empty());
    }

    /// Omitted `priority` defaults to INTERRUPT delivery: the binding
    /// resolves the absent argument to `"interrupt"` before hitting the
    /// service layer (binding-local — wire RPC defaults are untouched).
    #[tokio::test]
    async fn agent_send_omitted_priority_defaults_to_interrupt() {
        let (srv, api) = server();
        let resp = call(&srv, "return await ws.agent.send('a-1', 'hi');").await;
        assert_eq!(resp["result"]["isError"], json!(false));
        let calls = api.agent_send_calls.lock().unwrap();
        assert_eq!(calls[0].2.as_deref(), Some("interrupt"));
    }

    /// A `null` priority is the same as omitting it — interrupt delivery.
    #[tokio::test]
    async fn agent_send_null_priority_defaults_to_interrupt() {
        let (srv, api) = server();
        let resp = call(&srv, "return await ws.agent.send('a-1', 'hi', null);").await;
        assert_eq!(resp["result"]["isError"], json!(false));
        let calls = api.agent_send_calls.lock().unwrap();
        assert_eq!(calls[0].2.as_deref(), Some("interrupt"));
    }

    /// `priority: "queue"` is the explicit opt-out: it maps to the
    /// non-interrupt `"normal"` so the send queues if the target is busy.
    #[tokio::test]
    async fn agent_send_priority_queue_opts_out_of_interrupt() {
        let (srv, api) = server();
        let resp = call(&srv, "return await ws.agent.send('a-1', 'hi', 'queue');").await;
        assert_eq!(resp["result"]["isError"], json!(false));
        let calls = api.agent_send_calls.lock().unwrap();
        assert_eq!(calls[0].2.as_deref(), Some("normal"));
    }

    /// `sendToTask` shares the interrupt-by-default resolution and the
    /// `"queue"` opt-out.
    #[tokio::test]
    async fn agent_send_to_task_priority_defaults_and_queue_opt_out() {
        let (srv, api) = server();
        let resp = call(&srv, "return await ws.agent.sendToTask('tn-1', 'hi');").await;
        assert_eq!(resp["result"]["isError"], json!(false));
        let resp = call(
            &srv,
            "return await ws.agent.sendToTask('tn-1', 'hi', 'queue');",
        )
        .await;
        assert_eq!(resp["result"]["isError"], json!(false));
        let resp = call(
            &srv,
            "return await ws.agent.sendToTask('tn-1', 'hi', 'interrupt');",
        )
        .await;
        assert_eq!(resp["result"]["isError"], json!(false));
        let calls = api.agent_send_to_task_calls.lock().unwrap();
        assert_eq!(calls[0].2.as_deref(), Some("interrupt"));
        assert_eq!(calls[1].2.as_deref(), Some("normal"));
        assert_eq!(calls[2].2.as_deref(), Some("interrupt"));
    }

    #[tokio::test]
    async fn agent_send_with_caller_registers_sub1_watch() {
        let (srv, api) = server_with_caller("caller-1");
        let resp = call(&srv, "return await ws.agent.send('a-1', 'hi');").await;
        assert_eq!(resp["result"]["isError"], json!(false));
        let v = body(&resp);
        assert_eq!(v["subscriptionId"], json!("sender-watch-1"));
        let calls = api.watch_sender_calls.lock().unwrap();
        assert_eq!(calls[0], ("caller-1".to_string(), "a-1".to_string()));
    }

    #[tokio::test]
    async fn agent_send_without_caller_skips_sub1_watch() {
        let (srv, api) = server();
        let resp = call(&srv, "return await ws.agent.send('a-1', 'hi');").await;
        assert_eq!(resp["result"]["isError"], json!(false));
        let v = body(&resp);
        assert!(v
            .get("subscriptionId")
            .is_none_or(serde_json::Value::is_null));
        assert!(api.watch_sender_calls.lock().unwrap().is_empty());
    }

    #[tokio::test]
    async fn agent_send_missing_message_surfaces_reference_error() {
        let (srv, _api) = server();
        let resp = call(&srv, "return await ws.agent.send('a-1');").await;
        assert_eq!(resp["result"]["isError"], json!(true));
        assert!(text(&resp).contains("message is required"));
    }

    /// Agent-to-agent sends carry the `agent_message` sender-attribution
    /// metadata (`fromAgentId` + `fromAgentName` resolved via `agent_get`).
    #[tokio::test]
    async fn agent_send_with_caller_tags_sender_metadata() {
        let (srv, api) = server_with_caller("caller-1");
        let resp = call(&srv, "return await ws.agent.send('a-1', 'hi');").await;
        assert_eq!(resp["result"]["isError"], json!(false));
        let calls = api.agent_send_calls.lock().unwrap();
        assert_eq!(
            calls[0].3,
            Some(json!({
                "type": "agent_message",
                "fromAgentId": "caller-1",
                "fromAgentName": "agent-caller-1",
            }))
        );
    }

    /// The attribution schema stays stable when the caller name lookup fails:
    /// `fromAgentName` is present but `null`, never omitted.
    #[tokio::test]
    async fn agent_send_name_lookup_failure_keeps_null_from_agent_name() {
        let (srv, api) = server_with_caller("caller-1");
        *api.agent_get_error.lock().unwrap() = Some("agent not found".to_string());
        let resp = call(&srv, "return await ws.agent.send('a-1', 'hi');").await;
        assert_eq!(resp["result"]["isError"], json!(false));
        let calls = api.agent_send_calls.lock().unwrap();
        assert_eq!(
            calls[0].3,
            Some(json!({
                "type": "agent_message",
                "fromAgentId": "caller-1",
                "fromAgentName": null,
            }))
        );
    }

    /// Caller-less (FE/RPC front door) sends stay untagged — human sends must
    /// not grow an `agent_message` block.
    #[tokio::test]
    async fn agent_send_without_caller_has_no_sender_metadata() {
        let (srv, api) = server();
        let resp = call(&srv, "return await ws.agent.send('a-1', 'hi');").await;
        assert_eq!(resp["result"]["isError"], json!(false));
        let calls = api.agent_send_calls.lock().unwrap();
        assert_eq!(calls[0].3, None);
    }

    /// `sendToTask` threads the same sender-attribution metadata through the
    /// new `agent_send_to_task` metadata parameter.
    #[tokio::test]
    async fn agent_send_to_task_with_caller_tags_sender_metadata() {
        let (srv, api) = server_with_caller("caller-1");
        let resp = call(&srv, "return await ws.agent.sendToTask('tn-1', 'hi');").await;
        assert_eq!(resp["result"]["isError"], json!(false));
        let calls = api.agent_send_to_task_calls.lock().unwrap();
        assert_eq!(calls[0].0, "tn-1");
        assert_eq!(
            calls[0].3,
            Some(json!({
                "type": "agent_message",
                "fromAgentId": "caller-1",
                "fromAgentName": "agent-caller-1",
            }))
        );
    }

    #[tokio::test]
    async fn agent_send_to_task_without_caller_has_no_sender_metadata() {
        let (srv, api) = server();
        let resp = call(&srv, "return await ws.agent.sendToTask('tn-1', 'hi');").await;
        assert_eq!(resp["result"]["isError"], json!(false));
        let calls = api.agent_send_to_task_calls.lock().unwrap();
        assert_eq!(calls[0].3, None);
    }

    /// `create()`'s initial-message delivery to the child is also an
    /// agent-originated send and carries the same attribution block.
    #[tokio::test]
    async fn agent_create_initial_message_tags_sender_metadata() {
        let (srv, api) = server_with_caller("caller-1");
        let resp = call(&srv, "return await ws.agent.create('child', 'go');").await;
        assert_eq!(resp["result"]["isError"], json!(false));
        let calls = api.agent_send_calls.lock().unwrap();
        assert_eq!(calls[0].0, "child-1");
        assert_eq!(
            calls[0].3,
            Some(json!({
                "type": "agent_message",
                "fromAgentId": "caller-1",
                "fromAgentName": "agent-caller-1",
            }))
        );
    }

    /// Explicit `messageMetadata` on `send` keeps its own fields, but the
    /// attribution fields are daemon-stamped — the guard/ownership key
    /// (`fromAgentId`) can be neither omitted nor spoofed by the caller.
    #[tokio::test]
    async fn agent_send_explicit_metadata_merged_with_stamped_attribution() {
        let (srv, api) = server_with_caller("caller-1");
        let resp = call(
            &srv,
            "return await ws.agent.send('a-1', 'hi', null, { type: 'custom', reason: 'x' });",
        )
        .await;
        assert_eq!(resp["result"]["isError"], json!(false));
        let calls = api.agent_send_calls.lock().unwrap();
        assert_eq!(
            calls[0].3,
            Some(json!({
                "type": "custom",
                "reason": "x",
                "fromAgentId": "caller-1",
                "fromAgentName": "agent-caller-1",
            }))
        );
    }

    /// A spoofed `fromAgentId` in explicit metadata is overwritten with the
    /// real caller identity — attribution is daemon-stamped, never trusted.
    #[tokio::test]
    async fn agent_send_spoofed_from_agent_id_is_overwritten() {
        let (srv, api) = server_with_caller("caller-1");
        let resp = call(
            &srv,
            "return await ws.agent.send('a-1', 'hi', null, { fromAgentId: 'agent-victim', fromAgentName: 'Victim' });",
        )
        .await;
        assert_eq!(resp["result"]["isError"], json!(false));
        let calls = api.agent_send_calls.lock().unwrap();
        assert_eq!(
            calls[0].3,
            Some(json!({
                "fromAgentId": "caller-1",
                "fromAgentName": "agent-caller-1",
            }))
        );
    }

    /// Explicit `messageMetadata` on `sendToTask` keeps its own fields with
    /// the attribution fields daemon-stamped.
    #[tokio::test]
    async fn agent_send_to_task_explicit_metadata_merged_with_stamped_attribution() {
        let (srv, api) = server_with_caller("caller-1");
        let resp = call(
            &srv,
            "return await ws.agent.sendToTask('tn-1', 'hi', null, { type: 'custom' });",
        )
        .await;
        assert_eq!(resp["result"]["isError"], json!(false));
        let calls = api.agent_send_to_task_calls.lock().unwrap();
        assert_eq!(
            calls[0].3,
            Some(json!({
                "type": "custom",
                "fromAgentId": "caller-1",
                "fromAgentName": "agent-caller-1",
            }))
        );
    }

    /// Explicit `messageMetadata` in `create()` opts keeps its own fields on
    /// the kickoff delivery, with the attribution fields daemon-stamped.
    #[tokio::test]
    async fn agent_create_explicit_metadata_merged_with_stamped_attribution() {
        let (srv, api) = server_with_caller("caller-1");
        let resp = call(
            &srv,
            "return await ws.agent.create('child', 'go', { messageMetadata: { type: 'custom' } });",
        )
        .await;
        assert_eq!(resp["result"]["isError"], json!(false));
        let calls = api.agent_send_calls.lock().unwrap();
        assert_eq!(calls[0].0, "child-1");
        assert_eq!(
            calls[0].3,
            Some(json!({
                "type": "custom",
                "fromAgentId": "caller-1",
                "fromAgentName": "agent-caller-1",
            }))
        );
    }

    /// `wakeOrCreate`'s delivered context message is an agent-originated send
    /// and must carry the same `agent_message` attribution block as `send` /
    /// `sendToTask` (monorepo#1015). The caller name is reused from the
    /// depth-guard lookup — no second `agent_get` round-trip.
    #[tokio::test]
    async fn agent_wake_or_create_with_caller_tags_sender_metadata() {
        let (srv, api) = server_with_caller("caller-1");
        let resp = call(&srv, "return await ws.agent.wakeOrCreate('tn-1', 'ctx');").await;
        assert_eq!(resp["result"]["isError"], json!(false));
        let calls = api.agent_wake_or_create_calls.lock().unwrap();
        assert_eq!(calls[0].0, "tn-1");
        assert_eq!(calls[0].1, "ctx");
        assert_eq!(
            calls[0].3,
            Some(json!({
                "type": "agent_message",
                "fromAgentId": "caller-1",
                "fromAgentName": "agent-caller-1",
            }))
        );
        assert_eq!(api.agent_get_calls.lock().unwrap().len(), 1);
    }

    /// Explicit `messageMetadata` on `wakeOrCreate` (new optional 4th arg)
    /// keeps its own fields with the attribution fields daemon-stamped.
    #[tokio::test]
    async fn agent_wake_or_create_explicit_metadata_merged_with_stamped_attribution() {
        let (srv, api) = server_with_caller("caller-1");
        let resp = call(
            &srv,
            "return await ws.agent.wakeOrCreate('tn-1', 'ctx', null, { type: 'custom' });",
        )
        .await;
        assert_eq!(resp["result"]["isError"], json!(false));
        let calls = api.agent_wake_or_create_calls.lock().unwrap();
        assert_eq!(
            calls[0].3,
            Some(json!({
                "type": "custom",
                "fromAgentId": "caller-1",
                "fromAgentName": "agent-caller-1",
            }))
        );
    }

    /// Caller-less (FE/RPC front door) wakes stay untagged.
    #[tokio::test]
    async fn agent_wake_or_create_without_caller_has_no_sender_metadata() {
        let (srv, api) = server();
        let resp = call(&srv, "return await ws.agent.wakeOrCreate('tn-1', 'ctx');").await;
        assert_eq!(resp["result"]["isError"], json!(false));
        let calls = api.agent_wake_or_create_calls.lock().unwrap();
        assert_eq!(calls[0].3, None);
    }

    /// The attribution schema stays stable when the depth-guard name lookup
    /// fails: `fromAgentName` is present but `null`, never omitted.
    #[tokio::test]
    async fn agent_wake_or_create_name_lookup_failure_keeps_null_from_agent_name() {
        let (srv, api) = server_with_caller("caller-1");
        *api.agent_get_error.lock().unwrap() = Some("agent not found".to_string());
        let resp = call(&srv, "return await ws.agent.wakeOrCreate('tn-1', 'ctx');").await;
        assert_eq!(resp["result"]["isError"], json!(false));
        let calls = api.agent_wake_or_create_calls.lock().unwrap();
        assert_eq!(
            calls[0].3,
            Some(json!({
                "type": "agent_message",
                "fromAgentId": "caller-1",
                "fromAgentName": null,
            }))
        );
    }

    /// The three-arg `wakeOrCreate` form stays green: `model` still threads
    /// through with the auto-tag applied.
    #[tokio::test]
    async fn agent_wake_or_create_three_arg_form_threads_model() {
        let (srv, api) = server_with_caller("caller-1");
        let resp = call(
            &srv,
            "return await ws.agent.wakeOrCreate('tn-1', 'ctx', 'opus');",
        )
        .await;
        assert_eq!(resp["result"]["isError"], json!(false));
        let calls = api.agent_wake_or_create_calls.lock().unwrap();
        assert_eq!(calls[0].2.as_deref(), Some("opus"));
        assert_eq!(calls[0].3.as_ref().unwrap()["type"], json!("agent_message"));
    }

    #[tokio::test]
    async fn agent_delegate_threads_wait_mode_and_caller() {
        let (srv, api) = server_with_caller("coord-1");
        let resp = call(
            &srv,
            "return await ws.agent.delegate({ taskNoteId: 't-1', waitMode: 'after_all' });",
        )
        .await;
        assert_eq!(resp["result"]["isError"], json!(false));
        let calls = api.agent_delegate_calls.lock().unwrap();
        assert_eq!(calls[0].0.as_deref(), Some("after_all"));
        assert_eq!(calls[0].1.as_deref(), Some("coord-1"));
    }

    #[tokio::test]
    async fn agent_subscribe_missing_event_types_surfaces_error() {
        let (srv, _api) = server();
        let resp = call(&srv, "return await ws.agent.subscribe();").await;
        assert_eq!(resp["result"]["isError"], json!(true));
        assert!(text(&resp).contains("eventTypes is required"));
    }

    #[tokio::test]
    async fn agent_subscribe_forwards_options() {
        let (srv, api) = server();
        let resp = call(
            &srv,
            "return await ws.agent.subscribe(['agent:idle'], { excludeSelf: false, batchWindow: 250 });",
        )
        .await;
        assert_eq!(resp["result"]["isError"], json!(false));
        let calls = api.agent_subscribe_calls.lock().unwrap();
        assert_eq!(calls[0].0, vec!["agent:idle".to_string()]);
        assert_eq!(calls[0].1, Some(false));
        assert_eq!(calls[0].2, Some(250));
    }

    #[tokio::test]
    async fn agent_unsubscribe_returns_subscription_id() {
        let (srv, api) = server();
        let resp = call(&srv, "return await ws.agent.unsubscribe('sub-9');").await;
        assert_eq!(resp["result"]["isError"], json!(false));
        let v = body(&resp);
        assert_eq!(v["subscriptionId"], json!("sub-9"));
        assert_eq!(v["ok"], json!(true));
        assert_eq!(api.agent_unsubscribe_calls.lock().unwrap()[0], "sub-9");
    }

    /// monorepo#1229: `ws.agent.unwatch()` with a missing or empty argument
    /// fails the required-parameter validation instead of reaching the
    /// service with an empty subscriptionId.
    #[tokio::test]
    async fn agent_unwatch_missing_or_empty_argument_errors() {
        let (srv, _api) = server_with_caller("caller-1");
        for script in [
            "return await ws.agent.unwatch();",
            "return await ws.agent.unwatch('');",
        ] {
            let resp = call(&srv, script).await;
            assert_eq!(resp["result"]["isError"], json!(true), "script: {script}");
            assert!(
                text(&resp).contains("subscriptionId or agentId is required"),
                "script: {script}, text: {}",
                text(&resp)
            );
        }
    }

    #[tokio::test]
    async fn agent_report_to_parent_threads_caller() {
        let (srv, api) = server_with_caller("child-99");
        let resp = call(&srv, "return await ws.agent.reportToParent('done');").await;
        assert_eq!(resp["result"]["isError"], json!(false));
        assert_eq!(
            api.report_to_parent_calls.lock().unwrap()[0].as_deref(),
            Some("child-99")
        );
    }

    #[tokio::test]
    async fn agent_report_to_parent_missing_report_errors() {
        let (srv, _api) = server();
        let resp = call(&srv, "return await ws.agent.reportToParent();").await;
        assert_eq!(resp["result"]["isError"], json!(true));
        assert!(text(&resp).contains("report is required"));
    }

    #[tokio::test]
    async fn agent_request_discussion_threads_caller_kind_and_reason() {
        let (srv, api) = server_with_caller("child-7");
        let resp = call(
            &srv,
            "return await ws.agent.requestDiscussion('need input');",
        )
        .await;
        assert_eq!(resp["result"]["isError"], json!(false));
        let v = body(&resp);
        assert_eq!(v["ok"], json!(true));
        assert_eq!(v["kind"], json!("discussion"));
        assert_eq!(v["reason"], json!("need input"));
        assert_eq!(
            api.request_attention_calls.lock().unwrap()[0],
            (
                "discussion".to_string(),
                "need input".to_string(),
                Some("child-7".to_string())
            )
        );
    }

    #[tokio::test]
    async fn agent_report_blocker_threads_caller_kind_and_reason() {
        let (srv, api) = server_with_caller("child-8");
        let resp = call(
            &srv,
            "return await ws.agent.reportBlocker('sandbox is broken');",
        )
        .await;
        assert_eq!(resp["result"]["isError"], json!(false));
        let v = body(&resp);
        assert_eq!(v["ok"], json!(true));
        assert_eq!(v["kind"], json!("blocker"));
        assert_eq!(v["reason"], json!("sandbox is broken"));
        assert_eq!(
            api.request_attention_calls.lock().unwrap()[0],
            (
                "blocker".to_string(),
                "sandbox is broken".to_string(),
                Some("child-8".to_string())
            )
        );
    }

    #[tokio::test]
    async fn agent_request_discussion_missing_reason_errors() {
        let (srv, api) = server();
        let resp = call(&srv, "return await ws.agent.requestDiscussion();").await;
        assert_eq!(resp["result"]["isError"], json!(true));
        assert!(text(&resp).contains("reason is required"));
        assert!(api.request_attention_calls.lock().unwrap().is_empty());
    }

    #[tokio::test]
    async fn agent_report_blocker_missing_reason_errors() {
        let (srv, api) = server();
        let resp = call(&srv, "return await ws.agent.reportBlocker();").await;
        assert_eq!(resp["result"]["isError"], json!(true));
        assert!(text(&resp).contains("reason is required"));
        assert!(api.request_attention_calls.lock().unwrap().is_empty());
    }

    /// A bridge with `peerAgents` opted in (the toggle defaults off), so the
    /// `retire` binding is installed and dispatchable.
    fn peer_agents_features() -> intent_core::settings_file::AgentFeaturesSettings {
        intent_core::settings_file::AgentFeaturesSettings {
            peer_agents: true,
            ..Default::default()
        }
    }

    #[tokio::test]
    async fn agent_retire_soft_retires_exactly_the_caller() {
        let api = Arc::new(FakeApi::default());
        let srv = WorkspaceMcpServer::new(api.clone(), WorkspaceId::from_string("amber-forest"))
            .with_agent_features(peer_agents_features())
            .with_caller_agent_id(Some(AgentId::from("agent-self-1")));
        let resp = call(&srv, "return await ws.agent.retire('handing off to peer');").await;
        assert_eq!(resp["result"]["isError"], json!(false));
        let v = body(&resp);
        assert_eq!(v["ok"], json!(true));
        assert_eq!(v["retired"], json!(true));
        assert_eq!(v["retiredAt"], json!("2026-01-01T00:00:00Z"));
        assert_eq!(v["agentId"], json!("agent-self-1"));
        assert_eq!(v["reason"], json!("handing off to peer"));
        // Exactly one SOFT retire, targeting the caller, workspace-scoped,
        // carrying the reason — and no hard delete.
        assert_eq!(
            *api.agent_retire_calls.lock().unwrap(),
            vec![(
                "agent-self-1".to_string(),
                Some("amber-forest".to_string()),
                Some("handing off to peer".to_string())
            )]
        );
        assert!(api.agent_delete_calls.lock().unwrap().is_empty());
    }

    #[tokio::test]
    async fn agent_retire_reason_is_optional() {
        let api = Arc::new(FakeApi::default());
        let srv = WorkspaceMcpServer::new(api.clone(), WorkspaceId::from_string("amber-forest"))
            .with_agent_features(peer_agents_features())
            .with_caller_agent_id(Some(AgentId::from("agent-self-2")));
        let resp = call(&srv, "return await ws.agent.retire();").await;
        assert_eq!(resp["result"]["isError"], json!(false));
        let v = body(&resp);
        assert_eq!(v["retired"], json!(true));
        assert!(v.get("reason").is_none(), "no reason key when omitted");
        assert_eq!(api.agent_retire_calls.lock().unwrap().len(), 1);
        assert_eq!(api.agent_retire_calls.lock().unwrap()[0].2, None);
    }

    #[tokio::test]
    async fn agent_retire_without_caller_is_rejected() {
        // FE/RPC front door: no caller identity → clear error, no retire.
        let api = Arc::new(FakeApi::default());
        let srv = WorkspaceMcpServer::new(api.clone(), WorkspaceId::from_string("amber-forest"))
            .with_agent_features(peer_agents_features());
        let resp = call(
            &srv,
            "return await host({ method: 'agent.retire', args: {} });",
        )
        .await;
        assert_eq!(resp["result"]["isError"], json!(true));
        assert!(text(&resp).contains("retire requires an agent caller identity"));
        assert!(api.agent_retire_calls.lock().unwrap().is_empty());
    }

    #[tokio::test]
    async fn retired_caller_is_rejected_at_dispatch() {
        // Same-turn inertness: once the caller's session is marked retired,
        // every subsequent workspace_api host frame from it fails closed —
        // the retiring turn cannot keep acting after the mark lands.
        let api = Arc::new(FakeApi::default());
        api.retired_agent_ids
            .lock()
            .unwrap()
            .push("agent-self-3".to_string());
        let srv = WorkspaceMcpServer::new(api.clone(), WorkspaceId::from_string("amber-forest"))
            .with_caller_agent_id(Some(AgentId::from("agent-self-3")));
        let resp = call(&srv, "return await ws.agent.list();").await;
        assert_eq!(resp["result"]["isError"], json!(true));
        assert!(text(&resp).contains("retired"));
        assert_eq!(*api.agent_list_calls.lock().unwrap(), 0);
    }

    #[tokio::test]
    async fn non_retired_caller_passes_dispatch_guard() {
        // Control: a caller whose session is not retired dispatches normally.
        let api = Arc::new(FakeApi::default());
        let srv = WorkspaceMcpServer::new(api.clone(), WorkspaceId::from_string("amber-forest"))
            .with_caller_agent_id(Some(AgentId::from("agent-self-4")));
        let resp = call(&srv, "return await ws.agent.list();").await;
        assert_eq!(resp["result"]["isError"], json!(false));
        assert_eq!(*api.agent_list_calls.lock().unwrap(), 1);
    }

    // ================================================================
    // agent.create({ topLevel: true })
    // ================================================================

    /// A bridge with `peerAgents` on and an agent caller — the shape every
    /// functional `create({ topLevel: true })` test starts from (the
    /// gate/prelude layers are covered by the feature-gating tests).
    fn top_level_create_server(caller: &str) -> (WorkspaceMcpServer, Arc<FakeApi>) {
        let api = Arc::new(FakeApi::default());
        let srv = WorkspaceMcpServer::new(api.clone(), WorkspaceId::from_string("amber-forest"))
            .with_agent_features(peer_agents_features())
            .with_caller_agent_id(Some(AgentId::from(caller)));
        (srv, api)
    }

    /// Happy path: the agent is created parentless (independence seam), its
    /// metadata carries `sponsorAgentId` (never `createdByAgentId` /
    /// `parentAgentId`), the persisted `initialMessage` equals the delivered
    /// kickoff (sponsor preamble + caller message), the caller is NOT
    /// auto-subscribed, and the kickoff send is daemon-attributed.
    #[tokio::test]
    async fn agent_create_top_level_creates_independent_parentless_agent() {
        let (srv, api) = top_level_create_server("caller-1");
        let resp = call(
            &srv,
            "return await ws.agent.create('peer', 'go', { topLevel: true });",
        )
        .await;
        assert_eq!(resp["result"]["isError"], json!(false));
        let v = body(&resp);
        assert_eq!(v["ok"], json!(true));
        assert_eq!(v["agentId"], json!("child-1"));
        assert_eq!(v["id"], json!("child-1"));
        assert_eq!(v["name"], json!("child"));
        assert_eq!(v["sponsorAgentId"], json!("caller-1"));
        assert!(
            v.get("subscriptionId").is_none(),
            "create with topLevel must not report a completion-watch subscription"
        );

        let creates = api.agent_create_calls.lock().unwrap();
        assert_eq!(creates.len(), 1);
        let (name, specialist, parent, idem, metadata, is_background) = &creates[0];
        assert_eq!(name.as_deref(), Some("peer"));
        assert_eq!(*specialist, None);
        assert_eq!(*parent, None, "top-level agent must be created parentless");
        assert!(idem.is_some(), "an idempotency key is always supplied");
        assert_eq!(
            *is_background,
            Some(false),
            "top-level agents default to foreground"
        );
        let md = metadata.as_ref().unwrap();
        assert_eq!(md["sponsorAgentId"], json!("caller-1"));
        assert_eq!(md["isBackground"], json!(false));
        assert!(
            md.get("createdByAgentId").is_none() && md.get("parentAgentId").is_none(),
            "top-level agent metadata must carry no child-linkage fields: {md}"
        );
        let kickoff = md["initialMessage"].as_str().unwrap();
        assert!(
            kickoff.starts_with(
                "[You were spawned as an independent top-level agent by agent-caller-1 (caller-1)"
            ),
            "kickoff must open with the sponsor preamble, got: {kickoff}"
        );
        assert!(
            kickoff.ends_with("]\n\ngo"),
            "caller message follows the preamble"
        );

        // Delivered kickoff == persisted `initialMessage` (parity), with the
        // daemon-stamped sender attribution.
        let sends = api.agent_send_calls.lock().unwrap();
        assert_eq!(sends.len(), 1);
        assert_eq!(sends[0].0, "child-1");
        assert_eq!(sends[0].1, kickoff);
        assert_eq!(
            sends[0].3,
            Some(json!({
                "type": "agent_message",
                "fromAgentId": "caller-1",
                "fromAgentName": "agent-caller-1",
            }))
        );

        // Independence: no completion watch of any kind for the sponsor.
        assert_eq!(*api.agent_watch_completion_calls.lock().unwrap(), 0);
        assert!(api.watch_sender_calls.lock().unwrap().is_empty());
    }

    /// The `agents.maxTopLevelAgents` cap enforces on the settings value in
    /// its normalized float wire shape (`settings.get` serves numbers as
    /// floats — an integer-only read would silently ignore user overrides
    /// and fall back to the compiled default of 20).
    #[tokio::test]
    async fn agent_create_top_level_enforces_float_normalized_cap() {
        let (srv, api) = top_level_create_server("caller-1");
        // Two live top-level agents (the default `agent_list` rows) with the
        // cap overridden to 2.0 → at cap, creation denied, nothing created.
        *api.max_top_level_agents_setting.lock().unwrap() = Some(json!(2.0));
        let resp = call(
            &srv,
            "return await ws.agent.create('peer', 'go', { topLevel: true });",
        )
        .await;
        assert_eq!(resp["result"]["isError"], json!(true));
        let t = text(&resp);
        assert!(
            t.contains("agents.maxTopLevelAgents") && t.contains("(2)"),
            "expected cap denial naming the setting and value, got: {t}"
        );
        assert!(api.agent_create_calls.lock().unwrap().is_empty());

        // Raising the override above the live count admits the creation.
        *api.max_top_level_agents_setting.lock().unwrap() = Some(json!(3.0));
        let resp = call(
            &srv,
            "return await ws.agent.create('peer', 'go', { topLevel: true });",
        )
        .await;
        assert_eq!(resp["result"]["isError"], json!(false));
        assert_eq!(api.agent_create_calls.lock().unwrap().len(), 1);
    }

    /// Cap counting includes only LIVE TOP-LEVEL agents: deleted, retired,
    /// parented, and depth>0 rows are all excluded from the population.
    #[tokio::test]
    async fn agent_create_top_level_cap_counts_only_live_top_level_agents() {
        let (srv, api) = top_level_create_server("caller-1");
        let ws = WorkspaceId::from_string("amber-forest");
        let live = stub_agent("live-1", &ws);
        let mut deleted = stub_agent("gone-1", &ws);
        deleted.status = AgentStatus::Deleted;
        let mut retired = stub_agent("ret-1", &ws);
        retired.retired_at = Some("2026-01-01T00:00:00Z".to_string());
        let mut child = stub_agent("kid-1", &ws);
        child.parent_agent_id = Some(AgentId::from("live-1"));
        let mut deep = stub_agent("deep-1", &ws);
        deep.metadata.delegation_depth = Some(1);
        *api.agent_list_rows.lock().unwrap() = Some(vec![live, deleted, retired, child, deep]);
        // Cap 2 with only ONE live top-level row → creation admitted.
        *api.max_top_level_agents_setting.lock().unwrap() = Some(json!(2.0));
        let resp = call(
            &srv,
            "return await ws.agent.create('peer', 'go', { topLevel: true });",
        )
        .await;
        assert_eq!(resp["result"]["isError"], json!(false));
        assert_eq!(api.agent_create_calls.lock().unwrap().len(), 1);
    }

    /// A non-finite or negative settings value is ignored (compiled default
    /// of 20 applies), never truncated into a bogus cap.
    #[tokio::test]
    async fn agent_create_top_level_cap_ignores_invalid_setting_values() {
        for bad in [json!(-1.0), json!("many"), Value::Null] {
            let (srv, api) = top_level_create_server("caller-1");
            *api.max_top_level_agents_setting.lock().unwrap() = Some(bad);
            // 2 live top-level rows < default cap 20 → admitted.
            let resp = call(
                &srv,
                "return await ws.agent.create('peer', 'go', { topLevel: true });",
            )
            .await;
            assert_eq!(resp["result"]["isError"], json!(false));
            assert_eq!(api.agent_create_calls.lock().unwrap().len(), 1);
        }
    }

    /// Dispatch defense in depth: a sub-agent's `create` frame with
    /// `topLevel: true` is denied with the top-level-only redirect (NOT a
    /// settings complaint), even with `peerAgents` on, and nothing is
    /// created.
    #[tokio::test]
    async fn sub_agent_create_top_level_frame_denied_at_dispatch() {
        let api = Arc::new(FakeApi::default());
        let srv = WorkspaceMcpServer::new(api.clone(), WorkspaceId::from_string("amber-forest"))
            .with_agent_features(peer_agents_features())
            .with_sub_agent(true)
            .with_caller_agent_id(Some(AgentId::from("sub-1")));
        let resp = call(
            &srv,
            "return await ws.agent.create('peer', 'go', { topLevel: true });",
        )
        .await;
        assert_eq!(resp["result"]["isError"], json!(true));
        let t = text(&resp);
        assert!(
            t.contains("only available to top-level agents") && t.contains("ws.agent.delegate"),
            "expected the top-level-only redirect, got: {t}"
        );
        assert!(
            !t.contains("disabled in settings"),
            "sub-agent denial must not masquerade as a settings gate: {t}"
        );
        assert!(api.agent_create_calls.lock().unwrap().is_empty());
    }

    /// A sub-agent's PLAIN `create` frame (no `topLevel`) still dispatches —
    /// the arg-conditional gate must not over-deny child creation.
    #[tokio::test]
    async fn sub_agent_plain_create_still_dispatches() {
        let api = Arc::new(FakeApi::default());
        let srv = WorkspaceMcpServer::new(api.clone(), WorkspaceId::from_string("amber-forest"))
            .with_agent_features(peer_agents_features())
            .with_sub_agent(true)
            .with_caller_agent_id(Some(AgentId::from("sub-1")));
        let resp = call(&srv, "return await ws.agent.create('child', 'go');").await;
        assert_eq!(resp["result"]["isError"], json!(false));
        assert_eq!(api.agent_create_calls.lock().unwrap().len(), 1);
    }

    /// Arg-conditional feature gate: with `peerAgents` OFF (the default),
    /// `create({ topLevel: true })` is denied naming the toggle, while plain
    /// `create()` on the same bridge succeeds unchanged.
    #[tokio::test]
    async fn peer_agents_off_denies_top_level_but_not_plain_create() {
        let api = Arc::new(FakeApi::default());
        let srv = WorkspaceMcpServer::new(api.clone(), WorkspaceId::from_string("amber-forest"))
            .with_caller_agent_id(Some(AgentId::from("caller-1")));
        let resp = call(
            &srv,
            "return await ws.agent.create('peer', 'go', { topLevel: true });",
        )
        .await;
        assert_eq!(resp["result"]["isError"], json!(true));
        let t = text(&resp);
        assert!(
            t.contains("agentFeatures.peerAgents"),
            "expected the peerAgents settings denial, got: {t}"
        );
        assert!(api.agent_create_calls.lock().unwrap().is_empty());

        let resp = call(&srv, "return await ws.agent.create('child', 'go');").await;
        assert_eq!(resp["result"]["isError"], json!(false));
        assert_eq!(api.agent_create_calls.lock().unwrap().len(), 1);
    }

    /// A BACKGROUND top-level caller is denied at the handler level (the
    /// caller's persisted `isBackground` metadata), and nothing is created.
    #[tokio::test]
    async fn background_caller_create_top_level_is_rejected() {
        let (srv, api) = top_level_create_server("caller-1");
        api.background_agent_ids
            .lock()
            .unwrap()
            .push("caller-1".to_string());
        let resp = call(
            &srv,
            "return await ws.agent.create('peer', 'go', { topLevel: true });",
        )
        .await;
        assert_eq!(resp["result"]["isError"], json!(true));
        let t = text(&resp);
        assert!(
            t.contains("foreground top-level agents"),
            "expected the foreground-only denial, got: {t}"
        );
        assert!(api.agent_create_calls.lock().unwrap().is_empty());
    }

    /// FAIL CLOSED: a failed caller-session lookup rejects the call instead
    /// of skipping the foreground-caller check — an unavailable or deleted
    /// caller session must not bypass the restriction.
    #[tokio::test]
    async fn failed_caller_lookup_rejects_create_top_level() {
        let (srv, api) = top_level_create_server("caller-1");
        *api.agent_get_error.lock().unwrap() = Some("agent not found".to_string());
        let resp = call(
            &srv,
            "return await ws.agent.create('peer', 'go', { topLevel: true });",
        )
        .await;
        assert_eq!(resp["result"]["isError"], json!(true));
        let t = text(&resp);
        assert!(
            t.contains("could not verify the caller's session"),
            "expected the fail-closed lookup error, got: {t}"
        );
        assert!(api.agent_create_calls.lock().unwrap().is_empty());
    }

    /// `topLevel: true` + `taskNoteId` is rejected — task assignment stays a
    /// delegation concept; nothing is created or assigned.
    #[tokio::test]
    async fn agent_create_top_level_rejects_task_note_id() {
        let (srv, api) = top_level_create_server("caller-1");
        let resp = call(
            &srv,
            "return await ws.agent.create('peer', 'go', { topLevel: true, taskNoteId: 'tn-1' });",
        )
        .await;
        assert_eq!(resp["result"]["isError"], json!(true));
        let t = text(&resp);
        assert!(
            t.contains("cannot be combined with taskNoteId"),
            "expected the taskNoteId rejection, got: {t}"
        );
        assert!(api.agent_create_calls.lock().unwrap().is_empty());
    }

    /// FE/RPC front door (no caller identity) cannot create a top-level
    /// agent — the sponsor seam requires an agent caller.
    #[tokio::test]
    async fn agent_create_top_level_without_caller_is_rejected() {
        let api = Arc::new(FakeApi::default());
        let srv = WorkspaceMcpServer::new(api.clone(), WorkspaceId::from_string("amber-forest"))
            .with_agent_features(peer_agents_features());
        let resp = call(
            &srv,
            "return await host({ method: 'agent.create', args: { name: 'peer', message: 'go', topLevel: true } });",
        )
        .await;
        assert_eq!(resp["result"]["isError"], json!(true));
        assert!(text(&resp).contains("requires an agent caller identity"));
        assert!(api.agent_create_calls.lock().unwrap().is_empty());
    }

    /// `topLevel: false` is the plain child-create path — parent linkage,
    /// completion watch, and `subscriptionId` all behave exactly as an
    /// omitted `topLevel`.
    #[tokio::test]
    async fn agent_create_top_level_false_is_plain_create() {
        let (srv, api) = top_level_create_server("caller-1");
        let resp = call(
            &srv,
            "return await ws.agent.create('child', 'go', { topLevel: false });",
        )
        .await;
        assert_eq!(resp["result"]["isError"], json!(false));
        let v = body(&resp);
        assert_eq!(v["subscriptionId"], json!("watch-1"));
        assert!(v.get("sponsorAgentId").is_none());
        assert_eq!(*api.agent_watch_completion_calls.lock().unwrap(), 1);
        let creates = api.agent_create_calls.lock().unwrap();
        assert_eq!(creates.len(), 1);
        let (_, _, parent, _, metadata, _) = &creates[0];
        assert_eq!(parent.as_deref(), Some("caller-1"));
        let md = metadata.as_ref().unwrap();
        assert_eq!(md["createdByAgentId"], json!("caller-1"));
        assert!(md.get("sponsorAgentId").is_none());
    }

    // ================================================================
    // event.*
    // ================================================================

    /// `ws.event.recentFiles` / `ws.event.directoryChanges` were removed
    /// end-to-end: the prelude no longer defines them, so calling either fails.
    #[tokio::test]
    async fn removed_event_file_bindings_are_absent() {
        let (srv, _api) = server();
        for js in [
            "return await ws.event.recentFiles(5);",
            "return await ws.event.directoryChanges('src/');",
        ] {
            let resp = call(&srv, js).await;
            assert_eq!(resp["result"]["isError"], json!(true), "{js}");
            assert!(text(&resp).contains("not a function"), "{js}");
        }
    }

    #[tokio::test]
    async fn event_query_threads_all_options() {
        let (srv, api) = server();
        let resp = call(
            &srv,
            "return await ws.event.query({ eventType: 'file:changed', actorType: 'agent', actorId: 'a-1', path: 'src/', minutesAgo: 10, limit: 25 });",
        )
        .await;
        assert_eq!(resp["result"]["isError"], json!(false));
        let calls = api.event_query_calls.lock().unwrap();
        let p = &calls[0];
        assert_eq!(p.event_type.as_deref(), Some("file:changed"));
        assert_eq!(p.actor_type.as_deref(), Some("agent"));
        assert_eq!(p.actor_id.as_deref(), Some("a-1"));
        assert_eq!(p.path.as_deref(), Some("src/"));
        assert_eq!(p.minutes_ago, Some(10));
        assert_eq!(p.limit, Some(25));
    }

    #[tokio::test]
    async fn event_subscribe_passes_wildcard_star_through() {
        // The binding no longer expands `*` — the daemon resolves it
        // per-subscriber (agents get the non-agent categories only,
        // monorepo#1229), so the wildcard must reach the API verbatim.
        let (srv, api) = server();
        let resp = call(&srv, "return await ws.event.subscribe(['*']);").await;
        assert_eq!(resp["result"]["isError"], json!(false));
        let calls = api.event_subscribe_calls.lock().unwrap();
        assert_eq!(calls[0].0, vec!["*".to_string()]);
    }

    #[tokio::test]
    async fn event_subscribe_passes_specific_types_verbatim() {
        let (srv, api) = server();
        let resp = call(
            &srv,
            "return await ws.event.subscribe(['agent:idle', 'file:changed']);",
        )
        .await;
        assert_eq!(resp["result"]["isError"], json!(false));
        let calls = api.event_subscribe_calls.lock().unwrap();
        assert_eq!(
            calls[0].0,
            vec!["agent:idle".to_string(), "file:changed".to_string()]
        );
    }

    #[tokio::test]
    async fn event_unsubscribe_returns_shape() {
        let (srv, _api) = server();
        let resp = call(&srv, "return await ws.event.unsubscribe('sub-5');").await;
        assert_eq!(resp["result"]["isError"], json!(false));
        let v = body(&resp);
        assert_eq!(v["subscriptionId"], json!("sub-5"));
        assert_eq!(v["ok"], json!(true));
    }
}

/// `workspace_api` output shaping: the `workspaceApi.toonOutput` and
/// `workspaceApi.maxOutputChars` knobs read live per invocation. Covers TOON
/// on/off, under/over-limit, the oversized-output redirect into the
/// workspace folder's `tool-outputs/` directory (a SIBLING of the repo
/// checkout), unlimited (`0`), and the unresolvable-workspace-dir inline
/// truncation fallback.
#[cfg(test)]
mod workspace_api_output_limit_tests {
    use std::path::PathBuf;
    use std::sync::{Arc, Mutex};

    use intent_core::{
        BoxFuture, Result, Workspace, WorkspaceActivity, WorkspaceApi, WorkspaceAttention,
        WorkspaceId, WorkspaceStatus,
    };
    use serde_json::{json, Value};

    use crate::WorkspaceMcpServer;

    /// Mock whose `settings_get` serves configurable `workspaceApi.*` knobs
    /// and whose `get_workspace` reports an optional on-disk checkout path
    /// (`worktreePath`) plus an optional `repositoryPath` for the
    /// direct-checkout fallback (monorepo#3778).
    struct OutputLimitMockApi {
        checkout: Mutex<Option<String>>,
        repository_path: Mutex<Option<String>>,
        toon_output: Mutex<Value>,
        max_output_chars: Mutex<Value>,
    }

    impl OutputLimitMockApi {
        fn new(checkout: Option<&str>, toon_output: bool, max_output_chars: u64) -> Arc<Self> {
            Arc::new(Self {
                checkout: Mutex::new(checkout.map(str::to_string)),
                repository_path: Mutex::new(None),
                toon_output: Mutex::new(json!(toon_output)),
                max_output_chars: Mutex::new(json!(max_output_chars)),
            })
        }
    }

    impl WorkspaceApi for OutputLimitMockApi {
        fn get_workspace(&self, id: WorkspaceId) -> BoxFuture<'_, Result<Workspace>> {
            let checkout = self.checkout.lock().unwrap().clone();
            let repository_path = self.repository_path.lock().unwrap().clone();
            Box::pin(async move {
                let now = "2026-01-01T00:00:00Z".to_string();
                Ok(Workspace {
                    id: id.clone(),
                    title: id.as_str().to_string(),
                    branch: id.as_str().to_string(),
                    base_ref: None,
                    base_commit_sha: None,
                    status: WorkspaceStatus::Active,
                    status_message: None,
                    status_image_asset_id: None,
                    activity: WorkspaceActivity::Idle,
                    attention: WorkspaceAttention::None,
                    created_at: now.clone(),
                    updated_at: now,
                    last_activity: None,
                    tags: Vec::new(),
                    path: None,
                    repository_path,
                    repository_owner: None,
                    repository_name: None,
                    worktree_path: checkout,
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
                    context_links: None,
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
                })
            })
        }

        fn settings_get(&self, path: String) -> BoxFuture<'_, Result<Value>> {
            let toon = self.toon_output.lock().unwrap().clone();
            let max = self.max_output_chars.lock().unwrap().clone();
            Box::pin(async move {
                let value = match path.as_str() {
                    "workspaceApi.toonOutput" => toon,
                    "workspaceApi.maxOutputChars" => max,
                    _ => Value::Null,
                };
                Ok(json!({ "path": path, "value": value }))
            })
        }
    }

    /// A fresh `<workspaces-root>/<workspace-name>/<repo-name>` layout on
    /// disk; returns the workspace folder guard and the checkout path inside
    /// it. The folder is removed when the guard drops.
    fn temp_workspace_layout() -> (tempfile::TempDir, String) {
        let folder = super::test_temp_dir("intent-acp-outlimit-");
        let checkout = folder.path().join("repo");
        std::fs::create_dir_all(&checkout).unwrap();
        let checkout = checkout.to_string_lossy().into_owned();
        (folder, checkout)
    }

    fn server(
        checkout: Option<&str>,
        toon_output: bool,
        max_output_chars: u64,
    ) -> WorkspaceMcpServer {
        let api = OutputLimitMockApi::new(checkout, toon_output, max_output_chars);
        WorkspaceMcpServer::new(api, WorkspaceId::from_string("amber-forest"))
    }

    /// A server whose workspace persists ONLY `repositoryPath` — the
    /// direct-checkout shape from monorepo#3778.
    fn server_repository_only(
        repository: &str,
        toon_output: bool,
        max_output_chars: u64,
    ) -> WorkspaceMcpServer {
        let api = OutputLimitMockApi::new(None, toon_output, max_output_chars);
        *api.repository_path.lock().unwrap() = Some(repository.to_string());
        WorkspaceMcpServer::new(api, WorkspaceId::from_string("amber-forest"))
    }

    async fn call(srv: &WorkspaceMcpServer, code: &str) -> Value {
        srv.handle_message(&json!({
            "jsonrpc": "2.0", "id": 1, "method": "tools/call",
            "params": {
                "name": "workspace_api",
                "arguments": { "code": code, "summary": "unit test" }
            }
        }))
        .await
        .expect("tools/call must produce a response")
    }

    fn tool_text(resp: &Value) -> &str {
        assert_eq!(resp["result"]["isError"], json!(false));
        resp["result"]["content"][0]["text"].as_str().unwrap()
    }

    /// The single file the redirect wrote under `<folder>/tool-outputs/`.
    fn only_tool_output(folder: &std::path::Path) -> PathBuf {
        let dir = folder.join("tool-outputs");
        let entries: Vec<_> = std::fs::read_dir(&dir)
            .unwrap()
            .collect::<std::io::Result<Vec<_>>>()
            .unwrap();
        assert_eq!(entries.len(), 1, "expected exactly one redirect file");
        entries[0].path()
    }

    #[tokio::test]
    async fn under_limit_output_passes_through_unchanged() {
        let (folder, checkout) = temp_workspace_layout();
        let srv = server(Some(&checkout), false, 1000);
        let resp = call(&srv, "return { a: 1, b: 'two' };").await;
        let v: Value = serde_json::from_str(tool_text(&resp)).unwrap();
        assert_eq!(v, json!({ "a": 1, "b": "two" }));
        // No redirect file is created for an in-limit body.
        assert!(!folder.path().join("tool-outputs").exists());
    }

    #[tokio::test]
    async fn over_limit_output_redirects_to_tool_outputs_file() {
        let (folder, checkout) = temp_workspace_layout();
        let srv = server(Some(&checkout), false, 50);
        let resp = call(&srv, "return { data: 'x'.repeat(200) };").await;
        let text = tool_text(&resp);

        // The file lands in `<workspace-folder>/tool-outputs/`, a sibling of
        // the repo checkout, with the FULL body and a `.json` extension
        // (TOON disabled).
        let path = only_tool_output(folder.path());
        assert_eq!(path.parent().unwrap(), folder.path().join("tool-outputs"));
        assert_eq!(path.extension().unwrap(), "json");
        let full = std::fs::read_to_string(&path).unwrap();
        let v: Value = serde_json::from_str(&full).unwrap();
        assert_eq!(v["data"].as_str().unwrap().len(), 200);

        // The message carries total size, limit, the absolute path, a head
        // preview, and the ws.file.read caveat.
        let total = full.chars().count();
        assert!(text.contains(&format!("Output too large: {total} characters (limit: 50)")));
        assert!(text.contains(path.to_str().unwrap()));
        assert!(text.contains("First 2000 characters:"));
        let head: String = full.chars().take(50).collect();
        assert!(text.contains(&head));
        assert!(text.contains("`ws.file.read` cannot reach it"));
    }

    #[tokio::test]
    async fn repository_only_workspace_redirects_via_repository_path_fallback() {
        // Direct-checkout workspaces (`skipIsolation`) may persist only
        // `repositoryPath` (monorepo#3778): the oversized-output redirect
        // must fall back to it instead of truncating inline.
        let (folder, checkout) = temp_workspace_layout();
        let srv = server_repository_only(&checkout, false, 50);
        let resp = call(&srv, "return { data: 'x'.repeat(200) };").await;
        let text = tool_text(&resp);

        let path = only_tool_output(folder.path());
        let full = std::fs::read_to_string(&path).unwrap();
        let v: Value = serde_json::from_str(&full).unwrap();
        assert_eq!(v["data"].as_str().unwrap().len(), 200);
        assert!(text.contains("Output too large:"));
        assert!(text.contains(path.to_str().unwrap()));
        assert!(!text.contains("could NOT be written to a file"));
    }

    #[tokio::test]
    async fn toon_encodes_object_results_when_enabled() {
        let (_folder, checkout) = temp_workspace_layout();
        let srv = server(Some(&checkout), true, 0);
        let resp = call(&srv, "return { a: 1, b: 'two' };").await;
        let text = tool_text(&resp);
        let expected = toon_format::encode_default(&json!({ "a": 1, "b": "two" })).unwrap();
        assert_eq!(text, expected);
        assert!(
            serde_json::from_str::<Value>(text).is_err(),
            "TOON body must not parse as JSON: {text}"
        );
    }

    #[tokio::test]
    async fn toon_enabled_keeps_non_object_results_as_json() {
        let (_folder, checkout) = temp_workspace_layout();
        let srv = server(Some(&checkout), true, 0);
        assert_eq!(tool_text(&call(&srv, "return 'hello';").await), "\"hello\"");
        assert_eq!(tool_text(&call(&srv, "return 42;").await), "42");
        assert_eq!(tool_text(&call(&srv, "return null;").await), "null");
    }

    #[tokio::test]
    async fn toon_disabled_returns_pretty_json() {
        let (_folder, checkout) = temp_workspace_layout();
        let srv = server(Some(&checkout), false, 0);
        let resp = call(&srv, "return { a: 1, b: 'two' };").await;
        let text = tool_text(&resp);
        assert_eq!(
            text,
            serde_json::to_string_pretty(&json!({ "a": 1, "b": "two" })).unwrap()
        );
    }

    #[tokio::test]
    async fn toon_over_limit_redirect_writes_toon_file() {
        let (folder, checkout) = temp_workspace_layout();
        let srv = server(Some(&checkout), true, 50);
        let resp = call(
            &srv,
            "return [{ id: 1, data: 'x'.repeat(200) }, { id: 2, data: 'y' }];",
        )
        .await;
        let text = tool_text(&resp);
        let path = only_tool_output(folder.path());
        assert_eq!(path.extension().unwrap(), "toon");
        let full = std::fs::read_to_string(&path).unwrap();
        assert_eq!(
            full,
            toon_format::encode_default(
                &json!([{ "id": 1, "data": "x".repeat(200) }, { "id": 2, "data": "y" }])
            )
            .unwrap()
        );
        assert!(text.contains("Output too large:"));
        assert!(text.contains(path.to_str().unwrap()));
    }

    #[tokio::test]
    async fn zero_limit_means_unlimited() {
        let (folder, checkout) = temp_workspace_layout();
        let srv = server(Some(&checkout), false, 0);
        // Larger than the 100k catalog default, proving `0` disables the
        // limit rather than falling back to it.
        let resp = call(&srv, "return 'x'.repeat(120000);").await;
        let text = tool_text(&resp);
        assert_eq!(text.chars().count(), 120_002); // quotes included
        assert!(!folder.path().join("tool-outputs").exists());
    }

    #[tokio::test]
    async fn unresolvable_workspace_dir_truncates_output_inline() {
        // No on-disk checkout path: the redirect cannot be written, so the
        // body comes back TRUNCATED inline — never the full payload
        // (monorepo#3038) — and the call still succeeds.
        let srv = server(None, false, 50);
        let resp = call(&srv, "return { data: 'x'.repeat(200) };").await;
        let text = tool_text(&resp);

        // The notice carries total size, limit, and the redirect-failure
        // reason.
        let full = serde_json::to_string_pretty(&json!({ "data": "x".repeat(200) })).unwrap();
        let total = full.chars().count();
        assert!(text.contains(&format!("Output too large: {total} characters (limit: 50)")));
        assert!(text.contains("could NOT be written to a file"));
        assert!(text.contains("workspace has no on-disk checkout path"));

        // The head is the first `max_chars` characters of the output, and
        // the message never carries more of the output than that.
        let head: String = full.chars().take(50).collect();
        assert!(text.contains(&head));
        let head_plus_one: String = full.chars().take(51).collect();
        assert!(!text.contains(&head_plus_one));
        assert!(text.chars().count() < 500, "message must stay bounded");
    }
}
