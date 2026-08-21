//! Hermetic, in-process ACP integration scenarios (§13.2).
//!
//! These tests drive the real ACP client surface — [`Connection`], the
//! [`handshake`], the session lifecycle, and the client-served
//! [`ClientRequestHandler`] — against a deterministic mock agent that REPLAYS a
//! scripted NDJSON frame sequence loaded from a fixture (no node, no network, no
//! real provider binaries). The mock reads JSON-RPC on its stdin, answers the
//! handshake/session methods, streams canned `session/update` notifications, can
//! issue agent→client requests (`fs/read_text_file`, `terminal/create`,
//! `session/request_permission`), and finishes a turn with a stop reason.
//!
//! Fixtures live as `tests/fixtures/*.ndjson`. The fixture directory honors the
//! `MOCK_AGENT_SCRIPT_PATH` convention (used by the node E2E): if it points at a
//! directory it overrides the default in-repo fixtures dir; a file value (as the
//! node E2E sets) is ignored here so the two suites never collide.

use std::collections::HashMap;
use std::path::PathBuf;
use std::sync::atomic::{AtomicBool, AtomicI64, Ordering};
use std::sync::{Arc, Mutex};
use std::time::Duration;

use serde_json::{json, Value};
use tokio::io::{AsyncBufReadExt, AsyncWriteExt, BufReader, DuplexStream};
use tokio::sync::{mpsc, oneshot, Notify};

use intent_acp::session::{self, ContentBlock};
use intent_acp::transport::{Connection, ConnectionHooks, IncomingRequest};
use intent_acp::{
    AcpError, AcpResult, ClientRequestHandler, EventSink, FileService, IncomingNotification,
    MappedUpdate, PermissionOutcome, PermissionPolicy, PermissionRegistry, SinkEvent,
    TerminalCreateParams, TerminalExitInfo, TerminalHost, TerminalOutputInfo,
};
use intent_core::{AgentId, BoxFuture, WorkspaceId};

/// Marker substituted into a fixture for an oversized stderr line (> the
/// transport's per-entry char cap) to exercise ring-buffer truncation.
const BIG_LEN: usize = 20_000;

/// Records every event published through the client-served handler's sink.
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

/// Resolve the NDJSON fixtures directory, honoring `MOCK_AGENT_SCRIPT_PATH` when
/// it names a directory (otherwise the in-repo default).
fn fixtures_dir() -> PathBuf {
    if let Ok(p) = std::env::var("MOCK_AGENT_SCRIPT_PATH") {
        let path = PathBuf::from(&p);
        if path.is_dir() {
            return path;
        }
    }
    PathBuf::from(env!("CARGO_MANIFEST_DIR")).join("tests/fixtures")
}

/// A fresh, unique temp directory used as a session worktree (FS sandbox
/// root). The returned guard removes the dir on drop (including on panic);
/// set `INTENTD_TEST_KEEP_TMP` (non-empty) to keep it around for debugging.
fn temp_dir() -> tempfile::TempDir {
    let mut dir = tempfile::Builder::new()
        .prefix("intent-acp-int-")
        .tempdir()
        .expect("create test temp dir");
    if std::env::var_os("INTENTD_TEST_KEEP_TMP").is_some_and(|v| !v.is_empty()) {
        dir.disable_cleanup(true);
    }
    dir
}

/// One step the mock runs while serving a `session/prompt` turn.
enum Step {
    /// Emit a `session/update` notification with the given params.
    Update(Value),
    /// Issue an agent→client request and wait for (and record) its response.
    Call { method: String, params: Value },
    /// Block until the client sends a `session/cancel` notification.
    AwaitCancel,
    /// Finish the turn, replying to `session/prompt` with this stop reason.
    Stop(String),
    /// Write a raw (intentionally malformed) line to stdout to test resync.
    Raw(String),
    /// Write a line to the agent's stderr stream (feeds the stderr ring buffer).
    Stderr(String),
}

/// A canned reply to a single client→agent request method.
enum ReplyKind {
    Result(Value),
    Error(Value),
}

/// A parsed fixture: per-method handshake/session replies plus the prompt script.
struct Script {
    replies: HashMap<String, ReplyKind>,
    prompt: Vec<Step>,
}

/// Shared, observable mock state.
#[derive(Default)]
struct MockState {
    /// Every well-formed client→agent request frame the mock received.
    seen: Mutex<Vec<Value>>,
    /// Set if any inbound line failed to parse as JSON (frame interleaving).
    interleave: AtomicBool,
    /// Recorded responses to agent→client requests, keyed by method.
    call_responses: Mutex<Vec<(String, Value)>>,
}

/// Handle to a running mock agent. Its tasks are detached and reaped on runtime
/// shutdown at the end of each `#[tokio::test]`.
struct MockAgent {
    state: Arc<MockState>,
}

impl MockAgent {
    fn interleave_detected(&self) -> bool {
        self.state.interleave.load(Ordering::SeqCst)
    }

    fn seen_count(&self) -> usize {
        self.state.seen.lock().unwrap().len()
    }

    fn call_response(&self, method: &str) -> Option<Value> {
        self.state
            .call_responses
            .lock()
            .unwrap()
            .iter()
            .rfind(|(m, _)| m == method)
            .map(|(_, v)| v.clone())
    }
}

/// Parse one prompt-script step object (exactly one recognized key).
fn parse_step(v: &Value) -> Step {
    if let Some(p) = v.get("update") {
        Step::Update(p.clone())
    } else if let Some(c) = v.get("call") {
        Step::Call {
            method: c
                .get("method")
                .and_then(Value::as_str)
                .expect("call step needs a method")
                .to_string(),
            params: c.get("params").cloned().unwrap_or(Value::Null),
        }
    } else if v.get("await_cancel").is_some() {
        Step::AwaitCancel
    } else if let Some(s) = v.get("stop").and_then(Value::as_str) {
        Step::Stop(s.to_string())
    } else if let Some(s) = v.get("raw").and_then(Value::as_str) {
        Step::Raw(s.to_string())
    } else if let Some(s) = v.get("stderr").and_then(Value::as_str) {
        Step::Stderr(s.to_string())
    } else {
        panic!("unknown prompt step: {v}");
    }
}

/// Load and parse a fixture, substituting `${ROOT}` (session worktree) and
/// `${BIG}` (an oversized stderr line) tokens.
fn load_script(name: &str, root: &str) -> Script {
    let path = fixtures_dir().join(name);
    let raw = std::fs::read_to_string(&path)
        .unwrap_or_else(|e| panic!("read fixture {}: {e}", path.display()));
    let big = "x".repeat(BIG_LEN);
    let text = raw.replace("${ROOT}", root).replace("${BIG}", &big);

    let mut replies = HashMap::new();
    let mut prompt = Vec::new();
    for line in text.lines() {
        let line = line.trim();
        if line.is_empty() {
            continue;
        }
        let v: Value = serde_json::from_str(line)
            .unwrap_or_else(|e| panic!("fixture {name} has an invalid JSON line: {e}"));
        if let Some(method) = v.get("reply").and_then(Value::as_str) {
            let kind = if let Some(err) = v.get("error") {
                ReplyKind::Error(err.clone())
            } else {
                ReplyKind::Result(v.get("result").cloned().unwrap_or_else(|| json!({})))
            };
            replies.insert(method.to_string(), kind);
        } else if let Some(steps) = v.get("prompt").and_then(Value::as_array) {
            prompt = steps.iter().map(parse_step).collect();
        } else {
            panic!("fixture {name} line is neither a reply nor a prompt: {line}");
        }
    }
    Script { replies, prompt }
}

/// Type alias for the agent→client request correlation map.
type PendingCalls = Arc<Mutex<HashMap<i64, oneshot::Sender<Value>>>>;

/// Spawn the in-process mock agent over the supplied duplex streams.
fn spawn_mock(
    read: DuplexStream,
    write: DuplexStream,
    stderr: DuplexStream,
    script: Script,
) -> MockAgent {
    let state = Arc::new(MockState::default());
    let pending: PendingCalls = Arc::new(Mutex::new(HashMap::new()));
    let cancelled = Arc::new(AtomicBool::new(false));
    let cancel_notify = Arc::new(Notify::new());
    let next_call_id = Arc::new(AtomicI64::new(1));

    let (out_tx, mut out_rx) = mpsc::unbounded_channel::<String>();
    let (err_tx, mut err_rx) = mpsc::unbounded_channel::<String>();

    // Single stdout writer: whole lines, one at a time (mirrors the client's
    // serialized writer so the mock never interleaves its own frames).
    tokio::spawn(async move {
        let mut write = write;
        while let Some(line) = out_rx.recv().await {
            if write.write_all(line.as_bytes()).await.is_err() || write.flush().await.is_err() {
                break;
            }
        }
    });
    // Stderr writer.
    tokio::spawn(async move {
        let mut stderr = stderr;
        while let Some(line) = err_rx.recv().await {
            if stderr.write_all(line.as_bytes()).await.is_err() {
                break;
            }
            let _ = stderr.flush().await;
        }
    });

    let replies = Arc::new(script.replies);
    let prompt = Arc::new(script.prompt);
    let r_state = state.clone();
    tokio::spawn(async move {
        let mut lines = BufReader::new(read).lines();
        while let Ok(Some(line)) = lines.next_line().await {
            if line.trim().is_empty() {
                continue;
            }
            let value: Value = match serde_json::from_str(&line) {
                Ok(v) => v,
                Err(_) => {
                    r_state.interleave.store(true, Ordering::SeqCst);
                    continue;
                }
            };
            let method = value
                .get("method")
                .and_then(Value::as_str)
                .map(str::to_string);
            let id = value.get("id").cloned().filter(|v| !v.is_null());
            match (method, id) {
                (Some(method), Some(id)) => {
                    r_state.seen.lock().unwrap().push(value.clone());
                    if method == "session/prompt" {
                        spawn_prompt(
                            prompt.clone(),
                            id,
                            out_tx.clone(),
                            err_tx.clone(),
                            pending.clone(),
                            next_call_id.clone(),
                            cancelled.clone(),
                            cancel_notify.clone(),
                            r_state.clone(),
                        );
                    } else {
                        let frame = match replies.get(&method) {
                            Some(ReplyKind::Result(r)) => {
                                json!({ "jsonrpc": "2.0", "id": id, "result": r })
                            }
                            Some(ReplyKind::Error(e)) => {
                                json!({ "jsonrpc": "2.0", "id": id, "error": e })
                            }
                            None => json!({ "jsonrpc": "2.0", "id": id, "result": {} }),
                        };
                        let _ = out_tx.send(format!("{frame}\n"));
                    }
                }
                (Some(method), None) => {
                    if method == "session/cancel" {
                        cancelled.store(true, Ordering::SeqCst);
                        cancel_notify.notify_waiters();
                    }
                }
                (None, Some(id)) => {
                    if let Some(key) = id.as_i64() {
                        if let Some(tx) = pending.lock().unwrap().remove(&key) {
                            let _ = tx.send(value.clone());
                        }
                    }
                }
                _ => {}
            }
        }
    });

    MockAgent { state }
}

/// Run a single `session/prompt` turn, then reply with its stop reason.
#[allow(clippy::too_many_arguments)]
fn spawn_prompt(
    steps: Arc<Vec<Step>>,
    prompt_id: Value,
    out: mpsc::UnboundedSender<String>,
    err: mpsc::UnboundedSender<String>,
    pending: PendingCalls,
    next_call_id: Arc<AtomicI64>,
    cancelled: Arc<AtomicBool>,
    cancel_notify: Arc<Notify>,
    state: Arc<MockState>,
) {
    tokio::spawn(async move {
        let mut stop = "end_turn".to_string();
        for step in steps.iter() {
            match step {
                Step::Update(params) => {
                    let frame =
                        json!({ "jsonrpc": "2.0", "method": "session/update", "params": params });
                    let _ = out.send(format!("{frame}\n"));
                }
                Step::Stderr(s) => {
                    let _ = err.send(format!("{s}\n"));
                }
                Step::Raw(s) => {
                    let _ = out.send(format!("{s}\n"));
                }
                Step::AwaitCancel => {
                    while !cancelled.load(Ordering::SeqCst) {
                        let fut = cancel_notify.notified();
                        if cancelled.load(Ordering::SeqCst) {
                            break;
                        }
                        let _ = tokio::time::timeout(Duration::from_millis(25), fut).await;
                    }
                }
                Step::Call { method, params } => {
                    let id = next_call_id.fetch_add(1, Ordering::SeqCst);
                    let (tx, rx) = oneshot::channel();
                    pending.lock().unwrap().insert(id, tx);
                    let frame = json!({
                        "jsonrpc": "2.0", "id": id, "method": method, "params": params,
                    });
                    let _ = out.send(format!("{frame}\n"));
                    if let Ok(resp) = rx.await {
                        state
                            .call_responses
                            .lock()
                            .unwrap()
                            .push((method.clone(), resp));
                    }
                }
                Step::Stop(reason) => {
                    stop = reason.clone();
                    break;
                }
            }
        }
        let frame = json!({ "jsonrpc": "2.0", "id": prompt_id, "result": { "stopReason": stop } });
        let _ = out.send(format!("{frame}\n"));
    });
}

/// A fully wired scenario: a live [`Connection`] to the mock agent plus the
/// client-served handler loop (mirrors `AgentManager::create_agent`).
struct Scenario {
    conn: Arc<Connection>,
    notes: mpsc::UnboundedReceiver<IncomingNotification>,
    sink: Arc<MockSink>,
    registry: Arc<PermissionRegistry>,
    mock: MockAgent,
    root: PathBuf,
    /// RAII guard for `root`; removes the worktree when the scenario drops.
    _root_guard: tempfile::TempDir,
}

/// Wire a mock agent (from `fixture`) to a `Connection` and a serving
/// [`ClientRequestHandler`] under `policy`.
fn build(
    fixture: &str,
    policy: PermissionPolicy,
    registry: PermissionRegistry,
    auth_patterns: Vec<String>,
) -> Scenario {
    build_inner(fixture, policy, registry, auth_patterns, None)
}

/// Like [`build`] but also wires a client-served terminal host so `terminal/*`
/// requests run on the real PTY host (§6.7).
fn build_with_terminal(
    fixture: &str,
    policy: PermissionPolicy,
    registry: PermissionRegistry,
    terminal_host: Arc<dyn TerminalHost>,
) -> Scenario {
    build_inner(fixture, policy, registry, Vec::new(), Some(terminal_host))
}

fn build_inner(
    fixture: &str,
    policy: PermissionPolicy,
    registry: PermissionRegistry,
    auth_patterns: Vec<String>,
    terminal_host: Option<Arc<dyn TerminalHost>>,
) -> Scenario {
    let root_guard = temp_dir();
    let root = root_guard.path().to_path_buf();
    let script = load_script(fixture, &root.to_string_lossy());

    let (c2a_client, c2a_agent) = tokio::io::duplex(64 * 1024);
    let (a2c_agent, a2c_client) = tokio::io::duplex(64 * 1024);
    let (stderr_w, stderr_r) = tokio::io::duplex(64 * 1024);
    let mock = spawn_mock(c2a_agent, a2c_agent, stderr_w, script);

    let (req_tx, mut req_rx) = mpsc::unbounded_channel::<IncomingRequest>();
    let (note_tx, note_rx) = mpsc::unbounded_channel::<IncomingNotification>();
    let hooks = ConnectionHooks {
        requests: Some(req_tx),
        notifications: Some(note_tx),
        auth_error_patterns: auth_patterns,
        stderr_log_dir: None,
    };
    let conn = Arc::new(Connection::new(
        c2a_client,
        a2c_client,
        Some(Box::new(stderr_r)),
        hooks,
    ));

    let sink = Arc::new(MockSink::default());
    let registry = Arc::new(registry);
    let mut handler = ClientRequestHandler::new(
        WorkspaceId::from_string("ws-1"),
        AgentId::from_string("agent-1"),
        "auggie",
        FileService::new(&root),
        registry.clone(),
        policy,
        sink.clone(),
    );
    if let Some(host) = terminal_host {
        handler = handler.with_terminal_host(host);
    }
    let handler = Arc::new(handler);

    let serve_conn = conn.clone();
    tokio::spawn(async move {
        while let Some(req) = req_rx.recv().await {
            if handler.serve(serve_conn.as_ref(), req).await.is_err() {
                break;
            }
        }
    });

    Scenario {
        conn,
        notes: note_rx,
        sink,
        registry,
        mock,
        root,
        _root_guard: root_guard,
    }
}

/// Run `initialize` + `authenticate` + `session/new`, returning the session id.
async fn open_session(s: &Scenario) -> String {
    let provider = intent_providers::find_provider("auggie").unwrap();
    let hs = intent_acp::handshake(s.conn.as_ref(), provider)
        .await
        .expect("handshake");
    assert!(hs.authenticated, "auggie authenticates");
    let resp = session::new_session(s.conn.as_ref(), s.root.clone(), Vec::new(), None)
        .await
        .expect("session/new");
    resp.session_id.0.to_string()
}

/// Build a single text content block for a prompt.
fn text_block(text: &str) -> ContentBlock {
    serde_json::from_value(json!({ "type": "text", "text": text })).unwrap()
}

/// Receive the next streamed notification (with a generous timeout).
async fn recv_note(rx: &mut mpsc::UnboundedReceiver<IncomingNotification>) -> IncomingNotification {
    tokio::time::timeout(Duration::from_secs(5), rx.recv())
        .await
        .expect("notification timed out")
        .expect("notification stream closed")
}

/// Assert a notification is an agent message chunk and return its text.
fn chunk_text(note: &IncomingNotification) -> String {
    match session::map_notification(note) {
        Some(MappedUpdate::Chunk {
            text: Some(t),
            thought: false,
            ..
        }) => t,
        other => panic!("expected an agent message chunk, got {other:?}"),
    }
}

#[tokio::test]
async fn happy_path_streams_chunk_then_end_turn() {
    let mut s = build(
        "happy_path.ndjson",
        PermissionPolicy::Interactive,
        PermissionRegistry::new(),
        Vec::new(),
    );
    let sid = open_session(&s).await;
    let activity = session::ActivityTracker::new();
    let outcome = session::prompt(s.conn.as_ref(), &sid, vec![text_block("hi")], &activity)
        .await
        .expect("prompt resolves");
    assert_eq!(
        serde_json::to_value(outcome.stop_reason).unwrap(),
        json!("end_turn")
    );
    let note = recv_note(&mut s.notes).await;
    assert_eq!(chunk_text(&note), "Hello from the mock agent");
}

#[tokio::test]
async fn auth_required_surfaces_acp_auth_error() {
    let s = build(
        "auth_required.ndjson",
        PermissionPolicy::Interactive,
        PermissionRegistry::new(),
        Vec::new(),
    );
    let provider = intent_providers::find_provider("auggie").unwrap();
    let err = intent_acp::handshake(s.conn.as_ref(), provider)
        .await
        .expect_err("authentication must fail");
    match err {
        AcpError::Auth(msg) => assert!(msg.contains("auggie login"), "login hint present: {msg}"),
        other => panic!("expected AcpError::Auth, got {other:?}"),
    }
}

#[tokio::test]
async fn mid_turn_cancel_resolves_cancelled() {
    let mut s = build(
        "cancel.ndjson",
        PermissionPolicy::Interactive,
        PermissionRegistry::new(),
        Vec::new(),
    );
    let sid = open_session(&s).await;
    let conn = s.conn.clone();
    let sid2 = sid.clone();
    let task = tokio::spawn(async move {
        let activity = session::ActivityTracker::new();
        session::prompt(conn.as_ref(), &sid2, vec![text_block("go")], &activity).await
    });

    // The agent streams a chunk; only then do we cancel the in-flight turn.
    let note = recv_note(&mut s.notes).await;
    assert_eq!(chunk_text(&note), "working, please wait");
    session::cancel(s.conn.as_ref(), &sid)
        .await
        .expect("cancel notification sent");

    let outcome = task.await.unwrap().expect("prompt resolves");
    assert_eq!(
        serde_json::to_value(outcome.stop_reason).unwrap(),
        json!("cancelled")
    );
}

/// Drive the permission scenario, resolving the mediated prompt from "the UI"
/// via the registry with `option_id`. Returns the agent's recorded response.
async fn run_permission(option_id: &str) -> Value {
    let s = build(
        "permission_request.ndjson",
        PermissionPolicy::Interactive,
        PermissionRegistry::new(),
        Vec::new(),
    );
    let sid = open_session(&s).await;
    let conn = s.conn.clone();
    let task = tokio::spawn(async move {
        let activity = session::ActivityTracker::new();
        session::prompt(conn.as_ref(), &sid, vec![text_block("act")], &activity).await
    });

    let request_id = loop {
        if let Some(p) = s.registry.pending().first() {
            break p.request_id.clone();
        }
        tokio::time::sleep(Duration::from_millis(5)).await;
    };
    assert!(
        s.registry.resolve(
            &request_id,
            PermissionOutcome::Selected {
                option_id: option_id.to_string(),
            },
        ),
        "registry delivers the outcome to the waiting handler"
    );

    let outcome = task.await.unwrap().expect("prompt resolves");
    assert_eq!(
        serde_json::to_value(outcome.stop_reason).unwrap(),
        json!("end_turn")
    );
    assert!(s
        .sink
        .types()
        .contains(&"agent:permission:request".to_string()));
    assert!(s.sink.last_of("agent:permission:resolved").is_some());

    s.mock
        .call_response("session/request_permission")
        .expect("agent received a permission response")
}

#[tokio::test]
async fn permission_allow_selected_via_registry() {
    let resp = run_permission("allow_once").await;
    assert_eq!(resp["result"]["outcome"]["outcome"], json!("selected"));
    assert_eq!(resp["result"]["outcome"]["optionId"], json!("allow_once"));
}

#[tokio::test]
async fn permission_deny_selected_via_registry() {
    let resp = run_permission("reject_once").await;
    assert_eq!(resp["result"]["outcome"]["outcome"], json!("selected"));
    assert_eq!(resp["result"]["outcome"]["optionId"], json!("reject_once"));
}

#[tokio::test]
async fn fs_read_request_served_from_sandbox() {
    let mut s = build(
        "fs_read.ndjson",
        PermissionPolicy::Interactive,
        PermissionRegistry::new(),
        Vec::new(),
    );
    std::fs::write(s.root.join("data.txt"), "file data here").unwrap();
    let sid = open_session(&s).await;
    let activity = session::ActivityTracker::new();
    let outcome = session::prompt(s.conn.as_ref(), &sid, vec![text_block("read")], &activity)
        .await
        .expect("prompt resolves");
    assert_eq!(
        serde_json::to_value(outcome.stop_reason).unwrap(),
        json!("end_turn")
    );

    let resp = s
        .mock
        .call_response("fs/read_text_file")
        .expect("agent received the file content");
    assert_eq!(resp["result"]["content"], json!("file data here"));
    let note = recv_note(&mut s.notes).await;
    assert_eq!(chunk_text(&note), "read complete");
}

#[tokio::test]
async fn oversized_stderr_truncated_and_terminal_stub() {
    let s = build(
        "terminal_oversized.ndjson",
        PermissionPolicy::Interactive,
        PermissionRegistry::new(),
        Vec::new(),
    );
    let sid = open_session(&s).await;
    let activity = session::ActivityTracker::new();
    let outcome = session::prompt(s.conn.as_ref(), &sid, vec![text_block("term")], &activity)
        .await
        .expect("prompt resolves");
    assert_eq!(
        serde_json::to_value(outcome.stop_reason).unwrap(),
        json!("end_turn")
    );

    // `terminal/*` is a clean JSON-RPC "method not found" stub until M6.
    let resp = s
        .mock
        .call_response("terminal/create")
        .expect("agent received the terminal stub response");
    assert_eq!(resp["error"]["code"], json!(-32601));

    // The stderr ring buffer is bounded (5 entries) and truncates the oversized
    // line in the middle.
    let mut recent = Vec::new();
    for _ in 0..200 {
        recent = s.conn.recent_stderr();
        if recent.len() == 5 && recent[0].contains("[truncated]") {
            break;
        }
        tokio::time::sleep(Duration::from_millis(10)).await;
    }
    assert_eq!(recent.len(), 5, "ring buffer caps at 5 entries");
    assert!(
        recent[0].contains("[truncated]"),
        "oversized entry truncated"
    );
    assert!(
        recent[0].len() <= 10_000,
        "truncated entry within the char cap"
    );
    assert!(
        !recent.iter().any(|l| l == "terminal line 1"),
        "oldest line evicted"
    );
    assert!(
        recent.iter().any(|l| l == "terminal line 7"),
        "newest line retained"
    );
}

#[tokio::test]
async fn malformed_frame_recovery_resyncs_on_newline() {
    let mut s = build(
        "malformed_recovery.ndjson",
        PermissionPolicy::Interactive,
        PermissionRegistry::new(),
        Vec::new(),
    );
    let sid = open_session(&s).await;
    let activity = session::ActivityTracker::new();
    let outcome = session::prompt(s.conn.as_ref(), &sid, vec![text_block("x")], &activity)
        .await
        .expect("prompt resolves after garbage frame");
    assert_eq!(
        serde_json::to_value(outcome.stop_reason).unwrap(),
        json!("end_turn")
    );
    let note = recv_note(&mut s.notes).await;
    assert_eq!(chunk_text(&note), "recovered after garbage");
}

#[tokio::test]
async fn concurrent_sends_do_not_interleave() {
    let s = build(
        "happy_path.ndjson",
        PermissionPolicy::Interactive,
        PermissionRegistry::new(),
        Vec::new(),
    );
    // Large params (> PIPE_BUF) fired concurrently must each arrive as one intact
    // line — the mock's reader flags any frame it cannot parse.
    let big = "x".repeat(16 * 1024);
    let mut handles = Vec::new();
    for i in 0..24 {
        let conn = s.conn.clone();
        let big = big.clone();
        handles.push(tokio::spawn(async move {
            conn.request("session/ping", json!({ "i": i, "blob": big }))
                .await
        }));
    }
    for h in handles {
        h.await.unwrap().expect("each ping resolves");
    }
    assert_eq!(
        s.mock.seen_count(),
        24,
        "every request arrived as one intact frame"
    );
    assert!(!s.mock.interleave_detected(), "no frame interleaving");
}

/// A client-served terminal host backed by the real unified PTY host, exercising
/// the `TerminalHost` adapter contract end-to-end (§6.7).
struct PtyTermHost {
    pty: Arc<intent_pty::PtyHost>,
}

fn term_resolve(terminal_id: &str) -> AcpResult<intent_pty::PtyId> {
    intent_pty::PtyId::parse(terminal_id)
        .ok_or_else(|| AcpError::Terminal(format!("unknown terminal {terminal_id}")))
}

impl TerminalHost for PtyTermHost {
    fn create(&self, params: TerminalCreateParams) -> BoxFuture<'_, AcpResult<String>> {
        let pty = self.pty.clone();
        Box::pin(async move {
            let mut spec = intent_pty::SpawnSpec::new(params.session_id, params.command);
            spec.args = params.args;
            spec.env = params.env;
            spec.cwd = params.cwd;
            let id = pty
                .spawn(spec)
                .map_err(|e| AcpError::Terminal(e.to_string()))?;
            Ok(id.to_string())
        })
    }

    fn output(&self, terminal_id: String) -> BoxFuture<'_, AcpResult<TerminalOutputInfo>> {
        let pty = self.pty.clone();
        Box::pin(async move {
            let id = term_resolve(&terminal_id)?;
            let bytes = pty
                .scrollback(id)
                .map_err(|e| AcpError::Terminal(e.to_string()))?;
            let exit = pty.try_exit(id).ok().flatten().map(|e| TerminalExitInfo {
                exit_code: Some(e.exit_code),
                signal: None,
            });
            Ok(TerminalOutputInfo {
                output: String::from_utf8_lossy(&bytes).into_owned(),
                truncated: false,
                exit_status: exit,
            })
        })
    }

    fn wait_for_exit(&self, terminal_id: String) -> BoxFuture<'_, AcpResult<TerminalExitInfo>> {
        let pty = self.pty.clone();
        Box::pin(async move {
            let id = term_resolve(&terminal_id)?;
            let exit = pty
                .wait(id)
                .await
                .map_err(|e| AcpError::Terminal(e.to_string()))?;
            // The child's exit races the reader thread draining its final
            // output into scrollback; give the drain a bounded window so a
            // subsequent `terminal/output` sees the full text.
            //
            // Heuristic caveats, acceptable for the sole current consumer
            // (the single-burst `echo` test below): two identical non-empty
            // snapshots 20ms apart can break early if the final output lands
            // in chunks more than 20ms apart, and a genuinely output-free
            // command waits out the full 5s deadline. Revisit (e.g. an
            // explicit reader-drained signal from the PTY host) before
            // reusing this host for other commands.
            let deadline = tokio::time::Instant::now() + Duration::from_secs(5);
            let mut last = pty.scrollback(id).unwrap_or_default();
            loop {
                tokio::time::sleep(Duration::from_millis(20)).await;
                let cur = pty.scrollback(id).unwrap_or_default();
                if (!cur.is_empty() && cur == last) || tokio::time::Instant::now() >= deadline {
                    break;
                }
                last = cur;
            }
            Ok(TerminalExitInfo {
                exit_code: Some(exit.exit_code),
                signal: None,
            })
        })
    }

    fn release(&self, terminal_id: String) -> BoxFuture<'_, AcpResult<()>> {
        let pty = self.pty.clone();
        Box::pin(async move {
            let id = term_resolve(&terminal_id)?;
            pty.kill(id).await;
            Ok(())
        })
    }

    fn kill(&self, terminal_id: String) -> BoxFuture<'_, AcpResult<()>> {
        let pty = self.pty.clone();
        Box::pin(async move {
            let id = term_resolve(&terminal_id)?;
            pty.kill(id).await;
            Ok(())
        })
    }
}

/// An agent's `terminal/*` calls (create → `wait_for_exit` → output → release)
/// run on the real PTY host and return host-backed responses (§6.7).
#[cfg(unix)]
#[tokio::test]
async fn terminal_requests_run_on_real_pty_host() {
    let pty = Arc::new(intent_pty::PtyHost::new());
    let host: Arc<dyn TerminalHost> = Arc::new(PtyTermHost { pty });
    let s = build_with_terminal(
        "terminal_host.ndjson",
        PermissionPolicy::Interactive,
        PermissionRegistry::new(),
        host,
    );
    let sid = open_session(&s).await;
    let activity = session::ActivityTracker::new();
    let outcome = session::prompt(s.conn.as_ref(), &sid, vec![text_block("term")], &activity)
        .await
        .expect("prompt resolves");
    assert_eq!(
        serde_json::to_value(outcome.stop_reason).unwrap(),
        json!("end_turn")
    );

    // create → the fresh host mints `pty-0`.
    let created = s
        .mock
        .call_response("terminal/create")
        .expect("agent received the create response");
    assert_eq!(created["result"]["terminalId"], json!("pty-0"));

    // wait_for_exit → the real `echo` exits cleanly (code 0, flattened status).
    let exit = s
        .mock
        .call_response("terminal/wait_for_exit")
        .expect("agent received the exit status");
    assert_eq!(exit["result"]["exitCode"], json!(0));

    // output → host scrollback flows through (echoed text, not truncated).
    let out = s
        .mock
        .call_response("terminal/output")
        .expect("agent received the output");
    assert_eq!(out["result"]["truncated"], json!(false));
    assert!(
        out["result"]["output"]
            .as_str()
            .unwrap()
            .contains("acp-term-ok"),
        "terminal output carries the echoed text: {out}"
    );

    // release → a clean empty result.
    let released = s
        .mock
        .call_response("terminal/release")
        .expect("agent received the release ack");
    assert!(released["result"].is_object());
}
