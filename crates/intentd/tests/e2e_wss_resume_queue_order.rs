//! WSS end-to-end for the resume-ordering contract on preserved agent queues.
//!
//! An agent interrupted mid-turn by a daemon crash keeps its queued messages
//! (write-through persistence + startup rehydration). On resume — via
//! `agent.resolveInterrupted { resume }` or the headless `serve --resume-all`
//! sweep — the continuation message streams FIRST, and the preserved queue
//! drains after that turn completes, FIFO in original order. Abandoning
//! leaves the preserved queue intact and inert.
//!
//! Coverage:
//! - Restart rehydrates the queue in original order and never starts a turn
//! - RPC resume: continuation user message lands before both preserved
//!   queued messages, which drain FIFO
//! - Abandon: queue untouched (no auto-send), system interruption message
//!   appended, entries removable via `agent.removeQueuedMessage`
//! - `--resume-all`: same ordering contract as the RPC resume path

#![cfg(unix)]

mod common;

use std::os::unix::process::CommandExt;
use std::path::{Path, PathBuf};
use std::process::{Child, Command, Stdio};
use std::sync::Arc;
use std::time::Duration;

use futures_util::{SinkExt, StreamExt};
use rustls::client::danger::{HandshakeSignatureValid, ServerCertVerified, ServerCertVerifier};
use rustls::crypto::CryptoProvider;
use rustls::{ClientConfig, DigitallySignedStruct};
use rustls_pki_types::{CertificateDer, ServerName, UnixTime};
use serde_json::{json, Value};
use tokio::io::AsyncWriteExt;
use tokio::net::{TcpStream, UnixStream};
use tokio::time::timeout;
use tokio_tungstenite::tungstenite::Message;
use tokio_tungstenite::WebSocketStream;
use uuid::Uuid;

const TOKEN: &str = "efefefefefefefefefefefefefefefefefefefefefefefefefefefefefefefef";

const START_MSG: &str = "Start the long-running task";
const QUEUED_ONE: &str = "preserved queue message one";
const QUEUED_TWO: &str = "preserved queue message two";
/// Stable prefix of the continuation wording in
/// `Services::resume_interrupted_agent` — the delivered message embeds a
/// per-resume humanized outage duration, so asserts match on this prefix.
const CONTINUATION_PREFIX: &str = "You were interrupted for about ";

fn temp_data_dir() -> PathBuf {
    let id = Uuid::new_v4().simple().to_string();
    let dir = PathBuf::from("/tmp").join(format!("itd-wss-queue-order-{}", &id[..8]));
    std::fs::create_dir_all(&dir).expect("mkdir data dir");
    dir
}

async fn await_uds(socket: &Path) -> bool {
    timeout(common::daemon_startup_timeout(), async {
        loop {
            if UnixStream::connect(socket).await.is_ok() {
                return;
            }
            tokio::time::sleep(Duration::from_millis(25)).await;
        }
    })
    .await
    .is_ok()
}

async fn uds_rpc(socket: &Path, id: i64, method: &str, params: Value) -> Value {
    use tokio::io::{AsyncBufReadExt, BufReader};
    let stream = UnixStream::connect(socket).await.expect("connect uds");
    let (read_half, mut write_half) = stream.into_split();
    let mut line = serde_json::to_string(
        &json!({ "jsonrpc": "2.0", "id": id, "method": method, "params": params }),
    )
    .unwrap();
    line.push('\n');
    write_half.write_all(line.as_bytes()).await.unwrap();
    write_half.flush().await.unwrap();
    let mut reader = BufReader::new(read_half);
    let mut buf = String::new();
    timeout(common::rpc_read_timeout(), reader.read_line(&mut buf))
        .await
        .expect("uds rpc timed out")
        .expect("read uds response");
    serde_json::from_str(buf.trim_end()).expect("invalid JSON frame")
}

#[derive(Debug)]
struct PinnedVerifier {
    fingerprint: String,
    provider: Arc<CryptoProvider>,
}

impl ServerCertVerifier for PinnedVerifier {
    fn verify_server_cert(
        &self,
        end_entity: &CertificateDer<'_>,
        _intermediates: &[CertificateDer<'_>],
        _server_name: &ServerName<'_>,
        _ocsp_response: &[u8],
        _now: UnixTime,
    ) -> Result<ServerCertVerified, rustls::Error> {
        use sha2::{Digest, Sha256};
        let fp = Sha256::digest(end_entity.as_ref())
            .iter()
            .map(|b| format!("{b:02X}"))
            .collect::<Vec<_>>()
            .join(":");
        if fp == self.fingerprint {
            Ok(ServerCertVerified::assertion())
        } else {
            Err(rustls::Error::General("fingerprint mismatch".into()))
        }
    }

    fn verify_tls12_signature(
        &self,
        message: &[u8],
        cert: &CertificateDer<'_>,
        dss: &DigitallySignedStruct,
    ) -> Result<HandshakeSignatureValid, rustls::Error> {
        rustls::crypto::verify_tls12_signature(
            message,
            cert,
            dss,
            &self.provider.signature_verification_algorithms,
        )
    }

    fn verify_tls13_signature(
        &self,
        message: &[u8],
        cert: &CertificateDer<'_>,
        dss: &DigitallySignedStruct,
    ) -> Result<HandshakeSignatureValid, rustls::Error> {
        rustls::crypto::verify_tls13_signature(
            message,
            cert,
            dss,
            &self.provider.signature_verification_algorithms,
        )
    }

    fn supported_verify_schemes(&self) -> Vec<rustls::SignatureScheme> {
        self.provider
            .signature_verification_algorithms
            .supported_schemes()
    }
}

fn client_config(fingerprint: &str) -> ClientConfig {
    let provider = Arc::new(rustls::crypto::ring::default_provider());
    ClientConfig::builder_with_provider(provider.clone())
        .with_safe_default_protocol_versions()
        .expect("safe defaults")
        .dangerous()
        .with_custom_certificate_verifier(Arc::new(PinnedVerifier {
            fingerprint: fingerprint.to_string(),
            provider: provider.clone(),
        }))
        .with_no_client_auth()
}

async fn connect_ws(
    port: u16,
    cfg: ClientConfig,
) -> WebSocketStream<tokio_rustls::client::TlsStream<TcpStream>> {
    let url = format!("wss://localhost:{port}/ws?token={TOKEN}");
    common::wss_connect_with_retry(port, Arc::new(cfg), &url).await
}

async fn wss_rpc(
    ws: &mut WebSocketStream<tokio_rustls::client::TlsStream<TcpStream>>,
    id: i64,
    method: &str,
    params: Value,
) -> Value {
    let frame = json!({ "jsonrpc": "2.0", "id": id, "method": method, "params": params });
    ws.send(Message::Text(frame.to_string().into()))
        .await
        .expect("send rpc frame");
    loop {
        let next = timeout(Duration::from_secs(15), ws.next())
            .await
            .expect("wss rpc timed out");
        match next {
            Some(Ok(Message::Text(text))) => {
                let v: Value = serde_json::from_str(&text).expect("json frame");
                if v["id"] == json!(id) && v.get("result").is_some() {
                    return v["result"].clone();
                } else if v["id"] == json!(id) {
                    panic!("rpc errored: {v}");
                }
            }
            Some(Ok(Message::Ping(p))) => {
                let _ = ws.send(Message::Pong(p)).await;
            }
            Some(Ok(_)) => {}
            other => panic!("expected text frame, got {other:?}"),
        }
    }
}

fn spawn_serve(data_dir: &Path, listen: &str, env: &[(&str, &str)], resume_all: bool) -> Child {
    let log = std::fs::File::create(data_dir.join("daemon.log")).expect("create daemon log");
    let workspaces_dir = data_dir.join("workspaces");
    std::fs::create_dir_all(&workspaces_dir).expect("mkdir hermetic workspaces dir");
    let secrets_file = data_dir.join("secrets.json");
    if listen != "uds" {
        common::enable_ws_api(data_dir);
    }
    // Pin resumeInterruptedOnStart=off: this suite asserts pending rows stay
    // inert until the explicit resume path (`--resume-all` still forces the
    // sweep over the pin), but the `auto` default resumes on headless hosts.
    common::disable_resume_on_start(data_dir);
    let mut cmd = Command::new(env!("CARGO_BIN_EXE_intentd"));
    cmd.arg("serve")
        .env("INTENTD_DATA_DIR", data_dir)
        .env("INTENTD_WORKSPACES_DIR", &workspaces_dir)
        .env("INTENTD_SECRETS_FILE", &secrets_file)
        .env("INTENTD_ASSERT_HERMETIC_ROOT", "1")
        .stdout(Stdio::null())
        .stderr(Stdio::from(log));
    if resume_all {
        cmd.arg("--resume-all");
    }
    for (k, v) in env {
        cmd.env(k, v);
    }
    // Spawn in its own process group (pgid == child pid) so killing the daemon
    // also kills spawned Node.js ACP mock providers via killpg.
    #[cfg(unix)]
    cmd.process_group(0);
    cmd.spawn().expect("spawn intentd serve")
}

fn workspace_seed(id: &intent_core::WorkspaceId) -> intent_core::Workspace {
    use intent_core::{now_iso, Workspace, WorkspaceActivity, WorkspaceAttention, WorkspaceStatus};
    let ts = now_iso();
    Workspace {
        id: id.clone(),
        title: "QUEUE-ORDER-E2E".to_string(),
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
        waiting: false,
        checkout_mode: None,
        disk_usage: None,
        pending_delete_at: None,
    }
}

struct Daemon {
    child: Child,
    data_dir: PathBuf,
}

impl Drop for Daemon {
    fn drop(&mut self) {
        // Kill the whole process group FIRST (daemon + any Node.js ACP provider
        // children) BEFORE wait(), so children are reaped before reparenting.
        #[cfg(unix)]
        {
            use nix::sys::signal::{self, Signal};
            use nix::unistd::Pid;
            let pid = Pid::from_raw(self.child.id() as i32);
            let _ = signal::killpg(pid, Signal::SIGKILL);
        }
        #[cfg(not(unix))]
        {
            let _ = self.child.kill();
        }
        let _ = self.child.wait();
        // On test panic, print data-dir path + daemon log tail for diagnosability
        if std::thread::panicking() {
            eprintln!("\n=== DAEMON CLEANUP (test panicked) ===");
            eprintln!("Data dir: {}", self.data_dir.display());
            let log_path = self.data_dir.join("daemon.log");
            if let Ok(log) = std::fs::read_to_string(&log_path) {
                let lines: Vec<_> = log.lines().rev().take(30).collect();
                eprintln!("Last 30 lines of daemon.log:");
                for line in lines.iter().rev() {
                    eprintln!("  {line}");
                }
            }
        }
    }
}

fn gate(test: &str) -> Option<String> {
    let script = std::env::var("MOCK_AGENT_SCRIPT_PATH").unwrap_or_else(|_| {
        format!(
            "{}/tests/fixtures/mock-acp-agent.mjs",
            env!("CARGO_MANIFEST_DIR")
        )
    });
    if !std::path::Path::new(&script).exists() {
        eprintln!("Skip {test}: mock ACP not found at {script}");
        return None;
    }
    Some(script)
}

/// Shared phase 1: boot daemon1 whose mock parks the first turn for 10
/// minutes, create an agent, start a turn (mid-turn `Active` status is
/// persisted by the slot claim), queue two messages behind it — the
/// write-through snapshot is awaited inside `agent.queueMessage`, so both
/// are durable once the RPC returns — then SIGKILL the daemon's process
/// group (crash semantics). Returns `(workspace_id, agent_id)`.
async fn interrupt_midturn_with_queued_messages(data_dir: &Path, script: &str) -> (String, String) {
    let behavior = json!({
        "response": "first turn done",
        "firstTurnDelayMs": 600_000
    })
    .to_string();
    let env: [(&str, &str); 4] = [
        ("INTENTD_AUTH_TOKEN", TOKEN),
        ("INTENTD_TCP_PORT", "0"),
        ("MOCK_AGENT_SCRIPT_PATH", script),
        ("MOCK_AGENT_BEHAVIOR", &behavior),
    ];

    eprintln!("Phase 1: boot daemon1, run agent mid-turn, queue 2 messages");
    let child1 = spawn_serve(data_dir, "both", &env, false);
    let daemon1 = Daemon {
        child: child1,
        data_dir: data_dir.to_path_buf(),
    };
    let socket = data_dir.join("intentd.sock");
    assert!(await_uds(&socket).await, "daemon1 did not start");

    let ws_id = {
        use intent_core::WorkspaceId;
        use intent_store::Store;
        let store = Store::open(&data_dir.join("intentd.db"))
            .await
            .expect("open store");
        let ws = WorkspaceId::new();
        store
            .insert_workspace(&workspace_seed(&ws))
            .await
            .expect("insert ws");
        ws.0
    };

    let create_result = uds_rpc(
        &socket,
        1,
        "agent.create",
        json!({
            "workspaceId": ws_id,
            "name": "Queue Order Agent",
            "model": "mock:default"
        }),
    )
    .await;
    let agent_id = create_result["result"]["agent"]["id"]
        .as_str()
        .expect("agent id")
        .to_string();

    let send = uds_rpc(
        &socket,
        2,
        "agent.sendMessage",
        json!({
            "workspaceId": ws_id,
            "agentId": agent_id,
            "content": START_MSG
        }),
    )
    .await;
    assert_eq!(
        send["result"]["queued"],
        json!(false),
        "start message must stream (idle agent), not queue: {send}"
    );

    // Wait for the persisted `Active` status so the restart heal sweep will
    // capture the agent as interrupted.
    let mut active = false;
    for _ in 0..100 {
        let got = uds_rpc(
            &socket,
            3,
            "agent.get",
            json!({ "workspaceId": ws_id, "agentId": agent_id }),
        )
        .await;
        if got["result"]["agent"]["isActive"].as_bool() == Some(true) {
            active = true;
            break;
        }
        tokio::time::sleep(Duration::from_millis(100)).await;
    }
    assert!(active, "agent never reached active mid-turn state");

    // Queue two messages behind the parked turn.
    let q1 = uds_rpc(
        &socket,
        4,
        "agent.queueMessage",
        json!({ "agentId": agent_id, "content": QUEUED_ONE }),
    )
    .await;
    assert_eq!(q1["result"]["success"], json!(true), "queue one: {q1}");
    let q2 = uds_rpc(
        &socket,
        5,
        "agent.queueMessage",
        json!({ "agentId": agent_id, "content": QUEUED_TWO }),
    )
    .await;
    assert_eq!(q2["result"]["success"], json!(true), "queue two: {q2}");

    let queue = uds_rpc(&socket, 6, "agent.getQueue", json!({ "agentId": agent_id })).await;
    let entries = queue["result"]["queue"].as_array().expect("queue array");
    assert_eq!(entries.len(), 2, "expected 2 queued messages: {queue}");
    assert_eq!(entries[0]["content"], json!(QUEUED_ONE));
    assert_eq!(entries[1]["content"], json!(QUEUED_TWO));

    eprintln!("Killing daemon1 mid-turn (SIGKILL to process group)");
    drop(daemon1);
    tokio::time::sleep(Duration::from_secs(1)).await;
    (ws_id, agent_id)
}

/// Assert the restart invariants over WSS: the agent is pending in
/// `agent.listInterrupted`, the queue was rehydrated in original order, and
/// rehydration alone started no turn (`isActive == false`).
async fn assert_rehydrated_pending_state(
    ws: &mut WebSocketStream<tokio_rustls::client::TlsStream<TcpStream>>,
    ws_id: &str,
    agent_id: &str,
) {
    let list = wss_rpc(ws, 20, "agent.listInterrupted", json!({})).await;
    let agents = list["agents"].as_array().expect("agents array");
    assert!(
        agents.iter().any(|a| a["agentId"] == json!(agent_id)),
        "agent {agent_id} should be pending interrupted: {list}"
    );

    let queue = wss_rpc(ws, 21, "agent.getQueue", json!({ "agentId": agent_id })).await;
    let entries = queue["queue"].as_array().expect("queue array");
    assert_eq!(
        entries.len(),
        2,
        "rehydrated queue should hold both preserved messages: {queue}"
    );
    assert_eq!(entries[0]["content"], json!(QUEUED_ONE));
    assert_eq!(entries[0]["position"], json!(0));
    assert_eq!(entries[1]["content"], json!(QUEUED_TWO));
    assert_eq!(entries[1]["position"], json!(1));

    let got = wss_rpc(
        ws,
        22,
        "agent.get",
        json!({ "workspaceId": ws_id, "agentId": agent_id }),
    )
    .await;
    assert_eq!(
        got["agent"]["isActive"],
        json!(false),
        "rehydration alone must never start a turn: {got}"
    );
}

/// Poll over WSS until the continuation turn and the queue drain have fully
/// settled: queue empty AND agent idle. Panics after 30s.
async fn await_drained_and_idle(
    ws: &mut WebSocketStream<tokio_rustls::client::TlsStream<TcpStream>>,
    ws_id: &str,
    agent_id: &str,
) {
    let deadline = tokio::time::Instant::now() + Duration::from_secs(30);
    while tokio::time::Instant::now() < deadline {
        let queue = wss_rpc(ws, 30, "agent.getQueue", json!({ "agentId": agent_id })).await;
        let empty = queue["queue"]
            .as_array()
            .is_some_and(std::vec::Vec::is_empty);
        let got = wss_rpc(
            ws,
            31,
            "agent.get",
            json!({ "workspaceId": ws_id, "agentId": agent_id }),
        )
        .await;
        let idle = got["agent"]["isActive"].as_bool() == Some(false);
        if empty && idle {
            return;
        }
        tokio::time::sleep(Duration::from_millis(250)).await;
    }
    panic!("agent {agent_id} did not drain its queue and go idle within 30s");
}

/// Read the agent's transcript from the store and return the first text block
/// of every user-role message, in transcript order.
async fn user_message_texts(data_dir: &Path, agent_id: &str) -> Vec<String> {
    use intent_core::AgentId;
    let store = intent_store::Store::open(&data_dir.join("intentd.db"))
        .await
        .expect("open store");
    let session = store
        .get_agent_session(&AgentId(agent_id.to_string()))
        .await
        .expect("get agent session");
    session
        .messages
        .iter()
        .filter(|m| m.role == "user")
        .filter_map(|m| {
            m.content
                .as_array()
                .and_then(|blocks| blocks.first())
                .and_then(|b| b["text"].as_str())
                .map(String::from)
        })
        .collect()
}

/// Assert the resume-ordering contract on the transcript: the continuation
/// user message lands strictly BEFORE both preserved queued messages, which
/// appear exactly once each, in their original FIFO order.
fn assert_continuation_first_then_fifo(users: &[String]) {
    // Prefix match: drained queue entries carry the appended dequeue-wait
    // system note after the original content.
    let idx = |needle: &str| {
        users
            .iter()
            .position(|t| t.starts_with(needle))
            .unwrap_or_else(|| panic!("missing user message {needle:?} in transcript: {users:?}"))
    };
    let i_start = idx(START_MSG);
    let i_cont = idx(CONTINUATION_PREFIX);
    let i_one = idx(QUEUED_ONE);
    let i_two = idx(QUEUED_TWO);
    assert!(
        users[i_cont].contains("due to a harness shutdown and restart"),
        "continuation must carry the duration sentence: {users:?}"
    );
    assert!(
        i_start < i_cont,
        "original message must precede continuation: {users:?}"
    );
    assert!(
        i_cont < i_one,
        "continuation must land before the first preserved queued message: {users:?}"
    );
    assert!(
        i_one < i_two,
        "preserved queued messages must drain FIFO: {users:?}"
    );
    for needle in [CONTINUATION_PREFIX, QUEUED_ONE, QUEUED_TWO] {
        assert_eq!(
            users.iter().filter(|t| t.starts_with(needle)).count(),
            1,
            "user message {needle:?} must appear exactly once: {users:?}"
        );
    }
}

/// Boot a restart daemon (fast mock turns), fetch fingerprint + port over
/// UDS, and open a WSS connection.
async fn boot_restart_daemon(
    data_dir: &Path,
    script: &str,
    resume_all: bool,
) -> (
    Daemon,
    WebSocketStream<tokio_rustls::client::TlsStream<TcpStream>>,
) {
    let behavior = json!({ "response": "resumed turn done" }).to_string();
    let env: [(&str, &str); 4] = [
        ("INTENTD_AUTH_TOKEN", TOKEN),
        ("INTENTD_TCP_PORT", "0"),
        ("MOCK_AGENT_SCRIPT_PATH", script),
        ("MOCK_AGENT_BEHAVIOR", &behavior),
    ];
    let child = spawn_serve(data_dir, "both", &env, resume_all);
    let daemon = Daemon {
        child,
        data_dir: data_dir.to_path_buf(),
    };
    let socket = data_dir.join("intentd.sock");
    assert!(await_uds(&socket).await, "restart daemon did not start");

    let status = common::await_wss_status(&socket).await;
    let fp = status["result"]["fingerprint"]
        .as_str()
        .expect("fingerprint")
        .to_string();
    let port = status["result"]["port"].as_u64().expect("port") as u16;
    let ws = connect_ws(port, client_config(&fp)).await;
    (daemon, ws)
}

/// RPC resume path: after a mid-turn crash with two queued messages, the
/// restarted daemon rehydrates the queue in order and starts no turn; on
/// `agent.resolveInterrupted { resume }` the continuation streams first and
/// the preserved queue drains after it, FIFO.
#[tokio::test]
async fn resume_rpc_continuation_first_then_queue_fifo() {
    let Some(script) = gate("resume_rpc_continuation_first_then_queue_fifo") else {
        return;
    };
    let data_dir = temp_data_dir();
    let (ws_id, agent_id) = interrupt_midturn_with_queued_messages(&data_dir, &script).await;

    eprintln!("Phase 2: restart daemon, verify rehydrated pending state over WSS");
    let (daemon2, mut ws) = boot_restart_daemon(&data_dir, &script, false).await;
    assert_rehydrated_pending_state(&mut ws, &ws_id, &agent_id).await;

    eprintln!("Phase 3: resolveInterrupted {{ resume }} and await full drain");
    let result = wss_rpc(
        &mut ws,
        23,
        "agent.resolveInterrupted",
        json!({ "resume": [agent_id.clone()] }),
    )
    .await;
    assert_eq!(
        result["resumed"].as_array().map(Vec::len),
        Some(1),
        "resume should succeed: {result}"
    );
    assert_eq!(
        result["failed"].as_array().map(Vec::len),
        Some(0),
        "resume should not fail: {result}"
    );

    await_drained_and_idle(&mut ws, &ws_id, &agent_id).await;
    drop(ws);
    drop(daemon2);

    eprintln!("Phase 4: assert transcript ordering");
    let users = user_message_texts(&data_dir, &agent_id).await;
    assert_continuation_first_then_fifo(&users);

    let _ = std::fs::remove_dir_all(&data_dir);
}

/// Abandon path: the preserved queue stays intact and inert (no auto-send),
/// the system interruption message is appended, and entries remain
/// individually removable via `agent.removeQueuedMessage`.
#[tokio::test]
async fn abandon_keeps_preserved_queue_inert() {
    let Some(script) = gate("abandon_keeps_preserved_queue_inert") else {
        return;
    };
    let data_dir = temp_data_dir();
    let (ws_id, agent_id) = interrupt_midturn_with_queued_messages(&data_dir, &script).await;

    eprintln!("Phase 2: restart daemon, verify rehydrated pending state over WSS");
    let (daemon2, mut ws) = boot_restart_daemon(&data_dir, &script, false).await;
    assert_rehydrated_pending_state(&mut ws, &ws_id, &agent_id).await;

    eprintln!("Phase 3: resolveInterrupted {{ abandon }}");
    let result = wss_rpc(
        &mut ws,
        23,
        "agent.resolveInterrupted",
        json!({ "abandon": [agent_id.clone()] }),
    )
    .await;
    assert_eq!(
        result["abandoned"].as_array().map(Vec::len),
        Some(1),
        "abandon should succeed: {result}"
    );

    // Give any (incorrect) auto-send a chance to fire before asserting inertia.
    tokio::time::sleep(Duration::from_secs(2)).await;

    let queue = wss_rpc(
        &mut ws,
        24,
        "agent.getQueue",
        json!({ "agentId": agent_id }),
    )
    .await;
    let entries = queue["queue"].as_array().expect("queue array");
    assert_eq!(
        entries.len(),
        2,
        "abandon must leave the preserved queue untouched: {queue}"
    );
    assert_eq!(entries[0]["content"], json!(QUEUED_ONE));
    assert_eq!(entries[1]["content"], json!(QUEUED_TWO));
    let removable_id = entries[0]["id"]
        .as_str()
        .expect("queue entry id")
        .to_string();

    let got = wss_rpc(
        &mut ws,
        25,
        "agent.get",
        json!({ "workspaceId": ws_id, "agentId": agent_id }),
    )
    .await;
    assert_eq!(
        got["agent"]["isActive"],
        json!(false),
        "abandon must not start a turn: {got}"
    );

    // Queue entries stay individually removable.
    let removed = wss_rpc(
        &mut ws,
        26,
        "agent.removeQueuedMessage",
        json!({ "agentId": agent_id, "messageId": removable_id }),
    )
    .await;
    assert_eq!(removed["success"], json!(true), "remove failed: {removed}");
    let queue2 = wss_rpc(
        &mut ws,
        27,
        "agent.getQueue",
        json!({ "agentId": agent_id }),
    )
    .await;
    let entries2 = queue2["queue"].as_array().expect("queue array");
    assert_eq!(
        entries2.len(),
        1,
        "expected 1 entry after removal: {queue2}"
    );
    assert_eq!(entries2[0]["content"], json!(QUEUED_TWO));

    drop(ws);
    drop(daemon2);

    // Transcript: system interruption message appended; queued texts never sent.
    let users = user_message_texts(&data_dir, &agent_id).await;
    assert!(
        !users.iter().any(|t| t == QUEUED_ONE || t == QUEUED_TWO),
        "abandon must not auto-send preserved queue: {users:?}"
    );
    assert!(
        !users.iter().any(|t| t.starts_with("You were interrupted ")),
        "abandon must not send the continuation: {users:?}"
    );
    {
        use intent_core::AgentId;
        let store = intent_store::Store::open(&data_dir.join("intentd.db"))
            .await
            .expect("open store");
        let session = store
            .get_agent_session(&AgentId(agent_id.clone()))
            .await
            .expect("get agent session");
        let last = session.messages.last().expect("expected messages");
        assert_eq!(last.role, "system", "expected system interruption message");
        let blocks = last.content.as_array().expect("content blocks");
        assert_eq!(blocks[0]["meta"]["kind"], json!("interruption"));
    }

    let _ = std::fs::remove_dir_all(&data_dir);
}

/// Headless `serve --resume-all`: the startup sweep resumes the agent with
/// the same ordering contract — continuation first, preserved queue after,
/// FIFO — with no RPC involved.
#[tokio::test]
async fn resume_all_continuation_first_then_queue_fifo() {
    let Some(script) = gate("resume_all_continuation_first_then_queue_fifo") else {
        return;
    };
    let data_dir = temp_data_dir();
    let (ws_id, agent_id) = interrupt_midturn_with_queued_messages(&data_dir, &script).await;

    eprintln!("Phase 2: restart daemon with --resume-all, await headless drain");
    let (daemon2, mut ws) = boot_restart_daemon(&data_dir, &script, true).await;

    // The sweep runs at startup; the agent may already be resumed (or even
    // fully drained) by the time the WSS connection opens, so poll straight
    // for the settled state instead of asserting the pending snapshot.
    await_drained_and_idle(&mut ws, &ws_id, &agent_id).await;

    // The interrupted row must be resolved by the sweep.
    let list = wss_rpc(&mut ws, 40, "agent.listInterrupted", json!({})).await;
    let agents = list["agents"].as_array().expect("agents array");
    assert!(
        agents.iter().all(|a| a["agentId"] != json!(agent_id)),
        "resume-all should resolve the interrupted row: {list}"
    );

    drop(ws);
    drop(daemon2);

    eprintln!("Phase 3: assert transcript ordering");
    let users = user_message_texts(&data_dir, &agent_id).await;
    assert_continuation_first_then_fifo(&users);

    let _ = std::fs::remove_dir_all(&data_dir);
}
