//! WSS end-to-end mid-turn agent failure (STAB terminal turn-failure path):
//! the child spawns and handshakes fine, then dies while `session/prompt` is
//! in flight. The daemon must surface the failure — `agent:failed`,
//! `agent:stream:end`, persisted `status=error` — requeue the message, and
//! redrive it via `agent.retry` (no silent drop). The crash must also leave
//! the child's stderr captured under `<data_dir>/agent-logs/<agent-id>/`
//! with the terminal-failure WARN pointing at the capture path (STAB-53).
//!
//! Gated on `node` + the mock script; skips cleanly otherwise.

#![cfg(unix)]

use std::net::{Ipv4Addr, TcpListener as StdTcpListener};
use std::path::{Path, PathBuf};
use std::process::{Child, Command, Stdio};
use std::sync::Arc;
use std::time::Duration;

use futures_util::{SinkExt, StreamExt};
use rustls::client::danger::{HandshakeSignatureValid, ServerCertVerified, ServerCertVerifier};
use rustls::crypto::CryptoProvider;
use rustls::{ClientConfig, DigitallySignedStruct, SignatureScheme};
use rustls_pki_types::{CertificateDer, ServerName, UnixTime};
use serde_json::{json, Value};
use sha2::{Digest, Sha256};
use tokio::io::{AsyncBufReadExt, AsyncWriteExt, BufReader};
use tokio::net::{TcpStream, UnixStream};
use tokio::time::timeout;
use tokio_rustls::TlsConnector;
use tokio_tungstenite::tungstenite::Message;
use tokio_tungstenite::WebSocketStream;
use uuid::Uuid;

const TOKEN: &str = "efefefefefefefefefefefefefefefefefefefefefefefefefefefefefefefef";

struct Daemon {
    child: Child,
    data_dir: PathBuf,
}

impl Drop for Daemon {
    fn drop(&mut self) {
        let _ = self.child.kill();
        let _ = self.child.wait();
        let log_path = self.data_dir.join("daemon.log");
        if let Ok(log) = std::fs::read_to_string(&log_path) {
            eprintln!("=== DAEMON LOG ===\n{}\n=== END LOG ===", log);
        }
        let _ = std::fs::remove_dir_all(&self.data_dir);
    }
}

fn free_port() -> u16 {
    StdTcpListener::bind((Ipv4Addr::LOCALHOST, 0))
        .unwrap()
        .local_addr()
        .unwrap()
        .port()
}

fn temp_data_dir() -> PathBuf {
    let id = Uuid::new_v4().simple().to_string();
    let dir = PathBuf::from("/tmp").join(format!("itd-wss-midturn-{}", &id[..8]));
    std::fs::create_dir_all(&dir).expect("mkdir data dir");
    dir
}

fn spawn_serve(data_dir: &Path, listen: &str, env: &[(&str, &str)]) -> Child {
    let log = std::fs::File::create(data_dir.join("daemon.log")).expect("create daemon log");
    let workspaces_dir = data_dir.join("workspaces");
    std::fs::create_dir_all(&workspaces_dir).expect("mkdir hermetic workspaces dir");
    let mut cmd = Command::new(env!("CARGO_BIN_EXE_intentd"));
    cmd.arg("serve")
        .arg("--listen")
        .arg(listen)
        .env("INTENTD_DATA_DIR", data_dir)
        .env("INTENTD_WORKSPACES_DIR", &workspaces_dir)
        .env("INTENTD_ASSERT_HERMETIC_ROOT", "1")
        .stdout(Stdio::null())
        .stderr(Stdio::from(log));
    for (k, v) in env {
        cmd.env(k, v);
    }
    cmd.spawn().expect("spawn intentd serve")
}

async fn await_uds(socket: &Path) -> bool {
    timeout(Duration::from_secs(10), async {
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
    timeout(Duration::from_secs(5), reader.read_line(&mut buf))
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
        _ocsp: &[u8],
        _now: UnixTime,
    ) -> Result<ServerCertVerified, rustls::Error> {
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
    fn supported_verify_schemes(&self) -> Vec<SignatureScheme> {
        self.provider
            .signature_verification_algorithms
            .supported_schemes()
    }
}

fn client_config(fingerprint: &str) -> Arc<ClientConfig> {
    let provider = Arc::new(rustls::crypto::ring::default_provider());
    let config = ClientConfig::builder_with_provider(provider.clone())
        .with_safe_default_protocol_versions()
        .unwrap()
        .dangerous()
        .with_custom_certificate_verifier(Arc::new(PinnedVerifier {
            fingerprint: fingerprint.to_string(),
            provider,
        }))
        .with_no_client_auth();
    Arc::new(config)
}

async fn tls_connect(
    port: u16,
    cfg: Arc<ClientConfig>,
) -> tokio_rustls::client::TlsStream<TcpStream> {
    let tcp = TcpStream::connect((Ipv4Addr::LOCALHOST, port))
        .await
        .expect("tcp connect");
    let name = ServerName::try_from("localhost").unwrap();
    TlsConnector::from(cfg)
        .connect(name, tcp)
        .await
        .expect("tls connect")
}

async fn connect_ws(
    port: u16,
    cfg: Arc<ClientConfig>,
) -> WebSocketStream<tokio_rustls::client::TlsStream<TcpStream>> {
    let tls = tls_connect(port, cfg).await;
    let url = format!("wss://localhost:{port}/ws?token={TOKEN}");
    let (ws, _resp) = tokio_tungstenite::client_async(url, tls)
        .await
        .expect("ws handshake");
    ws
}

async fn wss_rpc<S>(ws: &mut WebSocketStream<S>, id: i64, method: &str, params: Value) -> Value
where
    S: tokio::io::AsyncRead + tokio::io::AsyncWrite + Unpin,
{
    let frame = json!({ "jsonrpc": "2.0", "id": id, "method": method, "params": params });
    ws.send(Message::Text(frame.to_string()))
        .await
        .expect("send rpc frame");
    loop {
        let next = timeout(Duration::from_secs(15), ws.next())
            .await
            .expect("wss rpc timed out");
        match next {
            Some(Ok(Message::Text(text))) => {
                let v: Value = serde_json::from_str(&text).expect("json frame");
                if v["id"] == json!(id) {
                    assert!(v.get("error").is_none(), "rpc {method} errored: {v}");
                    return v["result"].clone();
                }
            }
            Some(Ok(Message::Ping(p))) => {
                let _ = ws.send(Message::Pong(p)).await;
            }
            Some(Ok(_)) => continue,
            other => panic!("expected text frame, got {other:?}"),
        }
    }
}

async fn wss_event<S>(ws: &mut WebSocketStream<S>, secs: u64) -> Value
where
    S: tokio::io::AsyncRead + tokio::io::AsyncWrite + Unpin,
{
    loop {
        let next = timeout(Duration::from_secs(secs), ws.next())
            .await
            .expect("wss event timed out");
        match next {
            Some(Ok(Message::Text(text))) => {
                let v: Value = serde_json::from_str(&text).expect("json frame");
                if v["method"] == "events.event" {
                    return v;
                }
            }
            Some(Ok(Message::Ping(p))) => {
                let _ = ws.send(Message::Pong(p)).await;
            }
            Some(Ok(_)) => continue,
            other => panic!("expected text frame, got {other:?}"),
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
    if intent_providers::resolve_on_path("node").is_none() {
        eprintln!("skipping {test}: node not on PATH");
        return None;
    }
    if !std::path::Path::new(&script).exists() {
        eprintln!("skipping {test}: mock script missing at {script}");
        return None;
    }
    Some(script)
}

async fn seed_workspace_only(data_dir: &Path) -> String {
    use intent_core::WorkspaceId;
    use intent_store::Store;
    let db_path = data_dir.join("intentd.db");
    let store = Store::open(&db_path).await.expect("open store");
    let ws = WorkspaceId::new();
    store
        .insert_workspace(&workspace_seed(&ws))
        .await
        .expect("insert ws");
    ws.0
}

fn workspace_seed(id: &intent_core::WorkspaceId) -> intent_core::Workspace {
    use intent_core::{now_iso, Workspace, WorkspaceActivity, WorkspaceAttention, WorkspaceStatus};
    let ts = now_iso();
    Workspace {
        id: id.clone(),
        title: "WSS-MIDTURN-E2E".to_string(),
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

/// MIDTURN-1: child dies mid-`session/prompt` → terminal `agent:failed` +
/// `agent:stream:end` + persisted `status=error` + message requeued, then
/// `agent.retry` redrives the queued message to a successful turn.
///
/// The mock spawns and handshakes normally (so the spawn-retry path is NOT in
/// play), then `process.exit(1)`s inside `session/prompt` on the first attempt
/// only. The daemon's in-flight prompt fails ("agent stdout closed"), which the
/// worker must classify as a terminal mid-turn failure — the exact "silent
/// drop" scenario this test locks down.
#[tokio::test]
async fn agent_midturn_failure_surfaces_and_retries_over_wss() {
    let Some(script) = gate("WSS mid-turn failure E2E") else {
        return;
    };
    let data_dir = temp_data_dir();
    let ws_id = seed_workspace_only(&data_dir).await;
    let attempt_file = data_dir.join("attempts.txt");
    let attempt_file_s = attempt_file.to_string_lossy().into_owned();
    let behavior = json!({
        "exitDuringPromptAttempts": 1,
        "response": "recovered after mid-turn crash",
    })
    .to_string();
    let port_s = free_port().to_string();
    let env: [(&str, &str); 7] = [
        ("INTENTD_AUTH_TOKEN", TOKEN),
        ("INTENTD_TCP_PORT", &port_s),
        ("MOCK_AGENT_SCRIPT_PATH", &script),
        ("MOCK_AGENT_BEHAVIOR", &behavior),
        ("MOCK_AGENT_ATTEMPT_FILE", &attempt_file_s),
        ("INTENTD_SESSION_SETUP_TIMEOUT_MS", "2000"),
        ("INTENTD_SPAWN_RETRY_BACKOFF_MS", "100,200"),
    ];
    let child = spawn_serve(&data_dir, "both", &env);
    let _daemon = Daemon {
        child,
        data_dir: data_dir.clone(),
    };
    let socket = data_dir.join("intentd.sock");
    assert!(await_uds(&socket).await, "daemon did not start");
    let status = uds_rpc(&socket, 1, "system.status", json!({})).await;
    let port = status["result"]["port"].as_u64().expect("port") as u16;
    let fingerprint = status["result"]["fingerprint"]
        .as_str()
        .expect("fingerprint")
        .to_string();
    let cfg = client_config(&fingerprint);

    let mut sub = connect_ws(port, cfg.clone()).await;
    let sub_resp = wss_rpc(
        &mut sub,
        1,
        "events.subscribe",
        json!({ "eventTypes": ["agent:*"], "workspaceId": ws_id }),
    )
    .await;
    assert!(
        sub_resp["subscriptionId"].is_string(),
        "subscribed: {sub_resp}"
    );

    let mut rpc = connect_ws(port, cfg.clone()).await;
    let created = wss_rpc(
        &mut rpc,
        10,
        "agent.create",
        json!({ "workspaceId": ws_id, "name": "WSS-MIDTURN", "model": "mock:default" }),
    )
    .await;
    let agent_id = created["agent"]["id"]
        .as_str()
        .expect("agent id")
        .to_string();

    let sent = wss_rpc(
        &mut rpc,
        11,
        "agent.sendMessage",
        json!({ "workspaceId": ws_id, "agentId": agent_id, "content": "dies mid-turn" }),
    )
    .await;
    assert_eq!(sent["success"], true, "sendMessage ok: {sent}");

    // Wait for the full terminal triple: agent:failed, agent:stream:end, and
    // agent:status-changed(status=error). The status-changed event is emitted
    // AFTER the status persist, so seeing it guarantees the write is visible.
    let mut saw_failed = false;
    let mut saw_end = false;
    let mut saw_status_error = false;
    for _ in 0..200 {
        let frame = wss_event(&mut sub, 30).await;
        let event_agent_id = frame["params"]["event"]["data"]["agentId"].as_str();
        if event_agent_id != Some(agent_id.as_str()) {
            continue;
        }
        match frame["params"]["event"]["type"].as_str() {
            Some("agent:failed") => {
                // run_prompt_turn emits the raw prompt error (the mock's exit
                // closes stdout, failing the in-flight session/prompt).
                let err = frame["params"]["event"]["data"]["error"]
                    .as_str()
                    .unwrap_or("");
                assert!(
                    err.contains("agent stdout closed"),
                    "agent:failed carries the mid-turn prompt error, got: {err}"
                );
                saw_failed = true;
            }
            Some("agent:stream:end") => {
                saw_end = true;
            }
            Some("agent:status-changed")
                if frame["params"]["event"]["data"]["status"] == "error" =>
            {
                saw_status_error = true;
            }
            _ => {}
        }
        if saw_failed && saw_end && saw_status_error {
            break;
        }
    }
    assert!(
        saw_failed,
        "terminal agent:failed emitted after mid-turn death"
    );
    assert!(
        saw_end,
        "terminal agent:stream:end emitted after mid-turn death"
    );
    assert!(
        saw_status_error,
        "agent:status-changed with status=error emitted after mid-turn death"
    );

    // Persisted error status is visible over the RPC surface.
    let session = wss_rpc(
        &mut rpc,
        12,
        "agent.getSession",
        json!({ "workspaceId": ws_id, "agentId": agent_id }),
    )
    .await;
    assert_eq!(
        session["session"]["status"], "error",
        "session status is error after mid-turn failure"
    );

    // STAB-53: the mid-turn crash left the child's stderr captured under
    // `<data_dir>/agent-logs/<agent-id>/<today>.log` — the mock logs every
    // phase to stderr, including the deliberate exit inside session/prompt.
    let capture_path = intent_core::agent_logs_root(&data_dir)
        .join(&agent_id)
        .join(intent_core::current_agent_log_file_name());
    let mut captured = String::new();
    for _ in 0..100 {
        if let Ok(c) = tokio::fs::read_to_string(&capture_path).await {
            captured = c;
            if captured.contains("exiting during prompt") {
                break;
            }
        }
        tokio::time::sleep(Duration::from_millis(25)).await;
    }
    assert!(
        captured.contains("exiting during prompt"),
        "stderr capture at {} holds the child's last words; got: {captured:?}",
        capture_path.display()
    );

    // STAB-53: the terminal-failure WARN points at the capture path so the
    // crash is diagnosable straight from the daemon log.
    let daemon_log_path = data_dir.join("daemon.log");
    let expected_hint = format!("agent stderr captured at {}", capture_path.display());
    let mut daemon_log = String::new();
    for _ in 0..100 {
        daemon_log = tokio::fs::read_to_string(&daemon_log_path)
            .await
            .unwrap_or_default();
        if daemon_log.contains(&expected_hint) {
            break;
        }
        tokio::time::sleep(Duration::from_millis(25)).await;
    }
    assert!(
        daemon_log.contains(&expected_hint),
        "terminal-failure WARN includes the stderr capture path hint {expected_hint:?}"
    );

    // The failed message was requeued (not silently dropped): the queue holds
    // exactly the original content, ready for the retry redrive.
    let queue = wss_rpc(
        &mut rpc,
        13,
        "agent.getQueue",
        json!({ "workspaceId": ws_id, "agentId": agent_id }),
    )
    .await;
    let messages = queue["queue"].as_array().expect("queue array");
    assert_eq!(messages.len(), 1, "failed message requeued: {queue}");
    assert_eq!(
        messages[0]["content"], "dies mid-turn",
        "requeued message preserves the original content"
    );

    // agent.retry redrives the requeued message; the mock's attempt counter is
    // past the failure window, so the fresh child completes the turn.
    let retry_result = wss_rpc(
        &mut rpc,
        14,
        "agent.retry",
        json!({ "workspaceId": ws_id, "agentId": agent_id }),
    )
    .await;
    assert_eq!(retry_result["ok"], true, "agent.retry ok on error status");

    let mut saw_chunk = false;
    let mut saw_retry_end = false;
    for _ in 0..200 {
        let frame = wss_event(&mut sub, 30).await;
        let event_agent_id = frame["params"]["event"]["data"]["agentId"].as_str();
        if event_agent_id != Some(agent_id.as_str()) {
            continue;
        }
        match frame["params"]["event"]["type"].as_str() {
            Some("agent:stream:chunk") => {
                saw_chunk = true;
            }
            Some("agent:stream:end") => {
                saw_retry_end = true;
                break;
            }
            Some("agent:failed") => {
                panic!("agent:failed emitted AGAIN after retry: {frame}");
            }
            _ => {}
        }
    }
    assert!(saw_chunk, "agent:stream:chunk emitted after retry redrive");
    assert!(
        saw_retry_end,
        "agent:stream:end emitted after retry redrive"
    );

    // Status recovered from error.
    let final_session = wss_rpc(
        &mut rpc,
        15,
        "agent.getSession",
        json!({ "workspaceId": ws_id, "agentId": agent_id }),
    )
    .await;
    assert_ne!(
        final_session["session"]["status"], "error",
        "session status recovered after agent.retry"
    );

    // The retry redrive must NOT duplicate the user message: the original
    // send persisted it before the failed turn, and the requeued entry is
    // flagged `persisted` so the drain skips a second transcript append.
    let convo = wss_rpc(
        &mut rpc,
        16,
        "agent.getConversation",
        json!({ "workspaceId": ws_id, "agentId": agent_id }),
    )
    .await;
    let user_rows: Vec<&Value> = convo["messages"]
        .as_array()
        .expect("conversation messages array")
        .iter()
        .filter(|m| m["role"] == "user")
        .collect();
    assert_eq!(
        user_rows.len(),
        1,
        "exactly one user row after retry (no duplicate): {convo}"
    );
}
