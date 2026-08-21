//! WSS end-to-end agent spawn retry and exhaustion (RETRY-1): prove the retry
//! behavior over the real WSS transport per packages/intentd/AGENTS.md (every
//! feature needs a WSS e2e, not just unit tests).
//!
//! Drives the full spawn → retry → success or terminal-failure → agent:failed
//! paths over a pinned TLS WebSocket, asserting retry hints (agent:stream:status)
//! and terminal events reach the subscriber at the right ordinals.
//!
//! Gated on `node` + the mock script; skips cleanly otherwise.

#![cfg(unix)]

mod common;

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
use tokio::net::{TcpStream, UnixStream};
use tokio::time::timeout;
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
        // Print daemon log for debugging
        let log_path = self.data_dir.join("daemon.log");
        if let Ok(log) = std::fs::read_to_string(&log_path) {
            eprintln!("=== DAEMON LOG ===\n{log}\n=== END LOG ===");
        }
        let _ = std::fs::remove_dir_all(&self.data_dir);
    }
}

fn temp_data_dir() -> PathBuf {
    let id = Uuid::new_v4().simple().to_string();
    let dir = PathBuf::from("/tmp").join(format!("itd-wss-retry-{}", &id[..8]));
    std::fs::create_dir_all(&dir).expect("mkdir data dir");
    dir
}

fn spawn_serve(data_dir: &Path, listen: &str, env: &[(&str, &str)]) -> Child {
    let log = std::fs::File::create(data_dir.join("daemon.log")).expect("create daemon log");
    let workspaces_dir = data_dir.join("workspaces");
    std::fs::create_dir_all(&workspaces_dir).expect("mkdir hermetic workspaces dir");
    if listen != "uds" {
        common::enable_ws_api(data_dir);
    }
    let mut cmd = Command::new(env!("CARGO_BIN_EXE_intentd"));
    cmd.arg("serve")
        .env("INTENTD_DATA_DIR", data_dir)
        .env("INTENTD_WORKSPACES_DIR", &workspaces_dir)
        .env("INTENTD_ASSERT_HERMETIC_ROOT", "1")
        .stdout(Stdio::null())
        .stderr(Stdio::from(log));
    eprintln!("[spawn_serve] setting env vars:");
    for (k, v) in env {
        eprintln!("  {k}={v}");
        cmd.env(k, v);
    }
    cmd.spawn().expect("spawn intentd serve")
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

async fn connect_ws(
    port: u16,
    cfg: Arc<ClientConfig>,
) -> WebSocketStream<tokio_rustls::client::TlsStream<TcpStream>> {
    let url = format!("wss://localhost:{port}/ws?token={TOKEN}");
    common::wss_connect_with_retry(port, cfg, &url).await
}

async fn wss_rpc<S>(ws: &mut WebSocketStream<S>, id: i64, method: &str, params: Value) -> Value
where
    S: tokio::io::AsyncRead + tokio::io::AsyncWrite + Unpin,
{
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
                if v["id"] == json!(id) {
                    assert!(v.get("error").is_none(), "rpc {method} errored: {v}");
                    return v["result"].clone();
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
            Some(Ok(_)) => {}
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
        title: "WSS-RETRY-E2E".to_string(),
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

/// RETRY-1a: session/new stall → retry with status hint → success.
/// Mock ignores session/new on the first attempt (timeout), then succeeds on
/// retry. Assert agent:stream:status retry hint is observed before the turn
/// completes with agent:stream:activity + agent:stream:end.
#[tokio::test]
async fn agent_spawn_retry_session_new_stall_over_wss() {
    let Some(script) = gate("WSS spawn retry session/new stall E2E") else {
        return;
    };
    let data_dir = temp_data_dir();
    let ws_id = seed_workspace_only(&data_dir).await;
    let attempt_file = data_dir.join("attempts.txt");
    let attempt_file_s = attempt_file.to_string_lossy().into_owned();
    let behavior = json!({
        "ignoreSessionNewAttempts": 1,
        "response": "retry succeeded",
    })
    .to_string();
    // Fast retry: 500ms timeouts + 100ms,200ms backoff for fast e2e
    let env: [(&str, &str); 8] = [
        ("INTENTD_AUTH_TOKEN", TOKEN),
        ("INTENTD_TCP_PORT", "0"),
        ("MOCK_AGENT_SCRIPT_PATH", &script),
        ("MOCK_AGENT_BEHAVIOR", &behavior),
        ("MOCK_AGENT_ATTEMPT_FILE", &attempt_file_s),
        ("INTENTD_SESSION_SETUP_TIMEOUT_MS", "500"),
        ("INTENTD_ACP_INITIALIZE_TIMEOUT_MS", "500"),
        ("INTENTD_SPAWN_RETRY_BACKOFF_MS", "100,200"),
    ];
    let child = spawn_serve(&data_dir, "both", &env);
    let _daemon = Daemon {
        child,
        data_dir: data_dir.clone(),
    };
    let socket = data_dir.join("intentd.sock");
    assert!(await_uds(&socket).await, "daemon did not start");
    let status = common::await_wss_status(&socket).await;
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
        json!({ "workspaceId": ws_id, "name": "WSS-RETRY", "model": "mock:default" }),
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
        json!({ "workspaceId": ws_id, "agentId": agent_id, "content": "trigger retry" }),
    )
    .await;
    assert_eq!(sent["success"], true, "sendMessage ok: {sent}");

    // Collect events until terminal: expect >=1 agent:stream:status retry hint
    // (warning level, attempt N/3), then >=1 chunk, then exactly one stream:end.
    let mut status_frames = Vec::new();
    let mut chunks = 0u32;
    let mut ends = 0u32;
    for _ in 0..100 {
        let frame = wss_event(&mut sub, 30).await;
        match frame["params"]["event"]["type"].as_str() {
            Some("agent:stream:status") => {
                status_frames.push(frame["params"]["event"].clone());
            }
            Some("agent:stream:activity") => chunks += 1,
            Some("agent:stream:end") => {
                ends += 1;
                break;
            }
            _ => {}
        }
    }
    assert!(
        !status_frames.is_empty(),
        "expected >=1 agent:stream:status retry hint over WSS"
    );
    for ev in &status_frames {
        let data = &ev["data"];
        assert_eq!(
            data["agentId"].as_str(),
            Some(agent_id.as_str()),
            "agent:stream:status.agentId must match: {ev}"
        );
        let msg = data["message"].as_str().unwrap_or("");
        if msg.contains("retry") || msg.contains("attempt") {
            assert_eq!(
                data["level"].as_str(),
                Some("warning"),
                "retry hints are warning-level: {ev}"
            );
        }
    }
    assert!(chunks >= 1, "at least one agent:stream:activity over WSS");
    assert_eq!(ends, 1, "exactly one terminal agent:stream:end over WSS");
}

/// RETRY-1b: stdout closed (immediate exit) → retry with status hint → success.
/// Mock exits immediately on launch for the first attempt (handshake failure),
/// then succeeds on retry. Assert agent:stream:status retry hint is observed
/// before the turn completes.
#[tokio::test]
async fn agent_spawn_retry_stdout_closed_over_wss() {
    let Some(script) = gate("WSS spawn retry stdout closed E2E") else {
        return;
    };
    let data_dir = temp_data_dir();
    let ws_id = seed_workspace_only(&data_dir).await;
    let attempt_file = data_dir.join("attempts.txt");
    let attempt_file_s = attempt_file.to_string_lossy().into_owned();
    let behavior = json!({
        "exitImmediatelyAttempts": 1,
        "response": "retry succeeded after exit",
    })
    .to_string();
    let env: [(&str, &str); 8] = [
        ("INTENTD_AUTH_TOKEN", TOKEN),
        ("INTENTD_TCP_PORT", "0"),
        ("MOCK_AGENT_SCRIPT_PATH", &script),
        ("MOCK_AGENT_BEHAVIOR", &behavior),
        ("MOCK_AGENT_ATTEMPT_FILE", &attempt_file_s),
        ("INTENTD_SESSION_SETUP_TIMEOUT_MS", "500"),
        ("INTENTD_ACP_INITIALIZE_TIMEOUT_MS", "500"),
        ("INTENTD_SPAWN_RETRY_BACKOFF_MS", "100,200"),
    ];
    let child = spawn_serve(&data_dir, "both", &env);
    let _daemon = Daemon {
        child,
        data_dir: data_dir.clone(),
    };
    let socket = data_dir.join("intentd.sock");
    assert!(await_uds(&socket).await, "daemon did not start");
    let status = common::await_wss_status(&socket).await;
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
        json!({ "workspaceId": ws_id, "name": "WSS-RETRY-EXIT", "model": "mock:default" }),
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
        json!({ "workspaceId": ws_id, "agentId": agent_id, "content": "trigger retry after exit" }),
    )
    .await;
    assert_eq!(sent["success"], true, "sendMessage ok: {sent}");

    let mut status_frames = Vec::new();
    let mut chunks = 0u32;
    let mut ends = 0u32;
    for _ in 0..100 {
        let frame = wss_event(&mut sub, 30).await;
        match frame["params"]["event"]["type"].as_str() {
            Some("agent:stream:status") => {
                status_frames.push(frame["params"]["event"].clone());
            }
            Some("agent:stream:activity") => chunks += 1,
            Some("agent:stream:end") => {
                ends += 1;
                break;
            }
            _ => {}
        }
    }
    assert!(
        !status_frames.is_empty(),
        "expected >=1 agent:stream:status retry hint over WSS"
    );
    for ev in &status_frames {
        let data = &ev["data"];
        let msg = data["message"].as_str().unwrap_or("");
        if msg.contains("retry") || msg.contains("attempt") {
            assert_eq!(
                data["level"].as_str(),
                Some("warning"),
                "retry hints are warning-level: {ev}"
            );
        }
    }
    assert!(chunks >= 1, "at least one agent:stream:activity over WSS");
    assert_eq!(ends, 1, "exactly one terminal agent:stream:end over WSS");
}

/// RETRY-1c: terminal failure (always fails session/new) → agent:failed +
/// agent:stream:end. Mock ignores session/new on all 3 attempts (exhaustion),
/// then the daemon emits terminal agent:failed + agent:stream:end events with
/// no streaming chunks.
#[tokio::test]
async fn agent_spawn_exhaustion_terminal_failure_over_wss() {
    let Some(script) = gate("WSS spawn exhaustion terminal failure E2E") else {
        return;
    };
    let data_dir = temp_data_dir();
    let ws_id = seed_workspace_only(&data_dir).await;
    let attempt_file = data_dir.join("attempts.txt");
    let attempt_file_s = attempt_file.to_string_lossy().into_owned();
    // Ignore session/new on all 3 attempts → terminal failure
    let behavior = json!({
        "ignoreSessionNewAttempts": 999,
    })
    .to_string();
    let env: [(&str, &str); 8] = [
        ("INTENTD_AUTH_TOKEN", TOKEN),
        ("INTENTD_TCP_PORT", "0"),
        ("MOCK_AGENT_SCRIPT_PATH", &script),
        ("MOCK_AGENT_BEHAVIOR", &behavior),
        ("MOCK_AGENT_ATTEMPT_FILE", &attempt_file_s),
        ("INTENTD_SESSION_SETUP_TIMEOUT_MS", "500"),
        ("INTENTD_ACP_INITIALIZE_TIMEOUT_MS", "500"),
        ("INTENTD_SPAWN_RETRY_BACKOFF_MS", "100,200"),
    ];
    let child = spawn_serve(&data_dir, "both", &env);
    let _daemon = Daemon {
        child,
        data_dir: data_dir.clone(),
    };
    let socket = data_dir.join("intentd.sock");
    assert!(await_uds(&socket).await, "daemon did not start");
    let status = common::await_wss_status(&socket).await;
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
        json!({ "workspaceId": ws_id, "name": "WSS-RETRY-FAIL", "model": "mock:default" }),
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
        json!({ "workspaceId": ws_id, "agentId": agent_id, "content": "trigger exhaustion" }),
    )
    .await;
    assert_eq!(sent["success"], true, "sendMessage ok: {sent}");

    // Collect events until terminal: expect >=1 agent:stream:status retry hints,
    // then agent:failed + agent:stream:end (no chunks since all spawns failed).
    let mut status_frames = Vec::new();
    let mut saw_failed = false;
    let mut saw_end = false;
    for _ in 0..100 {
        let frame = wss_event(&mut sub, 30).await;
        match frame["params"]["event"]["type"].as_str() {
            Some("agent:stream:status") => {
                status_frames.push(frame["params"]["event"].clone());
            }
            Some("agent:failed") => {
                assert_eq!(
                    frame["params"]["event"]["data"]["agentId"].as_str(),
                    Some(agent_id.as_str()),
                    "agent:failed carries the agent id"
                );
                saw_failed = true;
            }
            Some("agent:stream:end") => {
                assert_eq!(
                    frame["params"]["event"]["data"]["agentId"].as_str(),
                    Some(agent_id.as_str()),
                    "agent:stream:end carries the agent id"
                );
                saw_end = true;
                break;
            }
            Some("agent:stream:activity") => {
                panic!("unexpected chunk after exhaustion: {frame}");
            }
            _ => {}
        }
    }
    assert!(
        !status_frames.is_empty(),
        "expected >=1 agent:stream:status retry hints over WSS before exhaustion"
    );
    assert!(saw_failed, "terminal agent:failed emitted after exhaustion");
    assert!(
        saw_end,
        "terminal agent:stream:end emitted after exhaustion"
    );
}

/// RETRY-4: agent.retry recovery path — exhaust spawn retries → assert error
/// status persisted → reconfigure mock to succeed → call agent.retry over WSS
/// → assert queued message is redriven, turn completes, and status recovers.
/// Also assert agent.retry on a non-failed agent is rejected.
#[tokio::test]
async fn agent_retry_rpc_recovery_path_over_wss() {
    let Some(script) = gate("WSS agent.retry recovery E2E") else {
        return;
    };
    let data_dir = temp_data_dir();
    let ws_id = seed_workspace_only(&data_dir).await;
    let attempt_file = data_dir.join("attempts.txt");
    let attempt_file_s = attempt_file.to_string_lossy().into_owned();
    // Behavior that fails many spawn attempts (guarantees exhaustion),
    // but we'll reset the counter before agent.retry so it succeeds
    let behavior = json!({
        "exitImmediatelyAttempts": 999,
        "response": "retry recovery succeeded",
    })
    .to_string();
    let env: [(&str, &str); 8] = [
        ("INTENTD_AUTH_TOKEN", TOKEN),
        ("INTENTD_TCP_PORT", "0"),
        ("MOCK_AGENT_SCRIPT_PATH", &script),
        ("MOCK_AGENT_BEHAVIOR", &behavior),
        ("MOCK_AGENT_ATTEMPT_FILE", &attempt_file_s),
        ("INTENTD_SESSION_SETUP_TIMEOUT_MS", "500"),
        ("INTENTD_ACP_INITIALIZE_TIMEOUT_MS", "500"),
        ("INTENTD_SPAWN_RETRY_BACKOFF_MS", "100,200"),
    ];
    let child = spawn_serve(&data_dir, "both", &env);
    let _daemon = Daemon {
        child,
        data_dir: data_dir.clone(),
    };
    let socket = data_dir.join("intentd.sock");
    assert!(await_uds(&socket).await, "daemon did not start");
    let status = common::await_wss_status(&socket).await;
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

    // Create agent and send message — will exhaust retries
    let created = wss_rpc(
        &mut rpc,
        10,
        "agent.create",
        json!({ "workspaceId": ws_id, "name": "WSS-RETRY-RECOVER", "model": "mock:default" }),
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
        json!({ "workspaceId": ws_id, "agentId": agent_id, "content": "will fail" }),
    )
    .await;
    assert_eq!(sent["success"], true, "sendMessage ok: {sent}");

    // Wait for agent:failed terminal event, agent:stream:end, AND agent:status-changed
    // (the status-changed event is published AFTER the status is persisted, so
    // waiting for it ensures the status write is visible to subsequent reads)
    let mut saw_failed = false;
    let mut saw_end_from_exhaustion = false;
    let mut saw_status_error = false;
    // Use a higher bound since continue skips unrelated agent events
    for _ in 0..200 {
        let frame = wss_event(&mut sub, 30).await;
        // Filter by agent ID to avoid breaking on another agent's events
        let event_agent_id = frame["params"]["event"]["data"]["agentId"].as_str();
        if event_agent_id != Some(agent_id.as_str()) {
            continue;
        }
        match frame["params"]["event"]["type"].as_str() {
            Some("agent:failed") => {
                saw_failed = true;
            }
            Some("agent:stream:end") => {
                saw_end_from_exhaustion = true;
            }
            Some("agent:status-changed")
                if frame["params"]["event"]["data"]["status"] == "error" =>
            {
                saw_status_error = true;
            }
            _ => {}
        }
        // Only break after seeing all three terminal events to handle out-of-order delivery
        if saw_failed && saw_end_from_exhaustion && saw_status_error {
            break;
        }
    }
    assert!(saw_failed, "agent:failed emitted after exhaustion");
    assert!(
        saw_end_from_exhaustion,
        "agent:stream:end emitted after exhaustion"
    );
    assert!(
        saw_status_error,
        "agent:status-changed with status=error emitted after exhaustion"
    );

    // Assert persisted error status via agent.getSession
    let session = wss_rpc(
        &mut rpc,
        12,
        "agent.getSession",
        json!({ "workspaceId": ws_id, "agentId": agent_id }),
    )
    .await;
    assert_eq!(
        session["session"]["status"], "error",
        "session status is error after exhaustion"
    );
    // STAB-STOP-REASON: the persisted stopReason must match the exhaustion error.
    let stop_reason = session["session"]["stopReason"]
        .as_str()
        .expect("stopReason should be present after spawn exhaustion");
    assert!(
        stop_reason.contains("session/new failed")
            || stop_reason.contains("retries exhausted")
            || stop_reason.contains("handshake failed")
            || stop_reason.contains("agent stdout closed"),
        "stopReason persisted and describes the spawn exhaustion, got: {stop_reason}"
    );

    // Reset the attempt counter so the next spawn (from agent.retry) will succeed
    std::fs::write(&attempt_file, "1000").expect("reset attempt counter");

    // Now call agent.retry over WSS to redrive the failed spawn
    let retry_result = wss_rpc(
        &mut rpc,
        13,
        "agent.retry",
        json!({ "workspaceId": ws_id, "agentId": agent_id }),
    )
    .await;
    assert_eq!(
        retry_result["ok"], true,
        "agent.retry succeeded on error status"
    );

    // Give the retry spawn a moment to start and complete
    tokio::time::sleep(Duration::from_secs(2)).await;

    // Wait for the turn to complete: expect agent:stream:activity and agent:stream:end
    let mut saw_chunk = false;
    let mut saw_end = false;
    for _ in 0..100 {
        let frame = wss_event(&mut sub, 30).await;
        match frame["params"]["event"]["type"].as_str() {
            Some("agent:stream:activity") => {
                saw_chunk = true;
            }
            Some("agent:stream:end") => {
                saw_end = true;
                break;
            }
            Some("agent:failed") => {
                panic!(
                    "agent:failed emitted AGAIN after retry - retry did not fix the spawn issue!"
                );
            }
            _ => {}
        }
    }
    assert!(
        saw_chunk,
        "agent:stream:activity emitted after retry recovery"
    );
    assert!(saw_end, "agent:stream:end emitted after retry recovery");

    // Verify final status is no longer error
    let final_session = wss_rpc(
        &mut rpc,
        14,
        "agent.getSession",
        json!({ "workspaceId": ws_id, "agentId": agent_id }),
    )
    .await;
    assert_ne!(
        final_session["session"]["status"], "error",
        "session status recovered from error after agent.retry"
    );
    // STAB-STOP-REASON: the retry cleared the stopReason (successful turn leaves it null).
    assert!(
        final_session["session"]["stopReason"].is_null(),
        "stopReason cleared after successful retry: {:?}",
        final_session["session"]["stopReason"]
    );

    // Test rejection: retry again on the same agent (now active/idle) should return ok:false
    let retry_again = wss_rpc(
        &mut rpc,
        14,
        "agent.retry",
        json!({ "workspaceId": ws_id, "agentId": agent_id }),
    )
    .await;
    assert_eq!(
        retry_again["ok"], false,
        "agent.retry on non-error agent returns ok:false"
    );
}

/// Spawn-time pi CLI fail-fast over the wire (monorepo#1662): a daemon whose
/// `PI_ACP_PI_COMMAND` points at a fake too-old `pi` must fail a pi-provider
/// turn on the FIRST spawn attempt (`Error::InvalidInput` from
/// `check_pi_cli_for_spawn` is non-retryable), surfacing the gate's
/// user-facing reason — found version, requirement, pi-acp pin — in the
/// terminal `agent:failed` event and the persisted `stopReason`, with no
/// retry hints and no `agent:stream:activity` (nothing was spawned).
#[tokio::test]
async fn pi_spawn_fails_fast_on_old_cli_over_wss() {
    // The pi provider is npx-only: `resolve_spawn` resolves npx BEFORE the
    // pi CLI check, so a host without npx would fail on npx instead.
    if intent_providers::find_npx().is_none() {
        eprintln!("skipping WSS pi CLI fail-fast E2E: npx not found");
        return;
    }
    let data_dir = temp_data_dir();
    let ws_id = seed_workspace_only(&data_dir).await;

    // Fake `pi` that reports a version older than PI_CLI_MIN_VERSION.
    let fake_pi = data_dir.join("fake-pi");
    {
        use std::os::unix::fs::PermissionsExt;
        std::fs::write(&fake_pi, "#!/bin/sh\necho 0.79.0\n").expect("write fake pi");
        std::fs::set_permissions(&fake_pi, std::fs::Permissions::from_mode(0o755))
            .expect("chmod fake pi");
    }
    let fake_pi_str = fake_pi.to_string_lossy().into_owned();

    let env: [(&str, &str); 4] = [
        ("INTENTD_AUTH_TOKEN", TOKEN),
        ("INTENTD_TCP_PORT", "0"),
        ("PI_ACP_PI_COMMAND", &fake_pi_str),
        ("INTENTD_SPAWN_RETRY_BACKOFF_MS", "100,200"),
    ];
    let child = spawn_serve(&data_dir, "both", &env);
    let _daemon = Daemon {
        child,
        data_dir: data_dir.clone(),
    };
    let socket = data_dir.join("intentd.sock");
    assert!(await_uds(&socket).await, "daemon did not start");
    let status = common::await_wss_status(&socket).await;
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
        json!({ "workspaceId": ws_id, "name": "WSS-PI-GATE", "provider": "pi" }),
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
        json!({ "workspaceId": ws_id, "agentId": agent_id, "content": "trigger pi gate" }),
    )
    .await;
    assert_eq!(sent["success"], true, "sendMessage ok: {sent}");

    // Terminal failure on the first attempt: agent:failed carries the gate
    // reason; no retry hints, no chunks (the child was never spawned).
    let mut failed_error: Option<String> = None;
    let mut saw_end = false;
    for _ in 0..100 {
        let frame = wss_event(&mut sub, 30).await;
        let event = &frame["params"]["event"];
        if event["data"]["agentId"].as_str() != Some(agent_id.as_str()) {
            continue;
        }
        match event["type"].as_str() {
            Some("agent:failed") => {
                failed_error = Some(
                    event["data"]["error"]
                        .as_str()
                        .expect("agent:failed carries the error text")
                        .to_string(),
                );
            }
            Some("agent:stream:status") => {
                let msg = event["data"]["message"].as_str().unwrap_or("");
                assert!(
                    !msg.contains("retrying"),
                    "pi CLI gate must be non-retryable (fail-fast), saw retry hint: {msg}"
                );
            }
            Some("agent:stream:activity") => {
                panic!("no chunks expected — the pi child must never spawn: {frame}");
            }
            Some("agent:stream:end") => {
                saw_end = true;
                break;
            }
            _ => {}
        }
    }
    let error = failed_error.expect("terminal agent:failed observed over WSS");
    assert!(error.contains("cannot start Pi agent"), "{error}");
    assert!(
        error.contains("0.79.0"),
        "gate names the found version: {error}"
    );
    assert!(
        error.contains(intent_providers::PI_CLI_REQUIREMENT),
        "gate names the requirement: {error}"
    );
    assert!(
        error.contains(intent_providers::PI_ACP_NPX_PACKAGE),
        "gate names the pi-acp pin: {error}"
    );
    assert!(
        saw_end,
        "terminal agent:stream:end emitted after the fail-fast"
    );

    // The persisted session carries the same gate reason as stopReason.
    // Deliberately a read-right-after-the-event: the daemon persists the
    // Error status BEFORE publishing the terminal pair (monorepo#2009,
    // durable-before-observable), so this read must never race the write.
    let session = wss_rpc(
        &mut rpc,
        12,
        "agent.getSession",
        json!({ "workspaceId": ws_id, "agentId": agent_id }),
    )
    .await;
    assert_eq!(
        session["session"]["status"], "error",
        "session status is error after the pi gate fail-fast (persisted before agent:stream:end, monorepo#2009)"
    );
    let stop_reason = session["session"]["stopReason"]
        .as_str()
        .expect("stopReason persisted after the pi gate fail-fast");
    assert!(
        stop_reason.contains("cannot start Pi agent"),
        "{stop_reason}"
    );
}

/// INIT-TIMEOUT (monorepo#616): a slow-to-initialize agent succeeds on the
/// FIRST attempt under the default 30s `initialize` timeout. The mock delays
/// its `initialize` reply by 6s — longer than the old hard-coded 5s
/// per-request timeout that made cold starts fail spuriously under host load —
/// and the turn must complete without any spawn retry.
#[tokio::test]
async fn agent_spawn_slow_initialize_succeeds_over_wss() {
    let Some(script) = gate("WSS slow initialize E2E") else {
        return;
    };
    let data_dir = temp_data_dir();
    let ws_id = seed_workspace_only(&data_dir).await;
    let behavior = json!({
        "initializeDelayMs": 6000,
        "response": "slow initialize succeeded",
    })
    .to_string();
    // No INTENTD_ACP_INITIALIZE_TIMEOUT_MS: exercise the 30s default, which
    // must tolerate the 6s initialize delay without a handshake timeout.
    let env: [(&str, &str); 5] = [
        ("INTENTD_AUTH_TOKEN", TOKEN),
        ("INTENTD_TCP_PORT", "0"),
        ("MOCK_AGENT_SCRIPT_PATH", &script),
        ("MOCK_AGENT_BEHAVIOR", &behavior),
        ("INTENTD_SPAWN_RETRY_BACKOFF_MS", "100,200"),
    ];
    let child = spawn_serve(&data_dir, "both", &env);
    let _daemon = Daemon {
        child,
        data_dir: data_dir.clone(),
    };
    let socket = data_dir.join("intentd.sock");
    assert!(await_uds(&socket).await, "daemon did not start");
    let status = common::await_wss_status(&socket).await;
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
        json!({ "workspaceId": ws_id, "name": "WSS-SLOW-INIT", "model": "mock:default" }),
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
        json!({ "workspaceId": ws_id, "agentId": agent_id, "content": "trigger slow initialize" }),
    )
    .await;
    assert_eq!(sent["success"], true, "sendMessage ok: {sent}");

    // The turn completes (chunk + end) with NO retry hints and NO failure —
    // the single spawn attempt rode out the 6s initialize delay.
    let mut retry_hints = Vec::new();
    let mut chunks = 0u32;
    let mut ends = 0u32;
    for _ in 0..100 {
        let frame = wss_event(&mut sub, 30).await;
        match frame["params"]["event"]["type"].as_str() {
            Some("agent:stream:status") => {
                let msg = frame["params"]["event"]["data"]["message"]
                    .as_str()
                    .unwrap_or("")
                    .to_string();
                if msg.contains("retry") || msg.contains("attempt") {
                    retry_hints.push(msg);
                }
            }
            Some("agent:failed") => {
                panic!("agent:failed on slow initialize — handshake timeout regressed: {frame}");
            }
            Some("agent:stream:activity") => chunks += 1,
            Some("agent:stream:end") => {
                ends += 1;
                break;
            }
            _ => {}
        }
    }
    assert!(
        retry_hints.is_empty(),
        "slow initialize must succeed on the first attempt, saw retry hints: {retry_hints:?}"
    );
    assert!(chunks >= 1, "at least one agent:stream:activity over WSS");
    assert_eq!(ends, 1, "exactly one terminal agent:stream:end over WSS");
}
