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
use tokio::io::{AsyncBufReadExt, AsyncWriteExt, BufReader};
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
        let log_path = self.data_dir.join("daemon.log");
        if let Ok(log) = std::fs::read_to_string(&log_path) {
            eprintln!("=== DAEMON LOG ===\n{}\n=== END LOG ===", log);
        }
        let _ = std::fs::remove_dir_all(&self.data_dir);
    }
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
    for (k, v) in env {
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
    let env: [(&str, &str); 7] = [
        ("INTENTD_AUTH_TOKEN", TOKEN),
        ("INTENTD_TCP_PORT", "0"),
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
    // STAB-STOP-REASON: the persisted stopReason must match the agent:failed event's error text.
    let stop_reason = session["session"]["stopReason"]
        .as_str()
        .expect("stopReason should be present after mid-turn failure");
    assert!(
        stop_reason.contains("agent stdout closed"),
        "stopReason persisted and matches the mid-turn prompt error, got: {stop_reason}"
    );

    // STAB-53: the mid-turn crash left the child's stderr captured under
    // `<data_dir>/agent-logs/<agent-id>/<YYYY-MM-DD>.log` — the mock logs
    // every phase to stderr, including the deliberate exit inside
    // session/prompt. Scan every daily file in the agent's capture dir
    // rather than hard-coding today's name: the writer rotates by UTC date,
    // so a midnight rollover between emit and read must not flake the test.
    let capture_dir = intent_core::agent_logs_root(&data_dir).join(&agent_id);
    let mut captured = String::new();
    for _ in 0..100 {
        captured.clear();
        if let Ok(mut entries) = tokio::fs::read_dir(&capture_dir).await {
            while let Ok(Some(entry)) = entries.next_entry().await {
                if let Ok(c) = tokio::fs::read_to_string(entry.path()).await {
                    captured.push_str(&c);
                }
            }
        }
        if captured.contains("exiting during prompt") {
            break;
        }
        tokio::time::sleep(Duration::from_millis(25)).await;
    }
    assert!(
        captured.contains("exiting during prompt"),
        "stderr capture under {} holds the child's last words; got: {captured:?}",
        capture_dir.display()
    );

    // STAB-53: the terminal-failure WARN points at the per-agent capture
    // directory (rollover-stable: the daily file name would be misleading
    // across a UTC midnight boundary) so the crash is diagnosable straight
    // from the daemon log.
    let daemon_log_path = data_dir.join("daemon.log");
    let expected_hint = format!("agent stderr captured at {}", capture_dir.display());
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
    assert_eq!(
        retry_result["redriven"], true,
        "agent.retry reports the requeued message is being redriven"
    );

    // Wait for the clearing agent:status-changed event that must carry stopReason: null
    // so the FE can clear its canonical session state (cloudlands-fe#147).
    let mut saw_clearing_status_changed = false;
    let mut saw_chunk = false;
    let mut saw_retry_end = false;
    for _ in 0..200 {
        let frame = wss_event(&mut sub, 30).await;
        let event_agent_id = frame["params"]["event"]["data"]["agentId"].as_str();
        if event_agent_id != Some(agent_id.as_str()) {
            continue;
        }
        match frame["params"]["event"]["type"].as_str() {
            Some("agent:status-changed") => {
                // The retry clears stop_reason, so the event must carry stopReason: null.
                let data = &frame["params"]["event"]["data"];
                if data.get("stopReason").is_some() {
                    assert!(
                        data["stopReason"].is_null(),
                        "agent:status-changed from retry must carry stopReason: null, got: {:?}",
                        data["stopReason"]
                    );
                    saw_clearing_status_changed = true;
                }
            }
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
    assert!(
        saw_clearing_status_changed,
        "agent:status-changed with stopReason: null emitted during retry"
    );
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
    // STAB-STOP-REASON: the retry cleared the stopReason (successful turn leaves it null).
    assert!(
        final_session["session"]["stopReason"].is_null(),
        "stopReason cleared after successful retry: {:?}",
        final_session["session"]["stopReason"]
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

/// MIDTURN-2 (STAB-54): `agent.retry` on an errored agent whose queue is
/// EMPTY must not be an invisible no-op. The response carries
/// `redriven: false` and the status clears to `idle` (not `pending` — nothing
/// queued will ever drive a pending agent forward), with the matching
/// `agent:status-changed` event on the wire.
#[tokio::test]
async fn agent_retry_with_empty_queue_clears_to_idle_over_wss() {
    let Some(script) = gate("WSS empty-queue retry E2E") else {
        return;
    };
    let data_dir = temp_data_dir();
    let ws_id = seed_workspace_only(&data_dir).await;
    let attempt_file = data_dir.join("attempts.txt");
    let attempt_file_s = attempt_file.to_string_lossy().into_owned();
    let behavior = json!({
        "exitDuringPromptAttempts": 1,
        "response": "unused",
    })
    .to_string();
    let env: [(&str, &str); 7] = [
        ("INTENTD_AUTH_TOKEN", TOKEN),
        ("INTENTD_TCP_PORT", "0"),
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
        json!({ "workspaceId": ws_id, "name": "WSS-RETRY-EMPTY", "model": "mock:default" }),
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

    // Wait for the persisted error status (the status-changed event is
    // emitted AFTER the persist, so seeing it guarantees the write landed).
    let mut saw_status_error = false;
    for _ in 0..200 {
        let frame = wss_event(&mut sub, 30).await;
        let event = &frame["params"]["event"];
        if event["data"]["agentId"].as_str() == Some(agent_id.as_str())
            && event["type"] == "agent:status-changed"
            && event["data"]["status"] == "error"
        {
            saw_status_error = true;
            break;
        }
    }
    assert!(
        saw_status_error,
        "agent parked in error after mid-turn death"
    );

    // Empty the queue: remove the requeued failed message so the retry hits
    // the STAB-54 empty-queue path.
    let queue = wss_rpc(
        &mut rpc,
        12,
        "agent.getQueue",
        json!({ "workspaceId": ws_id, "agentId": agent_id }),
    )
    .await;
    let messages = queue["queue"].as_array().expect("queue array");
    assert_eq!(messages.len(), 1, "failed message requeued: {queue}");
    let message_id = messages[0]["id"].as_str().expect("message id");
    let removed = wss_rpc(
        &mut rpc,
        13,
        "agent.removeQueuedMessage",
        json!({ "workspaceId": ws_id, "agentId": agent_id, "messageId": message_id }),
    )
    .await;
    assert_eq!(
        removed["success"], true,
        "queued message removed: {removed}"
    );

    // Retry with an empty queue: explicit `redriven: false` in the response.
    let retry_result = wss_rpc(
        &mut rpc,
        14,
        "agent.retry",
        json!({ "workspaceId": ws_id, "agentId": agent_id }),
    )
    .await;
    assert_eq!(retry_result["ok"], true, "agent.retry ok on error status");
    assert_eq!(
        retry_result["redriven"], false,
        "empty-queue retry reports nothing was redriven: {retry_result}"
    );

    // The status clears to idle — pending would park the agent forever — and
    // the transition is announced on the event stream.
    let mut saw_status_idle = false;
    for _ in 0..200 {
        let frame = wss_event(&mut sub, 30).await;
        let event = &frame["params"]["event"];
        if event["data"]["agentId"].as_str() == Some(agent_id.as_str())
            && event["type"] == "agent:status-changed"
            && event["data"]["status"] == "idle"
        {
            saw_status_idle = true;
            break;
        }
    }
    assert!(
        saw_status_idle,
        "agent:status-changed(status=idle) emitted for empty-queue retry"
    );

    // Persisted status is visible over the RPC surface.
    let session = wss_rpc(
        &mut rpc,
        15,
        "agent.getSession",
        json!({ "workspaceId": ws_id, "agentId": agent_id }),
    )
    .await;
    assert_eq!(
        session["session"]["status"], "idle",
        "session status cleared to idle after empty-queue retry"
    );
}
