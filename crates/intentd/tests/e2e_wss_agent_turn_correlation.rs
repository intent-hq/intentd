//! WSS end-to-end turn correlation id (monorepo#1022): a terminal mid-turn
//! failure's `agent:failed` carries the send's `turnId`; the requeued entry
//! in `agent:queue:updated` gets a NEW `id` but keeps the SAME `turnId`; and
//! the `agent.retry` redrive — its RPC response and the drain-start
//! `agent:queue:processing` signal — carries that same original `turnId`.
//!
//! This is the regression lock for `turn_id` preservation in
//! `persist_error_and_requeue`: if the requeue stops threading the failed
//! turn's original `turn_id` onto the fresh queue entry, the SAME-`turnId`
//! assertions below fail.
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
        let log_path = self.data_dir.join("daemon.log");
        if let Ok(log) = std::fs::read_to_string(&log_path) {
            eprintln!("=== DAEMON LOG ===\n{log}\n=== END LOG ===");
        }
        let _ = std::fs::remove_dir_all(&self.data_dir);
    }
}

fn temp_data_dir() -> PathBuf {
    let id = Uuid::new_v4().simple().to_string();
    let dir = PathBuf::from("/tmp").join(format!("itd-wss-turnid-{}", &id[..8]));
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
        title: "WSS-TURNID-E2E".to_string(),
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
        execution_environment: None,
        disk_usage: None,
        pending_delete_at: None,
    }
}

/// TURNID-1 (monorepo#1022): the turn correlation id survives a terminal
/// mid-turn failure and its `agent.retry` redrive.
///
/// The mock spawns and handshakes normally, then `process.exit(1)`s inside
/// `session/prompt` on the first TWO attempts: the first pre-output death is
/// consumed by the one-shot silent redrive (monorepo#764), the second is a
/// terminal mid-turn failure that parks the agent in Error and requeues the
/// message. The chain under test:
///
/// 1. `agent.sendMessage` (direct arm) responds with the minted `turnId`.
/// 2. `agent:failed` + terminal `agent:stream:end` carry that `turnId`.
/// 3. The requeued entry in `agent:queue:updated` (and `agent.getQueue`) has
///    a NEW `id` but the SAME `turnId` — the `persist_error_and_requeue`
///    preservation this test locks down.
/// 4. `agent.retry` responds with the same `turnId`, and the redrive's
///    drain-start `agent:queue:processing` names the requeued entry's new
///    `id` as `messageId` with the original `turnId`.
#[tokio::test]
async fn agent_retry_redrive_preserves_original_turn_id_over_wss() {
    let Some(script) = gate("WSS turn-correlation E2E") else {
        return;
    };
    let data_dir = temp_data_dir();
    let ws_id = seed_workspace_only(&data_dir).await;
    let attempt_file = data_dir.join("attempts.txt");
    let attempt_file_s = attempt_file.to_string_lossy().into_owned();
    let behavior = json!({
        "exitDuringPromptAttempts": 2,
        "response": "turn-correlation-recovered-1022",
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
    let status = common::await_wss_status(&socket).await;
    let port =
        u16::try_from(status["result"]["port"].as_u64().expect("port")).expect("value fits in u16");
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
        json!({ "eventTypes": ["agent:*", "chat:stream:delta"], "workspaceId": ws_id }),
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
        json!({ "workspaceId": ws_id, "name": "WSS-TURNID", "model": "default", "provider": "mock" }),
    )
    .await;
    let agent_id = created["agent"]["id"]
        .as_str()
        .expect("agent id")
        .to_string();

    // (1) The direct-send RPC response mints and names the turn's
    // correlation id — the anchor every later assertion compares against.
    let sent = wss_rpc(
        &mut rpc,
        11,
        "agent.sendMessage",
        json!({ "workspaceId": ws_id, "agentId": agent_id, "content": "dies mid-turn" }),
    )
    .await;
    assert_eq!(sent["success"], true, "sendMessage ok: {sent}");
    assert_eq!(sent["queued"], false, "direct send (not queued): {sent}");
    let turn_id = sent["turnId"]
        .as_str()
        .expect("direct sendMessage response carries turnId (monorepo#1022)")
        .to_string();
    assert!(!turn_id.is_empty(), "turnId is non-empty");

    // (2) + (3) Terminal failure surface: `agent:failed` and the terminal
    // `agent:stream:end` carry the send's turnId, and the requeued entry in
    // `agent:queue:updated` has a NEW id with the SAME turnId. The
    // status-changed(error) event is emitted after the status persist, so
    // seeing it guarantees the Error park landed.
    let mut saw_failed = false;
    let mut saw_terminal_end = false;
    let mut saw_status_error = false;
    let mut requeued_entry: Option<Value> = None;
    for _ in 0..300 {
        let frame = wss_event(&mut sub, 30).await;
        let event = &frame["params"]["event"];
        if event["data"]["agentId"].as_str() != Some(agent_id.as_str()) {
            continue;
        }
        match event["type"].as_str() {
            Some("agent:failed") => {
                assert_eq!(
                    event["data"]["turnId"].as_str(),
                    Some(turn_id.as_str()),
                    "agent:failed carries the send's turnId: {frame}"
                );
                saw_failed = true;
            }
            Some("agent:stream:end") => {
                assert_eq!(
                    event["data"]["turnId"].as_str(),
                    Some(turn_id.as_str()),
                    "terminal agent:stream:end carries the send's turnId: {frame}"
                );
                saw_terminal_end = true;
            }
            Some("agent:message") if event["data"]["role"] == "user" => {
                // The user-row echo of the direct send carries the same id.
                // (System rows — e.g. the turn-failure transcript notice —
                // carry no turnId and are skipped by the role guard.)
                assert_eq!(
                    event["data"]["turnId"].as_str(),
                    Some(turn_id.as_str()),
                    "user-row agent:message echo carries the send's turnId: {frame}"
                );
            }
            Some("agent:queue:updated") => {
                let queue = event["data"]["queue"].as_array().expect("queue array");
                if !queue.is_empty() {
                    requeued_entry = Some(queue[0].clone());
                }
            }
            Some("agent:status-changed") if event["data"]["status"] == "error" => {
                saw_status_error = true;
            }
            _ => {}
        }
        if saw_failed && saw_terminal_end && saw_status_error && requeued_entry.is_some() {
            break;
        }
    }
    assert!(saw_failed, "terminal agent:failed emitted");
    assert!(saw_terminal_end, "terminal agent:stream:end emitted");
    assert!(saw_status_error, "agent parked in error");
    let requeued = requeued_entry.expect("agent:queue:updated carried the requeued entry");
    assert_eq!(
        requeued["turnId"].as_str(),
        Some(turn_id.as_str()),
        "requeued entry keeps the failed turn's ORIGINAL turnId (persist_error_and_requeue preservation): {requeued}"
    );
    let requeued_id = requeued["id"].as_str().expect("requeued entry id");
    assert_ne!(
        requeued_id, turn_id,
        "requeued entry got a NEW id distinct from the preserved turnId: {requeued}"
    );
    assert_eq!(
        requeued["requeuedAfterFailure"], true,
        "requeued entry flagged requeuedAfterFailure: {requeued}"
    );
    let requeued_id = requeued_id.to_string();

    // The same NEW-id / SAME-turnId contract over the RPC read surface.
    let queue = wss_rpc(
        &mut rpc,
        12,
        "agent.getQueue",
        json!({ "workspaceId": ws_id, "agentId": agent_id }),
    )
    .await;
    let messages = queue["queue"].as_array().expect("queue array");
    assert_eq!(messages.len(), 1, "failed message requeued: {queue}");
    assert_eq!(
        messages[0]["id"].as_str(),
        Some(requeued_id.as_str()),
        "agent.getQueue names the same requeued entry: {queue}"
    );
    assert_eq!(
        messages[0]["turnId"].as_str(),
        Some(turn_id.as_str()),
        "agent.getQueue entry keeps the original turnId: {queue}"
    );

    // (4) agent.retry names the redriven turn in its response…
    let retry_result = wss_rpc(
        &mut rpc,
        13,
        "agent.retry",
        json!({ "workspaceId": ws_id, "agentId": agent_id }),
    )
    .await;
    assert_eq!(retry_result["ok"], true, "agent.retry ok on error status");
    assert_eq!(
        retry_result["redriven"], true,
        "agent.retry redrives the requeued message: {retry_result}"
    );
    assert_eq!(
        retry_result["turnId"].as_str(),
        Some(turn_id.as_str()),
        "agent.retry response carries the ORIGINAL turnId: {retry_result}"
    );

    // …and the redrive's drain-start `agent:queue:processing` signal pairs
    // the requeued entry's NEW messageId with the ORIGINAL turnId, then the
    // turn completes (the mock's attempt counter is past the failure window).
    let mut saw_processing = false;
    let mut saw_chunk = false;
    let mut saw_retry_end = false;
    for _ in 0..300 {
        let frame = wss_event(&mut sub, 30).await;
        let event = &frame["params"]["event"];
        if event["data"]["agentId"].as_str() != Some(agent_id.as_str()) {
            continue;
        }
        match event["type"].as_str() {
            Some("agent:queue:processing") => {
                assert_eq!(
                    event["data"]["messageId"].as_str(),
                    Some(requeued_id.as_str()),
                    "agent:queue:processing names the requeued entry's NEW id: {frame}"
                );
                assert_eq!(
                    event["data"]["turnId"].as_str(),
                    Some(turn_id.as_str()),
                    "agent:queue:processing carries the ORIGINAL turnId: {frame}"
                );
                saw_processing = true;
            }
            Some("chat:stream:delta") => {
                if event["data"]["content"]
                    .as_str()
                    .unwrap_or_default()
                    .contains("turn-correlation-recovered-1022")
                {
                    saw_chunk = true;
                }
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
        saw_processing,
        "agent:queue:processing emitted for the retry redrive"
    );
    assert!(saw_chunk, "redriven turn streamed the mock response");
    assert!(
        saw_retry_end,
        "agent:stream:end emitted after retry redrive"
    );
}
