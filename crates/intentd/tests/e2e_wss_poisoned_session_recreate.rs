//! WSS end-to-end regression for the monorepo#940 poisoned-session recreate:
//! a provider session resumed via `session/load` deterministically rejects
//! every `session/prompt` with the chat-stream 400 `invalidArgument` payload.
//! The daemon must classify the failure as session-fatal — parking the agent
//! in `error` with `sessionCorrupted: true` on both the terminal
//! `agent:status-changed` event and the `agent.getSession` projection — and
//! `agent.retry` must arm force-recreate so the redrive opens a fresh
//! `session/new` (which succeeds) instead of resuming the corrupted session
//! via `session/load` (which would replay the rejection forever).
//!
//! The mock advertises `loadSession`, records every session establishment
//! (one `{ method, sessionId, pid }` JSON line per `session/new` /
//! `session/load`) to `MOCK_AGENT_SESSION_LOG`, and fails prompts ONLY on
//! load-established sessions. Sequence proven on the wire:
//! turn 1 `session/new` + prompt OK → idle child `SIGKILLed` out-of-band →
//! turn 2 respawn resumes via `session/load`, prompt rejected (poisoned) →
//! `agent.retry` → fresh `session/new`, prompt OK. Session log must read
//! exactly `["new", "load", "new"]` — a second "load" is the #940 regression.
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

/// The chat-stream 400 `invalidArgument` detail (verbatim shape from the
/// monorepo#940 diagnosis) that the mock stuffs into the -32603 `data`. The
/// daemon renders it into the `session/prompt failed: JSON-RPC error -32603:
/// Internal error: …` wrapper that `is_deterministic_prompt_rejection`
/// classifies as session-fatal.
const BACKEND_400_DATA: &str = r#"HTTP error: 400 Bad Request: {"httpStatus":400,"apiStatus":"invalidArgument","message":"HTTP error: 400 Bad Request","requestId":"dab7bd0f-9663-4bfc-a341-0a1b2c3d4e5f","httpUrl":"https://e2.api.augmentcode.com/chat-stream"}"#;

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
    let dir = PathBuf::from("/tmp").join(format!("itd-wss-poisoned-{}", &id[..8]));
    std::fs::create_dir_all(&dir).expect("mkdir data dir");
    dir
}

fn spawn_serve(data_dir: &Path, env: &[(&str, &str)]) -> Child {
    let log = std::fs::File::create(data_dir.join("daemon.log")).expect("create daemon log");
    let workspaces_dir = data_dir.join("workspaces");
    std::fs::create_dir_all(&workspaces_dir).expect("mkdir hermetic workspaces dir");
    common::enable_ws_api(data_dir);
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
        title: "WSS-POISONED-E2E".to_string(),
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

/// Bounded poll: wait until the mock's `MOCK_AGENT_PID_FILE` holds at least
/// `n` pid lines (one appended per spawn) and return them all.
async fn await_pid_lines(path: &Path, n: usize) -> Vec<u32> {
    for _ in 0..400 {
        if let Ok(contents) = tokio::fs::read_to_string(path).await {
            let pids: Vec<u32> = contents
                .lines()
                .filter_map(|l| l.trim().parse().ok())
                .collect();
            if pids.len() >= n {
                return pids;
            }
        }
        tokio::time::sleep(Duration::from_millis(25)).await;
    }
    panic!("pid file {} never reached {n} line(s)", path.display());
}

/// Bounded poll: wait until the daemon log contains `needle`.
async fn await_daemon_log_contains(data_dir: &Path, needle: &str) {
    let log_path = data_dir.join("daemon.log");
    for _ in 0..400 {
        if tokio::fs::read_to_string(&log_path)
            .await
            .unwrap_or_default()
            .contains(needle)
        {
            return;
        }
        tokio::time::sleep(Duration::from_millis(25)).await;
    }
    panic!("daemon log never contained {needle:?}");
}

/// Read the mock's session-establishment log — one `{ method, sessionId, pid }`
/// JSON line per `session/new` / `session/load` — collapsed to `"new"` /
/// `"load"` markers for sequence assertions.
fn session_log_lines(path: &Path) -> Vec<String> {
    std::fs::read_to_string(path)
        .unwrap_or_default()
        .lines()
        .filter(|l| !l.trim().is_empty())
        .map(|l| {
            let v: Value = serde_json::from_str(l).expect("session log line json");
            match v["method"].as_str().expect("method") {
                "session/new" => "new".to_string(),
                "session/load" => "load".to_string(),
                other => panic!("unexpected session log method: {other}"),
            }
        })
        .collect()
}

/// Bounded poll: wait until `agent.getSession` reports `status == "idle"`,
/// returning the final response for follow-up assertions.
async fn await_session_idle<S>(
    ws: &mut WebSocketStream<S>,
    id_base: i64,
    ws_id: &str,
    agent_id: &str,
) -> Value
where
    S: tokio::io::AsyncRead + tokio::io::AsyncWrite + Unpin,
{
    let mut last = Value::Null;
    for i in 0..100 {
        last = wss_rpc(
            ws,
            id_base + i,
            "agent.getSession",
            json!({ "workspaceId": ws_id, "agentId": agent_id }),
        )
        .await;
        if last["session"]["status"] == "idle" {
            return last;
        }
        tokio::time::sleep(Duration::from_millis(100)).await;
    }
    panic!("agent session never settled to idle; last: {last}");
}

#[allow(clippy::similar_names)] // deliberate parallel naming across the scenario's instances
/// POISONED-RECREATE (monorepo#940): a `session/load`-resumed session that
/// deterministically rejects prompts with the chat-stream 400 `invalidArgument`
/// payload parks the agent in `error` with `sessionCorrupted: true`, and
/// `agent.retry` recreates the provider session (fresh `session/new`) instead
/// of resuming — the redrive succeeds and the session log shows NO second load.
#[tokio::test]
async fn poisoned_session_retry_recreates_instead_of_resuming_over_wss() {
    let Some(script) = gate("WSS poisoned-session recreate E2E") else {
        return;
    };
    let data_dir = temp_data_dir();
    let ws_id = seed_workspace_only(&data_dir).await;
    let pid_file = data_dir.join("pids.txt");
    let pid_file_s = pid_file.to_string_lossy().into_owned();
    let session_log = data_dir.join("sessions.txt");
    let session_log_s = session_log.to_string_lossy().into_owned();
    // The mock advertises loadSession, accepts session/load, and fails EVERY
    // prompt on a load-established session with the #940 rejection; prompts
    // on a fresh session/new succeed.
    let behavior = json!({
        "advertiseLoadSession": true,
        "failPromptIfLoadedRpcError": {
            "code": -32603,
            "message": "Internal error",
            "data": BACKEND_400_DATA,
        },
        "response": "recovered on a fresh session",
        "rules": [
            { "ifPromptContains": "poisoned-turn-two", "response": "recreated-turn-response-940" },
        ],
    })
    .to_string();
    let env: [(&str, &str); 7] = [
        ("INTENTD_AUTH_TOKEN", TOKEN),
        ("INTENTD_TCP_PORT", "0"),
        ("MOCK_AGENT_SCRIPT_PATH", &script),
        ("MOCK_AGENT_BEHAVIOR", &behavior),
        ("MOCK_AGENT_PID_FILE", &pid_file_s),
        ("MOCK_AGENT_SESSION_LOG", &session_log_s),
        ("INTENTD_SESSION_SETUP_TIMEOUT_MS", "2000"),
    ];
    let child = spawn_serve(&data_dir, &env);
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
        json!({ "workspaceId": ws_id, "name": "WSS-POISONED", "model": "mock:default" }),
    )
    .await;
    let agent_id = created["agent"]["id"]
        .as_str()
        .expect("agent id")
        .to_string();

    // Turn 1 completes on a fresh session/new.
    let sent = wss_rpc(
        &mut rpc,
        11,
        "agent.sendMessage",
        json!({ "workspaceId": ws_id, "agentId": agent_id, "content": "first turn" }),
    )
    .await;
    assert_eq!(sent["success"], true, "first sendMessage ok: {sent}");
    let mut saw_end = false;
    for _ in 0..200 {
        let frame = wss_event(&mut sub, 30).await;
        let event = &frame["params"]["event"];
        if event["data"]["agentId"].as_str() != Some(agent_id.as_str()) {
            continue;
        }
        match event["type"].as_str() {
            Some("agent:failed") => panic!("agent:failed during the healthy first turn: {frame}"),
            Some("agent:stream:end") => {
                saw_end = true;
                break;
            }
            _ => {}
        }
    }
    assert!(saw_end, "first turn ended with agent:stream:end");
    await_session_idle(&mut rpc, 100, &ws_id, &agent_id).await;
    assert_eq!(
        session_log_lines(&session_log),
        vec!["new"],
        "turn 1 opened exactly one fresh session"
    );

    // SIGKILL the idle child out-of-band so the next message respawns a fresh
    // child that resumes the persisted acpSessionId via session/load.
    let pids = await_pid_lines(&pid_file, 1).await;
    let pid1 = pids[0];
    let killed = Command::new("kill")
        .args(["-9", &pid1.to_string()])
        .status()
        .expect("run kill")
        .success();
    assert!(killed, "SIGKILL delivered to idle mock child {pid1}");
    await_daemon_log_contains(
        &data_dir,
        "idle agent child exited unexpectedly; handle reaped",
    )
    .await;

    // Turn 2: the respawned child resumes via session/load and the prompt is
    // deterministically rejected with the #940 payload → terminal failure
    // with the session classified as corrupted.
    let sent = wss_rpc(
        &mut rpc,
        12,
        "agent.sendMessage",
        json!({ "workspaceId": ws_id, "agentId": agent_id, "content": "poisoned-turn-two" }),
    )
    .await;
    assert_eq!(sent["success"], true, "post-kill sendMessage ok: {sent}");

    let mut saw_failed = false;
    let mut saw_status_error_corrupted = false;
    for _ in 0..200 {
        let frame = wss_event(&mut sub, 30).await;
        let event = &frame["params"]["event"];
        if event["data"]["agentId"].as_str() != Some(agent_id.as_str()) {
            continue;
        }
        match event["type"].as_str() {
            Some("agent:failed") => {
                let err = event["data"]["error"].as_str().unwrap_or("");
                assert!(
                    err.contains("400 Bad Request")
                        && err.contains("\"apiStatus\":\"invalidArgument\""),
                    "agent:failed carries the 400 invalidArgument rejection, got: {err}"
                );
                saw_failed = true;
            }
            Some("agent:status-changed") if event["data"]["status"] == "error" => {
                // monorepo#940: the terminal status-changed event carries the
                // structured corrupted-session flag.
                assert_eq!(
                    event["data"]["sessionCorrupted"],
                    json!(true),
                    "terminal agent:status-changed carries sessionCorrupted: true: {frame}"
                );
                saw_status_error_corrupted = true;
            }
            _ => {}
        }
        if saw_failed && saw_status_error_corrupted {
            break;
        }
    }
    assert!(
        saw_failed,
        "terminal agent:failed after the poisoned prompt"
    );
    assert!(
        saw_status_error_corrupted,
        "agent:status-changed(status=error, sessionCorrupted=true) emitted"
    );

    // The resume provably happened (this is the load-poisoned path, not a
    // fresh-session failure).
    assert_eq!(
        session_log_lines(&session_log),
        vec!["new", "load"],
        "turn 2 resumed the persisted session via session/load"
    );

    // The derived flag is also on the agent.getSession projection.
    let session = wss_rpc(
        &mut rpc,
        13,
        "agent.getSession",
        json!({ "workspaceId": ws_id, "agentId": agent_id }),
    )
    .await;
    assert_eq!(
        session["session"]["status"], "error",
        "session parked in error after the poisoned prompt"
    );
    assert_eq!(
        session["session"]["sessionCorrupted"],
        json!(true),
        "agent.getSession carries the derived sessionCorrupted flag: {session}"
    );
    let stop_reason = session["session"]["stopReason"]
        .as_str()
        .expect("stopReason present");
    assert!(
        stop_reason.contains("\"apiStatus\":\"invalidArgument\""),
        "stopReason persists the rejection payload, got: {stop_reason}"
    );

    // agent.retry on the poisoned session: arms force-recreate, redrives the
    // requeued message on a fresh session/new — which succeeds.
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
        "agent.retry redrives the requeued poisoned-turn message"
    );
    // The #940 WARN proves the poisoned classification armed force-recreate.
    await_daemon_log_contains(
        &data_dir,
        "retrying a poisoned session: arming force-recreate",
    )
    .await;

    let mut saw_retry_chunk = false;
    let mut saw_retry_end = false;
    for _ in 0..200 {
        let frame = wss_event(&mut sub, 30).await;
        let event = &frame["params"]["event"];
        if event["data"]["agentId"].as_str() != Some(agent_id.as_str()) {
            continue;
        }
        match event["type"].as_str() {
            Some("agent:failed") => {
                panic!("agent:failed AGAIN after the recreate retry (resume loop — the #940 regression): {frame}")
            }
            Some("chat:stream:delta") => {
                if event["data"]["content"]
                    .as_str()
                    .unwrap_or_default()
                    .contains("recreated-turn-response-940")
                {
                    saw_retry_chunk = true;
                }
            }
            Some("agent:stream:end") => {
                saw_retry_end = true;
                break;
            }
            _ => {}
        }
    }
    assert!(
        saw_retry_chunk,
        "retry turn streamed the fresh session's response"
    );
    assert!(saw_retry_end, "retry turn ended with agent:stream:end");

    // The retry recreated (session/new) instead of resuming: exactly one
    // "load" in the whole run. A trailing "load" here is the pre-fix loop.
    assert_eq!(
        session_log_lines(&session_log),
        vec!["new", "load", "new"],
        "agent.retry opened a fresh session/new instead of resuming the poisoned session"
    );

    // Status recovered; the corrupted flag is gone from the projection.
    let final_session = await_session_idle(&mut rpc, 300, &ws_id, &agent_id).await;
    assert!(
        final_session["session"]["stopReason"].is_null(),
        "stopReason cleared after the successful recreate retry: {:?}",
        final_session["session"]["stopReason"]
    );
    assert!(
        final_session["session"]["sessionCorrupted"].is_null()
            || final_session["session"]["sessionCorrupted"] == json!(false),
        "sessionCorrupted no longer set after recovery: {final_session}"
    );
}
