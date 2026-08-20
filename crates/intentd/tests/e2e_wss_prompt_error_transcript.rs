//! WSS end-to-end regression for monorepo#479: assistant text streamed by the
//! provider BEFORE a `session/prompt` JSON-RPC error must survive the failed
//! turn — persisted to the transcript and returned by `agent.getConversation`
//! — and the terminal `agent:failed` error string must carry the provider's
//! `error.data` detail (not just "-32603: Internal error"). The mock mirrors
//! codex-acp verbatim: it streams the "Model metadata … not found" warning as
//! an `agent_message_chunk`, then rejects the prompt with a -32603 whose
//! `data` holds the backend 400 JSON. The agent must land in `error` with the
//! message requeued, and `agent.retry` must redrive it WITHOUT losing the
//! pre-error assistant row.
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

/// codex-acp's non-fatal metadata warning, forwarded as agent text before the
/// failing turn (verbatim from the #479 diagnosis stderr/stream capture).
const WARNING_TEXT: &str = "Model metadata for `gpt-5.6-sol` not found. \
    Defaulting to fallback metadata; this can degrade performance and cause issues.";

/// The backend 400 that codex-acp stuffs — as a JSON *string* — into
/// `error.data.message` (verbatim from the #479 diagnosis).
const BACKEND_400: &str = r#"{"type":"error","status":400,"error":{"type":"invalid_request_error","message":"The 'gpt-5.6-sol' model requires a newer version of Codex. Please upgrade to the latest app or CLI and try again."}}"#;

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
    let dir = PathBuf::from("/tmp").join(format!("itd-wss-prompterr-{}", &id[..8]));
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
        title: "WSS-PROMPTERR-E2E".to_string(),
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

/// Serialize a conversation row's `contentBlocks` for substring assertions.
fn blocks_text(message: &Value) -> String {
    serde_json::to_string(&message["contentBlocks"]).unwrap_or_default()
}

/// The observed monorepo#3007 failure detail: a Node fetch EPIPE with the
/// provider's `apiStatus: unavailable` JSON, wrapped in a -32603 `data`.
const FETCH_EPIPE_UNAVAILABLE: &str = "fetch failed (EPIPE: connect EPIPE 34.36.229.120:443): \
    {\"apiStatus\":\"unavailable\",\"message\":\"fetch failed (EPIPE: connect EPIPE 34.36.229.120:443)\"}";

/// Regression for monorepo#3007 over the real WSS transport: the provider
/// fails `session/prompt` twice with a transient fetch failure (`-32603`
/// wrapping EPIPE + `apiStatus: unavailable`, zero output streamed), then
/// succeeds. The daemon must retry in place on the same connection — the
/// turn completes with the third attempt's response, NO `agent:failed` is
/// emitted, and the session never parks in `error`.
#[tokio::test]
async fn transient_prompt_fetch_failure_retries_in_place_over_wss() {
    let Some(script) = gate("WSS transient prompt retry E2E") else {
        return;
    };
    let data_dir = temp_data_dir();
    let ws_id = seed_workspace_only(&data_dir).await;
    let attempt_file = data_dir.join("attempts.txt");
    let attempt_file_s = attempt_file.to_string_lossy().into_owned();
    // Transient shape, output-free: NO streamBeforeErrorText — the failed
    // attempts stream nothing, so the in-place retry guard stays armed.
    let behavior = json!({
        "promptRpcError": {
            "code": -32603,
            "message": "Internal error",
            "data": FETCH_EPIPE_UNAVAILABLE,
        },
        "promptRpcErrorAttempts": 2,
        "response": "recovered after transient fetch failures",
    })
    .to_string();
    let env: [(&str, &str); 6] = [
        ("INTENTD_AUTH_TOKEN", TOKEN),
        ("INTENTD_TCP_PORT", "0"),
        ("MOCK_AGENT_SCRIPT_PATH", &script),
        ("MOCK_AGENT_BEHAVIOR", &behavior),
        ("MOCK_AGENT_ATTEMPT_FILE", &attempt_file_s),
        ("INTENTD_TRANSIENT_PROMPT_RETRY_BASE_MS", "10"),
    ];
    let child = spawn_serve(&data_dir, &env);
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
        json!({ "workspaceId": ws_id, "name": "WSS-3007-RETRY", "model": "mock:default" }),
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
        json!({ "workspaceId": ws_id, "agentId": agent_id, "content": "prompt that blips" }),
    )
    .await;
    assert_eq!(sent["success"], true, "sendMessage ok: {sent}");

    // The turn must settle successfully: agent:stream:end WITHOUT any
    // agent:failed or status=error along the way — the two transient
    // failures are absorbed by the in-place retry.
    let mut saw_end = false;
    for _ in 0..200 {
        let frame = wss_event(&mut sub, 30).await;
        let event = &frame["params"]["event"];
        if event["data"]["agentId"].as_str() != Some(agent_id.as_str()) {
            continue;
        }
        match event["type"].as_str() {
            Some("agent:failed") => {
                panic!("agent:failed emitted despite in-place retry (monorepo#3007): {frame}");
            }
            Some("agent:status-changed") if event["data"]["status"] == "error" => {
                panic!("session parked in error despite in-place retry: {frame}");
            }
            Some("agent:stream:end") => {
                saw_end = true;
                break;
            }
            _ => {}
        }
    }
    assert!(saw_end, "turn completed after transient-failure retries");

    // Exactly three attempts hit the mock: initial + 2 retries. The attempt
    // file holds the NEXT attempt number, so 3 calls leave "4".
    let attempts = std::fs::read_to_string(&attempt_file).unwrap_or_default();
    assert_eq!(
        attempts.trim(),
        "4",
        "mock served exactly 3 session/prompt attempts (initial + 2 retries)"
    );

    // The recovered response persisted as the turn's assistant row and the
    // session is NOT in error.
    let convo = wss_rpc(
        &mut rpc,
        12,
        "agent.getConversation",
        json!({ "workspaceId": ws_id, "agentId": agent_id }),
    )
    .await;
    let messages = convo["messages"]
        .as_array()
        .expect("conversation messages array");
    assert!(
        messages.iter().any(|m| m["role"] == "assistant"
            && blocks_text(m).contains("recovered after transient fetch failures")),
        "recovered turn's response persisted: {convo}"
    );
    let session = wss_rpc(
        &mut rpc,
        13,
        "agent.getSession",
        json!({ "workspaceId": ws_id, "agentId": agent_id }),
    )
    .await;
    assert_ne!(
        session["session"]["status"], "error",
        "recovered turn must not park the session in error: {session}"
    );
}

/// PROMPTERR-1 (#479): provider streams a warning chunk then fails the prompt
/// with a JSON-RPC -32603 carrying the real detail in `data`. The daemon must
/// (1) persist the pre-error chunk so `agent.getConversation` returns it,
/// (2) emit `agent:failed` whose error includes the `data` detail,
/// (3) park the agent in `error` with the message requeued, and
/// (4) redrive it via `agent.retry` without losing the pre-error row.
#[tokio::test]
async fn failed_prompt_turn_preserves_partial_transcript_over_wss() {
    let Some(script) = gate("WSS prompt-error transcript E2E") else {
        return;
    };
    let data_dir = temp_data_dir();
    let ws_id = seed_workspace_only(&data_dir).await;
    let attempt_file = data_dir.join("attempts.txt");
    let attempt_file_s = attempt_file.to_string_lossy().into_owned();
    // Verbatim codex-acp failure shape (#479 diagnosis): the -32603 `data` is
    // an object whose `message` holds the backend 400 JSON as a string.
    let behavior = json!({
        "promptRpcError": {
            "code": -32603,
            "message": "Internal error",
            "data": { "message": BACKEND_400, "codex_error_info": "other" },
        },
        "promptRpcErrorAttempts": 1,
        "streamBeforeErrorText": WARNING_TEXT,
        "response": "recovered after provider prompt error",
    })
    .to_string();
    let env: [(&str, &str); 5] = [
        ("INTENTD_AUTH_TOKEN", TOKEN),
        ("INTENTD_TCP_PORT", "0"),
        ("MOCK_AGENT_SCRIPT_PATH", &script),
        ("MOCK_AGENT_BEHAVIOR", &behavior),
        ("MOCK_AGENT_ATTEMPT_FILE", &attempt_file_s),
    ];
    let child = spawn_serve(&data_dir, &env);
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
        json!({ "workspaceId": ws_id, "name": "WSS-PROMPTERR", "model": "mock:default" }),
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
        json!({ "workspaceId": ws_id, "agentId": agent_id, "content": "prompt that fails" }),
    )
    .await;
    assert_eq!(sent["success"], true, "sendMessage ok: {sent}");

    // Wait for the full terminal set: the streamed warning chunk, agent:failed
    // with the -32603 detail, agent:stream:end, and agent:status-changed
    // (status=error). The status-changed event is emitted AFTER the status
    // persist, so seeing it guarantees the write is visible.
    let mut saw_chunk = false;
    let mut saw_failed = false;
    let mut saw_end = false;
    let mut saw_status_error = false;
    for _ in 0..200 {
        let frame = wss_event(&mut sub, 30).await;
        let event = &frame["params"]["event"];
        if event["data"]["agentId"].as_str() != Some(agent_id.as_str()) {
            continue;
        }
        match event["type"].as_str() {
            Some("chat:stream:delta") => {
                if serde_json::to_string(&event["data"])
                    .unwrap_or_default()
                    .contains("Model metadata for")
                {
                    saw_chunk = true;
                }
            }
            Some("agent:failed") => {
                // run_prompt_turn emits the raw prompt error; the JsonRpcError
                // Display must append the `data` payload (#479) so the backend
                // detail is no longer hidden behind "Internal error".
                let err = event["data"]["error"].as_str().unwrap_or("");
                assert!(
                    err.contains("JSON-RPC error -32603: Internal error"),
                    "agent:failed carries the JSON-RPC error envelope, got: {err}"
                );
                assert!(
                    err.contains("requires a newer version of Codex"),
                    "agent:failed surfaces the backend detail from error.data, got: {err}"
                );
                assert!(
                    err.contains("codex_error_info"),
                    "agent:failed renders the full data object, got: {err}"
                );
                saw_failed = true;
            }
            Some("agent:stream:end") => {
                saw_end = true;
            }
            Some("agent:status-changed") if event["data"]["status"] == "error" => {
                saw_status_error = true;
            }
            _ => {}
        }
        if saw_chunk && saw_failed && saw_end && saw_status_error {
            break;
        }
    }
    assert!(saw_chunk, "pre-error warning chunk streamed on the wire");
    assert!(
        saw_failed,
        "terminal agent:failed emitted after prompt error"
    );
    assert!(
        saw_end,
        "terminal agent:stream:end emitted after prompt error"
    );
    assert!(
        saw_status_error,
        "agent:status-changed with status=error emitted after prompt error"
    );

    // THE regression (#479): the pre-error streamed warning is persisted as an
    // assistant row and returned by agent.getConversation even though the turn
    // failed. Removing the pre-error transcript persistence in run_prompt_turn
    // makes this assertion fail.
    let convo = wss_rpc(
        &mut rpc,
        12,
        "agent.getConversation",
        json!({ "workspaceId": ws_id, "agentId": agent_id }),
    )
    .await;
    let messages = convo["messages"]
        .as_array()
        .expect("conversation messages array");
    assert!(
        messages
            .iter()
            .any(|m| m["role"] == "assistant" && blocks_text(m).contains("Model metadata for")),
        "pre-error warning chunk persisted to the transcript: {convo}"
    );

    // Persisted error status + stopReason carry the same detail over RPC.
    let session = wss_rpc(
        &mut rpc,
        13,
        "agent.getSession",
        json!({ "workspaceId": ws_id, "agentId": agent_id }),
    )
    .await;
    assert_eq!(
        session["session"]["status"], "error",
        "session status is error after failed prompt turn"
    );
    let stop_reason = session["session"]["stopReason"]
        .as_str()
        .expect("stopReason should be present after failed prompt turn");
    assert!(
        stop_reason.contains("-32603") && stop_reason.contains("requires a newer version of Codex"),
        "stopReason carries the -32603 detail, got: {stop_reason}"
    );

    // The failed message was requeued (not silently dropped), ready for retry.
    let queue = wss_rpc(
        &mut rpc,
        14,
        "agent.getQueue",
        json!({ "workspaceId": ws_id, "agentId": agent_id }),
    )
    .await;
    let queued = queue["queue"].as_array().expect("queue array");
    assert_eq!(queued.len(), 1, "failed message requeued: {queue}");
    assert_eq!(
        queued[0]["content"], "prompt that fails",
        "requeued message preserves the original content"
    );

    // agent.retry redrives the requeued message; the mock's attempt counter is
    // past the failure window, so the second turn completes normally.
    let retry_result = wss_rpc(
        &mut rpc,
        15,
        "agent.retry",
        json!({ "workspaceId": ws_id, "agentId": agent_id }),
    )
    .await;
    assert_eq!(retry_result["ok"], true, "agent.retry ok on error status");
    assert_eq!(
        retry_result["redriven"], true,
        "agent.retry reports the requeued message is being redriven"
    );

    let mut saw_retry_end = false;
    for _ in 0..200 {
        let frame = wss_event(&mut sub, 30).await;
        let event = &frame["params"]["event"];
        if event["data"]["agentId"].as_str() != Some(agent_id.as_str()) {
            continue;
        }
        match event["type"].as_str() {
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
        saw_retry_end,
        "agent:stream:end emitted after retry redrive"
    );

    // The retry must not lose the pre-error assistant row: the conversation
    // holds BOTH the persisted warning and the recovered response, and exactly
    // one user row (the requeued entry is flagged `persisted`, so the drain
    // does not append a duplicate).
    let final_convo = wss_rpc(
        &mut rpc,
        16,
        "agent.getConversation",
        json!({ "workspaceId": ws_id, "agentId": agent_id }),
    )
    .await;
    let final_messages = final_convo["messages"]
        .as_array()
        .expect("conversation messages array");
    assert!(
        final_messages
            .iter()
            .any(|m| m["role"] == "assistant" && blocks_text(m).contains("Model metadata for")),
        "pre-error warning row survives the retry: {final_convo}"
    );
    assert!(
        final_messages.iter().any(|m| m["role"] == "assistant"
            && blocks_text(m).contains("recovered after provider prompt error")),
        "retry turn appended the recovered response: {final_convo}"
    );
    let user_rows = final_messages
        .iter()
        .filter(|m| m["role"] == "user")
        .count();
    assert_eq!(
        user_rows, 1,
        "exactly one user row after retry (no duplicate): {final_convo}"
    );
}

/// monorepo#840: a session-fatal provider block ("blocked … for safety
/// reasons. Please start a new session") poisons the session — a follow-up
/// `agent.sendMessage` must NOT redrive the blocked turn. The JSON-RPC
/// result carries the quarantine envelope (`success: true, queued: true,
/// quarantined: true, queuedMessage`), the message parks in the queue behind
/// the requeued failure, the session stays in `error`, and no second
/// `agent:failed` replay hits the event stream.
#[tokio::test]
async fn poisoned_session_quarantines_send_message_over_wss() {
    let Some(script) = gate("WSS poisoned-session quarantine E2E") else {
        return;
    };
    let data_dir = temp_data_dir();
    let ws_id = seed_workspace_only(&data_dir).await;
    // Every prompt fails with the canonical provider safety block (no
    // attempt gating): the session is poisoned by the fatal stop_reason.
    let behavior = json!({
        "promptRpcError": {
            "code": -32603,
            "message": "The model provider blocked this response for safety reasons. \
                        Please start a new session",
        },
    })
    .to_string();
    let env: [(&str, &str); 4] = [
        ("INTENTD_AUTH_TOKEN", TOKEN),
        ("INTENTD_TCP_PORT", "0"),
        ("MOCK_AGENT_SCRIPT_PATH", &script),
        ("MOCK_AGENT_BEHAVIOR", &behavior),
    ];
    let child = spawn_serve(&data_dir, &env);
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
        json!({ "workspaceId": ws_id, "name": "WSS-POISON", "model": "mock:default" }),
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
        json!({ "workspaceId": ws_id, "agentId": agent_id, "content": "blocked prompt" }),
    )
    .await;
    assert_eq!(sent["success"], true, "first sendMessage ok: {sent}");

    // Wait for the terminal failure to persist (status-changed(error) is
    // emitted AFTER the persist).
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
    assert!(saw_status_error, "agent parked in error after the block");

    // The follow-up delivery hits the quarantine gate: the wire result
    // carries the full quarantine envelope instead of driving a turn.
    let followup = wss_rpc(
        &mut rpc,
        12,
        "agent.sendMessage",
        json!({ "workspaceId": ws_id, "agentId": agent_id, "content": "follow-up" }),
    )
    .await;
    assert_eq!(followup["success"], true, "quarantined send: {followup}");
    assert_eq!(followup["queued"], true, "message parked: {followup}");
    assert_eq!(
        followup["quarantined"], true,
        "result carries the quarantine flag: {followup}"
    );
    assert!(
        followup["queuedMessage"]["id"].is_string(),
        "queuedMessage envelope present: {followup}"
    );

    // Queue holds the requeued failure plus the parked follow-up.
    let queue = wss_rpc(
        &mut rpc,
        13,
        "agent.getQueue",
        json!({ "workspaceId": ws_id, "agentId": agent_id }),
    )
    .await;
    let queued = queue["queue"].as_array().expect("queue array");
    assert_eq!(
        queued.len(),
        2,
        "requeued failure + parked follow-up: {queue}"
    );
    assert_eq!(queued[0]["content"], "blocked prompt");
    assert_eq!(queued[1]["content"], "follow-up");

    // The session stays parked in error with the fatal stopReason — the
    // quarantined send did not clear or redrive anything.
    let session = wss_rpc(
        &mut rpc,
        14,
        "agent.getSession",
        json!({ "workspaceId": ws_id, "agentId": agent_id }),
    )
    .await;
    assert_eq!(
        session["session"]["status"], "error",
        "quarantined session stays in error"
    );
    let stop_reason = session["session"]["stopReason"]
        .as_str()
        .expect("stopReason present");
    assert!(
        stop_reason.contains("blocked") && stop_reason.contains("for safety reasons"),
        "stopReason carries the provider block, got: {stop_reason}"
    );

    // No agent:failed replay reached the stream after the quarantined send —
    // the gate parked the message without spawning a worker. Drain the
    // subscription briefly; any agent:failed for this agent is a regression.
    let deadline = tokio::time::Instant::now() + Duration::from_secs(3);
    loop {
        let remaining = deadline.saturating_duration_since(tokio::time::Instant::now());
        if remaining.is_zero() {
            break;
        }
        let Ok(Some(Ok(Message::Text(text)))) = timeout(remaining, sub.next()).await else {
            break;
        };
        let frame: Value = serde_json::from_str(&text).unwrap_or_default();
        let event = &frame["params"]["event"];
        if event["data"]["agentId"].as_str() == Some(agent_id.as_str()) {
            assert_ne!(
                event["type"], "agent:failed",
                "quarantined send must not replay the blocked turn: {frame}"
            );
        }
    }
}
