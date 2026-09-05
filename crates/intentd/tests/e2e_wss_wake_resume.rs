//! WSS end-to-end for sleep/wake auto-resume (wakeResume feature): a turn whose
//! upstream `session/prompt` drops transiently while a host suspend overlaps is
//! ENROLLED (not surfaced terminally) and then AUTO-RESUMES — all over the real
//! WSS transport (TLS upgrade → JSON-RPC 2.0 over WebSocket → router → services).
//!
//! Drives the real daemon with the deterministic mock ACP provider:
//! - The `INTENTD_TEST_FORCE_SUSPEND_OVERLAP_SECS` seam makes any transient
//!   disconnect count as suspend-overlapping, so the first prompt attempt
//!   (which fails with a connection-reset RPC error) is enrolled as
//!   `system_suspend` — the interrupted `agent:stream:end`, NOT `agent:failed`.
//! - No real host suspend is recorded, so the wake broadcast never fires; the
//!   turn recovers via the enrollment-driven SELF-HEAL resume (finding 2). The
//!   resume respawns a fresh child and reloads via `session/load` (finding 1),
//!   then the second prompt attempt succeeds and the turn completes.
//!
//! Asserts: the interrupted terminal event carries `interruptReason:
//! "system_suspend"`, no `agent:failed` is emitted, the turn resumes to a
//! successful completion, `agent.listInterrupted` drains to empty, and the mock
//! logged a `session/load` on the resume (proving the child was torn down).
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
    let dir = PathBuf::from("/tmp").join(format!("itd-wss-wake-{}", &id[..8]));
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
        title: "WSS-WAKE-RESUME-E2E".to_string(),
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
        setup_result: None,
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
    }
}

/// A suspend-shaped turn interruption drives enrollment + auto-resume over the
/// real WSS transport: the first `session/prompt` fails with a transient
/// connection-reset RPC error while a (forced) host suspend overlaps, so the
/// turn is enrolled as `system_suspend` and then self-heals to a successful
/// resume. Asserts the interrupted terminal event shape, the absence of
/// `agent:failed`, the drained interrupted list, and the `session/load` on
/// resume (child torn down, reloaded fresh).
#[tokio::test]
async fn suspend_interrupted_turn_enrolls_and_resumes_over_wss() {
    let Some(script) = gate("WSS wake-resume E2E") else {
        return;
    };
    let data_dir = temp_data_dir();
    let ws_id = seed_workspace_only(&data_dir).await;
    let attempt_file = data_dir.join("attempts.txt");
    let attempt_file_s = attempt_file.to_string_lossy().into_owned();
    let session_log = data_dir.join("sessions.log");
    let session_log_s = session_log.to_string_lossy().into_owned();
    // First prompt attempt fails with a transient (connection-class) RPC error
    // after streaming a warning chunk; the retry (attempt 2, on the reloaded
    // session) succeeds. `loadSession: true` makes the resume's `session/load`
    // reachable.
    let behavior = json!({
        "loadSession": true,
        "promptRpcError": { "code": -32603, "message": "Connection reset by peer" },
        "promptRpcErrorAttempts": 1,
        "streamBeforeErrorText": "partial ",
        "response": "resumed after suspend",
    })
    .to_string();
    let env: [(&str, &str); 8] = [
        ("INTENTD_AUTH_TOKEN", TOKEN),
        ("INTENTD_TCP_PORT", "0"),
        ("MOCK_AGENT_SCRIPT_PATH", &script),
        ("MOCK_AGENT_BEHAVIOR", &behavior),
        ("MOCK_AGENT_ATTEMPT_FILE", &attempt_file_s),
        ("MOCK_AGENT_SESSION_LOG", &session_log_s),
        // Force every transient disconnect to count as suspend-overlapping, and
        // compress the enrollment self-heal so the resume fires promptly.
        ("INTENTD_TEST_FORCE_SUSPEND_OVERLAP_SECS", "120"),
        ("INTENTD_WAKE_RESUME_SELF_HEAL_MS", "300"),
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
        json!({ "workspaceId": ws_id, "name": "WSS-WAKE", "model": "default", "provider": "mock" }),
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
        json!({ "workspaceId": ws_id, "agentId": agent_id, "content": "work through the sleep" }),
    )
    .await;
    assert_eq!(sent["success"], true, "sendMessage ok: {sent}");

    // Watch the event stream: the first turn is enrolled (interrupted
    // stream:end tagged system_suspend, never agent:failed), then the self-heal
    // resume drives the agent back to idle on a successful turn.
    let mut saw_suspend_end = false;
    let mut resumed_to_idle = false;
    for _ in 0..400 {
        let frame = wss_event(&mut sub, 30).await;
        let event = &frame["params"]["event"];
        if event["data"]["agentId"].as_str() != Some(agent_id.as_str()) {
            continue;
        }
        match event["type"].as_str() {
            Some("agent:failed") => {
                panic!("a suspend-induced interruption must NOT surface agent:failed: {event}");
            }
            Some("agent:stream:end")
                if event["data"]["interruptReason"].as_str() == Some("system_suspend") =>
            {
                assert_eq!(
                    event["data"]["stopReason"].as_str(),
                    Some("interrupted"),
                    "suspend enrollment emits an interrupted stream:end: {event}"
                );
                saw_suspend_end = true;
            }
            // The resumed turn completes normally: agent:idle after a fresh
            // (post-session/load) prompt succeeds.
            Some("agent:idle") if saw_suspend_end => {
                resumed_to_idle = true;
            }
            _ => {}
        }
        if saw_suspend_end && resumed_to_idle {
            break;
        }
    }
    assert!(
        saw_suspend_end,
        "the turn was enrolled with an interrupted stream:end tagged system_suspend"
    );
    assert!(
        resumed_to_idle,
        "the enrolled turn self-healed to a successful resumed completion (agent:idle)"
    );

    // The interrupted list drains: the enrolled row was claimed and resolved by
    // the resume (poll — the self-heal is asynchronous).
    let mut drained = false;
    for _ in 0..40 {
        let result = wss_rpc(&mut rpc, 20, "agent.listInterrupted", json!({})).await;
        let agents = result["agents"].as_array().cloned().unwrap_or_default();
        if !agents
            .iter()
            .any(|a| a["agentId"].as_str() == Some(agent_id.as_str()))
        {
            drained = true;
            break;
        }
        tokio::time::sleep(Duration::from_millis(100)).await;
    }
    assert!(
        drained,
        "the enrolled interrupted_agent row is resolved once the resume claims it"
    );

    // The resume tore down the child and reloaded the persisted session:
    // the mock logged a `session/load` (finding 1 — no live-child reuse).
    let sessions = std::fs::read_to_string(&session_log).unwrap_or_default();
    assert!(
        sessions
            .lines()
            .any(|l| l.contains("\"session/load\"") || l.contains("session/load")),
        "resume issues session/load against the persisted session: {sessions:?}"
    );
}
