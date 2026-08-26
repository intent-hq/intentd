//! WSS end-to-end mid-turn agent failure (STAB terminal turn-failure path):
//! the child spawns and handshakes fine, then dies while `session/prompt` is
//! in flight. The daemon must surface the failure — `agent:failed`,
//! `agent:stream:end`, persisted `status=error` — requeue the message, and
//! redrive it via `agent.retry` (no silent drop). The crash must also leave
//! the child's stderr captured under `<data_dir>/agent-logs/<agent-id>/`
//! with the terminal-failure WARN pointing at the capture path (STAB-53).
//!
//! Also home to the monorepo#764 dead-child RECOVERY paths, which reuse the
//! same fixture machinery: a child that dies while the agent is idle respawns
//! transparently on the next message, and a transport-closed prompt failure
//! BEFORE any streamed output is silently redriven once on a fresh child.
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
        title: "WSS-MIDTURN-E2E".to_string(),
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

/// MIDTURN-1: child dies mid-`session/prompt` → terminal `agent:failed` +
/// `agent:stream:end` + persisted `status=error` + message requeued, then
/// `agent.retry` redrives the queued message to a successful turn.
///
/// The mock spawns and handshakes normally (so the spawn-retry path is NOT in
/// play), then `process.exit(1)`s inside `session/prompt` on the first TWO
/// attempts. The daemon's in-flight prompt fails ("agent stdout closed")
/// before any output, so the first failure is consumed by the one-shot silent
/// redrive (monorepo#764); the second spends the budget and must be classified
/// as a terminal mid-turn failure — the exact "silent drop" scenario this test
/// locks down.
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
        "exitDuringPromptAttempts": 2,
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
                // This agent was created via the RPC front door (parentless):
                // the optional `parentAgentId` must be OMITTED — never `null`.
                assert!(
                    frame["params"]["event"]["data"]
                        .get("parentAgentId")
                        .is_none(),
                    "parentAgentId omitted on agent:failed for a parentless agent: {frame}"
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
            Some("agent:stream:activity") => {
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
    assert!(
        saw_chunk,
        "agent:stream:activity emitted after retry redrive"
    );
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

/// MIDTURN-1b (monorepo#3592): the terminal-failure stderr capture hint must
/// sweep the process group BEFORE awaiting drain settlement. The mock's final
/// gated exit leaves a same-group `sleep 300` holding the inherited stderr
/// write end open, so the drain can only hit EOF once the hint's own group
/// sweep kills the descendant. The settle bound is widened to 30s via
/// `INTENTD_STDERR_SETTLE_TIMEOUT_MS` and the WARN hint is required well
/// inside it: with the sweep-then-settle ordering the hint lands in a couple
/// of seconds, while the regressed settle-then-sweep ordering burns the full
/// widened bound and fails the ceiling deterministically (instead of racing
/// the default 2s timeout). The sweep must also actually reap the holder.
#[tokio::test]
async fn stderr_hint_settles_past_pipe_holding_descendant_over_wss() {
    let Some(script) = gate("WSS pipe-holding-descendant stderr hint E2E") else {
        return;
    };
    let data_dir = temp_data_dir();
    let ws_id = seed_workspace_only(&data_dir).await;
    let attempt_file = data_dir.join("attempts.txt");
    let attempt_file_s = attempt_file.to_string_lossy().into_owned();
    // Attempt 1's death is consumed by the one-shot silent redrive; attempt 2
    // (the terminal one) spawns the pipe-holding descendant before exiting.
    let behavior = json!({
        "exitDuringPromptAttempts": 2,
        "holdStderrOpenOnExit": true,
        "response": "unused",
    })
    .to_string();
    let env: [(&str, &str); 8] = [
        ("INTENTD_AUTH_TOKEN", TOKEN),
        ("INTENTD_TCP_PORT", "0"),
        ("MOCK_AGENT_SCRIPT_PATH", &script),
        ("MOCK_AGENT_BEHAVIOR", &behavior),
        ("MOCK_AGENT_ATTEMPT_FILE", &attempt_file_s),
        ("INTENTD_SESSION_SETUP_TIMEOUT_MS", "2000"),
        ("INTENTD_SPAWN_RETRY_BACKOFF_MS", "100,200"),
        ("INTENTD_STDERR_SETTLE_TIMEOUT_MS", "30000"),
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

    let mut rpc = connect_ws(port, cfg.clone()).await;
    let created = wss_rpc(
        &mut rpc,
        10,
        "agent.create",
        json!({ "workspaceId": ws_id, "name": "WSS-STDERR-HOLD", "model": "mock:default" }),
    )
    .await;
    let agent_id = created["agent"]["id"]
        .as_str()
        .expect("agent id")
        .to_string();

    let started = std::time::Instant::now();
    let sent = wss_rpc(
        &mut rpc,
        11,
        "agent.sendMessage",
        json!({ "workspaceId": ws_id, "agentId": agent_id, "content": "dies holding stderr" }),
    )
    .await;
    assert_eq!(sent["success"], true, "sendMessage ok: {sent}");

    // The WARN hint must name the capture path — and land well inside the
    // widened 30s settle bound: the hint's own group sweep (not the timeout)
    // is what unblocks the drain when a descendant holds the pipe.
    let capture_dir = intent_core::agent_logs_root(&data_dir).join(&agent_id);
    let expected_hint = format!("agent stderr captured at {}", capture_dir.display());
    await_daemon_log_contains(&data_dir, &expected_hint).await;
    let elapsed = started.elapsed();
    assert!(
        elapsed < Duration::from_secs(15),
        "stderr hint arrived via the group sweep, not by exhausting the 30s \
         settle bound (took {elapsed:?})"
    );

    // The capture holds the child's dying words, including the holder spawn —
    // proving the drain reached EOF and flushed past the descendant.
    let mut captured = String::new();
    if let Ok(mut entries) = tokio::fs::read_dir(&capture_dir).await {
        while let Ok(Some(entry)) = entries.next_entry().await {
            if let Ok(c) = tokio::fs::read_to_string(entry.path()).await {
                captured.push_str(&c);
            }
        }
    }
    assert!(
        captured.contains("exiting during prompt"),
        "stderr capture under {} holds the child's last words; got: {captured:?}",
        capture_dir.display()
    );
    let holder_pid: i32 = captured
        .lines()
        .find_map(|l| l.split("spawned stderr-holding descendant pid=").nth(1))
        .and_then(|s| s.trim().parse().ok())
        .unwrap_or_else(|| panic!("capture names the holder pid; got: {captured:?}"));

    // The sweep reaped the pipe-holding descendant (it would otherwise live
    // for 300s). Bounded poll: SIGKILL delivery + init reaping the orphan.
    let holder = nix::unistd::Pid::from_raw(holder_pid);
    let mut holder_dead = false;
    for _ in 0..100 {
        if nix::sys::signal::kill(holder, None).is_err() {
            holder_dead = true;
            break;
        }
        tokio::time::sleep(Duration::from_millis(25)).await;
    }
    assert!(
        holder_dead,
        "group sweep reaped the stderr-holding descendant (pid {holder_pid})"
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
    // Two pre-output deaths: the first is consumed by the one-shot silent
    // redrive (monorepo#764), the second parks the agent in error.
    let behavior = json!({
        "exitDuringPromptAttempts": 2,
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
/// DEAD-IDLE (monorepo#764): the child is `SIGKILLed` out-of-band while the
/// agent sits idle after a completed turn. The proactive child-exit watcher
/// reaps the handle (one WARN, no events, status untouched), and the next
/// message transparently spawns a FRESH child and completes the turn — no
/// terminal `agent:failed`, no error status, and the transcript carries both
/// turns.
#[tokio::test]
async fn agent_dead_while_idle_respawns_transparently_over_wss() {
    let Some(script) = gate("WSS dead-while-idle recovery E2E") else {
        return;
    };
    let data_dir = temp_data_dir();
    let ws_id = seed_workspace_only(&data_dir).await;
    let pid_file = data_dir.join("pids.txt");
    let pid_file_s = pid_file.to_string_lossy().into_owned();
    // Distinct per-turn responses so turn 2's stream provably comes from the
    // respawned child answering the SECOND message, not a turn-1 leftover.
    let behavior = json!({
        "response": "first-turn-response-764",
        "rules": [
            { "ifPromptContains": "idle-kill-turn-two", "response": "respawned-turn-response-764" },
        ],
    })
    .to_string();
    let env: [(&str, &str); 7] = [
        ("INTENTD_AUTH_TOKEN", TOKEN),
        ("INTENTD_TCP_PORT", "0"),
        ("MOCK_AGENT_SCRIPT_PATH", &script),
        ("MOCK_AGENT_BEHAVIOR", &behavior),
        ("MOCK_AGENT_PID_FILE", &pid_file_s),
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
        json!({ "workspaceId": ws_id, "name": "WSS-DEAD-IDLE", "model": "mock:default" }),
    )
    .await;
    let agent_id = created["agent"]["id"]
        .as_str()
        .expect("agent id")
        .to_string();

    // Turn 1 completes normally on the first child.
    let sent = wss_rpc(
        &mut rpc,
        11,
        "agent.sendMessage",
        json!({ "workspaceId": ws_id, "agentId": agent_id, "content": "first turn" }),
    )
    .await;
    assert_eq!(sent["success"], true, "first sendMessage ok: {sent}");
    let mut saw_chunk = false;
    let mut saw_end = false;
    for _ in 0..200 {
        let frame = wss_event(&mut sub, 30).await;
        let event = &frame["params"]["event"];
        if event["data"]["agentId"].as_str() != Some(agent_id.as_str()) {
            continue;
        }
        match event["type"].as_str() {
            Some("agent:failed") => panic!("agent:failed during the healthy first turn: {frame}"),
            Some("chat:stream:delta") => {
                if event["data"]["content"]
                    .as_str()
                    .unwrap_or_default()
                    .contains("first-turn-response-764")
                {
                    saw_chunk = true;
                }
            }
            Some("agent:stream:end") => {
                saw_end = true;
                break;
            }
            _ => {}
        }
    }
    assert!(saw_chunk, "first turn streamed the mock response");
    assert!(saw_end, "first turn ended with agent:stream:end");
    await_session_idle(&mut rpc, 100, &ws_id, &agent_id).await;

    // SIGKILL the mock child out-of-band while the agent is idle.
    let pids = await_pid_lines(&pid_file, 1).await;
    let pid1 = pids[0];
    let killed = Command::new("kill")
        .args(["-9", &pid1.to_string()])
        .status()
        .expect("run kill")
        .success();
    assert!(killed, "SIGKILL delivered to idle mock child {pid1}");
    // Deterministic sync point (no sleeps racing the watcher): the proactive
    // child-exit watcher observes the idle death, reaps the handle, and logs
    // exactly this WARN. Persisted status is untouched, so nothing else to
    // wait on.
    await_daemon_log_contains(
        &data_dir,
        "idle agent child exited unexpectedly; handle reaped",
    )
    .await;

    // Turn 2 succeeds transparently on a FRESH child — no agent:failed, no
    // error status, a normal `{ agentId }` stream:end.
    let sent = wss_rpc(
        &mut rpc,
        12,
        "agent.sendMessage",
        json!({ "workspaceId": ws_id, "agentId": agent_id, "content": "idle-kill-turn-two" }),
    )
    .await;
    assert_eq!(sent["success"], true, "post-kill sendMessage ok: {sent}");
    let mut saw_respawn_chunk = false;
    let mut saw_respawn_end = false;
    for _ in 0..200 {
        let frame = wss_event(&mut sub, 30).await;
        let event = &frame["params"]["event"];
        if event["data"]["agentId"].as_str() != Some(agent_id.as_str()) {
            continue;
        }
        match event["type"].as_str() {
            Some("agent:failed") => {
                panic!("dead-while-idle recovery must not surface agent:failed: {frame}")
            }
            Some("agent:status-changed") if event["data"]["status"] == "error" => {
                panic!("dead-while-idle recovery must not park the agent in error: {frame}")
            }
            Some("chat:stream:delta") => {
                if event["data"]["content"]
                    .as_str()
                    .unwrap_or_default()
                    .contains("respawned-turn-response-764")
                {
                    saw_respawn_chunk = true;
                }
            }
            Some("agent:stream:end") => {
                assert!(
                    event["data"].get("stopReason").is_none(),
                    "transparent respawn turn ends with a normal stream:end: {frame}"
                );
                saw_respawn_end = true;
                break;
            }
            _ => {}
        }
    }
    assert!(
        saw_respawn_chunk,
        "post-kill turn streamed the respawned child's response"
    );
    assert!(
        saw_respawn_end,
        "post-kill turn ended with agent:stream:end"
    );

    // Fresh-spawn proof: a second, distinct pid line from the mock.
    let pids = await_pid_lines(&pid_file, 2).await;
    assert_eq!(pids.len(), 2, "exactly two spawns (one respawn): {pids:?}");
    assert_ne!(pids[0], pids[1], "respawn used a fresh process: {pids:?}");

    // Status settled back to idle with no stopReason.
    let session = await_session_idle(&mut rpc, 300, &ws_id, &agent_id).await;
    assert!(
        session["session"]["stopReason"].is_null(),
        "no stopReason after transparent recovery: {:?}",
        session["session"]["stopReason"]
    );

    // Resume continuity: ONE transcript carries both turns.
    let convo = wss_rpc(
        &mut rpc,
        500,
        "agent.getConversation",
        json!({ "workspaceId": ws_id, "agentId": agent_id }),
    )
    .await;
    let user_rows = convo["messages"]
        .as_array()
        .expect("conversation messages array")
        .iter()
        .filter(|m| m["role"] == "user")
        .count();
    assert_eq!(user_rows, 2, "both user turns persisted: {convo}");
    let serialized = convo.to_string();
    assert!(
        serialized.contains("first-turn-response-764")
            && serialized.contains("respawned-turn-response-764"),
        "both assistant responses persisted in one transcript: {convo}"
    );
}

/// PRE-TOKEN REDRIVE (monorepo#764): the first child handshakes fine, then
/// dies on `session/prompt` BEFORE emitting any output (first attempt only —
/// the respawned child answers normally). The one-shot silent redrive must
/// complete the turn on a fresh child with nothing user-visible: no
/// `agent:failed`, no error status, no requeued message (no Retry surface),
/// and a normal stream on the wire. The daemon log carries the redrive WARN,
/// proving the failure actually happened and was consumed silently.
#[tokio::test]
async fn agent_pre_token_transport_failure_redrives_silently_over_wss() {
    let Some(script) = gate("WSS pre-token silent-redrive E2E") else {
        return;
    };
    let data_dir = temp_data_dir();
    let ws_id = seed_workspace_only(&data_dir).await;
    let attempt_file = data_dir.join("attempts.txt");
    let attempt_file_s = attempt_file.to_string_lossy().into_owned();
    let pid_file = data_dir.join("pids.txt");
    let pid_file_s = pid_file.to_string_lossy().into_owned();
    // ONE pre-output death: consumed by the one-shot silent redrive, so the
    // respawned child (attempt 2) answers normally.
    let behavior = json!({
        "exitDuringPromptAttempts": 1,
        "response": "silent-redrive-response-764",
    })
    .to_string();
    let env: [(&str, &str); 8] = [
        ("INTENTD_AUTH_TOKEN", TOKEN),
        ("INTENTD_TCP_PORT", "0"),
        ("MOCK_AGENT_SCRIPT_PATH", &script),
        ("MOCK_AGENT_BEHAVIOR", &behavior),
        ("MOCK_AGENT_ATTEMPT_FILE", &attempt_file_s),
        ("MOCK_AGENT_PID_FILE", &pid_file_s),
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
        json!({ "workspaceId": ws_id, "name": "WSS-SILENT-REDRIVE", "model": "mock:default" }),
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
        json!({ "workspaceId": ws_id, "agentId": agent_id, "content": "dies once before output" }),
    )
    .await;
    assert_eq!(sent["success"], true, "sendMessage ok: {sent}");

    // The turn must complete via the single silent redrive: any terminal
    // failure surface on the wire is a regression.
    let mut saw_chunk = false;
    let mut saw_end = false;
    for _ in 0..200 {
        let frame = wss_event(&mut sub, 30).await;
        let event = &frame["params"]["event"];
        if event["data"]["agentId"].as_str() != Some(agent_id.as_str()) {
            continue;
        }
        match event["type"].as_str() {
            Some("agent:failed") => {
                panic!("pre-token failure must be silently redriven, not surfaced: {frame}")
            }
            Some("agent:status-changed") if event["data"]["status"] == "error" => {
                panic!("silent redrive must not park the agent in error: {frame}")
            }
            Some("chat:stream:delta") => {
                if event["data"]["content"]
                    .as_str()
                    .unwrap_or_default()
                    .contains("silent-redrive-response-764")
                {
                    saw_chunk = true;
                }
            }
            Some("agent:stream:end") => {
                assert!(
                    event["data"].get("stopReason").is_none(),
                    "silently redriven turn ends with a normal stream:end: {frame}"
                );
                saw_end = true;
                break;
            }
            _ => {}
        }
    }
    assert!(
        saw_chunk,
        "redriven turn streamed the fresh child's response"
    );
    assert!(saw_end, "redriven turn ended with agent:stream:end");

    // The failure actually happened and took the redrive path (this is not a
    // plain healthy turn): the one-shot WARN is in the daemon log…
    await_daemon_log_contains(&data_dir, "redriving the prompt once on a fresh child").await;
    // …and the dead first child was replaced by a fresh process.
    let pids = await_pid_lines(&pid_file, 2).await;
    assert_eq!(pids.len(), 2, "exactly two spawns (one redrive): {pids:?}");
    assert_ne!(pids[0], pids[1], "redrive used a fresh process: {pids:?}");

    // No Retry surface: nothing was requeued for `agent.retry`.
    let queue = wss_rpc(
        &mut rpc,
        12,
        "agent.getQueue",
        json!({ "workspaceId": ws_id, "agentId": agent_id }),
    )
    .await;
    assert_eq!(
        queue["queue"].as_array().expect("queue array").len(),
        0,
        "silent redrive leaves nothing requeued: {queue}"
    );

    // Status settled to idle with no stopReason (no Error status ever
    // persisted — the event loop above panics on a status=error event).
    let session = await_session_idle(&mut rpc, 100, &ws_id, &agent_id).await;
    assert!(
        session["session"]["stopReason"].is_null(),
        "no stopReason after silent redrive: {:?}",
        session["session"]["stopReason"]
    );

    // The redrive must not duplicate the user message in the transcript.
    let convo = wss_rpc(
        &mut rpc,
        300,
        "agent.getConversation",
        json!({ "workspaceId": ws_id, "agentId": agent_id }),
    )
    .await;
    let user_rows = convo["messages"]
        .as_array()
        .expect("conversation messages array")
        .iter()
        .filter(|m| m["role"] == "user")
        .count();
    assert_eq!(user_rows, 1, "exactly one user row (no duplicate): {convo}");
    assert!(
        convo.to_string().contains("silent-redrive-response-764"),
        "redriven assistant response persisted: {convo}"
    );
}
