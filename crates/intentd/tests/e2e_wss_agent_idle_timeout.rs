//! WSS end-to-end prompt-idle-timeout warn-and-continue: an agent whose turn
//! goes the whole idle window silent (`INTENTD_PROMPT_IDLE_TIMEOUT_MS`) is NOT
//! failed — the daemon settles the hung prompt (`session/cancel`, keep-alive),
//! injects a user-visible `[SYSTEM WARNING]` message, and redrives the turn on
//! the SAME child. Covers the three observable behaviors over the real wire:
//!
//! 1. warn-and-continue on a standalone agent (no `agent:failed`, normal
//!    `agent:stream:end`, warning row, keep-alive recovery turn);
//! 2. a delegated child's timeout does NOT consume the parent's completion
//!    watch — the parent wakes exactly once, on real completion;
//! 3. the consecutive-timeout cap (3 warnings) — the 4th back-to-back silent
//!    timeout takes the terminal path (`agent:failed`, status=error, requeue).
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
    let dir = PathBuf::from("/tmp").join(format!("itd-wss-idleto-{}", &id[..8]));
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
        title: "WSS-IDLETO-E2E".to_string(),
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

/// Bounded poll: wait until `agent.getSession` reports `status == "idle"`,
/// returning the final response for follow-up assertions. The `agent:idle`
/// event is emitted by the turn worker BEFORE `end_turn` persists the
/// runtime-idle status, so a single read right after the event can race the
/// write (monorepo#1164).
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
        tokio::time::sleep(Duration::from_millis(50)).await;
    }
    panic!("agent session never settled to idle; last: {last}");
}

/// Bounded poll: wait until `agent.getQueue` reports exactly `expected` queued
/// messages, returning the final response for follow-up assertions. The
/// terminal-failure requeue (`persist_error_and_requeue`) lands AFTER the
/// `agent:failed` / `agent:status-changed(error)` events are published, so a
/// single read right after those events can race the write (monorepo#1164).
async fn await_queue_len<S>(
    ws: &mut WebSocketStream<S>,
    id_base: i64,
    ws_id: &str,
    agent_id: &str,
    expected: usize,
) -> Value
where
    S: tokio::io::AsyncRead + tokio::io::AsyncWrite + Unpin,
{
    let mut last = Value::Null;
    for i in 0..100 {
        last = wss_rpc(
            ws,
            id_base + i,
            "agent.getQueue",
            json!({ "workspaceId": ws_id, "agentId": agent_id }),
        )
        .await;
        if last["queue"].as_array().map(Vec::len) == Some(expected) {
            return last;
        }
        tokio::time::sleep(Duration::from_millis(50)).await;
    }
    panic!("agent queue never reached {expected} entries; last: {last}");
}

/// Serialize the messages of an `agent.getConversation` result into
/// per-message `(role, contentBlocks-as-string)` pairs for text assertions.
fn conversation_texts(conv: &Value) -> Vec<(String, String)> {
    conv["messages"]
        .as_array()
        .expect("messages array")
        .iter()
        .map(|m| {
            (
                m["role"].as_str().unwrap_or_default().to_string(),
                serde_json::to_string(&m["contentBlocks"]).unwrap_or_default(),
            )
        })
        .collect()
}

/// Warn-and-continue over WSS: a turn that goes the whole idle window silent
/// is settled (`session/cancel`, keep-alive), a `[SYSTEM WARNING]` user row is
/// injected, and the turn is redriven on the SAME child process. Asserts the
/// full wire sequence:
/// - the timed-out turn closes with a NORMAL `agent:stream:end` (no
///   `messageId` — zero output) and NO `agent:failed` / `agent:idle`;
/// - an `agent:message` (role=user) lands AFTER that stream:end — the
///   persisted warning row, `[SYSTEM WARNING] … (1.5s of silence) …` with the
///   ACTUAL configured window (sub-second precision preserved);
/// - the recovery turn streams and completes normally (`agent:stream:activity`,
///   `agent:stream:end` with `messageId`, trailing `agent:idle`);
/// - the assistant reply carries `turn=2` — the mock's per-process turn
///   counter — proving the daemon resumed the SAME child (keep-alive), not a
///   respawn (which would report `turn=1`);
/// - session status is `idle` with a null `stopReason` and the queue is empty
///   (nothing was requeued — the timeout was not treated as terminal).
#[tokio::test]
async fn idle_timeout_warns_and_continues_on_same_child_over_wss() {
    let Some(script) = gate("WSS idle-timeout warn-and-continue E2E") else {
        return;
    };

    let data_dir = temp_data_dir();
    let ws_id = seed_workspace_only(&data_dir).await;
    let behavior = json!({
        "silentUntilCancelTurns": 1,
        "response": "recovered after idle warning",
    })
    .to_string();
    let env: [(&str, &str); 5] = [
        ("INTENTD_AUTH_TOKEN", TOKEN),
        ("INTENTD_TCP_PORT", "0"),
        ("MOCK_AGENT_SCRIPT_PATH", &script),
        ("MOCK_AGENT_BEHAVIOR", &behavior),
        ("INTENTD_PROMPT_IDLE_TIMEOUT_MS", "1500"),
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

    // SUBSCRIBER conn — subscribe BEFORE the turn so we miss no events.
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
        json!({ "workspaceId": ws_id, "name": "IdleTO", "model": "mock:default" }),
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
        json!({ "workspaceId": ws_id, "agentId": agent_id, "content": "please do the silent work" }),
    )
    .await;
    assert_eq!(sent["success"], true, "sendMessage ok: {sent}");

    // Event sequence: stream:end #1 (the timed-out turn, no messageId) →
    // agent:message role=user (the warning row) → stream:activity + stream:end #2
    // (with messageId) → agent:idle. NEVER an agent:failed, and NEVER an
    // agent:idle before the recovery turn's stream:end.
    let mut ends = 0u32;
    let mut user_messages_after_first_end = 0u32;
    let mut recovery_chunks = 0u32;
    let mut saw_idle = false;
    for _ in 0..300 {
        let frame = wss_event(&mut sub, 30).await;
        let ev = &frame["params"]["event"];
        if ev["data"]["agentId"].as_str() != Some(agent_id.as_str()) {
            continue;
        }
        match ev["type"].as_str() {
            Some("agent:failed") => {
                panic!("idle timeout must NOT emit agent:failed: {ev}");
            }
            Some("agent:stream:end") => {
                ends += 1;
                if ends == 1 {
                    // The timed-out turn produced zero output — its terminal
                    // stream:end carries no messageId (no row persisted).
                    assert!(
                        ev["data"].get("messageId").is_none(),
                        "timed-out turn's stream:end has no messageId: {ev}"
                    );
                } else {
                    assert!(
                        ev["data"]["messageId"].is_string(),
                        "recovery turn's stream:end names the persisted row: {ev}"
                    );
                }
            }
            Some("agent:message") if ev["data"]["role"] == "user" && ends >= 1 => {
                // The injected warning row lands AFTER the timed-out turn's
                // stream:end and BEFORE the recovery turn completes.
                assert_eq!(ends, 1, "warning row precedes the recovery turn: {ev}");
                user_messages_after_first_end += 1;
            }
            Some("agent:stream:activity") if ends >= 1 => recovery_chunks += 1,
            Some("agent:idle") => {
                assert_eq!(
                    ends, 2,
                    "agent:idle only after the recovery turn's stream:end: {ev}"
                );
                saw_idle = true;
            }
            _ => {}
        }
        if saw_idle {
            break;
        }
    }
    assert_eq!(
        ends, 2,
        "two stream:ends: the timed-out turn + the recovery"
    );
    assert_eq!(
        user_messages_after_first_end, 1,
        "exactly one injected warning row between the turns"
    );
    assert!(recovery_chunks >= 1, "recovery turn streamed ≥1 chunk");
    assert!(saw_idle, "agent went idle after the recovery turn");

    // Transcript: original user row, `[SYSTEM WARNING]` user row rendering the
    // ACTUAL configured window (1500ms → 1s), and the recovery assistant row
    // stamped `turn=2` — the same-child keep-alive proof.
    let conv = wss_rpc(
        &mut rpc,
        12,
        "agent.getConversation",
        json!({ "workspaceId": ws_id, "agentId": agent_id }),
    )
    .await;
    let texts = conversation_texts(&conv);
    let warnings: Vec<&(String, String)> = texts
        .iter()
        .filter(|(_, t)| t.contains("[SYSTEM WARNING]"))
        .collect();
    assert_eq!(warnings.len(), 1, "exactly one warning row: {conv}");
    assert_eq!(warnings[0].0, "user", "warning row is user-role: {conv}");
    assert!(
        warnings[0]
            .1
            .contains("exceeded the inactivity timeout (1.5s of silence)"),
        "warning renders the configured window: {}",
        warnings[0].1
    );
    assert!(
        texts
            .iter()
            .any(|(role, t)| role == "assistant"
                && t.contains("recovered after idle warning turn=2")),
        "recovery reply came from the SAME child (turn=2): {conv}"
    );

    // The session settled idle with no stop reason, and nothing was requeued.
    // Poll for idle: `agent:idle` is emitted BEFORE `end_turn` persists the
    // runtime-idle status, so a single read here can race the write
    // (monorepo#1164).
    let session = await_session_idle(&mut rpc, 100, &ws_id, &agent_id).await;
    assert!(
        session["session"]["stopReason"].is_null(),
        "no stopReason after warn-and-continue: {session}"
    );
    let queue = wss_rpc(
        &mut rpc,
        14,
        "agent.getQueue",
        json!({ "workspaceId": ws_id, "agentId": agent_id }),
    )
    .await;
    assert_eq!(
        queue["queue"].as_array().map(Vec::len),
        Some(0),
        "queue empty — the timeout was not terminal: {queue}"
    );
}

/// Regression for monorepo#1599 — the timed-out turn's tail must not bleed
/// into the warning turn. The mock parks the first turn silent until the
/// daemon's idle-timeout `session/cancel`, then (via `tailAfterCancel`)
/// streams a trailing `agent_message_chunk` carrying `TAIL-AFTER-CANCEL`
/// before resolving the cancelled prompt — modelling a child that emits late
/// `session/update`s for the cancelled turn past the cancel. Today those
/// stragglers sit in the notifications channel and are consumed by the
/// injected `[SYSTEM WARNING]` turn, so the marker leaks into the warning
/// turn's stream and assistant message. Asserts:
/// - no `chat:stream:delta` AFTER the timed-out turn's stream:end (i.e.
///   attributed to the warning turn) carries the marker — a pre-warning-turn
///   drain re-emitting the straggler with the timed-out turn would land
///   BEFORE that stream:end and stays allowed;
/// - no assistant message in the transcript carries the marker;
/// - the warning turn otherwise completes normally on the SAME child
///   (`turn=2`, one warning row, no `agent:failed`).
#[tokio::test]
async fn idle_timeout_tail_does_not_bleed_into_warning_turn_over_wss() {
    let Some(script) = gate("WSS idle-timeout tail-bleed regression E2E") else {
        return;
    };

    const TAIL_MARKER: &str = "TAIL-AFTER-CANCEL";
    let data_dir = temp_data_dir();
    let ws_id = seed_workspace_only(&data_dir).await;
    let behavior = json!({
        "silentUntilCancelTurns": 1,
        "tailAfterCancel": TAIL_MARKER,
        "response": "recovered after idle warning",
    })
    .to_string();
    let env: [(&str, &str); 5] = [
        ("INTENTD_AUTH_TOKEN", TOKEN),
        ("INTENTD_TCP_PORT", "0"),
        ("MOCK_AGENT_SCRIPT_PATH", &script),
        ("MOCK_AGENT_BEHAVIOR", &behavior),
        ("INTENTD_PROMPT_IDLE_TIMEOUT_MS", "1500"),
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

    // SUBSCRIBER conn — `chat:stream:delta` is outside `agent:*`, so it is
    // subscribed explicitly; subscribe BEFORE the turn so we miss no events.
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
        json!({ "workspaceId": ws_id, "name": "IdleTail", "model": "mock:default" }),
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
        json!({ "workspaceId": ws_id, "agentId": agent_id, "content": "please do the silent work" }),
    )
    .await;
    assert_eq!(sent["success"], true, "sendMessage ok: {sent}");

    // Track the two stream:ends (timed-out turn + warning turn) and collect
    // every delta published AFTER the first end — those belong to the warning
    // turn and must never carry the timed-out turn's tail marker.
    let mut ends = 0u32;
    let mut saw_idle = false;
    let mut tainted_deltas: Vec<String> = Vec::new();
    for _ in 0..300 {
        let frame = wss_event(&mut sub, 30).await;
        let ev = &frame["params"]["event"];
        if ev["data"]["agentId"].as_str() != Some(agent_id.as_str()) {
            continue;
        }
        match ev["type"].as_str() {
            Some("agent:failed") => {
                panic!("idle timeout must NOT emit agent:failed: {ev}");
            }
            Some("agent:stream:end") => ends += 1,
            Some("chat:stream:delta") if ends >= 1 => {
                let data = serde_json::to_string(&ev["data"]).unwrap_or_default();
                if data.contains(TAIL_MARKER) {
                    tainted_deltas.push(data);
                }
            }
            Some("agent:idle") => saw_idle = true,
            _ => {}
        }
        if saw_idle && ends >= 2 {
            break;
        }
    }
    assert_eq!(
        ends, 2,
        "two stream:ends: the timed-out turn + the warning turn"
    );
    assert!(saw_idle, "agent went idle after the warning turn");
    assert!(
        tainted_deltas.is_empty(),
        "the timed-out turn's tail streamed into the warning turn (monorepo#1599): {tainted_deltas:?}"
    );

    // Transcript: the warning turn completed normally on the SAME child
    // (turn=2), the warning row is there — and NO assistant message carries
    // the tail marker.
    let conv = wss_rpc(
        &mut rpc,
        12,
        "agent.getConversation",
        json!({ "workspaceId": ws_id, "agentId": agent_id }),
    )
    .await;
    let texts = conversation_texts(&conv);
    assert_eq!(
        texts
            .iter()
            .filter(|(_, t)| t.contains("[SYSTEM WARNING]"))
            .count(),
        1,
        "exactly one warning row: {conv}"
    );
    assert!(
        texts
            .iter()
            .any(|(role, t)| role == "assistant"
                && t.contains("recovered after idle warning turn=2")),
        "warning turn completed on the SAME child (turn=2): {conv}"
    );
    let tainted_messages: Vec<&(String, String)> = texts
        .iter()
        .filter(|(role, t)| role == "assistant" && t.contains(TAIL_MARKER))
        .collect();
    assert!(
        tainted_messages.is_empty(),
        "the timed-out turn's tail bled into an assistant message (monorepo#1599): {tainted_messages:?}"
    );
}

/// Timeout→teardown half of the monorepo#1599 drain (PR review follow-up):
/// when the cancelled prompt's RESPONSE never lands within the watermark
/// window, the child may keep streaming stragglers indefinitely — so the
/// daemon must tear it down (fresh child, fresh notifications channel) rather
/// than warn-and-proceed into a shared channel. The mock parks the first turn
/// via `parkIfPromptEndsWith` (suffix-matched so the fresh child's replayed
/// history does not re-trigger the park), streams a `TAIL-AFTER-CANCEL`
/// straggler on `session/cancel`, and (via `neverResolveOnCancel`) never
/// resolves the cancelled prompt. Asserts:
/// - the warning turn still completes normally (no `agent:failed`, exactly
///   one warning row, the recovery response lands);
/// - no post-teardown delta or assistant message carries the tail marker;
/// - the prompt log proves the warning turn ran on a FRESH child (its
///   `session/prompt` is that process's turn 1, not the old child's turn 2).
#[tokio::test]
async fn idle_timeout_unresolved_cancel_tears_down_child_over_wss() {
    let Some(script) = gate("WSS idle-timeout unresolved-cancel teardown E2E") else {
        return;
    };

    const TAIL_MARKER: &str = "TAIL-AFTER-CANCEL";
    const PARK_MARKER: &str = "PARK-FIRST-TURN-1599";
    let data_dir = temp_data_dir();
    let ws_id = seed_workspace_only(&data_dir).await;
    let prompt_log = data_dir.join("prompt-log.jsonl");
    let prompt_log_str = prompt_log.to_string_lossy().to_string();
    let behavior = json!({
        "parkIfPromptEndsWith": PARK_MARKER,
        "tailAfterCancel": TAIL_MARKER,
        "neverResolveOnCancel": true,
        "response": "recovered after teardown",
    })
    .to_string();
    let env: [(&str, &str); 6] = [
        ("INTENTD_AUTH_TOKEN", TOKEN),
        ("INTENTD_TCP_PORT", "0"),
        ("MOCK_AGENT_SCRIPT_PATH", &script),
        ("MOCK_AGENT_BEHAVIOR", &behavior),
        ("MOCK_AGENT_PROMPT_LOG", &prompt_log_str),
        ("INTENTD_PROMPT_IDLE_TIMEOUT_MS", "1500"),
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
        json!({ "workspaceId": ws_id, "name": "IdleTear", "model": "mock:default" }),
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
        json!({ "workspaceId": ws_id, "agentId": agent_id, "content": format!("please do the parked work {PARK_MARKER}") }),
    )
    .await;
    assert_eq!(sent["success"], true, "sendMessage ok: {sent}");

    let mut ends = 0u32;
    let mut saw_idle = false;
    let mut tainted_deltas: Vec<String> = Vec::new();
    for _ in 0..300 {
        let frame = wss_event(&mut sub, 30).await;
        let ev = &frame["params"]["event"];
        if ev["data"]["agentId"].as_str() != Some(agent_id.as_str()) {
            continue;
        }
        match ev["type"].as_str() {
            Some("agent:failed") => {
                panic!("idle timeout must NOT emit agent:failed: {ev}");
            }
            Some("agent:stream:end") => ends += 1,
            Some("chat:stream:delta") if ends >= 1 => {
                let data = serde_json::to_string(&ev["data"]).unwrap_or_default();
                if data.contains(TAIL_MARKER) {
                    tainted_deltas.push(data);
                }
            }
            Some("agent:idle") => saw_idle = true,
            _ => {}
        }
        if saw_idle && ends >= 2 {
            break;
        }
    }
    assert_eq!(
        ends, 2,
        "two stream:ends: the timed-out turn + the warning turn"
    );
    assert!(saw_idle, "agent went idle after the warning turn");
    assert!(
        tainted_deltas.is_empty(),
        "the torn-down child's tail streamed into the warning turn (monorepo#1599): {tainted_deltas:?}"
    );

    // Transcript: the warning row is there, the recovery response landed, and
    // NO assistant message carries the tail marker.
    let conv = wss_rpc(
        &mut rpc,
        12,
        "agent.getConversation",
        json!({ "workspaceId": ws_id, "agentId": agent_id }),
    )
    .await;
    let texts = conversation_texts(&conv);
    assert_eq!(
        texts
            .iter()
            .filter(|(_, t)| t.contains("[SYSTEM WARNING]"))
            .count(),
        1,
        "exactly one warning row: {conv}"
    );
    assert!(
        texts
            .iter()
            .any(|(role, t)| role == "assistant" && t.contains("recovered after teardown")),
        "warning turn completed after the teardown: {conv}"
    );
    let tainted_messages: Vec<&(String, String)> = texts
        .iter()
        .filter(|(role, t)| role == "assistant" && t.contains(TAIL_MARKER))
        .collect();
    assert!(
        tainted_messages.is_empty(),
        "the torn-down child's tail bled into an assistant message (monorepo#1599): {tainted_messages:?}"
    );

    // Prompt log: one JSON line per `session/prompt`, `turn` = the receiving
    // PROCESS's per-spawn prompt counter. The warning prompt on `turn: 1`
    // proves the daemon spawned a FRESH child (the kept-alive old child would
    // have received it as its turn 2).
    let log = std::fs::read_to_string(&prompt_log).expect("prompt log");
    let entries: Vec<Value> = log
        .lines()
        .map(|l| serde_json::from_str(l).expect("prompt log line"))
        .collect();
    assert_eq!(entries.len(), 2, "two prompts total: {entries:?}");
    assert!(
        entries[0]["text"]
            .as_str()
            .is_some_and(|t| t.contains(PARK_MARKER)),
        "first prompt is the parked turn: {entries:?}"
    );
    assert_eq!(
        entries[1]["turn"], 1,
        "warning prompt is the FRESH child's turn 1 (teardown happened): {entries:?}"
    );
    assert!(
        entries[1]["text"]
            .as_str()
            .is_some_and(|t| t.contains("[SYSTEM WARNING]")),
        "second prompt is the warning turn: {entries:?}"
    );
}

/// A delegated child's idle timeout must NOT consume the parent's completion
/// watch: the warn-and-continue redrive keeps the child's turn alive, so the
/// parent wakes exactly ONCE — on the child's real completion (its terminal
/// `agent:idle` AFTER the recovery turn), never on the timed-out turn's
/// stream:end or the warning injection. Asserts over the wire:
/// - the child times out silently (no `agent:failed`), gets the warning, and
///   completes its recovery turn;
/// - the parent receives NO wake between the child's timed-out stream:end and
///   the child's recovery stream:end (its waiting flags stay set);
/// - the parent transcript carries EXACTLY ONE `[WORKSPACE EVENTS]` wake and
///   ZERO `[SYSTEM WARNING]` rows (the warning went to the child, not the
///   parent).
#[tokio::test]
async fn delegated_child_idle_timeout_does_not_wake_parent_over_wss() {
    let Some(script) = gate("WSS delegated-child idle-timeout E2E") else {
        return;
    };

    let data_dir = temp_data_dir();
    let ws_id = seed_workspace_only(&data_dir).await;
    const PARENT_GO: &str = "IDLETO_PARENT_GO";
    const CHILD_MARKER: &str = "IDLETO_CHILD_SILENT";
    let delegate_js = format!(
        "return await ws.agent.delegate({{ agentInstructions: {}, waitMode: 'immediate', model: 'mock:default' }});",
        json!(CHILD_MARKER),
    );
    // One behavior drives all three roles: the parent's opening turn (matched
    // on PARENT_GO) delegates the child; the child's first prompt carries
    // CHILD_MARKER → `parkIfPromptContains` parks it SILENTLY until the
    // daemon's idle-timeout `session/cancel`; the child's warning turn (no
    // marker — matched on the [SYSTEM WARNING] framing) responds; the
    // parent's wake turn (matched on [WORKSPACE EVENTS]) acknowledges.
    let behavior = json!({
        "parkIfPromptContains": CHILD_MARKER,
        "rules": [
            {
                "ifPromptContains": "[WORKSPACE EVENTS]",
                "response": "parent acknowledged the completion wake",
            },
            {
                "ifPromptContains": "[SYSTEM WARNING]",
                "response": "child recovered after idle warning",
            },
            {
                "ifPromptContains": PARENT_GO,
                "toolCall": {
                    "name": "workspace_api",
                    "arguments": { "code": delegate_js, "summary": "delegate silent child" }
                },
                "response": "parent delegated the silent child",
            },
        ],
    })
    .to_string();
    // 8s window: wide enough that the parent's own (tool-calling) turns never
    // have a silent gap that trips it, even on a heavily loaded CI host. The
    // child parks with ZERO activity, so its timeout fires deterministically
    // regardless of the window size — the wait below keys off the child's
    // warning row, not a fixed sleep, so the wider window costs nothing.
    let env: [(&str, &str); 5] = [
        ("INTENTD_AUTH_TOKEN", TOKEN),
        ("INTENTD_TCP_PORT", "0"),
        ("MOCK_AGENT_SCRIPT_PATH", &script),
        ("MOCK_AGENT_BEHAVIOR", &behavior),
        ("INTENTD_PROMPT_IDLE_TIMEOUT_MS", "8000"),
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
    let parent = wss_rpc(
        &mut rpc,
        10,
        "agent.create",
        json!({ "workspaceId": ws_id, "name": "Parent", "model": "mock:default" }),
    )
    .await;
    let parent_id = parent["agent"]["id"]
        .as_str()
        .expect("parent id")
        .to_string();
    let sent = wss_rpc(
        &mut rpc,
        11,
        "agent.sendMessage",
        json!({ "workspaceId": ws_id, "agentId": parent_id, "content": PARENT_GO }),
    )
    .await;
    assert_eq!(sent["success"], true, "sendMessage ok: {sent}");

    // Phase 1 — the parent's delegating turn ends with its first agent:idle
    // (the child is parked silent, so the parent settles first).
    let mut parent_idle = false;
    for _ in 0..200 {
        let frame = wss_event(&mut sub, 60).await;
        let ev = &frame["params"]["event"];
        if ev["type"] == "agent:idle" && ev["data"]["agentId"].as_str() == Some(&parent_id) {
            parent_idle = true;
            break;
        }
    }
    assert!(parent_idle, "parent went idle after delegating");

    // The parent is now waiting on exactly one child — the completion watch
    // this test proves the timeout must NOT consume.
    let lite = wss_rpc(&mut rpc, 12, "agent.get", json!({ "agentId": parent_id })).await;
    let lite = &lite["agent"];
    assert_eq!(lite["isWaitingForOtherAgents"], true, "waiting: {lite}");
    let waiting = lite["waitingForAgentIds"]
        .as_array()
        .expect("waitingForAgentIds");
    assert_eq!(waiting.len(), 1, "one watched child: {lite}");
    let child_id = waiting[0].as_str().expect("child id").to_string();

    // Phase 2 — the child times out, gets warned, recovers, and ONLY THEN the
    // parent wakes. Track the child's stream:ends and the parent's wake row;
    // any parent wake before the child's SECOND stream:end is the regression.
    let mut child_ends = 0u32;
    let mut child_warning_rows = 0u32;
    let mut parent_wakes = 0u32;
    let mut child_idle = false;
    let mut parent_idle_again = false;
    for _ in 0..400 {
        let frame = wss_event(&mut sub, 60).await;
        let ev = &frame["params"]["event"];
        let ev_agent = ev["data"]["agentId"].as_str().unwrap_or_default();
        match ev["type"].as_str() {
            Some("agent:failed") => panic!("no agent:failed anywhere: {ev}"),
            Some("agent:stream:end") if ev_agent == child_id => child_ends += 1,
            // Warning rows only count AFTER the child's first (timed-out)
            // stream:end — the delegated-instructions user row may race past
            // the parent's first idle into this loop.
            Some("agent:message")
                if ev_agent == child_id && ev["data"]["role"] == "user" && child_ends >= 1 =>
            {
                child_warning_rows += 1;
            }
            Some("agent:message") if ev_agent == parent_id && ev["data"]["role"] == "user" => {
                // The parent's wake row: it must arrive only after the child
                // finished its RECOVERY turn (two child stream:ends seen).
                assert_eq!(
                    child_ends, 2,
                    "parent woken only on the child's real completion: {ev}"
                );
                parent_wakes += 1;
            }
            Some("agent:idle") if ev_agent == child_id => child_idle = true,
            Some("agent:idle") if ev_agent == parent_id => {
                parent_idle_again = true;
            }
            _ => {}
        }
        if parent_idle_again && child_idle && parent_wakes >= 1 {
            break;
        }
    }
    assert_eq!(child_ends, 2, "child: timed-out turn + recovery turn");
    assert_eq!(child_warning_rows, 1, "child got exactly one warning row");
    assert_eq!(parent_wakes, 1, "parent woken exactly once");
    assert!(child_idle, "child settled idle after the recovery turn");
    assert!(parent_idle_again, "parent idled again after the wake turn");

    // The watch was consumed by the REAL completion: waiting flags cleared.
    let lite = wss_rpc(&mut rpc, 13, "agent.get", json!({ "agentId": parent_id })).await;
    let lite = &lite["agent"];
    assert_eq!(lite["isWaitingForOtherAgents"], false, "cleared: {lite}");
    assert_eq!(lite["waitingForAgentIds"], json!([]), "cleared: {lite}");

    // Parent transcript: exactly one [WORKSPACE EVENTS] wake, zero [SYSTEM
    // WARNING] rows (the warning belongs to the child), and the wake reports
    // the child COMPLETED (not failed).
    let conv = wss_rpc(
        &mut rpc,
        14,
        "agent.getConversation",
        json!({ "workspaceId": ws_id, "agentId": parent_id }),
    )
    .await;
    let texts = conversation_texts(&conv);
    assert_eq!(
        texts
            .iter()
            .filter(|(_, t)| t.contains("[WORKSPACE EVENTS]"))
            .count(),
        1,
        "exactly one parent wake: {conv}"
    );
    assert!(
        texts.iter().all(|(_, t)| !t.contains("[SYSTEM WARNING]")),
        "no warning row leaked into the parent transcript: {conv}"
    );
    let wake = &texts
        .iter()
        .find(|(_, t)| t.contains("[WORKSPACE EVENTS]"))
        .expect("wake row")
        .1;
    assert!(
        wake.contains("completed"),
        "the wake reports a real completion: {wake}"
    );

    // Child transcript: the delegated instructions, ONE user-role [SYSTEM
    // WARNING] row, and the recovery reply.
    let conv = wss_rpc(
        &mut rpc,
        15,
        "agent.getConversation",
        json!({ "workspaceId": ws_id, "agentId": child_id }),
    )
    .await;
    let texts = conversation_texts(&conv);
    let warnings: Vec<&(String, String)> = texts
        .iter()
        .filter(|(_, t)| t.contains("[SYSTEM WARNING]"))
        .collect();
    assert_eq!(warnings.len(), 1, "one warning row in the child: {conv}");
    assert_eq!(warnings[0].0, "user", "warning row is user-role");
    assert!(
        texts
            .iter()
            .any(|(role, t)| role == "assistant" && t.contains("child recovered")),
        "child's recovery reply persisted: {conv}"
    );
}

/// Consecutive-timeout cap over WSS: 3 back-to-back silent timeouts each get
/// a warning redrive; the 4th is terminal. The mock parks the first FOUR
/// turns (the original + all three warning turns) so every redrive times out
/// again. Asserts:
/// - exactly THREE `[SYSTEM WARNING]` user rows in the transcript;
/// - the terminal failure emits `agent:failed` whose error names the idle
///   timeout, plus `agent:status-changed(status=error)`;
/// - `agent.getSession` persists `status=error` with the idle-timeout
///   `stopReason`;
/// - the failing message (the LAST warning) was requeued for `agent.retry` —
///   not silently dropped;
/// - the agent NEVER goes idle (no turn ever completed).
#[tokio::test]
async fn idle_timeout_cap_fails_terminally_over_wss() {
    let Some(script) = gate("WSS idle-timeout cap-terminal E2E") else {
        return;
    };

    let data_dir = temp_data_dir();
    let ws_id = seed_workspace_only(&data_dir).await;
    let behavior = json!({
        "silentUntilCancelTurns": 4,
        "response": "never reached",
    })
    .to_string();
    let env: [(&str, &str); 5] = [
        ("INTENTD_AUTH_TOKEN", TOKEN),
        ("INTENTD_TCP_PORT", "0"),
        ("MOCK_AGENT_SCRIPT_PATH", &script),
        ("MOCK_AGENT_BEHAVIOR", &behavior),
        ("INTENTD_PROMPT_IDLE_TIMEOUT_MS", "1500"),
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
        json!({ "workspaceId": ws_id, "name": "IdleCap", "model": "mock:default" }),
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
        json!({ "workspaceId": ws_id, "agentId": agent_id, "content": "stays silent forever" }),
    )
    .await;
    assert_eq!(sent["success"], true, "sendMessage ok: {sent}");

    // Event sequence: 4 timed-out stream:ends interleaved with 3 warning
    // rows, then agent:failed + agent:status-changed(error). Never agent:idle.
    let mut ends = 0u32;
    let mut warning_rows = 0u32;
    let mut saw_failed = false;
    let mut saw_status_error = false;
    for _ in 0..400 {
        let frame = wss_event(&mut sub, 30).await;
        let ev = &frame["params"]["event"];
        if ev["data"]["agentId"].as_str() != Some(agent_id.as_str()) {
            continue;
        }
        match ev["type"].as_str() {
            Some("agent:idle") => panic!("no turn ever completed — agent:idle is wrong: {ev}"),
            Some("agent:stream:end") => ends += 1,
            Some("agent:message") if ev["data"]["role"] == "user" && ends >= 1 => {
                warning_rows += 1;
            }
            Some("agent:failed") => {
                let err = ev["data"]["error"].as_str().unwrap_or_default();
                assert!(
                    err.contains("session/prompt idle timeout"),
                    "agent:failed names the idle timeout: {err}"
                );
                // The terminal failure fires only after the cap: all three
                // warnings (and all four timed-out turns) came first.
                assert_eq!(ends, 4, "terminal failure after the 4th timeout: {ev}");
                assert_eq!(warning_rows, 3, "three warnings before terminal: {ev}");
                saw_failed = true;
            }
            Some("agent:status-changed") if ev["data"]["status"] == "error" => {
                saw_status_error = true;
            }
            _ => {}
        }
        if saw_failed && saw_status_error {
            break;
        }
    }
    assert!(saw_failed, "terminal agent:failed emitted after the cap");
    assert!(
        saw_status_error,
        "agent:status-changed(status=error) emitted after the cap"
    );

    // Transcript: the original user row + exactly THREE [SYSTEM WARNING]
    // user rows (the cap), and no assistant reply (every turn was silent).
    let conv = wss_rpc(
        &mut rpc,
        12,
        "agent.getConversation",
        json!({ "workspaceId": ws_id, "agentId": agent_id }),
    )
    .await;
    let texts = conversation_texts(&conv);
    let warnings: Vec<&(String, String)> = texts
        .iter()
        .filter(|(_, t)| t.contains("[SYSTEM WARNING]"))
        .collect();
    assert_eq!(warnings.len(), 3, "exactly three warning rows: {conv}");
    assert!(
        warnings.iter().all(|(role, _)| role == "user"),
        "warning rows are user-role: {conv}"
    );

    // Persisted terminal state: status=error with the idle-timeout stopReason.
    // A single read is safe here: the status + stopReason store write
    // completes BEFORE `agent:status-changed(error)` is published
    // (durable-before-observable), and the loop above waited for that event.
    let session = wss_rpc(
        &mut rpc,
        13,
        "agent.getSession",
        json!({ "workspaceId": ws_id, "agentId": agent_id }),
    )
    .await;
    assert_eq!(
        session["session"]["status"], "error",
        "session error after the cap: {session}"
    );
    let stop_reason = session["session"]["stopReason"]
        .as_str()
        .expect("stopReason present after terminal idle timeout");
    assert!(
        stop_reason.contains("session/prompt idle timeout"),
        "stopReason names the idle timeout: {stop_reason}"
    );

    // The failing message — the LAST warning turn's content — was requeued
    // for agent.retry, not silently dropped. Poll: the requeue lands AFTER
    // the `agent:status-changed(error)` publish the loop above waited on, so
    // a single read here can race the write (monorepo#1164).
    let queue = await_queue_len(&mut rpc, 100, &ws_id, &agent_id, 1).await;
    let messages = queue["queue"].as_array().expect("queue array");
    assert!(
        messages[0]["content"]
            .as_str()
            .unwrap_or_default()
            .starts_with("[SYSTEM WARNING]"),
        "the requeued message is the failing warning turn: {queue}"
    );
}
