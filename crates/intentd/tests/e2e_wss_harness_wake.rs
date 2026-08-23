//! WSS end-to-end coverage for implicit harness-wake turns (monorepo#855):
//! an unsolicited `session/update` burst from the ACP child — no
//! `session/prompt` in flight — must stream over the wire as its own
//! agent-initiated turn: `agent:stream:start` `{ agentId, messageId,
//! reason: "harness-wake" }`, the burst's chunks under that messageId,
//! exactly one terminal `agent:stream:end` carrying the persisted
//! messageId, then `agent:idle` `{ reason: "harness_wake_complete" }` —
//! and the assistant row must land in `agent.getConversation`.
//!
//! Also proves the single-flight race contract: a user `agent.sendMessage`
//! racing in mid-wake queues (never interleaves), preempts the settle
//! window, suppresses the wake idle, and drains as its own prompt turn
//! AFTER the wake turn's terminal `stream:end`.
//!
//! The mock fixture's `MOCK_AGENT_WAKE_TRIGGER_FILE` hook makes the burst
//! deterministic: the test creates the trigger file when it wants the
//! child to emit the out-of-turn updates. Gated on `node` + the mock
//! script; skips cleanly otherwise.

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

const TOKEN: &str = "abababababababababababababababababababababababababababababababab";

/// The three unsolicited chunk texts the mock emits on wake — one
/// `session/update` `agent_message_chunk` per trigger-file line.
const WAKE_LINES: [&str; 3] = [
    "[compaction] context window compacted. ",
    "Background task finished: ",
    "3 files changed.",
];

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
        title: "WSS-WAKE-E2E".to_string(),
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

/// Boot the daemon with the wake-trigger mock, create an agent, and drive one
/// normal prompt turn to completion (so the child is spawned, the session is
/// open, and the wake listener is armed against a genuinely idle agent).
/// Returns everything the test needs to trigger and observe the wake burst.
struct WakeSetup {
    _daemon: Daemon,
    ws_id: String,
    agent_id: String,
    trigger_file: PathBuf,
    sub: WebSocketStream<tokio_rustls::client::TlsStream<TcpStream>>,
    rpc: WebSocketStream<tokio_rustls::client::TlsStream<TcpStream>>,
}

async fn wake_setup(script: &str, behavior: &str) -> WakeSetup {
    let data_dir = temp_data_dir();
    let ws_id = seed_workspace_only(&data_dir).await;
    let trigger_file = data_dir.join("wake-trigger.txt");
    let trigger_file_s = trigger_file.to_string_lossy().into_owned();
    let env: [(&str, &str); 5] = [
        ("INTENTD_AUTH_TOKEN", TOKEN),
        ("INTENTD_TCP_PORT", "0"),
        ("MOCK_AGENT_SCRIPT_PATH", script),
        ("MOCK_AGENT_BEHAVIOR", behavior),
        ("MOCK_AGENT_WAKE_TRIGGER_FILE", &trigger_file_s),
    ];
    let child = spawn_serve(&data_dir, &env);
    let daemon = Daemon {
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
        json!({ "workspaceId": ws_id, "name": "WSS-WAKE", "model": "mock:default" }),
    )
    .await;
    let agent_id = created["agent"]["id"]
        .as_str()
        .expect("agent id")
        .to_string();

    // Warm-up prompt turn: spawns the child + opens the session; its terminal
    // stream:end + idle prove the agent is quiescent before the wake trigger.
    let sent = wss_rpc(
        &mut rpc,
        11,
        "agent.sendMessage",
        json!({ "workspaceId": ws_id, "agentId": agent_id, "content": "warmup prompt" }),
    )
    .await;
    assert_eq!(sent["success"], true, "warmup sendMessage ok: {sent}");
    let mut saw_end = false;
    let mut saw_idle = false;
    for _ in 0..200 {
        let frame = wss_event(&mut sub, 30).await;
        let event = &frame["params"]["event"];
        if event["data"]["agentId"].as_str() != Some(agent_id.as_str()) {
            continue;
        }
        match event["type"].as_str() {
            Some("agent:stream:end") => saw_end = true,
            Some("agent:idle") => saw_idle = true,
            Some("agent:failed") => panic!("warmup turn failed: {frame}"),
            _ => {}
        }
        if saw_end && saw_idle {
            break;
        }
    }
    assert!(saw_end && saw_idle, "warmup turn reached idle");

    WakeSetup {
        _daemon: daemon,
        ws_id,
        agent_id,
        trigger_file,
        sub,
        rpc,
    }
}

/// WAKE-1 (monorepo#855): an unsolicited `session/update` burst with no
/// prompt in flight streams as an implicit agent-initiated turn over WSS —
/// `agent:stream:start` `{ agentId, messageId, reason: "harness-wake" }`,
/// the burst's chunks under that messageId (never before the start),
/// exactly one terminal `agent:stream:end` carrying the persisted
/// messageId, then `agent:idle` `{ reason: "harness_wake_complete" }` —
/// and the coalesced assistant row lands in `agent.getConversation`.
#[tokio::test]
async fn harness_wake_burst_streams_as_agent_initiated_turn_over_wss() {
    let Some(script) = gate("WSS harness-wake E2E") else {
        return;
    };
    let behavior = json!({ "response": "warmup done" }).to_string();
    let mut setup = wake_setup(&script, &behavior).await;
    let agent_id = setup.agent_id.clone();

    // Fire the burst: the mock emits one out-of-turn agent_message_chunk per
    // trigger-file line, OUTSIDE any session/prompt.
    std::fs::write(&setup.trigger_file, WAKE_LINES.join("\n")).expect("write wake trigger");

    let mut wake_message_id: Option<String> = None;
    let mut chunk_text = String::new();
    let mut end_count = 0usize;
    let mut end_message_id: Option<String> = None;
    let mut saw_idle = false;
    for _ in 0..300 {
        let frame = wss_event(&mut setup.sub, 30).await;
        let event = &frame["params"]["event"];
        if event["data"]["agentId"].as_str() != Some(agent_id.as_str()) {
            continue;
        }
        match event["type"].as_str() {
            Some("agent:stream:start") => {
                assert_eq!(
                    event["data"]["reason"], "harness-wake",
                    "wake turn opens with reason harness-wake: {frame}"
                );
                let mid = event["data"]["messageId"]
                    .as_str()
                    .expect("stream:start carries messageId")
                    .to_string();
                assert!(!mid.is_empty(), "wake messageId is non-empty");
                wake_message_id = Some(mid);
            }
            Some("chat:stream:delta") => {
                // Ordering: no wake content may hit the wire before the
                // stream:start that names the turn.
                let mid = wake_message_id
                    .as_deref()
                    .expect("chunk arrived before agent:stream:start");
                assert_eq!(
                    event["data"]["messageId"].as_str(),
                    Some(mid),
                    "wake chunks ride the wake turn's messageId: {frame}"
                );
                chunk_text.push_str(&serde_json::to_string(&event["data"]).unwrap_or_default());
            }
            Some("agent:stream:end") => {
                end_count += 1;
                end_message_id = event["data"]["messageId"].as_str().map(String::from);
            }
            Some("agent:idle") => {
                assert_eq!(
                    end_count, 1,
                    "agent:idle follows exactly one terminal stream:end"
                );
                assert_eq!(
                    event["data"]["reason"], "harness_wake_complete",
                    "wake idle carries reason harness_wake_complete: {frame}"
                );
                assert_eq!(event["data"]["status"], "idle", "wake idle status: {frame}");
                assert_eq!(
                    event["data"]["isWaitingForOtherAgents"],
                    json!(false),
                    "wake idle carries the emit-time waiting flag (no pending watches): {frame}"
                );
                saw_idle = true;
            }
            Some("agent:failed") => panic!("agent:failed during wake turn: {frame}"),
            _ => {}
        }
        if saw_idle {
            break;
        }
    }
    let wake_message_id = wake_message_id.expect("agent:stream:start (harness-wake) observed");
    for marker in WAKE_LINES {
        assert!(
            chunk_text.contains(marker),
            "wake chunk stream carries {marker:?}, got: {chunk_text}"
        );
    }
    assert_eq!(
        end_message_id.as_deref(),
        Some(wake_message_id.as_str()),
        "terminal stream:end carries the persisted wake messageId"
    );
    assert!(
        saw_idle,
        "terminal agent:idle (harness_wake_complete) observed"
    );

    // The coalesced burst is persisted as ONE assistant row under the wake
    // messageId and returned by agent.getConversation.
    let convo = wss_rpc(
        &mut setup.rpc,
        12,
        "agent.getConversation",
        json!({ "workspaceId": setup.ws_id, "agentId": agent_id }),
    )
    .await;
    let messages = convo["messages"]
        .as_array()
        .expect("conversation messages array");
    let wake_row = messages
        .iter()
        .find(|m| m["id"].as_str() == Some(wake_message_id.as_str()))
        .unwrap_or_else(|| panic!("wake assistant row persisted under {wake_message_id}: {convo}"));
    assert_eq!(wake_row["role"], "assistant", "wake row is assistant");
    let row_text = blocks_text(wake_row);
    for marker in WAKE_LINES {
        assert!(
            row_text.contains(marker),
            "persisted wake row carries {marker:?}, got: {row_text}"
        );
    }
}

/// WAKE-3 (intent-hq/monorepo#3262): a harness-wake burst whose entire
/// output is one whitespace-only chunk (the incident's bare "\n") is a
/// FAILED recovery, not a successful completion. The agent here is
/// root/user-facing (not redrive-eligible), so over the wire the daemon
/// must: emit `agent:attention-requested` `{ kind: "blocker" }`, stamp the
/// wake `agent:idle` with `emptyWakeResponse: true`, and leave the
/// persisted session carrying `attentionRequest*` fields — the workspace
/// shows "turn ended unexpectedly" instead of idling silently.
#[tokio::test]
async fn empty_wake_response_surfaces_attention_over_wss() {
    let Some(script) = gate("WSS empty-wake attention E2E") else {
        return;
    };
    let behavior = json!({ "response": "warmup done" }).to_string();
    let mut setup = wake_setup(&script, &behavior).await;
    let agent_id = setup.agent_id.clone();

    // Fire the incident-shaped burst: ONE chunk containing a bare newline.
    std::fs::write(&setup.trigger_file, "<NEWLINE_ONLY>\n").expect("write wake trigger");

    let mut saw_attention = false;
    let mut saw_idle = false;
    let mut idle_frame = Value::Null;
    for _ in 0..300 {
        let frame = wss_event(&mut setup.sub, 30).await;
        let event = &frame["params"]["event"];
        if event["data"]["agentId"].as_str() != Some(agent_id.as_str()) {
            continue;
        }
        match event["type"].as_str() {
            Some("agent:attention-requested") => {
                assert_eq!(
                    event["data"]["kind"], "blocker",
                    "empty-wake attention is a blocker: {frame}"
                );
                let reason = event["data"]["reason"].as_str().unwrap_or_default();
                assert!(
                    reason.contains("Turn ended unexpectedly"),
                    "reason names the unexpected turn end: {frame}"
                );
                saw_attention = true;
            }
            Some("agent:idle") => {
                assert_eq!(
                    event["data"]["reason"], "harness_wake_complete",
                    "wake idle reason: {frame}"
                );
                saw_idle = true;
                idle_frame = frame.clone();
            }
            Some("agent:failed") => panic!("agent:failed during empty-wake scenario: {frame}"),
            _ => {}
        }
        if saw_attention && saw_idle {
            break;
        }
    }
    assert!(saw_attention, "agent:attention-requested observed");
    assert!(saw_idle, "wake agent:idle observed");
    assert_eq!(
        idle_frame["params"]["event"]["data"]["emptyWakeResponse"],
        json!(true),
        "wake idle carries the advisory emptyWakeResponse flag: {idle_frame}"
    );

    // The attention request is durable on the session (agent.getSession).
    let got = wss_rpc(
        &mut setup.rpc,
        12,
        "agent.getSession",
        json!({ "workspaceId": setup.ws_id, "agentId": agent_id }),
    )
    .await;
    assert_eq!(
        got["session"]["attentionRequestKind"], "blocker",
        "session carries the persisted attention request: {got}"
    );
}

/// WAKE-2 (monorepo#855): a user `agent.sendMessage` racing in mid-wake
/// queues behind the wake turn's single-flight slot (never interleaves),
/// preempts the settle window, suppresses the wake `agent:idle`, and
/// drains as its own prompt turn strictly AFTER the wake turn's terminal
/// `agent:stream:end` — with both rows in the final transcript.
#[tokio::test]
async fn racing_user_send_queues_behind_wake_turn_over_wss() {
    let Some(script) = gate("WSS harness-wake racing-send E2E") else {
        return;
    };
    let behavior = json!({
        "response": "warmup done",
        "rules": [
            { "ifPromptContains": "racing-marker", "response": "racing turn response" },
        ],
    })
    .to_string();
    let mut setup = wake_setup(&script, &behavior).await;
    let agent_id = setup.agent_id.clone();

    std::fs::write(&setup.trigger_file, WAKE_LINES.join("\n")).expect("write wake trigger");

    // Wait for the wake turn to open (stream:start is emitted AFTER the wake
    // listener claimed the single-flight slot), then race a user send in.
    let mut wake_message_id = String::new();
    for _ in 0..300 {
        let frame = wss_event(&mut setup.sub, 30).await;
        let event = &frame["params"]["event"];
        if event["data"]["agentId"].as_str() != Some(agent_id.as_str()) {
            continue;
        }
        if event["type"] == "agent:stream:start" {
            assert_eq!(event["data"]["reason"], "harness-wake", "{frame}");
            wake_message_id = event["data"]["messageId"]
                .as_str()
                .expect("wake messageId")
                .to_string();
            break;
        }
    }
    assert!(!wake_message_id.is_empty(), "wake turn opened");

    // The wake turn holds the slot for the 2s settle window; this send lands
    // well inside it and must QUEUE, not interleave.
    let sent = wss_rpc(
        &mut setup.rpc,
        20,
        "agent.sendMessage",
        json!({
            "workspaceId": setup.ws_id,
            "agentId": agent_id,
            "content": "racing-marker please",
        }),
    )
    .await;
    assert_eq!(sent["success"], true, "racing send ok: {sent}");
    assert_eq!(
        sent["queued"], true,
        "racing send queued behind the wake turn's slot: {sent}"
    );
    assert_eq!(
        sent["queuedMessage"]["content"], "racing-marker please",
        "queued entry preserves the content: {sent}"
    );

    // Phase 1 — wake turn finalizes first: chunks stay on the wake
    // messageId, the racing response never appears before the wake turn's
    // terminal stream:end, and NO agent:idle fires in between (suppressed —
    // the ready-to-send queue is non-empty).
    let mut saw_wake_end = false;
    let mut racing_message_id: Option<String> = None;
    let mut racing_text = String::new();
    let mut saw_racing_end = false;
    let mut saw_final_idle = false;
    for _ in 0..300 {
        let frame = wss_event(&mut setup.sub, 30).await;
        let event = &frame["params"]["event"];
        if event["data"]["agentId"].as_str() != Some(agent_id.as_str()) {
            continue;
        }
        match event["type"].as_str() {
            Some("chat:stream:delta") => {
                let data = serde_json::to_string(&event["data"]).unwrap_or_default();
                if saw_wake_end {
                    let mid = event["data"]["messageId"]
                        .as_str()
                        .expect("racing chunk messageId")
                        .to_string();
                    assert_ne!(
                        mid, wake_message_id,
                        "racing turn streams under its own messageId"
                    );
                    racing_message_id = Some(mid);
                    racing_text.push_str(&data);
                } else {
                    assert_eq!(
                        event["data"]["messageId"].as_str(),
                        Some(wake_message_id.as_str()),
                        "pre-end chunks belong to the wake turn: {frame}"
                    );
                    assert!(
                        !data.contains("racing turn response"),
                        "racing response must not interleave with the wake turn: {frame}"
                    );
                }
            }
            Some("agent:stream:end") => {
                if saw_wake_end {
                    assert_eq!(
                        event["data"]["messageId"].as_str(),
                        racing_message_id.as_deref(),
                        "racing turn's stream:end carries its own messageId: {frame}"
                    );
                    saw_racing_end = true;
                } else {
                    assert_eq!(
                        event["data"]["messageId"].as_str(),
                        Some(wake_message_id.as_str()),
                        "wake turn's terminal stream:end carries its messageId: {frame}"
                    );
                    saw_wake_end = true;
                }
            }
            Some("agent:idle") => {
                // The wake idle is suppressed (ready-to-send queue is
                // non-empty); the ONLY idle is the racing prompt turn's.
                assert!(
                    saw_racing_end,
                    "agent:idle before the racing turn ended — the wake idle \
                     was not suppressed: {frame}"
                );
                assert_eq!(
                    event["data"]["reason"], "stream_complete",
                    "terminal idle belongs to the racing prompt turn: {frame}"
                );
                saw_final_idle = true;
            }
            Some("agent:failed") => panic!("agent:failed during racing scenario: {frame}"),
            _ => {}
        }
        if saw_final_idle {
            break;
        }
    }
    assert!(saw_wake_end, "wake turn reached its terminal stream:end");
    assert!(
        racing_text.contains("racing turn response"),
        "racing turn streamed its response after the wake turn: {racing_text}"
    );
    assert!(
        saw_racing_end,
        "racing turn reached its terminal stream:end"
    );
    assert!(
        saw_final_idle,
        "terminal agent:idle (stream_complete) observed"
    );

    // Transcript ordering: the wake assistant row, then the racing user row,
    // then the racing assistant row — all three persisted.
    let convo = wss_rpc(
        &mut setup.rpc,
        21,
        "agent.getConversation",
        json!({ "workspaceId": setup.ws_id, "agentId": agent_id }),
    )
    .await;
    let messages = convo["messages"]
        .as_array()
        .expect("conversation messages array");
    let idx_of = |pred: &dyn Fn(&Value) -> bool| messages.iter().position(pred);
    let wake_idx = idx_of(&|m| m["id"].as_str() == Some(wake_message_id.as_str()))
        .unwrap_or_else(|| panic!("wake assistant row persisted: {convo}"));
    let user_idx =
        idx_of(&|m| m["role"] == "user" && blocks_text(m).contains("racing-marker please"))
            .unwrap_or_else(|| panic!("racing user row persisted: {convo}"));
    let racing_idx =
        idx_of(&|m| m["role"] == "assistant" && blocks_text(m).contains("racing turn response"))
            .unwrap_or_else(|| panic!("racing assistant row persisted: {convo}"));
    assert_eq!(
        messages[wake_idx]["role"], "assistant",
        "wake row is assistant"
    );
    assert!(
        wake_idx < user_idx && user_idx < racing_idx,
        "transcript order wake({wake_idx}) < user({user_idx}) < racing({racing_idx}): {convo}"
    );
}
