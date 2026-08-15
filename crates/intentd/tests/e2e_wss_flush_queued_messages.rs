//! WSS end-to-end coverage for the queued-message batch flush
//! (`agents.flushQueuedMessages`, default `"all"`): messages queued while an
//! agent is busy are delivered as ONE combined turn when the busy turn ends.
//!
//! Case 1 (default `"all"`): start a slow turn, queue 2 messages behind it,
//! let the turn end. The provider-received prompt (via the mock fixture's
//! `MOCK_AGENT_PROMPT_LOG` seam) is a single message starting with
//! `2 queued messages while you were working` carrying `Message #1:` /
//! `Message #2:` plus each entry's dequeue-wait `[SYSTEM NOTE]`; the
//! transcript keeps two separate user rows; `agent:queue:updated` empties in
//! one snapshot (2 → 0, never through 1) and exactly ONE
//! `agent:queue:processing` fires.
//!
//! Case 2 (`flushQueuedMessages = "off"` in `config.toml`): the same setup
//! drains legacy one-at-a-time — one turn per queued message, no combined
//! header, and the queue shrinks 2 → 1 → 0.
//!
//! Case 3 (`flushQueuedMessages = "systemOnly"`): two SYSTEM-origin messages
//! queued behind a busy turn (via `agent.queueMessage`'s system-origin path,
//! which parks as `user_origin: false`) are delivered as ONE combined turn,
//! same contract as case 1.
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
use tokio::net::UnixStream;
use tokio::time::timeout;
use tokio_tungstenite::tungstenite::Message;
use tokio_tungstenite::WebSocketStream;
use uuid::Uuid;

const TOKEN: &str = "efefefefefefefefefefefefefefefefefefefefefefefefefefefefefefefef";

const KICKOFF_MSG: &str = "kick-off slow turn";
const QUEUED_ONE: &str = "queued flush one";
const QUEUED_TWO: &str = "queued flush two";
const FLUSH_HEADER: &str = "2 queued messages while you were working";
const WAIT_NOTE_PREFIX: &str = "[SYSTEM NOTE] This message was queued at";

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
    let dir = PathBuf::from("/tmp").join(format!("itd-wss-flush-{}", &id[..8]));
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

async fn connect_ws(port: u16, cfg: Arc<ClientConfig>) -> common::TlsWs {
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
        title: "WSS-FLUSH-E2E".to_string(),
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

/// Seed `agents.flushQueuedMessages = <mode>` into the data dir's
/// `config.toml` BEFORE boot (must run before `enable_ws_api` appends the
/// `[server.wsApi]` table, and the two tables must not collide).
fn seed_flush_mode(data_dir: &Path, mode: &str) {
    std::fs::create_dir_all(data_dir).expect("mkdir data dir");
    let path = data_dir.join("config.toml");
    assert!(
        !path.exists(),
        "seed_flush_mode must run before other config seeding"
    );
    // stateSnapshot off: drain turns are built while messages are still
    // queued, so the per-turn snapshot line would otherwise lead the prompt
    // and break this suite's byte-precise prompt-prefix assertions (the
    // injection has its own e2e in e2e_wss_agent_state_snapshot.rs).
    std::fs::write(
        &path,
        format!(
            "[agents]\nflushQueuedMessages = \"{mode}\"\n\n[agentFeatures]\nstateSnapshot = false\n"
        ),
    )
    .expect("seed config.toml with flushQueuedMessages mode");
}

/// Boot a daemon with the slow-first-turn mock, create an agent, start the
/// kick-off turn, and queue TWO messages behind it (both `queued: true`).
/// Returns everything the per-case assertions need. The `sub` connection is
/// already subscribed to `agent:*` for the workspace — subscription happens
/// BEFORE the kick-off send, so no drain event can be missed.
struct FlushSetup {
    _daemon: Daemon,
    sub: common::TlsWs,
    rpc: common::TlsWs,
    agent_id: String,
    prompt_log: PathBuf,
}

async fn setup_busy_agent_with_two_queued(data_dir: &Path, script: &str) -> FlushSetup {
    let prompt_log = data_dir.join("prompts.jsonl");
    let prompt_log_str = prompt_log.to_string_lossy().into_owned();
    // First turn parks 2s: a deterministic window to queue both messages
    // while the worker is busy. Queue-drained turns run at full mock speed.
    let behavior = json!({ "response": "flush reply", "firstTurnDelayMs": 2000 }).to_string();
    let env: [(&str, &str); 6] = [
        ("INTENTD_AUTH_TOKEN", TOKEN),
        ("INTENTD_TCP_PORT", "0"),
        // The 2s busy window sits below the 5s dequeue-wait annotation
        // threshold (monorepo#2353); drop it so the wait-note assertions
        // exercise the annotation without slowing the suite.
        ("INTENTD_DEQUEUE_WAIT_MIN_MS", "0"),
        ("MOCK_AGENT_SCRIPT_PATH", script),
        ("MOCK_AGENT_BEHAVIOR", &behavior),
        ("MOCK_AGENT_PROMPT_LOG", &prompt_log_str),
    ];
    let ws_id = seed_workspace_only(data_dir).await;
    let child = spawn_serve(data_dir, &env);
    let daemon = Daemon {
        child,
        data_dir: data_dir.to_path_buf(),
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
        json!({ "workspaceId": ws_id, "name": "WSS-FLUSH", "model": "mock:default" }),
    )
    .await;
    let agent_id = created["agent"]["id"]
        .as_str()
        .expect("agent id")
        .to_string();

    // Kick off the slow turn (idle agent → streams immediately, not queued).
    let sent = wss_rpc(
        &mut rpc,
        11,
        "agent.sendMessage",
        json!({ "workspaceId": ws_id, "agentId": agent_id, "content": KICKOFF_MSG }),
    )
    .await;
    assert_eq!(sent["success"], true, "sendMessage ok: {sent}");
    assert_eq!(
        sent["queued"], false,
        "kick-off streams, not queued: {sent}"
    );

    // Queue two messages behind the parked turn. `queued: true` on both
    // proves they landed on the queue (no self-drain race).
    let q1 = wss_rpc(
        &mut rpc,
        12,
        "agent.queueMessage",
        json!({ "agentId": agent_id, "content": QUEUED_ONE }),
    )
    .await;
    assert_eq!(q1["success"], true, "queue one: {q1}");
    let q2 = wss_rpc(
        &mut rpc,
        13,
        "agent.queueMessage",
        json!({ "agentId": agent_id, "content": QUEUED_TWO }),
    )
    .await;
    assert_eq!(q2["success"], true, "queue two: {q2}");

    let queue = wss_rpc(
        &mut rpc,
        14,
        "agent.getQueue",
        json!({ "agentId": agent_id }),
    )
    .await;
    let entries = queue["queue"].as_array().expect("queue array");
    assert_eq!(entries.len(), 2, "both messages queued mid-turn: {queue}");
    assert_eq!(entries[0]["content"], json!(QUEUED_ONE));
    assert_eq!(entries[1]["content"], json!(QUEUED_TWO));

    FlushSetup {
        _daemon: daemon,
        sub,
        rpc,
        agent_id,
        prompt_log,
    }
}

/// Poll the mock fixture's prompt log until `min_lines` prompts have been
/// recorded, returning the prompt texts in turn order.
async fn await_prompts(prompt_log: &Path, min_lines: usize) -> Vec<String> {
    for _ in 0..150 {
        if let Ok(log) = std::fs::read_to_string(prompt_log) {
            let texts: Vec<String> = log
                .lines()
                .filter_map(|l| serde_json::from_str::<Value>(l).ok())
                .filter_map(|p| p["text"].as_str().map(str::to_string))
                .collect();
            if texts.len() >= min_lines {
                return texts;
            }
        }
        tokio::time::sleep(Duration::from_millis(200)).await;
    }
    panic!("prompt log never reached {min_lines} entries");
}

/// User-row texts from `agent.getConversation`, in transcript order.
fn user_row_texts(conv: &Value) -> Vec<String> {
    conv["messages"]
        .as_array()
        .expect("messages array")
        .iter()
        .filter(|m| m["role"] == "user")
        .filter_map(|m| {
            m["contentBlocks"]
                .as_array()
                .and_then(|blocks| blocks.first())
                .and_then(|b| b["text"].as_str())
                .map(String::from)
        })
        .collect()
}

/// Drain-phase observation shared by both cases: consume subscription events
/// until `want_stream_ends` terminal `agent:stream:end`s for the agent have
/// been seen (kick-off turn + drained turn(s)), recording every non-empty
/// `agent:queue:updated` queue length, every `agent:queue:processing`
/// `turnId`, and the `turnId` of every user-row `agent:message` echo (the
/// kick-off send's echo carries its own turn's id, so it appears first).
struct DrainObservation {
    queue_lengths: Vec<usize>,
    processing_turn_ids: Vec<String>,
    user_row_turn_ids: Vec<String>,
}

async fn observe_drain(
    sub: &mut common::TlsWs,
    agent_id: &str,
    want_stream_ends: usize,
) -> DrainObservation {
    let mut queue_lengths = Vec::new();
    let mut processing_turn_ids = Vec::new();
    let mut user_row_turn_ids = Vec::new();
    let mut stream_ends = 0usize;
    for _ in 0..400 {
        let frame = wss_event(sub, 30).await;
        let event = &frame["params"]["event"];
        if event["data"]["agentId"].as_str() != Some(agent_id) {
            continue;
        }
        match event["type"].as_str() {
            Some("agent:queue:updated") => {
                let len = event["data"]["queue"].as_array().map_or(0, Vec::len);
                queue_lengths.push(len);
            }
            Some("agent:queue:processing") => {
                processing_turn_ids.push(
                    event["data"]["turnId"]
                        .as_str()
                        .expect("queue:processing carries a turnId")
                        .to_string(),
                );
            }
            Some("agent:message") => {
                if event["data"]["role"] == "user" {
                    if let Some(tid) = event["data"]["turnId"].as_str() {
                        user_row_turn_ids.push(tid.to_string());
                    }
                }
            }
            Some("agent:stream:end") => {
                stream_ends += 1;
                if stream_ends >= want_stream_ends {
                    break;
                }
            }
            _ => {}
        }
    }
    assert_eq!(
        stream_ends, want_stream_ends,
        "expected {want_stream_ends} terminal stream:ends (saw {stream_ends})"
    );
    DrainObservation {
        queue_lengths,
        processing_turn_ids,
        user_row_turn_ids,
    }
}

/// Queue snapshot lengths from the DRAIN phase: everything after the last
/// length-2 snapshot (the enqueue phase publishes 1 → 2 before the busy turn
/// ends; the subscription sees those buffered events too).
fn shrink_lengths(queue_lengths: &[usize]) -> &[usize] {
    let last_two = queue_lengths
        .iter()
        .rposition(|&l| l == 2)
        .unwrap_or_else(|| panic!("never observed the 2-entry queue snapshot: {queue_lengths:?}"));
    &queue_lengths[last_two + 1..]
}

/// FLUSH-1 (default `agents.flushQueuedMessages = true`): two messages
/// queued behind a busy turn are delivered as ONE combined turn.
///
/// Contract locked down:
/// 1. The provider-received prompt (mock fixture's `MOCK_AGENT_PROMPT_LOG`)
///    is a single message starting with `2 queued messages while you were
///    working`, carrying `Message #1:` / `Message #2:` in queue order, each
///    followed by its dequeue-wait `[SYSTEM NOTE] This message was queued
///    at … and waited …` annotation.
/// 2. The transcript (`agent.getConversation`) keeps TWO separate user rows
///    for the queued messages — the combined prompt is wire-only.
/// 3. `agent:queue:updated` empties in ONE snapshot (2 → 0, never through
///    1) and exactly ONE `agent:queue:processing` fires for the batch.
/// 4. Turn correlation (monorepo#1022): BOTH user-row `agent:message`
///    echoes carry the combined turn's `turnId` — the one named by the
///    single `agent:queue:processing` — never a per-entry id that matches
///    no processing/stream lifecycle.
#[tokio::test]
async fn flush_combines_queued_messages_into_one_turn_over_wss() {
    let Some(script) = gate("WSS queued-message flush E2E") else {
        return;
    };
    let data_dir = temp_data_dir();
    let mut setup = setup_busy_agent_with_two_queued(&data_dir, &script).await;

    // Two terminal stream:ends: the kick-off turn, then the ONE combined
    // flush turn (a third would mean the drain split the batch).
    let obs = observe_drain(&mut setup.sub, &setup.agent_id, 2).await;

    // (3) One-shot queue shrink: after the last 2-entry snapshot (the
    // enqueue phase publishes 1 → 2), the batch dequeue publishes the fully
    // drained queue in a single snapshot — an intermediate length-1 snapshot
    // means one-at-a-time drain leaked through the flush arm.
    let shrink = shrink_lengths(&obs.queue_lengths);
    assert!(
        !shrink.contains(&1),
        "queue must empty in one snapshot (2 → 0), never through 1: {:?}",
        obs.queue_lengths
    );
    assert!(
        shrink.ends_with(&[0]),
        "final queue snapshot is empty: {:?}",
        obs.queue_lengths
    );
    assert_eq!(
        obs.processing_turn_ids.len(),
        1,
        "exactly ONE agent:queue:processing for the combined turn: {:?}",
        obs.processing_turn_ids
    );
    // (4) Every flushed row's echo correlates with the combined turn. The
    // first user-row echo is the kick-off's (its own direct turn's id).
    let combined_turn_id = &obs.processing_turn_ids[0];
    assert_eq!(
        obs.user_row_turn_ids.len(),
        3,
        "kick-off + two flushed user-row echoes: {:?}",
        obs.user_row_turn_ids
    );
    assert_eq!(
        &obs.user_row_turn_ids[1..],
        &[combined_turn_id.clone(), combined_turn_id.clone()],
        "both flushed user-row echoes carry the combined turn's turnId"
    );

    // (1) Outbound-prompt contract: prompt #2 is the combined flush prompt.
    let prompts = await_prompts(&setup.prompt_log, 2).await;
    assert_eq!(prompts.len(), 2, "kick-off + ONE flush turn: {prompts:?}");
    assert!(
        prompts[0].contains(KICKOFF_MSG),
        "first prompt is the kick-off: {}",
        prompts[0]
    );
    let flush = &prompts[1];
    assert!(
        flush.starts_with(FLUSH_HEADER),
        "flush prompt starts with the batch header: {flush}"
    );
    let m1 = flush
        .find("Message #1:")
        .unwrap_or_else(|| panic!("flush prompt carries Message #1: {flush}"));
    let m2 = flush
        .find("Message #2:")
        .unwrap_or_else(|| panic!("flush prompt carries Message #2: {flush}"));
    assert!(m1 < m2, "messages appear in queue order: {flush}");
    let i_one = flush
        .find(QUEUED_ONE)
        .unwrap_or_else(|| panic!("flush prompt carries {QUEUED_ONE:?}: {flush}"));
    let i_two = flush
        .find(QUEUED_TWO)
        .unwrap_or_else(|| panic!("flush prompt carries {QUEUED_TWO:?}: {flush}"));
    assert!(
        m1 < i_one && i_one < m2 && m2 < i_two,
        "each label precedes its content: {flush}"
    );
    // Each entry carries its own dequeue-wait note (queuedAt + wait info).
    assert_eq!(
        flush.matches(WAIT_NOTE_PREFIX).count(),
        2,
        "one dequeue-wait [SYSTEM NOTE] per batched entry: {flush}"
    );
    assert!(
        flush.contains("before delivery."),
        "wait note carries the waited duration: {flush}"
    );

    // (2) Transcript contract: two SEPARATE user rows for the queued
    // messages (prefix match — drained rows carry the appended wait note),
    // and no row carries the wire-only combined header.
    let conv = wss_rpc(
        &mut setup.rpc,
        20,
        "agent.getConversation",
        json!({ "agentId": setup.agent_id }),
    )
    .await;
    let users = user_row_texts(&conv);
    let idx = |needle: &str| {
        users
            .iter()
            .position(|t| t.starts_with(needle))
            .unwrap_or_else(|| panic!("missing user row {needle:?}: {users:?}"))
    };
    assert!(
        idx(KICKOFF_MSG) < idx(QUEUED_ONE) && idx(QUEUED_ONE) < idx(QUEUED_TWO),
        "three user rows in delivery order: {users:?}"
    );
    for needle in [QUEUED_ONE, QUEUED_TWO] {
        assert_eq!(
            users.iter().filter(|t| t.starts_with(needle)).count(),
            1,
            "queued message {needle:?} persists as exactly one row: {users:?}"
        );
    }
    assert!(
        users.iter().all(|t| !t.contains(FLUSH_HEADER)),
        "combined header is wire-only, never a transcript row: {users:?}"
    );

    // Queue is empty after the flush.
    let queue = wss_rpc(
        &mut setup.rpc,
        21,
        "agent.getQueue",
        json!({ "agentId": setup.agent_id }),
    )
    .await;
    assert!(
        queue["queue"].as_array().expect("queue array").is_empty(),
        "queue empty after flush: {queue}"
    );
}

/// FLUSH-2 (`agents.flushQueuedMessages = "off"` in `config.toml`): the same
/// two-queued setup drains legacy one-at-a-time — one turn per queued
/// message (three prompts total, none with the batch header), TWO
/// `agent:queue:processing` signals, and the queue shrinking through 1.
#[tokio::test]
async fn flush_disabled_drains_queue_one_turn_per_message_over_wss() {
    let Some(script) = gate("WSS queued-message flush-disabled E2E") else {
        return;
    };
    let data_dir = temp_data_dir();
    seed_flush_mode(&data_dir, "off");
    let mut setup = setup_busy_agent_with_two_queued(&data_dir, &script).await;

    // Three terminal stream:ends: kick-off + one turn PER queued message.
    let obs = observe_drain(&mut setup.sub, &setup.agent_id, 3).await;

    let shrink = shrink_lengths(&obs.queue_lengths);
    assert!(
        shrink.contains(&1),
        "one-at-a-time drain shrinks the queue through 1: {:?}",
        obs.queue_lengths
    );
    assert!(
        shrink.ends_with(&[0]),
        "final queue snapshot is empty: {:?}",
        obs.queue_lengths
    );
    assert_eq!(
        obs.processing_turn_ids.len(),
        2,
        "one agent:queue:processing per drained message: {:?}",
        obs.processing_turn_ids
    );
    // Legacy one-at-a-time correlation: each drained row's echo carries its
    // own turn's turnId, matching the processing signals in drain order (the
    // first user-row echo is the kick-off's, from its own direct turn).
    assert_eq!(
        obs.user_row_turn_ids.len(),
        3,
        "kick-off + one echo per drained message: {:?}",
        obs.user_row_turn_ids
    );
    assert_eq!(
        &obs.user_row_turn_ids[1..],
        &obs.processing_turn_ids[..],
        "each user-row echo correlates with its own turn"
    );

    // Prompts #2 and #3 carry one queued message each, FIFO, and neither
    // (nor any prompt) carries the batch header.
    let prompts = await_prompts(&setup.prompt_log, 3).await;
    assert_eq!(
        prompts.len(),
        3,
        "kick-off + one turn per queued message: {prompts:?}"
    );
    assert!(
        prompts[1].starts_with(QUEUED_ONE),
        "second turn delivers the first queued message: {}",
        prompts[1]
    );
    assert!(
        prompts[2].starts_with(QUEUED_TWO),
        "third turn delivers the second queued message: {}",
        prompts[2]
    );
    assert!(
        !prompts[1].contains(QUEUED_TWO),
        "messages are NOT combined when flush is disabled: {}",
        prompts[1]
    );
    for p in &prompts {
        assert!(
            !p.contains("queued messages while you were working"),
            "no batch header on the legacy drain path: {p}"
        );
    }
    // The legacy drain still annotates each delivery with its wait note.
    assert_eq!(
        prompts[1].matches(WAIT_NOTE_PREFIX).count(),
        1,
        "per-message dequeue-wait note: {}",
        prompts[1]
    );
    assert_eq!(
        prompts[2].matches(WAIT_NOTE_PREFIX).count(),
        1,
        "per-message dequeue-wait note: {}",
        prompts[2]
    );
}

/// FLUSH-3 (`agents.flushQueuedMessages = "systemOnly"` in `config.toml`):
/// `agent.queueMessage` enqueues with `user_origin: false` (system-origin),
/// so two messages queued behind a busy turn via that RPC batch into ONE
/// combined turn under `systemOnly` — the same wire contract as the default
/// `"all"` case (FLUSH-1).
#[tokio::test]
async fn flush_system_only_combines_queued_messages_into_one_turn_over_wss() {
    let Some(script) = gate("WSS queued-message flush systemOnly E2E") else {
        return;
    };
    let data_dir = temp_data_dir();
    seed_flush_mode(&data_dir, "systemOnly");
    let mut setup = setup_busy_agent_with_two_queued(&data_dir, &script).await;

    // Two terminal stream:ends: the kick-off turn, then the ONE combined
    // flush turn (a third would mean the drain split the batch).
    let obs = observe_drain(&mut setup.sub, &setup.agent_id, 2).await;

    let shrink = shrink_lengths(&obs.queue_lengths);
    assert!(
        !shrink.contains(&1),
        "queue must empty in one snapshot (2 → 0), never through 1: {:?}",
        obs.queue_lengths
    );
    assert!(
        shrink.ends_with(&[0]),
        "final queue snapshot is empty: {:?}",
        obs.queue_lengths
    );
    assert_eq!(
        obs.processing_turn_ids.len(),
        1,
        "exactly ONE agent:queue:processing for the combined turn: {:?}",
        obs.processing_turn_ids
    );

    let prompts = await_prompts(&setup.prompt_log, 2).await;
    assert_eq!(prompts.len(), 2, "kick-off + ONE flush turn: {prompts:?}");
    let flush = &prompts[1];
    assert!(
        flush.starts_with(FLUSH_HEADER),
        "systemOnly flush prompt starts with the batch header: {flush}"
    );
    let i_one = flush
        .find(QUEUED_ONE)
        .unwrap_or_else(|| panic!("flush prompt carries {QUEUED_ONE:?}: {flush}"));
    let i_two = flush
        .find(QUEUED_TWO)
        .unwrap_or_else(|| panic!("flush prompt carries {QUEUED_TWO:?}: {flush}"));
    assert!(i_one < i_two, "messages appear in queue order: {flush}");

    let queue = wss_rpc(
        &mut setup.rpc,
        21,
        "agent.getQueue",
        json!({ "agentId": setup.agent_id }),
    )
    .await;
    assert!(
        queue["queue"].as_array().expect("queue array").is_empty(),
        "queue empty after flush: {queue}"
    );
}
