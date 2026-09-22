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
//! Case 3 (`flushQueuedMessages = "systemOnly"`): `agent.queueMessage` is the
//! FE's user-typed mid-turn reply path and parks as `user_origin: true`, so
//! two messages queued behind a busy turn via that RPC are EXCLUDED from the
//! system-only batch and drain one-at-a-time — same observable shape as
//! case 2 (one turn per message, no combined header, queue 2 → 1 → 0).
//!
//! Case 4 (two members, default `"all"`): the owner and a collaborator each
//! queue one message behind the busy turn. Per-user queue visibility holds
//! on every egress — `agent.getQueue` and the `agent:queue:updated` push
//! show the collaborator only its own entry (owner sees both, `position`
//! not renumbered), the owner's edit of the guest's entry and the guest's
//! edit/remove of the owner's entry are refused (`-32602`) — and the flush
//! still drains BOTH entries in one combined turn.
//!
//! Gated on `node` + the mock script; skips cleanly otherwise.

#![cfg(unix)]

mod common;

use std::path::{Path, PathBuf};
use std::process::{Child, Stdio};
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

const TOKEN: &str = "efefefefefefefefefefefefefefefefefefefefefefefefefefefefefefefef";
const GUEST_TOKEN: &str = "beefbeefbeefbeefbeefbeefbeefbeefbeefbeefbeefbeefbeefbeefbeefbeef";

const KICKOFF_MSG: &str = "kick-off slow turn";
const QUEUED_ONE: &str = "queued flush one";
const QUEUED_TWO: &str = "queued flush two";
const OWNER_QUEUED: &str = "queued by owner";
const GUEST_QUEUED: &str = "queued by guest";
const GUEST_PREAMBLE: &str = "Message from @guest";
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
            eprintln!("=== DAEMON LOG ===\n{log}\n=== END LOG ===");
        }
    }
}

fn temp_data_dir() -> tempfile::TempDir {
    common::test_tempdir_in("/tmp", "itd-wss-flush-")
}

fn spawn_serve(data_dir: &Path, env: &[(&str, &str)]) -> Child {
    let log = std::fs::File::create(data_dir.join("daemon.log")).expect("create daemon log");
    let workspaces_dir = data_dir.join("workspaces");
    std::fs::create_dir_all(&workspaces_dir).expect("mkdir hermetic workspaces dir");
    common::enable_ws_api(data_dir);
    let mut cmd = common::serve_command();
    cmd.env("INTENTD_DATA_DIR", data_dir)
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
    connect_ws_as(port, cfg, TOKEN).await
}

async fn connect_ws_as(port: u16, cfg: Arc<ClientConfig>, token: &str) -> common::TlsWs {
    let url = format!("wss://localhost:{port}/ws?token={token}");
    common::wss_connect_with_retry(port, cfg, &url).await
}

/// One JSON-RPC round-trip returning the full response envelope (pushes and
/// pings interleaved on the same socket are skipped) — for refusal
/// assertions on `error`.
async fn wss_rpc_envelope<S>(
    ws: &mut WebSocketStream<S>,
    id: i64,
    method: &str,
    params: Value,
) -> Value
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

async fn wss_rpc<S>(ws: &mut WebSocketStream<S>, id: i64, method: &str, params: Value) -> Value
where
    S: tokio::io::AsyncRead + tokio::io::AsyncWrite + Unpin,
{
    let v = wss_rpc_envelope(ws, id, method, params).await;
    assert!(v.get("error").is_none(), "rpc {method} errored: {v}");
    v["result"].clone()
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

/// Seed a workspace plus a non-primary `guest` principal — credential bound
/// to `GUEST_TOKEN`, collaborator member of the workspace — BEFORE boot.
/// Returns `(workspace id, guest principal)`.
async fn seed_workspace_with_guest(data_dir: &Path) -> (String, intent_core::Principal) {
    use intent_core::{now_iso, Principal, PrincipalId, WorkspaceId, WorkspaceRole};
    use intent_store::Store;
    let store = Store::open(&data_dir.join("intentd.db"))
        .await
        .expect("open store");
    let ws = WorkspaceId::new();
    store
        .insert_workspace(&workspace_seed(&ws))
        .await
        .expect("insert ws");
    let guest = Principal {
        id: PrincipalId::new(),
        github_user_id: None,
        login: Some("guest".to_string()),
        display_name: Some("Guest User".to_string()),
        avatar_url: None,
        is_primary: false,
        created_at: now_iso(),
        updated_at: now_iso(),
    };
    store
        .upsert_principal(&guest)
        .await
        .expect("guest principal");
    let token_hash =
        Sha256::digest(GUEST_TOKEN.as_bytes())
            .iter()
            .fold(String::new(), |mut s, b| {
                use std::fmt::Write as _;
                let _ = write!(s, "{b:02x}");
                s
            });
    store
        .insert_principal_credential(&guest.id, &token_hash)
        .await
        .expect("guest credential");
    store
        .add_workspace_member(&ws, &guest.id, WorkspaceRole::Collaborator)
        .await
        .expect("guest membership");
    (ws.0, guest)
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
        context_links: None,
        archived: false,
        archived_at: None,
        task_stats: None,
        agent_summary: None,
        diff_summary: None,
        token_usage: None,
        cow_supported: None,
        browser_client_id: None,
        pull_requests_total: None,
        display_status: None,
        waiting: false,
        checkout_mode: None,
        disk_usage: None,
        pending_delete_at: None,
        membership: None,
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
    /// Queue entry ids of `QUEUED_ONE` / `QUEUED_TWO`, in queue order.
    queued_ids: [String; 2],
}

/// A booted daemon with the slow-first-turn mock: the first turn parks
/// `first_turn_delay_ms` (a deterministic window to queue messages while the
/// worker is busy); queue-drained turns run at full mock speed.
struct Booted {
    daemon: Daemon,
    port: u16,
    cfg: Arc<ClientConfig>,
    prompt_log: PathBuf,
}

async fn boot_daemon(data_dir: &Path, script: &str, first_turn_delay_ms: u64) -> Booted {
    let prompt_log = data_dir.join("prompts.jsonl");
    let prompt_log_str = prompt_log.to_string_lossy().into_owned();
    let behavior =
        json!({ "response": "flush reply", "firstTurnDelayMs": first_turn_delay_ms }).to_string();
    let env: [(&str, &str); 5] = [
        ("INTENTD_AUTH_TOKEN", TOKEN),
        // The busy window sits below the 5s dequeue-wait annotation
        // threshold (monorepo#2353); drop it so the wait-note assertions
        // exercise the annotation without slowing the suite.
        ("INTENTD_DEQUEUE_WAIT_MIN_MS", "0"),
        ("MOCK_AGENT_SCRIPT_PATH", script),
        ("MOCK_AGENT_BEHAVIOR", &behavior),
        ("MOCK_AGENT_PROMPT_LOG", &prompt_log_str),
    ];
    let child = spawn_serve(data_dir, &env);
    let daemon = Daemon {
        child,
        data_dir: data_dir.to_path_buf(),
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
    Booted {
        daemon,
        port,
        cfg: client_config(&fingerprint),
        prompt_log,
    }
}

async fn setup_busy_agent_with_two_queued(data_dir: &Path, script: &str) -> FlushSetup {
    let ws_id = seed_workspace_only(data_dir).await;
    let Booted {
        daemon,
        port,
        cfg,
        prompt_log,
    } = boot_daemon(data_dir, script, 2000).await;

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
        json!({ "workspaceId": ws_id, "name": "WSS-FLUSH", "model": "default", "provider": "mock" }),
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
    let entry_id = |resp: &Value| {
        resp["queuedMessage"]["id"]
            .as_str()
            .expect("queueMessage returns the entry id")
            .to_string()
    };
    let queued_ids = [entry_id(&q1), entry_id(&q2)];
    assert_ne!(queued_ids[0], queued_ids[1], "distinct entry ids");

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
    assert_eq!(entries[0]["id"], json!(queued_ids[0]));
    assert_eq!(entries[1]["id"], json!(queued_ids[1]));

    FlushSetup {
        _daemon: daemon,
        sub,
        rpc,
        agent_id,
        prompt_log,
        queued_ids,
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

/// The `agent.getConversation` user row whose first text block starts with
/// `needle` (metadata assertions need the full row, not just its text).
fn user_row<'a>(conv: &'a Value, needle: &str) -> &'a Value {
    conv["messages"]
        .as_array()
        .expect("messages array")
        .iter()
        .filter(|m| m["role"] == "user")
        .find(|m| {
            m["contentBlocks"][0]["text"]
                .as_str()
                .is_some_and(|t| t.starts_with(needle))
        })
        .unwrap_or_else(|| panic!("missing user row {needle:?}: {conv}"))
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
/// kick-off send's echo carries its own turn's id, so it appears first),
/// plus each user-row echo's drain identity link `queuedMessageId`
/// (intentd#1783; `None` for the direct-send kick-off echo).
struct DrainObservation {
    queue_lengths: Vec<usize>,
    processing_turn_ids: Vec<String>,
    user_row_turn_ids: Vec<String>,
    user_row_queued_message_ids: Vec<Option<String>>,
}

async fn observe_drain(
    sub: &mut common::TlsWs,
    agent_id: &str,
    want_stream_ends: usize,
) -> DrainObservation {
    let mut queue_lengths = Vec::new();
    let mut processing_turn_ids = Vec::new();
    let mut user_row_turn_ids = Vec::new();
    let mut user_row_queued_message_ids = Vec::new();
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
                    user_row_queued_message_ids.push(
                        event["data"]["queuedMessageId"]
                            .as_str()
                            .map(str::to_string),
                    );
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
        user_row_queued_message_ids,
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
/// 5. Batch grouping: both flushed rows carry the SAME
///    `metadata.queueInfo.batchId` on `agent.getConversation`; the direct
///    kick-off row carries none.
/// 6. Drain identity link (intentd#1783): the two flushed rows carry
///    DISTINCT `metadata.queueInfo.queuedMessageId`s — each its own queue
///    entry's id, in queue order — and each row's `agent:message` echo
///    lifts the same id as `queuedMessageId`; the kick-off row/echo carry
///    none.
#[tokio::test]
async fn flush_combines_queued_messages_into_one_turn_over_wss() {
    let Some(script) = gate("WSS queued-message flush E2E") else {
        return;
    };
    let data_dir_guard = temp_data_dir();
    let data_dir = data_dir_guard.path().to_path_buf();
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

    // (5) Batch grouping stamp: both flushed rows share ONE
    // metadata.queueInfo.batchId; the kick-off row (direct send) has none.
    let row = |needle: &str| user_row(&conv, needle);
    let batch_id = row(QUEUED_ONE)["metadata"]["queueInfo"]["batchId"]
        .as_str()
        .expect("flushed row carries queueInfo.batchId")
        .to_string();
    assert!(!batch_id.is_empty(), "batchId is a non-empty string");
    assert_eq!(
        row(QUEUED_TWO)["metadata"]["queueInfo"]["batchId"].as_str(),
        Some(batch_id.as_str()),
        "both flushed user rows share the batch's id"
    );
    assert!(
        row(KICKOFF_MSG)["metadata"]["queueInfo"]["batchId"].is_null(),
        "the direct-send kick-off row carries no batchId"
    );

    // (6) Drain identity link: each flushed row names ITS OWN queue entry
    // (distinct ids, queue order), next to the shared batchId; each row's
    // echo lifted the same id; the kick-off row/echo carry none.
    let [one_id, two_id] = &setup.queued_ids;
    assert_eq!(
        row(QUEUED_ONE)["metadata"]["queueInfo"]["queuedMessageId"],
        json!(one_id),
        "first flushed row links its own entry"
    );
    assert_eq!(
        row(QUEUED_TWO)["metadata"]["queueInfo"]["queuedMessageId"],
        json!(two_id),
        "second flushed row links its own entry"
    );
    assert!(
        row(KICKOFF_MSG)["metadata"]["queueInfo"]["queuedMessageId"].is_null(),
        "the direct-send kick-off row carries no queuedMessageId"
    );
    assert_eq!(
        obs.user_row_queued_message_ids,
        vec![None, Some(one_id.clone()), Some(two_id.clone())],
        "kick-off echo unlinked; each flushed echo lifts its own entry id"
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
    let data_dir_guard = temp_data_dir();
    let data_dir = data_dir_guard.path().to_path_buf();
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

    // One-at-a-time drains group nothing: no user row carries a batchId.
    let conv = wss_rpc(
        &mut setup.rpc,
        20,
        "agent.getConversation",
        json!({ "agentId": setup.agent_id }),
    )
    .await;
    for needle in [KICKOFF_MSG, QUEUED_ONE, QUEUED_TWO] {
        assert!(
            user_row(&conv, needle)["metadata"]["queueInfo"]["batchId"].is_null(),
            "single-message drains never stamp a batchId: {needle:?}"
        );
    }
}

/// FLUSH-3 (`agents.flushQueuedMessages = "systemOnly"` in `config.toml`):
/// `agent.queueMessage` is the FE's user-typed mid-turn reply path and
/// enqueues with `user_origin: true`, so two messages queued behind a busy
/// turn via that RPC are excluded from the system-only batch and drain
/// one-at-a-time over WSS — one turn per message, no batch header, TWO
/// `agent:queue:processing` signals, and the queue shrinking through 1
/// (the same observable shape as FLUSH-2). A combined turn here would mean
/// `agent.queueMessage` regressed to system-origin.
#[tokio::test]
async fn flush_system_only_excludes_queue_message_entries_over_wss() {
    let Some(script) = gate("WSS queued-message flush systemOnly E2E") else {
        return;
    };
    let data_dir_guard = temp_data_dir();
    let data_dir = data_dir_guard.path().to_path_buf();
    seed_flush_mode(&data_dir, "systemOnly");
    let mut setup = setup_busy_agent_with_two_queued(&data_dir, &script).await;

    // Three terminal stream:ends: kick-off + one turn PER queued message
    // (two would mean the user-origin entries were batched).
    let obs = observe_drain(&mut setup.sub, &setup.agent_id, 3).await;

    let shrink = shrink_lengths(&obs.queue_lengths);
    assert!(
        shrink.contains(&1),
        "user-origin entries drain one-at-a-time under systemOnly (2 → 1 → 0): {:?}",
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
    for p in &prompts {
        assert!(
            !p.starts_with(FLUSH_HEADER),
            "user-origin agent.queueMessage entries never batch under systemOnly: {p}"
        );
    }

    // One-at-a-time drains group nothing: no user row carries a batchId.
    let conv = wss_rpc(
        &mut setup.rpc,
        20,
        "agent.getConversation",
        json!({ "agentId": setup.agent_id }),
    )
    .await;
    for needle in [KICKOFF_MSG, QUEUED_ONE, QUEUED_TWO] {
        assert!(
            user_row(&conv, needle)["metadata"]["queueInfo"]["batchId"].is_null(),
            "single-message drains never stamp a batchId: {needle:?}"
        );
    }

    let queue = wss_rpc(
        &mut setup.rpc,
        21,
        "agent.getQueue",
        json!({ "agentId": setup.agent_id }),
    )
    .await;
    assert!(
        queue["queue"].as_array().expect("queue array").is_empty(),
        "queue empty after drain: {queue}"
    );
}

/// Queue entry ids of an `agent:queue:updated` payload, in drain order.
fn queue_ids(queue: &Value) -> Vec<String> {
    queue
        .as_array()
        .expect("queue array")
        .iter()
        .map(|e| e["id"].as_str().expect("entry id").to_string())
        .collect()
}

/// Enqueue-phase observation: consume subscription events until an
/// `agent:queue:updated` snapshot for the agent satisfies `done`, returning
/// EVERY `agent:queue:updated` payload seen for the agent (the matching one
/// last). A terminal `agent:stream:end` for the agent before then means the
/// busy window closed early — fail loudly rather than mis-attribute the
/// drain-phase snapshots.
async fn await_queue_snapshots(
    sub: &mut common::TlsWs,
    agent_id: &str,
    done: impl Fn(&Value) -> bool,
) -> Vec<Value> {
    let mut seen = Vec::new();
    for _ in 0..200 {
        let frame = wss_event(sub, 30).await;
        let event = &frame["params"]["event"];
        if event["data"]["agentId"].as_str() != Some(agent_id) {
            continue;
        }
        match event["type"].as_str() {
            Some("agent:queue:updated") => {
                let queue = event["data"]["queue"].clone();
                let finished = done(&queue);
                seen.push(queue);
                if finished {
                    return seen;
                }
            }
            Some("agent:stream:end") => {
                panic!("busy turn ended before the enqueue phase completed: {seen:?}")
            }
            _ => {}
        }
    }
    panic!("never observed the awaited agent:queue:updated snapshot: {seen:?}")
}

/// FLUSH-4 (two members, default `agents.flushQueuedMessages = "all"`): the
/// owner (administrator) and a collaborator (`guest`, seeded as a workspace
/// member) each queue ONE message behind the busy turn. Per-user queue
/// visibility holds on every egress, and the batched flush still drains
/// both entries in ONE combined turn.
///
/// Contract locked down:
/// 1. `agent.getQueue` — the owner sees both entries (its own first, the
///    guest's second, `author.principalId` resolved on each); the guest
///    sees ONLY its own entry, with `position` kept at 1 (not renumbered).
/// 2. `agent:queue:updated` — the owner's subscription is pushed the full
///    snapshots (1 → 2 entries); the guest's subscription is pushed the
///    projected ones: the owner's entry never appears, and the last
///    enqueue-phase snapshot is exactly `[guest entry]` at `position` 1.
/// 3. Ownership refusals (`-32602`, no snapshot published): the owner's
///    `agent.editQueuedMessage` of the guest's entry (`can only be edited by
///    its author`), the guest's `agent.editQueuedMessage` and
///    `agent.removeQueuedMessage` of the owner's entry (`queued message not
///    found` — an invisible entry reads as absent). Both queues are
///    unchanged afterwards. Administrator override: the guest queues a
///    scratch entry, the owner's `agent.removeQueuedMessage` of it succeeds
///    (`{ success: true }`) and BOTH views return to exactly their previous
///    two-entry / one-entry state (the removal snapshots are consumed on
///    both subscriptions).
/// 4. Flush intact: the drain publishes ONE empty snapshot to each
///    subscriber (2 → 0 for the owner, 1 → 0 projected for the guest),
///    exactly ONE `agent:queue:processing` fires (same `turnId` on both
///    subscriptions), the flushed user-row echoes link BOTH entry ids in
///    queue order, the provider-received prompt is one combined message
///    (batch header, owner body before the preambled guest body), and the
///    transcript keeps two rows sharing one `batchId` — the guest's stamped
///    `fromPrincipalId`. Both members read an empty queue afterwards.
#[tokio::test]
async fn two_members_see_disjoint_queues_and_flush_combines_both_over_wss() {
    let Some(script) = gate("WSS two-member queue visibility + flush E2E") else {
        return;
    };
    let data_dir_guard = temp_data_dir();
    let data_dir = data_dir_guard.path().to_path_buf();
    let (ws_id, guest) = seed_workspace_with_guest(&data_dir).await;
    // A wider busy window than the single-member cases: two connections'
    // worth of enqueue reads, pushes and refusals must all land before the
    // kick-off turn ends (an early end panics in `await_queue_snapshots`).
    let Booted {
        daemon: _daemon,
        port,
        cfg,
        prompt_log,
    } = boot_daemon(&data_dir, &script, 5000).await;

    // Both members subscribe to `agent:*` BEFORE the kick-off send.
    let mut owner_sub = connect_ws(port, cfg.clone()).await;
    let sub_resp = wss_rpc(
        &mut owner_sub,
        1,
        "events.subscribe",
        json!({ "eventTypes": ["agent:*"], "workspaceId": ws_id }),
    )
    .await;
    assert!(
        sub_resp["subscriptionId"].is_string(),
        "owner subscribed: {sub_resp}"
    );
    let mut guest_sub = connect_ws_as(port, cfg.clone(), GUEST_TOKEN).await;
    let sub_resp = wss_rpc(
        &mut guest_sub,
        2,
        "events.subscribe",
        json!({ "eventTypes": ["agent:*"], "workspaceId": ws_id }),
    )
    .await;
    assert!(
        sub_resp["subscriptionId"].is_string(),
        "guest subscribed: {sub_resp}"
    );

    let mut rpc = connect_ws(port, cfg.clone()).await;
    let mut guest_rpc = connect_ws_as(port, cfg.clone(), GUEST_TOKEN).await;
    let created = wss_rpc(
        &mut rpc,
        10,
        "agent.create",
        json!({ "workspaceId": ws_id, "name": "WSS-FLUSH-2M", "model": "default", "provider": "mock" }),
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
        json!({ "workspaceId": ws_id, "agentId": agent_id, "content": KICKOFF_MSG }),
    )
    .await;
    assert_eq!(sent["success"], true, "sendMessage ok: {sent}");
    assert_eq!(
        sent["queued"], false,
        "kick-off streams, not queued: {sent}"
    );

    // Owner queues first, guest second.
    let owner_q = wss_rpc(
        &mut rpc,
        12,
        "agent.queueMessage",
        json!({ "workspaceId": ws_id, "agentId": agent_id, "content": OWNER_QUEUED }),
    )
    .await;
    assert_eq!(owner_q["success"], true, "owner queue: {owner_q}");
    let owner_id = owner_q["queuedMessage"]["id"]
        .as_str()
        .expect("owner entry id")
        .to_string();
    let guest_q = wss_rpc(
        &mut guest_rpc,
        100,
        "agent.queueMessage",
        json!({ "workspaceId": ws_id, "agentId": agent_id, "content": GUEST_QUEUED }),
    )
    .await;
    assert_eq!(guest_q["success"], true, "guest queue: {guest_q}");
    let guest_id = guest_q["queuedMessage"]["id"]
        .as_str()
        .expect("guest entry id")
        .to_string();
    assert_ne!(owner_id, guest_id, "distinct entry ids");

    // (1) agent.getQueue: owner sees both; guest sees only its own.
    let owner_view = wss_rpc(
        &mut rpc,
        13,
        "agent.getQueue",
        json!({ "agentId": agent_id }),
    )
    .await;
    assert_eq!(
        queue_ids(&owner_view["queue"]),
        vec![owner_id.clone(), guest_id.clone()],
        "owner reads the full queue in order: {owner_view}"
    );
    let entries = owner_view["queue"].as_array().expect("queue array");
    assert_eq!(entries[0]["position"], json!(0), "{owner_view}");
    assert_eq!(entries[1]["position"], json!(1), "{owner_view}");
    assert_eq!(entries[0]["content"], json!(OWNER_QUEUED), "{owner_view}");
    let owner_principal_id = entries[0]["author"]["principalId"]
        .as_str()
        .unwrap_or_else(|| panic!("owner entry resolves its author: {owner_view}"))
        .to_string();
    assert_ne!(
        owner_principal_id, guest.id.0,
        "owner entry is authored by the administrator: {owner_view}"
    );
    assert_eq!(
        entries[1]["author"]["principalId"],
        json!(guest.id.0),
        "guest entry is authored by the guest: {owner_view}"
    );
    assert_eq!(
        entries[1]["author"]["login"],
        json!("guest"),
        "{owner_view}"
    );
    let guest_content = entries[1]["content"].as_str().expect("guest content");
    assert!(
        guest_content.starts_with(GUEST_PREAMBLE) && guest_content.ends_with(GUEST_QUEUED),
        "guest entry carries the sender preamble above its body: {guest_content}"
    );

    let guest_view = wss_rpc(
        &mut guest_rpc,
        101,
        "agent.getQueue",
        json!({ "agentId": agent_id }),
    )
    .await;
    assert_eq!(
        queue_ids(&guest_view["queue"]),
        vec![guest_id.clone()],
        "guest reads only its own entry: {guest_view}"
    );
    let guest_entry = &guest_view["queue"][0];
    assert_eq!(
        guest_entry["position"],
        json!(1),
        "position is not renumbered for the projected view: {guest_view}"
    );
    assert_eq!(guest_entry["author"]["principalId"], json!(guest.id.0));

    // (2) agent:queue:updated pushes: full snapshots to the owner, projected
    // ones to the guest (the owner's entry never appears).
    let owner_pushes = await_queue_snapshots(&mut owner_sub, &agent_id, |q| {
        queue_ids(q) == [owner_id.clone(), guest_id.clone()]
    })
    .await;
    assert!(
        owner_pushes
            .iter()
            .any(|q| queue_ids(q) == [owner_id.clone()]),
        "owner sees the 1-entry snapshot before the 2-entry one: {owner_pushes:?}"
    );
    let guest_pushes = await_queue_snapshots(&mut guest_sub, &agent_id, |q| {
        queue_ids(q).contains(&guest_id)
    })
    .await;
    assert!(
        guest_pushes
            .iter()
            .all(|q| !queue_ids(q).contains(&owner_id)),
        "the owner's entry is never pushed to the guest: {guest_pushes:?}"
    );
    let guest_last = guest_pushes.last().expect("guest saw a snapshot");
    assert_eq!(
        queue_ids(guest_last),
        vec![guest_id.clone()],
        "guest's projected snapshot is exactly its own entry: {guest_last}"
    );
    assert_eq!(
        guest_last[0]["position"],
        json!(1),
        "projected push keeps position 1: {guest_last}"
    );

    // (3) Ownership refusals — none publishes a snapshot.
    let owner_edit = wss_rpc_envelope(
        &mut rpc,
        14,
        "agent.editQueuedMessage",
        json!({ "agentId": agent_id, "messageId": guest_id, "content": "owner rewrite" }),
    )
    .await;
    assert_eq!(owner_edit["jsonrpc"], "2.0", "{owner_edit}");
    assert!(owner_edit.get("result").is_none(), "{owner_edit}");
    assert_eq!(owner_edit["error"]["code"], -32602, "{owner_edit}");
    assert!(
        owner_edit["error"]["message"]
            .as_str()
            .is_some_and(|m| m.contains("can only be edited by its author")),
        "owner cannot edit the guest's entry: {owner_edit}"
    );
    let guest_edit = wss_rpc_envelope(
        &mut guest_rpc,
        102,
        "agent.editQueuedMessage",
        json!({ "agentId": agent_id, "messageId": owner_id, "content": "guest rewrite" }),
    )
    .await;
    assert_eq!(guest_edit["error"]["code"], -32602, "{guest_edit}");
    assert!(
        guest_edit["error"]["message"]
            .as_str()
            .is_some_and(|m| m.contains("queued message not found")),
        "the owner's entry is invisible to the guest's edit: {guest_edit}"
    );
    let guest_remove = wss_rpc_envelope(
        &mut guest_rpc,
        103,
        "agent.removeQueuedMessage",
        json!({ "agentId": agent_id, "messageId": owner_id }),
    )
    .await;
    assert!(guest_remove.get("result").is_none(), "{guest_remove}");
    assert_eq!(guest_remove["error"]["code"], -32602, "{guest_remove}");
    assert!(
        guest_remove["error"]["message"]
            .as_str()
            .is_some_and(|m| m.contains("queued message not found")),
        "the owner's entry is invisible to the guest's remove: {guest_remove}"
    );
    let owner_after = wss_rpc(
        &mut rpc,
        15,
        "agent.getQueue",
        json!({ "agentId": agent_id }),
    )
    .await;
    assert_eq!(
        owner_after["queue"], owner_view["queue"],
        "refused edits/removes leave the queue untouched"
    );
    let guest_after = wss_rpc(
        &mut guest_rpc,
        104,
        "agent.getQueue",
        json!({ "agentId": agent_id }),
    )
    .await;
    assert_eq!(
        guest_after["queue"], guest_view["queue"],
        "refused edits/removes leave the guest's view untouched"
    );

    // (3b) Administrator override: the owner CAN remove a guest-authored
    // entry. The guest queues a scratch entry, the owner removes it, and
    // both views return to exactly their previous state — the removal's
    // snapshots are consumed here so (4) still sees only the drain's.
    let scratch_q = wss_rpc(
        &mut guest_rpc,
        105,
        "agent.queueMessage",
        json!({ "workspaceId": ws_id, "agentId": agent_id, "content": "guest scratch" }),
    )
    .await;
    assert_eq!(
        scratch_q["success"], true,
        "guest scratch queue: {scratch_q}"
    );
    let scratch_id = scratch_q["queuedMessage"]["id"]
        .as_str()
        .expect("scratch entry id")
        .to_string();
    let owner_grown = await_queue_snapshots(&mut owner_sub, &agent_id, |q| {
        queue_ids(q) == [owner_id.clone(), guest_id.clone(), scratch_id.clone()]
    })
    .await;
    assert_eq!(
        owner_grown.len(),
        1,
        "one enqueue snapshot: {owner_grown:?}"
    );
    let guest_grown = await_queue_snapshots(&mut guest_sub, &agent_id, |q| {
        queue_ids(q) == [guest_id.clone(), scratch_id.clone()]
    })
    .await;
    assert_eq!(
        guest_grown.len(),
        1,
        "one projected enqueue snapshot: {guest_grown:?}"
    );
    assert_eq!(
        guest_grown[0][1]["position"],
        json!(2),
        "scratch keeps its full-queue position in the projection: {guest_grown:?}"
    );

    let owner_remove = wss_rpc_envelope(
        &mut rpc,
        16,
        "agent.removeQueuedMessage",
        json!({ "agentId": agent_id, "messageId": scratch_id }),
    )
    .await;
    assert!(owner_remove.get("error").is_none(), "{owner_remove}");
    assert_eq!(
        owner_remove["result"],
        json!({ "success": true }),
        "the administrator removes the guest's entry: {owner_remove}"
    );
    let owner_shrunk = await_queue_snapshots(&mut owner_sub, &agent_id, |q| {
        queue_ids(q) == [owner_id.clone(), guest_id.clone()]
    })
    .await;
    assert_eq!(
        owner_shrunk.len(),
        1,
        "one removal snapshot: {owner_shrunk:?}"
    );
    let guest_shrunk = await_queue_snapshots(&mut guest_sub, &agent_id, |q| {
        queue_ids(q) == [guest_id.clone()]
    })
    .await;
    assert_eq!(
        guest_shrunk.len(),
        1,
        "one projected removal snapshot: {guest_shrunk:?}"
    );

    let owner_restored = wss_rpc(
        &mut rpc,
        17,
        "agent.getQueue",
        json!({ "agentId": agent_id }),
    )
    .await;
    assert_eq!(
        owner_restored["queue"], owner_view["queue"],
        "only the scratch entry is gone; the owner's view is exactly as before"
    );
    let guest_restored = wss_rpc(
        &mut guest_rpc,
        106,
        "agent.getQueue",
        json!({ "agentId": agent_id }),
    )
    .await;
    assert_eq!(
        guest_restored["queue"], guest_view["queue"],
        "the guest's own surviving entry is untouched"
    );

    // (4) Flush intact — observed on both subscriptions: kick-off
    // stream:end, then the ONE combined flush turn.
    let (owner_obs, guest_obs) = tokio::join!(
        observe_drain(&mut owner_sub, &agent_id, 2),
        observe_drain(&mut guest_sub, &agent_id, 2),
    );
    // The enqueue-phase snapshots were consumed above and the refusals
    // published none, so the only snapshot left is the drain's — one shot,
    // straight to empty, on both projections.
    assert_eq!(
        owner_obs.queue_lengths,
        vec![0],
        "owner: queue empties in one snapshot (2 → 0)"
    );
    assert_eq!(
        guest_obs.queue_lengths,
        vec![0],
        "guest: projected queue empties in one snapshot (1 → 0)"
    );
    assert_eq!(
        owner_obs.processing_turn_ids.len(),
        1,
        "exactly ONE agent:queue:processing for the combined turn: {:?}",
        owner_obs.processing_turn_ids
    );
    assert_eq!(
        guest_obs.processing_turn_ids, owner_obs.processing_turn_ids,
        "the guest observes the same single combined turn"
    );
    let combined_turn_id = &owner_obs.processing_turn_ids[0];
    for obs in [&owner_obs, &guest_obs] {
        let linked: Vec<&String> = obs.user_row_queued_message_ids.iter().flatten().collect();
        assert_eq!(
            linked,
            vec![&owner_id, &guest_id],
            "flushed echoes link both entries in queue order: {:?}",
            obs.user_row_queued_message_ids
        );
        assert!(
            obs.user_row_turn_ids
                .ends_with(&[combined_turn_id.clone(), combined_turn_id.clone()]),
            "both flushed echoes carry the combined turn's turnId: {:?}",
            obs.user_row_turn_ids
        );
    }

    let prompts = await_prompts(&prompt_log, 2).await;
    assert_eq!(prompts.len(), 2, "kick-off + ONE flush turn: {prompts:?}");
    let flush = &prompts[1];
    assert!(
        flush.starts_with(FLUSH_HEADER),
        "flush prompt starts with the batch header: {flush}"
    );
    let i_owner = flush
        .find(OWNER_QUEUED)
        .unwrap_or_else(|| panic!("flush prompt carries {OWNER_QUEUED:?}: {flush}"));
    let i_guest = flush
        .find(GUEST_QUEUED)
        .unwrap_or_else(|| panic!("flush prompt carries {GUEST_QUEUED:?}: {flush}"));
    assert!(i_owner < i_guest, "entries appear in queue order: {flush}");
    assert!(
        flush.contains(GUEST_PREAMBLE),
        "the guest's sender preamble survives into the combined prompt: {flush}"
    );

    let conv = wss_rpc(
        &mut rpc,
        20,
        "agent.getConversation",
        json!({ "agentId": agent_id }),
    )
    .await;
    let owner_row = user_row(&conv, OWNER_QUEUED);
    let guest_row = conv["messages"]
        .as_array()
        .expect("messages array")
        .iter()
        .filter(|m| m["role"] == "user")
        .find(|m| {
            m["contentBlocks"][0]["text"]
                .as_str()
                .is_some_and(|t| t.starts_with(GUEST_PREAMBLE) && t.contains(GUEST_QUEUED))
        })
        .unwrap_or_else(|| panic!("missing guest user row: {conv}"));
    let batch_id = owner_row["metadata"]["queueInfo"]["batchId"]
        .as_str()
        .expect("flushed row carries queueInfo.batchId");
    assert_eq!(
        guest_row["metadata"]["queueInfo"]["batchId"].as_str(),
        Some(batch_id),
        "both members' rows share the batch's id"
    );
    assert_eq!(
        owner_row["metadata"]["queueInfo"]["queuedMessageId"],
        json!(owner_id)
    );
    assert_eq!(
        guest_row["metadata"]["queueInfo"]["queuedMessageId"],
        json!(guest_id)
    );
    assert_eq!(
        guest_row["metadata"]["fromPrincipalId"],
        json!(guest.id.0),
        "guest row keeps its principal stamp: {guest_row}"
    );
    assert_eq!(
        owner_row["metadata"]["fromPrincipalId"],
        json!(owner_principal_id),
        "owner row keeps its principal stamp: {owner_row}"
    );

    for (ws, id) in [(&mut rpc, 21), (&mut guest_rpc, 105)] {
        let queue = wss_rpc(ws, id, "agent.getQueue", json!({ "agentId": agent_id })).await;
        assert!(
            queue["queue"].as_array().expect("queue array").is_empty(),
            "queue empty after flush: {queue}"
        );
    }
}
