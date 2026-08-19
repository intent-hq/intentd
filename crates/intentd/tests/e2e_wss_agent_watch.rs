//! WSS end-to-end for the agent event-subscription policy + `ws.agent.watch`
//! (monorepo#1229), driven through the REAL agent surface: a mock ACP child
//! whose `workspace_api` tool calls exercise the MCP bridge end-to-end.
//!
//! Covers the policy contract that the wire-level guard assertions in
//! `e2e_wss_event_subscription.rs` cannot reach:
//!  - an agent calling `ws.event.subscribe` with an `agent:`-prefixed type is
//!    rejected with the error pointing at `ws.agent.watch`; a bare `*`
//!    silently narrows to the non-agent categories;
//!  - `ws.agent.watch(agentId)` wakes the watcher on the target's idle,
//!    blocker (`ws.agent.reportBlocker`), discussion
//!    (`ws.agent.requestDiscussion`), and terminal failure; attention wakes
//!    never consume the watch, each completion wake retires it (deliver-once;
//!    the watcher re-arms with another `ws.agent.watch`); `ws.agent.unwatch`
//!    stops the wakes;
//!  - a bare-`*` agent subscription never wakes on another agent's
//!    message/tool-call/idle events but does wake on non-agent categories;
//!  - the FE `events.subscribe` stream still receives `agent:message` and
//!    `agent:tool:call` (the restriction is agent-caller-only);
//!  - agent-waiting deferral (monorepo#1468): a completion watch on an
//!    idle-but-waiting target (one that itself holds a live outgoing
//!    completion watch on a third agent) does not fire on the target's
//!    interim idle (stamped `isWaitingForOtherAgents: true`) — on both the
//!    live delivery path and the registration-time reconcile (re-arm on an
//!    already-idle-but-waiting target) — and delivers exactly once when the
//!    chain settles.
//!
//! Gated on `node` + the mock script; skips cleanly otherwise.
//!
//! The `ws.agent.watch` lifecycle is covered by one test per wake arm, and every
//! wait is clamped to a per-test [`TEST_BUDGET`] so a stall panics naming its
//! own step before nextest's 180s kill (monorepo#1562).

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

const TOKEN: &str = "cdcdcdcdcdcdcdcdcdcdcdcdcdcdcdcdcdcdcdcdcdcdcdcdcdcdcdcdcdcdcdcd";

type TlsWs = WebSocketStream<tokio_rustls::client::TlsStream<TcpStream>>;

struct Daemon {
    child: Child,
    data_dir: PathBuf,
}

impl Drop for Daemon {
    fn drop(&mut self) {
        let _ = self.child.kill();
        let _ = self.child.wait();
        if std::thread::panicking() {
            if let Ok(log) = std::fs::read_to_string(self.data_dir.join("daemon.log")) {
                eprintln!("=== DAEMON LOG ===\n{log}\n=== END LOG ===");
            }
        }
        let _ = std::fs::remove_dir_all(&self.data_dir);
    }
}

fn temp_data_dir() -> PathBuf {
    let id = Uuid::new_v4().simple().to_string();
    let dir = PathBuf::from("/tmp").join(format!("itd-wss-watch-{}", &id[..8]));
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

async fn await_uds(socket: &Path, deadline: tokio::time::Instant) -> bool {
    tokio::time::timeout_at(deadline, async {
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

async fn connect_ws(port: u16, cfg: Arc<ClientConfig>) -> TlsWs {
    let url = format!("wss://localhost:{port}/ws?token={TOKEN}");
    common::wss_connect_with_retry(port, cfg, &url).await
}

/// Whole-test wall-clock budget (monorepo#1562). nextest terminates these
/// binaries after `slow-timeout` 90s × `terminate-after` 2 = 180s, which masks
/// any in-test deadline that `INTENTD_TEST_TIMEOUT_MULTIPLIER` scales past it:
/// the run reports a bare "test timed out" instead of the step that stalled.
/// Daemon startup and every poll/event wait below is clamped to this deadline,
/// and each RPC round-trip is separately capped by [`rpc_read_budget`], so a
/// stall always panics naming its own step with headroom before the kill.
const TEST_BUDGET: Duration = Duration::from_secs(150);

/// Per-test deadline clamp, started at the top of each test — before the daemon
/// boots, so startup time counts against it.
#[derive(Clone, Copy)]
struct Budget {
    end: tokio::time::Instant,
}

impl Budget {
    fn start() -> Self {
        Self {
            end: tokio::time::Instant::now() + TEST_BUDGET,
        }
    }

    /// Deadline for one step: `secs` scaled by the multiplier, clamped to the
    /// whole-test budget.
    fn step(&self, secs: u64) -> tokio::time::Instant {
        let scaled = tokio::time::Instant::now() + common::test_timeout(Duration::from_secs(secs));
        scaled.min(self.end)
    }
}

/// Bound for one RPC round-trip. The shared `common::rpc_read_timeout` (60s
/// base) scales to the entire 180s kill window at multiplier 3, so clamp it
/// here — a live daemon answers in milliseconds.
fn rpc_read_budget() -> Duration {
    common::rpc_read_timeout().min(Duration::from_secs(45))
}

async fn wss_rpc(ws: &mut TlsWs, id: i64, method: &str, params: Value) -> Value {
    let frame = json!({ "jsonrpc": "2.0", "id": id, "method": method, "params": params });
    ws.send(Message::Text(frame.to_string().into()))
        .await
        .expect("send rpc frame");
    // One deadline for the whole round-trip, not per frame: heartbeat `Ping`s
    // and unrelated notifications must not extend the bound (monorepo#1562).
    let deadline = tokio::time::Instant::now() + rpc_read_budget();
    loop {
        let next = tokio::time::timeout_at(deadline, ws.next())
            .await
            .unwrap_or_else(|_| panic!("wss rpc {method} timed out"));
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

/// Read `events.event` notifications until `deadline`; `None` on timeout.
async fn wss_event_opt_until(ws: &mut TlsWs, deadline: tokio::time::Instant) -> Option<Value> {
    loop {
        let remaining = deadline.checked_duration_since(tokio::time::Instant::now())?;
        if remaining.is_zero() {
            return None;
        }
        let next = timeout(remaining, ws.next()).await.ok()?;
        match next {
            Some(Ok(Message::Text(text))) => {
                let v: Value = serde_json::from_str(&text).expect("json frame");
                if v["method"] == "events.event" {
                    return Some(v);
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
    if !Path::new(&script).exists() {
        eprintln!("skipping {test}: mock script missing at {script}");
        return None;
    }
    Some(script)
}

async fn seed_workspace_only(data_dir: &Path) -> String {
    use intent_core::{
        now_iso, Workspace, WorkspaceActivity, WorkspaceAttention, WorkspaceId, WorkspaceStatus,
    };
    use intent_store::Store;
    let store = Store::open(&data_dir.join("intentd.db"))
        .await
        .expect("open store");
    let ts = now_iso();
    let ws = WorkspaceId::new();
    store
        .insert_workspace(&Workspace {
            id: ws.clone(),
            title: "WSS-WATCH-E2E".to_string(),
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
            execution_environment: None,
            disk_usage: None,
            pending_delete_at: None,
        })
        .await
        .expect("insert ws");
    ws.0
}

/// Boot the daemon with the mock ACP provider + one FE subscriber connection
/// (`agent:*` + the named extra types) and one RPC connection.
struct Setup {
    _daemon: Daemon,
    ws_id: String,
    sub: TlsWs,
    rpc: TlsWs,
}

async fn boot_daemon(
    script: &str,
    behavior: &str,
    sub_event_types: Value,
    budget: Budget,
) -> Setup {
    let data_dir = temp_data_dir();
    let ws_id = seed_workspace_only(&data_dir).await;
    let env: [(&str, &str); 4] = [
        ("INTENTD_AUTH_TOKEN", TOKEN),
        ("INTENTD_TCP_PORT", "0"),
        ("MOCK_AGENT_SCRIPT_PATH", script),
        ("MOCK_AGENT_BEHAVIOR", behavior),
    ];
    let child = spawn_serve(&data_dir, &env);
    let daemon = Daemon {
        child,
        data_dir: data_dir.clone(),
    };
    let socket = data_dir.join("intentd.sock");
    // Startup is clamped to the same whole-test budget: the shared
    // `daemon_startup_timeout` / `await_wss_status` bounds are 60s each and
    // scale with the multiplier, so unclamped they can span the whole nextest
    // kill window on their own (monorepo#1562).
    assert!(
        await_uds(&socket, budget.step(60)).await,
        "daemon did not start"
    );
    let status = tokio::time::timeout_at(budget.step(60), common::await_wss_status(&socket))
        .await
        .expect("daemon wss status not ready within budget");
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
        json!({ "eventTypes": sub_event_types, "workspaceId": ws_id }),
    )
    .await;
    assert!(
        sub_resp["subscriptionId"].is_string(),
        "subscribed: {sub_resp}"
    );
    let rpc = connect_ws(port, cfg).await;
    Setup {
        _daemon: daemon,
        ws_id,
        sub,
        rpc,
    }
}

async fn create_agent(rpc: &mut TlsWs, id: i64, ws_id: &str, name: &str) -> String {
    let created = wss_rpc(
        rpc,
        id,
        "agent.create",
        json!({ "workspaceId": ws_id, "name": name, "model": "mock:default" }),
    )
    .await;
    created["agent"]["id"]
        .as_str()
        .expect("agent id")
        .to_string()
}

/// Await `agent:idle` for `agent_id` on the subscriber stream and return the
/// full event payload (for idle-annotation assertions such as
/// `isWaitingForOtherAgents`).
async fn await_idle_event(
    sub: &mut TlsWs,
    agent_id: &str,
    deadline: tokio::time::Instant,
) -> Value {
    loop {
        let frame = wss_event_opt_until(sub, deadline)
            .await
            .unwrap_or_else(|| panic!("timed out awaiting agent:idle for {agent_id}"));
        let event = &frame["params"]["event"];
        if event["data"]["agentId"].as_str() != Some(agent_id) {
            continue;
        }
        match event["type"].as_str() {
            Some("agent:idle") => return event.clone(),
            Some("agent:failed") => panic!("agent:failed while awaiting idle: {frame}"),
            _ => {}
        }
    }
}

/// Await `agent:idle` for `agent_id` on the subscriber stream.
async fn await_idle(sub: &mut TlsWs, agent_id: &str, deadline: tokio::time::Instant) {
    let _ = await_idle_event(sub, agent_id, deadline).await;
}

/// Serialized conversation text for an agent.
async fn conversation_text(rpc: &mut TlsWs, id: i64, ws_id: &str, agent_id: &str) -> String {
    let convo = wss_rpc(
        rpc,
        id,
        "agent.getConversation",
        json!({ "workspaceId": ws_id, "agentId": agent_id }),
    )
    .await;
    convo.to_string()
}

/// Poll the watcher's conversation until `needle` appears (or panic at the
/// deadline). Returns the conversation text containing the needle.
async fn await_conversation_contains(
    rpc: &mut TlsWs,
    req_id: &mut i64,
    ws_id: &str,
    agent_id: &str,
    needle: &str,
    deadline: tokio::time::Instant,
) -> String {
    loop {
        let text = conversation_text(rpc, *req_id, ws_id, agent_id).await;
        *req_id += 1;
        if text.contains(needle) {
            return text;
        }
        assert!(
            tokio::time::Instant::now() < deadline,
            "conversation for {agent_id} never contained {needle:?}: {text}"
        );
        tokio::time::sleep(Duration::from_millis(100)).await;
    }
}

/// Poll until the agent's conversation stops changing across two consecutive
/// reads 400ms apart (all queued wake turns drained). Returns the settled text.
async fn await_conversation_settled(
    rpc: &mut TlsWs,
    req_id: &mut i64,
    ws_id: &str,
    agent_id: &str,
    deadline: tokio::time::Instant,
) -> String {
    let mut prev = conversation_text(rpc, *req_id, ws_id, agent_id).await;
    *req_id += 1;
    loop {
        tokio::time::sleep(Duration::from_millis(400)).await;
        let next = conversation_text(rpc, *req_id, ws_id, agent_id).await;
        *req_id += 1;
        if next == prev {
            return next;
        }
        prev = next;
        assert!(
            tokio::time::Instant::now() < deadline,
            "conversation for {agent_id} never settled"
        );
    }
}

/// Serialize a conversation row's `contentBlocks` for substring assertions.
fn blocks_text(message: &Value) -> String {
    serde_json::to_string(&message["contentBlocks"]).unwrap_or_default()
}

/// Number of completion watches the agent owns on `target` (via the wire
/// `agent.getSubscriptions` introspection).
async fn watch_count_on_target(
    rpc: &mut TlsWs,
    id: i64,
    ws_id: &str,
    agent_id: &str,
    target: &str,
) -> usize {
    let subs = wss_rpc(
        rpc,
        id,
        "agent.getSubscriptions",
        json!({ "workspaceId": ws_id, "agentId": agent_id }),
    )
    .await;
    subs["subscriptions"]
        .as_array()
        .expect("subscriptions array")
        .iter()
        .filter(|s| s["actorIds"] == json!([target]))
        .count()
}

/// Poll until the agent owns exactly `expected` watches on `target`.
async fn await_watch_count(
    rpc: &mut TlsWs,
    req_id: &mut i64,
    ws_id: &str,
    agent_id: &str,
    target: &str,
    expected: usize,
    deadline: tokio::time::Instant,
) {
    loop {
        let n = watch_count_on_target(rpc, *req_id, ws_id, agent_id, target).await;
        *req_id += 1;
        if n == expected {
            return;
        }
        assert!(
            tokio::time::Instant::now() < deadline,
            "watch count on {target} never reached {expected} (last {n})"
        );
        tokio::time::sleep(Duration::from_millis(100)).await;
    }
}

/// POLICY-1 (monorepo#1229): through the real MCP bridge, an agent calling
/// `ws.event.subscribe` with an exact agent event type (`agent:message`,
/// `agent:tool:call`) is rejected with the error pointing at `ws.agent.watch`;
/// a bare `*` silently narrows (no `agent:*`, no `chat:stream:delta`; keeps
/// `file:*`). The narrowed live subscription then never wakes the subscriber
/// on another agent's message/tool-call/idle events but does wake on
/// `note:created` — while the FE `events.subscribe` stream still receives the
/// other agent's `agent:message` and `agent:tool:call`.
#[tokio::test]
async fn agent_subscribe_policy_and_bare_star_narrowing_via_mcp_over_wss() {
    let Some(script) = gate("WSS agent-subscribe policy E2E") else {
        return;
    };
    let budget = Budget::start();

    const SETUP_SUBS: &str = "WATCH1_SETUP_SUBS";
    const TARGET_GO: &str = "WATCH1_TARGET_GO";
    let subs_js = r#"
        const out = [];
        try { await ws.event.subscribe(['agent:message']); out.push('msgGuard=missing'); }
        catch (e) { out.push('msgGuard=' + (e.message.includes('ws.agent.watch') ? 'watch' : 'other')); }
        try { await ws.event.subscribe(['agent:tool:call']); out.push('toolGuard=missing'); }
        catch (e) { out.push('toolGuard=' + (e.message.includes('ws.agent.watch') ? 'watch' : 'other')); }
        try { await ws.event.subscribe(['agent:*']); out.push('wildGuard=missing'); }
        catch (e) { out.push('wildGuard=' + (e.message.includes('ws.agent.watch') ? 'watch' : 'other')); }
        const sub = await ws.event.subscribe(['*'], { batchWindow: 50 });
        out.push('starAgent=' + sub.eventTypes.includes('agent:*'));
        out.push('starDelta=' + sub.eventTypes.includes('chat:stream:delta'));
        out.push('starFile=' + sub.eventTypes.includes('file:*'));
        return out.join(' ');
    "#;
    let behavior = json!({
        "rules": [
            { "ifPromptContains": "[WORKSPACE EVENTS]", "response": "watcher acknowledged wake" },
            {
                "ifPromptContains": SETUP_SUBS,
                "toolCall": {
                    "name": "workspace_api",
                    "arguments": { "code": subs_js, "summary": "probe subscription policy" }
                },
                "emitToolBlocks": true,
                "response": "subs setup done",
            },
            {
                "ifPromptContains": TARGET_GO,
                "toolCall": {
                    "name": "workspace_api",
                    "arguments": { "code": "return await ws.workspace.info()", "summary": "target tool call" }
                },
                "emitToolBlocks": true,
                "response": "target turn done",
            },
        ],
    })
    .to_string();
    let mut setup = boot_daemon(&script, &behavior, json!(["agent:*"]), budget).await;
    let ws_id = setup.ws_id.clone();

    let watcher = create_agent(&mut setup.rpc, 10, &ws_id, "Watcher").await;
    let sent = wss_rpc(
        &mut setup.rpc,
        11,
        "agent.sendMessage",
        json!({ "workspaceId": ws_id, "agentId": watcher, "content": SETUP_SUBS }),
    )
    .await;
    assert_eq!(sent["success"], true, "watcher setup send ok: {sent}");
    await_idle(&mut setup.sub, &watcher, budget.step(60)).await;

    // The probe's tool result (persisted via emitToolBlocks) carries the
    // policy flags: both exact agent types rejected pointing at
    // ws.agent.watch; bare `*` narrowed.
    let mut req_id = 20i64;
    let text = await_conversation_contains(
        &mut setup.rpc,
        &mut req_id,
        &ws_id,
        &watcher,
        "msgGuard=",
        budget.step(30),
    )
    .await;
    for flag in [
        "msgGuard=watch",
        "toolGuard=watch",
        "wildGuard=watch",
        "starAgent=false",
        "starDelta=false",
        "starFile=true",
    ] {
        assert!(text.contains(flag), "policy flag {flag} in: {text}");
    }

    // Another agent's full turn: the FE subscriber (agent:*) must see its
    // agent:message and agent:tool:call — the restriction is
    // agent-caller-only, the FE stream is unaffected.
    let target = create_agent(&mut setup.rpc, 30, &ws_id, "Target").await;
    let sent = wss_rpc(
        &mut setup.rpc,
        31,
        "agent.sendMessage",
        json!({ "workspaceId": ws_id, "agentId": target, "content": TARGET_GO }),
    )
    .await;
    assert_eq!(sent["success"], true, "target send ok: {sent}");
    let deadline = budget.step(60);
    let mut saw_message = false;
    let mut saw_tool_call = false;
    let mut saw_idle = false;
    while !saw_idle {
        let frame = wss_event_opt_until(&mut setup.sub, deadline)
            .await
            .unwrap_or_else(|| {
                panic!("timed out: message={saw_message} toolCall={saw_tool_call} idle={saw_idle}")
            });
        let event = &frame["params"]["event"];
        if event["data"]["agentId"].as_str() != Some(target.as_str()) {
            continue;
        }
        match event["type"].as_str() {
            Some("agent:message") => saw_message = true,
            Some("agent:tool:call") => saw_tool_call = true,
            Some("agent:idle") => saw_idle = true,
            Some("agent:failed") => panic!("target turn failed: {frame}"),
            _ => {}
        }
    }
    assert!(saw_message, "FE stream received the target's agent:message");
    assert!(
        saw_tool_call,
        "FE stream received the target's agent:tool:call"
    );

    // A `note:created` (front door, non-agent category) DOES wake the
    // watcher's narrowed bare-* subscription.
    wss_rpc(
        &mut setup.rpc,
        40,
        "note.create",
        json!({ "workspaceId": ws_id, "title": "N1", "content": "one" }),
    )
    .await;
    await_conversation_contains(
        &mut setup.rpc,
        &mut req_id,
        &ws_id,
        &watcher,
        "note:created",
        budget.step(30),
    )
    .await;

    // Wake-row audit: the target's agent:message / agent:tool:call / agent:idle
    // all fired BEFORE the note event, so a policy leak would have delivered
    // an agent-event wake by now (same 50ms batch window). No wake row may
    // name an agent event.
    let convo = wss_rpc(
        &mut setup.rpc,
        50,
        "agent.getConversation",
        json!({ "workspaceId": ws_id, "agentId": watcher }),
    )
    .await;
    let messages = convo["messages"].as_array().expect("messages array");
    let wake_rows: Vec<String> = messages
        .iter()
        .filter(|m| m["role"] == "user")
        .map(blocks_text)
        .filter(|t| t.contains("[WORKSPACE EVENTS]"))
        .collect();
    assert!(
        wake_rows.iter().any(|t| t.contains("note:created")),
        "note:created wake row present: {wake_rows:?}"
    );
    for row in &wake_rows {
        for leaked in ["agent:message", "agent:tool:call", "agent:idle"] {
            assert!(
                !row.contains(leaked),
                "wake row must not carry {leaked}: {row}"
            );
        }
    }
}

// WATCH-1 (monorepo#1229): the `ws.agent.watch` lifecycle. Each wake arm below
// is a separate test (monorepo#1562): the arms are independent, and one
// daemon-boot-plus-five-target-turns test could not finish its scaled budgets
// inside nextest's 180s kill window, so a stall was reported as a bare timeout
// instead of a named step.
const DO_WATCH: &str = "WATCH2_DO_WATCH";
const DO_UNWATCH: &str = "WATCH2_DO_UNWATCH";
const TARGET_PLAIN1: &str = "WATCH2_PLAIN_ONE";
const TARGET_BLOCKER: &str = "WATCH2_BLOCKER";
const TARGET_DISCUSS: &str = "WATCH2_DISCUSS";
const TARGET_PLAIN2: &str = "WATCH2_PLAIN_TWO";
const TARGET_DIE: &str = "WATCH2_DIE";
const BLOCKER_REASON: &str = "WATCH2 sandbox is broken";
const DISCUSS_REASON: &str = "WATCH2 need a coordinator decision";

/// The mock behavior shared by every WATCH-2 arm.
fn watch_lifecycle_behavior() -> String {
    let watch_js = r#"
        const agents = await ws.agent.list();
        const t = agents.find(a => a.name === 'WatchTarget');
        const r = await ws.agent.watch(t.id);
        return 'watched=' + r.ok + ' watchTarget=' + r.agentId;
    "#;
    let unwatch_js = r#"
        const agents = await ws.agent.list();
        const t = agents.find(a => a.name === 'WatchTarget');
        const r = await ws.agent.unwatch(t.id);
        return 'unwatched=' + r.removed;
    "#;
    let blocker_js = format!(
        "return await ws.agent.reportBlocker({});",
        json!(BLOCKER_REASON)
    );
    let discuss_js = format!(
        "return await ws.agent.requestDiscussion({});",
        json!(DISCUSS_REASON)
    );
    json!({
        "exitIfPromptContains": TARGET_DIE,
        "rules": [
            { "ifPromptContains": "[WORKSPACE EVENTS]", "response": "watcher acknowledged wake" },
            {
                "ifPromptContains": DO_WATCH,
                "toolCall": {
                    "name": "workspace_api",
                    "arguments": { "code": watch_js, "summary": "register agent watch" }
                },
                "emitToolBlocks": true,
                "response": "watch registered",
            },
            {
                "ifPromptContains": DO_UNWATCH,
                "toolCall": {
                    "name": "workspace_api",
                    "arguments": { "code": unwatch_js, "summary": "remove agent watch" }
                },
                "emitToolBlocks": true,
                "response": "unwatch done",
            },
            {
                "ifPromptContains": TARGET_BLOCKER,
                "toolCall": {
                    "name": "workspace_api",
                    "arguments": { "code": blocker_js, "summary": "raise blocker" }
                },
                "response": "target raised a blocker",
            },
            {
                "ifPromptContains": TARGET_DISCUSS,
                "toolCall": {
                    "name": "workspace_api",
                    "arguments": { "code": discuss_js, "summary": "request discussion" }
                },
                "response": "target requested a discussion",
            },
            { "ifPromptContains": TARGET_PLAIN1, "response": "target plain turn one" },
            { "ifPromptContains": TARGET_PLAIN2, "response": "target plain turn two" },
        ],
    })
    .to_string()
}

/// One WATCH-2 arm's fixture: the daemon, the WatchTarget/Watcher pair, and the
/// watcher's first `ws.agent.watch(target)` already armed.
struct WatchLifecycle {
    setup: Setup,
    ws_id: String,
    target: String,
    watcher: String,
    req_id: i64,
}

async fn boot_watch_lifecycle(script: &str, budget: Budget) -> WatchLifecycle {
    let mut setup = boot_daemon(
        script,
        &watch_lifecycle_behavior(),
        json!(["agent:*"]),
        budget,
    )
    .await;
    let ws_id = setup.ws_id.clone();

    // Target FIRST (so the watcher's ws.agent.list lookup finds it), then the
    // watcher registers the watch through the bridge.
    let target = create_agent(&mut setup.rpc, 10, &ws_id, "WatchTarget").await;
    let watcher = create_agent(&mut setup.rpc, 11, &ws_id, "Watcher").await;
    let sent = wss_rpc(
        &mut setup.rpc,
        12,
        "agent.sendMessage",
        json!({ "workspaceId": ws_id, "agentId": watcher, "content": DO_WATCH }),
    )
    .await;
    assert_eq!(sent["success"], true, "watch send ok: {sent}");
    await_idle(&mut setup.sub, &watcher, budget.step(60)).await;
    let mut req_id = 20i64;
    let text = await_conversation_contains(
        &mut setup.rpc,
        &mut req_id,
        &ws_id,
        &watcher,
        "watched=true",
        budget.step(30),
    )
    .await;
    assert!(
        text.contains(&format!("watchTarget={target}")),
        "watch names the target: {text}"
    );
    WatchLifecycle {
        setup,
        ws_id,
        target,
        watcher,
        req_id,
    }
}

/// WATCH-2a: the target's idle completion wakes the watcher ("Child agent …
/// completed") and retires the watch (deliver-once).
#[tokio::test]
async fn agent_watch_wakes_on_target_idle_completion_over_wss() {
    let Some(script) = gate("WSS agent.watch idle-wake E2E") else {
        return;
    };
    let budget = Budget::start();
    let mut fx = boot_watch_lifecycle(&script, budget).await;

    let sent = wss_rpc(
        &mut fx.setup.rpc,
        30,
        "agent.sendMessage",
        json!({ "workspaceId": fx.ws_id, "agentId": fx.target, "content": TARGET_PLAIN1 }),
    )
    .await;
    assert_eq!(sent["success"], true, "target turn 1 ok: {sent}");
    let text = await_conversation_contains(
        &mut fx.setup.rpc,
        &mut fx.req_id,
        &fx.ws_id,
        &fx.watcher,
        "Child agent WatchTarget",
        budget.step(60),
    )
    .await;
    assert!(
        text.contains("completed."),
        "idle wake reports the target completed: {text}"
    );
    // monorepo#2051: the retiring wake says so explicitly and points at the
    // re-arm call.
    assert!(
        text.contains("the watch is now retired"),
        "idle wake states the watch retirement: {text}"
    );
    assert!(
        text.contains(&format!("ws.agent.watch(\\\"{}\\\")", fx.target)),
        "idle wake carries the re-arm instruction naming the target: {text}"
    );
    // monorepo#2060: the retiring wake's event_notification metadata carries
    // the machine-readable twin of the retirement note.
    assert!(
        text.contains("\"watchStillArmed\":false"),
        "idle wake metadata tags watchStillArmed=false: {text}"
    );
    await_watch_count(
        &mut fx.setup.rpc,
        &mut fx.req_id,
        &fx.ws_id,
        &fx.watcher,
        &fx.target,
        0,
        budget.step(60),
    )
    .await;
}

/// WATCH-2b: the target's blocker (`ws.agent.reportBlocker`) and discussion
/// (`ws.agent.requestDiscussion`) each deliver an attention wake carrying the
/// reason. An attention wake does not consume the watch — the trailing idle of
/// the same target turn does, so the watcher re-arms between the two turns.
#[tokio::test]
async fn agent_watch_wakes_on_blocker_and_discussion_attention_over_wss() {
    let Some(script) = gate("WSS agent.watch attention-wake E2E") else {
        return;
    };
    let budget = Budget::start();
    let mut fx = boot_watch_lifecycle(&script, budget).await;

    let sent = wss_rpc(
        &mut fx.setup.rpc,
        32,
        "agent.sendMessage",
        json!({ "workspaceId": fx.ws_id, "agentId": fx.target, "content": TARGET_BLOCKER }),
    )
    .await;
    assert_eq!(sent["success"], true, "target blocker turn ok: {sent}");
    let text = await_conversation_contains(
        &mut fx.setup.rpc,
        &mut fx.req_id,
        &fx.ws_id,
        &fx.watcher,
        "reports a blocker",
        budget.step(60),
    )
    .await;
    assert!(
        text.contains(BLOCKER_REASON),
        "blocker wake carries the reason: {text}"
    );
    // monorepo#2051: the attention wake is non-terminal — it states the
    // watch remains armed.
    assert!(
        text.contains("remains armed"),
        "blocker wake states the watch remains armed: {text}"
    );
    // monorepo#2060: the attention wake's event_notification metadata carries
    // the machine-readable twin of the "remains armed" note.
    assert!(
        text.contains("\"watchStillArmed\":true"),
        "blocker wake metadata tags watchStillArmed=true: {text}"
    );

    // The trailing idle of the blocker turn consumed the watch; re-arm, then
    // the discussion attention wake.
    await_watch_count(
        &mut fx.setup.rpc,
        &mut fx.req_id,
        &fx.ws_id,
        &fx.watcher,
        &fx.target,
        0,
        budget.step(60),
    )
    .await;
    let sent = wss_rpc(
        &mut fx.setup.rpc,
        33,
        "agent.sendMessage",
        json!({ "workspaceId": fx.ws_id, "agentId": fx.watcher, "content": DO_WATCH }),
    )
    .await;
    assert_eq!(sent["success"], true, "re-watch send ok: {sent}");
    await_watch_count(
        &mut fx.setup.rpc,
        &mut fx.req_id,
        &fx.ws_id,
        &fx.watcher,
        &fx.target,
        1,
        budget.step(60),
    )
    .await;
    let sent = wss_rpc(
        &mut fx.setup.rpc,
        34,
        "agent.sendMessage",
        json!({ "workspaceId": fx.ws_id, "agentId": fx.target, "content": TARGET_DISCUSS }),
    )
    .await;
    assert_eq!(sent["success"], true, "target discussion turn ok: {sent}");
    let text = await_conversation_contains(
        &mut fx.setup.rpc,
        &mut fx.req_id,
        &fx.ws_id,
        &fx.watcher,
        "requests a discussion",
        budget.step(60),
    )
    .await;
    assert!(
        text.contains(DISCUSS_REASON),
        "discussion wake carries the reason: {text}"
    );
}

/// WATCH-2c: `ws.agent.unwatch` removes the live watch, and a further target
/// completion delivers NO new wake.
#[tokio::test]
async fn agent_unwatch_stops_further_wakes_over_wss() {
    let Some(script) = gate("WSS agent.unwatch E2E") else {
        return;
    };
    let budget = Budget::start();
    let mut fx = boot_watch_lifecycle(&script, budget).await;

    // The fixture's watch must still be live going in, so a regression that
    // retires watches early fails here rather than as `unwatched=false` below.
    await_watch_count(
        &mut fx.setup.rpc,
        &mut fx.req_id,
        &fx.ws_id,
        &fx.watcher,
        &fx.target,
        1,
        budget.step(30),
    )
    .await;
    let sent = wss_rpc(
        &mut fx.setup.rpc,
        36,
        "agent.sendMessage",
        json!({ "workspaceId": fx.ws_id, "agentId": fx.watcher, "content": DO_UNWATCH }),
    )
    .await;
    assert_eq!(sent["success"], true, "unwatch send ok: {sent}");
    await_conversation_contains(
        &mut fx.setup.rpc,
        &mut fx.req_id,
        &fx.ws_id,
        &fx.watcher,
        "unwatched=true",
        budget.step(60),
    )
    .await;
    // The registry no longer holds a watcher→target watch.
    await_watch_count(
        &mut fx.setup.rpc,
        &mut fx.req_id,
        &fx.ws_id,
        &fx.watcher,
        &fx.target,
        0,
        budget.step(60),
    )
    .await;
    // Drain any wake turns still in flight before taking the baseline.
    let baseline = await_conversation_settled(
        &mut fx.setup.rpc,
        &mut fx.req_id,
        &fx.ws_id,
        &fx.watcher,
        budget.step(60),
    )
    .await;
    let sent = wss_rpc(
        &mut fx.setup.rpc,
        60,
        "agent.sendMessage",
        json!({ "workspaceId": fx.ws_id, "agentId": fx.target, "content": TARGET_PLAIN2 }),
    )
    .await;
    assert_eq!(sent["success"], true, "target turn 2 ok: {sent}");
    // Prove turn 2 finished via the target's OWN transcript (the shared
    // subscriber stream may hold stale buffered idles from earlier turns).
    await_conversation_contains(
        &mut fx.setup.rpc,
        &mut fx.req_id,
        &fx.ws_id,
        &fx.target,
        "target plain turn two",
        budget.step(60),
    )
    .await;
    tokio::time::sleep(Duration::from_millis(800)).await;
    let after = conversation_text(&mut fx.setup.rpc, 61, &fx.ws_id, &fx.watcher).await;
    assert_eq!(baseline, after, "no wake may be delivered after unwatch");
}

/// WATCH-2d: the target's terminal failure wakes the watcher ("Child agent …
/// failed") — the mock kills every attempt on the DIE marker, so the one-shot
/// silent redrive is spent and the failure goes terminal.
#[tokio::test]
async fn agent_watch_wakes_on_target_terminal_failure_over_wss() {
    let Some(script) = gate("WSS agent.watch failure-wake E2E") else {
        return;
    };
    let budget = Budget::start();
    let mut fx = boot_watch_lifecycle(&script, budget).await;

    let sent = wss_rpc(
        &mut fx.setup.rpc,
        63,
        "agent.sendMessage",
        json!({ "workspaceId": fx.ws_id, "agentId": fx.target, "content": TARGET_DIE }),
    )
    .await;
    assert_eq!(sent["success"], true, "target die turn accepted: {sent}");
    // The failure path includes a full silent-redrive cycle (kill + respawn +
    // re-prompt), so the window is generous. `format_completion_wake` renders
    // an AGENT_FAILED completion as "Child agent <label> failed." — the
    // `agent:failed` payload carries `agentName` (intent-hq/monorepo#2869), so
    // the label is the session name, not the bare agent id.
    let text = await_conversation_contains(
        &mut fx.setup.rpc,
        &mut fx.req_id,
        &fx.ws_id,
        &fx.watcher,
        "failed.",
        budget.step(90),
    )
    .await;
    assert!(
        text.contains("Child agent WatchTarget"),
        "failure wake names the target by its session name: {text}"
    );
    // monorepo#2051: the terminal failure wake retired the watch and says so.
    assert!(
        text.contains("the watch is now retired"),
        "failure wake states the watch retirement: {text}"
    );
    assert!(
        text.contains(&format!("ws.agent.watch(\\\"{}\\\")", fx.target)),
        "failure wake carries the re-arm instruction naming the target: {text}"
    );
    // monorepo#2060: the terminal failure wake's metadata also tags the
    // retirement machine-readably.
    assert!(
        text.contains("\"watchStillArmed\":false"),
        "failure wake metadata tags watchStillArmed=false: {text}"
    );
}

/// Number of the agent's persisted wake rows (user rows framed with
/// `[WORKSPACE EVENTS]`) whose text contains `needle`.
async fn wake_row_count(
    rpc: &mut TlsWs,
    id: i64,
    ws_id: &str,
    agent_id: &str,
    needle: &str,
) -> usize {
    let convo = wss_rpc(
        rpc,
        id,
        "agent.getConversation",
        json!({ "workspaceId": ws_id, "agentId": agent_id }),
    )
    .await;
    convo["messages"]
        .as_array()
        .expect("messages array")
        .iter()
        .filter(|m| m["role"] == "user")
        .map(blocks_text)
        .filter(|t| t.contains("[WORKSPACE EVENTS]") && t.contains(needle))
        .count()
}

/// DEFER-1 (monorepo#1468): live-path agent-waiting deferral through the real
/// MCP bridge. Coord watches Middle; Middle's turn registers its own watch on
/// Leaf, so Middle's idle is interim (stamped `isWaitingForOtherAgents: true`)
/// and Coord's watch neither delivers nor retires. When Leaf completes, the
/// chain settles bottom-up — Middle wakes, goes genuinely idle, and Coord
/// receives exactly ONE completion wake for Middle.
#[tokio::test]
async fn agent_waiting_defers_completion_watch_until_chain_settles_over_wss() {
    let Some(script) = gate("WSS agent-waiting deferral E2E") else {
        return;
    };
    let budget = Budget::start();

    const COORD_GO: &str = "WATCH3_COORD_GO";
    const MIDDLE_GO: &str = "WATCH3_MIDDLE_GO";
    const LEAF_GO: &str = "WATCH3_LEAF_GO";

    let coord_watch_js = r#"
        const agents = await ws.agent.list();
        const t = agents.find(a => a.name === 'DeferMiddle');
        const r = await ws.agent.watch(t.id);
        return 'coordWatched=' + r.ok;
    "#;
    let middle_watch_js = r#"
        const agents = await ws.agent.list();
        const t = agents.find(a => a.name === 'DeferLeaf');
        const r = await ws.agent.watch(t.id);
        return 'midWatched=' + r.ok;
    "#;
    let behavior = json!({
        "rules": [
            { "ifPromptContains": "[WORKSPACE EVENTS]", "response": "watcher acknowledged wake" },
            {
                "ifPromptContains": COORD_GO,
                "toolCall": {
                    "name": "workspace_api",
                    "arguments": { "code": coord_watch_js, "summary": "coord watches middle" }
                },
                "emitToolBlocks": true,
                "response": "coord watch registered",
            },
            {
                "ifPromptContains": MIDDLE_GO,
                "toolCall": {
                    "name": "workspace_api",
                    "arguments": { "code": middle_watch_js, "summary": "middle watches leaf" }
                },
                "emitToolBlocks": true,
                "response": "middle watch registered",
            },
            { "ifPromptContains": LEAF_GO, "response": "leaf turn done" },
        ],
    })
    .to_string();
    let mut setup = boot_daemon(&script, &behavior, json!(["agent:*"]), budget).await;
    let ws_id = setup.ws_id.clone();

    // Watch targets FIRST so the watchers' ws.agent.list lookups find them.
    let leaf = create_agent(&mut setup.rpc, 10, &ws_id, "DeferLeaf").await;
    let middle = create_agent(&mut setup.rpc, 11, &ws_id, "DeferMiddle").await;
    let coord = create_agent(&mut setup.rpc, 12, &ws_id, "DeferCoord").await;

    // Coord registers its watch on Middle through the bridge. Its own idle is
    // stamped interim — it now holds a live outgoing watch on Middle.
    let sent = wss_rpc(
        &mut setup.rpc,
        13,
        "agent.sendMessage",
        json!({ "workspaceId": ws_id, "agentId": coord, "content": COORD_GO }),
    )
    .await;
    assert_eq!(sent["success"], true, "coord watch send ok: {sent}");
    let coord_idle = await_idle_event(&mut setup.sub, &coord, budget.step(60)).await;
    assert_eq!(
        coord_idle["data"]["isWaitingForOtherAgents"],
        json!(true),
        "coord idle is stamped agent-waiting: {coord_idle}"
    );
    let mut req_id = 20i64;
    await_conversation_contains(
        &mut setup.rpc,
        &mut req_id,
        &ws_id,
        &coord,
        "coordWatched=true",
        budget.step(30),
    )
    .await;

    // Middle's turn registers ITS watch on Leaf, then ends. That idle is
    // interim (Middle waits on Leaf), so Coord's watch must neither deliver
    // nor retire.
    let sent = wss_rpc(
        &mut setup.rpc,
        30,
        "agent.sendMessage",
        json!({ "workspaceId": ws_id, "agentId": middle, "content": MIDDLE_GO }),
    )
    .await;
    assert_eq!(sent["success"], true, "middle send ok: {sent}");
    let middle_idle = await_idle_event(&mut setup.sub, &middle, budget.step(60)).await;
    assert_eq!(
        middle_idle["data"]["isWaitingForOtherAgents"],
        json!(true),
        "middle interim idle is stamped agent-waiting: {middle_idle}"
    );
    await_conversation_contains(
        &mut setup.rpc,
        &mut req_id,
        &ws_id,
        &middle,
        "midWatched=true",
        budget.step(30),
    )
    .await;

    // Deferred: no wake reached Coord and its watch on Middle stays armed.
    tokio::time::sleep(Duration::from_millis(800)).await;
    let text =
        await_conversation_settled(&mut setup.rpc, &mut req_id, &ws_id, &coord, budget.step(60))
            .await;
    assert!(
        !text.contains("Child agent"),
        "no completion wake may be delivered on the interim idle: {text}"
    );
    let n = watch_count_on_target(&mut setup.rpc, req_id, &ws_id, &coord, &middle).await;
    req_id += 1;
    assert_eq!(n, 1, "coord's watch on middle stays armed while deferred");

    // Leaf completes → Middle's watch fires → Middle's wake turn ends in a
    // GENUINE idle (its watch on Leaf was retired at delivery) → Coord's
    // deferred watch finally delivers, exactly once.
    let sent = wss_rpc(
        &mut setup.rpc,
        50,
        "agent.sendMessage",
        json!({ "workspaceId": ws_id, "agentId": leaf, "content": LEAF_GO }),
    )
    .await;
    assert_eq!(sent["success"], true, "leaf send ok: {sent}");
    let text = await_conversation_contains(
        &mut setup.rpc,
        &mut req_id,
        &ws_id,
        &coord,
        "Child agent DeferMiddle",
        budget.step(90),
    )
    .await;
    assert!(
        text.contains("completed."),
        "settlement wake reports middle completed: {text}"
    );
    await_watch_count(
        &mut setup.rpc,
        &mut req_id,
        &ws_id,
        &coord,
        &middle,
        0,
        budget.step(60),
    )
    .await;
    // Exactly-one wake: drain Coord's wake turns, then count the persisted
    // wake rows naming Middle.
    await_conversation_settled(&mut setup.rpc, &mut req_id, &ws_id, &coord, budget.step(60)).await;
    let wakes = wake_row_count(
        &mut setup.rpc,
        req_id,
        &ws_id,
        &coord,
        "Child agent DeferMiddle",
    )
    .await;
    assert_eq!(wakes, 1, "exactly one completion wake for middle");
}

/// DEFER-2 (monorepo#1468): registration-time reconcile honors the deferral.
/// Arming `ws.agent.watch` on a target that is ALREADY idle-but-agent-waiting
/// (Middle idled holding its own live watch on Leaf) must not fire the
/// synthetic completion — the watch stays armed until the chain settles, then
/// delivers exactly once.
#[tokio::test]
async fn agent_watch_rearm_on_idle_but_waiting_target_defers_over_wss() {
    let Some(script) = gate("WSS agent-waiting re-arm deferral E2E") else {
        return;
    };
    let budget = Budget::start();

    const MIDDLE_GO: &str = "WATCH4_MIDDLE_GO";
    const REARM_GO: &str = "WATCH4_REARM_GO";
    const LEAF_GO: &str = "WATCH4_LEAF_GO";

    let middle_watch_js = r#"
        const agents = await ws.agent.list();
        const t = agents.find(a => a.name === 'RearmLeaf');
        const r = await ws.agent.watch(t.id);
        return 'midWatched=' + r.ok;
    "#;
    let rearm_js = r#"
        const agents = await ws.agent.list();
        const t = agents.find(a => a.name === 'RearmMiddle');
        const r = await ws.agent.watch(t.id);
        return 'rearmed=' + r.ok;
    "#;
    let behavior = json!({
        "rules": [
            { "ifPromptContains": "[WORKSPACE EVENTS]", "response": "watcher acknowledged wake" },
            {
                "ifPromptContains": MIDDLE_GO,
                "toolCall": {
                    "name": "workspace_api",
                    "arguments": { "code": middle_watch_js, "summary": "middle watches leaf" }
                },
                "emitToolBlocks": true,
                "response": "middle watch registered",
            },
            {
                "ifPromptContains": REARM_GO,
                "toolCall": {
                    "name": "workspace_api",
                    "arguments": { "code": rearm_js, "summary": "watch the idle-but-waiting middle" }
                },
                "emitToolBlocks": true,
                "response": "rearm done",
            },
            { "ifPromptContains": LEAF_GO, "response": "leaf turn done" },
        ],
    })
    .to_string();
    let mut setup = boot_daemon(&script, &behavior, json!(["agent:*"]), budget).await;
    let ws_id = setup.ws_id.clone();

    let leaf = create_agent(&mut setup.rpc, 10, &ws_id, "RearmLeaf").await;
    let middle = create_agent(&mut setup.rpc, 11, &ws_id, "RearmMiddle").await;
    let watcher = create_agent(&mut setup.rpc, 12, &ws_id, "Rearmer").await;

    // Middle runs FIRST: it watches Leaf, then idles — the idle-but-waiting
    // shape the re-arm reconcile must treat as interim.
    let sent = wss_rpc(
        &mut setup.rpc,
        13,
        "agent.sendMessage",
        json!({ "workspaceId": ws_id, "agentId": middle, "content": MIDDLE_GO }),
    )
    .await;
    assert_eq!(sent["success"], true, "middle send ok: {sent}");
    let middle_idle = await_idle_event(&mut setup.sub, &middle, budget.step(60)).await;
    assert_eq!(
        middle_idle["data"]["isWaitingForOtherAgents"],
        json!(true),
        "middle interim idle is stamped agent-waiting: {middle_idle}"
    );
    let mut req_id = 20i64;
    await_conversation_contains(
        &mut setup.rpc,
        &mut req_id,
        &ws_id,
        &middle,
        "midWatched=true",
        budget.step(30),
    )
    .await;

    // NOW the watcher arms a watch on the already-idle Middle. Without the
    // deferral the registration-time reconcile would fire an instant
    // synthetic "completed" wake off the RuntimeIdle status.
    let sent = wss_rpc(
        &mut setup.rpc,
        30,
        "agent.sendMessage",
        json!({ "workspaceId": ws_id, "agentId": watcher, "content": REARM_GO }),
    )
    .await;
    assert_eq!(sent["success"], true, "rearm send ok: {sent}");
    await_conversation_contains(
        &mut setup.rpc,
        &mut req_id,
        &ws_id,
        &watcher,
        "rearmed=true",
        budget.step(60),
    )
    .await;

    // No synthetic completion: the watcher's transcript stays wake-free and
    // its watch on Middle stays armed.
    tokio::time::sleep(Duration::from_millis(800)).await;
    let text = await_conversation_settled(
        &mut setup.rpc,
        &mut req_id,
        &ws_id,
        &watcher,
        budget.step(60),
    )
    .await;
    assert!(
        !text.contains("Child agent"),
        "re-arm on an idle-but-waiting target must not fire synthetically: {text}"
    );
    let n = watch_count_on_target(&mut setup.rpc, req_id, &ws_id, &watcher, &middle).await;
    req_id += 1;
    assert_eq!(n, 1, "watch on the waiting middle stays armed");

    // Leaf completes → Middle wakes and settles → the watcher's deferred
    // watch delivers exactly once.
    let sent = wss_rpc(
        &mut setup.rpc,
        50,
        "agent.sendMessage",
        json!({ "workspaceId": ws_id, "agentId": leaf, "content": LEAF_GO }),
    )
    .await;
    assert_eq!(sent["success"], true, "leaf send ok: {sent}");
    let text = await_conversation_contains(
        &mut setup.rpc,
        &mut req_id,
        &ws_id,
        &watcher,
        "Child agent RearmMiddle",
        budget.step(90),
    )
    .await;
    assert!(
        text.contains("completed."),
        "settlement wake reports middle completed: {text}"
    );
    await_watch_count(
        &mut setup.rpc,
        &mut req_id,
        &ws_id,
        &watcher,
        &middle,
        0,
        budget.step(60),
    )
    .await;
    await_conversation_settled(
        &mut setup.rpc,
        &mut req_id,
        &ws_id,
        &watcher,
        budget.step(60),
    )
    .await;
    let wakes = wake_row_count(
        &mut setup.rpc,
        req_id,
        &ws_id,
        &watcher,
        "Child agent RearmMiddle",
    )
    .await;
    assert_eq!(wakes, 1, "exactly one completion wake for middle");
}

/// Poll `agent.list` until an agent named `name` appears; returns its id.
/// Needed for agents spawned by another agent's turn (`ws.agent.create`),
/// whose id the test does not learn from its own RPC call.
async fn await_agent_id_by_name(
    rpc: &mut TlsWs,
    req_id: &mut i64,
    ws_id: &str,
    name: &str,
    deadline: tokio::time::Instant,
) -> String {
    loop {
        let list = wss_rpc(rpc, *req_id, "agent.list", json!({ "workspaceId": ws_id })).await;
        *req_id += 1;
        if let Some(agent) = list["agents"]
            .as_array()
            .expect("agents array")
            .iter()
            .find(|a| a["name"] == json!(name))
        {
            return agent["id"].as_str().expect("agent id").to_string();
        }
        assert!(
            tokio::time::Instant::now() < deadline,
            "agent named {name} never appeared: {list}"
        );
        tokio::time::sleep(Duration::from_millis(100)).await;
    }
}

/// monorepo#2532 Gap A: re-arming after the reportToParent wake ADOPTS the
/// parent's surviving watch and must reset `report_delivered` so the child's
/// next genuine completion delivers. The child idles agent-waiting (own live
/// watch on a leaf, the #1468 deferral the report does NOT bypass), keeping
/// the reported-on watch alive for the adoption; pre-fix the adopted watch
/// kept the stale flag and the final idle was suppressed + silently retired
/// (no wake, ever).
#[tokio::test]
async fn agent_watch_rearm_adoption_after_report_fires_next_completion_over_wss() {
    let Some(script) = gate("WSS re-arm adoption after report E2E") else {
        return;
    };
    let budget = Budget::start();

    const SPAWN_GO: &str = "WATCH5_SPAWN_GO";
    const CHILD_GO: &str = "WATCH5_CHILD_GO";
    const REARM_GO: &str = "WATCH5_REARM_GO";
    const LEAF_GO: &str = "WATCH5_LEAF_GO";
    const REPORT: &str = "WATCH5_REPORT leaf watch armed; waiting on it";

    let spawn_js = format!(
        "const r = await ws.agent.create('AdoptChild', '{CHILD_GO} do your work', \
         {{ model: 'mock:default' }}); return 'spawned=' + r.ok;"
    );
    let child_watch_js = r#"
        const agents = await ws.agent.list();
        const t = agents.find(a => a.name === 'AdoptLeaf');
        const r = await ws.agent.watch(t.id);
        return 'leafWatched=' + r.ok;
    "#;
    let child_report_js = format!("return await ws.agent.reportToParent({});", json!(REPORT));
    let rearm_js = r#"
        const agents = await ws.agent.list();
        const t = agents.find(a => a.name === 'AdoptChild');
        const r = await ws.agent.watch(t.id);
        return 'rearmed=' + r.ok;
    "#;
    // Wake-ack rules FIRST: every wake turn (report wake, hook/completion
    // wakes) must ack, never re-run a marker rule off replayed history.
    let behavior = json!({
        "rules": [
            { "ifPromptContains": "[WORKSPACE EVENTS]", "response": "wake acknowledged" },
            {
                "ifPromptContains": SPAWN_GO,
                "toolCall": {
                    "name": "workspace_api",
                    "arguments": { "code": spawn_js, "summary": "spawn the reporting child" }
                },
                "emitToolBlocks": true,
                "response": "child spawned",
            },
            {
                "ifPromptContains": CHILD_GO,
                "toolCalls": [
                    {
                        "name": "workspace_api",
                        "arguments": { "code": child_watch_js, "summary": "child watches leaf" }
                    },
                    {
                        "name": "workspace_api",
                        "arguments": { "code": child_report_js, "summary": "child reports" }
                    },
                ],
                "emitToolBlocks": true,
                "response": "child parked on leaf",
            },
            {
                "ifPromptContains": REARM_GO,
                "toolCall": {
                    "name": "workspace_api",
                    "arguments": { "code": rearm_js, "summary": "re-arm the reported-on child" }
                },
                "emitToolBlocks": true,
                "response": "rearm done",
            },
            { "ifPromptContains": LEAF_GO, "response": "leaf turn done" },
        ],
    })
    .to_string();
    let mut setup = boot_daemon(&script, &behavior, json!(["agent:*"]), budget).await;
    let ws_id = setup.ws_id.clone();

    // Leaf FIRST (the child's ws.agent.list lookup must find it), then the
    // parent, who spawns the child through the bridge (parent linkage is what
    // makes reportToParent legal and arms the auto parent→child watch).
    let _leaf = create_agent(&mut setup.rpc, 10, &ws_id, "AdoptLeaf").await;
    let parent = create_agent(&mut setup.rpc, 11, &ws_id, "AdoptParent").await;
    let sent = wss_rpc(
        &mut setup.rpc,
        12,
        "agent.sendMessage",
        json!({ "workspaceId": ws_id, "agentId": parent, "content": SPAWN_GO }),
    )
    .await;
    assert_eq!(sent["success"], true, "spawn send ok: {sent}");
    let mut req_id = 20i64;
    let child = await_agent_id_by_name(
        &mut setup.rpc,
        &mut req_id,
        &ws_id,
        "AdoptChild",
        budget.step(60),
    )
    .await;

    // Child's kickoff turn: watches the leaf, reports (immediate parent wake
    // flips the auto watch to report_delivered), then idles agent-waiting —
    // the reported-on watch SURVIVES this interim idle.
    let child_idle = await_idle_event(&mut setup.sub, &child, budget.step(90)).await;
    assert_eq!(
        child_idle["data"]["isWaitingForOtherAgents"],
        json!(true),
        "child interim idle is stamped agent-waiting: {child_idle}"
    );
    let text = await_conversation_contains(
        &mut setup.rpc,
        &mut req_id,
        &ws_id,
        &parent,
        "reported. Report:",
        budget.step(60),
    )
    .await;
    assert!(
        text.contains("consumed your one-shot watch"),
        "report wake disclosed the disarm: {text}"
    );
    let n = watch_count_on_target(&mut setup.rpc, req_id, &ws_id, &parent, &child).await;
    req_id += 1;
    assert_eq!(n, 1, "reported-on watch survives the agent-waiting idle");

    // Re-arm: adoption of the surviving pair must reset report_delivered
    // (Gap A) and the registration-time reconcile defers (#1468) — no
    // instant synthetic wake.
    let sent = wss_rpc(
        &mut setup.rpc,
        30,
        "agent.sendMessage",
        json!({ "workspaceId": ws_id, "agentId": parent, "content": REARM_GO }),
    )
    .await;
    assert_eq!(sent["success"], true, "rearm send ok: {sent}");
    await_conversation_contains(
        &mut setup.rpc,
        &mut req_id,
        &ws_id,
        &parent,
        "rearmed=true",
        budget.step(60),
    )
    .await;
    tokio::time::sleep(Duration::from_millis(800)).await;
    let text = await_conversation_settled(
        &mut setup.rpc,
        &mut req_id,
        &ws_id,
        &parent,
        budget.step(60),
    )
    .await;
    assert!(
        !text.contains("completed."),
        "re-arm must not fire a synthetic completion: {text}"
    );
    let n = watch_count_on_target(&mut setup.rpc, req_id, &ws_id, &parent, &child).await;
    req_id += 1;
    assert_eq!(n, 1, "adopted watch stays armed (no duplicate)");

    // Leaf completes → the child's own watch fires its wake turn → the child
    // idles GENUINELY → the re-armed watch must deliver (pre-fix: the stale
    // report_delivered flag suppressed this idle and retired the watch with
    // no wake).
    let leaf = await_agent_id_by_name(
        &mut setup.rpc,
        &mut req_id,
        &ws_id,
        "AdoptLeaf",
        budget.step(30),
    )
    .await;
    let sent = wss_rpc(
        &mut setup.rpc,
        50,
        "agent.sendMessage",
        json!({ "workspaceId": ws_id, "agentId": leaf, "content": LEAF_GO }),
    )
    .await;
    assert_eq!(sent["success"], true, "leaf send ok: {sent}");
    let text = await_conversation_contains(
        &mut setup.rpc,
        &mut req_id,
        &ws_id,
        &parent,
        "completed.",
        budget.step(90),
    )
    .await;
    assert!(
        text.contains(&format!("Child agent AdoptChild ({child})")),
        "completion wake names the child: {text}"
    );
    await_conversation_settled(
        &mut setup.rpc,
        &mut req_id,
        &ws_id,
        &parent,
        budget.step(60),
    )
    .await;
    let wakes = wake_row_count(&mut setup.rpc, req_id, &ws_id, &parent, "completed.").await;
    assert_eq!(wakes, 1, "exactly one completion wake after the re-arm");
}

/// monorepo#2889: same-cycle suppression over the real WSS transport. The
/// child reports (immediate parent wake, report body included) and then
/// lingers in the SAME turn (mock silent tail); the parent re-arms mid-cycle
/// (adoption clears `report_delivered` — Gap A fresh interest). When the
/// child's turn resolves and it goes GENUINELY idle, the settlement must NOT
/// re-embed the already-delivered report: the report-time delivery marker
/// suppresses the completion wake and leaves the watch armed, and the
/// child's NEXT cycle still fires exactly one completion wake. Pre-fix, the
/// parent transcript carried the identical report twice in one cycle.
#[tokio::test]
async fn report_wake_then_rearm_suppresses_same_cycle_completion_over_wss() {
    let Some(script) = gate("WSS same-cycle report dedup E2E") else {
        return;
    };
    let budget = Budget::start();

    const SPAWN_GO: &str = "WATCH7_SPAWN_GO";
    const CHILD_GO: &str = "WATCH7_CHILD_GO";
    const REARM_GO: &str = "WATCH7_REARM_GO";
    const CHILD2_GO: &str = "WATCH7_CHILD2_GO";
    const REPORT: &str = "WATCH7_REPORT shipped the thing";

    let spawn_js = format!(
        "const r = await ws.agent.create('DedupChild', '{CHILD_GO} do your work', \
         {{ model: 'mock:default' }}); return 'spawned=' + r.ok;"
    );
    let child_report_js = format!("return await ws.agent.reportToParent({});", json!(REPORT));
    let rearm_js = r#"
        const agents = await ws.agent.list();
        const t = agents.find(a => a.name === 'DedupChild');
        const r = await ws.agent.watch(t.id);
        return 'rearmed=' + r.ok;
    "#;
    // Wake-ack rule FIRST: the report wake and any completion wake ack,
    // never re-run a marker rule off replayed history. The child's reporting
    // rule parks a silent tail AFTER the report tool call and BEFORE the
    // prompt resolves — the in-turn window where the parent's re-arm lands,
    // keeping the child's eventual idle in the SAME report cycle.
    let behavior = json!({
        "rules": [
            { "ifPromptContains": "[WORKSPACE EVENTS]", "response": "wake acknowledged" },
            {
                "ifPromptContains": SPAWN_GO,
                "toolCall": {
                    "name": "workspace_api",
                    "arguments": { "code": spawn_js, "summary": "spawn the reporting child" }
                },
                "emitToolBlocks": true,
                "response": "child spawned",
            },
            {
                "ifPromptContains": CHILD_GO,
                "toolCall": {
                    "name": "workspace_api",
                    "arguments": { "code": child_report_js, "summary": "child reports" }
                },
                "emitToolBlocks": true,
                "silentTailBeforeResultMs": 5000,
                "response": "child lingered then finished",
            },
            {
                "ifPromptContains": REARM_GO,
                "toolCall": {
                    "name": "workspace_api",
                    "arguments": { "code": rearm_js, "summary": "re-arm the reported-on child" }
                },
                "emitToolBlocks": true,
                "response": "rearm done",
            },
            { "ifPromptContains": CHILD2_GO, "response": "second turn done" },
        ],
    })
    .to_string();
    let mut setup = boot_daemon(&script, &behavior, json!(["agent:*"]), budget).await;
    let ws_id = setup.ws_id.clone();

    // Parent spawns the child through the bridge: parent linkage makes
    // reportToParent legal and arms the auto parent→child watch.
    let parent = create_agent(&mut setup.rpc, 10, &ws_id, "DedupParent").await;
    let sent = wss_rpc(
        &mut setup.rpc,
        11,
        "agent.sendMessage",
        json!({ "workspaceId": ws_id, "agentId": parent, "content": SPAWN_GO }),
    )
    .await;
    assert_eq!(sent["success"], true, "spawn send ok: {sent}");
    let mut req_id = 20i64;
    let child = await_agent_id_by_name(
        &mut setup.rpc,
        &mut req_id,
        &ws_id,
        "DedupChild",
        budget.step(60),
    )
    .await;

    // Report-time wake: delivered immediately, report body included, the
    // auto watch flipped to report_delivered. The child is still mid-turn
    // (silent tail running).
    let text = await_conversation_contains(
        &mut setup.rpc,
        &mut req_id,
        &ws_id,
        &parent,
        "reported. Report:",
        budget.step(60),
    )
    .await;
    assert!(
        text.contains("consumed your one-shot watch"),
        "report wake disclosed the disarm: {text}"
    );

    // Mid-cycle re-arm: adoption clears report_delivered (Gap A fresh
    // interest) — pre-fix, this reopened the settlement to the duplicate.
    let sent = wss_rpc(
        &mut setup.rpc,
        30,
        "agent.sendMessage",
        json!({ "workspaceId": ws_id, "agentId": parent, "content": REARM_GO }),
    )
    .await;
    assert_eq!(sent["success"], true, "rearm send ok: {sent}");
    await_conversation_contains(
        &mut setup.rpc,
        &mut req_id,
        &ws_id,
        &parent,
        "rearmed=true",
        budget.step(60),
    )
    .await;

    // The child's silent tail expires and it goes GENUINELY idle — the
    // same-cycle settlement. The marker recorded at report delivery must
    // suppress the completion wake.
    let child_idle = await_idle_event(&mut setup.sub, &child, budget.step(90)).await;
    assert_ne!(
        child_idle["data"]["isWaitingForOtherAgents"],
        json!(true),
        "child idle is genuine (no interim deferral): {child_idle}"
    );
    let text = await_conversation_settled(
        &mut setup.rpc,
        &mut req_id,
        &ws_id,
        &parent,
        budget.step(60),
    )
    .await;
    assert!(
        !text.contains("completed."),
        "same-cycle settlement must not re-deliver the heard report: {text}"
    );
    let reports =
        wake_row_count(&mut setup.rpc, req_id, &ws_id, &parent, "reported. Report:").await;
    req_id += 1;
    assert_eq!(reports, 1, "exactly one report wake in the cycle");
    let n = watch_count_on_target(&mut setup.rpc, req_id, &ws_id, &parent, &child).await;
    req_id += 1;
    assert_eq!(n, 1, "suppressed watch stays armed for a future completion");

    // FUTURE cycle: a new child turn (turn start clears the persisted
    // report) idles genuinely — the re-armed watch fires exactly one
    // completion wake and retires.
    let sent = wss_rpc(
        &mut setup.rpc,
        40,
        "agent.sendMessage",
        json!({ "workspaceId": ws_id, "agentId": child, "content": CHILD2_GO }),
    )
    .await;
    assert_eq!(sent["success"], true, "second child send ok: {sent}");
    let text = await_conversation_contains(
        &mut setup.rpc,
        &mut req_id,
        &ws_id,
        &parent,
        "completed.",
        budget.step(90),
    )
    .await;
    assert!(
        text.contains(&format!("Child agent DedupChild ({child})")),
        "future-cycle completion wake names the child: {text}"
    );
    await_conversation_settled(
        &mut setup.rpc,
        &mut req_id,
        &ws_id,
        &parent,
        budget.step(60),
    )
    .await;
    let wakes = wake_row_count(&mut setup.rpc, req_id, &ws_id, &parent, "completed.").await;
    req_id += 1;
    assert_eq!(wakes, 1, "exactly one completion wake on the future cycle");
    let n = watch_count_on_target(&mut setup.rpc, req_id, &ws_id, &parent, &child).await;
    assert_eq!(n, 0, "watch retired at the future completion");
}

/// monorepo#2532 Gap B: arming a watch on a child that REPORTED and idled
/// while holding an ACTIVE background hook must not fire instantly with the
/// stale report — the registration-time reconcile defers, the watch stays
/// armed, and the hook's dispatch (child's next real completion) delivers
/// exactly once, without the stale report.
#[tokio::test]
async fn agent_watch_on_reported_hook_waiting_child_defers_over_wss() {
    let Some(script) = gate("WSS hook-waiting registration deferral E2E") else {
        return;
    };
    let budget = Budget::start();

    const SPAWN_GO: &str = "WATCH6_SPAWN_GO";
    const CHILD_GO: &str = "WATCH6_CHILD_GO";
    const ARM_GO: &str = "WATCH6_ARM_GO";
    const REPORT: &str = "WATCH6_REPORT PR ready; hook stays armed for late comments";

    let spawn_js = format!(
        "const r = await ws.agent.create('HookChild', '{CHILD_GO} do your work', \
         {{ model: 'mock:default' }}); return 'spawned=' + r.ok;"
    );
    // Armed-timer hook: the immediate validation run holds (state marker),
    // every later run (driven by hook.runNow below) dispatches — the
    // one-shot dispatch is the hook's terminal transition.
    let child_hook_js = format!(
        "const r = await ws.hook.schedule({{ name: 'late-watch', code: {}, delayMs: 60000 }}); \
         return 'hooked=' + r.hook.state;",
        json!(
            "if (hookState === null) { return { dispatch: false, state: { armed: true } }; } \
             return { dispatch: true, message: 'late change detected' };"
        )
    );
    let child_report_js = format!("return await ws.agent.reportToParent({});", json!(REPORT));
    let arm_js = r#"
        const agents = await ws.agent.list();
        const t = agents.find(a => a.name === 'HookChild');
        const r = await ws.agent.watch(t.id);
        return 'armed=' + r.ok;
    "#;
    let behavior = json!({
        "rules": [
            { "ifPromptContains": "[Background hook", "response": "hook wake handled" },
            { "ifPromptContains": "[WORKSPACE EVENTS]", "response": "wake acknowledged" },
            {
                "ifPromptContains": SPAWN_GO,
                "toolCall": {
                    "name": "workspace_api",
                    "arguments": { "code": spawn_js, "summary": "spawn the hooked child" }
                },
                "emitToolBlocks": true,
                "response": "child spawned",
            },
            {
                "ifPromptContains": CHILD_GO,
                "toolCalls": [
                    {
                        "name": "workspace_api",
                        "arguments": { "code": child_hook_js, "summary": "child schedules hook" }
                    },
                    {
                        "name": "workspace_api",
                        "arguments": { "code": child_report_js, "summary": "child reports" }
                    },
                ],
                "emitToolBlocks": true,
                "response": "child settled behind its hook",
            },
            {
                "ifPromptContains": ARM_GO,
                "toolCall": {
                    "name": "workspace_api",
                    "arguments": { "code": arm_js, "summary": "watch the reported hooked child" }
                },
                "emitToolBlocks": true,
                "response": "arm done",
            },
        ],
    })
    .to_string();
    let mut setup = boot_daemon(&script, &behavior, json!(["agent:*"]), budget).await;
    let ws_id = setup.ws_id.clone();

    // The spawner is the child's parent (reportToParent target); the WATCHER
    // is a separate fresh agent whose NEW registration is the gap under test.
    let spawner = create_agent(&mut setup.rpc, 10, &ws_id, "HookSpawner").await;
    let watcher = create_agent(&mut setup.rpc, 11, &ws_id, "HookWatcher").await;
    let sent = wss_rpc(
        &mut setup.rpc,
        12,
        "agent.sendMessage",
        json!({ "workspaceId": ws_id, "agentId": spawner, "content": SPAWN_GO }),
    )
    .await;
    assert_eq!(sent["success"], true, "spawn send ok: {sent}");
    let mut req_id = 20i64;
    let child = await_agent_id_by_name(
        &mut setup.rpc,
        &mut req_id,
        &ws_id,
        "HookChild",
        budget.step(60),
    )
    .await;

    // Child schedules its hook, reports, and idles: the report SETTLES it
    // despite the active hook (#1945) — the spawner's auto watch retires on
    // the suppressed idle, leaving the child RuntimeIdle with a persisted
    // report and an active hook.
    let child_idle = await_idle_event(&mut setup.sub, &child, budget.step(90)).await;
    assert!(
        child_idle["data"]["waitingOnHooks"].is_array(),
        "child idle is stamped hook-waiting: {child_idle}"
    );
    await_conversation_contains(
        &mut setup.rpc,
        &mut req_id,
        &ws_id,
        &spawner,
        "reported. Report:",
        budget.step(60),
    )
    .await;
    await_watch_count(
        &mut setup.rpc,
        &mut req_id,
        &ws_id,
        &spawner,
        &child,
        0,
        budget.step(60),
    )
    .await;

    // The watcher arms a FRESH watch on the reported, hook-holding idle
    // child. Pre-fix the registration-time reconcile synthesized an instant
    // idle whose report bypassed the hook deferral — an immediate wake with
    // the STALE report.
    let sent = wss_rpc(
        &mut setup.rpc,
        30,
        "agent.sendMessage",
        json!({ "workspaceId": ws_id, "agentId": watcher, "content": ARM_GO }),
    )
    .await;
    assert_eq!(sent["success"], true, "arm send ok: {sent}");
    await_conversation_contains(
        &mut setup.rpc,
        &mut req_id,
        &ws_id,
        &watcher,
        "armed=true",
        budget.step(60),
    )
    .await;
    tokio::time::sleep(Duration::from_millis(800)).await;
    let text = await_conversation_settled(
        &mut setup.rpc,
        &mut req_id,
        &ws_id,
        &watcher,
        budget.step(60),
    )
    .await;
    assert!(
        !text.contains("Child agent"),
        "watch on a reported hook-waiting child must not fire instantly: {text}"
    );
    let n = watch_count_on_target(&mut setup.rpc, req_id, &ws_id, &watcher, &child).await;
    req_id += 1;
    assert_eq!(n, 1, "deferred watch stays armed");

    // Fire the hook (its terminal one-shot dispatch): the child's wake turn
    // ends in its REAL completion — the deferred watch delivers exactly
    // once, without the stale report (cleared at the wake turn's start).
    let listed = wss_rpc(
        &mut setup.rpc,
        40,
        "hook.list",
        json!({ "workspaceId": ws_id }),
    )
    .await;
    let hook_id = listed["hooks"]
        .as_array()
        .expect("hooks array")
        .iter()
        .find(|h| h["name"] == "late-watch" && h["state"] == "scheduled")
        .unwrap_or_else(|| panic!("scheduled late-watch hook in hook.list: {listed}"))["hookId"]
        .as_str()
        .expect("hookId")
        .to_string();
    let ran = wss_rpc(
        &mut setup.rpc,
        41,
        "hook.runNow",
        json!({ "workspaceId": ws_id, "hookId": hook_id }),
    )
    .await;
    assert_eq!(ran["ok"], true, "hook.runNow ok: {ran}");
    let text = await_conversation_contains(
        &mut setup.rpc,
        &mut req_id,
        &ws_id,
        &watcher,
        &format!("Child agent HookChild ({child})"),
        budget.step(90),
    )
    .await;
    assert!(
        text.contains("completed."),
        "settlement wake reports the child completed: {text}"
    );
    assert!(
        !text.contains("WATCH6_REPORT"),
        "the deferred wake must not carry the stale report: {text}"
    );
    await_watch_count(
        &mut setup.rpc,
        &mut req_id,
        &ws_id,
        &watcher,
        &child,
        0,
        budget.step(60),
    )
    .await;
    await_conversation_settled(
        &mut setup.rpc,
        &mut req_id,
        &ws_id,
        &watcher,
        budget.step(60),
    )
    .await;
    let wakes = wake_row_count(
        &mut setup.rpc,
        req_id,
        &ws_id,
        &watcher,
        &format!("Child agent HookChild ({child})"),
    )
    .await;
    assert_eq!(wakes, 1, "exactly one completion wake for the hooked child");
}

/// The agent's persisted wake rows (user rows framed with
/// `[WORKSPACE EVENTS]`), each serialized WHOLE — content blocks plus the
/// row's metadata — for per-row disclosure assertions (wake metadata such as
/// `watchStillArmed` round-trips on transcript reads).
async fn wake_rows_serialized(
    rpc: &mut TlsWs,
    id: i64,
    ws_id: &str,
    agent_id: &str,
) -> Vec<String> {
    let convo = wss_rpc(
        rpc,
        id,
        "agent.getConversation",
        json!({ "workspaceId": ws_id, "agentId": agent_id }),
    )
    .await;
    convo["messages"]
        .as_array()
        .expect("messages array")
        .iter()
        .filter(|m| m["role"] == "user")
        .map(|m| m.to_string())
        .filter(|t| t.contains("[WORKSPACE EVENTS]"))
        .collect()
}

/// monorepo#2528: the immediate `agent.reportToParent` wake over the real
/// transport says "reported" (a report is not necessarily a completion) and
/// discloses the watch disarm exactly when the call flips the parent's
/// ungrouped watch to `report_delivered`:
///  - the FIRST report in a turn carries the retirement NOTE with the
///    `ws.agent.watch` re-arm pointer (#2051 parity) plus
///    `watchStillArmed: false` on the wake metadata (#2060 parity);
///  - a REPEAT report in the same turn (watch already flipped) and a
///    post-retirement report (no watch left at all) still wake the parent but
///    carry neither the NOTE nor the `watchStillArmed` key;
///  - the child's genuine idle after the report delivers NO completion wake —
///    the flipped watch suppresses `agent:idle` and silently retires.
/// (The disclosed re-arm path itself — `ws.agent.watch` after the report wake
/// firing at the child's next genuine completion — is covered by the WATCH5
/// adoption test above.)
#[tokio::test]
async fn report_wake_disclosure_iff_watch_flipped_and_idle_suppressed_over_wss() {
    let Some(script) = gate("WSS report-wake disclosure E2E") else {
        return;
    };
    let budget = Budget::start();

    const SPAWN_GO: &str = "WATCH7_SPAWN_GO";
    const CHILD_GO: &str = "WATCH7_CHILD_GO";
    const CHILD_AGAIN: &str = "WATCH7_CHILD_AGAIN";
    const REPORT1: &str = "WATCH7_REPORT_ONE first slice landed";
    const REPORT2: &str = "WATCH7_REPORT_TWO second slice landed";
    const REPORT3: &str = "WATCH7_REPORT_THREE post-retirement progress";

    let spawn_js = format!(
        "const r = await ws.agent.create('DiscloseChild', '{CHILD_GO} do your work', \
         {{ model: 'mock:default' }}); return 'spawned=' + r.ok;"
    );
    let report1_js = format!("return await ws.agent.reportToParent({});", json!(REPORT1));
    let report2_js = format!("return await ws.agent.reportToParent({});", json!(REPORT2));
    let report3_js = format!("return await ws.agent.reportToParent({});", json!(REPORT3));
    // Wake-ack rule FIRST (wake turns must never re-run a marker rule off
    // replayed history), and CHILD_AGAIN before CHILD_GO: the child's second
    // turn replays its kickoff prompt in history, so the later marker must
    // match first.
    let behavior = json!({
        "rules": [
            { "ifPromptContains": "[WORKSPACE EVENTS]", "response": "wake acknowledged" },
            {
                "ifPromptContains": SPAWN_GO,
                "toolCall": {
                    "name": "workspace_api",
                    "arguments": { "code": spawn_js, "summary": "spawn the reporting child" }
                },
                "emitToolBlocks": true,
                "response": "child spawned",
            },
            {
                "ifPromptContains": CHILD_AGAIN,
                "toolCall": {
                    "name": "workspace_api",
                    "arguments": { "code": report3_js, "summary": "child reports post-retirement" }
                },
                "emitToolBlocks": true,
                "response": "child reported again",
            },
            {
                "ifPromptContains": CHILD_GO,
                "toolCalls": [
                    {
                        "name": "workspace_api",
                        "arguments": { "code": report1_js, "summary": "child first report" }
                    },
                    {
                        "name": "workspace_api",
                        "arguments": { "code": report2_js, "summary": "child repeat report" }
                    },
                ],
                "emitToolBlocks": true,
                "response": "child kickoff done",
            },
        ],
    })
    .to_string();
    let mut setup = boot_daemon(&script, &behavior, json!(["agent:*"]), budget).await;
    let ws_id = setup.ws_id.clone();

    // The parent spawns the child through the bridge (parent linkage makes
    // reportToParent legal and arms the auto parent→child watch).
    let parent = create_agent(&mut setup.rpc, 10, &ws_id, "DiscloseParent").await;
    let sent = wss_rpc(
        &mut setup.rpc,
        11,
        "agent.sendMessage",
        json!({ "workspaceId": ws_id, "agentId": parent, "content": SPAWN_GO }),
    )
    .await;
    assert_eq!(sent["success"], true, "spawn send ok: {sent}");
    let mut req_id = 20i64;
    let child = await_agent_id_by_name(
        &mut setup.rpc,
        &mut req_id,
        &ws_id,
        "DiscloseChild",
        budget.step(60),
    )
    .await;

    // The child's kickoff turn reports twice and ends holding nothing — its
    // idle is GENUINE (no outgoing watches, no hooks).
    let child_idle = await_idle_event(&mut setup.sub, &child, budget.step(90)).await;
    assert_ne!(
        child_idle["data"]["isWaitingForOtherAgents"],
        json!(true),
        "child idle is genuine: {child_idle}"
    );
    await_conversation_contains(
        &mut setup.rpc,
        &mut req_id,
        &ws_id,
        &parent,
        REPORT2,
        budget.step(60),
    )
    .await;

    // Per-row disclosure audit. First report flipped the auto watch: full
    // disclosure — "reported", retirement NOTE with the re-arm pointer, and
    // the machine-readable metadata twin.
    let rows = wake_rows_serialized(&mut setup.rpc, req_id, &ws_id, &parent).await;
    req_id += 1;
    let row1 = rows
        .iter()
        .find(|r| r.contains(REPORT1))
        .unwrap_or_else(|| panic!("first report wake row present: {rows:?}"));
    assert!(
        row1.contains("reported. Report:"),
        "first report wake says reported: {row1}"
    );
    assert!(
        row1.contains("consumed your one-shot watch"),
        "first report wake carries the disarm NOTE: {row1}"
    );
    assert!(
        row1.contains(&format!("ws.agent.watch(\\\"{child}\\\")")),
        "disarm NOTE carries the re-arm pointer naming the child: {row1}"
    );
    assert!(
        row1.contains("\"watchStillArmed\":false"),
        "first report wake metadata tags watchStillArmed=false: {row1}"
    );
    // Repeat report in the same turn found the watch already flipped: the
    // wake still delivers but carries NO disclosure — no NOTE, and the
    // watchStillArmed key is absent entirely.
    let row2 = rows
        .iter()
        .find(|r| r.contains(REPORT2))
        .unwrap_or_else(|| panic!("repeat report wake row present: {rows:?}"));
    assert!(
        row2.contains("reported. Report:"),
        "repeat report wake says reported: {row2}"
    );
    assert!(
        !row2.contains("consumed your one-shot watch"),
        "repeat report wake must not carry the disarm NOTE: {row2}"
    );
    assert!(
        !row2.contains("watchStillArmed"),
        "repeat report wake metadata must omit the watchStillArmed key: {row2}"
    );

    // Suppression: the flipped watch skips the child's genuine agent:idle and
    // silently retires — the parent gets NO completion wake, only the two
    // report wakes.
    await_watch_count(
        &mut setup.rpc,
        &mut req_id,
        &ws_id,
        &parent,
        &child,
        0,
        budget.step(60),
    )
    .await;
    tokio::time::sleep(Duration::from_millis(800)).await;
    let text = await_conversation_settled(
        &mut setup.rpc,
        &mut req_id,
        &ws_id,
        &parent,
        budget.step(60),
    )
    .await;
    assert!(
        !text.contains("completed."),
        "the reported child's idle must not deliver a second wake: {text}"
    );
    let wakes = wake_row_count(
        &mut setup.rpc,
        req_id,
        &ws_id,
        &parent,
        "Child agent DiscloseChild",
    )
    .await;
    req_id += 1;
    assert_eq!(wakes, 2, "exactly the two report wakes, nothing else");

    // Post-retirement report (the watch is gone, nothing to flip): the wake
    // still delivers, again with no disclosure.
    let sent = wss_rpc(
        &mut setup.rpc,
        40,
        "agent.sendMessage",
        json!({ "workspaceId": ws_id, "agentId": child, "content": CHILD_AGAIN }),
    )
    .await;
    assert_eq!(sent["success"], true, "child again send ok: {sent}");
    await_conversation_contains(
        &mut setup.rpc,
        &mut req_id,
        &ws_id,
        &parent,
        REPORT3,
        budget.step(60),
    )
    .await;
    let rows = wake_rows_serialized(&mut setup.rpc, req_id, &ws_id, &parent).await;
    req_id += 1;
    let row3 = rows
        .iter()
        .find(|r| r.contains(REPORT3))
        .unwrap_or_else(|| panic!("post-retirement report wake row present: {rows:?}"));
    assert!(
        row3.contains("reported. Report:"),
        "post-retirement report wake says reported: {row3}"
    );
    assert!(
        !row3.contains("consumed your one-shot watch"),
        "post-retirement report wake must not carry the disarm NOTE: {row3}"
    );
    assert!(
        !row3.contains("watchStillArmed"),
        "post-retirement report wake metadata must omit the watchStillArmed key: {row3}"
    );
    // And the watchless idle after this second turn delivers nothing either.
    tokio::time::sleep(Duration::from_millis(800)).await;
    let text = await_conversation_settled(
        &mut setup.rpc,
        &mut req_id,
        &ws_id,
        &parent,
        budget.step(60),
    )
    .await;
    assert!(
        !text.contains("completed."),
        "the watchless child's idle must not deliver a completion wake: {text}"
    );
    let wakes = wake_row_count(
        &mut setup.rpc,
        req_id,
        &ws_id,
        &parent,
        "Child agent DiscloseChild",
    )
    .await;
    assert_eq!(wakes, 3, "exactly the three report wakes, nothing else");
}
