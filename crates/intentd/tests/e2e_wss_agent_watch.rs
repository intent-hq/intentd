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
//!    `agent:tool:call` (the restriction is agent-caller-only).
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

async fn connect_ws(port: u16, cfg: Arc<ClientConfig>) -> TlsWs {
    let url = format!("wss://localhost:{port}/ws?token={TOKEN}");
    common::wss_connect_with_retry(port, cfg, &url).await
}

async fn wss_rpc(ws: &mut TlsWs, id: i64, method: &str, params: Value) -> Value {
    let frame = json!({ "jsonrpc": "2.0", "id": id, "method": method, "params": params });
    ws.send(Message::Text(frame.to_string().into()))
        .await
        .expect("send rpc frame");
    loop {
        let next = timeout(common::rpc_read_timeout(), ws.next())
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
            checkout_mode: None,
            execution_environment: None,
            disk_usage: None,
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

async fn boot_daemon(script: &str, behavior: &str, sub_event_types: Value) -> Setup {
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

/// Await `agent:idle` for `agent_id` on the subscriber stream.
async fn await_idle(sub: &mut TlsWs, agent_id: &str, secs: u64) {
    let deadline = tokio::time::Instant::now() + common::test_timeout(Duration::from_secs(secs));
    loop {
        let frame = wss_event_opt_until(sub, deadline)
            .await
            .unwrap_or_else(|| panic!("timed out awaiting agent:idle for {agent_id}"));
        let event = &frame["params"]["event"];
        if event["data"]["agentId"].as_str() != Some(agent_id) {
            continue;
        }
        match event["type"].as_str() {
            Some("agent:idle") => return,
            Some("agent:failed") => panic!("agent:failed while awaiting idle: {frame}"),
            _ => {}
        }
    }
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
    secs: u64,
) -> String {
    let deadline = tokio::time::Instant::now() + common::test_timeout(Duration::from_secs(secs));
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
) -> String {
    let deadline = tokio::time::Instant::now() + common::test_timeout(Duration::from_secs(60));
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
) {
    let deadline = tokio::time::Instant::now() + common::test_timeout(Duration::from_secs(60));
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
    let mut setup = boot_daemon(&script, &behavior, json!(["agent:*"])).await;
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
    await_idle(&mut setup.sub, &watcher, 60).await;

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
        30,
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
    let deadline = tokio::time::Instant::now() + common::test_timeout(Duration::from_secs(60));
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
        30,
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

/// WATCH-1 (monorepo#1229): `ws.agent.watch(agentId)` through the real MCP
/// bridge registers a watch that wakes the watcher on the target's idle
/// completion, blocker (`ws.agent.reportBlocker`), discussion
/// (`ws.agent.requestDiscussion`), and terminal failure. Every completion
/// wake retires the watch (deliver-once), so the watcher re-arms between
/// target turns; `ws.agent.unwatch` stops the wakes.
#[tokio::test]
async fn agent_watch_wakes_on_idle_attention_failed_and_unwatch_stops_over_wss() {
    let Some(script) = gate("WSS agent.watch lifecycle E2E") else {
        return;
    };

    const DO_WATCH: &str = "WATCH2_DO_WATCH";
    const DO_UNWATCH: &str = "WATCH2_DO_UNWATCH";
    const TARGET_PLAIN1: &str = "WATCH2_PLAIN_ONE";
    const TARGET_BLOCKER: &str = "WATCH2_BLOCKER";
    const TARGET_DISCUSS: &str = "WATCH2_DISCUSS";
    const TARGET_PLAIN2: &str = "WATCH2_PLAIN_TWO";
    const TARGET_DIE: &str = "WATCH2_DIE";
    const BLOCKER_REASON: &str = "WATCH2 sandbox is broken";
    const DISCUSS_REASON: &str = "WATCH2 need a coordinator decision";

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
    let behavior = json!({
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
    .to_string();
    let mut setup = boot_daemon(&script, &behavior, json!(["agent:*"])).await;
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
    await_idle(&mut setup.sub, &watcher, 60).await;
    let mut req_id = 20i64;
    let text = await_conversation_contains(
        &mut setup.rpc,
        &mut req_id,
        &ws_id,
        &watcher,
        "watched=true",
        30,
    )
    .await;
    assert!(
        text.contains(&format!("watchTarget={target}")),
        "watch names the target: {text}"
    );

    // 1. Target idle → completion wake ("Child agent … completed"), which
    // retires the watch (deliver-once).
    let sent = wss_rpc(
        &mut setup.rpc,
        30,
        "agent.sendMessage",
        json!({ "workspaceId": ws_id, "agentId": target, "content": TARGET_PLAIN1 }),
    )
    .await;
    assert_eq!(sent["success"], true, "target turn 1 ok: {sent}");
    let text = await_conversation_contains(
        &mut setup.rpc,
        &mut req_id,
        &ws_id,
        &watcher,
        "Child agent WatchTarget",
        60,
    )
    .await;
    assert!(
        text.contains("completed."),
        "idle wake reports the target completed: {text}"
    );
    await_watch_count(&mut setup.rpc, &mut req_id, &ws_id, &watcher, &target, 0).await;

    // 2. Re-arm, then blocker → attention wake ("Watched agent … reports a
    // blocker: …"). The attention wake does not consume the watch; the
    // trailing idle of the same target turn does.
    let sent = wss_rpc(
        &mut setup.rpc,
        31,
        "agent.sendMessage",
        json!({ "workspaceId": ws_id, "agentId": watcher, "content": DO_WATCH }),
    )
    .await;
    assert_eq!(sent["success"], true, "re-watch send ok: {sent}");
    await_watch_count(&mut setup.rpc, &mut req_id, &ws_id, &watcher, &target, 1).await;
    let sent = wss_rpc(
        &mut setup.rpc,
        32,
        "agent.sendMessage",
        json!({ "workspaceId": ws_id, "agentId": target, "content": TARGET_BLOCKER }),
    )
    .await;
    assert_eq!(sent["success"], true, "target blocker turn ok: {sent}");
    let text = await_conversation_contains(
        &mut setup.rpc,
        &mut req_id,
        &ws_id,
        &watcher,
        "reports a blocker",
        60,
    )
    .await;
    assert!(
        text.contains(BLOCKER_REASON),
        "blocker wake carries the reason: {text}"
    );

    // 3. Re-arm, then discussion → attention wake ("… requests a
    // discussion: …").
    await_watch_count(&mut setup.rpc, &mut req_id, &ws_id, &watcher, &target, 0).await;
    let sent = wss_rpc(
        &mut setup.rpc,
        33,
        "agent.sendMessage",
        json!({ "workspaceId": ws_id, "agentId": watcher, "content": DO_WATCH }),
    )
    .await;
    assert_eq!(sent["success"], true, "re-watch send ok: {sent}");
    await_watch_count(&mut setup.rpc, &mut req_id, &ws_id, &watcher, &target, 1).await;
    let sent = wss_rpc(
        &mut setup.rpc,
        34,
        "agent.sendMessage",
        json!({ "workspaceId": ws_id, "agentId": target, "content": TARGET_DISCUSS }),
    )
    .await;
    assert_eq!(sent["success"], true, "target discussion turn ok: {sent}");
    let text = await_conversation_contains(
        &mut setup.rpc,
        &mut req_id,
        &ws_id,
        &watcher,
        "requests a discussion",
        60,
    )
    .await;
    assert!(
        text.contains(DISCUSS_REASON),
        "discussion wake carries the reason: {text}"
    );

    // 4. Re-arm, unwatch, then a further target completion delivers NO new
    // wake (unwatch removes the live watch it just re-registered).
    await_watch_count(&mut setup.rpc, &mut req_id, &ws_id, &watcher, &target, 0).await;
    let sent = wss_rpc(
        &mut setup.rpc,
        35,
        "agent.sendMessage",
        json!({ "workspaceId": ws_id, "agentId": watcher, "content": DO_WATCH }),
    )
    .await;
    assert_eq!(sent["success"], true, "re-watch send ok: {sent}");
    await_watch_count(&mut setup.rpc, &mut req_id, &ws_id, &watcher, &target, 1).await;
    let sent = wss_rpc(
        &mut setup.rpc,
        36,
        "agent.sendMessage",
        json!({ "workspaceId": ws_id, "agentId": watcher, "content": DO_UNWATCH }),
    )
    .await;
    assert_eq!(sent["success"], true, "unwatch send ok: {sent}");
    await_conversation_contains(
        &mut setup.rpc,
        &mut req_id,
        &ws_id,
        &watcher,
        "unwatched=true",
        60,
    )
    .await;
    // The registry no longer holds a watcher→target watch.
    await_watch_count(&mut setup.rpc, &mut req_id, &ws_id, &watcher, &target, 0).await;
    // Drain any wake turns still in flight before taking the baseline.
    let baseline = await_conversation_settled(&mut setup.rpc, &mut req_id, &ws_id, &watcher).await;
    let sent = wss_rpc(
        &mut setup.rpc,
        60,
        "agent.sendMessage",
        json!({ "workspaceId": ws_id, "agentId": target, "content": TARGET_PLAIN2 }),
    )
    .await;
    assert_eq!(sent["success"], true, "target turn 2 ok: {sent}");
    // Prove turn 2 finished via the target's OWN transcript (the shared
    // subscriber stream may hold stale buffered idles from earlier turns).
    await_conversation_contains(
        &mut setup.rpc,
        &mut req_id,
        &ws_id,
        &target,
        "target plain turn two",
        60,
    )
    .await;
    tokio::time::sleep(Duration::from_millis(800)).await;
    let after = conversation_text(&mut setup.rpc, 61, &ws_id, &watcher).await;
    assert_eq!(baseline, after, "no wake may be delivered after unwatch");

    // 5. Re-watch, then the target's terminal failure wakes the watcher
    // ("Child agent … failed") — the mock kills every attempt on the DIE
    // marker, so the one-shot silent redrive is spent and the failure goes
    // terminal.
    let sent = wss_rpc(
        &mut setup.rpc,
        62,
        "agent.sendMessage",
        json!({ "workspaceId": ws_id, "agentId": watcher, "content": DO_WATCH }),
    )
    .await;
    assert_eq!(sent["success"], true, "re-watch send ok: {sent}");
    // The transcript already contains the FIRST watch turn's "watched=true",
    // so poll the registry (not the transcript) for the re-registration.
    await_watch_count(&mut setup.rpc, &mut req_id, &ws_id, &watcher, &target, 1).await;
    let sent = wss_rpc(
        &mut setup.rpc,
        63,
        "agent.sendMessage",
        json!({ "workspaceId": ws_id, "agentId": target, "content": TARGET_DIE }),
    )
    .await;
    assert_eq!(sent["success"], true, "target die turn accepted: {sent}");
    // The failure path includes a full silent-redrive cycle (kill + respawn +
    // re-prompt), so the window is generous. `format_completion_wake` renders
    // an AGENT_FAILED completion as "Child agent <label> failed." — the label
    // falls back to the bare agent id (the `agent:failed` payload carries no
    // agentName).
    let text = await_conversation_contains(
        &mut setup.rpc,
        &mut req_id,
        &ws_id,
        &watcher,
        "failed.",
        120,
    )
    .await;
    assert!(
        text.contains(&format!("Child agent {target}")),
        "failure wake names the target: {text}"
    );
}
