//! WSS e2e for the `needs_attention` workspace displayStatus (PROTOCOL §6.5
//! step 0): a **top-level foreground** agent waiting on the user promotes the
//! derived `displayStatus` to `needs_attention` — outranking everything,
//! including the `agent_running → in_progress` promotion — and the retire
//! paths demote it. Scenarios over the real WSS transport (TLS + bearer auth,
//! mock ACP agents):
//!
//! 1. A top-level agent's mid-turn `ws.agent.requestDiscussion` emits
//!    `workspace:displayStatus-changed { displayStatus: "needs_attention" }`
//!    and `workspace.get` / `workspace.list` serve `needs_attention`.
//! 2. The next USER `agent.sendMessage` retires the request: the status
//!    transitions away from `needs_attention` and settles at `idle`.
//! 3. A pending-question tail (turn ends with a trailing
//!    `application/vnd.intent.question+json` resource block) raises
//!    `needs_attention` PERSISTENTLY — an untagged user message and the turn
//!    it drives do not retire it; `agent.dismissQuestions` does.
//! 4. A delegated (child/background) agent raising a blocker never produces
//!    `needs_attention`, even though `agent:attention-requested` fires and
//!    the linked task moves to `blocked`.
//! 5. The transcript-mutation RPCs recompute the derivation over the wire
//!    (monorepo#1266): `agent.appendMessage` with an answer-tagged user row
//!    retires a pending-question `needs_attention`, and
//!    `agent.replaceMessages` swapping back to a question-tail transcript
//!    raises it again.
//!
//! Gated on `node` + the mock script; skips cleanly otherwise.

#![cfg(unix)]

mod common;

use std::path::{Path, PathBuf};
use std::process::{Child, Command, Stdio};
use std::sync::atomic::{AtomicI64, Ordering};
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
use uuid::Uuid;

use common::TlsWs;

/// Fixed 64-hex token, adopted by the daemon via the `INTENTD_AUTH_TOKEN` seam.
const TOKEN: &str = "1a1a1a1a1a1a1a1a1a1a1a1a1a1a1a1a1a1a1a1a1a1a1a1a1a1a1a1a1a1a1a1a";

/// MIME type of the structured-question resource block (PROTOCOL §7.x).
const QUESTION_MIME: &str = "application/vnd.intent.question+json";

/// Kickoff marker for the top-level agent's `ws.agent.requestDiscussion` turn.
const RAISE_MARKER: &str = "RAISE_DISCUSSION_NEEDS_ATTN_E2E";

/// Kickoff marker for the question-asking turn (scenario 3).
const ASK_MARKER: &str = "ASK_NEEDS_ATTN_QUESTION_E2E";

/// Instruction marker for the delegated agent's `ws.agent.reportBlocker` turn.
const BLOCK_MARKER: &str = "RAISE_BLOCKER_NEEDS_ATTN_E2E";

/// Reasons carried by the attention requests.
const DISCUSS_REASON: &str = "NATTN_WSS need a decision before continuing";
const BLOCKER_REASON: &str = "NATTN_WSS sandbox is broken, cannot proceed";

/// Monotonic JSON-RPC id source shared by all helpers/tests in this file.
static NEXT_ID: AtomicI64 = AtomicI64::new(100);

fn next_id() -> i64 {
    NEXT_ID.fetch_add(1, Ordering::Relaxed)
}

/// Live `intentd serve` process; killed and its data dir removed on drop.
struct Daemon {
    child: Child,
    data_dir: PathBuf,
}

impl Drop for Daemon {
    fn drop(&mut self) {
        let _ = self.child.kill();
        let _ = self.child.wait();
        // Only dump the (potentially large) daemon log on test failure — cuts
        // CI noise on the common green-run path.
        if std::thread::panicking() {
            let log_path = self.data_dir.join("daemon.log");
            if let Ok(log) = std::fs::read_to_string(&log_path) {
                eprintln!("=== DAEMON LOG ===\n{log}\n=== END LOG ===");
            }
        }
        let _ = std::fs::remove_dir_all(&self.data_dir);
    }
}

fn temp_data_dir() -> PathBuf {
    let id = Uuid::new_v4().simple().to_string();
    let dir = PathBuf::from("/tmp").join(format!("itd-nattn-{}", &id[..8]));
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

/// Pin the server's SHA-256 fingerprint (colon-UPPER hex over the DER cert).
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

/// Open an authenticated WSS connection (token in the query string).
async fn connect_ws(port: u16, cfg: Arc<ClientConfig>) -> TlsWs {
    let url = format!("wss://localhost:{port}/ws?token={TOKEN}");
    common::wss_connect_with_retry(port, cfg, &url).await
}

/// Send one JSON-RPC frame and return the `result` whose id matches,
/// asserting the response envelope (`jsonrpc: "2.0"`, echoed `id`, no
/// `error`); out-of-band notifications (`events.event`) are skipped.
async fn wss_rpc(ws: &mut TlsWs, method: &str, params: Value) -> Value {
    let id = next_id();
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
                    assert_eq!(v["jsonrpc"], "2.0", "response envelope: {v}");
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

/// Read the next `events.event` notification's `params.event` before the
/// deadline, asserting the notification envelope; `None` on deadline.
async fn wss_event_until(ws: &mut TlsWs, deadline: tokio::time::Instant) -> Option<Value> {
    loop {
        let Ok(next) = tokio::time::timeout_at(deadline, ws.next()).await else {
            return None;
        };
        match next {
            Some(Ok(Message::Text(text))) => {
                let v: Value = serde_json::from_str(&text).expect("json frame");
                if v["method"] == "events.event" {
                    assert_eq!(v["jsonrpc"], "2.0", "notification envelope: {v}");
                    return Some(v["params"]["event"].clone());
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

/// Mock-agent gate (parity with the other WSS suites).
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

/// Pre-seed the daemon's `SQLite` store with a regular (NON-chief) workspace.
async fn seed_workspace_only(data_dir: &Path) -> String {
    use intent_core::{
        now_iso, Workspace, WorkspaceActivity, WorkspaceAttention, WorkspaceId, WorkspaceStatus,
    };
    use intent_store::Store;
    let db_path = data_dir.join("intentd.db");
    let store = Store::open(&db_path).await.expect("open store");
    let ws = WorkspaceId::new();
    let ts = now_iso();
    store
        .insert_workspace(&Workspace {
            id: ws.clone(),
            title: "NATTN-E2E".to_string(),
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
        })
        .await
        .expect("insert ws");
    ws.0
}

/// Spawn the daemon + return `(daemon, ws_id, port, cfg)` for a behavior.
async fn boot(script: &str, behavior: &str) -> (Daemon, String, u16, Arc<ClientConfig>) {
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
    let port =
        u16::try_from(status["result"]["port"].as_u64().expect("port")).expect("value fits in u16");
    let fingerprint = status["result"]["fingerprint"]
        .as_str()
        .expect("fingerprint")
        .to_string();
    (daemon, ws_id, port, client_config(&fingerprint))
}

/// Create a top-level FOREGROUND mock agent (`agent.create` — no parent
/// linkage, `isBackground` defaults to false) and return its id.
async fn create_agent(rpc: &mut TlsWs, ws_id: &str, name: &str) -> String {
    let created = wss_rpc(
        rpc,
        "agent.create",
        json!({ "workspaceId": ws_id, "name": name, "model": "mock:default" }),
    )
    .await;
    created["agent"]["id"]
        .as_str()
        .expect("agent id")
        .to_string()
}

/// `workspace.get` → the derived `displayStatus` string.
async fn get_display_status(rpc: &mut TlsWs, ws_id: &str) -> String {
    let got = wss_rpc(rpc, "workspace.get", json!({ "workspaceId": ws_id })).await;
    got["workspace"]["displayStatus"]
        .as_str()
        .expect("displayStatus string")
        .to_string()
}

/// Poll `workspace.get` until `displayStatus == want` (bounded), asserting
/// the read path never serves `needs_attention` along the way when
/// `forbid_attention` (post-retire / never-raised scenarios).
async fn poll_display_status(rpc: &mut TlsWs, ws_id: &str, want: &str, forbid_attention: bool) {
    for _ in 0..120 {
        let status = get_display_status(rpc, ws_id).await;
        if forbid_attention {
            assert_ne!(
                status, "needs_attention",
                "read path must not serve needs_attention"
            );
        }
        if status == want {
            return;
        }
        tokio::time::sleep(Duration::from_millis(250)).await;
    }
    panic!("displayStatus never settled at {want}");
}

/// Scenarios 1 + 2 — a top-level foreground agent's `requestDiscussion`
/// promotes the derived `displayStatus` to `needs_attention` (event + both
/// read paths), and the next USER `agent.sendMessage` retires it (the
/// status transitions away and settles at `idle`, never re-serving
/// `needs_attention`).
#[tokio::test]
async fn discussion_request_promotes_and_user_message_retires_over_wss() {
    let Some(script) = gate("WSS needs_attention discussion E2E") else {
        return;
    };

    let request_js = format!(
        "return await ws.agent.requestDiscussion({});",
        json!(DISCUSS_REASON)
    );
    let behavior = json!({
        "rules": [{
            "ifPromptContains": RAISE_MARKER,
            "toolCall": {
                "name": "workspace_api",
                "arguments": { "code": request_js, "summary": "raise discussion request" }
            },
            "response": "turn ended after requestDiscussion",
        }],
        "response": "follow-up acknowledged",
    })
    .to_string();
    let (_daemon, ws_id, port, cfg) = boot(&script, &behavior).await;

    let mut rpc = connect_ws(port, cfg.clone()).await;
    // Baseline read: a freshly seeded workspace (no tasks, no PRs, no agents)
    // serves `idle` — and seeds the last-observed cache, so the raise below
    // is a real transition that emits.
    assert_eq!(get_display_status(&mut rpc, &ws_id).await, "idle");

    // SUBSCRIBER conn — registered BEFORE the turn so we miss nothing.
    let mut sub = connect_ws(port, cfg.clone()).await;
    let sub_resp = wss_rpc(
        &mut sub,
        "events.subscribe",
        json!({
            "eventTypes": ["workspace:displayStatus-changed", "agent:*"],
            "workspaceId": ws_id,
        }),
    )
    .await;
    assert!(
        sub_resp["subscriptionId"].is_string(),
        "subscribed: {sub_resp}"
    );

    // ---- (1) Raise: the marker turn calls ws.agent.requestDiscussion ----
    let agent_id = create_agent(&mut rpc, &ws_id, "raiser").await;
    let sent = wss_rpc(
        &mut rpc,
        "agent.sendMessage",
        json!({
            "workspaceId": ws_id,
            "agentId": agent_id,
            "content": format!("{RAISE_MARKER} please raise a discussion request"),
        }),
    )
    .await;
    assert_eq!(sent["success"], true, "kickoff sendMessage ok: {sent}");

    // Order-insensitive milestones under one deadline: the promotion event,
    // the self-sufficient attention event, and the terminal idle.
    let mut raised = false;
    let mut attention = false;
    let mut idle = false;
    let deadline = tokio::time::Instant::now() + common::test_timeout(Duration::from_secs(60));
    while !(raised && attention && idle) {
        let Some(ev) = wss_event_until(&mut sub, deadline).await else {
            panic!("timed out: raised={raised} attention={attention} idle={idle}")
        };
        let data = &ev["data"];
        match ev["type"].as_str().unwrap_or_default() {
            "workspace:displayStatus-changed" if data["displayStatus"] == "needs_attention" => {
                assert_eq!(
                    ev["workspaceId"],
                    json!(ws_id),
                    "promotion event carries the envelope workspaceId: {ev}"
                );
                assert_eq!(
                    data,
                    &json!({ "workspaceId": ws_id, "displayStatus": "needs_attention" }),
                    "self-sufficient promotion payload (PROTOCOL §6.5): {ev}"
                );
                raised = true;
            }
            "agent:attention-requested" if data["agentId"] == json!(agent_id) => {
                assert_eq!(data["kind"], "discussion", "attention kind: {data}");
                assert_eq!(data["reason"], DISCUSS_REASON, "attention reason: {data}");
                attention = true;
            }
            "agent:idle" if data["agentId"] == json!(agent_id) => {
                assert_eq!(
                    data["isBackground"],
                    json!(false),
                    "the raiser is a foreground agent: {data}"
                );
                idle = true;
            }
            _ => {}
        }
    }

    // The pending request outlives the turn (top-level foreground: only the
    // user retires it) — both read paths serve `needs_attention`, outranking
    // the idle demotion.
    assert_eq!(
        get_display_status(&mut rpc, &ws_id).await,
        "needs_attention",
        "workspace.get serves needs_attention while the request is pending"
    );
    let listed = wss_rpc(&mut rpc, "workspace.list", json!({})).await;
    let row = listed["workspaces"]
        .as_array()
        .expect("workspaces array")
        .iter()
        .find(|w| w["id"] == json!(ws_id))
        .cloned()
        .expect("seeded workspace listed");
    assert_eq!(
        row["displayStatus"], "needs_attention",
        "workspace.list serves needs_attention while the request is pending: {row}"
    );

    // ---- (2) Retire: the next USER message clears the pending request ----
    let sent = wss_rpc(
        &mut rpc,
        "agent.sendMessage",
        json!({
            "workspaceId": ws_id,
            "agentId": agent_id,
            "content": "user decision: proceed with option A",
        }),
    )
    .await;
    assert_eq!(sent["success"], true, "retire sendMessage ok: {sent}");

    // The FIRST post-retire transition moves AWAY from needs_attention (to
    // in_progress while the follow-up turn runs, or straight to idle).
    let deadline = tokio::time::Instant::now() + common::test_timeout(Duration::from_secs(60));
    loop {
        let Some(ev) = wss_event_until(&mut sub, deadline).await else {
            panic!("timed out waiting for the retire transition")
        };
        if ev["type"] == "workspace:displayStatus-changed" {
            assert_ne!(
                ev["data"]["displayStatus"], "needs_attention",
                "retire transitions away from needs_attention: {ev}"
            );
            break;
        }
    }
    // And the retired request never re-raises: the read path settles at
    // `idle` (the turn-end blue dot is not a displayStatus axis, §6.5)
    // without ever serving needs_attention again.
    poll_display_status(&mut rpc, &ws_id, "idle", true).await;
}

/// Scenario 3 — a pending-question tail promotes `needs_attention`: the
/// asker's turn ends with a trailing question resource block
/// (`ws.app.question.ask`). The promotion is PERSISTENT — an untagged user
/// message and the agent turn it drives push the question off the transcript
/// tail without retiring it — and only `agent.dismissQuestions` retires it.
#[tokio::test]
async fn question_tail_promotes_and_dismiss_retires_over_wss() {
    let Some(script) = gate("WSS needs_attention question E2E") else {
        return;
    };

    let ask_code = format!(
        "return await ws.app.question.ask({});",
        json!({
            "header": "Deploy target",
            "question": "Which environment should I deploy to?",
            "options": [
                { "label": "Staging" },
                { "label": "Production" }
            ]
        })
    );
    let behavior = json!({
        "rules": [{
            "ifPromptContains": ASK_MARKER,
            "toolCall": {
                "name": "workspace_api",
                "arguments": { "code": ask_code, "summary": "ask a structured question" }
            },
            "response": "I have a clarifying question before I proceed.",
        }],
        "response": "acknowledged",
    })
    .to_string();
    let (_daemon, ws_id, port, cfg) = boot(&script, &behavior).await;

    let mut rpc = connect_ws(port, cfg.clone()).await;
    assert_eq!(get_display_status(&mut rpc, &ws_id).await, "idle");

    let mut sub = connect_ws(port, cfg.clone()).await;
    let sub_resp = wss_rpc(
        &mut sub,
        "events.subscribe",
        json!({
            "eventTypes": ["workspace:displayStatus-changed", "agent:*"],
            "workspaceId": ws_id,
        }),
    )
    .await;
    assert!(
        sub_resp["subscriptionId"].is_string(),
        "subscribed: {sub_resp}"
    );

    // ---- Ask: the marker turn ends on a trailing question block ----
    let agent_id = create_agent(&mut rpc, &ws_id, "asker").await;
    let sent = wss_rpc(
        &mut rpc,
        "agent.sendMessage",
        json!({
            "workspaceId": ws_id,
            "agentId": agent_id,
            "content": format!("please plan the deploy {ASK_MARKER}"),
        }),
    )
    .await;
    assert_eq!(sent["success"], true, "ask kickoff ok: {sent}");

    let mut raised = false;
    let mut idle = false;
    let deadline = tokio::time::Instant::now() + common::test_timeout(Duration::from_secs(60));
    while !(raised && idle) {
        let Some(ev) = wss_event_until(&mut sub, deadline).await else {
            panic!("timed out: raised={raised} idle={idle}")
        };
        let data = &ev["data"];
        match ev["type"].as_str().unwrap_or_default() {
            "workspace:displayStatus-changed" if data["displayStatus"] == "needs_attention" => {
                assert_eq!(
                    data,
                    &json!({ "workspaceId": ws_id, "displayStatus": "needs_attention" }),
                    "self-sufficient promotion payload: {ev}"
                );
                raised = true;
            }
            "agent:idle" if data["agentId"] == json!(agent_id) => idle = true,
            _ => {}
        }
    }

    // Hold prerequisite over the wire: the transcript's LAST message is the
    // assistant row whose trailing block is the question resource.
    let conv = wss_rpc(
        &mut rpc,
        "agent.getConversation",
        json!({ "workspaceId": ws_id, "agentId": agent_id }),
    )
    .await;
    let messages = conv["messages"].as_array().expect("messages array");
    let last = messages.last().expect("non-empty transcript");
    assert_eq!(
        last["role"], "assistant",
        "last message is the assistant question row: {last}"
    );
    let trailing = last["contentBlocks"]
        .as_array()
        .expect("contentBlocks")
        .last()
        .expect("blocks non-empty")
        .clone();
    assert_eq!(
        trailing["resource"]["mimeType"], QUESTION_MIME,
        "trailing block is the question resource: {trailing}"
    );
    let question_mid = last["id"].as_str().expect("question message id");

    // The question outlives the turn: the read path serves needs_attention.
    assert_eq!(
        get_display_status(&mut rpc, &ws_id).await,
        "needs_attention",
        "workspace.get serves needs_attention while the question is pending"
    );

    // ---- Persistence: an untagged user message + the agent turn it drives
    // do NOT retire the question. The pending-questions marker only clears on
    // an answer tag or an explicit dismissal, so the workspace stays flagged
    // even though the transcript tail is no longer the question row.
    let plain = wss_rpc(
        &mut rpc,
        "agent.sendMessage",
        json!({
            "workspaceId": ws_id,
            "agentId": agent_id,
            "content": "unrelated aside while the question is pending",
        }),
    )
    .await;
    assert_eq!(plain["success"], true, "plain user send ok: {plain}");

    let deadline = tokio::time::Instant::now() + common::test_timeout(Duration::from_secs(60));
    loop {
        let Some(ev) = wss_event_until(&mut sub, deadline).await else {
            panic!("timed out waiting for the plain turn to go idle")
        };
        assert_ne!(
            ev["type"], "workspace:displayStatus-changed",
            "an untagged user message must not move displayStatus: {ev}"
        );
        if ev["type"] == "agent:idle" && ev["data"]["agentId"] == json!(agent_id) {
            break;
        }
    }
    let conv = wss_rpc(
        &mut rpc,
        "agent.getConversation",
        json!({ "workspaceId": ws_id, "agentId": agent_id }),
    )
    .await;
    let messages = conv["messages"].as_array().expect("messages array");
    assert_ne!(
        messages.last().expect("non-empty transcript")["id"],
        json!(question_mid),
        "the question row is no longer the transcript tail"
    );
    assert_eq!(
        get_display_status(&mut rpc, &ws_id).await,
        "needs_attention",
        "pendingness survives an untagged user message and the agent's turn"
    );

    // ---- Retire: agent.dismissQuestions clears the question hold ----
    let dismissed = wss_rpc(
        &mut rpc,
        "agent.dismissQuestions",
        json!({ "workspaceId": ws_id, "agentId": agent_id, "messageId": question_mid }),
    )
    .await;
    assert_eq!(
        dismissed["dismissedQuestionsMessageId"],
        json!(question_mid),
        "dismissal persisted: {dismissed}"
    );

    let deadline = tokio::time::Instant::now() + common::test_timeout(Duration::from_secs(60));
    loop {
        let Some(ev) = wss_event_until(&mut sub, deadline).await else {
            panic!("timed out waiting for the dismissal transition")
        };
        if ev["type"] == "workspace:displayStatus-changed" {
            assert_ne!(
                ev["data"]["displayStatus"], "needs_attention",
                "dismissal transitions away from needs_attention: {ev}"
            );
            break;
        }
    }
    // Settles at `idle`: the asker's completed turn raised the turn-end
    // blue dot, but the flag is not a displayStatus axis (§6.5).
    poll_display_status(&mut rpc, &ws_id, "idle", true).await;
}

/// Scenario 4 — isolation: a DELEGATED (background, task-linked) agent
/// raising `ws.agent.reportBlocker` never promotes the workspace to
/// `needs_attention`, even though the self-sufficient
/// `agent:attention-requested` event fires and the linked task moves to
/// `blocked`. Child/background attention surfaces belong to the
/// parent/subscriber, not the workspace status.
#[tokio::test]
async fn delegated_blocker_never_promotes_needs_attention_over_wss() {
    let Some(script) = gate("WSS needs_attention background-isolation E2E") else {
        return;
    };

    let block_js = format!(
        "return await ws.agent.reportBlocker({});",
        json!(BLOCKER_REASON)
    );
    let behavior = json!({
        "rules": [{
            "ifPromptContains": BLOCK_MARKER,
            "toolCall": {
                "name": "workspace_api",
                "arguments": { "code": block_js, "summary": "report a blocker" }
            },
            "response": "turn ended after reportBlocker",
        }],
        "response": "acknowledged",
    })
    .to_string();
    let (_daemon, ws_id, port, cfg) = boot(&script, &behavior).await;

    let mut rpc = connect_ws(port, cfg.clone()).await;
    assert_eq!(get_display_status(&mut rpc, &ws_id).await, "idle");

    // Task note for the delegate (router front door: note.create +
    // task.markAsTask), so reportBlocker can transition the linked task.
    let created = wss_rpc(
        &mut rpc,
        "note.create",
        json!({ "workspaceId": ws_id, "title": "Blocked target", "content": "# Target\n" }),
    )
    .await;
    let note_id = created["note"]["id"].as_str().expect("note id").to_string();
    let marked = wss_rpc(
        &mut rpc,
        "task.markAsTask",
        json!({ "workspaceId": ws_id, "noteId": note_id, "status": "in_progress" }),
    )
    .await;
    assert_eq!(marked["ok"], true, "markAsTask ok: {marked}");

    let mut sub = connect_ws(port, cfg.clone()).await;
    let sub_resp = wss_rpc(
        &mut sub,
        "events.subscribe",
        json!({
            "eventTypes": ["workspace:displayStatus-changed", "agent:*", "task:*"],
            "workspaceId": ws_id,
        }),
    )
    .await;
    assert!(
        sub_resp["subscriptionId"].is_string(),
        "subscribed: {sub_resp}"
    );

    // Task-linked delegate: `agent.delegate` persists `isBackground: true`.
    let delegated = wss_rpc(
        &mut rpc,
        "agent.delegate",
        json!({
            "workspaceId": ws_id,
            "taskNoteId": note_id,
            "agentInstructions": format!("{BLOCK_MARKER} report the blocker"),
            "model": "mock:default",
        }),
    )
    .await;
    assert_eq!(delegated["ok"], true, "delegate ok: {delegated}");
    let agent_id = delegated["agentId"].as_str().expect("agent id").to_string();

    // Milestones: the blocker attention event, the linked-task transition to
    // `blocked`, and the delegate's terminal idle — while asserting EVERY
    // displayStatus transition along the way stays clear of needs_attention.
    let mut attention = false;
    let mut task_blocked = false;
    let mut idle = false;
    let deadline = tokio::time::Instant::now() + common::test_timeout(Duration::from_secs(60));
    while !(attention && task_blocked && idle) {
        let Some(ev) = wss_event_until(&mut sub, deadline).await else {
            panic!("timed out: attention={attention} task_blocked={task_blocked} idle={idle}")
        };
        let data = &ev["data"];
        match ev["type"].as_str().unwrap_or_default() {
            "workspace:displayStatus-changed" => {
                assert_ne!(
                    data["displayStatus"], "needs_attention",
                    "a background delegate must never promote needs_attention: {ev}"
                );
            }
            "agent:attention-requested" if data["agentId"] == json!(agent_id) => {
                assert_eq!(data["kind"], "blocker", "attention kind: {data}");
                assert_eq!(data["reason"], BLOCKER_REASON, "attention reason: {data}");
                attention = true;
            }
            "task:status-changed" if data["noteId"] == json!(note_id) => {
                if data["newStatus"] == "blocked" {
                    task_blocked = true;
                }
            }
            "agent:idle" if data["agentId"] == json!(agent_id) => {
                assert_eq!(
                    data["isBackground"],
                    json!(true),
                    "the delegate is a background agent: {data}"
                );
                idle = true;
            }
            _ => {}
        }
    }

    // The blocker request is still PENDING on the session (nothing retired
    // it), yet the workspace never surfaces it: the read path settles at
    // `idle` — a background delegate's completed turn does not raise the
    // turn-end blue dot (monorepo#1781) — without ever serving
    // needs_attention — or blocked (child/background blockers never count).
    let got = wss_rpc(
        &mut rpc,
        "agent.getSession",
        json!({ "agentId": agent_id, "workspaceId": ws_id }),
    )
    .await;
    assert_eq!(
        got["session"]["attentionRequestKind"], "blocker",
        "the delegate's blocker request is still pending: {}",
        got["session"]["attentionRequestKind"]
    );
    poll_display_status(&mut rpc, &ws_id, "idle", true).await;
    let status = get_display_status(&mut rpc, &ws_id).await;
    assert_ne!(
        status, "blocked",
        "a delegated/background blocker never promotes the workspace"
    );
    let got = wss_rpc(&mut rpc, "workspace.get", json!({ "workspaceId": ws_id })).await;
    assert_eq!(
        got["workspace"]["attention"], "none",
        "a background delegate's turn end never raises the workspace blue dot: {got}"
    );
}

/// Scenario 5 — monorepo#1266: the transcript-mutation RPCs recompute the
/// derived `displayStatus` over the real WSS router. A question tail raises
/// `needs_attention` (as in scenario 3); then `agent.appendMessage` with the
/// ANSWER row (tagged `question_answers` for that message) resolves the
/// question hold and the op's own recompute emits the retire transition; then
/// `agent.replaceMessages` swapping back to an unanswered question-bearing
/// transcript raises it again.
#[tokio::test]
async fn transcript_mutation_ops_recompute_needs_attention_over_wss() {
    let Some(script) = gate("WSS needs_attention transcript-mutation E2E") else {
        return;
    };

    // Same ask behavior as scenario 3: the marker turn ends on a trailing
    // question resource block.
    let ask_code = format!(
        "return await ws.app.question.ask({});",
        json!({
            "header": "Deploy target",
            "question": "Which environment should I deploy to?",
            "options": [
                { "label": "Staging" },
                { "label": "Production" }
            ]
        })
    );
    let behavior = json!({
        "rules": [{
            "ifPromptContains": ASK_MARKER,
            "toolCall": {
                "name": "workspace_api",
                "arguments": { "code": ask_code, "summary": "ask a structured question" }
            },
            "response": "I have a clarifying question before I proceed.",
        }],
        "response": "acknowledged",
    })
    .to_string();
    let (_daemon, ws_id, port, cfg) = boot(&script, &behavior).await;

    let mut rpc = connect_ws(port, cfg.clone()).await;
    assert_eq!(get_display_status(&mut rpc, &ws_id).await, "idle");

    let mut sub = connect_ws(port, cfg.clone()).await;
    let sub_resp = wss_rpc(
        &mut sub,
        "events.subscribe",
        json!({
            "eventTypes": ["workspace:displayStatus-changed", "agent:*"],
            "workspaceId": ws_id,
        }),
    )
    .await;
    assert!(
        sub_resp["subscriptionId"].is_string(),
        "subscribed: {sub_resp}"
    );

    // ---- Seed the hold: the marker turn ends on a question tail ----
    let agent_id = create_agent(&mut rpc, &ws_id, "mutator").await;
    let sent = wss_rpc(
        &mut rpc,
        "agent.sendMessage",
        json!({
            "workspaceId": ws_id,
            "agentId": agent_id,
            "content": format!("please plan the deploy {ASK_MARKER}"),
        }),
    )
    .await;
    assert_eq!(sent["success"], true, "ask kickoff ok: {sent}");

    let mut raised = false;
    let mut idle = false;
    let deadline = tokio::time::Instant::now() + common::test_timeout(Duration::from_secs(60));
    while !(raised && idle) {
        let Some(ev) = wss_event_until(&mut sub, deadline).await else {
            panic!("timed out: raised={raised} idle={idle}")
        };
        let data = &ev["data"];
        match ev["type"].as_str().unwrap_or_default() {
            "workspace:displayStatus-changed" if data["displayStatus"] == "needs_attention" => {
                raised = true;
            }
            "agent:idle" if data["agentId"] == json!(agent_id) => idle = true,
            _ => {}
        }
    }

    // Let the turn's debounced not-running recompute (~3s grace window) run
    // its silent no-op (the baseline is already `needs_attention`) before
    // mutating, so every displayStatus event the loops below consume is
    // attributable to the transcript-mutation ops alone.
    tokio::time::sleep(Duration::from_secs(4)).await;

    // Capture the question row's blocks so replaceMessages can rebuild the
    // tail below — and pin the hold prerequisite while at it.
    let conv = wss_rpc(
        &mut rpc,
        "agent.getConversation",
        json!({ "workspaceId": ws_id, "agentId": agent_id }),
    )
    .await;
    let last = conv["messages"]
        .as_array()
        .expect("messages array")
        .last()
        .expect("non-empty transcript")
        .clone();
    assert_eq!(
        last["role"], "assistant",
        "last message is the assistant question row: {last}"
    );
    let question_blocks = last["contentBlocks"].clone();
    let trailing = question_blocks
        .as_array()
        .expect("contentBlocks")
        .last()
        .expect("blocks non-empty")
        .clone();
    assert_eq!(
        trailing["resource"]["mimeType"], QUESTION_MIME,
        "trailing block is the question resource: {trailing}"
    );

    // ---- Retire over the wire: agent.appendMessage persists the ANSWER row
    // (tagged `question_answers` for the question message — a plain user row
    // no longer resolves a pending Q&A) ----
    let asked_id = last["id"].as_str().expect("question row id").to_string();
    let appended = wss_rpc(
        &mut rpc,
        "agent.appendMessage",
        json!({
            "workspaceId": ws_id,
            "agentId": agent_id,
            "role": "user",
            "contentBlocks": [{ "type": "text", "text": "deploy to staging" }],
            "metadata": {
                "type": "question_answers",
                "answeredQuestionsMessageId": asked_id,
            },
        }),
    )
    .await;
    assert_eq!(appended["success"], true, "appendMessage ok: {appended}");

    // The op's own recompute emits the retire transition …
    let deadline = tokio::time::Instant::now() + common::test_timeout(Duration::from_secs(60));
    loop {
        let Some(ev) = wss_event_until(&mut sub, deadline).await else {
            panic!("timed out waiting for the appendMessage retire transition")
        };
        if ev["type"] == "workspace:displayStatus-changed" {
            assert_ne!(
                ev["data"]["displayStatus"], "needs_attention",
                "appendMessage retire transitions away from needs_attention: {ev}"
            );
            break;
        }
    }
    // … and the read path settles at `idle` (the asker's turn-end blue dot
    // is not a displayStatus axis) without re-serving needs_attention.
    poll_display_status(&mut rpc, &ws_id, "idle", true).await;

    // ---- Raise over the wire: agent.replaceMessages swaps the transcript
    // back to one ending on the question-bearing assistant row ----
    let replaced = wss_rpc(
        &mut rpc,
        "agent.replaceMessages",
        json!({
            "workspaceId": ws_id,
            "agentId": agent_id,
            "messages": [{ "role": "assistant", "contentBlocks": question_blocks }],
        }),
    )
    .await;
    assert_eq!(replaced["success"], true, "replaceMessages ok: {replaced}");

    let deadline = tokio::time::Instant::now() + common::test_timeout(Duration::from_secs(60));
    loop {
        let Some(ev) = wss_event_until(&mut sub, deadline).await else {
            panic!("timed out waiting for the replaceMessages raise transition")
        };
        if ev["type"] == "workspace:displayStatus-changed" {
            assert_eq!(
                ev["data"],
                json!({ "workspaceId": ws_id, "displayStatus": "needs_attention" }),
                "replaceMessages raise carries the self-sufficient payload: {ev}"
            );
            break;
        }
    }
    assert_eq!(
        get_display_status(&mut rpc, &ws_id).await,
        "needs_attention",
        "workspace.get serves needs_attention after the swap"
    );
}
