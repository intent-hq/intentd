//! WSS end-to-end for the `agents.reportToParentDebounceSeconds` debounce
//! (spec: Debounce reportToParent parent wake), driven through the REAL agent
//! surface: a mock ACP parent spawns a child whose `ws.agent.reportToParent`
//! call exercises the MCP bridge end-to-end.
//!
//!  - With a non-zero window, a child that reports and finishes its turn
//!    inside the window yields exactly ONE parent wake: the terminal
//!    completion wake, whose metadata folds the retracted held report's
//!    `agent:reportToParent` event ahead of the completion event and whose
//!    text carries the persisted report. No held entry survives on the
//!    parent's queue.
//!  - With the setting at 0 the legacy immediate report wake still arrives,
//!    followed by the separate completion wake (two wake rows).
//!
//! Gated on `node` + the mock script; skips cleanly otherwise. Every wait is
//! clamped to a per-test [`TEST_BUDGET`] so a stall panics naming its own
//! step before nextest's kill (monorepo#1562).

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
    let dir = PathBuf::from("/tmp").join(format!("itd-wss-repdeb-{}", &id[..8]));
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

/// Whole-test wall-clock budget (monorepo#1562): every wait below is clamped
/// to this deadline so a stall panics naming its own step with headroom
/// before nextest's 180s kill.
const TEST_BUDGET: Duration = Duration::from_secs(150);

/// Per-test deadline clamp, started before the daemon boots.
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

/// Bound for one RPC round-trip, clamped below the nextest kill window.
fn rpc_read_budget() -> Duration {
    common::rpc_read_timeout().min(Duration::from_secs(45))
}

async fn wss_rpc(ws: &mut TlsWs, id: i64, method: &str, params: Value) -> Value {
    let frame = json!({ "jsonrpc": "2.0", "id": id, "method": method, "params": params });
    ws.send(Message::Text(frame.to_string().into()))
        .await
        .expect("send rpc frame");
    // One deadline for the whole round-trip: heartbeat `Ping`s and unrelated
    // notifications must not extend the bound (monorepo#1562).
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
            title: "WSS-REPDEB-E2E".to_string(),
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

/// Boot the daemon with the mock ACP provider and one RPC connection. These
/// scenarios assert only persisted transcript/queue state, so no FE event
/// subscriber is needed.
struct Setup {
    _daemon: Daemon,
    ws_id: String,
    rpc: TlsWs,
}

async fn boot_daemon(script: &str, behavior: &str, budget: Budget) -> Setup {
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
    assert!(
        await_uds(&socket, budget.step(60)).await,
        "daemon did not start"
    );
    let status = tokio::time::timeout_at(budget.step(60), common::await_wss_status(&socket))
        .await
        .expect("daemon wss status not ready within budget");
    let port =
        u16::try_from(status["result"]["port"].as_u64().expect("port")).expect("value fits in u16");
    let fingerprint = status["result"]["fingerprint"]
        .as_str()
        .expect("fingerprint")
        .to_string();
    let cfg = client_config(&fingerprint);
    let rpc = connect_ws(port, cfg).await;
    Setup {
        _daemon: daemon,
        ws_id,
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

/// Set `agents.reportToParentDebounceSeconds` over the wire and assert it
/// applied.
async fn set_debounce(rpc: &mut TlsWs, id: i64, seconds: u64) {
    let upd = wss_rpc(
        rpc,
        id,
        "settings.update",
        json!({ "changes": [
            { "path": "agents.reportToParentDebounceSeconds", "value": seconds }
        ] }),
    )
    .await;
    assert_eq!(
        upd["applied"][0]["path"], "agents.reportToParentDebounceSeconds",
        "debounce setting applied: {upd}"
    );
}

/// Serialize a conversation row's `contentBlocks` for substring assertions.
fn blocks_text(message: &Value) -> String {
    serde_json::to_string(&message["contentBlocks"]).unwrap_or_default()
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

/// Poll until the parent's transcript holds exactly `expected` wake rows
/// containing `needle` (delivery is async after the triggering event).
async fn await_wake_row_count(
    rpc: &mut TlsWs,
    req_id: &mut i64,
    ws_id: &str,
    agent_id: &str,
    needle: &str,
    expected: usize,
    deadline: tokio::time::Instant,
) {
    let mut last = 0;
    while tokio::time::Instant::now() < deadline {
        last = wake_row_count(rpc, *req_id, ws_id, agent_id, needle).await;
        *req_id += 1;
        if last == expected {
            return;
        }
        tokio::time::sleep(Duration::from_millis(250)).await;
    }
    panic!("expected {expected} wake rows containing {needle:?}, last saw {last}");
}

/// The agent's persisted wake rows, each serialized WHOLE — content blocks
/// plus the row's metadata — for per-row folded-metadata assertions.
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
        .map(std::string::ToString::to_string)
        .filter(|t| t.contains("[WORKSPACE EVENTS]"))
        .collect()
}

/// The parent's queue entries carrying the report-debounce hold marker.
async fn held_report_entries(rpc: &mut TlsWs, id: i64, ws_id: &str, agent_id: &str) -> Vec<Value> {
    let q = wss_rpc(
        rpc,
        id,
        "agent.getQueue",
        json!({ "workspaceId": ws_id, "agentId": agent_id }),
    )
    .await;
    q["queue"]
        .as_array()
        .expect("queue array")
        .iter()
        .filter(|m| m["holdKind"] == "report-debounce")
        .cloned()
        .collect()
}

/// Debounce combine (spec Design §2/§4) over the REAL transport: with a
/// non-zero `agents.reportToParentDebounceSeconds`, a child that reports and
/// settles inside the window yields exactly ONE parent wake — the terminal
/// completion wake. While the child's turn is still in flight the parked
/// report is observable on the parent's queue as a held entry (`holdKind:
/// "report-debounce"` + `holdUntil`), and the terminal wake's metadata folds
/// the retracted report's `agent:reportToParent` event ahead of the
/// completion event. No held entry survives settlement.
#[tokio::test]
async fn debounced_report_combined_with_completion_wake_over_wss() {
    const SPAWN_GO: &str = "REPDEB1_SPAWN_GO";
    const CHILD_GO: &str = "REPDEB1_CHILD_GO";
    const REPORT: &str = "REPDEB1_REPORT debounced slice landed";
    let Some(script) = gate("WSS report-debounce combine E2E") else {
        return;
    };
    let budget = Budget::start();

    let spawn_js = format!(
        "const r = await ws.agent.create('DebounceChild', '{CHILD_GO} do your work', \
         {{ model: 'mock:default' }}); return 'spawned=' + r.ok;"
    );
    let report_js = format!("return await ws.agent.reportToParent({});", json!(REPORT));
    // Wake-ack rule FIRST (wake turns must never re-run a marker rule off
    // replayed history). `firstTurnDelayMs` holds each agent's first turn
    // open for 3s AFTER its tool calls run — on the child that pins a
    // deterministic window in which the parked report is observable on the
    // parent's queue before settlement retracts it.
    let behavior = json!({
        "firstTurnDelayMs": 3000,
        "rules": [
            { "ifPromptContains": "[WORKSPACE EVENTS]", "response": "parent acknowledged wake" },
            {
                "ifPromptContains": CHILD_GO,
                "toolCall": {
                    "name": "workspace_api",
                    "arguments": { "code": report_js, "summary": "child reports progress" }
                },
                "emitToolBlocks": true,
                "response": "child kickoff done",
            },
            {
                "ifPromptContains": SPAWN_GO,
                "toolCall": {
                    "name": "workspace_api",
                    "arguments": { "code": spawn_js, "summary": "spawn debounce child" }
                },
                "emitToolBlocks": true,
                "response": "spawn turn done",
            },
        ],
    })
    .to_string();
    let mut setup = boot_daemon(&script, &behavior, budget).await;
    let ws_id = setup.ws_id.clone();

    // A window far wider than the child's post-report turn tail: the combine
    // path is settlement-driven, so the test never waits it out.
    set_debounce(&mut setup.rpc, 9, 30).await;

    let parent = create_agent(&mut setup.rpc, 10, &ws_id, "DebounceParent").await;
    let sent = wss_rpc(
        &mut setup.rpc,
        11,
        "agent.sendMessage",
        json!({ "workspaceId": ws_id, "agentId": parent, "content": SPAWN_GO }),
    )
    .await;
    assert_eq!(sent["success"], true, "spawn send ok: {sent}");
    let mut req_id = 20i64;

    // While the child's delayed first turn is still in flight, the report is
    // PARKED: the parent's queue holds exactly one report-debounce entry
    // carrying the formatted progress wake and a release deadline.
    let deadline = budget.step(60);
    let held = loop {
        let entries = held_report_entries(&mut setup.rpc, req_id, &ws_id, &parent).await;
        req_id += 1;
        if let Some(entry) = entries.first() {
            break entry.clone();
        }
        assert!(
            tokio::time::Instant::now() < deadline,
            "held report-debounce entry never appeared on the parent's queue"
        );
        tokio::time::sleep(Duration::from_millis(100)).await;
    };
    assert!(
        held["content"]
            .as_str()
            .is_some_and(|c| c.contains("reported. Report:") && c.contains(REPORT)),
        "held entry carries the formatted report wake: {held}"
    );
    assert!(
        held["holdUntil"].is_string(),
        "held entry carries its release deadline: {held}"
    );

    // The child settles inside the window: the settlement retracts the held
    // entry and delivers exactly ONE wake — the terminal completion.
    await_wake_row_count(
        &mut setup.rpc,
        &mut req_id,
        &ws_id,
        &parent,
        "completed.",
        1,
        budget.step(90),
    )
    .await;
    // Settle the parent's wake turn, then audit: no separate progress wake
    // ever landed, and no held entry survives.
    let text = await_conversation_settled(
        &mut setup.rpc,
        &mut req_id,
        &ws_id,
        &parent,
        budget.step(60),
    )
    .await;
    assert!(
        !text.contains("reported. Report:"),
        "no separate progress wake was delivered: {text}"
    );
    let wakes = wake_row_count(
        &mut setup.rpc,
        req_id,
        &ws_id,
        &parent,
        "Child agent DebounceChild",
    )
    .await;
    req_id += 1;
    assert_eq!(wakes, 1, "exactly one wake for the child: {text}");
    let leftover = held_report_entries(&mut setup.rpc, req_id, &ws_id, &parent).await;
    req_id += 1;
    assert!(
        leftover.is_empty(),
        "settlement retracted the held entry: {leftover:?}"
    );

    // The single wake's text carries the persisted report; its metadata folds
    // the retracted report's event AHEAD of the completion event and retires
    // the one-shot watch.
    let rows = wake_rows_serialized(&mut setup.rpc, req_id, &ws_id, &parent).await;
    let combined = rows
        .iter()
        .find(|r| r.contains("completed."))
        .unwrap_or_else(|| panic!("combined terminal wake row present: {rows:?}"));
    assert!(
        combined.contains(REPORT),
        "combined wake text renders the persisted report: {combined}"
    );
    assert!(
        combined.contains("\"watchStillArmed\":false"),
        "combined wake metadata tags watchStillArmed=false: {combined}"
    );
    assert!(
        combined.contains("\"eventTypes\":[\"agent:reportToParent\",\"agent:idle\"]"),
        "combined wake metadata folds the report event ahead of the completion: {combined}"
    );
    assert!(
        combined.contains("\"eventCount\":2"),
        "combined wake metadata counts both folded events: {combined}"
    );
}

/// Debounce disabled (`agents.reportToParentDebounceSeconds` = 0): the legacy
/// immediate report wake still delivers on its own — nothing is parked — and
/// the child's settlement follows as a SECOND, separate completion wake.
#[tokio::test]
async fn immediate_report_wake_when_debounce_disabled_over_wss() {
    const SPAWN_GO: &str = "REPDEB2_SPAWN_GO";
    const CHILD_GO: &str = "REPDEB2_CHILD_GO";
    const REPORT: &str = "REPDEB2_REPORT immediate slice landed";
    let Some(script) = gate("WSS report-debounce disabled E2E") else {
        return;
    };
    let budget = Budget::start();

    let spawn_js = format!(
        "const r = await ws.agent.create('ZeroChild', '{CHILD_GO} do your work', \
         {{ model: 'mock:default' }}); return 'spawned=' + r.ok;"
    );
    let report_js = format!("return await ws.agent.reportToParent({});", json!(REPORT));
    let behavior = json!({
        "rules": [
            { "ifPromptContains": "[WORKSPACE EVENTS]", "response": "parent acknowledged wake" },
            {
                "ifPromptContains": CHILD_GO,
                "toolCall": {
                    "name": "workspace_api",
                    "arguments": { "code": report_js, "summary": "child reports progress" }
                },
                "emitToolBlocks": true,
                "response": "child kickoff done",
            },
            {
                "ifPromptContains": SPAWN_GO,
                "toolCall": {
                    "name": "workspace_api",
                    "arguments": { "code": spawn_js, "summary": "spawn zero-debounce child" }
                },
                "emitToolBlocks": true,
                "response": "spawn turn done",
            },
        ],
    })
    .to_string();
    let mut setup = boot_daemon(&script, &behavior, budget).await;
    let ws_id = setup.ws_id.clone();

    set_debounce(&mut setup.rpc, 9, 0).await;

    let parent = create_agent(&mut setup.rpc, 10, &ws_id, "ZeroParent").await;
    let sent = wss_rpc(
        &mut setup.rpc,
        11,
        "agent.sendMessage",
        json!({ "workspaceId": ws_id, "agentId": parent, "content": SPAWN_GO }),
    )
    .await;
    assert_eq!(sent["success"], true, "spawn send ok: {sent}");
    let mut req_id = 20i64;

    // Legacy shape: one immediate progress wake, then one separate terminal
    // completion wake.
    await_wake_row_count(
        &mut setup.rpc,
        &mut req_id,
        &ws_id,
        &parent,
        "reported. Report:",
        1,
        budget.step(90),
    )
    .await;
    await_wake_row_count(
        &mut setup.rpc,
        &mut req_id,
        &ws_id,
        &parent,
        "completed.",
        1,
        budget.step(90),
    )
    .await;
    let _ = await_conversation_settled(
        &mut setup.rpc,
        &mut req_id,
        &ws_id,
        &parent,
        budget.step(60),
    )
    .await;
    let wakes = wake_row_count(
        &mut setup.rpc,
        req_id,
        &ws_id,
        &parent,
        "Child agent ZeroChild",
    )
    .await;
    req_id += 1;
    assert_eq!(wakes, 2, "one progress wake plus one terminal wake");

    // Per-row audit: the progress wake keeps the watch armed and carries only
    // the report event; the terminal wake retires it.
    let rows = wake_rows_serialized(&mut setup.rpc, req_id, &ws_id, &parent).await;
    let progress = rows
        .iter()
        .find(|r| r.contains("reported. Report:"))
        .unwrap_or_else(|| panic!("progress wake row present: {rows:?}"));
    assert!(
        progress.contains(REPORT),
        "progress wake carries the report: {progress}"
    );
    assert!(
        progress.contains("\"watchStillArmed\":true"),
        "progress wake metadata tags watchStillArmed=true: {progress}"
    );
    assert!(
        progress.contains("\"eventTypes\":[\"agent:reportToParent\"]"),
        "progress wake metadata carries only the report event: {progress}"
    );
    let terminal = rows
        .iter()
        .find(|r| r.contains("completed."))
        .unwrap_or_else(|| panic!("terminal wake row present: {rows:?}"));
    assert!(
        terminal.contains("\"watchStillArmed\":false"),
        "terminal wake metadata tags watchStillArmed=false: {terminal}"
    );
}
