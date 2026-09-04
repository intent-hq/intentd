//! WSS end-to-end: a turn starting in an ARCHIVED workspace auto-unarchives
//! it (auto-unarchive on agent activity).
//!
//! Drives: create workspace → create agent → `workspace.archive` →
//! `agent.sendMessage` (a direct send claims the in-flight slot — the
//! archived gates only park queued wakes/drains) → asserts:
//! - the turn runs to completion (`agent:stream:end` with no `stopReason`),
//! - a §6.5 `workspace:updated` delta lands with
//!   `changes: { archived: false, status: "Active", archivedAt: null,
//!   autoUnarchive: { reason: "agent_activity", agentId, agentName } }`
//!   (unrelated activity deltas may interleave on the same event type),
//! - the flip persists ONE informational system transcript row (spec
//!   Contract wording, metadata `{ type: "auto_unarchived", reason:
//!   "agent_activity" }`) whose persist emits `agent:message` with
//!   `role: "system"`, and the SAME turn's outbound prompt carries the
//!   trailing `[SYSTEM NOTICE]` block (asserted via the mock fixture's
//!   `MOCK_AGENT_PROMPT_LOG` seam),
//! - a follow-up `workspace.get` shows the row Active with no `archivedAt`,
//! - a follow-up turn in the now-Active workspace persists NO new notice
//!   row and its prompt carries NO injected block (never replayed).
//!
//! Also covers the combined flush of parked archive notices
//! (intent-hq/intent#3883): archiving a workspace with an active hook parks
//! the hook-cancellation wake, and a later user `agent.sendMessage` delivers
//! the parked wake FIFO in ONE combined turn with the user message and the
//! trailing unarchive prompt notice.
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

/// Spec Contract: transcript text of the `auto_unarchived` system row.
const NOTICE_ROW_TEXT: &str =
    "Workspace was automatically unarchived because a message was sent to this agent.";

/// Spec Contract: trailing prompt block injected into the triggering turn.
const NOTICE_PROMPT_TEXT: &str =
    "[SYSTEM NOTICE] This workspace was archived; it has been automatically unarchived because this message was sent.";

/// Live `intentd serve` process; killed (whole process group) and its data
/// dir removed on drop, with the daemon log echoed for post-mortems.
struct Daemon {
    child: Child,
    data_dir: PathBuf,
}

impl Drop for Daemon {
    fn drop(&mut self) {
        {
            use nix::sys::signal::{self, Signal};
            use nix::unistd::Pid;
            let pid = Pid::from_raw(self.child.id().cast_signed());
            let _ = signal::killpg(pid, Signal::SIGKILL);
        }
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
    let dir = PathBuf::from("/tmp").join(format!("itd-wss-autoua-{}", &id[..8]));
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
    // Group leader so Daemon::drop can killpg the daemon + ACP children.
    {
        use std::os::unix::process::CommandExt;
        cmd.process_group(0);
    }
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
async fn connect_ws(port: u16, cfg: Arc<ClientConfig>) -> common::TlsWs {
    let url = format!("wss://localhost:{port}/ws?token={TOKEN}");
    common::wss_connect_with_retry(port, cfg, &url).await
}

/// Send one JSON-RPC frame and return the result whose id matches; any
/// out-of-band notifications (`events.event`) are ignored.
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

/// Read one `events.event` notification from a subscriber connection (bounded).
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

/// Mock-agent gate (parity with the other WSS E2E suites).
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
        title: "WSS-AUTO-UNARCHIVE-E2E".to_string(),
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
        execution_environment: None,
    }
}

/// A direct `agent.sendMessage` into an ARCHIVED workspace starts the turn
/// AND auto-unarchives the workspace: the §6.5 `workspace:updated` delta
/// carries the additive `autoUnarchive` stamp naming the triggering agent,
/// the turn runs to a normal completion, `workspace.get` shows Active, the
/// flip persists exactly one `auto_unarchived` system row (whose persist
/// emits `agent:message` with `role: "system"`), and only the triggering
/// turn's outbound prompt carries the trailing `[SYSTEM NOTICE]` block — a
/// follow-up turn in the Active workspace gets neither a new row nor the
/// injected block.
#[tokio::test]
async fn send_message_into_archived_workspace_auto_unarchives_over_wss() {
    let Some(script) = gate("WSS auto-unarchive E2E") else {
        return;
    };

    let data_dir = temp_data_dir();
    let ws_id = seed_workspace_only(&data_dir).await;
    let prompt_log = data_dir.join("prompts.jsonl");
    let prompt_log_str = prompt_log.to_string_lossy().into_owned();
    let behavior = json!({ "response": "auto-unarchive ok" }).to_string();
    let env: [(&str, &str); 5] = [
        ("INTENTD_AUTH_TOKEN", TOKEN),
        ("INTENTD_TCP_PORT", "0"),
        ("MOCK_AGENT_SCRIPT_PATH", &script),
        ("MOCK_AGENT_BEHAVIOR", &behavior),
        ("MOCK_AGENT_PROMPT_LOG", &prompt_log_str),
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

    let mut rpc = connect_ws(port, cfg.clone()).await;
    // Create the agent FIRST (while Active) so the session row exists and
    // the stamp resolves the agent name; then archive the idle workspace.
    let created = wss_rpc(
        &mut rpc,
        10,
        "agent.create",
        json!({ "workspaceId": ws_id, "name": "WSS-AUTO-UNARCHIVE", "model": "default", "provider": "mock" }),
    )
    .await;
    let agent_id = created["agent"]["id"]
        .as_str()
        .expect("agent id")
        .to_string();

    let archived = wss_rpc(
        &mut rpc,
        11,
        "workspace.archive",
        json!({ "workspaceId": ws_id }),
    )
    .await;
    assert_eq!(archived["workspace"]["archived"], json!(true));
    assert_eq!(archived["workspace"]["status"], json!("Archived"));

    // SUBSCRIBER conn — subscribe AFTER the archive so the archive's own
    // workspace:updated delta is never observable below.
    let mut sub = connect_ws(port, cfg.clone()).await;
    let sub_resp = wss_rpc(
        &mut sub,
        1,
        "events.subscribe",
        json!({
            "eventTypes": ["agent:*", "workspace:updated"],
            "workspaceId": ws_id,
        }),
    )
    .await;
    assert!(
        sub_resp["subscriptionId"].is_string(),
        "subscribed: {sub_resp}"
    );

    // Direct send into the archived workspace: the turn must start (direct
    // sends claim the slot; only queued wakes/drains park behind the
    // archived gates) and the claim auto-unarchives the workspace.
    let sent = wss_rpc(
        &mut rpc,
        12,
        "agent.sendMessage",
        json!({ "workspaceId": ws_id, "agentId": agent_id, "content": "hello" }),
    )
    .await;
    assert_eq!(sent["success"], true, "sendMessage ok: {sent}");

    // Collect the stamped delta, the notice row's system agent:message echo,
    // and the turn's normal completion; relative order is unspecified. Keep
    // only the workspace:updated frame carrying the autoUnarchive stamp —
    // under load unrelated activity deltas (e.g. a bare lastActivity change)
    // interleave on the same event type.
    let mut unarchive_delta = None;
    let mut notice_event = None;
    let mut stream_end = None;
    for _ in 0..80 {
        if unarchive_delta.is_some() && notice_event.is_some() && stream_end.is_some() {
            break;
        }
        let frame = wss_event(&mut sub, 30).await;
        let event = &frame["params"]["event"];
        match event["type"].as_str() {
            Some("workspace:updated") => {
                if unarchive_delta.is_none()
                    && event["data"]["changes"].get("autoUnarchive").is_some()
                {
                    unarchive_delta = Some(event["data"].clone());
                }
            }
            Some("agent:message") if event["data"]["role"] == "system" => {
                notice_event = Some(event["data"].clone());
            }
            Some("agent:stream:end") => {
                stream_end = Some(event["data"].clone());
            }
            _ => {}
        }
    }

    // §6.5 delta: the standard unarchive fields PLUS the additive
    // autoUnarchive stamp naming the triggering agent.
    let unarchive_delta = unarchive_delta.expect("turn start published workspace:updated");
    assert_eq!(
        unarchive_delta,
        json!({
            "workspaceId": ws_id,
            "changes": {
                "archived": false,
                "status": "Active",
                "archivedAt": null,
                "autoUnarchive": {
                    "reason": "agent_activity",
                    "agentId": agent_id,
                    "agentName": "WSS-AUTO-UNARCHIVE",
                },
            }
        }),
        "auto-unarchive delta shape per docs/protocol/06-events.md §6.5"
    );

    // The turn ran to a NORMAL completion (no stopReason): the unarchive
    // never blocked or interrupted the turn.
    let stream_end = stream_end.expect("the turn ran and emitted its terminal stream:end");
    assert_eq!(
        stream_end["agentId"].as_str().unwrap_or_default(),
        agent_id,
        "stream:end names the sending agent: {stream_end}"
    );
    assert!(
        stream_end.get("stopReason").is_none(),
        "normal completion carries no stopReason: {stream_end}"
    );

    // Durable state: workspace.get shows Active with archivedAt cleared.
    let fetched = wss_rpc(
        &mut rpc,
        20,
        "workspace.get",
        json!({ "workspaceId": ws_id }),
    )
    .await;
    assert_eq!(fetched["workspace"]["archived"], json!(false));
    assert_eq!(fetched["workspace"]["status"], json!("Active"));
    assert!(
        fetched["workspace"].get("archivedAt").is_none(),
        "archivedAt cleared: {fetched}"
    );

    // The confirmed flip persisted the informational notice row: its persist
    // emitted `agent:message` with `role: "system"` naming the transcript row.
    let notice_event = notice_event.expect("the notice persist emitted a system agent:message");
    assert_eq!(
        notice_event["agentId"],
        json!(agent_id),
        "system agent:message names the triggering agent: {notice_event}"
    );
    let notice_message_id = notice_event["messageId"]
        .as_str()
        .expect("system agent:message carries messageId")
        .to_string();

    // agent.getConversation serves exactly ONE auto_unarchived system row —
    // the spec Contract shape: role, text, and metadata, byte-for-byte —
    // and it is the row the agent:message event named.
    let conv = wss_rpc(
        &mut rpc,
        21,
        "agent.getConversation",
        json!({ "workspaceId": ws_id, "agentId": agent_id }),
    )
    .await;
    let messages = conv["messages"].as_array().expect("conversation messages");
    let notices: Vec<&Value> = messages
        .iter()
        .filter(|m| m["metadata"]["type"] == "auto_unarchived")
        .collect();
    assert_eq!(
        notices.len(),
        1,
        "exactly one auto_unarchived notice row: {messages:?}"
    );
    let notice = notices[0];
    assert_eq!(notice["role"], "system", "notice is a system row: {notice}");
    assert_eq!(
        notice["metadata"],
        json!({ "type": "auto_unarchived", "reason": "agent_activity" }),
        "notice metadata per spec Contract: {notice}"
    );
    let blocks = notice["contentBlocks"]
        .as_array()
        .expect("notice content blocks");
    assert_eq!(
        blocks.len(),
        1,
        "notice content is a single block: {notice}"
    );
    assert_eq!(blocks[0]["type"], "text", "notice block type: {notice}");
    assert_eq!(
        blocks[0]["text"],
        json!(NOTICE_ROW_TEXT),
        "notice block carries the Contract text: {notice}"
    );
    assert_eq!(
        notice["id"],
        json!(notice_message_id),
        "agent:message named the persisted notice row: {notice}"
    );

    // Outbound-prompt contract: the SAME turn that triggered the unarchive
    // carries one trailing `[SYSTEM NOTICE]` text block (the fixture logs
    // each prompt before resolving it, so the observed stream:end guarantees
    // the line is on disk).
    let log = std::fs::read_to_string(&prompt_log).expect("prompt log written");
    let prompts: Vec<Value> = log
        .lines()
        .map(|l| serde_json::from_str(l).expect("prompt log line"))
        .collect();
    assert_eq!(prompts.len(), 1, "one prompt so far: {prompts:?}");
    let first_text = prompts[0]["text"].as_str().expect("prompt text");
    assert!(
        first_text.contains("hello"),
        "triggering prompt carries the user message: {first_text:?}"
    );
    assert!(
        first_text.ends_with(NOTICE_PROMPT_TEXT),
        "triggering prompt ends with the trailing injected notice: {first_text:?}"
    );

    // Follow-up turn in the now-Active workspace: NO new notice row (no
    // system agent:message in the turn's event window, and the transcript
    // still holds exactly one) and NO injected prompt block (never replayed).
    let sent2 = wss_rpc(
        &mut rpc,
        22,
        "agent.sendMessage",
        json!({ "workspaceId": ws_id, "agentId": agent_id, "content": "follow-up" }),
    )
    .await;
    assert_eq!(sent2["success"], true, "follow-up sendMessage ok: {sent2}");
    let mut second_end = None;
    for _ in 0..80 {
        if second_end.is_some() {
            break;
        }
        let frame = wss_event(&mut sub, 30).await;
        let event = &frame["params"]["event"];
        match event["type"].as_str() {
            Some("agent:message") if event["data"]["role"] == "system" => {
                panic!(
                    "follow-up turn must not persist a new system notice: {}",
                    event["data"]
                );
            }
            Some("workspace:updated") => {
                assert!(
                    event["data"]["changes"].get("autoUnarchive").is_none(),
                    "no autoUnarchive stamp on an Active workspace: {}",
                    event["data"]
                );
            }
            Some("agent:stream:end") => {
                second_end = Some(event["data"].clone());
            }
            _ => {}
        }
    }
    let second_end = second_end.expect("the follow-up turn emitted its stream:end");
    assert!(
        second_end.get("stopReason").is_none(),
        "follow-up turn completed normally: {second_end}"
    );

    let conv2 = wss_rpc(
        &mut rpc,
        23,
        "agent.getConversation",
        json!({ "workspaceId": ws_id, "agentId": agent_id }),
    )
    .await;
    let messages2 = conv2["messages"].as_array().expect("conversation messages");
    assert_eq!(
        messages2
            .iter()
            .filter(|m| m["metadata"]["type"] == "auto_unarchived")
            .count(),
        1,
        "still exactly one auto_unarchived row after the follow-up: {messages2:?}"
    );

    let log2 = std::fs::read_to_string(&prompt_log).expect("prompt log written");
    let prompts2: Vec<Value> = log2
        .lines()
        .map(|l| serde_json::from_str(l).expect("prompt log line"))
        .collect();
    assert_eq!(prompts2.len(), 2, "two prompts total: {prompts2:?}");
    let second_text = prompts2[1]["text"].as_str().expect("prompt text");
    assert!(
        second_text.contains("follow-up"),
        "second prompt carries the follow-up message: {second_text:?}"
    );
    assert!(
        !second_text.contains("[SYSTEM NOTICE]"),
        "no injected notice on the follow-up turn: {second_text:?}"
    );
}

/// Marker the kickoff prompt carries so the mock agent schedules the hook.
const SCHEDULE_MARKER: &str = "SCHEDULE-THE-HOOK";

/// Combined flush of parked archive notices (intent-hq/intent#3883):
/// archiving a workspace with an active hook cancels the hook and parks its
/// cancellation wake behind the archived gate (the owner is idle); a later
/// USER `agent.sendMessage` converts to an enqueue + drain kick, so ONE
/// combined provider turn carries the parked wake FIFO ahead of the user
/// message with the trailing unarchive prompt notice — and the same claim
/// auto-unarchives the workspace and persists the `auto_unarchived` row.
#[tokio::test]
async fn user_send_flushes_parked_archive_notices_in_one_combined_turn() {
    let Some(script) = gate("WSS combined-flush auto-unarchive E2E") else {
        return;
    };

    let hook_code = "return { dispatch: false };";
    let schedule_js = format!(
        "return await ws.hook.schedule({{ name: 'watcher', code: {}, delayMs: 600000 }});",
        json!(hook_code)
    );
    let behavior = json!({
        "rules": [{
            "ifPromptContains": SCHEDULE_MARKER,
            "toolCall": {
                "name": "workspace_api",
                "arguments": { "code": schedule_js, "summary": "schedule watcher hook" }
            },
            "response": "scheduled the watcher hook",
        }],
        "response": "combined flush ok",
    })
    .to_string();

    let data_dir = temp_data_dir();
    let ws_id = seed_workspace_only(&data_dir).await;
    let prompt_log = data_dir.join("prompts.jsonl");
    let prompt_log_str = prompt_log.to_string_lossy().into_owned();
    let env: [(&str, &str); 5] = [
        ("INTENTD_AUTH_TOKEN", TOKEN),
        ("INTENTD_TCP_PORT", "0"),
        ("MOCK_AGENT_SCRIPT_PATH", &script),
        ("MOCK_AGENT_BEHAVIOR", &behavior),
        ("MOCK_AGENT_PROMPT_LOG", &prompt_log_str),
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

    // SUBSCRIBER conn — subscribe BEFORE any turn so no event can be missed.
    let mut sub = connect_ws(port, cfg.clone()).await;
    let sub_resp = wss_rpc(
        &mut sub,
        1,
        "events.subscribe",
        json!({
            "eventTypes": ["agent:*", "hook:*", "workspace:updated"],
            "workspaceId": ws_id,
        }),
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
        json!({ "workspaceId": ws_id, "name": "WSS-COMBINED-FLUSH", "model": "default", "provider": "mock" }),
    )
    .await;
    let agent_id = created["agent"]["id"]
        .as_str()
        .expect("agent id")
        .to_string();

    // Kickoff turn: the mock schedules the hook, then the turn completes.
    let sent = wss_rpc(
        &mut rpc,
        11,
        "agent.sendMessage",
        json!({
            "workspaceId": ws_id,
            "agentId": agent_id,
            "content": format!("{SCHEDULE_MARKER} please watch in the background"),
        }),
    )
    .await;
    assert_eq!(sent["success"], true, "kickoff sendMessage ok: {sent}");
    let mut hook_scheduled = false;
    let mut kickoff_done = false;
    for _ in 0..80 {
        if hook_scheduled && kickoff_done {
            break;
        }
        let frame = wss_event(&mut sub, 30).await;
        let event = &frame["params"]["event"];
        match event["type"].as_str() {
            Some("hook:scheduled") if event["data"]["name"] == json!("watcher") => {
                hook_scheduled = true;
            }
            Some("agent:stream:end") => {
                kickoff_done = true;
            }
            _ => {}
        }
    }
    assert!(hook_scheduled && kickoff_done, "kickoff turn settled");
    // The kickoff worker can stay registered busy for a moment after its
    // stream:end; settle fully so the archive sweep below has nothing to
    // interrupt (a sweep interrupt would emit a stray `interrupted`
    // stream:end into the subscription buffer).
    for _ in 0..100 {
        let listed = wss_rpc(&mut rpc, 30, "agent.list", json!({ "workspaceId": ws_id })).await;
        let row = listed["agents"]
            .as_array()
            .expect("agents array")
            .iter()
            .find(|a| a["id"] == json!(agent_id))
            .cloned()
            .unwrap_or_else(|| panic!("agent listed: {listed}"));
        if row["isResponding"] == json!(false) && row["turnInFlight"] == json!(false) {
            break;
        }
        tokio::time::sleep(Duration::from_millis(100)).await;
    }

    // Archive the idle workspace: the sweep cancels the hook and its
    // cancellation wake PARKS behind the archived gate (idle delivery arm).
    let archived = wss_rpc(
        &mut rpc,
        12,
        "workspace.archive",
        json!({ "workspaceId": ws_id }),
    )
    .await;
    assert_eq!(archived["workspace"]["archived"], json!(true));

    // The wake lands in the queue asynchronously after the cancel — poll.
    let mut parked = false;
    for _ in 0..100 {
        let queue = wss_rpc(
            &mut rpc,
            13,
            "agent.getQueue",
            json!({ "workspaceId": ws_id, "agentId": agent_id }),
        )
        .await;
        let entries = queue["queue"].as_array().expect("queue array");
        if entries.iter().any(|m| {
            m["content"]
                .as_str()
                .is_some_and(|c| c.contains("cancelled because its workspace was archived"))
        }) {
            parked = true;
            break;
        }
        tokio::time::sleep(Duration::from_millis(100)).await;
    }
    assert!(
        parked,
        "the hook-cancel wake parked behind the archived gate"
    );

    // The USER send converts to an enqueue + drain kick: one combined turn.
    let sent = wss_rpc(
        &mut rpc,
        14,
        "agent.sendMessage",
        json!({ "workspaceId": ws_id, "agentId": agent_id, "content": "back to work" }),
    )
    .await;
    assert_eq!(sent["success"], true, "user sendMessage ok: {sent}");
    assert_eq!(
        sent["queued"],
        json!(true),
        "the send converted to a queue-fallback enqueue: {sent}"
    );

    let mut unarchive_delta = None;
    let mut notice_event = None;
    let mut stream_end = None;
    for _ in 0..80 {
        if unarchive_delta.is_some() && notice_event.is_some() && stream_end.is_some() {
            break;
        }
        let frame = wss_event(&mut sub, 30).await;
        let event = &frame["params"]["event"];
        match event["type"].as_str() {
            Some("workspace:updated") => {
                if unarchive_delta.is_none()
                    && event["data"]["changes"].get("autoUnarchive").is_some()
                {
                    unarchive_delta = Some(event["data"].clone());
                }
            }
            Some("agent:message") if event["data"]["role"] == "system" => {
                notice_event = Some(event["data"].clone());
            }
            Some("agent:stream:end") => {
                stream_end = Some(event["data"].clone());
            }
            _ => {}
        }
    }
    let unarchive_delta = unarchive_delta.expect("the drain's claim published the stamped delta");
    assert_eq!(
        unarchive_delta["changes"]["autoUnarchive"]["reason"],
        json!("agent_activity"),
        "auto-unarchive stamp: {unarchive_delta}"
    );
    let stream_end = stream_end.expect("the combined turn emitted its stream:end");
    assert!(
        stream_end.get("stopReason").is_none(),
        "normal completion: {stream_end}"
    );
    notice_event.expect("the auto_unarchived notice persisted as a system row");

    // Durable state: Active, queue fully drained.
    let fetched = wss_rpc(
        &mut rpc,
        20,
        "workspace.get",
        json!({ "workspaceId": ws_id }),
    )
    .await;
    assert_eq!(fetched["workspace"]["archived"], json!(false));
    assert_eq!(fetched["workspace"]["status"], json!("Active"));
    let queue = wss_rpc(
        &mut rpc,
        21,
        "agent.getQueue",
        json!({ "workspaceId": ws_id, "agentId": agent_id }),
    )
    .await;
    assert_eq!(
        queue["queue"].as_array().map(Vec::len),
        Some(0),
        "no parked leftovers: {queue}"
    );

    // Transcript order: the parked wake row lands BEFORE the user message.
    let conv = wss_rpc(
        &mut rpc,
        22,
        "agent.getConversation",
        json!({ "workspaceId": ws_id, "agentId": agent_id }),
    )
    .await;
    let messages = conv["messages"].as_array().expect("conversation messages");
    let row_idx = |needle: &str| {
        messages.iter().position(|m| {
            m["role"] == "user"
                && m["contentBlocks"].as_array().is_some_and(|blocks| {
                    blocks
                        .iter()
                        .any(|b| b["text"].as_str().is_some_and(|t| t.contains(needle)))
                })
        })
    };
    let wake_idx =
        row_idx("cancelled because its workspace was archived").expect("wake row landed");
    let user_idx = row_idx("back to work").expect("user row landed");
    assert!(
        wake_idx < user_idx,
        "parked wake delivered FIFO ahead of the user message: wake={wake_idx} user={user_idx}"
    );
    assert_eq!(
        messages
            .iter()
            .filter(|m| m["metadata"]["type"] == "auto_unarchived")
            .count(),
        1,
        "exactly one auto_unarchived row: {messages:?}"
    );

    // Outbound-prompt contract: TWO prompts total (kickoff + combined); the
    // combined prompt carries wake → user message and ends with the one-shot
    // unarchive notice.
    let log = std::fs::read_to_string(&prompt_log).expect("prompt log written");
    let prompts: Vec<Value> = log
        .lines()
        .map(|l| serde_json::from_str(l).expect("prompt log line"))
        .collect();
    assert_eq!(prompts.len(), 2, "kickoff + one combined turn: {prompts:?}");
    let combined = prompts[1]["text"].as_str().expect("prompt text");
    let w = combined
        .find("cancelled because its workspace was archived")
        .expect("wake in the combined prompt");
    let u = combined.find("back to work").expect("user msg in prompt");
    assert!(w < u, "prompt order wake → user: {combined}");
    assert!(
        combined.ends_with(NOTICE_PROMPT_TEXT),
        "the combined prompt ends with the unarchive notice: {combined:?}"
    );
}
