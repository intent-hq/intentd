//! WSS end-to-end: a BACKGROUND HOOK archiving its own workspace via
//! `ws.workspace.archive` (intent-hq/monorepo#1577).
//!
//! The archive's own hook sweep cancels the initiating hook's scheduler task —
//! the task awaiting the `archive_workspace` future — so the post-persist tail
//! must survive that cancellation. Drives: create workspace → agent turn
//! schedules a hook whose second run archives the workspace → `hook.runNow`
//! over the wire → asserts:
//! - the §6.5 `workspace:updated` archive delta lands (the #1577 symptom was
//!   an archived row with NO event, leaving clients stale),
//! - the initiating hook itself ends `cancelled` (it does not keep polling an
//!   archived workspace),
//! - `workspace.get` reports the workspace archived.
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

const TOKEN: &str = "3c3c3c3c3c3c3c3c3c3c3c3c3c3c3c3c3c3c3c3c3c3c3c3c3c3c3c3c3c3c3c3c";

/// Kickoff marker for the hook-scheduling agent turn.
const SCHEDULE_MARKER: &str = "SCHEDULE_ARCHIVE_HOOK_E2E";

static NEXT_ID: AtomicI64 = AtomicI64::new(100);

fn next_id() -> i64 {
    NEXT_ID.fetch_add(1, Ordering::Relaxed)
}

/// Live `intentd serve` process; killed (whole process group) and its data
/// dir removed on drop, with the daemon log echoed on failure.
struct Daemon {
    child: Child,
    data_dir: PathBuf,
}

impl Drop for Daemon {
    fn drop(&mut self) {
        {
            use nix::sys::signal::{self, Signal};
            use nix::unistd::Pid;
            let pid = Pid::from_raw(self.child.id() as i32);
            let _ = signal::killpg(pid, Signal::SIGKILL);
        }
        let _ = self.child.wait();
        if std::thread::panicking() {
            let log_path = self.data_dir.join("daemon.log");
            if let Ok(log) = std::fs::read_to_string(&log_path) {
                eprintln!("=== DAEMON LOG ===\n{}\n=== END LOG ===", log);
            }
        }
        let _ = std::fs::remove_dir_all(&self.data_dir);
    }
}

fn temp_data_dir() -> PathBuf {
    let id = Uuid::new_v4().simple().to_string();
    let dir = PathBuf::from("/tmp").join(format!("itd-wss-archhook-{}", &id[..8]));
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
async fn connect_ws(port: u16, cfg: Arc<ClientConfig>) -> TlsWs {
    let url = format!("wss://localhost:{port}/ws?token={TOKEN}");
    common::wss_connect_with_retry(port, cfg, &url).await
}

/// Send one JSON-RPC frame and return the matching envelope (`result` or
/// `error` intact); out-of-band notifications are ignored.
async fn wss_rpc_envelope(ws: &mut TlsWs, method: &str, params: Value) -> Value {
    let id = next_id();
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

/// Send one JSON-RPC frame and return the matching result; out-of-band
/// notifications are ignored.
async fn wss_rpc(ws: &mut TlsWs, method: &str, params: Value) -> Value {
    let v = wss_rpc_envelope(ws, method, params).await;
    assert!(v.get("error").is_none(), "rpc {method} errored: {v}");
    v["result"].clone()
}

/// `hook.runNow`, retried while the scheduler task is not registered yet.
///
/// `hook_schedule_op` emits `hook:scheduled` BEFORE `spawn_hook_task` registers
/// the hook's control channel, so a `runNow` sent the instant that event lands
/// can beat registration and come back `Internal("... has no live scheduler
/// task")`. The window is a few statements wide server-side against a full WSS
/// round trip client-side, but a loaded CI runner can hit it — retry that one
/// error shape instead of failing the run.
async fn wss_hook_run_now(ws: &mut TlsWs, params: Value) -> Value {
    let deadline = tokio::time::Instant::now() + common::test_timeout(Duration::from_secs(30));
    loop {
        let v = wss_rpc_envelope(ws, "hook.runNow", params.clone()).await;
        let racing = v["error"]["message"]
            .as_str()
            .is_some_and(|m| m.contains("no live scheduler task"));
        if !racing {
            assert!(v.get("error").is_none(), "rpc hook.runNow errored: {v}");
            return v["result"].clone();
        }
        assert!(
            tokio::time::Instant::now() < deadline,
            "hook.runNow never saw a live scheduler task: {v}"
        );
        tokio::time::sleep(Duration::from_millis(50)).await;
    }
}

/// Read one `events.event` payload from a subscriber connection, bounded by
/// `deadline` (`None` on expiry).
async fn wss_event_until(ws: &mut TlsWs, deadline: tokio::time::Instant) -> Option<Value> {
    loop {
        let next = timeout(
            deadline.saturating_duration_since(tokio::time::Instant::now()),
            ws.next(),
        )
        .await
        .ok()?;
        match next {
            Some(Ok(Message::Text(text))) => {
                let v: Value = serde_json::from_str(&text).expect("json frame");
                if v["method"] == "events.event" {
                    return Some(v["params"]["event"].clone());
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
            title: "WSS-ARCHIVE-FROM-HOOK-E2E".to_string(),
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

/// Regression (intent-hq/monorepo#1577): a hook-initiated
/// `ws.workspace.archive` must publish the §6.5 `workspace:updated` archive
/// delta even though the archive's own hook sweep cancels the initiating
/// hook's task mid-call, and the initiating hook must end `cancelled` rather
/// than keep polling an archived workspace.
#[tokio::test]
async fn hook_archiving_its_own_workspace_publishes_the_archive_delta_over_wss() {
    let Some(script) = gate("WSS archive-from-hook E2E") else {
        return;
    };

    // The hook's first (schedule-time validation) run just arms itself via the
    // carry-over `state`; the `hook.runNow` run below archives the workspace.
    let hook_code = "if (hookState && hookState.armed) { \
                     await ws.workspace.archive(); \
                     } \
                     return { dispatch: false, state: { armed: true } };";
    let schedule_js = format!(
        "return await ws.hook.schedule({{ name: 'archiver', code: {}, delayMs: 600000 }});",
        json!(hook_code)
    );
    let behavior = json!({
        "rules": [{
            "ifPromptContains": SCHEDULE_MARKER,
            "toolCall": {
                "name": "workspace_api",
                "arguments": { "code": schedule_js, "summary": "schedule archiving hook" }
            },
            "response": "scheduled the archiving hook",
        }],
        "response": "acknowledged",
    })
    .to_string();

    let data_dir = temp_data_dir();
    let ws_id = seed_workspace_only(&data_dir).await;
    let env: [(&str, &str); 4] = [
        ("INTENTD_AUTH_TOKEN", TOKEN),
        ("INTENTD_TCP_PORT", "0"),
        ("MOCK_AGENT_SCRIPT_PATH", &script),
        ("MOCK_AGENT_BEHAVIOR", &behavior),
    ];
    let child = spawn_serve(&data_dir, &env);
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

    // SUBSCRIBER conn — subscribe BEFORE the turn so no event can be missed.
    let mut sub = connect_ws(port, cfg.clone()).await;
    let sub_resp = wss_rpc(
        &mut sub,
        "events.subscribe",
        json!({
            "eventTypes": ["hook:*", "workspace:updated"],
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
        "agent.create",
        json!({ "workspaceId": ws_id, "name": "WSS-ARCHIVE-HOOK", "model": "mock:default" }),
    )
    .await;
    let agent_id = created["agent"]["id"]
        .as_str()
        .expect("agent id")
        .to_string();
    let sent = wss_rpc(
        &mut rpc,
        "agent.sendMessage",
        json!({
            "workspaceId": ws_id,
            "agentId": agent_id,
            "content": format!("{SCHEDULE_MARKER} please watch in the background"),
        }),
    )
    .await;
    assert_eq!(sent["success"], true, "kickoff sendMessage ok: {sent}");

    // The scheduling turn persists the hook (`hook:scheduled` carries the id).
    let deadline = tokio::time::Instant::now() + common::test_timeout(Duration::from_secs(60));
    let mut hook_id = None::<String>;
    while hook_id.is_none() {
        let ev = match wss_event_until(&mut sub, deadline).await {
            Some(ev) => ev,
            None => panic!("hook:scheduled never landed"),
        };
        if ev["type"] == json!("hook:scheduled") && ev["data"]["name"] == json!("archiver") {
            assert_eq!(ev["data"]["agentId"], json!(agent_id), "hook owner: {ev}");
            hook_id = Some(ev["data"]["hookId"].as_str().expect("hookId").to_string());
        }
    }
    let hook_id = hook_id.expect("hook:scheduled carried the hookId");

    // Drive the armed run: this one calls `ws.workspace.archive()`, whose own
    // sweep cancels this very hook's task mid-call.
    let ran = wss_hook_run_now(&mut rpc, json!({ "workspaceId": ws_id, "hookId": hook_id })).await;
    assert_eq!(ran["ok"], json!(true), "{ran}");

    // The archive delta lands (#1577: it used to be dropped with the caller)
    // and the initiating hook itself ends cancelled.
    let mut archive_delta = None;
    let mut hook_cancelled = false;
    let deadline = tokio::time::Instant::now() + common::test_timeout(Duration::from_secs(60));
    while archive_delta.is_none() || !hook_cancelled {
        let ev = match wss_event_until(&mut sub, deadline).await {
            Some(ev) => ev,
            None => {
                panic!("timed out: archive_delta={archive_delta:?} hook_cancelled={hook_cancelled}")
            }
        };
        match ev["type"].as_str().unwrap_or_default() {
            "workspace:updated" if ev["data"]["changes"]["archived"] == json!(true) => {
                archive_delta = Some(ev["data"].clone());
            }
            "hook:cancelled" if ev["data"]["hookId"] == json!(hook_id) => {
                assert_eq!(ev["data"]["state"], "cancelled", "{ev}");
                hook_cancelled = true;
            }
            _ => {}
        }
    }

    let archive_delta = archive_delta.expect("hook-initiated archive published workspace:updated");
    assert_eq!(
        archive_delta["changes"]["status"],
        json!("Archived"),
        "archive delta per docs/protocol/06-events.md §6.5: {archive_delta}"
    );
    assert!(
        archive_delta["changes"]["archivedAt"].is_string(),
        "archive delta carries archivedAt: {archive_delta}"
    );

    // The workspace really is archived, and the hook is terminal — it will not
    // keep polling an archived workspace.
    let fetched = wss_rpc(&mut rpc, "workspace.get", json!({ "workspaceId": ws_id })).await;
    assert_eq!(fetched["workspace"]["archived"], json!(true));
    assert_eq!(fetched["workspace"]["status"], json!("Archived"));
    let listed = wss_rpc(&mut rpc, "hook.list", json!({ "workspaceId": ws_id })).await;
    let row = listed["hooks"]
        .as_array()
        .expect("hooks array")
        .iter()
        .find(|h| h["hookId"] == json!(hook_id))
        .cloned()
        .expect("the initiating hook is still listed");
    assert_eq!(row["state"], json!("cancelled"), "{row}");
    assert!(
        row["nextRunAt"].is_null(),
        "no further run scheduled: {row}"
    );
}

/// Regression (intent-hq/monorepo#2513): the hook-cancel wake from a
/// hook-initiated archive must STAY PARKED behind the archived gate even when
/// it lands while the hook owner is mid-turn.
///
/// The archive tail's hook sweep wakes the owner; when the owner's turn is
/// still in flight the wake takes `deliver_wake_message`'s fast enqueue
/// branch (busy → queue) — bypassing that path's archived gate, which only
/// covers the idle-delivery arm. The owner's worker then popped the parked
/// wake in its end-of-turn drain, whose `try_begin` re-claim auto-unarchived
/// (intentd#1216) the workspace that was just archived: subscribers saw the
/// §6.5 archive delta, yet a follow-up `workspace.get` read `archived:
/// false`. The fix gates the end-of-turn drain (and its post-release raced
/// re-check) on the archived row, mirroring `try_drain_queue`.
///
/// Drives the mid-turn window deterministically: the kickoff turn schedules
/// the hook, then stalls (`firstTurnDelayMs`) so `hook.runNow`'s archive +
/// cancel-wake land while the owner is provably busy. Asserts the workspace
/// REMAINS archived after the owner settles and the wake is parked in the
/// queue (not consumed by a stray turn).
#[tokio::test]
async fn hook_cancel_wake_parked_mid_turn_does_not_unarchive_the_workspace() {
    let Some(script) = gate("WSS archive-from-hook parked-wake E2E") else {
        return;
    };

    let hook_code = "if (hookState && hookState.armed) { \
                     await ws.workspace.archive(); \
                     } \
                     return { dispatch: false, state: { armed: true } };";
    let schedule_js = format!(
        "return await ws.hook.schedule({{ name: 'archiver', code: {}, delayMs: 600000 }});",
        json!(hook_code)
    );
    let behavior = json!({
        "rules": [{
            "ifPromptContains": SCHEDULE_MARKER,
            "toolCall": {
                "name": "workspace_api",
                "arguments": { "code": schedule_js, "summary": "schedule archiving hook" }
            },
            "response": "scheduled the archiving hook",
        }],
        // Stall the kickoff turn AFTER the tool call so the archive + the
        // hook-cancel wake land while this turn is still in flight: the wake
        // must take the busy fast-enqueue branch, putting the end-of-turn
        // drain (not the delivery-time gate) on the hook for parking it.
        // Scaled by INTENTD_TEST_TIMEOUT_MULTIPLIER: the flake this test
        // guards reproduced on slow coverage runners, where an unscaled
        // stall could elapse before `hook.runNow`'s archive lands — the wake
        // then takes the idle delivery-time gate instead and the test
        // silently stops covering the end-of-turn drain path.
        "firstTurnDelayMs": common::test_timeout(Duration::from_millis(4000)).as_millis() as u64,
        "response": "acknowledged",
    })
    .to_string();

    let data_dir = temp_data_dir();
    let ws_id = seed_workspace_only(&data_dir).await;
    let env: [(&str, &str); 4] = [
        ("INTENTD_AUTH_TOKEN", TOKEN),
        ("INTENTD_TCP_PORT", "0"),
        ("MOCK_AGENT_SCRIPT_PATH", &script),
        ("MOCK_AGENT_BEHAVIOR", &behavior),
    ];
    let child = spawn_serve(&data_dir, &env);
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

    // SUBSCRIBER conn — subscribe BEFORE the turn so no event can be missed.
    let mut sub = connect_ws(port, cfg.clone()).await;
    let sub_resp = wss_rpc(
        &mut sub,
        "events.subscribe",
        json!({
            "eventTypes": ["hook:*", "workspace:updated"],
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
        "agent.create",
        json!({ "workspaceId": ws_id, "name": "WSS-PARKED-WAKE", "model": "mock:default" }),
    )
    .await;
    let agent_id = created["agent"]["id"]
        .as_str()
        .expect("agent id")
        .to_string();
    let sent = wss_rpc(
        &mut rpc,
        "agent.sendMessage",
        json!({
            "workspaceId": ws_id,
            "agentId": agent_id,
            "content": format!("{SCHEDULE_MARKER} please watch in the background"),
        }),
    )
    .await;
    assert_eq!(sent["success"], true, "kickoff sendMessage ok: {sent}");

    // `hook:scheduled` is emitted mid-turn (during the tool call), so the
    // kickoff turn's post-tool-call stall keeps the owner busy from here on.
    let deadline = tokio::time::Instant::now() + common::test_timeout(Duration::from_secs(60));
    let mut hook_id = None::<String>;
    while hook_id.is_none() {
        let ev = match wss_event_until(&mut sub, deadline).await {
            Some(ev) => ev,
            None => panic!("hook:scheduled never landed"),
        };
        if ev["type"] == json!("hook:scheduled") && ev["data"]["name"] == json!("archiver") {
            hook_id = Some(ev["data"]["hookId"].as_str().expect("hookId").to_string());
        }
    }
    let hook_id = hook_id.expect("hook:scheduled carried the hookId");

    // Archive from the hook while the owner is mid-stall.
    let ran = wss_hook_run_now(&mut rpc, json!({ "workspaceId": ws_id, "hookId": hook_id })).await;
    assert_eq!(ran["ok"], json!(true), "{ran}");

    // The archive delta and the hook cancellation land while the owner's
    // kickoff turn is still in flight. If ANY `workspace:updated` flips
    // `archived` back to false, that is the #2513 auto-unarchive — fail fast.
    let mut archive_delta_seen = false;
    let mut hook_cancelled = false;
    let deadline = tokio::time::Instant::now() + common::test_timeout(Duration::from_secs(60));
    while !archive_delta_seen || !hook_cancelled {
        let ev = match wss_event_until(&mut sub, deadline).await {
            Some(ev) => ev,
            None => panic!(
                "timed out: archive_delta_seen={archive_delta_seen} hook_cancelled={hook_cancelled}"
            ),
        };
        match ev["type"].as_str().unwrap_or_default() {
            "workspace:updated" if ev["data"]["changes"]["archived"] == json!(true) => {
                archive_delta_seen = true;
            }
            "workspace:updated" if ev["data"]["changes"]["archived"] == json!(false) => {
                panic!("workspace auto-unarchived by the parked cancel wake (#2513): {ev}");
            }
            "hook:cancelled" if ev["data"]["hookId"] == json!(hook_id) => {
                hook_cancelled = true;
            }
            _ => {}
        }
    }

    // Let the owner settle: the stalled kickoff turn finishes, and the
    // end-of-turn drain must PARK the queued cancel wake behind the archived
    // gate instead of running it as a fresh turn.
    let deadline = tokio::time::Instant::now() + common::test_timeout(Duration::from_secs(60));
    loop {
        let listed = wss_rpc(&mut rpc, "agent.list", json!({ "workspaceId": ws_id })).await;
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
        assert!(
            tokio::time::Instant::now() < deadline,
            "the hook owner's turn never settled: {row}"
        );
        tokio::time::sleep(Duration::from_millis(100)).await;
    }

    // The workspace REMAINS archived after the owner settled (the #2513
    // symptom was `archived: false` here despite the delta above)...
    let fetched = wss_rpc(&mut rpc, "workspace.get", json!({ "workspaceId": ws_id })).await;
    assert_eq!(
        fetched["workspace"]["archived"],
        json!(true),
        "workspace stays archived; the parked wake must not restart a turn: {fetched}"
    );
    assert_eq!(fetched["workspace"]["status"], json!("Archived"));

    // ...and the cancel wake is still PARKED in the owner's queue — proof it
    // was gated rather than consumed by a stray turn.
    let queue = wss_rpc(
        &mut rpc,
        "agent.getQueue",
        json!({ "workspaceId": ws_id, "agentId": agent_id }),
    )
    .await;
    let entries = queue["queue"].as_array().expect("queue array");
    assert!(
        entries.iter().any(|m| m["content"]
            .as_str()
            .is_some_and(|c| c.contains("cancelled because its workspace was archived"))),
        "the hook-cancel wake stays parked until unarchive: {queue}"
    );
}
