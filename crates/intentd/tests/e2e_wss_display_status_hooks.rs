//! WSS e2e for the orthogonal `waiting` workspace flag (PROTOCOL §5.1): a
//! workspace whose idle agent owns an ACTIVE background hook serves its base
//! `displayStatus` rollup (`idle`) with additive `waiting: true` — wait
//! signals no longer fold into the `in_progress` promotion — and settling
//! the hook drops the flag. Over the real WSS transport (TLS + bearer auth,
//! mock ACP agent):
//!
//! 1. An agent turn schedules a watcher hook via the MCP-only
//!    `ws.hook.schedule`; the turn itself promotes `in_progress`
//!    (agent running), the newly ACTIVE hook emits the transition-only
//!    `workspace:waiting-changed` raise (`{ workspaceId, waiting: true }`,
//!    asserted over a real WSS subscription), and after the terminal
//!    `agent:idle` the debounced recompute demotes to `idle` despite the
//!    ACTIVE hook. `workspace.get` and `workspace.list` then serve `idle`
//!    with `waiting: true`.
//! 2. The FE settles the hook via the `hook.cancel` router method (§5.40):
//!    `hook:cancelled` fires, the `workspace:waiting-changed` drop
//!    (`{ workspaceId, waiting: false }`) is asserted on the same
//!    subscription, and both read paths drop the `waiting` field (omitted
//!    when false) while the rollup stays `idle`.
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
const TOKEN: &str = "2b2b2b2b2b2b2b2b2b2b2b2b2b2b2b2b2b2b2b2b2b2b2b2b2b2b2b2b2b2b2b2b";

/// Kickoff marker for the hook-scheduling agent turn.
const SCHEDULE_MARKER: &str = "SCHEDULE_HOOK_DSTATUS_E2E";

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
    let dir = PathBuf::from("/tmp").join(format!("itd-dshook-{}", &id[..8]));
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
            title: "DSHOOK-E2E".to_string(),
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
            setup_result: None,
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

/// `workspace.get` → the full workspace row.
async fn get_workspace_row(rpc: &mut TlsWs, ws_id: &str) -> Value {
    let got = wss_rpc(rpc, "workspace.get", json!({ "workspaceId": ws_id })).await;
    got["workspace"].clone()
}

/// `workspace.get` → the derived `displayStatus` string.
async fn get_display_status(rpc: &mut TlsWs, ws_id: &str) -> String {
    get_workspace_row(rpc, ws_id).await["displayStatus"]
        .as_str()
        .expect("displayStatus string")
        .to_string()
}

/// `workspace.list` → the seeded workspace's full row.
async fn list_workspace_row(rpc: &mut TlsWs, ws_id: &str) -> Value {
    let listed = wss_rpc(rpc, "workspace.list", json!({})).await;
    listed["workspaces"]
        .as_array()
        .expect("workspaces array")
        .iter()
        .find(|w| w["id"] == json!(ws_id))
        .cloned()
        .expect("seeded workspace listed")
}

/// Poll `workspace.get` until `displayStatus == want` (bounded), asserting
/// the read path never serves `needs_attention` along the way.
async fn poll_display_status(rpc: &mut TlsWs, ws_id: &str, want: &str) {
    for _ in 0..120 {
        let status = get_display_status(rpc, ws_id).await;
        assert_ne!(
            status, "needs_attention",
            "read path must not serve needs_attention"
        );
        if status == want {
            return;
        }
        tokio::time::sleep(Duration::from_millis(250)).await;
    }
    panic!("displayStatus never settled at {want}");
}

/// The active-hook `waiting` flag + `hook.cancel` settlement (see module
/// docs): the agent's turn schedules a persisting watcher hook, the turn's
/// terminal idle demotes the rollup to `idle` despite the ACTIVE hook, both
/// read paths serve `idle` with `waiting: true`, and cancelling the hook
/// over the wire drops the flag (omitted on the wire) with the rollup
/// unchanged.
#[tokio::test]
async fn active_hook_serves_waiting_and_hook_cancel_drops_it_over_wss() {
    let Some(script) = gate("WSS displayStatus active-hook E2E") else {
        return;
    };

    // The marker turn schedules a long-delay watcher whose validation run
    // keeps watching ({ dispatch: false }) — the hook stays ACTIVE
    // (scheduled) and no re-run fires inside the test window.
    let schedule_js = format!(
        "return await ws.hook.schedule({{ name: 'dswatch', code: {}, delayMs: 60000 }});",
        json!("return { dispatch: false };")
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
    let port =
        u16::try_from(status["result"]["port"].as_u64().expect("port")).expect("value fits in u16");
    let fingerprint = status["result"]["fingerprint"]
        .as_str()
        .expect("fingerprint")
        .to_string();
    let cfg = client_config(&fingerprint);

    let mut rpc = connect_ws(port, cfg.clone()).await;
    // Baseline read: a freshly seeded workspace (no tasks, no PRs, no agents,
    // no hooks) serves `idle` — and seeds the last-observed cache, so the
    // promotion below is a real transition that emits.
    assert_eq!(get_display_status(&mut rpc, &ws_id).await, "idle");

    // SUBSCRIBER conn — registered BEFORE the turn so we miss nothing.
    let mut sub = connect_ws(port, cfg.clone()).await;
    let sub_resp = wss_rpc(
        &mut sub,
        "events.subscribe",
        json!({
            "eventTypes": [
                "workspace:displayStatus-changed",
                "workspace:waiting-changed",
                "hook:*",
                "agent:*",
            ],
            "workspaceId": ws_id,
        }),
    )
    .await;
    assert!(
        sub_resp["subscriptionId"].is_string(),
        "subscribed: {sub_resp}"
    );

    // ---- (1) Hold: the marker turn schedules the watcher hook ----
    let created = wss_rpc(
        &mut rpc,
        "agent.create",
        json!({ "workspaceId": ws_id, "name": "hookowner", "model": "default", "provider": "mock" }),
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

    // Order-insensitive milestones under one deadline: the in_progress
    // promotion (the running turn — wait signals no longer promote), the
    // persisted hook:scheduled (carrying the hookId), the transition-only
    // `workspace:waiting-changed` raise (the newly ACTIVE hook flips the
    // orthogonal flag; self-sufficient `{ workspaceId, waiting }` payload,
    // PROTOCOL §6.5), the turn's terminal agent:idle, and the debounced
    // not-running demotion back to `idle` (the ACTIVE hook no longer holds
    // the rollup). Every displayStatus transition must be one of that
    // promotion/demotion pair — never needs_attention — and no
    // waiting-changed emission in this phase may report `false`.
    let mut promoted = false;
    let mut hook_id = None::<String>;
    let mut waiting_raised = false;
    let mut idle = false;
    let mut demoted = false;
    let deadline = tokio::time::Instant::now() + common::test_timeout(Duration::from_secs(60));
    while !(promoted && hook_id.is_some() && waiting_raised && idle && demoted) {
        let Some(ev) = wss_event_until(&mut sub, deadline).await else {
            panic!(
                "timed out: promoted={promoted} hook_id={hook_id:?} \
                 waiting_raised={waiting_raised} idle={idle} demoted={demoted}"
            )
        };
        let data = &ev["data"];
        match ev["type"].as_str().unwrap_or_default() {
            "workspace:displayStatus-changed" if data["displayStatus"] == "in_progress" => {
                assert_eq!(
                    data,
                    &json!({ "workspaceId": ws_id, "displayStatus": "in_progress" }),
                    "self-sufficient in_progress promotion payload (PROTOCOL §6.5): {ev}"
                );
                promoted = true;
            }
            "workspace:displayStatus-changed" => {
                assert_eq!(
                    data,
                    &json!({ "workspaceId": ws_id, "displayStatus": "idle" }),
                    "the post-turn demotion serves the base rollup despite the hook: {ev}"
                );
                demoted = true;
            }
            "workspace:waiting-changed" => {
                assert_eq!(
                    data,
                    &json!({ "workspaceId": ws_id, "waiting": true }),
                    "self-sufficient waiting raise payload (PROTOCOL §6.5): {ev}"
                );
                assert!(
                    !waiting_raised,
                    "transition-only: the raise must emit exactly once: {ev}"
                );
                waiting_raised = true;
            }
            "hook:scheduled" if data["name"] == "dswatch" => {
                assert_eq!(data["agentId"], json!(agent_id), "hook owner: {ev}");
                hook_id = Some(data["hookId"].as_str().expect("hookId").to_string());
            }
            "agent:idle" if data["agentId"] == json!(agent_id) => idle = true,
            _ => {}
        }
    }
    let hook_id = hook_id.expect("hook:scheduled carried the hookId");

    // Both read paths serve the base rollup with the orthogonal waiting
    // flag: `idle` + `waiting: true` while the hook is ACTIVE.
    let row = get_workspace_row(&mut rpc, &ws_id).await;
    assert_eq!(
        row["displayStatus"], "idle",
        "workspace.get serves the base rollup while the hook is active: {row}"
    );
    assert_eq!(
        row["waiting"],
        json!(true),
        "workspace.get carries waiting: true while the hook is active: {row}"
    );
    let row = list_workspace_row(&mut rpc, &ws_id).await;
    assert_eq!(
        row["displayStatus"], "idle",
        "workspace.list serves the base rollup while the hook is active: {row}"
    );
    assert_eq!(
        row["waiting"],
        json!(true),
        "workspace.list carries waiting: true while the hook is active: {row}"
    );

    // ---- (2) Settle: hook.cancel over the wire drops the flag ----
    let cancelled = wss_rpc(
        &mut rpc,
        "hook.cancel",
        json!({ "workspaceId": ws_id, "hookId": hook_id }),
    )
    .await;
    assert_eq!(cancelled["ok"], json!(true), "{cancelled}");
    assert_eq!(cancelled["hook"]["state"], "cancelled", "{cancelled}");
    assert_eq!(cancelled["hook"]["hookId"], json!(hook_id));

    // Milestones: hook:cancelled, the transition-only
    // `workspace:waiting-changed` drop (the last ACTIVE hook settled —
    // self-sufficient `{ workspaceId, waiting: false }` payload), and the
    // wake turn's terminal agent:idle (the FE cancel wakes the owner, whose
    // follow-up turn may transiently re-promote in_progress — tolerated;
    // needs_attention never appears).
    let mut hook_cancelled = false;
    let mut waiting_dropped = false;
    let mut wake_idle = false;
    let deadline = tokio::time::Instant::now() + common::test_timeout(Duration::from_secs(60));
    while !(hook_cancelled && waiting_dropped && wake_idle) {
        let Some(ev) = wss_event_until(&mut sub, deadline).await else {
            panic!(
                "timed out: hook_cancelled={hook_cancelled} \
                 waiting_dropped={waiting_dropped} wake_idle={wake_idle}"
            )
        };
        let data = &ev["data"];
        match ev["type"].as_str().unwrap_or_default() {
            "hook:cancelled" if data["hookId"] == json!(hook_id) => {
                assert_eq!(data["state"], "cancelled", "{ev}");
                hook_cancelled = true;
            }
            "workspace:waiting-changed" => {
                assert_eq!(
                    data,
                    &json!({ "workspaceId": ws_id, "waiting": false }),
                    "self-sufficient waiting drop payload (PROTOCOL §6.5): {ev}"
                );
                assert!(
                    !waiting_dropped,
                    "transition-only: the drop must emit exactly once: {ev}"
                );
                waiting_dropped = true;
            }
            "workspace:displayStatus-changed" => {
                assert_ne!(
                    data["displayStatus"], "needs_attention",
                    "hook settlement never raises needs_attention: {ev}"
                );
            }
            "agent:idle" if data["agentId"] == json!(agent_id) => wake_idle = true,
            _ => {}
        }
    }

    // With the hook settled and the wake turn over, both read paths settle
    // at the base rollup with the waiting field dropped — omitted on the
    // wire, never `false` (presence-detected, §5 convention).
    poll_display_status(&mut rpc, &ws_id, "idle").await;
    let row = get_workspace_row(&mut rpc, &ws_id).await;
    assert!(
        row.get("waiting").is_none(),
        "workspace.get omits waiting after hook.cancel: {row}"
    );
    let row = list_workspace_row(&mut rpc, &ws_id).await;
    assert_eq!(
        row["displayStatus"], "idle",
        "workspace.list settles at the base rollup after hook.cancel: {row}"
    );
    assert!(
        row.get("waiting").is_none(),
        "workspace.list omits waiting after hook.cancel: {row}"
    );
}
