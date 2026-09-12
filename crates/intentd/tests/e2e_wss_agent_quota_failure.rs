//! WSS end-to-end pins for the ADDITIVE quota signal on `agent:failed`: when a
//! turn dies because the LLM provider refused it on a usage/allowance grounds
//! (HTTP 429, an upstream `rate_limit_error`, a spent plan quota), the event
//! carries a machine-readable `errorCode: "quota-exceeded"` plus the
//! `providerId` of the provider the failing turn actually ran on — so a client
//! can offer "retry on another provider" without pattern-matching the rendered
//! `error` prose.
//!
//! Presence and ABSENCE are equally load-bearing, so all three directions are
//! driven over the real transport (TLS + bearer auth + fingerprint pinning,
//! `events.subscribe` before the action, assertions on the resulting
//! `events.event` notifications):
//!
//! 1. QUOTA-1 — a provider quota rejection stamps BOTH fields, while `error`
//!    and `turnId` keep the exact shape they had before the signal existed.
//!    The stamp is additive: nothing that was on the event before moves.
//! 2. QUOTA-2 — an ordinary (non-quota) terminal failure stamps NEITHER field.
//!    They must be ABSENT keys, never `null`/`false` — the same absent-not-false
//!    contract `sessionCorrupted` holds on `agent:status-changed`, so a client
//!    can branch on key presence alone.
//! 3. QUOTA-3 — the regression that matters most: a failure whose text says
//!    "Disk quota exceeded" (the standard `EDQUOT` strerror rendering) is a
//!    LOCAL resource problem, not a provider allowance problem. It must NOT be
//!    stamped, however broadly the `"quota"` marker would otherwise match —
//!    offering a provider switch for a full disk is actively misleading. This
//!    exercises the `LOCAL_RESOURCE_QUOTA_MARKERS` denylist end-to-end, over
//!    the wire, on the flattened text the terminal publisher actually holds.
//!
//! Each scenario drives the mock ACP child to reject EVERY `session/prompt`
//! with a configured JSON-RPC error (no attempt gate), so the daemon reaches a
//! terminal `agent:failed` rather than a silent redrive. The agent is created
//! with an explicit `provider: "mock"`, which is exactly what `providerId` must
//! report back in QUOTA-1.
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

const TOKEN: &str = "efefefefefefefefefefefefefefefefefefefefefefefefefefefefefefefef";

/// An Anthropic-style 429 body, as a provider bridge nests it — verbatim JSON
/// string — into the `data` of its `-32603`. This is where the real upstream
/// rejection text lives in production; the top-level `message` stays the
/// bridge's generic "Internal error", which is precisely why the classifier
/// reads the FULL rendered `AcpError` (message + bounded `data`) rather than
/// the message alone.
const QUOTA_429_BODY: &str = r#"{"type":"error","status":429,"error":{"type":"rate_limit_error","message":"429 Too Many Requests: usage limit reached for this organization; your quota resets at 2026-09-06T00:00:00Z"}}"#;

/// An ordinary provider-side failure with NO quota phrasing anywhere: terminal
/// (so it surfaces as `agent:failed` rather than being retried), but nothing a
/// provider switch would fix. Deliberately free of the transient fetch markers
/// too, so the turn fails once and stays failed.
const ORDINARY_FAILURE_BODY: &str = r#"{"type":"error","status":400,"error":{"type":"invalid_request_error","message":"The requested tool schema is not supported by this model."}}"#;

/// A LOCAL resource exhaustion, phrased exactly the way `EDQUOT` renders
/// through `strerror` on both platforms. It contains the literal substring
/// "quota exceeded" — the trap this test exists to keep sprung.
const DISK_QUOTA_BODY: &str = r#"{"type":"error","error":{"message":"failed to write the session log to /var/agent/state.jsonl: Disk quota exceeded (os error 69)"}}"#;

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
    let dir = PathBuf::from("/tmp").join(format!("itd-wss-quota-{}", &id[..8]));
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
        title: "WSS-QUOTA-E2E".to_string(),
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
        display_status: None,
        waiting: false,
        checkout_mode: None,
        disk_usage: None,
        pending_delete_at: None,
    }
}

/// Drive one terminal turn failure end-to-end over WSS and return the
/// `agent:failed` event's `data` object verbatim.
///
/// The mock rejects EVERY `session/prompt` with `error_data` nested in a
/// `-32603` (no attempt gate), which is the production shape: the bridge's
/// generic "Internal error" on top, the real upstream body inside `data`. That
/// makes the failure terminal on the first attempt — no silent redrive, no
/// transient in-place retry — so exactly one `agent:failed` is emitted and the
/// caller can assert its full shape.
///
/// `agent_name` and the temp data dir keep the three scenarios independent;
/// the agent is created with an explicit `provider: "mock"` so a stamped
/// `providerId` has a known expected value.
async fn failed_event_data(test: &str, agent_name: &str, error_data: &str) -> Option<Value> {
    let script = gate(test)?;
    let data_dir = temp_data_dir();
    let ws_id = seed_workspace_only(&data_dir).await;
    let behavior = json!({
        "promptRpcError": {
            "code": -32603,
            "message": "Internal error",
            "data": error_data,
        },
        "response": "unused — every prompt is rejected",
    })
    .to_string();
    let env: [(&str, &str); 5] = [
        ("INTENTD_AUTH_TOKEN", TOKEN),
        ("INTENTD_TCP_PORT", "0"),
        ("MOCK_AGENT_SCRIPT_PATH", &script),
        ("MOCK_AGENT_BEHAVIOR", &behavior),
        ("INTENTD_SESSION_SETUP_TIMEOUT_MS", "2000"),
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

    // Subscribe BEFORE driving the action: the failure is emitted as an
    // `events.event` notification and a late subscription would race it.
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
        json!({ "workspaceId": ws_id, "name": agent_name, "model": "default", "provider": "mock" }),
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
        json!({ "workspaceId": ws_id, "agentId": agent_id, "content": "drive a terminal failure" }),
    )
    .await;
    assert_eq!(sent["success"], true, "sendMessage ok: {sent}");

    for _ in 0..200 {
        let frame = wss_event(&mut sub, 30).await;
        let event = &frame["params"]["event"];
        if event["data"]["agentId"].as_str() != Some(agent_id.as_str()) {
            continue;
        }
        if event["type"] == "agent:failed" {
            return Some(event["data"].clone());
        }
    }
    panic!("no agent:failed event observed for {test}");
}

/// Seed a workspace plus an idle `mock` agent whose `last_turn_provider`
/// still names a DIFFERENT, registered provider — the persisted state after
/// `agent.setModel` moved the session from `auggie` onto `mock`: the switch
/// commits the new identity to `last_turn_provider` only once the new child
/// is up, so until then the column still names the previous provider.
async fn seed_switched_session(data_dir: &Path) -> (String, String) {
    use intent_core::{now_iso, AgentId, AgentSession, WorkspaceId};
    use intent_store::Store;
    let db_path = data_dir.join("intentd.db");
    let store = Store::open(&db_path).await.expect("open store");
    let ws = WorkspaceId::new();
    store
        .insert_workspace(&workspace_seed(&ws))
        .await
        .expect("insert ws");
    let agent_id = AgentId::from(format!("agent-{}", Uuid::new_v4()).as_str());
    let ts = now_iso();
    let session: AgentSession = serde_json::from_value(json!({
        "id": agent_id.0,
        "workspaceId": ws.0,
        "name": "WSS-QUOTA-STARTUP",
        "nameExplicitlySet": true,
        "model": "default", "provider": "mock",
        "status": "idle",
        "isActive": false,
        "createdAt": ts,
        "updatedAt": ts,
    }))
    .expect("seed session from wire shape");
    store
        .insert_agent_session(&session)
        .await
        .expect("insert session");
    store
        .set_agent_session_last_turn_model(&ws, &agent_id, Some("sonnet4.5"), "auggie")
        .await
        .expect("seed stale last_turn_provider");
    (ws.0, agent_id.0)
}

/// QUOTA-4: a quota rejection during ACP session SETUP (`session/new`, before
/// any prompt) stamps the provider whose allowance actually ran out — the one
/// the spawn attempt resolved — not the stale `last_turn_provider`.
///
/// After `agent.setModel` moves a session from A onto B, `last_turn_provider`
/// keeps naming A until B's first turn commits. If B's allowance is already
/// spent, its `session/new` is rejected before that commit; a publisher that
/// read `last_turn_provider` there would stamp `providerId: A` — the provider
/// the client should be steered TOWARD, not away from. The mock rejects every
/// `session/new` with the same bridge-wrapped 429 as QUOTA-1.
#[tokio::test]
async fn startup_quota_failure_names_the_attempted_provider_over_wss() {
    let test = "WSS startup quota-exceeded attribution E2E";
    let Some(script) = gate(test) else {
        return;
    };
    let data_dir = temp_data_dir();
    let (ws_id, agent_id) = seed_switched_session(&data_dir).await;
    let behavior = json!({
        "sessionNewRpcError": {
            "code": -32603,
            "message": "Internal error",
            "data": QUOTA_429_BODY,
        },
        "response": "unused — session setup is rejected",
    })
    .to_string();
    let env: [(&str, &str); 5] = [
        ("INTENTD_AUTH_TOKEN", TOKEN),
        ("INTENTD_TCP_PORT", "0"),
        ("MOCK_AGENT_SCRIPT_PATH", &script),
        ("MOCK_AGENT_BEHAVIOR", &behavior),
        ("INTENTD_SESSION_SETUP_TIMEOUT_MS", "2000"),
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
    let sent = wss_rpc(
        &mut rpc,
        11,
        "agent.sendMessage",
        json!({ "workspaceId": ws_id, "agentId": agent_id, "content": "drive a startup failure" }),
    )
    .await;
    assert_eq!(sent["success"], true, "sendMessage ok: {sent}");

    let mut data = None;
    for _ in 0..200 {
        let frame = wss_event(&mut sub, 30).await;
        let event = &frame["params"]["event"];
        if event["data"]["agentId"].as_str() != Some(agent_id.as_str()) {
            continue;
        }
        if event["type"] == "agent:failed" {
            data = Some(event["data"].clone());
            break;
        }
    }
    let data = data.unwrap_or_else(|| panic!("no agent:failed event observed for {test}"));

    assert_base_failure_shape(&data, "startup quota");
    // Precondition: the failure really came from session setup, not a prompt.
    let error = data["error"].as_str().unwrap_or_default();
    assert!(
        error.contains("session/new"),
        "the failure is the session-setup rejection: {error}"
    );
    assert_eq!(
        data["errorCode"], "quota-exceeded",
        "startup quota rejection stamps the machine-readable errorCode: {data}"
    );
    assert_eq!(
        data["providerId"], "mock",
        "providerId names the provider whose session/new was rejected, \
         not the stale last_turn_provider: {data}"
    );
}

/// Assert the fields that exist on EVERY `agent:failed`, quota or not: the
/// stamp is additive, so nothing here may shift when `errorCode` appears.
fn assert_base_failure_shape(data: &Value, agent_name_hint: &str) {
    assert!(
        data["agentId"].as_str().is_some_and(|s| !s.is_empty()),
        "agent:failed carries a non-empty agentId ({agent_name_hint}): {data}"
    );
    assert!(
        data["error"].as_str().is_some_and(|s| !s.is_empty()),
        "agent:failed carries the rendered error text ({agent_name_hint}): {data}"
    );
    assert!(
        data["turnId"].as_str().is_some_and(|s| !s.is_empty()),
        "agent:failed carries a non-empty turnId ({agent_name_hint}): {data}"
    );
}

/// QUOTA-1: a provider usage/quota rejection stamps `errorCode:
/// "quota-exceeded"` AND a non-empty `providerId` on `agent:failed`, on top of
/// the unchanged `agentId` / `error` / `turnId`.
///
/// The mock nests an Anthropic-shaped 429 body (`429 Too Many Requests`,
/// `rate_limit_error`, `usage limit`, `quota`) inside the `data` of a generic
/// `-32603`, exactly as a provider bridge does in production. The classifier
/// must reach into the rendered `data`, not just the bridge's "Internal error"
/// message — reading the message alone would miss every real instance.
///
/// `providerId` must name the provider the FAILING turn ran on. The agent is
/// created with an explicit `provider: "mock"`, so that is the expected value.
#[tokio::test]
async fn quota_failure_stamps_error_code_and_provider_over_wss() {
    let Some(data) = failed_event_data(
        "WSS quota-exceeded agent:failed E2E",
        "WSS-QUOTA-EXCEEDED",
        QUOTA_429_BODY,
    )
    .await
    else {
        return;
    };

    assert_base_failure_shape(&data, "quota");
    // The rendered prose is unchanged by the stamp: it still carries the
    // provider's own rejection detail, so a human reading the event learns
    // exactly what they learned before.
    let error = data["error"].as_str().unwrap_or_default();
    assert!(
        error.contains("rate_limit_error") || error.contains("429"),
        "agent:failed still renders the provider's quota detail: {error}"
    );

    assert_eq!(
        data["errorCode"], "quota-exceeded",
        "quota rejection stamps the machine-readable errorCode: {data}"
    );
    let provider_id = data["providerId"]
        .as_str()
        .unwrap_or_else(|| panic!("quota rejection stamps a string providerId: {data}"));
    assert!(
        !provider_id.is_empty(),
        "stamped providerId is non-empty: {data}"
    );
    assert_eq!(
        provider_id, "mock",
        "stamped providerId names the provider the failing turn ran on: {data}"
    );
}

/// QUOTA-2: an ordinary terminal failure — a `400 invalid_request_error` with
/// no quota phrasing anywhere — stamps NEITHER field.
///
/// Both keys must be genuinely ABSENT, not present-and-null: clients branch on
/// key presence (the same absent-not-false contract `sessionCorrupted` holds),
/// so a `null` would read as "a quota code we do not recognise" rather than
/// "not a quota failure". Checked with `.get(..).is_none()`, which a `null`
/// value would NOT satisfy.
#[tokio::test]
async fn ordinary_failure_omits_quota_fields_over_wss() {
    let Some(data) = failed_event_data(
        "WSS ordinary-failure quota-field absence E2E",
        "WSS-QUOTA-ABSENT",
        ORDINARY_FAILURE_BODY,
    )
    .await
    else {
        return;
    };

    assert_base_failure_shape(&data, "ordinary");
    assert!(
        data.get("errorCode").is_none(),
        "non-quota failure omits errorCode entirely (not null): {data}"
    );
    assert!(
        data.get("providerId").is_none(),
        "non-quota failure omits providerId entirely (not null): {data}"
    );
}

/// QUOTA-3: a LOCAL resource exhaustion must never read as a PROVIDER quota.
///
/// `EDQUOT`'s standard strerror rendering is literally "Disk quota exceeded",
/// which contains the broad `"quota"` marker the classifier otherwise matches
/// on. Without the `LOCAL_RESOURCE_QUOTA_MARKERS` denylist, a full disk would
/// be stamped `quota-exceeded` and the client would offer a provider switch
/// that cannot possibly help.
///
/// Driving a genuine `EDQUOT` through this fixture would mean filling a real
/// filesystem, so the denylist is exercised the way it actually runs in
/// production: on the flattened `session/prompt failed: …` text the terminal
/// publisher holds. The child rejects the prompt with a failure whose message
/// carries the exact strerror wording, and the daemon must leave it unstamped.
#[tokio::test]
async fn local_disk_quota_failure_is_not_a_provider_quota_over_wss() {
    let Some(data) = failed_event_data(
        "WSS local disk-quota denylist E2E",
        "WSS-QUOTA-DISK",
        DISK_QUOTA_BODY,
    )
    .await
    else {
        return;
    };

    assert_base_failure_shape(&data, "disk quota");
    // Precondition for the assertion below: the trap text really did reach the
    // event, so the denylist (not a lucky mismatch) is what kept it unstamped.
    let error = data["error"].as_str().unwrap_or_default();
    assert!(
        error.to_ascii_lowercase().contains("disk quota exceeded"),
        "the local-resource failure text reached agent:failed verbatim: {error}"
    );
    assert!(
        data.get("errorCode").is_none(),
        "a local disk-quota failure is not a provider quota — errorCode omitted: {data}"
    );
    assert!(
        data.get("providerId").is_none(),
        "a local disk-quota failure carries no providerId stamp: {data}"
    );
}
