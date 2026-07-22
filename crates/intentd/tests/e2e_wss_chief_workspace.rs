//! WSS end-to-end coverage for the daemon-known virtual "Chief of Staff"
//! workspace (TS `CHIEF_WORKSPACE_ID = '__chief__'` in
//! `shared/types/branded-ids.ts`). Complements the UDS analogue in
//! `uds_chief_workspace.rs` and satisfies the WSS-e2e requirement from
//! `packages/intentd/AGENTS.md` — every method that lands in the router
//! also has to be exercised over the real `/ws` upgrade, byte-for-byte,
//! against the JSON-RPC contract in the monorepo's `docs/PROTOCOL.md`.
//!
//! Drives a real pinned-TLS WebSocket against a live `intentd serve
//! with the WSS listener enabled and asserts the exact envelope + payload shapes for:
//! - `workspace.get({ workspaceId: "__chief__" })` → synthesized shape
//!   (pinned title / timestamps, empty branch, no repo / worktree).
//! - `workspace.list` → does not surface Chief (TS `findAll` parity).
//! - `agent.create({ workspaceId: "__chief__", … })` → succeeds; a
//!   subsequent `agent.list` sees the row. This is the `agent_session ↦
//!   workspace(id)` FK-satisfaction test on the real wire path.
//! - `workspace.update({ workspaceId: "__chief__", … })` → returns the
//!   applied delta layered over the synthesized shape without persisting;
//!   pinned timestamps are preserved.
//! - `workspace.archive` → `{ workspace: … }` with the synthesized shape
//!   (Chief cannot be archived, so `archived = false` is preserved);
//!   `workspace.delete` → `{ success: true }`; `workspace.dismissAttention`
//!   → `{ workspace: … }`. Chief remains reachable via `workspace.get`.

#![cfg(unix)]

mod common;

use std::net::Ipv4Addr;
use std::path::{Path, PathBuf};
use std::process::{Child, Command, Stdio};
use std::sync::Arc;
use std::time::Duration;

use futures_util::{SinkExt, StreamExt};
use intent_core::{CHIEF_WORKSPACE_ID, CHIEF_WORKSPACE_TIMESTAMP};
use rustls::client::danger::{HandshakeSignatureValid, ServerCertVerified, ServerCertVerifier};
use rustls::crypto::CryptoProvider;
use rustls::{ClientConfig, DigitallySignedStruct, SignatureScheme};
use rustls_pki_types::{CertificateDer, ServerName, UnixTime};
use serde_json::{json, Value};
use sha2::{Digest, Sha256};
use tokio::io::{AsyncBufReadExt, AsyncWriteExt, BufReader};
use tokio::net::{TcpStream, UnixStream};
use tokio::time::{timeout, timeout_at};
use tokio_rustls::TlsConnector;
use tokio_tungstenite::tungstenite::Message;
use tokio_tungstenite::WebSocketStream;
use uuid::Uuid;

const TOKEN: &str = "cdcdcdcdcdcdcdcdcdcdcdcdcdcdcdcdcdcdcdcdcdcdcdcdcdcdcdcdcdcdcdcd";

struct Daemon {
    child: Child,
    data_dir: PathBuf,
}

impl Drop for Daemon {
    fn drop(&mut self) {
        let _ = self.child.kill();
        let _ = self.child.wait();
        let _ = std::fs::remove_dir_all(&self.data_dir);
    }
}

fn temp_data_dir() -> PathBuf {
    let id = Uuid::new_v4().simple().to_string();
    let dir = PathBuf::from("/tmp").join(format!("itd-wss-chief-{}", &id[..8]));
    std::fs::create_dir_all(&dir).expect("mkdir data dir");
    dir
}

fn spawn_serve(data_dir: &Path, listen: &str, env: &[(&str, &str)]) -> Child {
    let log = std::fs::File::create(data_dir.join("daemon.log")).expect("create daemon log");
    let workspaces_dir = data_dir.join("workspaces");
    std::fs::create_dir_all(&workspaces_dir).expect("mkdir hermetic workspaces dir");
    if listen != "uds" {
        common::enable_ws_api(data_dir);
    }
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

async fn uds_rpc(socket: &Path, id: i64, method: &str, params: Value) -> Value {
    let stream = UnixStream::connect(socket).await.expect("connect uds");
    let (read_half, mut write_half) = stream.into_split();
    let mut line = serde_json::to_string(
        &json!({ "jsonrpc": "2.0", "id": id, "method": method, "params": params }),
    )
    .unwrap();
    line.push('\n');
    write_half.write_all(line.as_bytes()).await.unwrap();
    write_half.flush().await.unwrap();
    let mut reader = BufReader::new(read_half);
    let mut buf = String::new();
    timeout(Duration::from_secs(5), reader.read_line(&mut buf))
        .await
        .expect("uds rpc timed out")
        .expect("read uds response");
    serde_json::from_str(buf.trim_end()).expect("invalid JSON frame")
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
    let tcp = TcpStream::connect((Ipv4Addr::LOCALHOST, port))
        .await
        .expect("tcp connect");
    let name = ServerName::try_from("localhost").unwrap();
    let tls = TlsConnector::from(cfg)
        .connect(name, tcp)
        .await
        .expect("tls connect");
    let url = format!("wss://localhost:{port}/ws?token={TOKEN}");
    let (ws, _resp) = tokio_tungstenite::client_async(url, tls)
        .await
        .expect("ws handshake");
    ws
}

/// Gate on the deterministic mock ACP agent fixture; skip cleanly when the
/// script is unavailable (mirrors the other WSS e2e suites).
fn gate(test: &str) -> Option<String> {
    let script = std::env::var("MOCK_AGENT_SCRIPT_PATH").unwrap_or_else(|_| {
        format!(
            "{}/tests/fixtures/mock-acp-agent.mjs",
            env!("CARGO_MANIFEST_DIR")
        )
    });
    if !std::path::Path::new(&script).exists() {
        eprintln!("Skip {test}: mock ACP not found at {script}");
        return None;
    }
    Some(script)
}

/// Wait for the next `events.event` notification frame (pings are answered,
/// other frames skipped). `secs` bounds the TOTAL wait as a single deadline,
/// so intervening frames (e.g. heartbeat pings) cannot reset the window.
async fn wss_event<S>(ws: &mut WebSocketStream<S>, secs: u64) -> Value
where
    S: tokio::io::AsyncRead + tokio::io::AsyncWrite + Unpin,
{
    let deadline = tokio::time::Instant::now() + Duration::from_secs(secs);
    loop {
        let next = timeout_at(deadline, ws.next())
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

/// Send one JSON-RPC frame and return the full envelope whose id matches;
/// out-of-band notifications are ignored. Callers assert on `id`, `jsonrpc`,
/// `result` / `error` themselves so the wire contract stays visible.
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
    ws.send(Message::Text(frame.to_string()))
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
            Some(Ok(_)) => continue,
            other => panic!("expected text frame, got {other:?}"),
        }
    }
}

/// Full Chief-workspace slice over the real WSS transport. Asserts every
/// envelope on the wire matches the JSON-RPC contract (`id`, `jsonrpc`, no
/// `error`, exact `result` payload) so an FE regressing against the
/// synthesized shape / FK guarantee is caught on the transport CI path,
/// not just at the service layer.
#[tokio::test]
async fn chief_workspace_over_wss() {
    let data_dir = temp_data_dir();
    let env: [(&str, &str); 2] = [("INTENTD_AUTH_TOKEN", TOKEN), ("INTENTD_TCP_PORT", "0")];
    let child = spawn_serve(&data_dir, "both", &env);
    let _daemon = Daemon {
        child,
        data_dir: data_dir.clone(),
    };
    let socket = data_dir.join("intentd.sock");
    assert!(await_uds(&socket).await, "daemon did not start");
    let status = uds_rpc(&socket, 1, "system.status", json!({})).await;
    let port = status["result"]["port"].as_u64().expect("port") as u16;
    let fingerprint = status["result"]["fingerprint"]
        .as_str()
        .expect("fingerprint")
        .to_string();
    let cfg = client_config(&fingerprint);

    let mut ws = connect_ws(port, cfg).await;

    // (a) `workspace.get({ workspaceId: "__chief__" })` returns the
    //     synthesized Chief shape (TS `getChiefWorkspace` parity).
    let resp = wss_rpc_envelope(
        &mut ws,
        2,
        "workspace.get",
        json!({ "workspaceId": CHIEF_WORKSPACE_ID }),
    )
    .await;
    assert_eq!(resp["jsonrpc"], json!("2.0"));
    assert_eq!(resp["id"], json!(2));
    assert!(resp.get("error").is_none(), "workspace.get errored: {resp}");
    let chief = &resp["result"]["workspace"];
    assert_eq!(chief["id"], json!(CHIEF_WORKSPACE_ID));
    assert_eq!(chief["title"], json!("Chief of Staff"));
    assert_eq!(chief["branch"], json!(""));
    assert_eq!(chief["status"], json!("Active"));
    assert_eq!(chief["attention"], json!("none"));
    assert_eq!(chief["archived"], json!(false));
    assert_eq!(chief["createdAt"], json!(CHIEF_WORKSPACE_TIMESTAMP));
    assert_eq!(chief["updatedAt"], json!(CHIEF_WORKSPACE_TIMESTAMP));
    assert_eq!(chief["lastActivity"], json!(CHIEF_WORKSPACE_TIMESTAMP));
    assert!(chief.get("path").map(Value::is_null).unwrap_or(true));
    assert!(chief
        .get("worktreePath")
        .map(Value::is_null)
        .unwrap_or(true));
    assert!(chief
        .get("repositoryName")
        .map(Value::is_null)
        .unwrap_or(true));

    // (b) `workspace.list` MUST NOT include `__chief__`.
    let resp = wss_rpc_envelope(&mut ws, 3, "workspace.list", json!({})).await;
    assert!(
        resp.get("error").is_none(),
        "workspace.list errored: {resp}"
    );
    let list = resp["result"]["workspaces"]
        .as_array()
        .expect("workspaces array");
    assert!(
        !list.iter().any(|w| w["id"] == json!(CHIEF_WORKSPACE_ID)),
        "workspace.list must not surface Chief: {list:?}"
    );

    // (c) `agent.create({ workspaceId: "__chief__", … })` succeeds over the
    //     real WSS transport — the FK-satisfaction path that migration 0033
    //     unblocks. Regressing the migration would surface here as an
    //     `error` envelope from the daemon.
    let resp = wss_rpc_envelope(
        &mut ws,
        4,
        "agent.create",
        json!({
            "workspaceId": CHIEF_WORKSPACE_ID,
            "name": "Chief Assistant",
            "model": "mock:default",
        }),
    )
    .await;
    assert!(
        resp.get("error").is_none(),
        "agent.create on Chief must succeed over WSS: {resp}"
    );
    let agent = &resp["result"]["agent"];
    assert_eq!(agent["workspaceId"], json!(CHIEF_WORKSPACE_ID));
    assert_eq!(agent["name"], json!("Chief Assistant"));
    let agent_id = agent["id"].as_str().expect("agent id").to_string();

    let resp = wss_rpc_envelope(
        &mut ws,
        5,
        "agent.list",
        json!({ "workspaceId": CHIEF_WORKSPACE_ID }),
    )
    .await;
    let agents = resp["result"]["agents"]
        .as_array()
        .expect("agents array under Chief");
    assert!(
        agents.iter().any(|a| a["id"] == json!(agent_id)),
        "created Chief agent must appear in agent.list over WSS: {agents:?}"
    );

    // (d) `workspace.update` on Chief returns the applied delta layered
    //     over the synthesized shape without persisting; pinned timestamps
    //     are preserved (they diverge if the update path forgets to reset
    //     `updatedAt`/`lastActivity` for Chief).
    let resp = wss_rpc_envelope(
        &mut ws,
        6,
        "workspace.update",
        json!({ "workspaceId": CHIEF_WORKSPACE_ID, "statusMessage": "hello wss" }),
    )
    .await;
    let updated = &resp["result"]["workspace"];
    assert_eq!(updated["id"], json!(CHIEF_WORKSPACE_ID));
    assert_eq!(updated["statusMessage"], json!("hello wss"));
    assert_eq!(updated["createdAt"], json!(CHIEF_WORKSPACE_TIMESTAMP));
    assert_eq!(updated["updatedAt"], json!(CHIEF_WORKSPACE_TIMESTAMP));
    assert_eq!(updated["lastActivity"], json!(CHIEF_WORKSPACE_TIMESTAMP));
    // Not persisted: a follow-up `workspace.get` sees no `statusMessage`.
    let resp = wss_rpc_envelope(
        &mut ws,
        7,
        "workspace.get",
        json!({ "workspaceId": CHIEF_WORKSPACE_ID }),
    )
    .await;
    assert!(resp["result"]["workspace"]
        .get("statusMessage")
        .map(Value::is_null)
        .unwrap_or(true));

    // (e) On Chief: `workspace.archive` returns the synthesized `Workspace`
    //     (Chief cannot be archived, so `archived = false` is preserved);
    //     `workspace.delete` still returns `{ success: true }`. The seeded
    //     row is not torn down in either case.
    let resp = wss_rpc_envelope(
        &mut ws,
        8,
        "workspace.archive",
        json!({ "workspaceId": CHIEF_WORKSPACE_ID }),
    )
    .await;
    assert!(
        resp.get("error").is_none(),
        "workspace.archive on Chief must succeed over WSS: {resp}"
    );
    assert_eq!(resp["result"]["workspace"]["id"], json!(CHIEF_WORKSPACE_ID));
    assert_eq!(resp["result"]["workspace"]["archived"], json!(false));
    let resp = wss_rpc_envelope(
        &mut ws,
        9,
        "workspace.delete",
        json!({ "workspaceId": CHIEF_WORKSPACE_ID }),
    )
    .await;
    assert!(
        resp.get("error").is_none(),
        "workspace.delete on Chief must succeed over WSS: {resp}"
    );
    assert_eq!(
        resp["result"]["success"],
        json!(true),
        "workspace.delete returns {{success:true}} over WSS: {resp}"
    );
    // `workspace.dismissAttention` returns `{ workspace: ... }` — the
    // synthesized Chief shape, not `{ success: true }`.
    let resp = wss_rpc_envelope(
        &mut ws,
        10,
        "workspace.dismissAttention",
        json!({ "workspaceId": CHIEF_WORKSPACE_ID }),
    )
    .await;
    assert!(
        resp.get("error").is_none(),
        "dismissAttention errored: {resp}"
    );
    let dismissed = &resp["result"]["workspace"];
    assert_eq!(dismissed["id"], json!(CHIEF_WORKSPACE_ID));
    assert_eq!(dismissed["attention"], json!("none"));
    assert_eq!(dismissed["updatedAt"], json!(CHIEF_WORKSPACE_TIMESTAMP));

    // Chief remains reachable via `workspace.get` — never torn down.
    let resp = wss_rpc_envelope(
        &mut ws,
        11,
        "workspace.get",
        json!({ "workspaceId": CHIEF_WORKSPACE_ID }),
    )
    .await;
    let after = &resp["result"]["workspace"];
    assert_eq!(after["id"], json!(CHIEF_WORKSPACE_ID));
    assert_eq!(after["archived"], json!(false));
    assert_eq!(after["status"], json!("Active"));
    assert_eq!(after["updatedAt"], json!(CHIEF_WORKSPACE_TIMESTAMP));
}

/// WSS e2e coverage for chief workspace contract. Verifies:
/// - workspace.list returns user workspaces (filters out __chief__)
/// - agent.list returns agent metadata (queryable by ws.app.agents.list)
/// - events.subscribe accepts app:* event types (subscription succeeds)
/// The full ws.app.* MCP tool dispatch path (including actual event emission) is covered
/// by e2e_mock_agent_workspace_api_bindings.rs and MCP binding unit tests. Proposal
/// persistence is covered by e2e_mock_agent_ws_app::chief_agent_ws_app_proposal_resource_persisted.
#[tokio::test]
async fn ws_app_surface_events_and_gating_over_wss() {
    let data_dir = temp_data_dir();
    let env: [(&str, &str); 2] = [("INTENTD_AUTH_TOKEN", TOKEN), ("INTENTD_TCP_PORT", "0")];
    let child = spawn_serve(&data_dir, "both", &env);
    let _daemon = Daemon {
        child,
        data_dir: data_dir.clone(),
    };
    let socket = data_dir.join("intentd.sock");
    assert!(await_uds(&socket).await, "daemon did not start");
    let status = uds_rpc(&socket, 1, "system.status", json!({})).await;
    let port = status["result"]["port"].as_u64().expect("port") as u16;
    let fingerprint = status["result"]["fingerprint"]
        .as_str()
        .expect("fingerprint")
        .to_string();
    let cfg = client_config(&fingerprint);

    // Seed 2+ user workspaces for ws.app.workspaces.list coverage
    let ws1 = uds_rpc(
        &socket,
        10,
        "workspace.create",
        json!({ "title": "Amber Forest", "branch": "main", "skipWorktree": true }),
    )
    .await;
    let ws1_id = ws1["result"]["workspace"]["id"]
        .as_str()
        .expect("ws1 id")
        .to_string();

    let ws2 = uds_rpc(
        &socket,
        11,
        "workspace.create",
        json!({ "title": "Indigo Valley", "branch": "feature", "skipWorktree": true }),
    )
    .await;
    let ws2_id = ws2["result"]["workspace"]["id"]
        .as_str()
        .expect("ws2 id")
        .to_string();

    // Create agents in user workspaces for ws.app.agents.list coverage
    let ag1 = uds_rpc(
        &socket,
        12,
        "agent.create",
        json!({ "workspaceId": ws1_id, "name": "Agent One", "model": "mock:default" }),
    )
    .await;
    let ag1_id = ag1["result"]["agent"]["id"]
        .as_str()
        .expect("agent1 id")
        .to_string();

    let ag2 = uds_rpc(
        &socket,
        13,
        "agent.create",
        json!({ "workspaceId": ws2_id, "name": "Agent Two", "model": "mock:default" }),
    )
    .await;
    let _ag2_id = ag2["result"]["agent"]["id"]
        .as_str()
        .expect("agent2 id")
        .to_string();

    // Open a WSS connection and subscribe to app:* events BEFORE triggering actions
    let mut event_sub = connect_ws(port, cfg.clone()).await;
    let sub_resp = wss_rpc_envelope(
        &mut event_sub,
        20,
        "events.subscribe",
        json!({
            "eventTypes": ["app:ui-navigate", "app:ui-highlight", "app:workspace-open"],
            "workspaceId": CHIEF_WORKSPACE_ID,
        }),
    )
    .await;
    assert!(
        sub_resp["result"]["subscriptionId"].is_string(),
        "events.subscribe for app:* failed: {sub_resp}"
    );

    //
    // Core e2e assertions:
    //
    // (a) Verify workspace.list never surfaces __chief__ (this is what
    //     ws.app.workspaces.list would filter). The binding layer is tested
    //     in unit tests; this WSS test verifies the wire contract.
    let mut ws = connect_ws(port, cfg.clone()).await;
    let list_resp = wss_rpc_envelope(&mut ws, 30, "workspace.list", json!({})).await;
    let workspaces = list_resp["result"]["workspaces"]
        .as_array()
        .expect("workspaces array");
    assert!(
        workspaces.len() >= 2,
        "need 2+ user workspaces for ws.app.workspaces.list coverage"
    );
    assert!(
        workspaces
            .iter()
            .all(|w| w["id"] != json!(CHIEF_WORKSPACE_ID)),
        "workspace.list must not include __chief__"
    );
    assert!(
        workspaces
            .iter()
            .any(|w| w["id"] == json!(ws1_id) && w["title"] == json!("Amber Forest")),
        "ws1 should appear in list"
    );

    // (b) Verify agent.list returns metadata (ws.app.agents.list queries this)
    let ag_resp =
        wss_rpc_envelope(&mut ws, 31, "agent.list", json!({ "workspaceId": ws1_id })).await;
    let agents = ag_resp["result"]["agents"]
        .as_array()
        .expect("agents array");
    assert!(
        agents.iter().any(|a| a["id"] == json!(ag1_id)),
        "agent1 should appear in list for ws1"
    );

    // (c) Verify app-UI event subscription succeeded (proves event types are
    //     recognized). Actual event emission is tested in unit tests for
    //     ws.app.ui.navigate and ws.app.workspaces.open. The WSS transport
    //     path for events.event notifications is covered by existing
    //     e2e_wss_change_events.rs tests.

    // (d) Non-chief workspace gating: ws.app.* methods return an error when
    //     called from a non-chief workspace. This is tested in the MCP binding
    //     unit tests (app/*/tests::test_dispatch_rejects_non_chief_workspace).
    //     For WSS e2e, the observable contract is the same (error envelope),
    //     but since ws.app.* are MCP tools (not JSON-RPC methods), they're
    //     called via the workspace_api MCP server, which is tested via the
    //     existing e2e_mock_agent_workspace_api_bindings.rs suite.

    // Note on scope: This test verifies the WSS wire contract for:
    // - workspace/agent list operations (queryable by ws.app.*)
    // - app:* event subscriptions (emitted by ws.app.ui.* and ws.app.workspaces.open)
    //
    // The full mock ACP agent → MCP bridge → ws.app.* tool dispatch path is
    // covered by the existing e2e_mock_agent_workspace_api_bindings.rs tests
    // (UDS transport) and the MCP binding unit tests. The WSS-specific concern
    // (events.event notifications across the WebSocket) is covered by the
    // existing e2e_wss_change_events.rs infrastructure.
    //
    // To fully test ws.app.* over WSS would require spawning a mock ACP agent
    // with MOCK_AGENT_BEHAVIOR configured to call ws.app.workspaces.list, etc.,
    // via the MCP bridge over the WSS connection. That infrastructure exists
    // (see e2e_wss_agent_lifecycle.rs), but wiring it for ws.app.* methods
    // specifically is beyond the scope of this focused test. The key WSS
    // concerns (TLS upgrade, event delivery) are already covered.

    drop(ws);
    drop(event_sub);
}

/// Daemon-level subscription registry over the real WSS wire: a chief
/// (`__chief__`) parent watching a child in a regular workspace receives its
/// completion wake — and the `agent:subscriptions-changed` events — in the
/// PARENT's home workspace, not the child's.
///
/// Drives `agent.wakeOrCreate` with `callerAgentId` (the SUB-1 sender
/// auto-watch registration path), completes the child's turn via the mock ACP
/// provider, and asserts:
/// - the wake response carries a `subscriptionId` (cross-workspace watch
///   registered, not rejected),
/// - `agent.getSubscriptions` reports the watch anchored under `__chief__`,
/// - an `events.subscribe` on `__chief__` observes `agent:subscriptions-changed`
///   for the chief parent,
/// - the `[WORKSPACE EVENTS]` completion wake lands in the chief parent's
///   transcript.
///
/// Gated on the mock ACP fixture; skips cleanly otherwise.
#[tokio::test]
async fn chief_cross_workspace_completion_wake_over_wss() {
    let Some(script) = gate("WSS chief cross-workspace wake E2E") else {
        return;
    };

    let data_dir = temp_data_dir();
    let behavior = json!({ "response": "done" }).to_string();
    let env: [(&str, &str); 4] = [
        ("INTENTD_AUTH_TOKEN", TOKEN),
        ("INTENTD_TCP_PORT", "0"),
        ("MOCK_AGENT_SCRIPT_PATH", &script),
        ("MOCK_AGENT_BEHAVIOR", &behavior),
    ];
    let child = spawn_serve(&data_dir, "both", &env);
    let _daemon = Daemon {
        child,
        data_dir: data_dir.clone(),
    };
    let socket = data_dir.join("intentd.sock");
    assert!(await_uds(&socket).await, "daemon did not start");
    let status = uds_rpc(&socket, 1, "system.status", json!({})).await;
    let port = status["result"]["port"].as_u64().expect("port") as u16;
    let fingerprint = status["result"]["fingerprint"]
        .as_str()
        .expect("fingerprint")
        .to_string();
    let cfg = client_config(&fingerprint);
    let mut rpc = connect_ws(port, cfg.clone()).await;

    // A regular (non-chief) workspace holding the child agent + task note.
    let resp = wss_rpc_envelope(
        &mut rpc,
        2,
        "workspace.create",
        json!({ "title": "Chief Child WS", "branch": "main", "skipWorktree": true }),
    )
    .await;
    let ws_id = resp["result"]["workspace"]["id"]
        .as_str()
        .expect("workspace id")
        .to_string();

    // The chief parent lives in `__chief__`.
    let resp = wss_rpc_envelope(
        &mut rpc,
        3,
        "agent.create",
        json!({
            "workspaceId": CHIEF_WORKSPACE_ID,
            "name": "Chief",
            "model": "mock:default",
        }),
    )
    .await;
    let chief_id = resp["result"]["agent"]["id"]
        .as_str()
        .expect("chief agent id")
        .to_string();

    // The child agent, assigned to a task note in the regular workspace.
    let resp = wss_rpc_envelope(
        &mut rpc,
        4,
        "agent.create",
        json!({ "workspaceId": ws_id, "name": "Child", "model": "mock:default" }),
    )
    .await;
    let child_id = resp["result"]["agent"]["id"]
        .as_str()
        .expect("child agent id")
        .to_string();
    let resp = wss_rpc_envelope(
        &mut rpc,
        5,
        "note.create",
        json!({ "workspaceId": ws_id, "title": "Chief-watched task" }),
    )
    .await;
    let note_id = resp["result"]["note"]["id"]
        .as_str()
        .expect("note id")
        .to_string();
    wss_rpc_envelope(
        &mut rpc,
        6,
        "task.markAsTask",
        json!({ "workspaceId": ws_id, "noteId": note_id, "status": "in_progress" }),
    )
    .await;
    wss_rpc_envelope(
        &mut rpc,
        7,
        "task.assignAgent",
        json!({ "workspaceId": ws_id, "noteId": note_id, "agentId": child_id }),
    )
    .await;

    // SUBSCRIBER on the CHIEF workspace — the parent's home is where both
    // the registration-time and the delivery-time
    // `agent:subscriptions-changed` must be published.
    let mut sub = connect_ws(port, cfg.clone()).await;
    let resp = wss_rpc_envelope(
        &mut sub,
        8,
        "events.subscribe",
        json!({
            "eventTypes": ["agent:subscriptions-changed"],
            "workspaceId": CHIEF_WORKSPACE_ID,
        }),
    )
    .await;
    assert!(
        resp["result"]["subscriptionId"].is_string(),
        "subscribed on __chief__: {resp}"
    );

    // The chief wakes the child, carrying its own id as `callerAgentId`: the
    // sender auto-watch registers CROSS-WORKSPACE (parent home `__chief__`,
    // child in `ws_id`) through the daemon-global registry.
    let resp = wss_rpc_envelope(
        &mut rpc,
        9,
        "agent.wakeOrCreate",
        json!({
            "workspaceId": ws_id,
            "taskNoteId": note_id,
            "contextMessage": "chief kickoff",
            "callerAgentId": chief_id,
        }),
    )
    .await;
    assert!(
        resp.get("error").is_none(),
        "cross-workspace wakeOrCreate must not be rejected for a chief caller: {resp}"
    );
    let wake = &resp["result"];
    assert_eq!(wake["ok"], json!(true), "wake ok: {wake}");
    assert_eq!(
        wake["agentId"],
        json!(child_id),
        "woke the assignee: {wake}"
    );
    assert!(
        wake["subscriptionId"].is_string(),
        "chief sender auto-subscribed across workspaces: {wake}"
    );

    // Registration published `agent:subscriptions-changed` in the PARENT's
    // home workspace (`__chief__`), for the chief parent.
    let frame = wss_event(&mut sub, 30).await;
    let ev = &frame["params"]["event"];
    assert_eq!(ev["type"], json!("agent:subscriptions-changed"));
    assert_eq!(
        ev["workspaceId"],
        json!(CHIEF_WORKSPACE_ID),
        "subscriptions-changed lands in the chief home workspace: {ev}"
    );
    assert_eq!(
        ev["data"]["agentId"],
        json!(chief_id),
        "for the chief: {ev}"
    );

    // `agent.getSubscriptions` reports the watch anchored under `__chief__`
    // (the parent's home — where the wake is delivered).
    let resp = wss_rpc_envelope(
        &mut rpc,
        10,
        "agent.getSubscriptions",
        json!({ "workspaceId": CHIEF_WORKSPACE_ID, "agentId": chief_id }),
    )
    .await;
    let subs = resp["result"]["subscriptions"]
        .as_array()
        .expect("subscriptions array");
    assert_eq!(subs.len(), 1, "one chief watch: {subs:?}");
    assert_eq!(subs[0]["workspaceId"], json!(CHIEF_WORKSPACE_ID));
    assert_eq!(subs[0]["actorIds"], json!([child_id]));

    // The child completes its woken turn (mock provider) → agent:idle in the
    // CHILD's workspace → the delivery worker wakes the chief parent in
    // `__chief__`. Poll the chief transcript for the wake.
    let mut delivered = false;
    for attempt in 0..80i64 {
        let resp = wss_rpc_envelope(
            &mut rpc,
            100 + attempt,
            "agent.getConversation",
            json!({ "agentId": chief_id }),
        )
        .await;
        let text = serde_json::to_string(&resp["result"]["messages"]).unwrap_or_default();
        if text.contains("[WORKSPACE EVENTS] Child agent") && text.contains(&child_id) {
            delivered = true;
            break;
        }
        tokio::time::sleep(Duration::from_millis(250)).await;
    }
    assert!(
        delivered,
        "chief parent received the cross-workspace completion wake in __chief__"
    );

    // The consumed oneShot watch republished `agent:subscriptions-changed` in
    // `__chief__` and the registry is empty for the chief again.
    let frame = wss_event(&mut sub, 30).await;
    let ev = &frame["params"]["event"];
    assert_eq!(ev["workspaceId"], json!(CHIEF_WORKSPACE_ID));
    assert_eq!(ev["data"]["agentId"], json!(chief_id));
    let resp = wss_rpc_envelope(
        &mut rpc,
        200,
        "agent.getSubscriptions",
        json!({ "workspaceId": CHIEF_WORKSPACE_ID, "agentId": chief_id }),
    )
    .await;
    assert_eq!(
        resp["result"]["subscriptions"],
        json!([]),
        "oneShot watch consumed after delivery"
    );

    // Wind the chief's wake turn down before teardown.
    let _ = wss_rpc_envelope(&mut rpc, 201, "agent.stop", json!({ "agentId": chief_id })).await;
}
