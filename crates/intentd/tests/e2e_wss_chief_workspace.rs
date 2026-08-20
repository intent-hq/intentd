//! WSS end-to-end coverage for the daemon-known virtual "Chief of Staff"
//! workspace (TS `CHIEF_WORKSPACE_ID = '__chief__'` in
//! `shared/types/branded-ids.ts`). Complements the UDS analogue in
//! `uds_chief_workspace.rs` and satisfies the WSS-e2e requirement from
//! `packages/intentd/AGENTS.md` — every method that lands in the router
//! also has to be exercised over the real `/ws` upgrade, byte-for-byte,
//! against the JSON-RPC contract in the monorepo's `docs/protocol/`.
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
//! - `ws.app.agents.waitFor` (chief-gated cross-workspace waiting): the
//!   immediate and after_all modes end-to-end through the real MCP bridge
//!   (mock ACP provider → `workspace_api` tool → service registry → wake
//!   delivery), plus the non-chief gating error — see the three
//!   `*_waitfor_*` tests at the bottom of this file.
//! - `ws.workspace.archive` / `ws.workspace.unarchive` (#733): the regular-
//!   workspace roundtrip through the real MCP bridge (tool result shapes,
//!   `details()` reflecting the status flip, `workspace:updated` deltas over
//!   a live subscription) and the chief-workspace binding-layer refusal —
//!   see the two `*_archive_*` tests at the bottom of this file.

#![cfg(unix)]

mod common;

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
    timeout(common::rpc_read_timeout(), reader.read_line(&mut buf))
        .await
        .expect("uds rpc timed out")
        .expect("read uds response");
    serde_json::from_str(buf.trim_end()).expect("invalid JSON frame")
}

/// Pin `workspaceApi.toonOutput` off over UDS so `workspace_api` tool result
/// bodies stay plain JSON for the `serde_json::from_str` assertions below
/// (TOON encoding is on by default).
async fn disable_toon_output(socket: &Path) {
    let resp = uds_rpc(
        socket,
        900,
        "settings.update",
        json!({ "changes": [ { "path": "workspaceApi.toonOutput", "value": false } ] }),
    )
    .await;
    assert!(
        resp.get("error").is_none(),
        "disable toonOutput failed: {resp}"
    );
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
    let status = common::await_wss_status(&socket).await;
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

/// Chief provider children spawn in the dedicated, daemon-owned, EMPTY
/// `<data_dir>/chief-cwd` directory — never `/tmp` (STAB-50: auggie's
/// `--allow-indexing` over a large shared `/tmp` blew the child's V8 heap
/// cap). Drives a real chief agent turn over WSS with the mock ACP provider
/// echoing its `process.cwd()` into the response text, then asserts the
/// child's actual working directory resolves to `<data_dir>/chief-cwd`,
/// which was auto-created (fresh data dir) and left empty.
#[tokio::test]
async fn chief_agent_spawns_in_dedicated_cwd_over_wss() {
    let Some(script) = gate("WSS chief dedicated cwd E2E") else {
        return;
    };

    let data_dir = temp_data_dir();
    let behavior = json!({ "response": "done", "echoCwd": true }).to_string();
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
    let status = common::await_wss_status(&socket).await;
    let port = status["result"]["port"].as_u64().expect("port") as u16;
    let fingerprint = status["result"]["fingerprint"]
        .as_str()
        .expect("fingerprint")
        .to_string();
    let cfg = client_config(&fingerprint);
    let mut rpc = connect_ws(port, cfg).await;

    // Fresh data dir: the chief cwd must not pre-exist — the spawn path
    // creates it on demand.
    let chief_cwd = intent_core::chief_cwd_root(&data_dir);
    assert!(
        !chief_cwd.exists(),
        "fresh data dir must not carry {}",
        chief_cwd.display()
    );

    let resp = wss_rpc_envelope(
        &mut rpc,
        2,
        "agent.create",
        json!({
            "workspaceId": CHIEF_WORKSPACE_ID,
            "name": "Chief Cwd Probe",
            "model": "mock:default",
        }),
    )
    .await;
    assert!(resp.get("error").is_none(), "agent.create errored: {resp}");
    let agent_id = resp["result"]["agent"]["id"]
        .as_str()
        .expect("agent id")
        .to_string();

    let resp = wss_rpc_envelope(
        &mut rpc,
        3,
        "agent.sendMessage",
        json!({
            "workspaceId": CHIEF_WORKSPACE_ID,
            "agentId": agent_id,
            "content": "where do you live?",
        }),
    )
    .await;
    assert_eq!(
        resp["result"]["success"],
        json!(true),
        "sendMessage: {resp}"
    );

    // The mock child stamps `cwd=<process.cwd()>` into its response text.
    let echoed = poll_conversation(&mut rpc, 100, &agent_id, "echoed cwd", |m| {
        let text = serde_json::to_string(m).unwrap_or_default();
        text.split("cwd=")
            .nth(1)
            .and_then(|rest| rest.split(['"', ' ']).next())
            .map(str::to_string)
    })
    .await;

    // The kernel resolves symlinks in the spawn cwd (`/tmp` →
    // `/private/tmp` on macOS), so compare canonicalized paths.
    let expected = std::fs::canonicalize(&chief_cwd).expect("chief cwd created on demand");
    let actual = std::fs::canonicalize(&echoed).unwrap_or_else(|_| PathBuf::from(&echoed));
    assert_eq!(
        actual, expected,
        "chief child must spawn in the dedicated chief-cwd dir, got {echoed}"
    );
    assert_ne!(
        actual,
        Path::new("/tmp"),
        "chief child must never spawn in /tmp"
    );
    if let Ok(shared_tmp) = std::fs::canonicalize("/tmp") {
        assert_ne!(
            shared_tmp, actual,
            "chief child must never spawn in the shared temp dir"
        );
    }

    // The dedicated cwd stays empty: nothing to index.
    let entries = std::fs::read_dir(&chief_cwd)
        .expect("read chief cwd")
        .count();
    assert_eq!(entries, 0, "dedicated chief cwd must be empty");

    let _ = wss_rpc_envelope(&mut rpc, 200, "agent.stop", json!({ "agentId": agent_id })).await;
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
    let status = common::await_wss_status(&socket).await;
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
    let status = common::await_wss_status(&socket).await;
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

    // The consumed watch republished `agent:subscriptions-changed` in
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
        "watch consumed after delivery"
    );

    // Wind the chief's wake turn down before teardown.
    let _ = wss_rpc_envelope(&mut rpc, 201, "agent.stop", json!({ "agentId": chief_id })).await;
}

/// Extract every JSON-parsable `tool_result` text payload from an
/// `agent.getConversation` result's `messages` array. The mock provider's
/// `emitToolBlocks` persists each MCP tool call as a `tool_use` +
/// `tool_result` block pair whose `output[0].text` carries the JSON the
/// agent-side JS returned (same wire shape `e2e_mock_agent_ws_app.rs`
/// asserts at the service layer).
fn tool_result_jsons(messages: &Value) -> Vec<Value> {
    messages
        .as_array()
        .into_iter()
        .flatten()
        .filter_map(|m| m["contentBlocks"].as_array())
        .flatten()
        .filter(|b| b["type"] == json!("tool_result"))
        .filter_map(|b| b["output"].as_array().and_then(|arr| arr.first()))
        .filter_map(|item| item["text"].as_str())
        .filter_map(|text| serde_json::from_str(text).ok())
        .collect()
}

/// Poll the agent's transcript over WSS until `pred` returns Some, or panic
/// with `what` after ~20s. Returns the predicate's payload.
async fn poll_conversation<S, T>(
    ws: &mut WebSocketStream<S>,
    id_base: i64,
    agent_id: &str,
    what: &str,
    mut pred: impl FnMut(&Value) -> Option<T>,
) -> T
where
    S: tokio::io::AsyncRead + tokio::io::AsyncWrite + Unpin,
{
    for attempt in 0..80i64 {
        let resp = wss_rpc_envelope(
            ws,
            id_base + attempt,
            "agent.getConversation",
            json!({ "agentId": agent_id }),
        )
        .await;
        assert!(
            resp.get("error").is_none(),
            "agent.getConversation errored: {resp}"
        );
        if let Some(v) = pred(&resp["result"]["messages"]) {
            return v;
        }
        tokio::time::sleep(Duration::from_millis(250)).await;
    }
    panic!("timed out waiting for {what}");
}

/// Poll `agent.getSubscriptions` over WSS until `pred` accepts the result
/// payload, or panic with `what` after ~20s. Returns the accepted payload.
async fn poll_subscriptions<S>(
    ws: &mut WebSocketStream<S>,
    id_base: i64,
    agent_workspace: &str,
    agent_id: &str,
    what: &str,
    mut pred: impl FnMut(&Value) -> bool,
) -> Value
where
    S: tokio::io::AsyncRead + tokio::io::AsyncWrite + Unpin,
{
    for attempt in 0..80i64 {
        let resp = wss_rpc_envelope(
            ws,
            id_base + attempt,
            "agent.getSubscriptions",
            json!({ "workspaceId": agent_workspace, "agentId": agent_id }),
        )
        .await;
        assert!(
            resp.get("error").is_none(),
            "agent.getSubscriptions errored: {resp}"
        );
        if pred(&resp["result"]) {
            return resp["result"].clone();
        }
        tokio::time::sleep(Duration::from_millis(250)).await;
    }
    panic!("timed out waiting for {what}");
}

/// Seed one user workspace + one mock-model target agent over WSS; returns
/// `(workspace_id, agent_id)`. Target names are test-controlled so the
/// chief's JS can discover them via `ws.app.agents.list`.
async fn seed_target<S>(
    ws: &mut WebSocketStream<S>,
    id_base: i64,
    ws_title: &str,
    agent_name: &str,
) -> (String, String)
where
    S: tokio::io::AsyncRead + tokio::io::AsyncWrite + Unpin,
{
    let resp = wss_rpc_envelope(
        ws,
        id_base,
        "workspace.create",
        json!({ "title": ws_title, "branch": "main", "skipWorktree": true }),
    )
    .await;
    let ws_id = resp["result"]["workspace"]["id"]
        .as_str()
        .expect("workspace id")
        .to_string();
    let resp = wss_rpc_envelope(
        ws,
        id_base + 1,
        "agent.create",
        json!({ "workspaceId": ws_id, "name": agent_name, "model": "mock:default" }),
    )
    .await;
    let agent_id = resp["result"]["agent"]["id"]
        .as_str()
        .expect("agent id")
        .to_string();
    (ws_id, agent_id)
}

/// `ws.app.agents.waitFor` (immediate mode) end-to-end over the real WSS
/// wire: a chief-workspace agent registers completion watches on targets in
/// TWO different user workspaces through the real MCP bridge, and the daemon
/// wakes it once per settling target.
///
/// Asserts:
/// - the persisted tool result carries the documented `{ ok, waitMode,
///   results }` shape with a `subscriptionId` and `groupId: null` per target,
/// - a SECOND waitFor on the already-watched targets (same turn) is rejected
///   with the pair-uniqueness `-32602` error naming the target — the wire
///   contract for the duplicate-registration rejection,
/// - `agent.getSubscriptions` reports both completion watches anchored under
///   `__chief__` BEFORE the targets settle (the rejected duplicate added
///   nothing),
/// - registration publishes `agent:subscriptions-changed` in `__chief__`,
/// - each target's completion delivers a `[WORKSPACE EVENTS]` wake into the
///   chief transcript and the consumed watches drain the registry,
/// - a retried wait on the settled (idle, nothing-pending) targets is
///   rejected by the monorepo#2972 idle-target guard over the wire,
/// - waits re-registered on fresh targets are removed one-at-a-time by the
///   scoped `agent.cancelSubscriptions` (`subscriptionId`), an unknown id is
///   rejected with `-32602`, and the unscoped call removes the rest.
#[tokio::test]
async fn chief_waitfor_immediate_cross_workspace_over_wss() {
    let Some(script) = gate("WSS chief waitFor immediate E2E") else {
        return;
    };

    let data_dir = temp_data_dir();
    // The REGISTER_WAITS turn discovers the seeded targets by their
    // test-controlled names, registers immediate-mode waits on both, then
    // immediately retries the same registration — the duplicate must be
    // rejected (pair uniqueness) without disturbing the live watches.
    let js = "const listing = await ws.app.agents.list({ includeCompleted: true });\n\
              const targets = listing.threads.filter((t) => String(t.agentName).startsWith('Target ')).map((t) => t.agentId);\n\
              const first = await ws.app.agents.waitFor({ agentIds: targets, waitMode: 'immediate' });\n\
              let duplicateError = null;\n\
              try { await ws.app.agents.waitFor({ agentIds: targets, waitMode: 'immediate' }); }\n\
              catch (error) { duplicateError = error.message; }\n\
              return { ...first, duplicateError };";
    // The SECOND_ROUND turn first retries the settled (RuntimeIdle,
    // nothing-pending) targets — the monorepo#2972 idle-target guard must
    // reject them over the wire — then registers waits on the FRESH
    // (never-run) targets for the cancelSubscriptions arms below.
    let round2_js = "const listing = await ws.app.agents.list({ includeCompleted: true });\n\
              const settled = listing.threads.filter((t) => String(t.agentName).startsWith('Target ')).map((t) => t.agentId);\n\
              let idleError = null;\n\
              try { await ws.app.agents.waitFor({ agentIds: settled, waitMode: 'immediate' }); }\n\
              catch (error) { idleError = error.message; }\n\
              const fresh = listing.threads.filter((t) => String(t.agentName).startsWith('Fresh ')).map((t) => t.agentId);\n\
              const second = await ws.app.agents.waitFor({ agentIds: fresh, waitMode: 'immediate' });\n\
              return { ...second, idleError };";
    let behavior = json!({
        "response": "ok",
        "rules": [
            {
                "ifPromptContains": "SECOND_ROUND",
                "toolCall": {
                    "name": "workspace_api",
                    "arguments": { "code": round2_js, "summary": "waitFor round 2 e2e" },
                },
                "response": "round 2 waits registered",
                "emitToolBlocks": true,
            },
            {
                "ifPromptContains": "REGISTER_WAITS",
                "toolCall": {
                    "name": "workspace_api",
                    "arguments": { "code": js, "summary": "waitFor immediate e2e" },
                },
                "response": "waits registered",
                "emitToolBlocks": true,
            },
        ],
    })
    .to_string();
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
    disable_toon_output(&socket).await;
    let status = common::await_wss_status(&socket).await;
    let port = status["result"]["port"].as_u64().expect("port") as u16;
    let fingerprint = status["result"]["fingerprint"]
        .as_str()
        .expect("fingerprint")
        .to_string();
    let cfg = client_config(&fingerprint);
    let mut rpc = connect_ws(port, cfg.clone()).await;

    // Two targets in two DIFFERENT user workspaces (DoD: waits across >=2
    // workspaces) + the chief parent in `__chief__`.
    let (ws1_id, t1_id) = seed_target(&mut rpc, 2, "Wait WS One", "Target One").await;
    let (ws2_id, t2_id) = seed_target(&mut rpc, 4, "Wait WS Two", "Target Two").await;
    let resp = wss_rpc_envelope(
        &mut rpc,
        6,
        "agent.create",
        json!({
            "workspaceId": CHIEF_WORKSPACE_ID,
            "name": "Chief Waiter",
            "model": "mock:default",
        }),
    )
    .await;
    let chief_id = resp["result"]["agent"]["id"]
        .as_str()
        .expect("chief agent id")
        .to_string();

    // SUBSCRIBER on `__chief__` BEFORE registration: waitFor must publish
    // `agent:subscriptions-changed` in the caller's home workspace.
    let mut sub = connect_ws(port, cfg.clone()).await;
    let resp = wss_rpc_envelope(
        &mut sub,
        7,
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

    // Drive the chief's registration turn through the real provider + MCP
    // bridge path.
    let resp = wss_rpc_envelope(
        &mut rpc,
        8,
        "agent.sendMessage",
        json!({
            "workspaceId": CHIEF_WORKSPACE_ID,
            "agentId": chief_id,
            "content": "please REGISTER_WAITS on the targets",
        }),
    )
    .await;
    assert_eq!(
        resp["result"]["success"],
        json!(true),
        "sendMessage: {resp}"
    );

    // The persisted tool result is the documented waitFor payload.
    let wait_result = poll_conversation(&mut rpc, 300, &chief_id, "waitFor tool result", |m| {
        tool_result_jsons(m)
            .into_iter()
            .rev()
            .find(|v| v["ok"] == json!(true) && v["results"].is_array())
    })
    .await;
    assert_eq!(wait_result["waitMode"], json!("immediate"));
    let results = wait_result["results"].as_array().expect("results array");
    assert_eq!(results.len(), 2, "one result per target: {results:?}");
    for (tid, wsid, name) in [
        (&t1_id, &ws1_id, "Target One"),
        (&t2_id, &ws2_id, "Target Two"),
    ] {
        let entry = results
            .iter()
            .find(|r| r["agentId"] == json!(tid))
            .unwrap_or_else(|| panic!("no waitFor result for {tid}: {results:?}"));
        assert_eq!(entry["workspaceId"], json!(wsid), "target home: {entry}");
        assert_eq!(entry["agentName"], json!(name), "target name: {entry}");
        assert!(
            entry["subscriptionId"].is_string(),
            "subscriptionId: {entry}"
        );
        assert!(entry["groupId"].is_null(), "immediate ⇒ ungrouped: {entry}");
    }
    // Pair uniqueness over the wire: the same-turn duplicate waitFor was
    // rejected with the `-32602` InvalidParams error naming an
    // already-watched target (`already waiting on agent …`).
    let duplicate_error = wait_result["duplicateError"]
        .as_str()
        .expect("duplicate waitFor must surface an error message");
    assert!(
        duplicate_error.contains("already waiting on agent"),
        "pair-uniqueness rejection: {duplicate_error}"
    );
    assert!(
        duplicate_error.contains(t1_id.as_str()) || duplicate_error.contains(t2_id.as_str()),
        "rejection names the target: {duplicate_error}"
    );

    // Registration published `agent:subscriptions-changed` in the PARENT's
    // home workspace (`__chief__`), for the chief caller.
    let frame = wss_event(&mut sub, 30).await;
    let ev = &frame["params"]["event"];
    assert_eq!(ev["type"], json!("agent:subscriptions-changed"));
    assert_eq!(ev["workspaceId"], json!(CHIEF_WORKSPACE_ID));
    assert_eq!(ev["data"]["agentId"], json!(chief_id));

    // BEFORE the targets settle: both completion watches visible, anchored in
    // `__chief__`, ungrouped.
    let subs_payload = poll_subscriptions(
        &mut rpc,
        400,
        CHIEF_WORKSPACE_ID,
        &chief_id,
        "2 live waitFor watches",
        |r| r["subscriptions"].as_array().map(Vec::len) == Some(2),
    )
    .await;
    let subs = subs_payload["subscriptions"].as_array().unwrap();
    let mut actor_ids: Vec<&str> = Vec::new();
    for s in subs {
        assert_eq!(s["agentId"], json!(chief_id), "watch owner: {s}");
        assert_eq!(s["workspaceId"], json!(CHIEF_WORKSPACE_ID), "anchor: {s}");
        assert!(s.get("oneShot").is_none(), "oneShot dropped from wire: {s}");
        assert!(s["delegationGroup"].is_null(), "ungrouped: {s}");
        actor_ids.push(s["actorIds"][0].as_str().expect("actor id"));
    }
    assert!(
        actor_ids.contains(&t1_id.as_str()) && actor_ids.contains(&t2_id.as_str()),
        "watches cover both targets: {actor_ids:?}"
    );

    // Settle both targets (mock provider full turns) → each `agent:idle`
    // fires the matching watch → one wake per target in `__chief__`.
    for (i, (tid, wsid)) in [(&t1_id, &ws1_id), (&t2_id, &ws2_id)].iter().enumerate() {
        let resp = wss_rpc_envelope(
            &mut rpc,
            20 + i as i64,
            "agent.sendMessage",
            json!({ "workspaceId": wsid, "agentId": tid, "content": "please finish" }),
        )
        .await;
        assert_eq!(
            resp["result"]["success"],
            json!(true),
            "target turn: {resp}"
        );
    }
    // The target ids also appear in the registration tool-result JSON, so
    // match the exact `format_completion_wake` line per target — one
    // individual wake each is the immediate-mode contract.
    let wake1 = format!("[WORKSPACE EVENTS] Child agent Target One ({t1_id}) completed.");
    let wake2 = format!("[WORKSPACE EVENTS] Child agent Target Two ({t2_id}) completed.");
    poll_conversation(&mut rpc, 500, &chief_id, "both immediate wakes", |m| {
        let text = serde_json::to_string(m).unwrap_or_default();
        (text.contains(&wake1) && text.contains(&wake2)).then_some(())
    })
    .await;

    // The consumed watches drained the registry.
    poll_subscriptions(
        &mut rpc,
        600,
        CHIEF_WORKSPACE_ID,
        &chief_id,
        "registry drained after wakes",
        |r| r["subscriptions"] == json!([]),
    )
    .await;

    // Round 2: a retried wait on the settled targets is REJECTED by the
    // monorepo#2972 idle-target guard (they sit RuntimeIdle with nothing
    // pending — no future completion to watch), then waits on two FRESH
    // (never-run, `pending`) targets register normally.
    // → 2 fresh watches → scoped `agent.cancelSubscriptions` removes ONE by
    // `subscriptionId`, an unknown id errors, and the unscoped call removes
    // the rest — all over the wire.
    let (_ws3_id, _f1_id) = seed_target(&mut rpc, 40, "Wait WS Three", "Fresh One").await;
    let (_ws4_id, _f2_id) = seed_target(&mut rpc, 42, "Wait WS Four", "Fresh Two").await;
    let resp = wss_rpc_envelope(
        &mut rpc,
        30,
        "agent.sendMessage",
        json!({
            "workspaceId": CHIEF_WORKSPACE_ID,
            "agentId": chief_id,
            "content": "one more round: SECOND_ROUND",
        }),
    )
    .await;
    assert_eq!(resp["result"]["success"], json!(true), "round 2: {resp}");
    // The tool result surfaces the idle-target rejection for the settled
    // targets alongside the successful fresh registrations.
    let round2_result = poll_conversation(&mut rpc, 800, &chief_id, "round 2 tool result", |m| {
        tool_result_jsons(m)
            .into_iter()
            .rev()
            .find(|v| v.get("idleError").is_some())
    })
    .await;
    let idle_error = round2_result["idleError"]
        .as_str()
        .expect("settled-target retry must surface the idle-target guard error");
    assert!(
        idle_error.contains("idle with nothing pending"),
        "idle-target guard rejection over the wire: {idle_error}"
    );
    let resub = poll_subscriptions(
        &mut rpc,
        700,
        CHIEF_WORKSPACE_ID,
        &chief_id,
        "re-registered watches",
        |r| r["subscriptions"].as_array().map(Vec::len) == Some(2),
    )
    .await;
    let scoped_sid = resub["subscriptions"][0]["id"]
        .as_str()
        .expect("watch id")
        .to_string();

    // Unknown `subscriptionId` → -32602, registry untouched.
    let resp = wss_rpc_envelope(
        &mut rpc,
        31,
        "agent.cancelSubscriptions",
        json!({
            "workspaceId": CHIEF_WORKSPACE_ID,
            "agentId": chief_id,
            "subscriptionId": "no-such-watch",
        }),
    )
    .await;
    assert_eq!(
        resp["error"]["code"],
        json!(-32602),
        "unknown subscriptionId: {resp}"
    );

    // Present-but-non-string `subscriptionId` → -32602 at the router; it must
    // NOT be coerced to `None` (which would cancel everything).
    let resp = wss_rpc_envelope(
        &mut rpc,
        36,
        "agent.cancelSubscriptions",
        json!({
            "workspaceId": CHIEF_WORKSPACE_ID,
            "agentId": chief_id,
            "subscriptionId": 42,
        }),
    )
    .await;
    assert_eq!(
        resp["error"]["code"],
        json!(-32602),
        "non-string subscriptionId: {resp}"
    );
    assert_eq!(
        resp["error"]["message"],
        json!("subscriptionId must be a string"),
        "non-string subscriptionId message: {resp}"
    );

    // Scoped cancel removes EXACTLY the named watch; the other stays live.
    let resp = wss_rpc_envelope(
        &mut rpc,
        32,
        "agent.cancelSubscriptions",
        json!({
            "workspaceId": CHIEF_WORKSPACE_ID,
            "agentId": chief_id,
            "subscriptionId": scoped_sid,
        }),
    )
    .await;
    assert!(
        resp.get("error").is_none(),
        "scoped cancelSubscriptions errored: {resp}"
    );
    assert_eq!(
        resp["result"]["success"],
        json!(true),
        "scoped cancel: {resp}"
    );
    let remaining = poll_subscriptions(
        &mut rpc,
        750,
        CHIEF_WORKSPACE_ID,
        &chief_id,
        "one watch after scoped cancel",
        |r| r["subscriptions"].as_array().map(Vec::len) == Some(1),
    )
    .await;
    assert_ne!(
        remaining["subscriptions"][0]["id"],
        json!(scoped_sid),
        "the scoped watch is the one that was removed: {remaining}"
    );

    let resp = wss_rpc_envelope(
        &mut rpc,
        33,
        "agent.cancelSubscriptions",
        json!({ "workspaceId": CHIEF_WORKSPACE_ID, "agentId": chief_id }),
    )
    .await;
    assert!(
        resp.get("error").is_none(),
        "cancelSubscriptions errored: {resp}"
    );
    assert_eq!(resp["result"]["success"], json!(true), "cancel: {resp}");
    let resp = wss_rpc_envelope(
        &mut rpc,
        34,
        "agent.getSubscriptions",
        json!({ "workspaceId": CHIEF_WORKSPACE_ID, "agentId": chief_id }),
    )
    .await;
    assert_eq!(
        resp["result"]["subscriptions"],
        json!([]),
        "cancelSubscriptions cleared the registry: {resp}"
    );

    let _ = wss_rpc_envelope(&mut rpc, 35, "agent.stop", json!({ "agentId": chief_id })).await;
}

/// `ws.app.agents.waitFor` (after_all mode) end-to-end over the real WSS
/// wire: the chief enrolls targets in TWO different user workspaces in ONE
/// delegation group; the group seals when the chief's registering turn ends
/// (parent idle) and fires a SINGLE aggregated wake once both settle.
///
/// Asserts:
/// - each tool-result entry carries the SAME non-null `groupId`,
/// - `agent.getSubscriptions` shows both grouped watches (`delegationGroup`
///   with `awaitMode: "all"`) plus the `delegationGroups` record listing both
///   expected targets, before any target settles,
/// - exactly ONE aggregated `All 2 delegated child agent(s) settled
///   (completionStatus: completed)` wake lands in the chief transcript — and
///   NO per-target `[WORKSPACE EVENTS] Child agent` immediate wakes,
/// - subscriptions AND delegation groups are drained after settlement.
#[tokio::test]
async fn chief_waitfor_after_all_aggregated_wake_over_wss() {
    let Some(script) = gate("WSS chief waitFor after_all E2E") else {
        return;
    };

    let data_dir = temp_data_dir();
    let js = "const listing = await ws.app.agents.list({ includeCompleted: true });\n\
              const targets = listing.threads.filter((t) => String(t.agentName).startsWith('Target ')).map((t) => t.agentId);\n\
              return await ws.app.agents.waitFor({ agentIds: targets, waitMode: 'after_all' });";
    let behavior = json!({
        "response": "ok",
        "rules": [{
            "ifPromptContains": "REGISTER_GROUP",
            "toolCall": {
                "name": "workspace_api",
                "arguments": { "code": js, "summary": "waitFor after_all e2e" },
            },
            "response": "group waits registered",
            "emitToolBlocks": true,
        }],
    })
    .to_string();
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
    disable_toon_output(&socket).await;
    let status = common::await_wss_status(&socket).await;
    let port = status["result"]["port"].as_u64().expect("port") as u16;
    let fingerprint = status["result"]["fingerprint"]
        .as_str()
        .expect("fingerprint")
        .to_string();
    let cfg = client_config(&fingerprint);
    let mut rpc = connect_ws(port, cfg.clone()).await;

    let (ws1_id, t1_id) = seed_target(&mut rpc, 2, "Group WS One", "Target One").await;
    let (ws2_id, t2_id) = seed_target(&mut rpc, 4, "Group WS Two", "Target Two").await;
    let resp = wss_rpc_envelope(
        &mut rpc,
        6,
        "agent.create",
        json!({
            "workspaceId": CHIEF_WORKSPACE_ID,
            "name": "Chief Group Waiter",
            "model": "mock:default",
        }),
    )
    .await;
    let chief_id = resp["result"]["agent"]["id"]
        .as_str()
        .expect("chief agent id")
        .to_string();

    let resp = wss_rpc_envelope(
        &mut rpc,
        7,
        "agent.sendMessage",
        json!({
            "workspaceId": CHIEF_WORKSPACE_ID,
            "agentId": chief_id,
            "content": "please REGISTER_GROUP waits on the targets",
        }),
    )
    .await;
    assert_eq!(
        resp["result"]["success"],
        json!(true),
        "sendMessage: {resp}"
    );

    // Tool result: both entries share ONE non-null groupId.
    let wait_result = poll_conversation(&mut rpc, 300, &chief_id, "waitFor tool result", |m| {
        tool_result_jsons(m)
            .into_iter()
            .rev()
            .find(|v| v["ok"] == json!(true) && v["results"].is_array())
    })
    .await;
    assert_eq!(wait_result["waitMode"], json!("after_all"));
    let results = wait_result["results"].as_array().expect("results array");
    assert_eq!(results.len(), 2, "one result per target: {results:?}");
    let group_id = results[0]["groupId"]
        .as_str()
        .expect("after_all ⇒ groupId")
        .to_string();
    for (tid, wsid) in [(&t1_id, &ws1_id), (&t2_id, &ws2_id)] {
        let entry = results
            .iter()
            .find(|r| r["agentId"] == json!(tid))
            .unwrap_or_else(|| panic!("no waitFor result for {tid}: {results:?}"));
        assert_eq!(entry["workspaceId"], json!(wsid), "target home: {entry}");
        assert_eq!(
            entry["groupId"],
            json!(group_id),
            "both targets share one group: {entry}"
        );
        assert!(
            entry["subscriptionId"].is_string(),
            "subscriptionId: {entry}"
        );
    }

    // BEFORE any target settles: grouped watches + the delegation-group
    // record are visible, anchored under `__chief__`.
    let subs_payload = poll_subscriptions(
        &mut rpc,
        400,
        CHIEF_WORKSPACE_ID,
        &chief_id,
        "2 grouped watches",
        |r| r["subscriptions"].as_array().map(Vec::len) == Some(2),
    )
    .await;
    for s in subs_payload["subscriptions"].as_array().unwrap() {
        assert_eq!(s["workspaceId"], json!(CHIEF_WORKSPACE_ID), "anchor: {s}");
        assert!(s.get("oneShot").is_none(), "oneShot dropped from wire: {s}");
        assert_eq!(s["delegationGroup"]["groupId"], json!(group_id), "{s}");
        assert_eq!(s["delegationGroup"]["awaitMode"], json!("all"), "{s}");
    }
    let groups = subs_payload["delegationGroups"]
        .as_array()
        .expect("delegationGroups array");
    assert_eq!(groups.len(), 1, "one open group: {groups:?}");
    assert_eq!(groups[0]["groupId"], json!(group_id));
    assert_eq!(groups[0]["parentAgentId"], json!(chief_id));
    assert_eq!(groups[0]["awaitMode"], json!("all"));
    assert_eq!(groups[0]["delivered"], json!(false));
    let expected = groups[0]["expectedAgentIds"]
        .as_array()
        .expect("expectedAgentIds");
    assert!(
        expected.contains(&json!(t1_id)) && expected.contains(&json!(t2_id)),
        "group expects both targets: {expected:?}"
    );

    // Settle both targets. The group sealed when the chief's registering
    // turn went idle; the second settlement fires the ONE aggregated wake.
    for (i, (tid, wsid)) in [(&t1_id, &ws1_id), (&t2_id, &ws2_id)].iter().enumerate() {
        let resp = wss_rpc_envelope(
            &mut rpc,
            20 + i as i64,
            "agent.sendMessage",
            json!({ "workspaceId": wsid, "agentId": tid, "content": "please finish" }),
        )
        .await;
        assert_eq!(
            resp["result"]["success"],
            json!(true),
            "target turn: {resp}"
        );
    }
    let transcript_text =
        poll_conversation(&mut rpc, 500, &chief_id, "aggregated after_all wake", |m| {
            let text = serde_json::to_string(m).unwrap_or_default();
            text.contains("All 2 delegated child agent(s) settled (completionStatus: completed)")
                .then_some(text)
        })
        .await;
    assert_eq!(
        transcript_text
            .matches("All 2 delegated child agent(s) settled")
            .count(),
        1,
        "exactly ONE aggregated wake"
    );
    assert!(
        !transcript_text.contains("[WORKSPACE EVENTS] Child agent"),
        "after_all must not deliver per-target immediate wakes"
    );
    // The aggregated wake folds in one per-child line per target.
    assert!(
        transcript_text.contains(&format!("Target One ({t1_id}) completed."))
            && transcript_text.contains(&format!("Target Two ({t2_id}) completed.")),
        "aggregated wake carries both per-child lines"
    );

    // Group settlement drained both the watches and the group record.
    poll_subscriptions(
        &mut rpc,
        600,
        CHIEF_WORKSPACE_ID,
        &chief_id,
        "registry drained after group settlement",
        |r| r["subscriptions"] == json!([]) && r["delegationGroups"] == json!([]),
    )
    .await;

    let _ = wss_rpc_envelope(&mut rpc, 30, "agent.stop", json!({ "agentId": chief_id })).await;
}

/// Scoped `agent.cancelSubscriptions` by `groupId` end-to-end over the real
/// WSS wire: the chief registers an after_all delegation group on two
/// targets, an unknown `groupId` is rejected with `-32602` (registry
/// untouched), and cancelling the real `groupId` removes the group AND both
/// grouped watches in one call.
#[tokio::test]
async fn chief_scoped_group_cancel_over_wss() {
    let Some(script) = gate("WSS scoped groupId cancel E2E") else {
        return;
    };

    let data_dir = temp_data_dir();
    let js = "const listing = await ws.app.agents.list({ includeCompleted: true });\n\
              const targets = listing.threads.filter((t) => String(t.agentName).startsWith('Target ')).map((t) => t.agentId);\n\
              return await ws.app.agents.waitFor({ agentIds: targets, waitMode: 'after_all' });";
    let behavior = json!({
        "response": "ok",
        "rules": [{
            "ifPromptContains": "REGISTER_GROUP",
            "toolCall": {
                "name": "workspace_api",
                "arguments": { "code": js, "summary": "waitFor after_all for scoped cancel e2e" },
            },
            "response": "group waits registered",
            "emitToolBlocks": true,
        }],
    })
    .to_string();
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
    let status = common::await_wss_status(&socket).await;
    let port = status["result"]["port"].as_u64().expect("port") as u16;
    let fingerprint = status["result"]["fingerprint"]
        .as_str()
        .expect("fingerprint")
        .to_string();
    let cfg = client_config(&fingerprint);
    let mut rpc = connect_ws(port, cfg.clone()).await;

    let (_ws1_id, _t1_id) = seed_target(&mut rpc, 2, "Cancel WS One", "Target One").await;
    let (_ws2_id, _t2_id) = seed_target(&mut rpc, 4, "Cancel WS Two", "Target Two").await;
    let resp = wss_rpc_envelope(
        &mut rpc,
        6,
        "agent.create",
        json!({
            "workspaceId": CHIEF_WORKSPACE_ID,
            "name": "Chief Group Canceller",
            "model": "mock:default",
        }),
    )
    .await;
    let chief_id = resp["result"]["agent"]["id"]
        .as_str()
        .expect("chief agent id")
        .to_string();

    let resp = wss_rpc_envelope(
        &mut rpc,
        7,
        "agent.sendMessage",
        json!({
            "workspaceId": CHIEF_WORKSPACE_ID,
            "agentId": chief_id,
            "content": "please REGISTER_GROUP waits on the targets",
        }),
    )
    .await;
    assert_eq!(
        resp["result"]["success"],
        json!(true),
        "sendMessage: {resp}"
    );

    // Both grouped watches + the delegation-group record land in the registry.
    let subs_payload = poll_subscriptions(
        &mut rpc,
        400,
        CHIEF_WORKSPACE_ID,
        &chief_id,
        "2 grouped watches + group record",
        |r| {
            r["subscriptions"].as_array().map(Vec::len) == Some(2)
                && r["delegationGroups"].as_array().map(Vec::len) == Some(1)
        },
    )
    .await;
    let group_id = subs_payload["delegationGroups"][0]["groupId"]
        .as_str()
        .expect("groupId")
        .to_string();

    // Unknown `groupId` → -32602, registry untouched.
    let resp = wss_rpc_envelope(
        &mut rpc,
        20,
        "agent.cancelSubscriptions",
        json!({
            "workspaceId": CHIEF_WORKSPACE_ID,
            "agentId": chief_id,
            "groupId": "no-such-group",
        }),
    )
    .await;
    assert_eq!(
        resp["error"]["code"],
        json!(-32602),
        "unknown groupId: {resp}"
    );
    let resp = wss_rpc_envelope(
        &mut rpc,
        21,
        "agent.getSubscriptions",
        json!({ "workspaceId": CHIEF_WORKSPACE_ID, "agentId": chief_id }),
    )
    .await;
    assert_eq!(
        resp["result"]["subscriptions"].as_array().map(Vec::len),
        Some(2),
        "registry untouched after unknown groupId: {resp}"
    );

    // Scoped cancel by the REAL groupId removes the group and BOTH grouped
    // watches in one call.
    let resp = wss_rpc_envelope(
        &mut rpc,
        22,
        "agent.cancelSubscriptions",
        json!({
            "workspaceId": CHIEF_WORKSPACE_ID,
            "agentId": chief_id,
            "groupId": group_id,
        }),
    )
    .await;
    assert!(
        resp.get("error").is_none(),
        "scoped group cancel errored: {resp}"
    );
    assert_eq!(
        resp["result"]["success"],
        json!(true),
        "scoped group cancel: {resp}"
    );
    poll_subscriptions(
        &mut rpc,
        500,
        CHIEF_WORKSPACE_ID,
        &chief_id,
        "registry drained after scoped group cancel",
        |r| r["subscriptions"] == json!([]) && r["delegationGroups"] == json!([]),
    )
    .await;

    let _ = wss_rpc_envelope(&mut rpc, 30, "agent.stop", json!({ "agentId": chief_id })).await;
}

/// Safety gate over the real WSS wire: a NON-chief agent attempting
/// `ws.app.agents.waitFor` receives the chief-workspace gating error through
/// the MCP tool result, and no watch is registered for it.
#[tokio::test]
async fn non_chief_waitfor_gated_over_wss() {
    let Some(script) = gate("WSS non-chief waitFor gating E2E") else {
        return;
    };

    let data_dir = temp_data_dir();
    let js = "try {\n\
                const result = await ws.app.agents.waitFor({ agentIds: ['agent-in-another-workspace'] });\n\
                return { success: true, result };\n\
              } catch (error) {\n\
                return { success: false, error: error.message };\n\
              }";
    let behavior = json!({
        "response": "ok",
        "rules": [{
            "ifPromptContains": "TRY_WAITFOR",
            "toolCall": {
                "name": "workspace_api",
                "arguments": { "code": js, "summary": "waitFor gating e2e" },
            },
            "response": "gating checked",
            "emitToolBlocks": true,
        }],
    })
    .to_string();
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
    disable_toon_output(&socket).await;
    let status = common::await_wss_status(&socket).await;
    let port = status["result"]["port"].as_u64().expect("port") as u16;
    let fingerprint = status["result"]["fingerprint"]
        .as_str()
        .expect("fingerprint")
        .to_string();
    let cfg = client_config(&fingerprint);
    let mut rpc = connect_ws(port, cfg).await;

    // The caller is a REGULAR-workspace agent, so the chief-only gate must
    // reject the call before any target resolution or registration happens.
    let (ws_id, caller_id) = seed_target(&mut rpc, 2, "Non Chief WS", "Gated Caller").await;

    let resp = wss_rpc_envelope(
        &mut rpc,
        4,
        "agent.sendMessage",
        json!({
            "workspaceId": ws_id,
            "agentId": caller_id,
            "content": "TRY_WAITFOR from a regular workspace",
        }),
    )
    .await;
    assert_eq!(
        resp["result"]["success"],
        json!(true),
        "sendMessage: {resp}"
    );

    let gate_result = poll_conversation(&mut rpc, 300, &caller_id, "gating tool result", |m| {
        tool_result_jsons(m)
            .into_iter()
            .rev()
            .find(|v| v.get("success").is_some())
    })
    .await;
    assert_eq!(
        gate_result["success"],
        json!(false),
        "non-chief waitFor must fail: {gate_result}"
    );
    let error_msg = gate_result["error"].as_str().expect("error string");
    assert!(
        error_msg.contains("ws.app.* is only available in the Chief of Staff workspace"),
        "clear chief-gating error, got: {error_msg}"
    );

    // Side-effect free: no watch was registered for the rejected caller.
    let resp = wss_rpc_envelope(
        &mut rpc,
        5,
        "agent.getSubscriptions",
        json!({ "workspaceId": ws_id, "agentId": caller_id }),
    )
    .await;
    assert_eq!(
        resp["result"]["subscriptions"],
        json!([]),
        "no watches for the gated caller: {resp}"
    );
    assert_eq!(
        resp["result"]["delegationGroups"],
        json!([]),
        "no groups for the gated caller: {resp}"
    );
}

/// `ws.workspace.archive()` / `.unarchive()` end-to-end over the real WSS
/// wire (#733): a regular-workspace agent drives archive → details →
/// unarchive → details through the real MCP bridge (mock ACP provider →
/// `workspace_api` tool → binding dispatch → services → store), and the
/// existing service emitters publish `workspace:updated` with the applied
/// `{ archived }` delta over a live WSS subscription.
///
/// Asserts:
/// - the archive tool result is `{ ok: true, status: "Archived", archivedAt }`
///   and `ws.workspace.details()` reflects `Archived`,
/// - the unarchive tool result is `{ ok: true, status: "Active" }` and a
///   follow-up `details()` reflects `Active`,
/// - `workspace:updated` fires twice on the workspace — `archived: true`
///   then `archived: false` — without any binding-layer re-emit,
/// - `workspace.get` over the wire agrees with the persisted flip both ways.
#[tokio::test]
async fn workspace_archive_unarchive_bridge_over_wss() {
    let Some(script) = gate("WSS ws.workspace.archive E2E") else {
        return;
    };

    let data_dir = temp_data_dir();
    let js = "const archived = await ws.workspace.archive();\n\
              const afterArchive = await ws.workspace.details();\n\
              const unarchived = await ws.workspace.unarchive();\n\
              const afterUnarchive = await ws.workspace.details();\n\
              return { archived, afterArchive, unarchived, afterUnarchive };";
    let behavior = json!({
        "response": "ok",
        "rules": [{
            "ifPromptContains": "ARCHIVE_ROUNDTRIP",
            "toolCall": {
                "name": "workspace_api",
                "arguments": { "code": js, "summary": "archive roundtrip e2e" },
            },
            "response": "archive roundtrip done",
            "emitToolBlocks": true,
        }],
    })
    .to_string();
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
    disable_toon_output(&socket).await;
    let status = common::await_wss_status(&socket).await;
    let port = status["result"]["port"].as_u64().expect("port") as u16;
    let fingerprint = status["result"]["fingerprint"]
        .as_str()
        .expect("fingerprint")
        .to_string();
    let cfg = client_config(&fingerprint);
    let mut rpc = connect_ws(port, cfg.clone()).await;

    let (ws_id, agent_id) = seed_target(&mut rpc, 2, "Archive WS", "Archiver").await;

    // Live `workspace:updated` subscription BEFORE the turn: the service
    // emitters (not the binding) publish the `{ archived }` deltas.
    let mut sub = connect_ws(port, cfg.clone()).await;
    let resp = wss_rpc_envelope(
        &mut sub,
        4,
        "events.subscribe",
        json!({ "eventTypes": ["workspace:updated"], "workspaceId": ws_id }),
    )
    .await;
    assert!(
        resp["result"]["subscriptionId"].is_string(),
        "events.subscribe failed: {resp}"
    );

    let resp = wss_rpc_envelope(
        &mut rpc,
        5,
        "agent.sendMessage",
        json!({
            "workspaceId": ws_id,
            "agentId": agent_id,
            "content": "user asked: ARCHIVE_ROUNDTRIP this workspace",
        }),
    )
    .await;
    assert_eq!(
        resp["result"]["success"],
        json!(true),
        "sendMessage: {resp}"
    );

    // The persisted tool result carries all four stages of the roundtrip.
    let roundtrip = poll_conversation(&mut rpc, 300, &agent_id, "archive roundtrip result", |m| {
        tool_result_jsons(m)
            .into_iter()
            .rev()
            .find(|v| v.get("archived").is_some() && v.get("afterUnarchive").is_some())
    })
    .await;
    let archived = &roundtrip["archived"];
    assert_eq!(archived["ok"], json!(true), "archive ok: {archived}");
    assert_eq!(archived["status"], json!("Archived"), "{archived}");
    assert!(archived["archivedAt"].is_string(), "archivedAt: {archived}");
    assert_eq!(
        roundtrip["afterArchive"]["status"],
        json!("Archived"),
        "details after archive: {roundtrip}"
    );
    let unarchived = &roundtrip["unarchived"];
    assert_eq!(unarchived["ok"], json!(true), "unarchive ok: {unarchived}");
    assert_eq!(unarchived["status"], json!("Active"), "{unarchived}");
    assert_eq!(
        roundtrip["afterUnarchive"]["status"],
        json!("Active"),
        "details after unarchive: {roundtrip}"
    );

    // The service emitters published both `{ archived }` deltas in order
    // over the live WSS subscription. Other `workspace:updated` frames
    // (e.g. `lastActivity` deltas from the agent turn) may interleave, so
    // skip until the archived-change frames arrive.
    let mut archived_deltas = Vec::new();
    while archived_deltas.len() < 2 {
        let frame = wss_event(&mut sub, 30).await;
        let ev = &frame["params"]["event"];
        assert_eq!(ev["type"], json!("workspace:updated"), "{ev}");
        assert_eq!(ev["workspaceId"], json!(ws_id), "{ev}");
        if let Some(a) = ev["data"]["changes"]["archived"].as_bool() {
            archived_deltas.push(a);
        }
    }
    assert_eq!(
        archived_deltas,
        vec![true, false],
        "archive then unarchive deltas in order"
    );

    // The persisted row agrees with the final state on the wire.
    let resp = wss_rpc_envelope(
        &mut rpc,
        6,
        "workspace.get",
        json!({ "workspaceId": ws_id }),
    )
    .await;
    let w = &resp["result"]["workspace"];
    assert_eq!(w["status"], json!("Active"), "final status: {w}");
    assert_eq!(w["archived"], json!(false), "final archived flag: {w}");

    let _ = wss_rpc_envelope(&mut rpc, 200, "agent.stop", json!({ "agentId": agent_id })).await;
}

/// Chief-workspace gate for `ws.workspace.archive` / `.unarchive` over the
/// real WSS wire (#733): a `__chief__` agent attempting either method
/// receives the explicit binding-layer refusal through the MCP tool result —
/// NOT the silent service-layer no-op that would look like success — and the
/// Chief row keeps its synthesized non-archived shape.
#[tokio::test]
async fn chief_workspace_archive_gated_over_wss() {
    let Some(script) = gate("WSS chief archive gating E2E") else {
        return;
    };

    let data_dir = temp_data_dir();
    let js = "const out = {};\n\
              try { out.archive = { ok: true, result: await ws.workspace.archive() }; }\n\
              catch (e) { out.archive = { ok: false, error: e.message }; }\n\
              try { out.unarchive = { ok: true, result: await ws.workspace.unarchive() }; }\n\
              catch (e) { out.unarchive = { ok: false, error: e.message }; }\n\
              return out;";
    let behavior = json!({
        "response": "ok",
        "rules": [{
            "ifPromptContains": "TRY_ARCHIVE",
            "toolCall": {
                "name": "workspace_api",
                "arguments": { "code": js, "summary": "chief archive gating e2e" },
            },
            "response": "gating checked",
            "emitToolBlocks": true,
        }],
    })
    .to_string();
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
    disable_toon_output(&socket).await;
    let status = common::await_wss_status(&socket).await;
    let port = status["result"]["port"].as_u64().expect("port") as u16;
    let fingerprint = status["result"]["fingerprint"]
        .as_str()
        .expect("fingerprint")
        .to_string();
    let cfg = client_config(&fingerprint);
    let mut rpc = connect_ws(port, cfg).await;

    let resp = wss_rpc_envelope(
        &mut rpc,
        2,
        "agent.create",
        json!({
            "workspaceId": CHIEF_WORKSPACE_ID,
            "name": "Chief Archiver",
            "model": "mock:default",
        }),
    )
    .await;
    let agent_id = resp["result"]["agent"]["id"]
        .as_str()
        .expect("agent id")
        .to_string();

    let resp = wss_rpc_envelope(
        &mut rpc,
        3,
        "agent.sendMessage",
        json!({
            "workspaceId": CHIEF_WORKSPACE_ID,
            "agentId": agent_id,
            "content": "TRY_ARCHIVE from the chief workspace",
        }),
    )
    .await;
    assert_eq!(
        resp["result"]["success"],
        json!(true),
        "sendMessage: {resp}"
    );

    let gate_result = poll_conversation(&mut rpc, 300, &agent_id, "gating tool result", |m| {
        tool_result_jsons(m)
            .into_iter()
            .rev()
            .find(|v| v.get("archive").is_some() && v.get("unarchive").is_some())
    })
    .await;
    for method in ["archive", "unarchive"] {
        let outcome = &gate_result[method];
        assert_eq!(
            outcome["ok"],
            json!(false),
            "chief {method} must be refused: {outcome}"
        );
        let msg = outcome["error"].as_str().expect("error string");
        assert!(
            msg.contains("chief-of-staff"),
            "clear chief-gating error for {method}, got: {msg}"
        );
    }

    // Chief keeps its synthesized non-archived shape.
    let resp = wss_rpc_envelope(
        &mut rpc,
        4,
        "workspace.get",
        json!({ "workspaceId": CHIEF_WORKSPACE_ID }),
    )
    .await;
    let chief = &resp["result"]["workspace"];
    assert_eq!(chief["archived"], json!(false), "{chief}");
    assert_eq!(chief["status"], json!("Active"), "{chief}");

    let _ = wss_rpc_envelope(&mut rpc, 200, "agent.stop", json!({ "agentId": agent_id })).await;
}
