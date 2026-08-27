//! WSS end-to-end change-event emissions for the workspace-lifecycle
//! mutations (FIX 2 parity): drives a real pinned-TLS WebSocket against a
//! live `intentd serve` (WSS listener enabled via config) and asserts that `workspace.update`
//! and `workspace.delete` publish `workspace:updated` / `workspace:deleted`
//! (docs/protocol/06-events.md §6.5) so a subscribed client sees the mutation without a
//! follow-up read. The `git:commit` emission is exercised over UDS in
//! `uds_events.rs` and via unit tests where a git worktree is cheaper to
//! materialise; this file focuses on the pure-daemon lifecycle where no
//! git binary is required.

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
use tokio::io::{AsyncBufReadExt, AsyncWriteExt, BufReader};
use tokio::net::{TcpStream, UnixStream};
use tokio::time::timeout;
use tokio_tungstenite::tungstenite::Message;
use tokio_tungstenite::WebSocketStream;
use uuid::Uuid;

const TOKEN: &str = "abababababababababababababababababababababababababababababababab";

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

fn scratch_dir(prefix: &str) -> PathBuf {
    let id = Uuid::new_v4().simple().to_string();
    let dir = PathBuf::from("/tmp").join(format!("itd-wss-events-{prefix}-{}", &id[..8]));
    std::fs::create_dir_all(&dir).expect("mkdir scratch dir");
    dir
}

fn spawn_serve(data_dir: &Path, env: &[(&str, &str)]) -> Child {
    let log = std::fs::File::create(data_dir.join("daemon.log")).expect("create daemon log");
    let workspaces_dir = data_dir.join("workspaces");
    std::fs::create_dir_all(&workspaces_dir).expect("mkdir hermetic workspaces dir");
    common::enable_ws_api(data_dir);
    common::seed_default_provider(data_dir);
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
    let v = wss_rpc_envelope(ws, id, method, params).await;
    assert!(v.get("error").is_none(), "rpc {method} errored: {v}");
    v["result"].clone()
}

/// Like [`wss_rpc`] but returns the full response envelope so tests can
/// assert `error.code` / `error.message` for expected-failure paths.
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
            Some(Ok(_)) => {}
            other => panic!("expected text frame, got {other:?}"),
        }
    }
}

/// Wait up to `secs` for the next `events.event` notification whose payload
/// `type` matches one of `types`; ignore other frames. Returns the event
/// object (the `params.event` sub-object).
async fn next_event<S>(ws: &mut WebSocketStream<S>, types: &[&str], secs: u64) -> Value
where
    S: tokio::io::AsyncRead + tokio::io::AsyncWrite + Unpin,
{
    let deadline = tokio::time::Instant::now() + Duration::from_secs(secs);
    loop {
        let remaining = deadline.saturating_duration_since(tokio::time::Instant::now());
        assert!(!remaining.is_zero(), "timed out waiting for {types:?}");
        let next = timeout(remaining, ws.next())
            .await
            .expect("timeout elapsed");
        match next {
            Some(Ok(Message::Text(text))) => {
                let v: Value = match serde_json::from_str(&text) {
                    Ok(x) => x,
                    Err(_) => continue,
                };
                if v["method"] == json!("events.event") {
                    let evt = &v["params"]["event"];
                    let ty = evt["type"].as_str().unwrap_or("");
                    if types.contains(&ty) {
                        return evt.clone();
                    }
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

async fn boot() -> (Daemon, u16, Arc<ClientConfig>) {
    let data_dir = scratch_dir("data");
    let env: [(&str, &str); 2] = [("INTENTD_AUTH_TOKEN", TOKEN), ("INTENTD_TCP_PORT", "0")];
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
    (daemon, port, client_config(&fingerprint))
}

/// End-to-end: `workspace.update` over WSS publishes `workspace:updated` with
/// the applied `WorkspaceUpdate` delta as `changes` (§6.5). A previously
/// subscribed client sees the event without a follow-up `workspace.get`
/// round-trip.
#[tokio::test]
async fn workspace_update_emits_workspace_updated_over_wss() {
    let (daemon, port, cfg) = boot().await;
    let _ = &daemon;

    // Bootstrap a workspace off the UDS to avoid noise on the WSS event stream.
    let socket = daemon.data_dir.join("intentd.sock");
    let create = uds_rpc(
        &socket,
        2,
        "workspace.create",
        json!({ "title": "Original", "branch": "main", "skipWorktree": true }),
    )
    .await;
    let ws_id = create["result"]["workspace"]["id"]
        .as_str()
        .expect("workspace id")
        .to_string();

    // Subscribe over WSS before mutating so the emission is guaranteed to be
    // observed (subscribers created after publish miss the event).
    let mut sub = connect_ws(port, cfg.clone()).await;
    let sub_res = wss_rpc(
        &mut sub,
        1,
        "events.subscribe",
        json!({ "eventTypes": ["workspace:updated"], "workspaceId": ws_id }),
    )
    .await;
    assert!(sub_res["subscriptionId"].is_string(), "sub id: {sub_res}");

    // Drive workspace.update over a separate WSS RPC connection. The skip
    // toggle is sent under the deprecated `skipWorktree` alias to prove the
    // update-side rename (§5.1) still honors it on the wire.
    let mut rpc = connect_ws(port, cfg.clone()).await;
    wss_rpc(
        &mut rpc,
        2,
        "workspace.update",
        json!({
            "workspaceId": ws_id,
            "title": "Renamed",
            "tags": ["a", "b"],
            "skipWorktree": true,
        }),
    )
    .await;

    let evt = next_event(&mut sub, &["workspace:updated"], 10).await;
    assert_eq!(evt["workspaceId"], ws_id.as_str());
    assert!(evt["id"].is_string(), "event id: {evt}");
    assert!(evt["timestamp"].is_string(), "timestamp: {evt}");
    assert_eq!(
        evt["actor"],
        json!({ "type": "system", "id": "system", "name": "System" })
    );
    // `changes` is the applied delta only; `skip_serializing_if = "Option::is_none"`
    // keeps un-supplied fields out of the payload (reference-parity emitter).
    // The skip toggle round-trips under its canonical `skipIsolation` name
    // even when the request used the deprecated alias.
    assert_eq!(
        evt["data"],
        json!({
            "workspaceId": ws_id,
            "changes": { "title": "Renamed", "tags": ["a", "b"], "skipIsolation": true },
        })
    );

    // Second round-trip with the canonical `skipIsolation` request name so
    // both wire spellings are proven end-to-end over WSS.
    let update = wss_rpc(
        &mut rpc,
        3,
        "workspace.update",
        json!({ "workspaceId": ws_id, "skipIsolation": false }),
    )
    .await;
    assert_eq!(
        update["workspace"]["skipWorktree"],
        json!(false),
        "canonical skipIsolation applies to the persisted skipWorktree field: {update}"
    );
    let evt = next_event(&mut sub, &["workspace:updated"], 10).await;
    assert_eq!(
        evt["data"],
        json!({
            "workspaceId": ws_id,
            "changes": { "skipIsolation": false },
        })
    );
}

/// End-to-end: `workspace.delete` over WSS publishes `workspace:deleted` with
/// the minimal `{ workspaceId }` payload (§6.5). The event fires only after
/// the store row is actually removed.
#[tokio::test]
async fn workspace_delete_emits_workspace_deleted_over_wss() {
    let (daemon, port, cfg) = boot().await;

    let socket = daemon.data_dir.join("intentd.sock");
    let create = uds_rpc(
        &socket,
        2,
        "workspace.create",
        json!({ "title": "ToDelete", "branch": "main", "skipWorktree": true }),
    )
    .await;
    let ws_id = create["result"]["workspace"]["id"]
        .as_str()
        .expect("workspace id")
        .to_string();

    let mut sub = connect_ws(port, cfg.clone()).await;
    let sub_res = wss_rpc(
        &mut sub,
        1,
        "events.subscribe",
        json!({ "eventTypes": ["workspace:deleted"], "workspaceId": ws_id }),
    )
    .await;
    assert!(sub_res["subscriptionId"].is_string(), "sub id: {sub_res}");

    let mut rpc = connect_ws(port, cfg.clone()).await;
    wss_rpc(
        &mut rpc,
        2,
        "workspace.delete",
        json!({ "workspaceId": ws_id }),
    )
    .await;

    let evt = next_event(&mut sub, &["workspace:deleted"], 10).await;
    assert_eq!(evt["workspaceId"], ws_id.as_str());
    assert_eq!(evt["data"], json!({ "workspaceId": ws_id }));
}

/// End-to-end delete grace window over WSS (PROTOCOL §5.1): `workspace.delete`
/// with `undoDelayMs > 0` answers `{ success, scheduled, deleteAt }` and emits
/// `workspace:delete-scheduled`; while pending the row is still served by
/// `workspace.get` / `workspace.list` carrying `pendingDeleteAt`;
/// `workspace.cancelDelete` answers `{ cancelled: true }`, emits
/// `workspace:delete-cancelled`, and clears the projection; a re-run with a
/// short window commits for real (`workspace:deleted`), after which a late
/// cancel answers the race-safe `{ cancelled: false }`.
#[tokio::test]
async fn workspace_delete_grace_window_schedule_cancel_commit_over_wss() {
    let (daemon, port, cfg) = boot().await;

    let socket = daemon.data_dir.join("intentd.sock");
    let create = uds_rpc(
        &socket,
        2,
        "workspace.create",
        json!({ "title": "GraceWindow", "branch": "main", "skipWorktree": true }),
    )
    .await;
    let ws_id = create["result"]["workspace"]["id"]
        .as_str()
        .expect("workspace id")
        .to_string();

    let mut sub = connect_ws(port, cfg.clone()).await;
    let sub_res = wss_rpc(
        &mut sub,
        1,
        "events.subscribe",
        json!({
            "eventTypes": [
                "workspace:delete-scheduled",
                "workspace:delete-cancelled",
                "workspace:deleted",
            ],
            "workspaceId": ws_id,
        }),
    )
    .await;
    assert!(sub_res["subscriptionId"].is_string(), "sub id: {sub_res}");

    let mut rpc = connect_ws(port, cfg.clone()).await;

    // Schedule with a window long enough to observe the pending state.
    let scheduled = wss_rpc(
        &mut rpc,
        2,
        "workspace.delete",
        json!({ "workspaceId": ws_id, "undoDelayMs": 30_000 }),
    )
    .await;
    assert_eq!(scheduled["success"], json!(true), "{scheduled}");
    assert_eq!(scheduled["scheduled"], json!(true), "{scheduled}");
    let delete_at = scheduled["deleteAt"]
        .as_str()
        .expect("deleteAt")
        .to_string();
    assert!(!delete_at.is_empty());

    let evt = next_event(&mut sub, &["workspace:delete-scheduled"], 10).await;
    assert_eq!(evt["workspaceId"], ws_id.as_str());
    assert_eq!(
        evt["data"],
        json!({ "workspaceId": ws_id, "deleteAt": delete_at })
    );

    // Idempotent re-schedule: same deadline back, no new window.
    let again = wss_rpc(
        &mut rpc,
        3,
        "workspace.delete",
        json!({ "workspaceId": ws_id, "undoDelayMs": 30_000 }),
    )
    .await;
    assert_eq!(again["deleteAt"], json!(delete_at), "{again}");

    // Pending row is still served, carrying `pendingDeleteAt` (presence-
    // detected additive field).
    let got = wss_rpc(
        &mut rpc,
        4,
        "workspace.get",
        json!({ "workspaceId": ws_id }),
    )
    .await;
    assert_eq!(
        got["workspace"]["pendingDeleteAt"],
        json!(delete_at),
        "{got}"
    );
    let listed = wss_rpc(&mut rpc, 5, "workspace.list", json!({})).await;
    let row = listed["workspaces"]
        .as_array()
        .expect("workspaces array")
        .iter()
        .find(|w| w["id"] == json!(ws_id))
        .expect("pending row still listed")
        .clone();
    assert_eq!(row["pendingDeleteAt"], json!(delete_at), "{row}");

    // Cancel within the window: `{ cancelled: true }` + event + projection
    // cleared.
    let cancelled = wss_rpc(
        &mut rpc,
        6,
        "workspace.cancelDelete",
        json!({ "workspaceId": ws_id }),
    )
    .await;
    assert_eq!(cancelled, json!({ "cancelled": true }), "{cancelled}");
    let evt = next_event(&mut sub, &["workspace:delete-cancelled"], 10).await;
    assert_eq!(evt["data"], json!({ "workspaceId": ws_id }));
    let got = wss_rpc(
        &mut rpc,
        7,
        "workspace.get",
        json!({ "workspaceId": ws_id }),
    )
    .await;
    assert!(
        got["workspace"].get("pendingDeleteAt").is_none(),
        "pendingDeleteAt omitted after cancel: {got}"
    );

    // Re-schedule with a short window and let it commit: the daemon-owned
    // timer runs the immediate-delete cascade (`workspace:deleted`).
    let scheduled = wss_rpc(
        &mut rpc,
        8,
        "workspace.delete",
        json!({ "workspaceId": ws_id, "undoDelayMs": 250 }),
    )
    .await;
    assert_eq!(scheduled["scheduled"], json!(true), "{scheduled}");
    let evt = next_event(&mut sub, &["workspace:deleted"], 15).await;
    assert_eq!(evt["data"], json!({ "workspaceId": ws_id }));

    // Cancel after commit: the race-safe non-error `{ cancelled: false }`.
    let late = wss_rpc(
        &mut rpc,
        9,
        "workspace.cancelDelete",
        json!({ "workspaceId": ws_id }),
    )
    .await;
    assert_eq!(late, json!({ "cancelled": false }), "{late}");
}

/// End-to-end agent delete grace window over WSS (PROTOCOL §5.5):
/// `agent.delete` with `undoDelayMs > 0` answers `{ success, scheduled,
/// deleteAt }` and emits `agent:delete-scheduled`; while pending the session
/// is still served by `agent.get` / `agent.list` / `agent.getSession`
/// carrying `pendingDeleteAt` (and is NOT stopped); `agent.cancelDelete`
/// answers `{ cancelled: true }`, emits `agent:delete-cancelled`, and clears
/// the projection; a re-run with a short window commits for real
/// (`agent:deleted`), after which a late cancel answers the race-safe
/// `{ cancelled: false }`.
#[tokio::test]
async fn agent_delete_grace_window_schedule_cancel_commit_over_wss() {
    let (daemon, port, cfg) = boot().await;

    let socket = daemon.data_dir.join("intentd.sock");
    let create = uds_rpc(
        &socket,
        2,
        "workspace.create",
        json!({ "title": "AgentGrace", "branch": "main", "skipWorktree": true }),
    )
    .await;
    let ws_id = create["result"]["workspace"]["id"]
        .as_str()
        .expect("workspace id")
        .to_string();

    let mut rpc = connect_ws(port, cfg.clone()).await;
    let created = wss_rpc(
        &mut rpc,
        1,
        "agent.create",
        json!({ "workspaceId": ws_id, "name": "Grace Agent" }),
    )
    .await;
    let agent_id = created["agent"]["id"]
        .as_str()
        .expect("agent id")
        .to_string();

    let mut sub = connect_ws(port, cfg.clone()).await;
    let sub_res = wss_rpc(
        &mut sub,
        1,
        "events.subscribe",
        json!({
            "eventTypes": [
                "agent:delete-scheduled",
                "agent:delete-cancelled",
                "agent:deleted",
            ],
            "workspaceId": ws_id,
        }),
    )
    .await;
    assert!(sub_res["subscriptionId"].is_string(), "sub id: {sub_res}");

    // Schedule with a window long enough to observe the pending state.
    let scheduled = wss_rpc(
        &mut rpc,
        2,
        "agent.delete",
        json!({ "agentId": agent_id, "workspaceId": ws_id, "undoDelayMs": 30_000 }),
    )
    .await;
    assert_eq!(scheduled["success"], json!(true), "{scheduled}");
    assert_eq!(scheduled["scheduled"], json!(true), "{scheduled}");
    let delete_at = scheduled["deleteAt"]
        .as_str()
        .expect("deleteAt")
        .to_string();
    assert!(!delete_at.is_empty());

    let evt = next_event(&mut sub, &["agent:delete-scheduled"], 10).await;
    assert_eq!(evt["workspaceId"], ws_id.as_str());
    assert_eq!(
        evt["data"],
        json!({ "agentId": agent_id, "workspaceId": ws_id, "deleteAt": delete_at })
    );

    // Idempotent re-schedule: same deadline back, no new window.
    let again = wss_rpc(
        &mut rpc,
        3,
        "agent.delete",
        json!({ "agentId": agent_id, "workspaceId": ws_id, "undoDelayMs": 30_000 }),
    )
    .await;
    assert_eq!(again["deleteAt"], json!(delete_at), "{again}");

    // Pending session is still served, carrying `pendingDeleteAt`
    // (presence-detected additive field) on all three projections.
    let got = wss_rpc(&mut rpc, 4, "agent.get", json!({ "agentId": agent_id })).await;
    assert_eq!(got["agent"]["pendingDeleteAt"], json!(delete_at), "{got}");
    let listed = wss_rpc(&mut rpc, 5, "agent.list", json!({ "workspaceId": ws_id })).await;
    let row = listed["agents"]
        .as_array()
        .expect("agents array")
        .iter()
        .find(|a| a["id"] == json!(agent_id))
        .expect("pending row still listed")
        .clone();
    assert_eq!(row["pendingDeleteAt"], json!(delete_at), "{row}");
    let session = wss_rpc(
        &mut rpc,
        6,
        "agent.getSession",
        json!({ "agentId": agent_id }),
    )
    .await;
    assert_eq!(
        session["session"]["pendingDeleteAt"],
        json!(delete_at),
        "{session}"
    );

    // Cancel within the window: `{ cancelled: true }` + event + projection
    // cleared.
    let cancelled = wss_rpc(
        &mut rpc,
        7,
        "agent.cancelDelete",
        json!({ "agentId": agent_id, "workspaceId": ws_id }),
    )
    .await;
    assert_eq!(cancelled, json!({ "cancelled": true }), "{cancelled}");
    let evt = next_event(&mut sub, &["agent:delete-cancelled"], 10).await;
    assert_eq!(
        evt["data"],
        json!({ "agentId": agent_id, "workspaceId": ws_id })
    );
    let got = wss_rpc(&mut rpc, 8, "agent.get", json!({ "agentId": agent_id })).await;
    assert!(
        got["agent"].get("pendingDeleteAt").is_none(),
        "pendingDeleteAt omitted after cancel: {got}"
    );

    // Re-schedule with a short window and let it commit: the daemon-owned
    // timer runs the immediate-delete cascade (`agent:deleted`).
    let scheduled = wss_rpc(
        &mut rpc,
        9,
        "agent.delete",
        json!({ "agentId": agent_id, "workspaceId": ws_id, "undoDelayMs": 250 }),
    )
    .await;
    assert_eq!(scheduled["scheduled"], json!(true), "{scheduled}");
    let evt = next_event(&mut sub, &["agent:deleted"], 15).await;
    // intent-hq/monorepo#2869: the delete emit carries agentName.
    assert_eq!(
        evt["data"],
        json!({ "agentId": agent_id, "agentName": "Grace Agent" })
    );

    // Cancel after commit: the race-safe non-error `{ cancelled: false }`.
    let late = wss_rpc(
        &mut rpc,
        10,
        "agent.cancelDelete",
        json!({ "agentId": agent_id }),
    )
    .await;
    assert_eq!(late, json!({ "cancelled": false }), "{late}");
}

/// End-to-end TASKFLOW-1 flow over WSS: authoring a note whose content holds
/// an `@@@task` block auto-converts the fence into a linked child task note
/// (fence-free parent + `note:created` for the child + `note:updated` for the
/// rewritten parent), `note.listTasks` surfaces the linked task, and
/// `task.assignAgent` flips the `not_started` task to `in_progress` with a
/// `note:updated` (assignment write) + `task:status-changed` +
/// `task:ready-tasks-changed` fan-out (docs/protocol/06-events.md §6.5) — author → list →
/// delegate → in-progress, driven end-to-end over WSS with subscribers seeing
/// every emission live and one confirming `task.get` at the end.
#[tokio::test]
async fn task_block_author_list_assign_flow_over_wss() {
    let (daemon, port, cfg) = boot().await;

    let socket = daemon.data_dir.join("intentd.sock");
    let create = uds_rpc(
        &socket,
        2,
        "workspace.create",
        json!({ "title": "Taskflow", "branch": "main", "skipWorktree": true }),
    )
    .await;
    let ws_id = create["result"]["workspace"]["id"]
        .as_str()
        .expect("workspace id")
        .to_string();

    // Subscribe before authoring so all conversion emissions are observed.
    let mut sub = connect_ws(port, cfg.clone()).await;
    let sub_res = wss_rpc(
        &mut sub,
        1,
        "events.subscribe",
        json!({
            "eventTypes": [
                "note:created",
                "note:updated",
                "task:status-changed",
                "task:ready-tasks-changed",
            ],
            "workspaceId": ws_id,
        }),
    )
    .await;
    assert!(sub_res["subscriptionId"].is_string(), "sub id: {sub_res}");

    // Author: `note.create` with an `@@@task` fence auto-converts on write.
    let mut rpc = connect_ws(port, cfg.clone()).await;
    let created = wss_rpc(
        &mut rpc,
        2,
        "note.create",
        json!({
            "workspaceId": ws_id,
            "title": "Plan",
            "content": "intro\n@@@task\n# Ship It\nbody\n@@@\ntail",
        }),
    )
    .await;
    let parent_id = created["note"]["id"].as_str().expect("note id").to_string();
    let content = created["note"]["content"].as_str().expect("content");
    assert!(!content.contains("@@@task"), "fence not removed: {content}");
    assert!(
        content.contains("- [ ] [Ship It](intent://local/task/"),
        "no task link: {content}"
    );
    // The result surfaces the conversion outcome alongside the note (parity
    // with the four content-write ops): `convertedCount`, `createdTaskNoteIds`,
    // `createdTasks`, `warnings`.
    assert_eq!(created["convertedCount"], json!(1), "result: {created}");
    let created_ids = created["createdTaskNoteIds"]
        .as_array()
        .expect("createdTaskNoteIds array");
    assert_eq!(created_ids.len(), 1, "result: {created}");
    let created_tasks = created["createdTasks"]
        .as_array()
        .expect("createdTasks array");
    assert_eq!(created_tasks.len(), 1, "result: {created}");
    assert_eq!(created_tasks[0]["title"], json!("Ship It"));
    assert_eq!(created_tasks[0]["noteId"], created_ids[0]);
    assert_eq!(created["warnings"], json!([]), "result: {created}");

    // List: the converted block is a linked task row.
    let tasks = wss_rpc(
        &mut rpc,
        3,
        "note.listTasks",
        json!({ "workspaceId": ws_id, "noteId": parent_id }),
    )
    .await;
    let rows = tasks.as_array().expect("bare array");
    assert_eq!(rows.len(), 1, "rows: {tasks}");
    assert_eq!(rows[0]["text"], json!("Ship It"));
    assert_eq!(rows[0]["status"], json!("todo"));
    let task_id = rows[0]["taskNoteId"]
        .as_str()
        .expect("linked task note id")
        .to_string();
    assert_eq!(rows[0]["linkedTaskNoteId"], json!(task_id));

    // Authoring emitted `note:created` for the spawned child, `note:updated`
    // for the rewritten parent, and `note:created` for the parent itself.
    let mut saw_child_created = false;
    let mut saw_parent_updated = false;
    let mut saw_parent_created = false;
    for _ in 0..3 {
        let evt = next_event(&mut sub, &["note:created", "note:updated"], 10).await;
        let ty = evt["type"].as_str().unwrap_or("");
        let nid = evt["data"]["noteId"].as_str().unwrap_or("");
        match (ty, nid) {
            ("note:created", id) if id == task_id => {
                assert_eq!(evt["data"]["title"], json!("Ship It"));
                saw_child_created = true;
            }
            ("note:updated", id) if id == parent_id => saw_parent_updated = true,
            ("note:created", id) if id == parent_id => saw_parent_created = true,
            other => panic!("unexpected event {other:?}: {evt}"),
        }
    }
    assert!(saw_child_created && saw_parent_updated && saw_parent_created);

    // Delegate: assigning an agent to the `not_started` task flips it to
    // `in_progress` and publishes `task:status-changed`.
    let agent = "agent-b0a8044a-5eac-4b52-8456-15d3b784decb";
    let assign = wss_rpc(
        &mut rpc,
        4,
        "task.assignAgent",
        json!({ "workspaceId": ws_id, "noteId": task_id, "agentId": agent }),
    )
    .await;
    assert_eq!(assign["ok"], json!(true), "assign: {assign}");
    assert_eq!(assign["noteId"], json!(task_id));
    assert_eq!(assign["agentId"], json!(agent));

    // The assignment write routes through `updateNote` (TS parity), publishing
    // a `note:updated` for the task note before the status transition fires.
    let evt = next_event(&mut sub, &["note:updated"], 10).await;
    assert_eq!(
        evt["data"]["noteId"],
        json!(task_id),
        "assign updated: {evt}"
    );

    let evt = next_event(&mut sub, &["task:status-changed"], 10).await;
    assert_eq!(evt["data"]["noteId"], json!(task_id));
    assert_eq!(evt["data"]["previousStatus"], json!("not_started"));
    assert_eq!(evt["data"]["newStatus"], json!("in_progress"));
    assert!(evt["data"]["changedAt"].is_string(), "changedAt: {evt}");
    let changed_at = evt["data"]["changedAt"].clone();

    // The transition also recomputes the ready-task set: the now-in-progress
    // task drops out of `readyTaskIds`, and `computedAt` matches the
    // triggering status change's timestamp.
    let evt = next_event(&mut sub, &["task:ready-tasks-changed"], 10).await;
    assert_eq!(evt["data"]["triggeredBy"]["noteId"], json!(task_id));
    assert_eq!(
        evt["data"]["triggeredBy"]["previousStatus"],
        json!("not_started")
    );
    assert_eq!(
        evt["data"]["triggeredBy"]["newStatus"],
        json!("in_progress")
    );
    let ready = evt["data"]["readyTaskIds"]
        .as_array()
        .expect("readyTaskIds array");
    assert!(
        !ready.iter().any(|v| v == &json!(task_id)),
        "in-progress task still ready: {evt}"
    );
    assert_eq!(evt["data"]["computedAt"], changed_at, "computedAt: {evt}");

    // In-progress: the task note reflects the transition and the assignment.
    let got = wss_rpc(
        &mut rpc,
        5,
        "task.get",
        json!({ "workspaceId": ws_id, "taskNoteId": task_id }),
    )
    .await;
    assert_eq!(got["task"]["status"], json!("in_progress"), "task: {got}");
}

/// End-to-end `task:created` over WSS (docs/protocol/06-events.md §6.5): every path where a
/// note becomes a task publishes exactly one `task:created` carrying
/// `{ noteId, noteTitle, status, createdAt }`. Drives all three paths on one
/// subscription — `note.create` with an `@@@task` fence (auto-conversion),
/// `task.markAsTask` on a plain note (which also emits the `note:updated`
/// metadata refetch), and `task.createPrerequisite` — with a re-`markAsTask`
/// in between to prove an already-task note does not emit a second creation
/// (the next `task:created` observed is the prerequisite's, and the stream is
/// ordered).
#[tokio::test]
async fn task_created_emitted_on_every_creation_path_over_wss() {
    let (daemon, port, cfg) = boot().await;

    let socket = daemon.data_dir.join("intentd.sock");
    let create = uds_rpc(
        &socket,
        2,
        "workspace.create",
        json!({ "title": "TaskCreated", "branch": "main", "skipWorktree": true }),
    )
    .await;
    let ws_id = create["result"]["workspace"]["id"]
        .as_str()
        .expect("workspace id")
        .to_string();

    let mut sub = connect_ws(port, cfg.clone()).await;
    let sub_res = wss_rpc(
        &mut sub,
        1,
        "events.subscribe",
        json!({ "eventTypes": ["task:*", "note:updated"], "workspaceId": ws_id }),
    )
    .await;
    assert!(sub_res["subscriptionId"].is_string(), "sub id: {sub_res}");

    let mut rpc = connect_ws(port, cfg.clone()).await;

    // Path 1 — `@@@task` auto-conversion on `note.create`.
    let created = wss_rpc(
        &mut rpc,
        2,
        "note.create",
        json!({
            "workspaceId": ws_id,
            "title": "Plan",
            "content": "intro\n@@@task\n# Converted Task\nbody\n@@@",
        }),
    )
    .await;
    let parent_id = created["note"]["id"].as_str().expect("note id").to_string();
    let tasks = wss_rpc(
        &mut rpc,
        3,
        "note.listTasks",
        json!({ "workspaceId": ws_id, "noteId": parent_id }),
    )
    .await;
    let child_id = tasks[0]["taskNoteId"]
        .as_str()
        .expect("linked task note id")
        .to_string();

    let evt = next_event(&mut sub, &["task:created"], 10).await;
    assert_eq!(evt["workspaceId"], ws_id.as_str());
    assert!(evt["id"].is_string(), "event id: {evt}");
    assert!(evt["timestamp"].is_string(), "timestamp: {evt}");
    assert_eq!(
        evt["actor"],
        json!({ "type": "system", "id": "system", "name": "System" })
    );
    assert_eq!(evt["data"]["noteId"], json!(child_id));
    assert_eq!(evt["data"]["noteTitle"], json!("Converted Task"));
    assert_eq!(evt["data"]["status"], json!("not_started"));
    assert!(evt["data"]["createdAt"].is_string(), "createdAt: {evt}");
    // System-attributed creation: no agent provenance on the payload.
    assert!(evt["data"].get("agentId").is_none(), "agentId: {evt}");

    // Path 2 — `task.markAsTask` promotes a plain note.
    let plain = wss_rpc(
        &mut rpc,
        4,
        "note.create",
        json!({ "workspaceId": ws_id, "title": "Plain", "content": "body" }),
    )
    .await;
    let plain_id = plain["note"]["id"].as_str().expect("note id").to_string();
    let marked = wss_rpc(
        &mut rpc,
        5,
        "task.markAsTask",
        json!({ "workspaceId": ws_id, "noteId": plain_id, "status": "not_started" }),
    )
    .await;
    assert_eq!(marked["ok"], json!(true), "markAsTask: {marked}");

    // The metadata write emits `note:updated` so note-driven refetches fire.
    // The auto-conversion above left its own parent-rewrite `note:updated` on
    // the stream, so skip forward to this note's.
    loop {
        let evt = next_event(&mut sub, &["note:updated"], 10).await;
        if evt["data"]["noteId"] == json!(plain_id) {
            break;
        }
    }

    let evt = next_event(&mut sub, &["task:created"], 10).await;
    assert_eq!(evt["data"]["noteId"], json!(plain_id));
    assert_eq!(evt["data"]["noteTitle"], json!("Plain"));
    assert_eq!(evt["data"]["status"], json!("not_started"));

    // Re-marking an already-task note is a status move, not a creation: it
    // takes the same `task:status-changed` `task.updateNoteStatus` publishes.
    wss_rpc(
        &mut rpc,
        6,
        "task.markAsTask",
        json!({ "workspaceId": ws_id, "noteId": plain_id, "status": "in_progress" }),
    )
    .await;

    let evt = next_event(&mut sub, &["task:status-changed"], 10).await;
    assert_eq!(evt["data"]["noteId"], json!(plain_id));
    assert_eq!(evt["data"]["previousStatus"], json!("not_started"));
    assert_eq!(evt["data"]["newStatus"], json!("in_progress"));

    // Path 3 — `task.createPrerequisite`. Because the stream is ordered, the
    // next `task:created` being the prerequisite's proves the re-mark above
    // published none.
    let prereq = wss_rpc(
        &mut rpc,
        7,
        "task.createPrerequisite",
        json!({
            "workspaceId": ws_id,
            "dependentNoteId": plain_id,
            "title": "Prereq",
            "status": "not_started",
        }),
    )
    .await;
    let prereq_id = prereq["prerequisiteNoteId"]
        .as_str()
        .expect("prerequisite note id")
        .to_string();

    let evt = next_event(&mut sub, &["task:created"], 10).await;
    assert_eq!(
        evt["data"]["noteId"],
        json!(prereq_id),
        "re-marking an existing task must not emit task:created: {evt}"
    );
    assert_eq!(evt["data"]["noteTitle"], json!("Prereq"));
    assert_eq!(evt["data"]["status"], json!("not_started"));
    assert!(evt["data"]["createdAt"].is_string(), "createdAt: {evt}");
}

/// End-to-end reference-parity self-heal: a workspace whose `spec` note has
/// been deleted gets it reseeded on the next `note.list` (reference:
/// `notes.service.ts getNotes` → `ensureSpecExists`). The reseed emits a
/// single `note:created` for `noteId=spec`, and the WSS `note.list` response
/// includes the freshly-seeded spec in the returned `notes` array.
#[tokio::test]
async fn note_list_reseeds_missing_spec_over_wss() {
    let (daemon, port, cfg) = boot().await;

    // Bootstrap the workspace and delete the initial spec off the UDS so the
    // WSS event stream only carries the reseed emission.
    let socket = daemon.data_dir.join("intentd.sock");
    let create = uds_rpc(
        &socket,
        2,
        "workspace.create",
        json!({ "title": "SpecHeal", "branch": "main", "skipWorktree": true }),
    )
    .await;
    let ws_id = create["result"]["workspace"]["id"]
        .as_str()
        .expect("workspace id")
        .to_string();
    let del = uds_rpc(
        &socket,
        3,
        "note.delete",
        json!({ "workspaceId": ws_id, "noteId": "spec" }),
    )
    .await;
    assert_eq!(del["result"]["ok"], json!(true), "delete spec: {del}");

    // Subscribe over WSS before invoking `note.list` so the reseed emission is
    // guaranteed to be observed.
    let mut sub = connect_ws(port, cfg.clone()).await;
    let sub_res = wss_rpc(
        &mut sub,
        1,
        "events.subscribe",
        json!({ "eventTypes": ["note:created"], "workspaceId": ws_id }),
    )
    .await;
    assert!(sub_res["subscriptionId"].is_string(), "sub id: {sub_res}");

    // Drive `note.list` over a separate WSS RPC connection.
    let mut rpc = connect_ws(port, cfg.clone()).await;
    let list = wss_rpc(&mut rpc, 2, "note.list", json!({ "workspaceId": ws_id })).await;
    let notes = list["notes"].as_array().expect("notes array");
    let spec = notes
        .iter()
        .find(|n| n["id"] == json!("spec"))
        .expect("spec present in response");
    assert_eq!(spec["workspaceId"], json!(ws_id));
    assert_eq!(spec["title"], json!("Spec"));
    assert_eq!(spec["content"], json!(""));
    assert_eq!(spec["isPinned"], json!(true));
    assert_eq!(spec["isDefault"], json!(true));

    let evt = next_event(&mut sub, &["note:created"], 10).await;
    assert_eq!(evt["workspaceId"], ws_id.as_str());
    assert_eq!(evt["data"]["noteId"], json!("spec"));
    assert_eq!(evt["data"]["title"], json!("Spec"));
    assert_eq!(evt["data"]["action"], json!("create"));

    // The reseed publishes exactly one `note:created`; drain the socket for a
    // short window and fail if a second one arrives. Non-matching frames
    // (heartbeats, other event types) are ignored.
    let extra = timeout(Duration::from_millis(500), async {
        loop {
            match sub.next().await {
                Some(Ok(Message::Text(text))) => {
                    let v: Value = match serde_json::from_str(&text) {
                        Ok(x) => x,
                        Err(_) => continue,
                    };
                    if v["method"] == json!("events.event")
                        && v["params"]["event"]["type"] == json!("note:created")
                    {
                        return Some(v);
                    }
                }
                Some(Ok(Message::Ping(p))) => {
                    let _ = sub.send(Message::Pong(p)).await;
                }
                Some(Ok(_)) => {}
                // Stream close / error during the "should be quiet" window is
                // not the condition this drain is guarding against; surface
                // it instead of spinning silently until the timeout.
                Some(Err(e)) => panic!("subscription socket errored during drain: {e:?}"),
                None => panic!("subscription socket closed during drain"),
            }
        }
    })
    .await;
    assert!(
        extra.is_err(),
        "reseed must publish exactly one note:created, got extra: {extra:?}"
    );
}

/// Regression guard for monorepo#3404 over the wire: `note.list` for a
/// workspace that does not exist (deleted, or never created) returns the
/// standard not-found error envelope (`-32602` with `error.data.code:
/// "not-found"`, §9) instead of a best-effort empty list — the spec reseed
/// verifies the workspace row before attempting any INSERT, so no raw FK
/// violation is logged and no `note:created` is emitted.
#[tokio::test]
async fn note_list_unknown_workspace_not_found_over_wss() {
    let (daemon, port, cfg) = boot().await;

    // Bootstrap then delete a workspace off the UDS so the WSS call runs
    // against a genuinely-deleted row (the shape from the issue: a client
    // holding a stale workspace id across a deletion).
    let socket = daemon.data_dir.join("intentd.sock");
    let create = uds_rpc(
        &socket,
        2,
        "workspace.create",
        json!({ "title": "GoneSoon", "branch": "main", "skipWorktree": true }),
    )
    .await;
    let ws_id = create["result"]["workspace"]["id"]
        .as_str()
        .expect("workspace id")
        .to_string();
    let del = uds_rpc(
        &socket,
        3,
        "workspace.delete",
        json!({ "workspaceId": ws_id }),
    )
    .await;
    assert!(del.get("error").is_none(), "workspace.delete: {del}");

    let mut rpc = connect_ws(port, cfg.clone()).await;

    // Deleted workspace: standard not-found error envelope.
    let stale = wss_rpc_envelope(&mut rpc, 4, "note.list", json!({ "workspaceId": ws_id })).await;
    assert_eq!(stale["jsonrpc"], json!("2.0"));
    assert_eq!(stale["error"]["code"], json!(-32602), "{stale}");
    assert_eq!(
        stale["error"]["data"]["code"],
        json!("not-found"),
        "{stale}"
    );

    // Never-existed workspace id: same envelope.
    let missing = wss_rpc_envelope(
        &mut rpc,
        5,
        "note.list",
        json!({ "workspaceId": "ws_does_not_exist" }),
    )
    .await;
    assert_eq!(missing["error"]["code"], json!(-32602), "{missing}");
    assert_eq!(
        missing["error"]["data"]["code"],
        json!("not-found"),
        "{missing}"
    );
}

/// Drain any additional `events.event` frames matching `event_type` in
/// `window` ms; return the first extra observed, or `None` if the socket
/// stayed quiet. Non-matching frames (heartbeats, unrelated event types) are
/// ignored so the drain is scoped strictly to cardinality of the target
/// emission.
async fn drain_extra<S>(
    ws: &mut WebSocketStream<S>,
    event_type: &str,
    window_ms: u64,
) -> Option<Value>
where
    S: tokio::io::AsyncRead + tokio::io::AsyncWrite + Unpin,
{
    timeout(Duration::from_millis(window_ms), async {
        loop {
            match ws.next().await {
                Some(Ok(Message::Text(text))) => {
                    let v: Value = match serde_json::from_str(&text) {
                        Ok(x) => x,
                        Err(_) => continue,
                    };
                    if v["method"] == json!("events.event")
                        && v["params"]["event"]["type"] == json!(event_type)
                    {
                        return v;
                    }
                }
                Some(Ok(Message::Ping(p))) => {
                    let _ = ws.send(Message::Pong(p)).await;
                }
                Some(Ok(_)) => {}
                Some(Err(e)) => panic!("subscription socket errored during drain: {e:?}"),
                None => panic!("subscription socket closed during drain"),
            }
        }
    })
    .await
    .ok()
}

/// End-to-end (Audit D C2): `comment.respond` over WSS publishes
/// `comment:added` with `{ noteId, commentId }` for the reply so a subscribed
/// client sees the new thread comment without a re-read (PROTOCOL §6.5,
/// comment channel; reference `comment.respond` dispatches the same domain
/// event as `comment.add`).
#[tokio::test]
async fn comment_respond_emits_comment_added_over_wss() {
    let (daemon, port, cfg) = boot().await;

    // Bootstrap workspace + note + root comment off UDS so the WSS subscriber
    // observes only the respond emission.
    let socket = daemon.data_dir.join("intentd.sock");
    let create = uds_rpc(
        &socket,
        2,
        "workspace.create",
        json!({ "title": "Comments", "branch": "main", "skipWorktree": true }),
    )
    .await;
    let ws_id = create["result"]["workspace"]["id"]
        .as_str()
        .expect("workspace id")
        .to_string();
    let n = uds_rpc(
        &socket,
        3,
        "note.create",
        json!({ "workspaceId": ws_id, "title": "Note", "content": "anchor target text" }),
    )
    .await;
    let note_id = n["result"]["note"]["id"]
        .as_str()
        .expect("note id")
        .to_string();
    let add = uds_rpc(
        &socket,
        4,
        "comment.add",
        json!({
            "workspaceId": ws_id,
            "noteId": note_id,
            "searchContext": "anchor target text",
            "commentTarget": "target",
            "comment": "root"
        }),
    )
    .await;
    let root_comment_id = add["result"]["commentId"]
        .as_str()
        .expect("comment id")
        .to_string();

    // Subscribe over WSS before the respond, scoped to comment:added.
    let mut sub = connect_ws(port, cfg.clone()).await;
    let sub_res = wss_rpc(
        &mut sub,
        1,
        "events.subscribe",
        json!({ "eventTypes": ["comment:added"], "workspaceId": ws_id }),
    )
    .await;
    assert!(sub_res["subscriptionId"].is_string(), "sub id: {sub_res}");

    // Drive comment.respond on a separate WSS RPC connection.
    let mut rpc = connect_ws(port, cfg.clone()).await;
    let reply = wss_rpc(
        &mut rpc,
        2,
        "comment.respond",
        json!({
            "workspaceId": ws_id,
            "noteId": note_id,
            "commentId": root_comment_id,
            "comment": "reply body",
        }),
    )
    .await;
    let reply_id = reply["comment"]["id"]
        .as_str()
        .expect("reply id")
        .to_string();
    assert_ne!(reply_id, root_comment_id, "reply must have its own id");
    // Reply-anchoring contract (monorepo#729): the reply anchors via its
    // threadId/parentId — the wire omits `anchor`/`anchorText` entirely.
    assert!(
        reply["comment"].get("anchor").is_none(),
        "reply must not carry an anchor: {reply}"
    );
    assert!(
        reply["comment"].get("anchorText").is_none(),
        "reply must not carry anchorText: {reply}"
    );
    assert_eq!(reply["comment"]["parentId"], json!(root_comment_id));

    // The root comment keeps its authoritative anchor: getThread returns the
    // root with anchor/anchorText intact and the anchorless reply.
    let thread = wss_rpc(
        &mut rpc,
        3,
        "comment.getThread",
        json!({ "workspaceId": ws_id, "noteId": note_id, "commentId": root_comment_id }),
    )
    .await;
    let root = &thread["rootComment"];
    assert!(root["anchor"].is_object(), "root keeps anchor: {thread}");
    assert_eq!(root["anchorText"], json!("target"));
    let replies = thread["replies"].as_array().expect("replies");
    assert_eq!(replies.len(), 1);
    assert!(
        replies[0].get("anchor").is_none(),
        "reply in thread: {thread}"
    );
    assert!(replies[0].get("anchorText").is_none());

    let evt = next_event(&mut sub, &["comment:added"], 10).await;
    assert_eq!(evt["workspaceId"], ws_id.as_str());
    assert!(evt["id"].is_string(), "event id: {evt}");
    assert!(evt["timestamp"].is_string(), "timestamp: {evt}");
    assert_eq!(
        evt["actor"],
        json!({ "type": "system", "id": "system", "name": "System" })
    );
    assert_eq!(
        evt["data"],
        json!({ "noteId": note_id, "commentId": reply_id })
    );

    // Cardinality exactly 1: no second comment:added lands.
    let extra = drain_extra(&mut sub, "comment:added", 500).await;
    assert!(
        extra.is_none(),
        "comment.respond must publish exactly one comment:added, got extra: {extra:?}"
    );
}

/// End-to-end (Audit A F5): `comment.add` over WSS honours
/// `params.idempotencyKey` — a replay with the same key returns the ORIGINAL
/// result (same `commentId`) without re-executing, so no duplicate comment is
/// persisted and no second `comment:added` event is published.
#[tokio::test]
async fn comment_add_idempotency_key_dedupes_over_wss() {
    let (daemon, port, cfg) = boot().await;

    // Bootstrap workspace + note off UDS so the WSS side drives only the adds.
    let socket = daemon.data_dir.join("intentd.sock");
    let create = uds_rpc(
        &socket,
        2,
        "workspace.create",
        json!({ "title": "Comments", "branch": "main", "skipWorktree": true }),
    )
    .await;
    let ws_id = create["result"]["workspace"]["id"]
        .as_str()
        .expect("workspace id")
        .to_string();
    let n = uds_rpc(
        &socket,
        3,
        "note.create",
        json!({ "workspaceId": ws_id, "title": "Note", "content": "anchor target text" }),
    )
    .await;
    let note_id = n["result"]["note"]["id"]
        .as_str()
        .expect("note id")
        .to_string();

    // Subscribe over WSS before the adds, scoped to comment:added.
    let mut sub = connect_ws(port, cfg.clone()).await;
    let sub_res = wss_rpc(
        &mut sub,
        1,
        "events.subscribe",
        json!({ "eventTypes": ["comment:added"], "workspaceId": ws_id }),
    )
    .await;
    assert!(sub_res["subscriptionId"].is_string(), "sub id: {sub_res}");

    let mut rpc = connect_ws(port, cfg.clone()).await;
    let params = json!({
        "workspaceId": ws_id,
        "noteId": note_id,
        "searchContext": "anchor target text",
        "commentTarget": "target",
        "comment": "root",
        "idempotencyKey": "wss-comment-idem-1",
    });
    let first = wss_rpc(&mut rpc, 2, "comment.add", params.clone()).await;
    let comment_id = first["commentId"].as_str().expect("comment id").to_string();
    let evt = next_event(&mut sub, &["comment:added"], 10).await;
    assert_eq!(
        evt["data"],
        json!({ "noteId": note_id, "commentId": comment_id })
    );

    // Replay with the same idempotencyKey: the stored result comes back (same
    // commentId), nothing re-executes.
    let second = wss_rpc(&mut rpc, 3, "comment.add", params).await;
    assert_eq!(
        second["commentId"].as_str(),
        Some(comment_id.as_str()),
        "replay must return the original commentId: {second}"
    );

    // Exactly one comment persisted, exactly one comment:added published.
    let list = uds_rpc(
        &socket,
        4,
        "comment.list",
        json!({ "workspaceId": ws_id, "noteId": note_id, "includeComments": true }),
    )
    .await;
    assert_eq!(
        list["result"]["totalThreads"],
        json!(1),
        "replay must not duplicate the comment: {list}"
    );
    let extra = drain_extra(&mut sub, "comment:added", 500).await;
    assert!(
        extra.is_none(),
        "idempotent replay must not publish a second comment:added, got extra: {extra:?}"
    );
}

/// End-to-end (Round 15): over the real WSS router path, `comment.add`
/// (a) self-heals phantom anchor markers — UUID-format debris with no live
/// comment row no longer blocks the add and is scrubbed from the persisted
/// note — and (b) supports overlapping ranges: a second add whose target
/// spans the first comment's markers succeeds, the pairs interleave, and the
/// stored anchorText/anchorContext contain no raw marker text.
#[tokio::test]
async fn comment_add_scrubs_phantoms_and_overlaps_over_wss() {
    let (daemon, port, cfg) = boot().await;

    // Bootstrap workspace + a note pre-polluted with phantom debris off UDS.
    let socket = daemon.data_dir.join("intentd.sock");
    let create = uds_rpc(
        &socket,
        2,
        "workspace.create",
        json!({ "title": "Comments", "branch": "main", "skipWorktree": true }),
    )
    .await;
    let ws_id = create["result"]["workspace"]["id"]
        .as_str()
        .expect("workspace id")
        .to_string();
    let phantom = "aaaaaaaa-bbbb-4ccc-8ddd-eeeeeeeeeeee";
    let n = uds_rpc(
        &socket,
        3,
        "note.create",
        json!({
            "workspaceId": ws_id,
            "title": "Note",
            "content": format!(
                "alpha <!--anchor:{phantom}:start-->beta<!--anchor:{phantom}:end--> gamma delta"
            ),
        }),
    )
    .await;
    let note_id = n["result"]["note"]["id"]
        .as_str()
        .expect("note id")
        .to_string();

    // First add over WSS: the phantom debris around "beta" must not block the
    // match, and the persisted note must be debris-free.
    let mut rpc = connect_ws(port, cfg.clone()).await;
    let first = wss_rpc(
        &mut rpc,
        1,
        "comment.add",
        json!({
            "workspaceId": ws_id,
            "noteId": note_id,
            "searchContext": "alpha beta gamma delta",
            "commentTarget": "beta",
            "comment": "first",
        }),
    )
    .await;
    assert_eq!(first["anchored"], json!(true), "first add: {first}");
    let c1 = first["commentId"].as_str().expect("c1 id").to_string();
    let note = wss_rpc(
        &mut rpc,
        2,
        "note.get",
        json!({ "workspaceId": ws_id, "noteId": note_id }),
    )
    .await;
    let content = note["note"]["content"].as_str().expect("content");
    assert!(
        !content.contains(phantom),
        "phantom debris must be scrubbed: {content}"
    );
    assert!(content.contains(&format!(
        "<!--anchor:{c1}:start-->beta<!--anchor:{c1}:end-->"
    )));

    // Second add whose target spans c1's markers: overlapping ranges are
    // supported — the pairs interleave and both comments stay healthy.
    let second = wss_rpc(
        &mut rpc,
        3,
        "comment.add",
        json!({
            "workspaceId": ws_id,
            "noteId": note_id,
            "searchContext": "alpha beta gamma delta",
            "commentTarget": "beta gamma",
            "comment": "second",
        }),
    )
    .await;
    assert_eq!(second["anchored"], json!(true), "overlapping add: {second}");
    assert_eq!(second["location"]["anchoredText"], json!("beta gamma"));
    let c2 = second["commentId"].as_str().expect("c2 id").to_string();
    let note = wss_rpc(
        &mut rpc,
        4,
        "note.get",
        json!({ "workspaceId": ws_id, "noteId": note_id }),
    )
    .await;
    assert_eq!(
        note["note"]["content"],
        json!(format!(
            "alpha <!--anchor:{c1}:start--><!--anchor:{c2}:start-->beta<!--anchor:{c1}:end--> \
             gamma<!--anchor:{c2}:end--> delta"
        ))
    );

    // Stored rows over the wire: healthy, anchorText/context marker-free.
    let list = wss_rpc(
        &mut rpc,
        5,
        "comment.list",
        json!({ "workspaceId": ws_id, "noteId": note_id, "includeComments": true }),
    )
    .await;
    assert_eq!(list["totalThreads"], json!(2), "list: {list}");
    let row2 = list["threads"]
        .as_array()
        .expect("threads")
        .iter()
        .flat_map(|t| t["comments"].as_array().cloned().unwrap_or_default())
        .find(|c| c["id"] == json!(c2))
        .expect("c2 row");
    assert_ne!(row2["isOrphaned"], json!(true), "c2 row: {row2}");
    assert_eq!(row2["anchorText"], json!("beta gamma"));
    assert_eq!(row2["anchorContext"]["before"], json!("alpha "));
    assert_eq!(row2["anchorContext"]["after"], json!(" delta"));
}

/// End-to-end (monorepo#638): `comment.add` rewrites the note markdown
/// (anchor markers), so over WSS the result must echo the authoritative
/// post-rewrite `noteRev` AND publish exactly one `note:updated` alongside
/// `comment:added` — otherwise subscribed clients hold a stale rev and hit
/// spurious conflicts on their next versioned write. An idempotent replay
/// returns the cached `noteRev` without a second `note:updated`.
#[tokio::test]
async fn comment_add_echoes_note_rev_and_emits_note_updated_over_wss() {
    let (daemon, port, cfg) = boot().await;

    // Bootstrap workspace + note off UDS so the WSS side drives only the add.
    let socket = daemon.data_dir.join("intentd.sock");
    let create = uds_rpc(
        &socket,
        2,
        "workspace.create",
        json!({ "title": "Comments", "branch": "main", "skipWorktree": true }),
    )
    .await;
    let ws_id = create["result"]["workspace"]["id"]
        .as_str()
        .expect("workspace id")
        .to_string();
    let n = uds_rpc(
        &socket,
        3,
        "note.create",
        json!({ "workspaceId": ws_id, "title": "Note", "content": "anchor target text" }),
    )
    .await;
    let note_id = n["result"]["note"]["id"]
        .as_str()
        .expect("note id")
        .to_string();
    let rev_before = n["result"]["note"]["rev"].as_i64().expect("note rev");

    // Subscribe over WSS before the add, scoped to note:updated.
    let mut sub = connect_ws(port, cfg.clone()).await;
    let sub_res = wss_rpc(
        &mut sub,
        1,
        "events.subscribe",
        json!({ "eventTypes": ["note:updated"], "workspaceId": ws_id }),
    )
    .await;
    assert!(sub_res["subscriptionId"].is_string(), "sub id: {sub_res}");

    let mut rpc = connect_ws(port, cfg.clone()).await;
    let params = json!({
        "workspaceId": ws_id,
        "noteId": note_id,
        "searchContext": "anchor target text",
        "commentTarget": "target",
        "comment": "root",
        "idempotencyKey": "wss-comment-rev-1",
    });
    let add = wss_rpc(&mut rpc, 2, "comment.add", params.clone()).await;
    assert_eq!(add["success"], json!(true));
    // The result echoes the post-rewrite rev: exactly one bump over create.
    let note_rev = add["noteRev"].as_i64().expect("noteRev in result");
    assert_eq!(note_rev, rev_before + 1, "result: {add}");

    // The echoed rev is the authoritative stored value.
    let read = wss_rpc(
        &mut rpc,
        3,
        "note.get",
        json!({ "workspaceId": ws_id, "noteId": note_id }),
    )
    .await;
    assert_eq!(read["note"]["rev"].as_i64(), Some(note_rev));

    // Exactly one note:updated for the rewrite, with the §6.5 payload shape.
    let evt = next_event(&mut sub, &["note:updated"], 10).await;
    assert_eq!(evt["workspaceId"], ws_id.as_str());
    assert_eq!(
        evt["data"],
        json!({ "noteId": note_id, "title": "Note", "action": "update" })
    );
    let extra = drain_extra(&mut sub, "note:updated", 500).await;
    assert!(
        extra.is_none(),
        "comment.add must publish exactly one note:updated, got extra: {extra:?}"
    );

    // Idempotent replay: cached result carries the SAME noteRev, no rewrite,
    // no second note:updated.
    let replay = wss_rpc(&mut rpc, 4, "comment.add", params).await;
    assert_eq!(
        replay["noteRev"].as_i64(),
        Some(note_rev),
        "replay: {replay}"
    );
    let extra = drain_extra(&mut sub, "note:updated", 500).await;
    assert!(
        extra.is_none(),
        "idempotent replay must not publish a second note:updated, got extra: {extra:?}"
    );
}

/// End-to-end (Audit D C3): `workspace.archive` over WSS publishes
/// `workspace:updated` with the full applied delta
/// `changes: { archived: true, status: "Archived", archivedAt: <ts> }` where
/// `<ts>` equals the persisted `archivedAt`. §6.5 has no `workspace:archived`
/// event; the reference emitter dispatches `workspaceUpdated` with a
/// `changes` delta.
#[tokio::test]
async fn archive_workspace_emits_workspace_updated_over_wss() {
    let (daemon, port, cfg) = boot().await;

    let socket = daemon.data_dir.join("intentd.sock");
    let create = uds_rpc(
        &socket,
        2,
        "workspace.create",
        json!({ "title": "ToArchive", "branch": "main", "skipWorktree": true }),
    )
    .await;
    let ws_id = create["result"]["workspace"]["id"]
        .as_str()
        .expect("workspace id")
        .to_string();

    let mut sub = connect_ws(port, cfg.clone()).await;
    let sub_res = wss_rpc(
        &mut sub,
        1,
        "events.subscribe",
        json!({ "eventTypes": ["workspace:updated"], "workspaceId": ws_id }),
    )
    .await;
    assert!(sub_res["subscriptionId"].is_string(), "sub id: {sub_res}");

    let mut rpc = connect_ws(port, cfg.clone()).await;
    let archive_res = wss_rpc(
        &mut rpc,
        2,
        "workspace.archive",
        json!({ "workspaceId": ws_id }),
    )
    .await;
    // §5.1 return shape: `workspace.archive` returns the refreshed record so
    // callers do not need a follow-up `workspace.get`. `lastActivity` is
    // BE-derived and always populated on the wire (§9.1).
    assert_eq!(archive_res["workspace"]["id"], ws_id.as_str());
    assert_eq!(archive_res["workspace"]["archived"], json!(true));
    assert_eq!(archive_res["workspace"]["status"], json!("Archived"));
    assert!(archive_res["workspace"]["archivedAt"].is_string());
    assert!(archive_res["workspace"]["lastActivity"].is_string());
    assert!(archive_res.get("success").is_none());
    let archived_at = archive_res["workspace"]["archivedAt"].clone();

    let evt = next_event(&mut sub, &["workspace:updated"], 10).await;
    assert_eq!(evt["workspaceId"], ws_id.as_str());
    assert_eq!(
        evt["data"],
        json!({
            "workspaceId": ws_id,
            "changes": {
                "archived": true,
                "status": "Archived",
                "archivedAt": archived_at,
            }
        })
    );

    let extra = drain_extra(&mut sub, "workspace:updated", 500).await;
    assert!(
        extra.is_none(),
        "workspace.archive must publish exactly one workspace:updated, got extra: {extra:?}"
    );
}

/// End-to-end (Audit D C3, symmetric): `workspace.unarchive` over WSS
/// publishes `workspace:updated` with the full applied delta
/// `changes: { archived: false, status: "Active", archivedAt: null }` — the
/// explicit JSON `null` tells clients to clear the field.
#[tokio::test]
async fn unarchive_workspace_emits_workspace_updated_over_wss() {
    let (daemon, port, cfg) = boot().await;

    let socket = daemon.data_dir.join("intentd.sock");
    let create = uds_rpc(
        &socket,
        2,
        "workspace.create",
        json!({ "title": "ToUnarchive", "branch": "main", "skipWorktree": true }),
    )
    .await;
    let ws_id = create["result"]["workspace"]["id"]
        .as_str()
        .expect("workspace id")
        .to_string();
    // Archive off UDS so the WSS subscriber observes only the unarchive.
    uds_rpc(
        &socket,
        3,
        "workspace.archive",
        json!({ "workspaceId": ws_id }),
    )
    .await;

    let mut sub = connect_ws(port, cfg.clone()).await;
    let sub_res = wss_rpc(
        &mut sub,
        1,
        "events.subscribe",
        json!({ "eventTypes": ["workspace:updated"], "workspaceId": ws_id }),
    )
    .await;
    assert!(sub_res["subscriptionId"].is_string(), "sub id: {sub_res}");

    let mut rpc = connect_ws(port, cfg.clone()).await;
    let unarchive_res = wss_rpc(
        &mut rpc,
        2,
        "workspace.unarchive",
        json!({ "workspaceId": ws_id }),
    )
    .await;
    // §5.1 return shape mirror of archive: `archivedAt` cleared (omitted),
    // `archived: false`, `status: "Active"`.
    assert_eq!(unarchive_res["workspace"]["id"], ws_id.as_str());
    assert_eq!(unarchive_res["workspace"]["archived"], json!(false));
    assert_eq!(unarchive_res["workspace"]["status"], json!("Active"));
    assert!(unarchive_res["workspace"].get("archivedAt").is_none());
    assert!(unarchive_res["workspace"]["lastActivity"].is_string());
    assert!(unarchive_res.get("success").is_none());

    let evt = next_event(&mut sub, &["workspace:updated"], 10).await;
    assert_eq!(evt["workspaceId"], ws_id.as_str());
    assert_eq!(
        evt["data"],
        json!({
            "workspaceId": ws_id,
            "changes": {
                "archived": false,
                "status": "Active",
                "archivedAt": null,
            }
        })
    );

    let extra = drain_extra(&mut sub, "workspace:updated", 500).await;
    assert!(
        extra.is_none(),
        "workspace.unarchive must publish exactly one workspace:updated, got extra: {extra:?}"
    );
}

/// End-to-end (Audit D H1+M1): `comment.add` over WSS persists the
/// surrounding `anchorContext` and a subsequent `note.setContent` that wipes
/// the anchor markers must flip the comment to `isOrphaned: true` (reference
/// `updateNote` failed-recovery path).
#[tokio::test]
async fn note_edit_marks_destroyed_anchor_orphaned_over_wss() {
    let (daemon, port, cfg) = boot().await;
    let socket = daemon.data_dir.join("intentd.sock");
    let create = uds_rpc(
        &socket,
        2,
        "workspace.create",
        json!({ "title": "AnchorResilience", "branch": "main", "skipWorktree": true }),
    )
    .await;
    let ws_id = create["result"]["workspace"]["id"]
        .as_str()
        .expect("workspace id")
        .to_string();
    let n = uds_rpc(
        &socket,
        3,
        "note.create",
        json!({ "workspaceId": ws_id, "title": "Note", "content": "prefix target suffix" }),
    )
    .await;
    let note_id = n["result"]["note"]["id"]
        .as_str()
        .expect("note id")
        .to_string();

    // comment.add over WSS — the response nests `anchorContext` per PROTOCOL
    // §5 comment shape, sourced from the anchor_before / anchor_after fields
    // persisted by M1.
    let mut rpc = connect_ws(port, cfg.clone()).await;
    let add = wss_rpc(
        &mut rpc,
        1,
        "comment.add",
        json!({
            "workspaceId": ws_id,
            "noteId": note_id,
            "searchContext": "prefix target suffix",
            "commentTarget": "target",
            "comment": "root",
        }),
    )
    .await;
    let comment_id = add["commentId"].as_str().expect("comment id").to_string();

    // Read the comment back and assert `anchorContext` was persisted (M1).
    let list = wss_rpc(
        &mut rpc,
        2,
        "comment.list",
        json!({
            "workspaceId": ws_id,
            "noteId": note_id,
            "includeComments": true,
        }),
    )
    .await;
    let comment = list["threads"][0]["comments"][0].clone();
    assert_eq!(comment["id"], comment_id.as_str());
    let ctx = &comment["anchorContext"];
    assert!(ctx.is_object(), "anchorContext missing: {comment}");
    assert!(
        ctx["before"]
            .as_str()
            .unwrap_or_default()
            .ends_with("prefix "),
        "unexpected before: {ctx}"
    );
    assert!(
        ctx["after"]
            .as_str()
            .unwrap_or_default()
            .starts_with(" suffix"),
        "unexpected after: {ctx}"
    );
    assert!(
        !comment["isOrphaned"].as_bool().unwrap_or(false),
        "comment should not be orphaned yet: {comment}"
    );

    // Wipe both anchor markers via note.setContent → H1 orphan path.
    wss_rpc(
        &mut rpc,
        3,
        "note.setContent",
        json!({
            "workspaceId": ws_id,
            "noteId": note_id,
            "content": "totally different content with no markers",
            "confirmReplacement": true,
        }),
    )
    .await;

    let after = wss_rpc(
        &mut rpc,
        4,
        "comment.list",
        json!({
            "workspaceId": ws_id,
            "noteId": note_id,
            "includeComments": true,
        }),
    )
    .await;
    let comment_after = after["threads"][0]["comments"][0].clone();
    assert_eq!(comment_after["id"], comment_id.as_str());
    assert_eq!(
        comment_after["isOrphaned"],
        json!(true),
        "expected orphaned after anchor destruction: {comment_after}"
    );
}

/// End-to-end self-heal for the pre-#110 global-note-identity bug over WSS:
/// a workspace whose spec content lives on a UUID note titled "Spec"
/// (because the buggy agent path called `note.create` for the spec) is
/// adopted onto the reserved `id='spec'` on the next `note.list`. The
/// adoption emits an ordered pair — `note:deleted` for the stray UUID then
/// `note:created` for `spec` — with the adopted title on both, and the
/// WSS `note.list` response carries the adopted content on `id='spec'` so
/// live FE clients replace the stale tree entry without an extra read.
#[tokio::test]
async fn note_list_adopts_stray_spec_note_over_wss() {
    let (daemon, port, cfg) = boot().await;
    let socket = daemon.data_dir.join("intentd.sock");
    let create = uds_rpc(
        &socket,
        2,
        "workspace.create",
        json!({ "title": "AdoptHeal", "branch": "main", "skipWorktree": true }),
    )
    .await;
    let ws_id = create["result"]["workspace"]["id"]
        .as_str()
        .expect("workspace id")
        .to_string();

    // Reproduce the pre-#110 damaged shape: create a top-level, non-task
    // note titled "Spec" with real content on a random UUID id, then delete
    // the seeded `id='spec'` so `ensure_spec_note` sees exactly one
    // adoption candidate on the next `note.list`.
    let stray = uds_rpc(
        &socket,
        3,
        "note.create",
        json!({
            "workspaceId": ws_id,
            "title": "Spec",
            "content": "# Real spec content\n\nkeep me",
        }),
    )
    .await;
    let stray_id = stray["result"]["note"]["id"]
        .as_str()
        .expect("stray id")
        .to_string();
    assert_ne!(stray_id, "spec", "sanity: stray must have a UUID id");
    let del = uds_rpc(
        &socket,
        4,
        "note.delete",
        json!({ "workspaceId": ws_id, "noteId": "spec" }),
    )
    .await;
    assert_eq!(del["result"]["ok"], json!(true), "delete seed: {del}");

    // Subscribe over WSS *after* setup so the adoption pair is the only
    // note:*/note:deleted traffic we see on this socket.
    let mut sub = connect_ws(port, cfg.clone()).await;
    let sub_res = wss_rpc(
        &mut sub,
        1,
        "events.subscribe",
        json!({ "eventTypes": ["note:created", "note:deleted"], "workspaceId": ws_id }),
    )
    .await;
    assert!(sub_res["subscriptionId"].is_string(), "sub id: {sub_res}");

    // Drive `note.list` over a separate WSS RPC connection; the response
    // must carry the adopted content on `id='spec'` and no longer list the
    // stray UUID.
    let mut rpc = connect_ws(port, cfg.clone()).await;
    let list = wss_rpc(&mut rpc, 2, "note.list", json!({ "workspaceId": ws_id })).await;
    let notes = list["notes"].as_array().expect("notes array");
    assert!(
        !notes.iter().any(|n| n["id"] == json!(stray_id)),
        "stray UUID note must be replaced: {list}"
    );
    let spec = notes
        .iter()
        .find(|n| n["id"] == json!("spec"))
        .expect("spec present in response");
    assert_eq!(spec["workspaceId"], json!(ws_id));
    assert_eq!(spec["title"], json!("Spec"));
    assert_eq!(spec["content"], json!("# Real spec content\n\nkeep me"));
    assert_eq!(spec["isPinned"], json!(true));
    assert_eq!(spec["isDefault"], json!(true));

    // Event ordering: `note:deleted` for the stray, then `note:created` for
    // spec. Both carry the adopted title so a subscribed FE tree replaces
    // the stale node in one pass.
    let deleted = next_event(&mut sub, &["note:deleted"], 10).await;
    assert_eq!(deleted["workspaceId"], ws_id.as_str());
    assert_eq!(deleted["data"]["noteId"], json!(stray_id));
    assert_eq!(deleted["data"]["title"], json!("Spec"));
    assert_eq!(deleted["data"]["action"], json!("delete"));
    let created = next_event(&mut sub, &["note:created"], 10).await;
    assert_eq!(created["workspaceId"], ws_id.as_str());
    assert_eq!(created["data"]["noteId"], json!("spec"));
    assert_eq!(created["data"]["title"], json!("Spec"));
    assert_eq!(created["data"]["action"], json!("create"));

    // Adoption is one-shot: no additional note:deleted / note:created on a
    // second `note.list`.
    let _ = wss_rpc(&mut rpc, 3, "note.list", json!({ "workspaceId": ws_id })).await;
    assert!(
        drain_extra(&mut sub, "note:deleted", 400).await.is_none(),
        "adoption must not republish note:deleted on re-list"
    );
    assert!(
        drain_extra(&mut sub, "note:created", 400).await.is_none(),
        "adoption must not republish note:created on re-list"
    );
}

/// End-to-end: `comment.add` whose `searchContext`/`commentTarget` come from
/// the editor's *plain text* (markdown syntax stripped, blocks joined with no
/// separator — the FE's `doc.textBetween` shape) anchors successfully against
/// the formatted markdown source via the plaintext-tolerant fallback.
#[tokio::test]
async fn comment_add_plaintext_context_anchors_over_wss() {
    let (daemon, port, cfg) = boot().await;
    let socket = daemon.data_dir.join("intentd.sock");
    let create = uds_rpc(
        &socket,
        2,
        "workspace.create",
        json!({ "title": "PlainAnchors", "branch": "main", "skipWorktree": true }),
    )
    .await;
    let ws_id = create["result"]["workspace"]["id"]
        .as_str()
        .expect("workspace id")
        .to_string();
    let n = uds_rpc(
        &socket,
        3,
        "note.create",
        json!({
            "workspaceId": ws_id,
            "title": "Note",
            "content": "## Heading\n\nSome **bold words** in a [link](https://example.com) here.",
        }),
    )
    .await;
    let note_id = n["result"]["note"]["id"]
        .as_str()
        .expect("note id")
        .to_string();

    let mut rpc = connect_ws(port, cfg.clone()).await;
    let add = wss_rpc(
        &mut rpc,
        1,
        "comment.add",
        json!({
            "workspaceId": ws_id,
            "noteId": note_id,
            // Plain text of the doc: heading joined to the paragraph with no
            // separator, bold/link markdown stripped.
            "searchContext": "HeadingSome bold words in a link here.",
            "commentTarget": "bold words",
            "comment": "needs review",
        }),
    )
    .await;
    assert_eq!(add["success"], json!(true));
    assert_eq!(add["anchored"], json!(true));
    assert_eq!(add["location"]["anchoredText"], json!("bold words"));
    let comment_id = add["commentId"].as_str().expect("comment id").to_string();

    // The anchor markers landed around the markdown-source text.
    let read = wss_rpc(
        &mut rpc,
        2,
        "note.get",
        json!({ "workspaceId": ws_id, "noteId": note_id }),
    )
    .await;
    let content = read["note"]["content"].as_str().expect("content");
    assert!(
        content.contains(&format!(
            "<!--anchor:{comment_id}:start-->bold words<!--anchor:{comment_id}:end-->"
        )),
        "anchor markers missing: {content}"
    );
}

/// End-to-end: `comment.add` from a *stale* editor doc — the note gained a
/// paragraph on the server after the editor loaded it, so the ±50-char
/// `searchContext` includes text that no longer neighbors the selection. The
/// target-rescue path still anchors the (unique) `commentTarget`.
#[tokio::test]
async fn comment_add_stale_context_target_rescue_over_wss() {
    let (daemon, port, cfg) = boot().await;
    let socket = daemon.data_dir.join("intentd.sock");
    let create = uds_rpc(
        &socket,
        2,
        "workspace.create",
        json!({ "title": "StaleAnchors", "branch": "main", "skipWorktree": true }),
    )
    .await;
    let ws_id = create["result"]["workspace"]["id"]
        .as_str()
        .expect("workspace id")
        .to_string();
    let n = uds_rpc(
        &socket,
        3,
        "note.create",
        json!({
            "workspaceId": ws_id,
            "title": "Note",
            // Server copy already has the "Status" paragraph inserted between
            // Goal and Diagnosis; the client context below predates it.
            "content": "## Goal\nOld goal paragraph tail.\n\n**Status:** new paragraph the editor never saw.\n\n## Diagnosis\n\n**Symptom:** things broke badly.",
        }),
    )
    .await;
    let note_id = n["result"]["note"]["id"]
        .as_str()
        .expect("note id")
        .to_string();

    let mut rpc = connect_ws(port, cfg.clone()).await;
    let add = wss_rpc(
        &mut rpc,
        1,
        "comment.add",
        json!({
            "workspaceId": ws_id,
            "noteId": note_id,
            // Stale plain-text context: Goal tail joined directly to the
            // Diagnosis heading (the Status paragraph is missing).
            "searchContext": "Old goal paragraph tail.DiagnosisSymptom: things broke badly.",
            "commentTarget": "DiagnosisSymptom: things broke",
            "comment": "cross-block comment",
        }),
    )
    .await;
    assert_eq!(add["success"], json!(true), "response: {add}");
    assert_eq!(add["anchored"], json!(true));
    let comment_id = add["commentId"].as_str().expect("comment id").to_string();

    let read = wss_rpc(
        &mut rpc,
        2,
        "note.get",
        json!({ "workspaceId": ws_id, "noteId": note_id }),
    )
    .await;
    let content = read["note"]["content"].as_str().expect("content");
    assert!(
        content.contains(&format!("<!--anchor:{comment_id}:start-->Diagnosis")),
        "start marker missing before the heading text: {content}"
    );
    assert!(
        content.contains(&format!("things broke<!--anchor:{comment_id}:end-->")),
        "end marker missing after the target text: {content}"
    );
}

/// End-to-end: a `comment.add` whose context cannot be found returns the
/// actionable `-32602` error with the descriptive message (not an opaque
/// `-32603 "Internal error"`).
#[tokio::test]
async fn comment_add_context_not_found_returns_invalid_params_over_wss() {
    let (daemon, port, cfg) = boot().await;
    let socket = daemon.data_dir.join("intentd.sock");
    let create = uds_rpc(
        &socket,
        2,
        "workspace.create",
        json!({ "title": "AnchorErrors", "branch": "main", "skipWorktree": true }),
    )
    .await;
    let ws_id = create["result"]["workspace"]["id"]
        .as_str()
        .expect("workspace id")
        .to_string();
    let n = uds_rpc(
        &socket,
        3,
        "note.create",
        json!({ "workspaceId": ws_id, "title": "Note", "content": "some note content" }),
    )
    .await;
    let note_id = n["result"]["note"]["id"]
        .as_str()
        .expect("note id")
        .to_string();

    let mut rpc = connect_ws(port, cfg.clone()).await;
    let v = wss_rpc_envelope(
        &mut rpc,
        1,
        "comment.add",
        json!({
            "workspaceId": ws_id,
            "noteId": note_id,
            "searchContext": "totally absent context",
            "commentTarget": "absent",
            "comment": "c",
        }),
    )
    .await;
    assert_eq!(v["error"]["code"], json!(-32602), "envelope: {v}");
    assert_eq!(
        v["error"]["message"],
        json!("invalid params: Could not find the search context in the document."),
        "envelope: {v}"
    );
}

/// End-to-end (Round 14 root cause A): a client-supplied `commentId` is used
/// for the comment row, the thread id, and the embedded anchor markers, so
/// the FE's optimistic anchors converge with the daemon's rewrite.
#[tokio::test]
async fn comment_add_supplied_comment_id_round_trips_over_wss() {
    let (daemon, port, cfg) = boot().await;
    let socket = daemon.data_dir.join("intentd.sock");
    let create = uds_rpc(
        &socket,
        2,
        "workspace.create",
        json!({ "title": "ClientCommentId", "branch": "main", "skipWorktree": true }),
    )
    .await;
    let ws_id = create["result"]["workspace"]["id"]
        .as_str()
        .expect("workspace id")
        .to_string();
    let n = uds_rpc(
        &socket,
        3,
        "note.create",
        json!({ "workspaceId": ws_id, "title": "Note", "content": "anchor target text" }),
    )
    .await;
    let note_id = n["result"]["note"]["id"]
        .as_str()
        .expect("note id")
        .to_string();

    let supplied = "0f1e2d3c-4b5a-6978-8796-a5b4c3d2e1f0";
    let mut rpc = connect_ws(port, cfg.clone()).await;
    let add = wss_rpc(
        &mut rpc,
        1,
        "comment.add",
        json!({
            "workspaceId": ws_id,
            "noteId": note_id,
            "searchContext": "anchor target text",
            "commentTarget": "target",
            "comment": "root",
            "commentId": supplied,
        }),
    )
    .await;
    assert_eq!(add["success"], json!(true), "add: {add}");
    assert_eq!(add["commentId"], json!(supplied), "add: {add}");

    // The rewrite embedded the SUPPLIED id in the markers.
    let read = wss_rpc(
        &mut rpc,
        2,
        "note.get",
        json!({ "workspaceId": ws_id, "noteId": note_id }),
    )
    .await;
    let content = read["note"]["content"].as_str().expect("content");
    assert!(
        content.contains(&format!(
            "<!--anchor:{supplied}:start-->target<!--anchor:{supplied}:end-->"
        )),
        "markers must embed the supplied id: {content}"
    );

    // The stored row + thread use the supplied id too.
    let thread = wss_rpc(
        &mut rpc,
        3,
        "comment.getThread",
        json!({ "workspaceId": ws_id, "noteId": note_id, "commentId": supplied }),
    )
    .await;
    assert_eq!(thread["threadId"], json!(supplied), "thread: {thread}");
    assert_eq!(thread["rootComment"]["id"], json!(supplied));

    // A duplicate supplied id (fresh idempotency scope) is rejected -32602.
    let dup = wss_rpc_envelope(
        &mut rpc,
        4,
        "comment.add",
        json!({
            "workspaceId": ws_id,
            "noteId": note_id,
            "searchContext": "anchor target text",
            "commentTarget": "anchor",
            "comment": "again",
            "commentId": supplied,
        }),
    )
    .await;
    assert_eq!(dup["error"]["code"], json!(-32602), "envelope: {dup}");
    assert!(
        dup["error"]["message"]
            .as_str()
            .expect("message")
            .contains("commentId"),
        "envelope: {dup}"
    );
}

/// End-to-end: a malformed `commentId` returns `-32602` naming the param.
#[tokio::test]
async fn comment_add_invalid_comment_id_returns_invalid_params_over_wss() {
    let (daemon, port, cfg) = boot().await;
    let socket = daemon.data_dir.join("intentd.sock");
    let create = uds_rpc(
        &socket,
        2,
        "workspace.create",
        json!({ "title": "BadCommentId", "branch": "main", "skipWorktree": true }),
    )
    .await;
    let ws_id = create["result"]["workspace"]["id"]
        .as_str()
        .expect("workspace id")
        .to_string();
    let n = uds_rpc(
        &socket,
        3,
        "note.create",
        json!({ "workspaceId": ws_id, "title": "Note", "content": "some note content" }),
    )
    .await;
    let note_id = n["result"]["note"]["id"]
        .as_str()
        .expect("note id")
        .to_string();

    let mut rpc = connect_ws(port, cfg.clone()).await;
    let v = wss_rpc_envelope(
        &mut rpc,
        1,
        "comment.add",
        json!({
            "workspaceId": ws_id,
            "noteId": note_id,
            "searchContext": "some note content",
            "commentTarget": "content",
            "comment": "c",
            "commentId": "not-a-uuid",
        }),
    )
    .await;
    assert_eq!(v["error"]["code"], json!(-32602), "envelope: {v}");
    assert_eq!(
        v["error"]["message"],
        json!(
            "invalid params: Invalid 'commentId': not-a-uuid. Must be a canonical hyphenated UUID."
        ),
        "envelope: {v}"
    );
}

/// End-to-end: `comment.add` with `authorType: "user"` persists the author
/// type and round-trips it through `comment.list`; omitting the param keeps
/// the backward-compatible `agent` default.
#[tokio::test]
async fn comment_add_author_type_round_trips_over_wss() {
    let (daemon, port, cfg) = boot().await;
    let socket = daemon.data_dir.join("intentd.sock");
    let create = uds_rpc(
        &socket,
        2,
        "workspace.create",
        json!({ "title": "AuthorType", "branch": "main", "skipWorktree": true }),
    )
    .await;
    let ws_id = create["result"]["workspace"]["id"]
        .as_str()
        .expect("workspace id")
        .to_string();
    let n = uds_rpc(
        &socket,
        3,
        "note.create",
        json!({ "workspaceId": ws_id, "title": "Note", "content": "alpha target-a and target-b omega" }),
    )
    .await;
    let note_id = n["result"]["note"]["id"]
        .as_str()
        .expect("note id")
        .to_string();

    let mut rpc = connect_ws(port, cfg.clone()).await;
    let user_add = wss_rpc(
        &mut rpc,
        1,
        "comment.add",
        json!({
            "workspaceId": ws_id,
            "noteId": note_id,
            "searchContext": "alpha target-a and",
            "commentTarget": "target-a",
            "comment": "from the user",
            "authorType": "user",
        }),
    )
    .await;
    let user_comment_id = user_add["commentId"].as_str().expect("id").to_string();

    let agent_add = wss_rpc(
        &mut rpc,
        2,
        "comment.add",
        json!({
            "workspaceId": ws_id,
            "noteId": note_id,
            "searchContext": "and target-b omega",
            "commentTarget": "target-b",
            "comment": "from an agent",
        }),
    )
    .await;
    let agent_comment_id = agent_add["commentId"].as_str().expect("id").to_string();

    let list = wss_rpc(
        &mut rpc,
        3,
        "comment.list",
        json!({ "workspaceId": ws_id, "noteId": note_id, "includeComments": true }),
    )
    .await;
    let comments: Vec<Value> = list["threads"]
        .as_array()
        .expect("threads")
        .iter()
        .flat_map(|t| t["comments"].as_array().cloned().unwrap_or_default())
        .collect();
    let by_id = |id: &str| {
        comments
            .iter()
            .find(|c| c["id"] == json!(id))
            .unwrap_or_else(|| panic!("comment {id} missing: {list}"))
    };
    let user_comment = by_id(&user_comment_id);
    assert_eq!(user_comment["authorType"], json!("user"), "{user_comment}");
    assert_eq!(user_comment["author"], json!("User"), "{user_comment}");
    let agent_comment = by_id(&agent_comment_id);
    assert_eq!(
        agent_comment["authorType"],
        json!("agent"),
        "{agent_comment}"
    );

    // Invalid authorType is rejected with -32602.
    let bad = wss_rpc_envelope(
        &mut rpc,
        4,
        "comment.add",
        json!({
            "workspaceId": ws_id,
            "noteId": note_id,
            "searchContext": "alpha target-a and",
            "commentTarget": "target-a",
            "comment": "c",
            "authorType": "robot",
        }),
    )
    .await;
    assert_eq!(bad["error"]["code"], json!(-32602), "envelope: {bad}");
}

/// End-to-end: `comment.respond` with `authorType: "user"` persists the
/// author type and round-trips it through `comment.getThread`; omitting the
/// param keeps the backward-compatible `agent` default.
#[tokio::test]
async fn comment_respond_author_type_round_trips_over_wss() {
    let (daemon, port, cfg) = boot().await;
    let socket = daemon.data_dir.join("intentd.sock");
    let create = uds_rpc(
        &socket,
        2,
        "workspace.create",
        json!({ "title": "RespondAuthorType", "branch": "main", "skipWorktree": true }),
    )
    .await;
    let ws_id = create["result"]["workspace"]["id"]
        .as_str()
        .expect("workspace id")
        .to_string();
    let n = uds_rpc(
        &socket,
        3,
        "note.create",
        json!({ "workspaceId": ws_id, "title": "Note", "content": "alpha reply-target omega" }),
    )
    .await;
    let note_id = n["result"]["note"]["id"]
        .as_str()
        .expect("note id")
        .to_string();

    let mut rpc = connect_ws(port, cfg.clone()).await;
    let add = wss_rpc(
        &mut rpc,
        1,
        "comment.add",
        json!({
            "workspaceId": ws_id,
            "noteId": note_id,
            "searchContext": "alpha reply-target omega",
            "commentTarget": "reply-target",
            "comment": "root",
        }),
    )
    .await;
    let root_id = add["commentId"].as_str().expect("id").to_string();

    let user_reply = wss_rpc(
        &mut rpc,
        2,
        "comment.respond",
        json!({
            "workspaceId": ws_id,
            "noteId": note_id,
            "commentId": root_id,
            "comment": "hi from the user",
            "authorType": "user",
        }),
    )
    .await;
    let user_reply_id = user_reply["comment"]["id"]
        .as_str()
        .expect("reply id")
        .to_string();

    let agent_reply = wss_rpc(
        &mut rpc,
        3,
        "comment.respond",
        json!({
            "workspaceId": ws_id,
            "noteId": note_id,
            "commentId": root_id,
            "comment": "hi from an agent",
        }),
    )
    .await;
    let agent_reply_id = agent_reply["comment"]["id"]
        .as_str()
        .expect("reply id")
        .to_string();

    let thread = wss_rpc(
        &mut rpc,
        4,
        "comment.getThread",
        json!({ "workspaceId": ws_id, "noteId": note_id, "commentId": root_id }),
    )
    .await;
    let replies = thread["replies"].as_array().expect("replies");
    let by_id = |id: &str| {
        replies
            .iter()
            .find(|c| c["id"] == json!(id))
            .unwrap_or_else(|| panic!("reply {id} missing: {thread}"))
    };
    let user = by_id(&user_reply_id);
    assert_eq!(user["authorType"], json!("user"), "{user}");
    assert_eq!(user["author"], json!("User"), "{user}");
    let agent = by_id(&agent_reply_id);
    assert_eq!(agent["authorType"], json!("agent"), "{agent}");
    assert_eq!(agent["author"], json!("Agent"), "{agent}");

    // Invalid authorType is rejected with -32602.
    let bad = wss_rpc_envelope(
        &mut rpc,
        5,
        "comment.respond",
        json!({
            "workspaceId": ws_id,
            "noteId": note_id,
            "commentId": root_id,
            "comment": "c",
            "authorType": "robot",
        }),
    )
    .await;
    assert_eq!(bad["error"]["code"], json!(-32602), "envelope: {bad}");

    // The pre-existing caller-input validations also reject with -32602, not
    // -32603 (monorepo#632): missing threadId/commentId, empty comment, and a
    // suggestion without the suggestionOriginal/suggestionProposed pair.
    let bad = wss_rpc_envelope(
        &mut rpc,
        6,
        "comment.respond",
        json!({
            "workspaceId": ws_id,
            "noteId": note_id,
            "comment": "no target",
        }),
    )
    .await;
    assert_eq!(bad["error"]["code"], json!(-32602), "envelope: {bad}");

    let bad = wss_rpc_envelope(
        &mut rpc,
        7,
        "comment.respond",
        json!({
            "workspaceId": ws_id,
            "noteId": note_id,
            "commentId": root_id,
            "comment": "   ",
        }),
    )
    .await;
    assert_eq!(bad["error"]["code"], json!(-32602), "envelope: {bad}");

    let bad = wss_rpc_envelope(
        &mut rpc,
        8,
        "comment.respond",
        json!({
            "workspaceId": ws_id,
            "noteId": note_id,
            "commentId": root_id,
            "comment": "try this",
            "type": "suggestion",
            "suggestionOriginal": "only original",
        }),
    )
    .await;
    assert_eq!(bad["error"]["code"], json!(-32602), "envelope: {bad}");

    // The sibling caller-input validations on `comment.getThread` /
    // `comment.resolveThread` (missing threadId AND commentId) and the
    // `comment.list` filter checks also reject with -32602, not -32603
    // (monorepo#649).
    let bad = wss_rpc_envelope(
        &mut rpc,
        9,
        "comment.getThread",
        json!({ "workspaceId": ws_id, "noteId": note_id }),
    )
    .await;
    assert_eq!(bad["error"]["code"], json!(-32602), "envelope: {bad}");

    let bad = wss_rpc_envelope(
        &mut rpc,
        10,
        "comment.resolveThread",
        json!({ "workspaceId": ws_id, "noteId": note_id, "resolved": true }),
    )
    .await;
    assert_eq!(bad["error"]["code"], json!(-32602), "envelope: {bad}");

    let bad = wss_rpc_envelope(
        &mut rpc,
        11,
        "comment.list",
        json!({ "workspaceId": ws_id, "noteId": note_id, "authorType": "robot" }),
    )
    .await;
    assert_eq!(bad["error"]["code"], json!(-32602), "envelope: {bad}");
}

/// Wait up to `secs` for the next `subscription.push` notification on `ws`;
/// ignore other frames (heartbeats, unrelated notifications). Returns the
/// `params` sub-object (`{ subscriptionId, kind, seq, snapshot|delta }`).
async fn next_subscription_push<S>(ws: &mut WebSocketStream<S>, secs: u64) -> Value
where
    S: tokio::io::AsyncRead + tokio::io::AsyncWrite + Unpin,
{
    let deadline = tokio::time::Instant::now() + Duration::from_secs(secs);
    loop {
        let remaining = deadline.saturating_duration_since(tokio::time::Instant::now());
        assert!(
            !remaining.is_zero(),
            "timed out waiting for subscription.push"
        );
        let next = timeout(remaining, ws.next())
            .await
            .expect("timeout elapsed");
        match next {
            Some(Ok(Message::Text(text))) => {
                let v: Value = match serde_json::from_str(&text) {
                    Ok(x) => x,
                    Err(_) => continue,
                };
                if v["method"] == json!("subscription.push") {
                    return v["params"].clone();
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

/// End-to-end (intent-hq/monorepo#775): the `workspace.subscribe` seq-0
/// snapshot over WSS includes archived workspaces with their `Archived`
/// status — matching the deltas, which upsert archived workspaces. Mirrors
/// the UDS assertion in
/// `uds_channel_subscriptions::workspace_channel_snapshot_then_updated_delta`;
/// the channel logic is transport-shared, but the WSS wire path (§3.3
/// `subscription.push` after the `{ subscriptionId }` response on the same
/// connection) is the contract production clients drive.
#[tokio::test]
async fn workspace_subscribe_snapshot_includes_archived_over_wss() {
    let (daemon, port, cfg) = boot().await;

    // Seed one active and one archived workspace off UDS so the WSS side
    // drives only the subscribe.
    let socket = daemon.data_dir.join("intentd.sock");
    let create = uds_rpc(
        &socket,
        2,
        "workspace.create",
        json!({ "title": "Active WS", "branch": "main", "skipWorktree": true }),
    )
    .await;
    let active_id = create["result"]["workspace"]["id"]
        .as_str()
        .expect("active workspace id")
        .to_string();
    let create = uds_rpc(
        &socket,
        3,
        "workspace.create",
        json!({ "title": "Archived WS", "branch": "main", "skipWorktree": true }),
    )
    .await;
    let archived_id = create["result"]["workspace"]["id"]
        .as_str()
        .expect("archived workspace id")
        .to_string();
    uds_rpc(
        &socket,
        4,
        "workspace.archive",
        json!({ "workspaceId": archived_id }),
    )
    .await;

    // Subscribe over WSS. The workspace channel is global (no `workspaceId`
    // param): `{ subscriptionId }` response first, then the seq-0 snapshot
    // push on the same connection (§3.4 ordering).
    let mut sub = connect_ws(port, cfg.clone()).await;
    let sub_res = wss_rpc(&mut sub, 1, "workspace.subscribe", json!({})).await;
    let sub_id = sub_res["subscriptionId"]
        .as_str()
        .expect("subscriptionId")
        .to_string();

    let push = next_subscription_push(&mut sub, 10).await;
    assert_eq!(push["subscriptionId"], sub_id.as_str(), "push: {push}");
    assert_eq!(push["kind"], json!("snapshot"), "push: {push}");
    assert_eq!(push["seq"], json!(0), "push: {push}");
    let snap = push["snapshot"].as_array().expect("snapshot array");
    let active = snap
        .iter()
        .find(|e| e["id"] == json!(active_id))
        .expect("active workspace in snapshot");
    assert_eq!(active["status"], json!("Active"), "active: {active}");
    let archived = snap
        .iter()
        .find(|e| e["id"] == json!(archived_id))
        .expect("archived workspace in snapshot (intent-hq/monorepo#775)");
    assert_eq!(
        archived["status"],
        json!("Archived"),
        "snapshot includes archived workspaces with their status: {archived}"
    );
}

/// End-to-end `task.setRelations` over WSS (docs/protocol/methods/notes-tasks.md §5.4): relation writes
/// round-trip `dependsOn`/`conflictsWith` (echoed normalized in the result and
/// visible in `task.getMyTask` / `task.list` with the computed
/// `unmetDependsOn`), emit `note:updated` for subscriber refetch, and reject
/// invalid writes — a `dependsOn` closing a cycle (error names the cycle
/// path), self-edges, and non-task ids. `task.markAsTask` accepts the same
/// relation params.
#[tokio::test]
async fn task_set_relations_round_trip_and_cycle_rejection_over_wss() {
    let (daemon, port, cfg) = boot().await;

    let socket = daemon.data_dir.join("intentd.sock");
    let create = uds_rpc(
        &socket,
        2,
        "workspace.create",
        json!({ "title": "Relations", "branch": "main", "skipWorktree": true }),
    )
    .await;
    let ws_id = create["result"]["workspace"]["id"]
        .as_str()
        .expect("workspace id")
        .to_string();

    let mut sub = connect_ws(port, cfg.clone()).await;
    let sub_res = wss_rpc(
        &mut sub,
        1,
        "events.subscribe",
        json!({ "eventTypes": ["note:updated"], "workspaceId": ws_id }),
    )
    .await;
    assert!(sub_res["subscriptionId"].is_string(), "sub id: {sub_res}");

    let mut rpc = connect_ws(port, cfg.clone()).await;

    // Three spec-child task notes: a (complete), b (in_progress), c.
    let mut task_ids = Vec::new();
    for (i, (title, status)) in [
        ("Alpha", "complete"),
        ("Beta", "in_progress"),
        ("Gamma", "not_started"),
    ]
    .iter()
    .enumerate()
    {
        let id = i64::try_from(i).expect("value fits in i64") * 2 + 2;
        let created = wss_rpc(
            &mut rpc,
            id,
            "note.create",
            json!({ "workspaceId": ws_id, "title": title, "content": "body", "parentId": "spec" }),
        )
        .await;
        let note_id = created["note"]["id"].as_str().expect("note id").to_string();
        wss_rpc(
            &mut rpc,
            id + 1,
            "task.markAsTask",
            json!({ "workspaceId": ws_id, "noteId": note_id, "status": status }),
        )
        .await;
        task_ids.push(note_id);
    }
    let (a, b, c) = (
        task_ids[0].clone(),
        task_ids[1].clone(),
        task_ids[2].clone(),
    );

    // Drain the setup emissions (each markAsTask queued a `note:updated`,
    // ending with c's) so the next `note:updated` for c observed below is
    // unambiguously the `task.setRelations` write's.
    loop {
        let evt = next_event(&mut sub, &["note:updated"], 10).await;
        if evt["data"]["noteId"] == json!(c) {
            break;
        }
    }

    // Write relations on c: dependsOn [a, b, a] (dedup) + conflictsWith [b].
    let set = wss_rpc(
        &mut rpc,
        10,
        "task.setRelations",
        json!({
            "workspaceId": ws_id,
            "noteId": c,
            "dependsOn": [a, b, a],
            "conflictsWith": [b],
        }),
    )
    .await;
    assert_eq!(set["ok"], json!(true), "setRelations: {set}");
    assert_eq!(set["noteId"], json!(c));
    assert_eq!(set["dependsOn"], json!([a, b]), "deduped: {set}");
    assert_eq!(set["conflictsWith"], json!([b]));

    // Relation write emits `note:updated` for the task note (§6.5). The
    // setup emissions were drained above, so this is the write's own event.
    let evt = next_event(&mut sub, &["note:updated"], 10).await;
    assert_eq!(
        evt["data"]["noteId"],
        json!(c),
        "setRelations note:updated: {evt}"
    );

    // getMyTask carries the stored relations + computed unmetDependsOn (the
    // complete dep `a` is met, `b` is not).
    let mine = wss_rpc(
        &mut rpc,
        11,
        "task.getMyTask",
        json!({ "workspaceId": ws_id, "taskNoteId": c }),
    )
    .await;
    assert_eq!(mine["taskMetadata"]["dependsOn"], json!([a, b]));
    assert_eq!(mine["taskMetadata"]["conflictsWith"], json!([b]));
    assert_eq!(mine["unmetDependsOn"], json!([b]), "getMyTask: {mine}");

    // task.list rows project the same fields. Membership is workspace-wide,
    // so the notes above (children of `spec` but unlinked from its body) are
    // returned with `specLinked: false`.
    let listed = wss_rpc(&mut rpc, 12, "task.list", json!({ "workspaceId": ws_id })).await;
    let row = listed["tasks"]
        .as_array()
        .expect("tasks array")
        .iter()
        .find(|t| t["id"] == json!(c))
        .expect("task c row")
        .clone();
    assert_eq!(row["dependsOn"], json!([a, b]), "task.list row: {row}");
    assert_eq!(row["conflictsWith"], json!([b]));
    assert_eq!(row["unmetDependsOn"], json!([b]));
    assert_eq!(row["specLinked"], false, "unlinked from spec body: {row}");
    // Relation-less rows omit the fields entirely (additive wire shape).
    let row_a = listed["tasks"]
        .as_array()
        .unwrap()
        .iter()
        .find(|t| t["id"] == json!(a))
        .expect("task a row");
    assert!(row_a.get("dependsOn").is_none(), "omitted: {row_a}");

    // Closing the cycle b → c → b is rejected with the cycle path named.
    let err = wss_rpc_envelope(
        &mut rpc,
        13,
        "task.setRelations",
        json!({ "workspaceId": ws_id, "noteId": b, "dependsOn": [c] }),
    )
    .await;
    assert_eq!(err["error"]["code"], json!(-32603), "cycle: {err}");
    let detail = err["error"]["data"].as_str().expect("cycle error detail");
    assert!(detail.contains("cycle"), "cycle rejection: {err}");
    assert!(
        detail.contains(&format!("{b} -> {c} -> {b}")),
        "cycle path named: {detail}"
    );

    // Self-edge and non-task ids are rejected.
    let err = wss_rpc_envelope(
        &mut rpc,
        14,
        "task.setRelations",
        json!({ "workspaceId": ws_id, "noteId": c, "dependsOn": [c] }),
    )
    .await;
    assert!(
        err["error"]["data"]
            .as_str()
            .unwrap_or("")
            .contains("cannot reference the task itself"),
        "self-edge: {err}"
    );
    let err = wss_rpc_envelope(
        &mut rpc,
        15,
        "task.setRelations",
        json!({ "workspaceId": ws_id, "noteId": c, "conflictsWith": ["ghost"] }),
    )
    .await;
    assert!(
        err["error"]["data"]
            .as_str()
            .unwrap_or("")
            .contains("not a task note"),
        "non-task id: {err}"
    );

    // markAsTask accepts the relation params on an existing task (b dependsOn
    // a is acyclic) and getMyTask reflects them.
    wss_rpc(
        &mut rpc,
        16,
        "task.markAsTask",
        json!({
            "workspaceId": ws_id,
            "noteId": b,
            "status": "in_progress",
            "dependsOn": [a],
        }),
    )
    .await;
    let mine = wss_rpc(
        &mut rpc,
        17,
        "task.getMyTask",
        json!({ "workspaceId": ws_id, "taskNoteId": b }),
    )
    .await;
    assert_eq!(mine["taskMetadata"]["dependsOn"], json!([a]));
    assert!(
        mine.get("unmetDependsOn").is_none(),
        "complete dep is met (field omitted): {mine}"
    );
}

/// End-to-end `task.subscribe` `specLinked` enrichment over WSS: the seq-0
/// snapshot rows and the delta `added`/`updated` rows carry the additive
/// `specLinked` flag with the same semantics as `task.list` (§5.4) — true iff
/// the task id appears in the spec body's `intent://local/task/{id}` links —
/// so a live task update no longer drops the flag until the next `task.list`
/// refetch. Cost contract: the snapshot derives the flag from its own
/// `note.list` read; each delta adds exactly ONE bounded spec-note read.
#[tokio::test]
async fn task_subscribe_snapshot_and_deltas_carry_spec_linked_over_wss() {
    let (daemon, port, cfg) = boot().await;

    let socket = daemon.data_dir.join("intentd.sock");
    let create = uds_rpc(
        &socket,
        2,
        "workspace.create",
        json!({ "title": "SpecLinked", "branch": "main", "skipWorktree": true }),
    )
    .await;
    let ws_id = create["result"]["workspace"]["id"]
        .as_str()
        .expect("workspace id")
        .to_string();

    // One linked task, authored before subscribing: create + markAsTask, then
    // link it from the spec body.
    let mut rpc = connect_ws(port, cfg.clone()).await;
    let created = wss_rpc(
        &mut rpc,
        2,
        "note.create",
        json!({ "workspaceId": ws_id, "title": "Linked", "content": "body" }),
    )
    .await;
    let linked_id = created["note"]["id"].as_str().expect("note id").to_string();
    wss_rpc(
        &mut rpc,
        3,
        "task.markAsTask",
        json!({ "workspaceId": ws_id, "noteId": linked_id, "status": "not_started" }),
    )
    .await;
    wss_rpc(
        &mut rpc,
        4,
        "note.setContent",
        json!({
            "workspaceId": ws_id,
            "noteId": "spec",
            "content": format!("- [ ] [Linked](intent://local/task/{linked_id})"),
            "confirmReplacement": true,
        }),
    )
    .await;

    // Snapshot (seq 0): the linked task's row carries `specLinked: true`.
    let mut sub = connect_ws(port, cfg.clone()).await;
    let sub_res = wss_rpc(
        &mut sub,
        1,
        "task.subscribe",
        json!({ "workspaceId": ws_id }),
    )
    .await;
    assert!(sub_res["subscriptionId"].is_string(), "sub id: {sub_res}");
    let push = next_subscription_push(&mut sub, 10).await;
    assert_eq!(push["kind"], json!("snapshot"), "push: {push}");
    let snap = push["snapshot"].as_array().expect("snapshot array");
    let row = snap
        .iter()
        .find(|t| t["id"] == json!(linked_id))
        .expect("linked task in snapshot");
    assert_eq!(row["specLinked"], true, "snapshot row: {row}");

    // A second task, unlinked from the spec body: its markAsTask promotion
    // lands as an `updated` delta carrying `specLinked: false`.
    let created = wss_rpc(
        &mut rpc,
        5,
        "note.create",
        json!({ "workspaceId": ws_id, "title": "Unlinked", "content": "body" }),
    )
    .await;
    let unlinked_id = created["note"]["id"].as_str().expect("note id").to_string();
    wss_rpc(
        &mut rpc,
        6,
        "task.markAsTask",
        json!({ "workspaceId": ws_id, "noteId": unlinked_id, "status": "not_started" }),
    )
    .await;
    let unlinked_row = 'outer: loop {
        let push = next_subscription_push(&mut sub, 10).await;
        assert_eq!(push["kind"], json!("delta"), "push: {push}");
        for key in ["added", "updated"] {
            if let Some(rows) = push["delta"][key].as_array() {
                for entry in rows {
                    if entry["id"] == json!(unlinked_id) {
                        break 'outer entry.clone();
                    }
                }
            }
        }
    };
    assert_eq!(
        unlinked_row["specLinked"], false,
        "unlinked delta row: {unlinked_row}"
    );

    // A status update on the linked task keeps the flag live on the `updated`
    // delta — the gap this enrichment closes.
    wss_rpc(
        &mut rpc,
        7,
        "task.updateNoteStatus",
        json!({ "workspaceId": ws_id, "noteId": linked_id, "status": "in_progress" }),
    )
    .await;
    let linked_row = 'outer: loop {
        let push = next_subscription_push(&mut sub, 10).await;
        if let Some(rows) = push["delta"]["updated"].as_array() {
            for entry in rows {
                if entry["id"] == json!(linked_id) {
                    break 'outer entry.clone();
                }
            }
        }
    };
    assert_eq!(
        linked_row["metadata"]["task"]["status"], "in_progress",
        "row: {linked_row}"
    );
    assert_eq!(
        linked_row["specLinked"], true,
        "linked delta row: {linked_row}"
    );

    // A spec-body edit that flips linkage both ways — dropping the linked
    // task's link and adding the unlinked one — emits ONE delta with
    // `updated` rows for exactly the flipped tasks (monorepo#2407): the
    // subscriber no longer holds stale `specLinked` flags until the tasks'
    // own next events.
    wss_rpc(
        &mut rpc,
        8,
        "note.setContent",
        json!({
            "workspaceId": ws_id,
            "noteId": "spec",
            "content": format!("- [ ] [Unlinked](intent://local/task/{unlinked_id})"),
            "confirmReplacement": true,
        }),
    )
    .await;
    let push = next_subscription_push(&mut sub, 10).await;
    assert_eq!(push["kind"], json!("delta"), "push: {push}");
    let rows = push["delta"]["updated"].as_array().expect("updated rows");
    assert_eq!(rows.len(), 2, "exactly the flipped tasks: {push}");
    let by_id = |id: &str| {
        rows.iter()
            .find(|r| r["id"] == json!(id))
            .unwrap_or_else(|| panic!("row for {id}: {push}"))
    };
    assert_eq!(by_id(&linked_id)["specLinked"], false, "push: {push}");
    assert_eq!(by_id(&unlinked_id)["specLinked"], true, "push: {push}");

    // A spec edit that leaves the link set unchanged emits no task delta at
    // all: the next push is the status update driven afterwards, not a
    // spec-edit artifact.
    wss_rpc(
        &mut rpc,
        9,
        "note.setContent",
        json!({
            "workspaceId": ws_id,
            "noteId": "spec",
            "content": format!("edited body\n- [ ] [Unlinked](intent://local/task/{unlinked_id})"),
            "confirmReplacement": true,
        }),
    )
    .await;
    wss_rpc(
        &mut rpc,
        10,
        "task.updateNoteStatus",
        json!({ "workspaceId": ws_id, "noteId": unlinked_id, "status": "in_progress" }),
    )
    .await;
    let push = next_subscription_push(&mut sub, 10).await;
    let rows = push["delta"]["updated"].as_array().expect("updated rows");
    assert_eq!(rows.len(), 1, "no spec-edit artifact push: {push}");
    assert_eq!(rows[0]["id"], json!(unlinked_id), "push: {push}");
    assert_eq!(
        rows[0]["metadata"]["task"]["status"], "in_progress",
        "push: {push}"
    );
}

/// End-to-end readiness-over-dependsOn (docs/protocol/methods/notes-tasks.md §5.4 /
/// docs/protocol/06-events.md §6.5; spec §2.2,
/// monorepo#1974) over WSS: a task with `dependsOn` edges onto two
/// sibling-subtree tasks stays out of `task:ready-tasks-changed`'s
/// `readyTaskIds` until BOTH deps are `complete` — the event shape itself is
/// unchanged (readyTaskIds + triggeredBy + computedAt).
#[tokio::test]
async fn ready_tasks_gate_on_depends_on_over_wss() {
    let (daemon, port, cfg) = boot().await;

    let socket = daemon.data_dir.join("intentd.sock");
    let create = uds_rpc(
        &socket,
        2,
        "workspace.create",
        json!({ "title": "Readiness", "branch": "main", "skipWorktree": true }),
    )
    .await;
    let ws_id = create["result"]["workspace"]["id"]
        .as_str()
        .expect("workspace id")
        .to_string();

    let mut sub = connect_ws(port, cfg.clone()).await;
    let sub_res = wss_rpc(
        &mut sub,
        1,
        "events.subscribe",
        json!({ "eventTypes": ["task:ready-tasks-changed"], "workspaceId": ws_id }),
    )
    .await;
    assert!(sub_res["subscriptionId"].is_string(), "sub id: {sub_res}");

    let mut rpc = connect_ws(port, cfg.clone()).await;

    // Two sibling task notes (dep-x, dep-y) plus a `gated` task depending on
    // both — all root-level (the ready traversal starts at the root level),
    // all not_started.
    let mut ids = Vec::new();
    for (i, title) in ["DepX", "DepY", "Gated"].iter().enumerate() {
        let id = i64::try_from(i).expect("value fits in i64") * 2 + 2;
        let created = wss_rpc(
            &mut rpc,
            id,
            "note.create",
            json!({ "workspaceId": ws_id, "title": title, "content": "body" }),
        )
        .await;
        let note_id = created["note"]["id"].as_str().expect("note id").to_string();
        wss_rpc(
            &mut rpc,
            id + 1,
            "task.markAsTask",
            json!({ "workspaceId": ws_id, "noteId": note_id, "status": "not_started" }),
        )
        .await;
        ids.push(note_id);
    }
    let (x, y, gated) = (ids[0].clone(), ids[1].clone(), ids[2].clone());
    let set = wss_rpc(
        &mut rpc,
        10,
        "task.setRelations",
        json!({ "workspaceId": ws_id, "noteId": gated, "dependsOn": [x, y] }),
    )
    .await;
    assert_eq!(set["ok"], json!(true), "setRelations: {set}");

    // The dependsOn write itself recomputes the ready set (monorepo#1981):
    // consume the relations-changed event — `gated` just left the set.
    let evt = next_event(&mut sub, &["task:ready-tasks-changed"], 10).await;
    assert_eq!(
        evt["data"]["triggeredBy"],
        json!({ "noteId": gated, "reason": "relations-changed" }),
    );
    let ready = evt["data"]["readyTaskIds"]
        .as_array()
        .expect("readyTaskIds array");
    assert!(
        !ready.iter().any(|v| v == &json!(gated)),
        "gated ready right after gaining unmet deps: {evt}"
    );

    // Complete dep-x: the recomputed ready set must still exclude `gated`
    // (dep-y is unmet) while including the completed-child-free dep-y.
    wss_rpc(
        &mut rpc,
        11,
        "task.updateNoteStatus",
        json!({ "workspaceId": ws_id, "noteId": x, "status": "complete" }),
    )
    .await;
    let evt = next_event(&mut sub, &["task:ready-tasks-changed"], 10).await;
    assert_eq!(evt["data"]["triggeredBy"]["noteId"], json!(x));
    assert_eq!(evt["data"]["triggeredBy"]["newStatus"], json!("complete"));
    assert!(evt["data"]["computedAt"].is_string(), "computedAt: {evt}");
    let ready = evt["data"]["readyTaskIds"]
        .as_array()
        .expect("readyTaskIds array");
    assert!(
        !ready.iter().any(|v| v == &json!(gated)),
        "gated ready with an unmet dep: {evt}"
    );
    assert!(
        ready.iter().any(|v| v == &json!(y)),
        "dep-y missing from ready set: {evt}"
    );

    // Complete dep-y: both edges are satisfied, `gated` joins the ready set.
    wss_rpc(
        &mut rpc,
        12,
        "task.updateNoteStatus",
        json!({ "workspaceId": ws_id, "noteId": y, "status": "complete" }),
    )
    .await;
    let evt = next_event(&mut sub, &["task:ready-tasks-changed"], 10).await;
    assert_eq!(evt["data"]["triggeredBy"]["noteId"], json!(y));
    let ready = evt["data"]["readyTaskIds"]
        .as_array()
        .expect("readyTaskIds array");
    assert!(
        ready.iter().any(|v| v == &json!(gated)),
        "gated not ready with all deps complete: {evt}"
    );

    // Cancelled deps do NOT satisfy edges: cancelling dep-x drops `gated`
    // back out of the ready set.
    wss_rpc(
        &mut rpc,
        13,
        "task.updateNoteStatus",
        json!({ "workspaceId": ws_id, "noteId": x, "status": "cancelled" }),
    )
    .await;
    let evt = next_event(&mut sub, &["task:ready-tasks-changed"], 10).await;
    assert_eq!(evt["data"]["triggeredBy"]["newStatus"], json!("cancelled"));
    let ready = evt["data"]["readyTaskIds"]
        .as_array()
        .expect("readyTaskIds array");
    assert!(
        !ready.iter().any(|v| v == &json!(gated)),
        "gated ready over a cancelled dep: {evt}"
    );
}

/// End-to-end ready-set recompute on relation writes and dep-note deletion
/// (docs/protocol/06-events.md §6.5, monorepo#1981) over WSS: a `task.setRelations` that
/// changes `dependsOn` emits `task:ready-tasks-changed` with the additive
/// `triggeredBy: { noteId, reason: "relations-changed" }` variant (no status
/// fields), and deleting a task note that other tasks dependOn emits the same
/// event with `reason: "note-deleted"` (the dangling edge counts as unmet).
#[tokio::test]
async fn ready_tasks_recompute_on_relations_write_and_deletion_over_wss() {
    let (daemon, port, cfg) = boot().await;

    let socket = daemon.data_dir.join("intentd.sock");
    let create = uds_rpc(
        &socket,
        2,
        "workspace.create",
        json!({ "title": "Recompute", "branch": "main", "skipWorktree": true }),
    )
    .await;
    let ws_id = create["result"]["workspace"]["id"]
        .as_str()
        .expect("workspace id")
        .to_string();

    let mut sub = connect_ws(port, cfg.clone()).await;
    let sub_res = wss_rpc(
        &mut sub,
        1,
        "events.subscribe",
        json!({ "eventTypes": ["task:ready-tasks-changed"], "workspaceId": ws_id }),
    )
    .await;
    assert!(sub_res["subscriptionId"].is_string(), "sub id: {sub_res}");

    let mut rpc = connect_ws(port, cfg.clone()).await;

    // Two root-level task notes: `dep` (complete) and `gated` (not_started).
    let mut ids = Vec::new();
    for (i, (title, status)) in [("Dep", "complete"), ("Gated", "not_started")]
        .iter()
        .enumerate()
    {
        let id = i64::try_from(i).expect("value fits in i64") * 2 + 2;
        let created = wss_rpc(
            &mut rpc,
            id,
            "note.create",
            json!({ "workspaceId": ws_id, "title": title, "content": "body" }),
        )
        .await;
        let note_id = created["note"]["id"].as_str().expect("note id").to_string();
        wss_rpc(
            &mut rpc,
            id + 1,
            "task.markAsTask",
            json!({ "workspaceId": ws_id, "noteId": note_id, "status": status }),
        )
        .await;
        ids.push(note_id);
    }
    let (dep, gated) = (ids[0].clone(), ids[1].clone());

    // A relations write that changes dependsOn recomputes the ready set and
    // emits the reason-shaped trigger. The edge onto the complete `dep` is
    // met, so `gated` stays in the set.
    let set = wss_rpc(
        &mut rpc,
        10,
        "task.setRelations",
        json!({ "workspaceId": ws_id, "noteId": gated, "dependsOn": [dep] }),
    )
    .await;
    assert_eq!(set["ok"], json!(true), "setRelations: {set}");
    let evt = next_event(&mut sub, &["task:ready-tasks-changed"], 10).await;
    assert_eq!(
        evt["data"]["triggeredBy"],
        json!({ "noteId": gated, "reason": "relations-changed" }),
        "relations trigger: {evt}"
    );
    assert!(evt["data"]["computedAt"].is_string(), "computedAt: {evt}");
    assert_eq!(evt["data"]["readyTaskIds"], json!([gated]), "ready: {evt}");

    // Deleting the depended-on note leaves a dangling (unmet) edge: `gated`
    // drops out of the ready set, with the deletion-shaped trigger.
    let del = wss_rpc(
        &mut rpc,
        11,
        "note.delete",
        json!({ "workspaceId": ws_id, "noteId": dep }),
    )
    .await;
    assert_eq!(del["ok"], json!(true), "delete: {del}");
    let evt = next_event(&mut sub, &["task:ready-tasks-changed"], 10).await;
    assert_eq!(
        evt["data"]["triggeredBy"],
        json!({ "noteId": dep, "reason": "note-deleted" }),
        "deletion trigger: {evt}"
    );
    assert_eq!(evt["data"]["readyTaskIds"], json!([]), "ready: {evt}");
}

/// End-to-end ready-set recompute on task-note deletion (docs/protocol/06-events.md §6.5,
/// monorepo#2006) over WSS: deleting a task note that is itself in the ready
/// set emits `task:ready-tasks-changed` with the deleted id gone from
/// `readyTaskIds`, and deleting the last incomplete task child of a parent
/// emits the recompute with the parent now present — both with the
/// `triggeredBy: { noteId, reason: "note-deleted" }` variant.
#[tokio::test]
async fn ready_tasks_recompute_on_task_note_delete_over_wss() {
    let (daemon, port, cfg) = boot().await;

    let socket = daemon.data_dir.join("intentd.sock");
    let create = uds_rpc(
        &socket,
        2,
        "workspace.create",
        json!({ "title": "DeleteRecompute", "branch": "main", "skipWorktree": true }),
    )
    .await;
    let ws_id = create["result"]["workspace"]["id"]
        .as_str()
        .expect("workspace id")
        .to_string();

    let mut sub = connect_ws(port, cfg.clone()).await;
    let sub_res = wss_rpc(
        &mut sub,
        1,
        "events.subscribe",
        json!({ "eventTypes": ["task:ready-tasks-changed"], "workspaceId": ws_id }),
    )
    .await;
    assert!(sub_res["subscriptionId"].is_string(), "sub id: {sub_res}");

    let mut rpc = connect_ws(port, cfg.clone()).await;

    // A root-level parent task, its single incomplete child, and a
    // free-standing ready task.
    let mk = |rpc_id: i64, title: &str, parent: Option<&str>| {
        let mut params = json!({ "workspaceId": ws_id, "title": title, "content": "body" });
        if let Some(p) = parent {
            params["parentId"] = json!(p);
        }
        (rpc_id, params)
    };
    let mut ids = Vec::new();
    let (id, params) = mk(2, "Parent", None);
    let created = wss_rpc(&mut rpc, id, "note.create", params).await;
    let parent = created["note"]["id"].as_str().expect("note id").to_string();
    ids.push(parent.clone());
    for (i, title) in ["Child", "Loner"].iter().enumerate() {
        let (id, params) = mk(
            4 + i64::try_from(i).expect("value fits in i64") * 2,
            title,
            (*title == "Child").then_some(parent.as_str()),
        );
        let created = wss_rpc(&mut rpc, id, "note.create", params).await;
        ids.push(created["note"]["id"].as_str().expect("note id").to_string());
    }
    for (i, note_id) in ids.iter().enumerate() {
        wss_rpc(
            &mut rpc,
            10 + i64::try_from(i).expect("value fits in i64"),
            "task.markAsTask",
            json!({ "workspaceId": ws_id, "noteId": note_id, "status": "not_started" }),
        )
        .await;
    }
    let (child, loner) = (ids[1].clone(), ids[2].clone());

    // Deleting the ready `loner` announces the recompute without its id
    // (only `child` remains ready — `parent` is blocked by its open child).
    let del = wss_rpc(
        &mut rpc,
        20,
        "note.delete",
        json!({ "workspaceId": ws_id, "noteId": loner }),
    )
    .await;
    assert_eq!(del["ok"], json!(true), "delete loner: {del}");
    let evt = next_event(&mut sub, &["task:ready-tasks-changed"], 10).await;
    assert_eq!(
        evt["data"]["triggeredBy"],
        json!({ "noteId": loner, "reason": "note-deleted" }),
        "loner trigger: {evt}"
    );
    assert_eq!(evt["data"]["readyTaskIds"], json!([child]), "ready: {evt}");
    assert!(evt["data"]["computedAt"].is_string(), "computedAt: {evt}");

    // Deleting the last incomplete child satisfies the tree rule, so the
    // parent ENTERS the ready set.
    let del = wss_rpc(
        &mut rpc,
        21,
        "note.delete",
        json!({ "workspaceId": ws_id, "noteId": child }),
    )
    .await;
    assert_eq!(del["ok"], json!(true), "delete child: {del}");
    let evt = next_event(&mut sub, &["task:ready-tasks-changed"], 10).await;
    assert_eq!(
        evt["data"]["triggeredBy"],
        json!({ "noteId": child, "reason": "note-deleted" }),
        "child trigger: {evt}"
    );
    assert_eq!(evt["data"]["readyTaskIds"], json!([parent]), "ready: {evt}");
}

/// End-to-end tree-relative `dependsOn` rejection (docs/protocol/methods/notes-tasks.md §5.4,
/// monorepo#1982) over WSS: a `dependsOn` edge naming a tree ancestor or
/// descendant of the task — the permanent mutual readiness block — is
/// rejected by both `task.setRelations` and `task.markAsTask` with `-32603`
/// and the offending relationship named in `error.data`, mirroring the
/// cycle-rejection envelope; sibling edges stay valid.
#[tokio::test]
async fn task_relations_reject_tree_relative_edges_over_wss() {
    let (daemon, port, cfg) = boot().await;

    let socket = daemon.data_dir.join("intentd.sock");
    let create = uds_rpc(
        &socket,
        2,
        "workspace.create",
        json!({ "title": "TreeEdges", "branch": "main", "skipWorktree": true }),
    )
    .await;
    let ws_id = create["result"]["workspace"]["id"]
        .as_str()
        .expect("workspace id")
        .to_string();

    let mut rpc = connect_ws(port, cfg.clone()).await;

    // parent (root-level task) → child (task nested under parent), plus a
    // root-level sibling task.
    let created = wss_rpc(
        &mut rpc,
        2,
        "note.create",
        json!({ "workspaceId": ws_id, "title": "Parent", "content": "body" }),
    )
    .await;
    let parent = created["note"]["id"].as_str().expect("note id").to_string();
    let created = wss_rpc(
        &mut rpc,
        3,
        "note.create",
        json!({ "workspaceId": ws_id, "title": "Child", "content": "body", "parentId": parent }),
    )
    .await;
    let child = created["note"]["id"].as_str().expect("note id").to_string();
    let created = wss_rpc(
        &mut rpc,
        4,
        "note.create",
        json!({ "workspaceId": ws_id, "title": "Sibling", "content": "body" }),
    )
    .await;
    let sibling = created["note"]["id"].as_str().expect("note id").to_string();
    for (i, id) in [&parent, &child, &sibling].iter().enumerate() {
        wss_rpc(
            &mut rpc,
            i64::try_from(i).expect("value fits in i64") + 5,
            "task.markAsTask",
            json!({ "workspaceId": ws_id, "noteId": id, "status": "not_started" }),
        )
        .await;
    }

    // child dependsOn parent → -32603, ancestor relationship named in data.
    // The full JSON-RPC error envelope shape is asserted (§1): `jsonrpc`,
    // echoed `id`, `error` with `code`/`message`/`data`, and no `result`.
    let err = wss_rpc_envelope(
        &mut rpc,
        10,
        "task.setRelations",
        json!({ "workspaceId": ws_id, "noteId": child, "dependsOn": [parent] }),
    )
    .await;
    assert_eq!(err["jsonrpc"], json!("2.0"), "envelope jsonrpc: {err}");
    assert_eq!(err["id"], json!(10), "envelope id echoed: {err}");
    assert!(err.get("result").is_none(), "no result on error: {err}");
    assert!(
        err["error"]["message"].is_string(),
        "error message present: {err}"
    );
    assert_eq!(err["error"]["code"], json!(-32603), "ancestor: {err}");
    let detail = err["error"]["data"]
        .as_str()
        .expect("ancestor error detail");
    assert!(
        detail.contains("tree ancestor"),
        "ancestor rejection: {err}"
    );
    assert!(
        detail.contains(&format!("{parent} is an ancestor of {child}")),
        "relationship named: {detail}"
    );

    // parent dependsOn child → -32603, descendant relationship named.
    let err = wss_rpc_envelope(
        &mut rpc,
        11,
        "task.setRelations",
        json!({ "workspaceId": ws_id, "noteId": parent, "dependsOn": [child] }),
    )
    .await;
    assert_eq!(err["jsonrpc"], json!("2.0"), "envelope jsonrpc: {err}");
    assert_eq!(err["id"], json!(11), "envelope id echoed: {err}");
    assert_eq!(err["error"]["code"], json!(-32603), "descendant: {err}");
    let detail = err["error"]["data"]
        .as_str()
        .expect("descendant error detail");
    assert!(
        detail.contains(&format!("{child} is a descendant of {parent}")),
        "relationship named: {detail}"
    );

    // markAsTask enforces the same rejection envelope.
    let err = wss_rpc_envelope(
        &mut rpc,
        12,
        "task.markAsTask",
        json!({
            "workspaceId": ws_id,
            "noteId": child,
            "status": "not_started",
            "dependsOn": [parent],
        }),
    )
    .await;
    assert_eq!(err["jsonrpc"], json!("2.0"), "envelope jsonrpc: {err}");
    assert_eq!(err["id"], json!(12), "envelope id echoed: {err}");
    assert_eq!(err["error"]["code"], json!(-32603), "markAsTask: {err}");
    assert!(
        err["error"]["data"]
            .as_str()
            .unwrap_or("")
            .contains("tree ancestor"),
        "markAsTask ancestor rejection: {err}"
    );

    // A sibling edge stays valid, and the rejected writes persisted nothing.
    let set = wss_rpc(
        &mut rpc,
        13,
        "task.setRelations",
        json!({ "workspaceId": ws_id, "noteId": child, "dependsOn": [sibling] }),
    )
    .await;
    assert_eq!(set["ok"], json!(true), "sibling edge: {set}");
    let mine = wss_rpc(
        &mut rpc,
        14,
        "task.getMyTask",
        json!({ "workspaceId": ws_id, "taskNoteId": parent }),
    )
    .await;
    assert!(
        mine["taskMetadata"].get("dependsOn").is_none(),
        "rejected write persisted: {mine}"
    );
}

/// End-to-end `unmetDependsOn` on note-shaped payloads (docs/protocol/methods/notes-tasks.md §5.3;
/// monorepo#1979) over WSS: `note.get` / `note.list` project the computed
/// field under `metadata.task` using the same rule as the `task.list`
/// projection (dep unmet unless its task note is `complete`; cancelled =
/// unmet), the field is omitted when empty (additive wire shape), a
/// dependency status change re-announces dependents via `note:updated` so
/// note-channel subscribers refetch, and the note-subscription delta carries
/// the refreshed projection.
#[tokio::test]
async fn note_payloads_project_unmet_depends_on_over_wss() {
    let (daemon, port, cfg) = boot().await;

    let socket = daemon.data_dir.join("intentd.sock");
    let create = uds_rpc(
        &socket,
        2,
        "workspace.create",
        json!({ "title": "UnmetDeps", "branch": "main", "skipWorktree": true }),
    )
    .await;
    let ws_id = create["result"]["workspace"]["id"]
        .as_str()
        .expect("workspace id")
        .to_string();

    let mut rpc = connect_ws(port, cfg.clone()).await;

    // Three task notes: a (complete), b (in_progress), c (not_started).
    let mut task_ids = Vec::new();
    for (i, (title, status)) in [
        ("Alpha", "complete"),
        ("Beta", "in_progress"),
        ("Gamma", "not_started"),
    ]
    .iter()
    .enumerate()
    {
        let id = i64::try_from(i).expect("value fits in i64") * 2 + 2;
        let created = wss_rpc(
            &mut rpc,
            id,
            "note.create",
            json!({ "workspaceId": ws_id, "title": title, "content": "body" }),
        )
        .await;
        let note_id = created["note"]["id"].as_str().expect("note id").to_string();
        wss_rpc(
            &mut rpc,
            id + 1,
            "task.markAsTask",
            json!({ "workspaceId": ws_id, "noteId": note_id, "status": status }),
        )
        .await;
        task_ids.push(note_id);
    }
    let (a, b, c) = (
        task_ids[0].clone(),
        task_ids[1].clone(),
        task_ids[2].clone(),
    );

    // c dependsOn [a, b]: a is met (complete), b is unmet (in_progress).
    wss_rpc(
        &mut rpc,
        10,
        "task.setRelations",
        json!({ "workspaceId": ws_id, "noteId": c, "dependsOn": [a, b] }),
    )
    .await;

    // note.get projects `metadata.task.unmetDependsOn`.
    let got = wss_rpc(
        &mut rpc,
        11,
        "note.get",
        json!({ "workspaceId": ws_id, "noteId": c }),
    )
    .await;
    assert_eq!(got["note"]["metadata"]["task"]["dependsOn"], json!([a, b]));
    assert_eq!(
        got["note"]["metadata"]["task"]["unmetDependsOn"],
        json!([b]),
        "note.get: {got}"
    );

    // note.list projects the same field; dep-less tasks omit it entirely.
    let listed = wss_rpc(&mut rpc, 12, "note.list", json!({ "workspaceId": ws_id })).await;
    let notes = listed["notes"].as_array().expect("notes array");
    let row_c = notes.iter().find(|n| n["id"] == json!(c)).expect("c row");
    assert_eq!(
        row_c["metadata"]["task"]["unmetDependsOn"],
        json!([b]),
        "note.list row: {row_c}"
    );
    let row_b = notes.iter().find(|n| n["id"] == json!(b)).expect("b row");
    assert!(
        row_b["metadata"]["task"].get("unmetDependsOn").is_none(),
        "dep-less task omits the field: {row_b}"
    );

    // Subscribe to the note channel: the seq-0 snapshot carries the
    // projection too.
    let mut sub = connect_ws(port, cfg.clone()).await;
    let sub_res = wss_rpc(
        &mut sub,
        1,
        "note.subscribe",
        json!({ "workspaceId": ws_id }),
    )
    .await;
    let sub_id = sub_res["subscriptionId"]
        .as_str()
        .expect("subscriptionId")
        .to_string();
    let push = next_subscription_push(&mut sub, 10).await;
    assert_eq!(push["subscriptionId"], sub_id.as_str(), "push: {push}");
    assert_eq!(push["kind"], json!("snapshot"), "push: {push}");
    let snap = push["snapshot"].as_array().expect("snapshot array");
    let snap_c = snap.iter().find(|n| n["id"] == json!(c)).expect("c row");
    assert_eq!(
        snap_c["metadata"]["task"]["unmetDependsOn"],
        json!([b]),
        "snapshot row: {snap_c}"
    );

    // Also watch the event firehose for the dependent's `note:updated`.
    let mut events = connect_ws(port, cfg.clone()).await;
    let evt_res = wss_rpc(
        &mut events,
        1,
        "events.subscribe",
        json!({ "eventTypes": ["note:updated"], "workspaceId": ws_id }),
    )
    .await;
    assert!(evt_res["subscriptionId"].is_string(), "sub id: {evt_res}");

    // Complete dep b: the daemon re-announces dependent c via `note:updated`
    // (b's own update event fires first), and the note-channel delta for c
    // carries the emptied projection (field omitted).
    wss_rpc(
        &mut rpc,
        13,
        "task.updateNoteStatus",
        json!({ "workspaceId": ws_id, "noteId": b, "status": "complete" }),
    )
    .await;
    loop {
        let evt = next_event(&mut events, &["note:updated"], 10).await;
        if evt["data"]["noteId"] == json!(c) {
            break;
        }
    }
    // Drain note-channel deltas until c's arrives with the refreshed
    // projection: both dep edges are now met, so the field is omitted.
    let updated_c = 'outer: loop {
        let push = next_subscription_push(&mut sub, 10).await;
        assert_eq!(push["kind"], json!("delta"), "push: {push}");
        if let Some(updated) = push["delta"]["updated"].as_array() {
            for entry in updated {
                if entry["id"] == json!(c) {
                    break 'outer entry.clone();
                }
            }
        }
    };
    assert_eq!(
        updated_c["metadata"]["task"]["dependsOn"],
        json!([a, b]),
        "stored relations survive: {updated_c}"
    );
    assert!(
        updated_c["metadata"]["task"]
            .get("unmetDependsOn")
            .is_none(),
        "all deps met → field omitted: {updated_c}"
    );

    // Cancel dep b: cancelled counts as unmet again, and the re-announced
    // delta for c carries it.
    wss_rpc(
        &mut rpc,
        14,
        "task.updateNoteStatus",
        json!({ "workspaceId": ws_id, "noteId": b, "status": "cancelled" }),
    )
    .await;
    let updated_c = 'outer: loop {
        let push = next_subscription_push(&mut sub, 10).await;
        if let Some(updated) = push["delta"]["updated"].as_array() {
            for entry in updated {
                if entry["id"] == json!(c) {
                    break 'outer entry.clone();
                }
            }
        }
    };
    assert_eq!(
        updated_c["metadata"]["task"]["unmetDependsOn"],
        json!([b]),
        "cancelled dep is unmet: {updated_c}"
    );
}

/// End-to-end `task.convertBlocks` relation seeding (monorepo#2018) over WSS:
/// `@@@task` header attributes resolve at conversion time (sibling `key=` →
/// sibling title → existing task-note id) and seed `dependsOn` through the
/// same validated writer as `task.setRelations`, so `task:ready-tasks-changed`
/// fires with the `relations-changed` trigger; the explicit RPC result carries
/// the additive `createdTasks` array (`{ key?, title, noteId }` in block
/// order) and `warnings` array naming skipped references. Auto-conversion on
/// `note.create` is asserted first, then `note.restoreVersion` (which does not
/// auto-convert) restores the fence content so the explicit `task.convertBlocks`
/// arm serializes `{ ok, convertedCount, createdNoteIds, createdTasks,
/// warnings }` on the wire. A final `note.setContent` arm asserts the
/// note-write results surface the same additive fields alongside
/// `convertedCount`/`createdTaskNoteIds`, with the seeded relation verifiable
/// via `note.get`.
#[tokio::test]
async fn task_convert_blocks_relation_seeding_and_warnings_over_wss() {
    let (daemon, port, cfg) = boot().await;

    let socket = daemon.data_dir.join("intentd.sock");
    let create = uds_rpc(
        &socket,
        2,
        "workspace.create",
        json!({ "title": "ConvertRelations", "branch": "main", "skipWorktree": true }),
    )
    .await;
    let ws_id = create["result"]["workspace"]["id"]
        .as_str()
        .expect("workspace id")
        .to_string();

    let mut sub = connect_ws(port, cfg.clone()).await;
    let sub_res = wss_rpc(
        &mut sub,
        1,
        "events.subscribe",
        json!({ "eventTypes": ["task:ready-tasks-changed"], "workspaceId": ws_id }),
    )
    .await;
    assert!(sub_res["subscriptionId"].is_string(), "sub id: {sub_res}");

    // Author: `note.create` auto-converts; Beta's `dependsOn=a` resolves via
    // Alpha's `key=` and seeds, `ghost` is unknown and skipped with a warning
    // (logged on this path; surfaced on the wire by the explicit RPC below).
    let mut rpc = connect_ws(port, cfg.clone()).await;
    let created = wss_rpc(
        &mut rpc,
        2,
        "note.create",
        json!({
            "workspaceId": ws_id,
            "title": "Plan",
            "content": "@@@task key=a\n# Alpha\nbody\n@@@\n@@@task dependsOn=a,ghost\n# Beta\nbody\n@@@",
        }),
    )
    .await;
    let parent_id = created["note"]["id"].as_str().expect("note id").to_string();
    let content = created["note"]["content"].as_str().expect("content");
    assert!(
        !content.contains("@@@task"),
        "fences not removed: {content}"
    );

    // The seeded edge recomputed the ready set with the relations-changed
    // trigger; Beta (unmet dep on Alpha) is not in `readyTaskIds`.
    let evt = next_event(&mut sub, &["task:ready-tasks-changed"], 10).await;
    assert_eq!(
        evt["data"]["triggeredBy"]["reason"],
        json!("relations-changed"),
        "auto-convert seeding trigger: {evt}"
    );
    let beta_id = evt["data"]["triggeredBy"]["noteId"]
        .as_str()
        .expect("triggering note id")
        .to_string();
    let ready = evt["data"]["readyTaskIds"]
        .as_array()
        .expect("readyTaskIds array");
    assert!(
        !ready.iter().any(|v| v == &json!(beta_id)),
        "dep-blocked task still ready: {evt}"
    );

    // The seeded relation is visible on the child task note.
    let got = wss_rpc(
        &mut rpc,
        3,
        "note.get",
        json!({ "workspaceId": ws_id, "noteId": beta_id }),
    )
    .await;
    assert_eq!(got["note"]["title"], json!("Beta"), "child: {got}");
    let deps = got["note"]["metadata"]["task"]["dependsOn"]
        .as_array()
        .expect("dependsOn array");
    assert_eq!(deps.len(), 1, "seeded dep: {got}");

    // Explicit-RPC arm: delete the converted children, then restore v1 (the
    // pre-conversion snapshot — `note.restoreVersion` does not auto-convert),
    // leaving fence content in place for `task.convertBlocks` to consume.
    let tasks = wss_rpc(
        &mut rpc,
        4,
        "note.listTasks",
        json!({ "workspaceId": ws_id, "noteId": parent_id }),
    )
    .await;
    let rows = tasks.as_array().expect("bare array");
    assert_eq!(rows.len(), 2, "rows: {tasks}");
    for (i, row) in rows.iter().enumerate() {
        let child_id = row["taskNoteId"].as_str().expect("child id");
        let del = wss_rpc(
            &mut rpc,
            5 + i64::try_from(i).expect("value fits in i64"),
            "note.delete",
            json!({ "workspaceId": ws_id, "noteId": child_id }),
        )
        .await;
        assert_eq!(del["ok"], json!(true), "delete: {del}");
    }
    let restored = wss_rpc(
        &mut rpc,
        7,
        "note.restoreVersion",
        json!({ "workspaceId": ws_id, "noteId": parent_id, "v": 1 }),
    )
    .await;
    let content = restored["note"]["content"].as_str().expect("content");
    assert!(content.contains("@@@task"), "fences restored: {content}");

    let conv = wss_rpc(
        &mut rpc,
        8,
        "task.convertBlocks",
        json!({ "workspaceId": ws_id, "noteId": parent_id }),
    )
    .await;
    assert_eq!(conv["ok"], json!(true), "convert: {conv}");
    assert_eq!(conv["convertedCount"], json!(2), "convert: {conv}");
    assert_eq!(
        conv["createdNoteIds"].as_array().map(std::vec::Vec::len),
        Some(2),
        "convert: {conv}"
    );
    let warnings = conv["warnings"].as_array().expect("warnings array");
    assert_eq!(warnings.len(), 1, "convert: {conv}");
    let w = warnings[0].as_str().expect("warning string");
    assert!(
        w.contains("\"Beta\"") && w.contains("ghost"),
        "warning names block and reference: {w}"
    );
    let created_tasks = conv["createdTasks"].as_array().expect("createdTasks array");
    assert_eq!(created_tasks.len(), 2, "convert: {conv}");
    assert_eq!(created_tasks[0]["key"], json!("a"), "convert: {conv}");
    assert_eq!(created_tasks[0]["title"], json!("Alpha"), "convert: {conv}");
    assert_eq!(
        created_tasks[0]["noteId"], conv["createdNoteIds"][0],
        "createdTasks parallels createdNoteIds: {conv}"
    );
    assert!(
        created_tasks[1].get("key").is_none(),
        "no key= → field omitted: {conv}"
    );
    assert_eq!(created_tasks[1]["title"], json!("Beta"), "convert: {conv}");
    assert_eq!(
        created_tasks[1]["noteId"], conv["createdNoteIds"][1],
        "createdTasks parallels createdNoteIds: {conv}"
    );

    // The re-seeded edge recomputes readiness again, triggered by the new
    // Beta child. Child deletions above may also have emitted recomputes;
    // scan past them to the relations-changed trigger.
    let deadline = tokio::time::Instant::now() + Duration::from_secs(10);
    loop {
        assert!(
            tokio::time::Instant::now() < deadline,
            "timed out waiting for relations-changed recompute"
        );
        let evt = next_event(&mut sub, &["task:ready-tasks-changed"], 10).await;
        if evt["data"]["triggeredBy"]["reason"] == json!("relations-changed") {
            let new_beta = evt["data"]["triggeredBy"]["noteId"]
                .as_str()
                .expect("triggering note id");
            assert_ne!(new_beta, beta_id, "fresh child triggered: {evt}");
            assert!(
                conv["createdNoteIds"]
                    .as_array()
                    .expect("createdNoteIds")
                    .iter()
                    .any(|v| v == &json!(new_beta)),
                "trigger is a created child: {evt}"
            );
            break;
        }
    }

    // Note-write surface: a `note.setContent` write containing fences
    // carries the same additive `createdTasks`/`warnings` fields alongside
    // the existing `convertedCount`/`createdTaskNoteIds`.
    let plan2 = wss_rpc(
        &mut rpc,
        9,
        "note.create",
        json!({ "workspaceId": ws_id, "title": "Plan2", "content": "placeholder" }),
    )
    .await;
    let plan2_id = plan2["note"]["id"].as_str().expect("note id").to_string();
    let set = wss_rpc(
        &mut rpc,
        10,
        "note.setContent",
        json!({
            "workspaceId": ws_id,
            "noteId": plan2_id,
            "content": "@@@task key=g\n# Gamma\nbody\n@@@\n@@@task dependsOn=g,phantom\n# Delta\nbody\n@@@",
        }),
    )
    .await;
    assert_eq!(set["ok"], json!(true), "setContent: {set}");
    assert_eq!(set["convertedCount"], json!(2), "setContent: {set}");
    let ids = set["createdTaskNoteIds"]
        .as_array()
        .expect("createdTaskNoteIds array");
    assert_eq!(ids.len(), 2, "setContent: {set}");
    let created = set["createdTasks"].as_array().expect("createdTasks array");
    assert_eq!(created.len(), 2, "setContent: {set}");
    assert_eq!(created[0]["key"], json!("g"), "setContent: {set}");
    assert_eq!(created[0]["title"], json!("Gamma"), "setContent: {set}");
    assert_eq!(
        created[0]["noteId"], ids[0],
        "createdTasks parallels createdTaskNoteIds: {set}"
    );
    assert!(
        created[1].get("key").is_none(),
        "no key= → field omitted: {set}"
    );
    assert_eq!(created[1]["title"], json!("Delta"), "setContent: {set}");
    assert_eq!(
        created[1]["noteId"], ids[1],
        "createdTasks parallels createdTaskNoteIds: {set}"
    );
    let set_warnings = set["warnings"].as_array().expect("warnings array");
    assert_eq!(set_warnings.len(), 1, "setContent: {set}");
    let w = set_warnings[0].as_str().expect("warning string");
    assert!(
        w.contains("\"Delta\"") && w.contains("phantom"),
        "warning names block and reference: {w}"
    );
    let new_content = set["newContent"].as_str().expect("newContent");
    assert!(
        !new_content.contains("@@@task"),
        "fences converted: {new_content}"
    );

    // The `key=`-resolved edge seeded by the write is visible via `note.get`.
    let gamma_id = created[0]["noteId"].as_str().expect("gamma id");
    let delta_id = created[1]["noteId"].as_str().expect("delta id");
    let got = wss_rpc(
        &mut rpc,
        11,
        "note.get",
        json!({ "workspaceId": ws_id, "noteId": delta_id }),
    )
    .await;
    assert_eq!(got["note"]["title"], json!("Delta"), "child: {got}");
    assert_eq!(
        got["note"]["metadata"]["task"]["dependsOn"],
        json!([gamma_id]),
        "seeded dep: {got}"
    );
}

/// Regression for monorepo#3586 over the real WSS wire: the `note.subscribe`
/// seq-0 snapshot (and note-channel deltas) serialized FULL note rows, so a
/// client that adopted `projection: "slim"` on `note.list` (v8.1,
/// monorepo#3573) to stay under the 1 MiB outbound cap still received
/// full-content frames on the subscription surface. `projection: "slim"` on
/// `note.subscribe` (v8.2) serves the same bounded rows — `content` omitted,
/// replaced by `contentPreview` (500 chars) + `contentLength` — on the
/// snapshot AND every `added` / `updated` delta; the default (absent /
/// `null` / `"full"`) stays full rows byte-identical to before, and any
/// other value is `-32602`, mirroring `note.list`.
#[tokio::test]
async fn note_subscribe_slim_projection_bounds_snapshot_and_deltas() {
    let (daemon, port, cfg) = boot().await;

    let socket = daemon.data_dir.join("intentd.sock");
    let create = uds_rpc(
        &socket,
        1,
        "workspace.create",
        json!({ "title": "SlimSub", "branch": "main", "skipWorktree": true }),
    )
    .await;
    let ws_id = create["result"]["workspace"]["id"]
        .as_str()
        .expect("workspace id")
        .to_string();

    // One giant note (~1.1 MiB) reproduces the oversized-frame report.
    let mut rpc = connect_ws(port, cfg.clone()).await;
    let big = "x".repeat(1_100_000);
    let created = wss_rpc(
        &mut rpc,
        2,
        "note.create",
        json!({ "workspaceId": ws_id, "title": "Giant", "content": big }),
    )
    .await;
    let giant_id = created["note"]["id"].as_str().expect("note id").to_string();

    // Any projection value other than absent/null/"full"/"slim" is -32602.
    let mut sub = connect_ws(port, cfg.clone()).await;
    let bad = wss_rpc_envelope(
        &mut sub,
        1,
        "note.subscribe",
        json!({ "workspaceId": ws_id, "projection": "bogus" }),
    )
    .await;
    assert_eq!(bad["error"]["code"], json!(-32602), "envelope: {bad}");

    // Slim subscribe: seq-0 snapshot rows are bounded.
    let slim_res = wss_rpc(
        &mut sub,
        2,
        "note.subscribe",
        json!({ "workspaceId": ws_id, "projection": "slim" }),
    )
    .await;
    assert!(slim_res["subscriptionId"].is_string(), "sub: {slim_res}");
    let push = next_subscription_push(&mut sub, 10).await;
    assert_eq!(push["kind"], json!("snapshot"), "push kind");
    let snap = push["snapshot"].as_array().expect("snapshot array");
    let giant = snap
        .iter()
        .find(|n| n["id"] == json!(giant_id))
        .expect("giant note in snapshot");
    assert!(
        giant.get("content").is_none(),
        "slim snapshot omits content: {giant}"
    );
    assert_eq!(
        giant["contentPreview"].as_str().map(str::len),
        Some(500),
        "preview bounded: {giant}"
    );
    assert_eq!(giant["contentLength"].as_i64(), Some(1_100_000));
    let frame = serde_json::to_string(&push).expect("serialize");
    assert!(
        frame.len() < 256 * 1024,
        "slim snapshot stays bounded: {} bytes",
        frame.len()
    );

    // `added` delta (note:created re-read) is slim too.
    let big2 = "y".repeat(1_100_000);
    let created2 = wss_rpc(
        &mut rpc,
        3,
        "note.create",
        json!({ "workspaceId": ws_id, "title": "Giant2", "content": big2 }),
    )
    .await;
    let second_id = created2["note"]["id"]
        .as_str()
        .expect("note id")
        .to_string();
    let added = 'added: loop {
        let push = next_subscription_push(&mut sub, 10).await;
        assert_eq!(push["kind"], json!("delta"), "push: {push}");
        if let Some(rows) = push["delta"]["added"].as_array() {
            for row in rows {
                if row["id"] == json!(second_id) {
                    break 'added row.clone();
                }
            }
        }
    };
    assert!(
        added.get("content").is_none(),
        "slim added delta omits content: {added}"
    );
    assert_eq!(
        added["contentPreview"].as_str().map(str::len),
        Some(500),
        "added preview bounded: {added}"
    );
    assert_eq!(added["contentLength"].as_i64(), Some(1_100_000));

    // `updated` delta (note:updated re-read) is slim too.
    let big3 = "z".repeat(1_100_000);
    wss_rpc(
        &mut rpc,
        4,
        "note.setContent",
        json!({ "workspaceId": ws_id, "noteId": giant_id, "content": big3 }),
    )
    .await;
    let updated = 'updated: loop {
        let push = next_subscription_push(&mut sub, 10).await;
        assert_eq!(push["kind"], json!("delta"), "push: {push}");
        if let Some(rows) = push["delta"]["updated"].as_array() {
            for row in rows {
                if row["id"] == json!(giant_id) {
                    break 'updated row.clone();
                }
            }
        }
    };
    assert!(
        updated.get("content").is_none(),
        "slim updated delta omits content: {updated}"
    );
    assert_eq!(
        updated["contentPreview"].as_str().map(str::len),
        Some(500),
        "updated preview bounded: {updated}"
    );
    assert_eq!(updated["contentLength"].as_i64(), Some(1_100_000));

    // Default (absent projection): full rows, complete content intact — the
    // pre-8.2 wire shape for existing clients. Explicit "full" matches.
    for (id, params) in [
        (1, json!({ "workspaceId": ws_id })),
        (2, json!({ "workspaceId": ws_id, "projection": "full" })),
    ] {
        let mut full_sub = connect_ws(port, cfg.clone()).await;
        let res = wss_rpc(&mut full_sub, id, "note.subscribe", params).await;
        assert!(res["subscriptionId"].is_string(), "sub: {res}");
        let push = next_subscription_push(&mut full_sub, 10).await;
        assert_eq!(push["kind"], json!("snapshot"), "push kind");
        let snap = push["snapshot"].as_array().expect("snapshot array");
        let giant = snap
            .iter()
            .find(|n| n["id"] == json!(giant_id))
            .expect("giant note in snapshot");
        assert_eq!(
            giant["content"].as_str().map(str::len),
            Some(1_100_000),
            "full snapshot keeps content"
        );
        assert!(giant.get("contentPreview").is_none(), "no preview: {giant}");
    }
}
