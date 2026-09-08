//! WSS end-to-end for the REV-2 per-workspace browser-client pin RPCs
//! (PROTOCOL §5.1 `workspace.getBrowserClient` / `workspace.setBrowserClient`,
//! §5.17 `client.list`) over the production transport: a real `intentd serve`
//! with the TLS listener, bearer auth, and fingerprint pinning all in play.
//!
//! Every reply passes through [`assert_envelope`] (`jsonrpc` / `id` echo /
//! exactly one of `result` | `error`, error object shape) so the contract is
//! checked on the success and `-32602` paths alike. The in-process
//! reverse-dispatch routing of a pinned `browser.exec` (which needs the
//! daemon's `WorkspaceApi` / registry handles) stays in
//! `e2e_wss_sticky_reverse.rs`.

#![cfg(unix)]

mod common;

use std::path::{Path, PathBuf};
use std::process::{Child, Command, Stdio};
use std::sync::Arc;
use std::time::Duration;

use futures_util::{SinkExt, StreamExt};
use intent_core::CHIEF_WORKSPACE_ID;
use rustls::client::danger::{HandshakeSignatureValid, ServerCertVerified, ServerCertVerifier};
use rustls::crypto::CryptoProvider;
use rustls::{ClientConfig, DigitallySignedStruct, SignatureScheme};
use rustls_pki_types::{CertificateDer, ServerName, UnixTime};
use serde_json::{json, Value};
use sha2::{Digest, Sha256};
use tokio::net::UnixStream;
use tokio::time::{timeout, Instant};
use tokio_tungstenite::tungstenite::Message;

use common::TlsWs;

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

/// Short base under /tmp (UDS `SUN_LEN` cap); the returned guard removes the
/// root on drop — hold it for the full test.
fn scratch_dir() -> tempfile::TempDir {
    common::test_tempdir_in("/tmp", "itd-wss-bcpin-")
}

fn spawn_serve(data_dir: &Path) -> Child {
    let log = std::fs::File::create(data_dir.join("daemon.log")).expect("create daemon log");
    let workspaces_dir = data_dir.join("workspaces");
    std::fs::create_dir_all(&workspaces_dir).expect("mkdir hermetic workspaces dir");
    common::enable_ws_api(data_dir);
    Command::new(env!("CARGO_BIN_EXE_intentd"))
        .arg("serve")
        .env("INTENTD_DATA_DIR", data_dir)
        .env("INTENTD_WORKSPACES_DIR", &workspaces_dir)
        .env("INTENTD_ASSERT_HERMETIC_ROOT", "1")
        .env("INTENTD_AUTH_TOKEN", TOKEN)
        .env("INTENTD_TCP_PORT", "0")
        .stdout(Stdio::null())
        .stderr(Stdio::from(log))
        .spawn()
        .expect("spawn intentd serve")
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

async fn boot(root: &Path) -> (Daemon, u16, Arc<ClientConfig>) {
    let data_dir = root.join("data");
    std::fs::create_dir_all(&data_dir).expect("mkdir data");
    let child = spawn_serve(&data_dir);
    let socket = data_dir.join("intentd.sock");
    assert!(await_uds(&socket).await, "daemon did not start");
    let status = common::await_wss_status(&socket).await;
    let fp_hex = status["result"]["fingerprint"]
        .as_str()
        .expect("fingerprint");
    let port = u16::try_from(status["result"]["port"].as_u64().expect("bound port"))
        .expect("value fits in u16");
    let cfg = client_config(fp_hex);
    (Daemon { child, data_dir }, port, cfg)
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

fn client_config(fp_hex: &str) -> Arc<ClientConfig> {
    let provider = Arc::new(rustls::crypto::ring::default_provider());
    let config = ClientConfig::builder_with_provider(provider.clone())
        .with_safe_default_protocol_versions()
        .expect("default protocol versions")
        .dangerous()
        .with_custom_certificate_verifier(Arc::new(PinnedVerifier {
            fingerprint: fp_hex.to_string(),
            provider: provider.clone(),
        }))
        .with_no_client_auth();
    Arc::new(config)
}

async fn connect(port: u16, tls_cfg: Arc<ClientConfig>) -> TlsWs {
    let url = format!("wss://localhost:{port}/ws?token={TOKEN}");
    common::wss_connect_with_retry(port, tls_cfg, &url).await
}

/// Assert the full JSON-RPC 2.0 response envelope: `jsonrpc == "2.0"`, the
/// request `id` echoed, and exactly one of `result` / `error` with no other
/// top-level keys. An `error` member carries an integer `code` and a string
/// `message` (plus optional `data`).
fn assert_envelope(v: &Value, id: i64, method: &str) {
    let obj = v
        .as_object()
        .unwrap_or_else(|| panic!("{method}: response is not an object: {v}"));
    assert_eq!(obj.get("jsonrpc"), Some(&json!("2.0")), "{method}: {v}");
    assert_eq!(obj.get("id"), Some(&json!(id)), "{method}: {v}");
    let mut keys: Vec<&str> = obj.keys().map(String::as_str).collect();
    keys.sort_unstable();
    match (obj.get("result"), obj.get("error")) {
        (Some(_), None) => assert_eq!(keys, ["id", "jsonrpc", "result"], "{method}: {v}"),
        (None, Some(err)) => {
            assert_eq!(keys, ["error", "id", "jsonrpc"], "{method}: {v}");
            let err = err
                .as_object()
                .unwrap_or_else(|| panic!("{method}: error is not an object: {v}"));
            assert!(err["code"].is_i64(), "{method}: error.code: {v}");
            assert!(err["message"].is_string(), "{method}: error.message: {v}");
            let mut err_keys: Vec<&str> = err.keys().map(String::as_str).collect();
            err_keys.sort_unstable();
            assert!(
                err_keys == ["code", "message"] || err_keys == ["code", "data", "message"],
                "{method}: error keys {err_keys:?}: {v}"
            );
        }
        _ => panic!("{method}: exactly one of result/error required: {v}"),
    }
}

/// One bounded JSON-RPC round-trip on `ws`: send `method`/`params` with `id`,
/// then wait for the matching response frame under a single overall deadline,
/// answering pings and skipping unrelated notifications inline. The full
/// envelope is asserted before the frame is returned.
async fn wss_rpc(ws: &mut TlsWs, id: i64, method: &str, params: Value) -> Value {
    let req = json!({ "jsonrpc": "2.0", "id": id, "method": method, "params": params });
    ws.send(Message::Text(req.to_string().into()))
        .await
        .expect("send rpc");
    let deadline = Instant::now() + common::rpc_read_timeout();
    loop {
        let remaining = deadline.saturating_duration_since(Instant::now());
        assert!(!remaining.is_zero(), "timed out waiting for {method} reply");
        match timeout(remaining, ws.next())
            .await
            .unwrap_or_else(|_| panic!("timed out waiting for {method} reply"))
        {
            Some(Ok(Message::Text(text))) => {
                let v: Value = serde_json::from_str(&text).expect("json frame");
                if v.get("id") == Some(&json!(id)) {
                    assert_envelope(&v, id, method);
                    return v;
                }
            }
            Some(Ok(Message::Ping(p))) => {
                let _ = ws.send(Message::Pong(p)).await;
            }
            Some(Ok(_)) => {}
            other => panic!("unexpected ws frame: {other:?}"),
        }
    }
}

/// Pump an `events.subscribe` connection until an `events.event` of
/// `event_type` arrives (bounded), answering pings inline. Returns the event.
async fn await_event(ws: &mut TlsWs, event_type: &str, dur: Duration) -> Value {
    let deadline = Instant::now() + dur;
    loop {
        let remaining = deadline.saturating_duration_since(Instant::now());
        assert!(
            !remaining.is_zero(),
            "timed out waiting for {event_type} event"
        );
        match timeout(remaining, ws.next())
            .await
            .unwrap_or_else(|_| panic!("timed out waiting for {event_type} event"))
        {
            Some(Ok(Message::Text(text))) => {
                let v: Value = serde_json::from_str(&text).expect("json frame");
                if v["method"] == "events.event" && v["params"]["event"]["type"] == event_type {
                    return v["params"]["event"].clone();
                }
            }
            Some(Ok(Message::Ping(p))) => {
                let _ = ws.send(Message::Pong(p)).await;
            }
            Some(Ok(_)) => {}
            other => panic!("unexpected ws frame: {other:?}"),
        }
    }
}

/// Close `ws` and drain it until the server's close frame (or the deadline),
/// so the connection is really gone before the caller waits on the
/// `client:disconnected` event that proves the daemon deregistered it.
async fn close(mut ws: TlsWs) {
    let _ = ws.close(None).await;
    let deadline = Instant::now() + Duration::from_secs(2);
    loop {
        let remaining = deadline.saturating_duration_since(Instant::now());
        if remaining.is_zero() {
            return;
        }
        match timeout(remaining, ws.next()).await {
            Err(_) | Ok(None | Some(Ok(Message::Close(_)) | Err(_))) => return,
            Ok(Some(Ok(Message::Ping(p)))) => {
                let _ = ws.send(Message::Pong(p)).await;
            }
            Ok(Some(Ok(_))) => {}
        }
    }
}

/// `client.hello` params for logical client `client_id`, advertising (or not)
/// the `browserExec` capability (REV-2 eligibility, PROTOCOL §5.17), with the
/// client's own host identification (`hostname` / `prettyHostname` /
/// `deviceKind`, mirroring `host.status`).
fn hello(client_id: &str, browser_exec: bool) -> Value {
    json!({
        "clientId": client_id,
        "name": format!("Intent Desktop @ {client_id}"),
        "capabilities": { "browserExec": browser_exec },
        "hostname": format!("{client_id}.local"),
        "prettyHostname": format!("{client_id} (pretty)"),
        "deviceKind": "laptop",
    })
}

/// The REV-2 per-workspace browser-client pin over the secure wire:
/// `client.list` groups live connections per `clientId` with the per-client
/// `browserExec` aggregate (an auxiliary socket without the capability does
/// not mask the eligible one) and mirrors host identification presence-
/// detected; `workspace.getBrowserClient` / `setBrowserClient` read, persist,
/// echo and announce the pin (`workspace:updated { changes: { browserClientId
/// } }`, `browserClientId` on the `Workspace` payload); the documented
/// `-32602` rejections hold and leave the pin untouched; a pinned client that
/// goes away resolves to `null` without falling back; `null` clears the pin.
#[tokio::test]
async fn workspace_browser_client_pin_rpcs_over_secure_wss() {
    let root = scratch_dir();
    let (_daemon, port, cfg) = boot(root.path()).await;

    // Observe client:* so a closed connection's deregistration is awaited on
    // the daemon's own signal rather than on a sleep.
    let mut client_sub = connect(port, cfg.clone()).await;
    let ack = wss_rpc(
        &mut client_sub,
        1,
        "events.subscribe",
        json!({ "eventTypes": ["client:connected", "client:disconnected"] }),
    )
    .await;
    assert!(ack["result"]["subscriptionId"].is_string(), "{ack}");

    let mut a = connect(port, cfg.clone()).await;
    let _ = wss_rpc(&mut a, 1, "client.hello", hello("desktop-a", true)).await;
    let mut b = connect(port, cfg.clone()).await;
    let _ = wss_rpc(&mut b, 1, "client.hello", hello("desktop-b", true)).await;
    // desktop-b's newer auxiliary connection lacks the capability.
    let mut aux = connect(port, cfg.clone()).await;
    let _ = wss_rpc(&mut aux, 1, "client.hello", hello("desktop-b", false)).await;
    for id in ["desktop-a", "desktop-b"] {
        let ev = await_event(&mut client_sub, "client:connected", Duration::from_secs(5)).await;
        assert_eq!(ev["data"]["clientId"], id);
    }

    // client.list — grouped, ordered by first connection, aggregate capability.
    let listed = wss_rpc(&mut a, 2, "client.list", json!({})).await;
    let clients = listed["result"]["clients"]
        .as_array()
        .unwrap_or_else(|| panic!("clients array: {listed}"));
    assert_eq!(clients.len(), 2, "{listed}");
    assert_eq!(clients[0]["clientId"], "desktop-a");
    assert_eq!(clients[0]["connections"], 1);
    assert_eq!(clients[1]["clientId"], "desktop-b");
    assert_eq!(clients[1]["name"], "Intent Desktop @ desktop-b");
    assert_eq!(clients[1]["connections"], 2);
    assert_eq!(clients[1]["transports"], json!(["wss", "wss"]));
    assert_eq!(
        clients[1]["capabilities"],
        json!({ "browserExec": true }),
        "the newer non-capable socket must not mask the eligible one"
    );
    assert!(clients[1]["connectedAt"].is_string());
    assert_eq!(clients[1]["hostname"], "desktop-b.local");
    assert_eq!(clients[1]["prettyHostname"], "desktop-b (pretty)");
    assert_eq!(clients[1]["deviceKind"], "laptop");
    let mut keys: Vec<&str> = clients[1]
        .as_object()
        .unwrap()
        .keys()
        .map(String::as_str)
        .collect();
    keys.sort_unstable();
    assert_eq!(
        keys,
        [
            "capabilities",
            "clientId",
            "connectedAt",
            "connections",
            "deviceKind",
            "hostname",
            "name",
            "prettyHostname",
            "transports"
        ]
    );

    // A hello without host identification lists no host keys (presence-
    // detected, never null).
    let mut bare = connect(port, cfg.clone()).await;
    let _ = wss_rpc(
        &mut bare,
        1,
        "client.hello",
        json!({ "clientId": "bare-c", "capabilities": { "browserExec": false } }),
    )
    .await;
    let listed = wss_rpc(&mut a, 3, "client.list", json!({})).await;
    let bare_entry = listed["result"]["clients"]
        .as_array()
        .unwrap()
        .iter()
        .find(|c| c["clientId"] == "bare-c")
        .unwrap_or_else(|| panic!("bare-c listed: {listed}"));
    for key in ["hostname", "prettyHostname", "deviceKind", "name"] {
        assert!(bare_entry.get(key).is_none(), "{key} omitted: {bare_entry}");
    }
    let ev = await_event(&mut client_sub, "client:connected", Duration::from_secs(5)).await;
    assert_eq!(ev["data"]["clientId"], "bare-c");
    close(bare).await;
    let ev = await_event(
        &mut client_sub,
        "client:disconnected",
        Duration::from_secs(5),
    )
    .await;
    assert_eq!(ev["data"]["clientId"], "bare-c");

    let created = wss_rpc(
        &mut a,
        4,
        "workspace.create",
        json!({ "title": "Pinned browser" }),
    )
    .await;
    let ws_id = created["result"]["workspace"]["id"]
        .as_str()
        .unwrap_or_else(|| panic!("created id: {created}"))
        .to_string();
    assert!(
        created["result"]["workspace"]
            .get("browserClientId")
            .is_none(),
        "unpinned workspaces omit browserClientId: {created}"
    );

    // Unpinned: default source, resolved = first-connected eligible client.
    let got = wss_rpc(
        &mut a,
        5,
        "workspace.getBrowserClient",
        json!({ "workspaceId": ws_id }),
    )
    .await;
    assert_eq!(
        got["result"],
        json!({ "browserClient": {
            "source": "default",
            "resolved": { "clientId": "desktop-a", "name": "Intent Desktop @ desktop-a" }
        } }),
        "{got}"
    );

    let mut sub = connect(port, cfg.clone()).await;
    let ack = wss_rpc(
        &mut sub,
        1,
        "events.subscribe",
        json!({ "eventTypes": ["workspace:updated"], "workspaceId": ws_id }),
    )
    .await;
    assert!(ack["result"]["subscriptionId"].is_string(), "{ack}");

    // Pin desktop-b: the setter echoes the get shape and announces the delta.
    let set = wss_rpc(
        &mut a,
        6,
        "workspace.setBrowserClient",
        json!({ "workspaceId": ws_id, "clientId": "desktop-b" }),
    )
    .await;
    let pinned_state = json!({
        "clientId": "desktop-b",
        "source": "workspace",
        "resolved": { "clientId": "desktop-b", "name": "Intent Desktop @ desktop-b" }
    });
    assert_eq!(
        set["result"],
        json!({ "browserClient": pinned_state }),
        "{set}"
    );
    let ev = await_event(&mut sub, "workspace:updated", Duration::from_secs(5)).await;
    assert_eq!(ev["workspaceId"], ws_id.as_str());
    assert_eq!(
        ev["data"]["changes"],
        json!({ "browserClientId": "desktop-b" })
    );
    let got = wss_rpc(
        &mut a,
        7,
        "workspace.getBrowserClient",
        json!({ "workspaceId": ws_id }),
    )
    .await;
    assert_eq!(got["result"]["browserClient"], pinned_state);
    let ws_row = wss_rpc(&mut a, 8, "workspace.get", json!({ "workspaceId": ws_id })).await;
    assert_eq!(
        ws_row["result"]["workspace"]["browserClientId"],
        "desktop-b"
    );

    // Documented rejections — all -32602, none of them touch the pin.
    let ghost = wss_rpc(
        &mut a,
        9,
        "workspace.setBrowserClient",
        json!({ "workspaceId": ws_id, "clientId": "ghost" }),
    )
    .await;
    assert_eq!(ghost["error"]["code"], -32602, "{ghost}");
    assert!(
        ghost["error"]["message"]
            .as_str()
            .is_some_and(|m| m.contains("ghost")),
        "{ghost}"
    );
    // A connection that never said hello gets a connection-scoped clientId
    // (and `client` row) minted on its first draft write; that id is not a
    // hello'd client and is rejected the same way, pin untouched.
    let mut draft_sub = connect(port, cfg.clone()).await;
    let ack = wss_rpc(
        &mut draft_sub,
        1,
        "events.subscribe",
        json!({ "eventTypes": ["draft:changed"], "workspaceId": ws_id }),
    )
    .await;
    assert!(ack["result"]["subscriptionId"].is_string(), "{ack}");
    let mut anon = connect(port, cfg.clone()).await;
    let set_draft = wss_rpc(
        &mut anon,
        1,
        "drafts.set",
        json!({ "workspaceId": ws_id, "agentId": "agent-1", "text": "half-typed" }),
    )
    .await;
    assert_eq!(set_draft["result"]["ok"], true, "{set_draft}");
    let changed = await_event(&mut draft_sub, "draft:changed", Duration::from_secs(5)).await;
    let minted = changed["data"]["clientId"]
        .as_str()
        .expect("draft:changed names the minted clientId")
        .to_string();
    let draft_only = wss_rpc(
        &mut a,
        10,
        "workspace.setBrowserClient",
        json!({ "workspaceId": ws_id, "clientId": minted }),
    )
    .await;
    assert_eq!(draft_only["error"]["code"], -32602, "{draft_only}");
    assert!(
        draft_only["error"]["message"]
            .as_str()
            .is_some_and(|m| m.contains(&minted)),
        "{draft_only}"
    );
    close(anon).await;
    close(draft_sub).await;
    let chief = wss_rpc(
        &mut a,
        11,
        "workspace.setBrowserClient",
        json!({ "workspaceId": CHIEF_WORKSPACE_ID, "clientId": "desktop-a" }),
    )
    .await;
    assert_eq!(chief["error"]["code"], -32602, "{chief}");
    let missing_param = wss_rpc(
        &mut a,
        12,
        "workspace.setBrowserClient",
        json!({ "workspaceId": ws_id }),
    )
    .await;
    assert_eq!(missing_param["error"]["code"], -32602);
    assert_eq!(
        missing_param["error"]["message"],
        "Missing required parameter: clientId (string | null)"
    );
    let wrong_type = wss_rpc(
        &mut a,
        13,
        "workspace.setBrowserClient",
        json!({ "workspaceId": ws_id, "clientId": 42 }),
    )
    .await;
    assert_eq!(wrong_type["error"]["code"], -32602);
    assert_eq!(
        wrong_type["error"]["message"],
        "Invalid parameter: clientId must be a non-empty string or null"
    );
    for (id, method) in [
        (14, "workspace.getBrowserClient"),
        (15, "workspace.setBrowserClient"),
    ] {
        let unknown = wss_rpc(
            &mut a,
            id,
            method,
            json!({ "workspaceId": "ws-none", "clientId": null }),
        )
        .await;
        assert_eq!(unknown["error"]["code"], -32602, "{unknown}");
        assert_eq!(unknown["error"]["message"], "Workspace not found");
    }
    let got = wss_rpc(
        &mut a,
        16,
        "workspace.getBrowserClient",
        json!({ "workspaceId": ws_id }),
    )
    .await;
    assert_eq!(
        got["result"]["browserClient"], pinned_state,
        "pin untouched"
    );

    // desktop-b goes away entirely: the pin stays and resolves to null — no
    // silent fallback to desktop-a.
    close(b).await;
    close(aux).await;
    let ev = await_event(
        &mut client_sub,
        "client:disconnected",
        Duration::from_secs(5),
    )
    .await;
    assert_eq!(ev["data"]["clientId"], "desktop-b");
    let got = wss_rpc(
        &mut a,
        17,
        "workspace.getBrowserClient",
        json!({ "workspaceId": ws_id }),
    )
    .await;
    assert_eq!(
        got["result"]["browserClient"],
        json!({ "clientId": "desktop-b", "source": "workspace", "resolved": null }),
        "{got}"
    );
    let listed = wss_rpc(&mut a, 18, "client.list", json!({})).await;
    assert_eq!(
        listed["result"]["clients"]
            .as_array()
            .map(|c| c.iter().map(|c| c["clientId"].clone()).collect::<Vec<_>>()),
        Some(vec![json!("desktop-a")]),
        "{listed}"
    );

    // Clearing with null returns the workspace to the default client.
    let cleared = wss_rpc(
        &mut a,
        19,
        "workspace.setBrowserClient",
        json!({ "workspaceId": ws_id, "clientId": null }),
    )
    .await;
    assert_eq!(
        cleared["result"],
        json!({ "browserClient": {
            "source": "default",
            "resolved": { "clientId": "desktop-a", "name": "Intent Desktop @ desktop-a" }
        } }),
        "{cleared}"
    );
    let ev = await_event(&mut sub, "workspace:updated", Duration::from_secs(5)).await;
    assert_eq!(ev["data"]["changes"], json!({ "browserClientId": null }));
    let ws_row = wss_rpc(&mut a, 20, "workspace.get", json!({ "workspaceId": ws_id })).await;
    assert!(
        ws_row["result"]["workspace"]
            .get("browserClientId")
            .is_none(),
        "{ws_row}"
    );
}
