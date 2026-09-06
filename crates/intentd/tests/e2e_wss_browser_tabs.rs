//! End-to-end WSS coverage for the daemon-owned browser tab registry (REV-2
//! Model 2 & 6): `browser.upsertTab` / `browser.removeTab` /
//! `browser.syncTabs` (host-only reports keyed by the connection's
//! `client.hello` identity) and `browser.listTabs` (any client; decorated with
//! `hostName` / `hostConnected` from the live reverse registry), plus the
//! `browser:tab-opened` / `browser:tab-updated` / `browser:tab-closed`
//! change events.
//!
//! Drives the real WSS transport against a live `intentd serve` (WSS listener
//! enabled via config): TLS with a pinned fingerprint, bearer-token auth, and
//! the production upgrade path — the same harness as `e2e_wss_browser_exec.rs`.

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
use tokio::time::{timeout, Instant};
use tokio_tungstenite::tungstenite::Message;
use uuid::Uuid;

use common::TlsWs;

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
    let dir = PathBuf::from("/tmp").join(format!("itd-wss-tabs-{}", &id[..8]));
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

struct Fixture {
    _daemon: Daemon,
    port: u16,
    cfg: Arc<ClientConfig>,
}

async fn boot() -> Fixture {
    let data_dir = temp_data_dir();
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
    Fixture {
        _daemon: daemon,
        port,
        cfg: client_config(&fingerprint),
    }
}

/// Pinned-TLS, bearer-authenticated WebSocket to the daemon's `/ws`.
async fn connect(fx: &Fixture) -> TlsWs {
    let url = format!("wss://localhost:{}/ws?token={TOKEN}", fx.port);
    common::wss_connect_with_retry(fx.port, fx.cfg.clone(), &url).await
}

/// The JSON-RPC 2.0 response envelope contract (docs/protocol §1): `jsonrpc`
/// is exactly `"2.0"`, `id` echoes the request, and exactly one of `result` /
/// `error` is present — an error carrying an integer `code` and a string
/// `message` (§9), never both members and never neither.
fn assert_envelope(res: &Value, id: i64, method: &str) {
    let obj = res
        .as_object()
        .unwrap_or_else(|| panic!("{method}: response is not an object: {res}"));
    assert_eq!(obj.get("jsonrpc"), Some(&json!("2.0")), "{method}: {res}");
    assert_eq!(obj.get("id"), Some(&json!(id)), "{method}: {res}");
    match (obj.get("result"), obj.get("error")) {
        (Some(_), None) => {}
        (None, Some(err)) => {
            assert!(err["code"].is_i64(), "{method}: error.code: {res}");
            assert!(err["message"].is_string(), "{method}: error.message: {res}");
        }
        (Some(_), Some(_)) => panic!("{method}: both result and error: {res}"),
        (None, None) => panic!("{method}: neither result nor error: {res}"),
    }
}

/// One bounded JSON-RPC round-trip on `ws` (pings answered inline; the read
/// budget is a total budget across all frames). Every response is checked
/// against [`assert_envelope`] before it is returned.
async fn wss_rpc(ws: &mut TlsWs, id: i64, method: &str, params: Value) -> Value {
    let req = json!({ "jsonrpc": "2.0", "id": id, "method": method, "params": params });
    ws.send(Message::Text(req.to_string().into()))
        .await
        .unwrap();
    let deadline = Instant::now() + common::rpc_read_timeout();
    loop {
        let remaining = deadline.saturating_duration_since(Instant::now());
        assert!(
            !remaining.is_zero(),
            "wss_rpc timed out waiting for response to id={id} method={method}"
        );
        match timeout(remaining, ws.next()).await.unwrap_or_else(|_| {
            panic!("wss_rpc timed out waiting for response to id={id} method={method}")
        }) {
            Some(Ok(Message::Text(text))) => {
                let v: Value = serde_json::from_str(&text).expect("json");
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

/// Pump an `events.subscribe` connection until an `events.event` whose type
/// starts with `browser:` arrives (bounded). `None` when `dur` elapses.
async fn next_tab_event(ws: &mut TlsWs, dur: Duration) -> Option<Value> {
    let deadline = Instant::now() + dur;
    loop {
        let remaining = deadline.saturating_duration_since(Instant::now());
        if remaining.is_zero() {
            return None;
        }
        match timeout(remaining, ws.next()).await {
            Err(_) => return None,
            Ok(Some(Ok(Message::Text(text)))) => {
                let v: Value = serde_json::from_str(&text).expect("json frame");
                if v["method"] == "events.event"
                    && v["params"]["event"]["type"]
                        .as_str()
                        .is_some_and(|t| t.starts_with("browser:"))
                {
                    return Some(v["params"]["event"].clone());
                }
            }
            Ok(Some(Ok(Message::Ping(p)))) => {
                let _ = ws.send(Message::Pong(p)).await;
            }
            Ok(Some(Ok(_))) => {}
            Ok(other) => panic!("unexpected ws frame: {other:?}"),
        }
    }
}

/// Close `ws` (draining the server's close handshake) and drop it.
async fn close_ws(mut ws: TlsWs) {
    let _ = ws.close(None).await;
    let deadline = Instant::now() + Duration::from_secs(2);
    loop {
        let remaining = deadline.saturating_duration_since(Instant::now());
        if remaining.is_zero() {
            break;
        }
        match timeout(remaining, ws.next()).await {
            Err(_) | Ok(None | Some(Ok(Message::Close(_)) | Err(_))) => break,
            Ok(Some(Ok(Message::Ping(p)))) => {
                let _ = ws.send(Message::Pong(p)).await;
            }
            Ok(Some(Ok(_))) => {}
        }
    }
    drop(ws);
}

/// Poll `browser.listTabs` over `ws` until the workspace's single tab reports
/// `hostConnected == false` (the daemon deregisters the closed host
/// connection asynchronously); returns the final list result.
async fn await_host_offline(ws: &mut TlsWs, first_id: i64, workspace_id: &str) -> Value {
    let deadline = Instant::now() + common::test_timeout(Duration::from_secs(5));
    let mut id = first_id;
    loop {
        let res = wss_rpc(
            ws,
            id,
            "browser.listTabs",
            json!({ "workspaceId": workspace_id }),
        )
        .await;
        let tabs = res["result"]["tabs"].as_array().expect("tabs");
        if tabs.len() == 1 && tabs[0]["hostConnected"] == false {
            return res;
        }
        assert!(
            Instant::now() < deadline,
            "host never listed as disconnected: {res}"
        );
        id += 1;
        tokio::time::sleep(Duration::from_millis(25)).await;
    }
}

fn hello(client_id: &str) -> Value {
    json!({
        "clientId": client_id,
        "name": format!("Intent Desktop @ {client_id}"),
        "capabilities": { "browserExec": true },
    })
}

fn tab(tab_id: &str, url: &str) -> Value {
    json!({ "tabId": tab_id, "url": url, "title": "Page", "visibility": "visible" })
}

#[tokio::test]
async fn browser_tab_registry_round_trip_over_wss() {
    let fx = boot().await;

    // Bootstrap a workspace and an event subscriber.
    let mut rpc = connect(&fx).await;
    let created = wss_rpc(
        &mut rpc,
        1,
        "workspace.create",
        json!({ "title": "Tabs", "branch": "main", "skipWorktree": true }),
    )
    .await;
    let ws_id = created["result"]["workspace"]["id"]
        .as_str()
        .unwrap_or_else(|| panic!("workspace id: {created}"))
        .to_string();
    let mut sub = connect(&fx).await;
    let ack = wss_rpc(
        &mut sub,
        1,
        "events.subscribe",
        json!({
            "eventTypes": ["browser:tab-opened", "browser:tab-updated", "browser:tab-closed"],
            "workspaceId": ws_id,
        }),
    )
    .await;
    assert!(ack.get("error").is_none(), "subscribe failed: {ack}");

    // Host A and viewer B are hello'd logical clients; `rpc` never says hello.
    let mut host = connect(&fx).await;
    let _ = wss_rpc(&mut host, 1, "client.hello", hello("desktop-a")).await;
    let mut viewer = connect(&fx).await;
    let _ = wss_rpc(&mut viewer, 1, "client.hello", hello("desktop-b")).await;

    // 1. Host reports a new tab → `{ tab }` bound to the caller + tab-opened.
    let res = wss_rpc(
        &mut host,
        2,
        "browser.upsertTab",
        json!({ "workspaceId": ws_id, "tab": tab("tab-1", "https://a.test/") }),
    )
    .await;
    assert_eq!(res["jsonrpc"], "2.0");
    let opened = &res["result"]["tab"];
    assert_eq!(opened["tabId"], "tab-1", "{res}");
    assert_eq!(opened["workspaceId"], ws_id);
    assert_eq!(opened["hostClientId"], "desktop-a");
    assert_eq!(opened["url"], "https://a.test/");
    assert_eq!(opened["title"], "Page");
    assert_eq!(opened["visibility"], "visible");
    assert!(opened["createdAt"].is_string() && opened["updatedAt"].is_string());
    assert!(opened.get("requestedUrl").is_none());
    let ev = next_tab_event(&mut sub, Duration::from_secs(2))
        .await
        .expect("tab-opened event");
    assert_eq!(ev["type"], "browser:tab-opened");
    assert_eq!(ev["workspaceId"], ws_id);
    assert_eq!(ev["data"]["tab"], *opened);
    assert!(ev["data"].get("changes").is_none());
    assert_eq!(
        ev["actor"],
        json!({ "type": "user", "id": "desktop-a" }),
        "attributed to the reporting host's clientId: {ev}"
    );

    // 2. Any client lists the tab with live host presence.
    let res = wss_rpc(
        &mut viewer,
        2,
        "browser.listTabs",
        json!({ "workspaceId": ws_id }),
    )
    .await;
    let tabs = res["result"]["tabs"].as_array().expect("tabs");
    assert_eq!(tabs.len(), 1);
    assert_eq!(tabs[0]["tabId"], "tab-1");
    assert_eq!(tabs[0]["hostClientId"], "desktop-a");
    assert_eq!(tabs[0]["hostConnected"], true);
    assert_eq!(tabs[0]["hostName"], "Intent Desktop @ desktop-a");
    let res = wss_rpc(
        &mut rpc,
        2,
        "browser.listTabs",
        json!({ "workspaceId": ws_id }),
    )
    .await;
    assert_eq!(res["result"]["tabs"].as_array().map(Vec::len), Some(1));

    // 3. A navigation report → tab-updated with the field-wise `changes`;
    //    an identical re-report changes nothing and emits nothing.
    let res = wss_rpc(
        &mut host,
        3,
        "browser.upsertTab",
        json!({ "workspaceId": ws_id, "tab": tab("tab-1", "https://a.test/next") }),
    )
    .await;
    assert_eq!(res["result"]["tab"]["url"], "https://a.test/next");
    let ev = next_tab_event(&mut sub, Duration::from_secs(2))
        .await
        .expect("tab-updated event");
    assert_eq!(ev["type"], "browser:tab-updated");
    assert_eq!(ev["data"]["tab"]["url"], "https://a.test/next");
    assert_eq!(
        ev["data"]["changes"],
        json!({ "url": "https://a.test/next" })
    );
    let res = wss_rpc(
        &mut host,
        4,
        "browser.upsertTab",
        json!({ "workspaceId": ws_id, "tab": tab("tab-1", "https://a.test/next") }),
    )
    .await;
    assert!(res.get("error").is_none(), "{res}");
    assert!(
        next_tab_event(&mut sub, Duration::from_millis(300))
            .await
            .is_none(),
        "no event for an unchanged report"
    );

    // 4. Host-only: another client and an un-hello'd connection are refused.
    let res = wss_rpc(
        &mut viewer,
        3,
        "browser.upsertTab",
        json!({ "workspaceId": ws_id, "tab": tab("tab-1", "https://b.test/") }),
    )
    .await;
    assert_eq!(res["error"]["code"], -32602, "{res}");
    assert_eq!(res["error"]["data"]["code"], "invalid-params");
    let res = wss_rpc(
        &mut viewer,
        4,
        "browser.removeTab",
        json!({ "tabId": "tab-1" }),
    )
    .await;
    assert_eq!(res["error"]["code"], -32602, "{res}");
    // A tab is bound to the workspace that created it: the host re-reporting
    // it under another workspace is refused, and nothing is published.
    let other = wss_rpc(
        &mut rpc,
        7,
        "workspace.create",
        json!({ "title": "Other", "branch": "main", "skipWorktree": true }),
    )
    .await;
    let other_id = other["result"]["workspace"]["id"]
        .as_str()
        .unwrap_or_else(|| panic!("workspace id: {other}"));
    let res = wss_rpc(
        &mut host,
        9,
        "browser.upsertTab",
        json!({ "workspaceId": other_id, "tab": tab("tab-1", "https://a.test/next") }),
    )
    .await;
    assert_eq!(res["error"]["code"], -32602, "{res}");
    assert!(res["error"]["message"]
        .as_str()
        .unwrap()
        .contains("do not move between workspaces"));
    let res = wss_rpc(
        &mut host,
        10,
        "browser.syncTabs",
        json!({ "tabs": [
            { "tabId": "tab-1", "workspaceId": other_id, "url": "https://a.test/next", "title": "Page" }
        ] }),
    )
    .await;
    assert_eq!(res["error"]["code"], -32602, "{res}");
    assert!(
        next_tab_event(&mut sub, Duration::from_millis(300))
            .await
            .is_none(),
        "no event for a rejected workspace change"
    );
    let res = wss_rpc(
        &mut viewer,
        7,
        "browser.listTabs",
        json!({ "workspaceId": ws_id }),
    )
    .await;
    assert_eq!(res["result"]["tabs"][0]["workspaceId"], ws_id, "{res}");
    let res = wss_rpc(
        &mut viewer,
        8,
        "browser.listTabs",
        json!({ "workspaceId": other_id }),
    )
    .await;
    assert_eq!(res["result"], json!({ "tabs": [] }));
    let res = wss_rpc(
        &mut rpc,
        3,
        "browser.upsertTab",
        json!({ "workspaceId": ws_id, "tab": tab("tab-x", "https://x.test/") }),
    )
    .await;
    assert_eq!(res["error"]["code"], -32602, "{res}");
    assert!(res["error"]["message"]
        .as_str()
        .unwrap()
        .contains("client.hello"));
    // A `drafts.*` write mints a connection `clientId` without a handshake
    // (§5.16); that lazily minted binding must not qualify the connection as
    // a tab host — only `client.hello` does.
    let res = wss_rpc(
        &mut rpc,
        4,
        "drafts.clear",
        json!({ "workspaceId": ws_id, "agentId": "__initializer__" }),
    )
    .await;
    assert_eq!(res["result"]["ok"], true, "{res}");
    let res = wss_rpc(
        &mut rpc,
        5,
        "browser.upsertTab",
        json!({ "workspaceId": ws_id, "tab": tab("tab-x", "https://x.test/") }),
    )
    .await;
    assert_eq!(
        res["error"]["code"], -32602,
        "drafts-minted clientId must not bypass the hello gate: {res}"
    );
    assert!(res["error"]["message"]
        .as_str()
        .unwrap()
        .contains("client.hello"));
    let res = wss_rpc(
        &mut rpc,
        6,
        "browser.syncTabs",
        json!({ "tabs": [{ "tabId": "tab-x", "workspaceId": ws_id, "url": "https://x.test/" }] }),
    )
    .await;
    assert_eq!(res["error"]["code"], -32602, "{res}");
    let res = wss_rpc(
        &mut rpc,
        8,
        "browser.removeTab",
        json!({ "tabId": "tab-1" }),
    )
    .await;
    assert_eq!(
        res["error"]["code"], -32602,
        "drafts-minted clientId must not close a hello'd host's tab: {res}"
    );
    assert!(res["error"]["message"]
        .as_str()
        .unwrap()
        .contains("client.hello"));

    // 5. Snapshot reconciliation: tab-1 unchanged, tab-2 new → opened; a
    //    later snapshot without tab-2 closes it.
    let res = wss_rpc(
        &mut host,
        5,
        "browser.syncTabs",
        json!({ "tabs": [
            { "tabId": "tab-1", "workspaceId": ws_id, "url": "https://a.test/next", "title": "Page" },
            { "tabId": "tab-2", "workspaceId": ws_id, "url": "https://a.test/two" }
        ] }),
    )
    .await;
    assert_eq!(res["result"], json!({ "drop": [] }), "{res}");
    let ev = next_tab_event(&mut sub, Duration::from_secs(2))
        .await
        .expect("tab-opened for tab-2");
    assert_eq!(ev["type"], "browser:tab-opened");
    assert_eq!(ev["data"]["tab"]["tabId"], "tab-2");
    let res = wss_rpc(
        &mut host,
        6,
        "browser.syncTabs",
        json!({ "tabs": [
            { "tabId": "tab-1", "workspaceId": ws_id, "url": "https://a.test/next", "title": "Page" }
        ] }),
    )
    .await;
    assert_eq!(res["result"], json!({ "drop": [] }));
    let ev = next_tab_event(&mut sub, Duration::from_secs(2))
        .await
        .expect("tab-closed for tab-2");
    assert_eq!(ev["type"], "browser:tab-closed");
    assert_eq!(ev["data"]["tab"]["tabId"], "tab-2");

    // 6. Host close report → row gone + tab-closed.
    let res = wss_rpc(
        &mut host,
        7,
        "browser.removeTab",
        json!({ "tabId": "tab-1" }),
    )
    .await;
    assert_eq!(res["result"], json!({ "ok": true }), "{res}");
    let ev = next_tab_event(&mut sub, Duration::from_secs(2))
        .await
        .expect("tab-closed for tab-1");
    assert_eq!(ev["type"], "browser:tab-closed");
    assert_eq!(ev["data"]["tab"]["tabId"], "tab-1");
    let res = wss_rpc(
        &mut viewer,
        5,
        "browser.listTabs",
        json!({ "workspaceId": ws_id }),
    )
    .await;
    assert_eq!(res["result"], json!({ "tabs": [] }));

    // 7. A tab whose host went offline lists as disconnected, without a name.
    let _ = wss_rpc(
        &mut host,
        8,
        "browser.upsertTab",
        json!({ "workspaceId": ws_id, "tab": tab("tab-3", "https://a.test/three") }),
    )
    .await;
    close_ws(host).await;
    let res = await_host_offline(&mut viewer, 6, &ws_id).await;
    let tabs = res["result"]["tabs"].as_array().expect("tabs");
    assert_eq!(tabs.len(), 1);
    assert_eq!(tabs[0]["tabId"], "tab-3");
    assert_eq!(tabs[0]["hostConnected"], false);
    assert!(tabs[0].get("hostName").is_none(), "{res}");
}
