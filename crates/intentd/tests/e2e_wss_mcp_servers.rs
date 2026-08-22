//! WSS end-to-end `mcp.servers.*` remote-probe slice (PROTOCOL §5.22): the UDS
//! analogue in `uds_mcp_servers.rs` ported to the production WebSocket
//! transport (TLS + fingerprint pinning + bearer auth). Boots a real
//! `intentd serve`, points an `http` MCP config at the mock streamable-HTTP
//! fixture, and proves over the wire: create (headers redacted) → toggle
//! enable (daemon-host probe → `running`, no pid, toolCount) → getStatus →
//! `mcp.servers:status-changed` events → update to a dead URL re-probes the
//! NEW config (error-state fix) → update back to the live URL recovers →
//! delete. Gated on `node` + the mock fixture; skips cleanly otherwise.

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
use tokio::io::{AsyncBufReadExt, BufReader};
use tokio::net::{TcpStream, UnixStream};
use tokio::time::timeout;
use tokio_tungstenite::tungstenite::Message;
use tokio_tungstenite::WebSocketStream;
use uuid::Uuid;

/// Fixed 64-hex token, adopted by the daemon via the `INTENTD_AUTH_TOKEN` seam.
const TOKEN: &str = "cdcdcdcdcdcdcdcdcdcdcdcdcdcdcdcdcdcdcdcdcdcdcdcdcdcdcdcdcdcdcdcd";
/// Sensitive header value that must never appear on the wire un-redacted.
const SECRET: &str = "supersecret_header_value_0123456789";

/// Live `intentd serve` process; killed and its data dir removed on drop.
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
    let dir = PathBuf::from("/tmp").join(format!("itd-wss-mcp-{}", &id[..8]));
    std::fs::create_dir_all(&dir).expect("mkdir data dir");
    dir
}

fn spawn_serve(data_dir: &Path, env: &[(&str, &str)]) -> Child {
    let log = std::fs::File::create(data_dir.join("daemon.log")).expect("create daemon log");
    let workspaces_dir = data_dir.join("workspaces");
    std::fs::create_dir_all(&workspaces_dir).expect("mkdir hermetic workspaces dir");
    let secrets_file = data_dir.join("secrets.json");
    common::enable_ws_api(data_dir);
    let mut cmd = Command::new(env!("CARGO_BIN_EXE_intentd"));
    cmd.arg("serve")
        .env("INTENTD_DATA_DIR", data_dir)
        .env("INTENTD_WORKSPACES_DIR", &workspaces_dir)
        .env("INTENTD_SECRETS_FILE", &secrets_file)
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
async fn connect_ws(
    port: u16,
    cfg: Arc<ClientConfig>,
) -> WebSocketStream<tokio_rustls::client::TlsStream<TcpStream>> {
    let url = format!("wss://localhost:{port}/ws?token={TOKEN}");
    common::wss_connect_with_retry(port, cfg, &url).await
}

/// Send one JSON-RPC frame and return the `result` whose id matches; any
/// out-of-band notifications (`events.event`) are ignored. Asserts the §1
/// response envelope (jsonrpc/id, no error).
async fn wss_rpc<S>(ws: &mut WebSocketStream<S>, id: i64, method: &str, params: Value) -> Value
where
    S: tokio::io::AsyncRead + tokio::io::AsyncWrite + Unpin,
{
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
                    assert_eq!(v["jsonrpc"], json!("2.0"), "envelope jsonrpc");
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

/// Read `mcp.servers:status-changed` events off a subscriber connection until
/// one for `server_id` reports `state`, returning its status payload.
async fn wait_for_state<S>(ws: &mut WebSocketStream<S>, server_id: &str, state: &str) -> Value
where
    S: tokio::io::AsyncRead + tokio::io::AsyncWrite + Unpin,
{
    for _ in 0..20 {
        let next = timeout(common::rpc_read_timeout(), ws.next())
            .await
            .expect("wss event timed out");
        match next {
            Some(Ok(Message::Text(text))) => {
                let v: Value = serde_json::from_str(&text).expect("json frame");
                if v["method"] != "events.event" {
                    continue;
                }
                let event = &v["params"]["event"];
                assert_eq!(event["type"], "mcp.servers:status-changed");
                if event["data"]["serverId"] == server_id
                    && event["data"]["status"]["state"] == state
                {
                    return event["data"]["status"].clone();
                }
            }
            Some(Ok(Message::Ping(p))) => {
                let _ = ws.send(Message::Pong(p)).await;
            }
            Some(Ok(_)) => {}
            other => panic!("expected text frame, got {other:?}"),
        }
    }
    panic!("never observed mcp status-changed serverId={server_id} state={state}");
}

fn node_available() -> bool {
    Command::new("node")
        .arg("--version")
        .stdout(Stdio::null())
        .stderr(Stdio::null())
        .status()
        .is_ok_and(|s| s.success())
}

/// Spawn the mock MCP server in `--http` mode and return (child, base url).
/// The fixture announces its ephemeral port as `PORT=<n>` on stdout.
async fn spawn_http_fixture(script: &str) -> (tokio::process::Child, String) {
    let mut child = tokio::process::Command::new("node")
        .arg(script)
        .arg("--http")
        .stdout(Stdio::piped())
        .stderr(Stdio::null())
        .kill_on_drop(true)
        .spawn()
        .expect("spawn mock http mcp server");
    let stdout = child.stdout.take().expect("fixture stdout");
    let mut lines = BufReader::new(stdout).lines();
    let line = timeout(Duration::from_secs(10), lines.next_line())
        .await
        .expect("timed out waiting for fixture PORT line")
        .expect("read fixture stdout")
        .expect("fixture exited before announcing PORT");
    let port = line
        .strip_prefix("PORT=")
        .expect("fixture PORT line")
        .trim()
        .to_string();
    (child, format!("http://127.0.0.1:{port}"))
}

/// Remote `http` MCP lifecycle over the production WSS transport: probe →
/// status/events → error-state update re-probes the new config → recovery.
#[tokio::test]
async fn mcp_servers_remote_probe_over_wss() {
    if !node_available() {
        eprintln!("skipping mcp.servers WSS E2E: node not on PATH");
        return;
    }
    let script = concat!(
        env!("CARGO_MANIFEST_DIR"),
        "/tests/fixtures/mock-mcp-server.mjs"
    );
    if !PathBuf::from(script).exists() {
        eprintln!("skipping mcp.servers WSS E2E: fixture not found at {script}");
        return;
    }
    let (_fixture, base_url) = spawn_http_fixture(script).await;

    let data_dir = temp_data_dir();
    let env: [(&str, &str); 2] = [("INTENTD_AUTH_TOKEN", TOKEN), ("INTENTD_TCP_PORT", "0")];
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

    // SUBSCRIBER conn — events.subscribe BEFORE any lifecycle action.
    let mut sub = connect_ws(port, cfg.clone()).await;
    let sub_resp = wss_rpc(
        &mut sub,
        1,
        "events.subscribe",
        json!({ "eventTypes": ["mcp.servers:status-changed"] }),
    )
    .await;
    assert!(
        sub_resp["subscriptionId"].is_string(),
        "subscriptionId in subscribe result: {sub_resp}"
    );

    // RPC conn.
    let mut rpc = connect_ws(port, cfg.clone()).await;

    // list — empty catalog.
    let list = wss_rpc(&mut rpc, 2, "mcp.servers.list", json!({})).await;
    assert_eq!(list["servers"].as_array().expect("servers array").len(), 0);

    // create — http transport with a sensitive header; response redacts it.
    let created = wss_rpc(
        &mut rpc,
        3,
        "mcp.servers.create",
        json!({ "config": {
            "name": "Mock HTTP",
            "transport": "http",
            "url": base_url,
            "headers": { "Authorization": SECRET },
            "enabled": false,
        } }),
    )
    .await;
    let server_id = created["server"]["id"].as_str().expect("id").to_string();
    assert_eq!(created["server"]["transport"], "http");
    assert!(
        !serde_json::to_string(&created).unwrap().contains(SECRET),
        "secret leaked in create result"
    );

    // toggle enable → daemon-host probe; running, no pid, toolCount 2.
    let toggled = wss_rpc(
        &mut rpc,
        4,
        "mcp.servers.toggle",
        json!({ "serverId": server_id, "enabled": true }),
    )
    .await;
    assert_eq!(toggled["status"]["state"], "running");
    assert!(
        toggled["status"]["pid"].is_null(),
        "remote servers have no pid"
    );
    assert_eq!(toggled["status"]["toolCount"], json!(2));
    let ev = wait_for_state(&mut sub, &server_id, "running").await;
    assert_eq!(ev["toolCount"], json!(2));

    // getStatus → live running snapshot.
    let got = wss_rpc(
        &mut rpc,
        5,
        "mcp.servers.getStatus",
        json!({ "serverId": server_id }),
    )
    .await;
    assert_eq!(got["status"]["state"], "running");

    // update to a dead URL: the error-state fix — an update must re-probe the
    // NEW config immediately (running → error), not wait for a health tick.
    let dead = std::net::TcpListener::bind("127.0.0.1:0").unwrap();
    let dead_url = format!("http://{}", dead.local_addr().unwrap());
    drop(dead);
    let updated = wss_rpc(
        &mut rpc,
        6,
        "mcp.servers.update",
        json!({ "serverId": server_id, "config": {
            "name": "Mock HTTP",
            "transport": "http",
            "url": dead_url,
            "enabled": true,
        } }),
    )
    .await;
    assert_eq!(updated["server"]["id"], json!(server_id));
    let ev = wait_for_state(&mut sub, &server_id, "error").await;
    assert!(
        ev["lastError"]
            .as_str()
            .expect("lastError")
            .contains("unreachable from daemon host"),
        "got: {}",
        ev["lastError"]
    );
    let got = wss_rpc(
        &mut rpc,
        7,
        "mcp.servers.getStatus",
        json!({ "serverId": server_id }),
    )
    .await;
    assert_eq!(got["status"]["state"], "error");

    // update back to the live URL: an error-state server must also re-probe
    // (the reviewer-reported bug) and recover to running.
    let _ = wss_rpc(
        &mut rpc,
        8,
        "mcp.servers.update",
        json!({ "serverId": server_id, "config": {
            "name": "Mock HTTP",
            "transport": "http",
            "url": base_url,
            "enabled": true,
        } }),
    )
    .await;
    let _ = wait_for_state(&mut sub, &server_id, "running").await;
    let got = wss_rpc(
        &mut rpc,
        9,
        "mcp.servers.getStatus",
        json!({ "serverId": server_id }),
    )
    .await;
    assert_eq!(got["status"]["state"], "running");
    assert_eq!(got["status"]["toolCount"], json!(2));

    // delete — removes the definition; list empty again.
    let deleted = wss_rpc(
        &mut rpc,
        10,
        "mcp.servers.delete",
        json!({ "serverId": server_id }),
    )
    .await;
    assert_eq!(deleted["success"], json!(true));
    let list = wss_rpc(&mut rpc, 11, "mcp.servers.list", json!({})).await;
    assert_eq!(list["servers"].as_array().expect("servers array").len(), 0);
}
