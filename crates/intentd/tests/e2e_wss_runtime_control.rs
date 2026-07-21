//! WSS end-to-end runtime listener control: prove the runtime WS listener toggle
//! over a real WSS connection per packages/intentd/AGENTS.md (enable via settings.update
//! over UDS → connect over WSS → verify RPCs work → disable over UDS → verify listener
//! stops and TCP clients are refused from disabling per the guard).

#![cfg(unix)]

mod common;

use std::net::Ipv4Addr;
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
use tokio_rustls::TlsConnector;
use tokio_tungstenite::tungstenite::Message;
use tokio_tungstenite::WebSocketStream;
use uuid::Uuid;

const TOKEN: &str = "efefefefefefefefefefefefefefefefefefefefefefefefefefefefefefefef";

fn free_port() -> u16 {
    use std::net::TcpListener;
    TcpListener::bind(("127.0.0.1", 0))
        .expect("bind")
        .local_addr()
        .expect("addr")
        .port()
}

struct Daemon {
    child: Child,
    data_dir: PathBuf,
    /// If false, skip data_dir cleanup in Drop (for tests that reuse the same data_dir)
    cleanup_data_dir: bool,
}

impl Drop for Daemon {
    fn drop(&mut self) {
        let _ = self.child.kill();
        let _ = self.child.wait();
        if self.cleanup_data_dir {
            let log_path = self.data_dir.join("daemon.log");
            if let Ok(log) = std::fs::read_to_string(&log_path) {
                eprintln!("=== DAEMON LOG ===\n{}\n=== END LOG ===", log);
            }
            let _ = std::fs::remove_dir_all(&self.data_dir);
        }
    }
}

fn temp_data_dir() -> PathBuf {
    let id = Uuid::new_v4().simple().to_string();
    let dir = PathBuf::from("/tmp").join(format!("itd-wss-runtime-ctrl-{}", &id[..8]));
    std::fs::create_dir_all(&dir).expect("mkdir data dir");
    dir
}

fn spawn_serve(data_dir: &Path, listen: &str, env: &[(&str, &str)]) -> Child {
    use std::fs::OpenOptions;
    std::fs::create_dir_all(data_dir).expect("mkdir data dir");
    // Append to daemon.log instead of truncating, so multi-boot tests preserve all logs
    let log = OpenOptions::new()
        .create(true)
        .append(true)
        .open(data_dir.join("daemon.log"))
        .expect("open daemon log");
    let workspaces_dir = data_dir.join("workspaces");
    std::fs::create_dir_all(&workspaces_dir).expect("mkdir hermetic workspaces dir");
    let mut cmd = Command::new(env!("CARGO_BIN_EXE_intentd"));
    cmd.arg("serve")
        .arg("--listen")
        .arg(listen)
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

async fn tls_connect(
    port: u16,
    cfg: Arc<ClientConfig>,
) -> tokio_rustls::client::TlsStream<TcpStream> {
    let tcp = TcpStream::connect((Ipv4Addr::LOCALHOST, port))
        .await
        .expect("tcp connect");
    let name = ServerName::try_from("localhost").unwrap();
    TlsConnector::from(cfg)
        .connect(name, tcp)
        .await
        .expect("tls connect")
}

async fn connect_ws(
    port: u16,
    cfg: Arc<ClientConfig>,
) -> WebSocketStream<tokio_rustls::client::TlsStream<TcpStream>> {
    let tls = tls_connect(port, cfg).await;
    let url = format!("wss://localhost:{port}/ws?token={TOKEN}");
    let (ws, _resp) = tokio_tungstenite::client_async(url, tls)
        .await
        .expect("ws handshake");
    ws
}

async fn wss_rpc<S>(ws: &mut WebSocketStream<S>, id: i64, method: &str, params: Value) -> Value
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

/// Runtime WSS listener control: enable via settings.update over UDS → connect over WSS
/// → verify RPCs work → disable over UDS → verify listener stops and new connections fail.
#[tokio::test]
async fn runtime_ws_listener_toggle_over_wss() {
    let data_dir = temp_data_dir();
    let env: [(&str, &str); 2] = [("INTENTD_AUTH_TOKEN", TOKEN), ("INTENTD_TCP_PORT", "0")];
    // Start daemon with both UDS and TCP (--listen both)
    let child = spawn_serve(&data_dir, "both", &env);
    let _daemon = Daemon {
        child,
        data_dir: data_dir.clone(),
        cleanup_data_dir: true,
    };
    let socket = data_dir.join("intentd.sock");
    assert!(await_uds(&socket).await, "daemon did not start");

    // Get the actual bound port from system.status
    let status = uds_rpc(&socket, 999, "system.status", json!({})).await;
    let port_value = status["result"]["port"].as_u64().expect("port");

    // Persist the ephemeral port to the setting so re-enable uses the same port
    let set_port = uds_rpc(
        &socket,
        1,
        "settings.update",
        json!({ "changes": [{ "path": "server.wsApi.port", "value": port_value }] }),
    )
    .await;
    assert!(
        set_port.get("error").is_none(),
        "settings.update port should succeed: {set_port}"
    );

    // Verify initial system.status shows the WSS listener (started at boot)
    let status = uds_rpc(&socket, 2, "system.status", json!({})).await;
    let initial_port = status["result"]["port"]
        .as_u64()
        .expect("port should be set at boot") as u16;
    let fingerprint = status["result"]["fingerprint"]
        .as_str()
        .expect("fingerprint should be set")
        .to_string();

    // Connect over WSS and verify RPCs work
    let cfg = client_config(&fingerprint);
    let mut ws = connect_ws(initial_port, cfg.clone()).await;
    let sub_resp = wss_rpc(
        &mut ws,
        10,
        "events.subscribe",
        json!({ "eventTypes": ["agent:*"] }),
    )
    .await;
    assert!(
        sub_resp.get("error").is_none(),
        "events.subscribe over WSS should work: {sub_resp}"
    );
    assert!(
        sub_resp["result"]["subscriptionId"].is_string(),
        "subscriptionId should be returned"
    );

    // Disable the WSS listener via settings.update over UDS (not over WSS to avoid self-termination)
    let disable = uds_rpc(
        &socket,
        3,
        "settings.update",
        json!({ "changes": [{ "path": "server.wsApi.enabled", "value": false }] }),
    )
    .await;
    assert!(
        disable.get("error").is_none(),
        "settings.update disable should succeed: {disable}"
    );

    // Give the listener a moment to stop
    tokio::time::sleep(Duration::from_millis(500)).await;

    // Verify system.status shows no listener
    let status = uds_rpc(&socket, 4, "system.status", json!({})).await;
    assert!(
        status["result"]["port"].is_null(),
        "port should be null after disable"
    );

    // Verify new WSS connections are refused (TCP listener should be closed)
    let connect_result = timeout(
        Duration::from_secs(2),
        TcpStream::connect((Ipv4Addr::LOCALHOST, initial_port)),
    )
    .await;
    assert!(
        connect_result.is_err() || connect_result.unwrap().is_err(),
        "TCP connection should fail after listener is stopped"
    );

    // Re-enable the WSS listener via settings.update over UDS
    let enable = uds_rpc(
        &socket,
        5,
        "settings.update",
        json!({ "changes": [{ "path": "server.wsApi.enabled", "value": true }] }),
    )
    .await;
    assert!(
        enable.get("error").is_none(),
        "settings.update enable should succeed: {enable}"
    );

    // Give the listener a moment to start
    tokio::time::sleep(Duration::from_millis(500)).await;

    // Verify system.status shows the WSS listener again
    let status = uds_rpc(&socket, 6, "system.status", json!({})).await;
    let new_port = status["result"]["port"]
        .as_u64()
        .expect("port should be set after re-enable") as u16;
    assert_eq!(
        new_port, initial_port,
        "re-enable should bind the same port (persisted in server.wsApi.port)"
    );

    // Connect over WSS again and verify RPCs work
    let mut ws2 = connect_ws(new_port, cfg.clone()).await;
    let sub_resp2 = wss_rpc(
        &mut ws2,
        20,
        "events.subscribe",
        json!({ "eventTypes": ["agent:*"] }),
    )
    .await;
    assert!(
        sub_resp2.get("error").is_none(),
        "events.subscribe over WSS should work after re-enable: {sub_resp2}"
    );
    assert!(
        sub_resp2["result"]["subscriptionId"].is_string(),
        "subscriptionId should be returned after re-enable"
    );
}

/// Regression test: boot with --listen uds + persisted server.wsApi.enabled=true
/// should auto-start the WSS listener. This tests the sidecar/packaged posture
/// where the daemon is spawned with --listen uds but the user has previously
/// enabled the WSS listener via the UI toggle. Before the fix, the persisted
/// setting was ignored at boot and the listener stayed down until manual toggle.
#[tokio::test]
async fn persisted_wss_enabled_auto_starts_at_boot_uds_mode() {
    let data_dir = temp_data_dir();
    let port_s = free_port().to_string();
    let env: [(&str, &str); 2] = [("INTENTD_AUTH_TOKEN", TOKEN), ("INTENTD_TCP_PORT", &port_s)];

    // STEP 1: Boot with --listen uds, persist server.wsApi.enabled=true
    let child = spawn_serve(&data_dir, "uds", &env);
    let mut _daemon = Daemon {
        child,
        data_dir: data_dir.clone(),
        cleanup_data_dir: false, // Don't cleanup - we'll reuse this data_dir for second boot
    };
    let socket = data_dir.join("intentd.sock");
    assert!(await_uds(&socket).await, "daemon did not start");

    // INTENTD_TCP_PORT pins server.wsApi.port at boot (§9.8 flag > file):
    // the effective port already IS port_s, and settings.update on the pinned
    // key must reject with -32602 naming the flag.
    let port_value: u64 = port_s.parse().unwrap();
    let set_port = uds_rpc(
        &socket,
        1,
        "settings.update",
        json!({ "changes": [{ "path": "server.wsApi.port", "value": port_value }] }),
    )
    .await;
    assert_eq!(
        set_port["error"]["code"],
        json!(-32602),
        "pinned server.wsApi.port must reject settings.update: {set_port}"
    );
    assert!(
        set_port["error"]["message"]
            .as_str()
            .unwrap_or_default()
            .contains("INTENTD_TCP_PORT"),
        "rejection names the pinning flag: {set_port}"
    );
    let get_port = uds_rpc(
        &socket,
        11,
        "settings.get",
        json!({ "path": "server.wsApi.port" }),
    )
    .await;
    assert_eq!(
        get_port["result"]["value"],
        json!(port_value as f64),
        "pinned value is effective: {get_port}"
    );
    assert_eq!(
        get_port["result"]["origin"],
        json!("flag"),
        "pinned key reports origin=flag: {get_port}"
    );

    // Enable the WSS listener (this will start it at runtime)
    let enable = uds_rpc(
        &socket,
        2,
        "settings.update",
        json!({ "changes": [{ "path": "server.wsApi.enabled", "value": true }] }),
    )
    .await;
    assert!(
        enable.get("error").is_none(),
        "settings.update enable should succeed: {enable}"
    );

    // Give the listener a moment to start
    tokio::time::sleep(Duration::from_millis(500)).await;

    // Verify system.status shows the WSS listener is running
    let status = uds_rpc(&socket, 3, "system.status", json!({})).await;
    let first_boot_port = status["result"]["port"]
        .as_u64()
        .expect("port should be set after enable") as u16;
    let fingerprint = status["result"]["fingerprint"]
        .as_str()
        .expect("fingerprint should be set")
        .to_string();

    // Connect over WSS to verify it works
    let cfg = client_config(&fingerprint);
    let mut ws = connect_ws(first_boot_port, cfg.clone()).await;
    let ping = wss_rpc(
        &mut ws,
        10,
        "events.subscribe",
        json!({ "eventTypes": ["agent:*"] }),
    )
    .await;
    assert!(
        ping.get("error").is_none(),
        "events.subscribe should work: {ping}"
    );

    // Verify setting is persisted before shutdown
    let get_setting = uds_rpc(
        &socket,
        5,
        "settings.get",
        json!({ "path": "server.wsApi.enabled" }),
    )
    .await;
    assert!(
        get_setting["result"]["value"].as_bool() == Some(true),
        "setting should be true before shutdown: {get_setting}"
    );

    // STEP 2: Shutdown the daemon gracefully (simulating app relaunch)
    // Call system.shutdown to ensure clean SQLite close and WAL flush.
    // Wait for the process to exit naturally (don't drop the Daemon which would kill it).
    let shutdown = uds_rpc(&socket, 6, "system.shutdown", json!({})).await;
    assert!(
        shutdown.get("result").is_some(),
        "system.shutdown should succeed: {shutdown}"
    );
    // Wait for the daemon to exit gracefully (up to 3 seconds)
    let mut exited = false;
    for _ in 0..30 {
        if matches!(_daemon.child.try_wait(), Ok(Some(_))) {
            exited = true;
            break;
        }
        tokio::time::sleep(Duration::from_millis(100)).await;
    }
    assert!(
        exited,
        "daemon did not exit within 3 seconds after system.shutdown"
    );
    // Drop the first daemon without cleanup; process already exited
    drop(_daemon);

    // STEP 3: Boot again with --listen uds (same data dir, persisted setting is true)
    let child2 = spawn_serve(&data_dir, "uds", &env);
    let _daemon2 = Daemon {
        child: child2,
        data_dir: data_dir.clone(),
        cleanup_data_dir: true, // Second instance cleans up at end
    };
    assert!(await_uds(&socket).await, "daemon did not start on reboot");

    // Give the listener a moment to auto-start
    tokio::time::sleep(Duration::from_millis(500)).await;

    // REGRESSION TEST: Verify system.status shows the WSS listener is running
    // (before the fix, port would be null because persisted setting was ignored)
    let status2 = uds_rpc(&socket, 4, "system.status", json!({})).await;
    let reboot_port = status2["result"]["port"]
        .as_u64()
        .expect("port should be set at reboot with persisted enabled=true")
        as u16;
    assert_eq!(
        reboot_port, first_boot_port,
        "listener should bind the same port after reboot"
    );

    // Verify we can connect over WSS (listener is actually running, not just a stale setting)
    let mut ws2 = connect_ws(reboot_port, cfg.clone()).await;
    let ping2 = wss_rpc(
        &mut ws2,
        20,
        "events.subscribe",
        json!({ "eventTypes": ["agent:*"] }),
    )
    .await;
    assert!(
        ping2.get("error").is_none(),
        "events.subscribe should work after reboot: {ping2}"
    );

    // Verify server.pairingInfo returns the port (FE uses this for QR code)
    let pairing = uds_rpc(&socket, 5, "server.pairingInfo", json!({})).await;
    let pairing_port = pairing["result"]["port"]
        .as_u64()
        .expect("pairingInfo port should be set") as u16;
    assert_eq!(
        pairing_port, reboot_port,
        "pairingInfo port should match system.status port"
    );
}

/// Batch hook ordering: reverse input order test. A batch with changes in
/// non-dependency input order should still apply hooks deterministically:
/// value-setting keys (server.wsApi.port, priority 0) apply before
/// server.wsApi.enabled (priority 10), regardless of input order. This test
/// provides {wsApi.enabled=true, wsApi.port=NEW} with wsApi.enabled FIRST in
/// the input array, proving the sort happens before application, so the
/// listener starts on the NEW port.
#[tokio::test]
async fn batch_hook_ordering_port_before_enable() {
    let data_dir = temp_data_dir();
    let env: [(&str, &str); 2] = [("INTENTD_AUTH_TOKEN", TOKEN), ("INTENTD_TCP_PORT", "0")];
    let child = spawn_serve(&data_dir, "both", &env);
    let _daemon = Daemon {
        child,
        data_dir: data_dir.clone(),
        cleanup_data_dir: true,
    };
    let socket = data_dir.join("intentd.sock");
    assert!(await_uds(&socket).await, "daemon did not start");

    // Disable the listener so the batch below exercises a cold start.
    let disable = uds_rpc(
        &socket,
        1,
        "settings.update",
        json!({ "changes": [{ "path": "server.wsApi.enabled", "value": false }] }),
    )
    .await;
    assert!(
        disable.get("error").is_none(),
        "disable should succeed: {disable}"
    );
    tokio::time::sleep(Duration::from_millis(500)).await;

    // Batch update: provide changes in REVERSE dependency order (wsApi.enabled
    // before wsApi.port in the input array). The hook ordering ensures the port
    // value (priority 0) applies before wsApi.enabled (priority 10), so the
    // listener starts directly on the NEW port. Pick a dynamically-available port
    // to avoid hard-coded collisions.
    let new_port = free_port();
    let batch_reverse = uds_rpc(
        &socket,
        2,
        "settings.update",
        json!({
            "changes": [
                { "path": "server.wsApi.enabled", "value": true },
                { "path": "server.wsApi.port", "value": new_port }
            ]
        }),
    )
    .await;
    assert!(
        batch_reverse.get("error").is_none(),
        "batch reverse order should succeed: {batch_reverse}"
    );

    tokio::time::sleep(Duration::from_millis(500)).await;

    // Verify system.status shows the listener bound to the NEW port
    let status = uds_rpc(&socket, 3, "system.status", json!({})).await;
    let port = status["result"]["port"]
        .as_u64()
        .expect("port should be set after batch enable") as u16;
    assert_eq!(port, new_port, "listener should bind the NEW port");

    // Connect over WSS to verify the listener is functional
    let fingerprint = status["result"]["fingerprint"]
        .as_str()
        .expect("fingerprint should be set")
        .to_string();
    let cfg = client_config(&fingerprint);
    let mut ws = connect_ws(port, cfg.clone()).await;
    let ping_resp = wss_rpc(
        &mut ws,
        10,
        "events.subscribe",
        json!({ "eventTypes": ["agent:*"] }),
    )
    .await;
    assert!(
        ping_resp.get("error").is_none(),
        "events.subscribe over WSS after reverse-order batch should work: {ping_resp}"
    );
}

#[tokio::test]
async fn wss_system_status_includes_capacity_version_uptime() {
    // system.status over WSS reports maxAgents, version, uptimeSeconds alongside
    // existing fields (additive change for FE health menu).
    let data_dir = temp_data_dir();
    let env: [(&str, &str); 2] = [("INTENTD_AUTH_TOKEN", TOKEN), ("INTENTD_TCP_PORT", "0")];
    let _daemon = Daemon {
        child: spawn_serve(&data_dir, "both", &env),
        data_dir: data_dir.clone(),
        cleanup_data_dir: true,
    };
    let socket = data_dir.join("intentd.sock");
    assert!(await_uds(&socket).await, "daemon did not start");

    // Get status over UDS to find WSS port + fingerprint
    let status = uds_rpc(&socket, 1, "system.status", json!({})).await;
    let port = status["result"]["port"].as_u64().expect("port") as u16;
    let fingerprint = status["result"]["fingerprint"]
        .as_str()
        .expect("fingerprint")
        .to_string();

    // Connect over WSS and verify system.status returns the new fields
    let cfg = client_config(&fingerprint);
    let mut ws = connect_ws(port, cfg).await;
    let resp = wss_rpc(&mut ws, 7, "system.status", json!({})).await;
    let r = &resp["result"];
    assert_eq!(resp["id"], 7);
    assert_eq!(r["running"], true, "response: {resp}");
    assert!(r["host"]["locality"].is_string(), "locality present");
    assert!(r["host"]["os"].is_string(), "os present");
    assert!(r["host"]["arch"].is_string(), "arch present");
    // New fields: maxAgents, version, uptimeSeconds.
    assert!(r["maxAgents"].as_u64().unwrap() > 0, "maxAgents > 0: {r}");
    assert!(r["version"].is_string(), "version is string: {r}");
    assert!(r["uptimeSeconds"].is_u64(), "uptimeSeconds is u64: {r}");
}

#[tokio::test]
async fn runtime_toggled_wss_serves_system_status() {
    // Daemon starts with --listen uds, then toggles WSS on at runtime via
    // settings.update. Verify system.status works over the runtime-started
    // WSS listener (tests OnceLock control population, §5.7).
    let data_dir = temp_data_dir();
    let env: [(&str, &str); 1] = [("INTENTD_AUTH_TOKEN", TOKEN)];
    // Start daemon with ONLY UDS (--listen uds)
    let _daemon = Daemon {
        child: spawn_serve(&data_dir, "uds", &env),
        data_dir: data_dir.clone(),
        cleanup_data_dir: true,
    };
    let socket = data_dir.join("intentd.sock");
    assert!(await_uds(&socket).await, "daemon did not start");

    // Verify no WSS port initially
    let status_before = uds_rpc(&socket, 1, "system.status", json!({})).await;
    assert_eq!(
        status_before["result"]["port"],
        json!(null),
        "WSS should not be running"
    );

    // Toggle WSS on at runtime via settings.update
    let port = free_port();
    let enable_resp = uds_rpc(
        &socket,
        2,
        "settings.update",
        json!({
            "changes": [
                { "path": "server.wsApi.enabled", "value": true },
                { "path": "server.wsApi.port", "value": port }
            ]
        }),
    )
    .await;
    assert!(
        enable_resp.get("error").is_none(),
        "settings.update should succeed: {enable_resp}"
    );

    // Wait a moment for the listener to start
    tokio::time::sleep(std::time::Duration::from_millis(200)).await;

    // Verify WSS is now running
    let status_after = uds_rpc(&socket, 3, "system.status", json!({})).await;
    let runtime_port = status_after["result"]["port"]
        .as_u64()
        .expect("port after toggle") as u16;
    assert_eq!(runtime_port, port, "WSS should bind to configured port");

    // Connect over WSS and verify system.status works
    let fingerprint = status_after["result"]["fingerprint"]
        .as_str()
        .expect("fingerprint")
        .to_string();
    let cfg = client_config(&fingerprint);
    let mut ws = connect_ws(runtime_port, cfg).await;
    let resp = wss_rpc(&mut ws, 4, "system.status", json!({})).await;
    let r = &resp["result"];
    assert_eq!(resp["id"], 4);
    assert_eq!(
        r["running"], true,
        "system.status over runtime-toggled WSS: {resp}"
    );
    // Verify the new fields are present (proving control is wired)
    assert!(r["maxAgents"].as_u64().unwrap() > 0, "maxAgents > 0: {r}");
    assert!(r["version"].is_string(), "version is string: {r}");
    assert!(r["uptimeSeconds"].is_u64(), "uptimeSeconds is u64: {r}");
}
