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
    timeout(
        common::test_timeout(Duration::from_secs(30)),
        reader.read_line(&mut buf),
    )
    .await
    .expect("uds rpc timed out")
    .expect("read uds response");
    serde_json::from_str(buf.trim_end()).expect("invalid JSON frame")
}

/// Poll until new TCP connections to `port` are refused — the listener socket
/// closes asynchronously after a `settings.update` disable clears the status
/// port, so a single-shot connect can still succeed under load (monorepo#515).
/// Only a connect *error* counts as refusal; an elapsed connect timeout is
/// inconclusive (a stalled-but-live listener) and keeps polling.
async fn await_tcp_refused(port: u16) {
    let budget = common::daemon_startup_timeout();
    let deadline = tokio::time::Instant::now() + budget;
    let connect_budget = common::test_timeout(Duration::from_secs(2));
    loop {
        let connect = timeout(
            connect_budget,
            TcpStream::connect((Ipv4Addr::LOCALHOST, port)),
        )
        .await;
        if matches!(connect, Ok(Err(_))) {
            return;
        }
        assert!(
            tokio::time::Instant::now() < deadline,
            "TCP port {port} still accepting connections {budget:?} after listener disable"
        );
        tokio::time::sleep(Duration::from_millis(50)).await;
    }
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
        let next = timeout(common::test_timeout(Duration::from_secs(30)), ws.next())
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
    // Start daemon with both UDS and TCP (server.wsApi.enabled seeded in config.toml)
    let child = spawn_serve(&data_dir, "both", &env);
    let _daemon = Daemon {
        child,
        data_dir: data_dir.clone(),
        cleanup_data_dir: true,
    };
    let socket = data_dir.join("intentd.sock");
    assert!(await_uds(&socket).await, "daemon did not start");

    // Get the actual bound port from system.status (INTENTD_TCP_PORT=0 seam:
    // every listener start binds a fresh OS-assigned ephemeral port)
    let status = common::await_wss_status_logged(&socket, &data_dir.join("daemon.log")).await;
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

    // Verify system.status shows no listener (poll — teardown is async)
    common::await_wss_stopped_logged(&socket, &data_dir.join("daemon.log")).await;

    // Verify new WSS connections are refused (TCP listener should be closed)
    await_tcp_refused(initial_port).await;

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

    // Verify system.status shows the WSS listener again (poll — start is
    // async). With the INTENTD_TCP_PORT=0 seam the re-enable binds a fresh
    // ephemeral port, so re-read it rather than expecting the boot port
    // (settings-port reuse on re-enable is covered seam-free by
    // runtime_toggled_wss_serves_system_status below).
    let status = common::await_wss_status_logged(&socket, &data_dir.join("daemon.log")).await;
    let new_port = status["result"]["port"]
        .as_u64()
        .expect("port should be set after re-enable") as u16;

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

/// Regression test: boot UDS-only + persisted server.wsApi.enabled=true
/// should auto-start the WSS listener. This tests the sidecar/packaged posture
/// where the daemon is spawned UDS-only but the user has previously
/// enabled the WSS listener via the UI toggle. Before the fix, the persisted
/// setting was ignored at boot and the listener stayed down until manual toggle.
#[tokio::test]
async fn persisted_wss_enabled_auto_starts_at_boot_uds_mode() {
    let data_dir = temp_data_dir();
    let port_s = free_port().to_string();
    let env: [(&str, &str); 2] = [("INTENTD_AUTH_TOKEN", TOKEN), ("INTENTD_TCP_PORT", &port_s)];

    // STEP 1: Boot UDS-only, persist server.wsApi.enabled=true
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

    // Verify system.status shows the WSS listener is running (poll — start is async)
    let status = common::await_wss_status_logged(&socket, &data_dir.join("daemon.log")).await;
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
    // Wait for the daemon to exit gracefully (liveness bound: WAL flush under
    // parallel-suite load can exceed a fixed few-second window, monorepo#515)
    let exit_budget = common::daemon_startup_timeout();
    let exit_deadline = tokio::time::Instant::now() + exit_budget;
    let mut exited = false;
    while tokio::time::Instant::now() < exit_deadline {
        if matches!(_daemon.child.try_wait(), Ok(Some(_))) {
            exited = true;
            break;
        }
        tokio::time::sleep(Duration::from_millis(100)).await;
    }
    assert!(
        exited,
        "daemon did not exit within {exit_budget:?} after system.shutdown"
    );
    // Drop the first daemon without cleanup; process already exited
    drop(_daemon);

    // STEP 3: Boot again UDS-only (same data dir, persisted setting is true)
    let child2 = spawn_serve(&data_dir, "uds", &env);
    let _daemon2 = Daemon {
        child: child2,
        data_dir: data_dir.clone(),
        cleanup_data_dir: true, // Second instance cleans up at end
    };
    assert!(await_uds(&socket).await, "daemon did not start on reboot");

    // REGRESSION TEST: Verify system.status shows the WSS listener is running
    // (before the fix, port would be null because persisted setting was
    // ignored). Poll — the boot auto-start is async relative to UDS readiness.
    let status2 = common::await_wss_status_logged(&socket, &data_dir.join("daemon.log")).await;
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
    // No INTENTD_TCP_PORT: the env-0 ephemeral seam would override the batch's
    // explicit port and the bound port is exactly what proves hook ordering.
    // Boot UDS-only (no wsApi seed) so the batch below exercises a cold start.
    let env: [(&str, &str); 1] = [("INTENTD_AUTH_TOKEN", TOKEN)];
    let child = spawn_serve(&data_dir, "uds", &env);
    let _daemon = Daemon {
        child,
        data_dir: data_dir.clone(),
        cleanup_data_dir: true,
    };
    let socket = data_dir.join("intentd.sock");
    assert!(await_uds(&socket).await, "daemon did not start");

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

    // Verify system.status shows the listener bound to the NEW port (poll —
    // the runtime listener start is async)
    let status = common::await_wss_status_logged(&socket, &data_dir.join("daemon.log")).await;
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
    let status = common::await_wss_status_logged(&socket, &data_dir.join("daemon.log")).await;
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
    // Process resource sample: cpuPercent may be 0 right after start (sysinfo
    // needs two refreshes), but memoryBytes must be live for a running daemon.
    assert!(r["cpuPercent"].is_number(), "cpuPercent is number: {r}");
    assert!(
        r["memoryBytes"].as_u64().unwrap() > 0,
        "memoryBytes > 0: {r}"
    );
    // Routing fields (additive): localIps is a string array (may be empty on
    // hosts with no routable interface), hostname is a non-empty string.
    let local_ips = r["localIps"].as_array().expect("localIps is array");
    assert!(
        local_ips.iter().all(Value::is_string),
        "localIps entries are strings: {r}"
    );
    assert!(
        !r["hostname"]
            .as_str()
            .expect("hostname is string")
            .is_empty(),
        "hostname non-empty: {r}"
    );
    // Aggregate-budget fields (monorepo#2063): the budget is ON by default
    // (auto resolves to recommended), so agentMemoryBudgetBytes and queuedSpawns
    // are present. agentMemoryChargedBytes only appears once the descendant-tree
    // sampler lands its first sample.
    let obj = r.as_object().expect("result is object");
    assert!(
        obj.contains_key("agentMemoryBudgetBytes"),
        "agentMemoryBudgetBytes must be present (auto is on by default): {r}"
    );
    assert!(
        obj["agentMemoryBudgetBytes"].as_u64().is_some(),
        "agentMemoryBudgetBytes must be a positive u64: {r}"
    );
    assert!(
        obj.contains_key("queuedSpawns"),
        "queuedSpawns must be present when budget is active: {r}"
    );
    assert_eq!(
        obj["queuedSpawns"].as_u64(),
        Some(0),
        "no spawn is queued in a fresh daemon: {r}"
    );
    // agentMemoryChargedBytes is absent until the sampler's first sample, but
    // "before the first sample" is not deterministically observable from a
    // client: the sampler can land its first sample before this RPC (seen on
    // CI — monorepo#2567), making the field present-with-0 on a fresh daemon.
    // Accept both orderings; when present it must be a u64, never null. Do
    // not tighten this back to "must be absent" — the deterministic
    // absent-until-first-sample contract is covered by the status_json unit
    // tests in intent-transport's control/tests.rs.
    match obj.get("agentMemoryChargedBytes") {
        None => {}
        Some(v) => {
            assert!(
                v.is_u64(),
                "agentMemoryChargedBytes is u64 when present, never null: {r}"
            );
        }
    }
    // Workspaces-root disk fields (additive): the harness points
    // INTENTD_WORKSPACES_DIR at an existing tempdir and the sampler takes a
    // synchronous first sample before the listeners come up, so both fields
    // are present with plausible volume sizes.
    let disk_avail = obj["workspacesDiskAvailableBytes"]
        .as_u64()
        .expect("workspacesDiskAvailableBytes is u64");
    let disk_total = obj["workspacesDiskTotalBytes"]
        .as_u64()
        .expect("workspacesDiskTotalBytes is u64");
    assert!(disk_total > 0, "workspacesDiskTotalBytes > 0: {r}");
    assert!(
        disk_avail <= disk_total,
        "available must not exceed total: {r}"
    );
}

#[tokio::test]
async fn wss_system_status_reports_budget_fields_when_installed() {
    // With `agents.memoryBudgetMb` set, system.status carries the aggregate
    // budget visibility fields (monorepo#2063): agentMemoryBudgetBytes always,
    // agentMemoryChargedBytes once the descendant-tree sampler has landed a
    // sample (absent before — the budget is inert until then), and
    // queuedSpawns (0 with nothing queued).
    let data_dir = temp_data_dir();
    std::fs::write(
        data_dir.join("config.toml"),
        "[agents]\nmemoryBudgetMb = 20480\n",
    )
    .expect("seed config.toml with agents.memoryBudgetMb");
    let env: [(&str, &str); 2] = [("INTENTD_AUTH_TOKEN", TOKEN), ("INTENTD_TCP_PORT", "0")];
    let _daemon = Daemon {
        child: spawn_serve(&data_dir, "both", &env),
        data_dir: data_dir.clone(),
        cleanup_data_dir: true,
    };
    let socket = data_dir.join("intentd.sock");
    assert!(await_uds(&socket).await, "daemon did not start");

    let status = common::await_wss_status_logged(&socket, &data_dir.join("daemon.log")).await;
    let port = status["result"]["port"].as_u64().expect("port") as u16;
    let fingerprint = status["result"]["fingerprint"]
        .as_str()
        .expect("fingerprint")
        .to_string();

    let cfg = client_config(&fingerprint);
    let mut ws = connect_ws(port, cfg).await;
    let resp = wss_rpc(&mut ws, 8, "system.status", json!({})).await;
    let r = &resp["result"];
    assert_eq!(resp["id"], 8);
    assert_eq!(
        r["agentMemoryBudgetBytes"].as_u64(),
        Some(20_480u64 * 1024 * 1024),
        "installed budget rides the wire: {r}"
    );
    assert_eq!(
        r["queuedSpawns"].as_u64(),
        Some(0),
        "no spawn is queued in a fresh daemon: {r}"
    );
    // Charged bytes appear only once the sampler has landed its first tree
    // sample (~5s cadence; a fast test can beat it). When present it must be
    // a u64, never null — presence-detected like the other two.
    match r
        .as_object()
        .expect("result is object")
        .get("agentMemoryChargedBytes")
    {
        None => {}
        Some(v) => {
            assert!(
                v.is_u64(),
                "agentMemoryChargedBytes is u64 when present, never null: {r}"
            );
        }
    }
}

#[tokio::test]
async fn runtime_toggled_wss_serves_system_status() {
    // Daemon starts UDS-only, then toggles WSS on at runtime via
    // settings.update. Verify system.status works over the runtime-started
    // WSS listener (tests OnceLock control population, §5.7).
    let data_dir = temp_data_dir();
    let env: [(&str, &str); 1] = [("INTENTD_AUTH_TOKEN", TOKEN)];
    // Start daemon with ONLY UDS (no wsApi config seed)
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

    // Verify WSS is now running (poll — the runtime listener start is async)
    let status_after = common::await_wss_status_logged(&socket, &data_dir.join("daemon.log")).await;
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

    // The descendant-tree fields ride the real WSS wire, not just
    // `status_json`. All three are sampled together, so the contract is
    // all-null (sampler has not ticked yet) or all-present — never a mix,
    // which would let a bundle read a count without a byte total. Asserting
    // the pair rather than a concrete value keeps this deterministic: the
    // first tick fires at startup but a fast test can still beat it.
    for field in ["childProcesses", "childMemoryBytes", "childMemoryPeakBytes"] {
        assert!(
            r.get(field).is_some(),
            "{field} must ride the WSS status result: {r}"
        );
    }
    let sampled = [
        &r["childProcesses"],
        &r["childMemoryBytes"],
        &r["childMemoryPeakBytes"],
    ];
    let nulls = sampled.iter().filter(|v| v.is_null()).count();
    assert!(
        nulls == 0 || nulls == 3,
        "descendant-tree fields must be all-null or all-present, got {nulls} nulls: {r}"
    );
    if nulls == 0 {
        let bytes = r["childMemoryBytes"].as_u64().expect("bytes when sampled");
        let peak = r["childMemoryPeakBytes"]
            .as_u64()
            .expect("peak when sampled");
        assert!(
            r["childProcesses"].is_u64(),
            "childProcesses is u64 when sampled: {r}"
        );
        assert!(
            peak >= bytes,
            "peak {peak} must be >= instantaneous {bytes}"
        );
    }
}
