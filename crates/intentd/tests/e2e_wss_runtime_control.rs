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
    /// If false, skip `data_dir` cleanup in Drop (for tests that reuse the same `data_dir`)
    cleanup_data_dir: bool,
}

impl Drop for Daemon {
    fn drop(&mut self) {
        let _ = self.child.kill();
        let _ = self.child.wait();
        if self.cleanup_data_dir {
            let log_path = self.data_dir.join("daemon.log");
            if let Ok(log) = std::fs::read_to_string(&log_path) {
                eprintln!("=== DAEMON LOG ===\n{log}\n=== END LOG ===");
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

/// Shared `serve` setup: hermetic dirs, log redirection, env. Used by the
/// direct spawn and the stand-in-sitter wrapper spawn below.
fn configure_serve(cmd: &mut Command, data_dir: &Path, listen: &str, env: &[(&str, &str)]) {
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
    cmd.env("INTENTD_DATA_DIR", data_dir)
        .env("INTENTD_WORKSPACES_DIR", &workspaces_dir)
        .env("INTENTD_ASSERT_HERMETIC_ROOT", "1")
        .stdout(Stdio::null())
        .stderr(Stdio::from(log));
    for (k, v) in env {
        cmd.env(k, v);
    }
}

fn spawn_serve(data_dir: &Path, listen: &str, env: &[(&str, &str)]) -> Child {
    let mut cmd = Command::new(env!("CARGO_BIN_EXE_intentd"));
    cmd.arg("serve");
    configure_serve(&mut cmd, data_dir, listen, env);
    cmd.spawn().expect("spawn intentd serve")
}

/// Spawn `intentd serve` as the CHILD of a stand-in sitter: `sitter_bin` (a
/// shell symlinked as `intentd-sitter`, so the process name follows the
/// executed path's basename) backgrounds the daemon, records the daemon's pid
/// in `daemon_pid_path`, and waits. The returned child (the wrapper) is thus
/// both sitter-named AND the daemon's parent — the conjunction
/// `signal_sitter_update` requires.
fn spawn_serve_under_stand_in_sitter(
    data_dir: &Path,
    listen: &str,
    env: &[(&str, &str)],
    sitter_bin: &Path,
    daemon_pid_path: &Path,
) -> Child {
    let mut cmd = Command::new(sitter_bin);
    cmd.arg("-c")
        .arg(r#""$1" serve & echo "$!" > "$2"; wait"#)
        .arg("intentd-sitter")
        .arg(env!("CARGO_BIN_EXE_intentd"))
        .arg(daemon_pid_path);
    configure_serve(&mut cmd, data_dir, listen, env);
    cmd.spawn().expect("spawn stand-in sitter wrapper")
}

/// SIGKILLs a raw pid on drop — cleanup for the daemon grandchild, which
/// survives its wrapper parent's death and is not reachable via `Child`.
struct KillPidOnDrop(i32);

impl Drop for KillPidOnDrop {
    fn drop(&mut self) {
        unsafe {
            libc::kill(self.0, libc::SIGKILL);
        }
    }
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
            Some(Ok(_)) => {}
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
    let initial_port = u16::try_from(
        status["result"]["port"]
            .as_u64()
            .expect("port should be set at boot"),
    )
    .expect("value fits in u16");
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
    let new_port = u16::try_from(
        status["result"]["port"]
            .as_u64()
            .expect("port should be set after re-enable"),
    )
    .expect("value fits in u16");

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
// Port numbers are far below 2^53: loss-free in f64.
#[allow(clippy::cast_precision_loss)]
async fn persisted_wss_enabled_auto_starts_at_boot_uds_mode() {
    let data_dir = temp_data_dir();
    let port_s = free_port().to_string();
    let env: [(&str, &str); 2] = [("INTENTD_AUTH_TOKEN", TOKEN), ("INTENTD_TCP_PORT", &port_s)];

    // STEP 1: Boot UDS-only, persist server.wsApi.enabled=true
    let child = spawn_serve(&data_dir, "uds", &env);
    let mut daemon = Daemon {
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
    let first_boot_port = u16::try_from(
        status["result"]["port"]
            .as_u64()
            .expect("port should be set after enable"),
    )
    .expect("value fits in u16");
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
        if matches!(daemon.child.try_wait(), Ok(Some(_))) {
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
    drop(daemon);

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
    let reboot_port = u16::try_from(
        status2["result"]["port"]
            .as_u64()
            .expect("port should be set at reboot with persisted enabled=true"),
    )
    .expect("value fits in u16");
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
    let pairing_port = u16::try_from(
        pairing["result"]["port"]
            .as_u64()
            .expect("pairingInfo port should be set"),
    )
    .expect("value fits in u16");
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
    let port = u16::try_from(
        status["result"]["port"]
            .as_u64()
            .expect("port should be set after batch enable"),
    )
    .expect("value fits in u16");
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
    let port =
        u16::try_from(status["result"]["port"].as_u64().expect("port")).expect("value fits in u16");
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
    // Supervision probe (intent-hq/intent#3875): always present, and false
    // here — the daemon was spawned by the test harness, not a sitter.
    assert_eq!(r["updateSupported"], false, "updateSupported: {r}");
    // Routing fields (additive): localIps is a string array (may be empty on
    // hosts with no routable interface), hostname and prettyHostname are
    // non-empty strings.
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
    assert!(
        !r["prettyHostname"]
            .as_str()
            .expect("prettyHostname is string")
            .is_empty(),
        "prettyHostname non-empty: {r}"
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

/// `system.requestUpdate` over the real WSS transport (PROTOCOL §5.7, v8.6):
/// remote callers ARE allowed (unlike `system.shutdown` — a remote client is
/// exactly who needs to trigger an update). Supervision requires BOTH signals
/// — the pidfile pid must be the daemon's direct parent AND a sitter-named
/// process (`intentd-sitter` in dev, `intentd` after the packaged rename) —
/// so the daemon here runs as the child of a shell symlinked as
/// `intentd-sitter` (the process name follows the executed path's basename).
/// Unsupervised (no sitter pidfile, a live non-sitter pid, or a sitter-named
/// pid that is not the parent) the daemon answers `-32603` with the reason;
/// with the wrapper's pid recorded in `<data_dir>/sitter/sitter.pid` it
/// answers `{ ok: true }` and delivers SIGUSR1 to it (proven by the
/// wrapper's exit signal — SIGUSR1's default disposition terminates).
#[tokio::test]
async fn wss_system_request_update_signals_the_sitter() {
    use std::os::unix::process::ExitStatusExt;

    let data_dir = temp_data_dir();
    let sitter_dir = data_dir.join("sitter");
    std::fs::create_dir_all(&sitter_dir).expect("mkdir sitter dir");
    let sitter_bin = sitter_dir.join("intentd-sitter");
    std::os::unix::fs::symlink("/bin/sh", &sitter_bin).expect("symlink stand-in sitter shell");
    let daemon_pid_path = sitter_dir.join("daemon.pid");

    let env: [(&str, &str); 2] = [("INTENTD_AUTH_TOKEN", TOKEN), ("INTENTD_TCP_PORT", "0")];
    let mut daemon = Daemon {
        child: spawn_serve_under_stand_in_sitter(
            &data_dir,
            "both",
            &env,
            &sitter_bin,
            &daemon_pid_path,
        ),
        data_dir: data_dir.clone(),
        cleanup_data_dir: true,
    };
    let socket = data_dir.join("intentd.sock");
    assert!(await_uds(&socket).await, "daemon did not start");
    // The wrapper wrote the daemon's pid before the daemon could bind the UDS
    // socket; the guard SIGKILLs the daemon grandchild on drop — it outlives
    // its wrapper parent and is not reachable via the wrapper's `Child`.
    let daemon_pid: i32 = std::fs::read_to_string(&daemon_pid_path)
        .expect("read daemon pid file")
        .trim()
        .parse()
        .expect("daemon pid is an integer");
    let _kill_daemon = KillPidOnDrop(daemon_pid);

    let status = common::await_wss_status_logged(&socket, &data_dir.join("daemon.log")).await;
    let port =
        u16::try_from(status["result"]["port"].as_u64().expect("port")).expect("value fits in u16");
    let fingerprint = status["result"]["fingerprint"]
        .as_str()
        .expect("fingerprint")
        .to_string();
    let cfg = client_config(&fingerprint);
    let mut ws = connect_ws(port, cfg).await;

    // No sitter pidfile ⇒ the daemon is not supervised: -32603 with the reason.
    let resp = wss_rpc(&mut ws, 41, "system.requestUpdate", json!({})).await;
    assert_eq!(resp["jsonrpc"], "2.0");
    assert_eq!(resp["id"], 41);
    assert_eq!(resp["error"]["code"], -32603, "response: {resp}");
    assert!(
        resp["error"]["message"]
            .as_str()
            .expect("message is string")
            .contains("not supervised"),
        "message names the cause: {resp}"
    );

    // A pidfile naming a live process that is neither sitter-named nor the
    // daemon's parent (a stale pid the OS recycled) ⇒ still not supervised —
    // never a signal target.
    let mut not_sitter = std::process::Command::new("sleep")
        .arg("30")
        .spawn()
        .expect("spawn non-sitter process");
    std::fs::write(
        sitter_dir.join("sitter.pid"),
        format!("{}\n", not_sitter.id()),
    )
    .expect("write sitter pidfile");
    let resp = wss_rpc(&mut ws, 42, "system.requestUpdate", json!({})).await;
    assert_eq!(resp["error"]["code"], -32603, "response: {resp}");
    not_sitter.kill().expect("kill non-sitter");
    not_sitter.wait().expect("wait non-sitter");

    // A live process that IS sitter-named but is NOT the daemon's parent (a
    // recycled pid landing on an unrelated sitter-named process) ⇒ rejected:
    // the name alone is not proof of supervision.
    let sleep_bin = ["/bin/sleep", "/usr/bin/sleep"]
        .iter()
        .find(|p| Path::new(p).exists())
        .expect("sleep binary");
    let decoy_dir = sitter_dir.join("decoy");
    std::fs::create_dir_all(&decoy_dir).expect("mkdir decoy dir");
    let decoy_bin = decoy_dir.join("intentd-sitter");
    std::os::unix::fs::symlink(sleep_bin, &decoy_bin).expect("symlink decoy sitter");
    let mut decoy = std::process::Command::new(&decoy_bin)
        .arg("30")
        .spawn()
        .expect("spawn decoy sitter");
    std::fs::write(sitter_dir.join("sitter.pid"), format!("{}\n", decoy.id()))
        .expect("write sitter pidfile");
    let resp = wss_rpc(&mut ws, 43, "system.requestUpdate", json!({})).await;
    assert_eq!(resp["error"]["code"], -32603, "response: {resp}");
    decoy.kill().expect("kill decoy sitter");
    decoy.wait().expect("wait decoy sitter");

    // The wrapper's pid — sitter-named AND the daemon's parent ⇒ signaled.
    std::fs::write(
        sitter_dir.join("sitter.pid"),
        format!("{}\n", daemon.child.id()),
    )
    .expect("write sitter pidfile");
    let resp = wss_rpc(&mut ws, 44, "system.requestUpdate", json!({})).await;
    assert_eq!(resp["id"], 44);
    assert_eq!(resp["result"], json!({ "ok": true }), "response: {resp}");

    let status = daemon.child.wait().expect("wait stand-in sitter wrapper");
    assert_eq!(
        status.signal(),
        Some(libc::SIGUSR1),
        "stand-in sitter wrapper must be terminated by SIGUSR1"
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
    let port =
        u16::try_from(status["result"]["port"].as_u64().expect("port")).expect("value fits in u16");
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

    // Verify no WSS port initially — and that a UDS-only daemon advertises
    // no TCP routes: with the listener down every localIps entry would be a
    // dead route, so the set is empty (not the interface enumeration).
    let status_before = uds_rpc(&socket, 1, "system.status", json!({})).await;
    assert_eq!(
        status_before["result"]["port"],
        json!(null),
        "WSS should not be running"
    );
    assert_eq!(
        status_before["result"]["localIps"],
        json!([]),
        "UDS-only daemon must not advertise TCP routes: {status_before}"
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
    let runtime_port = u16::try_from(
        status_after["result"]["port"]
            .as_u64()
            .expect("port after toggle"),
    )
    .expect("value fits in u16");
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

/// Runtime `server.bindAddress` hook (monorepo#2900): changing the bind
/// address while the WSS listener is running restarts it on the new address.
/// Observable end to end: the listener stays connectable on the fixed port
/// across both restarts, and both `pairing.getInfo` hosts and
/// `system.status` localIps advertise the bind-aware set (exactly the
/// specific address for a loopback bind; the non-loopback enumeration for
/// 0.0.0.0 — which never contains 127.0.0.1).
#[tokio::test]
async fn runtime_bind_address_change_restarts_listener() {
    let data_dir = temp_data_dir();
    // Fixed seeded port (no INTENTD_TCP_PORT=0 seam) so the restarted
    // listener rebinds the same port and only the address changes.
    let env: [(&str, &str); 1] = [("INTENTD_AUTH_TOKEN", TOKEN)];
    let child = spawn_serve(&data_dir, "both", &env);
    let _daemon = Daemon {
        child,
        data_dir: data_dir.clone(),
        cleanup_data_dir: true,
    };
    let socket = data_dir.join("intentd.sock");
    assert!(await_uds(&socket).await, "daemon did not start");

    let status = common::await_wss_status_logged(&socket, &data_dir.join("daemon.log")).await;
    let port = u16::try_from(
        status["result"]["port"]
            .as_u64()
            .expect("port should be set at boot"),
    )
    .expect("value fits in u16");
    let fingerprint = status["result"]["fingerprint"]
        .as_str()
        .expect("fingerprint should be set")
        .to_string();
    let cfg = client_config(&fingerprint);

    // Default loopback bind: pairing advertises exactly 127.0.0.1, and
    // system.status localIps agrees (same bind-aware semantics — never the
    // full interface enumeration for a loopback-only listener).
    let info = uds_rpc(&socket, 2, "pairing.getInfo", json!({})).await;
    assert_eq!(
        info["result"]["hosts"],
        json!(["127.0.0.1"]),
        "loopback bind advertises exactly 127.0.0.1: {info}"
    );
    let status = uds_rpc(&socket, 102, "system.status", json!({})).await;
    assert_eq!(
        status["result"]["localIps"],
        json!(["127.0.0.1"]),
        "loopback bind: system.status localIps is exactly 127.0.0.1: {status}"
    );

    // Widen the bind while the listener is running: the hook restarts it on
    // 0.0.0.0 (same port). settings.update awaits the hook, so the restart is
    // complete when the RPC returns.
    let widen = uds_rpc(
        &socket,
        3,
        "settings.update",
        json!({ "changes": [{ "path": "server.bindAddress", "value": "0.0.0.0" }] }),
    )
    .await;
    assert!(
        widen.get("error").is_none(),
        "settings.update bindAddress → 0.0.0.0 should succeed: {widen}"
    );

    // Listener is back up on the same port (0.0.0.0 includes loopback).
    let mut ws = connect_ws(port, cfg.clone()).await;
    let sub = wss_rpc(
        &mut ws,
        10,
        "events.subscribe",
        json!({ "eventTypes": ["agent:*"] }),
    )
    .await;
    assert!(
        sub.get("error").is_none(),
        "events.subscribe after bindAddress widen should work: {sub}"
    );

    // Bind-aware advertisement followed the restart: an unspecified bind
    // enumerates non-loopback local IPs, never 127.0.0.1 — on both surfaces.
    let info = uds_rpc(&socket, 4, "pairing.getInfo", json!({})).await;
    let hosts = info["result"]["hosts"].as_array().expect("hosts array");
    assert!(
        !hosts.iter().any(|h| h == "127.0.0.1"),
        "0.0.0.0 bind must not advertise loopback: {info}"
    );
    let status = uds_rpc(&socket, 104, "system.status", json!({})).await;
    let local_ips = status["result"]["localIps"]
        .as_array()
        .expect("localIps array");
    assert!(
        !local_ips.iter().any(|h| h == "127.0.0.1"),
        "0.0.0.0 bind: system.status localIps must not contain loopback: {status}"
    );

    // Narrow back to loopback: hook restarts again, listener survives, and
    // the advertisement returns to exactly 127.0.0.1.
    let narrow = uds_rpc(
        &socket,
        5,
        "settings.update",
        json!({ "changes": [{ "path": "server.bindAddress", "value": "127.0.0.1" }] }),
    )
    .await;
    assert!(
        narrow.get("error").is_none(),
        "settings.update bindAddress → 127.0.0.1 should succeed: {narrow}"
    );
    let mut ws2 = connect_ws(port, cfg).await;
    let sub2 = wss_rpc(
        &mut ws2,
        20,
        "events.subscribe",
        json!({ "eventTypes": ["agent:*"] }),
    )
    .await;
    assert!(
        sub2.get("error").is_none(),
        "events.subscribe after narrowing back to loopback should work: {sub2}"
    );
    let info = uds_rpc(&socket, 6, "pairing.getInfo", json!({})).await;
    assert_eq!(
        info["result"]["hosts"],
        json!(["127.0.0.1"]),
        "loopback bind advertises exactly 127.0.0.1 again: {info}"
    );
    let status = uds_rpc(&socket, 106, "system.status", json!({})).await;
    assert_eq!(
        status["result"]["localIps"],
        json!(["127.0.0.1"]),
        "narrowed bind: system.status localIps returns to exactly 127.0.0.1: {status}"
    );

    // A non-IP value is rejected at write time (never deferred to the next
    // listener start) and the running listener is untouched.
    let bad = uds_rpc(
        &socket,
        7,
        "settings.update",
        json!({ "changes": [{ "path": "server.bindAddress", "value": "not-an-ip" }] }),
    )
    .await;
    let err = bad
        .get("error")
        .expect("non-IP bindAddress must be rejected")
        .to_string();
    assert!(err.contains("server.bindAddress"), "{bad}");
    let mut ws3 = connect_ws(port, client_config(&fingerprint)).await;
    let sub3 = wss_rpc(
        &mut ws3,
        30,
        "events.subscribe",
        json!({ "eventTypes": ["agent:*"] }),
    )
    .await;
    assert!(
        sub3.get("error").is_none(),
        "listener must survive a rejected bindAddress write: {sub3}"
    );
}

/// Runtime `server.bindAddress` list form (monorepo#3314): a list of IPs is
/// accepted end to end over the settings surface — the restart hook applies
/// it, the listener stays connectable, and `pairing.getInfo` advertises
/// exactly the configured set. Invalid sets (duplicates, unspecified mixed
/// with specific) are rejected at write time with the running listener
/// untouched.
#[tokio::test]
async fn runtime_bind_address_list_applies_and_validates() {
    // The two-address set is loopback-family (127.0.0.1 + ::1) so CI needs no
    // real interfaces; skip only when the sandbox lacks IPv6 entirely.
    if std::net::TcpListener::bind(("::1", 0)).is_err() {
        eprintln!("skipping: IPv6 loopback unavailable");
        return;
    }
    let data_dir = temp_data_dir();
    let env: [(&str, &str); 1] = [("INTENTD_AUTH_TOKEN", TOKEN)];
    let child = spawn_serve(&data_dir, "both", &env);
    let _daemon = Daemon {
        child,
        data_dir: data_dir.clone(),
        cleanup_data_dir: true,
    };
    let socket = data_dir.join("intentd.sock");
    assert!(await_uds(&socket).await, "daemon did not start");

    let status = common::await_wss_status_logged(&socket, &data_dir.join("daemon.log")).await;
    let port = u16::try_from(
        status["result"]["port"]
            .as_u64()
            .expect("port should be set at boot"),
    )
    .expect("value fits in u16");
    let fingerprint = status["result"]["fingerprint"]
        .as_str()
        .expect("fingerprint should be set")
        .to_string();
    let cfg = client_config(&fingerprint);

    // Switch to the list form while the listener runs: the hook restarts it
    // bound to both loopback addresses on the same port.
    let widen = uds_rpc(
        &socket,
        2,
        "settings.update",
        json!({ "changes": [{ "path": "server.bindAddress", "value": ["127.0.0.1", "::1"] }] }),
    )
    .await;
    assert!(
        widen.get("error").is_none(),
        "settings.update bindAddress → list should succeed: {widen}"
    );

    // Listener is back up on the same port and the persisted value reads back
    // in its array shape.
    let mut ws = connect_ws(port, cfg.clone()).await;
    let sub = wss_rpc(
        &mut ws,
        10,
        "events.subscribe",
        json!({ "eventTypes": ["agent:*"] }),
    )
    .await;
    assert!(
        sub.get("error").is_none(),
        "events.subscribe after bindAddress list should work: {sub}"
    );
    let get = uds_rpc(
        &socket,
        3,
        "settings.get",
        json!({ "path": "server.bindAddress" }),
    )
    .await;
    assert_eq!(
        get["result"]["value"],
        json!(["127.0.0.1", "::1"]),
        "list form persists and reads back as an array: {get}"
    );

    // Pairing advertises exactly the configured set (specific addresses),
    // and system.status localIps mirrors it (same bind-aware semantics).
    let info = uds_rpc(&socket, 4, "pairing.getInfo", json!({})).await;
    assert_eq!(
        info["result"]["hosts"],
        json!(["127.0.0.1", "::1"]),
        "list bind advertises exactly its entries: {info}"
    );
    let status = uds_rpc(&socket, 104, "system.status", json!({})).await;
    assert_eq!(
        status["result"]["localIps"],
        json!(["127.0.0.1", "::1"]),
        "list bind: system.status localIps is exactly the configured set: {status}"
    );

    // Invalid sets are rejected at write time; the listener stays up.
    for (id, bad_value) in [
        (5, json!(["127.0.0.1", "127.0.0.1"])),
        (6, json!(["0.0.0.0", "127.0.0.1"])),
        (7, json!([])),
        (8, json!(["not-an-ip"])),
    ] {
        let bad = uds_rpc(
            &socket,
            id,
            "settings.update",
            json!({ "changes": [{ "path": "server.bindAddress", "value": bad_value }] }),
        )
        .await;
        let err = bad
            .get("error")
            .unwrap_or_else(|| panic!("invalid bindAddress set {id} must be rejected: {bad}"))
            .to_string();
        assert!(err.contains("server.bindAddress"), "{bad}");
    }
    let mut ws2 = connect_ws(port, cfg).await;
    let sub2 = wss_rpc(
        &mut ws2,
        20,
        "events.subscribe",
        json!({ "eventTypes": ["agent:*"] }),
    )
    .await;
    assert!(
        sub2.get("error").is_none(),
        "listener must survive rejected bindAddress writes: {sub2}"
    );
}

/// Tunnel-only advertisement: with `server.tunnel.only = true` the listener
/// binds loopback regardless of a wide `server.bindAddress`, and both
/// `pairing.getInfo` hosts and `system.status` localIps advertise exactly
/// 127.0.0.1 — never the machine's interface enumeration, whose routes are
/// all dead in this posture (direct LAN connects are refused).
#[tokio::test]
async fn tunnel_only_advertises_loopback_only() {
    let data_dir = temp_data_dir();
    // Seed a wide bindAddress alongside tunnel-only BEFORE boot, so the test
    // proves the loopback override wins on the advertised surfaces.
    // configure_serve appends [server.wsApi] after these tables.
    std::fs::write(
        data_dir.join("config.toml"),
        "[server]\nbindAddress = \"0.0.0.0\"\n\n[server.tunnel]\nonly = true\n",
    )
    .expect("seed config.toml");
    let env: [(&str, &str); 1] = [("INTENTD_AUTH_TOKEN", TOKEN)];
    let child = spawn_serve(&data_dir, "both", &env);
    let _daemon = Daemon {
        child,
        data_dir: data_dir.clone(),
        cleanup_data_dir: true,
    };
    let socket = data_dir.join("intentd.sock");
    assert!(await_uds(&socket).await, "daemon did not start");

    let status = common::await_wss_status_logged(&socket, &data_dir.join("daemon.log")).await;
    assert_eq!(
        status["result"]["localIps"],
        json!(["127.0.0.1"]),
        "tunnel-only: system.status localIps is exactly loopback despite the \
         wide bindAddress: {status}"
    );
    let info = uds_rpc(&socket, 2, "pairing.getInfo", json!({})).await;
    assert_eq!(
        info["result"]["hosts"],
        json!(["127.0.0.1"]),
        "tunnel-only: pairing advertises exactly loopback: {info}"
    );
}

/// Write an executable fake-tailcat script into `dir`: `genkey` creates the
/// key file, `serve` prints the JSON address derived from the key file's
/// content and sleeps (mirrors the unit-test seam in src/tunnel.rs).
fn write_fake_tailcat(dir: &Path) -> PathBuf {
    use std::os::unix::fs::PermissionsExt;
    let path = dir.join("fake-tailcat.sh");
    let script = r#"#!/bin/sh
key=""
for arg in "$@"; do
  case "$arg" in
    --key=*) key="${arg#--key=}" ;;
  esac
done
case "$1" in
  genkey)
    printf 'key-%s' $$ > "$key"
    ;;
  serve)
    printf '{"listenAddr":"tc-%s"}\n' "$(cat "$key")"
    sleep 600
    ;;
esac
"#;
    std::fs::write(&path, script).expect("write fake tailcat");
    std::fs::set_permissions(&path, std::fs::Permissions::from_mode(0o755))
        .expect("chmod fake tailcat");
    path
}

/// server.tunnel.* settings over the real TLS/WSS path (per
/// packages/intentd/AGENTS.md): enable the tunnel via settings.update over an
/// authenticated WSS connection (fake tailcat sidecar via the
/// `INTENTD_TAILCAT_BIN` seam), read it back over WSS, change derpUrl while
/// the sidecar runs, and prove the tunnel-only TCP self-termination guard
/// fires for a WSS caller and rolls the setting back.
#[tokio::test]
async fn tunnel_settings_over_wss() {
    let data_dir = temp_data_dir();
    let tailcat = write_fake_tailcat(&data_dir);
    let tailcat_s = tailcat.to_string_lossy().to_string();
    let env: [(&str, &str); 3] = [
        ("INTENTD_AUTH_TOKEN", TOKEN),
        ("INTENTD_TCP_PORT", "0"),
        ("INTENTD_TAILCAT_BIN", &tailcat_s),
    ];
    let child = spawn_serve(&data_dir, "both", &env);
    let _daemon = Daemon {
        child,
        data_dir: data_dir.clone(),
        cleanup_data_dir: true,
    };
    let socket = data_dir.join("intentd.sock");
    assert!(await_uds(&socket).await, "daemon did not start");

    let status = common::await_wss_status_logged(&socket, &data_dir.join("daemon.log")).await;
    let port = u16::try_from(
        status["result"]["port"]
            .as_u64()
            .expect("port should be set"),
    )
    .expect("value fits in u16");
    let fingerprint = status["result"]["fingerprint"]
        .as_str()
        .expect("fingerprint should be set")
        .to_string();
    let cfg = client_config(&fingerprint);
    let mut ws = connect_ws(port, cfg.clone()).await;

    // Enable the tunnel over the real WSS path. The hook errors when the
    // sidecar fails to report an address, so a success response proves the
    // supervised fake tailcat started and reported its stable tc address.
    let enable = wss_rpc(
        &mut ws,
        1,
        "settings.update",
        json!({ "changes": [{ "path": "server.tunnel.enabled", "value": true }] }),
    )
    .await;
    assert!(
        enable.get("error").is_none(),
        "settings.update server.tunnel.enabled over WSS should succeed: {enable}"
    );
    let got = wss_rpc(
        &mut ws,
        2,
        "settings.get",
        json!({ "path": "server.tunnel.enabled" }),
    )
    .await;
    assert_eq!(got["result"]["value"], json!(true), "{got}");

    // derpUrl change over WSS: persists and restarts the running sidecar
    // (an error would surface here if the restart failed).
    let derp = wss_rpc(
        &mut ws,
        3,
        "settings.update",
        json!({ "changes": [{ "path": "server.tunnel.derpUrl", "value": "https://derp.example.com/map.json" }] }),
    )
    .await;
    assert!(
        derp.get("error").is_none(),
        "settings.update server.tunnel.derpUrl over WSS should succeed: {derp}"
    );
    let got = wss_rpc(
        &mut ws,
        4,
        "settings.get",
        json!({ "path": "server.tunnel.derpUrl" }),
    )
    .await;
    assert_eq!(
        got["result"]["value"],
        json!("https://derp.example.com/map.json"),
        "{got}"
    );

    // The tunnel-only self-termination guard fires for a direct TCP (WSS)
    // caller and the setting rolls back.
    let only = wss_rpc(
        &mut ws,
        5,
        "settings.update",
        json!({ "changes": [{ "path": "server.tunnel.only", "value": true }] }),
    )
    .await;
    let err = only
        .get("error")
        .unwrap_or_else(|| panic!("tunnel-only from a WSS caller must be refused: {only}"))
        .to_string();
    assert!(err.contains("server.tunnel.only"), "{only}");
    let got = wss_rpc(
        &mut ws,
        6,
        "settings.get",
        json!({ "path": "server.tunnel.only" }),
    )
    .await;
    assert_eq!(
        got["result"]["value"],
        json!(false),
        "must not flip on: {got}"
    );

    // Disabling the tunnel over WSS is allowed (only the listener disable and
    // tunnel-only enable are TCP-guarded).
    let disable = wss_rpc(
        &mut ws,
        7,
        "settings.update",
        json!({ "changes": [{ "path": "server.tunnel.enabled", "value": false }] }),
    )
    .await;
    assert!(
        disable.get("error").is_none(),
        "settings.update server.tunnel.enabled=false over WSS should succeed: {disable}"
    );
}
