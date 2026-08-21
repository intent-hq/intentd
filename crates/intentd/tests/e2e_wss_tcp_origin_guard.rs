//! WSS e2e: TCP connection origin guard regression test.
//!
//! Proves that the task-local connection origin context (UDS vs TCP) survives
//! into spawned handler tasks and correctly enforces local-only guards for:
//! 1. `server.pairingInfo` / `server.rotateToken` (fast-path, inline)
//! 2. `settings.update` with `server.wsApi.enabled=false` (slow-path, spawned)
//!
//! This locks in the invariant documented in `intent-transport/src/context.rs`:
//! - Transport layer establishes context via `with_connection_context(is_tcp, ...)`
//! - Spawned tasks re-establish context by reading before spawn and wrapping work
//! - Origin checks run within established context (never missing)
//!
//! The test drives an ADVERSARIAL scenario: a remote WSS client attempts to:
//! a) call `server.rotateToken` (fast-path) → expect -32001
//! b) disable `server.wsApi.enabled` (slow-path, spawned) → expect `InvalidParams` error
//!
//! Both must refuse, proving the origin context survives into spawned tasks.

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
use tokio::io::{AsyncBufReadExt, AsyncWriteExt};
use tokio::net::TcpStream;
use tokio::time::timeout;
use tokio_tungstenite::tungstenite::Message;
use tokio_tungstenite::WebSocketStream;
use uuid::Uuid;

const TOKEN: &str = "0f0f0f0f0f0f0f0f0f0f0f0f0f0f0f0f0f0f0f0f0f0f0f0f0f0f0f0f0f0f0f0f";

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
    let dir = PathBuf::from("/tmp").join(format!("itd-tcp-guard-{}", &id[..8]));
    std::fs::create_dir_all(&dir).expect("mkdir data dir");
    dir
}

fn spawn_serve(data_dir: &Path) -> Child {
    let log = std::fs::File::create(data_dir.join("daemon.log")).expect("create daemon log");
    let workspaces_dir = data_dir.join("workspaces");
    std::fs::create_dir_all(&workspaces_dir).expect("mkdir hermetic workspaces dir");
    common::enable_ws_api(data_dir);
    let mut cmd = Command::new(env!("CARGO_BIN_EXE_intentd"));
    cmd.arg("serve")
        .env("INTENTD_DATA_DIR", data_dir)
        .env("INTENTD_WORKSPACES_DIR", &workspaces_dir)
        .env("INTENTD_TCP_PORT", "0")
        .env("INTENTD_AUTH_TOKEN", TOKEN)
        .env("INTENTD_ASSERT_HERMETIC_ROOT", "1")
        .env("MOCK_ACP_HOST", "localhost:0")
        .stdout(Stdio::null())
        .stderr(Stdio::from(log));
    cmd.spawn().expect("spawn intentd serve")
}

async fn await_uds(socket: &Path) -> bool {
    tokio::time::timeout(common::daemon_startup_timeout(), async {
        loop {
            if tokio::net::UnixStream::connect(socket).await.is_ok() {
                return;
            }
            tokio::time::sleep(Duration::from_millis(50)).await;
        }
    })
    .await
    .is_ok()
}

async fn uds_rpc(socket: &Path, id: i64, method: &str, params: Value) -> Value {
    let stream = tokio::net::UnixStream::connect(socket)
        .await
        .expect("UDS connect");
    let (read_half, mut write_half) = stream.into_split();
    let frame = json!({ "jsonrpc": "2.0", "id": id, "method": method, "params": params });
    let mut line = frame.to_string();
    line.push('\n');
    write_half.write_all(line.as_bytes()).await.unwrap();
    write_half.flush().await.unwrap();
    let mut reader = tokio::io::BufReader::new(read_half);
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
    Arc::new(
        ClientConfig::builder_with_provider(provider.clone())
            .with_safe_default_protocol_versions()
            .unwrap()
            .dangerous()
            .with_custom_certificate_verifier(Arc::new(PinnedVerifier {
                fingerprint: fingerprint.to_string(),
                provider,
            }))
            .with_no_client_auth(),
    )
}

async fn connect_ws(
    port: u16,
    cfg: Arc<ClientConfig>,
) -> WebSocketStream<tokio_rustls::client::TlsStream<TcpStream>> {
    let url = format!("wss://localhost:{port}/ws?token={TOKEN}");
    common::wss_connect_with_retry(port, cfg, &url).await
}

async fn wss_rpc(
    ws: &mut WebSocketStream<tokio_rustls::client::TlsStream<TcpStream>>,
    id: i64,
    method: &str,
    params: Value,
) -> Value {
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

async fn boot(data_dir: &Path) -> (u16, String) {
    let socket = data_dir.join("intentd.sock");
    assert!(await_uds(&socket).await, "daemon did not start");
    let status = common::await_wss_status(&socket).await;
    let actual_port =
        u16::try_from(status["result"]["port"].as_u64().expect("port")).expect("value fits in u16");
    let fingerprint = status["result"]["fingerprint"]
        .as_str()
        .expect("fingerprint")
        .to_string();
    (actual_port, fingerprint)
}

/// Adversarial test: remote WSS client attempts to call local-only `server.rotateToken`.
/// This is a fast-path handler (inline on read loop), proving the origin context is
/// visible at the call site (conn.rs:138).
#[tokio::test]
async fn tcp_client_refused_server_rotate_token_fast_path() {
    let data_dir = temp_data_dir();
    let mut daemon = Daemon {
        child: spawn_serve(&data_dir),
        data_dir: data_dir.clone(),
    };
    let (port, fp) = boot(&data_dir).await;
    let cfg = client_config(&fp);

    // Remote WSS client attempts server.rotateToken (local-only)
    let mut ws = connect_ws(port, cfg.clone()).await;
    let response = wss_rpc(&mut ws, 10, "server.rotateToken", json!({})).await;

    // Expect -32001 auth error (local-only gating)
    let error = &response["error"];
    assert_eq!(
        error["code"].as_i64().unwrap(),
        -32001,
        "server.rotateToken over WSS must be rejected with -32001: {response}"
    );
    assert!(
        error["message"].as_str().unwrap().contains("local"),
        "error message should mention local-only: {response}"
    );

    daemon.child.kill().ok();
}

/// Adversarial test: remote WSS client attempts to disable `server.wsApi.enabled` via
/// `settings.update`. This is a slow-path handler (spawned task, conn.rs:220-227),
/// proving the origin context survives into the spawned `handle_message` task.
#[tokio::test]
async fn tcp_client_refused_settings_disable_wss_slow_path() {
    let data_dir = temp_data_dir();
    let mut daemon = Daemon {
        child: spawn_serve(&data_dir),
        data_dir: data_dir.clone(),
    };
    let (port, fp) = boot(&data_dir).await;
    let cfg = client_config(&fp);

    // Remote WSS client attempts to disable server.wsApi.enabled (would self-terminate)
    let mut ws = connect_ws(port, cfg.clone()).await;
    let response = wss_rpc(
        &mut ws,
        10,
        "settings.update",
        json!({ "changes": [{ "path": "server.wsApi.enabled", "value": false }] }),
    )
    .await;

    // Expect exact InvalidParams error code (-32602) and TCP connection message
    let error = &response["error"];
    assert_eq!(
        error["code"].as_i64(),
        Some(-32602),
        "settings.update disable from WSS must be rejected with InvalidParams (-32602): {response}"
    );
    assert!(
        error["message"]
            .as_str()
            .unwrap()
            .contains("TCP connection"),
        "error message should mention TCP connection: {response}"
    );

    daemon.child.kill().ok();
}

/// Positive control: UDS client CAN disable `server.wsApi.enabled` (local origin).
#[tokio::test]
async fn uds_client_allowed_settings_disable_wss() {
    let data_dir = temp_data_dir();
    let mut daemon = Daemon {
        child: spawn_serve(&data_dir),
        data_dir: data_dir.clone(),
    };
    let (_port, _fp) = boot(&data_dir).await;
    let socket = data_dir.join("intentd.sock");

    // UDS client (local) disables server.wsApi.enabled
    let response = uds_rpc(
        &socket,
        10,
        "settings.update",
        json!({ "changes": [{ "path": "server.wsApi.enabled", "value": false }] }),
    )
    .await;

    // Expect success
    assert!(
        response.get("error").is_none(),
        "settings.update disable from UDS must not return error: {response}"
    );
    assert!(
        response["result"]["applied"].is_array(),
        "settings.update disable from UDS must return applied array: {response}"
    );

    daemon.child.kill().ok();
}

/// Regression test for --mode local: TCP clients must STILL be refused even when
/// daemon is started with `--mode local` or `server.locality = "local"`. This
/// verifies that spawned tasks capture the real transport origin (UDS vs TCP) from
/// `is_tcp_connection()` rather than deriving it from the locality flag.
#[tokio::test]
async fn tcp_client_refused_settings_disable_wss_when_mode_local() {
    let data_dir = temp_data_dir();

    // Spawn daemon with --mode local
    let log = std::fs::File::create(data_dir.join("daemon.log")).expect("create daemon log");
    let workspaces_dir = data_dir.join("workspaces");
    std::fs::create_dir_all(&workspaces_dir).expect("mkdir hermetic workspaces dir");
    common::enable_ws_api(&data_dir);
    let mut cmd = Command::new(env!("CARGO_BIN_EXE_intentd"));
    cmd.arg("serve")
        .arg("--mode")
        .arg("local")
        .env("INTENTD_DATA_DIR", &data_dir)
        .env("INTENTD_WORKSPACES_DIR", &workspaces_dir)
        .env("INTENTD_TCP_PORT", "0")
        .env("INTENTD_AUTH_TOKEN", TOKEN)
        .env("INTENTD_ASSERT_HERMETIC_ROOT", "1")
        .env("MOCK_ACP_HOST", "localhost:0")
        .stdout(Stdio::null())
        .stderr(Stdio::from(log));
    let child = cmd.spawn().expect("spawn intentd serve");

    let mut daemon = Daemon {
        child,
        data_dir: data_dir.clone(),
    };

    let (port, fp) = boot(&data_dir).await;
    let cfg = client_config(&fp);

    // Remote WSS client attempts to disable server.wsApi.enabled (would self-terminate)
    // Even though --mode local, this must STILL be refused because it's TCP transport
    let mut ws = connect_ws(port, cfg.clone()).await;
    let response = wss_rpc(
        &mut ws,
        10,
        "settings.update",
        json!({ "changes": [{ "path": "server.wsApi.enabled", "value": false }] }),
    )
    .await;

    // Expect exact InvalidParams error code (-32602) and TCP connection message
    let error = &response["error"];
    assert_eq!(
        error["code"].as_i64(),
        Some(-32602),
        "--mode local does not bypass TCP guard: settings.update disable from WSS must be rejected with InvalidParams (-32602): {response}"
    );
    assert!(
        error["message"]
            .as_str()
            .unwrap()
            .contains("TCP connection"),
        "error message should mention TCP connection: {response}"
    );

    daemon.child.kill().ok();
}
