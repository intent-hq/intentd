//! WSS e2e for local-only server/control methods (§5.2, §5.7).
//!
//! Drives the real WSS transport with TLS + bearer auth to prove:
//! - WSS (TCP) connections are rejected with -32001 (local-only gating)
//! - UDS connections receive credentials and can rotate tokens
//!
//! Per AGENTS.md: every new JSON-RPC method must have a WSS e2e test exercising
//! the full production path. These RPCs are local-only so the WSS path is the
//! rejection case; the success case is UDS.

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
use uuid::Uuid;

/// Fixed 64-char hex token (matching generated token shape) for e2e test.
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

fn temp_data_dir() -> PathBuf {
    let id = Uuid::new_v4().simple().to_string();
    let dir = PathBuf::from("/tmp").join(format!("itd-wss-server-{}", &id[..8]));
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
        .stdout(Stdio::null())
        .stderr(Stdio::from(log));
    cmd.env("MOCK_ACP_HOST", "localhost:0");
    cmd.spawn().expect("spawn intentd serve")
}

async fn await_uds(socket: &Path) -> bool {
    tokio::time::timeout(common::daemon_startup_timeout(), async {
        loop {
            if socket.exists() {
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
    timeout(Duration::from_secs(5), reader.read_line(&mut buf))
        .await
        .expect("uds rpc timed out")
        .expect("read uds response");
    serde_json::from_str(buf.trim_end()).expect("invalid JSON frame")
}

/// Pinned-fingerprint cert verifier (mirrors wss_integration.rs).
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
) -> tokio_tungstenite::WebSocketStream<tokio_rustls::client::TlsStream<TcpStream>> {
    let url = format!("wss://localhost:{port}/ws?token={TOKEN}");
    common::wss_connect_with_retry(port, cfg, &url).await
}

async fn wss_call(port: u16, cfg: Arc<ClientConfig>, frame: &str) -> Value {
    let mut ws = connect_ws(port, cfg).await;
    ws.send(Message::Text(frame.to_string()))
        .await
        .expect("send");
    loop {
        match ws.next().await {
            Some(Ok(Message::Text(text))) => return serde_json::from_str(&text).expect("json"),
            Some(Ok(_)) => continue,
            other => panic!("expected text frame, got {other:?}"),
        }
    }
}

async fn boot(data_dir: &Path) -> (u16, String) {
    let socket = data_dir.join("intentd.sock");
    assert!(await_uds(&socket).await, "daemon did not start");
    let status = common::await_wss_status(&socket).await;
    let actual_port = status["result"]["port"].as_u64().expect("port") as u16;
    let fingerprint = status["result"]["fingerprint"]
        .as_str()
        .expect("fingerprint")
        .to_string();
    (actual_port, fingerprint)
}

#[tokio::test]
async fn server_pairing_info_over_uds() {
    let data_dir = temp_data_dir();
    let mut daemon = Daemon {
        child: spawn_serve(&data_dir),
        data_dir: data_dir.clone(),
    };
    let (port, fp) = boot(&data_dir).await;
    let socket = data_dir.join("intentd.sock");

    // server.pairingInfo over UDS (local) returns credentials
    let response = uds_rpc(&socket, 2, "server.pairingInfo", json!({})).await;
    let result = &response["result"];

    assert_eq!(result["token"].as_str().unwrap(), TOKEN);
    assert_eq!(result["certFingerprint"].as_str().unwrap(), fp);
    assert_eq!(result["port"].as_u64().unwrap(), port as u64);
    assert_eq!(result["path"].as_str().unwrap(), "/ws");
    assert!(result["localIps"].is_array());
    assert!(result["hostname"].is_string());

    daemon.child.kill().ok();
}

#[tokio::test]
async fn server_rotate_token_env_fixed_rejects() {
    let data_dir = temp_data_dir();
    let mut daemon = Daemon {
        child: spawn_serve(&data_dir),
        data_dir: data_dir.clone(),
    };
    let (_port, _fp) = boot(&data_dir).await;
    let socket = data_dir.join("intentd.sock");

    // INTENTD_AUTH_TOKEN is set in spawn_serve, so rotation should reject over UDS
    let response = uds_rpc(&socket, 2, "server.rotateToken", json!({})).await;
    let error = &response["error"];

    assert_eq!(error["code"].as_i64().unwrap(), -32602); // InvalidParams
    assert!(error["message"]
        .as_str()
        .unwrap()
        .contains("cannot rotate token"));

    daemon.child.kill().ok();
}

#[tokio::test]
async fn server_pairing_info_over_wss_rejects() {
    let data_dir = temp_data_dir();
    let mut daemon = Daemon {
        child: spawn_serve(&data_dir),
        data_dir: data_dir.clone(),
    };
    let (port, fp) = boot(&data_dir).await;
    let cfg = client_config(&fp);

    // server.pairingInfo over WSS (TCP) is rejected with -32001
    let frame = json!({ "jsonrpc": "2.0", "id": 1, "method": "server.pairingInfo", "params": {} })
        .to_string();
    let response = wss_call(port, cfg.clone(), &frame).await;
    let error = &response["error"];

    assert_eq!(error["code"].as_i64().unwrap(), -32001);
    assert!(error["message"].as_str().unwrap().contains("local"));

    daemon.child.kill().ok();
}

#[tokio::test]
async fn pairing_get_info_over_uds() {
    let data_dir = temp_data_dir();
    let mut daemon = Daemon {
        child: spawn_serve(&data_dir),
        data_dir: data_dir.clone(),
    };
    let (port, fp) = boot(&data_dir).await;
    let socket = data_dir.join("intentd.sock");

    // pairing.getInfo over UDS (local) returns the structured QR payload
    let response = uds_rpc(&socket, 2, "pairing.getInfo", json!({})).await;
    let result = &response["result"];

    assert_eq!(result["token"].as_str().unwrap(), TOKEN);
    assert_eq!(result["fingerprint"].as_str().unwrap(), fp);
    assert_eq!(result["port"].as_u64().unwrap(), port as u64);
    assert_eq!(result["version"].as_u64().unwrap(), 1);
    assert!(result["hosts"].is_array());

    // The uri field is consistent with the component fields
    let hosts: Vec<String> = serde_json::from_value(result["hosts"].clone()).unwrap();
    let expected_uri = format!(
        "intent://pair?v=1&host={}&port={port}&fp={fp}&token={TOKEN}",
        hosts.join(",")
    );
    assert_eq!(result["uri"].as_str().unwrap(), expected_uri);

    daemon.child.kill().ok();
}

#[tokio::test]
async fn pairing_get_info_over_wss_rejects() {
    let data_dir = temp_data_dir();
    let mut daemon = Daemon {
        child: spawn_serve(&data_dir),
        data_dir: data_dir.clone(),
    };
    let (port, fp) = boot(&data_dir).await;
    let cfg = client_config(&fp);

    // pairing.getInfo over WSS (TCP) is rejected with -32001: the payload
    // embeds the bearer token, so it never crosses the network.
    let frame =
        json!({ "jsonrpc": "2.0", "id": 1, "method": "pairing.getInfo", "params": {} }).to_string();
    let response = wss_call(port, cfg.clone(), &frame).await;
    let error = &response["error"];

    assert_eq!(error["code"].as_i64().unwrap(), -32001);
    assert!(error["message"].as_str().unwrap().contains("local"));

    daemon.child.kill().ok();
}

#[tokio::test]
async fn server_rotate_token_over_wss_rejects() {
    let data_dir = temp_data_dir();
    let mut daemon = Daemon {
        child: spawn_serve(&data_dir),
        data_dir: data_dir.clone(),
    };
    let (port, fp) = boot(&data_dir).await;
    let cfg = client_config(&fp);

    // server.rotateToken over WSS (TCP) is rejected with -32001
    let frame = json!({ "jsonrpc": "2.0", "id": 1, "method": "server.rotateToken", "params": {} })
        .to_string();
    let response = wss_call(port, cfg.clone(), &frame).await;
    let error = &response["error"];

    assert_eq!(error["code"].as_i64().unwrap(), -32001);
    assert!(error["message"].as_str().unwrap().contains("local"));

    daemon.child.kill().ok();
}

#[tokio::test]
async fn system_import_legacy_over_wss_rejects() {
    let data_dir = temp_data_dir();
    let mut daemon = Daemon {
        child: spawn_serve(&data_dir),
        data_dir: data_dir.clone(),
    };
    let (port, fp) = boot(&data_dir).await;
    let frame = json!({
        "jsonrpc": "2.0", "id": 1, "method": "system.importLegacy",
        "params": { "force": false }
    })
    .to_string();
    let response = wss_call(port, client_config(&fp), &frame).await;

    assert_eq!(response["jsonrpc"], "2.0", "{response}");
    assert_eq!(response["id"], 1, "{response}");
    assert_eq!(response["error"]["code"], -32001, "{response}");
    assert!(response["error"]["message"]
        .as_str()
        .unwrap()
        .contains("UDS only"));
    daemon.child.kill().ok();
}
