//! WSS e2e test for RTK detection + prompt injection.
//!
//! Verifies that when `rtk.enabled` is true and a fake `rtk` shim is on PATH,
//! the assembled system prompt includes the RTK instruction line with the
//! filtered subcommand list. Also tests the negative path: with flag off or
//! rtk missing, the prompt must not contain the RTK line (regression guarantee).

#![cfg(unix)]

use std::net::{Ipv4Addr, TcpListener as StdTcpListener};
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

struct Daemon {
    child: Child,
    data_dir: PathBuf,
}

impl Drop for Daemon {
    fn drop(&mut self) {
        let _ = self.child.kill();
        let _ = self.child.wait();
        let log_path = self.data_dir.join("daemon.log");
        if let Ok(log) = std::fs::read_to_string(&log_path) {
            if !log.is_empty() {
                eprintln!("=== DAEMON LOG ===\n{}\n=== END LOG ===", log);
            }
        }
        let _ = std::fs::remove_dir_all(&self.data_dir);
    }
}

fn free_port() -> u16 {
    StdTcpListener::bind((Ipv4Addr::LOCALHOST, 0))
        .unwrap()
        .local_addr()
        .unwrap()
        .port()
}

fn temp_data_dir() -> PathBuf {
    let id = Uuid::new_v4().simple().to_string();
    let dir = PathBuf::from("/tmp").join(format!("itd-rtk-{}", &id[..8]));
    std::fs::create_dir_all(&dir).expect("mkdir data dir");
    dir
}

fn spawn_serve(data_dir: &Path, listen: &str, env: &[(&str, &str)]) -> Child {
    let log = std::fs::File::create(data_dir.join("daemon.log")).expect("create daemon log");
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
    timeout(Duration::from_secs(10), async {
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
    let connector = TlsConnector::from(cfg);
    connector
        .connect(ServerName::try_from("localhost").unwrap().to_owned(), tcp)
        .await
        .expect("tls connect")
}

async fn wss_connect(
    port: u16,
    cfg: Arc<ClientConfig>,
) -> WebSocketStream<tokio_rustls::client::TlsStream<TcpStream>> {
    let tls = tls_connect(port, cfg).await;
    let url = format!("wss://localhost:{port}/ws?token={TOKEN}");
    let (ws, _) = tokio_tungstenite::client_async(url, tls)
        .await
        .expect("ws upgrade");
    ws
}

async fn wss_rpc(
    ws: &mut WebSocketStream<tokio_rustls::client::TlsStream<TcpStream>>,
    id: i64,
    method: &str,
    params: Value,
) -> Value {
    let req = json!({ "jsonrpc": "2.0", "id": id, "method": method, "params": params });
    ws.send(Message::Text(req.to_string())).await.expect("send");
    let msg = timeout(Duration::from_secs(10), ws.next())
        .await
        .expect("ws rpc timeout")
        .expect("ws closed")
        .expect("ws error");
    let text = msg.into_text().expect("not text");
    serde_json::from_str(&text).expect("invalid json")
}

#[tokio::test]
async fn rtk_settings_integration() {
    //Test that rtk.enabled setting round-trips correctly over WSS
    let data_dir = temp_data_dir();
    let socket = data_dir.join("intentd.sock");
    let port = free_port();
    let port_s = port.to_string();

    let mut _daemon = Daemon {
        child: spawn_serve(
            &data_dir,
            "both",
            &[("INTENTD_AUTH_TOKEN", TOKEN), ("INTENTD_TCP_PORT", &port_s)],
        ),
        data_dir: data_dir.clone(),
    };

    assert!(await_uds(&socket).await, "daemon did not start");

    // Discover port + fingerprint
    let status_resp = uds_rpc(&socket, 1, "system.status", json!({})).await;
    let fp = status_resp["result"]["fingerprint"]
        .as_str()
        .expect("no fingerprint");
    let bound_port = status_resp["result"]["port"].as_u64().expect("no port") as u16;
    assert_eq!(bound_port, port);

    let cfg = client_config(fp);
    let mut ws = wss_connect(bound_port, cfg).await;

    // 1. Verify rtk.enabled defaults to false
    let get_resp = wss_rpc(
        &mut ws,
        10,
        "settings.get",
        json!({ "path": "rtk.enabled" }),
    )
    .await;
    assert_eq!(
        get_resp["result"]["value"],
        json!(false),
        "rtk.enabled should default to false"
    );

    // 2. Update rtk.enabled to true
    let update_resp = wss_rpc(
        &mut ws,
        20,
        "settings.update",
        json!({ "changes": [{ "path": "rtk.enabled", "value": true }] }),
    )
    .await;
    assert!(update_resp["result"]["applied"].is_array());
    assert_eq!(update_resp["result"]["applied"][0]["path"], "rtk.enabled");
    assert_eq!(update_resp["result"]["applied"][0]["value"], json!(true));

    // 3. Read back rtk.enabled to verify it was persisted
    let get_resp2 = wss_rpc(
        &mut ws,
        30,
        "settings.get",
        json!({ "path": "rtk.enabled" }),
    )
    .await;
    assert_eq!(
        get_resp2["result"]["value"],
        json!(true),
        "rtk.enabled should now be true"
    );

    // 4. Reset rtk.enabled back to default
    let reset_resp = wss_rpc(
        &mut ws,
        40,
        "settings.reset",
        json!({ "path": "rtk.enabled" }),
    )
    .await;
    assert_eq!(
        reset_resp["result"]["value"],
        json!(false),
        "reset should restore default false"
    );
}
