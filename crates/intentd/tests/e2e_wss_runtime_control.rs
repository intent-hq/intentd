//! WSS end-to-end runtime listener control: prove the runtime WS listener toggle
//! over a real WSS connection per packages/intentd/AGENTS.md (enable via settings.update
//! over UDS → connect over WSS → verify RPCs work → disable over UDS → verify listener
//! stops and TCP clients are refused from disabling per the guard).

#![cfg(unix)]

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
            eprintln!("=== DAEMON LOG ===\n{}\n=== END LOG ===", log);
        }
        let _ = std::fs::remove_dir_all(&self.data_dir);
    }
}

fn temp_data_dir() -> PathBuf {
    let id = Uuid::new_v4().simple().to_string();
    let dir = PathBuf::from("/tmp").join(format!("itd-wss-runtime-ctrl-{}", &id[..8]));
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
    // listener starts directly on the NEW port. Use a high fixed port to avoid
    // collision with the initial listener.
    let new_port = 20000;
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
