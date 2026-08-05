//! WSS end-to-end `script.*` persistence: drives the real pinned-TLS WebSocket
//! against a live `intentd serve` (WSS listener enabled via config), creates a script, restarts the
//! daemon on the same data dir, and asserts the definition survives (hydrated
//! with a fresh idle runtime state) — then that `script.remove` unpersists it
//! across another restart. Regression for the registry living only in memory.

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
use tokio::net::{TcpStream, UnixStream};
use tokio::time::timeout;
use tokio_tungstenite::tungstenite::Message;
use tokio_tungstenite::WebSocketStream;
use uuid::Uuid;

const TOKEN: &str = "abababababababababababababababababababababababababababababababab";

fn scratch_dir(prefix: &str) -> PathBuf {
    let id = Uuid::new_v4().simple().to_string();
    let dir = PathBuf::from("/tmp").join(format!("itd-wss-scr-{prefix}-{}", &id[..8]));
    std::fs::create_dir_all(&dir).expect("mkdir scratch dir");
    dir
}

fn spawn_serve(data_dir: &Path) -> Child {
    let log = std::fs::File::options()
        .create(true)
        .append(true)
        .open(data_dir.join("daemon.log"))
        .expect("open daemon log");
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
        let next = timeout(Duration::from_secs(15), ws.next())
            .await
            .expect("wss rpc timed out");
        match next {
            Some(Ok(Message::Text(text))) => {
                let v: Value = serde_json::from_str(&text).expect("json frame");
                if v["id"] == json!(id) {
                    assert!(v.get("error").is_none(), "rpc {method} errored: {v}");
                    return v["result"].clone();
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

/// Boot (or re-boot) a daemon over an existing data dir, returning the child
/// and a pinned-TLS WSS client config for its live port.
async fn boot(data_dir: &Path) -> (Child, u16, Arc<ClientConfig>) {
    let child = spawn_serve(data_dir);
    let socket = data_dir.join("intentd.sock");
    assert!(await_uds(&socket).await, "daemon did not start");
    let status = common::await_wss_status(&socket).await;
    let port = status["result"]["port"].as_u64().expect("port") as u16;
    let fingerprint = status["result"]["fingerprint"]
        .as_str()
        .expect("fingerprint")
        .to_string();
    (child, port, client_config(&fingerprint))
}

fn stop(mut child: Child) {
    let _ = child.kill();
    let _ = child.wait();
}

/// `script.create` persists the definition; a daemon restart on the same data
/// dir hydrates it back into `script.list` (fresh idle runtime), and
/// `script.remove` unpersists it across yet another restart.
#[tokio::test]
async fn scripts_survive_daemon_restart_over_wss() {
    let data_dir = scratch_dir("data");

    // Boot #1: create a script over WSS.
    let (child, port, cfg) = boot(&data_dir).await;
    let mut ws = connect_ws(port, cfg).await;
    let created = wss_rpc(
        &mut ws,
        2,
        "script.create",
        json!({
            "workspaceId": "ws-scripts",
            "name": "dev server",
            "command": "npm run dev",
            "mode": "service",
            "cwd": "web",
            "env": { "PORT": "3000" },
            "category": "dev",
            "autoStart": true,
            "scriptId": "persist-1",
        }),
    )
    .await;
    assert_eq!(created["id"], "persist-1");
    drop(ws);
    stop(child);

    // Boot #2: the definition survives with a fresh idle runtime state.
    let (child, port, cfg) = boot(&data_dir).await;
    let mut ws = connect_ws(port, cfg).await;
    let listed = wss_rpc(
        &mut ws,
        3,
        "script.list",
        json!({ "workspaceId": "ws-scripts" }),
    )
    .await;
    let scripts = listed["scripts"].as_array().expect("scripts array");
    assert_eq!(scripts.len(), 1, "persisted script hydrated: {listed}");
    let entry = &scripts[0];
    assert_eq!(entry["id"], "persist-1");
    assert_eq!(entry["name"], "dev server");
    assert_eq!(entry["command"], "npm run dev");
    assert_eq!(entry["cwd"], "web");
    assert_eq!(entry["env"]["PORT"], "3000");
    assert_eq!(entry["mode"], "service");
    assert_eq!(entry["category"], "dev");
    assert_eq!(entry["autoStart"], true);
    assert_eq!(entry["source"], "user");
    assert_eq!(entry["runtime"]["status"], "idle");

    // Remove it, restart again: it stays gone.
    let removed = wss_rpc(
        &mut ws,
        4,
        "script.remove",
        json!({ "workspaceId": "ws-scripts", "scriptId": "persist-1" }),
    )
    .await;
    assert_eq!(removed["ok"], json!(true));
    drop(ws);
    stop(child);

    let (child, port, cfg) = boot(&data_dir).await;
    let mut ws = connect_ws(port, cfg).await;
    let listed = wss_rpc(
        &mut ws,
        5,
        "script.list",
        json!({ "workspaceId": "ws-scripts" }),
    )
    .await;
    assert_eq!(
        listed["scripts"].as_array().expect("scripts array").len(),
        0,
        "removed script stays unpersisted: {listed}"
    );
    drop(ws);
    stop(child);
    let _ = std::fs::remove_dir_all(&data_dir);
}
