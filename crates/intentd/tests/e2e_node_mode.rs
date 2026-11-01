//! Node mode end to end (monorepo nodes.md "Node mode"): a real `intentd serve`
//! started with `--node`, or with the persisted `daemon.mode = "node"`, serves
//! only the node-mode method list on every transport and refuses the rest with
//! `-32003` `data.code: "node-mode"`; `settings.*` is limited to the
//! `remoteAgents.*`, `daemon.*` and `server.*` keys; `daemon.mode` changes only
//! over the local socket; and the WSS listener refuses the guest invite
//! endpoint.

#![cfg(unix)]

use crate::common;

use intentd_test_support::GuardedChild;
use std::path::{Path, PathBuf};
use std::process::Stdio;
use std::sync::Arc;
use std::time::Duration;

use futures_util::{SinkExt, StreamExt};
use rustls::client::danger::{HandshakeSignatureValid, ServerCertVerified, ServerCertVerifier};
use rustls::crypto::CryptoProvider;
use rustls::{ClientConfig, DigitallySignedStruct, SignatureScheme};
use rustls_pki_types::{CertificateDer, ServerName, UnixTime};
use serde_json::{json, Value};
use sha2::{Digest, Sha256};
use tokio::io::{AsyncBufReadExt, AsyncReadExt, AsyncWriteExt, BufReader};
use tokio::net::unix::{OwnedReadHalf, OwnedWriteHalf};
use tokio::net::UnixStream;
use tokio::time::timeout;
use tokio_tungstenite::tungstenite::Message;

const TOKEN: &str = "abababababababababababababababababababababababababababababababab";

struct Daemon {
    child: GuardedChild,
    data_dir: PathBuf,
}

impl Daemon {
    fn spawn(data_dir: &Path, node_flag: bool, wss: bool) -> Self {
        let log = std::fs::File::create(data_dir.join("daemon.log")).expect("create daemon log");
        let workspaces_dir = data_dir.join("workspaces");
        std::fs::create_dir_all(&workspaces_dir).expect("mkdir hermetic workspaces dir");
        if wss {
            common::enable_ws_api(data_dir);
        }
        let mut cmd = common::hermetic_serve_command(data_dir);
        if node_flag {
            cmd.arg("--node");
        }
        cmd.env("INTENTD_WORKSPACES_DIR", &workspaces_dir)
            .env("INTENTD_ASSERT_HERMETIC_ROOT", "1")
            .env("INTENTD_AUTH_TOKEN", TOKEN)
            .stdout(Stdio::null())
            .stderr(Stdio::from(log));
        common::hermetic_fixture_identity(&mut cmd, data_dir);
        Self {
            child: GuardedChild::spawn(&mut cmd).expect("spawn intentd serve"),
            data_dir: data_dir.to_path_buf(),
        }
    }

    async fn ready(&mut self) -> Uds {
        let socket = self.data_dir.join("intentd.sock");
        let log = self.data_dir.join("daemon.log");
        common::await_daemon_listening(&mut self.child, &socket, &log).await;
        Uds::connect(&socket).await
    }

    /// Stop gracefully and wait for the process, so a restart on the same
    /// data dir does not race the single-instance guard.
    async fn stop(mut self, uds: &mut Uds) {
        let reply = uds.call("system.shutdown", json!({})).await;
        assert!(reply.get("error").is_none(), "system.shutdown: {reply}");
        let exited = self
            .child
            .wait_with_timeout(common::daemon_startup_timeout())
            .expect("wait for daemon exit");
        assert!(
            exited.is_some(),
            "daemon did not exit after system.shutdown"
        );
    }
}

impl Drop for Daemon {
    fn drop(&mut self) {
        let _ = self.child.kill_tree();
        if std::thread::panicking() {
            if let Ok(log) = std::fs::read_to_string(self.data_dir.join("daemon.log")) {
                eprintln!("=== DAEMON LOG ===\n{log}\n=== END LOG ===");
            }
        }
    }
}

struct Uds {
    reader: BufReader<OwnedReadHalf>,
    writer: OwnedWriteHalf,
    next_id: i64,
}

impl Uds {
    async fn connect(socket: &Path) -> Self {
        let (read, writer) = UnixStream::connect(socket)
            .await
            .expect("connect uds")
            .into_split();
        Self {
            reader: BufReader::new(read),
            writer,
            next_id: 1,
        }
    }

    async fn send(&mut self, frame: &Value) {
        let mut line = frame.to_string();
        line.push('\n');
        self.writer.write_all(line.as_bytes()).await.expect("write");
        self.writer.flush().await.expect("flush");
    }

    async fn call(&mut self, method: &str, params: Value) -> Value {
        let id = self.next_id;
        self.next_id += 1;
        self.send(&json!({"jsonrpc": "2.0", "id": id, "method": method, "params": params}))
            .await;
        loop {
            let mut line = String::new();
            let n = timeout(
                intentd_test_support::test_timeout(Duration::from_secs(10)),
                self.reader.read_line(&mut line),
            )
            .await
            .expect("uds rpc timeout")
            .expect("uds read");
            assert!(n > 0, "uds closed during {method}");
            let value: Value = serde_json::from_str(line.trim_end()).expect("json frame");
            if value["id"] == id {
                return value;
            }
        }
    }
}

#[track_caller]
fn assert_node_mode_refusal(reply: &Value, what: &str) {
    assert_eq!(reply["error"]["code"], -32003, "{what}: {reply}");
    assert_eq!(
        reply["error"]["data"]["code"], "node-mode",
        "{what}: {reply}"
    );
}

#[track_caller]
fn assert_ok(reply: &Value, what: &str) -> Value {
    assert!(reply.get("error").is_none(), "{what}: {reply}");
    reply["result"].clone()
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

async fn wss_call(ws: &mut common::TlsWs, id: i64, method: &str, params: Value) -> Value {
    let req = json!({ "jsonrpc": "2.0", "id": id, "method": method, "params": params });
    ws.send(Message::Text(req.to_string().into()))
        .await
        .expect("send");
    loop {
        let msg = timeout(
            intentd_test_support::test_timeout(Duration::from_secs(10)),
            ws.next(),
        )
        .await
        .expect("wss rpc timeout")
        .expect("wss closed")
        .expect("wss error");
        if let Message::Text(text) = msg {
            let value: Value = serde_json::from_str(&text).expect("json frame");
            if value["id"] == id {
                return value;
            }
        }
    }
}

/// The HTTP status line of a WebSocket upgrade to `path` on the WSS listener.
async fn upgrade_status(port: u16, cfg: Arc<ClientConfig>, path: &str) -> String {
    let mut tls = common::tls_connect_with_retry(port, cfg).await;
    let request = format!(
        "GET {path} HTTP/1.1\r\nHost: localhost:{port}\r\nUpgrade: websocket\r\nConnection: Upgrade\r\nSec-WebSocket-Version: 13\r\nSec-WebSocket-Key: dGhlIHNhbXBsZSBub25jZQ==\r\n\r\n"
    );
    tls.write_all(request.as_bytes())
        .await
        .expect("write upgrade");
    let mut buf = vec![0_u8; 256];
    let n = timeout(
        intentd_test_support::test_timeout(Duration::from_secs(10)),
        tls.read(&mut buf),
    )
    .await
    .expect("upgrade response timeout")
    .expect("read upgrade response");
    String::from_utf8_lossy(&buf[..n])
        .lines()
        .next()
        .unwrap_or_default()
        .to_string()
}

#[tokio::test]
async fn serve_node_serves_only_the_node_mode_list() {
    let dir = common::test_tempdir_in("/tmp", "itd-node-");
    let mut daemon = Daemon::spawn(dir.path(), true, true);
    let mut uds = daemon.ready().await;

    // Listed methods are served over the local socket.
    let status = assert_ok(&uds.call("system.status", json!({})).await, "system.status");
    assert!(status.is_object());
    assert_ok(&uds.call("principal.me", json!({})).await, "principal.me");
    assert_ok(&uds.call("executor.list", json!({})).await, "executor.list");
    let enabled = assert_ok(
        &uds.call("settings.get", json!({ "path": "remoteAgents.enabled" }))
            .await,
        "settings.get remoteAgents.enabled",
    );
    assert_eq!(enabled["value"], json!(false));
    assert_ok(
        &uds.call(
            "settings.update",
            json!({ "changes": [{ "path": "remoteAgents.enabled", "value": true }] }),
        )
        .await,
        "settings.update remoteAgents.enabled",
    );
    assert_ok(
        &uds.call(
            "settings.update",
            json!({ "changes": [{ "path": "daemon.mode", "value": "regular" }] }),
        )
        .await,
        "settings.update daemon.mode over UDS",
    );

    // Orchestration and everything unlisted is refused, even to the owner.
    for (method, params) in [
        ("workspace.list", json!({})),
        ("workspace.create", json!({ "title": "x" })),
        ("agent.listActive", json!({})),
        ("hook.list", json!({})),
        ("script.list", json!({ "workspaceId": "w" })),
        ("settings.list", json!({})),
        ("host.exec", json!({ "command": "true" })),
    ] {
        assert_node_mode_refusal(&uds.call(method, params).await, method);
    }
    for path in ["agents.maxConcurrent", "experimental.remoteAgents"] {
        assert_node_mode_refusal(
            &uds.call("settings.get", json!({ "path": path })).await,
            path,
        );
        assert_node_mode_refusal(
            &uds.call(
                "settings.update",
                json!({ "changes": [{ "path": path, "value": 1 }] }),
            )
            .await,
            path,
        );
    }
    // A refused notification gets no reply; the connection keeps serving.
    uds.send(&json!({"jsonrpc": "2.0", "method": "workspace.list", "params": {}}))
        .await;
    assert_ok(
        &uds.call("system.status", json!({})).await,
        "after notification",
    );

    // The same gate applies over WSS; daemon.mode is local-only.
    let status = common::await_wss_status(&dir.path().join("intentd.sock")).await;
    let fp = status["result"]["fingerprint"]
        .as_str()
        .expect("fp")
        .to_string();
    let port = u16::try_from(status["result"]["port"].as_u64().expect("port")).expect("port");
    let url = format!("wss://localhost:{port}/ws?token={TOKEN}");
    let mut ws = common::wss_connect_with_retry(port, client_config(&fp), &url).await;
    assert_ok(
        &wss_call(&mut ws, 1, "principal.me", json!({})).await,
        "wss principal.me",
    );
    assert_node_mode_refusal(
        &wss_call(&mut ws, 2, "workspace.list", json!({})).await,
        "wss workspace.list",
    );
    let reply = wss_call(
        &mut ws,
        3,
        "settings.update",
        json!({ "changes": [{ "path": "daemon.mode", "value": "node" }] }),
    )
    .await;
    assert_eq!(reply["error"]["code"], -32003, "wss daemon.mode: {reply}");
    assert_eq!(
        reply["error"]["data"]["code"], "forbidden",
        "wss daemon.mode: {reply}"
    );
    assert_ok(
        &wss_call(
            &mut ws,
            4,
            "settings.update",
            json!({ "changes": [{ "path": "remoteAgents.permission", "value": "denyAll" }] }),
        )
        .await,
        "wss settings.update remoteAgents.permission",
    );

    // The guest invite endpoint and the preview tunnel are refused.
    for path in ["/invite", "/tunnel"] {
        let line = upgrade_status(port, client_config(&fp), path).await;
        assert!(line.contains(" 403 "), "{path}: {line}");
    }
    drop(ws);
    daemon.stop(&mut uds).await;
}

#[tokio::test]
async fn persisted_daemon_mode_selects_node_mode_at_next_start() {
    let dir = common::test_tempdir_in("/tmp", "itd-nodeset-");

    // Regular start: the setting is accepted over UDS but applies next start.
    let mut daemon = Daemon::spawn(dir.path(), false, false);
    let mut uds = daemon.ready().await;
    assert_ok(
        &uds.call("workspace.list", json!({})).await,
        "regular workspace.list",
    );
    assert_ok(
        &uds.call(
            "settings.update",
            json!({ "changes": [{ "path": "daemon.mode", "value": "node" }] }),
        )
        .await,
        "settings.update daemon.mode node",
    );
    assert_ok(
        &uds.call("workspace.list", json!({})).await,
        "still regular",
    );
    daemon.stop(&mut uds).await;

    // Next start (no flag) is node mode; switching back also waits a restart.
    let mut daemon = Daemon::spawn(dir.path(), false, false);
    let mut uds = daemon.ready().await;
    assert_node_mode_refusal(&uds.call("workspace.list", json!({})).await, "node start");
    assert_ok(
        &uds.call(
            "settings.update",
            json!({ "changes": [{ "path": "daemon.mode", "value": "regular" }] }),
        )
        .await,
        "settings.update daemon.mode regular",
    );
    assert_node_mode_refusal(
        &uds.call("workspace.list", json!({})).await,
        "until restart",
    );
    daemon.stop(&mut uds).await;

    let mut daemon = Daemon::spawn(dir.path(), false, false);
    let mut uds = daemon.ready().await;
    assert_ok(
        &uds.call("workspace.list", json!({})).await,
        "regular again",
    );
    daemon.stop(&mut uds).await;
}
