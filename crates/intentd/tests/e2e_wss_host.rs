//! WSS end-to-end host-services (AUDIT-P2-1 / -P2-4): drives the additive
//! `host.*` detection methods — `host.findBinary`, `host.toolAvailability`,
//! `host.env`, `host.findApp`, and `host.listInstalledEditors` — over a real
//! pinned-TLS WebSocket against a live `intentd serve --listen both`. These
//! methods resolve binaries / PATH / environment / GUI apps on the daemon host
//! so a remote client sees what actually lives where workspaces run; this
//! suite proves the §5.14 wire contract end-to-end (HTTPS upgrade → JSON-RPC
//! 2.0 over WebSocket → host fast-path → response).
//!
//! Unlike the agent-lifecycle suite, host-services need neither a workspace nor
//! the mock ACP provider, so this test is self-contained and always runs.

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

/// Fixed 64-hex token, adopted by the daemon via the `INTENTD_AUTH_TOKEN` seam.
const TOKEN: &str = "cdcdcdcdcdcdcdcdcdcdcdcdcdcdcdcdcdcdcdcdcdcdcdcdcdcdcdcdcdcdcdcd";

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

fn free_port() -> u16 {
    StdTcpListener::bind((Ipv4Addr::LOCALHOST, 0))
        .unwrap()
        .local_addr()
        .unwrap()
        .port()
}

fn temp_data_dir() -> PathBuf {
    let id = Uuid::new_v4().simple().to_string();
    let dir = PathBuf::from("/tmp").join(format!("itd-wss-host-{}", &id[..8]));
    std::fs::create_dir_all(&dir).expect("mkdir data dir");
    dir
}

fn spawn_serve(data_dir: &Path, listen: &str, env: &[(&str, &str)]) -> Child {
    let log = std::fs::File::create(data_dir.join("daemon.log")).expect("create daemon log");
    let mut cmd = Command::new(env!("CARGO_BIN_EXE_intentd"));
    cmd.arg("serve")
        .arg("--listen")
        .arg(listen)
        .env("INTENTD_DATA_DIR", data_dir)
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

/// One UDS JSON-RPC round-trip (used only to discover bound port + fingerprint).
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

/// Open an authenticated WSS connection (token in the query string).
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

/// Send one JSON-RPC frame and return the result whose id matches.
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

/// Boot a daemon over `--listen both` and return the live handle + a pinned WSS
/// client config plus the bound TCP port (discovered via UDS `system.status`).
async fn boot() -> (Daemon, u16, Arc<ClientConfig>) {
    let data_dir = temp_data_dir();
    let port_s = free_port().to_string();
    let env: [(&str, &str); 2] = [("INTENTD_AUTH_TOKEN", TOKEN), ("INTENTD_TCP_PORT", &port_s)];
    let child = spawn_serve(&data_dir, "both", &env);
    let daemon = Daemon {
        child,
        data_dir: data_dir.clone(),
    };
    let socket = data_dir.join("intentd.sock");
    assert!(await_uds(&socket).await, "daemon did not start");
    let status = uds_rpc(&socket, 1, "system.status", json!({})).await;
    let port = status["result"]["port"].as_u64().expect("port") as u16;
    let fingerprint = status["result"]["fingerprint"]
        .as_str()
        .expect("fingerprint")
        .to_string();
    (daemon, port, client_config(&fingerprint))
}

/// host.findBinary / host.toolAvailability / host.env over the real WSS wire.
#[tokio::test]
async fn host_detection_services_over_wss() {
    let (_daemon, port, cfg) = boot().await;
    let mut ws = connect_ws(port, cfg).await;

    // §5.14 sanity: WSS connections report remote locality.
    let status = wss_rpc(&mut ws, 1, "host.status", json!({})).await;
    assert_eq!(status["locality"], "remote", "WSS ⇒ remote (§5.14)");

    // host.findBinary requires a `name` — missing ⇒ -32602 (PROTOCOL §9).
    {
        let frame = json!({ "jsonrpc": "2.0", "id": 2, "method": "host.findBinary", "params": {} });
        ws.send(Message::Text(frame.to_string())).await.unwrap();
        let err = loop {
            let next = timeout(Duration::from_secs(15), ws.next())
                .await
                .expect("timed out")
                .unwrap()
                .unwrap();
            if let Message::Text(t) = next {
                let v: Value = serde_json::from_str(&t).unwrap();
                if v["id"] == json!(2) {
                    break v;
                }
            }
        };
        assert_eq!(err["error"]["code"], -32602, "missing name ⇒ -32602: {err}");
    }

    // host.findBinary { name } ⇒ { available, path?, version? }. `git` ships on
    // the CI/host image, so assert the resolved shape rather than just a boolean.
    let git = wss_rpc(&mut ws, 3, "host.findBinary", json!({ "name": "git" })).await;
    assert!(
        git["available"].is_boolean(),
        "available always present: {git}"
    );
    if git["available"] == json!(true) {
        assert!(git["path"].is_string(), "available ⇒ path present: {git}");
    }

    // An unsafe binary name never errors — it resolves to available:false.
    let unsafe_name = wss_rpc(
        &mut ws,
        4,
        "host.findBinary",
        json!({ "name": "../../bin/sh" }),
    )
    .await;
    assert_eq!(unsafe_name["available"], false, "unsafe name ⇒ unavailable");

    // host.toolAvailability (default set) ⇒ { tools: { <name>: { available } } }.
    let tools = wss_rpc(&mut ws, 5, "host.toolAvailability", json!({})).await;
    let map = tools["tools"].as_object().expect("tools object");
    for name in ["claude", "codex", "cortex", "opencode", "git", "code"] {
        assert!(
            map.contains_key(name),
            "default set includes {name}: {tools}"
        );
        assert!(map[name]["available"].is_boolean());
    }

    // host.toolAvailability with an explicit list returns exactly those keys.
    let explicit = wss_rpc(
        &mut ws,
        6,
        "host.toolAvailability",
        json!({ "tools": ["git", "definitely-not-installed-xyzzy"] }),
    )
    .await;
    let explicit_map = explicit["tools"].as_object().unwrap();
    assert_eq!(explicit_map.len(), 2, "explicit list honoured: {explicit}");
    assert_eq!(
        explicit_map["definitely-not-installed-xyzzy"]["available"],
        false
    );

    // host.env ⇒ secret-safe PATH/env probe: path + entries + enhancedPath +
    // varNames (names only, no arbitrary values).
    let env = wss_rpc(&mut ws, 7, "host.env", json!({})).await;
    assert!(env["path"].is_string(), "path present: {env}");
    assert!(env["pathEntries"].is_array(), "pathEntries present: {env}");
    assert!(
        env["enhancedPath"].is_string(),
        "enhancedPath present: {env}"
    );
    assert!(env["varNames"].is_array(), "varNames present: {env}");
    // The auth token is injected as INTENTD_AUTH_TOKEN — its NAME may appear but
    // its VALUE must never cross the wire.
    assert!(
        !env.to_string().contains(TOKEN),
        "host.env must not leak secret env values"
    );
}

/// host.findApp / host.listInstalledEditors over the real WSS wire.
#[tokio::test]
async fn host_app_detection_services_over_wss() {
    let (_daemon, port, cfg) = boot().await;
    let mut ws = connect_ws(port, cfg).await;

    // host.findApp requires a `name` — missing ⇒ -32602 (PROTOCOL §9).
    {
        let frame = json!({ "jsonrpc": "2.0", "id": 100, "method": "host.findApp", "params": {} });
        ws.send(Message::Text(frame.to_string())).await.unwrap();
        let err = loop {
            let next = timeout(Duration::from_secs(15), ws.next())
                .await
                .expect("timed out")
                .unwrap()
                .unwrap();
            if let Message::Text(t) = next {
                let v: Value = serde_json::from_str(&t).unwrap();
                if v["id"] == json!(100) {
                    break v;
                }
            }
        };
        assert_eq!(err["error"]["code"], -32602, "missing name ⇒ -32602: {err}");
    }

    // host.findApp { name } ⇒ { installed, path?, source? }. A bogus but
    // syntactically-safe name resolves to `installed:false` on every host.
    let bogus = wss_rpc(
        &mut ws,
        101,
        "host.findApp",
        json!({ "name": "DefinitelyNotInstalledXyzzy" }),
    )
    .await;
    assert!(
        bogus["installed"].is_boolean(),
        "installed always present: {bogus}"
    );
    assert_eq!(bogus["installed"], false, "bogus app is not installed");

    // An unsafe name never errors — it resolves to installed:false.
    let unsafe_name = wss_rpc(
        &mut ws,
        102,
        "host.findApp",
        json!({ "name": "../../etc/passwd" }),
    )
    .await;
    assert_eq!(
        unsafe_name["installed"], false,
        "unsafe app name ⇒ uninstalled"
    );

    // host.listInstalledEditors ⇒ { editors: [{ id, installed, path?, source?,
    // flatpakId? }] }. Always replies; every entry carries id + installed.
    let editors_result = wss_rpc(&mut ws, 103, "host.listInstalledEditors", json!({})).await;
    let editors = editors_result["editors"].as_array().expect("editors array");
    assert!(!editors.is_empty(), "default catalog is non-empty");
    let ids: std::collections::HashSet<&str> = editors
        .iter()
        .map(|e| e["id"].as_str().expect("id"))
        .collect();
    for expected in ["vscode", "cursor", "zed"] {
        assert!(
            ids.contains(expected),
            "catalog includes {expected}: {editors_result}"
        );
    }
    for entry in editors {
        assert!(
            entry["installed"].is_boolean(),
            "installed boolean: {entry}"
        );
        if entry["installed"] == json!(true) {
            assert!(
                entry["source"].is_string(),
                "installed entries carry a source: {entry}"
            );
        }
    }
}
