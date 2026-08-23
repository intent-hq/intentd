//! WSS end-to-end `specialists:changed` emission: drives a real pinned-TLS
//! WebSocket against a live `intentd serve`, subscribes via `events.subscribe`,
//! mutates a project-tier specialist `<id>.md` file on disk, and asserts the
//! resulting `events.event` notification carries `{ workspaceId }` (PROTOCOL
//! §6.5). The specialists watcher enumerates workspaces at boot, so the
//! workspace is created first and the daemon restarted over the same data dir
//! before subscribing — mirroring the persistence-restart harness.

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
use tokio::io::{AsyncBufReadExt, AsyncWriteExt, BufReader};
use tokio::net::{TcpStream, UnixStream};
use tokio::time::timeout;
use tokio_tungstenite::tungstenite::Message;
use tokio_tungstenite::WebSocketStream;
use uuid::Uuid;

const TOKEN: &str = "abababababababababababababababababababababababababababababababab";

fn scratch_dir(prefix: &str) -> PathBuf {
    let id = Uuid::new_v4().simple().to_string();
    let dir = PathBuf::from("/tmp").join(format!("itd-wss-spec-chg-{prefix}-{}", &id[..8]));
    std::fs::create_dir_all(&dir).expect("mkdir scratch dir");
    dir
}

/// Spawn `intentd serve` with a hermetic HOME so the user-tier specialists
/// directory (`~/.intent/specialists`) never touches the real home.
fn spawn_serve(data_dir: &Path, home_dir: &Path) -> Child {
    let log = std::fs::File::create(data_dir.join("daemon.log")).expect("create daemon log");
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
        .env("HOME", home_dir)
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
            Some(Ok(_)) => {}
            other => panic!("expected text frame, got {other:?}"),
        }
    }
}

/// Wait up to `secs` for the next `events.event` notification whose payload
/// `type` matches one of `types`; ignore other frames. Returns the event
/// object (the `params.event` sub-object).
async fn next_event<S>(ws: &mut WebSocketStream<S>, types: &[&str], secs: u64) -> Value
where
    S: tokio::io::AsyncRead + tokio::io::AsyncWrite + Unpin,
{
    let deadline = tokio::time::Instant::now() + Duration::from_secs(secs);
    loop {
        let remaining = deadline.saturating_duration_since(tokio::time::Instant::now());
        assert!(!remaining.is_zero(), "timed out waiting for {types:?}");
        let next = timeout(remaining, ws.next())
            .await
            .expect("timeout elapsed");
        match next {
            Some(Ok(Message::Text(text))) => {
                let v: Value = match serde_json::from_str(&text) {
                    Ok(x) => x,
                    Err(_) => continue,
                };
                if v["method"] == json!("events.event") {
                    let evt = &v["params"]["event"];
                    let ty = evt["type"].as_str().unwrap_or("");
                    if types.contains(&ty) {
                        return evt.clone();
                    }
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

/// Drain any additional `events.event` frames matching `event_type` in
/// `window_ms`; return the first extra observed, or `None` if the socket
/// stayed quiet.
async fn drain_extra<S>(
    ws: &mut WebSocketStream<S>,
    event_type: &str,
    window_ms: u64,
) -> Option<Value>
where
    S: tokio::io::AsyncRead + tokio::io::AsyncWrite + Unpin,
{
    timeout(Duration::from_millis(window_ms), async {
        loop {
            match ws.next().await {
                Some(Ok(Message::Text(text))) => {
                    let v: Value = match serde_json::from_str(&text) {
                        Ok(x) => x,
                        Err(_) => continue,
                    };
                    if v["method"] == json!("events.event")
                        && v["params"]["event"]["type"] == json!(event_type)
                    {
                        return v;
                    }
                }
                Some(Ok(Message::Ping(p))) => {
                    let _ = ws.send(Message::Pong(p)).await;
                }
                Some(Ok(_)) => {}
                Some(Err(e)) => panic!("subscription socket errored during drain: {e:?}"),
                None => panic!("subscription socket closed during drain"),
            }
        }
    })
    .await
    .ok()
}

/// Boot (or re-boot) a daemon over an existing data dir, returning the child
/// and a pinned-TLS WSS client config for its live port.
async fn boot(data_dir: &Path, home_dir: &Path) -> (Child, u16, Arc<ClientConfig>) {
    let child = spawn_serve(data_dir, home_dir);
    let socket = data_dir.join("intentd.sock");
    assert!(await_uds(&socket).await, "daemon did not start");
    let status = common::await_wss_status(&socket).await;
    let port =
        u16::try_from(status["result"]["port"].as_u64().expect("port")).expect("value fits in u16");
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

fn specialist_md(name: &str, body: &str) -> String {
    format!("---\nname: \"{name}\"\ndescription: \"d\"\n---\n\n{body}")
}

/// End-to-end: a subscribed WSS client sees `specialists:changed` with the
/// minimal `{ workspaceId }` payload when a project-tier specialist `<id>.md`
/// file is created under `<workspace>/.intent/specialists/`. The watcher
/// enumerates workspaces at boot, so the workspace is bootstrapped over UDS on
/// a first daemon and the emission asserted against a restarted one.
#[tokio::test]
async fn specialist_file_change_emits_specialists_changed_over_wss() {
    let data_dir = scratch_dir("data");
    let home_dir = data_dir.join("home");
    std::fs::create_dir_all(&home_dir).expect("mkdir hermetic home");
    // On-disk workspace checkout whose project tier the watcher will cover.
    let checkout = data_dir.join("checkout");
    let specialists_dir = checkout.join(".intent").join("specialists");
    std::fs::create_dir_all(&specialists_dir).expect("mkdir project specialists tier");

    // Boot #1: create the workspace pointing at the on-disk checkout.
    let (child, _port, _cfg) = boot(&data_dir, &home_dir).await;
    let socket = data_dir.join("intentd.sock");
    let create = uds_rpc(
        &socket,
        2,
        "workspace.create",
        json!({
            "title": "Specialists",
            "branch": "main",
            "skipWorktree": true,
            "path": checkout.to_string_lossy(),
        }),
    )
    .await;
    let ws_id = create["result"]["workspace"]["id"]
        .as_str()
        .expect("workspace id")
        .to_string();
    stop(child);

    // Boot #2: the specialists watcher now covers the workspace's project tier.
    let (child, port, cfg) = boot(&data_dir, &home_dir).await;
    let _guard = common::DaemonGuard::new(child, data_dir.clone(), true);

    let mut sub = connect_ws(port, cfg.clone()).await;
    let sub_res = wss_rpc(
        &mut sub,
        1,
        "events.subscribe",
        json!({ "eventTypes": ["specialists:changed"], "workspaceId": ws_id }),
    )
    .await;
    assert!(sub_res["subscriptionId"].is_string(), "sub id: {sub_res}");

    // Let the OS watch establish before mutating (FSEvents/inotify warm-up).
    tokio::time::sleep(Duration::from_millis(500)).await;

    // Mutate: create a project-tier specialist file.
    std::fs::write(
        specialists_dir.join("custom.md"),
        specialist_md("Custom", "project-tier body"),
    )
    .expect("write specialist");

    let evt = next_event(&mut sub, &["specialists:changed"], 20).await;
    assert_eq!(evt["type"], json!("specialists:changed"));
    assert_eq!(evt["workspaceId"], ws_id.as_str());
    assert!(evt["id"].is_string(), "event id: {evt}");
    assert!(evt["timestamp"].is_string(), "timestamp: {evt}");
    // The watcher emits a bare system actor (no id/name; optional fields are
    // omitted from the wire per §9.1).
    assert_eq!(evt["actor"], json!({ "type": "system" }));
    assert_eq!(evt["data"], json!({ "workspaceId": ws_id }));

    // Debounce coalesces the single write to exactly one emission.
    let extra = drain_extra(&mut sub, "specialists:changed", 700).await;
    assert!(
        extra.is_none(),
        "single specialist write must publish exactly one specialists:changed, got extra: {extra:?}"
    );
}
