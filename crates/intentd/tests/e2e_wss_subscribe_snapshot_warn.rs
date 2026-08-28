//! WSS end-to-end test for the subscribe fast-path snapshot duration WARN
//! (intent-hq/intent#3707): drives a real pinned-TLS WebSocket against a live
//! `intentd serve` with the threshold lowered to 0 via
//! `INTENTD_SUBSCRIBE_SNAPSHOT_WARN_MS`, sends a real `note.subscribe`, waits
//! for the seq-0 `subscription.push` snapshot, and asserts the daemon logged
//! the `intent_transport::subscribe_profile` WARN carrying the channel and
//! scope — proving `process_frame` carries the timer through the spawned
//! forwarder on the production wire path. A second daemon at the default
//! threshold proves a normal subscribe logs nothing.

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

fn spawn_serve(data_dir: &Path, env: &[(&str, &str)]) -> Child {
    let log = std::fs::File::create(data_dir.join("daemon.log")).expect("create daemon log");
    let workspaces_dir = data_dir.join("workspaces");
    std::fs::create_dir_all(&workspaces_dir).expect("mkdir hermetic workspaces dir");
    common::enable_ws_api(data_dir);
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

/// Boot a real daemon (UDS + WSS listener) with the given extra env, and
/// return the guard, WSS port, pinned client config, UDS socket path, and
/// stderr log path.
async fn boot(prefix: &str, envs: &[(&str, &str)]) -> (Daemon, u16, Arc<ClientConfig>, PathBuf) {
    let id = Uuid::new_v4().simple().to_string();
    let data_dir = PathBuf::from("/tmp").join(format!("{prefix}-{}", &id[..8]));
    std::fs::create_dir_all(&data_dir).expect("mkdir data dir");
    let mut env: Vec<(&str, &str)> = vec![("INTENTD_AUTH_TOKEN", TOKEN), ("INTENTD_TCP_PORT", "0")];
    env.extend_from_slice(envs);
    let child = spawn_serve(&data_dir, &env);
    let daemon = Daemon {
        child,
        data_dir: data_dir.clone(),
    };
    let socket = data_dir.join("intentd.sock");
    assert!(await_uds(&socket).await, "daemon did not start");
    let status = common::await_wss_status(&socket).await;
    let port =
        u16::try_from(status["result"]["port"].as_u64().expect("port")).expect("value fits in u16");
    let fingerprint = status["result"]["fingerprint"]
        .as_str()
        .expect("fingerprint")
        .to_string();
    let cfg = client_config(&fingerprint);
    (daemon, port, cfg, socket)
}

/// Send a `*.subscribe` over WSS and wait for both the response envelope
/// (`subscriptionId`) and the seq-0 `subscription.push` snapshot.
async fn subscribe_and_await_snapshot<S>(ws: &mut WebSocketStream<S>, method: &str, params: Value)
where
    S: tokio::io::AsyncRead + tokio::io::AsyncWrite + Unpin,
{
    let frame = json!({ "jsonrpc": "2.0", "id": 1, "method": method, "params": params });
    ws.send(Message::Text(frame.to_string().into()))
        .await
        .expect("send subscribe frame");
    let mut got_response = false;
    let mut got_snapshot = false;
    let deadline = tokio::time::Instant::now() + common::rpc_read_timeout();
    while !(got_response && got_snapshot) {
        let remaining = deadline.saturating_duration_since(tokio::time::Instant::now());
        assert!(
            !remaining.is_zero(),
            "timed out waiting for {method} response + snapshot"
        );
        let next = timeout(remaining, ws.next())
            .await
            .expect("frame timed out");
        match next {
            Some(Ok(Message::Text(text))) => {
                let v: Value = serde_json::from_str(&text).expect("json frame");
                if v["id"] == json!(1) {
                    assert!(
                        v["result"]["subscriptionId"].is_string(),
                        "{method} response: {v}"
                    );
                    got_response = true;
                } else if v["method"] == json!("subscription.push")
                    && v["params"]["kind"] == json!("snapshot")
                {
                    assert_eq!(v["params"]["seq"], 0, "seq-0 snapshot: {v}");
                    got_snapshot = true;
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

/// Strip ANSI escape sequences (the stderr fmt layer colors its output even
/// when redirected to a file) so field needles match.
fn strip_ansi(log: &str) -> String {
    let mut out = String::with_capacity(log.len());
    let mut chars = log.chars();
    while let Some(c) = chars.next() {
        if c == '\u{1b}' {
            for c in chars.by_ref() {
                if c.is_ascii_alphabetic() {
                    break;
                }
            }
        } else {
            out.push(c);
        }
    }
    out
}

/// Count log lines containing all of `needles` (ANSI codes stripped).
fn count_lines(log: &str, needles: &[&str]) -> usize {
    strip_ansi(log)
        .lines()
        .filter(|line| needles.iter().all(|n| line.contains(n)))
        .count()
}

/// End-to-end: with the threshold lowered to 0, a real `note.subscribe` over
/// WSS emits the seq-0 snapshot AND the slow-snapshot WARN naming the channel
/// and workspace scope — the timer travels `process_frame` → spawned
/// forwarder → `snapshot_emitted()` on the production wire path.
#[tokio::test]
async fn lowered_threshold_fires_snapshot_warn_over_wss() {
    let (daemon, port, cfg, socket) = boot(
        "itd-subwarn",
        &[("INTENTD_SUBSCRIBE_SNAPSHOT_WARN_MS", "0")],
    )
    .await;

    // Bootstrap a workspace off the UDS to keep the WSS stream quiet.
    let create = uds_rpc(
        &socket,
        2,
        "workspace.create",
        json!({ "title": "SubWarn", "branch": "main", "skipWorktree": true }),
    )
    .await;
    let ws_id = create["result"]["workspace"]["id"]
        .as_str()
        .expect("workspace id")
        .to_string();

    let mut ws = connect_ws(port, cfg).await;
    subscribe_and_await_snapshot(&mut ws, "note.subscribe", json!({ "workspaceId": ws_id })).await;

    // The WARN is emitted just after the seq-0 frame is queued; poll briefly
    // to absorb stderr write scheduling.
    let log_path = daemon.data_dir.join("daemon.log");
    let deadline = tokio::time::Instant::now() + common::rpc_read_timeout();
    let needles: [&str; 4] = [
        "subscribe fast-path snapshot exceeded duration budget",
        "channel=\"note\"",
        &format!("scope=\"{ws_id}\""),
        "threshold_ms=0",
    ];
    let log = loop {
        let log = std::fs::read_to_string(&log_path).expect("read daemon log");
        if count_lines(&log, &needles) >= 1 || tokio::time::Instant::now() >= deadline {
            break log;
        }
        tokio::time::sleep(Duration::from_millis(50)).await;
    };
    assert_eq!(
        count_lines(&log, &needles),
        1,
        "expected exactly one slow-snapshot WARN for the note channel, log:\n{log}"
    );
}

/// End-to-end: at the default threshold a normal `note.subscribe` emits its
/// snapshot without any slow-snapshot WARN.
#[tokio::test]
async fn default_threshold_stays_quiet_over_wss() {
    let (daemon, port, cfg, socket) = boot("itd-subquiet", &[]).await;

    let create = uds_rpc(
        &socket,
        2,
        "workspace.create",
        json!({ "title": "SubQuiet", "branch": "main", "skipWorktree": true }),
    )
    .await;
    let ws_id = create["result"]["workspace"]["id"]
        .as_str()
        .expect("workspace id")
        .to_string();

    let mut ws = connect_ws(port, cfg).await;
    subscribe_and_await_snapshot(&mut ws, "note.subscribe", json!({ "workspaceId": ws_id })).await;

    // The WARN (were it wrongly emitted) lands on stderr before the seq-0
    // frame is written, so a single read after the snapshot is sufficient.
    let log_path = daemon.data_dir.join("daemon.log");
    let log = std::fs::read_to_string(&log_path).expect("read daemon log");
    assert_eq!(
        count_lines(&log, &["intent_transport::subscribe_profile"]),
        0,
        "expected no slow-snapshot WARNs at the default threshold, log:\n{log}"
    );
}
