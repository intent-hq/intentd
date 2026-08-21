//! WSS end-to-end gitignore suppression (intent-hq/monorepo#1457): the
//! per-workspace `FileWatcher` drops git-ignored paths before they reach the
//! event bus, so a live `events.subscribe` client never sees `file:*` frames
//! for them. Drives a real `intentd serve` over pinned-TLS WSS: a workspace
//! whose checkout is a git repo with a `.gitignore` gets an ignored write and
//! a control write; only the control surfaces as `events.event`. Mirrors the
//! harness of `e2e_wss_workspace_lifecycle_watchers.rs` (runtime watcher
//! registration on `workspace.create`).

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
    let dir = PathBuf::from("/tmp").join(format!("itd-wss-gitignore-{prefix}-{}", &id[..8]));
    std::fs::create_dir_all(&dir).expect("mkdir scratch dir");
    dir
}

/// Spawn `intentd serve` with a hermetic HOME so host git config (global
/// excludes) never leaks into the watcher under test.
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

/// `git init` the checkout (plus local user config) so the daemon's watcher
/// sees a real repo.
fn git_init(root: &Path) {
    let run = |args: &[&str]| {
        let out = Command::new("git")
            .arg("-C")
            .arg(root)
            .args(args)
            .output()
            .expect("run git");
        assert!(
            out.status.success(),
            "git {args:?} failed: {}",
            String::from_utf8_lossy(&out.stderr)
        );
    };
    run(&["init", "-q"]);
    run(&["config", "user.email", "e2e@example.com"]);
    run(&["config", "user.name", "E2E"]);
}

/// Repeatedly write the ignored paths followed by the control file (fresh
/// content each pass, ~1 s cadence) while draining `events.event` frames,
/// until the control path's `file:*` event arrives (proving the watcher
/// processed a batch), asserting no frame for a suppressed path shows up —
/// then keep draining through an 800 ms quiet window to catch stragglers
/// flushed after the control.
///
/// The bounded write-retry loop makes the test robust under parallel test
/// load (intent-hq/monorepo#1621): a one-shot write after a fixed warm-up
/// sleep is lost forever if the OS watch (FSEvents/inotify) establishes
/// late, whereas re-writing until the control surfaces guarantees a
/// late-established watch still observes a fresh batch. Re-writes stop once
/// the control is seen so the quiet window stays meaningful.
async fn expect_suppressed_over_wss<S>(
    ws: &mut WebSocketStream<S>,
    checkout: &Path,
    suppressed: &[&str],
    control: &str,
) where
    S: tokio::io::AsyncRead + tokio::io::AsyncWrite + Unpin,
{
    let deadline = tokio::time::Instant::now() + common::test_timeout(Duration::from_secs(20));
    let write_cadence = Duration::from_secs(1);
    let mut seen_control = false;
    let mut next_write = tokio::time::Instant::now();
    let mut attempt: u64 = 0;
    loop {
        if !seen_control && tokio::time::Instant::now() >= next_write {
            attempt += 1;
            let body = format!("attempt-{attempt}");
            // Ignored writes first, then the control: the control frame
            // arriving proves the batch was processed while the ignored
            // paths stayed silent.
            std::fs::write(checkout.join("generated/out.js"), &body).expect("write ignored");
            std::fs::write(checkout.join("data.secret"), &body).expect("write ignored glob");
            tokio::time::sleep(Duration::from_millis(100)).await;
            std::fs::write(checkout.join(control), &body).expect("write control");
            next_write = tokio::time::Instant::now() + write_cadence;
        }
        let wait = if seen_control {
            Duration::from_millis(800)
        } else {
            let now = tokio::time::Instant::now();
            let remaining = deadline.saturating_duration_since(now);
            assert!(
                !remaining.is_zero(),
                "control event for {control} never arrived"
            );
            remaining.min(next_write.saturating_duration_since(now))
        };
        match timeout(wait, ws.next()).await {
            Ok(Some(Ok(Message::Text(text)))) => {
                let v: Value = match serde_json::from_str(&text) {
                    Ok(x) => x,
                    Err(_) => continue,
                };
                if v["method"] != json!("events.event") {
                    continue;
                }
                let evt = &v["params"]["event"];
                let ty = evt["type"].as_str().unwrap_or("");
                if !ty.starts_with("file:") {
                    continue;
                }
                let rel = evt["data"]["relativePath"].as_str().unwrap_or_default();
                assert!(
                    !suppressed.contains(&rel),
                    "suppressed path {rel} surfaced over WSS: {evt}"
                );
                if rel == control {
                    seen_control = true;
                }
            }
            Ok(Some(Ok(Message::Ping(p)))) => {
                let _ = ws.send(Message::Pong(p)).await;
            }
            Ok(Some(Ok(_))) => {}
            Ok(other) => panic!("subscription socket ended: {other:?}"),
            Err(_) => {
                if seen_control {
                    return;
                }
                // No control yet: loop back to re-write a fresh batch (or
                // trip the deadline assert once the budget is exhausted).
            }
        }
    }
}

/// End-to-end (intent-hq/monorepo#1457): with a live `events.subscribe` on
/// `file:*`, a write to a `.gitignore`d path inside the workspace checkout
/// never surfaces as an `events.event` frame, while a non-ignored control
/// write in the same batch does — proving suppression holds through the
/// full daemon transport (watcher → bus → WSS fan-out), not just in-crate.
#[tokio::test]
async fn gitignored_write_is_suppressed_over_wss() {
    let data_dir = scratch_dir("data");
    let home_dir = data_dir.join("home");
    std::fs::create_dir_all(&home_dir).expect("mkdir hermetic home");
    // On-disk checkout: a git repo whose .gitignore suppresses generated/.
    let checkout = data_dir.join("checkout");
    std::fs::create_dir_all(&checkout).expect("mkdir checkout");
    git_init(&checkout);
    std::fs::write(checkout.join(".gitignore"), "generated/\n*.secret\n")
        .expect("write .gitignore");
    std::fs::create_dir_all(checkout.join("generated")).expect("mkdir generated");

    let child = spawn_serve(&data_dir, &home_dir);
    let _guard = common::DaemonGuard::new(child, data_dir.clone(), true);
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

    // Create the workspace over the existing checkout: `workspace:created`
    // registers the FileWatcher at runtime (#611), gitignore matcher included.
    let create = uds_rpc(
        &socket,
        2,
        "workspace.create",
        json!({
            "title": "Gitignore suppression",
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

    let mut sub = connect_ws(port, cfg.clone()).await;
    let sub_res = wss_rpc(
        &mut sub,
        1,
        "events.subscribe",
        json!({ "eventTypes": ["file:*"], "workspaceId": ws_id }),
    )
    .await;
    assert!(sub_res["subscriptionId"].is_string(), "sub id: {sub_res}");

    // The write phase lives inside the drain loop: it re-writes the ignored
    // paths and the control on a ~1 s cadence until the control event lands,
    // so no fixed FSEvents/inotify warm-up sleep is needed (#1621).
    expect_suppressed_over_wss(
        &mut sub,
        &checkout,
        &["generated", "generated/out.js", "data.secret"],
        "control.txt",
    )
    .await;
}
