//! WSS end-to-end setup lifecycle events (§6.5): a live `events.subscribe`
//! client sees `workspace:setup:started` then exactly one
//! `workspace:setup:completed` for a `workspace.create` with a setup script,
//! and NO `file:*` frame for the workspace before the completion — watcher
//! registration is deferred until the setup stage finishes, so setup-script
//! churn is dropped (never published, never buffered). After completion the
//! watcher is live: a control write surfaces as `file:*` normally while the
//! setup-window artifact stays silent forever. Drives a real `intentd serve`
//! over pinned-TLS WSS; mirrors the harness of
//! `e2e_wss_gitignore_suppression.rs`.

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

const TOKEN: &str = "cdcdcdcdcdcdcdcdcdcdcdcdcdcdcdcdcdcdcdcdcdcdcdcdcdcdcdcdcdcdcdcd";

fn scratch_dir(prefix: &str) -> PathBuf {
    let id = Uuid::new_v4().simple().to_string();
    let dir = PathBuf::from("/tmp").join(format!("itd-wss-setuplc-{prefix}-{}", &id[..8]));
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

/// Create a git repo with one commit so `workspace.create` can provision a
/// worktree from it.
fn create_test_repo() -> PathBuf {
    let repo_path = scratch_dir("repo");
    let run = |args: &[&str]| {
        let out = Command::new("git")
            .arg("-C")
            .arg(&repo_path)
            .args(args)
            .output()
            .expect("run git");
        assert!(
            out.status.success(),
            "git {args:?} failed: {}",
            String::from_utf8_lossy(&out.stderr)
        );
    };
    run(&["init", "-q", "--initial-branch=main"]);
    run(&["config", "user.email", "e2e@example.com"]);
    run(&["config", "user.name", "E2E"]);
    std::fs::write(repo_path.join("README.md"), "# Test\n").expect("write readme");
    run(&["add", "."]);
    run(&["commit", "-q", "-m", "initial commit"]);
    repo_path
}

/// The next `events.event` frame's event object (answers pings, skips
/// non-event frames).
async fn next_event<S>(ws: &mut WebSocketStream<S>, wait: Duration) -> Option<Value>
where
    S: tokio::io::AsyncRead + tokio::io::AsyncWrite + Unpin,
{
    let deadline = tokio::time::Instant::now() + wait;
    loop {
        let remaining = deadline.saturating_duration_since(tokio::time::Instant::now());
        if remaining.is_zero() {
            return None;
        }
        match timeout(remaining, ws.next()).await {
            Ok(Some(Ok(Message::Text(text)))) => {
                let v: Value = match serde_json::from_str(&text) {
                    Ok(x) => x,
                    Err(_) => continue,
                };
                if v["method"] == json!("events.event") {
                    return Some(v["params"]["event"].clone());
                }
            }
            Ok(Some(Ok(Message::Ping(p)))) => {
                let _ = ws.send(Message::Pong(p)).await;
            }
            Ok(Some(Ok(_))) => {}
            Ok(other) => panic!("subscription socket ended: {other:?}"),
            Err(_) => return None,
        }
    }
}

/// End-to-end: the setup lifecycle events surface over WSS in order and the
/// deferred watcher keeps the setup window silent — no `file:*` frame for the
/// workspace arrives before `workspace:setup:completed`, the setup-written
/// artifact never surfaces at all (dropped, not buffered), and a post-setup
/// control write emits `file:*` normally.
#[tokio::test]
async fn setup_lifecycle_events_and_file_suppression_over_wss() {
    let data_dir = scratch_dir("data");
    let home_dir = data_dir.join("home");
    std::fs::create_dir_all(&home_dir).expect("mkdir hermetic home");
    let repo_path = create_test_repo();

    let child = spawn_serve(&data_dir, &home_dir);
    let _guard = common::DaemonGuard::new(child, data_dir.clone(), true);
    let socket = data_dir.join("intentd.sock");
    assert!(await_uds(&socket).await, "daemon did not start");
    let status = common::await_wss_status(&socket).await;
    let port = status["result"]["port"].as_u64().expect("port") as u16;
    let fingerprint = status["result"]["fingerprint"]
        .as_str()
        .expect("fingerprint")
        .to_string();
    let cfg = client_config(&fingerprint);

    // Subscribe BEFORE the create (globally: the workspace id does not exist
    // yet) so no lifecycle or file event can be missed.
    let mut sub = connect_ws(port, cfg.clone()).await;
    let sub_res = wss_rpc(
        &mut sub,
        1,
        "events.subscribe",
        json!({ "eventTypes": ["workspace:setup:*", "file:*"] }),
    )
    .await;
    assert!(sub_res["subscriptionId"].is_string(), "sub id: {sub_res}");

    // The setup script writes a non-gitignored artifact into the worktree and
    // lingers, widening the setup window: were the watcher live during setup
    // (regression), inotify + the watcher debounce would emit the artifact's
    // `file:*` before `workspace:setup:completed`.
    let setup_script = r#"#!/bin/sh
echo artifact > "${WORKTREE_PATH}/setup-artifact.txt"
sleep 2
exit 0
"#;
    let create = uds_rpc(
        &socket,
        2,
        "workspace.create",
        json!({
            "title": "Setup lifecycle",
            "repositoryPath": repo_path.to_string_lossy(),
            "setupScript": setup_script,
        }),
    )
    .await;
    let ws_id = create["result"]["workspace"]["id"]
        .as_str()
        .expect("workspace id")
        .to_string();
    let worktree = PathBuf::from(
        create["result"]["workspace"]["worktreePath"]
            .as_str()
            .expect("worktreePath"),
    );

    // Phase 1: drain frames until `workspace:setup:completed`. The setup
    // window must be silent on `file:*` for this workspace, and the lifecycle
    // events arrive in order with the §6.5 payload shapes.
    let mut seen_started = false;
    let deadline = tokio::time::Instant::now() + common::test_timeout(Duration::from_secs(30));
    loop {
        let remaining = deadline.saturating_duration_since(tokio::time::Instant::now());
        assert!(
            !remaining.is_zero(),
            "workspace:setup:completed never arrived"
        );
        let evt = next_event(&mut sub, remaining)
            .await
            .expect("subscription frame before completion");
        let ty = evt["type"].as_str().unwrap_or("");
        if ty.starts_with("file:") {
            assert_ne!(
                evt["workspaceId"],
                json!(ws_id),
                "file event for the workspace leaked during the setup window: {evt}"
            );
            continue;
        }
        match ty {
            "workspace:setup:started" => {
                assert_eq!(evt["workspaceId"], json!(ws_id));
                assert_eq!(evt["data"], json!({ "workspaceId": ws_id }));
                assert!(!seen_started, "started must fire exactly once");
                seen_started = true;
            }
            "workspace:setup:completed" => {
                assert!(seen_started, "completed must follow started");
                assert_eq!(evt["workspaceId"], json!(ws_id));
                assert_eq!(
                    evt["data"],
                    json!({ "workspaceId": ws_id, "ranScript": true, "exitCode": 0 })
                );
                break;
            }
            other => panic!("unexpected event type {other}: {evt}"),
        }
    }

    // Phase 2: the watcher is registered on completion. Re-write the control
    // until its `file:*` frame arrives (watch establishment can lag, #1621);
    // the setup artifact must never surface — dropped, not buffered.
    let control = "post-setup.txt";
    assert!(
        worktree.join("setup-artifact.txt").exists(),
        "setup script should have written the artifact"
    );
    let deadline = tokio::time::Instant::now() + common::test_timeout(Duration::from_secs(30));
    let mut attempt: u64 = 0;
    let mut next_write = tokio::time::Instant::now();
    loop {
        if tokio::time::Instant::now() >= next_write {
            attempt += 1;
            std::fs::write(worktree.join(control), format!("attempt-{attempt}"))
                .expect("write control");
            next_write = tokio::time::Instant::now() + Duration::from_secs(1);
        }
        let remaining = deadline.saturating_duration_since(tokio::time::Instant::now());
        assert!(!remaining.is_zero(), "control file event never arrived");
        let wait = remaining.min(next_write.saturating_duration_since(tokio::time::Instant::now()));
        let Some(evt) = next_event(&mut sub, wait.max(Duration::from_millis(10))).await else {
            continue;
        };
        let ty = evt["type"].as_str().unwrap_or("");
        if !ty.starts_with("file:") || evt["workspaceId"] != json!(ws_id) {
            continue;
        }
        let rel = evt["data"]["relativePath"].as_str().unwrap_or_default();
        assert_ne!(
            rel, "setup-artifact.txt",
            "setup-window artifact surfaced after completion (buffered, not dropped): {evt}"
        );
        if rel == control {
            break;
        }
    }
}
