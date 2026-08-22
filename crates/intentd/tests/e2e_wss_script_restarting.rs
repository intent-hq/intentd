//! WSS end-to-end coverage for the `restarting` script status (monorepo#1318),
//! over the real pinned-TLS WebSocket against a live `intentd serve`:
//!
//! - A service script that exits after outliving the too-fast floor commits an
//!   auto-restart; the `script:state` stream observes `restarting` strictly
//!   between the old run's `exited` and the respawn's `running`.
//! - `script.restart` surfaces its stop→start gap the same way: `exited` →
//!   `restarting` (counter reset to 0) → `running`.

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

type TlsWs = WebSocketStream<tokio_rustls::client::TlsStream<TcpStream>>;

fn scratch_dir(prefix: &str) -> PathBuf {
    let id = Uuid::new_v4().simple().to_string();
    let dir = PathBuf::from("/tmp").join(format!("itd-wss-scrst-{prefix}-{}", &id[..8]));
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

async fn connect_ws(port: u16, cfg: Arc<ClientConfig>) -> TlsWs {
    let url = format!("wss://localhost:{port}/ws?token={TOKEN}");
    common::wss_connect_with_retry(port, cfg, &url).await
}

/// One round-trip RPC that must succeed: send, await the response envelope
/// (skipping interleaved `events.event` notifications), assert the envelope
/// (`jsonrpc: "2.0"`, `result` present, no `error`), return `result`.
async fn wss_rpc(ws: &mut TlsWs, id: i64, method: &str, params: Value) -> Value {
    let frame = json!({ "jsonrpc": "2.0", "id": id, "method": method, "params": params });
    ws.send(Message::Text(frame.to_string().into()))
        .await
        .expect("send rpc frame");
    loop {
        let next = timeout(Duration::from_secs(30), ws.next())
            .await
            .expect("wss rpc timed out");
        match next {
            Some(Ok(Message::Text(text))) => {
                let v: Value = serde_json::from_str(&text).expect("json frame");
                if v["id"] == json!(id) {
                    assert_eq!(v["jsonrpc"], "2.0", "response envelope: {v}");
                    assert!(v.get("error").is_none(), "rpc {method} errored: {v}");
                    assert!(
                        v.get("result").is_some(),
                        "rpc {method} missing result: {v}"
                    );
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

/// Wait up to `secs` for the next `script:state` `events.event` notification
/// for `script_id`, returning its `data` payload. Other frames (responses,
/// `script:output`, pings) are skipped — so consecutive calls observe the
/// `script:state` stream in bus order.
async fn next_state(ws: &mut TlsWs, script_id: &str, secs: u64) -> Value {
    let deadline = tokio::time::Instant::now() + Duration::from_secs(secs);
    loop {
        let remaining = deadline.saturating_duration_since(tokio::time::Instant::now());
        assert!(
            !remaining.is_zero(),
            "timed out waiting for script:state of {script_id}"
        );
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
                    if evt["type"] == json!("script:state")
                        && evt["data"]["scriptId"] == json!(script_id)
                    {
                        return evt["data"].clone();
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

/// Boot a daemon over `data_dir`, returning the child and a pinned-TLS WSS
/// client config for its live port.
async fn boot(data_dir: &Path) -> (Child, u16, Arc<ClientConfig>) {
    let child = spawn_serve(data_dir);
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

/// A minimal committed git repo for `workspace.create` (the store row is what
/// `script.*` needs; `skipWorktree` keeps provisioning out of the test).
fn create_test_repo() -> PathBuf {
    let repo_path = std::env::temp_dir().join(format!("scrst-repo-{}", Uuid::new_v4()));
    std::fs::create_dir_all(&repo_path).expect("create temp repo dir");
    let git = |args: &[&str]| {
        let status = Command::new("git")
            .args(args)
            .current_dir(&repo_path)
            .stdout(Stdio::null())
            .status()
            .expect("git spawn");
        assert!(status.success(), "git {args:?} failed");
    };
    git(&["init", "--initial-branch=main"]);
    git(&["config", "user.email", "test@example.com"]);
    git(&["config", "user.name", "Test"]);
    std::fs::write(repo_path.join("README.md"), "# Test\n").expect("write readme");
    git(&["add", "."]);
    git(&["commit", "-m", "initial commit"]);
    repo_path
}

/// The `restarting` status (monorepo#1318) is observable over the wire, in
/// order, for both restart flavors:
///
/// 1. Auto-restart: a service that outlives the too-fast floor (2s) and then
///    exits emits `running` → `exited` → `restarting` (counter bumped to 1)
///    strictly before the respawn's `running`.
/// 2. `script.restart`: the manual stop→start gap emits `exited` →
///    `restarting` (counter reset to 0) → `running`.
#[tokio::test]
async fn restarting_status_is_observable_over_wss() {
    let data_dir = scratch_dir("data");
    let repo_path = create_test_repo();
    let (child, port, cfg) = boot(&data_dir).await;

    // SUBSCRIBER conn — create the workspace and subscribe to script:* BEFORE
    // any script runs so no transition is missed.
    let mut sub = connect_ws(port, cfg.clone()).await;
    let created = wss_rpc(
        &mut sub,
        1,
        "workspace.create",
        json!({
            "title": "script-restarting",
            "repositoryPath": repo_path.to_string_lossy(),
            "skipWorktree": true,
        }),
    )
    .await;
    let ws_id = created["workspace"]["id"]
        .as_str()
        .expect("workspace id")
        .to_string();
    let sub_resp = wss_rpc(
        &mut sub,
        2,
        "events.subscribe",
        json!({ "eventTypes": ["script:*"], "workspaceId": ws_id }),
    )
    .await;
    assert!(
        sub_resp["subscriptionId"].is_string(),
        "subscribed: {sub_resp}"
    );

    // RPC conn — a service that outlives the 2s too-fast floor, then exits,
    // committing an auto-restart on every cycle.
    let mut rpc = connect_ws(port, cfg.clone()).await;
    let script = wss_rpc(
        &mut rpc,
        10,
        "script.create",
        json!({
            "workspaceId": ws_id,
            "name": "cycler",
            "command": "sleep 3",
            "mode": "service",
            "scriptId": "restarting-1",
        }),
    )
    .await;
    assert_eq!(script["id"], json!("restarting-1"));
    let started = wss_rpc(
        &mut rpc,
        11,
        "script.start",
        json!({ "workspaceId": ws_id, "scriptId": "restarting-1" }),
    )
    .await;
    assert_eq!(started["ok"], json!(true));

    // Auto-restart cycle: running → exited → restarting → running, in strict
    // stream order (nothing else can interleave on the state stream).
    let st = next_state(&mut sub, "restarting-1", 120).await;
    assert_eq!(st["status"], "running", "first run: {st}");
    let st = next_state(&mut sub, "restarting-1", 120).await;
    assert_eq!(st["status"], "exited", "first exit: {st}");
    let st = next_state(&mut sub, "restarting-1", 120).await;
    assert_eq!(st["status"], "restarting", "backoff window: {st}");
    assert_eq!(st["restartCount"], 1, "attempt counter bumped: {st}");
    let st = next_state(&mut sub, "restarting-1", 120).await;
    assert_eq!(st["status"], "running", "respawned: {st}");
    assert_eq!(st["restartCount"], 1, "counter carried: {st}");

    // Manual restart while the respawn is live: the stop→start gap reports
    // restarting with the counter reset.
    let restarted = wss_rpc(
        &mut rpc,
        12,
        "script.restart",
        json!({ "workspaceId": ws_id, "scriptId": "restarting-1" }),
    )
    .await;
    assert_eq!(restarted["ok"], json!(true));
    let st = next_state(&mut sub, "restarting-1", 120).await;
    assert_eq!(st["status"], "exited", "manual stop: {st}");
    let st = next_state(&mut sub, "restarting-1", 120).await;
    assert_eq!(st["status"], "restarting", "stop\u{2192}start gap: {st}");
    assert_eq!(st["restartCount"], 0, "restart() resets the counter: {st}");
    let st = next_state(&mut sub, "restarting-1", 120).await;
    assert_eq!(st["status"], "running", "manual respawn: {st}");

    // Teardown: stop the service for a clean daemon shutdown.
    let stopped = wss_rpc(
        &mut rpc,
        13,
        "script.stop",
        json!({ "workspaceId": ws_id, "scriptId": "restarting-1" }),
    )
    .await;
    assert_eq!(stopped["ok"], json!(true));

    drop(rpc);
    drop(sub);
    stop(child);
    let _ = std::fs::remove_dir_all(&repo_path);
    let _ = std::fs::remove_dir_all(&data_dir);
}
