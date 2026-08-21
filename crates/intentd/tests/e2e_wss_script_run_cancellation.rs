//! WSS end-to-end coverage for `script.run` cancellation safety and the
//! concurrent-run guard (monorepo#1155), over the real pinned-TLS WebSocket
//! against a live `intentd serve`:
//!
//! - A client that disconnects mid-`script.run` must not orphan the child:
//!   the detached completion task still enforces the script-level timeout,
//!   kills the PTY, and flips `script.status` to `exited` (a reconnecting
//!   client observes the teardown; `kill -0` on the recorded pid fails).
//! - `script.run` while the script is already running warn-and-returns with
//!   the docs/protocol/methods/scripts.md §5.8 envelope `{ exitCode?, output, timedOut?, warning? }`
//!   (only `output` + `warning`, no second PTY).

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

/// Pure-liveness deadline for polls that return as soon as the awaited
/// condition holds; only has to outlast a worst-case CI machine stall.
const LIVENESS: Duration = Duration::from_secs(120);

type TlsWs = WebSocketStream<tokio_rustls::client::TlsStream<TcpStream>>;

fn scratch_dir(prefix: &str) -> PathBuf {
    let id = Uuid::new_v4().simple().to_string();
    let dir = PathBuf::from("/tmp").join(format!("itd-wss-scrun-{prefix}-{}", &id[..8]));
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

/// Send a JSON-RPC request frame without waiting for its response (the
/// in-flight `script.run` whose client is about to vanish).
async fn wss_send(ws: &mut TlsWs, id: i64, method: &str, params: Value) {
    let frame = json!({ "jsonrpc": "2.0", "id": id, "method": method, "params": params });
    ws.send(Message::Text(frame.to_string().into()))
        .await
        .expect("send rpc frame");
}

/// Read frames until the response envelope with `id` arrives, returning the
/// whole envelope (callers assert `result` / `error` shape themselves).
async fn wss_read_response(ws: &mut TlsWs, id: i64, deadline: Duration) -> Value {
    let deadline = tokio::time::Instant::now() + deadline;
    loop {
        let remaining = deadline.saturating_duration_since(tokio::time::Instant::now());
        let next = timeout(remaining, ws.next())
            .await
            .expect("wss response timed out");
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
            Some(Ok(_)) => {}
            other => panic!("expected text frame, got {other:?}"),
        }
    }
}

/// One round-trip RPC that must succeed: send, await the envelope, assert no
/// error, return `result`.
async fn wss_rpc(ws: &mut TlsWs, id: i64, method: &str, params: Value) -> Value {
    wss_send(ws, id, method, params).await;
    let v = wss_read_response(ws, id, Duration::from_secs(30)).await;
    assert!(v.get("error").is_none(), "rpc {method} errored: {v}");
    v["result"].clone()
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
/// `script.run` needs; `skipWorktree` keeps provisioning out of the test).
fn create_test_repo() -> PathBuf {
    let repo_path = std::env::temp_dir().join(format!("scrun-repo-{}", Uuid::new_v4()));
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

/// Create a workspace (skipWorktree) and a command-mode script, returning the
/// workspace id.
async fn create_workspace_and_script(
    ws: &mut TlsWs,
    repo_path: &Path,
    script_id: &str,
    command: &str,
) -> String {
    let created = wss_rpc(
        ws,
        1,
        "workspace.create",
        json!({
            "title": "script-run-cancel",
            "repositoryPath": repo_path.to_string_lossy(),
            "skipWorktree": true,
        }),
    )
    .await;
    let workspace_id = created["workspace"]["id"]
        .as_str()
        .expect("workspace id")
        .to_string();
    let script = wss_rpc(
        ws,
        2,
        "script.create",
        json!({
            "workspaceId": workspace_id,
            "name": "long",
            "command": command,
            "mode": "command",
            "scriptId": script_id,
        }),
    )
    .await;
    assert_eq!(script["id"], json!(script_id));
    workspace_id
}

/// Poll `script.status` until `pred` holds on the result, returning it.
async fn await_script_status<F>(
    ws: &mut TlsWs,
    base_id: i64,
    workspace_id: &str,
    script_id: &str,
    what: &str,
    mut pred: F,
) -> Value
where
    F: FnMut(&Value) -> bool,
{
    let deadline = tokio::time::Instant::now() + LIVENESS;
    let mut attempt = 0;
    loop {
        let st = wss_rpc(
            ws,
            base_id + attempt,
            "script.status",
            json!({ "workspaceId": workspace_id, "scriptId": script_id }),
        )
        .await;
        attempt += 1;
        if pred(&st) {
            return st;
        }
        assert!(
            tokio::time::Instant::now() < deadline,
            "script.status never reached {what}: {st}"
        );
        tokio::time::sleep(Duration::from_millis(50)).await;
    }
}

/// `kill -0` liveness probe for the recorded script pid.
fn pid_alive(pid: i64) -> bool {
    Command::new("kill")
        .args(["-0", &pid.to_string()])
        .stderr(Stdio::null())
        .status()
        .expect("run kill -0")
        .success()
}

/// Regression (monorepo#1155): a WSS client that disconnects mid-`script.run`
/// must not orphan the run. The transport spawns the handler detached, so the
/// detached completion task still enforces the script-level timeout, kills
/// the PTY, and emits the `exited` transition — a reconnecting client
/// observes `script.status = exited` and the recorded pid is gone.
#[tokio::test]
async fn script_run_client_disconnect_reaps_and_marks_exited() {
    let data_dir = scratch_dir("disc");
    let repo_path = create_test_repo();
    let (child, port, cfg) = boot(&data_dir).await;

    // Conn A: create the workspace + script, then fire script.run (10s
    // script-level timeout — the detached task's kill trigger) without
    // reading the response.
    let mut conn_a = connect_ws(port, cfg.clone()).await;
    let workspace_id =
        create_workspace_and_script(&mut conn_a, &repo_path, "cancel-1", "sleep 600").await;
    wss_send(
        &mut conn_a,
        10,
        "script.run",
        json!({ "workspaceId": workspace_id, "scriptId": "cancel-1", "timeoutSeconds": 10 }),
    )
    .await;

    // Conn B: wait until the run is live and record its pid (status flips to
    // `running` when the run is reserved; the pid lands with `mark_running`
    // once the PTY exists).
    let mut conn_b = connect_ws(port, cfg.clone()).await;
    let running = await_script_status(
        &mut conn_b,
        100,
        &workspace_id,
        "cancel-1",
        "running with a pid",
        |st| st["status"] == "running" && st["pid"].is_i64(),
    )
    .await;
    let pid = running["pid"].as_i64().expect("pid");

    // Disconnect the issuing client mid-run (dead client, no close frame
    // handshake needed) — the daemon-side completion path must not care.
    drop(conn_a);
    drop(conn_b);

    // Reconnect and poll: the detached completion task times the run out,
    // kills the PTY, and flips the status to exited.
    let mut conn_c = connect_ws(port, cfg).await;
    await_script_status(
        &mut conn_c,
        200,
        &workspace_id,
        "cancel-1",
        "exited",
        |st| st["status"] == "exited",
    )
    .await;

    // No orphaned process: kill -0 on the recorded pid fails.
    let deadline = tokio::time::Instant::now() + LIVENESS;
    while pid_alive(pid) {
        assert!(
            tokio::time::Instant::now() < deadline,
            "script process {pid} still alive after client disconnect"
        );
        tokio::time::sleep(Duration::from_millis(50)).await;
    }

    drop(conn_c);
    stop(child);
    let _ = std::fs::remove_dir_all(&repo_path);
    let _ = std::fs::remove_dir_all(&data_dir);
}

/// Regression (monorepo#1155): `script.run` while the script is already
/// running warn-and-returns instead of spawning a second PTY, and the
/// envelope matches docs/protocol/methods/scripts.md §5.8 `{ exitCode?, output, timedOut?,
/// warning? }` — only `output` (empty) + `warning`, no `exitCode`/`timedOut`.
#[tokio::test]
async fn script_run_while_running_returns_warning_envelope() {
    let data_dir = scratch_dir("warn");
    let repo_path = create_test_repo();
    let (child, port, cfg) = boot(&data_dir).await;

    // Conn A: fire a long script.run (backstop script-level timeout far
    // beyond the assertion window, so `timedOut` stays deterministic).
    let mut conn_a = connect_ws(port, cfg.clone()).await;
    let workspace_id =
        create_workspace_and_script(&mut conn_a, &repo_path, "busy-1", "sleep 600").await;
    wss_send(
        &mut conn_a,
        10,
        "script.run",
        json!({ "workspaceId": workspace_id, "scriptId": "busy-1", "timeoutSeconds": 300 }),
    )
    .await;

    // Conn B: wait until it is running (with the pid recorded, i.e. past
    // `mark_running`), then run again.
    let mut conn_b = connect_ws(port, cfg).await;
    let running = await_script_status(
        &mut conn_b,
        100,
        &workspace_id,
        "busy-1",
        "running with a pid",
        |st| st["status"] == "running" && st["pid"].is_i64(),
    )
    .await;
    let pid = running["pid"].as_i64().expect("pid");

    wss_send(
        &mut conn_b,
        11,
        "script.run",
        json!({ "workspaceId": workspace_id, "scriptId": "busy-1", "timeoutSeconds": 300 }),
    )
    .await;
    let envelope = wss_read_response(&mut conn_b, 11, Duration::from_secs(30)).await;
    assert_eq!(envelope["jsonrpc"], json!("2.0"));
    assert_eq!(envelope["id"], json!(11));
    assert!(
        envelope.get("error").is_none(),
        "warn-and-return is a success envelope: {envelope}"
    );
    let result = &envelope["result"];
    assert_eq!(result["output"], json!(""), "empty output: {result}");
    assert!(
        result["warning"]
            .as_str()
            .unwrap_or("")
            .contains("already running"),
        "warning says already running: {result}"
    );
    assert!(
        result.get("exitCode").is_none(),
        "no exitCode on the warning envelope: {result}"
    );
    assert!(
        result.get("timedOut").is_none(),
        "no timedOut on the warning envelope: {result}"
    );

    // No second PTY was spawned: status still reports the first run's pid.
    let st = wss_rpc(
        &mut conn_b,
        12,
        "script.status",
        json!({ "workspaceId": workspace_id, "scriptId": "busy-1" }),
    )
    .await;
    assert_eq!(st["status"], "running");
    assert_eq!(st["pid"], json!(pid), "pid unchanged: {st}");

    // Stop the run; the first client's pending response resolves with the
    // full §5.8 success shape (output + timedOut, not the warning).
    let stopped = wss_rpc(
        &mut conn_b,
        13,
        "script.stop",
        json!({ "workspaceId": workspace_id, "scriptId": "busy-1" }),
    )
    .await;
    assert_eq!(stopped["ok"], json!(true));
    let first = wss_read_response(&mut conn_a, 10, LIVENESS).await;
    assert!(
        first.get("error").is_none(),
        "first run resolves cleanly: {first}"
    );
    assert!(first["result"]["output"].is_string(), "output: {first}");
    assert_eq!(first["result"]["timedOut"], json!(false));
    assert!(
        first["result"].get("warning").is_none(),
        "first run is not the warning envelope: {first}"
    );

    drop(conn_a);
    drop(conn_b);
    stop(child);
    let _ = std::fs::remove_dir_all(&repo_path);
    let _ = std::fs::remove_dir_all(&data_dir);
}
