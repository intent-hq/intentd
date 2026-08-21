//! WSS end-to-end streaming `git.clone` (AUDIT-P2-14): drives the additive
//! `git.clone` method over a real pinned-TLS WebSocket against a live
//! `intentd serve` (WSS listener enabled via config). Asserts the JSON-RPC ack shape from
//! docs/protocol/methods/git.md §5.6 and the streamed `git:clone:progress` / `git:clone:done`
//! bus events (docs/protocol/06-events.md §6.5) — including the failure branch where a bogus URL yields
//! `done { ok: false }`.
//!
//! Uses a tiny local bare repository as the clone source so the test never
//! touches the network. Gated on `git` being on PATH; skips cleanly otherwise.

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

struct Daemon {
    child: Child,
    data_dir: PathBuf,
    scratch: PathBuf,
}

impl Drop for Daemon {
    fn drop(&mut self) {
        let _ = self.child.kill();
        let _ = self.child.wait();
        let _ = std::fs::remove_dir_all(&self.data_dir);
        let _ = std::fs::remove_dir_all(&self.scratch);
    }
}

fn scratch_dir(prefix: &str) -> PathBuf {
    let id = Uuid::new_v4().simple().to_string();
    let dir = PathBuf::from("/tmp").join(format!("itd-wss-clone-{prefix}-{}", &id[..8]));
    std::fs::create_dir_all(&dir).expect("mkdir scratch dir");
    dir
}

fn spawn_serve(data_dir: &Path, listen: &str, env: &[(&str, &str)]) -> Child {
    let log = std::fs::File::create(data_dir.join("daemon.log")).expect("create daemon log");
    let workspaces_dir = data_dir.join("workspaces");
    std::fs::create_dir_all(&workspaces_dir).expect("mkdir hermetic workspaces dir");
    if listen != "uds" {
        common::enable_ws_api(data_dir);
    }
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

/// Read `events.event` frames of the given `types`, returning them as they
/// arrive up to `secs`. Non-matching frames are ignored.
async fn drain_events<S>(ws: &mut WebSocketStream<S>, request_id: &str, secs: u64) -> Vec<Value>
where
    S: tokio::io::AsyncRead + tokio::io::AsyncWrite + Unpin,
{
    let mut out: Vec<Value> = Vec::new();
    let deadline = tokio::time::Instant::now() + Duration::from_secs(secs);
    loop {
        let remaining = deadline.saturating_duration_since(tokio::time::Instant::now());
        if remaining.is_zero() {
            return out;
        }
        let next = match timeout(remaining, ws.next()).await {
            Ok(x) => x,
            Err(_) => return out,
        };
        match next {
            Some(Ok(Message::Text(text))) => {
                let v: Value = match serde_json::from_str(&text) {
                    Ok(x) => x,
                    Err(_) => continue,
                };
                if v["method"] == json!("events.event") {
                    let evt = &v["params"]["event"];
                    let ty = evt["type"].as_str().unwrap_or("");
                    if evt["data"]["requestId"].as_str() == Some(request_id)
                        && (ty == "git:clone:progress" || ty == "git:clone:done")
                    {
                        let done = ty == "git:clone:done";
                        out.push(evt.clone());
                        if done {
                            return out;
                        }
                    }
                }
            }
            Some(Ok(Message::Ping(p))) => {
                let _ = ws.send(Message::Pong(p)).await;
            }
            Some(Ok(_)) => {}
            None => return out,
            Some(Err(_)) => return out,
        }
    }
}

/// Gate: skip if `git` is not on PATH.
fn gate() -> bool {
    match Command::new("git")
        .arg("--version")
        .stdout(Stdio::null())
        .stderr(Stdio::null())
        .status()
    {
        Ok(s) if s.success() => true,
        _ => {
            eprintln!("skipping git.clone WSS e2e: git not on PATH");
            false
        }
    }
}

/// Materialise a tiny bare source repository inside `dir` and return its path.
fn make_source_repo(dir: &Path) -> PathBuf {
    let seed = dir.join("seed");
    let bare = dir.join("src.git");
    std::fs::create_dir_all(&seed).expect("mkdir seed");
    let run = |args: &[&str], cwd: &Path| {
        let status = Command::new("git")
            .args(args)
            .env("GIT_AUTHOR_NAME", "e2e")
            .env("GIT_AUTHOR_EMAIL", "e2e@example.com")
            .env("GIT_COMMITTER_NAME", "e2e")
            .env("GIT_COMMITTER_EMAIL", "e2e@example.com")
            .current_dir(cwd)
            .stdout(Stdio::null())
            .stderr(Stdio::null())
            .status()
            .expect("run git");
        assert!(status.success(), "git {args:?} failed");
    };
    run(&["init", "-q", "-b", "main"], &seed);
    std::fs::write(seed.join("README.md"), "hello\n").unwrap();
    run(&["add", "README.md"], &seed);
    run(&["commit", "-q", "-m", "init"], &seed);
    run(
        &[
            "clone",
            "-q",
            "--bare",
            seed.to_str().unwrap(),
            bare.to_str().unwrap(),
        ],
        dir,
    );
    bare
}

async fn boot() -> (Daemon, u16, Arc<ClientConfig>) {
    let data_dir = scratch_dir("data");
    let scratch = scratch_dir("scratch");
    let env: [(&str, &str); 2] = [("INTENTD_AUTH_TOKEN", TOKEN), ("INTENTD_TCP_PORT", "0")];
    let child = spawn_serve(&data_dir, "both", &env);
    let daemon = Daemon {
        child,
        data_dir: data_dir.clone(),
        scratch,
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
    (daemon, port, client_config(&fingerprint))
}

/// Happy-path clone from a local bare repo: subscribe → clone → assert ≥1
/// `git:clone:progress` frame + one `git:clone:done { ok: true }` frame,
/// correlated by `requestId`, and that the target path was materialised.
#[tokio::test]
async fn git_clone_streams_progress_and_done_over_wss() {
    if !gate() {
        return;
    }
    let (daemon, port, cfg) = boot().await;
    let src = make_source_repo(&daemon.scratch);
    let parent_dir = daemon.scratch.join("out-happy");
    std::fs::create_dir_all(&parent_dir).unwrap();

    // SUBSCRIBER conn — subscribe BEFORE issuing the clone so we miss nothing.
    let mut sub = connect_ws(port, cfg.clone()).await;
    let sub_res = wss_rpc(
        &mut sub,
        1,
        "events.subscribe",
        json!({ "eventTypes": ["git:clone:progress", "git:clone:done"] }),
    )
    .await;
    assert!(sub_res["subscriptionId"].is_string(), "sub id: {sub_res}");

    // RPC conn — issue git.clone and assert the ack shape.
    let mut rpc = connect_ws(port, cfg.clone()).await;
    let request_id = "clone-happy-1";
    let ack = wss_rpc(
        &mut rpc,
        2,
        "git.clone",
        json!({
            "url": src.to_string_lossy(),
            "parentDir": parent_dir.to_string_lossy(),
            "targetName": "repo",
            "requestId": request_id,
        }),
    )
    .await;
    assert_eq!(ack["requestId"], json!(request_id), "ack shape: {ack}");
    assert!(ack["targetPath"].is_string(), "ack has targetPath: {ack}");
    assert!(
        ack["targetPath"]
            .as_str()
            .unwrap()
            .ends_with("out-happy/repo"),
        "targetPath under parent: {ack}",
    );

    // Drain the streamed frames.
    let events = drain_events(&mut sub, request_id, 30).await;
    let progress_count = events
        .iter()
        .filter(|e| e["type"] == json!("git:clone:progress"))
        .count();
    let done = events
        .iter()
        .find(|e| e["type"] == json!("git:clone:done"))
        .cloned()
        .expect("terminal git:clone:done event");
    assert!(
        progress_count >= 1,
        "at least one progress frame: {events:?}",
    );
    assert_eq!(done["data"]["ok"], json!(true), "done ok=true: {done}");
    assert_eq!(
        done["data"]["requestId"],
        json!(request_id),
        "done requestId correlates: {done}"
    );
    // Every streamed frame carries the phase/percent/message shape.
    for e in &events {
        if e["type"] == json!("git:clone:progress") {
            assert!(e["data"]["phase"].is_string(), "phase: {e}");
            assert!(e["data"]["percent"].is_number(), "percent: {e}");
            assert!(e["data"]["message"].is_string(), "message: {e}");
        }
    }
    // The clone materialised on disk.
    assert!(
        parent_dir.join("repo").join(".git").exists()
            || parent_dir.join("repo").join("HEAD").exists(),
        "clone target populated",
    );
}

/// Failure path: an obviously-bad local URL exits non-zero. Assert a terminal
/// `done { ok: false }` and that no credential fragment leaks into `error`.
#[tokio::test]
async fn git_clone_failure_emits_done_ok_false_over_wss() {
    if !gate() {
        return;
    }
    let (daemon, port, cfg) = boot().await;
    let parent_dir = daemon.scratch.join("out-fail");
    std::fs::create_dir_all(&parent_dir).unwrap();

    let mut sub = connect_ws(port, cfg.clone()).await;
    let _ = wss_rpc(
        &mut sub,
        1,
        "events.subscribe",
        json!({ "eventTypes": ["git:clone:progress", "git:clone:done"] }),
    )
    .await;

    let mut rpc = connect_ws(port, cfg.clone()).await;
    let request_id = "clone-fail-1";
    // Non-existent local path — git will error out immediately (no network).
    let bogus = daemon.scratch.join("definitely-not-a-repo.git");
    let ack = wss_rpc(
        &mut rpc,
        2,
        "git.clone",
        json!({
            "url": bogus.to_string_lossy(),
            "parentDir": parent_dir.to_string_lossy(),
            "targetName": "repo-fail",
            "requestId": request_id,
        }),
    )
    .await;
    assert_eq!(ack["requestId"], json!(request_id));

    let events = drain_events(&mut sub, request_id, 30).await;
    let done = events
        .iter()
        .find(|e| e["type"] == json!("git:clone:done"))
        .cloned()
        .expect("terminal git:clone:done event");
    assert_eq!(done["data"]["ok"], json!(false), "done ok=false: {done}");
    // No credential fragment ever crosses the wire.
    let serialized = serde_json::to_string(&events).unwrap();
    assert!(
        !serialized.contains("user:"),
        "no credential fragment on the wire: {serialized}",
    );
}

/// Like [`wss_rpc`] but returns the full response frame without asserting
/// success — for tests that assert the JSON-RPC `error` shape.
async fn wss_rpc_raw<S>(ws: &mut WebSocketStream<S>, id: i64, method: &str, params: Value) -> Value
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

/// The stored GitHub token used by the credential-injection e2es. Never a
/// real credential — asserted absent from argv and every wire frame.
const E2E_TOKEN: &str = "e2e-825-stored-token-value";

/// Materialise a stub `git` in `dir` that records its argv plus the
/// `INTENT_GIT_GITHUB_TOKEN` and `GIT_CONFIG_PARAMETERS` env vars to capture
/// files, then fails with the auth-shaped stderr `GIT_TERMINAL_PROMPT=0`
/// produces for a private HTTPS repo. Returns the PATH value (stub dir first)
/// for the daemon.
fn make_stub_git(dir: &Path, capture: &Path) -> String {
    std::fs::create_dir_all(dir).expect("mkdir stub dir");
    let script = format!(
        "#!/bin/sh\n\
         for a in \"$@\"; do printf '%s\\n' \"$a\"; done > \"{capture}.argv\"\n\
         printf '%s' \"${{INTENT_GIT_GITHUB_TOKEN-}}\" > \"{capture}.token\"\n\
         printf '%s' \"${{GIT_CONFIG_PARAMETERS-}}\" > \"{capture}.params\"\n\
         echo \"fatal: could not read Username for 'https://github.com': terminal prompts disabled\" >&2\n\
         exit 128\n",
        capture = capture.display()
    );
    let stub = dir.join("git");
    std::fs::write(&stub, script).expect("write stub git");
    #[cfg(unix)]
    {
        use std::os::unix::fs::PermissionsExt;
        std::fs::set_permissions(&stub, std::fs::Permissions::from_mode(0o755)).unwrap();
    }
    format!(
        "{}:{}",
        dir.display(),
        std::env::var("PATH").unwrap_or_default()
    )
}

/// Seed a secrets file carrying the stored GitHub token and boot a daemon
/// whose `git` is the capturing stub (monorepo#825 regression harness).
async fn boot_with_stub_git() -> (Daemon, u16, Arc<ClientConfig>, PathBuf) {
    let data_dir = scratch_dir("data");
    let scratch = scratch_dir("scratch");
    let secrets = data_dir.join("secrets.json");
    std::fs::create_dir_all(&data_dir).unwrap();
    std::fs::write(
        &secrets,
        format!("{{\"sourceControl.github.token\":\"{E2E_TOKEN}\"}}"),
    )
    .unwrap();
    let capture = scratch.join("git-capture");
    let path = make_stub_git(&scratch.join("stub-bin"), &capture);
    let secrets_str = secrets.to_string_lossy().to_string();
    let env: [(&str, &str); 4] = [
        ("INTENTD_AUTH_TOKEN", TOKEN),
        ("INTENTD_TCP_PORT", "0"),
        ("INTENTD_SECRETS_FILE", &secrets_str),
        ("PATH", &path),
    ];
    let child = spawn_serve(&data_dir, "both", &env);
    let daemon = Daemon {
        child,
        data_dir: data_dir.clone(),
        scratch,
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
    (daemon, port, client_config(&fingerprint), capture)
}

/// Regression for monorepo#825 (credential injection): a `git.clone` of a
/// private HTTPS github.com repo offers the stored token to the child git via
/// the env-backed credential helper — the helper config travels in
/// `GIT_CONFIG_PARAMETERS`, the token bytes only in `INTENT_GIT_GITHUB_TOKEN`
/// (neither in argv) — and the auth-shaped failure is classified as
/// `errorCode: "auth-required"` on `git:clone:done` with no token leaking
/// into any wire frame.
#[tokio::test]
async fn git_clone_injects_stored_token_and_classifies_auth_failure() {
    let (daemon, port, cfg, capture) = boot_with_stub_git().await;
    let parent_dir = daemon.scratch.join("out-auth");
    std::fs::create_dir_all(&parent_dir).unwrap();

    let mut sub = connect_ws(port, cfg.clone()).await;
    let _ = wss_rpc(
        &mut sub,
        1,
        "events.subscribe",
        json!({ "eventTypes": ["git:clone:progress", "git:clone:done"] }),
    )
    .await;

    let mut rpc = connect_ws(port, cfg.clone()).await;
    let request_id = "clone-auth-1";
    let ack = wss_rpc(
        &mut rpc,
        2,
        "git.clone",
        json!({
            "url": "https://github.com/acme/private-repo.git",
            "parentDir": parent_dir.to_string_lossy(),
            "targetName": "private-repo",
            "requestId": request_id,
        }),
    )
    .await;
    assert_eq!(ack["requestId"], json!(request_id));

    let events = drain_events(&mut sub, request_id, 30).await;
    let done = events
        .iter()
        .find(|e| e["type"] == json!("git:clone:done"))
        .cloned()
        .expect("terminal git:clone:done event");
    assert_eq!(done["data"]["ok"], json!(false), "done ok=false: {done}");
    assert_eq!(
        done["data"]["errorCode"],
        json!("auth-required"),
        "auth-shaped stderr is classified: {done}"
    );
    assert!(
        done["data"]["error"]
            .as_str()
            .unwrap()
            .contains("terminal prompts disabled"),
        "human-readable detail preserved: {done}"
    );
    // The token never crosses the wire in any frame.
    let serialized = serde_json::to_string(&events).unwrap();
    assert!(
        !serialized.contains(E2E_TOKEN),
        "token must not leak onto the wire: {serialized}"
    );

    // The stub captured the spawn: helper config in GIT_CONFIG_PARAMETERS,
    // token in its own env var, neither in argv.
    let argv =
        std::fs::read_to_string(capture.with_extension("argv")).expect("stub git captured argv");
    assert!(
        !argv.contains(E2E_TOKEN),
        "token must never appear in argv: {argv}"
    );
    let params = std::fs::read_to_string(capture.with_extension("params"))
        .expect("stub git captured GIT_CONFIG_PARAMETERS");
    assert!(
        params.contains("credential.https://github.com.helper="),
        "credential helper offered via GIT_CONFIG_PARAMETERS: {params}"
    );
    assert!(
        !params.contains(E2E_TOKEN),
        "token must never appear in GIT_CONFIG_PARAMETERS: {params}"
    );
    let token = std::fs::read_to_string(capture.with_extension("token"))
        .expect("stub git captured token env");
    assert_eq!(
        token, E2E_TOKEN,
        "stored token travels via INTENT_GIT_GITHUB_TOKEN"
    );
}

/// Regression for monorepo#825 (error classification): a `workspace.create`
/// whose `githubUrl` clone fails auth-shaped surfaces a structured JSON-RPC
/// error — `-32603` with `error.data = { code: "auth-required", detail }` —
/// instead of an opaque Internal error, and the token never leaks into the
/// response.
#[tokio::test]
async fn workspace_create_clone_auth_failure_maps_to_structured_error() {
    let (daemon, port, cfg, _capture) = boot_with_stub_git().await;
    let clone_path = daemon.scratch.join("ws-auth").join("repo");

    let mut rpc = connect_ws(port, cfg).await;
    let resp = wss_rpc_raw(
        &mut rpc,
        3,
        "workspace.create",
        json!({
            "title": "Private clone",
            "githubUrl": "https://github.com/acme/private-repo.git",
            "clonePath": clone_path.to_string_lossy(),
        }),
    )
    .await;
    let err = &resp["error"];
    assert_eq!(err["code"], json!(-32603), "clone failure code: {resp}");
    assert_eq!(
        err["data"]["code"],
        json!("auth-required"),
        "machine-readable category: {resp}"
    );
    assert!(
        err["data"]["detail"]
            .as_str()
            .unwrap()
            .contains("terminal prompts disabled"),
        "sanitized detail preserved: {resp}"
    );
    let serialized = resp.to_string();
    assert!(
        !serialized.contains(E2E_TOKEN),
        "token must not leak into the error: {serialized}"
    );
}

/// Missing required params → -32602.
#[tokio::test]
async fn git_clone_missing_params_rejected_over_wss() {
    if !gate() {
        return;
    }
    let (_daemon, port, cfg) = boot().await;
    let mut ws = connect_ws(port, cfg).await;
    let frame = json!({ "jsonrpc": "2.0", "id": 5, "method": "git.clone", "params": {} });
    ws.send(Message::Text(frame.to_string().into()))
        .await
        .unwrap();
    let err = loop {
        let next = timeout(Duration::from_secs(15), ws.next())
            .await
            .expect("timed out")
            .unwrap()
            .unwrap();
        if let Message::Text(t) = next {
            let v: Value = serde_json::from_str(&t).unwrap();
            if v["id"] == json!(5) {
                break v;
            }
        }
    };
    assert_eq!(
        err["error"]["code"], -32602,
        "missing params ⇒ -32602: {err}"
    );
}
