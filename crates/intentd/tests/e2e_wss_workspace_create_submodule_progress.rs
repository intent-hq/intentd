//! WSS end-to-end tests for `workspace.create` provisioning progress over the
//! real pinned-TLS transport (PROTOCOL §5.1 / §6.5): drives a live
//! `intentd serve` (TLS, bearer auth, fingerprint pinning) and asserts the
//! create-scoped `git:clone:progress` / `git:clone:done` frames — every frame
//! echoes the client-supplied `progressId`, percents form one non-decreasing
//! normalized 0–100 series, the clone-from-URL create against a local
//! `file://` fixture repo carrying a submodule surfaces the `submodules`
//! phase inside its 70–85 segment, and exactly one terminal done closes each
//! stream (success and failure alike).
//!
//! Complements `e2e_wss_workspace_create_progress.rs` (same contract over
//! in-process plain `ws://`): this suite adds the real daemon + TLS path and
//! the recursive-submodule clone shape. Harness modeled on
//! `e2e_wss_git_clone.rs`. Gated on `git` being on PATH; skips cleanly
//! otherwise. The daemon is spawned with
//! `GIT_CONFIG_PARAMETERS='protocol.file.allow=always'` so its child
//! `git clone --recurse-submodules` may fetch `file://` submodules (modern
//! git blocks the file transport for submodules by default).

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
use tokio::net::UnixStream;
use tokio::time::timeout;
use tokio_tungstenite::tungstenite::Message;
use tokio_tungstenite::WebSocketStream;
use uuid::Uuid;

const TOKEN: &str = "cdcdcdcdcdcdcdcdcdcdcdcdcdcdcdcdcdcdcdcdcdcdcdcdcdcdcdcdcdcdcdcd";

/// Boundary constants of the unified create-progress scale (PROTOCOL §6.5,
/// mirrored from `intent-services::create_progress`): the clone/cache segment
/// tops out at 85, with submodule work filling 70–85 inside it.
const SUBMODULE_SEGMENT_START: i64 = 70;
const CLONE_SEGMENT_END: i64 = 85;

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
    let dir = PathBuf::from("/tmp").join(format!("itd-wss-createsub-{prefix}-{}", &id[..8]));
    std::fs::create_dir_all(&dir).expect("mkdir scratch dir");
    dir
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

async fn connect_ws(port: u16, cfg: Arc<ClientConfig>) -> common::TlsWs {
    let url = format!("wss://localhost:{port}/ws?token={TOKEN}");
    common::wss_connect_with_retry(port, cfg, &url).await
}

/// Send one JSON-RPC request and return the full response frame (success or
/// error) — callers assert the envelope shape themselves.
async fn wss_rpc_raw<S>(ws: &mut WebSocketStream<S>, id: i64, method: &str, params: Value) -> Value
where
    S: tokio::io::AsyncRead + tokio::io::AsyncWrite + Unpin,
{
    let frame = json!({ "jsonrpc": "2.0", "id": id, "method": method, "params": params });
    ws.send(Message::Text(frame.to_string().into()))
        .await
        .expect("send rpc frame");
    loop {
        let next = timeout(common::rpc_read_timeout(), ws.next())
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

/// Drain `events.event` frames whose `data.progressId` matches, in arrival
/// order. After the first `git:clone:done`, keeps listening for a short quiet
/// grace window so a duplicate done (or any stray frame after the terminal
/// one) is captured and fails the "exactly one done / done is terminal"
/// assertions instead of going unread. Gives up after `secs` of quiet.
async fn drain_progress_events<S>(
    ws: &mut WebSocketStream<S>,
    progress_id: &str,
    secs: u64,
) -> Vec<Value>
where
    S: tokio::io::AsyncRead + tokio::io::AsyncWrite + Unpin,
{
    let mut out: Vec<Value> = Vec::new();
    let mut deadline =
        tokio::time::Instant::now() + common::test_timeout(Duration::from_secs(secs));
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
                    if evt["data"]["progressId"].as_str() == Some(progress_id) {
                        let done = evt["type"] == json!("git:clone:done");
                        out.push(evt.clone());
                        if done {
                            // Terminal frame observed: shrink the deadline to
                            // a short grace window rather than returning, so
                            // any post-done frame still lands in `out`.
                            deadline = deadline.min(
                                tokio::time::Instant::now()
                                    + common::test_timeout(Duration::from_secs(2)),
                            );
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

/// Subscribe `ws` to the `git:clone:*` event stream.
async fn subscribe_clone_events<S>(ws: &mut WebSocketStream<S>, id: i64)
where
    S: tokio::io::AsyncRead + tokio::io::AsyncWrite + Unpin,
{
    let resp = wss_rpc_raw(
        ws,
        id,
        "events.subscribe",
        json!({ "eventTypes": ["git:clone:progress", "git:clone:done"] }),
    )
    .await;
    assert!(
        resp["result"]["subscriptionId"].is_string(),
        "subscription ack: {resp}"
    );
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
            eprintln!("skipping workspace.create submodule-progress WSS e2e: git not on PATH");
            false
        }
    }
}

fn run_git(args: &[&str], cwd: &Path) {
    let out = Command::new("git")
        .args(args)
        .env("GIT_AUTHOR_NAME", "e2e")
        .env("GIT_AUTHOR_EMAIL", "e2e@example.com")
        .env("GIT_COMMITTER_NAME", "e2e")
        .env("GIT_COMMITTER_EMAIL", "e2e@example.com")
        .current_dir(cwd)
        .output()
        .expect("run git");
    assert!(
        out.status.success(),
        "git {args:?} failed: {}",
        String::from_utf8_lossy(&out.stderr)
    );
}

/// Materialise a source repository inside `dir` that carries a real submodule
/// gitlink at `sub` (cloned from a second tiny repo in `dir`), returning its
/// path. The `protocol.file.allow` override is required for `file://`-style
/// submodule clones on modern git.
fn make_source_repo_with_submodule(dir: &Path) -> PathBuf {
    let child = dir.join("child-repo");
    std::fs::create_dir_all(&child).expect("mkdir child repo");
    run_git(&["init", "-q", "-b", "main"], &child);
    run_git(&["config", "user.name", "e2e"], &child);
    run_git(&["config", "user.email", "e2e@example.com"], &child);
    std::fs::write(child.join("inner.txt"), "inner\n").unwrap();
    run_git(&["add", "inner.txt"], &child);
    run_git(&["commit", "-q", "-m", "inner seed"], &child);

    let repo = dir.join("source-repo");
    std::fs::create_dir_all(&repo).expect("mkdir source repo");
    run_git(&["init", "-q", "-b", "main"], &repo);
    run_git(&["config", "user.name", "e2e"], &repo);
    run_git(&["config", "user.email", "e2e@example.com"], &repo);
    std::fs::write(repo.join("tracked.txt"), "seed\n").unwrap();
    run_git(&["add", "tracked.txt"], &repo);
    run_git(&["commit", "-q", "-m", "seed"], &repo);
    run_git(
        &[
            "-c",
            "protocol.file.allow=always",
            "submodule",
            "add",
            "-q",
            &format!("file://{}", child.display()),
            "sub",
        ],
        &repo,
    );
    run_git(&["commit", "-q", "-m", "add submodule"], &repo);
    repo
}

/// Boot a daemon whose child git processes may fetch `file://` submodules:
/// `GIT_CONFIG_PARAMETERS` ranks as command-line-scoped git config and is
/// inherited by every git the daemon spawns (the clone pipeline appends its
/// credential-helper entries after any inherited value, so this coexists
/// with production behavior).
async fn boot() -> (Daemon, u16, Arc<ClientConfig>) {
    let data_dir = scratch_dir("data");
    let scratch = scratch_dir("scratch");
    let env: [(&str, &str); 3] = [
        ("INTENTD_AUTH_TOKEN", TOKEN),
        ("INTENTD_TCP_PORT", "0"),
        ("GIT_CONFIG_PARAMETERS", "'protocol.file.allow=always'"),
    ];
    let child = spawn_serve(&data_dir, &env);
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

/// Shared assertions on a successful create's frame stream: every frame
/// echoes `progressId`, percent is a normalized non-decreasing 0–100 series
/// ending in `complete 100`, and exactly one `git:clone:done { ok:true }`
/// terminates the stream.
fn assert_progress_stream(events: &[Value], progress_id: &str) {
    assert!(!events.is_empty(), "create emitted progress frames");
    for e in events {
        assert_eq!(
            e["data"]["progressId"].as_str(),
            Some(progress_id),
            "every frame echoes progressId: {e}"
        );
    }
    let progress: Vec<&Value> = events
        .iter()
        .filter(|e| e["type"] == json!("git:clone:progress"))
        .collect();
    assert!(
        progress.len() >= 2,
        "at least two progress frames: {events:?}"
    );
    let mut last = 0i64;
    for e in &progress {
        assert!(e["data"]["phase"].is_string(), "phase: {e}");
        assert!(e["data"]["message"].is_string(), "message: {e}");
        let pct = e["data"]["percent"].as_i64().expect("percent");
        assert!(
            (0..=100).contains(&pct) && pct >= last,
            "percent in range and non-decreasing: {pct} after {last} ({e})"
        );
        last = pct;
    }
    let final_progress = progress.last().unwrap();
    assert_eq!(final_progress["data"]["phase"], json!("complete"));
    assert_eq!(final_progress["data"]["percent"], json!(100));
    let dones: Vec<&Value> = events
        .iter()
        .filter(|e| e["type"] == json!("git:clone:done"))
        .collect();
    assert_eq!(dones.len(), 1, "exactly one terminal done: {events:?}");
    assert_eq!(dones[0]["data"]["ok"], json!(true), "{:?}", dones[0]);
    assert_eq!(
        events.last().unwrap()["type"],
        json!("git:clone:done"),
        "done is the terminal frame"
    );
}

/// (a) Worktree-mode create from a local repo over the real WSS transport:
/// `workspace.create { progressId }` streams milestone frames each echoing
/// the id, scoped to the new workspace id, ending in `complete 100` + one
/// `done { ok:true }`, with the worktree provisioning milestone present.
#[tokio::test]
async fn workspace_create_local_worktree_streams_progress_over_wss() {
    if !gate() {
        return;
    }
    let (daemon, port, cfg) = boot().await;
    let repo = daemon.scratch.join("local-src");
    std::fs::create_dir_all(&repo).unwrap();
    run_git(&["init", "-q", "-b", "main"], &repo);
    run_git(&["config", "user.name", "e2e"], &repo);
    run_git(&["config", "user.email", "e2e@example.com"], &repo);
    std::fs::write(repo.join("README.md"), "hello\n").unwrap();
    run_git(&["add", "README.md"], &repo);
    run_git(&["commit", "-q", "-m", "init"], &repo);

    let mut sub = connect_ws(port, cfg.clone()).await;
    subscribe_clone_events(&mut sub, 1).await;

    let mut rpc = connect_ws(port, cfg.clone()).await;
    let progress_id = "prog-wss-wt-1";
    let resp = wss_rpc_raw(
        &mut rpc,
        2,
        "workspace.create",
        json!({
            "title": "WSS Worktree Progress",
            "repositoryPath": repo.to_string_lossy(),
            "progressId": progress_id,
        }),
    )
    .await;
    assert_eq!(resp["jsonrpc"], json!("2.0"), "envelope: {resp}");
    assert!(
        resp.get("error").is_none(),
        "create must succeed, got: {resp}"
    );
    let ws_id = resp["result"]["workspace"]["id"]
        .as_str()
        .expect("workspace id")
        .to_string();

    let events = drain_progress_events(&mut sub, progress_id, 60).await;
    assert_progress_stream(&events, progress_id);
    for e in &events {
        assert_eq!(
            e["workspaceId"].as_str(),
            Some(ws_id.as_str()),
            "frame scoped to the new workspace: {e}"
        );
    }
    let phases: Vec<&str> = events
        .iter()
        .filter(|e| e["type"] == json!("git:clone:progress"))
        .filter_map(|e| e["data"]["phase"].as_str())
        .collect();
    assert!(
        phases.contains(&"worktree"),
        "worktree milestone present: {phases:?}"
    );
}

/// (b) Clone-from-URL create against a local `file://` fixture repo carrying
/// a submodule: the explicit `clonePath` keeps the legacy network-clone arm
/// (`git clone --recurse-submodules --progress`), whose stderr streams
/// through the normalized reporter — the `submodules` phase appears inside
/// its 70–85 segment, every frame echoes the `progressId`, the series is
/// non-decreasing to `complete 100`, one `done { ok:true }` terminates, and
/// the checkout materialises with the submodule populated.
#[tokio::test]
async fn workspace_create_clone_with_submodule_streams_normalized_progress_over_wss() {
    if !gate() {
        return;
    }
    let (daemon, port, cfg) = boot().await;
    let src = make_source_repo_with_submodule(&daemon.scratch);
    let clone_path = daemon.scratch.join("checkout").join("source-repo");

    let mut sub = connect_ws(port, cfg.clone()).await;
    subscribe_clone_events(&mut sub, 1).await;

    let mut rpc = connect_ws(port, cfg.clone()).await;
    let progress_id = "prog-wss-sub-1";
    let resp = wss_rpc_raw(
        &mut rpc,
        2,
        "workspace.create",
        json!({
            "title": "WSS Submodule Clone Progress",
            "githubUrl": format!("file://{}", src.display()),
            "clonePath": clone_path.to_string_lossy(),
            "progressId": progress_id,
        }),
    )
    .await;
    assert_eq!(resp["jsonrpc"], json!("2.0"), "envelope: {resp}");
    assert!(
        resp.get("error").is_none(),
        "create must succeed, got: {resp}"
    );
    let ws_id = resp["result"]["workspace"]["id"]
        .as_str()
        .expect("workspace id")
        .to_string();

    let events = drain_progress_events(&mut sub, progress_id, 60).await;
    assert_progress_stream(&events, progress_id);
    for e in &events {
        assert_eq!(
            e["workspaceId"].as_str(),
            Some(ws_id.as_str()),
            "frame scoped to the new workspace: {e}"
        );
    }
    // The submodule phase appears, normalized into its 70–85 segment.
    let submodule_frames: Vec<&Value> = events
        .iter()
        .filter(|e| e["type"] == json!("git:clone:progress"))
        .filter(|e| e["data"]["phase"] == json!("submodules"))
        .collect();
    assert!(
        !submodule_frames.is_empty(),
        "submodules phase present: {events:?}"
    );
    for e in &submodule_frames {
        let pct = e["data"]["percent"].as_i64().expect("percent");
        assert!(
            (SUBMODULE_SEGMENT_START..=CLONE_SEGMENT_END).contains(&pct),
            "submodule percent normalized into {SUBMODULE_SEGMENT_START}..={CLONE_SEGMENT_END}: {e}"
        );
    }
    // The clone materialised with the submodule populated (the
    // `--recurse-submodules` contract from the streaming-progress batch).
    assert!(
        clone_path.join("tracked.txt").exists(),
        "clone target populated"
    );
    assert!(
        clone_path.join("sub").join("inner.txt").exists(),
        "submodule populated by --recurse-submodules"
    );
}

/// Failure shape over the real transport: a clone that cannot succeed
/// (`file://` URL to a missing path) fails the create with a structured
/// JSON-RPC error AND still closes the progress stream with exactly one
/// `git:clone:done { ok:false }` echoing the `progressId`.
#[tokio::test]
async fn workspace_create_clone_failure_emits_done_ok_false_over_wss() {
    if !gate() {
        return;
    }
    let (daemon, port, cfg) = boot().await;

    let mut sub = connect_ws(port, cfg.clone()).await;
    subscribe_clone_events(&mut sub, 1).await;

    let mut rpc = connect_ws(port, cfg.clone()).await;
    let progress_id = "prog-wss-fail-1";
    let missing = daemon.scratch.join("definitely-not-a-repo.git");
    let resp = wss_rpc_raw(
        &mut rpc,
        2,
        "workspace.create",
        json!({
            "title": "WSS Failure Progress",
            "githubUrl": format!("file://{}", missing.display()),
            "clonePath": daemon.scratch.join("fail-checkout").to_string_lossy(),
            "progressId": progress_id,
        }),
    )
    .await;
    let err = &resp["error"];
    assert_eq!(err["code"], json!(-32603), "clone failure code: {resp}");
    assert!(
        err["data"]["detail"]
            .as_str()
            .is_some_and(|s| !s.is_empty()),
        "structured detail travels on the error: {resp}"
    );

    let events = drain_progress_events(&mut sub, progress_id, 60).await;
    let dones: Vec<&Value> = events
        .iter()
        .filter(|e| e["type"] == json!("git:clone:done"))
        .collect();
    assert_eq!(dones.len(), 1, "exactly one terminal done: {events:?}");
    let done = dones[0];
    assert_eq!(done["data"]["ok"], json!(false), "{done}");
    assert_eq!(done["data"]["progressId"], json!(progress_id));
    assert!(
        done["data"]["error"]
            .as_str()
            .is_some_and(|s| !s.is_empty()),
        "done carries the sanitized error detail: {done}"
    );
    assert_eq!(
        events.last().unwrap()["type"],
        json!("git:clone:done"),
        "done is the terminal frame"
    );
}
