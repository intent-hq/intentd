//! WSS end-to-end coverage for the `CoW` provisioning matrix (PROTOCOL §5.1 /
//! §5.5 / §6): drives the real pinned-TLS WebSocket against a live
//! `intentd serve` and asserts:
//!
//! - `workspace.create` with `workspace.cowIsolation` ON provisions a
//!   standalone `CoW` clone (`checkoutMode: "cow"`, working checkout on the
//!   workspace branch at the base tip, no worktree registration in the
//!   source repo).
//! - `workspace.cowIsolation` OFF keeps the linked-worktree path
//!   (`checkoutMode: "worktree"`).
//! - `workspace.cowIsolation` ON on a non-CoW filesystem falls back to the
//!   linked-worktree path (`checkoutMode: "worktree"`) — the setting is a
//!   preference, not a guarantee.
//! - `skipIsolation: true` wins over `workspace.cowIsolation` ON: direct
//!   mode, no checkout provisioned at all (no probe, no fallback).
//! - Per-agent `CoW` sandboxes are retired: an explicit `isolation: "cow"`
//!   is rejected with `-32602` / `isolation-removed` before side effects,
//!   `sandbox.cow.merge` / `sandbox.cow.discard` answer `-32601`, and the
//!   `workspace.cowIsolation` preference no longer puts a delegated child in
//!   a sandbox: it works in the workspace checkout.
//! - `workspace.delete` of a `CoW` workspace removes the clone from disk and
//!   leaves the source repository untouched.
//! - `workspace.duplicate` of a `CoW` workspace provisions a fresh standalone
//!   `CoW` clone for the duplicate (same decision matrix as create).
//!
//! Gated on `git` on PATH plus a CoW-capable filesystem via
//! `intent_git::cow_probe` (APFS/Btrfs/XFS-reflink); skips cleanly
//! elsewhere. The worktree-fallback scenario is inverse-gated (runs only
//! where `CoW` is NOT supported, e.g. ext4 CI runners). The sandbox
//! retirement scenarios run on every filesystem; the delegation one is
//! additionally gated on `node` + the mock ACP agent fixture.

#![cfg(unix)]

use crate::common;

use intentd_test_support::GuardedChild;
use std::path::{Path, PathBuf};
use std::process::{Command, Stdio};
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

const TOKEN: &str = "cdcdcdcdcdcdcdcdcdcdcdcdcdcdcdcdcdcdcdcdcdcdcdcdcdcdcdcdcdcdcdcd";

struct Daemon {
    child: GuardedChild,
    _data_dir: tempfile::TempDir,
    scratch: tempfile::TempDir,
}

impl Drop for Daemon {
    fn drop(&mut self) {
        let _ = self.child.kill_tree();
    }
}

fn scratch_dir(prefix: &str) -> tempfile::TempDir {
    common::test_tempdir_in("/tmp", &format!("itd-wss-cow-{prefix}-"))
}

fn spawn_serve(data_dir: &Path, env: &[(&str, &str)]) -> GuardedChild {
    let log = std::fs::File::create(data_dir.join("daemon.log")).expect("create daemon log");
    common::enable_ws_api(data_dir);
    common::seed_default_provider(data_dir);
    let mut cmd = common::hermetic_serve_command(data_dir);
    cmd.env("INTENTD_DATA_DIR", data_dir)
        .env("INTENTD_ASSERT_HERMETIC_ROOT", "1")
        .stdout(Stdio::null())
        .stderr(Stdio::from(log));
    for (k, v) in env {
        cmd.env(k, v);
    }
    common::hermetic_fixture_identity(&mut cmd, data_dir);
    GuardedChild::spawn(&mut cmd).expect("spawn intentd serve")
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

/// Send one JSON-RPC frame and return `result`; panics on an `error` member.
async fn wss_rpc<S>(ws: &mut WebSocketStream<S>, id: i64, method: &str, params: Value) -> Value
where
    S: tokio::io::AsyncRead + tokio::io::AsyncWrite + Unpin,
{
    let frame = json!({ "jsonrpc": "2.0", "id": id, "method": method, "params": params });
    ws.send(Message::Text(frame.to_string().into()))
        .await
        .expect("send rpc frame");
    loop {
        let next = timeout(
            intentd_test_support::test_timeout(Duration::from_secs(20)),
            ws.next(),
        )
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

/// Gate: skip if `git` is not on PATH.
fn git_gate(test: &str) -> bool {
    match Command::new("git")
        .arg("--version")
        .stdout(Stdio::null())
        .stderr(Stdio::null())
        .status()
    {
        Ok(s) if s.success() => true,
        _ => {
            eprintln!("skipping {test}: git not on PATH");
            false
        }
    }
}

/// Whether the filesystem hosting `/tmp` scratch dirs can CoW-clone
/// (`intent_git::cow_probe` — the same capability check the daemon runs).
fn cow_supported() -> bool {
    let probe = scratch_dir("probe");
    let src = probe.path().join("src");
    let dst = probe.path().join("dst");
    std::fs::create_dir_all(&src).expect("mkdir probe src");
    std::fs::create_dir_all(&dst).expect("mkdir probe dst");
    matches!(
        intent_git::cow_probe(&src, &dst),
        Ok(intent_git::CowSupport::Supported)
    )
}

/// Gate: skip on non-CoW filesystems.
fn cow_gate(test: &str) -> bool {
    let supported = cow_supported();
    if !supported {
        eprintln!("skipping {test}: filesystem does not support CoW cloning");
    }
    supported
}

/// Inverse gate: skip on CoW-capable filesystems (for the worktree-fallback
/// scenario, which needs an environment where the daemon's probe reports
/// Unsupported — e.g. ext4 CI runners).
fn no_cow_gate(test: &str) -> bool {
    let supported = cow_supported();
    if supported {
        eprintln!("skipping {test}: filesystem supports CoW cloning (fallback not reachable)");
    }
    !supported
}

/// Mock-agent gate (node + fixture script), for the delegation scenario.
fn mock_gate(test: &str) -> Option<String> {
    let script = std::env::var("MOCK_AGENT_SCRIPT_PATH").unwrap_or_else(|_| {
        format!(
            "{}/tests/fixtures/mock-acp-agent.mjs",
            env!("CARGO_MANIFEST_DIR")
        )
    });
    if intent_providers::resolve_on_path("node").is_none() {
        eprintln!("skipping {test}: node not on PATH");
        return None;
    }
    if !std::path::Path::new(&script).exists() {
        eprintln!("skipping {test}: mock script missing at {script}");
        return None;
    }
    Some(script)
}

#[track_caller]
fn run_git(args: &[&str], cwd: &Path) -> String {
    let out = Command::new("git")
        .args(args)
        .env("GIT_AUTHOR_NAME", "e2e")
        .env("GIT_AUTHOR_EMAIL", "e2e@example.com")
        .env("GIT_COMMITTER_NAME", "e2e")
        .env("GIT_COMMITTER_EMAIL", "e2e@example.com")
        .current_dir(cwd)
        .stderr(Stdio::null())
        .output()
        .expect("run git");
    assert!(out.status.success(), "git {args:?} failed");
    String::from_utf8_lossy(&out.stdout).trim().to_string()
}

/// Materialise a tiny source repository (one commit on `main`) inside `dir`,
/// returning its path and HEAD SHA.
fn make_source_repo(dir: &Path) -> (PathBuf, String) {
    let repo = dir.join("source-repo");
    std::fs::create_dir_all(&repo).expect("mkdir source repo");
    run_git(&["init", "-q", "-b", "main"], &repo);
    std::fs::write(repo.join("README.md"), "hello\n").unwrap();
    run_git(&["add", "README.md"], &repo);
    run_git(&["commit", "-q", "-m", "init"], &repo);
    let sha = run_git(&["rev-parse", "HEAD"], &repo);
    (repo, sha)
}

/// Boot a live daemon with the workspaces root at `workspaces_root` and any
/// extra env (e.g. the mock agent seams), returning the WSS port + TLS config.
async fn boot(
    workspaces_root: &Path,
    extra_env: &[(&str, &str)],
) -> (Daemon, u16, Arc<ClientConfig>) {
    let data_dir_guard = scratch_dir("data");
    let data_dir = data_dir_guard.path().to_path_buf();
    let scratch = scratch_dir("scratch");
    let root_s = workspaces_root.to_string_lossy().to_string();
    let mut env: Vec<(&str, &str)> = vec![
        ("INTENTD_AUTH_TOKEN", TOKEN),
        ("INTENTD_WORKSPACES_DIR", &root_s),
    ];
    env.extend_from_slice(extra_env);
    let child = spawn_serve(&data_dir, &env);
    let daemon = Daemon {
        child,
        _data_dir: data_dir_guard,
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

/// Flip `workspace.cowIsolation` over the wire and assert the change applied.
async fn set_cow_isolation<S>(ws: &mut WebSocketStream<S>, id: i64, value: bool)
where
    S: tokio::io::AsyncRead + tokio::io::AsyncWrite + Unpin,
{
    let resp = wss_rpc(
        ws,
        id,
        "settings.update",
        json!({ "changes": [{ "path": "workspace.cowIsolation", "value": value }] }),
    )
    .await;
    assert_eq!(
        resp["applied"][0]["path"],
        json!("workspace.cowIsolation"),
        "cowIsolation change applied: {resp}"
    );
    assert_eq!(resp["applied"][0]["value"], json!(value));
}

/// Scenario A — `workspace.create` with `workspace.cowIsolation` ON: the
/// checkout is a standalone `CoW` clone (`checkoutMode: "cow"`), a working
/// checkout on the workspace branch at the base tip, with NO worktree
/// registration in the source repo (the clone is fully independent) and the
/// workspace branch existing only inside the clone.
#[tokio::test]
async fn workspace_create_provisions_cow_checkout_over_wss() {
    const TEST: &str = "workspace.create CoW WSS e2e";
    if !git_gate(TEST) || !cow_gate(TEST) {
        return;
    }
    let root_dir = scratch_dir("cowroot");
    let root = root_dir.path().to_path_buf();
    let (daemon, port, cfg) = boot(&root, &[]).await;
    let (repo, head_sha) = make_source_repo(daemon.scratch.path());

    let mut ws = connect_ws(port, cfg).await;
    set_cow_isolation(&mut ws, 1, true).await;

    let result = wss_rpc(
        &mut ws,
        2,
        "workspace.create",
        json!({
            "title": "CoW E2E",
            "repositoryPath": repo.to_string_lossy(),
            "repositoryName": "source-repo",
            "baseRef": "main",
            "initialAgent": { "prompt": "fix the auth flow" },
            "idempotencyKey": Uuid::new_v4().to_string(),
        }),
    )
    .await;

    let workspace = &result["workspace"];
    let id = workspace["id"].as_str().expect("workspace id");
    let wt = workspace["worktreePath"].as_str().expect("worktreePath");
    assert_eq!(
        workspace["checkoutMode"],
        json!("cow"),
        "cowIsolation on ⇒ checkoutMode cow: {workspace}"
    );
    assert_eq!(
        wt,
        root.join(id).join("source-repo").to_string_lossy().as_ref(),
        "CoW clone lives at <root>/<workspaceId>/<repo-slug>"
    );
    assert_eq!(workspace["baseCommitSha"], json!(head_sha));
    let branch = workspace["branch"].as_str().expect("branch");
    assert_eq!(branch, "auth-fix");

    // Working checkout: populated, on the workspace branch at the base tip.
    let wt_path = PathBuf::from(wt);
    assert!(wt_path.join("README.md").exists(), "checkout populated");
    assert_eq!(
        run_git(&["rev-parse", "--abbrev-ref", "HEAD"], &wt_path),
        branch
    );
    assert_eq!(run_git(&["rev-parse", "HEAD"], &wt_path), head_sha);

    // Standalone clone, not a linked worktree: `.git` is a real directory and
    // the source repo has no worktree registration for it.
    assert!(
        wt_path.join(".git").is_dir(),
        "CoW clone has a standalone .git directory (not a worktree gitfile)"
    );
    let worktrees = run_git(&["worktree", "list", "--porcelain"], &repo);
    assert!(
        !worktrees.contains(wt),
        "no worktree registration for the CoW clone in the source repo: {worktrees}"
    );
    // The workspace branch exists only inside the clone.
    let src_branches = run_git(&["branch", "--list", branch], &repo);
    assert!(
        src_branches.is_empty(),
        "workspace branch must not leak into the source repo: {src_branches}"
    );

    drop(daemon);
}

/// Scenario B — `workspace.cowIsolation` OFF (explicitly, matching the
/// default): `workspace.create` keeps the linked-worktree provisioning path
/// (`checkoutMode: "worktree"`, gitfile `.git`, registration in the source
/// repo).
#[tokio::test]
async fn workspace_create_defaults_to_worktree_when_cow_isolation_off() {
    const TEST: &str = "workspace.create worktree-default WSS e2e";
    if !git_gate(TEST) {
        return;
    }
    let root_dir = scratch_dir("wtroot");
    let root = root_dir.path().to_path_buf();
    let (daemon, port, cfg) = boot(&root, &[]).await;
    let (repo, head_sha) = make_source_repo(daemon.scratch.path());

    let mut ws = connect_ws(port, cfg).await;
    set_cow_isolation(&mut ws, 1, false).await;

    let result = wss_rpc(
        &mut ws,
        2,
        "workspace.create",
        json!({
            "title": "Worktree default E2E",
            "repositoryPath": repo.to_string_lossy(),
            "repositoryName": "source-repo",
            "baseRef": "main",
            "initialAgent": { "prompt": "fix the auth flow" },
            "idempotencyKey": Uuid::new_v4().to_string(),
        }),
    )
    .await;

    let workspace = &result["workspace"];
    let wt = workspace["worktreePath"].as_str().expect("worktreePath");
    assert_eq!(
        workspace["checkoutMode"],
        json!("worktree"),
        "cowIsolation off ⇒ checkoutMode worktree: {workspace}"
    );
    let wt_path = PathBuf::from(wt);
    assert!(wt_path.join("README.md").exists(), "checkout populated");
    assert_eq!(run_git(&["rev-parse", "HEAD"], &wt_path), head_sha);
    // Linked worktree: `.git` is a gitfile pointing at the source repo, and
    // the registration shows up in `git worktree list`.
    assert!(
        wt_path.join(".git").is_file(),
        "worktree checkout uses a .git gitfile"
    );
    let worktrees = run_git(&["worktree", "list", "--porcelain"], &repo);
    assert!(
        worktrees.contains(wt),
        "worktree registered in the source repo: {worktrees}"
    );

    drop(daemon);
}

/// Scenario B2 — mid-flight `CoW` failure safety net: `workspace.cowIsolation`
/// ON and the probe passes, but the clone itself fails as unsupported (forced
/// via the `INTENT_GIT_TEST_COW_CLONE_UNSUPPORTED_PATH` daemon seam, standing
/// in for e.g. a live socket tree the probe's tiny temp file cannot see).
/// `workspace.create` must transparently fall back to a linked worktree
/// instead of failing.
#[tokio::test]
async fn workspace_create_falls_back_to_worktree_when_clone_fails_midflight() {
    const TEST: &str = "workspace.create CoW mid-flight fallback WSS e2e";
    if !git_gate(TEST) || !cow_gate(TEST) {
        return;
    }
    let root_dir = scratch_dir("cowmidroot");
    let root = root_dir.path().to_path_buf();
    let (daemon, port, cfg) = boot(
        &root,
        &[("INTENT_GIT_TEST_COW_CLONE_UNSUPPORTED_PATH", "source-repo")],
    )
    .await;
    let (repo, head_sha) = make_source_repo(daemon.scratch.path());

    let mut ws = connect_ws(port, cfg).await;
    set_cow_isolation(&mut ws, 1, true).await;

    let result = wss_rpc(
        &mut ws,
        2,
        "workspace.create",
        json!({
            "title": "CoW mid-flight fallback E2E",
            "repositoryPath": repo.to_string_lossy(),
            "repositoryName": "source-repo",
            "baseRef": "main",
            "initialAgent": { "prompt": "fix the auth flow" },
            "idempotencyKey": Uuid::new_v4().to_string(),
        }),
    )
    .await;

    let workspace = &result["workspace"];
    let wt = workspace["worktreePath"].as_str().expect("worktreePath");
    assert_eq!(
        workspace["checkoutMode"],
        json!("worktree"),
        "clone-time Unsupported ⇒ transparent worktree fallback: {workspace}"
    );
    let wt_path = PathBuf::from(wt);
    assert!(wt_path.join("README.md").exists(), "checkout populated");
    assert_eq!(run_git(&["rev-parse", "HEAD"], &wt_path), head_sha);
    assert!(
        wt_path.join(".git").is_file(),
        "fallback checkout is a linked worktree (gitfile .git)"
    );

    drop(daemon);
}

/// Scenario B3 — `repositoryPath` is itself a linked git worktree: CoW-cloning
/// it would give the clone a gitfile `.git` still pointing at the ORIGINAL
/// repo (the branch switch + reset would rewrite the user's source checkout),
/// so `workspace.create` with `workspace.cowIsolation` ON must route it to
/// linked-worktree provisioning and leave the source checkout untouched.
#[tokio::test]
async fn workspace_create_routes_linked_worktree_source_to_worktree_mode() {
    const TEST: &str = "workspace.create linked-worktree source WSS e2e";
    if !git_gate(TEST) || !cow_gate(TEST) {
        return;
    }
    let root_dir = scratch_dir("cowwtroot");
    let root = root_dir.path().to_path_buf();
    let (daemon, port, cfg) = boot(&root, &[]).await;
    let (repo, head_sha) = make_source_repo(daemon.scratch.path());

    // The user's checkout is a linked worktree of the main repo.
    let user_wt = daemon.scratch.path().join("user-worktree");
    run_git(
        &[
            "worktree",
            "add",
            "-b",
            "user-branch",
            user_wt.to_str().unwrap(),
            "main",
        ],
        &repo,
    );
    assert!(user_wt.join(".git").is_file(), "user checkout is a gitfile");

    let mut ws = connect_ws(port, cfg).await;
    set_cow_isolation(&mut ws, 1, true).await;

    let result = wss_rpc(
        &mut ws,
        2,
        "workspace.create",
        json!({
            "title": "Linked worktree source E2E",
            "repositoryPath": user_wt.to_string_lossy(),
            "repositoryName": "source-repo",
            "baseRef": "user-branch",
            "initialAgent": { "prompt": "fix the auth flow" },
            "idempotencyKey": Uuid::new_v4().to_string(),
        }),
    )
    .await;

    let workspace = &result["workspace"];
    let wt = workspace["worktreePath"].as_str().expect("worktreePath");
    assert_eq!(
        workspace["checkoutMode"],
        json!("worktree"),
        "linked-worktree source ⇒ worktree mode even with cowIsolation on: {workspace}"
    );
    let wt_path = PathBuf::from(wt);
    assert!(wt_path.join("README.md").exists(), "checkout populated");
    assert_eq!(run_git(&["rev-parse", "HEAD"], &wt_path), head_sha);
    // The user's source worktree is untouched: still on its own branch.
    assert_eq!(
        run_git(&["rev-parse", "--abbrev-ref", "HEAD"], &user_wt),
        "user-branch",
        "source checkout is not rewritten"
    );

    drop(daemon);
}

/// Send one JSON-RPC frame and return its `error` member; panics on a result.
async fn wss_rpc_err<S>(ws: &mut WebSocketStream<S>, id: i64, method: &str, params: Value) -> Value
where
    S: tokio::io::AsyncRead + tokio::io::AsyncWrite + Unpin,
{
    let frame = json!({ "jsonrpc": "2.0", "id": id, "method": method, "params": params });
    ws.send(Message::Text(frame.to_string().into()))
        .await
        .expect("send rpc frame");
    loop {
        let next = timeout(
            intentd_test_support::test_timeout(Duration::from_secs(20)),
            ws.next(),
        )
        .await
        .expect("wss rpc timed out");
        match next {
            Some(Ok(Message::Text(text))) => {
                let v: Value = serde_json::from_str(&text).expect("json frame");
                if v["id"] == json!(id) {
                    assert!(v.get("result").is_none(), "rpc {method} must fail: {v}");
                    return v["error"].clone();
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

/// A direct-mode workspace (`skipIsolation: true`) over the user's own
/// repository: the shape that was eligible for a per-agent `CoW` sandbox on
/// every filesystem, so the retirement scenarios run on ext4 CI too.
async fn create_direct_workspace<S>(ws: &mut WebSocketStream<S>, id: i64, repo: &Path) -> String
where
    S: tokio::io::AsyncRead + tokio::io::AsyncWrite + Unpin,
{
    let created = wss_rpc(
        ws,
        id,
        "workspace.create",
        json!({
            "title": "Retired Sandbox E2E",
            "repositoryPath": repo.to_string_lossy(),
            "repositoryName": "source-repo",
            "baseRef": "main",
            "skipIsolation": true,
            "idempotencyKey": Uuid::new_v4().to_string(),
        }),
    )
    .await;
    created["workspace"]["id"]
        .as_str()
        .expect("workspace id")
        .to_string()
}

/// Scenario C — per-agent `CoW` sandboxes are retired (`methods/nodes.md`
/// "Landing sequence" step 2): an explicit `isolation: "cow"` is rejected
/// with `-32602` / `data.code: "isolation-removed"` BEFORE any side effect,
/// on both the single and the batch form — no session row, no sandbox folder.
#[tokio::test]
async fn delegate_with_cow_isolation_is_rejected_before_side_effects() {
    const TEST: &str = "agent.delegate isolation-removed WSS e2e";
    if !git_gate(TEST) {
        return;
    }
    let root_dir = scratch_dir("sbgone");
    let root = root_dir.path().to_path_buf();
    let (daemon, port, cfg) = boot(&root, &[]).await;
    let (repo, _head_sha) = make_source_repo(daemon.scratch.path());

    let mut rpc = connect_ws(port, cfg).await;
    set_cow_isolation(&mut rpc, 1, true).await;
    let ws_id = create_direct_workspace(&mut rpc, 2, &repo).await;

    let single = wss_rpc_err(
        &mut rpc,
        3,
        "agent.delegate",
        json!({
            "workspaceId": ws_id,
            "agentInstructions": "work in a sandbox",
            "isolation": "cow",
            "model": "default", "provider": "mock",
        }),
    )
    .await;
    assert_eq!(single["code"], json!(-32602), "invalid params: {single}");
    assert_eq!(
        single["data"]["code"],
        json!("isolation-removed"),
        "machine-readable retirement code: {single}"
    );

    let batch = wss_rpc_err(
        &mut rpc,
        4,
        "agent.delegate",
        json!({
            "workspaceId": ws_id,
            "tasks": ["no-such-task"],
            "isolation": "cow",
        }),
    )
    .await;
    assert_eq!(batch["code"], json!(-32602), "invalid params: {batch}");
    assert_eq!(
        batch["data"]["code"],
        json!("isolation-removed"),
        "the batch form is rejected before task validation: {batch}"
    );

    let list = wss_rpc(&mut rpc, 5, "agent.list", json!({ "workspaceId": ws_id })).await;
    assert_eq!(
        list["agents"].as_array().map(Vec::len),
        Some(0),
        "no session row persisted by the rejected calls: {list}"
    );
    assert!(
        !root.join(&ws_id).join("sandboxes").exists(),
        "no sandbox folder materialised"
    );

    drop(daemon);
}

/// Scenario C2 — the removed `sandbox.cow.merge` / `sandbox.cow.discard`
/// routes answer the normal `-32601` method-not-found, with no alias.
#[tokio::test]
async fn sandbox_cow_routes_are_removed_over_wss() {
    const TEST: &str = "sandbox.cow.* removed WSS e2e";
    if !git_gate(TEST) {
        return;
    }
    let root_dir = scratch_dir("sbroutes");
    let (daemon, port, cfg) = boot(root_dir.path(), &[]).await;
    let mut rpc = connect_ws(port, cfg).await;
    for (id, method) in [(1, "sandbox.cow.merge"), (2, "sandbox.cow.discard")] {
        let error = wss_rpc_err(
            &mut rpc,
            id,
            method,
            json!({ "workspaceId": "ws-x", "agentId": "agent-x" }),
        )
        .await;
        assert_eq!(error["code"], json!(-32601), "{method} is removed: {error}");
    }
    drop(daemon);
}

/// Scenario C3 — the persisted `workspace.cowIsolation` preference no longer
/// selects a per-agent sandbox: a delegate with no `isolation` (and one with
/// the accepted no-op `"shared"`) runs the child in the workspace checkout,
/// the result carries no `effectiveIsolation`, and no sandbox folder or
/// session sandbox field appears.
#[tokio::test]
async fn delegate_with_cow_isolation_setting_works_in_workspace_checkout() {
    const TEST: &str = "agent.delegate cowIsolation ignored WSS e2e";
    if !git_gate(TEST) {
        return;
    }
    let Some(script) = mock_gate(TEST) else {
        return;
    };
    let root_dir = scratch_dir("sbshared");
    let root = root_dir.path().to_path_buf();
    let behavior = json!({ "response": "done", "echoCwd": true }).to_string();
    let extra: [(&str, &str); 2] = [
        ("MOCK_AGENT_SCRIPT_PATH", &script),
        ("MOCK_AGENT_BEHAVIOR", &behavior),
    ];
    let (daemon, port, cfg) = boot(&root, &extra).await;
    let (repo, _head_sha) = make_source_repo(daemon.scratch.path());

    let mut rpc = connect_ws(port, cfg).await;
    set_cow_isolation(&mut rpc, 1, true).await;
    let ws_id = create_direct_workspace(&mut rpc, 2, &repo).await;
    let expected = std::fs::canonicalize(&repo).expect("repo exists");

    for (id, isolation) in [(3, None), (4, Some("shared"))] {
        let mut params = json!({
            "workspaceId": ws_id,
            "agentInstructions": "do shared work",
            "model": "default", "provider": "mock",
        });
        if let Some(isolation) = isolation {
            params["isolation"] = json!(isolation);
        }
        let delegated = wss_rpc(&mut rpc, id, "agent.delegate", params).await;
        assert_eq!(delegated["ok"], json!(true), "delegate ok: {delegated}");
        assert!(
            delegated.get("effectiveIsolation").is_none(),
            "no sandbox provisioning is reported: {delegated}"
        );
        let agent_id = delegated["agentId"].as_str().expect("agentId").to_string();

        let echoed = poll_echoed_cwd(&mut rpc, id * 1000, &agent_id).await;
        let actual = std::fs::canonicalize(&echoed).unwrap_or_else(|_| PathBuf::from(&echoed));
        assert_eq!(
            actual, expected,
            "the child works in the workspace checkout, got {echoed}"
        );

        let session = wss_rpc(
            &mut rpc,
            id * 1000 + 500,
            "agent.getSession",
            json!({ "agentId": agent_id }),
        )
        .await;
        let text = session.to_string();
        assert!(
            !text.contains("sandboxPath") && !text.contains("sandboxId"),
            "no sandbox fields on the session: {session}"
        );
    }
    assert!(
        !root.join(&ws_id).join("sandboxes").exists(),
        "no sandbox folder materialised"
    );

    drop(daemon);
}

/// Poll the agent's transcript over WSS until the mock child's `echoCwd`
/// stamp (`cwd=<process.cwd()>`) appears, or panic after ~30s. Returns the
/// echoed path.
async fn poll_echoed_cwd<S>(ws: &mut WebSocketStream<S>, id_base: i64, agent_id: &str) -> String
where
    S: tokio::io::AsyncRead + tokio::io::AsyncWrite + Unpin,
{
    for attempt in 0..120i64 {
        let resp = wss_rpc(
            ws,
            id_base + attempt,
            "agent.getConversation",
            json!({ "agentId": agent_id }),
        )
        .await;
        let text = serde_json::to_string(&resp["messages"]).unwrap_or_default();
        if let Some(cwd) = text
            .split("cwd=")
            .nth(1)
            .and_then(|rest| rest.split(['"', ' ']).next())
        {
            return cwd.to_string();
        }
        tokio::time::sleep(Duration::from_millis(250)).await;
    }
    panic!("timed out waiting for the mock child's echoed cwd");
}

/// Scenario D — `workspace.delete` of a `CoW` workspace removes the clone from
/// disk (`<root>/<workspaceId>` swept) and leaves the source repository
/// untouched: no worktree registrations to prune, no workspace branch, the
/// original checkout intact.
#[tokio::test]
async fn workspace_delete_removes_cow_clone_over_wss() {
    const TEST: &str = "workspace.delete CoW WSS e2e";
    if !git_gate(TEST) || !cow_gate(TEST) {
        return;
    }
    let root_dir = scratch_dir("cowdel");
    let root = root_dir.path().to_path_buf();
    let (daemon, port, cfg) = boot(&root, &[]).await;
    let (repo, head_sha) = make_source_repo(daemon.scratch.path());

    let mut ws = connect_ws(port, cfg).await;
    set_cow_isolation(&mut ws, 1, true).await;

    let created = wss_rpc(
        &mut ws,
        2,
        "workspace.create",
        json!({
            "title": "CoW delete E2E",
            "repositoryPath": repo.to_string_lossy(),
            "repositoryName": "source-repo",
            "baseRef": "main",
            "initialAgent": { "prompt": "fix the auth flow" },
            "idempotencyKey": Uuid::new_v4().to_string(),
        }),
    )
    .await;
    let workspace = &created["workspace"];
    assert_eq!(workspace["checkoutMode"], json!("cow"));
    let id = workspace["id"].as_str().expect("id").to_string();
    let wt = PathBuf::from(workspace["worktreePath"].as_str().expect("worktreePath"));
    assert!(wt.exists());

    let deleted = wss_rpc(&mut ws, 3, "workspace.delete", json!({ "workspaceId": id })).await;
    assert_eq!(deleted, json!({ "success": true }));
    // Fast-ack: cleanup runs in the background; poll for the final state.
    for _ in 0..50 {
        if !wt.exists() && !root.join(&id).exists() {
            break;
        }
        tokio::time::sleep(Duration::from_millis(100)).await;
    }
    assert!(!wt.exists(), "CoW clone removed from disk");
    assert!(
        !root.join(&id).exists(),
        "<root>/<workspaceId> parent swept"
    );

    // Source repo untouched: single worktree entry (itself), no leaked
    // workspace branch, original checkout intact at the same HEAD.
    let worktrees = run_git(&["worktree", "list", "--porcelain"], &repo);
    assert_eq!(
        worktrees.matches("worktree ").count(),
        1,
        "only the source repo's own entry remains: {worktrees}"
    );
    let branches = run_git(&["branch", "--list", "auth-fix"], &repo);
    assert!(
        branches.is_empty(),
        "no workspace branch leaked into the source repo: {branches}"
    );
    assert!(repo.join("README.md").exists(), "source checkout intact");
    assert_eq!(run_git(&["rev-parse", "HEAD"], &repo), head_sha);

    drop(daemon);
}

/// Scenario E — worktree fallback: `workspace.cowIsolation` ON on a
/// filesystem whose probe reports Unsupported falls back to the
/// linked-worktree path — the create succeeds with
/// `checkoutMode: "worktree"` and a working linked-worktree checkout
/// instead of failing with `-32603`. Inverse-gated: runs only where `CoW` is
/// NOT supported (e.g. ext4 CI runners); on APFS the daemon's probe would
/// succeed.
#[tokio::test]
async fn workspace_create_falls_back_to_worktree_when_cow_unsupported_over_wss() {
    const TEST: &str = "workspace.create CoW worktree-fallback WSS e2e";
    if !git_gate(TEST) || !no_cow_gate(TEST) {
        return;
    }
    let root_dir = scratch_dir("cownope");
    let root = root_dir.path().to_path_buf();
    let (daemon, port, cfg) = boot(&root, &[]).await;
    let (repo, head_sha) = make_source_repo(daemon.scratch.path());

    let mut ws = connect_ws(port, cfg).await;
    set_cow_isolation(&mut ws, 1, true).await;

    let result = wss_rpc(
        &mut ws,
        2,
        "workspace.create",
        json!({
            "title": "CoW fallback E2E",
            "repositoryPath": repo.to_string_lossy(),
            "repositoryName": "source-repo",
            "baseRef": "main",
            "idempotencyKey": Uuid::new_v4().to_string(),
        }),
    )
    .await;

    let workspace = &result["workspace"];
    let id = workspace["id"].as_str().expect("workspace id");
    assert_eq!(
        workspace["checkoutMode"],
        json!("worktree"),
        "unsupported probe falls back to a linked worktree: {workspace}"
    );
    let wt_path = workspace["worktreePath"].as_str().expect("worktreePath");
    assert_eq!(
        Path::new(wt_path),
        root.join(id).join("source-repo"),
        "checkout lives at <root>/<workspaceId>/<repo-slug>"
    );
    assert_eq!(workspace["baseCommitSha"], json!(head_sha));
    // The fallback checkout is a real linked worktree registered against the
    // source repo (unlike a standalone CoW clone).
    let worktrees = run_git(&["worktree", "list", "--porcelain"], &repo);
    assert!(
        worktrees.contains(wt_path),
        "fallback worktree registered in the source repo: {worktrees}"
    );

    drop(daemon);
}

/// Scenario F — `skipIsolation: true` wins over `workspace.cowIsolation` ON:
/// the workspace is direct-mode (no worktree, no `CoW` clone, `checkoutMode`
/// omitted) and the `CoW` probe never runs — no checkout of any kind is
/// provisioned. Runs on any filesystem.
#[tokio::test]
async fn workspace_create_skip_isolation_wins_over_cow_isolation() {
    const TEST: &str = "workspace.create skipIsolation-over-CoW WSS e2e";
    if !git_gate(TEST) {
        return;
    }
    let root_dir = scratch_dir("skipcow");
    let root = root_dir.path().to_path_buf();
    let (daemon, port, cfg) = boot(&root, &[]).await;
    let (repo, _head_sha) = make_source_repo(daemon.scratch.path());

    let mut ws = connect_ws(port, cfg).await;
    set_cow_isolation(&mut ws, 1, true).await;

    let result = wss_rpc(
        &mut ws,
        2,
        "workspace.create",
        json!({
            "title": "Skip over CoW E2E",
            "repositoryPath": repo.to_string_lossy(),
            "repositoryName": "source-repo",
            "baseRef": "main",
            "skipIsolation": true,
            "idempotencyKey": Uuid::new_v4().to_string(),
        }),
    )
    .await;

    let workspace = &result["workspace"];
    let id = workspace["id"].as_str().expect("workspace id");
    assert!(
        workspace["worktreePath"].is_null(),
        "direct mode: no checkout provisioned: {workspace}"
    );
    assert!(
        workspace["checkoutMode"].is_null(),
        "checkoutMode omitted for direct-mode rows: {workspace}"
    );
    assert!(
        workspace["baseCommitSha"].is_null(),
        "no baseCommitSha without a checkout: {workspace}"
    );
    assert!(
        !root.join(id).join("source-repo").exists(),
        "no clone materialised under the workspaces root"
    );
    // The source repo is untouched (no worktree registration, no branch).
    let worktrees = run_git(&["worktree", "list", "--porcelain"], &repo);
    assert_eq!(
        worktrees.matches("worktree ").count(),
        1,
        "only the source repo's own entry: {worktrees}"
    );

    drop(daemon);
}

/// Scenario G — `workspace.duplicate` of a `CoW` workspace: the duplicate gets
/// its own fresh standalone `CoW` clone (`checkoutMode: "cow"`, real `.git`
/// dir, no worktree registration in the source repo) at
/// `<root>/<newId>/<repo-slug>` on the duplicate's branch — the same decision
/// matrix as create.
#[tokio::test]
async fn workspace_duplicate_provisions_cow_clone_over_wss() {
    const TEST: &str = "workspace.duplicate CoW WSS e2e";
    if !git_gate(TEST) || !cow_gate(TEST) {
        return;
    }
    let root_dir = scratch_dir("cowdup");
    let root = root_dir.path().to_path_buf();
    let (daemon, port, cfg) = boot(&root, &[]).await;
    let (repo, head_sha) = make_source_repo(daemon.scratch.path());

    let mut ws = connect_ws(port, cfg).await;
    set_cow_isolation(&mut ws, 1, true).await;

    let created = wss_rpc(
        &mut ws,
        2,
        "workspace.create",
        json!({
            "title": "CoW Dup Source",
            "repositoryPath": repo.to_string_lossy(),
            "repositoryName": "source-repo",
            "baseRef": "main",
            "initialAgent": { "prompt": "fix the auth flow" },
            "idempotencyKey": Uuid::new_v4().to_string(),
        }),
    )
    .await;
    let source_id = created["workspace"]["id"].as_str().expect("id").to_string();
    assert_eq!(created["workspace"]["checkoutMode"], json!("cow"));

    let dup = wss_rpc(
        &mut ws,
        3,
        "workspace.duplicate",
        json!({ "workspaceId": source_id }),
    )
    .await;
    let workspace = &dup["workspace"];
    let dup_id = workspace["id"].as_str().expect("dup id");
    assert_ne!(dup_id, source_id, "duplicate mints a fresh id");
    assert_eq!(
        workspace["checkoutMode"],
        json!("cow"),
        "duplicate of a CoW workspace is CoW too: {workspace}"
    );
    let wt = workspace["worktreePath"].as_str().expect("worktreePath");
    assert_eq!(
        wt,
        root.join(dup_id)
            .join("source-repo")
            .to_string_lossy()
            .as_ref(),
        "duplicate clone lives at <root>/<newId>/<repo-slug>"
    );
    assert_eq!(workspace["baseCommitSha"], json!(head_sha));

    // Fresh standalone clone on the duplicate's branch at the base tip.
    let branch = workspace["branch"].as_str().expect("branch").to_string();
    let wt_path = PathBuf::from(wt);
    assert!(
        wt_path.join("README.md").exists(),
        "duplicate checkout populated"
    );
    assert!(
        wt_path.join(".git").is_dir(),
        "duplicate is a standalone CoW clone (not a worktree gitfile)"
    );
    assert_eq!(
        run_git(&["rev-parse", "--abbrev-ref", "HEAD"], &wt_path),
        branch
    );
    assert_eq!(run_git(&["rev-parse", "HEAD"], &wt_path), head_sha);
    // No worktree registration or branch leak in the source repo.
    let worktrees = run_git(&["worktree", "list", "--porcelain"], &repo);
    assert!(
        !worktrees.contains(wt),
        "no worktree registration for the duplicate clone: {worktrees}"
    );
    let src_branches = run_git(&["branch", "--list", &branch], &repo);
    assert!(
        src_branches.is_empty(),
        "duplicate branch must not leak into the source repo: {src_branches}"
    );

    drop(daemon);
}

/// Scenario E2 — `workspace.duplicate` mid-flight `CoW` failure safety net:
/// with the clone forced to fail as unsupported (same seam as Scenario B2),
/// both the create and the duplicate must transparently fall back to a linked
/// worktree instead of failing — exercising the duplicate path's
/// `Ok(Err(Unsupported))` retry arm.
#[tokio::test]
async fn workspace_duplicate_falls_back_to_worktree_when_clone_fails_midflight() {
    const TEST: &str = "workspace.duplicate CoW mid-flight fallback WSS e2e";
    if !git_gate(TEST) || !cow_gate(TEST) {
        return;
    }
    let root_dir = scratch_dir("cowdupmid");
    let root = root_dir.path().to_path_buf();
    let (daemon, port, cfg) = boot(
        &root,
        &[("INTENT_GIT_TEST_COW_CLONE_UNSUPPORTED_PATH", "source-repo")],
    )
    .await;
    let (repo, head_sha) = make_source_repo(daemon.scratch.path());

    let mut ws = connect_ws(port, cfg).await;
    set_cow_isolation(&mut ws, 1, true).await;

    let created = wss_rpc(
        &mut ws,
        2,
        "workspace.create",
        json!({
            "title": "CoW Dup Mid-flight Source",
            "repositoryPath": repo.to_string_lossy(),
            "repositoryName": "source-repo",
            "baseRef": "main",
            "initialAgent": { "prompt": "fix the auth flow" },
            "idempotencyKey": Uuid::new_v4().to_string(),
        }),
    )
    .await;
    let source_id = created["workspace"]["id"].as_str().expect("id").to_string();
    assert_eq!(
        created["workspace"]["checkoutMode"],
        json!("worktree"),
        "create falls back mid-flight: {created}"
    );

    let dup = wss_rpc(
        &mut ws,
        3,
        "workspace.duplicate",
        json!({ "workspaceId": source_id }),
    )
    .await;
    let workspace = &dup["workspace"];
    assert_eq!(
        workspace["checkoutMode"],
        json!("worktree"),
        "duplicate falls back mid-flight instead of failing: {workspace}"
    );
    let wt = workspace["worktreePath"].as_str().expect("worktreePath");
    let wt_path = PathBuf::from(wt);
    assert!(
        wt_path.join("README.md").exists(),
        "duplicate checkout populated"
    );
    assert!(
        wt_path.join(".git").is_file(),
        "duplicate fallback is a linked worktree (gitfile .git)"
    );
    assert_eq!(run_git(&["rev-parse", "HEAD"], &wt_path), head_sha);

    drop(daemon);
}
