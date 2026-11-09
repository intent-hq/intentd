//! WSS end-to-end coverage for local-repository checkouts after the
//! `workspace.cowIsolation` retirement (PROTOCOL §5.1 / §5.5 / §6): drives
//! the real pinned-TLS WebSocket against a live `intentd serve` and asserts:
//!
//! - A local-repo `workspace.create` always provisions a linked worktree
//!   (`checkoutMode: "worktree"`), even when an upgraded config.toml still
//!   carries `cowIsolation = true`; the boot strip removes that key, the
//!   retired path is ignored by `settings.update` and unknown to
//!   `settings.get`, `workspace.duplicate` answers `-32601`, and neither
//!   `workspace.get` nor `system.capabilities` reports `cowSupported`.
//! - A linked-worktree `repositoryPath` is provisioned as a worktree and the
//!   user's source checkout is left untouched.
//! - `skipIsolation: true` is direct mode: no checkout is provisioned.
//! - Per-agent `CoW` sandboxes are retired: an explicit `isolation: "cow"`
//!   is rejected with `-32602` / `isolation-removed` before side effects,
//!   `sandbox.cow.merge` / `sandbox.cow.discard` answer `-32601`, and a
//!   delegated child works in the workspace checkout.
//!
//! Gated on `git` on PATH; the delegation scenario is additionally gated on
//! `node` + the mock ACP agent fixture. Every scenario runs on any
//! filesystem.

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
    data_dir: tempfile::TempDir,
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
    boot_with_config(workspaces_root, extra_env, None).await
}

/// [`boot`] with a config.toml seeded before the daemon starts (e.g. an
/// upgraded user's file still carrying a retired key).
async fn boot_with_config(
    workspaces_root: &Path,
    extra_env: &[(&str, &str)],
    config_seed: Option<&str>,
) -> (Daemon, u16, Arc<ClientConfig>) {
    let data_dir_guard = scratch_dir("data");
    let data_dir = data_dir_guard.path().to_path_buf();
    if let Some(seed) = config_seed {
        std::fs::write(data_dir.join("config.toml"), seed).expect("seed config.toml");
    }
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
        data_dir: data_dir_guard,
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

/// Write the retired `workspace.cowIsolation` path as an old client would
/// and assert `settings.update` tolerates and ignores it (nothing applied).
async fn write_retired_cow_isolation<S>(ws: &mut WebSocketStream<S>, id: i64)
where
    S: tokio::io::AsyncRead + tokio::io::AsyncWrite + Unpin,
{
    let resp = wss_rpc(
        ws,
        id,
        "settings.update",
        json!({ "changes": [{ "path": "workspace.cowIsolation", "value": true }] }),
    )
    .await;
    assert_eq!(
        resp["applied"],
        json!([]),
        "retired path is ignored: {resp}"
    );
}

/// Scenario A — an upgraded config.toml still carrying the retired
/// `cowIsolation = true` boots, has the key stripped, and a local-repo
/// `workspace.create` provisions a linked worktree (`checkoutMode:
/// "worktree"`). The retired surfaces stay gone: `settings.update` ignores
/// the path, `settings.get` rejects it, `workspace.duplicate` answers
/// `-32601`, and `cowSupported` is absent from `workspace.get` and
/// `system.capabilities`. Runs on any filesystem.
#[tokio::test]
async fn workspace_create_ignores_legacy_cow_isolation_config_over_wss() {
    const TEST: &str = "workspace.create legacy cowIsolation WSS e2e";
    if !git_gate(TEST) {
        return;
    }
    let root_dir = scratch_dir("cowlegacy");
    let root = root_dir.path().to_path_buf();
    let (daemon, port, cfg) =
        boot_with_config(&root, &[], Some("[workspace]\ncowIsolation = true\n")).await;
    let config = std::fs::read_to_string(daemon.data_dir.path().join("config.toml"))
        .expect("read config.toml");
    assert!(
        !config.contains("cowIsolation"),
        "boot strips the retired key: {config}"
    );
    let (repo, head_sha) = make_source_repo(daemon.scratch.path());

    let mut ws = connect_ws(port, cfg).await;
    write_retired_cow_isolation(&mut ws, 1).await;
    let get_err = wss_rpc_err(
        &mut ws,
        2,
        "settings.get",
        json!({ "path": "workspace.cowIsolation" }),
    )
    .await;
    assert_eq!(get_err["code"], json!(-32602), "unknown setting: {get_err}");

    let result = wss_rpc(
        &mut ws,
        3,
        "workspace.create",
        json!({
            "title": "Legacy cowIsolation E2E",
            "repositoryPath": repo.to_string_lossy(),
            "repositoryName": "source-repo",
            "baseRef": "main",
            "initialAgent": { "prompt": "fix the auth flow" },
            "idempotencyKey": Uuid::new_v4().to_string(),
        }),
    )
    .await;
    let workspace = &result["workspace"];
    let id = workspace["id"].as_str().expect("workspace id").to_string();
    let wt = workspace["worktreePath"].as_str().expect("worktreePath");
    assert_eq!(
        workspace["checkoutMode"],
        json!("worktree"),
        "local-repo create is always a worktree: {workspace}"
    );
    assert_eq!(
        wt,
        root.join(&id)
            .join("source-repo")
            .to_string_lossy()
            .as_ref(),
        "worktree lives at <root>/<workspaceId>/<repo-slug>"
    );
    let wt_path = PathBuf::from(wt);
    assert!(wt_path.join(".git").is_file(), "linked worktree gitfile");
    assert_eq!(run_git(&["rev-parse", "HEAD"], &wt_path), head_sha);
    assert!(workspace.get("cowSupported").is_none(), "{workspace}");

    let got = wss_rpc(&mut ws, 4, "workspace.get", json!({ "workspaceId": id })).await;
    assert!(
        got["workspace"].get("cowSupported").is_none(),
        "workspace.get has no cowSupported: {got}"
    );
    let caps = wss_rpc(&mut ws, 5, "system.capabilities", json!({})).await;
    assert!(
        caps.get("cowSupported").is_none(),
        "system.capabilities has no cowSupported: {caps}"
    );
    let dup = wss_rpc_err(
        &mut ws,
        6,
        "workspace.duplicate",
        json!({ "workspaceId": id }),
    )
    .await;
    assert_eq!(
        dup["code"],
        json!(-32601),
        "workspace.duplicate removed: {dup}"
    );

    drop(daemon);
}

/// Scenario B — `workspace.create` from a local repository provisions a
/// linked worktree (`checkoutMode: "worktree"`, gitfile `.git`,
/// registration in the source repo).
#[tokio::test]
async fn workspace_create_provisions_linked_worktree_over_wss() {
    const TEST: &str = "workspace.create worktree-default WSS e2e";
    if !git_gate(TEST) {
        return;
    }
    let root_dir = scratch_dir("wtroot");
    let root = root_dir.path().to_path_buf();
    let (daemon, port, cfg) = boot(&root, &[]).await;
    let (repo, head_sha) = make_source_repo(daemon.scratch.path());

    let mut ws = connect_ws(port, cfg).await;

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
        "local repo ⇒ checkoutMode worktree: {workspace}"
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

/// Scenario B3 — `repositoryPath` is itself a linked git worktree: CoW-cloning
/// it would give the clone a gitfile `.git` still pointing at the ORIGINAL
/// repo (the branch switch + reset would rewrite the user's source checkout),
/// so `workspace.create` provisions a linked worktree and leaves the source
/// checkout untouched, even when an old client writes the retired
/// `workspace.cowIsolation` path first.
#[tokio::test]
async fn workspace_create_routes_linked_worktree_source_to_worktree_mode() {
    const TEST: &str = "workspace.create linked-worktree source WSS e2e";
    if !git_gate(TEST) {
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
    write_retired_cow_isolation(&mut ws, 1).await;

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
        "linked-worktree source ⇒ worktree mode: {workspace}"
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
    write_retired_cow_isolation(&mut rpc, 1).await;
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

/// Scenario C3 — an old client's `workspace.cowIsolation` write does not
/// select a per-agent sandbox: a delegate with no `isolation` (and one with
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
    write_retired_cow_isolation(&mut rpc, 1).await;
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

/// Scenario F — `skipIsolation: true` is direct mode (no worktree, no `CoW`
/// clone, `checkoutMode` omitted): no checkout of any kind is provisioned.
/// Runs on any filesystem.
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
    write_retired_cow_isolation(&mut ws, 1).await;

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
