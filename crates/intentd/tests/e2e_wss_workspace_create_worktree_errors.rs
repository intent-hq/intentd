//! WSS end-to-end test for workspace.create worktree failure error codes.
//! Drives the real WS transport and asserts that a "branch already checked
//! out" failure surfaces as -32602 (`InvalidParams`) with an actionable message
//! instead of a generic -32603 (Internal error).

#![cfg(unix)]

use crate::common;

use std::net::Ipv4Addr;
use std::path::Path;
use std::sync::Arc;

use futures_util::{SinkExt, StreamExt};
use intent_core::WorkspaceApi;
use intent_services::{EventBus, Services};
use intent_store::Store;
use intent_transport::{WsApiServer, WsOptions};
use serde_json::{json, Value};
use tokio::net::TcpStream;
use tokio::time::timeout;
use tokio_tungstenite::tungstenite::Message;
use tokio_tungstenite::{MaybeTlsStream, WebSocketStream};

type PlainWs = WebSocketStream<MaybeTlsStream<TcpStream>>;

struct Fixture {
    _ws: WsApiServer,
    port: u16,
    dir: tempfile::TempDir,
}

async fn boot() -> Fixture {
    boot_with(|services| services).await
}

async fn boot_with(configure: impl FnOnce(Services) -> Services) -> Fixture {
    let dir_guard = common::test_tempdir("intentd-wt-err-");
    let dir = dir_guard.path().to_path_buf();
    let store = Store::open(&dir.join("intentd.db")).await.expect("store");
    let bus = EventBus::new(store.clone());
    let workspaces_root = dir.join("workspaces");
    std::fs::create_dir_all(&workspaces_root).expect("mkdir hermetic root");
    let services = Services::new_isolated(store)
        .with_workspaces_root(workspaces_root)
        .with_event_bus(bus.clone());
    let services = configure(services);
    let api: Arc<dyn WorkspaceApi> = Arc::new(services);
    let opts = WsOptions {
        base_port: 0,
        bind_addresses: vec![Ipv4Addr::LOCALHOST.into()],
        ..Default::default()
    };
    let ws = WsApiServer::new_insecure(api, bus, opts, None);
    let port = ws.start().await.expect("start");
    Fixture {
        _ws: ws,
        port,
        dir: dir_guard,
    }
}

async fn connect(port: u16) -> PlainWs {
    let url = format!("ws://127.0.0.1:{port}/ws");
    let (sock, _resp) = tokio_tungstenite::connect_async(&url)
        .await
        .expect("plain ws handshake");
    sock
}

async fn wss_rpc_raw(ws: &mut PlainWs, id: i64, method: &str, params: Value) -> Value {
    let req = json!({ "jsonrpc": "2.0", "id": id, "method": method, "params": params });
    ws.send(Message::Text(req.to_string().into()))
        .await
        .unwrap();
    timeout(common::rpc_read_timeout(), async {
        loop {
            match ws.next().await.unwrap().unwrap() {
                Message::Text(text) => {
                    let v: Value = serde_json::from_str(&text).unwrap();
                    if v.get("id") == Some(&json!(id)) {
                        return v;
                    }
                }
                Message::Ping(_) | Message::Pong(_) => {}
                _ => panic!("unexpected message"),
            }
        }
    })
    .await
    .expect("response timeout")
}

/// Helper to initialize a git repository with one commit using the git CLI.
fn init_git_repo(path: &Path) -> String {
    std::process::Command::new("git")
        .args(["init"])
        .current_dir(path)
        .output()
        .expect("git init");
    std::process::Command::new("git")
        .args(["config", "user.name", "Test"])
        .current_dir(path)
        .output()
        .expect("git config user.name");
    std::process::Command::new("git")
        .args(["config", "user.email", "test@example.com"])
        .current_dir(path)
        .output()
        .expect("git config user.email");
    std::fs::write(path.join("README.md"), "# Test repo\n").unwrap();
    std::process::Command::new("git")
        .args(["add", "README.md"])
        .current_dir(path)
        .output()
        .expect("git add");
    std::process::Command::new("git")
        .args(["commit", "-m", "Initial commit"])
        .current_dir(path)
        .output()
        .expect("git commit");
    let output = std::process::Command::new("git")
        .args(["rev-parse", "--abbrev-ref", "HEAD"])
        .current_dir(path)
        .output()
        .expect("git rev-parse");
    String::from_utf8(output.stdout).unwrap().trim().to_string()
}

#[intent_test_macros::daemon_test]
async fn workspace_create_branch_already_checked_out_returns_invalid_params() {
    let fx = boot().await;
    let mut rpc = connect(fx.port).await;

    // Create a local git repository with one commit.
    let repo_dir = common::test_tempdir("wt-err-repo-");
    let branch_name = init_git_repo(repo_dir.path());

    // Attempt to create a workspace on the same branch that's already checked
    // out in the main working tree (the repository's current HEAD).
    let resp = wss_rpc_raw(
        &mut rpc,
        1,
        "workspace.create",
        json!({
            "repositoryPath": repo_dir.path().to_string_lossy(),
            "branch": branch_name,
        }),
    )
    .await;

    // Assert the error envelope: -32602 (InvalidParams) with "already checked out" message.
    let err = resp.get("error").expect("workspace.create should error");
    assert_eq!(
        err["code"], -32602,
        "expected -32602 (InvalidParams), got: {resp}"
    );
    let msg = err["message"].as_str().unwrap_or_default();
    assert!(
        msg.contains("already checked out"),
        "expected 'already checked out' in message, got: {msg}"
    );
    assert!(
        msg.contains(&branch_name),
        "expected branch name in message, got: {msg}"
    );
}

#[intent_test_macros::daemon_test]
async fn workspace_create_unresolvable_base_ref_returns_invalid_params_with_data() {
    let fx = boot().await;
    let mut rpc = connect(fx.port).await;

    // Create a local git repository with one commit.
    let repo_dir = common::test_tempdir("wt-err-repo-");
    init_git_repo(repo_dir.path());

    // Attempt to create a workspace from a base ref that does not exist.
    let resp = wss_rpc_raw(
        &mut rpc,
        1,
        "workspace.create",
        json!({
            "repositoryPath": repo_dir.path().to_string_lossy(),
            "branch": "feature-from-bogus-ref",
            "baseRef": "no-such-ref",
        }),
    )
    .await;

    // Assert the error envelope: -32602, unchanged human message, plus the
    // machine-readable data payload (monorepo#761).
    let err = resp.get("error").expect("workspace.create should error");
    assert_eq!(
        err["code"], -32602,
        "expected -32602 (InvalidParams), got: {resp}"
    );
    assert_eq!(
        err["message"],
        json!("invalid params: cannot resolve base ref 'no-such-ref'"),
        "human message must stay byte-identical, got: {resp}"
    );
    assert_eq!(
        err["data"],
        json!({ "code": "base-ref-unresolvable", "baseRef": "no-such-ref" }),
        "expected structured error.data, got: {resp}"
    );
}

#[track_caller]
fn git(path: &Path, args: &[&str]) -> String {
    let output = std::process::Command::new("git")
        .args(args)
        .current_dir(path)
        .output()
        .unwrap();
    assert!(
        output.status.success(),
        "{}",
        String::from_utf8_lossy(&output.stderr)
    );
    String::from_utf8(output.stdout).unwrap().trim().to_string()
}

#[intent_test_macros::daemon_test]
async fn workspace_create_refreshes_stale_base_and_preserves_source_changes() {
    let fx = boot().await;
    let mut rpc = connect(fx.port).await;
    let upstream = common::test_tempdir("wt-refresh-upstream-");
    let base = init_git_repo(upstream.path());
    let source = common::test_tempdir("wt-refresh-source-");
    git(
        source.path(),
        &["clone", upstream.path().to_str().unwrap(), "."],
    );
    let old = git(source.path(), &["rev-parse", "HEAD"]);
    std::fs::write(upstream.path().join("README.md"), "updated remote\n").unwrap();
    git(upstream.path(), &["commit", "-am", "remote update"]);
    let tip = git(upstream.path(), &["rev-parse", "HEAD"]);
    std::fs::write(source.path().join("README.md"), "source draft\n").unwrap();

    let resp = wss_rpc_raw(
        &mut rpc,
        1,
        "workspace.create",
        json!({"repositoryPath": source.path(), "branch": "fresh-workspace", "baseRef": base}),
    )
    .await;
    let workspace = &resp["result"]["workspace"];
    assert_eq!(workspace["baseCommitSha"], tip, "{resp}");
    let checkout = workspace["worktreePath"].as_str().unwrap();
    assert_eq!(
        intent_git::refs::rev_parse(Path::new(checkout), "HEAD").unwrap(),
        tip
    );
    assert_eq!(
        std::fs::read_to_string(Path::new(checkout).join("README.md")).unwrap(),
        "updated remote\n"
    );
    assert_eq!(git(source.path(), &["rev-parse", "HEAD"]), old);
    assert_eq!(
        std::fs::read_to_string(source.path().join("README.md")).unwrap(),
        "source draft\n"
    );
}

/// Clone `upstream` into a fresh source repo (so `refs/remotes/origin/<base>`
/// exists and the create refreshes it), then point `origin` at `url`.
fn source_with_origin(upstream: &Path, url: &str) -> tempfile::TempDir {
    let source = common::test_tempdir("wt-refresh-failed-source-");
    git(source.path(), &["clone", upstream.to_str().unwrap(), "."]);
    git(source.path(), &["remote", "set-url", "origin", url]);
    source
}

/// Write an executable `core.sshCommand` stub into `dir` and install it on
/// `repo`, so SSH remotes run `body` instead of a real `ssh`.
fn install_ssh_stub(repo: &Path, dir: &Path, body: &str) {
    use std::os::unix::fs::PermissionsExt;
    let stub = dir.join("ssh-stub.sh");
    std::fs::write(&stub, format!("#!/bin/sh\n{body}\n")).unwrap();
    std::fs::set_permissions(&stub, std::fs::Permissions::from_mode(0o755)).unwrap();
    git(repo, &["config", "core.sshCommand", stub.to_str().unwrap()]);
}

/// Send a worktree create off `base` and assert the documented
/// `base-refresh-failed` envelope (§9.2) with `category`, plus that nothing
/// (branch, folder) is left behind. Returns the error object.
async fn assert_base_refresh_failed(
    fx: &Fixture,
    source: &Path,
    base: &str,
    branch: &str,
    category: &str,
) -> Value {
    let mut rpc = connect(fx.port).await;
    let resp = wss_rpc_raw(
        &mut rpc,
        1,
        "workspace.create",
        json!({"repositoryPath": source, "branch": branch, "baseRef": base}),
    )
    .await;
    let err = resp["error"].clone();
    assert_eq!(err["code"], -32603, "{resp}");
    assert_eq!(err["data"]["code"], "base-refresh-failed", "{resp}");
    assert_eq!(err["data"]["category"], category, "{resp}");
    assert_eq!(err["data"]["baseRef"], base, "{resp}");
    assert_eq!(err["data"]["remote"], "origin", "{resp}");
    let detail = err["data"]["detail"].as_str().expect("detail string");
    assert!(!detail.is_empty(), "{resp}");
    let message = err["message"].as_str().unwrap();
    assert_eq!(
        message,
        format!("Couldn't update {base} from origin: {detail}"),
        "{resp}"
    );
    assert!(
        git(source, &["branch", "--list", branch]).is_empty(),
        "{resp}"
    );
    assert_eq!(
        std::fs::read_dir(fx.dir.path().join("workspaces"))
            .unwrap()
            .count(),
        0,
        "no workspace folder may be left behind: {resp}"
    );
    err
}

#[intent_test_macros::daemon_test]
async fn workspace_create_rejects_stale_remote_base_when_fetch_fails() {
    let fx = boot().await;
    let upstream = common::test_tempdir("wt-refresh-missing-upstream-");
    let base = init_git_repo(upstream.path());
    let missing = upstream.path().join("missing");
    let source = source_with_origin(upstream.path(), missing.to_str().unwrap());
    let err = assert_base_refresh_failed(
        &fx,
        source.path(),
        &base,
        "failed-refresh",
        "repo-not-found",
    )
    .await;
    assert!(
        err["data"]["detail"]
            .as_str()
            .unwrap()
            .contains("does not appear to be a git repository"),
        "{err}"
    );
}

/// HTTPS remote demanding credentials, none available and prompts disabled.
#[intent_test_macros::daemon_test]
async fn workspace_create_base_refresh_auth_required() {
    use std::io::{Read, Write};
    let fx = boot().await;
    let listener = std::net::TcpListener::bind("127.0.0.1:0").unwrap();
    let port = listener.local_addr().unwrap().port();
    std::thread::spawn(move || {
        for mut stream in listener.incoming().flatten() {
            let mut buf = [0u8; 4096];
            let _ = stream.read(&mut buf);
            let _ = stream.write_all(
                b"HTTP/1.1 401 Unauthorized\r\nWWW-Authenticate: Basic realm=\"test\"\r\nContent-Length: 0\r\nConnection: close\r\n\r\n",
            );
        }
    });
    let upstream = common::test_tempdir("wt-refresh-auth-upstream-");
    let base = init_git_repo(upstream.path());
    let source = source_with_origin(
        upstream.path(),
        &format!("http://127.0.0.1:{port}/repo.git"),
    );
    assert_base_refresh_failed(&fx, source.path(), &base, "auth-refresh", "auth-required").await;
}

/// Remote host that does not resolve.
#[intent_test_macros::daemon_test]
async fn workspace_create_base_refresh_network() {
    let fx = boot().await;
    let upstream = common::test_tempdir("wt-refresh-net-upstream-");
    let base = init_git_repo(upstream.path());
    let source = source_with_origin(
        upstream.path(),
        "http://intent-no-such-host.invalid/repo.git",
    );
    assert_base_refresh_failed(&fx, source.path(), &base, "net-refresh", "network").await;
}

/// A remote that never answers outlives the (shortened) fetch deadline: the
/// daemon kills git and reports `timeout`.
#[intent_test_macros::daemon_test]
async fn workspace_create_base_refresh_timeout() {
    let fx =
        boot_with(|s| s.with_base_refresh_timeout(std::time::Duration::from_millis(500))).await;
    let listener = std::net::TcpListener::bind("127.0.0.1:0").unwrap();
    let port = listener.local_addr().unwrap().port();
    let upstream = common::test_tempdir("wt-refresh-timeout-upstream-");
    let base = init_git_repo(upstream.path());
    let source = source_with_origin(
        upstream.path(),
        &format!("http://127.0.0.1:{port}/repo.git"),
    );
    let err =
        assert_base_refresh_failed(&fx, source.path(), &base, "timeout-refresh", "timeout").await;
    assert!(
        err["data"]["detail"]
            .as_str()
            .unwrap()
            .contains("timed out"),
        "{err}"
    );
    drop(listener);
}

/// SSH remote whose host key the daemon's machine has never trusted.
#[intent_test_macros::daemon_test]
async fn workspace_create_base_refresh_ssh_host_key() {
    let fx = boot().await;
    let upstream = common::test_tempdir("wt-refresh-hostkey-upstream-");
    let base = init_git_repo(upstream.path());
    let source = source_with_origin(upstream.path(), "ssh://git@example.invalid/repo.git");
    let stubs = common::test_tempdir("wt-refresh-hostkey-stub-");
    install_ssh_stub(
        source.path(),
        stubs.path(),
        "echo 'Host key verification failed.' >&2\nexit 255",
    );
    assert_base_refresh_failed(&fx, source.path(), &base, "hostkey-refresh", "ssh-host-key").await;
}

/// A credential embedded in the remote URL never reaches `detail` or
/// `message`, even when git's output echoes it.
#[intent_test_macros::daemon_test]
async fn workspace_create_base_refresh_redacts_credentials() {
    let fx = boot().await;
    let upstream = common::test_tempdir("wt-refresh-redact-upstream-");
    let base = init_git_repo(upstream.path());
    let source = source_with_origin(
        upstream.path(),
        "ssh://ghp_SECRETtoken123@example.invalid/repo.git",
    );
    let stubs = common::test_tempdir("wt-refresh-redact-stub-");
    install_ssh_stub(
        source.path(),
        stubs.path(),
        "echo \"ssh: connect to $*: fatal: unable to access 'https://x-access-token:hunter2pass@github.com/o/r.git/'\" >&2\nexit 255",
    );
    let err =
        assert_base_refresh_failed(&fx, source.path(), &base, "redact-refresh", "other").await;
    let text = err.to_string();
    for secret in ["ghp_SECRETtoken123", "hunter2pass"] {
        assert!(!text.contains(secret), "credential leaked: {text}");
    }
    assert!(text.contains("***@"), "redaction marker expected: {text}");
}

/// `skipBaseRefresh: true` never fetches: the worktree starts at the local
/// `refs/remotes/origin/<base>` as last fetched — not the newer upstream tip,
/// and not the local branch with its unpushed commit.
#[intent_test_macros::daemon_test]
async fn workspace_create_skip_base_refresh_starts_from_last_fetched_ref() {
    let fx = boot().await;
    let mut rpc = connect(fx.port).await;
    let upstream = common::test_tempdir("wt-skip-upstream-");
    let base = init_git_repo(upstream.path());
    let source = common::test_tempdir("wt-skip-source-");
    git(
        source.path(),
        &["clone", upstream.path().to_str().unwrap(), "."],
    );
    let last_fetched = git(
        source.path(),
        &["rev-parse", &format!("refs/remotes/origin/{base}")],
    );
    std::fs::write(upstream.path().join("README.md"), "newer remote\n").unwrap();
    git(upstream.path(), &["commit", "-am", "remote update"]);
    let upstream_tip = git(upstream.path(), &["rev-parse", "HEAD"]);
    std::fs::write(source.path().join("local.txt"), "unpushed\n").unwrap();
    git(source.path(), &["add", "local.txt"]);
    git(
        source.path(),
        &[
            "-c",
            "user.name=T",
            "-c",
            "user.email=t@e.x",
            "commit",
            "-m",
            "unpushed",
        ],
    );
    let local_head = git(source.path(), &["rev-parse", "HEAD"]);
    assert_ne!(last_fetched, upstream_tip);
    assert_ne!(last_fetched, local_head);

    let resp = wss_rpc_raw(
        &mut rpc,
        1,
        "workspace.create",
        json!({
            "repositoryPath": source.path(),
            "branch": "skip-refresh",
            "baseRef": base,
            "skipBaseRefresh": true,
        }),
    )
    .await;
    let workspace = &resp["result"]["workspace"];
    assert_eq!(workspace["baseCommitSha"], last_fetched, "{resp}");
    let checkout = workspace["worktreePath"].as_str().expect("worktreePath");
    assert_eq!(
        git(Path::new(checkout), &["rev-parse", "HEAD"]),
        last_fetched
    );
    assert_eq!(
        git(
            source.path(),
            &["rev-parse", &format!("refs/remotes/origin/{base}")]
        ),
        last_fetched,
        "skipBaseRefresh must not fetch"
    );
}

/// Continue path: the same create that failed its base refresh succeeds with
/// `skipBaseRefresh: true` (unreachable remote, no fetch attempted).
#[intent_test_macros::daemon_test]
async fn workspace_create_continue_after_base_refresh_failed() {
    let fx = boot().await;
    let upstream = common::test_tempdir("wt-continue-upstream-");
    let base = init_git_repo(upstream.path());
    let missing = upstream.path().join("missing");
    let source = source_with_origin(upstream.path(), missing.to_str().unwrap());
    let tracking = git(
        source.path(),
        &["rev-parse", &format!("refs/remotes/origin/{base}")],
    );
    assert_base_refresh_failed(&fx, source.path(), &base, "continue-ws", "repo-not-found").await;

    let mut rpc = connect(fx.port).await;
    let resp = wss_rpc_raw(
        &mut rpc,
        2,
        "workspace.create",
        json!({
            "repositoryPath": source.path(),
            "branch": "continue-ws",
            "baseRef": base,
            "skipBaseRefresh": true,
        }),
    )
    .await;
    assert_eq!(
        resp["result"]["workspace"]["baseCommitSha"], tracking,
        "{resp}"
    );
}

/// M5 time-box measurement: two creates off the same repository serialize
/// their base fetches under the per-repo provisioning lock, so the second
/// create's wall time is about lock wait + its own fetch ≈ 2× the fetch
/// bound. With the production 100 s bound that is ≈ 200 s, beyond the app's
/// old 120 s create timeout, even though each fetch stays inside the daemon's
/// own bound. Scaled down here to a 1.5 s bound.
#[intent_test_macros::daemon_test(flavor = "multi_thread")]
async fn concurrent_creates_serialize_base_refresh_under_repo_lock() {
    let fx =
        boot_with(|s| s.with_base_refresh_timeout(std::time::Duration::from_millis(1500))).await;
    let listener = std::net::TcpListener::bind("127.0.0.1:0").unwrap();
    let port = listener.local_addr().unwrap().port();
    let upstream = common::test_tempdir("wt-lock-upstream-");
    let base = init_git_repo(upstream.path());
    let source = source_with_origin(
        upstream.path(),
        &format!("http://127.0.0.1:{port}/repo.git"),
    );
    let started = std::time::Instant::now();
    let run = |branch: &'static str| {
        let source = source.path().to_path_buf();
        let base = base.clone();
        let port = fx.port;
        async move {
            let mut rpc = connect(port).await;
            let resp = wss_rpc_raw(
                &mut rpc,
                1,
                "workspace.create",
                json!({"repositoryPath": source, "branch": branch, "baseRef": base}),
            )
            .await;
            (resp, started.elapsed())
        }
    };
    let ((a, a_elapsed), (b, b_elapsed)) = tokio::join!(run("lock-a"), run("lock-b"));
    for resp in [&a, &b] {
        assert_eq!(resp["error"]["data"]["category"], "timeout", "{resp}");
    }
    let (first, second) = if a_elapsed <= b_elapsed {
        (a_elapsed, b_elapsed)
    } else {
        (b_elapsed, a_elapsed)
    };
    eprintln!("M5 measurement: first create {first:?}, second create {second:?}");
    assert!(
        second >= std::time::Duration::from_millis(2900),
        "second create waits for the first fetch, then runs its own: {second:?}"
    );
    drop(listener);
}
