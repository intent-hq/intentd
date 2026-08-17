//! WSS end-to-end for the read-side `git.*` extensions added in
//! docs/protocol/methods/git.md §5.6: `git.numstat`, `git.branchDiff`, `git.getRemoteUrl`,
//! and `git.getConfig` (STAB-10a).
//! Drives a real pinned-TLS WebSocket against a live `intentd serve` (WSS listener enabled via
//! config) and asserts the response envelope shape from docs/protocol/methods/git.md §5.6 plus the
//! `-32602` error envelope for the validation paths.
//!
//! Uses a tiny local repository as the workspace source so the test never
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
    let dir = PathBuf::from("/tmp").join(format!("itd-wss-gitr-{prefix}-{}", &id[..8]));
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
                    return v;
                }
            }
            Some(Ok(Message::Ping(p))) => {
                let _ = ws.send(Message::Pong(p)).await;
            }
            Some(Ok(_)) => continue,
            other => panic!("expected text frame, got {other:?}"),
        }
    }
}

fn gate() -> bool {
    match Command::new("git")
        .arg("--version")
        .stdout(Stdio::null())
        .stderr(Stdio::null())
        .status()
    {
        Ok(s) if s.success() => true,
        _ => {
            eprintln!("skipping git read WSS e2e: git not on PATH");
            false
        }
    }
}

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

/// Materialise a source repo with an origin URL configured (needed by
/// `git.getRemoteUrl`). Also runs one seed commit on `main`.
fn make_source_repo(dir: &Path) -> PathBuf {
    let repo = dir.join("source-repo");
    std::fs::create_dir_all(&repo).expect("mkdir source repo");
    run_git(&["init", "-q", "-b", "main"], &repo);
    run_git(
        &["remote", "add", "origin", "https://example.invalid/o/r.git"],
        &repo,
    );
    std::fs::write(repo.join("tracked.txt"), "one\ntwo\nthree\n").unwrap();
    run_git(&["add", "tracked.txt"], &repo);
    run_git(&["commit", "-q", "-m", "seed"], &repo);
    repo
}

async fn boot(workspaces_root: &Path) -> (Daemon, u16, Arc<ClientConfig>) {
    let data_dir = scratch_dir("data");
    let scratch = scratch_dir("scratch");
    let root_s = workspaces_root.to_string_lossy().to_string();
    let env: [(&str, &str); 3] = [
        ("INTENTD_AUTH_TOKEN", TOKEN),
        ("INTENTD_TCP_PORT", "0"),
        ("INTENTD_WORKSPACES_DIR", &root_s),
    ];
    let child = spawn_serve(&data_dir, "both", &env);
    let daemon = Daemon {
        child,
        data_dir: data_dir.clone(),
        scratch,
    };
    let socket = daemon.data_dir.join("intentd.sock");
    assert!(await_uds(&socket).await, "daemon did not start");
    let status =
        common::await_wss_status_logged(&socket, &daemon.data_dir.join("daemon.log")).await;
    let port = status["result"]["port"].as_u64().expect("port") as u16;
    let fingerprint = status["result"]["fingerprint"]
        .as_str()
        .expect("fingerprint")
        .to_string();
    (daemon, port, client_config(&fingerprint))
}

async fn create_workspace<S>(
    ws: &mut WebSocketStream<S>,
    repo: &Path,
    title: &str,
) -> (String, PathBuf)
where
    S: tokio::io::AsyncRead + tokio::io::AsyncWrite + Unpin,
{
    let created = wss_rpc(
        ws,
        2,
        "workspace.create",
        json!({
            "title": title,
            "repositoryPath": repo.to_string_lossy(),
            "repositoryName": "source-repo",
            "baseRef": "main",
            "idempotencyKey": Uuid::new_v4().to_string(),
        }),
    )
    .await;
    let ws_id = created["result"]["workspace"]["id"]
        .as_str()
        .expect("workspace id")
        .to_string();
    let wt = PathBuf::from(
        created["result"]["workspace"]["worktreePath"]
            .as_str()
            .expect("worktreePath"),
    );
    (ws_id, wt)
}

/// `git.numstat` — working-tree default (`staged` omitted → HEAD→workdir
/// tracked), plus the `staged: true` / `staged: false` variants over WSS.
/// Untracked files are excluded (numstat is tracked-only per legacy parity).
#[tokio::test]
async fn git_numstat_working_tree_over_wss() {
    if !gate() {
        return;
    }
    let root = scratch_dir("root-numstat");
    let (daemon, port, cfg) = boot(&root).await;
    let repo = make_source_repo(&daemon.scratch);
    let mut ws = connect_ws(port, cfg).await;
    let (ws_id, wt) = create_workspace(&mut ws, &repo, "Git Read E2E — numstat").await;

    // Modify tracked.txt + write an untracked file (must NOT show in numstat).
    std::fs::write(wt.join("tracked.txt"), "one\nCHANGED\nthree\nfour\n").unwrap();
    std::fs::write(wt.join("untracked.txt"), "hello\n").unwrap();

    // Default (staged omitted) → HEAD→workdir tracked, untracked excluded.
    let resp = wss_rpc(&mut ws, 3, "git.numstat", json!({ "workspaceId": ws_id })).await;
    assert!(resp.get("error").is_none(), "numstat default: {resp}");
    let items = resp["result"].as_array().expect("numstat array");
    assert_eq!(items.len(), 1, "only the tracked change appears: {resp}");
    assert_eq!(items[0]["filePath"], json!("tracked.txt"));
    assert_eq!(items[0]["additions"], json!(2));
    assert_eq!(items[0]["deletions"], json!(1));

    // staged: false → same tracked entry, still no untracked.
    let resp = wss_rpc(
        &mut ws,
        4,
        "git.numstat",
        json!({ "workspaceId": ws_id, "staged": false }),
    )
    .await;
    let items = resp["result"].as_array().expect("numstat array");
    let paths: Vec<&str> = items
        .iter()
        .filter_map(|i| i["filePath"].as_str())
        .collect();
    assert!(paths.contains(&"tracked.txt"), "staged=false: {resp}");
    assert!(!paths.contains(&"untracked.txt"), "staged=false: {resp}");

    // staged: true (nothing staged) → empty array.
    let resp = wss_rpc(
        &mut ws,
        5,
        "git.numstat",
        json!({ "workspaceId": ws_id, "staged": true }),
    )
    .await;
    assert_eq!(resp["result"], json!([]), "staged=true empty: {resp}");

    let _ = std::fs::remove_dir_all(&root);
    drop(daemon);
}

/// `git.numstat` branch-base range: after a commit on the feature branch,
/// the boundary→target diff picks up exactly that commit's tracked change.
#[tokio::test]
async fn git_numstat_branch_range_over_wss() {
    if !gate() {
        return;
    }
    let root = scratch_dir("root-numstat-range");
    let (daemon, port, cfg) = boot(&root).await;
    let repo = make_source_repo(&daemon.scratch);
    let mut ws = connect_ws(port, cfg).await;
    let (ws_id, wt) = create_workspace(&mut ws, &repo, "Git Read E2E — numstat range").await;

    // Commit a change on the worktree's feature branch so it diverges from `main`.
    std::fs::write(wt.join("tracked.txt"), "one\ntwo\nthree\nfour\n").unwrap();
    run_git(&["add", "tracked.txt"], &wt);
    run_git(&["commit", "-q", "-m", "wt-change"], &wt);

    // baseRef=main → merge-base(HEAD, main) → HEAD; the one committed
    // addition surfaces as `{additions: 1, deletions: 0}`.
    let resp = wss_rpc(
        &mut ws,
        3,
        "git.numstat",
        json!({ "workspaceId": ws_id, "baseRef": "main" }),
    )
    .await;
    let items = resp["result"].as_array().expect("numstat array");
    let entry = items
        .iter()
        .find(|i| i["filePath"] == json!("tracked.txt"))
        .expect("tracked.txt appears in range: {resp}");
    assert_eq!(entry["additions"], json!(1));
    assert_eq!(entry["deletions"], json!(0));

    let _ = std::fs::remove_dir_all(&root);
    drop(daemon);
}

/// `git.branchDiff` — full-file `oldContent`/`newContent` for each changed
/// file in the two-dot `<boundary>..<targetRef>` range. `chunks` is always
/// an empty array (branch-base viewer parity).
#[tokio::test]
async fn git_branch_diff_over_wss() {
    if !gate() {
        return;
    }
    let root = scratch_dir("root-branch-diff");
    let (daemon, port, cfg) = boot(&root).await;
    let repo = make_source_repo(&daemon.scratch);
    let mut ws = connect_ws(port, cfg).await;
    let (ws_id, wt) = create_workspace(&mut ws, &repo, "Git Read E2E — branchDiff").await;

    // Commit a modification on the worktree's feature branch.
    std::fs::write(wt.join("tracked.txt"), "one\nTWO\nthree\n").unwrap();
    run_git(&["add", "tracked.txt"], &wt);
    run_git(&["commit", "-q", "-m", "wt-modify"], &wt);

    let resp = wss_rpc(
        &mut ws,
        3,
        "git.branchDiff",
        json!({ "workspaceId": ws_id, "baseRef": "main" }),
    )
    .await;
    let items = resp["result"].as_array().expect("branchDiff array");
    let entry = items
        .iter()
        .find(|i| i["file"] == json!("tracked.txt"))
        .expect("tracked.txt appears: {resp}");
    assert_eq!(entry["chunks"], json!([]));
    assert_eq!(entry["oldContent"], json!("one\ntwo\nthree\n"));
    assert_eq!(entry["newContent"], json!("one\nTWO\nthree\n"));

    // Missing both baseRef and baseCommitSha → -32602.
    let resp = wss_rpc(
        &mut ws,
        4,
        "git.branchDiff",
        json!({ "workspaceId": ws_id }),
    )
    .await;
    assert!(resp.get("result").is_none(), "missing base: {resp}");
    assert_eq!(resp["error"]["code"], json!(-32602));

    let _ = std::fs::remove_dir_all(&root);
    drop(daemon);
}

/// `git.getRemoteUrl` — returns `{ url }` for a configured `origin`, `null`
/// for a missing remote, and `-32602` for a non-git or nonexistent repo path.
#[tokio::test]
async fn git_get_remote_url_over_wss() {
    if !gate() {
        return;
    }
    let root = scratch_dir("root-remote-url");
    let (daemon, port, cfg) = boot(&root).await;
    let repo = make_source_repo(&daemon.scratch);
    let mut ws = connect_ws(port, cfg).await;
    // No workspace needed — `git.getRemoteUrl` is path-based.

    // Configured `origin` — returns the URL.
    let resp = wss_rpc(
        &mut ws,
        3,
        "git.getRemoteUrl",
        json!({ "repoPath": repo.to_string_lossy() }),
    )
    .await;
    assert!(resp.get("error").is_none(), "getRemoteUrl: {resp}");
    assert_eq!(
        resp["result"]["url"],
        json!("https://example.invalid/o/r.git")
    );

    // Explicit remoteName pointing at a missing remote → { url: null }.
    let resp = wss_rpc(
        &mut ws,
        4,
        "git.getRemoteUrl",
        json!({
            "repoPath": repo.to_string_lossy(),
            "remoteName": "no-such-remote",
        }),
    )
    .await;
    assert!(resp["result"]["url"].is_null(), "missing remote: {resp}");

    // Nonexistent path → -32602 (path-based validation parity with getBranches).
    let resp = wss_rpc(
        &mut ws,
        5,
        "git.getRemoteUrl",
        json!({ "repoPath": "/no/such/dir" }),
    )
    .await;
    assert_eq!(resp["error"]["code"], json!(-32602));

    // Non-git directory → -32602.
    let non_git = daemon.scratch.join("non-git");
    std::fs::create_dir_all(&non_git).unwrap();
    let resp = wss_rpc(
        &mut ws,
        6,
        "git.getRemoteUrl",
        json!({ "repoPath": non_git.to_string_lossy() }),
    )
    .await;
    assert_eq!(resp["error"]["code"], json!(-32602));

    let _ = std::fs::remove_dir_all(&root);
    drop(daemon);
}

/// `git.getConfig` — returns `{ config: String }` with the raw `.git/config`
/// content for a git repository workspace, or empty string for remote/non-repo
/// workspaces. Exercises the WSS envelope and validates error codes.
#[tokio::test]
async fn git_get_config_over_wss() {
    if !gate() {
        return;
    }
    let root = scratch_dir("root-get-config");
    let (daemon, port, cfg) = boot(&root).await;
    let repo = make_source_repo(&daemon.scratch);
    let mut ws = connect_ws(port, cfg).await;
    let (ws_id, _wt) = create_workspace(&mut ws, &repo, "Git Read E2E — getConfig").await;

    // Success: should return raw .git/config content.
    let resp = wss_rpc(&mut ws, 3, "git.getConfig", json!({ "workspaceId": ws_id })).await;
    let config = resp["result"]["config"].as_str().expect("config field");
    assert!(
        config.contains("[core]"),
        "config has [core] section: {config}"
    );

    // Missing workspaceId → -32602 with "workspaceId is required".
    let resp = wss_rpc(&mut ws, 4, "git.getConfig", json!({})).await;
    assert_eq!(resp["error"]["code"], json!(-32602));
    assert!(
        resp["error"]["message"]
            .as_str()
            .unwrap()
            .contains("workspaceId"),
        "error mentions workspaceId: {:?}",
        resp
    );

    // Non-existent workspace → -32602.
    let resp = wss_rpc(
        &mut ws,
        5,
        "git.getConfig",
        json!({ "workspaceId": "nonexistent-ws-id" }),
    )
    .await;
    assert_eq!(resp["error"]["code"], json!(-32602));

    // Create a remote workspace (skipWorktree=true) → empty string.
    let remote_resp = wss_rpc(
        &mut ws,
        6,
        "workspace.create",
        json!({ "title": "Remote test", "skipWorktree": true }),
    )
    .await;
    let remote_id = remote_resp["result"]["workspace"]["id"]
        .as_str()
        .expect("remote workspace id");
    let config_resp = wss_rpc(
        &mut ws,
        7,
        "git.getConfig",
        json!({ "workspaceId": remote_id }),
    )
    .await;
    assert_eq!(config_resp["result"]["config"], json!(""));

    // Non-repo workspace: remove .git file/directory → empty string.
    let git_path = _wt.join(".git");
    if git_path.is_file() {
        std::fs::remove_file(&git_path).expect("remove .git file");
    } else if git_path.is_dir() {
        std::fs::remove_dir_all(&git_path).expect("remove .git dir");
    }
    let nonrepo_resp = wss_rpc(&mut ws, 8, "git.getConfig", json!({ "workspaceId": ws_id })).await;
    assert_eq!(nonrepo_resp["result"]["config"], json!(""));

    let _ = std::fs::remove_dir_all(&root);
    drop(daemon);
}
