//! WSS end-to-end for the write-side `git.*` methods added in
//! docs/protocol/methods/git.md §5.6: `git.createBranch`, `git.checkoutBranch`,
//! `git.renameBranch`, `git.stage`, `git.stageHunk`, `git.unstageHunk`,
//! `git.removeLockFile`, `git.push`, and `git.fetch`. Drives a real
//! pinned-TLS WebSocket against a live `intentd serve` (WSS listener enabled via config) and
//! asserts the response envelope shape from docs/protocol/methods/git.md §5.6 (`{ ok, ... }`)
//! plus the `-32602` error envelope from docs/protocol/09-error-codes.md §9 for the validation paths.
//!
//! `git.push`/`git.fetch` add a local bare-remote fixture wired up as
//! `origin` on the source repo (linked worktrees share the object store
//! and refs, so the workspace's worktree inherits the remote).
//!
//! Uses a tiny local repository as the workspace source so the test never
//! touches the network. Gated on `git` being on PATH; skips cleanly
//! otherwise.

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
    let dir = PathBuf::from("/tmp").join(format!("itd-wss-gitw-{prefix}-{}", &id[..8]));
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
            Some(Ok(_)) => {}
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
            eprintln!("skipping git write WSS e2e: git not on PATH");
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

/// Materialise a tiny source repository (`tracked.txt` on `main`) inside
/// `dir`, returning its path.
fn make_source_repo(dir: &Path) -> PathBuf {
    let repo = dir.join("source-repo");
    std::fs::create_dir_all(&repo).expect("mkdir source repo");
    run_git(&["init", "-q", "-b", "main"], &repo);
    // Repo-level identity: linked worktrees share the repo config, and the
    // daemon-side commit paths (`repo.signature()`) need `user.name`/`user.email`
    // — CI runners have no global git identity.
    run_git(&["config", "user.name", "e2e"], &repo);
    run_git(&["config", "user.email", "e2e@example.com"], &repo);
    std::fs::write(repo.join("tracked.txt"), "seed\n").unwrap();
    run_git(&["add", "tracked.txt"], &repo);
    run_git(&["commit", "-q", "-m", "seed"], &repo);
    repo
}

/// Materialise a source repository that carries a real submodule gitlink at
/// `sub` (cloned from a second tiny repo in `dir`), returning its path. The
/// `protocol.file.allow` override is required for `file://`-style submodule
/// clones on modern git.
fn make_source_repo_with_submodule(dir: &Path) -> PathBuf {
    let child = dir.join("child-repo");
    std::fs::create_dir_all(&child).expect("mkdir child repo");
    run_git(&["init", "-q", "-b", "main"], &child);
    run_git(&["config", "user.name", "e2e"], &child);
    run_git(&["config", "user.email", "e2e@example.com"], &child);
    std::fs::write(child.join("inner.txt"), "inner\n").unwrap();
    run_git(&["add", "inner.txt"], &child);
    run_git(&["commit", "-q", "-m", "inner seed"], &child);

    let repo = make_source_repo(dir);
    run_git(
        &[
            "-c",
            "protocol.file.allow=always",
            "submodule",
            "add",
            "-q",
            child.to_str().unwrap(),
            "sub",
        ],
        &repo,
    );
    run_git(&["commit", "-q", "-m", "add submodule"], &repo);
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

/// Exercises the branch write triad (`git.createBranch` → `git.checkoutBranch`
/// → `git.renameBranch`) over WSS, plus the empty-name `-32602` guard. Each
/// response envelope is verified against docs/protocol/methods/git.md §5.6, and the observable
/// side effect (`git.status.branch` reflecting the new HEAD) is asserted after
/// each mutation.
#[tokio::test]
async fn git_branch_ops_round_trip_over_wss() {
    if !gate() {
        return;
    }
    let root = scratch_dir("root-branch");
    let (daemon, port, cfg) = boot(&root).await;
    let repo = make_source_repo(&daemon.scratch);

    let mut ws = connect_ws(port, cfg).await;
    let (ws_id, _wt) = create_workspace(&mut ws, &repo, "Git Write E2E — branches").await;

    // Read the freshly-materialised worktree's current branch — `workspace.create`
    // provisions a linked worktree on its own feature branch, not the source
    // `main`, so the test resolves it at runtime rather than hard-coding a name.
    let status = wss_rpc(&mut ws, 3, "git.status", json!({ "workspaceId": ws_id })).await;
    let base_branch = status["result"]["branch"]
        .as_str()
        .expect("initial branch")
        .to_string();
    assert!(!base_branch.is_empty(), "initial branch: {status}");

    // createBranch (default checkout=true) — HEAD moves to `feature`.
    let resp = wss_rpc(
        &mut ws,
        4,
        "git.createBranch",
        json!({ "workspaceId": ws_id, "branchName": "feature" }),
    )
    .await;
    assert_eq!(resp["jsonrpc"], json!("2.0"));
    assert_eq!(resp["id"], json!(4));
    assert!(resp.get("error").is_none(), "createBranch: {resp}");
    assert_eq!(resp["result"]["ok"], json!(true));
    assert_eq!(resp["result"]["branch"], json!("feature"));

    let status = wss_rpc(&mut ws, 5, "git.status", json!({ "workspaceId": ws_id })).await;
    assert_eq!(status["result"]["branch"], json!("feature"));

    // checkoutBranch back to the base branch — HEAD tracks the returned branch.
    let resp = wss_rpc(
        &mut ws,
        6,
        "git.checkoutBranch",
        json!({ "workspaceId": ws_id, "branchName": base_branch }),
    )
    .await;
    assert_eq!(resp["result"]["ok"], json!(true), "checkoutBranch: {resp}");
    assert_eq!(
        resp["result"]["branch"].as_str(),
        Some(base_branch.as_str())
    );
    let status = wss_rpc(&mut ws, 7, "git.status", json!({ "workspaceId": ws_id })).await;
    assert_eq!(
        status["result"]["branch"].as_str(),
        Some(base_branch.as_str())
    );

    // renameBranch swaps the current branch's name; response echoes both names.
    let resp = wss_rpc(
        &mut ws,
        8,
        "git.renameBranch",
        json!({
            "workspaceId": ws_id,
            "oldBranchName": base_branch,
            "newBranchName": "trunk",
        }),
    )
    .await;
    assert_eq!(resp["result"]["ok"], json!(true), "renameBranch: {resp}");
    assert_eq!(
        resp["result"]["oldBranch"].as_str(),
        Some(base_branch.as_str())
    );
    assert_eq!(resp["result"]["newBranch"], json!("trunk"));
    let status = wss_rpc(&mut ws, 9, "git.status", json!({ "workspaceId": ws_id })).await;
    assert_eq!(status["result"]["branch"], json!("trunk"));

    // Empty new name ⇒ InvalidParams (-32602) per docs/protocol/09-error-codes.md §9.
    let resp = wss_rpc(
        &mut ws,
        10,
        "git.renameBranch",
        json!({
            "workspaceId": ws_id,
            "oldBranchName": "trunk",
            "newBranchName": "",
        }),
    )
    .await;
    assert!(resp.get("result").is_none(), "empty new name: {resp}");
    assert_eq!(resp["error"]["code"], json!(-32602));

    // Empty oldBranchName is also rejected as InvalidParams (parity with the
    // newBranchName guard — reviewer feedback).
    let resp = wss_rpc(
        &mut ws,
        11,
        "git.renameBranch",
        json!({
            "workspaceId": ws_id,
            "oldBranchName": "",
            "newBranchName": "any",
        }),
    )
    .await;
    assert_eq!(resp["error"]["code"], json!(-32602));

    let _ = std::fs::remove_dir_all(&root);
    drop(daemon);
}

/// A stale or ignored untracked path does not block a valid path in the same
/// `git.stage` request. Prohibited and unsafe entries reject the whole batch,
/// while fully unmatched and ignored-only requests keep a pathspec error.
#[tokio::test]
async fn git_stage_tolerates_stale_path_in_valid_batch_over_wss() {
    if !gate() {
        return;
    }
    let root = scratch_dir("root-stage-stale");
    let (daemon, port, cfg) = boot(&root).await;
    let repo = make_source_repo(&daemon.scratch);

    let mut ws = connect_ws(port, cfg).await;
    let (ws_id, wt) = create_workspace(&mut ws, &repo, "Git Write E2E — stale stage").await;
    std::fs::write(wt.join("tracked.txt"), "changed\n").unwrap();

    let resp = wss_rpc(
        &mut ws,
        3,
        "git.stage",
        json!({
            "workspaceId": ws_id,
            "paths": ["vanished.txt", "tracked.txt"],
        }),
    )
    .await;
    assert_eq!(
        resp,
        json!({
            "jsonrpc": "2.0",
            "id": 3,
            "result": {
                "ok": true,
                "paths": ["vanished.txt", "tracked.txt"],
            },
        })
    );

    let status = wss_rpc(&mut ws, 4, "git.status", json!({ "workspaceId": ws_id })).await;
    let files = status["result"]["files"].as_array().expect("files array");
    let tracked = files
        .iter()
        .find(|file| file["path"] == json!("tracked.txt"))
        .expect("tracked.txt in status after stage");
    assert_eq!(tracked["staged"], json!(true));
    assert!(files
        .iter()
        .all(|file| file["path"] != json!("vanished.txt")));

    let resp = wss_rpc(
        &mut ws,
        5,
        "git.unstage",
        json!({ "workspaceId": ws_id, "paths": ["tracked.txt"] }),
    )
    .await;
    assert_eq!(resp["result"]["ok"], json!(true), "unstage: {resp}");

    std::fs::write(wt.join("tracked.log"), "one\n").unwrap();
    run_git(&["add", "tracked.log"], &wt);
    run_git(&["commit", "-q", "-m", "tracked ignored fixture"], &wt);
    std::fs::write(wt.join(".gitignore"), "*.log\n").unwrap();
    std::fs::write(wt.join("tracked.log"), "two\n").unwrap();
    std::fs::write(wt.join("ignored.log"), "ignored\n").unwrap();

    let resp = wss_rpc(
        &mut ws,
        6,
        "git.stage",
        json!({
            "workspaceId": ws_id,
            "paths": ["ignored.log", "tracked.log"],
        }),
    )
    .await;
    assert_eq!(resp["result"]["ok"], json!(true), "mixed ignored: {resp}");
    assert_eq!(
        run_git(&["diff", "--cached", "--name-only"], &wt),
        "tracked.log"
    );

    let resp = wss_rpc(
        &mut ws,
        7,
        "git.unstage",
        json!({ "workspaceId": ws_id, "paths": ["tracked.log"] }),
    )
    .await;
    assert_eq!(resp["result"]["ok"], json!(true), "unstage: {resp}");

    let resp = wss_rpc(
        &mut ws,
        8,
        "git.stage",
        json!({ "workspaceId": ws_id, "paths": ["ignored.log"] }),
    )
    .await;
    assert_eq!(resp["error"]["code"], json!(-32603), "ignored-only: {resp}");
    assert!(run_git(&["diff", "--cached", "--name-only"], &wt).is_empty());

    let resp = wss_rpc(
        &mut ws,
        9,
        "git.stage",
        json!({ "workspaceId": ws_id, "paths": ["tracked.txt", "*"] }),
    )
    .await;
    assert_eq!(resp["error"]["code"], json!(-32603), "stage-all: {resp}");
    assert!(run_git(&["diff", "--cached", "--name-only"], &wt).is_empty());

    let resp = wss_rpc(
        &mut ws,
        10,
        "git.stage",
        json!({ "workspaceId": ws_id, "paths": ["tracked.txt", "../outside"] }),
    )
    .await;
    assert_eq!(resp["error"]["code"], json!(-32602), "traversal: {resp}");
    assert!(run_git(&["diff", "--cached", "--name-only"], &wt).is_empty());

    let resp = wss_rpc(
        &mut ws,
        11,
        "git.stage",
        json!({ "workspaceId": ws_id, "paths": ["still-missing.txt"] }),
    )
    .await;
    assert_eq!(resp["jsonrpc"], json!("2.0"));
    assert_eq!(resp["id"], json!(11));
    assert!(resp.get("result").is_none(), "unmatched stage: {resp}");
    assert_eq!(resp["error"]["code"], json!(-32603));
    let message = resp["error"]["data"]
        .as_str()
        .or_else(|| resp["error"]["message"].as_str())
        .unwrap_or_default();
    assert!(message.contains("pathspec 'still-missing.txt' did not match any files"));

    let _ = std::fs::remove_dir_all(&root);
    drop(daemon);
}

#[tokio::test]
async fn git_status_force_refresh_bypasses_cached_status_over_wss() {
    if !gate() {
        return;
    }
    let root = scratch_dir("root-status-force-refresh");
    let (daemon, port, cfg) = boot(&root).await;
    let repo = make_source_repo(&daemon.scratch);
    let mut ws = connect_ws(port, cfg).await;
    let (ws_id, wt) = create_workspace(&mut ws, &repo, "Git Write E2E — forced status").await;

    std::fs::write(wt.join("transient.txt"), "present\n").unwrap();
    let present = wss_rpc(
        &mut ws,
        3,
        "git.status",
        json!({ "workspaceId": ws_id, "forceRefresh": true }),
    )
    .await;
    assert_eq!(present["jsonrpc"], json!("2.0"));
    assert_eq!(present["id"], json!(3));
    assert!(present.get("error").is_none(), "forced status: {present}");
    assert!(present["result"]["files"]
        .as_array()
        .unwrap()
        .iter()
        .any(|file| file["path"] == json!("transient.txt")));

    std::fs::remove_file(wt.join("transient.txt")).unwrap();
    let refreshed = wss_rpc(
        &mut ws,
        4,
        "git.status",
        json!({ "workspaceId": ws_id, "forceRefresh": true }),
    )
    .await;
    assert_eq!(refreshed["jsonrpc"], json!("2.0"));
    assert_eq!(refreshed["id"], json!(4));
    assert!(
        refreshed.get("error").is_none(),
        "forced status refresh: {refreshed}"
    );
    assert!(refreshed["result"]["files"]
        .as_array()
        .unwrap()
        .iter()
        .all(|file| file["path"] != json!("transient.txt")));

    let _ = std::fs::remove_dir_all(&root);
    drop(daemon);
}

/// Exercises `git.push` and `git.fetch` over WSS against a local bare-remote
/// fixture. Response envelopes are checked against docs/protocol/methods/git.md §5.6 and the
/// bare-remote / local tracking ref advance is asserted after each call.
#[tokio::test]
async fn git_push_and_fetch_round_trip_over_wss() {
    if !gate() {
        return;
    }
    let root = scratch_dir("root-remote");
    let (daemon, port, cfg) = boot(&root).await;
    let repo = make_source_repo(&daemon.scratch);

    // Bare remote wired to the source repo as `origin`. Linked worktrees
    // share the object store and refs, so the workspace's worktree inherits
    // the remote without any extra config.
    let bare = daemon.scratch.join("bare-remote.git");
    run_git(
        &["init", "--bare", "-q", bare.to_str().unwrap()],
        &daemon.scratch,
    );
    run_git(&["remote", "add", "origin", bare.to_str().unwrap()], &repo);

    let mut ws = connect_ws(port, cfg).await;
    let (ws_id, wt) = create_workspace(&mut ws, &repo, "Git Write E2E — push/fetch").await;

    // Seed a commit on the workspace's worktree so there is something to push.
    std::fs::write(wt.join("tracked.txt"), "seed\nnew line\n").unwrap();
    run_git(&["add", "tracked.txt"], &wt);
    run_git(&["commit", "-q", "-m", "wt-change"], &wt);
    let local_sha = run_git(&["rev-parse", "HEAD"], &wt);
    let wt_branch = run_git(&["rev-parse", "--abbrev-ref", "HEAD"], &wt);

    // git.push — response carries `{ ok, branch, pushedSha }` per §5.6.
    let resp = wss_rpc(
        &mut ws,
        3,
        "git.push",
        json!({ "workspaceId": ws_id, "force": false }),
    )
    .await;
    assert!(resp.get("error").is_none(), "push: {resp}");
    assert_eq!(resp["result"]["ok"], json!(true));
    assert_eq!(resp["result"]["branch"].as_str(), Some(wt_branch.as_str()));
    assert_eq!(
        resp["result"]["pushedSha"].as_str(),
        Some(local_sha.as_str())
    );

    // Bare remote now carries the branch at the pushed sha.
    let remote_sha = run_git(&["rev-parse", &format!("refs/heads/{wt_branch}")], &bare);
    assert_eq!(remote_sha, local_sha);

    // Advance the bare remote out-of-band so a subsequent git.fetch has
    // something to pull down into the worktree's tracking ref.
    let clone_dir = daemon.scratch.join("bare-clone");
    run_git(
        &[
            "clone",
            "-q",
            bare.to_str().unwrap(),
            clone_dir.to_str().unwrap(),
        ],
        &daemon.scratch,
    );
    run_git(&["checkout", "-q", &wt_branch], &clone_dir);
    std::fs::write(
        clone_dir.join("tracked.txt"),
        "seed\nnew line\nremote-only\n",
    )
    .unwrap();
    run_git(&["add", "tracked.txt"], &clone_dir);
    run_git(&["commit", "-q", "-m", "remote-advance"], &clone_dir);
    run_git(&["push", "-q", "origin", &wt_branch], &clone_dir);
    let advanced_sha = run_git(&["rev-parse", "HEAD"], &clone_dir);

    // git.fetch — response carries `{ ok: true }` per §5.6.
    let resp = wss_rpc(&mut ws, 4, "git.fetch", json!({ "workspaceId": ws_id })).await;
    assert!(resp.get("error").is_none(), "fetch: {resp}");
    assert_eq!(resp["result"]["ok"], json!(true));

    // Local tracking ref for `origin/<branch>` now points at the advanced sha.
    let tracked = run_git(
        &["rev-parse", &format!("refs/remotes/origin/{wt_branch}")],
        &wt,
    );
    assert_eq!(tracked, advanced_sha);

    let _ = std::fs::remove_dir_all(&root);
    drop(daemon);
}

/// Exercises the working-tree write pair (`git.stageHunk` → `git.unstageHunk`)
/// plus `git.removeLockFile`. Each call is checked against its docs/protocol/methods/git.md §5.6
/// response shape and the resulting `git.status` file entry's `staged` flag.
/// Also covers the `git.agentCommit` no-`files` fallback semantics
/// (monorepo#939): the transport path carries no agent context, so an
/// agent-initiated commit is refused (-32603) rather than sweeping the
/// worktree, while a `userRequested` checkpoint commits only the staged paths.
#[tokio::test]
async fn git_hunk_and_lockfile_ops_round_trip_over_wss() {
    if !gate() {
        return;
    }
    let root = scratch_dir("root-hunk");
    let (daemon, port, cfg) = boot(&root).await;
    let repo = make_source_repo(&daemon.scratch);

    let mut ws = connect_ws(port, cfg).await;
    let (ws_id, wt) = create_workspace(&mut ws, &repo, "Git Write E2E — hunks").await;

    // Working-tree modification: append a line to `tracked.txt`.
    std::fs::write(wt.join("tracked.txt"), "seed\nnew line\n").unwrap();
    let patch = "diff --git a/tracked.txt b/tracked.txt\n--- a/tracked.txt\n+++ b/tracked.txt\n@@ -1 +1,2 @@\n seed\n+new line\n";

    let resp = wss_rpc(
        &mut ws,
        3,
        "git.stageHunk",
        json!({
            "workspaceId": ws_id,
            "filePath": "tracked.txt",
            "hunkPatch": patch,
        }),
    )
    .await;
    assert_eq!(resp["result"]["ok"], json!(true), "stageHunk: {resp}");

    let status = wss_rpc(&mut ws, 4, "git.status", json!({ "workspaceId": ws_id })).await;
    let files = status["result"]["files"].as_array().expect("files array");
    let entry = files
        .iter()
        .find(|f| f["path"] == json!("tracked.txt"))
        .expect("tracked.txt in status after stageHunk");
    assert_eq!(entry["staged"], json!(true));

    // Reverse the same hunk with `git.unstageHunk`.
    let resp = wss_rpc(
        &mut ws,
        5,
        "git.unstageHunk",
        json!({
            "workspaceId": ws_id,
            "filePath": "tracked.txt",
            "hunkPatch": patch,
        }),
    )
    .await;
    assert_eq!(resp["result"]["ok"], json!(true), "unstageHunk: {resp}");

    let status = wss_rpc(&mut ws, 6, "git.status", json!({ "workspaceId": ws_id })).await;
    let files = status["result"]["files"].as_array().expect("files array");
    let entry = files
        .iter()
        .find(|f| f["path"] == json!("tracked.txt"))
        .expect("tracked.txt in status after unstageHunk");
    assert_eq!(entry["staged"], json!(false));

    // No lock file present ⇒ `{ removed: false }`.
    let resp = wss_rpc(
        &mut ws,
        7,
        "git.removeLockFile",
        json!({ "workspaceId": ws_id }),
    )
    .await;
    assert_eq!(resp["result"]["ok"], json!(true));
    assert_eq!(resp["result"]["removed"], json!(false));

    // git.agentCommit fallback semantics over WSS (monorepo#939, §5.6):
    //
    // (1) agent-initiated (no `files`, no `userRequested`): the transport
    // path carries no agent context, so attribution is impossible — refuse
    // with -32603 rather than sweeping the dirty worktree.
    let resp = wss_rpc(
        &mut ws,
        20,
        "git.agentCommit",
        json!({ "workspaceId": ws_id, "message": "agent sweep" }),
    )
    .await;
    assert!(resp.get("result").is_none(), "no-agentId refusal: {resp}");
    assert_eq!(resp["error"]["code"], json!(-32603), "{resp}");
    assert!(
        resp["error"]["data"]
            .as_str()
            .unwrap_or_default()
            .contains("cannot be attributed"),
        "refusal names the attribution gap: {resp}"
    );
    let status = wss_rpc(&mut ws, 21, "git.status", json!({ "workspaceId": ws_id })).await;
    let files = status["result"]["files"].as_array().expect("files array");
    assert!(
        files.iter().any(|f| f["path"] == json!("tracked.txt")),
        "refusal left the dirty worktree untouched: {files:?}"
    );

    // (2) userRequested with no `files` commits only the already-staged
    // paths — a second unstaged file stays dirty in the worktree.
    std::fs::write(wt.join("unstaged.txt"), "left behind\n").unwrap();
    let resp = wss_rpc(
        &mut ws,
        22,
        "git.stage",
        json!({ "workspaceId": ws_id, "paths": ["tracked.txt"] }),
    )
    .await;
    assert_eq!(resp["result"]["ok"], json!(true), "stage: {resp}");
    let resp = wss_rpc(
        &mut ws,
        23,
        "git.agentCommit",
        json!({
            "workspaceId": ws_id,
            "message": "user checkpoint",
            "userRequested": true,
        }),
    )
    .await;
    assert_eq!(resp["result"]["ok"], json!(true), "agentCommit: {resp}");
    assert_eq!(resp["result"]["files"], json!(["tracked.txt"]));
    assert_eq!(resp["result"]["fileCount"], json!(1));
    assert_eq!(resp["result"]["hash"].as_str().expect("hash").len(), 40);
    let status = wss_rpc(&mut ws, 24, "git.status", json!({ "workspaceId": ws_id })).await;
    let files = status["result"]["files"].as_array().expect("files array");
    assert!(
        files.iter().any(|f| f["path"] == json!("unstaged.txt")),
        "unstaged file survives the userRequested commit: {files:?}"
    );
    assert!(
        files.iter().all(|f| f["path"] != json!("tracked.txt")),
        "staged file was committed: {files:?}"
    );

    // Plant a lock file inside the linked worktree's git dir. Workspace
    // worktrees are `git worktree add`-style linked, so `.git` is a file
    // pointing at the real gitdir; resolve it before writing.
    let git_pointer = std::fs::read_to_string(wt.join(".git")).expect("read .git file");
    let gitdir = git_pointer
        .lines()
        .find_map(|l| l.strip_prefix("gitdir: ").map(PathBuf::from))
        .expect("gitdir line in linked worktree pointer");
    let lock = gitdir.join("index.lock");
    std::fs::write(&lock, b"pid").unwrap();

    let resp = wss_rpc(
        &mut ws,
        8,
        "git.removeLockFile",
        json!({ "workspaceId": ws_id }),
    )
    .await;
    assert_eq!(resp["result"]["ok"], json!(true));
    assert_eq!(resp["result"]["removed"], json!(true));
    assert!(!lock.exists(), "index.lock deleted from linked gitdir");

    let _ = std::fs::remove_dir_all(&root);
    drop(daemon);
}

/// WSS counterpart of the UDS submodule-gitlink guard (monorepo#1714 follow-up):
/// over the production TLS/WebSocket envelope, `git.agentCommit`'s explicit
/// `files` list refuses a path strictly inside a registered submodule with the
/// docs/protocol/09-error-codes.md §9 `-32603` error naming the path and its containing submodule,
/// and no commit lands. Both the relative and the in-worktree absolute spelling
/// are refused — the absolute form used to slip past the guard and be
/// normalized into the submodule-internal relative path on the way to the index.
#[tokio::test]
async fn git_agent_commit_rejects_submodule_internal_file_over_wss() {
    if !gate() {
        return;
    }
    let root = scratch_dir("root-submodule");
    let (daemon, port, cfg) = boot(&root).await;
    let repo = make_source_repo_with_submodule(&daemon.scratch);

    let mut ws = connect_ws(port, cfg).await;
    let (ws_id, wt) = create_workspace(&mut ws, &repo, "Git Write E2E — submodule guard").await;

    let head_before = run_git(&["rev-parse", "HEAD"], &wt);

    // Dirty a file strictly inside the submodule's own worktree. A linked
    // worktree does not check out submodules, so materialise the directory
    // first — the guard is a pathspec check, not an on-disk probe.
    let sub = wt.join("sub");
    std::fs::create_dir_all(&sub).unwrap();
    std::fs::write(sub.join("inner.txt"), "changed inside sub\n").unwrap();

    for (id, spelling) in [
        (30i64, "sub/inner.txt".to_string()),
        (31, sub.join("inner.txt").to_string_lossy().to_string()),
    ] {
        let resp = wss_rpc(
            &mut ws,
            id,
            "git.agentCommit",
            json!({
                "workspaceId": ws_id,
                "message": "flatten sub",
                "files": [spelling],
                "userRequested": true,
            }),
        )
        .await;
        assert_eq!(resp["jsonrpc"], json!("2.0"));
        assert_eq!(resp["id"], json!(id));
        assert!(resp.get("result").is_none(), "{spelling}: {resp}");
        assert_eq!(resp["error"]["code"], json!(-32603), "{spelling}: {resp}");
        let msg = resp["error"]["data"]
            .as_str()
            .or_else(|| resp["error"]["message"].as_str())
            .unwrap_or_default();
        assert!(
            msg.contains("inner.txt"),
            "{spelling}: names the path: {resp}"
        );
        assert!(
            msg.contains("submodule 'sub'"),
            "{spelling}: names the containing submodule: {resp}"
        );
    }

    // No commit landed on the superproject, and the gitlink survived.
    assert_eq!(
        run_git(&["rev-parse", "HEAD"], &wt),
        head_before,
        "no commit must have been made"
    );
    let ls = run_git(&["ls-files", "-s", "sub"], &wt);
    assert!(
        ls.starts_with("160000"),
        "gitlink entry intact, got: {ls:?}"
    );

    let _ = std::fs::remove_dir_all(&root);
    drop(daemon);
}

/// WSS counterpart of the discard submodule-gitlink guard (monorepo#1733):
/// over the production TLS/WebSocket envelope, `git.discard` refuses a path
/// strictly inside a registered submodule with the docs/protocol/09-error-codes.md §9 `-32603`
/// error naming the path and its containing submodule — instead of treating it
/// as untracked in the superproject and unlinking it. Both the relative and the
/// in-worktree absolute spelling are refused, the file survives on disk, and
/// the gitlink stays a `160000` entry.
#[tokio::test]
async fn git_discard_rejects_submodule_internal_path_over_wss() {
    if !gate() {
        return;
    }
    let root = scratch_dir("root-submodule-discard");
    let (daemon, port, cfg) = boot(&root).await;
    let repo = make_source_repo_with_submodule(&daemon.scratch);

    let mut ws = connect_ws(port, cfg).await;
    let (ws_id, wt) = create_workspace(&mut ws, &repo, "Git Write E2E — discard guard").await;

    // A linked worktree does not check out submodules, so materialise the
    // directory and its working-copy edit first — the guard is a pathspec
    // check, not an on-disk probe.
    let sub = wt.join("sub");
    std::fs::create_dir_all(&sub).unwrap();
    let inner = sub.join("inner.txt");
    std::fs::write(&inner, "uncommitted inside sub\n").unwrap();

    for (id, spelling) in [
        (40i64, "sub/inner.txt".to_string()),
        (41, inner.to_string_lossy().to_string()),
    ] {
        let resp = wss_rpc(
            &mut ws,
            id,
            "git.discard",
            json!({ "workspaceId": ws_id, "paths": [spelling] }),
        )
        .await;
        assert_eq!(resp["jsonrpc"], json!("2.0"));
        assert_eq!(resp["id"], json!(id));
        assert!(resp.get("result").is_none(), "{spelling}: {resp}");
        assert_eq!(resp["error"]["code"], json!(-32603), "{spelling}: {resp}");
        let msg = resp["error"]["data"]
            .as_str()
            .or_else(|| resp["error"]["message"].as_str())
            .unwrap_or_default();
        assert!(
            msg.contains("inner.txt"),
            "{spelling}: names the path: {resp}"
        );
        assert!(
            msg.contains("submodule 'sub'"),
            "{spelling}: names the containing submodule: {resp}"
        );
        assert_eq!(
            std::fs::read_to_string(&inner).unwrap(),
            "uncommitted inside sub\n",
            "{spelling}: submodule working-copy file must survive"
        );
    }

    let ls = run_git(&["ls-files", "-s", "sub"], &wt);
    assert!(
        ls.starts_with("160000"),
        "gitlink entry intact, got: {ls:?}"
    );

    let _ = std::fs::remove_dir_all(&root);
    drop(daemon);
}
