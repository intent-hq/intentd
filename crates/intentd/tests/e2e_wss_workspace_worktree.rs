//! WSS end-to-end `workspace.create` worktree provisioning: drives the real
//! pinned-TLS WebSocket against a live `intentd serve` (WSS listener enabled via config) and
//! asserts that creating a workspace off a local git repository provisions a
//! linked worktree (docs/protocol/methods/workspace.md §5.1) — the returned `workspace.worktreePath`
//! exists on disk, is a git worktree checked out on the workspace branch, and
//! `baseCommitSha` records the base tip. Regression for agents spawning in a
//! temp dir because `workspace.create` persisted a row without a checkout.
//!
//! Uses a tiny local repository as the source so the test never touches the
//! network. Gated on `git` being on PATH; skips cleanly otherwise.

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
    let dir = PathBuf::from("/tmp").join(format!("itd-wss-wt-{prefix}-{}", &id[..8]));
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
            eprintln!("skipping workspace.create worktree WSS e2e: git not on PATH");
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
    let port = status["result"]["port"].as_u64().expect("port") as u16;
    let fingerprint = status["result"]["fingerprint"]
        .as_str()
        .expect("fingerprint")
        .to_string();
    (daemon, port, client_config(&fingerprint))
}

/// `workspace.create` with a local `repositoryPath` + `baseRef` provisions a
/// linked worktree: the result's `worktreePath` is on disk under
/// `<root>/<workspaceId>/<repo-slug>`, checked out on the workspace branch at
/// the base tip, and `baseCommitSha` records that SHA. The auto-generated
/// branch is a friendly slug derived from `initialAgent.prompt` (never the
/// raw workspace UUID).
#[tokio::test]
async fn workspace_create_provisions_worktree_over_wss() {
    if !gate() {
        return;
    }
    let root = scratch_dir("root");
    let (daemon, port, cfg) = boot(&root).await;
    let (repo, head_sha) = make_source_repo(&daemon.scratch);

    let mut ws = connect_ws(port, cfg).await;
    let result = wss_rpc(
        &mut ws,
        2,
        "workspace.create",
        json!({
            "title": "Worktree E2E",
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
        wt,
        root.join(id).join("source-repo").to_string_lossy().as_ref(),
        "worktree lives at <root>/<workspaceId>/<repo-slug>"
    );
    assert_eq!(workspace["baseCommitSha"], json!(head_sha));

    // Branch naming: prompt-derived slug ("fix the auth flow" → `auth-fix`,
    // TS `generateLocalSlug` parity). The workspace id itself is now the
    // same slug (human-readable directory names replace opaque UUIDs).
    let branch = workspace["branch"].as_str().expect("branch");
    assert_eq!(branch, "auth-fix");
    assert_eq!(id, "auth-fix", "workspace id must be a slug, not a UUID");

    // The worktree is a real checkout on the workspace branch at the base tip.
    let wt_path = PathBuf::from(wt);
    assert!(wt_path.join("README.md").exists(), "checkout populated");
    assert_eq!(
        run_git(&["rev-parse", "--is-inside-work-tree"], &wt_path),
        "true"
    );
    assert_eq!(
        run_git(&["rev-parse", "--abbrev-ref", "HEAD"], &wt_path),
        workspace["branch"].as_str().expect("branch"),
    );
    assert_eq!(run_git(&["rev-parse", "HEAD"], &wt_path), head_sha);

    let _ = std::fs::remove_dir_all(&root);
    drop(daemon);
}

/// `workspace.create` with a local `repositoryPath` and no caller-supplied
/// `repositoryName` derives the name from the path basename (`known_repo_name`
/// fallback parity) and round-trips it: both the create result and a
/// subsequent `workspace.list` carry `repositoryName`, so FE recent-repos
/// surfaces populate for locally-created workspaces.
#[tokio::test]
async fn workspace_create_derives_repository_name_over_wss() {
    if !gate() {
        return;
    }
    let root = scratch_dir("reponame");
    let (daemon, port, cfg) = boot(&root).await;
    let (repo, _head_sha) = make_source_repo(&daemon.scratch);

    let mut ws = connect_ws(port, cfg).await;
    let created = wss_rpc(
        &mut ws,
        2,
        "workspace.create",
        json!({
            "title": "Repo Name E2E",
            "repositoryPath": repo.to_string_lossy(),
            "baseRef": "main",
            "idempotencyKey": Uuid::new_v4().to_string(),
        }),
    )
    .await;
    let id = created["workspace"]["id"].as_str().expect("id").to_string();
    assert_eq!(
        created["workspace"]["repositoryName"],
        json!("source-repo"),
        "create result carries the basename-derived repositoryName"
    );

    let listed = wss_rpc(&mut ws, 3, "workspace.list", json!({})).await;
    let row = listed["workspaces"]
        .as_array()
        .expect("workspaces array")
        .iter()
        .find(|w| w["id"] == json!(id))
        .expect("created workspace listed");
    assert_eq!(
        row["repositoryName"],
        json!("source-repo"),
        "workspace.list round-trips the derived repositoryName"
    );

    let _ = std::fs::remove_dir_all(&root);
    drop(daemon);
}

/// `workspace.delete` cleans up the provisioned checkout over the wire: the
/// worktree directory (and its `<root>/<workspaceId>` parent) is removed, the
/// registration is pruned from the source repo, and the auto-generated
/// workspace branch is deleted — while an explicitly-named branch survives its
/// workspace (TS `removeGitWorktree` guard parity).
#[tokio::test]
async fn workspace_delete_cleans_worktree_and_branch_over_wss() {
    if !gate() {
        return;
    }
    let root = scratch_dir("delroot");
    let (daemon, port, cfg) = boot(&root).await;
    let (repo, _head_sha) = make_source_repo(&daemon.scratch);
    let mut ws = connect_ws(port, cfg).await;

    // Auto-generated branch: deleted with the workspace.
    let created = wss_rpc(
        &mut ws,
        2,
        "workspace.create",
        json!({
            "title": "Delete E2E",
            "repositoryPath": repo.to_string_lossy(),
            "repositoryName": "source-repo",
            "baseRef": "main",
            "initialAgent": { "prompt": "fix the auth flow" },
            "idempotencyKey": Uuid::new_v4().to_string(),
        }),
    )
    .await;
    let id = created["workspace"]["id"].as_str().expect("id").to_string();
    let branch = created["workspace"]["branch"]
        .as_str()
        .expect("branch")
        .to_string();
    let wt = PathBuf::from(
        created["workspace"]["worktreePath"]
            .as_str()
            .expect("worktreePath"),
    );
    assert!(wt.exists());

    let deleted = wss_rpc(&mut ws, 3, "workspace.delete", json!({ "workspaceId": id })).await;
    assert_eq!(deleted, json!({ "success": true }));
    // Fast-ack: the response returns immediately while the worktree cleanup
    // runs in the background. Poll for the expected final state.
    for _ in 0..30 {
        let branches = run_git(&["branch", "--list", &branch], &repo);
        if !wt.exists() && !root.join(&id).exists() && branches.is_empty() {
            break;
        }
        tokio::time::sleep(tokio::time::Duration::from_millis(100)).await;
    }
    assert!(!wt.exists(), "worktree directory removed");
    assert!(
        !root.join(&id).exists(),
        "empty <root>/<workspaceId> parent removed"
    );
    let branches = run_git(&["branch", "--list", &branch], &repo);
    assert!(
        branches.is_empty(),
        "auto-generated branch deleted, got: {branches}"
    );

    // Explicit branch: the worktree goes, the branch stays.
    let created = wss_rpc(
        &mut ws,
        4,
        "workspace.create",
        json!({
            "title": "Delete E2E explicit",
            "repositoryPath": repo.to_string_lossy(),
            "repositoryName": "source-repo",
            "baseRef": "main",
            "branch": "keep-me",
            "idempotencyKey": Uuid::new_v4().to_string(),
        }),
    )
    .await;
    let id = created["workspace"]["id"].as_str().expect("id").to_string();
    let wt = PathBuf::from(
        created["workspace"]["worktreePath"]
            .as_str()
            .expect("worktreePath"),
    );
    let deleted = wss_rpc(&mut ws, 5, "workspace.delete", json!({ "workspaceId": id })).await;
    assert_eq!(deleted, json!({ "success": true }));
    // Fast-ack: poll for worktree removal.
    for _ in 0..30 {
        if !wt.exists() {
            break;
        }
        tokio::time::sleep(tokio::time::Duration::from_millis(100)).await;
    }
    assert!(!wt.exists(), "worktree directory removed");
    let branches = run_git(&["branch", "--list", "keep-me"], &repo);
    assert!(
        branches.contains("keep-me"),
        "explicit branch preserved, got: {branches}"
    );

    let _ = std::fs::remove_dir_all(&root);
    drop(daemon);
}

/// `workspace.delete` is idempotent over the wire: a second delete of the
/// same id after the row is gone succeeds with `{ success: true }` instead of
/// bubbling up the store's `NotFound` — the renderer retries the daemon after
/// its own local cleanup and must not surface `"Failed to delete space"`.
#[tokio::test]
async fn workspace_delete_is_idempotent_over_wss() {
    if !gate() {
        return;
    }
    let root = scratch_dir("idem");
    let (daemon, port, cfg) = boot(&root).await;
    let (repo, _head_sha) = make_source_repo(&daemon.scratch);
    let mut ws = connect_ws(port, cfg).await;

    let created = wss_rpc(
        &mut ws,
        2,
        "workspace.create",
        json!({
            "title": "Idempotent delete",
            "repositoryPath": repo.to_string_lossy(),
            "repositoryName": "source-repo",
            "baseRef": "main",
            "initialAgent": { "prompt": "fix the auth flow" },
            "idempotencyKey": Uuid::new_v4().to_string(),
        }),
    )
    .await;
    let id = created["workspace"]["id"].as_str().expect("id").to_string();

    let first = wss_rpc(&mut ws, 3, "workspace.delete", json!({ "workspaceId": id })).await;
    assert_eq!(first, json!({ "success": true }));
    // Fast-ack: poll for background cleanup to complete.
    for _ in 0..30 {
        if !root.join(&id).exists() {
            break;
        }
        tokio::time::sleep(tokio::time::Duration::from_millis(100)).await;
    }
    assert!(!root.join(&id).exists(), "workspace directory removed");

    // Row is gone; a repeat delete must still succeed.
    let second = wss_rpc(&mut ws, 4, "workspace.delete", json!({ "workspaceId": id })).await;
    assert_eq!(second, json!({ "success": true }));

    let _ = std::fs::remove_dir_all(&root);
    drop(daemon);
}

/// `workspace.delete` sweeps the whole `<workspaces_root>/<id>/` directory —
/// not just the git worktree and the `.workspace/` metadata dir. The FE
/// writes ancillary files (agent sessions, event caches, etc.) into the same
/// workspace directory, and leaving them behind re-surfaces the deleted id in
/// `FileSystemWorkspaceRepository.findAll`'s scan (ENOENT WARN spam every PR
/// refresh tick).
#[tokio::test]
async fn workspace_delete_sweeps_residual_workspace_directory_over_wss() {
    if !gate() {
        return;
    }
    let root = scratch_dir("resid");
    let (daemon, port, cfg) = boot(&root).await;
    let (repo, _head_sha) = make_source_repo(&daemon.scratch);
    let mut ws = connect_ws(port, cfg).await;

    let created = wss_rpc(
        &mut ws,
        2,
        "workspace.create",
        json!({
            "title": "Residual sweep",
            "repositoryPath": repo.to_string_lossy(),
            "repositoryName": "source-repo",
            "baseRef": "main",
            "initialAgent": { "prompt": "fix the auth flow" },
            "idempotencyKey": Uuid::new_v4().to_string(),
        }),
    )
    .await;
    let id = created["workspace"]["id"].as_str().expect("id").to_string();
    let ws_dir = root.join(&id);
    // Drop a stray file into `<root>/<id>/` alongside the worktree and
    // `.workspace/`; the pre-fix `remove_dir` (empty-only) cleanup left this
    // behind and the id kept re-appearing in FE scans.
    std::fs::write(ws_dir.join("residual.log"), b"leftover renderer artefact\n").unwrap();

    let deleted = wss_rpc(&mut ws, 3, "workspace.delete", json!({ "workspaceId": id })).await;
    assert_eq!(deleted, json!({ "success": true }));
    // Fast-ack: poll for background cleanup to complete.
    for _ in 0..30 {
        if !ws_dir.exists() {
            break;
        }
        tokio::time::sleep(tokio::time::Duration::from_millis(100)).await;
    }
    assert!(
        !ws_dir.exists(),
        "<root>/<id>/ (with residual content) fully removed"
    );

    let _ = std::fs::remove_dir_all(&root);
    drop(daemon);
}

/// `workspace.delete` cleans up an orphan `<workspaces_root>/<id>/` directory
/// that the daemon has no DB row for (legacy pre-daemon workspaces with a
/// UUID directory and no `.workspace/workspace.json`). The RPC must succeed
/// and remove the directory so the FE stops warning about it on every scan.
#[tokio::test]
async fn workspace_delete_cleans_orphan_directory_over_wss() {
    if !gate() {
        return;
    }
    let root = scratch_dir("orphan");
    let (daemon, port, cfg) = boot(&root).await;
    let mut ws = connect_ws(port, cfg).await;

    // Fabricate a legacy directory (UUID name, no metadata) directly on disk;
    // the daemon has no row for it.
    let id = format!("7d274735-{}", &Uuid::new_v4().simple().to_string()[..8]);
    let orphan = root.join(&id);
    std::fs::create_dir_all(orphan.join("some-repo")).unwrap();
    std::fs::write(orphan.join("some-repo").join("README.md"), b"legacy\n").unwrap();
    assert!(orphan.exists());

    let deleted = wss_rpc(&mut ws, 2, "workspace.delete", json!({ "workspaceId": id })).await;
    assert_eq!(deleted, json!({ "success": true }));
    // Fast-ack: poll for background cleanup to complete.
    for _ in 0..30 {
        if !orphan.exists() {
            break;
        }
        tokio::time::sleep(tokio::time::Duration::from_millis(100)).await;
    }
    assert!(!orphan.exists(), "orphan workspace directory removed");

    let _ = std::fs::remove_dir_all(&root);
    drop(daemon);
}

/// `workspace.create` over the real WSS transport with an explicit empty
/// `title` persists `""` (reference parity with `workspace.service`
/// `title: request.title || ''`) rather than seeding the slug id. A
/// follow-up `workspace.list` round-trips that empty title so FE reads see
/// the same shape (rendered as "Untitled"). Regression for the slug-seeded
/// title that broke Untitled parity on the wire.
#[tokio::test]
async fn workspace_create_stores_empty_title_when_title_empty_over_wss() {
    if !gate() {
        return;
    }
    let root = scratch_dir("titleempty");
    let (daemon, port, cfg) = boot(&root).await;
    let (repo, _head_sha) = make_source_repo(&daemon.scratch);

    let mut ws = connect_ws(port, cfg).await;
    let created = wss_rpc(
        &mut ws,
        2,
        "workspace.create",
        json!({
            "title": "",
            "repositoryPath": repo.to_string_lossy(),
            "repositoryName": "source-repo",
            "baseRef": "main",
            "initialAgent": { "prompt": "fix the auth flow" },
            "idempotencyKey": Uuid::new_v4().to_string(),
        }),
    )
    .await;
    let id = created["workspace"]["id"].as_str().expect("id").to_string();
    assert_eq!(id, "auth-fix", "workspace id is the prompt-derived slug");
    assert_eq!(
        created["workspace"]["title"],
        json!(""),
        "empty title stored verbatim on the wire (Untitled parity)"
    );

    let listed = wss_rpc(&mut ws, 3, "workspace.list", json!({})).await;
    let row = listed["workspaces"]
        .as_array()
        .expect("workspaces array")
        .iter()
        .find(|w| w["id"] == json!(id))
        .expect("created workspace listed");
    assert_eq!(
        row["title"],
        json!(""),
        "workspace.list round-trips the empty title"
    );

    let _ = std::fs::remove_dir_all(&root);
    drop(daemon);
}

/// `workspace.create` over WSS with the `title` field entirely omitted from
/// the `params` object (other fields still present) persists `""` — the
/// reference contract collapses missing and blank titles to the same
/// Untitled shape. Guards the JSON-RPC request path where callers send a
/// `params` object without a `title` key, matching what onboarding sends
/// today.
#[tokio::test]
async fn workspace_create_stores_empty_title_when_title_omitted_over_wss() {
    if !gate() {
        return;
    }
    let root = scratch_dir("titleomit");
    let (daemon, port, cfg) = boot(&root).await;
    let (repo, _head_sha) = make_source_repo(&daemon.scratch);

    let mut ws = connect_ws(port, cfg).await;
    let created = wss_rpc(
        &mut ws,
        2,
        "workspace.create",
        json!({
            "repositoryPath": repo.to_string_lossy(),
            "repositoryName": "source-repo",
            "baseRef": "main",
            "initialAgent": { "prompt": "fix the auth flow" },
            "idempotencyKey": Uuid::new_v4().to_string(),
        }),
    )
    .await;
    let id = created["workspace"]["id"].as_str().expect("id").to_string();
    assert_eq!(
        created["workspace"]["title"],
        json!(""),
        "omitted title persists as empty on the wire"
    );

    let listed = wss_rpc(&mut ws, 3, "workspace.list", json!({})).await;
    let row = listed["workspaces"]
        .as_array()
        .expect("workspaces array")
        .iter()
        .find(|w| w["id"] == json!(id))
        .expect("created workspace listed");
    assert_eq!(
        row["title"],
        json!(""),
        "workspace.list round-trips the omitted-title empty shape"
    );

    let _ = std::fs::remove_dir_all(&root);
    drop(daemon);
}

/// `workspace.duplicate` off a workspace backed by a local git repository
/// provisions a fresh linked worktree for the duplicate (TS
/// `duplicateWorkspace` parity, mirroring the `workspace.create` flow): the
/// returned `worktreePath` lives at `<root>/<newId>/<repo-slug>`, is a real
/// checkout on the duplicate's branch at the source `baseRef` tip, and
/// `baseCommitSha` records that SHA. Regression for the deferred item from PR
/// #127 (`workspace.duplicate` persisted a row without a checkout).
#[tokio::test]
async fn workspace_duplicate_provisions_worktree_over_wss() {
    if !gate() {
        return;
    }
    let root = scratch_dir("dupwt");
    let (daemon, port, cfg) = boot(&root).await;
    let (repo, head_sha) = make_source_repo(&daemon.scratch);
    let mut ws = connect_ws(port, cfg).await;

    // Seed a source workspace with a real worktree so `workspace.duplicate`
    // has repository metadata to clone into the new row.
    let created = wss_rpc(
        &mut ws,
        2,
        "workspace.create",
        json!({
            "title": "Dup Source",
            "repositoryPath": repo.to_string_lossy(),
            "repositoryName": "source-repo",
            "baseRef": "main",
            "initialAgent": { "prompt": "fix the auth flow" },
            "idempotencyKey": Uuid::new_v4().to_string(),
        }),
    )
    .await;
    let source_id = created["workspace"]["id"].as_str().expect("id").to_string();

    let dup = wss_rpc(
        &mut ws,
        3,
        "workspace.duplicate",
        json!({ "workspaceId": source_id }),
    )
    .await;
    let workspace = &dup["workspace"];
    let dup_id = workspace["id"].as_str().expect("dup id");
    assert_ne!(dup_id, source_id, "duplicate must mint a fresh id");
    assert_eq!(workspace["title"], json!("Dup Source (Copy)"));

    // `workspace.duplicate` returns a `Workspace` on the wire, so the
    // backend-authored `lastActivity` (§9.1) must be populated — clients
    // should never see a missing value on this path (parity with
    // `workspace.create`).
    assert!(
        workspace["lastActivity"].is_string(),
        "duplicate must return authoritative lastActivity, got: {}",
        workspace["lastActivity"]
    );

    let wt = workspace["worktreePath"]
        .as_str()
        .expect("worktreePath populated on duplicate");
    assert_eq!(
        wt,
        root.join(dup_id)
            .join("source-repo")
            .to_string_lossy()
            .as_ref(),
        "duplicate worktree lives at <root>/<newId>/<repo-slug>"
    );
    assert_eq!(
        workspace["baseCommitSha"],
        json!(head_sha),
        "duplicate baseCommitSha records the source baseRef tip"
    );

    // The duplicate's worktree is a real checkout on its branch at the base
    // tip. The branch may have gained a `-N` suffix if the raw id collided
    // with a pre-existing branch in the source repo; the check-out branch is
    // whatever the workspace row reports.
    let branch = workspace["branch"]
        .as_str()
        .expect("duplicate branch")
        .to_string();
    let wt_path = PathBuf::from(wt);
    assert!(
        wt_path.join("README.md").exists(),
        "duplicate checkout populated"
    );
    assert_eq!(
        run_git(&["rev-parse", "--is-inside-work-tree"], &wt_path),
        "true"
    );
    assert_eq!(
        run_git(&["rev-parse", "--abbrev-ref", "HEAD"], &wt_path),
        branch
    );
    assert_eq!(run_git(&["rev-parse", "HEAD"], &wt_path), head_sha);

    // Cleaning up the duplicate must sweep its worktree registration so the
    // source repo stays healthy for subsequent tests.
    let _ = wss_rpc(
        &mut ws,
        4,
        "workspace.delete",
        json!({ "workspaceId": dup_id }),
    )
    .await;

    // `workspace.delete` trash-renames the duplicate's checkout asynchronously
    // (`source-repo` → `source-repo.deleting-*` under `<root>/<dup_id>`) and
    // removes it in the background, which can race the sweep below and
    // resurrect entries after `remove_dir_all` (leaves `itd-wss-wt-dupwt-*`
    // residue under /tmp). Wait (bounded) for the async delete to settle, kill
    // the daemon so nothing can recreate files, then sweep with one retry.
    let dup_dir = root.join(dup_id);
    let deadline = tokio::time::Instant::now() + Duration::from_secs(10);
    let mut settled = false;
    while tokio::time::Instant::now() < deadline {
        let busy = std::fs::read_dir(&dup_dir)
            .map(|entries| entries.flatten().next().is_some())
            .unwrap_or(false);
        if !busy {
            settled = true;
            break;
        }
        tokio::time::sleep(Duration::from_millis(50)).await;
    }
    if !settled {
        let remaining: Vec<String> = std::fs::read_dir(&dup_dir)
            .map(|entries| {
                entries
                    .flatten()
                    .map(|e| e.file_name().to_string_lossy().into_owned())
                    .collect()
            })
            .unwrap_or_default();
        eprintln!(
            "dupwt: async workspace.delete did not settle within 10s; {} still contains {remaining:?} — sweep may race and leave itd-wss-wt-dupwt-* residue",
            dup_dir.display()
        );
    }
    drop(daemon);
    let _ = std::fs::remove_dir_all(&root);
    if root.exists() {
        tokio::time::sleep(Duration::from_millis(200)).await;
        let _ = std::fs::remove_dir_all(&root);
    }
}

/// `workspace.duplicate` off a workspace created with `skipIsolation: true`
/// (the canonical name for the pre-CoW `skipWorktree` alias) does NOT
/// provision a worktree for the duplicate — the source configuration flows
/// through verbatim, so a metadata-only source stays metadata-only. Guards the
/// skip-arm on the duplicate's provisioning gate and exercises the new wire
/// name end-to-end (other suites cover the deprecated `skipWorktree` alias).
#[tokio::test]
async fn workspace_duplicate_skips_worktree_when_source_skips() {
    if !gate() {
        return;
    }
    let root = scratch_dir("dupskip");
    let (daemon, port, cfg) = boot(&root).await;
    let (repo, _head_sha) = make_source_repo(&daemon.scratch);
    let mut ws = connect_ws(port, cfg).await;

    let created = wss_rpc(
        &mut ws,
        2,
        "workspace.create",
        json!({
            "title": "Skip Source",
            "repositoryPath": repo.to_string_lossy(),
            "repositoryName": "source-repo",
            "baseRef": "main",
            "skipIsolation": true,
            "idempotencyKey": Uuid::new_v4().to_string(),
        }),
    )
    .await;
    assert!(
        created["workspace"]["worktreePath"].is_null(),
        "source with skipIsolation has no worktree"
    );
    let source_id = created["workspace"]["id"].as_str().expect("id").to_string();

    let dup = wss_rpc(
        &mut ws,
        3,
        "workspace.duplicate",
        json!({ "workspaceId": source_id }),
    )
    .await;
    assert!(
        dup["workspace"]["worktreePath"].is_null(),
        "duplicate inherits skip_worktree and stays worktree-less"
    );
    assert!(
        dup["workspace"]["baseCommitSha"].is_null(),
        "no baseCommitSha when no worktree is provisioned"
    );

    let _ = std::fs::remove_dir_all(&root);
    drop(daemon);
}
