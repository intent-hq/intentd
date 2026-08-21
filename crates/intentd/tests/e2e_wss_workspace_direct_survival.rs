//! WSS end-to-end coverage for intent-hq/monorepo#1560: duplicating a
//! standalone (`direct`) workspace yields a **self-contained** repository that
//! outlives the original.
//!
//! Drives the real pinned-TLS WebSocket against a live `intentd serve` and
//! asserts that `workspace.duplicate` of a `direct` source:
//!
//! - reports `checkoutMode: "cow"` or `"direct"` — never `"worktree"`, which
//!   would root the duplicate's checkout inside the source workspace,
//! - materialises a real `.git` **directory** (not a linked-worktree gitfile)
//!   whose `repositoryPath` points at the duplicate's own checkout,
//! - keeps `git.*` RPCs working after the original workspace is deleted and
//!   its repository directory is removed from disk.
//!
//! The `direct` outcome is forced deterministically via the
//! `INTENT_GIT_TEST_COW_CLONE_UNSUPPORTED_PATH` daemon seam, so the test does
//! not depend on host `CoW` support: on a non-CoW filesystem the duplicate's
//! probe already selects `direct`, and on a `CoW` filesystem the seam makes the
//! clone itself report Unsupported, exercising the standalone-source retry arm
//! that falls back to a plain local clone (never a linked worktree).
//!
//! Gated on `git` on PATH.

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

const TOKEN: &str = "dededededededededededededededededededededededededededededededede";

/// Substring of the source repository path the CoW-clone seam treats as
/// unsupported (see the module docs).
const UNSUPPORTED_NEEDLE: &str = "direct-src";

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
    let dir = PathBuf::from("/tmp").join(format!("itd-wss-dup-{prefix}-{}", &id[..8]));
    std::fs::create_dir_all(&dir).expect("mkdir scratch dir");
    dir
}

fn spawn_serve(data_dir: &Path, env: &[(&str, &str)]) -> Child {
    let log = std::fs::File::create(data_dir.join("daemon.log")).expect("create daemon log");
    common::enable_ws_api(data_dir);
    let mut cmd = Command::new(env!("CARGO_BIN_EXE_intentd"));
    cmd.arg("serve")
        .env("INTENTD_DATA_DIR", data_dir)
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

/// Send one JSON-RPC frame and return the whole response envelope.
async fn wss_call<S>(ws: &mut WebSocketStream<S>, id: i64, method: &str, params: Value) -> Value
where
    S: tokio::io::AsyncRead + tokio::io::AsyncWrite + Unpin,
{
    let frame = json!({ "jsonrpc": "2.0", "id": id, "method": method, "params": params });
    ws.send(Message::Text(frame.to_string().into()))
        .await
        .expect("send rpc frame");
    loop {
        let next = timeout(Duration::from_secs(20), ws.next())
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

/// [`wss_call`] asserting the documented success envelope (`jsonrpc`, echoed
/// `id`, no `error`) and returning `result`.
async fn wss_rpc<S>(ws: &mut WebSocketStream<S>, id: i64, method: &str, params: Value) -> Value
where
    S: tokio::io::AsyncRead + tokio::io::AsyncWrite + Unpin,
{
    let v = wss_call(ws, id, method, params).await;
    assert_eq!(v["jsonrpc"], json!("2.0"), "rpc {method} envelope: {v}");
    assert_eq!(v["id"], json!(id), "rpc {method} echoes id: {v}");
    assert!(v.get("error").is_none(), "rpc {method} errored: {v}");
    v["result"].clone()
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

/// Boot a live daemon with the workspaces root at `workspaces_root` and the
/// CoW-clone seam armed, returning the WSS port + pinned TLS config.
async fn boot(workspaces_root: &Path) -> (Daemon, u16, Arc<ClientConfig>) {
    let data_dir = scratch_dir("data");
    let scratch = scratch_dir("scratch");
    let root_s = workspaces_root.to_string_lossy().to_string();
    let child = spawn_serve(
        &data_dir,
        &[
            ("INTENTD_AUTH_TOKEN", TOKEN),
            ("INTENTD_TCP_PORT", "0"),
            ("INTENTD_WORKSPACES_DIR", &root_s),
            (
                "INTENT_GIT_TEST_COW_CLONE_UNSUPPORTED_PATH",
                UNSUPPORTED_NEEDLE,
            ),
        ],
    );
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

/// `workspace.duplicate` of a standalone `direct` workspace produces a
/// self-contained repository (monorepo#1560): the duplicate is standalone
/// (`cow`/`direct`, never `worktree`) with a real `.git` directory and a
/// `repositoryPath` pointing at its own checkout, and its `git.*` RPCs keep
/// working after the original workspace is deleted and its repository
/// directory is removed from disk.
#[tokio::test]
async fn workspace_duplicate_of_direct_survives_original_deletion_over_wss() {
    const TEST: &str = "workspace.duplicate direct-survival WSS e2e";
    if !git_gate(TEST) {
        return;
    }
    let root = scratch_dir("droot");
    let (daemon, port, cfg) = boot(&root).await;
    // `isNewRepo` gives a standalone `direct` source: the daemon initializes
    // the folder and the workspace works directly in it (no worktree, no CoW
    // clone). The folder name carries `UNSUPPORTED_NEEDLE`, so a CoW clone OF
    // THIS SOURCE reports Unsupported and the duplicate lands on the plain
    // local-clone (`direct`) arm regardless of host CoW support.
    let source_repo = daemon.scratch.join(UNSUPPORTED_NEEDLE);

    let mut ws = connect_ws(port, cfg).await;
    let created = wss_rpc(
        &mut ws,
        1,
        "workspace.create",
        json!({
            "title": "Direct Survival Source",
            "repositoryPath": source_repo.to_string_lossy(),
            "isNewRepo": true,
            "idempotencyKey": Uuid::new_v4().to_string(),
        }),
    )
    .await;
    let source = &created["workspace"];
    let source_id = source["id"].as_str().expect("source id").to_string();
    assert_eq!(
        source["checkoutMode"],
        json!("direct"),
        "source is a standalone direct workspace: {source}"
    );
    assert_eq!(
        source["worktreePath"].as_str(),
        source["repositoryPath"].as_str(),
        "direct source works in the repository folder itself (worktreePath carries it, intent-hq/monorepo#2611): {source}"
    );
    let source_head = run_git(&["rev-parse", "HEAD"], &source_repo);

    // Duplicate: standalone, never a linked worktree rooted in the source.
    let dup = wss_rpc(
        &mut ws,
        2,
        "workspace.duplicate",
        json!({ "workspaceId": source_id }),
    )
    .await;
    let workspace = &dup["workspace"];
    let dup_id = workspace["id"].as_str().expect("dup id").to_string();
    assert_ne!(dup_id, source_id, "duplicate mints a fresh id");
    let mode = workspace["checkoutMode"].as_str().unwrap_or_default();
    assert!(
        matches!(mode, "cow" | "direct"),
        "duplicate of a standalone source is standalone, never a worktree: {workspace}"
    );
    assert_eq!(
        mode, "direct",
        "the CoW-clone seam forces the plain local-clone arm: {workspace}"
    );
    let wt = workspace["worktreePath"].as_str().expect("worktreePath");
    let wt_path = PathBuf::from(wt);
    assert_eq!(
        wt_path,
        root.join(&dup_id).join(UNSUPPORTED_NEEDLE),
        "duplicate checkout lives at <root>/<newId>/<repo-slug>"
    );
    assert_eq!(
        workspace["repositoryPath"].as_str(),
        Some(wt),
        "self-contained duplicate: repositoryPath is its own checkout, not the source's dir: {workspace}"
    );
    assert!(
        wt_path.join(".git").is_dir(),
        "duplicate is a standalone repository (real .git dir, not a worktree gitfile)"
    );
    assert_eq!(run_git(&["rev-parse", "HEAD"], &wt_path), source_head);

    // Baseline: git RPCs resolve against the duplicate's own checkout.
    let branch = workspace["branch"].as_str().expect("branch").to_string();
    let status = wss_rpc(&mut ws, 3, "git.status", json!({ "workspaceId": dup_id })).await;
    assert_eq!(status["branch"], json!(branch), "git.status: {status}");

    // Delete the original workspace, then remove its repository directory —
    // the duplicate must not hold any live reference into it.
    let deleted = wss_rpc(
        &mut ws,
        4,
        "workspace.delete",
        json!({ "workspaceId": source_id }),
    )
    .await;
    assert_eq!(deleted, json!({ "success": true }));
    // Fast-ack: cleanup runs in the background; poll for the row to go.
    for _ in 0..50 {
        let list = wss_rpc(&mut ws, 5, "workspace.list", json!({})).await;
        let gone = list["workspaces"]
            .as_array()
            .expect("workspaces array")
            .iter()
            .all(|w| w["id"].as_str() != Some(source_id.as_str()));
        if gone {
            break;
        }
        tokio::time::sleep(Duration::from_millis(100)).await;
    }
    std::fs::remove_dir_all(&source_repo).expect("remove the original repository directory");
    assert!(!source_repo.exists());

    // The duplicate is untouched and still fully functional over the wire.
    assert!(
        wt_path.join(".git").is_dir(),
        "duplicate checkout survives the original's deletion"
    );
    let status = wss_rpc(&mut ws, 6, "git.status", json!({ "workspaceId": dup_id })).await;
    assert_eq!(
        status["branch"],
        json!(branch),
        "git.status still resolves after the original is gone: {status}"
    );
    assert_eq!(status["hasUncommittedChanges"], json!(false));
    let commits = wss_rpc(
        &mut ws,
        7,
        "git.commits",
        json!({ "workspaceId": dup_id, "page": { "limit": 10 } }),
    )
    .await;
    assert!(
        !commits["items"].as_array().expect("items").is_empty(),
        "history is self-contained in the duplicate: {commits}"
    );
    // No config value references the deleted source path.
    assert_eq!(run_git(&["remote"], &wt_path), "");
    assert_eq!(run_git(&["rev-parse", "HEAD"], &wt_path), source_head);

    let _ = std::fs::remove_dir_all(&root);
    drop(daemon);
}
