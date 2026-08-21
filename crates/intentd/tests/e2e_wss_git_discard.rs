//! WSS end-to-end `git.discard`: drives the additive `git.discard` method
//! over a real pinned-TLS WebSocket against a live `intentd serve` (WSS listener enabled via
//! config), exercising both the happy path (tracked-file restore + untracked
//! deletion via a single request) and the `-32602` path-safety guard
//! (`..` traversal). Asserts the response envelope shape from
//! `docs/protocol/methods/git.md` §5.6 (`{ ok: true, paths: [...] }`) and the JSON-RPC
//! error envelope shape from `docs/protocol/09-error-codes.md` §9.
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
    let dir = PathBuf::from("/tmp").join(format!("itd-wss-discard-{prefix}-{}", &id[..8]));
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
            eprintln!("skipping git.discard WSS e2e: git not on PATH");
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
    std::fs::write(repo.join("tracked.txt"), "clean\n").unwrap();
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

/// Happy path: `workspace.create` provisions a worktree; the client dirties
/// a tracked file + drops an untracked file; a single `git.discard` call
/// restores the tracked file from the index and unlinks the untracked one.
/// Asserts the response envelope shape from docs/protocol/methods/git.md §5.6 —
/// `{ ok: true, paths: [...] }` echoing the input `paths`.
#[tokio::test]
async fn git_discard_restores_tracked_and_deletes_untracked_over_wss() {
    if !gate() {
        return;
    }
    let root = scratch_dir("root");
    let (daemon, port, cfg) = boot(&root).await;
    let repo = make_source_repo(&daemon.scratch);

    let mut ws = connect_ws(port, cfg).await;

    // Provision a workspace off the source repo — the daemon materialises a
    // linked worktree under `<root>/<workspaceId>/<repo-slug>`.
    let created = wss_rpc(
        &mut ws,
        2,
        "workspace.create",
        json!({
            "title": "Discard E2E",
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

    // Dirty tracked.txt + drop an untracked file next to it.
    std::fs::write(wt.join("tracked.txt"), "dirty\n").unwrap();
    std::fs::write(wt.join("untracked.txt"), "junk\n").unwrap();
    assert!(wt.join("untracked.txt").exists());

    // Single `git.discard` — both paths handled together.
    let resp = wss_rpc(
        &mut ws,
        3,
        "git.discard",
        json!({
            "workspaceId": ws_id,
            "paths": ["tracked.txt", "untracked.txt"],
        }),
    )
    .await;

    // Envelope shape (docs/protocol/05-method-catalog.md §5): { jsonrpc, id, result: { ok, paths } }.
    assert_eq!(resp["jsonrpc"], json!("2.0"));
    assert_eq!(resp["id"], json!(3));
    assert!(resp.get("error").is_none(), "unexpected error: {resp}");
    let result = &resp["result"];
    assert_eq!(result["ok"], json!(true));
    let paths = result["paths"].as_array().expect("paths array");
    assert_eq!(paths.len(), 2, "echoed paths: {result}");
    assert!(paths.iter().any(|p| p == &json!("tracked.txt")));
    assert!(paths.iter().any(|p| p == &json!("untracked.txt")));

    // Observable side-effects: tracked.txt restored, untracked.txt gone.
    assert_eq!(
        std::fs::read_to_string(wt.join("tracked.txt")).unwrap(),
        "clean\n",
        "tracked.txt restored from index",
    );
    assert!(
        !wt.join("untracked.txt").exists(),
        "untracked.txt unlinked from disk",
    );

    let _ = std::fs::remove_dir_all(&root);
    drop(daemon);
}

/// `git.discard` refuses a pathspec containing `..` up-front with `-32602`
/// (JSON-RPC error envelope from docs/protocol/09-error-codes.md §9) and never touches the
/// worktree — regression for the traversal-bypass vector where the OS
/// resolves `..` at unlink time and would unlink a tracked file.
#[tokio::test]
async fn git_discard_refuses_traversal_over_wss() {
    if !gate() {
        return;
    }
    let root = scratch_dir("root-bypass");
    let (daemon, port, cfg) = boot(&root).await;
    let repo = make_source_repo(&daemon.scratch);

    let mut ws = connect_ws(port, cfg).await;
    let created = wss_rpc(
        &mut ws,
        2,
        "workspace.create",
        json!({
            "title": "Discard Bypass E2E",
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

    // Traversal pathspec whose lexical resolution lands back inside the
    // worktree — must be refused BEFORE any filesystem interaction.
    let resp = wss_rpc(
        &mut ws,
        3,
        "git.discard",
        json!({
            "workspaceId": ws_id,
            "paths": ["a/../tracked.txt"],
        }),
    )
    .await;

    // Error envelope shape (docs/protocol/09-error-codes.md §9): { jsonrpc, id, error: { code, message } }.
    assert_eq!(resp["jsonrpc"], json!("2.0"));
    assert_eq!(resp["id"], json!(3));
    assert!(resp.get("result").is_none(), "unexpected result: {resp}");
    assert_eq!(resp["error"]["code"], -32602, "traversal ⇒ -32602: {resp}");
    assert!(
        resp["error"]["message"].is_string(),
        "error carries message: {resp}",
    );

    // The tracked file was never touched — no unlink slipped through.
    assert_eq!(
        std::fs::read_to_string(wt.join("tracked.txt")).unwrap(),
        "clean\n",
        "tracked.txt untouched after refused discard",
    );

    let _ = std::fs::remove_dir_all(&root);
    drop(daemon);
}
