//! WSS end-to-end for `github.branches.listCached` (PROTOCOL §5.27): listing
//! branches from the daemon's local repo cache, with a one-shot `git
//! ls-remote` fallback on a cache miss. Asserts the success envelope from a
//! warm cache (`{ cached: true, source: "cache", branches, defaultBranch }`
//! with sorted names and `HEAD` excluded), the fallback envelope from a cold
//! cache with a reachable remote (`{ cached: false, source: "ls-remote",
//! branches, defaultBranch }`), the graceful cold cache with an unreachable
//! remote (`{ cached: false, branches: [] }`, no `defaultBranch` key), and
//! the `-32602` invalid-params envelope for missing/invalid `owner`/`repo`.
//! Drives a real [`WsApiServer`] over TLS with bearer-token auth and a pinned
//! self-signed fingerprint (the production transport path). The cache is
//! seeded through the production `ensure_cached_repo` path from a local
//! `file://` origin — then its `origin` remote is retargeted at the matching
//! github.com URL, since the reader only serves slots whose recorded origin
//! is `github.com/<owner>/<repo>` — and the ls-remote fallback is pointed at
//! a hermetic `file://` fixture base via `with_branches_ls_remote_base`, so
//! the test never touches the network. Gated on `git` being on PATH; skips
//! cleanly otherwise.

#![cfg(unix)]

mod common;

use std::net::Ipv4Addr;
use std::path::{Path, PathBuf};
use std::process::{Command, Stdio};
use std::sync::{Arc, Mutex};

use futures_util::{SinkExt, StreamExt};
use intent_core::{Result as CoreResult, WorkspaceApi};
use intent_services::{EventBus, Services};
use intent_store::Store;
use intent_transport::{
    ensure_tls_certificate, AsyncTokenStore, TokenStore, WsApiServer, WsOptions,
};
use rustls::client::danger::{HandshakeSignatureValid, ServerCertVerified, ServerCertVerifier};
use rustls::crypto::CryptoProvider;
use rustls::{ClientConfig, DigitallySignedStruct, SignatureScheme};
use rustls_pki_types::{CertificateDer, ServerName, UnixTime};
use serde_json::{json, Value};
use sha2::{Digest, Sha256};
use tokio::net::TcpStream;
use tokio::time::timeout;
use tokio_tungstenite::tungstenite::Message;
use tokio_tungstenite::WebSocketStream;

/// A fixed 64-char hex token (valid shape) shared by server + client.
const TOKEN: &str = "abababababababababababababababababababababababababababababababab";

type TlsWs = WebSocketStream<tokio_rustls::client::TlsStream<TcpStream>>;

struct TempDir(PathBuf);
impl Drop for TempDir {
    fn drop(&mut self) {
        let _ = std::fs::remove_dir_all(&self.0);
    }
}

/// In-memory [`TokenStore`] so tests never touch the real OS keychain.
#[derive(Default)]
struct MemTokenStore(Mutex<Option<String>>);

impl TokenStore for MemTokenStore {
    fn load_token(&self) -> Option<String> {
        self.0.lock().unwrap().clone()
    }
    fn store_token(&self, token: &str) -> CoreResult<()> {
        *self.0.lock().unwrap() = Some(token.to_string());
        Ok(())
    }
}

/// Client cert verifier that pins the server's SHA-256 fingerprint (colon hex)
/// and otherwise validates the handshake signature with the ring provider.
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

/// Skip gate: system `git` must be on PATH (the cache seed shells out).
fn gate() -> bool {
    match Command::new("git")
        .arg("--version")
        .stdout(Stdio::null())
        .stderr(Stdio::null())
        .status()
    {
        Ok(s) if s.success() => true,
        _ => {
            eprintln!("skipping github.branches.listCached WSS e2e: git not on PATH");
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
        .stderr(Stdio::null())
        .output()
        .expect("run git");
    assert!(out.status.success(), "git {args:?} failed");
}

/// Materialise a local origin repo with `main` (default) + `feature-x`
/// branches, to be cloned into the cache over `file://`.
fn make_origin_repo(dir: &Path) -> PathBuf {
    let repo = dir.join("origin-repo");
    std::fs::create_dir_all(&repo).expect("mkdir origin repo");
    run_git(&["init", "-q", "-b", "main"], &repo);
    std::fs::write(repo.join("a.txt"), "one\n").unwrap();
    run_git(&["add", "a.txt"], &repo);
    run_git(&["commit", "-q", "-m", "seed"], &repo);
    run_git(&["branch", "feature-x"], &repo);
    repo
}

fn file_url(path: &Path) -> String {
    format!("file://{}", path.display())
}

struct Fixture {
    _ws: WsApiServer,
    port: u16,
    cfg: Arc<ClientConfig>,
    workspaces_root: PathBuf,
    _dir: TempDir,
}

/// Boot a TLS + bearer-auth WSS listener whose services resolve the repo
/// cache under a hermetic workspaces root, and whose ls-remote fallback
/// targets `file://<dir>/remotes/<owner>/<repo>.git` — hermetic fixtures
/// instead of github.com, so a cold-cache read never leaves the machine.
async fn boot() -> Fixture {
    let short = uuid::Uuid::new_v4().simple().to_string();
    let dir = std::env::temp_dir().join(format!("intentd-gh-brcached-{}", &short[..8]));
    std::fs::create_dir_all(&dir).unwrap();
    let store = Store::open(&dir.join("intentd.db")).await.expect("store");
    let bus = EventBus::new(store.clone());
    let workspaces_root = dir.join("workspaces");
    std::fs::create_dir_all(&workspaces_root).expect("mkdir hermetic root");

    let services = Arc::new(
        Services::new(store)
            .with_workspaces_root(workspaces_root.clone())
            .with_branches_ls_remote_base(file_url(&dir.join("remotes")))
            .with_event_bus(bus.clone()),
    );
    let api: Arc<dyn WorkspaceApi> = services;
    let tls = ensure_tls_certificate(&dir).expect("cert");
    let token_store_inner = Arc::new(MemTokenStore::default());
    token_store_inner.store_token(TOKEN).unwrap();
    let token_store = Arc::new(AsyncTokenStore::new(token_store_inner));
    let opts = WsOptions {
        base_port: 0,
        bind_address: Ipv4Addr::LOCALHOST.into(),
        ..Default::default()
    };
    let ws_srv = WsApiServer::new(api, bus, &tls, &token_store, opts, None).expect("server");
    let cfg = client_config(&tls.fingerprint256);
    let port = ws_srv.start().await.expect("start");
    Fixture {
        _ws: ws_srv,
        port,
        cfg,
        workspaces_root,
        _dir: TempDir(dir),
    }
}

/// Establish an authenticated WSS connection over pinned TLS (token in the
/// query string).
async fn connect(port: u16, cfg: Arc<ClientConfig>) -> TlsWs {
    let url = format!("wss://localhost:{port}/ws?token={TOKEN}");
    common::wss_connect_with_retry(port, cfg, &url).await
}

/// Send a JSON-RPC request and return the full response envelope (success or
/// error) so tests can assert either arm. Asserts the JSON-RPC 2.0 envelope
/// invariants (PROTOCOL §1) on every response: `jsonrpc: "2.0"`, the echoed
/// `id`, and exactly one of `result` / `error`.
async fn wss_rpc_envelope(ws: &mut TlsWs, id: i64, method: &str, params: Value) -> Value {
    let req = json!({ "jsonrpc": "2.0", "id": id, "method": method, "params": params });
    ws.send(Message::Text(req.to_string().into()))
        .await
        .unwrap();
    let v = timeout(common::rpc_read_timeout(), async {
        loop {
            match ws.next().await.unwrap().unwrap() {
                Message::Text(text) => {
                    let v: Value = serde_json::from_str(&text).unwrap();
                    if v.get("id") == Some(&json!(id)) {
                        return v;
                    }
                }
                Message::Ping(p) => {
                    let _ = ws.send(Message::Pong(p)).await;
                }
                Message::Pong(_) => {}
                _ => panic!("unexpected message"),
            }
        }
    })
    .await
    .expect("response timeout");
    assert_eq!(v["jsonrpc"], json!("2.0"), "envelope jsonrpc: {v}");
    assert_eq!(v["id"], json!(id), "envelope id: {v}");
    assert!(
        v.get("result").is_some() ^ v.get("error").is_some(),
        "envelope must carry exactly one of result/error: {v}"
    );
    v
}

async fn wss_rpc(ws: &mut TlsWs, id: i64, method: &str, params: Value) -> Value {
    let v = wss_rpc_envelope(ws, id, method, params).await;
    assert!(v.get("error").is_none(), "rpc {method} errored: {v}");
    v["result"].clone()
}

/// A warm cache: the response is `{ cached: true, branches, defaultBranch }`
/// with sorted branch names, `HEAD` excluded, and the default branch resolved
/// from `origin/HEAD` — all served locally, no network.
#[tokio::test]
async fn list_cached_returns_branches_from_warm_cache() {
    if !gate() {
        return;
    }
    let fx = boot().await;
    let origin = make_origin_repo(&fx.workspaces_root.parent().unwrap().join("seed"));
    let cache_root = fx.workspaces_root.join(".repo-cache");
    let cache_path = intent_git::repo_cache::ensure_cached_repo(
        &cache_root,
        &file_url(&origin),
        "acme",
        "widget",
        None,
    )
    .await
    .expect("seed cache");
    // The reader only serves slots whose recorded `origin` is
    // `github.com/<owner>/<repo>`; retarget the file:// seed's origin.
    run_git(
        &[
            "remote",
            "set-url",
            "origin",
            "https://github.com/acme/widget.git",
        ],
        &cache_path,
    );
    let mut ws = connect(fx.port, fx.cfg.clone()).await;

    let r = wss_rpc(
        &mut ws,
        1,
        "github.branches.listCached",
        json!({ "owner": "acme", "repo": "widget" }),
    )
    .await;
    assert_eq!(r["cached"], json!(true));
    assert_eq!(r["source"], json!("cache"));
    assert_eq!(r["branches"], json!(["feature-x", "main"]));
    assert_eq!(r["defaultBranch"], json!("main"));
}

/// A cold cache with a reachable remote falls back to one `git ls-remote`:
/// `{ cached: false, source: "ls-remote", branches, defaultBranch }` with
/// sorted names and the default branch from the remote's `HEAD` symref.
#[tokio::test]
async fn list_cached_cold_cache_falls_back_to_ls_remote() {
    if !gate() {
        return;
    }
    let fx = boot().await;
    // Materialise the fixture the fallback URL resolves to:
    // `<dir>/remotes/acme/widget.git` (a plain repo works as a file:// remote).
    let remotes = fx.workspaces_root.parent().unwrap().join("remotes");
    let repo = remotes.join("acme").join("widget.git");
    std::fs::create_dir_all(&repo).expect("mkdir fixture remote");
    run_git(&["init", "-q", "-b", "main"], &repo);
    std::fs::write(repo.join("a.txt"), "one\n").unwrap();
    run_git(&["add", "a.txt"], &repo);
    run_git(&["commit", "-q", "-m", "seed"], &repo);
    run_git(&["branch", "feature-x"], &repo);
    let mut ws = connect(fx.port, fx.cfg.clone()).await;

    let r = wss_rpc(
        &mut ws,
        1,
        "github.branches.listCached",
        json!({ "owner": "acme", "repo": "widget" }),
    )
    .await;
    assert_eq!(r["cached"], json!(false));
    assert_eq!(r["source"], json!("ls-remote"));
    assert_eq!(r["branches"], json!(["feature-x", "main"]));
    assert_eq!(r["defaultBranch"], json!("main"));
}

/// A cold cache whose remote is also unreachable stays the graceful
/// `{ cached: false, branches: [] }` with no `defaultBranch` key — never an
/// error.
#[tokio::test]
async fn list_cached_cold_cache_is_graceful() {
    if !gate() {
        return;
    }
    let fx = boot().await;
    let mut ws = connect(fx.port, fx.cfg.clone()).await;

    let r = wss_rpc(
        &mut ws,
        1,
        "github.branches.listCached",
        json!({ "owner": "acme", "repo": "widget" }),
    )
    .await;
    assert_eq!(r["cached"], json!(false));
    assert_eq!(r["branches"], json!([]));
    assert!(
        r.get("defaultBranch").is_none(),
        "cold cache must omit defaultBranch: {r}"
    );
    assert!(
        r.get("source").is_none(),
        "failed fallback must omit source: {r}"
    );
}

/// Missing or traversal-shaped params fail with the JSON-RPC `-32602`
/// invalid-params envelope.
#[tokio::test]
async fn list_cached_invalid_params_fail_with_32602() {
    let fx = boot().await;
    let mut ws = connect(fx.port, fx.cfg.clone()).await;

    // Missing repo.
    let env = wss_rpc_envelope(
        &mut ws,
        1,
        "github.branches.listCached",
        json!({ "owner": "acme" }),
    )
    .await;
    assert!(env.get("result").is_none(), "expected error: {env}");
    assert_eq!(env["error"]["code"], json!(-32602));

    // Missing owner.
    let env2 = wss_rpc_envelope(
        &mut ws,
        2,
        "github.branches.listCached",
        json!({ "repo": "widget" }),
    )
    .await;
    assert_eq!(env2["error"]["code"], json!(-32602));

    // Path traversal segment.
    let env3 = wss_rpc_envelope(
        &mut ws,
        3,
        "github.branches.listCached",
        json!({ "owner": "..", "repo": "widget" }),
    )
    .await;
    assert_eq!(env3["error"]["code"], json!(-32602));
}
