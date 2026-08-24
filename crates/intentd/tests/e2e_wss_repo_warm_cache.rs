//! WSS end-to-end test for `repo.warmCache` (PROTOCOL §5.6): drives the real
//! WSS transport (TLS + fingerprint pinning + bearer auth) against a
//! `file://` fixture repo and asserts the `{ started, owner, repo }` result
//! shape, the busy rejection envelope (`error.data.code === "warm-in-flight"`),
//! and the `-32602` invalid-URL arm.

#![cfg(unix)]

mod common;

use std::net::Ipv4Addr;
use std::path::PathBuf;
use std::sync::{Arc, Mutex};
use std::time::Duration;

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

type TlsWs = WebSocketStream<tokio_rustls::client::TlsStream<TcpStream>>;

/// A fixed 64-char hex token (valid shape) shared by server + client.
const TOKEN: &str = "abababababababababababababababababababababababababababababababab";

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

/// Pin the server's SHA-256 fingerprint (colon-UPPER hex over the DER cert).
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

struct TempDir(PathBuf);
impl Drop for TempDir {
    fn drop(&mut self) {
        let _ = std::fs::remove_dir_all(&self.0);
    }
}

struct Fixture {
    _ws: WsApiServer,
    port: u16,
    cfg: Arc<ClientConfig>,
    root: PathBuf,
    _dir: TempDir,
}

async fn boot() -> Fixture {
    let short = uuid::Uuid::new_v4().simple().to_string();
    let dir = std::env::temp_dir().join(format!("intentd-warm-{}", &short[..8]));
    std::fs::create_dir_all(&dir).unwrap();
    let store = Store::open(&dir.join("intentd.db")).await.expect("store");
    let bus = EventBus::new(store.clone());
    let workspaces_root = dir.join("workspaces");
    std::fs::create_dir_all(&workspaces_root).expect("mkdir hermetic root");
    let services = Services::new(store)
        .with_workspaces_root(workspaces_root.clone())
        .with_event_bus(bus.clone());
    let api: Arc<dyn WorkspaceApi> = Arc::new(services);
    let tls = ensure_tls_certificate(&dir).expect("cert");
    let token_store_inner = Arc::new(MemTokenStore::default());
    token_store_inner.store_token(TOKEN).unwrap();
    let token_store = Arc::new(AsyncTokenStore::new(token_store_inner));
    let opts = WsOptions {
        base_port: 0,
        bind_addresses: vec![Ipv4Addr::LOCALHOST.into()],
        ..Default::default()
    };
    let ws = WsApiServer::new(api, bus, &tls, &token_store, opts, None).expect("server");
    let cfg = client_config(&tls.fingerprint256);
    let port = ws.start().await.expect("start");
    Fixture {
        _ws: ws,
        port,
        cfg,
        root: workspaces_root,
        _dir: TempDir(dir),
    }
}

/// Open an authenticated WSS connection (pinned TLS, token in the query
/// string).
async fn connect(fx: &Fixture) -> TlsWs {
    let url = format!("wss://localhost:{}/ws?token={TOKEN}", fx.port);
    common::wss_connect_with_retry(fx.port, fx.cfg.clone(), &url).await
}

async fn wss_rpc(ws: &mut TlsWs, id: i64, method: &str, params: Value) -> Value {
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
                Message::Ping(p) => {
                    let _ = ws.send(Message::Pong(p)).await;
                }
                Message::Pong(_) => {}
                _ => panic!("unexpected message"),
            }
        }
    })
    .await
    .expect("response timeout")
}

/// Init a small git repo with one commit using the git CLI; returns its path.
fn seed_repo(dir: &PathBuf) {
    std::fs::create_dir_all(dir).unwrap();
    let git = |args: &[&str]| {
        let out = std::process::Command::new("git")
            .args(args)
            .current_dir(dir)
            .output()
            .expect("git");
        assert!(out.status.success(), "git {args:?}: {out:?}");
    };
    git(&["init"]);
    git(&["config", "user.name", "Test"]);
    git(&["config", "user.email", "test@example.com"]);
    std::fs::write(dir.join("README.md"), "# warm fixture\n").unwrap();
    git(&["add", "README.md"]);
    git(&["commit", "-m", "Initial commit"]);
}

/// Owner/repo as the daemon derives them from a `file://` URL: the last two
/// path segments.
fn owner_repo_of(dir: &std::path::Path) -> (String, String) {
    let repo = dir.file_name().unwrap().to_str().unwrap().to_string();
    let owner = dir
        .parent()
        .and_then(|p| p.file_name())
        .unwrap()
        .to_str()
        .unwrap()
        .to_string();
    (owner, repo)
}

/// Poll `repo.warmCache` until the detached warm completes: an accepted
/// re-warm proves the in-flight flag cleared; the populated cache slot
/// proves the ensure ran.
async fn wait_for_warm_completion(ws: &mut TlsWs, root: &std::path::Path, url: &str, base: i64) {
    let deadline = tokio::time::Instant::now() + Duration::from_secs(30);
    let mut id = base;
    loop {
        let v = wss_rpc(ws, id, "repo.warmCache", json!({ "githubUrl": url })).await;
        id += 1;
        if let Some(result) = v.get("result").filter(|r| !r.is_null()) {
            let cache = root
                .join(".repo-cache")
                .join(result["owner"].as_str().unwrap())
                .join(result["repo"].as_str().unwrap());
            assert!(cache.join(".git").exists(), "repo cache populated");
            return;
        }
        assert_eq!(
            v["error"]["data"]["code"],
            json!("warm-in-flight"),
            "only the busy error is expected while polling: {v}"
        );
        assert!(
            tokio::time::Instant::now() < deadline,
            "warm did not complete in time"
        );
        tokio::time::sleep(Duration::from_millis(50)).await;
    }
}

/// Happy path over WSS: `repo.warmCache` returns the documented
/// `{ started: true, owner, repo }` result immediately, and the detached
/// ensure populates `<root>/.repo-cache/<owner>/<repo>` from the `file://`
/// fixture.
#[tokio::test]
async fn repo_warm_cache_starts_and_populates_cache_over_wss() {
    let fx = boot().await;
    let mut rpc = connect(&fx).await;

    let repo_dir = fx.root.parent().unwrap().join("warm-fixture-src");
    seed_repo(&repo_dir);
    let (owner, repo) = owner_repo_of(&repo_dir);
    let url = format!("file://{}", repo_dir.to_string_lossy());

    let v = wss_rpc(&mut rpc, 1, "repo.warmCache", json!({ "githubUrl": url })).await;
    assert_eq!(v["jsonrpc"], json!("2.0"));
    assert_eq!(v["id"], json!(1));
    assert_eq!(
        v["result"],
        json!({ "started": true, "owner": owner, "repo": repo }),
        "result shape per PROTOCOL §5.6"
    );

    wait_for_warm_completion(&mut rpc, &fx.root, &url, 100).await;
}

/// Busy rejection over WSS: with the warm's ensure parked behind the held
/// per-repo cache lock, a second `repo.warmCache` is rejected with `-32603`
/// carrying `error.data = { code: "warm-in-flight", owner, repo }`.
#[tokio::test(flavor = "multi_thread", worker_threads = 2)]
async fn repo_warm_cache_busy_rejection_envelope_over_wss() {
    let fx = boot().await;
    let mut rpc = connect(&fx).await;

    let repo_dir = fx.root.parent().unwrap().join("warm-busy-src");
    seed_repo(&repo_dir);
    let (owner, repo) = owner_repo_of(&repo_dir);
    let url = format!("file://{}", repo_dir.to_string_lossy());

    // Hold the per-repo cache lock (same in-process lock map as the daemon
    // services) so the accepted warm's ensure parks and the in-flight window
    // is deterministic.
    let cache_path = fx.root.join(".repo-cache").join(&owner).join(&repo);
    let (release_tx, release_rx) = std::sync::mpsc::channel::<()>();
    let (held_tx, held_rx) = std::sync::mpsc::channel::<()>();
    let lock_holder = tokio::spawn(async move {
        intent_git::repo_cache::with_cache_lock_blocking(&cache_path, move || {
            held_tx.send(()).unwrap();
            release_rx.recv().unwrap();
            Ok(())
        })
        .await
    });
    held_rx.recv_timeout(Duration::from_secs(10)).unwrap();

    let v = wss_rpc(&mut rpc, 1, "repo.warmCache", json!({ "githubUrl": url })).await;
    assert_eq!(v["result"]["started"], json!(true), "first warm accepted");

    let v = wss_rpc(&mut rpc, 2, "repo.warmCache", json!({ "githubUrl": url })).await;
    assert_eq!(v["jsonrpc"], json!("2.0"));
    assert_eq!(v["id"], json!(2));
    assert_eq!(v["error"]["code"], json!(-32603));
    assert_eq!(
        v["error"]["data"],
        json!({ "code": "warm-in-flight", "owner": owner, "repo": repo }),
        "busy envelope names the repo being warmed"
    );

    release_tx.send(()).unwrap();
    lock_holder.await.unwrap().unwrap();
    wait_for_warm_completion(&mut rpc, &fx.root, &url, 100).await;
}

/// Invalid URL over WSS: a `githubUrl` with no owner/repo pair is `-32602`,
/// and a missing `githubUrl` param is `-32602` as well.
#[tokio::test]
async fn repo_warm_cache_invalid_params_over_wss() {
    let fx = boot().await;
    let mut rpc = connect(&fx).await;

    let v = wss_rpc(
        &mut rpc,
        1,
        "repo.warmCache",
        json!({ "githubUrl": "not-a-repo-url" }),
    )
    .await;
    assert_eq!(v["error"]["code"], json!(-32602));

    let v = wss_rpc(&mut rpc, 2, "repo.warmCache", json!({})).await;
    assert_eq!(v["error"]["code"], json!(-32602));

    // A traversal owner segment is rejected up front (-32602) without
    // claiming the single-flight slot.
    let v = wss_rpc(
        &mut rpc,
        3,
        "repo.warmCache",
        json!({ "githubUrl": "https://github.com/../repo" }),
    )
    .await;
    assert_eq!(v["error"]["code"], json!(-32602));
}
