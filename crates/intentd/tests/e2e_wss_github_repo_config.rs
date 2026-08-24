//! WSS end-to-end for `github.repoConfig.get` (PROTOCOL §5.27): fetching a
//! remote repository's `.intent/config.json` through the contents API without
//! a clone. Asserts the request params (`owner`, `repo`, optional `ref`)
//! reach the engine as a `.intent/config.json` content fetch, that the
//! response carries `{ config, exists }` with camelCase fields + unknown keys
//! preserved, and that the tolerant semantics hold on the wire: a missing
//! file yields `{ config: null, exists: false }`, invalid JSON folds to
//! `{ config: {}, exists: true }` (never an error), and missing
//! `owner`/`repo` fail with `-32602`. Drives a real [`WsApiServer`] over TLS
//! with bearer-token auth and a pinned self-signed fingerprint (the
//! production transport path) with a recording stub forge injected via
//! `with_source_control`.

#![cfg(unix)]

mod common;

use std::net::Ipv4Addr;
use std::path::PathBuf;
use std::sync::{Arc, Mutex};

use async_trait::async_trait;
use futures_util::{SinkExt, StreamExt};
use intent_core::{Result as CoreResult, WorkspaceApi};
use intent_services::{EventBus, Services};
use intent_sourcecontrol::{
    AuthStatus, Branch, CheckRun, Comment, CommentAnchor, Issue, IssueQuery, MergeMethod,
    MergeOptions, MergeOutcome, Mergeability, NewPullRequest, Page, PageParams, PrPatch, PrQuery,
    PullRequest, Repo, RepoRef, Result as ScResult, Review, ReviewComment, ReviewThread,
    ReviewVerdict, ScCapabilities, SourceControl, UserIdentity,
};
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

/// Recording stub forge: captures the `(repo, path, ref)` triple the services
/// layer hands to `get_file_content` so tests can assert the wire params
/// landed on the engine call, and returns the configured `file_content`
/// (`None` → absent file).
#[derive(Default)]
struct RecordingForge {
    /// `(owner/name, path, ref)` per `get_file_content` call.
    content_calls: Mutex<Vec<(String, String, Option<String>)>>,
    /// What `get_file_content` returns (`None` → the file is absent).
    file_content: Mutex<Option<String>>,
}

#[async_trait]
impl SourceControl for RecordingForge {
    fn provider_id(&self) -> &'static str {
        "stub"
    }
    fn capabilities(&self) -> ScCapabilities {
        ScCapabilities {
            draft_prs: true,
            squash_merge: true,
            rebase_merge: true,
            review_required_changes: true,
            check_runs: true,
            issues: true,
        }
    }
    async fn check_auth(&self) -> ScResult<AuthStatus> {
        unimplemented!()
    }
    async fn get_user(&self) -> ScResult<UserIdentity> {
        unimplemented!()
    }
    async fn list_repos(&self, _: PageParams) -> ScResult<Page<Repo>> {
        unimplemented!()
    }
    async fn search_repos(&self, _: &str, _: PageParams) -> ScResult<Page<Repo>> {
        unimplemented!()
    }
    async fn get_repo(&self, _: &str, _: &str) -> ScResult<Repo> {
        unimplemented!()
    }
    async fn list_remote_branches(
        &self,
        _: &str,
        _: &str,
        _: Option<&str>,
        _: PageParams,
    ) -> ScResult<Page<Branch>> {
        unimplemented!()
    }
    async fn get_file_content(
        &self,
        repo: &RepoRef,
        path: &str,
        git_ref: Option<&str>,
    ) -> ScResult<Option<String>> {
        self.content_calls.lock().unwrap().push((
            format!("{}/{}", repo.owner, repo.name),
            path.to_string(),
            git_ref.map(str::to_string),
        ));
        Ok(self.file_content.lock().unwrap().clone())
    }
    async fn create_pr(&self, _: &RepoRef, _: NewPullRequest) -> ScResult<PullRequest> {
        unimplemented!()
    }
    async fn get_pr(&self, _: &RepoRef, _: u64) -> ScResult<PullRequest> {
        unimplemented!()
    }
    async fn list_prs(&self, _: &RepoRef, _: PrQuery) -> ScResult<Page<PullRequest>> {
        unimplemented!()
    }
    async fn update_pr(&self, _: &RepoRef, _: u64, _: PrPatch) -> ScResult<PullRequest> {
        unimplemented!()
    }
    async fn merge_pr(
        &self,
        _: &RepoRef,
        _: u64,
        _: MergeMethod,
        _: MergeOptions,
    ) -> ScResult<MergeOutcome> {
        unimplemented!()
    }
    async fn mergeability(&self, _: &RepoRef, _: u64) -> ScResult<Mergeability> {
        unimplemented!()
    }
    async fn update_branch(&self, _: &RepoRef, _: u64) -> ScResult<()> {
        unimplemented!()
    }
    async fn submit_review(
        &self,
        _: &RepoRef,
        _: u64,
        _: ReviewVerdict,
        _: Option<String>,
    ) -> ScResult<Review> {
        unimplemented!()
    }
    async fn list_reviews(&self, _: &RepoRef, _: u64) -> ScResult<Vec<Review>> {
        unimplemented!()
    }
    async fn list_comments(&self, _: &RepoRef, _: u64) -> ScResult<Vec<Comment>> {
        unimplemented!()
    }
    async fn add_comment(
        &self,
        _: &RepoRef,
        _: u64,
        _: &str,
        _: Option<CommentAnchor>,
    ) -> ScResult<Comment> {
        unimplemented!()
    }
    async fn list_review_comments(
        &self,
        _: &RepoRef,
        _: u64,
        _: PageParams,
    ) -> ScResult<Page<ReviewComment>> {
        unimplemented!()
    }
    async fn reply_to_review_comment(
        &self,
        _: &RepoRef,
        _: u64,
        _: u64,
        _: &str,
    ) -> ScResult<ReviewComment> {
        unimplemented!()
    }
    async fn get_review_threads(
        &self,
        _: &RepoRef,
        _: u64,
        _: PageParams,
    ) -> ScResult<Page<ReviewThread>> {
        unimplemented!()
    }
    async fn resolve_thread(&self, _: &str) -> ScResult<bool> {
        unimplemented!()
    }
    async fn unresolve_thread(&self, _: &str) -> ScResult<bool> {
        unimplemented!()
    }
    async fn check_runs(&self, _: &RepoRef, _: &str) -> ScResult<Vec<CheckRun>> {
        unimplemented!()
    }
    async fn create_issue(&self, _: &RepoRef, _: &str, _: Option<&str>) -> ScResult<Issue> {
        unimplemented!()
    }
    async fn get_issue(&self, _: &RepoRef, _: u64) -> ScResult<Issue> {
        unimplemented!()
    }
    async fn list_issues(&self, _: &RepoRef, _: IssueQuery) -> ScResult<Page<Issue>> {
        unimplemented!()
    }
}

struct Fixture {
    _ws: WsApiServer,
    port: u16,
    cfg: Arc<ClientConfig>,
    forge: Arc<RecordingForge>,
    _dir: TempDir,
}

/// Boot a TLS + bearer-auth WSS listener whose services carry the recording
/// stub forge.
async fn boot() -> Fixture {
    let short = uuid::Uuid::new_v4().simple().to_string();
    let dir = std::env::temp_dir().join(format!("intentd-gh-repocfg-{}", &short[..8]));
    std::fs::create_dir_all(&dir).unwrap();
    let store = Store::open(&dir.join("intentd.db")).await.expect("store");
    let bus = EventBus::new(store.clone());
    let workspaces_root = dir.join("workspaces");
    std::fs::create_dir_all(&workspaces_root).expect("mkdir hermetic root");

    let forge = Arc::new(RecordingForge::default());
    let services = Arc::new(
        Services::new(store)
            .with_workspaces_root(workspaces_root)
            .with_event_bus(bus.clone())
            .with_source_control(forge.clone()),
    );
    let api: Arc<dyn WorkspaceApi> = services;
    let tls = ensure_tls_certificate(&dir).expect("cert");
    let token_store_inner = Arc::new(MemTokenStore::default());
    token_store_inner.store_token(TOKEN).unwrap();
    let token_store = Arc::new(AsyncTokenStore::new(token_store_inner));
    let opts = WsOptions {
        base_port: 0,
        bind_addresses: vec![Ipv4Addr::LOCALHOST.into()],
        ..Default::default()
    };
    let ws_srv = WsApiServer::new(api, bus, &tls, &token_store, opts, None).expect("server");
    let cfg = client_config(&tls.fingerprint256);
    let port = ws_srv.start().await.expect("start");
    Fixture {
        _ws: ws_srv,
        port,
        cfg,
        forge,
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
/// error) so tests can assert either arm.
async fn wss_rpc_envelope(ws: &mut TlsWs, id: i64, method: &str, params: Value) -> Value {
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

async fn wss_rpc(ws: &mut TlsWs, id: i64, method: &str, params: Value) -> Value {
    let v = wss_rpc_envelope(ws, id, method, params).await;
    assert!(v.get("error").is_none(), "rpc {method} errored: {v}");
    v["result"].clone()
}

/// A present remote `.intent/config.json`: the wire params land on the engine
/// content fetch (`.intent/config.json` at the requested ref) and the response
/// is `{ config, exists: true }` with camelCase fields and unknown keys
/// preserved.
#[tokio::test]
async fn repo_config_get_returns_parsed_remote_config() {
    let fx = boot().await;
    *fx.forge.file_content.lock().unwrap() = Some(
        r#"{ "branchPrefix": "feat/", "setupScript": "pnpm install", "customKey": 42 }"#.into(),
    );
    let mut ws = connect(fx.port, fx.cfg.clone()).await;

    let r = wss_rpc(
        &mut ws,
        1,
        "github.repoConfig.get",
        json!({ "owner": "octocat", "repo": "hello", "ref": "main" }),
    )
    .await;
    assert_eq!(r["config"]["branchPrefix"], "feat/");
    assert_eq!(r["config"]["setupScript"], "pnpm install");
    // Unknown keys round-trip (RepoConfig `extra` flatten).
    assert_eq!(r["config"]["customKey"], 42);
    assert_eq!(r["exists"], true);

    let calls = fx.forge.content_calls.lock().unwrap();
    assert_eq!(
        calls.as_slice(),
        &[(
            "octocat/hello".to_string(),
            ".intent/config.json".to_string(),
            Some("main".to_string()),
        )]
    );
}

/// Tolerant semantics on the wire: a missing file yields
/// `{ config: null, exists: false }`, invalid JSON folds to
/// `{ config: {}, exists: true }` (never an error), and an omitted `ref`
/// reaches the engine as `None` (default-branch read).
#[tokio::test]
async fn repo_config_get_missing_or_invalid_yields_empty_config() {
    let fx = boot().await;
    let mut ws = connect(fx.port, fx.cfg.clone()).await;

    // Absent file (engine returns None), no ref param.
    let r = wss_rpc(
        &mut ws,
        1,
        "github.repoConfig.get",
        json!({ "owner": "octocat", "repo": "hello" }),
    )
    .await;
    assert_eq!(r["config"], Value::Null);
    assert_eq!(r["exists"], false);

    // Invalid JSON in the fetched file.
    *fx.forge.file_content.lock().unwrap() = Some("{ not json".into());
    let r2 = wss_rpc(
        &mut ws,
        2,
        "github.repoConfig.get",
        json!({ "owner": "octocat", "repo": "hello" }),
    )
    .await;
    assert_eq!(r2["config"], json!({}));
    assert_eq!(r2["exists"], true);

    let calls = fx.forge.content_calls.lock().unwrap();
    assert_eq!(calls.len(), 2);
    assert_eq!(calls[0].2, None, "omitted ref reaches the engine as None");
}

/// Missing required params fail with the JSON-RPC `-32602` invalid-params
/// envelope and never reach the engine.
#[tokio::test]
async fn repo_config_get_requires_owner_and_repo() {
    let fx = boot().await;
    let mut ws = connect(fx.port, fx.cfg.clone()).await;

    let env = wss_rpc_envelope(
        &mut ws,
        1,
        "github.repoConfig.get",
        json!({ "owner": "octocat" }),
    )
    .await;
    assert!(env.get("result").is_none(), "expected error: {env}");
    assert_eq!(env["error"]["code"], json!(-32602));

    let env2 = wss_rpc_envelope(&mut ws, 2, "github.repoConfig.get", json!({ "repo": "r" })).await;
    assert_eq!(env2["error"]["code"], json!(-32602));

    assert!(fx.forge.content_calls.lock().unwrap().is_empty());
}
