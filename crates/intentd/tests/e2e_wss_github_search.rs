//! WSS end-to-end for the free-text `query` on `github.pulls.search` /
//! `github.issues.search` (PROTOCOL §5.27): the wire `query` must reach the
//! engine as `PrQuery.search` / `IssueQuery.search` (trimmed, blanks dropped),
//! the `nextToken` cursor must round-trip onto the engine cursor, and the
//! no-query call must keep the pre-existing listing behavior (`search: None`).
//! Drives a real [`WsApiServer`] over TLS with bearer-token auth and a pinned
//! self-signed fingerprint (the production transport path) with a recording
//! stub forge injected via `with_source_control`.

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
    MergeOptions, MergeOutcome, Mergeability, NewPullRequest, Page, PageParams, PrInvolvement,
    PrPatch, PrQuery, PrState, PullRequest, Repo, RepoRef, Result as ScResult, Review,
    ReviewComment, ReviewThread, ReviewVerdict, ScCapabilities, SourceControl, UserIdentity,
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

fn sample_pr() -> PullRequest {
    PullRequest {
        number: 42,
        url: "https://github.com/o/r/pull/42".into(),
        title: "Add thing".into(),
        body: None,
        state: PrState::Open,
        draft: false,
        source_branch: "feature".into(),
        target_branch: "main".into(),
        author: "octocat".into(),
        mergeable: Some(true),
        mergeable_state: Some("clean".into()),
        head_sha: Some("deadbeef".into()),
        created_at: String::new(),
        updated_at: String::new(),
    }
}

fn sample_issue() -> Issue {
    Issue {
        number: 11,
        title: "Login bug".into(),
        body: None,
        state: "open".into(),
        url: "https://github.com/o/r/issues/11".into(),
    }
}

/// Recording stub forge: captures the [`PrQuery`] / [`IssueQuery`] the
/// services layer hands to the engine so tests can assert the wire `query` /
/// `nextToken` landed on `search` / `cursor`, and returns one PR / issue with
/// a fixed `next_cursor` (`"2"`) so `nextToken` round-trips on the response.
/// Branch listings record the `(prefix, cursor)` pair the engine saw so the
/// `github.branches.list` `prefix` threading is assertable end-to-end.
#[derive(Default)]
struct RecordingForge {
    pr_queries: Mutex<Vec<PrQuery>>,
    issue_queries: Mutex<Vec<IssueQuery>>,
    branch_queries: Mutex<Vec<(Option<String>, Option<String>)>>,
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
        prefix: Option<&str>,
        page: PageParams,
    ) -> ScResult<Page<Branch>> {
        self.branch_queries
            .lock()
            .unwrap()
            .push((prefix.map(str::to_string), page.cursor.clone()));
        let all = ["main", "feature/login", "feature/logout"];
        let items = all
            .iter()
            .filter(|n| prefix.is_none_or(|p| n.starts_with(p)))
            .map(|n| Branch {
                name: (*n).into(),
                commit_sha: None,
                protected: false,
            })
            .collect();
        Ok(Page {
            items,
            next_cursor: Some("2".into()),
        })
    }
    async fn get_file_content(
        &self,
        _: &RepoRef,
        _: &str,
        _: Option<&str>,
    ) -> ScResult<Option<String>> {
        unimplemented!()
    }
    async fn create_pr(&self, _: &RepoRef, _: NewPullRequest) -> ScResult<PullRequest> {
        unimplemented!()
    }
    async fn get_pr(&self, _: &RepoRef, _: u64) -> ScResult<PullRequest> {
        unimplemented!()
    }
    async fn list_prs(&self, _: &RepoRef, query: PrQuery) -> ScResult<Page<PullRequest>> {
        self.pr_queries.lock().unwrap().push(query);
        Ok(Page {
            items: vec![sample_pr()],
            next_cursor: Some("2".into()),
        })
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
    async fn list_issues(&self, _: &RepoRef, query: IssueQuery) -> ScResult<Page<Issue>> {
        self.issue_queries.lock().unwrap().push(query);
        Ok(Page {
            items: vec![sample_issue()],
            next_cursor: Some("2".into()),
        })
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
    let dir = std::env::temp_dir().join(format!("intentd-gh-search-{}", &short[..8]));
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

/// The opaque wire `nextToken` for an engine page cursor: no-pad base64 of
/// `{"c":"<cursor>"}` (mirrors the services-layer encoding).
fn wire_next_token(cursor: &str) -> String {
    use base64::Engine as _;
    base64::engine::general_purpose::STANDARD_NO_PAD
        .encode(serde_json::to_vec(&json!({ "c": cursor })).unwrap())
}

/// `github.pulls.search` with a free-text `query`: the trimmed text reaches
/// the engine as `PrQuery.search`, the involvement filter still parses, the
/// `nextToken` decodes onto the engine cursor, and the response carries the
/// PR page plus an encoded `nextToken` for the engine's `next_cursor`.
#[tokio::test]
async fn pulls_search_forwards_query_and_cursor() {
    let fx = boot().await;
    let mut ws = connect(fx.port, fx.cfg.clone()).await;

    let r = wss_rpc(
        &mut ws,
        1,
        "github.pulls.search",
        json!({
            "owner": "o", "repo": "r", "filter": "created", "state": "open",
            "query": "  panic on save  ", "limit": 10,
            "nextToken": wire_next_token("3"),
        }),
    )
    .await;
    assert_eq!(r["pulls"][0]["number"], 42);
    assert_eq!(r["nextToken"], json!(wire_next_token("2")));

    let queries = fx.forge.pr_queries.lock().unwrap();
    assert_eq!(queries.len(), 1);
    let q = &queries[0];
    assert_eq!(q.search.as_deref(), Some("panic on save"));
    assert_eq!(q.state, Some(PrState::Open));
    assert!(q.involvement.is_some());
    assert_eq!(q.limit, Some(10));
    assert_eq!(q.cursor.as_deref(), Some("3"));
}

/// `github.issues.search` with a free-text `query`: the trimmed text reaches
/// the engine as `IssueQuery.search` with the state filter intact, and the
/// cursor round-trips both ways.
#[tokio::test]
async fn issues_search_forwards_query_and_cursor() {
    let fx = boot().await;
    let mut ws = connect(fx.port, fx.cfg.clone()).await;

    let r = wss_rpc(
        &mut ws,
        1,
        "github.issues.search",
        json!({
            "owner": "o", "repo": "r", "state": "closed",
            "query": "  login bug  ",
            "nextToken": wire_next_token("5"),
        }),
    )
    .await;
    assert_eq!(r["issues"][0]["number"], 11);
    assert_eq!(r["issues"][0]["owner"], "o");
    assert_eq!(r["issues"][0]["repo"], "r");
    // The response token encodes the engine's returned next_cursor ("2"),
    // not an echo of the request token (cursor "5").
    assert_eq!(r["nextToken"], json!(wire_next_token("2")));

    let queries = fx.forge.issue_queries.lock().unwrap();
    assert_eq!(queries.len(), 1);
    let q = &queries[0];
    assert_eq!(q.search.as_deref(), Some("login bug"));
    assert_eq!(q.state.as_deref(), Some("closed"));
    assert_eq!(q.cursor.as_deref(), Some("5"));
}

/// Without a `query` (or with a blank one) the engine sees `search: None` —
/// the pre-existing listing behavior is unchanged.
#[tokio::test]
async fn search_without_query_leaves_listing_unchanged() {
    let fx = boot().await;
    let mut ws = connect(fx.port, fx.cfg.clone()).await;

    let r = wss_rpc(
        &mut ws,
        1,
        "github.issues.search",
        json!({ "owner": "o", "repo": "r" }),
    )
    .await;
    assert_eq!(r["issues"][0]["number"], 11);

    let r2 = wss_rpc(
        &mut ws,
        2,
        "github.pulls.search",
        json!({ "owner": "o", "repo": "r", "query": "   " }),
    )
    .await;
    assert_eq!(r2["pulls"][0]["number"], 42);

    let issue_queries = fx.forge.issue_queries.lock().unwrap();
    assert_eq!(issue_queries.len(), 1);
    assert_eq!(issue_queries[0].search, None);
    assert_eq!(issue_queries[0].state.as_deref(), Some("open"));

    let pr_queries = fx.forge.pr_queries.lock().unwrap();
    assert_eq!(pr_queries.len(), 1);
    assert_eq!(pr_queries[0].search, None);
    assert_eq!(pr_queries[0].involvement, None);
}

/// `github.issues.search` rejects the PR-only `review-requested` filter with
/// the wire `-32603 "Internal error"` envelope whose `data` lists the issues
/// filter set from PROTOCOL §5 (`all, assigned, created, involves`) and never
/// reaches the engine, while `github.pulls.search` continues to accept
/// `review-requested` (monorepo#551).
#[tokio::test]
async fn issues_search_rejects_pr_only_review_requested_filter() {
    let fx = boot().await;
    let mut ws = connect(fx.port, fx.cfg.clone()).await;

    let env = wss_rpc_envelope(
        &mut ws,
        1,
        "github.issues.search",
        json!({ "owner": "o", "repo": "r", "filter": "review-requested" }),
    )
    .await;
    assert!(
        env.get("result").is_none(),
        "expected error envelope: {env}"
    );
    assert_eq!(env["error"]["code"], json!(-32603));
    assert_eq!(env["error"]["message"], json!("Internal error"));
    let data = env["error"]["data"].as_str().unwrap();
    assert!(
        data.contains("all, assigned, created, involves"),
        "error data must list the issues filter set: {data}"
    );
    assert!(fx.forge.issue_queries.lock().unwrap().is_empty());

    // Valid issue filters still pass through unchanged.
    let r = wss_rpc(
        &mut ws,
        2,
        "github.issues.search",
        json!({ "owner": "o", "repo": "r", "filter": "involves" }),
    )
    .await;
    assert_eq!(r["issues"][0]["number"], 11);

    // The PR search keeps its wider filter set.
    let r2 = wss_rpc(
        &mut ws,
        3,
        "github.pulls.search",
        json!({ "owner": "o", "repo": "r", "filter": "review-requested" }),
    )
    .await;
    assert_eq!(r2["pulls"][0]["number"], 42);
    let pr_queries = fx.forge.pr_queries.lock().unwrap();
    assert_eq!(pr_queries.len(), 1);
    assert_eq!(
        pr_queries[0].involvement,
        Some(PrInvolvement::ReviewRequested)
    );
}

/// `github.branches.list` with an optional `prefix`: the wire `prefix`
/// reaches the engine and narrows the branch names, the `nextToken` cursor
/// round-trips onto the engine cursor, and the no-prefix (or blank-prefix)
/// call keeps the pre-existing unfiltered listing (`prefix: None`).
#[tokio::test]
async fn branches_list_forwards_optional_prefix() {
    let fx = boot().await;
    let mut ws = connect(fx.port, fx.cfg.clone()).await;

    // Prefixed: filtered names + the encoded engine next_cursor ("2").
    let r = wss_rpc(
        &mut ws,
        1,
        "github.branches.list",
        json!({
            "owner": "o", "repo": "r", "prefix": "feature/",
            "nextToken": wire_next_token("3"),
        }),
    )
    .await;
    assert_eq!(r["branches"], json!(["feature/login", "feature/logout"]));
    assert_eq!(r["nextToken"], json!(wire_next_token("2")));

    // No prefix and a blank prefix both reach the engine as `None`.
    let r2 = wss_rpc(
        &mut ws,
        2,
        "github.branches.list",
        json!({ "owner": "o", "repo": "r" }),
    )
    .await;
    assert_eq!(
        r2["branches"],
        json!(["main", "feature/login", "feature/logout"])
    );
    let r3 = wss_rpc(
        &mut ws,
        3,
        "github.branches.list",
        json!({ "owner": "o", "repo": "r", "prefix": "   " }),
    )
    .await;
    assert_eq!(
        r3["branches"],
        json!(["main", "feature/login", "feature/logout"])
    );

    let queries = fx.forge.branch_queries.lock().unwrap();
    assert_eq!(
        *queries,
        vec![
            (Some("feature/".to_string()), Some("3".to_string())),
            (None, None),
            (None, None),
        ]
    );
}
