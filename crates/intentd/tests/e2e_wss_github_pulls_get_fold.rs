//! WSS end-to-end for the `github.pulls.get` fold (docs/protocol/methods/
//! github.md, docs/protocol/06-events.md): the on-demand PR fetch behind the
//! FE hover card passively folds the fetched snapshot into the daemon-owned
//! PR state of every workspace / git root referencing the PR by URL, so the
//! `displayStatus`-grouped sidebar reflects the fresh status through the
//! existing event plumbing. Drives a real [`WsApiServer`] over TLS with
//! bearer-token auth and a pinned self-signed fingerprint (the production
//! transport path) with a stub forge injected via `with_source_control`, and
//! asserts the response envelope, the follow-up `workspace.get`, and the
//! `events.event` notifications observed over the wire.

#![cfg(unix)]

mod common;

use std::net::Ipv4Addr;
use std::sync::{Arc, Mutex};
use std::time::Duration;

use async_trait::async_trait;
use futures_util::{SinkExt, StreamExt};
use intent_core::{
    now_iso, PullRequestInfo, PullRequestStatus, Result as CoreResult, Workspace,
    WorkspaceActivity, WorkspaceApi, WorkspaceAttention, WorkspaceGitRoot, WorkspaceGitRootId,
    WorkspaceGitRootSource, WorkspaceId, WorkspaceStatus,
};
use intent_services::{EventBus, Services};
use intent_sourcecontrol::{
    AuthStatus, Branch, CheckRun, Comment, CommentAnchor, Issue, IssueQuery, MergeMethod,
    MergeOptions, MergeOutcome, Mergeability, NewPullRequest, Page, PageParams, PrPatch, PrQuery,
    PrState, PullRequest, Repo, RepoRef, Result as ScResult, Review, ReviewComment, ReviewThread,
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
use tokio::time::timeout;
use tokio_tungstenite::tungstenite::Message;

use common::TlsWs;

/// A fixed 64-char hex token (valid shape) shared by server + client.
const TOKEN: &str = "abababababababababababababababababababababababababababababababab";

const PR_URL: &str = "https://github.com/o/r/pull/42";

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

/// Stub forge: `get_pr` reports every PR merged (head `feature`) at the
/// canonical `o/r` URL for its number; nothing else is exercised.
#[derive(Default)]
struct StubForge;

fn merged_pr(number: u64) -> PullRequest {
    PullRequest {
        number,
        url: format!("https://github.com/o/r/pull/{number}"),
        title: "Add thing".into(),
        body: None,
        state: PrState::Merged,
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

#[async_trait]
impl SourceControl for StubForge {
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
        _: &RepoRef,
        _: &str,
        _: Option<&str>,
    ) -> ScResult<Option<String>> {
        unimplemented!()
    }
    async fn create_pr(&self, _: &RepoRef, _: NewPullRequest) -> ScResult<PullRequest> {
        unimplemented!()
    }
    async fn get_pr(&self, _: &RepoRef, number: u64) -> ScResult<PullRequest> {
        Ok(merged_pr(number))
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

/// The persisted (stale) Open snapshot of PR #42: mergeable + clean so the
/// pre-fold rollup reads `pr_ready`.
fn open_pr_info() -> PullRequestInfo {
    PullRequestInfo {
        id: "42".into(),
        number: 42,
        url: PR_URL.into(),
        title: "Add thing".into(),
        status: PullRequestStatus::Open,
        created_at: String::new(),
        updated_at: String::new(),
        base_ref: Some("main".into()),
        head_ref: Some("feature".into()),
        head_sha: Some("deadbeef".into()),
        author: Some("octocat".into()),
        mergeable: Some(true),
        mergeable_state: Some("clean".into()),
        is_draft: Some(false),
    }
}

struct Fixture {
    _ws: WsApiServer,
    port: u16,
    cfg: Arc<ClientConfig>,
    ws_id: WorkspaceId,
    root_id: WorkspaceGitRootId,
    _dir: tempfile::TempDir,
}

/// Boot a TLS + bearer-auth WSS listener whose services carry the stub forge
/// and a seeded `o/r` workspace on branch `feature` linked to PR #42 with the
/// stale Open snapshot persisted on the linked columns and the pool, plus a
/// git root whose pool holds the same stale entry.
async fn boot() -> Fixture {
    let dir_guard = common::test_tempdir("intentd-pulls-get-fold-");
    let dir = dir_guard.path().to_path_buf();
    let store = Store::open(&dir.join("intentd.db")).await.expect("store");
    let bus = EventBus::new(store.clone());
    let workspaces_root = dir.join("workspaces");
    std::fs::create_dir_all(&workspaces_root).expect("mkdir hermetic root");

    let ws_id = WorkspaceId::new();
    let ts = now_iso();
    let ws = Workspace {
        id: ws_id.clone(),
        title: "Pulls get fold".into(),
        branch: "feature".into(),
        base_ref: None,
        base_commit_sha: None,
        status: WorkspaceStatus::Active,
        status_message: None,
        status_image_asset_id: None,
        activity: WorkspaceActivity::Idle,
        attention: WorkspaceAttention::None,
        created_at: ts.clone(),
        updated_at: ts.clone(),
        last_activity: None,
        tags: vec![],
        path: None,
        repository_path: None,
        repository_owner: Some("o".into()),
        repository_name: Some("r".into()),
        worktree_path: None,
        scope: None,
        skip_worktree: false,
        setup_script: None,
        is_remote: false,
        default_model: None,
        pr_number: Some(42),
        pr_url: Some(PR_URL.into()),
        pr_status: Some(PullRequestStatus::Open),
        active_pull_request: Some(open_pr_info()),
        pull_requests: Some(vec![open_pr_info()]),
        context_links: None,
        archived: false,
        archived_at: None,
        task_stats: None,
        agent_summary: None,
        diff_summary: None,
        token_usage: None,
        cow_supported: None,
        browser_client_id: None,
        display_status: None,
        waiting: false,
        checkout_mode: None,
        disk_usage: None,
        pending_delete_at: None,
        membership: None,
    };
    store.insert_workspace(&ws).await.expect("seed workspace");
    let root_id = WorkspaceGitRootId::new();
    store
        .upsert_workspace_git_root(&WorkspaceGitRoot {
            id: root_id.clone(),
            workspace_id: ws_id.clone(),
            path: dir.join("root-a").to_string_lossy().into_owned(),
            source: WorkspaceGitRootSource::Agent,
            repo_owner: Some("o".into()),
            repo_name: Some("r".into()),
            registered_by_agent_ids: vec![],
            registered_commit_sha: None,
            pr_number: None,
            pr_url: None,
            pr_status: None,
            pull_requests: Some(vec![open_pr_info()]),
            created_at: ts.clone(),
            updated_at: ts,
        })
        .await
        .expect("seed git root");

    let services = Arc::new(
        Services::new(store)
            .with_workspaces_root(workspaces_root)
            .with_event_bus(bus.clone())
            .with_source_control(Arc::new(StubForge)),
    );
    let api: Arc<dyn WorkspaceApi> = services.clone();
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
        ws_id,
        root_id,
        _dir: dir_guard,
    }
}

/// Establish an authenticated WSS connection over pinned TLS (token in the
/// query string).
async fn connect(port: u16, cfg: Arc<ClientConfig>) -> TlsWs {
    let url = format!("wss://localhost:{port}/ws?token={TOKEN}");
    common::wss_connect_with_retry(port, cfg, &url).await
}

/// Send one JSON-RPC request and return the FULL response envelope.
async fn wss_call(ws: &mut TlsWs, id: i64, method: &str, params: Value) -> Value {
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

/// Send one JSON-RPC request and return `result` (asserting no `error`).
async fn wss_rpc(ws: &mut TlsWs, id: i64, method: &str, params: Value) -> Value {
    let v = wss_call(ws, id, method, params).await;
    assert!(v.get("error").is_none(), "rpc {method} errored: {v}");
    v["result"].clone()
}

/// Collect the next `n` `events.event` notifications (any type), in order.
async fn next_events(ws: &mut TlsWs, n: usize) -> Vec<Value> {
    let mut out = Vec::with_capacity(n);
    timeout(Duration::from_secs(10), async {
        while out.len() < n {
            match ws.next().await.unwrap().unwrap() {
                Message::Text(text) => {
                    let v: Value = serde_json::from_str(&text).unwrap();
                    if v["method"] == json!("events.event") {
                        out.push(v["params"]["event"].clone());
                    }
                }
                Message::Ping(p) => {
                    let _ = ws.send(Message::Pong(p)).await;
                }
                _ => {}
            }
        }
    })
    .await
    .unwrap_or_else(|_| panic!("timed out after {} of {n} events: {out:?}", out.len()));
    out
}

/// Assert no further `events.event` notification arrives within a short
/// window (the fold writes nothing for a PR nobody references).
async fn assert_no_events(ws: &mut TlsWs) {
    let idle = timeout(Duration::from_millis(600), async {
        loop {
            match ws.next().await.unwrap().unwrap() {
                Message::Text(text) => {
                    let v: Value = serde_json::from_str(&text).unwrap();
                    if v["method"] == json!("events.event") {
                        return v["params"]["event"].clone();
                    }
                }
                Message::Ping(p) => {
                    let _ = ws.send(Message::Pong(p)).await;
                }
                _ => {}
            }
        }
    })
    .await;
    if let Ok(evt) = idle {
        panic!("unexpected event: {evt}");
    }
}

/// `github.pulls.get` for a PR the workspace is linked to (#42, persisted as
/// the stale Open snapshot on `prStatus` / `activePullRequest` / the pool,
/// plus the same stale entry on a git root's pool) returns the hover card's
/// `{ pull }` and folds the fetched merged snapshot into the daemon-owned
/// state: `pr:updated`, the `pr_ready` → `pr_merged`
/// `workspace:displayStatus-changed` transition, and `gitRoot:updated` all
/// land over the wire, and the follow-up `workspace.get` serves the merged
/// snapshot. A fetch for a PR nobody references writes nothing.
#[tokio::test]
async fn github_pulls_get_folds_fetched_pr_into_workspace_pr_state_over_wss() {
    let fx = boot().await;
    let mut rpc = connect(fx.port, fx.cfg.clone()).await;

    // Baseline read: the stale Open snapshot reads `pr_ready`, and this
    // first observation seeds the displayStatus transition baseline.
    let before = wss_rpc(
        &mut rpc,
        1,
        "workspace.get",
        json!({ "workspaceId": fx.ws_id.as_str() }),
    )
    .await;
    assert_eq!(before["workspace"]["prStatus"], "Open");
    assert_eq!(before["workspace"]["displayStatus"], "pr_ready");

    // Subscribe on a separate connection before driving the fetch.
    let mut sub = connect(fx.port, fx.cfg.clone()).await;
    let sub_res = wss_rpc(
        &mut sub,
        10,
        "events.subscribe",
        json!({
            "eventTypes": ["pr:updated", "gitRoot:updated", "workspace:displayStatus-changed"],
            "workspaceId": fx.ws_id.as_str(),
        }),
    )
    .await;
    assert!(sub_res["subscriptionId"].is_string(), "sub id: {sub_res}");

    // The hover card's on-demand fetch: response envelope per §5.27.
    let envelope = wss_call(
        &mut rpc,
        2,
        "github.pulls.get",
        json!({ "owner": "o", "repo": "r", "number": 42 }),
    )
    .await;
    assert_eq!(envelope["jsonrpc"], "2.0");
    assert_eq!(envelope["id"], 2);
    assert!(envelope.get("error").is_none(), "errored: {envelope}");
    let pull = &envelope["result"]["pull"];
    assert_eq!(pull["number"], 42);
    assert_eq!(pull["htmlUrl"], PR_URL);
    assert_eq!(pull["state"], "closed");
    assert_eq!(pull["merged"], true);
    assert_eq!(pull["draft"], false);
    assert_eq!(pull["headRef"], "feature");

    // The fold's events: the workspace delta first (pool + linked columns,
    // then the derived rollup), the git root's pool delta after.
    let events = next_events(&mut sub, 3).await;
    assert_eq!(events[0]["type"], "pr:updated", "events: {events:?}");
    assert_eq!(events[0]["workspaceId"], fx.ws_id.as_str());
    assert_eq!(events[0]["data"]["prNumber"], 42);
    assert_eq!(events[0]["data"]["prStatus"], "Merged");
    assert_eq!(events[0]["data"]["activePullRequest"]["status"], "Merged");
    assert_eq!(events[0]["data"]["activePullRequest"]["url"], PR_URL);
    let pooled = events[0]["data"]["pullRequests"]
        .as_array()
        .expect("pullRequests array");
    assert_eq!(pooled.len(), 1, "pool upserted in place: {pooled:?}");
    assert_eq!(pooled[0]["status"], "Merged");

    assert_eq!(
        events[1]["type"], "workspace:displayStatus-changed",
        "events: {events:?}"
    );
    assert_eq!(
        events[1]["data"],
        json!({ "workspaceId": fx.ws_id.as_str(), "displayStatus": "pr_merged" })
    );

    assert_eq!(events[2]["type"], "gitRoot:updated", "events: {events:?}");
    assert_eq!(events[2]["data"]["gitRoot"]["id"], fx.root_id.as_str());
    let root_pool = events[2]["data"]["gitRoot"]["pullRequests"]
        .as_array()
        .expect("root pullRequests array");
    assert_eq!(root_pool.len(), 1, "root pool upserted: {root_pool:?}");
    assert_eq!(root_pool[0]["status"], "Merged");
    assert_eq!(root_pool[0]["url"], PR_URL);

    // The persisted state the sidebar reads back.
    let after = wss_rpc(
        &mut rpc,
        3,
        "workspace.get",
        json!({ "workspaceId": fx.ws_id.as_str() }),
    )
    .await;
    assert_eq!(after["workspace"]["prStatus"], "Merged");
    assert_eq!(after["workspace"]["prUrl"], PR_URL);
    assert_eq!(after["workspace"]["activePullRequest"]["status"], "Merged");
    assert_eq!(after["workspace"]["pullRequests"][0]["status"], "Merged");
    assert_eq!(after["workspace"]["displayStatus"], "pr_merged");

    // A PR nobody references: the hover card still gets its `{ pull }`, and
    // the fold writes nothing (no events).
    let other = wss_rpc(
        &mut rpc,
        4,
        "github.pulls.get",
        json!({ "owner": "o", "repo": "r", "number": 99 }),
    )
    .await;
    assert_eq!(other["pull"]["number"], 99);
    assert_no_events(&mut sub).await;
}
