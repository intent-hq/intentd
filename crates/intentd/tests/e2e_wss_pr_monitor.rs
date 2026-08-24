//! WSS end-to-end for the centralized PR-monitor wire surface (§6.9):
//! `prMonitor.list` / `prMonitor.cancel` / `prMonitor.flush` plus the
//! `prMonitor:*` lifecycle events a subscribed FE renders from. Drives a real
//! [`WsApiServer`] over TLS with bearer-token auth and a pinned self-signed
//! fingerprint (the production transport path) with a stub forge injected via
//! `with_source_control`.
//!
//! There is deliberately NO wire registration method — monitors are
//! agent-owned via the `ws.pr.monitor` MCP binding — so registration here goes
//! through the service surface the binding calls.

#![cfg(unix)]

mod common;

use std::net::Ipv4Addr;
use std::path::PathBuf;
use std::sync::{Arc, Mutex};
use std::time::Duration;

use async_trait::async_trait;
use futures_util::{SinkExt, StreamExt};
use intent_core::{
    now_iso, AgentId, AgentSession, AgentStatus, Result as CoreResult, Workspace,
    WorkspaceActivity, WorkspaceApi, WorkspaceAttention, WorkspaceId, WorkspaceStatus,
};
use intent_services::{EventBus, Services};
use intent_sourcecontrol::{
    AuthStatus, Branch, BranchRules, CheckRun, CheckState, Comment, CommentAnchor, Issue,
    IssueQuery, MergeMethod, MergeOptions, MergeOutcome, MergeRequirementSignals, Mergeability,
    NewPullRequest, Page, PageParams, PrPatch, PrQuery, PrState, PullRequest, Repo, RepoRef,
    Result as ScResult, Review, ReviewComment, ReviewDecision, ReviewThread, ReviewVerdict,
    RollupCheck, ScCapabilities, SourceControl, UserIdentity,
};
use intent_store::{PrMonitorPollUpdate, Store};
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
const TOKEN: &str = "cdcdcdcdcdcdcdcdcdcdcdcdcdcdcdcdcdcdcdcdcdcdcdcdcdcdcdcdcdcdcdcd";

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
        let digest = Sha256::digest(end_entity.as_ref());
        let hex: Vec<String> = digest.iter().map(|b| format!("{b:02X}")).collect();
        if hex.join(":").eq_ignore_ascii_case(&self.fingerprint) {
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

/// Mutable forge state the tests advance between polls.
#[derive(Clone)]
struct ForgeState {
    merged: bool,
    conversation_comments: usize,
    /// `get_pr` call count — one per PR snapshot fetch (dedup assertions).
    get_pr_calls: usize,
    /// `(name, state, required)` triples served as the check rollup.
    checks: Vec<(String, CheckState, bool)>,
    /// The authoritative review decision served by `merge_requirements`.
    review_decision: ReviewDecision,
}

impl Default for ForgeState {
    fn default() -> Self {
        Self {
            merged: false,
            conversation_comments: 0,
            get_pr_calls: 0,
            checks: vec![("build".into(), CheckState::Pending, true)],
            review_decision: ReviewDecision::ReviewRequired,
        }
    }
}

/// Stub forge serving one open PR (#42, one pending required check) whose
/// comment count / merge state the tests move.
#[derive(Clone, Default)]
struct StubForge {
    state: Arc<Mutex<ForgeState>>,
}

impl StubForge {
    fn edit(&self, f: impl FnOnce(&mut ForgeState)) {
        f(&mut self.state.lock().unwrap());
    }
    fn fetches(&self) -> usize {
        self.state.lock().unwrap().get_pr_calls
    }
}

fn unsupported<T>(what: &str) -> ScResult<T> {
    Err(intent_sourcecontrol::Error::Unsupported(what.to_string()))
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
        Ok(AuthStatus {
            authenticated: true,
            login: Some("octocat".into()),
            scopes: vec![],
        })
    }
    async fn get_user(&self) -> ScResult<UserIdentity> {
        unsupported("get_user")
    }
    async fn list_repos(&self, _: PageParams) -> ScResult<Page<Repo>> {
        unsupported("list_repos")
    }
    async fn search_repos(&self, _: &str, _: PageParams) -> ScResult<Page<Repo>> {
        unsupported("search_repos")
    }
    async fn get_repo(&self, _: &str, _: &str) -> ScResult<Repo> {
        unsupported("get_repo")
    }
    async fn list_remote_branches(
        &self,
        _: &str,
        _: &str,
        _: Option<&str>,
        _: PageParams,
    ) -> ScResult<Page<Branch>> {
        unsupported("list_remote_branches")
    }
    async fn get_file_content(
        &self,
        _: &RepoRef,
        _: &str,
        _: Option<&str>,
    ) -> ScResult<Option<String>> {
        Ok(None)
    }
    async fn create_pr(&self, _: &RepoRef, _: NewPullRequest) -> ScResult<PullRequest> {
        unsupported("create_pr")
    }
    async fn get_pr(&self, _: &RepoRef, number: u64) -> ScResult<PullRequest> {
        let merged = {
            let mut state = self.state.lock().unwrap();
            state.get_pr_calls += 1;
            state.merged
        };
        Ok(PullRequest {
            number,
            url: format!("https://github.com/o/r/pull/{number}"),
            title: "Add thing".into(),
            body: None,
            state: if merged {
                PrState::Merged
            } else {
                PrState::Open
            },
            draft: false,
            source_branch: "feature".into(),
            target_branch: "main".into(),
            author: "octocat".into(),
            mergeable: Some(true),
            mergeable_state: Some("clean".into()),
            head_sha: Some("aaaaaaaa".into()),
            created_at: String::new(),
            updated_at: String::new(),
        })
    }
    async fn list_prs(&self, _: &RepoRef, _: PrQuery) -> ScResult<Page<PullRequest>> {
        // Empty page (not `Unsupported`): a merged/closed linked PR triggers
        // relink discovery inside the terminal refresh, and an empty page
        // exercises the clean "no matching open PR" branch.
        Ok(Page {
            items: vec![],
            next_cursor: None,
        })
    }
    async fn update_pr(&self, _: &RepoRef, _: u64, _: PrPatch) -> ScResult<PullRequest> {
        unsupported("update_pr")
    }
    async fn merge_pr(
        &self,
        _: &RepoRef,
        _: u64,
        _: MergeMethod,
        _: MergeOptions,
    ) -> ScResult<MergeOutcome> {
        unsupported("merge_pr")
    }
    async fn mergeability(&self, _: &RepoRef, _: u64) -> ScResult<Mergeability> {
        unsupported("mergeability")
    }
    async fn update_branch(&self, _: &RepoRef, _: u64) -> ScResult<()> {
        unsupported("update_branch")
    }
    async fn submit_review(
        &self,
        _: &RepoRef,
        _: u64,
        _: ReviewVerdict,
        _: Option<String>,
    ) -> ScResult<Review> {
        unsupported("submit_review")
    }
    async fn list_reviews(&self, _: &RepoRef, _: u64) -> ScResult<Vec<Review>> {
        Ok(Vec::new())
    }
    async fn merge_requirements(&self, _: &RepoRef, _: u64) -> ScResult<MergeRequirementSignals> {
        let (checks, review_decision) = {
            let s = self.state.lock().unwrap();
            (s.checks.clone(), s.review_decision)
        };
        Ok(MergeRequirementSignals {
            merge_state_status: Some("CLEAN".into()),
            review_decision: Some(review_decision),
            checks: checks
                .iter()
                .map(|(name, state, required)| RollupCheck {
                    name: name.clone(),
                    state: *state,
                    is_required: *required,
                    url: None,
                })
                .collect(),
            checks_known: true,
            branch_rules: Some(BranchRules {
                required_approving_review_count: Some(1),
                required_conversation_resolution: Some(true),
                required_status_checks: checks
                    .iter()
                    .filter(|(_, _, required)| *required)
                    .map(|(name, _, _)| name.clone())
                    .collect(),
            }),
        })
    }
    async fn list_comments(&self, _: &RepoRef, _: u64) -> ScResult<Vec<Comment>> {
        let n = self.state.lock().unwrap().conversation_comments;
        Ok((0..n)
            .map(|i| Comment {
                id: i.to_string(),
                author: "octocat".into(),
                body: "hi".into(),
                path: None,
                line: None,
                created_at: String::new(),
                url: None,
            })
            .collect())
    }
    async fn add_comment(
        &self,
        _: &RepoRef,
        _: u64,
        _: &str,
        _: Option<CommentAnchor>,
    ) -> ScResult<Comment> {
        unsupported("add_comment")
    }
    async fn list_review_comments(
        &self,
        _: &RepoRef,
        _: u64,
        _: PageParams,
    ) -> ScResult<Page<ReviewComment>> {
        unsupported("list_review_comments")
    }
    async fn reply_to_review_comment(
        &self,
        _: &RepoRef,
        _: u64,
        _: u64,
        _: &str,
    ) -> ScResult<ReviewComment> {
        unsupported("reply_to_review_comment")
    }
    async fn get_review_threads(
        &self,
        _: &RepoRef,
        _: u64,
        _: PageParams,
    ) -> ScResult<Page<ReviewThread>> {
        Ok(Page {
            items: Vec::new(),
            next_cursor: None,
        })
    }
    async fn resolve_thread(&self, _: &str) -> ScResult<bool> {
        unsupported("resolve_thread")
    }
    async fn unresolve_thread(&self, _: &str) -> ScResult<bool> {
        unsupported("unresolve_thread")
    }
    async fn check_runs(&self, _: &RepoRef, _: &str) -> ScResult<Vec<CheckRun>> {
        Ok(Vec::new())
    }
    async fn create_issue(&self, _: &RepoRef, _: &str, _: Option<&str>) -> ScResult<Issue> {
        unsupported("create_issue")
    }
    async fn get_issue(&self, _: &RepoRef, _: u64) -> ScResult<Issue> {
        unsupported("get_issue")
    }
    async fn list_issues(&self, _: &RepoRef, _: IssueQuery) -> ScResult<Page<Issue>> {
        unsupported("list_issues")
    }
}

struct Fixture {
    _ws: WsApiServer,
    port: u16,
    cfg: Arc<ClientConfig>,
    services: Arc<Services>,
    forge: StubForge,
    ws_id: WorkspaceId,
    agent_id: AgentId,
    _dir: TempDir,
}

fn workspace(id: &WorkspaceId) -> Workspace {
    let ts = now_iso();
    Workspace {
        id: id.clone(),
        title: "PR monitor".into(),
        branch: "feature".into(),
        base_ref: None,
        base_commit_sha: None,
        status: WorkspaceStatus::Active,
        status_message: None,
        status_image_asset_id: None,
        activity: WorkspaceActivity::Idle,
        attention: WorkspaceAttention::None,
        created_at: ts.clone(),
        updated_at: ts,
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
        pr_number: None,
        pr_url: None,
        pr_status: None,
        active_pull_request: None,
        pull_requests: None,
        archived: false,
        archived_at: None,
        task_stats: None,
        agent_summary: None,
        diff_summary: None,
        token_usage: None,
        cow_supported: None,
        display_status: None,
        waiting: false,
        checkout_mode: None,
        disk_usage: None,
        pending_delete_at: None,
    }
}

fn agent_session(ws: &WorkspaceId, id: &str) -> AgentSession {
    AgentSession {
        harness_version: intent_core::CURRENT_HARNESS_VERSION.to_string(),
        harness_features: None,
        id: AgentId::from(id),
        workspace_id: ws.clone(),
        parent_agent_id: None,
        backend_session_id: None,
        acp_session_id: None,
        name: "Owner".into(),
        name_explicitly_set: true,
        model: None,
        reasoning_effort: None,
        effort_levels: None,
        provider: None,
        system_prompt: None,
        specialist: None,
        status: AgentStatus::Active,
        is_active: false,
        messages: vec![],
        stats: None,
        task_note_id: None,
        skip_auto_commit: false,
        completion_report: None,
        completion_report_timestamp: None,
        attention_request_kind: None,
        attention_request_reason: None,
        attention_request_timestamp: None,
        delegation_depth: None,
        initial_message: None,
        context_references: None,
        image_blocks: None,
        file_blocks: None,
        is_background: false,
        metadata: None,
        created_at: now_iso(),
        updated_at: now_iso(),
        sandbox_id: None,
        sandbox_path: None,
        sandbox_branch: None,
        stop_reason: None,
        stop_reason_timestamp: None,
        session_corrupted: false,
        pending_delete_at: None,
    }
}

/// Boot a TLS + bearer-auth WSS listener whose services carry the stub forge,
/// a seeded workspace on `o/r`, and an owning agent for the monitor. The
/// debounce is parked at an hour so a detected change stays pending until the
/// test flushes it over the wire.
async fn boot() -> Fixture {
    let short = uuid::Uuid::new_v4().simple().to_string();
    let dir = std::env::temp_dir().join(format!("intentd-pr-monitor-{}", &short[..8]));
    std::fs::create_dir_all(&dir).unwrap();
    let store = Store::open(&dir.join("intentd.db")).await.expect("store");
    let bus = EventBus::new(store.clone());
    let workspaces_root = dir.join("workspaces");
    std::fs::create_dir_all(&workspaces_root).expect("mkdir hermetic root");

    let ws_id = WorkspaceId::new();
    store
        .insert_workspace(&workspace(&ws_id))
        .await
        .expect("seed workspace");
    let agent_id = AgentId::from("agent-prmon-e2e");
    store
        .insert_agent_session(&agent_session(&ws_id, agent_id.as_str()))
        .await
        .expect("seed agent");

    let forge = StubForge::default();
    let services = Arc::new(
        Services::new(store)
            .with_workspaces_root(workspaces_root)
            .with_event_bus(bus.clone())
            .with_source_control(Arc::new(forge.clone()))
            .with_pr_monitor_debounce_seconds(3600),
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
        services,
        forge,
        ws_id,
        agent_id,
        _dir: TempDir(dir),
    }
}

async fn connect(port: u16, cfg: Arc<ClientConfig>) -> TlsWs {
    let url = format!("wss://localhost:{port}/ws?token={TOKEN}");
    common::wss_connect_with_retry(port, cfg, &url).await
}

/// Send one JSON-RPC request and return the full response envelope so callers
/// can assert both `result` and `error` shapes.
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

async fn wss_rpc(ws: &mut TlsWs, id: i64, method: &str, params: Value) -> Value {
    let v = wss_call(ws, id, method, params).await;
    assert!(v.get("error").is_none(), "rpc {method} errored: {v}");
    v["result"].clone()
}

/// Wait for the next `events.event` notification whose `type` matches.
async fn next_event(ws: &mut TlsWs, event_type: &str) -> Value {
    timeout(Duration::from_secs(10), async {
        loop {
            match ws.next().await.unwrap().unwrap() {
                Message::Text(text) => {
                    let v: Value = serde_json::from_str(&text).unwrap();
                    if v["method"] == json!("events.event")
                        && v["params"]["event"]["type"] == json!(event_type)
                    {
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
    .await
    .unwrap_or_else(|_| panic!("timed out waiting for {event_type}"))
}

/// Assert no `event_type` notification arrives within a short quiet window.
async fn assert_no_event(ws: &mut TlsWs, event_type: &str) {
    let quiet = timeout(Duration::from_millis(500), async {
        loop {
            match ws.next().await.unwrap().unwrap() {
                Message::Text(text) => {
                    let v: Value = serde_json::from_str(&text).unwrap();
                    if v["method"] == json!("events.event")
                        && v["params"]["event"]["type"] == json!(event_type)
                    {
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
    assert!(quiet.is_err(), "expected no {event_type} event: {quiet:?}");
}

/// The owning agent's persisted messages, serialized (wake assertions).
async fn owner_messages(fx: &Fixture) -> String {
    let session = fx
        .services
        .store()
        .get_agent_session(&fx.agent_id)
        .await
        .unwrap();
    serde_json::to_string(&session.messages).unwrap()
}

/// Registration (via the service surface the `ws.pr.monitor` binding calls)
/// emits `prMonitor:registered`, and `prMonitor.list` over the wire carries
/// the identity + hover payload PROTOCOL §6.9 documents.
#[tokio::test]
async fn pr_monitor_list_carries_the_ui_payload_over_wss() {
    let fx = boot().await;
    let mut sub = connect(fx.port, fx.cfg.clone()).await;
    let sub_res = wss_rpc(
        &mut sub,
        1,
        "events.subscribe",
        json!({ "eventTypes": ["prMonitor:registered"], "workspaceId": fx.ws_id.as_str() }),
    )
    .await;
    assert!(sub_res["subscriptionId"].is_string(), "sub id: {sub_res}");

    let (monitor, requirements) = fx
        .services
        .pr_monitor_register(&fx.ws_id, &fx.agent_id, "o", "r", 42)
        .await
        .expect("register");
    assert_eq!(requirements.state, "open");

    let evt = next_event(&mut sub, "prMonitor:registered").await;
    assert_eq!(evt["workspaceId"], fx.ws_id.as_str());
    assert_eq!(evt["data"]["monitorId"], monitor.monitor_id.as_str());
    assert_eq!(evt["data"]["repo"], "o/r");
    assert_eq!(evt["data"]["prNumber"], 42);
    assert_eq!(evt["data"]["state"], "active");

    let mut rpc = connect(fx.port, fx.cfg.clone()).await;
    let listed = wss_rpc(
        &mut rpc,
        2,
        "prMonitor.list",
        json!({ "workspaceId": fx.ws_id.as_str() }),
    )
    .await;
    let rows = listed["monitors"].as_array().expect("monitors array");
    assert_eq!(rows.len(), 1, "one monitor: {listed}");
    let row = &rows[0];
    assert_eq!(row["monitorId"], monitor.monitor_id.as_str());
    assert_eq!(row["agentId"], fx.agent_id.as_str());
    assert_eq!(row["repo"], "o/r");
    assert_eq!(row["prNumber"], 42);
    assert_eq!(row["state"], "active");
    assert_eq!(row["title"], "Add thing");
    assert_eq!(row["url"], "https://github.com/o/r/pull/42");
    assert_eq!(row["hasPendingChanges"], false);
    assert_eq!(row["lastSnapshot"]["state"], "open");
    assert_eq!(row["lastSnapshot"]["checks"]["pending"], 1);
    assert_eq!(
        row["lastSnapshot"]["checks"]["pendingRequired"],
        json!(["build"])
    );
    assert_eq!(row["lastSnapshot"]["approvals"]["needed"], 1);
    assert_eq!(row["lastSnapshot"]["threads"]["resolutionRequired"], true);

    // The owning agent's per-turn state snapshot carries the monitor label.
    let snap = fx
        .services
        .agent_snapshot(fx.ws_id.clone(), fx.agent_id.clone())
        .await
        .expect("agent snapshot");
    assert_eq!(snap["prMonitors"], json!(["o/r#42"]), "snapshot: {snap}");
}

/// `prMonitor.flush` over the wire delivers the pending debounced wake right
/// away (emitting `prMonitor:emitted`) and is an explicit no-op afterwards.
#[tokio::test]
async fn pr_monitor_flush_delivers_pending_changes_over_wss() {
    let fx = boot().await;
    let monitor = fx
        .services
        .pr_monitor_register(&fx.ws_id, &fx.agent_id, "o", "r", 42)
        .await
        .expect("register")
        .0;

    // A new comment lands; the hour-long debounce holds the wake.
    fx.forge.edit(|s| s.conversation_comments = 1);
    fx.services.poll_pr_monitors().await;
    assert!(
        !owner_messages(&fx).await.contains("[PR monitor o/r#42]"),
        "the wake must stay held by the debounce window"
    );

    let mut sub = connect(fx.port, fx.cfg.clone()).await;
    let sub_res = wss_rpc(
        &mut sub,
        1,
        "events.subscribe",
        json!({ "eventTypes": ["prMonitor:emitted"], "workspaceId": fx.ws_id.as_str() }),
    )
    .await;
    assert!(sub_res["subscriptionId"].is_string(), "sub id: {sub_res}");

    let mut rpc = connect(fx.port, fx.cfg.clone()).await;
    // The pending state is visible to the UI before the flush.
    let listed = wss_rpc(
        &mut rpc,
        2,
        "prMonitor.list",
        json!({ "workspaceId": fx.ws_id.as_str() }),
    )
    .await;
    assert_eq!(listed["monitors"][0]["hasPendingChanges"], true);
    assert!(listed["monitors"][0]["lastChangeAt"].is_string());

    let flushed = wss_rpc(
        &mut rpc,
        3,
        "prMonitor.flush",
        json!({ "workspaceId": fx.ws_id.as_str(), "monitorId": monitor.monitor_id.as_str() }),
    )
    .await;
    assert_eq!(flushed, json!({ "ok": true, "flushed": true }));

    let evt = next_event(&mut sub, "prMonitor:emitted").await;
    assert_eq!(evt["data"]["monitorId"], monitor.monitor_id.as_str());
    assert!(
        owner_messages(&fx).await.contains("[PR monitor o/r#42]"),
        "the flush must deliver the consolidated wake"
    );

    // Nothing pending → an explicit no-op, not an error.
    let again = wss_rpc(
        &mut rpc,
        4,
        "prMonitor.flush",
        json!({ "workspaceId": fx.ws_id.as_str(), "monitorId": monitor.monitor_id.as_str() }),
    )
    .await;
    assert_eq!(again, json!({ "ok": true, "flushed": false }));
}

/// `prMonitor.flush { check: true }` over the wire re-polls the PR on demand
/// first: a change the loop has NOT seen yet is fetched fresh and the wake
/// delivered immediately (emitting `prMonitor:emitted`), while a checked
/// flush with nothing changed returns `flushed: false`. A non-boolean
/// `check` is `-32602`.
#[tokio::test]
async fn pr_monitor_flush_with_check_repolls_on_demand_over_wss() {
    let fx = boot().await;
    let monitor = fx
        .services
        .pr_monitor_register(&fx.ws_id, &fx.agent_id, "o", "r", 42)
        .await
        .expect("register")
        .0;

    let mut sub = connect(fx.port, fx.cfg.clone()).await;
    let sub_res = wss_rpc(
        &mut sub,
        1,
        "events.subscribe",
        json!({ "eventTypes": ["prMonitor:emitted"], "workspaceId": fx.ws_id.as_str() }),
    )
    .await;
    assert!(sub_res["subscriptionId"].is_string(), "sub id: {sub_res}");

    // A comment lands AFTER registration, with no sweep in between: the
    // un-checked flush sees no pending set, the checked one re-polls.
    fx.forge.edit(|s| s.conversation_comments = 1);
    let mut rpc = connect(fx.port, fx.cfg.clone()).await;
    let unchecked = wss_rpc(
        &mut rpc,
        2,
        "prMonitor.flush",
        json!({ "workspaceId": fx.ws_id.as_str(), "monitorId": monitor.monitor_id.as_str() }),
    )
    .await;
    assert_eq!(unchecked, json!({ "ok": true, "flushed": false }));

    let before = fx.forge.fetches();
    let checked = wss_rpc(
        &mut rpc,
        3,
        "prMonitor.flush",
        json!({
            "workspaceId": fx.ws_id.as_str(),
            "monitorId": monitor.monitor_id.as_str(),
            "check": true,
        }),
    )
    .await;
    assert_eq!(checked, json!({ "ok": true, "flushed": true }));
    assert_eq!(fx.forge.fetches(), before + 1, "one on-demand fetch");

    let evt = next_event(&mut sub, "prMonitor:emitted").await;
    assert_eq!(evt["data"]["monitorId"], monitor.monitor_id.as_str());
    assert!(
        owner_messages(&fx).await.contains("[PR monitor o/r#42]"),
        "the checked flush must deliver the consolidated wake"
    );

    // Nothing changed since the emit → the check finds nothing to flush.
    let quiet = wss_rpc(
        &mut rpc,
        4,
        "prMonitor.flush",
        json!({
            "workspaceId": fx.ws_id.as_str(),
            "monitorId": monitor.monitor_id.as_str(),
            "check": true,
        }),
    )
    .await;
    assert_eq!(quiet, json!({ "ok": true, "flushed": false }));

    // A non-boolean `check` is rejected with -32602 before any side effect.
    let bad = wss_call(
        &mut rpc,
        5,
        "prMonitor.flush",
        json!({
            "workspaceId": fx.ws_id.as_str(),
            "monitorId": monitor.monitor_id.as_str(),
            "check": "yes",
        }),
    )
    .await;
    assert_eq!(bad["error"]["code"], json!(-32602), "envelope: {bad}");
}

/// `prMonitor.cancel` over the wire (the FE path) cancels the monitor, removes
/// it from `prMonitor.list`, emits `prMonitor:cancelled`, and notifies the
/// owning agent — unlike an agent's own `ws.pr.unmonitor`.
#[tokio::test]
async fn pr_monitor_cancel_removes_the_row_and_notifies_the_owner_over_wss() {
    let fx = boot().await;
    let monitor = fx
        .services
        .pr_monitor_register(&fx.ws_id, &fx.agent_id, "o", "r", 42)
        .await
        .expect("register")
        .0;

    let mut sub = connect(fx.port, fx.cfg.clone()).await;
    let sub_res = wss_rpc(
        &mut sub,
        1,
        "events.subscribe",
        json!({ "eventTypes": ["prMonitor:cancelled"], "workspaceId": fx.ws_id.as_str() }),
    )
    .await;
    assert!(sub_res["subscriptionId"].is_string(), "sub id: {sub_res}");

    let mut rpc = connect(fx.port, fx.cfg.clone()).await;
    let cancelled = wss_rpc(
        &mut rpc,
        2,
        "prMonitor.cancel",
        json!({ "workspaceId": fx.ws_id.as_str(), "monitorId": monitor.monitor_id.as_str() }),
    )
    .await;
    assert_eq!(cancelled["ok"], true);
    assert_eq!(cancelled["monitor"]["state"], "cancelled");

    let evt = next_event(&mut sub, "prMonitor:cancelled").await;
    assert_eq!(evt["data"]["monitorId"], monitor.monitor_id.as_str());
    assert_eq!(evt["data"]["state"], "cancelled");

    let listed = wss_rpc(
        &mut rpc,
        3,
        "prMonitor.list",
        json!({ "workspaceId": fx.ws_id.as_str() }),
    )
    .await;
    assert_eq!(
        listed,
        json!({ "monitors": [] }),
        "cancelled rows leave the list surface"
    );
    assert!(
        owner_messages(&fx).await.contains("cancelled from the app"),
        "an app-side cancel notifies the owning agent"
    );

    // An unknown monitor id surfaces as `-32602` (NotFound → invalid params),
    // matching the `hook.cancel` precedent.
    let resp = wss_call(
        &mut rpc,
        4,
        "prMonitor.cancel",
        json!({ "workspaceId": fx.ws_id.as_str(), "monitorId": "no-such-monitor" }),
    )
    .await;
    assert_eq!(resp["error"]["code"], -32602, "error envelope: {resp}");
}

/// A merged PR terminalizes the monitor: `prMonitor:completed` fires, the
/// owner is woken immediately, and the `completed` row STAYS visible in
/// `prMonitor.list` so merged PRs remain in the UI's list. The wake's
/// persisted user row carries the PROTOCOL §5.42 `messageMetadata`
/// (`type`/`monitorId`/`repo`/`prNumber`/`reason` + the baseline-sourced
/// `url`), asserted through `agent.getConversation` over the wire — the
/// client-visible read path.
#[tokio::test]
async fn merged_pr_completes_the_monitor_but_keeps_it_listed_over_wss() {
    let fx = boot().await;
    let monitor = fx
        .services
        .pr_monitor_register(&fx.ws_id, &fx.agent_id, "o", "r", 42)
        .await
        .expect("register")
        .0;

    let mut sub = connect(fx.port, fx.cfg.clone()).await;
    let sub_res = wss_rpc(
        &mut sub,
        1,
        "events.subscribe",
        json!({ "eventTypes": ["prMonitor:completed"], "workspaceId": fx.ws_id.as_str() }),
    )
    .await;
    assert!(sub_res["subscriptionId"].is_string(), "sub id: {sub_res}");

    fx.forge.edit(|s| s.merged = true);
    fx.services.poll_pr_monitors().await;

    let evt = next_event(&mut sub, "prMonitor:completed").await;
    assert_eq!(evt["data"]["monitorId"], monitor.monitor_id.as_str());
    assert_eq!(evt["data"]["state"], "completed");
    let text = owner_messages(&fx).await;
    assert!(
        text.contains("was MERGED") && text.contains("Monitoring has STOPPED"),
        "the terminal wake must fire immediately: {text}"
    );

    let mut rpc = connect(fx.port, fx.cfg.clone()).await;
    let listed = wss_rpc(
        &mut rpc,
        2,
        "prMonitor.list",
        json!({ "workspaceId": fx.ws_id.as_str() }),
    )
    .await;
    let rows = listed["monitors"].as_array().expect("monitors array");
    assert_eq!(rows.len(), 1, "completed rows stay visible: {listed}");
    assert_eq!(rows[0]["state"], "completed");
    assert_eq!(rows[0]["lastSnapshot"]["state"], "merged");

    // The client-visible transcript row carries the wake's messageMetadata,
    // including the baseline-sourced `url` (PROTOCOL §5.42).
    let convo = wss_rpc(
        &mut rpc,
        3,
        "agent.getConversation",
        json!({ "workspaceId": fx.ws_id.as_str(), "agentId": fx.agent_id.as_str() }),
    )
    .await;
    let messages = convo["messages"].as_array().expect("messages array");
    let wake = messages
        .iter()
        .find(|m| m["metadata"]["type"] == json!("pr_monitor_wake"))
        .unwrap_or_else(|| panic!("wake row carries pr_monitor_wake metadata: {convo}"));
    let metadata = &wake["metadata"];
    assert_eq!(metadata["monitorId"], monitor.monitor_id.as_str());
    assert_eq!(metadata["repo"], "o/r");
    assert_eq!(metadata["prNumber"], 42);
    assert_eq!(metadata["reason"], "completed");
    assert_eq!(metadata["url"], "https://github.com/o/r/pull/42");
}

/// A merged PR on a LINKED workspace refreshes the persisted PR linkage as
/// part of the terminal completion (intent-hq/monorepo#2094): `pr:updated`
/// fires over the wire and `workspace.get` serves `prStatus: "Merged"` +
/// the refreshed `activePullRequest` — with no explicit `pr.refresh` call.
#[tokio::test]
async fn merged_pr_terminal_wake_refreshes_workspace_linkage_over_wss() {
    let fx = boot().await;
    // Link the fixture workspace to the monitored PR up front (the fixture
    // branch "feature" matches the stub PR's head ref).
    let mut row = fx.services.store().get_workspace(&fx.ws_id).await.unwrap();
    row.pr_number = Some(42);
    row.pr_url = Some("https://github.com/o/r/pull/42".into());
    row.pr_status = Some(intent_core::PullRequestStatus::Open);
    fx.services.store().update_workspace(&row).await.unwrap();
    fx.services
        .pr_monitor_register(&fx.ws_id, &fx.agent_id, "o", "r", 42)
        .await
        .expect("register");

    let mut sub = connect(fx.port, fx.cfg.clone()).await;
    let sub_res = wss_rpc(
        &mut sub,
        1,
        "events.subscribe",
        json!({ "eventTypes": ["pr:updated"], "workspaceId": fx.ws_id.as_str() }),
    )
    .await;
    assert!(sub_res["subscriptionId"].is_string(), "sub id: {sub_res}");

    fx.forge.edit(|s| s.merged = true);
    fx.services.poll_pr_monitors().await;

    // The terminal completion itself emitted the linkage delta — no
    // `pr.refresh` call anywhere in this test.
    let evt = next_event(&mut sub, "pr:updated").await;
    assert_eq!(evt["data"]["workspaceId"], fx.ws_id.as_str());
    assert_eq!(evt["data"]["prNumber"], 42);
    assert_eq!(evt["data"]["prStatus"], "Merged");
    assert_eq!(evt["data"]["activePullRequest"]["status"], "Merged");

    // The client-visible read path serves the refreshed linkage.
    let mut rpc = connect(fx.port, fx.cfg.clone()).await;
    let got = wss_rpc(
        &mut rpc,
        2,
        "workspace.get",
        json!({ "workspaceId": fx.ws_id.as_str() }),
    )
    .await;
    let ws = &got["workspace"];
    assert_eq!(ws["prStatus"], "Merged", "workspace.get: {got}");
    assert_eq!(ws["prNumber"], 42, "link retained (empty discovery page)");
    assert_eq!(ws["activePullRequest"]["status"], "Merged");
}

/// Active-monitor `waiting` flag + PR-rung derivation over the wire
/// (PROTOCOL §5.1/§6.5): registering a monitor on an open PR whose full
/// merge-requirements checklist is clear (checks passed, approved, no
/// unresolved threads — not merely conflict-free) moves the derived
/// `displayStatus` to `pr_ready` — both `workspace.get` and
/// `workspace.list` serve it with additive `waiting: true` while the
/// monitor is ACTIVE — and cancelling it over the wire (`prMonitor.cancel`)
/// lapses the signal back to `idle` and drops the field (omitted, never
/// `false`). Both transitions emit `workspace:displayStatus-changed`.
#[tokio::test]
async fn active_monitor_serves_waiting_and_pr_ready_over_wss() {
    let fx = boot().await;
    // Clear every checklist blocker: the required check passes and the
    // review decision reads approved (the stub serves no threads).
    fx.forge.edit(|s| {
        s.checks[0].1 = CheckState::Success;
        s.review_decision = ReviewDecision::Approved;
    });
    let mut rpc = connect(fx.port, fx.cfg.clone()).await;

    // Baseline read: the seeded workspace (no tasks, no hooks, no running
    // agents) serves `idle` with the waiting field omitted.
    let got = wss_rpc(
        &mut rpc,
        1,
        "workspace.get",
        json!({ "workspaceId": fx.ws_id.as_str() }),
    )
    .await;
    assert_eq!(got["workspace"]["displayStatus"], "idle", "baseline: {got}");
    assert!(
        got["workspace"].get("waiting").is_none(),
        "baseline omits waiting: {got}"
    );

    // Subscriber registered BEFORE the transitions so we see both
    // displayStatus emits.
    let mut sub = connect(fx.port, fx.cfg.clone()).await;
    let sub_res = wss_rpc(
        &mut sub,
        2,
        "events.subscribe",
        json!({
            "eventTypes": ["workspace:displayStatus-changed"],
            "workspaceId": fx.ws_id.as_str(),
        }),
    )
    .await;
    assert!(sub_res["subscriptionId"].is_string(), "sub id: {sub_res}");

    // Registration (via the service surface the `ws.pr.monitor` binding
    // calls): the open mergeable PR promotes the derivation to `pr_ready`
    // and raises the orthogonal flag on both read paths.
    let monitor = fx
        .services
        .pr_monitor_register(&fx.ws_id, &fx.agent_id, "o", "r", 42)
        .await
        .expect("register")
        .0;
    let evt = next_event(&mut sub, "workspace:displayStatus-changed").await;
    assert_eq!(evt["data"]["displayStatus"], "pr_ready", "register: {evt}");
    let got = wss_rpc(
        &mut rpc,
        3,
        "workspace.get",
        json!({ "workspaceId": fx.ws_id.as_str() }),
    )
    .await;
    assert_eq!(
        got["workspace"]["displayStatus"], "pr_ready",
        "workspace.get serves the monitor-derived PR rung: {got}"
    );
    assert_eq!(
        got["workspace"]["waiting"],
        json!(true),
        "workspace.get carries waiting: true while the monitor is active: {got}"
    );
    let listed = wss_rpc(&mut rpc, 4, "workspace.list", json!({})).await;
    let row = listed["workspaces"]
        .as_array()
        .expect("workspaces array")
        .iter()
        .find(|w| w["id"] == json!(fx.ws_id.as_str()))
        .cloned()
        .expect("seeded workspace listed");
    assert_eq!(
        row["displayStatus"], "pr_ready",
        "workspace.list serves the monitor-derived PR rung: {row}"
    );
    assert_eq!(
        row["waiting"],
        json!(true),
        "workspace.list carries waiting: true while the monitor is active: {row}"
    );

    // Cancelling over the wire (the FE path) lapses the signal: both read
    // paths return to the base rollup with the field omitted.
    let cancelled = wss_rpc(
        &mut rpc,
        5,
        "prMonitor.cancel",
        json!({ "workspaceId": fx.ws_id.as_str(), "monitorId": monitor.monitor_id.as_str() }),
    )
    .await;
    assert_eq!(cancelled["ok"], true, "{cancelled}");
    let evt = next_event(&mut sub, "workspace:displayStatus-changed").await;
    assert_eq!(evt["data"]["displayStatus"], "idle", "cancel: {evt}");
    let got = wss_rpc(
        &mut rpc,
        6,
        "workspace.get",
        json!({ "workspaceId": fx.ws_id.as_str() }),
    )
    .await;
    assert_eq!(
        got["workspace"]["displayStatus"], "idle",
        "workspace.get returns to the base rollup after cancel: {got}"
    );
    assert!(
        got["workspace"].get("waiting").is_none(),
        "workspace.get omits waiting after cancel: {got}"
    );
    let listed = wss_rpc(&mut rpc, 7, "workspace.list", json!({})).await;
    let row = listed["workspaces"]
        .as_array()
        .expect("workspaces array")
        .iter()
        .find(|w| w["id"] == json!(fx.ws_id.as_str()))
        .cloned()
        .expect("seeded workspace listed");
    assert!(
        row.get("waiting").is_none(),
        "workspace.list omits waiting after cancel: {row}"
    );
}

/// `workspace.archive` over the wire cancels the workspace's ACTIVE PR
/// monitors (intent-hq/monorepo#1828): the archive sweep persists the row as
/// `cancelled`, emits `prMonitor:cancelled`, and wakes the owner with an
/// archive-specific notice, so an archived workspace never serves
/// `waiting: true` off a stale monitor signal. The sweep runs on a detached
/// tail after the archive RPC returns, so assertions ride the subscribed
/// events.
#[tokio::test]
async fn archive_over_wss_cancels_active_monitors_and_drops_waiting() {
    let fx = boot().await;
    let mut rpc = connect(fx.port, fx.cfg.clone()).await;

    // Baseline read: idle, waiting omitted.
    let got = wss_rpc(
        &mut rpc,
        1,
        "workspace.get",
        json!({ "workspaceId": fx.ws_id.as_str() }),
    )
    .await;
    assert_eq!(got["workspace"]["displayStatus"], "idle", "baseline: {got}");

    // Subscriber registered BEFORE the transitions so we miss nothing.
    let mut sub = connect(fx.port, fx.cfg.clone()).await;
    let sub_res = wss_rpc(
        &mut sub,
        2,
        "events.subscribe",
        json!({
            "eventTypes": ["prMonitor:cancelled", "workspace:displayStatus-changed"],
            "workspaceId": fx.ws_id.as_str(),
        }),
    )
    .await;
    assert!(sub_res["subscriptionId"].is_string(), "sub id: {sub_res}");

    // Registration (via the service surface the `ws.pr.monitor` binding
    // calls) sets the orthogonal waiting flag on the read path.
    let monitor = fx
        .services
        .pr_monitor_register(&fx.ws_id, &fx.agent_id, "o", "r", 42)
        .await
        .expect("register")
        .0;
    let got = wss_rpc(
        &mut rpc,
        3,
        "workspace.get",
        json!({ "workspaceId": fx.ws_id.as_str() }),
    )
    .await;
    assert_eq!(got["workspace"]["waiting"], json!(true), "{got}");

    // Archive over the wire — the FE path (PROTOCOL §5 `workspace.archive`).
    let archived = wss_rpc(
        &mut rpc,
        4,
        "workspace.archive",
        json!({ "workspaceId": fx.ws_id.as_str() }),
    )
    .await;
    assert_eq!(archived["workspace"]["archived"], true, "{archived}");

    // The detached archive sweep cancels the monitor.
    let evt = next_event(&mut sub, "prMonitor:cancelled").await;
    assert_eq!(evt["data"]["monitorId"], monitor.monitor_id.as_str());
    assert_eq!(evt["data"]["state"], "cancelled");

    // Cancelled rows leave the list surface, and the owner learned why.
    let listed = wss_rpc(
        &mut rpc,
        5,
        "prMonitor.list",
        json!({ "workspaceId": fx.ws_id.as_str() }),
    )
    .await;
    assert_eq!(
        listed,
        json!({ "monitors": [] }),
        "no active monitors survive the archive sweep"
    );
    assert!(
        owner_messages(&fx).await.contains("workspace was archived"),
        "the archive sweep notifies the owning agent"
    );

    // The archived workspace's read path drops the waiting field, returns
    // to the base rollup (the cancelled monitor's open-PR signal lapsed),
    // and never raised needs_attention.
    let got = wss_rpc(
        &mut rpc,
        6,
        "workspace.get",
        json!({ "workspaceId": fx.ws_id.as_str() }),
    )
    .await;
    assert_eq!(got["workspace"]["archived"], true);
    assert!(
        got["workspace"].get("waiting").is_none(),
        "workspace.get omits waiting after the archive sweep: {got}"
    );
    assert_eq!(
        got["workspace"]["displayStatus"], "idle",
        "the cancelled monitor's open-PR signal lapses with the sweep: {got}"
    );
}

/// Cross-repo monitor feeds the displayStatus derivation (§6.5): the
/// workspace lives on `o/r` with NO PR linkage of its own, yet a monitor on
/// a DIFFERENT repo's PR (`other/repo#7`) serves `pr_ready` from
/// `workspace.get`, and the PR merging flips the derivation to `pr_merged`
/// off the COMPLETED monitor's final snapshot — with no further transition
/// after the terminal one.
#[tokio::test]
async fn cross_repo_monitor_drives_pr_ready_then_pr_merged_over_wss() {
    let fx = boot().await;
    // Clear every checklist blocker so the open PR reads truly mergeable.
    fx.forge.edit(|s| {
        s.checks[0].1 = CheckState::Success;
        s.review_decision = ReviewDecision::Approved;
    });
    let mut rpc = connect(fx.port, fx.cfg.clone()).await;

    // Baseline read seeds the transition emitter's last-observed status
    // (a seed never emits), so the register below emits its promotion.
    let got = wss_rpc(
        &mut rpc,
        0,
        "workspace.get",
        json!({ "workspaceId": fx.ws_id.as_str() }),
    )
    .await;
    assert_eq!(got["workspace"]["displayStatus"], "idle", "baseline: {got}");

    let mut sub = connect(fx.port, fx.cfg.clone()).await;
    let sub_res = wss_rpc(
        &mut sub,
        1,
        "events.subscribe",
        json!({
            "eventTypes": ["workspace:displayStatus-changed"],
            "workspaceId": fx.ws_id.as_str(),
        }),
    )
    .await;
    assert!(sub_res["subscriptionId"].is_string(), "sub id: {sub_res}");

    // Watch a PR on a repo that is NOT the workspace's own `o/r` (the stub
    // forge serves any repo ref): the open truly-mergeable PR reads
    // pr_ready.
    fx.services
        .pr_monitor_register(&fx.ws_id, &fx.agent_id, "other", "repo", 7)
        .await
        .expect("register cross-repo");
    let evt = next_event(&mut sub, "workspace:displayStatus-changed").await;
    assert_eq!(evt["data"]["displayStatus"], "pr_ready", "register: {evt}");
    let got = wss_rpc(
        &mut rpc,
        2,
        "workspace.get",
        json!({ "workspaceId": fx.ws_id.as_str() }),
    )
    .await;
    assert_eq!(
        got["workspace"]["displayStatus"], "pr_ready",
        "a cross-repo monitor feeds the PR rungs without any linkage: {got}"
    );
    assert!(
        got["workspace"]["prNumber"].is_null(),
        "no workspace PR linkage involved: {got}"
    );

    // The PR merges: the completed monitor's final snapshot keeps the
    // derivation at pr_merged instead of falling back to idle.
    fx.forge.edit(|s| s.merged = true);
    fx.services.poll_pr_monitors().await;
    let evt = next_event(&mut sub, "workspace:displayStatus-changed").await;
    assert_eq!(evt["data"]["displayStatus"], "pr_merged", "merge: {evt}");
    let got = wss_rpc(
        &mut rpc,
        3,
        "workspace.get",
        json!({ "workspaceId": fx.ws_id.as_str() }),
    )
    .await;
    assert_eq!(
        got["workspace"]["displayStatus"], "pr_merged",
        "the completed monitor's merged snapshot persists the stage: {got}"
    );
    assert!(
        got["workspace"].get("waiting").is_none(),
        "no active monitor left, waiting omitted: {got}"
    );
    assert_no_event(&mut sub, "workspace:displayStatus-changed").await;
}

/// Idle-visibility (unified external-wait, mirrors the hook-lifecycle
/// `waitingOnHooks` e2e coverage): `agent.get` over the wire overlays the
/// light `waitingOnPrMonitors` list on the `AgentLite` projection for an
/// agent owning an ACTIVE monitor, and omits the field entirely for a
/// monitor-less agent. (Emit-site coverage of the same stamp on the
/// `agent:idle` event itself lives in `intent-services` unit tests, which
/// can reach the private `annotate_waiting_on_pr_monitors` helper directly;
/// this fixture has no ACP provider to drive a real agent turn.)
#[tokio::test]
async fn agent_get_surfaces_waiting_on_pr_monitors_over_wss() {
    let fx = boot().await;
    let monitor = fx
        .services
        .pr_monitor_register(&fx.ws_id, &fx.agent_id, "o", "r", 42)
        .await
        .expect("register")
        .0;

    let mut rpc = connect(fx.port, fx.cfg.clone()).await;
    let got = wss_rpc(
        &mut rpc,
        1,
        "agent.get",
        json!({ "workspaceId": fx.ws_id.as_str(), "agentId": fx.agent_id.as_str() }),
    )
    .await;
    let waiting = got["agent"]["waitingOnPrMonitors"]
        .as_array()
        .unwrap_or_else(|| panic!("agent.get serves waitingOnPrMonitors: {got}"));
    assert_eq!(waiting.len(), 1);
    assert_eq!(waiting[0]["monitorId"], monitor.monitor_id.as_str());
    assert_eq!(waiting[0]["repo"], "o/r");
    assert_eq!(waiting[0]["prNumber"], 42);
    assert_eq!(waiting[0]["title"], "Add thing");
    assert!(
        waiting[0].get("lastSnapshot").is_none(),
        "payload stays light: {got}"
    );

    // Cancelled: no longer active, field omitted entirely (never `[]`).
    fx.services
        .pr_monitor_cancel(&fx.ws_id, &monitor.monitor_id, Some(&fx.agent_id))
        .await
        .expect("cancel");
    let got_after = wss_rpc(
        &mut rpc,
        2,
        "agent.get",
        json!({ "workspaceId": fx.ws_id.as_str(), "agentId": fx.agent_id.as_str() }),
    )
    .await;
    assert!(
        got_after["agent"].get("waitingOnPrMonitors").is_none(),
        "field omitted once the monitor is cancelled: {got_after}"
    );
}

/// The production loop's sweep (`poll_due_pr_monitors`) verified end-to-end
/// over the wire: a just-registered monitor is skipped as fresh (no forge
/// fetch), a due sweep fetches ONE shared snapshot for two sibling monitors
/// on the same PR, both siblings' pending changes surface via
/// `prMonitor.list`, and `prMonitor.flush` delivers each owner's wake and
/// emits `prMonitor:emitted` — the same behavior FEs observe from the loop.
#[tokio::test]
async fn due_sweep_dedups_fetches_and_surfaces_changes_over_wss() {
    let fx = boot().await;
    let sibling_id = AgentId::from("agent-prmon-sibling");
    fx.services
        .store()
        .insert_agent_session(&agent_session(&fx.ws_id, sibling_id.as_str()))
        .await
        .expect("seed sibling agent");

    // Two monitors on the SAME PR (a monitor is unique per (agent, repo, pr)),
    // registered via the service surface the `ws.pr.monitor` binding calls.
    let first = fx
        .services
        .pr_monitor_register(&fx.ws_id, &fx.agent_id, "o", "r", 42)
        .await
        .expect("register first")
        .0;
    let second = fx
        .services
        .pr_monitor_register(&fx.ws_id, &sibling_id, "o", "r", 42)
        .await
        .expect("register sibling")
        .0;

    // Registration just stamped `lastPolledAt`: the loop-driven sweep skips
    // both monitors as fresh — zero forge fetches.
    let before = fx.forge.fetches();
    fx.services.poll_due_pr_monitors().await;
    assert_eq!(fx.forge.fetches(), before, "fresh monitors are skipped");

    // The PR moves and both monitors go stale (backdated `lastPolledAt`):
    // the next due sweep fetches ONE shared snapshot for the pair.
    fx.forge.edit(|s| s.conversation_comments = 1);
    for monitor in [&first, &second] {
        let row = fx
            .services
            .store()
            .get_pr_monitor(&monitor.monitor_id)
            .await
            .expect("load row");
        assert!(fx
            .services
            .store()
            .update_pr_monitor_poll(
                &monitor.monitor_id,
                PrMonitorPollUpdate {
                    last_snapshot: row.last_snapshot.as_deref(),
                    baseline_snapshot: row.baseline_snapshot.as_deref(),
                    pending_changes: &row.pending_changes,
                    pending_since: row.pending_since.as_deref(),
                    last_change_at: row.last_change_at.as_deref(),
                    last_polled_at: Some("2020-01-01T00:00:00Z"),
                    last_error: None,
                    updated_at: &now_iso(),
                    expected_updated_at: &row.updated_at,
                },
            )
            .await
            .expect("backdate lastPolledAt"));
    }
    let before = fx.forge.fetches();
    fx.services.poll_due_pr_monitors().await;
    assert_eq!(
        fx.forge.fetches(),
        before + 1,
        "one shared fetch serves both sibling monitors"
    );

    // Both siblings diffed the shared snapshot against their OWN baselines:
    // `prMonitor.list` over the wire shows both rows pending (the hour-long
    // debounce holds the wakes).
    let mut rpc = connect(fx.port, fx.cfg.clone()).await;
    let listed = wss_rpc(
        &mut rpc,
        1,
        "prMonitor.list",
        json!({ "workspaceId": fx.ws_id.as_str() }),
    )
    .await;
    let rows = listed["monitors"].as_array().expect("monitors array");
    assert_eq!(rows.len(), 2, "both monitors listed: {listed}");
    for row in rows {
        assert_eq!(row["hasPendingChanges"], true, "row pending: {row}");
    }

    // Flushing the first monitor delivers ITS owner's wake (and emits the
    // event) without draining the sibling's independent pending state.
    let mut sub = connect(fx.port, fx.cfg.clone()).await;
    let sub_res = wss_rpc(
        &mut sub,
        2,
        "events.subscribe",
        json!({ "eventTypes": ["prMonitor:emitted"], "workspaceId": fx.ws_id.as_str() }),
    )
    .await;
    assert!(sub_res["subscriptionId"].is_string(), "sub id: {sub_res}");
    let flushed = wss_rpc(
        &mut rpc,
        3,
        "prMonitor.flush",
        json!({ "workspaceId": fx.ws_id.as_str(), "monitorId": first.monitor_id.as_str() }),
    )
    .await;
    assert_eq!(flushed, json!({ "ok": true, "flushed": true }));
    let evt = next_event(&mut sub, "prMonitor:emitted").await;
    assert_eq!(evt["data"]["monitorId"], first.monitor_id.as_str());
    assert!(
        owner_messages(&fx).await.contains("[PR monitor o/r#42]"),
        "the flush delivers the owner's consolidated wake"
    );
    let listed_after = wss_rpc(
        &mut rpc,
        4,
        "prMonitor.list",
        json!({ "workspaceId": fx.ws_id.as_str() }),
    )
    .await;
    for row in listed_after["monitors"].as_array().expect("monitors array") {
        let expect_pending = row["monitorId"] != json!(first.monitor_id.as_str());
        assert_eq!(
            row["hasPendingChanges"],
            json!(expect_pending),
            "sibling pending state independent: {row}"
        );
    }
}

/// Intermediate check successes stay quiet over the production transport: a
/// `pending → passed` transition accumulates NO pending change and emits NO
/// `prMonitor:changed`; the suite completing produces exactly ONE aggregate
/// line, and the consolidated wake carries it.
#[tokio::test]
async fn intermediate_check_successes_stay_quiet_until_the_completion_aggregate_over_wss() {
    let fx = boot().await;
    // Two pending checks so one can pass while the suite is still running.
    fx.forge.edit(|s| {
        s.checks = vec![
            ("build".into(), CheckState::Pending, true),
            ("lint".into(), CheckState::Pending, false),
        ];
    });
    let monitor = fx
        .services
        .pr_monitor_register(&fx.ws_id, &fx.agent_id, "o", "r", 42)
        .await
        .expect("register")
        .0;

    let mut sub = connect(fx.port, fx.cfg.clone()).await;
    let sub_res = wss_rpc(
        &mut sub,
        1,
        "events.subscribe",
        json!({ "eventTypes": ["prMonitor:changed"], "workspaceId": fx.ws_id.as_str() }),
    )
    .await;
    assert!(sub_res["subscriptionId"].is_string(), "sub id: {sub_res}");

    // Intermediate success: `build` passes while `lint` is still pending —
    // the diff is empty, so nothing accumulates on the row.
    fx.forge.edit(|s| s.checks[0].1 = CheckState::Success);
    fx.services.poll_pr_monitors().await;

    let mut rpc = connect(fx.port, fx.cfg.clone()).await;
    let listed = wss_rpc(
        &mut rpc,
        2,
        "prMonitor.list",
        json!({ "workspaceId": fx.ws_id.as_str() }),
    )
    .await;
    let row = &listed["monitors"][0];
    assert_eq!(
        row["hasPendingChanges"], false,
        "an intermediate success must not accumulate a pending change: {row}"
    );
    assert_eq!(row["pendingChanges"], json!([]));
    assert_eq!(row["lastSnapshot"]["checks"]["pending"], 1);

    // The suite completes: the LAST pending check passing produces exactly
    // one aggregate line. The first `prMonitor:changed` seen on the wire is
    // this completion — proving the intermediate success never emitted.
    fx.forge.edit(|s| s.checks[1].1 = CheckState::Success);
    fx.services.poll_pr_monitors().await;

    let evt = next_event(&mut sub, "prMonitor:changed").await;
    assert_eq!(evt["data"]["monitorId"], monitor.monitor_id.as_str());
    assert_eq!(
        evt["data"]["changes"],
        json!(["all checks passed (2)"]),
        "the completion emit carries ONLY the aggregate line: {evt}"
    );

    // The consolidated wake delivers the aggregate line, with no per-check
    // success lines.
    let flushed = wss_rpc(
        &mut rpc,
        3,
        "prMonitor.flush",
        json!({ "workspaceId": fx.ws_id.as_str(), "monitorId": monitor.monitor_id.as_str() }),
    )
    .await;
    assert_eq!(flushed, json!({ "ok": true, "flushed": true }));
    let text = owner_messages(&fx).await;
    assert!(
        text.contains("all checks passed (2)"),
        "the wake carries the aggregate line: {text}"
    );
    assert!(
        !text.contains("check build") && !text.contains("check lint"),
        "no per-check success lines in the wake: {text}"
    );
}

/// Coalesced-diff semantics end-to-end: `pendingChanges` over the wire is the
/// NET diff against the emit baseline — A→B→C renders one initial→final line,
/// and a full revert (C→A) empties the pending set, resets the debounce
/// anchors, and produces NO owner wake even though the debounce window had
/// already elapsed (the pre-coalescing accumulated-log behavior would have
/// delivered the whole journey here).
#[tokio::test]
async fn full_revert_coalesces_to_empty_and_produces_no_owner_wake_over_wss() {
    let fx = boot().await;
    let monitor = fx
        .services
        .pr_monitor_register(&fx.ws_id, &fx.agent_id, "o", "r", 42)
        .await
        .expect("register")
        .0;

    // A→B: one new comment; the coalesced set carries the single net line.
    fx.forge.edit(|s| s.conversation_comments = 1);
    fx.services.poll_pr_monitors().await;
    let mut rpc = connect(fx.port, fx.cfg.clone()).await;
    let listed = wss_rpc(
        &mut rpc,
        1,
        "prMonitor.list",
        json!({ "workspaceId": fx.ws_id.as_str() }),
    )
    .await;
    assert_eq!(
        listed["monitors"][0]["pendingChanges"],
        json!(["+1 conversation comment (1 total)"]),
        "first poll: {listed}"
    );

    // B→C: a second comment; the set is RECOMPUTED against the emit baseline
    // on every poll — a single net 0→2 line, never a two-line journey.
    fx.forge.edit(|s| s.conversation_comments = 2);
    fx.services.poll_pr_monitors().await;
    let listed = wss_rpc(
        &mut rpc,
        2,
        "prMonitor.list",
        json!({ "workspaceId": fx.ws_id.as_str() }),
    )
    .await;
    assert_eq!(
        listed["monitors"][0]["pendingChanges"],
        json!(["+2 conversation comments (2 total)"]),
        "coalesced net line, not the journey: {listed}"
    );

    // Backdate the debounce anchors so the NEXT poll would deliver the wake
    // if anything stayed pending — the revert below must suppress it.
    let row = fx
        .services
        .store()
        .get_pr_monitor(&monitor.monitor_id)
        .await
        .expect("load row");
    assert!(fx
        .services
        .store()
        .update_pr_monitor_poll(
            &monitor.monitor_id,
            PrMonitorPollUpdate {
                last_snapshot: row.last_snapshot.as_deref(),
                baseline_snapshot: row.baseline_snapshot.as_deref(),
                pending_changes: &row.pending_changes,
                pending_since: Some("2020-01-01T00:00:00Z"),
                last_change_at: Some("2020-01-01T00:00:00Z"),
                last_polled_at: row.last_polled_at.as_deref(),
                last_error: None,
                updated_at: &now_iso(),
                expected_updated_at: &row.updated_at,
            },
        )
        .await
        .expect("backdate debounce anchors"));

    // C→A: both comments deleted — the PR is back at the emit baseline. The
    // FE-facing `prMonitor:changed` fires with the set shrinking to empty.
    let mut sub = connect(fx.port, fx.cfg.clone()).await;
    let sub_res = wss_rpc(
        &mut sub,
        3,
        "events.subscribe",
        json!({ "eventTypes": ["prMonitor:changed"], "workspaceId": fx.ws_id.as_str() }),
    )
    .await;
    assert!(sub_res["subscriptionId"].is_string(), "sub id: {sub_res}");
    fx.forge.edit(|s| s.conversation_comments = 0);
    fx.services.poll_pr_monitors().await;
    let evt = next_event(&mut sub, "prMonitor:changed").await;
    assert_eq!(evt["data"]["monitorId"], monitor.monitor_id.as_str());
    assert_eq!(
        evt["data"]["changes"],
        json!([]),
        "the revert shrinks the net set to empty: {evt}"
    );

    // Nothing pending, anchors reset, and — despite the elapsed debounce —
    // the owner was never woken.
    let listed = wss_rpc(
        &mut rpc,
        4,
        "prMonitor.list",
        json!({ "workspaceId": fx.ws_id.as_str() }),
    )
    .await;
    let row = &listed["monitors"][0];
    assert_eq!(row["pendingChanges"], json!([]), "empty net set: {row}");
    assert_eq!(row["hasPendingChanges"], false);
    assert!(row["pendingSince"].is_null(), "anchor reset: {row}");
    assert!(row["lastChangeAt"].is_null(), "anchor reset: {row}");
    assert!(
        !owner_messages(&fx).await.contains("[PR monitor o/r#42]"),
        "a full revert must produce no owner wake"
    );

    // A flush finds nothing pending either — an explicit no-op.
    let flushed = wss_rpc(
        &mut rpc,
        5,
        "prMonitor.flush",
        json!({ "workspaceId": fx.ws_id.as_str(), "monitorId": monitor.monitor_id.as_str() }),
    )
    .await;
    assert_eq!(flushed, json!({ "ok": true, "flushed": false }));
}
