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
#[derive(Clone, Default)]
struct ForgeState {
    merged: bool,
    conversation_comments: usize,
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
        let merged = self.state.lock().unwrap().merged;
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
        unsupported("list_prs")
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
        Ok(MergeRequirementSignals {
            merge_state_status: Some("CLEAN".into()),
            review_decision: Some(ReviewDecision::ReviewRequired),
            checks: vec![RollupCheck {
                name: "build".into(),
                state: CheckState::Pending,
                is_required: true,
                url: None,
            }],
            checks_known: true,
            branch_rules: Some(BranchRules {
                required_approving_review_count: Some(1),
                required_conversation_resolution: Some(true),
                required_status_checks: vec!["build".into()],
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
        checkout_mode: None,
        disk_usage: None,
    }
}

fn agent_session(ws: &WorkspaceId, id: &str) -> AgentSession {
    AgentSession {
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
        bind_address: Ipv4Addr::LOCALHOST.into(),
        ..Default::default()
    };
    let ws_srv = WsApiServer::new(api, bus, &tls, token_store, opts, None).expect("server");
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
/// `prMonitor.list` so merged PRs remain in the UI's list.
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
