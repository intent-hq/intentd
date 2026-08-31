//! WSS end-to-end for the derived `Workspace.displayStatus` aggregate and its
//! `workspace:displayStatus-changed` transition event (PROTOCOL §6.5):
//! `workspace.list` / `workspace.get` responses carry `displayStatus`, and a
//! task-status change plus a PR-linkage change driven over the wire each emit
//! exactly one `events.event` notification with the self-sufficient
//! `{ workspaceId, displayStatus }` payload. Drives a real [`WsApiServer`]
//! over TLS with bearer-token auth and a pinned self-signed fingerprint (the
//! production transport path) with a stub forge injected via
//! `with_source_control`.

#![cfg(unix)]

mod common;

use std::net::Ipv4Addr;
use std::path::PathBuf;
use std::sync::{Arc, Mutex};
use std::time::Duration;

use async_trait::async_trait;
use futures_util::{SinkExt, StreamExt};
use intent_core::{
    now_iso, PullRequestStatus, Result as CoreResult, Workspace, WorkspaceActivity, WorkspaceApi,
    WorkspaceAttention, WorkspaceId, WorkspaceStatus,
};
use intent_services::{AgentManager, BusEventSink, EventBus, Services};
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
const TOKEN: &str = "cdcdcdcdcdcdcdcdcdcdcdcdcdcdcdcdcdcdcdcdcdcdcdcdcdcdcdcdcdcdcdcd";

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

/// Stub forge: `get_pr` reports the linked PR (#42, head `feature`) as merged;
/// when `open_pr_number` is set, `list_prs` offers that open PR on the same
/// head ref.
#[derive(Default)]
struct StubForge {
    open_pr_number: Option<u64>,
}

fn sample_pr() -> PullRequest {
    PullRequest {
        number: 42,
        url: "https://github.com/o/r/pull/42".into(),
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
        let mut pr = sample_pr();
        if Some(number) == self.open_pr_number {
            pr.number = number;
            pr.url = format!("https://github.com/o/r/pull/{number}");
            pr.state = PrState::Open;
        }
        Ok(pr)
    }
    async fn list_prs(&self, _: &RepoRef, _: PrQuery) -> ScResult<Page<PullRequest>> {
        let items = match self.open_pr_number {
            Some(n) => {
                let mut pr = sample_pr();
                pr.number = n;
                pr.url = format!("https://github.com/o/r/pull/{n}");
                pr.state = PrState::Open;
                vec![pr]
            }
            None => vec![],
        };
        Ok(Page {
            items,
            next_cursor: None,
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
    async fn list_issues(&self, _: &RepoRef, _: IssueQuery) -> ScResult<Page<Issue>> {
        unimplemented!()
    }
}

struct Fixture {
    _ws: WsApiServer,
    _manager: Arc<AgentManager>,
    port: u16,
    cfg: Arc<ClientConfig>,
    ws_id: WorkspaceId,
    store: Store,
    _dir: TempDir,
}

/// Boot a TLS + bearer-auth WSS listener over a seeded workspace. When
/// `linkable` is true the workspace carries repo info on branch `feature`
/// (unlinked) so the stub forge can discover the open PR; otherwise it has no
/// repo info at all (PR paths inert for the task-driven test). `pr_status`
/// seeds only the persisted `prStatus` column (no rich PR objects).
async fn boot(forge: StubForge, linkable: bool, pr_status: Option<PullRequestStatus>) -> Fixture {
    let short = uuid::Uuid::new_v4().simple().to_string();
    let dir = std::env::temp_dir().join(format!("intentd-display-status-{}", &short[..8]));
    std::fs::create_dir_all(&dir).unwrap();
    let store = Store::open(&dir.join("intentd.db")).await.expect("store");
    let bus = EventBus::new(store.clone());
    let workspaces_root = dir.join("workspaces");
    std::fs::create_dir_all(&workspaces_root).expect("mkdir hermetic root");

    let ws_id = WorkspaceId::new();
    let ts = now_iso();
    let ws = Workspace {
        id: ws_id.clone(),
        title: "Display status".into(),
        branch: if linkable {
            "feature".into()
        } else {
            String::new()
        },
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
        repository_owner: linkable.then(|| "o".into()),
        repository_name: linkable.then(|| "r".into()),
        worktree_path: None,
        scope: None,
        skip_worktree: false,
        setup_script: None,
        is_remote: false,
        default_model: None,
        pr_number: None,
        pr_url: None,
        pr_status,
        active_pull_request: None,
        pull_requests: None,
        context_links: None,
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
    };
    store.insert_workspace(&ws).await.expect("seed workspace");

    let services = Arc::new(
        Services::new(store.clone())
            .with_workspaces_root(workspaces_root)
            .with_event_bus(bus.clone())
            .with_source_control(Arc::new(forge)),
    );
    // Runtime manager so `agent.retry` has a live handler (no provider is
    // ever spawned: the tests only exercise the status-flip path).
    let sink: Arc<dyn intent_acp::EventSink> = Arc::new(BusEventSink::new(bus.clone()));
    let manager = Arc::new(AgentManager::new((*services).clone(), sink, 4));
    services.attach_agent_manager(&manager);
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
        _manager: manager,
        port,
        cfg,
        ws_id,
        store,
        _dir: TempDir(dir),
    }
}

/// Establish an authenticated WSS connection over pinned TLS (token in the
/// query string).
async fn connect(port: u16, cfg: Arc<ClientConfig>) -> TlsWs {
    let url = format!("wss://localhost:{port}/ws?token={TOKEN}");
    common::wss_connect_with_retry(port, cfg, &url).await
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
                        assert!(v.get("error").is_none(), "rpc {method} errored: {v}");
                        return v["result"].clone();
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

/// Assert no `workspace:displayStatus-changed` notification arrives within the
/// window (transition-only emission — no spam on no-op recomputes).
async fn assert_no_display_status_event(ws: &mut TlsWs) {
    let res = timeout(Duration::from_millis(500), async {
        loop {
            match ws.next().await.unwrap().unwrap() {
                Message::Text(text) => {
                    let v: Value = serde_json::from_str(&text).unwrap();
                    if v["method"] == json!("events.event")
                        && v["params"]["event"]["type"] == json!("workspace:displayStatus-changed")
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
    assert!(res.is_err(), "unexpected displayStatus event: {res:?}");
}

/// Task-driven transition over the wire: `workspace.get`/`workspace.list`
/// carry the derived `displayStatus`; completing the only spec task over
/// `task.updateNoteStatus` emits `workspace:displayStatus-changed` with the
/// self-sufficient `{ workspaceId, displayStatus: "complete" }` payload, and a
/// repeat no-op status write emits nothing.
#[tokio::test]
async fn task_completion_transition_over_wss() {
    let fx = boot(StubForge::default(), false, None).await;

    let mut rpc = connect(fx.port, fx.cfg.clone()).await;
    // Seed a spec-child task note (in_progress) over the wire.
    let created = wss_rpc(
        &mut rpc,
        1,
        "note.create",
        json!({ "workspaceId": fx.ws_id.as_str(), "title": "Task A", "parentId": "spec" }),
    )
    .await;
    let note_id = created["note"]["id"].as_str().expect("note id").to_string();
    let marked = wss_rpc(
        &mut rpc,
        2,
        "task.markAsTask",
        json!({ "workspaceId": fx.ws_id.as_str(), "noteId": note_id, "status": "in_progress" }),
    )
    .await;
    assert_eq!(marked["ok"], true, "markAsTask ok: {marked}");

    // The emit path derives displayStatus on get/list (also seeding the
    // last-observed cache for the transition below). No agent is running,
    // so the task-stage rollup reads as idle.
    let got = wss_rpc(
        &mut rpc,
        3,
        "workspace.get",
        json!({ "workspaceId": fx.ws_id.as_str() }),
    )
    .await;
    assert_eq!(got["workspace"]["displayStatus"], "idle");
    let listed = wss_rpc(&mut rpc, 4, "workspace.list", json!({})).await;
    let ws_row = listed["workspaces"]
        .as_array()
        .expect("workspaces array")
        .iter()
        .find(|w| w["id"] == fx.ws_id.as_str())
        .expect("seeded workspace listed");
    assert_eq!(ws_row["displayStatus"], "idle");

    // Subscribe on a separate connection, then complete the task.
    let mut sub = connect(fx.port, fx.cfg.clone()).await;
    let sub_res = wss_rpc(
        &mut sub,
        10,
        "events.subscribe",
        json!({
            "eventTypes": ["workspace:displayStatus-changed"],
            "workspaceId": fx.ws_id.as_str(),
        }),
    )
    .await;
    assert!(sub_res["subscriptionId"].is_string(), "sub id: {sub_res}");

    let updated = wss_rpc(
        &mut rpc,
        5,
        "task.updateNoteStatus",
        json!({ "workspaceId": fx.ws_id.as_str(), "noteId": note_id, "status": "complete" }),
    )
    .await;
    assert_eq!(updated["ok"], true, "updateNoteStatus ok: {updated}");

    let evt = next_event(&mut sub, "workspace:displayStatus-changed").await;
    assert_eq!(evt["workspaceId"], fx.ws_id.as_str());
    assert_eq!(
        evt["data"],
        json!({ "workspaceId": fx.ws_id.as_str(), "displayStatus": "complete" })
    );

    // A repeat write to the same status is a no-op transition: no event.
    let again = wss_rpc(
        &mut rpc,
        6,
        "task.updateNoteStatus",
        json!({ "workspaceId": fx.ws_id.as_str(), "noteId": note_id, "status": "complete" }),
    )
    .await;
    assert_eq!(again["ok"], true, "repeat updateNoteStatus ok: {again}");
    assert_no_display_status_event(&mut sub).await;
}

/// PR-driven transition over the wire: an unlinked workspace on branch
/// `feature` discovers the stub forge's open PR (#300, mergeable) via
/// `pr.refresh` — the linkage flips the derived rollup to `pr_ready` and emits
/// `workspace:displayStatus-changed` alongside `pr:linked`.
#[tokio::test]
async fn pr_linkage_transition_over_wss() {
    let fx = boot(
        StubForge {
            open_pr_number: Some(300),
        },
        true,
        None,
    )
    .await;

    let mut rpc = connect(fx.port, fx.cfg.clone()).await;
    // Seed the last-observed cache over the wire (idle: no PR yet, no tasks
    // started, no agent running).
    let got = wss_rpc(
        &mut rpc,
        1,
        "workspace.get",
        json!({ "workspaceId": fx.ws_id.as_str() }),
    )
    .await;
    assert_eq!(got["workspace"]["displayStatus"], "idle");

    let mut sub = connect(fx.port, fx.cfg.clone()).await;
    let sub_res = wss_rpc(
        &mut sub,
        10,
        "events.subscribe",
        json!({
            "eventTypes": ["workspace:displayStatus-changed"],
            "workspaceId": fx.ws_id.as_str(),
        }),
    )
    .await;
    assert!(sub_res["subscriptionId"].is_string(), "sub id: {sub_res}");

    // Drive the same refresh the 60s background sweep runs: discovery links
    // open PR #300 (mergeable, non-draft) → displayStatus flips to pr_ready.
    let refreshed = wss_rpc(
        &mut rpc,
        2,
        "pr.refresh",
        json!({ "workspaceId": fx.ws_id.as_str() }),
    )
    .await;
    assert_eq!(refreshed["outcome"], "linked", "refresh: {refreshed}");

    let evt = next_event(&mut sub, "workspace:displayStatus-changed").await;
    assert_eq!(evt["workspaceId"], fx.ws_id.as_str());
    assert_eq!(
        evt["data"],
        json!({ "workspaceId": fx.ws_id.as_str(), "displayStatus": "pr_ready" })
    );

    // The linked state is visible on the read path too.
    let after = wss_rpc(
        &mut rpc,
        3,
        "workspace.get",
        json!({ "workspaceId": fx.ws_id.as_str() }),
    )
    .await;
    assert_eq!(after["workspace"]["displayStatus"], "pr_ready");

    // A second refresh re-fetches the same open PR: no transition, no event.
    let again = wss_rpc(
        &mut rpc,
        4,
        "pr.refresh",
        json!({ "workspaceId": fx.ws_id.as_str() }),
    )
    .await;
    assert_eq!(again["outcome"], "unchanged", "second refresh: {again}");
    assert_no_display_status_event(&mut sub).await;
}

/// Persisted-column-only derivation over the wire: a workspace whose
/// `prStatus` column is `Open` but which carries no rich PR objects
/// (`activePullRequest` / `pullRequests` unset) reports
/// `displayStatus: "pr_open"` on both `workspace.get` and `workspace.list`.
#[tokio::test]
async fn persisted_pr_status_only_is_pr_open_over_wss() {
    let fx = boot(StubForge::default(), false, Some(PullRequestStatus::Open)).await;

    let mut rpc = connect(fx.port, fx.cfg.clone()).await;
    let got = wss_rpc(
        &mut rpc,
        1,
        "workspace.get",
        json!({ "workspaceId": fx.ws_id.as_str() }),
    )
    .await;
    assert!(
        got["workspace"]["activePullRequest"].is_null(),
        "no rich PR objects seeded: {got}"
    );
    assert_eq!(got["workspace"]["displayStatus"], "pr_open");

    let listed = wss_rpc(&mut rpc, 2, "workspace.list", json!({})).await;
    let ws_row = listed["workspaces"]
        .as_array()
        .expect("workspaces array")
        .iter()
        .find(|w| w["id"] == fx.ws_id.as_str())
        .expect("seeded workspace listed");
    assert_eq!(ws_row["displayStatus"], "pr_open");
}

/// Minimal top-level (no parent, foreground) agent session for seeding the
/// attention-axis fixtures directly in the store.
fn top_level_session(ws: &WorkspaceId, id: &str) -> intent_core::AgentSession {
    let ts = now_iso();
    intent_core::AgentSession {
        harness_version: intent_core::CURRENT_HARNESS_VERSION.to_string(),
        harness_features: None,
        id: intent_core::AgentId::from(id),
        workspace_id: ws.clone(),
        parent_agent_id: None,
        backend_session_id: None,
        acp_session_id: None,
        name: id.to_string(),
        name_explicitly_set: false,
        model: None,
        reasoning_effort: None,
        effort_levels: None,
        provider: None,
        system_prompt: None,
        specialist: None,
        status: intent_core::AgentStatus::Waiting,
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
        created_at: ts.clone(),
        updated_at: ts,
        sandbox_id: None,
        sandbox_path: None,
        sandbox_branch: None,
        stop_reason: None,
        stop_reason_timestamp: None,
        session_corrupted: false,
        pending_delete_at: None,
        retired_at: None,
    }
}

/// Failed axis over the wire: a top-level agent parked in `error` reads as
/// `displayStatus: "failed"` on `workspace.get`; `agent.retry` clears the
/// park and emits the `failed → idle` demotion with the self-sufficient
/// payload.
#[tokio::test]
async fn failed_agent_and_retry_transition_over_wss() {
    let fx = boot(StubForge::default(), false, None).await;
    let mut session = top_level_session(&fx.ws_id, "agent-e2e-err");
    session.status = intent_core::AgentStatus::Error;
    fx.store
        .insert_agent_session(&session)
        .await
        .expect("seed errored session");

    let mut rpc = connect(fx.port, fx.cfg.clone()).await;
    // Read path serves the new wire value (and seeds the baseline).
    let got = wss_rpc(
        &mut rpc,
        1,
        "workspace.get",
        json!({ "workspaceId": fx.ws_id.as_str() }),
    )
    .await;
    assert_eq!(got["workspace"]["displayStatus"], "failed");

    let mut sub = connect(fx.port, fx.cfg.clone()).await;
    let sub_res = wss_rpc(
        &mut sub,
        10,
        "events.subscribe",
        json!({
            "eventTypes": ["workspace:displayStatus-changed"],
            "workspaceId": fx.ws_id.as_str(),
        }),
    )
    .await;
    assert!(sub_res["subscriptionId"].is_string(), "sub id: {sub_res}");

    // agent.retry clears the Error park (empty queue → idle) and emits the
    // failed → idle demotion.
    let retried = wss_rpc(
        &mut rpc,
        2,
        "agent.retry",
        json!({ "workspaceId": fx.ws_id.as_str(), "agentId": session.id.0 }),
    )
    .await;
    assert_eq!(retried["ok"], true, "retry ok: {retried}");
    let evt = next_event(&mut sub, "workspace:displayStatus-changed").await;
    assert_eq!(
        evt["data"],
        json!({ "workspaceId": fx.ws_id.as_str(), "displayStatus": "idle" })
    );
}

/// Blocked axis over the wire: a top-level pending blocker attention request
/// reads as `displayStatus: "blocked"` on `workspace.get` — outranking
/// `needs_attention` from a sibling discussion request — and `agent.delete`
/// of the blocker-holding agent emits the demotion.
#[tokio::test]
async fn blocked_transition_over_wss() {
    let fx = boot(StubForge::default(), false, None).await;
    let blocker = top_level_session(&fx.ws_id, "agent-e2e-blk");
    fx.store
        .insert_agent_session(&blocker)
        .await
        .expect("seed blocker session");
    fx.store
        .set_attention_request(&fx.ws_id, &blocker.id, "blocker", "env broken", &now_iso())
        .await
        .expect("raise blocker");
    let discuss = top_level_session(&fx.ws_id, "agent-e2e-disc");
    fx.store
        .insert_agent_session(&discuss)
        .await
        .expect("seed discussion session");
    fx.store
        .set_attention_request(
            &fx.ws_id,
            &discuss.id,
            "discussion",
            "need input",
            &now_iso(),
        )
        .await
        .expect("raise discussion");

    let mut rpc = connect(fx.port, fx.cfg.clone()).await;
    // blocked outranks needs_attention on the read path (seeds baseline).
    let got = wss_rpc(
        &mut rpc,
        1,
        "workspace.get",
        json!({ "workspaceId": fx.ws_id.as_str() }),
    )
    .await;
    assert_eq!(got["workspace"]["displayStatus"], "blocked");

    let mut sub = connect(fx.port, fx.cfg.clone()).await;
    let sub_res = wss_rpc(
        &mut sub,
        10,
        "events.subscribe",
        json!({
            "eventTypes": ["workspace:displayStatus-changed"],
            "workspaceId": fx.ws_id.as_str(),
        }),
    )
    .await;
    assert!(sub_res["subscriptionId"].is_string(), "sub id: {sub_res}");

    // Deleting the blocker-holder demotes to the next rung: the sibling
    // discussion request keeps the workspace at needs_attention.
    let deleted = wss_rpc(
        &mut rpc,
        2,
        "agent.delete",
        json!({ "workspaceId": fx.ws_id.as_str(), "agentId": blocker.id.0 }),
    )
    .await;
    assert_eq!(deleted["success"], true, "delete ok: {deleted}");
    let evt = next_event(&mut sub, "workspace:displayStatus-changed").await;
    assert_eq!(
        evt["data"],
        json!({ "workspaceId": fx.ws_id.as_str(), "displayStatus": "needs_attention" })
    );
}

/// Attention flags over the wire: the `unread` flag is not a displayStatus
/// axis — `workspace.update { attention: "unread" }` and `workspace.markSeen`
/// leave `displayStatus: "idle"` and emit no
/// `workspace:displayStatus-changed`. Served `attention` is DERIVED from
/// per-agent seen markers (§5.1): with no agent sessions a stored `unread`
/// reads back as `none` — the stale stored flag cannot show the blue dot. A
/// `review_required` flag reads as `needs_attention` and
/// `workspace.dismissAttention` retires it; the ordered event stream (first
/// event observed is the `review_required` promotion) proves the unread
/// mutations stayed silent.
#[tokio::test]
async fn attention_flag_transitions_over_wss() {
    let fx = boot(StubForge::default(), false, None).await;

    let mut rpc = connect(fx.port, fx.cfg.clone()).await;
    // Seed the baseline: idle (no PR, no tasks, no agents).
    let got = wss_rpc(
        &mut rpc,
        1,
        "workspace.get",
        json!({ "workspaceId": fx.ws_id.as_str() }),
    )
    .await;
    assert_eq!(got["workspace"]["displayStatus"], "idle");

    let mut sub = connect(fx.port, fx.cfg.clone()).await;
    let sub_res = wss_rpc(
        &mut sub,
        10,
        "events.subscribe",
        json!({
            "eventTypes": ["workspace:displayStatus-changed"],
            "workspaceId": fx.ws_id.as_str(),
        }),
    )
    .await;
    assert!(sub_res["subscriptionId"].is_string(), "sub id: {sub_res}");

    // unread flag: no displayStatus axis — the rollup stays idle.
    wss_rpc(
        &mut rpc,
        2,
        "workspace.update",
        json!({ "workspaceId": fx.ws_id.as_str(), "attention": "unread" }),
    )
    .await;
    let got = wss_rpc(
        &mut rpc,
        3,
        "workspace.get",
        json!({ "workspaceId": fx.ws_id.as_str() }),
    )
    .await;
    assert_eq!(got["workspace"]["displayStatus"], "idle");
    // Derived unread (§5.1): no agent session has an unseen assistant last
    // message, so the served value is `none` despite the stored flag.
    assert_eq!(got["workspace"]["attention"], "none");

    // markSeen retires the flag; the rollup never moved.
    wss_rpc(
        &mut rpc,
        4,
        "workspace.markSeen",
        json!({ "workspaceId": fx.ws_id.as_str() }),
    )
    .await;
    let got = wss_rpc(
        &mut rpc,
        11,
        "workspace.get",
        json!({ "workspaceId": fx.ws_id.as_str() }),
    )
    .await;
    assert_eq!(got["workspace"]["displayStatus"], "idle");
    assert_eq!(got["workspace"]["attention"], "none");

    // review_required flag → needs_attention. This is the FIRST event on the
    // ordered subscription stream, proving the unread raise + markSeen above
    // emitted no workspace:displayStatus-changed.
    wss_rpc(
        &mut rpc,
        5,
        "workspace.update",
        json!({ "workspaceId": fx.ws_id.as_str(), "attention": "review_required" }),
    )
    .await;
    let evt = next_event(&mut sub, "workspace:displayStatus-changed").await;
    assert_eq!(
        evt["data"],
        json!({ "workspaceId": fx.ws_id.as_str(), "displayStatus": "needs_attention" })
    );

    // dismissAttention retires review_required → idle.
    wss_rpc(
        &mut rpc,
        6,
        "workspace.dismissAttention",
        json!({ "workspaceId": fx.ws_id.as_str() }),
    )
    .await;
    let evt = next_event(&mut sub, "workspace:displayStatus-changed").await;
    assert_eq!(
        evt["data"],
        json!({ "workspaceId": fx.ws_id.as_str(), "displayStatus": "idle" })
    );
}
