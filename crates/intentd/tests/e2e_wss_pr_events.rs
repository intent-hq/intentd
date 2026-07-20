//! WSS end-to-end for the `pr:linked` / `pr:updated` event payloads
//! (PROTOCOL §6.5): both must carry the daemon-owned `pullRequests` list so a
//! subscribed client can render the full per-branch PR list without a refetch.
//! Drives a real [`WsApiServer`] over plain `ws://` (insecure dev mode) with a
//! stub forge injected via `with_source_control`, then triggers the same
//! refresh the 60s background sweep runs and asserts the `events.event`
//! notifications observed over the wire.

#![cfg(unix)]

use std::net::Ipv4Addr;
use std::path::PathBuf;
use std::sync::Arc;
use std::time::Duration;

use async_trait::async_trait;
use futures_util::{SinkExt, StreamExt};
use intent_core::{
    now_iso, Workspace, WorkspaceActivity, WorkspaceApi, WorkspaceAttention, WorkspaceId,
    WorkspaceStatus,
};
use intent_services::{EventBus, Services};
use intent_sourcecontrol::{
    AuthStatus, Branch, CheckRun, Comment, CommentAnchor, Issue, IssueQuery, MergeMethod,
    MergeOptions, MergeOutcome, Mergeability, NewPullRequest, Page, PageParams, PrPatch, PrQuery,
    PrState, PullRequest, Repo, RepoRef, Result as ScResult, Review, ReviewComment, ReviewThread,
    ReviewVerdict, ScCapabilities, SourceControl, UserIdentity,
};
use intent_store::Store;
use intent_transport::{WsApiServer, WsOptions};
use serde_json::{json, Value};
use tokio::net::TcpStream;
use tokio::time::timeout;
use tokio_tungstenite::tungstenite::Message;
use tokio_tungstenite::{MaybeTlsStream, WebSocketStream};

type PlainWs = WebSocketStream<MaybeTlsStream<TcpStream>>;

struct TempDir(PathBuf);
impl Drop for TempDir {
    fn drop(&mut self) {
        let _ = std::fs::remove_dir_all(&self.0);
    }
}

/// Stub forge: `get_pr` reports the linked PR (#42, head `feature`) as merged;
/// when `open_pr_number` is set, `list_prs` offers that open PR on the same
/// head ref (the relink successor).
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
        _: PageParams,
    ) -> ScResult<Page<Branch>> {
        unimplemented!()
    }
    async fn create_pr(&self, _: &RepoRef, _: NewPullRequest) -> ScResult<PullRequest> {
        unimplemented!()
    }
    async fn get_pr(&self, _: &RepoRef, _: u64) -> ScResult<PullRequest> {
        Ok(sample_pr())
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
    port: u16,
    services: Arc<Services>,
    ws_id: WorkspaceId,
    _dir: TempDir,
}

/// Boot an insecure WSS listener whose services carry the stub forge and a
/// seeded workspace linked to PR #42 on branch `feature`.
async fn boot(forge: StubForge) -> Fixture {
    let short = uuid::Uuid::new_v4().simple().to_string();
    let dir = std::env::temp_dir().join(format!("intentd-prevents-{}", &short[..8]));
    std::fs::create_dir_all(&dir).unwrap();
    let store = Store::open(&dir.join("intentd.db")).await.expect("store");
    let bus = EventBus::new(store.clone());
    let workspaces_root = dir.join("workspaces");
    std::fs::create_dir_all(&workspaces_root).expect("mkdir hermetic root");

    let ws_id = WorkspaceId::new();
    let ts = now_iso();
    let ws = Workspace {
        id: ws_id.clone(),
        title: "PR events".into(),
        branch: "feature".into(),
        base_ref: None,
        base_commit_sha: None,
        status: WorkspaceStatus::Active,
        status_message: None,
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
        pr_number: Some(42),
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
    };
    store.insert_workspace(&ws).await.expect("seed workspace");

    let services = Arc::new(
        Services::new(store)
            .with_workspaces_root(workspaces_root)
            .with_event_bus(bus.clone())
            .with_source_control(Arc::new(forge)),
    );
    let api: Arc<dyn WorkspaceApi> = services.clone();
    let opts = WsOptions {
        base_port: 0,
        bind_address: Ipv4Addr::LOCALHOST.into(),
        ..Default::default()
    };
    let ws_srv = WsApiServer::new_insecure(api, bus, opts, None);
    let port = ws_srv.start().await.expect("start");
    Fixture {
        _ws: ws_srv,
        port,
        services,
        ws_id,
        _dir: TempDir(dir),
    }
}

async fn connect(port: u16) -> PlainWs {
    let url = format!("ws://127.0.0.1:{port}/ws");
    let (sock, _resp) = tokio_tungstenite::connect_async(&url)
        .await
        .expect("plain ws handshake");
    sock
}

async fn wss_rpc(ws: &mut PlainWs, id: i64, method: &str, params: Value) -> Value {
    let req = json!({ "jsonrpc": "2.0", "id": id, "method": method, "params": params });
    ws.send(Message::Text(req.to_string())).await.unwrap();
    timeout(Duration::from_secs(5), async {
        loop {
            match ws.next().await.unwrap().unwrap() {
                Message::Text(text) => {
                    let v: Value = serde_json::from_str(&text).unwrap();
                    if v.get("id") == Some(&json!(id)) {
                        assert!(v.get("error").is_none(), "rpc {method} errored: {v}");
                        return v["result"].clone();
                    }
                }
                Message::Ping(_) | Message::Pong(_) => {}
                _ => panic!("unexpected message"),
            }
        }
    })
    .await
    .expect("response timeout")
}

/// Wait for the next `events.event` notification whose `type` matches.
async fn next_event(ws: &mut PlainWs, event_type: &str) -> Value {
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
                Message::Ping(_) | Message::Pong(_) => {}
                _ => {}
            }
        }
    })
    .await
    .unwrap_or_else(|_| panic!("timed out waiting for {event_type}"))
}

/// Relink-after-merge over the wire: the linked PR (#42) is fetched as merged
/// and an open successor (#300) exists on the same branch → the refresh emits
/// `pr:linked` whose payload carries prNumber 300 plus the full `pullRequests`
/// list (merged #42 retained, open #300 added), matching PROTOCOL §6.5.
#[tokio::test]
async fn pr_linked_event_carries_pull_requests_list_over_wss() {
    let fx = boot(StubForge {
        open_pr_number: Some(300),
    })
    .await;

    let mut sub = connect(fx.port).await;
    let sub_res = wss_rpc(
        &mut sub,
        1,
        "events.subscribe",
        json!({ "eventTypes": ["pr:linked"], "workspaceId": fx.ws_id.as_str() }),
    )
    .await;
    assert!(sub_res["subscriptionId"].is_string(), "sub id: {sub_res}");

    // Drive the same per-workspace refresh the 60s background sweep runs.
    let outcome = fx
        .services
        .refresh_workspace_pr(&fx.ws_id)
        .await
        .expect("refresh");
    assert_eq!(outcome, intent_services::PrRefreshOutcome::Linked);

    let evt = next_event(&mut sub, "pr:linked").await;
    assert_eq!(evt["workspaceId"], fx.ws_id.as_str());
    let data = &evt["data"];
    assert_eq!(data["workspaceId"], fx.ws_id.as_str());
    assert_eq!(data["prNumber"], 300);
    assert_eq!(data["prUrl"], "https://github.com/o/r/pull/300");
    assert_eq!(data["prStatus"], "Open");
    assert_eq!(data["activePullRequest"]["number"], 300);
    let list = data["pullRequests"].as_array().expect("pullRequests array");
    assert_eq!(list.len(), 2);
    let merged = list.iter().find(|p| p["number"] == 42).expect("merged #42");
    assert_eq!(merged["status"], "Merged");
    let open = list.iter().find(|p| p["number"] == 300).expect("open #300");
    assert_eq!(open["status"], "Open");
}

/// Merged-without-successor over the wire: the linked PR (#42) is fetched as
/// merged and no open successor exists → the refresh emits `pr:updated` whose
/// payload carries the status delta plus the seeded `pullRequests` list.
#[tokio::test]
async fn pr_updated_event_carries_pull_requests_list_over_wss() {
    let fx = boot(StubForge {
        open_pr_number: None,
    })
    .await;

    let mut sub = connect(fx.port).await;
    let sub_res = wss_rpc(
        &mut sub,
        1,
        "events.subscribe",
        json!({ "eventTypes": ["pr:updated"], "workspaceId": fx.ws_id.as_str() }),
    )
    .await;
    assert!(sub_res["subscriptionId"].is_string(), "sub id: {sub_res}");

    let outcome = fx
        .services
        .refresh_workspace_pr(&fx.ws_id)
        .await
        .expect("refresh");
    assert_eq!(outcome, intent_services::PrRefreshOutcome::Updated);

    let evt = next_event(&mut sub, "pr:updated").await;
    assert_eq!(evt["workspaceId"], fx.ws_id.as_str());
    let data = &evt["data"];
    assert_eq!(data["workspaceId"], fx.ws_id.as_str());
    assert_eq!(data["prNumber"], 42);
    assert_eq!(data["prStatus"], "Merged");
    assert_eq!(data["activePullRequest"]["number"], 42);
    assert_eq!(data["activePullRequest"]["status"], "Merged");
    let list = data["pullRequests"].as_array().expect("pullRequests array");
    assert_eq!(list.len(), 1);
    assert_eq!(list[0]["number"], 42);
    assert_eq!(list[0]["status"], "Merged");
}
