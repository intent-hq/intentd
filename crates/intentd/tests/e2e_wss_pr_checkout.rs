//! PR checkout correctness over the real WebSocket router and local bare Git.
#![cfg(unix)]

mod common;

use async_trait::async_trait;
use intent_sourcecontrol::{
    AuthStatus, Branch, CheckRun, Comment, CommentAnchor, Issue, IssueQuery, MergeMethod,
    MergeOptions, MergeOutcome, Mergeability, NewPullRequest, Page, PageParams, PrPatch, PrQuery,
    PullRequest, Repo, RepoRef, Result as ScResult, Review, ReviewComment, ReviewThread,
    ReviewVerdict, ScCapabilities, SourceControl, UserIdentity,
};

use std::net::Ipv4Addr;
use std::sync::Arc;
use std::time::Instant;

use futures_util::{SinkExt, StreamExt};
use intent_core::WorkspaceApi;
use intent_services::{EventBus, Services};
use intent_store::Store;
use intent_transport::{WsApiServer, WsOptions};
use serde_json::{json, Value};
use tokio::net::TcpStream;
use tokio::time::timeout;
use tokio_tungstenite::tungstenite::Message;
use tokio_tungstenite::{MaybeTlsStream, WebSocketStream};

type PlainWs = WebSocketStream<MaybeTlsStream<TcpStream>>;

struct Fixture {
    _ws: WsApiServer,
    port: u16,
    dir: tempfile::TempDir,
}

async fn boot(branch: &str) -> Fixture {
    let tmp = common::test_tempdir("intentd-ctxlink-");
    let dir = tmp.path().to_path_buf();
    let store = Store::open(&dir.join("intentd.db")).await.expect("store");
    let bus = EventBus::new(store.clone());
    let workspaces_root = dir.join("workspaces");
    std::fs::create_dir_all(&workspaces_root).expect("mkdir hermetic root");
    let services = Services::new(store)
        .with_workspaces_root(workspaces_root)
        .with_event_bus(bus.clone())
        .with_source_control(Arc::new(RecordingForge {
            branch: branch.into(),
        }));
    let api: Arc<dyn WorkspaceApi> = Arc::new(services);
    let opts = WsOptions {
        base_port: 0,
        bind_addresses: vec![Ipv4Addr::LOCALHOST.into()],
        ..Default::default()
    };
    let ws = WsApiServer::new_insecure(api, bus, opts, None);
    let port = ws.start().await.expect("start");
    Fixture {
        _ws: ws,
        port,
        dir: tmp,
    }
}

async fn connect(port: u16) -> PlainWs {
    let url = format!("ws://127.0.0.1:{port}/ws");
    let (sock, _resp) = tokio_tungstenite::connect_async(&url)
        .await
        .expect("plain ws handshake");
    sock
}

/// Return the full envelope so negative cases can assert on errors.
async fn wss_rpc_raw(ws: &mut PlainWs, id: i64, method: &str, params: Value) -> Value {
    let frame = json!({ "jsonrpc": "2.0", "id": id, "method": method, "params": params });
    ws.send(Message::Text(frame.to_string().into()))
        .await
        .expect("send");
    let deadline = Instant::now() + common::rpc_read_timeout();
    loop {
        let remaining = deadline
            .checked_duration_since(Instant::now())
            .unwrap_or_default();
        assert!(
            !remaining.is_zero(),
            "wss_rpc timed out: id={id} method={method}"
        );
        let frame = timeout(remaining, ws.next())
            .await
            .unwrap_or_else(|_| panic!("wss_rpc read timeout id={id} method={method}"));
        match frame {
            Some(Ok(Message::Text(text))) => {
                let v: Value = serde_json::from_str(&text).expect("json");
                if v.get("id") == Some(&json!(id)) {
                    return v;
                }
            }
            Some(Ok(Message::Ping(p))) => {
                let _ = ws.send(Message::Pong(p)).await;
            }
            Some(Ok(_)) => {}
            other => panic!("unexpected ws frame: {other:?}"),
        }
    }
}

struct RecordingForge {
    branch: String,
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
        Err(intent_sourcecontrol::Error::Unsupported("fixture".into()))
    }
    async fn get_user(&self) -> ScResult<UserIdentity> {
        Err(intent_sourcecontrol::Error::Unsupported("fixture".into()))
    }
    async fn list_repos(&self, _: PageParams) -> ScResult<Page<Repo>> {
        Err(intent_sourcecontrol::Error::Unsupported("fixture".into()))
    }
    async fn search_repos(&self, _: &str, _: PageParams) -> ScResult<Page<Repo>> {
        Err(intent_sourcecontrol::Error::Unsupported("fixture".into()))
    }
    async fn get_repo(&self, _: &str, _: &str) -> ScResult<Repo> {
        Err(intent_sourcecontrol::Error::Unsupported("fixture".into()))
    }
    async fn list_remote_branches(
        &self,
        _: &str,
        _: &str,
        _: Option<&str>,
        _: PageParams,
    ) -> ScResult<Page<Branch>> {
        Err(intent_sourcecontrol::Error::Unsupported("fixture".into()))
    }
    async fn get_file_content(
        &self,
        repo: &RepoRef,
        path: &str,
        git_ref: Option<&str>,
    ) -> ScResult<Option<String>> {
        let _ = (repo, path, git_ref);
        Ok(None)
    }
    async fn create_pr(&self, _: &RepoRef, _: NewPullRequest) -> ScResult<PullRequest> {
        Err(intent_sourcecontrol::Error::Unsupported("fixture".into()))
    }
    async fn get_pr(&self, _: &RepoRef, number: u64) -> ScResult<PullRequest> {
        Ok(serde_json::from_value(json!({"number":number,"url":"https://github.com/o/r/pull/42","title":"PR","state":"open","draft":false,"sourceBranch":self.branch,"targetBranch":"main","author":"test","createdAt":"2026-01-01","updatedAt":"2026-01-01"})).unwrap())
    }
    async fn list_prs(&self, _: &RepoRef, _: PrQuery) -> ScResult<Page<PullRequest>> {
        Err(intent_sourcecontrol::Error::Unsupported("fixture".into()))
    }
    async fn update_pr(&self, _: &RepoRef, _: u64, _: PrPatch) -> ScResult<PullRequest> {
        Err(intent_sourcecontrol::Error::Unsupported("fixture".into()))
    }
    async fn merge_pr(
        &self,
        _: &RepoRef,
        _: u64,
        _: MergeMethod,
        _: MergeOptions,
    ) -> ScResult<MergeOutcome> {
        Err(intent_sourcecontrol::Error::Unsupported("fixture".into()))
    }
    async fn mergeability(&self, _: &RepoRef, _: u64) -> ScResult<Mergeability> {
        Err(intent_sourcecontrol::Error::Unsupported("fixture".into()))
    }
    async fn update_branch(&self, _: &RepoRef, _: u64) -> ScResult<()> {
        Err(intent_sourcecontrol::Error::Unsupported("fixture".into()))
    }
    async fn submit_review(
        &self,
        _: &RepoRef,
        _: u64,
        _: ReviewVerdict,
        _: Option<String>,
    ) -> ScResult<Review> {
        Err(intent_sourcecontrol::Error::Unsupported("fixture".into()))
    }
    async fn list_reviews(&self, _: &RepoRef, _: u64) -> ScResult<Vec<Review>> {
        Err(intent_sourcecontrol::Error::Unsupported("fixture".into()))
    }
    async fn list_comments(&self, _: &RepoRef, _: u64) -> ScResult<Vec<Comment>> {
        Err(intent_sourcecontrol::Error::Unsupported("fixture".into()))
    }
    async fn add_comment(
        &self,
        _: &RepoRef,
        _: u64,
        _: &str,
        _: Option<CommentAnchor>,
    ) -> ScResult<Comment> {
        Err(intent_sourcecontrol::Error::Unsupported("fixture".into()))
    }
    async fn list_review_comments(
        &self,
        _: &RepoRef,
        _: u64,
        _: PageParams,
    ) -> ScResult<Page<ReviewComment>> {
        Err(intent_sourcecontrol::Error::Unsupported("fixture".into()))
    }
    async fn reply_to_review_comment(
        &self,
        _: &RepoRef,
        _: u64,
        _: u64,
        _: &str,
    ) -> ScResult<ReviewComment> {
        Err(intent_sourcecontrol::Error::Unsupported("fixture".into()))
    }
    async fn get_review_threads(
        &self,
        _: &RepoRef,
        _: u64,
        _: PageParams,
    ) -> ScResult<Page<ReviewThread>> {
        Err(intent_sourcecontrol::Error::Unsupported("fixture".into()))
    }
    async fn resolve_thread(&self, _: &str) -> ScResult<bool> {
        Err(intent_sourcecontrol::Error::Unsupported("fixture".into()))
    }
    async fn unresolve_thread(&self, _: &str) -> ScResult<bool> {
        Err(intent_sourcecontrol::Error::Unsupported("fixture".into()))
    }
    async fn check_runs(&self, _: &RepoRef, _: &str) -> ScResult<Vec<CheckRun>> {
        Err(intent_sourcecontrol::Error::Unsupported("fixture".into()))
    }
    async fn create_issue(&self, _: &RepoRef, _: &str, _: Option<&str>) -> ScResult<Issue> {
        Err(intent_sourcecontrol::Error::Unsupported("fixture".into()))
    }
    async fn get_issue(&self, _: &RepoRef, _: u64) -> ScResult<Issue> {
        Err(intent_sourcecontrol::Error::Unsupported("fixture".into()))
    }
    async fn list_issues(&self, _: &RepoRef, _: IssueQuery) -> ScResult<Page<Issue>> {
        Err(intent_sourcecontrol::Error::Unsupported("fixture".into()))
    }
}

fn git(path: &std::path::Path, args: &[&str]) -> String {
    let output = std::process::Command::new("git")
        .arg("-C")
        .arg(path)
        .args(args)
        .output()
        .expect("git");
    assert!(
        output.status.success(),
        "git {args:?}: {}",
        String::from_utf8_lossy(&output.stderr)
    );
    String::from_utf8(output.stdout).unwrap().trim().to_string()
}

// Each case owns a local bare origin. Only refs/pull/42/head exposes the PR
// commit: this reproduces a fork without network, credentials or a fork remote.
async fn checkout_case(branch: &str, available: bool, conflict: bool, mode: &str) {
    let cached = mode.starts_with("cache");
    let fx = boot(branch).await;
    let origin = fx.dir.path().join("o/r.git");
    std::fs::create_dir_all(&origin).unwrap();
    git(&origin, &["init", "--bare", "-b", "main"]);
    let repo = fx.dir.path().join("repo");
    std::fs::create_dir(&repo).unwrap();
    git(&repo, &["init", "-b", "main"]);
    git(&repo, &["config", "user.name", "Fixture"]);
    git(&repo, &["config", "user.email", "fixture@example.invalid"]);
    git(&repo, &["commit", "--allow-empty", "-m", "base"]);
    let base = git(&repo, &["rev-parse", "HEAD"]);
    git(
        &repo,
        &["remote", "add", "origin", origin.to_str().unwrap()],
    );
    git(&repo, &["push", "origin", "main"]);
    git(&repo, &["checkout", "-b", "fixture-head"]);
    std::fs::write(repo.join("pr-only.txt"), "PR head evidence\n").unwrap();
    git(&repo, &["add", "pr-only.txt"]);
    git(&repo, &["commit", "-m", "PR head"]);
    let head = git(&repo, &["rev-parse", "HEAD"]);
    if available {
        git(&repo, &["push", "origin", "HEAD:refs/pull/42/head"]);
        if mode == "cache-topic" {
            git(
                &repo,
                &["push", "origin", &format!("HEAD:refs/heads/{branch}")],
            );
        }
    }
    git(&repo, &["checkout", "main"]);
    git(&repo, &["branch", "-D", "fixture-head"]);
    if conflict && branch != "main" {
        git(&repo, &["branch", branch]);
    }
    let cache = intent_git::repo_cache::cache_path_for(
        &intent_git::repo_cache::cache_root_for(&fx.dir.path().join("workspaces")),
        "o",
        "r",
    );
    if cached && conflict {
        std::fs::create_dir_all(cache.parent().unwrap()).unwrap();
        git(
            fx.dir.path(),
            &[
                "clone",
                &format!("file://{}", origin.display()),
                cache.to_str().unwrap(),
            ],
        );
        if branch != "main" {
            git(&cache, &["branch", branch]);
        }
    }
    let mut params = json!({
        "title":"PR checkout", "repositoryOwner":"o", "repositoryName":"r",
        "branch":branch, "baseRef":"main",
        "contextLinks":[{"kind":"pr","owner":"o","repo":"r","number":42,"url":"https://github.com/o/r/pull/42"}]
    });
    if cached {
        params["githubUrl"] = json!(format!("file://{}", origin.display()));
    } else {
        params["repositoryPath"] = json!(repo);
        params["isNewRepo"] = json!(mode == "direct");
    }
    let mut rpc = connect(fx.port).await;
    let response = wss_rpc_raw(&mut rpc, 1, "workspace.create", params.clone()).await;
    if available && !conflict {
        assert!(response.get("error").is_none(), "{response}");
        let workspace = &response["result"]["workspace"];
        let checkout = std::path::Path::new(workspace["worktreePath"].as_str().unwrap());
        assert_eq!(git(checkout, &["rev-parse", "HEAD"]), head);
        assert_eq!(git(checkout, &["branch", "--show-current"]), branch);
        assert_eq!(workspace["baseRef"], "main");
        assert_eq!(workspace["baseCommitSha"], base);
        assert!(checkout.join("pr-only.txt").exists());
        if mode == "direct" {
            assert_eq!(workspace["checkoutMode"], "direct");
        }
        if cached {
            assert!(cache.join(".git").exists());
            assert_ne!(checkout, repo);
            let mut imports = vec![response.clone()];
            for (id, parent) in [(2, head.as_str()), (3, base.as_str())] {
                git(&repo, &["checkout", "--detach", parent]);
                git(&repo, &["commit", "--allow-empty", "-m", "PR moved"]);
                let moved = git(&repo, &["rev-parse", "HEAD"]);
                git(
                    &repo,
                    &["push", "--force", "origin", "HEAD:refs/pull/42/head"],
                );
                if mode == "cache-topic" {
                    git(
                        &repo,
                        &[
                            "push",
                            "--force",
                            "origin",
                            &format!("HEAD:refs/heads/{branch}"),
                        ],
                    );
                    assert_eq!(
                        git(
                            &cache,
                            &["rev-parse", &format!("refs/remotes/origin/{branch}")]
                        ),
                        head,
                        "repeat import starts with a stale cache remote-tracking ref"
                    );
                }
                let imported = wss_rpc_raw(&mut rpc, id, "workspace.create", params.clone()).await;
                assert!(imported.get("error").is_none(), "repeat import: {imported}");
                let path = std::path::Path::new(
                    imported["result"]["workspace"]["worktreePath"]
                        .as_str()
                        .unwrap(),
                );
                assert_eq!(git(path, &["rev-parse", "HEAD"]), moved);
                assert_eq!(
                    git(checkout, &["rev-parse", "HEAD"]),
                    head,
                    "prior destination stays unchanged"
                );
                imports.push(imported);
            }
            git(
                &cache,
                &["update-ref", &format!("refs/heads/{branch}"), &base],
            );
            let rejected = wss_rpc_raw(&mut rpc, 4, "workspace.create", params.clone()).await;
            assert!(
                rejected.get("error").is_some(),
                "changed cache branch must survive"
            );
            assert_eq!(git(&cache, &["rev-parse", branch]), base);
            imports.push(rejected);
            std::fs::write(
                fx.dir.path().join("repeat-pr-import-wire.json"),
                serde_json::to_vec_pretty(&imports).unwrap(),
            )
            .unwrap();
            assert!(matches!(
                workspace["checkoutMode"].as_str(),
                Some("cow" | "direct")
            ));
        }
        println!(
            "PR_CHECKOUT_MODE requested={mode} actual={}",
            workspace["checkoutMode"]
        );
    } else {
        assert!(
            response.get("error").is_some(),
            "must refuse unsafe checkout: {response}"
        );
        assert_eq!(git(&repo, &["rev-parse", "main"]), base);
        if conflict {
            assert_eq!(git(&repo, &["rev-parse", branch]), base);
            if cached {
                assert_eq!(git(&cache, &["rev-parse", branch]), base);
            }
        }
    }
    println!("PR_CHECKOUT_EVIDENCE mode={mode} branch={branch} available={available} conflict={conflict} base={base} head={head} response={response}");
}

#[intent_test_macros::daemon_test]
async fn canonical_fork_pr_head_is_checked_out() {
    for mode in ["worktree", "direct", "cache"] {
        checkout_case("fork-feature", true, false, mode).await;
    }
}
#[intent_test_macros::daemon_test]
async fn cached_same_repository_pr_head_survives_stale_tracking_ref() {
    checkout_case("topic", true, false, "cache-topic").await;
}
#[intent_test_macros::daemon_test]
async fn missing_pr_ref_cannot_fall_back_to_base() {
    for mode in ["worktree", "direct", "cache"] {
        checkout_case("fork-feature", false, false, mode).await;
    }
}
#[intent_test_macros::daemon_test]
async fn existing_local_branch_is_never_overwritten() {
    for mode in ["worktree", "direct", "cache"] {
        checkout_case("fork-feature", true, true, mode).await;
    }
}
#[intent_test_macros::daemon_test]
async fn fork_main_cannot_reuse_base_main() {
    for mode in ["worktree", "direct", "cache"] {
        checkout_case("main", true, true, mode).await;
    }
}
