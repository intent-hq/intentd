//! Stub upstreams for the `ExternalProxy` methods: a GitHub forge, a Linear
//! engine and a Sentry engine whose pages are as large as the real upstream
//! allows. Each honours the page size it is asked for, capped at the real
//! upstream's per-page maximum (GitHub REST 100, Linear 250, Sentry 100 —
//! the same caps the real engines clamp to), and fills every item with long
//! but realistic text ([`LONG_TEXT`]), so the sweep measures what one
//! clamped page can weigh.
//!
//! Calls that are not reads (create, merge, comment, …) delegate to the
//! scripted [`StubForge`].

use async_trait::async_trait;
use intent_services::test_support::stub_forge::{sample_pr, stub_issue, StubForge};
use intent_sourcecontrol::{
    AuthStatus, Branch, CheckRun, CheckState, Comment, CommentAnchor, Issue, IssueQuery,
    MergeMethod, MergeOptions, MergeOutcome, Mergeability, NewPullRequest, Page, PageParams,
    PrPatch, PrQuery, PullRequest, PullRequestFile, PullRequestFilesPage, PullRequestReview, Repo,
    RepoRef, Result as ScResult, Review, ReviewComment, ReviewThread, ReviewThreadComment,
    ReviewVerdict, ScCapabilities, SourceControl, UserIdentity,
};

/// Bytes of one long free-text field (PR or issue body, comment, patch,
/// description): a long agent-written PR description, a quarter of
/// GitHub's 65,536-character body limit.
pub const LONG_TEXT: usize = 16 * 1024;
/// GitHub REST's `per_page` maximum.
pub const GITHUB_PAGE_MAX: u8 = 100;
/// `intent-linear`'s engine clamp.
pub const LINEAR_PAGE_MAX: u32 = 250;
/// `intent-sentry`'s engine clamp.
pub const SENTRY_PAGE_MAX: u32 = 100;

fn text(seed: usize, bytes: usize) -> String {
    let mut s = format!("Item {seed}: ");
    while s.len() < bytes {
        s.push_str("the daemon must keep one response under its payload budget. ");
    }
    s.truncate(bytes);
    s
}

fn page_len(limit: u8) -> usize {
    usize::from(limit.clamp(1, GITHUB_PAGE_MAX))
}

fn next(cursor: Option<&str>) -> String {
    let n: u32 = cursor.and_then(|c| c.parse().ok()).unwrap_or(1);
    (n + 1).to_string()
}

fn pr(i: usize) -> PullRequest {
    PullRequest {
        number: 1_000 + i as u64,
        url: format!(
            "https://github.com/intent-hq/large-state-repo-0/pull/{}",
            1_000 + i
        ),
        title: text(i, 250),
        body: Some(text(i, LONG_TEXT)),
        source_branch: format!("agent/large-state-branch-{i}"),
        created_at: "2026-01-01T00:00:00Z".into(),
        updated_at: "2026-01-02T00:00:00Z".into(),
        ..sample_pr()
    }
}

fn issue(i: usize) -> Issue {
    Issue {
        number: 2_000 + i as u64,
        title: text(i, 250),
        body: Some(text(i, LONG_TEXT)),
        url: format!(
            "https://github.com/intent-hq/large-state-repo-0/issues/{}",
            2_000 + i
        ),
        ..stub_issue()
    }
}

fn repo(i: usize) -> Repo {
    Repo {
        owner_avatar_url: None,
        owner: "intent-hq".into(),
        name: format!("large-state-repo-{i}"),
        url: Some(format!("https://github.com/intent-hq/large-state-repo-{i}")),
        default_branch: Some("main".into()),
        created_at: Some("2026-01-01T00:00:00Z".into()),
        updated_at: Some("2026-01-02T00:00:00Z".into()),
    }
}

fn user(i: usize) -> UserIdentity {
    UserIdentity {
        login: format!("member-{i}"),
        id: Some(1_000_000 + i as u64),
        name: Some(format!("Member {i}")),
        avatar_url: Some(format!("https://avatars.example/u/{i}")),
        html_url: Some(format!("https://github.com/member-{i}")),
    }
}

/// A forge whose read endpoints answer full pages of long items.
#[derive(Default)]
pub struct OversizedForge {
    inner: StubForge,
}

#[async_trait]
impl SourceControl for OversizedForge {
    fn provider_id(&self) -> &'static str {
        "github"
    }
    fn capabilities(&self) -> ScCapabilities {
        self.inner.capabilities()
    }
    async fn check_auth(&self) -> ScResult<AuthStatus> {
        self.inner.check_auth().await
    }
    async fn get_user(&self) -> ScResult<UserIdentity> {
        self.inner.get_user().await
    }
    async fn get_user_by_login(&self, login: &str) -> ScResult<UserIdentity> {
        let n = login
            .rsplit('-')
            .next()
            .and_then(|n| n.parse().ok())
            .unwrap_or(0);
        Ok(UserIdentity {
            login: login.to_string(),
            ..user(10_000 + n)
        })
    }
    async fn search_users(&self, _query: &str, limit: u8) -> ScResult<Vec<UserIdentity>> {
        Ok((0..usize::from(limit)).map(user).collect())
    }
    async fn list_repos(&self, page: PageParams) -> ScResult<Page<Repo>> {
        Ok(Page {
            items: (0..page_len(page.limit)).map(repo).collect(),
            next_cursor: Some(next(page.cursor.as_deref())),
        })
    }
    async fn search_repos(&self, _query: &str, page: PageParams) -> ScResult<Page<Repo>> {
        self.list_repos(page).await
    }
    async fn get_repo(&self, _owner: &str, _name: &str) -> ScResult<Repo> {
        Ok(repo(0))
    }
    async fn list_remote_branches(
        &self,
        _owner: &str,
        _name: &str,
        _prefix: Option<&str>,
        page: PageParams,
    ) -> ScResult<Page<Branch>> {
        Ok(Page {
            items: (0..page_len(page.limit))
                .map(|i| Branch {
                    name: format!("agent/large-state-branch-{i}-{}", "x".repeat(80)),
                    commit_sha: Some(format!("{i:040x}")),
                    protected: false,
                })
                .collect(),
            next_cursor: Some(next(page.cursor.as_deref())),
        })
    }
    async fn get_file_content(
        &self,
        _repo: &RepoRef,
        _path: &str,
        _git_ref: Option<&str>,
    ) -> ScResult<Option<String>> {
        Ok(Some(repo_config_json()))
    }
    async fn create_pr(&self, repo: &RepoRef, input: NewPullRequest) -> ScResult<PullRequest> {
        self.inner.create_pr(repo, input).await
    }
    async fn get_pr(&self, _repo: &RepoRef, number: u64) -> ScResult<PullRequest> {
        Ok(PullRequest { number, ..pr(0) })
    }
    async fn list_prs(&self, _repo: &RepoRef, query: PrQuery) -> ScResult<Page<PullRequest>> {
        Ok(Page {
            items: (0..page_len(query.limit.unwrap_or(30))).map(pr).collect(),
            next_cursor: Some(next(query.cursor.as_deref())),
        })
    }
    async fn list_org_prs(&self, _org: &str, query: PrQuery) -> ScResult<Page<PullRequest>> {
        self.list_prs(&RepoRef::new("intent-hq", "x"), query).await
    }
    async fn pull_files(
        &self,
        _repo: &RepoRef,
        _number: u64,
        page: PageParams,
    ) -> ScResult<PullRequestFilesPage> {
        Ok(PullRequestFilesPage {
            items: (0..page_len(page.limit))
                .map(|i| PullRequestFile {
                    filename: format!("src/history/module_{i}.rs"),
                    previous_filename: None,
                    status: "modified".into(),
                    additions: 300,
                    deletions: 120,
                    changes: 420,
                    patch: Some(text(i, LONG_TEXT)),
                    url: Some(format!(
                        "https://github.com/intent-hq/x/blob/HEAD/src/history/module_{i}.rs"
                    )),
                })
                .collect(),
            next_cursor: Some(next(page.cursor.as_deref())),
            truncated: false,
        })
    }
    async fn pull_reviews(
        &self,
        _repo: &RepoRef,
        _number: u64,
        page: PageParams,
    ) -> ScResult<Page<PullRequestReview>> {
        Ok(Page {
            items: (0..page_len(page.limit))
                .map(|i| PullRequestReview {
                    id: i as u64,
                    author: format!("member-{i}"),
                    state: "COMMENTED".into(),
                    body: Some(text(i, LONG_TEXT)),
                    submitted_at: Some("2026-01-01T00:00:00Z".into()),
                    url: Some(format!("https://github.com/intent-hq/x/pull/1#review-{i}")),
                })
                .collect(),
            next_cursor: Some(next(page.cursor.as_deref())),
        })
    }
    async fn update_pr(
        &self,
        repo: &RepoRef,
        number: u64,
        patch: PrPatch,
    ) -> ScResult<PullRequest> {
        self.inner.update_pr(repo, number, patch).await
    }
    async fn merge_pr(
        &self,
        repo: &RepoRef,
        number: u64,
        method: MergeMethod,
        options: MergeOptions,
    ) -> ScResult<MergeOutcome> {
        self.inner.merge_pr(repo, number, method, options).await
    }
    async fn mergeability(&self, repo: &RepoRef, number: u64) -> ScResult<Mergeability> {
        self.inner.mergeability(repo, number).await
    }
    async fn update_branch(&self, repo: &RepoRef, number: u64) -> ScResult<()> {
        self.inner.update_branch(repo, number).await
    }
    async fn submit_review(
        &self,
        repo: &RepoRef,
        number: u64,
        verdict: ReviewVerdict,
        body: Option<String>,
    ) -> ScResult<Review> {
        self.inner.submit_review(repo, number, verdict, body).await
    }
    async fn list_reviews(&self, _repo: &RepoRef, _number: u64) -> ScResult<Vec<Review>> {
        Ok((0..usize::from(GITHUB_PAGE_MAX))
            .map(|i| Review {
                author: format!("member-{i}"),
                verdict: ReviewVerdict::Comment,
                body: Some(text(i, LONG_TEXT)),
                submitted_at: "2026-01-01T00:00:00Z".into(),
                id: Some(i.to_string()),
                commit_sha: Some(format!("{i:040x}")),
            })
            .collect())
    }
    async fn list_comments(&self, repo: &RepoRef, number: u64) -> ScResult<Vec<Comment>> {
        self.inner.list_comments(repo, number).await
    }
    async fn add_comment(
        &self,
        repo: &RepoRef,
        number: u64,
        body: &str,
        anchor: Option<CommentAnchor>,
    ) -> ScResult<Comment> {
        self.inner.add_comment(repo, number, body, anchor).await
    }
    async fn list_review_comments(
        &self,
        _repo: &RepoRef,
        _number: u64,
        page: PageParams,
    ) -> ScResult<Page<ReviewComment>> {
        Ok(Page {
            items: (0..page_len(page.limit))
                .map(|i| ReviewComment {
                    id: i as u64,
                    body: text(i, LONG_TEXT),
                    path: format!("src/history/module_{i}.rs"),
                    line: Some(i as u64),
                    author: format!("member-{i}"),
                    created_at: "2026-01-01T00:00:00Z".into(),
                    updated_at: "2026-01-01T00:00:00Z".into(),
                    in_reply_to_id: None,
                    url: format!("https://github.com/intent-hq/x/pull/1#discussion_r{i}"),
                })
                .collect(),
            next_cursor: Some(next(page.cursor.as_deref())),
        })
    }
    async fn reply_to_review_comment(
        &self,
        repo: &RepoRef,
        number: u64,
        comment_id: u64,
        body: &str,
    ) -> ScResult<ReviewComment> {
        self.inner
            .reply_to_review_comment(repo, number, comment_id, body)
            .await
    }
    async fn get_review_threads(
        &self,
        _repo: &RepoRef,
        _number: u64,
        page: PageParams,
    ) -> ScResult<Page<ReviewThread>> {
        Ok(Page {
            items: (0..page_len(page.limit))
                .map(|i| ReviewThread {
                    id: format!("RT{i}"),
                    is_resolved: false,
                    comments: (0..3)
                        .map(|c| ReviewThreadComment {
                            id: format!("RC{i}-{c}"),
                            body: text(i * 3 + c, LONG_TEXT / 4),
                            author: format!("member-{c}"),
                            path: format!("src/history/module_{i}.rs"),
                            line: Some(i as u64),
                            created_at: "2026-01-01T00:00:00Z".into(),
                        })
                        .collect(),
                })
                .collect(),
            next_cursor: Some(next(page.cursor.as_deref())),
        })
    }
    async fn resolve_thread(&self, thread_id: &str) -> ScResult<bool> {
        self.inner.resolve_thread(thread_id).await
    }
    async fn unresolve_thread(&self, thread_id: &str) -> ScResult<bool> {
        self.inner.unresolve_thread(thread_id).await
    }
    async fn check_runs(&self, _repo: &RepoRef, _git_ref: &str) -> ScResult<Vec<CheckRun>> {
        Ok((0..usize::from(GITHUB_PAGE_MAX))
            .map(|i| CheckRun {
                name: format!("ci / job {i} ({})", "matrix-entry ".repeat(6)),
                state: CheckState::Success,
                url: Some(format!(
                    "https://github.com/intent-hq/x/actions/runs/{i}/job/{i}"
                )),
                started_at: None,
            })
            .collect())
    }
    async fn create_issue(
        &self,
        repo: &RepoRef,
        title: &str,
        body: Option<&str>,
    ) -> ScResult<Issue> {
        self.inner.create_issue(repo, title, body).await
    }
    async fn get_issue(&self, _repo: &RepoRef, number: u64) -> ScResult<Issue> {
        Ok(Issue { number, ..issue(0) })
    }
    async fn list_issues(&self, _repo: &RepoRef, query: IssueQuery) -> ScResult<Page<Issue>> {
        Ok(Page {
            items: (0..page_len(query.limit.unwrap_or(30)))
                .map(issue)
                .collect(),
            next_cursor: Some(next(query.cursor.as_deref())),
        })
    }
}

fn linear_issue(i: usize) -> intent_linear::LinearIssueResult {
    intent_linear::LinearIssueResult {
        id: format!("issue-{i}"),
        identifier: format!("ENG-{i}"),
        title: text(i, 250),
        description: Some(text(i, LONG_TEXT)),
        url: Some(format!("https://linear.app/intent/issue/ENG-{i}")),
        team_name: Some("Engineering".into()),
        team_key: Some("ENG".into()),
        state: Some("In Progress".into()),
        priority: Some(2.0),
        assignee: Some(format!("Member {i}")),
        labels: Some((0..5).map(|l| format!("label-{l}")).collect()),
        project: Some("Large state".into()),
        creator: Some("Owner".into()),
        created_at: Some("2026-01-01T00:00:00Z".into()),
        updated_at: Some("2026-01-02T00:00:00Z".into()),
    }
}

fn linear_len(limit: Option<u32>) -> usize {
    limit.unwrap_or(50).clamp(1, LINEAR_PAGE_MAX) as usize
}

/// A Linear engine answering full pages of long issues.
pub struct OversizedLinear;

#[async_trait]
impl intent_linear::LinearEngine for OversizedLinear {
    async fn auth_status(&self) -> intent_linear::Result<intent_linear::AuthStatus> {
        Ok(intent_linear::AuthStatus {
            authenticated: true,
            login: Some("owner".into()),
            scopes: vec!["read".into()],
        })
    }
    async fn list_issues(
        &self,
        _filter: intent_linear::IssueFilter,
        limit: Option<u32>,
        _next_token: Option<&str>,
    ) -> intent_linear::Result<intent_linear::LinearIssuePage> {
        Ok(intent_linear::LinearIssuePage {
            issues: (0..linear_len(limit)).map(linear_issue).collect(),
            next_token: Some("next".into()),
        })
    }
    async fn search_issues(
        &self,
        _query: &str,
        limit: Option<u32>,
        next_token: Option<&str>,
    ) -> intent_linear::Result<intent_linear::LinearIssuePage> {
        self.list_issues(intent_linear::IssueFilter::default(), limit, next_token)
            .await
    }
    async fn get_issue(
        &self,
        _id: &str,
    ) -> intent_linear::Result<intent_linear::LinearIssueResult> {
        Ok(linear_issue(0))
    }
    async fn viewer(&self) -> intent_linear::Result<intent_linear::LinearUser> {
        Ok(intent_linear::LinearUser {
            id: "viewer".into(),
            name: "Owner".into(),
            display_name: Some("owner".into()),
            email: Some("owner@example.com".into()),
            avatar_url: None,
        })
    }
    async fn list_teams(
        &self,
        limit: Option<u32>,
    ) -> intent_linear::Result<Vec<intent_linear::LinearTeam>> {
        Ok((0..linear_len(limit))
            .map(|i| intent_linear::LinearTeam {
                id: format!("team-{i}"),
                key: format!("T{i}"),
                name: format!("Team {i}"),
                description: Some(text(i, 1_000)),
            })
            .collect())
    }
    async fn list_workflow_states(
        &self,
        limit: Option<u32>,
    ) -> intent_linear::Result<Vec<intent_linear::LinearWorkflowState>> {
        Ok((0..linear_len(limit))
            .map(|i| intent_linear::LinearWorkflowState {
                id: format!("state-{i}"),
                name: format!("State {i}"),
                r#type: "started".into(),
                description: Some(text(i, 500)),
                color: Some("#ffaa00".into()),
            })
            .collect())
    }
    async fn list_projects(
        &self,
        limit: Option<u32>,
    ) -> intent_linear::Result<Vec<intent_linear::LinearProject>> {
        Ok((0..linear_len(limit))
            .map(|i| intent_linear::LinearProject {
                id: format!("project-{i}"),
                name: format!("Project {i}"),
                description: Some(text(i, 2_000)),
                state: "started".into(),
                url: Some(format!("https://linear.app/intent/project/{i}")),
            })
            .collect())
    }
    async fn list_labels(
        &self,
        limit: Option<u32>,
    ) -> intent_linear::Result<Vec<intent_linear::LinearLabel>> {
        Ok((0..linear_len(limit))
            .map(|i| intent_linear::LinearLabel {
                id: format!("label-{i}"),
                name: format!("label-{i}"),
                description: Some(text(i, 300)),
                color: Some("#00aaff".into()),
            })
            .collect())
    }
    async fn create_issue(
        &self,
        _req: intent_linear::CreateIssueRequest,
    ) -> intent_linear::Result<intent_linear::LinearIssueResult> {
        Ok(linear_issue(0))
    }
    async fn update_issue(
        &self,
        _req: intent_linear::UpdateIssueRequest,
    ) -> intent_linear::Result<intent_linear::LinearIssueResult> {
        Ok(linear_issue(0))
    }
}

fn sentry_issue(i: usize) -> intent_sentry::SentryIssueResult {
    serde_json::from_value(serde_json::json!({
        "id": i.to_string(),
        "shortId": format!("APP-{i}"),
        "title": text(i, 500),
        "culprit": text(i, 300),
        "status": "unresolved",
        "level": "error",
        "count": "1200",
        "userCount": 40,
        "firstSeen": "2026-01-01T00:00:00Z",
        "lastSeen": "2026-01-02T00:00:00Z",
        "projectName": "App",
        "projectSlug": "app",
        "url": format!("https://sentry.io/organizations/intent/issues/{i}/"),
        "type": "TypeError",
        "value": text(i, 2_000),
        "filename": "src/app.ts",
        "function": "render",
    }))
    .expect("sentry issue")
}

fn sentry_len(limit: Option<u32>) -> usize {
    limit.unwrap_or(25).clamp(1, SENTRY_PAGE_MAX) as usize
}

/// A Sentry engine answering full pages of long issues.
pub struct OversizedSentry;

#[async_trait]
impl intent_sentry::SentryEngine for OversizedSentry {
    async fn auth_status(&self) -> intent_sentry::Result<intent_sentry::SentryAuthState> {
        Ok(intent_sentry::SentryAuthState {
            authenticated: true,
            organization: Some("intent".into()),
            error: None,
        })
    }
    async fn list_issues(
        &self,
        request: intent_sentry::FetchIssuesRequest,
    ) -> intent_sentry::Result<intent_sentry::SentryIssuePage> {
        Ok(intent_sentry::SentryIssuePage {
            issues: (0..sentry_len(request.limit)).map(sentry_issue).collect(),
            next_token: Some("next".into()),
        })
    }
    async fn search_issues(
        &self,
        _query: &str,
        _project: Option<&str>,
        limit: Option<u32>,
        _cursor: Option<&str>,
    ) -> intent_sentry::Result<intent_sentry::SentryIssuePage> {
        Ok(intent_sentry::SentryIssuePage {
            issues: (0..sentry_len(limit)).map(sentry_issue).collect(),
            next_token: Some("next".into()),
        })
    }
    async fn list_projects(
        &self,
        limit: Option<u32>,
    ) -> intent_sentry::Result<Vec<intent_sentry::SentryProject>> {
        Ok((0..sentry_len(limit))
            .map(|i| intent_sentry::SentryProject {
                id: i.to_string(),
                slug: format!("project-{i}"),
                name: format!("Project {i}"),
                platform: Some("javascript".into()),
                is_member: Some(true),
            })
            .collect())
    }
    async fn get_issue(
        &self,
        _id: &str,
    ) -> intent_sentry::Result<intent_sentry::SentryIssueResult> {
        Ok(sentry_issue(0))
    }
    async fn resolve_issue(
        &self,
        _id: &str,
    ) -> intent_sentry::Result<intent_sentry::SentryIssueResult> {
        Ok(sentry_issue(0))
    }
    async fn ignore_issue(
        &self,
        _id: &str,
    ) -> intent_sentry::Result<intent_sentry::SentryIssueResult> {
        Ok(sentry_issue(0))
    }
    async fn assign_issue(
        &self,
        _id: &str,
        _assigned_to: Option<&str>,
    ) -> intent_sentry::Result<intent_sentry::SentryIssueResult> {
        Ok(sentry_issue(0))
    }
}

/// A long `.intent/config.json`: every documented field filled, with long
/// agent instructions and many shared scripts. Served by the forge stub
/// and written into the big worktree.
pub fn repo_config_json() -> String {
    serde_json::json!({
        "branchPrefix": "agent/",
        "setupScript": text(1, 4 * 1024),
        "instructions": text(2, LONG_TEXT),
        "runScript": text(3, 1_024),
        "archiveScript": text(4, 1_024),
        "scripts": (0..40).map(|i| serde_json::json!({
            "name": format!("script-{i}"),
            "command": text(i, 400),
            "mode": "command",
            "cwd": "packages/app",
        })).collect::<Vec<_>>(),
        // Retired key that older repo configs still carry through `extra`.
        "cowCloneExclude": (0..20).map(|i| format!("cache/dir-{i}")).collect::<Vec<_>>(),
    })
    .to_string()
}

/// Inline replies carry at most this many codebase hits; more stream as
/// events (`search_ops::INLINE_THRESHOLD`).
pub const SEARCH_INLINE_MAX: usize = 25;

/// A context engine answering the largest inline page: exactly
/// [`SEARCH_INLINE_MAX`] hits with long previews.
pub struct OversizedContext;

#[async_trait]
impl intent_core::ContextEngine for OversizedContext {
    async fn availability(&self) -> intent_core::EngineAvailability {
        intent_core::EngineAvailability::Available {
            name: "large-state".into(),
            version: None,
        }
    }
    async fn retrieve(
        &self,
        _req: intent_core::RetrieveRequest,
    ) -> Result<intent_core::RetrieveResult, intent_core::ContextError> {
        Ok(intent_core::RetrieveResult {
            items: (0..SEARCH_INLINE_MAX)
                .map(|i| intent_core::RetrievedItem {
                    file: format!("src/history/module_{i}.rs"),
                    symbol: Some(format!("render_payload_{i}")),
                    line: Some(i as u64 + 1),
                    preview: text(i, 2 * 1024),
                    score: Some(1.0 / f64::from(u32::try_from(i + 1).unwrap_or(u32::MAX))),
                })
                .collect(),
        })
    }
}

/// GitHub's maximum username length.
const GITHUB_LOGIN_MAX: usize = 39;
/// `host.invite.searchAccounts`' result cap (its `limit` is 1–10).
const ACCOUNT_SEARCH_MAX: usize = 10;

/// A loopback stand-in for GitHub's public account directory
/// (`GET /search/users`), wired through `Services::with_github_api_base_uri`
/// so `host.invite.searchAccounts` never reaches `api.github.com` (a 504
/// there failed the sweep, intent-hq/intent#8703). Every reply carries the
/// most accounts the method returns, each with a maximal login and long
/// name and avatar fields, so the sweep measures the largest page.
///
/// The override redirects every GitHub API read that honours
/// `github_api_base_uri` (invite identity and collaboration verification
/// too), not only account search. Those paths get a 404 and are reported as
/// unexpected requests: a row that needs them must teach this directory the
/// endpoint rather than reach the real GitHub.
pub struct AccountDirectory {
    base: String,
    hits: std::sync::Arc<std::sync::atomic::AtomicUsize>,
    unexpected: std::sync::Arc<std::sync::Mutex<Vec<String>>>,
    task: tokio::task::JoinHandle<()>,
}

impl AccountDirectory {
    pub async fn start() -> Self {
        use std::sync::atomic::Ordering;
        use tokio::io::{AsyncReadExt, AsyncWriteExt};
        let listener = tokio::net::TcpListener::bind("127.0.0.1:0")
            .await
            .expect("bind account directory");
        let base = format!("http://{}", listener.local_addr().expect("local addr"));
        let hits = std::sync::Arc::new(std::sync::atomic::AtomicUsize::new(0));
        let unexpected = std::sync::Arc::new(std::sync::Mutex::new(Vec::new()));
        let body = account_page().to_string();
        let task = tokio::spawn({
            let hits = hits.clone();
            let unexpected = unexpected.clone();
            async move {
                loop {
                    let Ok((mut stream, _)) = listener.accept().await else {
                        return;
                    };
                    let (hits, unexpected, body) = (hits.clone(), unexpected.clone(), body.clone());
                    tokio::spawn(async move {
                        let mut head = Vec::new();
                        let mut chunk = [0u8; 1024];
                        while !head.windows(4).any(|w| w == b"\r\n\r\n") {
                            match stream.read(&mut chunk).await {
                                Ok(0) | Err(_) => return,
                                Ok(n) => head.extend_from_slice(&chunk[..n]),
                            }
                        }
                        let head = String::from_utf8_lossy(&head);
                        let line = head.lines().next().unwrap_or_default().to_string();
                        let (status, body) = if line.starts_with("GET /search/users?") {
                            hits.fetch_add(1, Ordering::SeqCst);
                            ("200 OK", body)
                        } else {
                            unexpected.lock().expect("unexpected log").push(line);
                            ("404 Not Found", "{}".to_string())
                        };
                        let reply = format!(
                            "HTTP/1.1 {status}\r\nContent-Type: application/json\r\n\
                             Content-Length: {}\r\nConnection: close\r\n\r\n{body}",
                            body.len()
                        );
                        let _ = stream.write_all(reply.as_bytes()).await;
                    });
                }
            }
        });
        Self {
            base,
            hits,
            unexpected,
            task,
        }
    }

    /// The API root to hand `with_github_api_base_uri`.
    pub fn base(&self) -> &str {
        &self.base
    }

    /// Searches served so far.
    pub fn hits(&self) -> usize {
        self.hits.load(std::sync::atomic::Ordering::SeqCst)
    }

    /// Request lines for any path the directory does not serve.
    pub fn unexpected(&self) -> Vec<String> {
        self.unexpected.lock().expect("unexpected log").clone()
    }
}

impl Drop for AccountDirectory {
    fn drop(&mut self) {
        self.task.abort();
    }
}

/// One `/search/users` page at the method's cap: distinct positive ids,
/// valid maximal logins, and long display names and avatar URLs.
fn account_page() -> serde_json::Value {
    let items: Vec<serde_json::Value> = (1..=ACCOUNT_SEARCH_MAX)
        .map(|i| {
            let mut login = format!("member{i}-");
            while login.len() < GITHUB_LOGIN_MAX {
                login.push('x');
            }
            serde_json::json!({
                "id": 9_000_000 + i,
                "login": login,
                "name": text(i, 255),
                "avatar_url": format!(
                    "https://avatars.githubusercontent.com/u/{}?v=4&{}",
                    9_000_000 + i,
                    "s".repeat(200)
                ),
            })
        })
        .collect();
    serde_json::json!({ "total_count": items.len(), "incomplete_results": false, "items": items })
}
