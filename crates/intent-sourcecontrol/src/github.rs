//! `GitHubSourceControl` — the v1 [`SourceControl`] impl backed by `octocrab`
//! (§7.3, §7.5).
//!
//! REST calls go through octocrab's generic `get`/`post`/`patch`/`put` helpers
//! (returning raw JSON we map ourselves, mirroring the TS ground truth which
//! parsed raw GitHub payloads), and review-thread resolution uses the GraphQL
//! client. octocrab does not surface GraphQL-level `errors`, so we validate
//! them here like `pr-comment.service.ts` did.

use async_trait::async_trait;
use serde_json::{json, Value};

use crate::error::{Error, Result};
use crate::model::*;
use crate::SourceControl;

/// GitHub implementation of [`SourceControl`].
pub struct GitHubSourceControl {
    client: octocrab::Octocrab,
}

impl GitHubSourceControl {
    /// Build a client from a personal token, optionally targeting a GitHub
    /// Enterprise instance via `api_base_url` (`octocrab` `.base_uri(...)`).
    pub fn new(token: &str, api_base_url: Option<&str>) -> Result<Self> {
        let mut builder = octocrab::Octocrab::builder().personal_token(token.to_string());
        if let Some(base) = api_base_url {
            builder = builder
                .base_uri(base)
                .map_err(|e| Error::Config(format!("invalid github apiBaseUrl {base:?}: {e}")))?;
        }
        let client = builder.build()?;
        Ok(Self { client })
    }

    fn repo_path(repo: &RepoRef, suffix: &str) -> String {
        format!("/repos/{}/{}{}", repo.owner, repo.name, suffix)
    }
}

/// Capabilities of the GitHub host (everything the trait models is supported).
pub(crate) const GITHUB_CAPABILITIES: ScCapabilities = ScCapabilities {
    draft_prs: true,
    squash_merge: true,
    rebase_merge: true,
    review_required_changes: true,
    check_runs: true,
    issues: true,
};

// --- REST pagination helpers (§5.5) ---

/// GitHub honors at most 100 items per REST page regardless of the requested
/// `limit`, so the page size sent upstream is clamped here and the
/// next-page heuristic is measured against the clamped size.
const REST_MAX_PER_PAGE: u8 = 100;

/// Parse an engine REST cursor (a 1-based `"<page>"` string) into a page
/// number; an absent or malformed cursor starts at page 1.
fn rest_page(cursor: Option<&str>) -> u64 {
    cursor
        .and_then(|c| c.parse::<u64>().ok())
        .filter(|p| *p >= 1)
        .unwrap_or(1)
}

/// Per-page size sent to GitHub for a requested `limit` (`1..=100`).
fn rest_per_page(limit: u8) -> u64 {
    limit.clamp(1, REST_MAX_PER_PAGE) as u64
}

/// The next REST cursor when the fetched page filled the per-page window — the
/// same count heuristic the prior single-page branch listing used (a final
/// exactly-full page costs one extra empty fetch; GitHub's `Link` header is not
/// surfaced by octocrab's generic `get`).
fn rest_next_cursor(page: u64, fetched: usize, per_page: u64) -> Option<String> {
    if per_page > 0 && fetched as u64 == per_page {
        Some((page + 1).to_string())
    } else {
        None
    }
}

// --- JSON → model mapping (pure; unit-tested with fixtures) ---

fn login_of(user: &Option<dto::User>) -> String {
    user.as_ref()
        .and_then(|u| u.login.clone())
        .unwrap_or_else(|| "unknown".to_string())
}

/// Derive normalized PR state (parity: `merged` wins over `closed`).
pub(crate) fn derive_pr_state(merged: bool, merged_at: Option<&str>, state: &str) -> PrState {
    if merged || merged_at.is_some() {
        PrState::Merged
    } else if state == "closed" {
        PrState::Closed
    } else {
        PrState::Open
    }
}

/// Map a check-run's `status`/`conclusion` onto [`CheckState`].
pub(crate) fn derive_check_state(status: &str, conclusion: Option<&str>) -> CheckState {
    if status != "completed" {
        return CheckState::Pending;
    }
    match conclusion {
        Some("success") => CheckState::Success,
        Some("neutral") | Some("skipped") => CheckState::Neutral,
        Some("cancelled") => CheckState::Cancelled,
        _ => CheckState::Failure,
    }
}

/// Map a GitHub review `state` string onto a [`ReviewVerdict`].
pub(crate) fn verdict_from_state(state: &str) -> ReviewVerdict {
    match state {
        "APPROVED" => ReviewVerdict::Approve,
        "CHANGES_REQUESTED" => ReviewVerdict::RequestChanges,
        _ => ReviewVerdict::Comment,
    }
}

/// The GitHub `event` value for a verdict when submitting a review.
fn verdict_event(verdict: ReviewVerdict) -> &'static str {
    match verdict {
        ReviewVerdict::Approve => "APPROVE",
        ReviewVerdict::RequestChanges => "REQUEST_CHANGES",
        ReviewVerdict::Comment => "COMMENT",
    }
}

pub(crate) fn map_pull(value: Value) -> Result<PullRequest> {
    let p: dto::Pull = serde_json::from_value(value)?;
    let head_sha = p.head.as_ref().and_then(|r| r.sha.clone());
    let source_branch = p.head.and_then(|r| r.r#ref).unwrap_or_default();
    Ok(PullRequest {
        number: p.number,
        url: p.html_url.unwrap_or_default(),
        title: p.title.unwrap_or_default(),
        body: p.body,
        state: derive_pr_state(
            p.merged,
            p.merged_at.as_deref(),
            p.state.as_deref().unwrap_or("open"),
        ),
        draft: p.draft,
        source_branch,
        target_branch: p.base.and_then(|r| r.r#ref).unwrap_or_default(),
        author: login_of(&p.user),
        mergeable: p.mergeable,
        mergeable_state: p.mergeable_state,
        head_sha,
        created_at: p.created_at.unwrap_or_default(),
        updated_at: p.updated_at.unwrap_or_default(),
    })
}

pub(crate) fn map_mergeability(value: Value) -> Result<Mergeability> {
    let p: dto::Pull = serde_json::from_value(value)?;
    let state = p.mergeable_state.as_deref().unwrap_or_default();
    Ok(Mergeability {
        mergeable: p.mergeable,
        conflicts: state == "dirty",
        required_checks_passed: state == "clean",
    })
}

pub(crate) fn map_issue(value: Value) -> Result<Issue> {
    let i: dto::Issue = serde_json::from_value(value)?;
    Ok(Issue {
        number: i.number,
        title: i.title.unwrap_or_default(),
        body: i.body,
        state: i.state.unwrap_or_default(),
        url: i.html_url.unwrap_or_default(),
    })
}

pub(crate) fn map_repo(value: Value) -> Result<Repo> {
    let r: dto::Repo = serde_json::from_value(value)?;
    Ok(Repo {
        owner: r.owner.and_then(|o| o.login).unwrap_or_default(),
        name: r.name.unwrap_or_default(),
        url: r.html_url,
        default_branch: r.default_branch,
        created_at: r.created_at,
        updated_at: r.updated_at,
    })
}

pub(crate) fn map_branch(value: Value) -> Result<Branch> {
    let b: dto::Branch = serde_json::from_value(value)?;
    Ok(Branch {
        name: b.name.unwrap_or_default(),
        commit_sha: b.commit.and_then(|c| c.sha),
        protected: b.protected,
    })
}

pub(crate) fn map_user_identity(value: Value) -> Result<UserIdentity> {
    let u: dto::UserFull = serde_json::from_value(value)?;
    Ok(UserIdentity {
        login: u.login.unwrap_or_default(),
        id: u.id,
        name: u.name,
        avatar_url: u.avatar_url,
        html_url: u.html_url,
    })
}

/// Rewrite raw repo-search input into GitHub `/search/repositories` syntax
/// (parity with the FE `buildRepoSearchQuery`): `owner/name` → `name
/// user:owner`, `owner/` → `user:owner`, `/name` → `name`, bare → unchanged.
pub(crate) fn build_repo_search_query(input: &str) -> String {
    let trimmed = input.trim();
    if trimmed.is_empty() {
        return String::new();
    }
    match trimmed.find('/') {
        None => trimmed.to_string(),
        Some(idx) => {
            let owner = trimmed[..idx].trim();
            let name = trimmed[idx + 1..].trim();
            match (owner.is_empty(), name.is_empty()) {
                (false, false) => format!("{name} user:{owner}"),
                (false, true) => format!("user:{owner}"),
                (true, false) => name.to_string(),
                (true, true) => String::new(),
            }
        }
    }
}

pub(crate) fn map_review(value: Value) -> Result<Review> {
    let r: dto::Review = serde_json::from_value(value)?;
    Ok(Review {
        author: login_of(&r.user),
        verdict: verdict_from_state(r.state.as_deref().unwrap_or_default()),
        body: r.body,
        submitted_at: r.submitted_at.unwrap_or_default(),
    })
}

pub(crate) fn map_issue_comment(value: Value) -> Result<Comment> {
    let c: dto::IssueComment = serde_json::from_value(value)?;
    Ok(Comment {
        id: c.id.to_string(),
        author: login_of(&c.user),
        body: c.body.unwrap_or_default(),
        path: None,
        line: None,
        created_at: c.created_at.unwrap_or_default(),
        url: c.html_url,
    })
}

pub(crate) fn map_review_comment(value: Value) -> Result<ReviewComment> {
    let c: dto::ReviewComment = serde_json::from_value(value)?;
    Ok(ReviewComment {
        id: c.id,
        body: c.body.unwrap_or_default(),
        path: c.path.unwrap_or_default(),
        line: c.line,
        author: login_of(&c.user),
        created_at: c.created_at.unwrap_or_default(),
        updated_at: c.updated_at.unwrap_or_default(),
        in_reply_to_id: c.in_reply_to_id,
        url: c.html_url.unwrap_or_default(),
    })
}

fn review_comment_as_comment(rc: ReviewComment) -> Comment {
    Comment {
        id: rc.id.to_string(),
        author: rc.author,
        body: rc.body,
        path: Some(rc.path),
        line: rc.line,
        created_at: rc.created_at,
        url: Some(rc.url),
    }
}

pub(crate) fn map_check_run(value: Value) -> Result<CheckRun> {
    let c: dto::CheckRun = serde_json::from_value(value)?;
    Ok(CheckRun {
        name: c.name.unwrap_or_default(),
        state: derive_check_state(
            c.status.as_deref().unwrap_or_default(),
            c.conclusion.as_deref(),
        ),
        url: c.html_url.or(c.details_url),
    })
}

pub(crate) fn map_review_thread(value: Value) -> Result<ReviewThread> {
    let t: dto::ReviewThreadNode = serde_json::from_value(value)?;
    let comments = t
        .comments
        .map(|c| c.nodes)
        .unwrap_or_default()
        .into_iter()
        .map(|c| ReviewThreadComment {
            id: c.id.unwrap_or_default(),
            body: c.body.unwrap_or_default(),
            author: login_of(&c.author),
            path: c.path.unwrap_or_default(),
            line: c.line,
            created_at: c.created_at.unwrap_or_default(),
        })
        .collect();
    Ok(ReviewThread {
        id: t.id.unwrap_or_default(),
        is_resolved: t.is_resolved,
        comments,
    })
}

fn map_list<T>(value: Value, f: impl Fn(Value) -> Result<T>) -> Result<Vec<T>> {
    let items: Vec<Value> = serde_json::from_value(value)?;
    items.into_iter().map(f).collect()
}

/// Validate a GraphQL envelope and return its `data` object (parity with
/// `validateGraphQLResponse` in `pr-comment.service.ts`).
fn graphql_data(mut resp: Value) -> Result<Value> {
    if let Some(errors) = resp.get("errors").and_then(Value::as_array) {
        if !errors.is_empty() {
            let msg = errors[0]
                .get("message")
                .and_then(Value::as_str)
                .unwrap_or("unknown GraphQL error");
            return Err(Error::Api(format!("graphql error: {msg}")));
        }
    }
    match resp.get_mut("data") {
        Some(data) if !data.is_null() => Ok(data.take()),
        _ => Err(Error::Api("graphql response returned no data".to_string())),
    }
}

mod dto {
    use serde::Deserialize;

    #[derive(Deserialize)]
    pub(super) struct User {
        pub login: Option<String>,
    }

    #[derive(Deserialize)]
    pub(super) struct UserFull {
        pub login: Option<String>,
        pub id: Option<u64>,
        pub name: Option<String>,
        pub avatar_url: Option<String>,
        pub html_url: Option<String>,
    }

    #[derive(Deserialize)]
    pub(super) struct RepoOwner {
        pub login: Option<String>,
    }

    #[derive(Deserialize)]
    pub(super) struct Repo {
        pub name: Option<String>,
        pub owner: Option<RepoOwner>,
        pub html_url: Option<String>,
        pub default_branch: Option<String>,
        pub created_at: Option<String>,
        pub updated_at: Option<String>,
    }

    #[derive(Deserialize)]
    pub(super) struct BranchCommit {
        pub sha: Option<String>,
    }

    #[derive(Deserialize)]
    pub(super) struct Branch {
        pub name: Option<String>,
        pub commit: Option<BranchCommit>,
        #[serde(default)]
        pub protected: bool,
    }

    #[derive(Deserialize)]
    pub(super) struct GitRef {
        #[serde(rename = "ref")]
        pub r#ref: Option<String>,
        pub sha: Option<String>,
    }

    #[derive(Deserialize)]
    pub(super) struct Pull {
        pub number: u64,
        pub html_url: Option<String>,
        pub title: Option<String>,
        pub body: Option<String>,
        pub state: Option<String>,
        #[serde(default)]
        pub draft: bool,
        #[serde(default)]
        pub merged: bool,
        pub merged_at: Option<String>,
        pub mergeable: Option<bool>,
        pub mergeable_state: Option<String>,
        pub user: Option<User>,
        pub head: Option<GitRef>,
        pub base: Option<GitRef>,
        pub created_at: Option<String>,
        pub updated_at: Option<String>,
    }

    #[derive(Deserialize)]
    pub(super) struct Issue {
        pub number: u64,
        pub title: Option<String>,
        pub body: Option<String>,
        pub state: Option<String>,
        pub html_url: Option<String>,
    }

    #[derive(Deserialize)]
    pub(super) struct Review {
        pub user: Option<User>,
        pub state: Option<String>,
        pub body: Option<String>,
        pub submitted_at: Option<String>,
    }

    #[derive(Deserialize)]
    pub(super) struct IssueComment {
        pub id: u64,
        pub user: Option<User>,
        pub body: Option<String>,
        pub created_at: Option<String>,
        pub html_url: Option<String>,
    }

    #[derive(Deserialize)]
    pub(super) struct ReviewComment {
        pub id: u64,
        pub body: Option<String>,
        pub path: Option<String>,
        pub line: Option<u64>,
        pub user: Option<User>,
        pub created_at: Option<String>,
        pub updated_at: Option<String>,
        pub in_reply_to_id: Option<u64>,
        pub html_url: Option<String>,
    }

    #[derive(Deserialize)]
    pub(super) struct CheckRun {
        pub name: Option<String>,
        pub status: Option<String>,
        pub conclusion: Option<String>,
        pub html_url: Option<String>,
        pub details_url: Option<String>,
    }

    #[derive(Deserialize)]
    pub(super) struct ReviewThreadComment {
        pub id: Option<String>,
        pub body: Option<String>,
        pub author: Option<User>,
        pub path: Option<String>,
        pub line: Option<u64>,
        #[serde(rename = "createdAt")]
        pub created_at: Option<String>,
    }

    #[derive(Deserialize)]
    pub(super) struct ReviewThreadComments {
        pub nodes: Vec<ReviewThreadComment>,
    }

    #[derive(Deserialize)]
    pub(super) struct ReviewThreadNode {
        pub id: Option<String>,
        #[serde(default, rename = "isResolved")]
        pub is_resolved: bool,
        pub comments: Option<ReviewThreadComments>,
    }
}

const REVIEW_THREADS_QUERY: &str = r#"
query GetReviewThreads($owner: String!, $repo: String!, $prNumber: Int!, $first: Int!, $after: String) {
  repository(owner: $owner, name: $repo) {
    pullRequest(number: $prNumber) {
      reviewThreads(first: $first, after: $after) {
        pageInfo { hasNextPage endCursor }
        nodes {
          id
          isResolved
          comments(first: 100) {
            nodes { id body author { login } path line createdAt }
          }
        }
      }
    }
  }
}
"#;

#[async_trait]
impl SourceControl for GitHubSourceControl {
    fn provider_id(&self) -> &'static str {
        "github"
    }

    fn capabilities(&self) -> ScCapabilities {
        GITHUB_CAPABILITIES
    }

    async fn check_auth(&self) -> Result<AuthStatus> {
        match self.client.get::<Value, _, ()>("/user", None::<&()>).await {
            Ok(user) => Ok(AuthStatus {
                authenticated: true,
                login: user.get("login").and_then(Value::as_str).map(String::from),
                scopes: Vec::new(),
            }),
            Err(_) => Ok(AuthStatus {
                authenticated: false,
                login: None,
                scopes: Vec::new(),
            }),
        }
    }

    async fn get_user(&self) -> Result<UserIdentity> {
        let v: Value = self.client.get("/user", None::<&()>).await?;
        map_user_identity(v)
    }

    async fn list_repos(&self, page: PageParams) -> Result<Page<Repo>> {
        let per_page = rest_per_page(page.limit);
        let page_no = rest_page(page.cursor.as_deref());
        let params: Vec<(&str, String)> = vec![
            ("per_page", per_page.to_string()),
            ("page", page_no.to_string()),
            ("sort", "updated".to_string()),
        ];
        let v: Value = self.client.get("/user/repos", Some(&params)).await?;
        let items: Vec<Value> = serde_json::from_value(v)?;
        let fetched = items.len();
        let repos = items
            .into_iter()
            .map(map_repo)
            .collect::<Result<Vec<_>>>()?;
        Ok(Page {
            items: repos,
            next_cursor: rest_next_cursor(page_no, fetched, per_page),
        })
    }

    async fn search_repos(&self, query: &str, page: PageParams) -> Result<Page<Repo>> {
        let search_query = build_repo_search_query(query);
        if search_query.is_empty() {
            return Ok(Page {
                items: Vec::new(),
                next_cursor: None,
            });
        }
        let per_page = rest_per_page(page.limit);
        let page_no = rest_page(page.cursor.as_deref());
        let params: Vec<(&str, String)> = vec![
            ("q", search_query),
            ("sort", "stars".to_string()),
            ("order", "desc".to_string()),
            ("per_page", per_page.to_string()),
            ("page", page_no.to_string()),
        ];
        let v: Value = self
            .client
            .get("/search/repositories", Some(&params))
            .await?;
        let items: Vec<Value> = serde_json::from_value(
            v.get("items")
                .cloned()
                .unwrap_or_else(|| Value::Array(Vec::new())),
        )?;
        let fetched = items.len();
        let repos = items
            .into_iter()
            .map(map_repo)
            .collect::<Result<Vec<_>>>()?;
        Ok(Page {
            items: repos,
            next_cursor: rest_next_cursor(page_no, fetched, per_page),
        })
    }

    async fn get_repo(&self, owner: &str, name: &str) -> Result<Repo> {
        let route = format!("/repos/{owner}/{name}");
        let v: Value = self.client.get(&route, None::<&()>).await?;
        map_repo(v)
    }

    async fn list_remote_branches(
        &self,
        owner: &str,
        name: &str,
        page: PageParams,
    ) -> Result<Page<Branch>> {
        let per_page = rest_per_page(page.limit);
        let page_no = rest_page(page.cursor.as_deref());
        let route = format!("/repos/{owner}/{name}/branches");
        let params: Vec<(&str, String)> = vec![
            ("per_page", per_page.to_string()),
            ("page", page_no.to_string()),
        ];
        let v: Value = self.client.get(&route, Some(&params)).await?;
        let items: Vec<Value> = serde_json::from_value(v)?;
        let fetched = items.len();
        let branches = items
            .into_iter()
            .map(map_branch)
            .collect::<Result<Vec<_>>>()?;
        Ok(Page {
            items: branches,
            next_cursor: rest_next_cursor(page_no, fetched, per_page),
        })
    }

    async fn create_pr(&self, repo: &RepoRef, input: NewPullRequest) -> Result<PullRequest> {
        let body = json!({
            "title": input.title,
            "head": input.source_branch,
            "base": input.target_branch,
            "body": input.body.unwrap_or_default(),
            "draft": input.draft,
        });
        let route = Self::repo_path(repo, "/pulls");
        let v: Value = self.client.post(route, Some(&body)).await?;
        map_pull(v)
    }

    async fn get_pr(&self, repo: &RepoRef, number: u64) -> Result<PullRequest> {
        let route = Self::repo_path(repo, &format!("/pulls/{number}"));
        let v: Value = self.client.get(&route, None::<&()>).await?;
        map_pull(v)
    }

    async fn list_prs(&self, repo: &RepoRef, query: PrQuery) -> Result<Page<PullRequest>> {
        let per_page = rest_per_page(query.limit.unwrap_or(30));
        let page_no = rest_page(query.cursor.as_deref());
        if let Some(involvement) = query.involvement {
            // GitHub's `/pulls` listing cannot express assignee/review-requested/
            // involves @me, so route involvement queries through `/search/issues`
            // (parity with the FE `searchGitHubPullRequests`).
            let state = match query.state {
                Some(PrState::Closed) => "closed",
                Some(PrState::Merged) => "merged",
                _ => "open",
            };
            let involve = match involvement {
                PrInvolvement::Created => "author:@me",
                PrInvolvement::Assigned => "assignee:@me",
                PrInvolvement::ReviewRequested => "review-requested:@me",
                PrInvolvement::Involves => "involves:@me",
            };
            let q = format!(
                "is:pr repo:{}/{} is:{state} {involve}",
                repo.owner, repo.name
            );
            let params: Vec<(&str, String)> = vec![
                ("q", q),
                ("sort", "updated".to_string()),
                ("order", "desc".to_string()),
                ("per_page", per_page.to_string()),
                ("page", page_no.to_string()),
            ];
            let v: Value = self.client.get("/search/issues", Some(&params)).await?;
            let items: Vec<Value> = serde_json::from_value(
                v.get("items")
                    .cloned()
                    .unwrap_or_else(|| Value::Array(Vec::new())),
            )?;
            let fetched = items.len();
            let prs = items
                .into_iter()
                .map(map_pull)
                .collect::<Result<Vec<_>>>()?;
            return Ok(Page {
                items: prs,
                next_cursor: rest_next_cursor(page_no, fetched, per_page),
            });
        }
        let mut params: Vec<(&str, String)> = vec![(
            "state",
            match query.state {
                Some(PrState::Open) => "open",
                Some(PrState::Closed) => "closed",
                _ => "all",
            }
            .to_string(),
        )];
        if let Some(base) = &query.base {
            params.push(("base", base.clone()));
        }
        if let Some(head) = &query.head {
            params.push(("head", head.clone()));
        }
        params.push(("per_page", per_page.to_string()));
        params.push(("page", page_no.to_string()));
        let route = Self::repo_path(repo, "/pulls");
        let v: Value = self.client.get(&route, Some(&params)).await?;
        let raw: Vec<Value> = serde_json::from_value(v)?;
        // Paging is measured on the raw GitHub page; the optional client-side
        // `author` filter only narrows what this page returns.
        let next_cursor = rest_next_cursor(page_no, raw.len(), per_page);
        let prs = raw.into_iter().map(map_pull).collect::<Result<Vec<_>>>()?;
        let items = match &query.author {
            Some(author) => prs.into_iter().filter(|p| &p.author == author).collect(),
            None => prs,
        };
        Ok(Page { items, next_cursor })
    }

    async fn update_pr(&self, repo: &RepoRef, number: u64, patch: PrPatch) -> Result<PullRequest> {
        let mut body = serde_json::Map::new();
        if let Some(title) = patch.title {
            body.insert("title".into(), json!(title));
        }
        if let Some(b) = patch.body {
            body.insert("body".into(), json!(b));
        }
        if let Some(base) = patch.target_branch {
            body.insert("base".into(), json!(base));
        }
        match patch.state {
            Some(PrState::Open) => {
                body.insert("state".into(), json!("open"));
            }
            Some(PrState::Closed) | Some(PrState::Merged) => {
                body.insert("state".into(), json!("closed"));
            }
            None => {}
        }
        let route = Self::repo_path(repo, &format!("/pulls/{number}"));
        let v: Value = self.client.patch(route, Some(&Value::Object(body))).await?;
        map_pull(v)
    }

    async fn merge_pr(
        &self,
        repo: &RepoRef,
        number: u64,
        method: MergeMethod,
        options: MergeOptions,
    ) -> Result<MergeOutcome> {
        let mut body = serde_json::Map::new();
        body.insert(
            "merge_method".into(),
            json!(match method {
                MergeMethod::Merge => "merge",
                MergeMethod::Squash => "squash",
                MergeMethod::Rebase => "rebase",
            }),
        );
        if let Some(title) = options.commit_title {
            body.insert("commit_title".into(), json!(title));
        }
        if let Some(message) = options.commit_message {
            body.insert("commit_message".into(), json!(message));
        }
        let route = Self::repo_path(repo, &format!("/pulls/{number}/merge"));
        let v: Value = self.client.put(route, Some(&Value::Object(body))).await?;
        Ok(MergeOutcome {
            merged: v.get("merged").and_then(Value::as_bool).unwrap_or(false),
            message: v
                .get("message")
                .and_then(Value::as_str)
                .unwrap_or_default()
                .to_string(),
            sha: v.get("sha").and_then(Value::as_str).map(String::from),
        })
    }

    async fn mergeability(&self, repo: &RepoRef, number: u64) -> Result<Mergeability> {
        let route = Self::repo_path(repo, &format!("/pulls/{number}"));
        let v: Value = self.client.get(&route, None::<&()>).await?;
        map_mergeability(v)
    }

    async fn update_branch(&self, repo: &RepoRef, number: u64) -> Result<()> {
        let route = Self::repo_path(repo, &format!("/pulls/{number}/update-branch"));
        let _: Value = self.client.put(route, None::<&()>).await?;
        Ok(())
    }

    async fn submit_review(
        &self,
        repo: &RepoRef,
        number: u64,
        verdict: ReviewVerdict,
        body: Option<String>,
    ) -> Result<Review> {
        let mut payload = serde_json::Map::new();
        payload.insert("event".into(), json!(verdict_event(verdict)));
        if let Some(b) = body {
            payload.insert("body".into(), json!(b));
        }
        let route = Self::repo_path(repo, &format!("/pulls/{number}/reviews"));
        let v: Value = self
            .client
            .post(route, Some(&Value::Object(payload)))
            .await?;
        map_review(v)
    }

    async fn list_reviews(&self, repo: &RepoRef, number: u64) -> Result<Vec<Review>> {
        let route = Self::repo_path(repo, &format!("/pulls/{number}/reviews?per_page=100"));
        let v: Value = self.client.get(&route, None::<&()>).await?;
        map_list(v, map_review)
    }

    async fn list_comments(&self, repo: &RepoRef, number: u64) -> Result<Vec<Comment>> {
        let route = Self::repo_path(
            repo,
            &format!("/issues/{number}/comments?per_page=100&sort=created&direction=desc"),
        );
        let v: Value = self.client.get(&route, None::<&()>).await?;
        map_list(v, map_issue_comment)
    }

    async fn add_comment(
        &self,
        repo: &RepoRef,
        number: u64,
        body: &str,
        anchor: Option<CommentAnchor>,
    ) -> Result<Comment> {
        match anchor {
            None => {
                let route = Self::repo_path(repo, &format!("/issues/{number}/comments"));
                let v: Value = self
                    .client
                    .post(route, Some(&json!({ "body": body })))
                    .await?;
                map_issue_comment(v)
            }
            Some(anchor) => {
                let mut payload = serde_json::Map::new();
                payload.insert("body".into(), json!(body));
                payload.insert("path".into(), json!(anchor.path));
                payload.insert("line".into(), json!(anchor.line));
                if let Some(side) = anchor.side {
                    payload.insert("side".into(), json!(side));
                }
                let route = Self::repo_path(repo, &format!("/pulls/{number}/comments"));
                let v: Value = self
                    .client
                    .post(route, Some(&Value::Object(payload)))
                    .await?;
                Ok(review_comment_as_comment(map_review_comment(v)?))
            }
        }
    }

    async fn list_review_comments(
        &self,
        repo: &RepoRef,
        number: u64,
        page: PageParams,
    ) -> Result<Page<ReviewComment>> {
        let per_page = rest_per_page(page.limit);
        let page_no = rest_page(page.cursor.as_deref());
        let route = Self::repo_path(repo, &format!("/pulls/{number}/comments"));
        let params: Vec<(&str, String)> = vec![
            ("per_page", per_page.to_string()),
            ("page", page_no.to_string()),
            ("sort", "created".to_string()),
            ("direction", "desc".to_string()),
        ];
        let v: Value = self.client.get(&route, Some(&params)).await?;
        let items: Vec<Value> = serde_json::from_value(v)?;
        let fetched = items.len();
        let comments = items
            .into_iter()
            .map(map_review_comment)
            .collect::<Result<Vec<_>>>()?;
        Ok(Page {
            items: comments,
            next_cursor: rest_next_cursor(page_no, fetched, per_page),
        })
    }

    async fn reply_to_review_comment(
        &self,
        repo: &RepoRef,
        number: u64,
        comment_id: u64,
        body: &str,
    ) -> Result<ReviewComment> {
        let route = Self::repo_path(repo, &format!("/pulls/{number}/comments"));
        let payload = json!({ "body": body, "in_reply_to": comment_id });
        let v: Value = self.client.post(route, Some(&payload)).await?;
        map_review_comment(v)
    }

    async fn get_review_threads(
        &self,
        repo: &RepoRef,
        number: u64,
        page: PageParams,
    ) -> Result<Page<ReviewThread>> {
        // GraphQL caps `first` at 100; the cursor is the native `endCursor`.
        let first = page.limit.clamp(1, 100) as i64;
        let after = page.cursor.clone();
        let payload = json!({
            "query": REVIEW_THREADS_QUERY,
            "variables": {
                "owner": repo.owner,
                "repo": repo.name,
                "prNumber": number,
                "first": first,
                "after": after,
            },
        });
        let resp: Value = self.client.graphql(&payload).await?;
        let data = graphql_data(resp)?;
        let threads = data.pointer("/repository/pullRequest/reviewThreads");
        let nodes = threads
            .and_then(|t| t.get("nodes"))
            .cloned()
            .unwrap_or_else(|| Value::Array(Vec::new()));
        let has_next = threads
            .and_then(|t| t.pointer("/pageInfo/hasNextPage"))
            .and_then(Value::as_bool)
            .unwrap_or(false);
        let end_cursor = threads
            .and_then(|t| t.pointer("/pageInfo/endCursor"))
            .and_then(Value::as_str)
            .map(String::from);
        let items = map_list(nodes, map_review_thread)?;
        Ok(Page {
            items,
            next_cursor: if has_next { end_cursor } else { None },
        })
    }

    async fn resolve_thread(&self, thread_id: &str) -> Result<bool> {
        self.set_thread_resolution(thread_id, true).await
    }

    async fn unresolve_thread(&self, thread_id: &str) -> Result<bool> {
        self.set_thread_resolution(thread_id, false).await
    }

    async fn check_runs(&self, repo: &RepoRef, git_ref: &str) -> Result<Vec<CheckRun>> {
        let route = Self::repo_path(repo, &format!("/commits/{git_ref}/check-runs"));
        let v: Value = self.client.get(&route, None::<&()>).await?;
        let runs = v
            .get("check_runs")
            .cloned()
            .unwrap_or_else(|| Value::Array(Vec::new()));
        map_list(runs, map_check_run)
    }

    async fn create_issue(&self, repo: &RepoRef, title: &str, body: Option<&str>) -> Result<Issue> {
        let mut payload = serde_json::Map::new();
        payload.insert("title".into(), json!(title));
        if let Some(b) = body {
            payload.insert("body".into(), json!(b));
        }
        let route = Self::repo_path(repo, "/issues");
        let v: Value = self
            .client
            .post(route, Some(&Value::Object(payload)))
            .await?;
        map_issue(v)
    }

    async fn get_issue(&self, repo: &RepoRef, number: u64) -> Result<Issue> {
        let route = Self::repo_path(repo, &format!("/issues/{number}"));
        let v: Value = self.client.get(&route, None::<&()>).await?;
        map_issue(v)
    }

    async fn list_issues(&self, repo: &RepoRef, query: IssueQuery) -> Result<Page<Issue>> {
        let per_page = rest_per_page(query.limit.unwrap_or(30));
        let page_no = rest_page(query.cursor.as_deref());
        let mut params: Vec<(&str, String)> = vec![(
            "state",
            query.state.clone().unwrap_or_else(|| "open".into()),
        )];
        if let Some(labels) = &query.labels {
            params.push(("labels", labels.clone()));
        }
        params.push(("per_page", per_page.to_string()));
        params.push(("page", page_no.to_string()));
        let route = Self::repo_path(repo, "/issues");
        let v: Value = self.client.get(&route, Some(&params)).await?;
        let raw: Vec<Value> = serde_json::from_value(v)?;
        // Paging is measured on the raw GitHub page; the `pull_request` filter
        // only narrows what this page returns (parity with the `author` filter
        // on `list_prs`).
        let next_cursor = rest_next_cursor(page_no, raw.len(), per_page);
        let items = raw
            .into_iter()
            .filter(|i| i.get("pull_request").is_none())
            .map(map_issue)
            .collect::<Result<Vec<_>>>()?;
        Ok(Page { items, next_cursor })
    }
}

impl GitHubSourceControl {
    async fn set_thread_resolution(&self, thread_id: &str, resolve: bool) -> Result<bool> {
        let (mutation, field) = if resolve {
            (
                "mutation($id: ID!) { resolveReviewThread(input: { threadId: $id }) { thread { id isResolved } } }",
                "resolveReviewThread",
            )
        } else {
            (
                "mutation($id: ID!) { unresolveReviewThread(input: { threadId: $id }) { thread { id isResolved } } }",
                "unresolveReviewThread",
            )
        };
        let payload = json!({ "query": mutation, "variables": { "id": thread_id } });
        let resp: Value = self.client.graphql(&payload).await?;
        let data = graphql_data(resp)?;
        Ok(data
            .pointer(&format!("/{field}/thread/isResolved"))
            .and_then(Value::as_bool)
            .unwrap_or(false))
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn maps_merged_pull_request() {
        let v = json!({
            "number": 42,
            "html_url": "https://github.com/o/r/pull/42",
            "title": "Add thing",
            "body": "does the thing",
            "state": "closed",
            "draft": false,
            "merged": true,
            "merged_at": "2026-01-02T03:04:05Z",
            "mergeable": null,
            "user": { "login": "octocat" },
            "head": { "ref": "feature" },
            "base": { "ref": "main" },
            "created_at": "2026-01-01T00:00:00Z",
            "updated_at": "2026-01-02T03:04:05Z"
        });
        let pr = map_pull(v).unwrap();
        assert_eq!(pr.number, 42);
        assert_eq!(pr.state, PrState::Merged);
        assert_eq!(pr.source_branch, "feature");
        assert_eq!(pr.target_branch, "main");
        assert_eq!(pr.author, "octocat");
    }

    #[test]
    fn maps_head_sha_and_mergeable_state() {
        let v = json!({
            "number": 7,
            "state": "open",
            "mergeable": true,
            "mergeable_state": "clean",
            "head": { "ref": "feature", "sha": "abc123" },
            "base": { "ref": "main" }
        });
        let pr = map_pull(v).unwrap();
        assert_eq!(pr.head_sha.as_deref(), Some("abc123"));
        assert_eq!(pr.mergeable_state.as_deref(), Some("clean"));
        assert_eq!(pr.source_branch, "feature");
    }

    #[test]
    fn open_draft_pull_request_state() {
        let v = json!({ "number": 1, "state": "open", "draft": true });
        let pr = map_pull(v).unwrap();
        assert_eq!(pr.state, PrState::Open);
        assert!(pr.draft);
        assert_eq!(pr.author, "unknown");
    }

    #[test]
    fn maps_mergeability_states() {
        let dirty = map_mergeability(
            json!({ "number": 1, "mergeable": false, "mergeable_state": "dirty" }),
        )
        .unwrap();
        assert!(dirty.conflicts);
        assert!(!dirty.required_checks_passed);
        let clean =
            map_mergeability(json!({ "number": 1, "mergeable": true, "mergeable_state": "clean" }))
                .unwrap();
        assert!(!clean.conflicts);
        assert!(clean.required_checks_passed);
    }

    #[test]
    fn check_state_mapping() {
        assert_eq!(derive_check_state("in_progress", None), CheckState::Pending);
        assert_eq!(
            derive_check_state("completed", Some("success")),
            CheckState::Success
        );
        assert_eq!(
            derive_check_state("completed", Some("skipped")),
            CheckState::Neutral
        );
        assert_eq!(
            derive_check_state("completed", Some("cancelled")),
            CheckState::Cancelled
        );
        assert_eq!(
            derive_check_state("completed", Some("timed_out")),
            CheckState::Failure
        );
    }

    #[test]
    fn maps_check_run_fixture() {
        let cr = map_check_run(json!({
            "name": "build",
            "status": "completed",
            "conclusion": "failure",
            "details_url": "https://ci/run/1"
        }))
        .unwrap();
        assert_eq!(cr.name, "build");
        assert_eq!(cr.state, CheckState::Failure);
        assert_eq!(cr.url.as_deref(), Some("https://ci/run/1"));
    }

    #[test]
    fn maps_review_and_verdict() {
        let r = map_review(json!({
            "user": { "login": "rev" },
            "state": "CHANGES_REQUESTED",
            "body": "please fix",
            "submitted_at": "2026-01-01T00:00:00Z"
        }))
        .unwrap();
        assert_eq!(r.author, "rev");
        assert_eq!(r.verdict, ReviewVerdict::RequestChanges);
        assert_eq!(verdict_from_state("APPROVED"), ReviewVerdict::Approve);
        assert_eq!(verdict_from_state("COMMENTED"), ReviewVerdict::Comment);
    }

    #[test]
    fn maps_review_comment_fixture() {
        let rc = map_review_comment(json!({
            "id": 555,
            "body": "nit",
            "path": "src/lib.rs",
            "line": 10,
            "user": { "login": "rev" },
            "created_at": "2026-01-01T00:00:00Z",
            "updated_at": "2026-01-01T00:00:00Z",
            "in_reply_to_id": 554,
            "html_url": "https://github.com/o/r/pull/1#c555"
        }))
        .unwrap();
        assert_eq!(rc.id, 555);
        assert_eq!(rc.line, Some(10));
        assert_eq!(rc.in_reply_to_id, Some(554));
    }

    #[test]
    fn maps_review_thread_fixture() {
        let thread = map_review_thread(json!({
            "id": "RT_1",
            "isResolved": true,
            "comments": { "nodes": [
                { "id": "RC_1", "body": "hi", "author": { "login": "rev" }, "path": "a.rs", "line": 3, "createdAt": "2026-01-01T00:00:00Z" }
            ] }
        }))
        .unwrap();
        assert_eq!(thread.id, "RT_1");
        assert!(thread.is_resolved);
        assert_eq!(thread.comments.len(), 1);
        assert_eq!(thread.comments[0].author, "rev");
    }

    #[test]
    fn graphql_data_extracts_and_validates() {
        let data = graphql_data(json!({ "data": { "x": 1 } })).unwrap();
        assert_eq!(data, json!({ "x": 1 }));

        let err =
            graphql_data(json!({ "errors": [{ "message": "boom" }], "data": null })).unwrap_err();
        assert!(matches!(err, Error::Api(_)));

        let no_data = graphql_data(json!({ "data": null })).unwrap_err();
        assert!(matches!(no_data, Error::Api(_)));
    }

    #[test]
    fn maps_issue_and_comment() {
        let issue = map_issue(json!({
            "number": 7, "title": "bug", "body": "broken", "state": "open",
            "html_url": "https://github.com/o/r/issues/7"
        }))
        .unwrap();
        assert_eq!(issue.number, 7);
        assert_eq!(issue.state, "open");

        let c = map_issue_comment(json!({
            "id": 99, "user": { "login": "u" }, "body": "hello",
            "created_at": "2026-01-01T00:00:00Z"
        }))
        .unwrap();
        assert_eq!(c.id, "99");
        assert_eq!(c.author, "u");
        assert!(c.path.is_none());
    }

    #[test]
    fn maps_repo_fixture_and_camel_case_shape() {
        let repo = map_repo(json!({
            "name": "react",
            "owner": { "login": "facebook" },
            "html_url": "https://github.com/facebook/react",
            "default_branch": "main",
            "created_at": "2013-05-24T16:15:54Z",
            "updated_at": "2026-01-02T03:04:05Z"
        }))
        .unwrap();
        assert_eq!(repo.owner, "facebook");
        assert_eq!(repo.name, "react");
        assert_eq!(
            repo.url.as_deref(),
            Some("https://github.com/facebook/react")
        );
        assert_eq!(repo.default_branch.as_deref(), Some("main"));
        let wire = serde_json::to_value(&repo).unwrap();
        assert_eq!(wire["url"], "https://github.com/facebook/react");
        assert_eq!(wire["defaultBranch"], "main");
        assert_eq!(wire["createdAt"], "2013-05-24T16:15:54Z");
        assert_eq!(wire["updatedAt"], "2026-01-02T03:04:05Z");
    }

    #[test]
    fn map_repo_omits_absent_optionals() {
        let repo = map_repo(json!({ "name": "r", "owner": { "login": "o" } })).unwrap();
        let wire = serde_json::to_value(&repo).unwrap();
        assert_eq!(wire["owner"], "o");
        assert_eq!(wire["name"], "r");
        assert!(wire.get("url").is_none());
        assert!(wire.get("defaultBranch").is_none());
        assert!(wire.get("createdAt").is_none());
        assert!(wire.get("updatedAt").is_none());
    }

    #[test]
    fn maps_branch_fixture() {
        let b = map_branch(json!({
            "name": "main",
            "commit": { "sha": "deadbeef" },
            "protected": true
        }))
        .unwrap();
        assert_eq!(b.name, "main");
        assert_eq!(b.commit_sha.as_deref(), Some("deadbeef"));
        assert!(b.protected);
        let wire = serde_json::to_value(&b).unwrap();
        assert_eq!(wire["commitSha"], "deadbeef");
        assert_eq!(wire["protected"], true);
    }

    #[test]
    fn branch_defaults_protected_and_omits_sha() {
        let b = map_branch(json!({ "name": "dev" })).unwrap();
        assert_eq!(b.name, "dev");
        assert!(!b.protected);
        let wire = serde_json::to_value(&b).unwrap();
        assert!(wire.get("commitSha").is_none());
        assert_eq!(wire["protected"], false);
    }

    #[test]
    fn rest_pagination_helpers() {
        // Cursor parsing: absent / garbage / sub-1 all start at page 1.
        assert_eq!(rest_page(None), 1);
        assert_eq!(rest_page(Some("bad")), 1);
        assert_eq!(rest_page(Some("0")), 1);
        assert_eq!(rest_page(Some("3")), 3);
        // Per-page is clamped into `1..=100` regardless of the requested limit.
        assert_eq!(rest_per_page(0), 1);
        assert_eq!(rest_per_page(50), 50);
        assert_eq!(rest_per_page(200), 100);
        // A full page yields the next cursor; a short (final) page does not.
        assert_eq!(rest_next_cursor(1, 100, 100).as_deref(), Some("2"));
        assert_eq!(rest_next_cursor(2, 40, 100), None);
        assert_eq!(rest_next_cursor(1, 0, 100), None);
    }

    #[test]
    fn maps_user_identity_and_omits_credentials() {
        let u = map_user_identity(json!({
            "login": "octocat",
            "id": 583231,
            "name": "The Octocat",
            "avatar_url": "https://avatars.githubusercontent.com/u/583231",
            "html_url": "https://github.com/octocat"
        }))
        .unwrap();
        assert_eq!(u.login, "octocat");
        assert_eq!(u.id, Some(583231));
        let wire = serde_json::to_value(&u).unwrap();
        assert_eq!(wire["login"], "octocat");
        assert_eq!(
            wire["avatarUrl"],
            "https://avatars.githubusercontent.com/u/583231"
        );
        assert_eq!(wire["htmlUrl"], "https://github.com/octocat");
    }

    #[test]
    fn user_identity_minimal_shape() {
        let u = map_user_identity(json!({ "login": "u" })).unwrap();
        let wire = serde_json::to_value(&u).unwrap();
        assert_eq!(wire["login"], "u");
        assert!(wire.get("id").is_none());
        assert!(wire.get("name").is_none());
        assert!(wire.get("avatarUrl").is_none());
        assert!(wire.get("htmlUrl").is_none());
    }

    #[test]
    fn rewrites_repo_search_query() {
        assert_eq!(
            build_repo_search_query("facebook/react"),
            "react user:facebook"
        );
        assert_eq!(build_repo_search_query("facebook/"), "user:facebook");
        assert_eq!(build_repo_search_query("/react"), "react");
        assert_eq!(build_repo_search_query("react"), "react");
        assert_eq!(
            build_repo_search_query("  vercel/next.js  "),
            "next.js user:vercel"
        );
        assert_eq!(build_repo_search_query("   "), "");
        assert_eq!(build_repo_search_query(""), "");
    }

    #[test]
    fn pr_involvement_serializes_kebab_case() {
        assert_eq!(
            serde_json::to_value(PrInvolvement::ReviewRequested).unwrap(),
            json!("review-requested")
        );
        assert_eq!(
            serde_json::to_value(PrInvolvement::Created).unwrap(),
            json!("created")
        );
        assert_eq!(
            serde_json::from_value::<PrInvolvement>(json!("involves")).unwrap(),
            PrInvolvement::Involves
        );
    }
}
