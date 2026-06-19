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
        source_branch: p.head.and_then(|r| r.r#ref).unwrap_or_default(),
        target_branch: p.base.and_then(|r| r.r#ref).unwrap_or_default(),
        author: login_of(&p.user),
        mergeable: p.mergeable,
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
    pub(super) struct GitRef {
        #[serde(rename = "ref")]
        pub r#ref: Option<String>,
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
query GetReviewThreads($owner: String!, $repo: String!, $prNumber: Int!) {
  repository(owner: $owner, name: $repo) {
    pullRequest(number: $prNumber) {
      reviewThreads(first: 100) {
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

    async fn list_prs(&self, repo: &RepoRef, query: PrQuery) -> Result<Vec<PullRequest>> {
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
        params.push(("per_page", query.limit.unwrap_or(30).to_string()));
        let route = Self::repo_path(repo, "/pulls");
        let v: Value = self.client.get(&route, Some(&params)).await?;
        let prs = map_list(v, map_pull)?;
        Ok(match &query.author {
            Some(author) => prs.into_iter().filter(|p| &p.author == author).collect(),
            None => prs,
        })
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
    ) -> Result<Vec<ReviewComment>> {
        let route = Self::repo_path(
            repo,
            &format!("/pulls/{number}/comments?per_page=100&sort=created&direction=desc"),
        );
        let v: Value = self.client.get(&route, None::<&()>).await?;
        map_list(v, map_review_comment)
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

    async fn get_review_threads(&self, repo: &RepoRef, number: u64) -> Result<Vec<ReviewThread>> {
        let payload = json!({
            "query": REVIEW_THREADS_QUERY,
            "variables": { "owner": repo.owner, "repo": repo.name, "prNumber": number },
        });
        let resp: Value = self.client.graphql(&payload).await?;
        let data = graphql_data(resp)?;
        let nodes = data
            .pointer("/repository/pullRequest/reviewThreads/nodes")
            .cloned()
            .unwrap_or_else(|| Value::Array(Vec::new()));
        map_list(nodes, map_review_thread)
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

    async fn list_issues(&self, repo: &RepoRef, query: IssueQuery) -> Result<Vec<Issue>> {
        let mut params: Vec<(&str, String)> = vec![(
            "state",
            query.state.clone().unwrap_or_else(|| "open".into()),
        )];
        if let Some(labels) = &query.labels {
            params.push(("labels", labels.clone()));
        }
        params.push(("per_page", query.limit.unwrap_or(30).to_string()));
        let route = Self::repo_path(repo, "/issues");
        let v: Value = self.client.get(&route, Some(&params)).await?;
        let items: Vec<Value> = serde_json::from_value(v)?;
        items
            .into_iter()
            .filter(|i| i.get("pull_request").is_none())
            .map(map_issue)
            .collect()
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
}
